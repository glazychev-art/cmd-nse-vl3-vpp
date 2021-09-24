// Copyright (c) 2021 Doc.ai and/or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build linux

package main

import (
	"context"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/cidr"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/ipam/vl3ipam"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/ipcontext/ipaddress"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/ipcontext/routes"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/ipcontext/unnumbered"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/loopback"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/vrf"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/internal/utils"
	"github.com/networkservicemesh/sdk-vpp/pkg/networkservice/connectioncontext/mtu"
	"github.com/networkservicemesh/sdk-vpp/pkg/networkservice/mechanisms/memif"
	"github.com/networkservicemesh/sdk-vpp/pkg/networkservice/up"
	"github.com/networkservicemesh/sdk/pkg/networkservice/chains/client"
	"github.com/networkservicemesh/sdk/pkg/networkservice/common/mechanisms/recvfd"
	"github.com/networkservicemesh/sdk/pkg/networkservice/utils/metadata"
	"github.com/networkservicemesh/sdk/pkg/tools/token"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/edwarnicke/grpcfd"
	"github.com/edwarnicke/vpphelper"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/networkservicemesh/api/pkg/api/networkservice"
	registryapi "github.com/networkservicemesh/api/pkg/api/registry"
	"github.com/networkservicemesh/sdk-vpp/pkg/networkservice/tag"

	"github.com/networkservicemesh/sdk/pkg/networkservice/chains/endpoint"
	"github.com/networkservicemesh/sdk/pkg/networkservice/common/authorize"
	"github.com/networkservicemesh/sdk/pkg/networkservice/common/mechanisms"
	"github.com/networkservicemesh/sdk/pkg/networkservice/common/mechanisms/sendfd"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/chain"
	registryclient "github.com/networkservicemesh/sdk/pkg/registry/chains/client"
	"github.com/networkservicemesh/sdk/pkg/tools/debug"
	"github.com/networkservicemesh/sdk/pkg/tools/grpcutils"
	"github.com/networkservicemesh/sdk/pkg/tools/jaeger"
	"github.com/networkservicemesh/sdk/pkg/tools/log"
	"github.com/networkservicemesh/sdk/pkg/tools/log/logruslogger"
	"github.com/networkservicemesh/sdk/pkg/tools/opentracing"
	"github.com/networkservicemesh/sdk/pkg/tools/spiffejwt"
)

// Config holds configuration parameters from environment variables
type Config struct {
	Name             string            `default:"icmp-server" desc:"Name of ICMP Server"`
	DialTimeout      time.Duration     `default:"5s" desc:"timeout to dial NSMgr" split_words:"true"`
	RequestTimeout   time.Duration     `default:"15s" desc:"timeout to request NSE" split_words:"true"`
	ListenOn         string            `default:"listen.on.sock" desc:"listen on socket" split_words:"true"`
	ConnectTo        url.URL           `default:"unix:///var/lib/networkservicemesh/nsm.io.sock" desc:"url to connect to" split_words:"true"`
	MaxTokenLifetime time.Duration     `default:"10m" desc:"maximum lifetime of tokens" split_words:"true"`
	ServiceNames     []string          `default:"icmp-responder" desc:"Name of providing service" split_words:"true"`
	Payload          string            `default:"ETHERNET" desc:"Name of provided service payload" split_words:"true"`
	Labels           map[string]string `default:"" desc:"Endpoint labels"`
	CidrPrefix       string            `default:"169.254.0.0/16" desc:"CIDR Prefix to assign IPs from" split_words:"true"`
	PrefixLen        uint32            `default:"24" desc:"CIDR Prefix to assign IPs from" split_words:"true"`
	RegisterService  bool              `default:"true" desc:"if true then registers network service on startup" split_words:"true"`
}

// Process prints and processes env to config
func (c *Config) Process() error {
	if err := envconfig.Usage("nsm", c); err != nil {
		return errors.Wrap(err, "cannot show usage of envconfig nse")
	}
	if err := envconfig.Process("nsm", c); err != nil {
		return errors.Wrap(err, "cannot process envconfig nse")
	}
	return nil
}

type CloseFunc func()

func main() {
	// ********************************************************************************
	// setup context to catch signals
	// ********************************************************************************
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// ********************************************************************************
	// setup logging
	// ********************************************************************************
	logrus.SetLevel(logrus.TraceLevel)
	logrus.SetFormatter(&nested.Formatter{})
	ctx = log.WithFields(ctx, map[string]interface{}{"cmd": os.Args[0]})
	ctx = log.WithLog(ctx, logruslogger.New(ctx))

	if err := debug.Self(); err != nil {
		log.FromContext(ctx).Infof("%s", err)
	}

	// ********************************************************************************
	// Configure open tracing
	// ********************************************************************************
	log.EnableTracing(true)
	jaegerCloser := jaeger.InitJaeger(ctx, "cmd-nse-vl3-vpp")
	defer func() { _ = jaegerCloser.Close() }()

	// enumerating phases
	log.FromContext(ctx).Infof("there are 6 main phases which will be executed followed by a success message:")
	log.FromContext(ctx).Infof("the phases include:")
	log.FromContext(ctx).Infof("1: get config from environment")
	log.FromContext(ctx).Infof("2: run vpp and get a connection to it")
	log.FromContext(ctx).Infof("3: retrieve spiffe svid")
	log.FromContext(ctx).Infof("4: prepare shared resources")
	log.FromContext(ctx).Infof("5: get vl3-nses from registry")
	log.FromContext(ctx).Infof("6: connect, create and register nse with nsm (attempts in a loop)")
	log.FromContext(ctx).Infof("6.1: are we master or not")
	log.FromContext(ctx).Infof("6.2: create and register nse with nsm")
	log.FromContext(ctx).Infof("6.3: check if any other nses were registered while we were doing the previous steps")
	log.FromContext(ctx).Infof("6.3.1: check is master the same")
	log.FromContext(ctx).Infof("6.3.2: check if we need to make additional requests")
	log.FromContext(ctx).Infof("a final success message with start time duration")

	starttime := time.Now()

	// ********************************************************************************
	log.FromContext(ctx).Infof("executing phase 1: get config from environment")
	// ********************************************************************************
	config := new(Config)
	if err := config.Process(); err != nil {
		logrus.Fatal(err.Error())
	}

	log.FromContext(ctx).Infof("Config: %#v", config)

	// ********************************************************************************
	log.FromContext(ctx).Infof("executing phase 2: run vpp and get a connection to it")
	// ********************************************************************************
	vppConn, vppErrCh := vpphelper.StartAndDialContext(ctx)
	exitOnErr(ctx, cancel, vppErrCh)

	defer func() {
		cancel()
		<-vppErrCh
	}()

	// ********************************************************************************
	log.FromContext(ctx).Infof("executing phase 3: retrieving svid, check spire agent logs if this is the last line you see")
	// ********************************************************************************
	source, err := workloadapi.NewX509Source(ctx)
	if err != nil {
		logrus.Fatalf("error getting x509 source: %+v", err)
	}
	svid, err := source.GetX509SVID()
	if err != nil {
		logrus.Fatalf("error getting x509 svid: %+v", err)
	}
	log.FromContext(ctx).Infof("SVID: %q", svid.ID)


	// ********************************************************************************
	log.FromContext(ctx).Infof("executing phase 4: prepare shared between client and server resources")
	// ********************************************************************************
	var globalCIDR string
	var ownPrefixes []string
	var excludePrefixes []string

	var ipamPrefix *net.IPNet
	var ownIp *net.IPNet

	globalCIDR = config.CidrPrefix

	vrfOptions := []vrf.Option{
		vrf.WithSharedVrfV4(vrf.CreateSharedMap()),
		vrf.WithSharedVrfV6(vrf.CreateSharedMap()),
	}
	loopOptions:= []loopback.Option {
		loopback.WithSharedLoopbackMap(loopback.CreateLoopbackMap()),
	}

	var nseRequested []*registryapi.NetworkServiceEndpoint
	var nseList []*registryapi.NetworkServiceEndpoint
	var nseRegistration *registryapi.NetworkServiceEndpoint
	var closeFuncs []CloseFunc
	var masterConn *networkservice.Connection

	masterIndex := 0

	signalCtx, cancelSignalCtx := notifyContext(ctx)
	defer cancelSignalCtx()

	// ********************************************************************************
	log.FromContext(ctx).Infof("executing phase 5: get all vl3-nses from registry")
	// ********************************************************************************
	clientOptions := append(
		opentracing.WithTracingDial(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithTransportCredentials(
			grpcfd.TransportCredentials(
				credentials.NewTLS(
					tlsconfig.MTLSClientConfig(source, source, tlsconfig.AuthorizeAny()),
				),
			),
		),
	)

	nsName := config.ServiceNames[0]
	nseRegistryClient := registryclient.NewNetworkServiceEndpointRegistryClient(ctx, &config.ConnectTo, registryclient.WithDialOptions(clientOptions...))
	nseStream, err := nseRegistryClient.Find(ctx, &registryapi.NetworkServiceEndpointQuery{
		NetworkServiceEndpoint: &registryapi.NetworkServiceEndpoint{
			NetworkServiceNames: []string{nsName},
		},
	})
	if err != nil {
		log.FromContext(ctx).Fatalf("error getting nses: %+v", err)
	}
	nseList = utils.SortNses(utils.RemoveDuplicates(registryapi.ReadNetworkServiceEndpointList(nseStream)))

	// ********************************************************************************
	log.FromContext(ctx).Infof("executing phase 6: connect to existing nses and register itself")
	// ********************************************************************************
	vl3Client := createVl3Client(ctx, config, vppConn, source, loopOptions, vrfOptions)

	for {
		log.FromContext(ctx).Info("already requested nses:")
		printList(ctx, nseRequested)

		log.FromContext(ctx).Info("nses to request:")
		printList(ctx, nseList)

		if len(nseList) == 0 && masterConn == nil {
			// ********************************************************************************
			log.FromContext(ctx).Infof("executing phase 6.1: there are no vl3-endpoints. Let's appoint itself as a master")
			// ********************************************************************************
			ipamPrefix, ownPrefixes, err = utils.AllocateCidr(globalCIDR, config.PrefixLen)
			if err != nil {
				log.FromContext(ctx).Fatal(err)
			}
		} else {
			// ********************************************************************************
			log.FromContext(ctx).Infof("executing phase 6.1: connect to existing nses")
			// ********************************************************************************
			/* Sort all received nses to detect the master vl3-NSE */
			utils.SortNses(nseList)
			for i, nse := range nseList {
				if nseRegistration != nil && nse.Name == nseRegistration.Name {
					continue
				}

				log.FromContext(ctx).Infof("connect to %v", nse.String())

				request := &networkservice.NetworkServiceRequest{
					Connection: &networkservice.Connection{
						NetworkServiceEndpointName: nse.Name,
						NetworkService:             nsName,
						Payload:                    config.Payload,
					},
				}
				/* If we already have master connection, fill known fields */
				if masterConn != nil {
					ipContext := &networkservice.IPContext{}
					ipContext.ExtraPrefixes = append(ipContext.ExtraPrefixes, masterConn.GetContext().GetIpContext().ExtraPrefixes...)
					ipContext.SrcIpAddrs = append(ipContext.SrcIpAddrs, masterConn.GetContext().GetIpContext().SrcIpAddrs...)

					request.GetConnection().Context = &networkservice.ConnectionContext{
						IpContext: ipContext,
					}
				}

				/* Save requested nse even if request will be failed */
				nseRequested = append(nseRequested, nse)

				requestCtx, cancelRequest := context.WithTimeout(signalCtx, config.RequestTimeout)
				defer cancelRequest()

				conn, err := vl3Client.Request(requestCtx, request)
				if err != nil {
					if masterConn == nil && i == masterIndex {
						masterIndex++
					}
					log.FromContext(ctx).Errorf("request has failed: %v", err.Error())
					continue
				}

				/* Save master connection */
				if masterConn == nil && i == masterIndex {
					masterConn = conn.Clone()
				}

				/* If there is a route to somewhere, we don't need to allocate this prefix */
				for _, route := range conn.GetContext().GetIpContext().GetSrcRoutes() {
					excludePrefixes = append(excludePrefixes, route.Prefix)
				}

				closeFunc := func() {
					closeCtx, cancelClose := context.WithTimeout(ctx, config.RequestTimeout)
					defer cancelClose()
					_, _ = vl3Client.Close(closeCtx, conn)
				}
				closeFuncs = append(closeFuncs, closeFunc)
				defer closeFunc()
			}

			/* We definitely should have master connection on this point. If not - try again */
			if masterConn == nil {
				/* we already tried to connect to all of the endpoints */
				nseList = nil
				continue
			}

			/* Prepare prefixes for itself */
			_, ipamPrefix, err = net.ParseCIDR(masterConn.GetContext().GetIpContext().ExtraPrefixes[0])
			if err != nil {
				log.FromContext(ctx).Fatalf("error parsing extraPrefix: %+v", err)
			}
			ownPrefixes = append(ownPrefixes, masterConn.GetContext().GetIpContext().ExtraPrefixes[0])

			_, ipNet, err := net.ParseCIDR(masterConn.GetContext().GetIpContext().GetSrcIpAddrs()[0])
			if err != nil {
				log.FromContext(ctx).Fatalf("error parsing srcAddrs: %+v", err)
			}
			_, bits := ipNet.Mask.Size()
			ownIp = &net.IPNet{
				IP:   ipNet.IP,
				Mask: net.CIDRMask(bits, bits),
			}
		}

		if nseRegistration == nil {
			// ********************************************************************************
			log.FromContext(ctx).Infof("executing phase 6.2: create and register nse with nsm")
			// ********************************************************************************

			server := createVl3Endpoint(ctx, config, vppConn, source, loopOptions, vrfOptions, []string{globalCIDR}, ownPrefixes, excludePrefixes, ownIp, ipamPrefix)

			tmpDir, err := ioutil.TempDir("", config.Name)
			if err != nil {
				logrus.Fatalf("error creating tmpDir %+v", err)
			}
			defer func(tmpDir string) { _ = os.Remove(tmpDir) }(tmpDir)
			listenOn := &(url.URL{Scheme: "unix", Path: filepath.Join(tmpDir, config.ListenOn)})

			srvErrCh := grpcutils.ListenAndServe(ctx, listenOn, server)
			exitOnErr(ctx, cancel, srvErrCh)
			log.FromContext(ctx).Infof("grpc server started")

			if config.RegisterService {
				for _, serviceName := range config.ServiceNames {
					nsRegistryClient := registryclient.NewNetworkServiceRegistryClient(ctx, &config.ConnectTo, registryclient.WithDialOptions(clientOptions...))
					_, err = nsRegistryClient.Register(ctx, &registryapi.NetworkService{
						Name:    serviceName,
						Payload: config.Payload,
					})
					if err != nil {
						log.FromContext(ctx).Fatalf("unable to register ns %+v", err)
					}
				}
			}

			nseRegistration = &registryapi.NetworkServiceEndpoint{
				Name:                 config.Name,
				NetworkServiceNames:  config.ServiceNames,
				NetworkServiceLabels: make(map[string]*registryapi.NetworkServiceLabels),
				Url:                  listenOn.String(),
			}
			for _, serviceName := range config.ServiceNames {
				nseRegistration.NetworkServiceLabels[serviceName] = &registryapi.NetworkServiceLabels{Labels: config.Labels}
			}
			nseRegistration, err = nseRegistryClient.Register(ctx, nseRegistration)
			log.FromContext(ctx).Infof("registered nse: %+v", nseRegistration)

			if err != nil {
				log.FromContext(ctx).Fatalf("unable to register nse %+v", err)
			}
		}

		// ********************************************************************************
		log.FromContext(ctx).Infof("executing phase 6.3: check if any other endpoints were registered while we were doing the previous steps")
		// ********************************************************************************

		nseStream, err = nseRegistryClient.Find(ctx, &registryapi.NetworkServiceEndpointQuery{
			NetworkServiceEndpoint: &registryapi.NetworkServiceEndpoint{
				NetworkServiceNames: []string{nsName},
			},
		})
		if err != nil {
			log.FromContext(ctx).Fatalf("error getting nse: %+v", err)
		}
		nseListRegistry := utils.SortNses(utils.RemoveDuplicates(registryapi.ReadNetworkServiceEndpointList(nseStream)))

		// ********************************************************************************
		log.FromContext(ctx).Infof("executing phase 6.3.1: check is master the same")
		// ********************************************************************************

		if !utils.IsMasterTheSame(utils.SortNses(append(nseRequested, nseRegistration)), nseListRegistry) {
			// ********************************************************************************
			//  If master was changed we have no guarantees that CIDR allocation for NSEs is correct.
			//  In this case we need to close all connections and try again.
			//  Unregistration, closing connections and cleanup resources
			// ********************************************************************************
			log.FromContext(ctx).Infof("master is not the same. clean up and try again")

			_, _ = nseRegistryClient.Unregister(ctx, nseRegistration)
			nseRegistration = nil

			for _, closeFunc := range closeFuncs {
				closeFunc()
			}
			closeFuncs = nil

			masterConn = nil
			masterIndex = 0

			ipamPrefix = nil
			ownPrefixes = nil
			ownIp = nil

			nseRequested = nil
			nseList = nseListRegistry

			continue
		}

		// ********************************************************************************
		log.FromContext(ctx).Infof("executing phase 6.3.2: check if we need to make additional requests")
		// ********************************************************************************
		nseList = utils.ListsDiff(nseRequested, utils.GetListBeforeMe(nseListRegistry, nseRegistration))
		if len(nseList) != 0 {
			/* If we have a diff between requested nses and nses that were registered before us - do additional connections */
			log.FromContext(ctx).Infof("there were another nses registered. Connect to them")
			continue
		}
		break
	}

	// ********************************************************************************
	log.FromContext(ctx).Infof("startup completed in %v", time.Since(starttime))
	// ********************************************************************************
	// wait for server to exit
	<-signalCtx.Done()
	//<-vppErrCh
}

func createVl3Client(ctx context.Context, config *Config, vppConn vpphelper.Connection, source *workloadapi.X509Source,
	loopOpts []loopback.Option, vrfOpts []vrf.Option) networkservice.NetworkServiceClient {
	dialOptions := append(opentracing.WithTracingDial(),
		grpc.WithDefaultCallOptions(
			grpc.WaitForReady(true),
			grpc.PerRPCCredentials(token.NewPerRPCCredentials(spiffejwt.TokenGeneratorFunc(source, config.MaxTokenLifetime))),
		),
		grpc.WithTransportCredentials(
			grpcfd.TransportCredentials(
				credentials.NewTLS(
					tlsconfig.MTLSClientConfig(source, source, tlsconfig.AuthorizeAny()),
				),
			),
		),
		grpcfd.WithChainStreamInterceptor(),
		grpcfd.WithChainUnaryInterceptor(),
	)

	c := client.NewClient(
		ctx,
		client.WithClientURL(&config.ConnectTo),
		client.WithName(config.Name),
		client.WithAdditionalFunctionality(
			metadata.NewClient(),
			cidr.NewClient(config.PrefixLen, utils.GetIpFamily(config.CidrPrefix)),
			up.NewClient(ctx, vppConn, up.WithLoadSwIfIndex(loopback.Load)),
			ipaddress.NewClient(vppConn, ipaddress.WithLoadSwIfIndex(loopback.Load)),
			loopback.NewClient(vppConn, loopOpts...),
			up.NewClient(ctx, vppConn),
			mtu.NewClient(vppConn),
			routes.NewClient(vppConn),
			unnumbered.NewClient(vppConn),
			vrf.NewClient(vppConn, vrfOpts...),
			memif.NewClient(vppConn),
			sendfd.NewClient(),
			recvfd.NewClient(),
		),
		client.WithDialTimeout(config.DialTimeout),
		client.WithDialOptions(dialOptions...),
	)

	return c
}

func createVl3Endpoint(ctx context.Context, config *Config, vppConn vpphelper.Connection,
	source *workloadapi.X509Source, loopOpts []loopback.Option, vrfOpts []vrf.Option,
	globalCIDR, ownPrefixes, excludePrefixes []string, ownIp, ipamPrefix *net.IPNet) *grpc.Server {

	vl3Endpoint := endpoint.NewServer(ctx,
		spiffejwt.TokenGeneratorFunc(source, config.MaxTokenLifetime),
		endpoint.WithName(config.Name),
		endpoint.WithAuthorizeServer(authorize.NewServer()),
		endpoint.WithAdditionalFunctionality(
			cidr.NewServer(globalCIDR, ownPrefixes, excludePrefixes),
			vl3ipam.NewServer(ownIp, ipamPrefix),
			up.NewServer(ctx, vppConn, up.WithLoadSwIfIndex(loopback.Load)),
			ipaddress.NewServer(vppConn, ipaddress.WithLoadSwIfIndex(loopback.Load)),
			unnumbered.NewServer(vppConn),
			vrf.NewServer(vppConn, vrfOpts...),
			loopback.NewServer(vppConn, loopOpts...),
			mechanisms.NewServer(map[string]networkservice.NetworkServiceServer{
				memif.MECHANISM: chain.NewNetworkServiceServer(
					sendfd.NewServer(),
					up.NewServer(ctx, vppConn),
					mtu.NewServer(vppConn),
					routes.NewServer(vppConn),
					tag.NewServer(ctx, vppConn),
					memif.NewServer(vppConn),
				),
			}),
		),
	)

	options := append(
		opentracing.WithTracing(),
		grpc.Creds(
			grpcfd.TransportCredentials(
				credentials.NewTLS(
					tlsconfig.MTLSServerConfig(source, source, tlsconfig.AuthorizeAny()),
				),
			),
		),
	)
	server := grpc.NewServer(options...)
	vl3Endpoint.Register(server)

	return server
}

func printList(ctx context.Context, list []*registryapi.NetworkServiceEndpoint)  {
	for _, l := range list {
		log.FromContext(ctx).Info(l.String())
	}
}

func exitOnErr(ctx context.Context, cancel context.CancelFunc, errCh <-chan error) {
	// If we already have an error, log it and exit
	select {
	case err := <-errCh:
		log.FromContext(ctx).Fatal(err)
	default:
	}
	// Otherwise wait for an error in the background to log and cancel
	go func(ctx context.Context, errCh <-chan error) {
		err := <-errCh
		log.FromContext(ctx).Error(err)
		cancel()
	}(ctx, errCh)
}

func notifyContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		ctx,
		os.Interrupt,
		// More Linux signals here
		syscall.SIGHUP,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
}
