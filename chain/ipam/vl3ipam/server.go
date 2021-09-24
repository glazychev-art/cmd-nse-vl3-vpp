// Copyright (c) 2020-2021 Doc.ai and/or its affiliates.
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

// Package point2pointipam provides a p2p IPAM server chain element.
package vl3ipam

import (
	"context"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/ippool"
	"github.com/networkservicemesh/sdk/pkg/tools/log"
	"net"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"

	"github.com/networkservicemesh/api/pkg/api/networkservice"

	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
)

type info struct {
	/* IP */
	ipPool  *ippool.IPPool
	ip      string

	/* count - the number of clients using this IPs */
	count uint32
}

type serviceNameMap struct {
	/* entries - is a map[NetworkServiceName]{} */
	entries map[string]*info

	/* mutex for entries */
	mut sync.Mutex
}

type ipamServer struct {
	Map
	serviceMap  serviceNameMap
	ipPools  []*ippool.IPPool
	prefixes []*net.IPNet
	own      *net.IPNet
	once     sync.Once
	initErr  error
}

type connectionInfo struct {
	ipPool  *ippool.IPPool
	srcAddr string
	dstAddr string
}

func (i *connectionInfo) shouldUpdate(exclude *ippool.IPPool) bool {
	srcIP, _, srcErr := net.ParseCIDR(i.srcAddr)
	dstIP, _, dstErr := net.ParseCIDR(i.dstAddr)

	return srcErr == nil && exclude.ContainsString(srcIP.String()) || dstErr == nil && exclude.ContainsString(dstIP.String())
}

// NewServer - creates a new NetworkServiceServer chain element that implements IPAM service.
func NewServer(own *net.IPNet, prefixes ...*net.IPNet) networkservice.NetworkServiceServer {
	return &ipamServer{
		own: own,
		prefixes: prefixes,
		serviceMap: serviceNameMap{
			entries: make(map[string]*info),
		},
	}
}

func (s *ipamServer) init(conn *networkservice.Connection) {
	if len(s.prefixes) == 0 {
		s.initErr = errors.New("required one or more prefixes")
		return
	}

	for _, prefix := range s.prefixes {
		if prefix == nil {
			s.initErr = errors.Errorf("prefix must not be nil: %+v", s.prefixes)
			return
		}
		pool := ippool.NewWithNet(prefix)
		if s.own != nil {
			pool.Exclude(s.own)
			if conn != nil {
				s.serviceMap.entries[conn.GetNetworkService()] = &info{
					ipPool: pool,
					ip:     s.own.String(),
					count:  1,
				}
			}
		}
		s.ipPools = append(s.ipPools, pool)
	}
}

func (s *ipamServer) Request(ctx context.Context, request *networkservice.NetworkServiceRequest) (*networkservice.Connection, error) {
	s.once.Do(func() {
		s.init(request.GetConnection())
	})
	if s.initErr != nil {
		return nil, s.initErr
	}

	conn := request.GetConnection()
	if conn.GetContext() == nil {
		conn.Context = &networkservice.ConnectionContext{}
	}
	if conn.GetContext().GetIpContext() == nil {
		conn.GetContext().IpContext = &networkservice.IPContext{}
	}
	ipContext := conn.GetContext().GetIpContext()
	ipContext.SrcIpRequired = true

	excludeIP4, excludeIP6 := exclude(ipContext.GetExcludedPrefixes()...)

	if len(ipContext.ExtraPrefixes) != 0 {
		if len(ipContext.SrcIpAddrs) == 0 {
			_, ipNet, err := net.ParseCIDR(ipContext.ExtraPrefixes[0])
			if err != nil {
				return nil, err
			}

			addr, err := getAddrFromIpPool(ippool.NewWithNet(ipNet), excludeIP4, excludeIP6)
			if err != nil {
				return nil, err
			}
			addAddr(&ipContext.SrcIpAddrs, addr)
			addRoute(&ipContext.DstRoutes, addr)

		}
		ipContext.SrcIpRequired = false
	}

	nsName := request.GetConnection().GetNetworkService()
	connInfo, loaded := s.Load(conn.GetId())
	var err error
	if loaded && (connInfo.shouldUpdate(excludeIP4) || connInfo.shouldUpdate(excludeIP6)) {
		// some of the existing addresses are excluded
		deleteAddr(&ipContext.SrcIpAddrs, connInfo.srcAddr)
		deleteAddr(&ipContext.DstIpAddrs, connInfo.dstAddr)
		deleteRoute(&ipContext.SrcRoutes, connInfo.dstAddr)
		deleteRoute(&ipContext.DstRoutes, connInfo.srcAddr)
		s.free(connInfo, nsName)
		loaded = false
	}
	if !loaded {
		if connInfo, err = s.getAddr(conn.GetId(), nsName, excludeIP4, excludeIP6, false); err != nil {
			return nil, err
		}
		s.Store(conn.GetId(), connInfo)
		log.FromContext(ctx).Infof("=== RETURNED DstIp %v", connInfo.dstAddr)
		log.FromContext(ctx).Infof("=== RETURNED SrcIp %v", connInfo.srcAddr)


		if ipContext.SrcIpRequired {
			if connInfo, err = s.getAddr(conn.GetId(), nsName, excludeIP4, excludeIP6, true); err != nil {
				return nil, err
			}
			ipContext.SrcIpRequired = false
		}
		log.FromContext(ctx).Infof("=== RETURNED2 DstIp %v", connInfo.dstAddr)
		log.FromContext(ctx).Infof("=== RETURNED2 SrcIp %v", connInfo.srcAddr)
		s.Store(conn.GetId(), connInfo)
	}

	if connInfo.srcAddr != "" {
		addAddr(&ipContext.SrcIpAddrs, connInfo.srcAddr)
		addRoute(&ipContext.DstRoutes, connInfo.srcAddr)
	}

	if connInfo.dstAddr != "" {
		addAddr(&ipContext.DstIpAddrs, connInfo.dstAddr)
		addRoute(&ipContext.SrcRoutes, connInfo.dstAddr)
	}

	conn, err = next.Server(ctx).Request(ctx, request)

	if err != nil {
		if !loaded {
			s.free(connInfo, nsName)
		}
		return nil, err
	}

	return conn, nil
}

func (s *ipamServer) getP2PAddrs(excludeIP4, excludeIP6 *ippool.IPPool) (connInfo *connectionInfo, err error) {
	var dstAddr, srcAddr *net.IPNet
	for _, ipPool := range s.ipPools {
		if dstAddr, srcAddr, err = ipPool.PullP2PAddrs(excludeIP4, excludeIP6); err == nil {
			return &connectionInfo{
				ipPool:  ipPool,
				srcAddr: srcAddr.String(),
				dstAddr: dstAddr.String(),
			}, nil
		}
	}
	return nil, err
}

func (s *ipamServer) getAddr(connId, nsName string, excludeIP4, excludeIP6 *ippool.IPPool, isSrcIP bool) (connInfo *connectionInfo, err error) {
	s.serviceMap.mut.Lock()
	defer s.serviceMap.mut.Unlock()

	if !isSrcIP {
		println("==== getAddr !isSrcIP")
		inf, ok := s.serviceMap.entries[nsName]
		if ok {
			println("==== ok")
			connInfo, loaded := s.Load(connId)
			if !loaded {
				println("==== !loaded")
				connInfo = &connectionInfo{
					ipPool:  inf.ipPool,
					dstAddr: inf.ip,
				}
				inf.count++
			}
			return connInfo, nil
		}
	}

	var addr *net.IPNet
	for _, ipPool := range s.ipPools {
		if addr, err = ipPool.PullWithExclude(excludeIP4, excludeIP6); err == nil {
			connInfo, loaded := s.Load(connId)
			if !loaded {
				connInfo = &connectionInfo{
					ipPool:  ipPool,
				}
			}
			if isSrcIP {
				connInfo.srcAddr = addr.String()
			} else {
				connInfo.dstAddr = addr.String()
				s.serviceMap.entries[nsName] = &info{
					ip: connInfo.dstAddr,
					ipPool: connInfo.ipPool,
					count: 1,
				}
			}
			return connInfo, nil
		}
	}
	return nil, err
}

func getAddrFromIpPool(ipPool, excludeIP4, excludeIP6 *ippool.IPPool) (ip string, err error) {
	if addr, err := ipPool.PullWithExclude(excludeIP4, excludeIP6); err == nil {
		return addr.String(), nil
	}
	return "", err
}

func deleteRoute(routes *[]*networkservice.Route, prefix string) {
	for i, route := range *routes {
		if route.Prefix == prefix {
			*routes = append((*routes)[:i], (*routes)[i+1:]...)
			return
		}
	}
}

func addRoute(routes *[]*networkservice.Route, prefix string) {
	for _, route := range *routes {
		if route.Prefix == prefix {
			return
		}
	}
	*routes = append(*routes, &networkservice.Route{
		Prefix: prefix,
	})
}

func deleteAddr(addrs *[]string, addr string) {
	for i, a := range *addrs {
		if a == addr {
			*addrs = append((*addrs)[:i], (*addrs)[i+1:]...)
			return
		}
	}
}

func addAddr(addrs *[]string, addr string) {
	for _, a := range *addrs {
		if a == addr {
			return
		}
	}
	*addrs = append(*addrs, addr)
}

func (s *ipamServer) Close(ctx context.Context, conn *networkservice.Connection) (_ *empty.Empty, err error) {
	if s.initErr != nil {
		return nil, s.initErr
	}

	nsName := conn.GetNetworkService()
	if connInfo, ok := s.Load(conn.GetId()); ok {
		s.free(connInfo, nsName)
	}
	return next.Server(ctx).Close(ctx, conn)
}

func (s *ipamServer) free(connInfo *connectionInfo, nsName string) {
	connInfo.ipPool.AddNetString(connInfo.srcAddr)

	if connInfo.dstAddr != "" {
		s.serviceMap.mut.Lock()
		if info, ok := s.serviceMap.entries[nsName]; ok {
			info.count--
			if info.count == 0 {
				connInfo.ipPool.AddNetString(connInfo.dstAddr)
				delete(s.serviceMap.entries, nsName)
			}
		}
		s.serviceMap.mut.Unlock()
	}
}
