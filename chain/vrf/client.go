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

package vrf

import (
	"context"

	"git.fd.io/govpp.git/api"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/networkservicemesh/api/pkg/api/networkservice"

	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/loopback"
	"github.com/networkservicemesh/sdk-vpp/pkg/tools/ifindex"
	"github.com/networkservicemesh/sdk/pkg/networkservice/core/next"
	"github.com/networkservicemesh/sdk/pkg/networkservice/utils/metadata"
)

type vrfClient struct {
	vppConn api.Connection

	v4 *vrfMap
	v6 *vrfMap
}

// NewServer creates a NetworkServiceServer chain element to create the ip table in vpp
func NewClient(vppConn api.Connection, opts ...Option) networkservice.NetworkServiceClient {
	o := &options{
		v4: CreateSharedMap(),
		v6: CreateSharedMap(),
	}

	for _, opt := range opts {
		opt(o)
	}

	return &vrfClient{
		vppConn:   vppConn,
		v4:  o.v4,
		v6:  o.v6,
	}
}

func (v *vrfClient) Request(ctx context.Context, request *networkservice.NetworkServiceRequest, opts ...grpc.CallOption) (*networkservice.Connection, error) {
	intAttached := false
	lbAttached := false
	networkService := request.GetConnection().NetworkService
	for	_, isIPv6 := range []bool{false, true} {
		t := v.v4
		if isIPv6 {
			t = v.v6
		}
		if _, ok := Load(ctx, metadata.IsClient(v), isIPv6); !ok {
			vrfId, loaded, err := create(ctx, v.vppConn, networkService, t, isIPv6);
			if err != nil {
				return nil, err
			}
			Store(ctx, metadata.IsClient(v), isIPv6, vrfId)
			lbAttached = loaded
		} else {
			intAttached = true
			lbAttached = true
		}
	}

	conn, err := next.Client(ctx).Request(ctx, request, opts...)
	if err != nil {
		v.delV46(ctx, networkService)
		return conn, err
	}

	if !lbAttached {
		/* Attach loopback to vrf-tables */
		if swIfIndex, ok := loopback.Load(ctx, metadata.IsClient(v)); ok {
			if err := attach(ctx, v.vppConn, swIfIndex, metadata.IsClient(v)); err != nil {
				return nil, err
			}
		}
	}
	if !intAttached {
		/* Attach interface to vrf-tables */
		if swIfIndex, ok := ifindex.Load(ctx, metadata.IsClient(v)); ok {
			if err := attach(ctx, v.vppConn, swIfIndex, metadata.IsClient(v)); err != nil {
				return nil, err
			}
		}
	}
	return conn, err
}

func (v *vrfClient) Close(ctx context.Context, conn *networkservice.Connection, opts ...grpc.CallOption) (*empty.Empty, error) {
	_, err := next.Client(ctx).Close(ctx, conn, opts...)

	v.delV46(ctx, conn.NetworkService)
	return &empty.Empty{}, err
}

/* Delete from ipv4 and ipv6 vrfs */
func (v *vrfClient) delV46(ctx context.Context, networkService string)  {
	for _, isIpV6 := range[]bool{false, true} {
		t := v.v4
		if isIpV6 {
			t = v.v6
		}
		del(ctx, v.vppConn, networkService, t, isIpV6, metadata.IsClient(v))
	}
}