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

package utils

import (
	"net"
	"sort"

	"github.com/pkg/errors"

	"github.com/networkservicemesh/api/pkg/api/networkservice"
	registryapi "github.com/networkservicemesh/api/pkg/api/registry"
	"github.com/networkservicemesh/cmd-nse-vl3-vpp/chain/prefixpool"
)

func RemoveDuplicates(nseList []*registryapi.NetworkServiceEndpoint) []*registryapi.NetworkServiceEndpoint {
	allKeys := make(map[string]bool)
	list := []*registryapi.NetworkServiceEndpoint{}
	for _, item := range nseList {
		if _, value := allKeys[item.Name]; !value {
			allKeys[item.Name] = true
			list = append(list, item)
		}
	}
	return list
}

/* Received sorted lists */
func IsMasterTheSame(nseList1, nseList2 []*registryapi.NetworkServiceEndpoint) bool {
	if len(nseList1) == 0 || len(nseList2) == 0 {
		return false
	}
	return nseList1[0].Name == nseList2[0].Name && nseList1[0].InitialRegistrationTime.AsTime().Equal(nseList2[0].InitialRegistrationTime.AsTime())
}

/* Received sorted lists */
func GetListBeforeMe(nseList []*registryapi.NetworkServiceEndpoint, me *registryapi.NetworkServiceEndpoint) []*registryapi.NetworkServiceEndpoint {
	var beforeMe []*registryapi.NetworkServiceEndpoint
	for _, nse := range nseList {
		if nse.InitialRegistrationTime.AsTime().After(me.InitialRegistrationTime.AsTime()) || nse.Name == me.Name {
			break
		}
		beforeMe = append(beforeMe, nse)
	}

	return beforeMe
}

/* Received sorted lists */
func ListsDiff(requestedList, newList []*registryapi.NetworkServiceEndpoint) (ret []*registryapi.NetworkServiceEndpoint) {
	for _, nseN := range newList {
		found := false
		for _, nseR := range requestedList {
			if nseN.Name == nseR.Name && nseN.InitialRegistrationTime.AsTime().Equal(nseR.InitialRegistrationTime.AsTime()){
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, nseN)
		}
	}
	return
}

func SortNses(list []*registryapi.NetworkServiceEndpoint) []*registryapi.NetworkServiceEndpoint {
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].InitialRegistrationTime != nil {
			if list[j].InitialRegistrationTime != nil {
				return list[i].InitialRegistrationTime.AsTime().Before(list[j].InitialRegistrationTime.AsTime())
			}
			return true
		}
		return false
	})
	return list
}

func GetIpFamily(cidr string) networkservice.IpFamily_Family {
	if _, ipam, err := net.ParseCIDR(cidr); err == nil && ipam.IP.To4() == nil {
		return networkservice.IpFamily_IPV6
	}
	return networkservice.IpFamily_IPV4
}


func AllocateCidr(cidr string, prefixLen uint32) (ipam *net.IPNet, ownCidr []string, err error) {
	ipFamily := GetIpFamily(cidr)
	requested, _, err := prefixpool.ExtractPrefixes([]string{cidr}, &networkservice.ExtraPrefixRequest{
		AddrFamily:      &networkservice.IpFamily{Family: ipFamily},
		PrefixLen:       prefixLen,
		RequiredNumber:  1,
		RequestedNumber: 1,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "error extracting own prefix")
	}
	ownCidr = append(ownCidr, requested...)

	_, ipam, err = net.ParseCIDR(ownCidr[0])
	if err != nil {
		return nil, nil, errors.Wrap(err, "error parsing cidr")
	}
	return
}