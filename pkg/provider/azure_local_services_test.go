/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/stretchr/testify/assert"

	"go.uber.org/mock/gomock"

	v1 "k8s.io/api/core/v1"
	discovery_v1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/backendaddresspoolclient/mock_backendaddresspoolclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/config"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

func TestLoadBalancerBackendPoolUpdater(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	addOperationPool1 := getAddIPsToBackendPoolOperation("ns1/svc1", "lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"})
	removeOperationPool1 := getRemoveIPsFromBackendPoolOperation("ns1/svc1", "lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"})
	addOperationPool2 := getAddIPsToBackendPoolOperation("ns1/svc1", "lb1", "pool2", []string{"10.0.0.1", "10.0.0.2"})

	testCases := []struct {
		name                               string
		operations                         []batchOperation
		existingBackendPools               []*armnetwork.BackendAddressPool
		expectedGetBackendPool             *armnetwork.BackendAddressPool
		extraWait                          bool
		notLocal                           bool
		changeLB                           bool
		removeOperationServiceName         string
		expectedCreateOrUpdateBackendPools []*armnetwork.BackendAddressPool
		expectedBackendPools               []*armnetwork.BackendAddressPool
	}{
		{
			name:       "Add node IPs to backend pool",
			operations: []batchOperation{addOperationPool1},
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			expectedCreateOrUpdateBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"}),
			},
			expectedBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"}),
			},
		},
		{
			name:       "Remove node IPs from backend pool",
			operations: []batchOperation{addOperationPool1, removeOperationPool1},
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			expectedCreateOrUpdateBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			expectedBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
		},
		{
			name:       "Multiple operations targeting different backend pools",
			operations: []batchOperation{addOperationPool1, addOperationPool2, removeOperationPool1},
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
				getTestBackendAddressPoolWithIPs("lb1", "pool2", []string{}),
			},
			expectedCreateOrUpdateBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
				getTestBackendAddressPoolWithIPs("lb1", "pool2", []string{"10.0.0.1", "10.0.0.2"}),
			},
			expectedBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
				getTestBackendAddressPoolWithIPs("lb1", "pool2", []string{"10.0.0.1", "10.0.0.2"}),
			},
		},
		{
			name:       "Multiple operations in two batches",
			operations: []batchOperation{addOperationPool1, removeOperationPool1},
			extraWait:  true,
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			expectedCreateOrUpdateBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"}),
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			expectedGetBackendPool: getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"}),
			expectedBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
		},
		{
			name:                       "remove operations by service name",
			operations:                 []batchOperation{addOperationPool1, removeOperationPool1},
			removeOperationServiceName: "ns1/svc1",
		},
		{
			name:       "not local service",
			operations: []batchOperation{addOperationPool1},
			notLocal:   true,
		},
		{
			name:       "not on this load balancer",
			operations: []batchOperation{addOperationPool1},
			changeLB:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			cloud := GetTestCloud(ctrl)
			cloud.localServiceNameToServiceInfoMap = sync.Map{}
			if !tc.notLocal {
				cloud.localServiceNameToServiceInfoMap.Store("ns1/svc1", &serviceInfo{lbName: "lb1"})
			}
			if tc.changeLB {
				cloud.localServiceNameToServiceInfoMap.Store("ns1/svc1", &serviceInfo{lbName: "lb2"})
			}
			svc := getTestService("svc1", v1.ProtocolTCP, nil, false)
			client := fake.NewSimpleClientset(&svc)
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			cloud.serviceLister = informerFactory.Core().V1().Services().Lister()
			mockbpClient := cloud.NetworkClientFactory.GetBackendAddressPoolClient().(*mock_backendaddresspoolclient.MockInterface)
			if len(tc.existingBackendPools) > 0 {
				mockbpClient.EXPECT().Get(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.existingBackendPools[0].Name,
				).Return(tc.existingBackendPools[0], nil)
			}
			if len(tc.existingBackendPools) == 2 {
				mockbpClient.EXPECT().Get(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.existingBackendPools[1].Name,
				).Return(tc.existingBackendPools[1], nil)
			}
			if tc.extraWait {
				mockbpClient.EXPECT().Get(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.expectedGetBackendPool.Name,
				).Return(tc.expectedGetBackendPool, nil)
			}
			if len(tc.expectedCreateOrUpdateBackendPools) > 0 {
				mockbpClient.EXPECT().CreateOrUpdate(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.expectedCreateOrUpdateBackendPools[0].Name,
					*tc.expectedCreateOrUpdateBackendPools[0],
				).Return(nil, nil)
			}
			if len(tc.existingBackendPools) == 2 || tc.extraWait {
				mockbpClient.EXPECT().CreateOrUpdate(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.expectedCreateOrUpdateBackendPools[1].Name,
					*tc.expectedCreateOrUpdateBackendPools[1],
				).Return(nil, nil)
			}

			u := newLoadBalancerBackendPoolUpdater(cloud, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Use WaitGroup to properly synchronize goroutine completion
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				u.run(ctx)
			}()

			results := sync.Map{}
			operationsDone := make(chan struct{})
			var operationsWg sync.WaitGroup

			for _, op := range tc.operations {
				op := op
				operationsWg.Add(1)
				go func() {
					defer operationsWg.Done()
					u.addOperation(op)
					result := op.wait()
					results.Store(result, true)
				}()
				// Small delay to ensure operations are properly queued
				time.Sleep(50 * time.Millisecond)
				if tc.extraWait {
					time.Sleep(time.Second)
				}
			}

			// Handle operation removal if specified
			if tc.removeOperationServiceName != "" {
				u.removeOperation(tc.removeOperationServiceName)
			}

			// Wait for all operations to complete with timeout
			go func() {
				operationsWg.Wait()
				close(operationsDone)
			}()

			select {
			case <-operationsDone:
				// Operations completed successfully
				// Allow extra time for backend processing
				time.Sleep(2 * time.Second)
			case <-time.After(8 * time.Second):
				// Timeout - cancel context and wait for cleanup
				t.Logf("Test timeout waiting for operations to complete")
			}

			// Ensure proper cleanup - cancel context and wait for goroutine
			cancel()
			wg.Wait()
		})
	}
}

func TestLoadBalancerBackendPoolUpdaterFailed(t *testing.T) {
	addOperationPool1 := getAddIPsToBackendPoolOperation("ns1/svc1", "lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"})

	testCases := []struct {
		name                               string
		operations                         []batchOperation
		existingBackendPools               []*armnetwork.BackendAddressPool
		expectedGetBackendPool             *armnetwork.BackendAddressPool
		getBackendPoolErr                  error
		putBackendPoolErr                  error
		expectedCreateOrUpdateBackendPools []*armnetwork.BackendAddressPool
		expectedBackendPools               []*armnetwork.BackendAddressPool
	}{
		{
			name:       "Non-retriable error when getting backend pool",
			operations: []batchOperation{addOperationPool1},
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			getBackendPoolErr: &azcore.ResponseError{ErrorCode: "error"},
			expectedBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
		},
		{
			name:       "Non-retriable error when updating backend pool",
			operations: []batchOperation{addOperationPool1},
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			expectedGetBackendPool: getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			putBackendPoolErr:      &azcore.ResponseError{ErrorCode: "error"},
			expectedCreateOrUpdateBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{"10.0.0.1", "10.0.0.2"}),
			},
		},
		{
			name:       "Backend pool not found",
			operations: []batchOperation{addOperationPool1},
			existingBackendPools: []*armnetwork.BackendAddressPool{
				getTestBackendAddressPoolWithIPs("lb1", "pool1", []string{}),
			},
			getBackendPoolErr: &azcore.ResponseError{StatusCode: http.StatusNotFound, ErrorCode: "error"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(_ *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cloud := GetTestCloud(ctrl)
			cloud.localServiceNameToServiceInfoMap = sync.Map{}
			cloud.localServiceNameToServiceInfoMap.Store("ns1/svc1", &serviceInfo{lbName: "lb1"})
			svc := getTestService("svc1", v1.ProtocolTCP, nil, false)
			client := fake.NewSimpleClientset(&svc)
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			cloud.serviceLister = informerFactory.Core().V1().Services().Lister()
			mockLBClient := cloud.NetworkClientFactory.GetBackendAddressPoolClient().(*mock_backendaddresspoolclient.MockInterface)
			mockBPClient := cloud.NetworkClientFactory.GetBackendAddressPoolClient().(*mock_backendaddresspoolclient.MockInterface)
			mockLBClient.EXPECT().Get(
				gomock.Any(),
				gomock.Any(),
				"lb1",
				*tc.existingBackendPools[0].Name,
			).Return(tc.existingBackendPools[0], tc.getBackendPoolErr)
			if len(tc.existingBackendPools) == 2 {
				mockLBClient.EXPECT().Get(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.existingBackendPools[1].Name,
				).Return(tc.existingBackendPools[1], nil)
			}
			if len(tc.expectedCreateOrUpdateBackendPools) > 0 {
				mockBPClient.EXPECT().CreateOrUpdate(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.expectedCreateOrUpdateBackendPools[0].Name,
					*tc.expectedCreateOrUpdateBackendPools[0],
				).Return(nil, tc.putBackendPoolErr)
			}
			if len(tc.expectedCreateOrUpdateBackendPools) == 2 {
				mockLBClient.EXPECT().CreateOrUpdate(
					gomock.Any(),
					gomock.Any(),
					"lb1",
					*tc.expectedCreateOrUpdateBackendPools[1].Name,
					*tc.expectedCreateOrUpdateBackendPools[1],
				).Return(nil, nil)
			}

			u := newLoadBalancerBackendPoolUpdater(cloud, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Use WaitGroup to properly synchronize goroutine completion
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				u.run(ctx)
			}()

			operationsDone := make(chan struct{})
			go func() {
				defer close(operationsDone)
				for _, op := range tc.operations {
					op := op
					u.addOperation(op)
					time.Sleep(50 * time.Millisecond)
				}
				// Allow time for processing
				time.Sleep(2 * time.Second)
			}()

			// Wait for operations to complete with timeout
			select {
			case <-operationsDone:
				// Operations completed successfully
			case <-time.After(8 * time.Second):
				// Timeout - cancel context
				t.Logf("Test timeout waiting for operations to complete")
			}

			// Ensure proper cleanup - cancel context and wait for goroutine
			cancel()
			wg.Wait()
		})
	}
}

func getTestBackendAddressPoolWithIPs(lbName, bpName string, ips []string) *armnetwork.BackendAddressPool {
	bp := &armnetwork.BackendAddressPool{
		ID:   ptr.To(fmt.Sprintf("/subscriptions/subscriptionID/resourceGroups/rg/providers/Microsoft.Network/loadBalancers/%s/backendAddressPools/%s", lbName, bpName)),
		Name: ptr.To(bpName),
		Properties: &armnetwork.BackendAddressPoolPropertiesFormat{
			VirtualNetwork: &armnetwork.SubResource{
				ID: ptr.To("/subscriptions/subscriptionID/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vnet"),
			},
			Location:                     ptr.To("eastus"),
			LoadBalancerBackendAddresses: []*armnetwork.LoadBalancerBackendAddress{},
		},
	}
	for _, ip := range ips {
		if len(ip) > 0 {
			bp.Properties.LoadBalancerBackendAddresses = append(bp.Properties.LoadBalancerBackendAddresses, &armnetwork.LoadBalancerBackendAddress{
				Name: ptr.To(""),
				Properties: &armnetwork.LoadBalancerBackendAddressPropertiesFormat{
					IPAddress: ptr.To(ip),
				},
			})
		}
	}
	return bp
}

func getTestEndpointSlice(name, namespace, svcName string, nodeNames ...string) *discovery_v1.EndpointSlice {
	endpoints := make([]discovery_v1.Endpoint, 0)
	for _, nodeName := range nodeNames {
		nodeName := nodeName
		endpoints = append(endpoints, discovery_v1.Endpoint{
			NodeName: &nodeName,
		})
	}
	return &discovery_v1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				consts.ServiceNameLabel: svcName,
			},
		},
		Endpoints: endpoints,
	}
}

func getTestEndpointSliceWithAddressesAndServiceOwnerReference(
	name, namespace, svcName string,
	svcUID types.UID,
	addresses []string,
	nodeNames ...string,
) *discovery_v1.EndpointSlice {
	if len(nodeNames) != len(addresses) {
		panic("nodeNames and addresses must have the same length")
	}

	endpointSlice := getTestEndpointSlice(name, namespace, svcName, nodeNames...)

	// Set addresses for each endpoint
	for i := range endpointSlice.Endpoints {
		endpointSlice.Endpoints[i].Addresses = []string{addresses[i]}
	}

	endpointSlice.OwnerReferences = []metav1.OwnerReference{
		{
			Kind: "Service",
			Name: svcName,
			UID:  svcUID,
		},
	}

	return endpointSlice
}

// TestGetPodIPToNodeIPMapFromEndpointSlice_NodeCacheRace runs the EndpointSlice reader concurrently
// with the node informer's cache writer under the race detector to verify that nodePrivateIPs access
// is synchronized by nodeCachesLock.
func TestGetPodIPToNodeIPMapFromEndpointSlice_NodeCacheRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	az := GetTestCloud(ctrl)

	const nodeName = "race-node"
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{Addresses: []v1.NodeAddress{
			{Type: v1.NodeInternalIP, Address: "10.0.0.5"},
		}},
	}
	es := getTestEndpointSlice("es-race", "default", "svc-race", nodeName)
	es.AddressType = discovery_v1.AddressTypeIPv4

	const iterations = 2000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			az.updateNodeCaches(nil, node)
			az.updateNodeCaches(node, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = az.getPodIPToNodeIPMapFromEndpointSlice(es, false)
		}
	}()
	wg.Wait()
}

func TestGetPodIPToNodeIPMapFromEndpointSlice_ReadinessFiltering(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	trueVal := true
	falseVal := false

	testCases := []struct {
		name           string
		endpointSlice  *discovery_v1.EndpointSlice
		ipv6           bool
		nodePrivateIPs map[string]*utilsets.IgnoreCaseSet
		expectedResult map[string]string
	}{
		{
			name: "Ready=true endpoints are included",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
			},
			expectedResult: map[string]string{"10.0.0.1": "192.168.1.1"},
		},
		{
			name: "Ready=false endpoints are filtered out",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &falseVal},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
			},
			expectedResult: map[string]string{},
		},
		{
			name: "Ready=nil endpoints are included (k8s contract: nil Ready means ready)",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: nil},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
			},
			expectedResult: map[string]string{"10.0.0.1": "192.168.1.1"},
		},
		{
			name: "Mixed readiness states - Ready=true and Ready=nil included, Ready=false excluded",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
					},
					{
						Addresses:  []string{"10.0.0.2"},
						NodeName:   ptr.To("node2"),
						Conditions: discovery_v1.EndpointConditions{Ready: &falseVal},
					},
					{
						Addresses:  []string{"10.0.0.3"},
						NodeName:   ptr.To("node3"),
						Conditions: discovery_v1.EndpointConditions{Ready: nil},
					},
					{
						Addresses:  []string{"10.0.0.4"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
				"node2": utilsets.NewString("192.168.1.2"),
				"node3": utilsets.NewString("192.168.1.3"),
			},
			expectedResult: map[string]string{
				"10.0.0.1": "192.168.1.1",
				"10.0.0.3": "192.168.1.3",
				"10.0.0.4": "192.168.1.1",
			},
		},
		{
			name: "Only explicit Ready=false excluded; Ready=nil treated as ready",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &falseVal},
					},
					{
						Addresses:  []string{"10.0.0.2"},
						NodeName:   ptr.To("node2"),
						Conditions: discovery_v1.EndpointConditions{Ready: nil},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
				"node2": utilsets.NewString("192.168.1.2"),
			},
			expectedResult: map[string]string{"10.0.0.2": "192.168.1.2"},
		},
		{
			name:           "Nil EndpointSlice returns empty map",
			endpointSlice:  nil,
			ipv6:           false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{},
			expectedResult: map[string]string{},
		},
		{
			name: "IPv6 endpoints with Ready=true are included",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv6,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"fd00::1"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
					},
				},
			},
			ipv6: true,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("fd00::100"),
			},
			expectedResult: map[string]string{"fd00::1": "fd00::100"},
		},
		{
			name: "Endpoint without NodeName is skipped",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1"},
						NodeName:   nil,
						Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
			},
			expectedResult: map[string]string{},
		},
		{
			name: "Malformed addresses are skipped, valid ones on the same endpoint are kept",
			endpointSlice: &discovery_v1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
				AddressType: discovery_v1.AddressTypeIPv4,
				Endpoints: []discovery_v1.Endpoint{
					{
						Addresses:  []string{"10.0.0.1", "not-an-ip"},
						NodeName:   ptr.To("node1"),
						Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
					},
				},
			},
			ipv6: false,
			nodePrivateIPs: map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("192.168.1.1"),
			},
			expectedResult: map[string]string{"10.0.0.1": "192.168.1.1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cloud := GetTestCloud(ctrl)
			cloud.nodePrivateIPs = tc.nodePrivateIPs

			result := cloud.getPodIPToNodeIPMapFromEndpointSlice(tc.endpointSlice, tc.ipv6)

			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

// A non-canonical IPv6 endpoint address or node InternalIP must be canonicalized so the resulting
// location keys match init (buildNodeNameToIPsMap/processK8sEndpoints) and NRP state; a raw key would
// diff as a spurious add plus delete against the canonical NRP location on reconcile/restart.
func TestGetPodIPToNodeIPMapFromEndpointSlice_CanonicalizesIPv6(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	trueVal := true
	es := &discovery_v1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: "eps1", Namespace: "default"},
		AddressType: discovery_v1.AddressTypeIPv6,
		Endpoints: []discovery_v1.Endpoint{
			{
				Addresses:  []string{"2001:DB8::0001"}, // uppercase + leading zeros
				NodeName:   ptr.To("node1"),
				Conditions: discovery_v1.EndpointConditions{Ready: &trueVal},
			},
		},
	}

	cloud := GetTestCloud(ctrl)
	cloud.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
		"node1": utilsets.NewString("2001:DB8::00AB"), // non-canonical node InternalIP
	}

	result := cloud.getPodIPToNodeIPMapFromEndpointSlice(es, true)

	assert.Equal(t, map[string]string{"2001:db8::1": "2001:db8::ab"}, result,
		"pod IP key and node IP value must be canonicalized to match init and NRP state")
}

func TestEndpointSlicesInformer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tc := range []struct {
		name                        string
		existingEPS                 *discovery_v1.EndpointSlice
		updatedEPS                  *discovery_v1.EndpointSlice
		notLocal                    bool
		expectedGetBackendPoolCount int
		expectedPutBackendPoolCount int
	}{
		{
			name:                        "remove unwanted ips and add wanted ones",
			existingEPS:                 getTestEndpointSlice("eps1", "test", "svc1", "node1"),
			updatedEPS:                  getTestEndpointSlice("eps1", "test", "svc1", "node2"),
			expectedGetBackendPoolCount: 1,
			expectedPutBackendPoolCount: 1,
		},
		{
			name:        "skip non-local services",
			existingEPS: getTestEndpointSlice("eps1", "test", "svc2", "node1"),
			updatedEPS:  getTestEndpointSlice("eps1", "test", "svc2", "node2"),
		},
		{
			name:        "skip an endpoint slice that don't belong to a service",
			existingEPS: getTestEndpointSlice("eps1", "test", "", "node1"),
			updatedEPS:  getTestEndpointSlice("eps1", "test", "", "node2"),
		},
		{
			name:        "not a local service",
			existingEPS: getTestEndpointSlice("eps1", "test", "", "node1"),
			updatedEPS:  getTestEndpointSlice("eps1", "test", "", "node2"),
			notLocal:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cloud := GetTestCloud(ctrl)
			cloud.localServiceNameToServiceInfoMap = sync.Map{}
			if !tc.notLocal {
				cloud.localServiceNameToServiceInfoMap.Store("test/svc1", &serviceInfo{lbName: "lb1"})
			}
			svc := getTestService("svc1", v1.ProtocolTCP, nil, false)
			client := fake.NewSimpleClientset(&svc, tc.existingEPS)
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			cloud.serviceLister = informerFactory.Core().V1().Services().Lister()
			cloud.LoadBalancerBackendPoolUpdateIntervalInSeconds = 1
			cloud.LoadBalancerSKU = consts.LoadBalancerSKUStandard
			cloud.MultipleStandardLoadBalancerConfigurations = []config.MultipleStandardLoadBalancerConfiguration{
				{
					Name: "lb1",
				},
			}
			cloud.localServiceNameToServiceInfoMap.Store("test/svc1", newServiceInfo(consts.IPVersionIPv4String, "lb1"))
			cloud.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("10.0.0.1"),
				"node2": utilsets.NewString("10.0.0.2"),
			}

			existingBackendPool := getTestBackendAddressPoolWithIPs("lb1", "test-svc1", []string{"10.0.0.1"})
			expectedBackendPool := getTestBackendAddressPoolWithIPs("lb1", "test-svc1", []string{"10.0.0.2"})
			mockLBClient := cloud.NetworkClientFactory.GetBackendAddressPoolClient().(*mock_backendaddresspoolclient.MockInterface)
			mockLBClient.EXPECT().Get(gomock.Any(), gomock.Any(), "lb1", "test-svc1").Return(existingBackendPool, nil).Times(tc.expectedGetBackendPoolCount)
			mockLBClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), "lb1", "test-svc1", *expectedBackendPool).Return(nil, nil).Times(tc.expectedPutBackendPoolCount)

			u := newLoadBalancerBackendPoolUpdater(cloud, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cloud.backendPoolUpdater = u

			// Use WaitGroup to properly synchronize goroutine completion
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				cloud.backendPoolUpdater.run(ctx)
			}()

			cloud.setUpEndpointSlicesInformer(informerFactory)
			stopChan := make(chan struct{})
			informerFactory.Start(stopChan)

			// Allow informer to initialize
			time.Sleep(100 * time.Millisecond)

			// Perform the update operation
			_, err := client.DiscoveryV1().EndpointSlices("test").Update(context.Background(), tc.updatedEPS, metav1.UpdateOptions{})
			assert.NoError(t, err)

			// Wait for operations to complete with timeout
			operationsDone := make(chan struct{})
			go func() {
				defer close(operationsDone)
				time.Sleep(2 * time.Second)
			}()

			select {
			case <-operationsDone:
				// Operations completed successfully
			case <-time.After(8 * time.Second):
				// Timeout
				t.Logf("Test timeout waiting for operations to complete")
			}

			// Cleanup - stop informer first, then cancel context and wait for goroutine
			close(stopChan)
			cancel()
			wg.Wait()
		})
	}
}

func TestEndpointSlicesInformerContainerLoadBalancer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	existingAddresses := []string{"1.1.1.1"}
	updatedAddresses := []string{"2.2.2.2", "3.3.3.3"}

	for _, tc := range []struct {
		name                        string
		existingEPS                 *discovery_v1.EndpointSlice
		updatedEPS                  *discovery_v1.EndpointSlice
		notLocal                    bool
		expectedGetBackendPoolCount int
		expectedPutBackendPoolCount int
	}{
		{
			name:                        "remove unwanted ips and add wanted ones",
			existingEPS:                 getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "svc1", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", existingAddresses, "node1"),
			updatedEPS:                  getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "svc1", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", updatedAddresses, "node2", "node1"),
			expectedGetBackendPoolCount: 1,
			expectedPutBackendPoolCount: 1,
		},
		// {
		// 	name:        "skip non-local services",
		// 	existingEPS: getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "svc2", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", existingAddresses, "node1"),
		// 	updatedEPS:  getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "svc2", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", updatedAddresses, "node2", "node1"),
		// },
		// {
		// 	name:        "skip an endpoint slice that don't belong to a service",
		// 	existingEPS: getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "", "", existingAddresses, "node1"),
		// 	updatedEPS:  getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "", "", updatedAddresses, "node2", "node1"),
		// },
		// {
		// 	name:        "not a local service",
		// 	existingEPS: getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "", "", existingAddresses, "node1"),
		// 	updatedEPS:  getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps1", "test", "", "", updatedAddresses, "node2", "node1"),
		// 	notLocal:    true,
		// },
	} {
		t.Run(tc.name, func(t *testing.T) {
			cloud := GetTestCloudWithContainerLoadBalancer(ctrl)

			k8s := difftracker.K8sState{
				Services: utilsets.NewString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				Egresses: utilsets.NewString(),
				Nodes:    make(map[string]difftracker.Node),
			}
			nrp := difftracker.NRPState{
				LoadBalancers: utilsets.NewString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				NATGateways:   utilsets.NewString(),
				Locations:     make(map[string]difftracker.NRPLocation),
				// map[string]difftracker.NRPLocation{
				// 	"10.0.0.1": {
				// 		Addresses: map[string]difftracker.NRPAddress{
				// 			"1.1.1.1": {
				// 				Services: utilsets.NewString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				// 			},
				// 		},
				// 	},
				// },
			}

			var err error
			cloud.diffTracker, err = difftracker.New(log.Noop(), k8s, nrp, difftracker.Config{
				SubscriptionID:             cloud.SubscriptionID,
				ResourceGroup:              cloud.ResourceGroup,
				Location:                   cloud.Location,
				VNetName:                   cloud.VnetName,
				ServiceGatewayResourceName: consts.DefaultServiceGatewayResourceName,
				ServiceGatewayID:           cloud.GetServiceGatewayID(),
			}, cloud.NetworkClientFactory, fake.NewSimpleClientset())
			if err != nil {
				t.Fatalf("failed to initialize diffTracker: %v", err)
			}

			cloud.localServiceNameToServiceInfoMap = sync.Map{}
			if !tc.notLocal {
				cloud.localServiceNameToServiceInfoMap.Store("test/svc1", &serviceInfo{lbName: "lb1"})
			}
			svc := getTestService("svc1", v1.ProtocolTCP, nil, false)
			client := fake.NewSimpleClientset(&svc, tc.existingEPS)
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			cloud.serviceLister = informerFactory.Core().V1().Services().Lister()
			cloud.localServiceNameToServiceInfoMap.Store("test/svc1", newServiceInfo(consts.IPVersionIPv4String, "lb1"))
			cloud.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("10.0.0.1"),
				"node2": utilsets.NewString("10.0.0.2"),
			}

			// Engine pattern is now wired (Phase 6 complete)
			// EndpointSlice informer calls diffTracker.UpdateEndpoints which handles buffering/state
			// ServiceUpdater and LocationsUpdater goroutines handle async resource creation

			cloud.setUpEndpointSlicesInformer(informerFactory)
			stopChan := make(chan struct{})
			defer func() {
				stopChan <- struct{}{}
			}()
			informerFactory.Start(stopChan)
			time.Sleep(100 * time.Millisecond)

			_, err = client.DiscoveryV1().EndpointSlices("test").Update(context.Background(), tc.updatedEPS, metav1.UpdateOptions{})
			assert.NoError(t, err)
			time.Sleep(2 * time.Second)
		})
	}
}

// TestSeedInboundEndpointsFromCache verifies that seedInboundEndpointsFromCache pushes the
// current ready endpoints of an inbound service into the engine from the EndpointSlice cache.
// This is the path that keeps a ClusterIP<->LoadBalancer type flip (which re-registers the
// service without any EndpointSlice change) from coming up with an empty backend pool.
func TestSeedInboundEndpointsFromCache(t *testing.T) {
	const svcUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	// newCloud builds a Cloud whose diffTracker already considers svcUID a registered NRP
	// load balancer, so UpdateEndpoints registers seeded addresses synchronously (rather than
	// only buffering them), letting us assert the result via GetSyncLocationsAddresses.
	newCloud := func(t *testing.T) *Cloud {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		cloud := GetTestCloudWithContainerLoadBalancer(ctrl)
		k8s := difftracker.K8sState{
			Services: utilsets.NewString(svcUID),
			Egresses: utilsets.NewString(),
			Nodes:    make(map[string]difftracker.Node),
		}
		nrp := difftracker.NRPState{
			LoadBalancers: utilsets.NewString(svcUID),
			NATGateways:   utilsets.NewString(),
			Locations:     make(map[string]difftracker.NRPLocation),
		}
		var err error
		cloud.diffTracker, err = difftracker.New(log.Noop(), k8s, nrp, difftracker.Config{
			SubscriptionID:             cloud.SubscriptionID,
			ResourceGroup:              cloud.ResourceGroup,
			Location:                   cloud.Location,
			VNetName:                   cloud.VnetName,
			ServiceGatewayResourceName: consts.DefaultServiceGatewayResourceName,
			ServiceGatewayID:           cloud.GetServiceGatewayID(),
		}, cloud.NetworkClientFactory, fake.NewSimpleClientset())
		if err != nil {
			t.Fatalf("failed to initialize diffTracker: %v", err)
		}
		cloud.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
			"node1": utilsets.NewString("10.0.0.1"),
			"node2": utilsets.NewString("10.0.0.2"),
		}
		return cloud
	}

	// ipv4Slice returns an IPv4 EndpointSlice owned by the given service UID. The base helper
	// leaves AddressType empty, which getPodIPToNodeIPMapFromEndpointSlice would skip, so it is
	// set explicitly here.
	ipv4Slice := func(ownerUID string) *discovery_v1.EndpointSlice {
		es := getTestEndpointSliceWithAddressesAndServiceOwnerReference(
			"eps1", "test", "svc1", types.UID(ownerUID),
			[]string{"1.1.1.1", "2.2.2.2"}, "node1", "node2")
		es.AddressType = discovery_v1.AddressTypeIPv4
		return es
	}

	t.Run("seeds endpoints for the matching service", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.endpointSlicesCache.Store("test/eps1", ipv4Slice(svcUID))

		cloud.seedInboundEndpointsFromCache(svcUID)

		ld := cloud.diffTracker.GetSyncLocationsAddresses()
		loc1, ok := ld.Locations["10.0.0.1"]
		assert.True(t, ok, "node1 IP should have a location entry")
		_, ok = loc1.Addresses["1.1.1.1"]
		assert.True(t, ok, "pod 1.1.1.1 should be registered on node1")
		loc2, ok := ld.Locations["10.0.0.2"]
		assert.True(t, ok, "node2 IP should have a location entry")
		addr2, ok := loc2.Addresses["2.2.2.2"]
		assert.True(t, ok, "pod 2.2.2.2 should be registered on node2")
		assert.True(t, addr2.ServiceRef.Has(svcUID), "address should reference the seeded service")
	})

	t.Run("ignores slices owned by a different service", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.endpointSlicesCache.Store("test/eps1", ipv4Slice("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"))

		cloud.seedInboundEndpointsFromCache(svcUID)

		ld := cloud.diffTracker.GetSyncLocationsAddresses()
		assert.Empty(t, ld.Locations, "no addresses should be registered when no slice matches the UID")
	})

	t.Run("is a no-op for an empty UID", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.endpointSlicesCache.Store("test/eps1", ipv4Slice(svcUID))

		cloud.seedInboundEndpointsFromCache("")

		ld := cloud.diffTracker.GetSyncLocationsAddresses()
		assert.Empty(t, ld.Locations, "an empty UID must not register anything")
	})
}

// TestReconcileServiceGatewayNodeIPChange checks that a node InternalIP change, addition, or deletion
// re-homes the affected inbound endpoints, since the EndpointSlice informer emits no event for a
// node-only change (a slice carries the endpoint's nodeName, not the node IP).
func TestReconcileServiceGatewayNodeIPChange(t *testing.T) {
	const (
		svcUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		podIP  = "1.1.1.1"
		oldLoc = "10.0.0.1"
		newLoc = "10.0.0.2"
	)

	// newCloud builds a Cloud whose diffTracker already tracks svcUID as an NRP load balancer, so
	// UpdateEndpoints applies the addresses to K8sResources synchronously (rather than buffering),
	// letting the tests assert the resulting desired state directly.
	newCloud := func(t *testing.T) *Cloud {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		cloud := GetTestCloudWithContainerLoadBalancer(ctrl)
		var err error
		cloud.diffTracker, err = difftracker.New(log.Noop(), difftracker.K8sState{
			Services: utilsets.NewString(svcUID),
			Egresses: utilsets.NewString(),
			Nodes:    make(map[string]difftracker.Node),
		}, difftracker.NRPState{
			LoadBalancers: utilsets.NewString(svcUID),
			NATGateways:   utilsets.NewString(),
			Locations:     make(map[string]difftracker.NRPLocation),
		}, difftracker.Config{
			SubscriptionID:             cloud.SubscriptionID,
			ResourceGroup:              cloud.ResourceGroup,
			Location:                   cloud.Location,
			VNetName:                   cloud.VnetName,
			ServiceGatewayResourceName: consts.DefaultServiceGatewayResourceName,
			ServiceGatewayID:           cloud.GetServiceGatewayID(),
		}, cloud.NetworkClientFactory, fake.NewSimpleClientset())
		if err != nil {
			t.Fatalf("failed to initialize diffTracker: %v", err)
		}
		return cloud
	}

	singleEndpointSlice := func() *discovery_v1.EndpointSlice {
		es := getTestEndpointSliceWithAddressesAndServiceOwnerReference(
			"eps1", "test", "svc1", types.UID(svcUID), []string{podIP}, "node1")
		es.AddressType = discovery_v1.AddressTypeIPv4
		return es
	}

	t.Run("node InternalIP change moves the pod off the stale location", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.endpointSlicesCache.Store("test/eps1", singleEndpointSlice())
		cloud.diffTracker.UpdateEndpoints(svcUID, nil, map[string]string{podIP: oldLoc})

		cloud.reconcileServiceGatewayNodeIPChange("node1", []string{oldLoc}, []string{newLoc})

		nodes := cloud.diffTracker.K8sResources.Nodes
		assert.Contains(t, nodes, newLoc, "pod must move to the new node IP")
		assert.Contains(t, nodes[newLoc].Pods, podIP)
		if stale, ok := nodes[oldLoc]; ok {
			assert.NotContains(t, stale.Pods, podIP, "pod must be removed from the old node IP")
		}
	})

	t.Run("node addition registers a pod dropped while its node was uncached", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.endpointSlicesCache.Store("test/eps1", singleEndpointSlice())

		cloud.reconcileServiceGatewayNodeIPChange("node1", nil, []string{oldLoc})

		nodes := cloud.diffTracker.K8sResources.Nodes
		assert.Contains(t, nodes, oldLoc, "pod must be registered once its node appears")
		assert.Contains(t, nodes[oldLoc].Pods, podIP)
	})

	t.Run("node deletion drains the pod", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.endpointSlicesCache.Store("test/eps1", singleEndpointSlice())
		cloud.diffTracker.UpdateEndpoints(svcUID, nil, map[string]string{podIP: oldLoc})

		cloud.reconcileServiceGatewayNodeIPChange("node1", []string{oldLoc}, nil)

		if stale, ok := cloud.diffTracker.K8sResources.Nodes[oldLoc]; ok {
			assert.NotContains(t, stale.Pods, podIP, "pod must drain when its node is deleted")
		}
	})

	t.Run("is a no-op when ServiceGateway is disabled", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.ServiceGatewayEnabled = false
		cloud.endpointSlicesCache.Store("test/eps1", singleEndpointSlice())
		cloud.diffTracker.UpdateEndpoints(svcUID, nil, map[string]string{podIP: oldLoc})

		cloud.reconcileServiceGatewayNodeIPChange("node1", []string{oldLoc}, []string{newLoc})

		assert.NotContains(t, cloud.diffTracker.K8sResources.Nodes, newLoc, "disabled SGW must not touch difftracker state")
	})

	// A node event can fire before InitializeCloudFromConfig assigns az.diffTracker (see the
	// ordering note in SetInformers), so the reconcile must be nil-safe.
	t.Run("does not panic when the difftracker is not yet initialized", func(t *testing.T) {
		cloud := newCloud(t)
		cloud.diffTracker = nil
		cloud.endpointSlicesCache.Store("test/eps1", singleEndpointSlice())

		assert.NotPanics(t, func() {
			cloud.reconcileServiceGatewayNodeIPChange("node1", []string{oldLoc}, []string{newLoc})
		})
	})
}

func TestGetBackendPoolNamesAndIDsForService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cloud := GetTestCloud(ctrl)
	cloud.MultipleStandardLoadBalancerConfigurations = []config.MultipleStandardLoadBalancerConfiguration{
		{},
	}
	svc := getTestService("test", v1.ProtocolTCP, nil, false)
	svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
	_ = cloud.getBackendPoolNamesForService(&svc, "test")
	_ = cloud.getBackendPoolIDsForService(&svc, "test", "lb")
}

func TestCheckAndApplyLocalServiceBackendPoolUpdates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tc := range []struct {
		description string
		existingEPS *discovery_v1.EndpointSlice
	}{
		{
			description: "should update backend pool as expected",
			existingEPS: getTestEndpointSlice("eps1", "default", "svc1", "node2"),
		},
		{
			description: "should not report an error if failed to get the endpointslice",
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			cloud := GetTestCloud(ctrl)
			cloud.localServiceNameToServiceInfoMap.Store("default/svc1", &serviceInfo{lbName: "lb1"})
			svc := getTestService("svc1", v1.ProtocolTCP, nil, false)
			client := fake.NewSimpleClientset(&svc)
			cloud.KubeClient = client
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			cloud.serviceLister = informerFactory.Core().V1().Services().Lister()
			cloud.LoadBalancerBackendPoolUpdateIntervalInSeconds = 1
			cloud.LoadBalancerSKU = consts.LoadBalancerSKUStandard
			cloud.MultipleStandardLoadBalancerConfigurations = []config.MultipleStandardLoadBalancerConfiguration{
				{
					Name: "lb1",
				},
			}
			cloud.localServiceNameToServiceInfoMap.Store("default/svc1", newServiceInfo(consts.IPVersionIPv4String, "lb1"))
			cloud.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
				"node1": utilsets.NewString("10.0.0.1", "fd00::1"),
				"node2": utilsets.NewString("10.0.0.2", "fd00::2"),
			}
			if tc.existingEPS != nil {
				cloud.endpointSlicesCache.Store(fmt.Sprintf("%s/%s", tc.existingEPS.Name, tc.existingEPS.Namespace), tc.existingEPS)
			}

			existingBackendPool := getTestBackendAddressPoolWithIPs("lb1", "default-svc1", []string{"10.0.0.1"})
			existingBackendPoolIPv6 := getTestBackendAddressPoolWithIPs("lb1", "default-svc1-ipv6", []string{"fd00::1"})
			existingLB := armnetwork.LoadBalancer{
				Name: ptr.To("lb1"),
				Properties: &armnetwork.LoadBalancerPropertiesFormat{
					BackendAddressPools: []*armnetwork.BackendAddressPool{
						existingBackendPool,
						existingBackendPoolIPv6,
					},
				},
			}
			expectedBackendPool := getTestBackendAddressPoolWithIPs("lb1", "default-svc1", []string{"10.0.0.2"})
			expectedBackendPoolIPv6 := getTestBackendAddressPoolWithIPs("lb1", "default-svc1-ipv6", []string{"fd00::2"})
			mockLBClient := cloud.NetworkClientFactory.GetBackendAddressPoolClient().(*mock_backendaddresspoolclient.MockInterface)
			if tc.existingEPS != nil {
				mockLBClient.EXPECT().Get(gomock.Any(), gomock.Any(), "lb1", "default-svc1").Return(existingBackendPool, nil)
				mockLBClient.EXPECT().Get(gomock.Any(), gomock.Any(), "lb1", "default-svc1-ipv6").Return(existingBackendPoolIPv6, nil)
				mockLBClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), "lb1", "default-svc1", *expectedBackendPool).Return(nil, nil)
				mockLBClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), "lb1", "default-svc1-ipv6", *expectedBackendPoolIPv6).Return(nil, nil)
			}

			u := newLoadBalancerBackendPoolUpdater(cloud, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cloud.backendPoolUpdater = u

			// Use WaitGroup to properly synchronize goroutine completion
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				cloud.backendPoolUpdater.run(ctx)
			}()

			if tc.existingEPS != nil {
				_, _ = client.DiscoveryV1().EndpointSlices("default").Create(context.Background(), tc.existingEPS, metav1.CreateOptions{})
			}

			err := cloud.checkAndApplyLocalServiceBackendPoolUpdates(existingLB, &svc)
			assert.NoError(t, err)

			// Wait for operations to complete with timeout
			operationsDone := make(chan struct{})
			go func() {
				defer close(operationsDone)
				time.Sleep(2 * time.Second)
			}()

			select {
			case <-operationsDone:
				// Operations completed successfully
			case <-time.After(8 * time.Second):
				// Timeout
				t.Logf("Test timeout waiting for operations to complete")
			}

			// Ensure proper cleanup - cancel context and wait for goroutine
			cancel()
			wg.Wait()
		})
	}
}

func TestServiceGatewayEndpointSliceInformer_TracksService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getTestService("clb-wiring-eps", v1.ProtocolTCP, nil, false, 80)
	svc.Namespace = "test"
	serviceUID := svc.UID

	existingEPS := getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps-sgw", "test", svc.Name, serviceUID, []string{"10.0.0.1"}, "node1")
	updatedEPS := getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps-sgw", "test", svc.Name, serviceUID, []string{"10.0.0.2"}, "node2")
	updatedEPS.ResourceVersion = "2"
	updatedEPS.AddressType = discovery_v1.AddressTypeIPv4

	kubeClient := fake.NewSimpleClientset(&svc, existingEPS)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	az.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
		"node1": utilsets.NewString("192.168.0.1"),
		"node2": utilsets.NewString("192.168.0.2"),
	}

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	az.serviceLister = informerFactory.Core().V1().Services().Lister()
	az.setUpEndpointSlicesInformer(informerFactory)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)

	_, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = kubeClient.DiscoveryV1().EndpointSlices("test").Update(context.Background(), updatedEPS, metav1.UpdateOptions{})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	time.Sleep(200 * time.Millisecond)

	assert.True(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)))
}
