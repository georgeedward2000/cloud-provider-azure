/*
Copyright 2026 The Kubernetes Authors.

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

package difftracker

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	discovery_v1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

func TestReconcileNodeIPChange(t *testing.T) {
	const (
		svcUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		podIP  = "1.1.1.1"
		oldLoc = "10.0.0.1"
		newLoc = "10.0.0.2"
	)

	// newTracker builds an engine that already tracks svcUID as an NRP load balancer, so
	// UpdateEndpoints applies the addresses to K8sResources synchronously (rather than buffering),
	// letting the tests assert the resulting desired state directly. It publishes the EndpointSlice
	// cache the reconciler replays.
	newTracker := func(t *testing.T) (*DiffTracker, *sync.Map) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		dt := seedDiffTracker(t, mock_azclient.NewMockClientFactory(ctrl), fake.NewSimpleClientset(),
			K8sState{Services: utilsets.NewString(svcUID), Egresses: utilsets.NewString(), Nodes: map[string]Node{}},
			NRPState{LoadBalancers: utilsets.NewString(svcUID), NATGateways: utilsets.NewString(), Locations: map[string]NRPLocation{}})
		cache := &sync.Map{}
		dt.SetEndpointSlicesCache(cache)
		return dt, cache
	}

	singleEndpointSlice := func() *discovery_v1.EndpointSlice {
		return &discovery_v1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "eps1",
				Namespace:       "test",
				OwnerReferences: []metav1.OwnerReference{{Kind: "Service", UID: types.UID(svcUID)}},
			},
			AddressType: discovery_v1.AddressTypeIPv4,
			Endpoints: []discovery_v1.Endpoint{{
				Addresses:  []string{podIP},
				NodeName:   ptr.To("node1"),
				Conditions: discovery_v1.EndpointConditions{Ready: ptr.To(true)},
			}},
		}
	}

	t.Run("node InternalIP change moves the pod off the stale location", func(t *testing.T) {
		dt, cache := newTracker(t)
		cache.Store("test/eps1", singleEndpointSlice())
		dt.UpdateEndpoints(svcUID, nil, map[string]string{podIP: oldLoc})

		dt.ReconcileNodeIPChange("node1", []string{oldLoc}, []string{newLoc})

		nodes := dt.K8sResources.Nodes
		assert.Contains(t, nodes, newLoc, "pod must move to the new node IP")
		assert.Contains(t, nodes[newLoc].Pods, podIP)
		if stale, ok := nodes[oldLoc]; ok {
			assert.NotContains(t, stale.Pods, podIP, "pod must be removed from the old node IP")
		}
	})

	t.Run("node addition registers a pod dropped while its node was uncached", func(t *testing.T) {
		dt, cache := newTracker(t)
		cache.Store("test/eps1", singleEndpointSlice())

		dt.ReconcileNodeIPChange("node1", nil, []string{oldLoc})

		nodes := dt.K8sResources.Nodes
		assert.Contains(t, nodes, oldLoc, "pod must be registered once its node appears")
		assert.Contains(t, nodes[oldLoc].Pods, podIP)
	})

	t.Run("node deletion drains the pod", func(t *testing.T) {
		dt, cache := newTracker(t)
		cache.Store("test/eps1", singleEndpointSlice())
		dt.UpdateEndpoints(svcUID, nil, map[string]string{podIP: oldLoc})

		dt.ReconcileNodeIPChange("node1", []string{oldLoc}, nil)

		if stale, ok := dt.K8sResources.Nodes[oldLoc]; ok {
			assert.NotContains(t, stale.Pods, podIP, "pod must drain when its node is deleted")
		}
	})

	// The reconcile must be nil-safe: node handlers may fire before the engine is constructed.
	t.Run("does not panic when the diff tracker is not yet initialized", func(t *testing.T) {
		var dt *DiffTracker
		assert.NotPanics(t, func() {
			dt.ReconcileNodeIPChange("node1", []string{oldLoc}, []string{newLoc})
		})
	})
}
