/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Adversarial guard tests for the initialization (cold-start) seeding path.
//
// These tests assert the CORRECT behavior for a set of proven init-time defects.
// Because the underlying production bugs are NOT yet fixed at this revision, each
// guard is EXPECTED TO FAIL (RED). A guard turns GREEN when the bug is fixed and
// goes RED again if the fix is reverted — mirroring the committed RED guards in
// azure_servicegateway_guard_test.go.
//
// The init path (initialization.go) rebuilds K8s state from the live cluster on
// startup. It MUST apply the same admission filters the steady-state informer
// handlers apply, otherwise cold-start state diverges from runtime state.

package difftracker

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// newInitTestK8sState returns an empty K8sState suitable for the process* seeders.
func newInitTestK8sState(trackedServices ...string) K8sState {
	return K8sState{
		Services: utilsets.NewString(trackedServices...),
		Egresses: utilsets.NewString(),
		Nodes:    make(map[string]Node),
	}
}

// makeServiceOwnedEndpointSlice builds an EndpointSlice owned by the given Service UID,
// as extractServiceUIDFromEndpointSlice resolves ownership via the Service OwnerReference.
func makeServiceOwnedEndpointSlice(name, namespace, svcUID string, addrType discoveryv1.AddressType, endpoints []discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Service", Name: name, UID: types.UID(svcUID)},
			},
		},
		AddressType: addrType,
		Endpoints:   endpoints,
	}
}

// makeEgressPod builds an egress-labeled pod for the processK8sEgresses seeder.
func makeEgressPod(name, namespace, egressVal, nodeName, podIP string, phase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: egressVal},
		},
		Spec:   v1.PodSpec{NodeName: nodeName},
		Status: v1.PodStatus{Phase: phase, PodIP: podIP},
	}
}

// podIPInK8sState reports whether any node in K8s state tracks a pod at the given IP.
func podIPInK8sState(k8s *K8sState, podIP string) bool {
	for _, node := range k8s.Nodes {
		if _, ok := node.Pods[podIP]; ok {
			return true
		}
	}
	return false
}

// C5_UNREADY_EPS — processK8sEndpoints must SKIP EndpointSlice endpoints whose
// Conditions.Ready == false (a nil Ready must be treated as true). This mirrors the
// runtime informer filter in azure_local_services.go (`!ptr.Deref(ep.Conditions.Ready, true)`).
// Correct behavior: a not-ready endpoint's pod IP is NOT imported into K8s state at init.
func TestGuardInitAdversarial_C5_UnreadyEndpointsSkipped(t *testing.T) {
	const (
		svcUID     = "svc-c5-unready"
		nodeName   = "node-c5"
		nodeIP     = "10.0.0.5"
		readyIP    = "10.244.0.10"
		notReadyIP = "10.244.0.11"
	)

	k8s := newInitTestK8sState(svcUID)
	nodeNameToIPMap := map[string]string{nodeName: nodeIP}

	eps := makeServiceOwnedEndpointSlice("eps-c5", "default", svcUID, discoveryv1.AddressTypeIPv4, []discoveryv1.Endpoint{
		{
			Addresses:  []string{readyIP},
			NodeName:   ptr.To(nodeName),
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
		},
		{
			Addresses:  []string{notReadyIP},
			NodeName:   ptr.To(nodeName),
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)},
		},
	})
	kube := fake.NewSimpleClientset(eps)

	_, err := processK8sEndpoints(context.Background(), kube, &k8s, nodeNameToIPMap)
	assert.NoError(t, err)

	// Positive control: a ready endpoint MUST be imported.
	assert.True(t, podIPInK8sState(&k8s, readyIP),
		"ready endpoint must be imported into K8s state at init")

	// C5_UNREADY_EPS: a not-ready endpoint MUST be excluded from K8s state.
	assert.False(t, podIPInK8sState(&k8s, notReadyIP),
		"C5_UNREADY_EPS: endpoint with Conditions.Ready==false must NOT be imported into K8s state at init")
}

// C5b_EPS_ADDRTYPE — processK8sEndpoints must only import addresses whose IP family
// matches the EndpointSlice's declared AddressType. The runtime path
// (azure_local_services.go) enforces address-family discipline by rejecting slices
// whose AddressType does not match the family being processed; the init seeder must
// likewise not import an off-family address (an IPv6 literal from an IPv4 slice, or
// vice-versa).
//
// Two distinct slices (both owned by the same Service) keep the positive control and the
// guard assertion independent, so the test is robust whether the fix filters per-address
// or skips the whole off-family slice:
//   - a consistent slice (AddressType matches its address family) MUST import, and
//   - a contradictory slice (AddressType contradicts its address family) MUST NOT import.
func TestGuardInitAdversarial_C5b_EndpointAddressTypeFamilyFiltered(t *testing.T) {
	tests := []struct {
		name            string
		consistentType  discoveryv1.AddressType
		consistentIP    string // family matches consistentType (positive control: imported)
		contradictType  discoveryv1.AddressType
		contradictoryIP string // family contradicts contradictType (guard: NOT imported)
	}{
		{
			name:            "IPv4-declared slice must not import an IPv6 address",
			consistentType:  discoveryv1.AddressTypeIPv4,
			consistentIP:    "10.244.1.1",
			contradictType:  discoveryv1.AddressTypeIPv4,
			contradictoryIP: "fd00::1",
		},
		{
			name:            "IPv6-declared slice must not import an IPv4 address",
			consistentType:  discoveryv1.AddressTypeIPv6,
			consistentIP:    "fd00::2",
			contradictType:  discoveryv1.AddressTypeIPv6,
			contradictoryIP: "10.244.1.2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const (
				svcUID   = "svc-c5b-addrtype"
				nodeName = "node-c5b"
				nodeIP   = "10.0.0.6"
			)

			k8s := newInitTestK8sState(svcUID)
			nodeNameToIPMap := map[string]string{nodeName: nodeIP}

			consistentSlice := makeServiceOwnedEndpointSlice("eps-c5b-consistent", "default", svcUID, tc.consistentType, []discoveryv1.Endpoint{
				{
					Addresses:  []string{tc.consistentIP},
					NodeName:   ptr.To(nodeName),
					Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				},
			})
			contradictorySlice := makeServiceOwnedEndpointSlice("eps-c5b-contradictory", "default", svcUID, tc.contradictType, []discoveryv1.Endpoint{
				{
					Addresses:  []string{tc.contradictoryIP},
					NodeName:   ptr.To(nodeName),
					Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				},
			})
			kube := fake.NewSimpleClientset(consistentSlice, contradictorySlice)

			_, err := processK8sEndpoints(context.Background(), kube, &k8s, nodeNameToIPMap)
			assert.NoError(t, err)

			// Positive control: address matching its slice's AddressType family is imported.
			assert.True(t, podIPInK8sState(&k8s, tc.consistentIP),
				"address matching its slice AddressType family (%s) must be imported", tc.consistentType)

			// C5b_EPS_ADDRTYPE: address contradicting its slice's AddressType must NOT be imported.
			assert.False(t, podIPInK8sState(&k8s, tc.contradictoryIP),
				"C5b_EPS_ADDRTYPE: address not matching its slice AddressType (%s) must NOT be imported at init", tc.contradictType)
		})
	}
}

// C7_TERMINAL_PODS — processK8sEgresses must SKIP egress pods in a terminal Phase
// (Succeeded/Failed), matching the runtime informer gate in azure_servicegateway_pods.go
// (only PodRunning/PodPending are processed). Correct behavior: a terminal egress pod does
// not seed K8s egress state, the outbound ref-count, or any pod address.
func TestGuardInitAdversarial_C7_TerminalEgressPodsSkipped(t *testing.T) {
	tests := []struct {
		name  string
		phase v1.PodPhase
	}{
		{"PodSucceeded egress pod must be skipped", v1.PodSucceeded},
		{"PodFailed egress pod must be skipped", v1.PodFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const (
				egressVal = "egress-c7"
				nodeName  = "node-c7"
				nodeIP    = "10.0.0.7"
				podIP     = "10.244.2.2"
			)

			k8s := newInitTestK8sState()
			nodeNameToIPMap := map[string]string{nodeName: nodeIP}

			pod := makeEgressPod("pod-c7", "default", egressVal, nodeName, podIP, tc.phase)
			kube := fake.NewSimpleClientset(pod)

			_, err := processK8sEgresses(context.Background(), kube, &k8s, nodeNameToIPMap)
			assert.NoError(t, err)

			// C7_TERMINAL_PODS: a terminal egress pod must not be seeded anywhere.
			assert.False(t, k8s.Egresses.Has(egressVal),
				"C7_TERMINAL_PODS: terminal-phase (%s) egress pod must NOT be inserted into K8s egress state", tc.phase)
			assert.False(t, podIPInK8sState(&k8s, podIP),
				"C7_TERMINAL_PODS: terminal-phase (%s) egress pod IP must NOT be added to K8s state", tc.phase)
		})
	}
}

// C21_REFCOUNT_NEG — the outbound ref-counter must never be seeded NEGATIVE when an
// inbound LoadBalancer service UID collides with a pod egress label. The legacy
// localServiceNameToNRPServiceMap (which mixed the inbound -34 sentinel into the same map
// that fed the egress counter) has been removed; outboundIdentityPodRefCount is now seeded
// solely by New() from real egress pods, so a collision must yield the egress pod count (1),
// never a poisoned negative value that would block DeletePod's `counter <= 0` guard.
func TestGuardInitAdversarial_C21_RefCountNeverNegativeOnUIDCollision(t *testing.T) {
	const (
		collideID = "collide-c21" // serves as BOTH the inbound svc UID and the egress label
		nodeName  = "node-c21"
		nodeIP    = "10.0.0.21"
		podIP     = "10.244.3.3"
	)

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-c21",
			Namespace: "default",
			UID:       types.UID(collideID),
		},
		Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
	}
	pod := makeEgressPod("pod-c21", "default", collideID, nodeName, podIP, v1.PodRunning)
	kube := fake.NewSimpleClientset(svc, pod)

	k8s := newInitTestK8sState()
	nodeNameToIPMap := map[string]string{nodeName: nodeIP}

	// Build the cold-start K8s state: inbound LB service + colliding egress pod.
	_, _, err := processK8sServices(context.Background(), kube, &k8s)
	assert.NoError(t, err)
	_, err = processK8sEgresses(context.Background(), kube, &k8s, nodeNameToIPMap)
	assert.NoError(t, err)

	// New() seeds outboundIdentityPodRefCount from the egress pods now present in k8s state.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	nrp := NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(),
		Locations:     map[string]NRPLocation{},
	}
	cfg := Config{
		SubscriptionID:             "sub",
		ResourceGroup:              "rg",
		Location:                   "loc",
		ServiceGatewayResourceName: "sgw",
		ServiceGatewayID:           "/sgw/id",
		VNetName:                   "vnet",
	}
	dt, err := New(logr.Discard(), k8s, nrp, cfg, mock_azclient.NewMockClientFactory(ctrl), kube)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	v, ok := dt.outboundIdentityPodRefCount.Load(strings.ToLower(collideID))
	assert.True(t, ok, "C21_REFCOUNT_NEG: colliding egress identity must be seeded")
	assert.GreaterOrEqual(t, v.(int), 0,
		"C21_REFCOUNT_NEG: inbound-UID/egress-label collision must not seed a NEGATIVE outbound ref-count (got %v)", v)
	assert.Equal(t, 1, v.(int),
		"C21_REFCOUNT_NEG: outbound ref-count must equal the egress pod count (1), not a poisoned value")

	// C21_REFCOUNT_NEG (downstream effect): a correct (non-negative) counter must allow DeletePod
	// to complete; a poisoned negative counter would trip the `counter <= 0` guard and block removal.
	res := dt.DeletePod(collideID, nodeIP, podIP, "default", "pod-c21")
	assert.True(t, res.IsLastPod,
		"C21_REFCOUNT_NEG: with exactly one live egress pod the ref-count must be 1 (last pod); a poisoned negative counter blocks DeletePod")
	assert.False(t, podIPInK8sState(&dt.K8sResources, podIP),
		"C21_REFCOUNT_NEG: DeletePod must remove the egress pod; a poisoned negative counter must not block removal")
}
