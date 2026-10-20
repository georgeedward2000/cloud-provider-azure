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
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// TestPodInformerAddPod tests the podInformerAddPod function
func TestPodInformerAddPod(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name           string
		pod            *v1.Pod
		expectAddPod   bool
		expectedCalls  int
		expectedPodKey string
		expectedEgress string
		expectedHostIP string
		expectedPodIP  string
	}{
		{
			name:           "Valid pod with egress label and IPs should trigger AddPod",
			pod:            newTestPod("default", "test-pod", "egress-gateway-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectAddPod:   true,
			expectedCalls:  1,
			expectedPodKey: "default/test-pod",
			expectedEgress: "egress-gateway-a",
			expectedHostIP: "10.0.0.1",
			expectedPodIP:  "10.0.1.1",
		},
		{
			name:           "Pod in Pending phase with IPs should trigger AddPod",
			pod:            newTestPod("default", "pending-pod", "egress-b", "10.0.0.2", "10.0.1.2", v1.PodPending, nil),
			expectAddPod:   true,
			expectedCalls:  1,
			expectedPodKey: "default/pending-pod",
			expectedEgress: "egress-b",
		},
		{
			name:          "Pod without egress label should be skipped",
			pod:           newTestPod("default", "no-label", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod with DeletionTimestamp should be skipped",
			pod:           newTestPod("default", "deleting", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, &now),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod in Failed phase should be skipped",
			pod:           newTestPod("default", "failed-pod", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod in Succeeded phase should be skipped",
			pod:           newTestPod("default", "succeeded-pod", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodSucceeded, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod in Unknown phase should be skipped",
			pod:           newTestPod("default", "unknown-pod", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodUnknown, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod without HostIP should be skipped",
			pod:           newTestPod("default", "no-hostip", "egress-a", "", "10.0.1.1", v1.PodRunning, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod without PodIP should be skipped",
			pod:           newTestPod("default", "no-podip", "egress-a", "10.0.0.1", "", v1.PodRunning, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod without any IPs should be skipped",
			pod:           newTestPod("default", "no-ips", "egress-a", "", "", v1.PodPending, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:           "Egress label should be case-insensitive (converted to lowercase)",
			pod:            newTestPod("default", "case-test", "Egress-Gateway-UPPER", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectAddPod:   true,
			expectedCalls:  1,
			expectedEgress: "egress-gateway-upper",
		},
		{
			name:          "Pod with path-traversal egress label should be skipped",
			pod:           newTestPod("default", "evil-pod", "../hijacked-nat", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
		{
			name:          "Pod with slash in egress label should be skipped",
			pod:           newTestPod("default", "slash-pod", "egress/gw", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectAddPod:  false,
			expectedCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			dt := newProviderDiffTracker(t, ctrl, fake.NewSimpleClientset(tt.pod))

			dt.podInformerAddPod(tt.pod)

			// On an empty engine a registered egress pod is buffered under pendingPods, keyed by its
			// (lowercased) egress identity and carrying the node location (HostIP) and address (PodIP).
			if !tt.expectAddPod {
				assert.Empty(t, dt.pendingPods, "a skipped pod must not be registered with the engine")
				return
			}
			assert.Len(t, dt.pendingPods, tt.expectedCalls)
			buffered := dt.pendingPods[tt.expectedEgress]
			if !assert.Len(t, buffered, 1, "expected the egress pod buffered under %q", tt.expectedEgress) {
				return
			}
			if tt.expectedPodKey != "" {
				assert.Equal(t, tt.expectedPodKey, buffered[0].PodKey)
			}
			if tt.expectedHostIP != "" {
				assert.Equal(t, tt.expectedHostIP, buffered[0].Location)
			}
			if tt.expectedPodIP != "" {
				assert.Equal(t, tt.expectedPodIP, buffered[0].Address)
			}
		})
	}
}

// TestPodInformerDualStackRegistersEveryFamily verifies the egress informer registers each IP family
// of a dual-stack pod under its SAME-FAMILY node location - the IPv6 PodIP under the node's IPv6 IP
// (Status.HostIPs), NOT under the (IPv4) HostIP. NRP rejects a location that mixes families
// (IPv4LocationCannotContainIPv6Addresses), so filing the IPv6 address under the IPv4 node location
// makes the whole registration fail. It runs against a real engine (NAT gateway pre-seeded so the pod
// registers live rather than buffering) and fails if the add path drops a family or misfiles it.
func TestPodInformerDualStackRegistersEveryFamily(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-a"
		v4Node = "10.0.0.1"
		v6Node = "fd00::a"
		v4Pod  = "10.244.0.1"
		v6Pod  = "fd00:244::1"
	)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ds", Namespace: "default", UID: types.UID("uid-ds"),
			Labels: map[string]string{consts.PodLabelServiceEgressGateway: egress},
		},
		Status: v1.PodStatus{
			HostIP:  v4Node,
			HostIPs: []v1.HostIP{{IP: v4Node}, {IP: v6Node}},
			PodIP:   v4Pod,
			PodIPs:  []v1.PodIP{{IP: v4Pod}, {IP: v6Pod}},
			Phase:   v1.PodRunning,
		},
	}

	kube := fake.NewSimpleClientset(pod)
	dt, _ := newSeededDiffTracker(t, ctrl, kube,
		K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(egress), Nodes: map[string]Node{}},
		NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(egress), Locations: map[string]NRPLocation{}})

	dt.podInformerAddPod(pod)
	v4Pods := dt.K8sResources.Nodes[v4Node].Pods
	v6Pods := dt.K8sResources.Nodes[v6Node].Pods
	assert.Contains(t, v4Pods, v4Pod, "the IPv4 PodIP must register under the IPv4 node location")
	assert.Contains(t, v6Pods, v6Pod, "the IPv6 PodIP must register under the IPv6 node location")
	assert.NotContains(t, v4Pods, v6Pod, "the IPv6 PodIP must NOT be filed under the IPv4 node location (NRP rejects mixed-family locations)")

	dt.podInformerRemovePod(pod)
	assert.NotContains(t, dt.K8sResources.Nodes[v4Node].Pods, v4Pod, "the IPv4 family must drain on remove")
	assert.NotContains(t, dt.K8sResources.Nodes[v6Node].Pods, v6Pod, "the IPv6 family must drain on remove")
}

// TestPodInformerAddPod_FinalizerAddFailureRegistersAndAlerts verifies that when AddPodFinalizer fails
// after its retries (sustained apiserver outage), podInformerAddPod must STILL register the pod
// (returning would silently kill its egress) and make the rare unprotected-pod state observable via
// a warning Event (and the pod_finalizer_add_failed_total metric).
func TestPodInformerAddPod_FinalizerAddFailureRegistersAndAlerts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "egress-p",
			Namespace: "default",
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: "10.244.0.1"},
	}
	kube := fake.NewSimpleClientset(pod)
	// Persistent non-NotFound error on the finalizer Update -> AddPodFinalizer exhausts its retries.
	kube.PrependReactor("update", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(fmt.Errorf("apiserver down"))
	})

	dt := newProviderDiffTracker(t, ctrl, kube)
	rec := record.NewFakeRecorder(10)
	dt.eventRecorder = rec

	dt.podInformerAddPod(pod)

	// The pod must still be registered with the engine despite the finalizer add failure.
	assert.True(t, dt.IsServiceTracked("egress-svc"),
		"pod must still be registered (AddPod called) even when the finalizer could not be added")

	// And the rare unprotected-pod state must be surfaced as a warning Event.
	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, "ServiceGatewayFinalizerAddFailed",
			"a warning Event must be emitted when an egress pod is registered without its cleanup finalizer")
	default:
		t.Fatal("expected a ServiceGatewayFinalizerAddFailed warning Event on finalizer add failure")
	}
}

// TestPodInformerAddPod_RejectsInvalidEgressLabel verifies that an egress pod whose label is not a
// valid Azure resource name (e.g. a path-traversal value) is NOT registered with the engine and a
// warning Event is emitted, so the label can never be interpolated raw into a NAT Gateway ARM ID.
func TestPodInformerAddPod_RejectsInvalidEgressLabel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "evil-egress",
			Namespace: "default",
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: "../hijacked-nat"},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: "10.244.0.1"},
	}
	kube := fake.NewSimpleClientset(pod)

	dt := newProviderDiffTracker(t, ctrl, kube)
	rec := record.NewFakeRecorder(10)
	dt.eventRecorder = rec

	dt.podInformerAddPod(pod)

	assert.False(t, dt.IsServiceTracked("../hijacked-nat"),
		"a pod with an invalid egress label must not be registered with the engine")

	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, "ServiceGatewayInvalidEgressLabel",
			"a warning Event must be emitted for an invalid egress gateway label")
	default:
		t.Fatal("expected a ServiceGatewayInvalidEgressLabel warning Event")
	}
}

// TestPodInformerAddPod_SkipsStaleReplacedPod verifies that a stale Add event for a pod that has
// since been replaced by a same-name pod with a different UID does NOT register the stale event
// pod's address. AddPodFinalizer declines to add the finalizer to the replacement (it is UID-guarded)
// and signals ErrPodGoneOrReplaced; the handler must abort rather than register an unprotected
// mapping (no finalizer to drain it) for an IP that may already be reclaimed.
func TestPodInformerAddPod_SkipsStaleReplacedPod(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const egress = "egress-svc"
	// The live pod at default/egress-p is the replacement (new UID).
	livePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "egress-p",
			Namespace: "default",
			UID:       "uid-new",
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: egress},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: "10.244.0.2"},
	}
	// The stale Add event carries the old pod (old UID, different IP).
	stalePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "egress-p",
			Namespace: "default",
			UID:       "uid-old",
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: egress},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: "10.244.0.1"},
	}
	kube := fake.NewSimpleClientset(livePod)

	dt := newProviderDiffTracker(t, ctrl, kube)

	dt.podInformerAddPod(stalePod)

	assert.False(t, dt.IsServiceTracked(egress),
		"a stale event for a UID-replaced pod must not register the stale address")
	if node, ok := dt.K8sResources.Nodes["10.0.0.1"]; ok {
		assert.NotContains(t, node.Pods, "10.244.0.1",
			"the stale pod's IP must not be registered under the node location")
	}
}

func TestPodInformerAddPod_RejectsMalformedPodIP(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "malformed-ip-egress",
			Namespace: "default",
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: "not-an-ip"},
	}
	kube := fake.NewSimpleClientset(pod)

	dt := newProviderDiffTracker(t, ctrl, kube)
	rec := record.NewFakeRecorder(10)
	dt.eventRecorder = rec

	dt.podInformerAddPod(pod)

	assert.False(t, dt.IsServiceTracked("egress-svc"),
		"a pod with a malformed PodIP must not be registered with the engine")

	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, "ServiceGatewayInvalidPodIP",
			"a warning Event must be emitted for a malformed pod IP")
	default:
		t.Fatal("expected a ServiceGatewayInvalidPodIP warning Event")
	}
}

// TestPodInformerRemovePod tests the podInformerRemovePod function
func TestPodInformerRemovePod(t *testing.T) {
	tests := []struct {
		name            string
		pod             *v1.Pod
		expectDeletePod bool
	}{
		{
			name:            "Valid pod with egress label and IPs should trigger DeletePod",
			pod:             newTestPod("default", "test-pod", "egress-gateway-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: true,
		},
		{
			name:            "Pod without egress label should be skipped",
			pod:             newTestPod("default", "no-label", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: false,
		},
		{
			name:            "Pod without HostIP should be skipped with warning",
			pod:             newTestPod("default", "no-hostip", "egress-a", "", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: false,
		},
		{
			name:            "Pod without PodIP should be skipped with warning",
			pod:             newTestPod("default", "no-podip", "egress-a", "10.0.0.1", "", v1.PodRunning, nil),
			expectDeletePod: false,
		},
		{
			name:            "Pod in any phase with IPs should trigger DeletePod (phase doesn't matter for removal)",
			pod:             newTestPod("default", "failed-pod", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			expectDeletePod: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Seed the engine so a valid pod's address is registered: routing it to DeletePod then
			// drain-gates the address (observable via HasPendingPodDeletion), while the no-label and
			// no-IP paths never reach DeletePod.
			egress := tt.pod.Labels[consts.PodLabelServiceEgressGateway]
			k8s := K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(), Nodes: map[string]Node{}}
			nrp := NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(), Locations: map[string]NRPLocation{}}
			if egress != "" && tt.pod.Status.HostIP != "" && tt.pod.Status.PodIP != "" {
				k8s.Egresses.Insert(egress)
				k8s.Nodes[tt.pod.Status.HostIP] = Node{Pods: map[string]Pod{
					tt.pod.Status.PodIP: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
				}}
				nrp.NATGateways.Insert(egress)
			}
			pod := tt.pod.DeepCopy()
			pod.Finalizers = []string{ServiceGatewayPodCleanupFinalizer}
			dt := seededDT(t, ctrl, fake.NewSimpleClientset(pod), k8s, nrp)

			dt.podInformerRemovePod(pod)

			if tt.expectDeletePod {
				assert.True(t, dt.HasPendingPodDeletion(pod.Namespace, pod.Name, string(pod.UID)),
					"a valid egress pod must be routed to DeletePod, which drain-gates the registered address")
			} else {
				assert.False(t, dt.HasPendingPodDeletion(pod.Namespace, pod.Name, string(pod.UID)),
					"a skipped pod must not be drain-gated")
			}
		})
	}
}

// TestPodInformerRemovePod_UntrackedPodFinalizerRemovedDirectly verifies that when the engine is
// not tracking the pod (a stale/duplicate delete, or a pod no longer in live state after a CCM
// restart), podInformerRemovePod removes the ServiceGateway finalizer directly. There is nothing
// to drain from NRP, so the pod must not be stranded in Terminating. Regression test for the
// drain-gating gap where DeletePod's stale-pod early return skipped the pendingPodDeletions enqueue.
func TestPodInformerRemovePod_UntrackedPodFinalizerRemovedDirectly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "egress-stale",
			Namespace:  "default",
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{HostIP: "10.0.0.1", PodIP: "10.244.0.1"},
	}
	kubeClient := fake.NewSimpleClientset(pod)

	dt := newProviderDiffTracker(t, ctrl, kubeClient)

	dt.podInformerRemovePod(pod)

	got, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "egress-stale", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"an untracked egress pod's finalizer must be removed directly so it is not stranded in Terminating")
}

// TestPodInformerRemovePod_NoIPPodFinalizerRemovedDirectly verifies the same direct removal when a
// deleted egress pod has no IPs (so its NRP address cannot be identified): there is nothing to
// drain, so the finalizer must be removed rather than stranding the pod.
func TestPodInformerRemovePod_NoIPPodFinalizerRemovedDirectly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "egress-noip",
			Namespace:  "default",
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{}, // no HostIP/PodIP
	}
	kubeClient := fake.NewSimpleClientset(pod)

	dt := newProviderDiffTracker(t, ctrl, kubeClient)

	dt.podInformerRemovePod(pod)

	got, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "egress-noip", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"a no-IP egress pod's finalizer must be removed directly so it is not stranded")
}

// TestPodInformerDrainForReplace_LiveReRegistrationDoesNotStripFinalizer proves the live
// re-registration drain enqueues no strippable finalizer record. Two live pods share the service (so
// removing one is a non-last delete) and the address is absent from NRP, so a pending record would be
// strippable at once; the contrast sub-test uses podInformerRemovePod to show the record path would
// strip the finalizer.
func TestPodInformerDrainForReplace_LiveReRegistrationDoesNotStripFinalizer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-svc"
		hostIP = "10.0.0.1"
		addrA  = "10.244.0.1"
		addrB  = "10.244.0.2"
	)

	// Two live pods on the same node/egress service so removing pod A is a non-last delete. New()
	// seeds the outbound ref-counter to 2 from these pods.
	seedState := func() K8sState {
		return K8sState{
			Services: utilsets.NewString(),
			Egresses: utilsets.NewString(egress),
			Nodes: map[string]Node{
				hostIP: {Pods: map[string]Pod{
					addrA: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
					addrB: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
				}},
			},
		}
	}
	// addrA is not registered in NRP (already drained), so a non-last pending record would strip at once.
	drainedNRP := func() NRPState {
		return NRPState{
			LoadBalancers: utilsets.NewString(),
			NATGateways:   utilsets.NewString(egress),
			Locations:     make(map[string]NRPLocation),
		}
	}
	podA := func(ip string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "pod-a",
				Namespace:  "default",
				UID:        types.UID("uid-a"),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
				Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{HostIP: hostIP, PodIP: ip, PodIPs: []v1.PodIP{{IP: ip}}, Phase: v1.PodRunning},
		}
	}

	t.Run("drainForReplace leaves the live pod's finalizer intact", func(t *testing.T) {
		kube := fake.NewSimpleClientset(podA(addrA))
		dt := seededDT(t, ctrl, kube, seedState(), drainedNRP())

		// A same-service address change (addrA -> addrC) drains addrA with empty namespace/name, so
		// no finalizer-deletion record is enqueued for the still-live pod.
		dt.podInformerDrainForReplace(podA(addrA), podA("10.244.0.3"))
		dt.CheckPendingPodDeletions(context.Background())

		got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
			"a live re-registration drain must not enqueue a strippable record, so the finalizer must survive")
	})

	t.Run("contrast: the record-enqueuing removal path would strip it", func(t *testing.T) {
		kube := fake.NewSimpleClientset(podA(addrA))
		dt := seededDT(t, ctrl, kube, seedState(), drainedNRP())

		dt.podInformerRemovePod(podA(addrA)) // enqueues a non-last drain-gated record
		dt.CheckPendingPodDeletions(context.Background())

		got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
			"the record-enqueuing removal drains the address then strips the finalizer once it has left NRP")
	})
}

// TestPodInformerDrainForReplace_SoleDualStackGainKeepsSharedAddress guards the delta-drain: a sole
// egress pod gaining a secondary family ([v4] -> [v4,v6]) must NOT drain the shared v4. Draining the
// full old set would drop the sole pod's service ref-count to zero, transiently marking the NAT
// Gateway for deletion - which a concurrent ServiceUpdater could act on and tear down under the
// still-live pod (egress outage). Only (old - new) is drained, which is empty here.
func TestPodInformerDrainForReplace_SoleDualStackGainKeepsSharedAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-svc"
		hostIP = "10.0.0.1"
		v4     = "10.244.0.1"
		v6     = "fd00::1"
	)

	// A single live pod carrying only v4: New() seeds the ref-count to 1.
	k8s := K8sState{
		Services: utilsets.NewString(),
		Egresses: utilsets.NewString(egress),
		Nodes: map[string]Node{
			hostIP: {Pods: map[string]Pod{
				v4: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
			}},
		},
	}
	nrp := NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(egress),
		Locations:     make(map[string]NRPLocation),
	}
	pod := func(ips ...string) *v1.Pod {
		podIPs := make([]v1.PodIP, 0, len(ips))
		for _, ip := range ips {
			podIPs = append(podIPs, v1.PodIP{IP: ip})
		}
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-a", Namespace: "default", UID: types.UID("uid-a"),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
				Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{HostIP: hostIP, PodIP: ips[0], PodIPs: podIPs, Phase: v1.PodRunning},
		}
	}

	kube := fake.NewSimpleClientset(pod(v4, v6))
	dt, _ := newSeededDiffTracker(t, ctrl, kube, k8s, nrp)

	dt.podInformerDrainForReplace(pod(v4), pod(v4, v6))

	// v4 must still be the sole live address: draining it now is the last-pod case. Had the full old
	// set been drained, v4 would already be gone and this would be a no-op (IsLastPod=false).
	res := dt.DeletePod(egress, hostIP, []string{v4}, "default", "pod-a", "uid-a")
	assert.True(t, res.IsLastPod,
		"a dual-stack gain must keep the shared v4 registered, so the sole pod's service is never emptied/torn down mid-replace")
	assert.True(t, res.Enqueued)
}

// TestPodInformerRemovePod_NoIPDeleteKeepsFinalizerWhileDrainPending checks that a terminating egress
// pod whose IPs the kubelet already cleared keeps its cleanup finalizer while an earlier drain is
// pending: stripping it inline would reclaim the pod (and its IP) while the NAT Gateway still maps the
// address, misrouting egress. CheckPendingPodDeletions strips it once NRP confirms the drain.
func TestPodInformerRemovePod_NoIPDeleteKeepsFinalizerWhileDrainPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-svc"
		hostIP = "10.0.0.1"
		v4     = "10.244.0.1"
	)

	k8s := K8sState{
		Services: utilsets.NewString(),
		Egresses: utilsets.NewString(egress),
		Nodes: map[string]Node{
			hostIP: {Pods: map[string]Pod{
				v4: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
			}},
		},
	}
	nrp := NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(egress),
		Locations:     make(map[string]NRPLocation),
	}

	newPod := func(withIPs bool) *v1.Pod {
		status := v1.PodStatus{Phase: v1.PodRunning}
		if withIPs {
			status.HostIP = hostIP
			status.PodIP = v4
			status.PodIPs = []v1.PodIP{{IP: v4}}
		}
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-a", Namespace: "default", UID: types.UID("uid-a"),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
				Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
			},
			Status: status,
		}
	}

	kube := fake.NewSimpleClientset(newPod(true))
	dt, _ := newSeededDiffTracker(t, ctrl, kube, k8s, nrp)

	// First termination event still carries the IPs: it drain-gates the address (Enqueued) and keeps
	// the finalizer for the drain-gate.
	dt.podInformerRemovePod(newPod(true))
	assert.True(t, dt.HasPendingPodDeletion("default", "pod-a", "uid-a"),
		"the with-IP delete must record a pending drain")

	// Second event arrives after the kubelet cleared the IPs. The finalizer must NOT be stripped.
	dt.podInformerRemovePod(newPod(false))

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"a no-IP delete must not strip the finalizer while a drain is still pending")
	assert.True(t, dt.HasPendingPodDeletion("default", "pod-a", "uid-a"),
		"the pending drain must remain until NRP confirms the address is gone")
}

// TestPodInformerRemovePod_NoIPDeleteStripsUntrackedPod checks the complementary case: a no-IP egress
// pod the engine never registered (no pending drain) still has its finalizer removed directly.
func TestPodInformerRemovePod_NoIPDeleteStripsUntrackedPod(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-b", Namespace: "default", UID: types.UID("uid-b"),
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{Phase: v1.PodFailed},
	}
	kube := fake.NewSimpleClientset(pod)
	dt := newProviderDiffTracker(t, ctrl, kube)

	dt.podInformerRemovePod(pod)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-b", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"an untracked no-IP pod with no pending drain must have its finalizer removed to avoid stranding")
}

func TestPodInformerRemovePod_NoIPDeleteWaitsForUnbackedNRPAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress      = "egress-svc"
		oldAddress  = "10.244.0.1"
		liveAddress = "10.244.0.2"
		location    = "10.0.0.1"
	)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deleted", Namespace: "default", UID: types.UID("deleted-uid"),
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{Phase: v1.PodFailed},
	}
	kube := fake.NewSimpleClientset(pod)
	dt := newProviderDiffTracker(t, ctrl, kube)
	dt.pendingServiceOps[egress] = &ServiceOperationState{
		ServiceUID: egress,
		Config:     NewOutboundServiceConfig(egress, nil),
		State:      StateCreated,
	}
	dt.K8sResources.Nodes[location] = Node{
		Pods: map[string]Pod{
			liveAddress: {
				InboundIdentities:      utilsets.NewString(),
				PublicOutboundIdentity: egress,
			},
		},
	}
	dt.NRPResources.NATGateways.Insert(egress)
	dt.NRPResources.Locations[location] = NRPLocation{
		Addresses: map[string]NRPAddress{
			oldAddress:  {Services: utilsets.NewString(egress)},
			liveAddress: {Services: utilsets.NewString(egress)},
		},
	}

	dt.podInformerRemovePod(pod)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "deleted", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer)
	entry := dt.pendingPodDeletions["default/deleted"]
	if assert.NotNil(t, entry) {
		assert.True(t, entry.VerifyServiceDrain)
		assert.False(t, entry.IsLastPod)
	}

	dt.CheckPendingPodDeletions(context.Background())
	got, err = kube.CoreV1().Pods("default").Get(context.Background(), "deleted", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"the no-IP finalizer must remain while an unbacked NRP address can belong to the pod")

	delete(dt.NRPResources.Locations[location].Addresses, oldAddress)
	dt.CheckPendingPodDeletions(context.Background())

	got, err = kube.CoreV1().Pods("default").Get(context.Background(), "deleted", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"once every remaining NRP address is backed by a live pod, the no-IP pod can terminate")
}

func TestPodInformerRemovePod_NoIPLastPodWaitsForServiceDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const egress = "egress-svc"
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deleted", Namespace: "default", UID: types.UID("deleted-uid"),
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{Phase: v1.PodFailed},
	}
	kube := fake.NewSimpleClientset(pod)
	dt := newProviderDiffTracker(t, ctrl, kube)
	dt.pendingServiceOps[egress] = &ServiceOperationState{
		ServiceUID: egress,
		Config:     NewOutboundServiceConfig(egress, nil),
		State:      StateCreated,
	}
	dt.K8sResources.Nodes["10.0.0.1"] = Node{
		Pods: map[string]Pod{
			"10.244.0.1": {
				InboundIdentities:      utilsets.NewString(),
				PublicOutboundIdentity: egress,
				OutboundPodKey:         "default/deleted",
			},
		},
	}
	dt.outboundIdentityPodRefCount.Store(egress, 1)
	dt.NRPResources.NATGateways.Insert(egress)
	dt.NRPResources.Locations["10.0.0.1"] = NRPLocation{
		Addresses: map[string]NRPAddress{
			"10.244.0.1": {Services: utilsets.NewString(egress)},
		},
	}

	dt.podInformerRemovePod(pod)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "deleted", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer)
	entry := dt.pendingPodDeletions["default/deleted"]
	if assert.NotNil(t, entry) {
		assert.False(t, entry.VerifyServiceDrain,
			"the pod identity should recover its stale live address even though status PodIPs are empty")
		assert.Equal(t, []string{"10.244.0.1"}, entry.Addresses)
		assert.True(t, entry.IsLastPod)
	}
	assert.NotContains(t, dt.K8sResources.Nodes, "10.0.0.1",
		"the deleted pod's stale desired address must be removed before NRP verification")
	assert.Equal(t, StateDeletionPending, dt.pendingServiceOps[egress].State)

	assert.NoError(t, dt.RemoveLastPodFinalizers(context.Background(), egress))
	got, err = kube.CoreV1().Pods("default").Get(context.Background(), "deleted", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer)
}

// A local K8s-state miss is not proof that an egress pod is untracked: NRP can still map its
// address while the engine is between snapshots or recovering state. The terminating pod must keep
// its finalizer and reconstruct drain tracking instead of taking the inline "untracked" release.
func TestPodInformerRemovePod_LocalStateMissingButNRPAddressPresentKeepsFinalizer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-svc"
		hostIP = "10.0.0.1"
		podIP  = "10.244.0.1"
	)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-a", Namespace: "default", UID: types.UID("uid-a"),
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{
			HostIP: hostIP,
			PodIP:  podIP,
			PodIPs: []v1.PodIP{{IP: podIP}},
			Phase:  v1.PodRunning,
		},
	}
	kube := fake.NewSimpleClientset(pod)
	dt := newProviderDiffTracker(t, ctrl, kube)
	dt.NRPResources.NATGateways.Insert(egress)
	dt.NRPResources.Locations[hostIP] = NRPLocation{
		Addresses: map[string]NRPAddress{
			podIP: {Services: utilsets.NewString(egress)},
		},
	}
	dt.pendingServiceOps[egress] = &ServiceOperationState{
		ServiceUID: egress,
		Config:     NewOutboundServiceConfig(egress, nil),
		State:      StateCreated,
	}

	// Deliberately do not seed K8sResources.Nodes: this models the production failure where the
	// informer delete raced a transient local-state gap even though NRP still owned the address.
	dt.podInformerRemovePod(pod)
	dt.podInformerRemovePod(pod.DeepCopy()) // duplicate terminating update
	noIPs := pod.DeepCopy()
	noIPs.Status.HostIP = ""
	noIPs.Status.PodIP = ""
	noIPs.Status.PodIPs = nil
	dt.podInformerRemovePod(noIPs) // later terminating update after kubelet clears addresses

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"NRP still maps the pod address, so a local state miss must not authorize finalizer removal")
	pending, ok := dt.pendingPodDeletions["default/pod-a"]
	if assert.True(t, ok, "the local-state miss must reconstruct pending drain tracking") {
		assert.Equal(t, "uid-a", pending.UID)
		assert.Equal(t, []string{podIP}, pending.Addresses)
		assert.True(t, pending.IsLastPod,
			"with no other live or buffered egress pods, cleanup must wait for NAT Gateway deletion")
	}

	// Simulate the outbound delete worker's post-NAT/PIP finalizer sweep.
	assert.NoError(t, dt.RemoveLastPodFinalizers(context.Background(), egress))
	got, err = kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"the finalizer should release only when the outbound delete worker completes cleanup")
}

// TestReconcileEgressPodUpdate_LiveReRegistrationKeepsFinalizerAndReAdds drives the real update
// executor end-to-end on a live engine: a dual-stack pod that gains a secondary family
// ([v4] -> [v4,v6]) is a live re-registration. The executor must drain the old set without a
// finalizer record and re-add the full set, leaving the pod finalized and both addresses tracked.
func TestReconcileEgressPodUpdate_LiveReRegistrationKeepsFinalizerAndReAdds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-svc"
		hostIP = "10.0.0.1"
		v6Node = "fd00::a"
		v4     = "10.244.0.1"
		v6     = "fd00::1"
		other  = "10.244.0.9"
	)

	// One live pod (v4) plus a second pod so the service is not torn down mid-update.
	k8s := K8sState{
		Services: utilsets.NewString(),
		Egresses: utilsets.NewString(egress),
		Nodes: map[string]Node{
			hostIP: {Pods: map[string]Pod{
				v4:    {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
				other: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
			}},
		},
	}
	nrp := NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(egress),
		Locations:     make(map[string]NRPLocation),
	}

	pod := func(ips ...string) *v1.Pod {
		podIPs := make([]v1.PodIP, 0, len(ips))
		for _, ip := range ips {
			podIPs = append(podIPs, v1.PodIP{IP: ip})
		}
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "pod-a",
				Namespace:  "default",
				UID:        types.UID("uid-a"),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
				Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{HostIP: hostIP, HostIPs: []v1.HostIP{{IP: hostIP}, {IP: v6Node}}, PodIP: ips[0], PodIPs: podIPs, Phase: v1.PodRunning},
		}
	}

	oldPod, newPod := pod(v4), pod(v4, v6)
	kube := fake.NewSimpleClientset(newPod)
	dt, _ := newSeededDiffTracker(t, ctrl, kube, k8s, nrp)

	dt.reconcileEgressPodUpdate(oldPod, newPod)
	dt.CheckPendingPodDeletions(context.Background())

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"a live re-registration must leave the pod finalized")
	assert.True(t, dt.IsServiceTracked(egress), "the egress service must remain tracked")

	// Both IP families must be registered under their SAME-FAMILY node location: the dual-stack gain
	// added v6 under the node's IPv6 IP and kept the shared v4 under the IPv4 node IP.
	v4Pods := dt.K8sResources.Nodes[hostIP].Pods
	v6Pods := dt.K8sResources.Nodes[v6Node].Pods
	if assert.Contains(t, v4Pods, v4, "the primary family must remain registered under the IPv4 node location") {
		assert.Equal(t, egress, v4Pods[v4].PublicOutboundIdentity)
	}
	if assert.Contains(t, v6Pods, v6, "the gained secondary family must be registered under the IPv6 node location") {
		assert.Equal(t, egress, v6Pods[v6].PublicOutboundIdentity)
	}
	assert.NotContains(t, v4Pods, v6, "the IPv6 address must not be filed under the IPv4 node location")
}

func TestReconcileEgressPodUpdate_SolePodIPReplacementKeepsNATGateway(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		egress = "egress-svc"
		hostIP = "10.0.0.1"
		oldIP  = "10.244.0.1"
		newIP  = "10.244.0.2"
		podKey = "default/pod-a"
		podUID = "uid-a"
	)
	k8s := K8sState{
		Services: utilsets.NewString(),
		Egresses: utilsets.NewString(egress),
		Nodes: map[string]Node{
			hostIP: {Pods: map[string]Pod{
				oldIP: {
					InboundIdentities:      utilsets.NewString(),
					PublicOutboundIdentity: egress,
					OutboundPodKey:         podKey,
					OutboundPodUID:         podUID,
				},
			}},
		},
	}
	nrp := NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(egress),
		Locations:     make(map[string]NRPLocation),
	}
	makePod := func(ip string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "pod-a",
				Namespace:  "default",
				UID:        types.UID(podUID),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
				Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{
				HostIP: hostIP,
				PodIP:  ip,
				PodIPs: []v1.PodIP{{IP: ip}},
				Phase:  v1.PodRunning,
			},
		}
	}

	oldPod, newPod := makePod(oldIP), makePod(newIP)
	kube := fake.NewSimpleClientset(newPod)
	dt, _ := newSeededDiffTracker(t, ctrl, kube, k8s, nrp)

	dt.reconcileEgressPodUpdate(oldPod, newPod)

	assert.NotContains(t, dt.pendingServiceDeletions, egress,
		"a same-service IP replacement must not schedule NAT Gateway deletion in the drain/add gap")
	if op, ok := dt.pendingServiceOps[egress]; ok {
		assert.NotEqual(t, StateDeletionPending, op.State)
		assert.NotEqual(t, StateDeletionInProgress, op.State)
	}
	assert.NotContains(t, dt.K8sResources.Nodes[hostIP].Pods, oldIP)
	replacement := dt.K8sResources.Nodes[hostIP].Pods[newIP]
	assert.Equal(t, egress, replacement.PublicOutboundIdentity)
	assert.Equal(t, podKey, replacement.OutboundPodKey)
	assert.Equal(t, podUID, replacement.OutboundPodUID)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer)
}

// TestEgressPodUpdateActions verifies the pure UPDATE decision function directly: for each pod
// transition it must report whether the pod has to be removed from its old gateway and/or (re-)added
// to its current one. Driving egressPodUpdateActions itself (rather than a mock informer) keeps this
// a genuine guard - the routing that consumes these decisions is covered by the real-Cloud tests
// (TestPodInformerDrainForReplace_*, TestReconcileEgressPodUpdate_*).
func TestEgressPodUpdateActions(t *testing.T) {
	now := metav1.Now()

	idle := func(minutes string) map[string]string {
		return map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: minutes}
	}

	tests := []struct {
		name       string
		oldPod     *v1.Pod
		newPod     *v1.Pod
		wantRemove bool
		wantAdd    bool
		wantVote   bool
	}{
		{
			name:       "Label change from A to B with IPs",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:    "Pod gets IPs AND label changes (never had IPs in A)",
			oldPod:  newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod:  newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantAdd: true,
		},
		{
			name:    "Pod just gets IPs (no label change)",
			oldPod:  newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod:  newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantAdd: true,
		},
		{
			name:       "IP change (pod moved to different node)",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "10.0.0.2", "10.0.1.2", v1.PodRunning, nil),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:       "Dual-stack secondary IP changes while primary PodIP is unchanged",
			oldPod:     withPodIPs(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), "10.0.1.1", "fd00::old"),
			newPod:     withPodIPs(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), "10.0.1.1", "fd00::new"),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:       "Dual-stack secondary IP added after the primary",
			oldPod:     withPodIPs(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), "10.0.1.1"),
			newPod:     withPodIPs(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), "10.0.1.1", "fd00::1"),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:       "Dual-stack pod downgraded to single stack (loses its secondary family)",
			oldPod:     withPodIPs(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), "10.0.1.1", "fd00::1"),
			newPod:     withPodIPs(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), "10.0.1.1"),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:       "Pod loses IPs",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "", "", v1.PodRunning, nil),
			wantRemove: true,
		},
		{
			name:       "Label removed (pod no longer egress)",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantRemove: true,
		},
		{
			name:       "Pod transitions to Failed phase",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			wantRemove: true,
		},
		{
			name:       "Pod transitions to Succeeded phase",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodSucceeded, nil),
			wantRemove: true,
		},
		{
			name:       "Pod gets DeletionTimestamp",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, &now),
			wantRemove: true,
		},
		{
			name:       "Pod gets DeletionTimestamp AND label changes",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, &now),
			wantRemove: true,
		},
		{
			name:   "No relevant changes (same state)",
			oldPod: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
		},
		{
			name:   "Pod in Pending without IPs stays in Pending without IPs",
			oldPod: newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod: newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
		},
		{
			name:       "IP change on terminating pod",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "10.0.0.2", "10.0.1.2", v1.PodRunning, &now),
			wantRemove: true,
		},
		{
			name:    "Pod recovers to Running from an invalid phase with unchanged IPs",
			oldPod:  newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			newPod:  newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantAdd: true,
		},
		{
			name:       "Egress pod goes Unknown while keeping its IPs",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodUnknown, nil),
			wantRemove: true,
		},
		{
			name:    "Egress pod recovers from Unknown to Running with the same IPs",
			oldPod:  newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodUnknown, nil),
			newPod:  newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantAdd: true,
		},
		{
			name:     "Egress annotations alone change: the vote is replaced",
			oldPod:   newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:   withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), idle("10")),
			wantVote: true,
		},
		{
			name:     "Egress annotation value changes on a Pending pod with IPs",
			oldPod:   withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodPending, nil), idle("10")),
			newPod:   withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodPending, nil), idle("20")),
			wantVote: true,
		},
		{
			name:     "Egress annotations removed: the pod votes for the default",
			oldPod:   withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), idle("10")),
			newPod:   newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			wantVote: true,
		},
		{
			name:     "Egress label value changes case only while the annotations change: same identity, vote replaced",
			oldPod:   newTestPod("default", "test", "Egress-A", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:   withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), idle("10")),
			wantVote: true,
		},
		{
			name:   "Other annotations change: nothing to do",
			oldPod: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod: withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
				map[string]string{"example.com/other": "x", "kubernetes.azure.com/other": "y"}),
		},
		{
			name:       "Egress annotations change with the addresses: the re-registration carries the vote",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.2", "10.0.1.2", v1.PodRunning, nil), idle("10")),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:       "Egress annotations change with a label move: the move carries the vote",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     withAnnotations(newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), idle("10")),
			wantRemove: true,
			wantAdd:    true,
		},
		{
			name:       "Egress annotations change as the pod becomes invalid: the removal withdraws the vote",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:     withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil), idle("10")),
			wantRemove: true,
		},
		{
			name:    "Egress annotations change as the pod regains validity: the add carries the vote",
			oldPod:  newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodUnknown, nil),
			newPod:  withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), idle("10")),
			wantAdd: true,
		},
		{
			name:   "Egress annotations change on a pod without IPs: no member, nothing to do",
			oldPod: newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod: withAnnotations(newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil), idle("10")),
		},
		{
			name:       "Egress annotations change on an invalid pod with IPs: the idempotent removal, no vote",
			oldPod:     newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			newPod:     withAnnotations(newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil), idle("10")),
			wantRemove: true,
		},
		{
			name:   "Egress annotations change on a pod without an egress label value: nothing to do",
			oldPod: newTestPod("default", "test", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod: withAnnotations(newTestPod("default", "test", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil), idle("10")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			needsRemove, needsAdd, needsVote, _ := egressPodUpdateActions(tt.oldPod, tt.newPod)
			assert.Equal(t, tt.wantRemove, needsRemove, "needsRemove")
			assert.Equal(t, tt.wantAdd, needsAdd, "needsAdd")
			assert.Equal(t, tt.wantVote, needsVote, "needsVote")
		})
	}
}

// TestEgressPodUpdateActions_NodeLocationChange verifies re-registration is triggered when a
// secondary-family node IP (Status.HostIPs) changes or appears, even though PodIPs and the primary
// HostIP are unchanged - otherwise the stale IPv6 location leaks or the IPv6 address never registers.
func TestEgressPodUpdateActions_NodeLocationChange(t *testing.T) {
	const v4, v6, v4Node, v6NodeOld, v6NodeNew = "10.244.0.1", "fd00::1", "10.0.0.1", "fd00::a", "fd00::b"
	dsPod := func(hostV6 string) *v1.Pod {
		hostIPs := []v1.HostIP{{IP: v4Node}}
		if hostV6 != "" {
			hostIPs = append(hostIPs, v1.HostIP{IP: hostV6})
		}
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "p", Namespace: "default",
				Labels: map[string]string{consts.PodLabelServiceEgressGateway: "egress-a"},
			},
			Status: v1.PodStatus{HostIP: v4Node, HostIPs: hostIPs, PodIP: v4, PodIPs: []v1.PodIP{{IP: v4}, {IP: v6}}, Phase: v1.PodRunning},
		}
	}

	t.Run("secondary-family node IP change re-registers", func(t *testing.T) {
		needsRemove, needsAdd, needsVote, _ := egressPodUpdateActions(dsPod(v6NodeOld), dsPod(v6NodeNew))
		assert.True(t, needsRemove)
		assert.True(t, needsAdd)
		assert.False(t, needsVote, "the re-registration carries the vote")
	})
	t.Run("secondary-family node IP appearing re-registers", func(t *testing.T) {
		needsRemove, needsAdd, needsVote, _ := egressPodUpdateActions(dsPod(""), dsPod(v6NodeNew))
		assert.True(t, needsRemove)
		assert.True(t, needsAdd)
		assert.False(t, needsVote, "the re-registration carries the vote")
	})
}
func TestPodInformerDeleteFunc(t *testing.T) {
	tests := []struct {
		name            string
		obj             interface{}
		expectDeletePod bool
		shouldError     bool
	}{
		{
			name:            "Direct pod object",
			obj:             newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: true,
		},
		{
			name: "Tombstone with valid pod",
			obj: cache.DeletedFinalStateUnknown{
				Key: "default/test",
				Obj: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			},
			expectDeletePod: true,
		},
		{
			name: "Tombstone with invalid object type",
			obj: cache.DeletedFinalStateUnknown{
				Key: "default/test",
				Obj: "not-a-pod",
			},
			expectDeletePod: false,
			shouldError:     true,
		},
		{
			name:            "Invalid object type (not pod or tombstone)",
			obj:             "invalid-type",
			expectDeletePod: false,
			shouldError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Drive the production decode the informer's DeleteFunc uses. Re-implementing the
			// type switch here would leave the real one untested: dropping its
			// DeletedFinalStateUnknown arm silently ignores the deletion of every egress pod
			// whose delete event the watch missed, stranding their cleanup finalizers.
			pod, ok := podFromDeleteObj(tt.obj)
			assert.Equal(t, !tt.shouldError, ok, "podFromDeleteObj decode result mismatch")
			if !ok {
				assert.Nil(t, pod, "a rejected object must not yield a pod")
				return
			}
			assert.NotNil(t, pod)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			// A decoded egress pod is routed to podInformerRemovePod; on an untracked (empty) engine
			// that strips its cleanup finalizer directly (nothing to drain).
			pod = pod.DeepCopy()
			pod.Finalizers = []string{ServiceGatewayPodCleanupFinalizer}
			kube := fake.NewSimpleClientset(pod)
			dt := newProviderDiffTracker(t, ctrl, kube)

			dt.podInformerRemovePod(pod)

			if tt.expectDeletePod {
				got, err := kube.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
					"a decoded egress pod must reach podInformerRemovePod, which strips the finalizer on an untracked pod")
			}
		})
	}
}

// TestPodInformerRemovePod_BufferedPodFinalizerRemovedByCaller verifies that a pod deleted while it
// is still buffered for an in-flight (never-created) egress service does not strand its cleanup
// finalizer. DeletePod returns early with Enqueued=false (nothing to drain from NRP), so
// podInformerRemovePod's !result.IsLastPod && !result.Enqueued branch removes the finalizer directly.
func TestPodInformerRemovePod_BufferedPodFinalizerRemovedByCaller(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "egress-buffered",
			Namespace:  "default",
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{HostIP: "10.0.0.1", PodIP: "10.244.0.7"},
	}
	kubeClient := fake.NewSimpleClientset(pod)

	dt := newProviderDiffTracker(t, ctrl, kubeClient)

	// Buffer the pod for an in-flight egress service (the harness runs no async workers, so the
	// service stays in StateNotStarted with the pod buffered in pendingPods).
	dt.AddPod("egress-svc", "default/egress-buffered", "10.0.0.1", "10.244.0.7")

	dt.podInformerRemovePod(pod)

	got, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "egress-buffered", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"a pod deleted while buffered must have its finalizer removed by the caller, not stranded in Terminating")
}

// TestPodInformerAddPod_IgnoresReservedEgressLabel pins that a pod cannot make this controller
// manage the cluster's default outbound NAT Gateway.
//
// The egress label is user-controlled and becomes the Azure resource name verbatim: the NAT Gateway
// is named after it and its Public IP is "<label>-pip". Labelling a pod with the RP-owned default
// gateway's name therefore aims the whole egress lifecycle at it - registering the first pod PUTs
// that gateway and its Public IP, and removing the last pod DELETEs them, taking out the cluster's
// default egress and reallocating its address. Nothing else prevents this: the default gateway is
// deliberately excluded from NRP state at startup, so the pod-add path always concludes it must be
// created, and the pod informer watches every namespace filtered only by this label.
func TestPodInformerAddPod_IgnoresReservedEgressLabel(t *testing.T) {
	for _, label := range []string{"default-natgw", "Default-NatGW", "DEFAULT-NATGW"} {
		t.Run(label, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "evil-pod",
					Namespace: "attacker-ns",
					UID:       types.UID("pod-uid"),
					Labels:    map[string]string{consts.PodLabelServiceEgressGateway: label},
				},
				Status: v1.PodStatus{
					Phase:  v1.PodRunning,
					HostIP: "10.0.0.1",
					PodIP:  "10.244.0.9",
					PodIPs: []v1.PodIP{{IP: "10.244.0.9"}},
				},
				Spec: v1.PodSpec{NodeName: "node-1"},
			}

			dt := newProviderDiffTracker(t, ctrl, fake.NewSimpleClientset(pod))
			rec := record.NewFakeRecorder(10)
			dt.eventRecorder = rec

			dt.podInformerAddPod(pod)

			assert.False(t, dt.IsServiceTracked(DefaultOutboundNATGatewayName),
				"a pod must never make the controller manage the default outbound NAT Gateway")
			dt.mu.Lock()
			opCount := len(dt.pendingServiceOps)
			dt.mu.Unlock()
			assert.Zero(t, opCount, "no Azure operation may be scheduled for a reserved egress identity")

			select {
			case ev := <-rec.Events:
				assert.Contains(t, ev, "ServiceGatewayReservedEgressLabel",
					"the user must be told the label is reserved, not merely malformed")
			default:
				t.Fatal("expected a ServiceGatewayReservedEgressLabel warning Event")
			}
		})
	}
}

// TestPodInformerAddPod_AcceptsOrdinaryEgressLabel is the control: the reserved-name guard must not
// reject ordinary egress identities.
func TestPodInformerAddPod_AcceptsOrdinaryEgressLabel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app-pod", Namespace: "team-ns", UID: types.UID("pod-uid"),
			Labels: map[string]string{consts.PodLabelServiceEgressGateway: "team-egress"},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: "10.244.0.9",
			PodIPs: []v1.PodIP{{IP: "10.244.0.9"}},
		},
		Spec: v1.PodSpec{NodeName: "node-1"},
	}

	dt := newProviderDiffTracker(t, ctrl, fake.NewSimpleClientset(pod))
	dt.eventRecorder = record.NewFakeRecorder(10)

	dt.podInformerAddPod(pod)

	assert.True(t, dt.IsServiceTracked("team-egress"),
		"an ordinary egress identity must still be registered")
}

// TestPodInformerRemovePod_CountsForgottenFinalizerRemoval pins that a failed direct finalizer
// removal is observable.
//
// This removal path is not retried: the engine has already proven there is nothing to drain, so no
// pending record exists to retry from, and the informer only re-drives a pod whose state changes -
// an informer resync delivers an unchanged object and is skipped by egressPodUpdateActions. The pod
// therefore stays Terminating until the CCM restarts, blocking node drain and namespace deletion,
// so the condition must at least be countable.
func TestPodInformerRemovePod_CountsForgottenFinalizerRemoval(t *testing.T) {
	RegisterMetrics()
	before, err := testutil.GetCounterMetricValue(podFinalizerRemoveFailedTotal)
	assert.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gone-pod", Namespace: "ns", UID: types.UID("pod-uid"),
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: "team-egress"},
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
		// No HostIP/PodIP: takes the direct, non-drain-gated removal path.
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	kube := fake.NewSimpleClientset(pod)
	kube.PrependReactor("update", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(fmt.Errorf("transient apiserver failure"))
	})

	dt := newProviderDiffTracker(t, ctrl, kube)
	dt.eventRecorder = record.NewFakeRecorder(10)

	dt.podInformerRemovePod(pod)

	after, err := testutil.GetCounterMetricValue(podFinalizerRemoveFailedTotal)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), after-before,
		"a forgotten finalizer removal must be counted so the resulting Terminating pod is detectable")
}

// TestPodInformerRemovePod_ReleasesFinalizerWhenLabelValueEmptied pins that an unlabelled pod
// carrying our cleanup finalizer is still finished off. The informer selects on the label key alone,
// so a pod whose value was emptied keeps matching and is still delivered here. Returning early on
// the missing label leaves the finalizer attached with nothing able to remove it, blocking node
// drain and namespace deletion.
func TestPodInformerRemovePod_ReleasesFinalizerWhenLabelValueEmptied(t *testing.T) {
	newEgressPod := func(labelValue string) *v1.Pod {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "egress-pod", Namespace: "default", UID: types.UID("pod-uid-1"),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: labelValue},
				Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{Phase: v1.PodRunning, PodIP: "10.244.0.5", HostIP: "10.0.0.1"},
		}
		return pod
	}

	// The label value was emptied, so this is all that is left.
	emptied := newEgressPod("")
	kube := fake.NewSimpleClientset(emptied)
	dt := newTestDiffTracker()
	dt.kubeClient = kube

	dt.podInformerRemovePod(emptied)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "egress-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.False(t, hasFinalizer(got.Finalizers, ServiceGatewayPodCleanupFinalizer),
		"a pod carrying our cleanup finalizer must be released even once its egress label value is gone")

	// Control 1: an unlabelled pod that is NOT ours must be left completely alone.
	foreign := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foreign-pod", Namespace: "default", UID: types.UID("pod-uid-2"),
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: ""},
			Finalizers: []string{"example.com/other-controller"},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, PodIP: "10.244.0.6", HostIP: "10.0.0.1"},
	}
	foreignKube := fake.NewSimpleClientset(foreign)
	foreignDT := newTestDiffTracker()
	foreignDT.kubeClient = foreignKube

	foreignDT.podInformerRemovePod(foreign)

	gotForeign, err := foreignKube.CoreV1().Pods("default").Get(context.Background(), "foreign-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, []string{"example.com/other-controller"}, gotForeign.Finalizers,
		"control: a pod we do not own must not be touched")

	// Control 2: a labelled pod whose address is still in NRP keeps its finalizer until it drains.
	labelled := newEgressPod("team-egress")
	labelledKube := fake.NewSimpleClientset(labelled)
	labelledDT := newTestDiffTracker()
	labelledDT.kubeClient = labelledKube
	labelledDT.NRPResources.NATGateways.Insert("team-egress")
	labelledDT.NRPResources.Locations["10.0.0.1"] = NRPLocation{
		Addresses: map[string]NRPAddress{"10.244.0.5": {Services: utilsets.NewString("team-egress")}},
	}
	labelledDT.pendingServiceOps["team-egress"] = &ServiceOperationState{
		ServiceUID: "team-egress",
		Config:     NewOutboundServiceConfig("team-egress", nil),
		State:      StateCreated,
	}

	labelledDT.podInformerRemovePod(labelled)

	gotLabelled, err := labelledKube.CoreV1().Pods("default").Get(context.Background(), "egress-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.True(t, hasFinalizer(gotLabelled.Finalizers, ServiceGatewayPodCleanupFinalizer),
		"control: a labelled pod keeps its finalizer until its addresses drain from NRP")
}

// TestPodInformerRemovePod_QueuesRetryWhenFinalizerRemovalFails pins that a transient apiserver
// error on a direct finalizer removal is retried rather than lost, for a labelled pod without IPs and
// for a pod whose egress label value was emptied: a finalizer-only record pinned to the pod's UID, for
// the pod's identity (none without a label value), which the next pending-deletion check retries.
//
// These paths run only once the engine has proved there is nothing to drain, so no pending record
// exists and the informer will not re-drive the pod: a resync delivers an unchanged object and is
// skipped. Without a queued record the pod stays Terminating until the CCM restarts, blocking node
// drain and namespace deletion. The successful removal is the control: it must queue nothing.
func TestPodInformerRemovePod_QueuesRetryWhenFinalizerRemovalFails(t *testing.T) {
	for _, tc := range []struct {
		name, label string
		// status is the pod's status: no IPs for the labelled pod, so the engine takes the "nothing to drain" path.
		status v1.PodStatus
	}{
		{name: "labelled pod without IPs", label: "team-egress", status: v1.PodStatus{Phase: v1.PodRunning}},
		{name: "pod whose egress label value was emptied",
			status: v1.PodStatus{Phase: v1.PodRunning, PodIP: "10.244.0.5", HostIP: "10.0.0.1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "egress-pod", Namespace: "default", UID: types.UID("pod-uid"),
					Labels:            map[string]string{consts.PodLabelServiceEgressGateway: tc.label},
					Finalizers:        []string{ServiceGatewayPodCleanupFinalizer},
					DeletionTimestamp: ptr.To(metav1.Now()),
				},
				Status: tc.status,
			}
			run := func(t *testing.T, removalFails bool) (*DiffTracker, *fake.Clientset, *atomic.Bool) {
				t.Helper()
				kube := fake.NewSimpleClientset(pod.DeepCopy())
				failing := &atomic.Bool{}
				failing.Store(removalFails)
				kube.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
					return failing.Load(), nil, errors.New("apiserver unavailable")
				})
				dt := newTestDiffTracker()
				dt.kubeClient = kube
				dt.podInformerRemovePod(pod.DeepCopy())
				return dt, kube, failing
			}

			failed, kube, failing := run(t, true)
			failed.mu.Lock()
			pending, queued := failed.pendingPodDeletions["default/egress-pod"]
			failed.mu.Unlock()
			if !assert.True(t, queued, "a failed finalizer removal must be queued for retry, not dropped") {
				return
			}
			assert.Equal(t, &PendingPodDeletion{Namespace: "default", Name: "egress-pod", UID: "pod-uid", ServiceUID: tc.label,
				Timestamp: pending.Timestamp}, pending, "a finalizer-only record pinned to the pod UID, never waiting on a drain")
			assert.True(t, apiHasFinalizer(t, kube, pod))

			failing.Store(false)
			assert.False(t, failed.CheckPendingPodDeletions(context.Background()), "the retry completes")
			assert.False(t, apiHasFinalizer(t, kube, pod), "the retry removes the finalizer")
			assert.False(t, failed.HasPendingPodDeletion("default", "egress-pod", ""), "and the record")

			ok, _, _ := run(t, false)
			assert.False(t, ok.HasPendingPodDeletion("default", "egress-pod", ""), "control: a successful removal queues nothing")
		})
	}
}

// ================================================================================================
// Egress membership (3.2) and the pod events that change it, each one engine call (4.1)
// ================================================================================================

// membershipNodeV6 is the IPv6 address of the dual-stack node memberNode of the membership pods.
const membershipNodeV6 = "fd00::a"

// withAnnotations sets the annotations of pod and returns it.
func withAnnotations(pod *v1.Pod, annotations map[string]string) *v1.Pod {
	pod.Annotations = annotations
	return pod
}

// podWith returns a copy of pod changed by mutate.
func podWith(pod *v1.Pod, mutate func(*v1.Pod)) *v1.Pod {
	changed := pod.DeepCopy()
	mutate(changed)
	return changed
}

// onIPv4OnlyNode moves pod to a node that exposes only its IPv4 address.
func onIPv4OnlyNode(pod *v1.Pod) {
	pod.Status.HostIPs = []v1.HostIP{{IP: pod.Status.HostIP}}
}

// membershipPod returns the Running egress pod ns/<name> of memberPod(name) (its UID and creation time) labelled
// egress, carrying the cleanup finalizer and the egress annotations ann, on the dual-stack node memberNode, with the
// addresses ips (memberIP(name) when none).
func membershipPod(name, egress string, ann map[string]string, ips ...string) *v1.Pod {
	ref := memberPod(name)
	if len(ips) == 0 {
		ips = []string{memberIP(ref)}
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ref.Namespace, Name: ref.Name, UID: ref.UID, CreationTimestamp: metav1.NewTime(ref.Created),
			Labels:      map[string]string{consts.PodLabelServiceEgressGateway: egress},
			Annotations: ann,
			Finalizers:  []string{ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: memberNode,
			HostIPs: []v1.HostIP{{IP: memberNode}, {IP: membershipNodeV6}}},
	}
	return withPodIPs(pod, ips...)
}

// annotationsA, annotationsB and invalidAnnotations are egress annotations voting for consensusA, for consensusB
// and invalidly, on an identity whose NAT Gateway serves IPv4 only.
func annotationsA() map[string]string {
	return map[string]string{
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:   "10",
		consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
	}
}

func annotationsB() map[string]string {
	return map[string]string{
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:          "20",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
	}
}

func invalidAnnotations() map[string]string {
	return map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: "ten"}
}

// podEventsRecorder records the events emitted on pods as "<reason> <namespace>/<name>".
type podEventsRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *podEventsRecorder) Event(object runtime.Object, _, reason, _ string) {
	name := "?"
	if accessor, err := meta.Accessor(object); err == nil {
		name = accessor.GetNamespace() + "/" + accessor.GetName()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, reason+" "+name)
}

func (r *podEventsRecorder) Eventf(object runtime.Object, eventType, reason, messageFmt string, args ...interface{}) {
	r.Event(object, eventType, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *podEventsRecorder) AnnotatedEventf(object runtime.Object, _ map[string]string, eventType, reason, messageFmt string, args ...interface{}) {
	r.Eventf(object, eventType, reason, messageFmt, args...)
}

// take returns the events recorded since the last call.
func (r *podEventsRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := r.events
	r.events = nil
	return events
}

// newMembershipTracker returns a real engine on a fake API server holding pods, whose egress identity
// memberIdentity is a created managed one, in NRP, with the IPv4 default configuration applied, and the recorder of
// the events it emits.
func newMembershipTracker(t *testing.T, pods ...*v1.Pod) (*DiffTracker, *fake.Clientset, *podEventsRecorder) {
	t.Helper()
	objects := make([]runtime.Object, 0, len(pods))
	for _, pod := range pods {
		objects = append(objects, pod.DeepCopy())
	}
	kube := fake.NewSimpleClientset(objects...)
	dt := seededDT(t, gomock.NewController(t), kube,
		K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(), Nodes: map[string]Node{}},
		NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(), Locations: map[string]NRPLocation{}})
	trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
	recorder := &podEventsRecorder{}
	dt.SetEventRecorder(recorder)
	return dt, kube, recorder
}

// egressAddressesOf returns the addresses of identity, live or buffered, as "address@location", sorted.
func egressAddressesOf(dt *DiffTracker, identity string) []string {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	var addresses []string
	for location, node := range dt.K8sResources.Nodes {
		for address, pod := range node.Pods {
			if pod.PublicOutboundIdentity != "" && strings.EqualFold(pod.PublicOutboundIdentity, identity) {
				addresses = append(addresses, address+"@"+location)
			}
		}
	}
	for _, pod := range dt.pendingPods[identity] {
		addresses = append(addresses, pod.Address+"@"+pod.Location)
	}
	slices.Sort(addresses)
	return addresses
}

// noEgressOperation is the state egressStateOf reports for an identity without an operation.
const noEgressOperation ResourceState = -1

// egressStateOf returns the state of the operation of identity; noEgressOperation without one.
func egressStateOf(dt *DiffTracker, identity string) ResourceState {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	if op, ok := dt.pendingServiceOps[identity]; ok {
		return op.State
	}
	return noEgressOperation
}

// egressVoteView is what a test compares of a registered vote: the parse error only as present or not.
type egressVoteView struct {
	Identity       string
	Config         EgressConfig
	Invalid        bool
	HasAnnotations bool
	PodUID         types.UID
	PodCreated     time.Time
}

// registeredVoteOf returns the registered vote of the pod podKey.
func registeredVoteOf(dt *DiffTracker, podKey string) (egressVoteView, bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	vote, ok := dt.egress.votes.get(podKey)
	if !ok {
		return egressVoteView{}, false
	}
	return egressVoteView{Identity: vote.Identity, Config: vote.Config, Invalid: vote.Invalid != "",
		HasAnnotations: vote.HasAnnotations, PodUID: vote.PodUID, PodCreated: vote.PodCreated}, true
}

// expectedVote returns the vote view of the membership pod name for identity: cfg, or invalid when cfg is nil.
func expectedVote(name, identity string, cfg *EgressConfig, hasAnnotations bool) egressVoteView {
	ref := memberPod(name)
	view := egressVoteView{Identity: identity, Invalid: cfg == nil, HasAnnotations: hasAnnotations, PodUID: ref.UID, PodCreated: ref.Created}
	if cfg != nil {
		view.Config = *cfg
	}
	return view
}

// apiHasFinalizer reports whether the API server's pod still carries the cleanup finalizer.
func apiHasFinalizer(t *testing.T, kube *fake.Clientset, pod *v1.Pod) bool {
	t.Helper()
	got, err := kube.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if !assert.NoError(t, err) {
		return false
	}
	return hasFinalizer(got.Finalizers, ServiceGatewayPodCleanupFinalizer)
}

// TestEgressMembership pins the membership predicate (3.2): the identity is the lower-cased label value and the
// accepted addresses are the pod's addresses whose IP family has a node location, so a partial dual-stack pod keeps
// the families that have one; the pod is a member iff one is accepted.
func TestEgressMembership(t *testing.T) {
	base := func(mutate func(*v1.Pod)) *v1.Pod {
		return podWith(membershipPod("p1", memberIdentity, nil, "10.244.1.1"), mutate)
	}
	v4 := egressAddress{IP: "10.244.1.1", Location: memberNode}
	v6 := egressAddress{IP: "fd00::1", Location: membershipNodeV6}
	now := metav1.Now()

	cases := []struct {
		name         string
		pod          *v1.Pod
		wantIdentity string
		wantAccepted []egressAddress
		wantReason   string // a substring; empty: no reason
	}{
		{name: "a Running pod is a member with its address under its node", pod: base(func(*v1.Pod) {}),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v4}},
		{name: "a Pending pod with addresses is a member", pod: base(func(p *v1.Pod) { p.Status.Phase = v1.PodPending }),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v4}},
		{name: "the identity is the label value, lower-cased",
			pod:          base(func(p *v1.Pod) { p.Labels[consts.PodLabelServiceEgressGateway] = "Team-EGRESS" }),
			wantIdentity: "team-egress", wantAccepted: []egressAddress{v4}},
		{name: "a dual-stack pod on a dual-stack node is a member with both families",
			pod:          base(func(p *v1.Pod) { withPodIPs(p, "10.244.1.1", "fd00::1") }),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v4, v6}},
		{name: "partial dual-stack: on a node without IPv6 the pod keeps its IPv4 family",
			pod:          base(func(p *v1.Pod) { withPodIPs(p, "10.244.1.1", "fd00::1"); onIPv4OnlyNode(p) }),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v4}, wantReason: `No same-family node IP for pod address "fd00::1"`},
		{name: "no address with a node location of its family: no member",
			pod:          base(func(p *v1.Pod) { withPodIPs(p, "fd00::1"); onIPv4OnlyNode(p) }),
			wantIdentity: memberIdentity, wantReason: `No same-family node IP for pod address "fd00::1"`},
		{name: "addresses and node locations are canonical",
			pod: base(func(p *v1.Pod) {
				withPodIPs(p, "FD00:0:0:0:0:0:0:1")
				p.Status.HostIPs = []v1.HostIP{{IP: memberNode}, {IP: "FD00:0:0:0:0:0:0:A"}}
			}),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v6}},
		{name: "a malformed address is skipped, the others are accepted",
			pod:          base(func(p *v1.Pod) { withPodIPs(p, "10.244.1.1", "bogus") }),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v4}, wantReason: `Malformed PodIP "bogus"`},
		{name: "a pod not bound to a node name but with addresses is a member",
			pod:          base(func(p *v1.Pod) { p.Spec.NodeName = "" }),
			wantIdentity: memberIdentity, wantAccepted: []egressAddress{v4}},
		{name: "a pod without an egress label value is no member",
			pod:        base(func(p *v1.Pod) { p.Labels[consts.PodLabelServiceEgressGateway] = "" }),
			wantReason: "no egress label value"},
		{name: "a pod without the egress label is no member", pod: base(func(p *v1.Pod) { p.Labels = nil }),
			wantReason: "no egress label value"},
		{name: "a pod being deleted is no member", pod: base(func(p *v1.Pod) { p.DeletionTimestamp = &now }),
			wantIdentity: memberIdentity, wantReason: "being deleted"},
		{name: "a Succeeded pod is no member", pod: base(func(p *v1.Pod) { p.Status.Phase = v1.PodSucceeded }),
			wantIdentity: memberIdentity, wantReason: "phase Succeeded"},
		{name: "a Failed pod is no member", pod: base(func(p *v1.Pod) { p.Status.Phase = v1.PodFailed }),
			wantIdentity: memberIdentity, wantReason: "phase Failed"},
		{name: "a pod in phase Unknown is no member", pod: base(func(p *v1.Pod) { p.Status.Phase = v1.PodUnknown }),
			wantIdentity: memberIdentity, wantReason: "phase Unknown"},
		{name: "a pod without a HostIP is no member", pod: base(func(p *v1.Pod) { p.Status.HostIP = "" }),
			wantIdentity: memberIdentity, wantReason: "no HostIP or PodIP"},
		{name: "a pod without a PodIP is no member", pod: base(func(p *v1.Pod) { p.Status.PodIP, p.Status.PodIPs = "", nil }),
			wantIdentity: memberIdentity, wantReason: "no HostIP or PodIP"},
		{name: "a malformed HostIP: no member", pod: base(func(p *v1.Pod) { p.Status.HostIP = "bad-host" }),
			wantIdentity: memberIdentity, wantReason: `Malformed HostIP "bad-host"`},
		{name: "only malformed addresses: no member", pod: base(func(p *v1.Pod) { withPodIPs(p, "bogus") }),
			wantIdentity: memberIdentity, wantReason: "no valid PodIP"},
		{name: "an identity that is no valid resource name: no member",
			pod:          base(func(p *v1.Pod) { p.Labels[consts.PodLabelServiceEgressGateway] = "../evil" }),
			wantIdentity: "../evil", wantReason: `Invalid egress gateway label "../evil"`},
		{name: "the reserved identity: no member",
			pod:          base(func(p *v1.Pod) { p.Labels[consts.PodLabelServiceEgressGateway] = "Default-NatGW" }),
			wantIdentity: "default-natgw", wantReason: `Egress gateway label "default-natgw" is reserved`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			identity, accepted, reason := egressMembership(tc.pod)
			assert.Equal(t, tc.wantIdentity, identity, "identity")
			assert.Equal(t, tc.wantAccepted, accepted, "accepted addresses")
			if tc.wantReason == "" {
				assert.Empty(t, reason)
			} else {
				assert.Contains(t, reason, tc.wantReason)
			}
		})
	}
}

// TestEgressMembership_RuntimeAndStartupAgree pins that the pod informer and startup admit the same pods with the
// same addresses and votes: both decide with egressMembership and parse the annotations alike.
func TestEgressMembership_RuntimeAndStartupAgree(t *testing.T) {
	now := metav1.Now()
	pods := map[string]*v1.Pod{
		"member voting A":                 membershipPod("p1", memberIdentity, annotationsA()),
		"member with invalid annotations": membershipPod("p1", memberIdentity, invalidAnnotations()),
		"member with an upper-case label": membershipPod("p1", "Team-EGRESS", nil),
		"dual-stack member":               membershipPod("p1", memberIdentity, nil, "10.244.1.1", "fd00::1"),
		"partial dual-stack member":       podWith(membershipPod("p1", memberIdentity, annotationsB(), "10.244.1.1", "fd00::1"), onIPv4OnlyNode),
		"no address with a node location": podWith(membershipPod("p1", memberIdentity, nil, "fd00::1"), onIPv4OnlyNode),
		"pod being deleted":               podWith(membershipPod("p1", memberIdentity, nil), func(p *v1.Pod) { p.DeletionTimestamp = &now }),
		"failed pod":                      podWith(membershipPod("p1", memberIdentity, nil), func(p *v1.Pod) { p.Status.Phase = v1.PodFailed }),
		"malformed HostIP":                podWith(membershipPod("p1", memberIdentity, nil), func(p *v1.Pod) { p.Status.HostIP = "bad" }),
		"reserved identity":               membershipPod("p1", DefaultOutboundNATGatewayName, nil),
		"invalid identity":                membershipPod("p1", "../evil", nil),
	}
	for name, pod := range pods {
		t.Run(name, func(t *testing.T) {
			identity, accepted, _ := egressMembership(pod)
			var want []string
			for _, address := range accepted {
				want = append(want, address.IP+"@"+address.Location)
			}
			slices.Sort(want)

			// The pod informer, on an engine whose identity is in NRP so the addresses register live.
			runtimeTracker := seededDT(t, gomock.NewController(t), fake.NewSimpleClientset(pod.DeepCopy()),
				K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(), Nodes: map[string]Node{}},
				NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(identity), Locations: map[string]NRPLocation{}})
			runtimeTracker.podInformerAddPod(pod.DeepCopy())
			runtimeVote, runtimeVoted := registeredVoteOf(runtimeTracker, "ns/p1")

			// Startup: the listing admits the addresses and the votes are seeded once the tracker exists.
			k8s := newK8sStateForSeeders()
			list, err := processK8sEgresses(context.Background(), fake.NewSimpleClientset(pod.DeepCopy()), &k8s)
			if !assert.NoError(t, err) {
				return
			}
			startupTracker := seededDT(t, gomock.NewController(t), fake.NewSimpleClientset(), k8s,
				NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(identity), Locations: map[string]NRPLocation{}})
			startupTracker.seedEgressVotes(list)
			startupVote, startupVoted := registeredVoteOf(startupTracker, "ns/p1")

			assert.Equal(t, want, egressAddressesOf(runtimeTracker, identity), "the pod informer registers the accepted addresses")
			assert.Equal(t, want, egressAddressesOf(startupTracker, identity), "startup admits the accepted addresses")
			assert.Equal(t, len(accepted) > 0, runtimeVoted, "a member pod votes, at runtime")
			assert.Equal(t, runtimeVoted, startupVoted, "a member pod votes, at startup")
			assert.Equal(t, runtimeVote, startupVote, "the pod votes alike at runtime and at startup")
			assert.Equal(t, len(accepted) > 0, k8s.Egresses.Has(identity), "startup desires the identity of a member")
		})
	}
}

// TestPodInformer_EgressMembershipRows pins every row of 4.1 through the real informer handlers: each pod event is
// one engine call, the votes stay those of the members (I1) and the engine alone reports the events of a vote. A
// live pod that stays an egress pod keeps its finalizer through a move or an address change, also out of every
// identity (I12); a pod that is deleted or loses its egress label has it released once its addresses drained.
func TestPodInformer_EgressMembershipRows(t *testing.T) {
	const (
		otherIdentity     = "other-egress"
		collidingIdentity = "11111111-2222-3333-4444-555555555555"
		label             = consts.PodLabelServiceEgressGateway
	)
	now := metav1.Now()
	p1A := membershipPod("p1", memberIdentity, annotationsA())
	p2A := membershipPod("p2", memberIdentity, annotationsA())
	p1Address := memberIP(memberPod("p1")) + "@" + memberNode
	p2Address := memberIP(memberPod("p2")) + "@" + memberNode
	cfgA, cfgB := consensusA, consensusB
	voteA := func(name, identity string) egressVoteView { return expectedVote(name, identity, &cfgA, true) }

	add := func(pod *v1.Pod) func(*DiffTracker) {
		return func(dt *DiffTracker) { dt.podInformerAddPod(pod) }
	}
	update := func(oldPod, newPod *v1.Pod) func(*DiffTracker) {
		return func(dt *DiffTracker) { dt.reconcileEgressPodUpdate(oldPod, newPod) }
	}
	remove := func(obj interface{}) func(*DiffTracker) {
		return func(dt *DiffTracker) {
			if pod, ok := podFromDeleteObj(obj); ok {
				dt.podInformerRemovePod(pod)
			}
		}
	}
	relabel := func(pod *v1.Pod, identity string) *v1.Pod {
		return podWith(pod, func(p *v1.Pod) { p.Labels[label] = identity })
	}
	lostAddresses := func(pod *v1.Pod) *v1.Pod {
		return podWith(pod, func(p *v1.Pod) { withPodIPs(p, "fd00::1"); onIPv4OnlyNode(p) })
	}

	cases := []struct {
		name        string
		api         []*v1.Pod // the pods of the API server; nil: p1A and p2A
		setup       func(dt *DiffTracker)
		members     []*v1.Pod // added through the ADD handler
		beforeEvent func(t *testing.T, kube *fake.Clientset)
		event       func(dt *DiffTracker)

		wantVotes     map[string]egressVoteView // pod key -> registered vote; pods not listed have none
		wantAddresses map[string][]string       // identity -> addresses, live or buffered
		wantStates    map[string]ResourceState  // identity -> operation state
		wantDesired   map[string]EgressConfig   // identity -> desired configuration
		wantRecord    bool                      // ns/p1 has a finalizer-removal record
		wantEvents    []string
		wantFinalizer string // ns/p1 after the drains: "kept", "absent", or "" not checked
	}{
		{
			name:      "a pod becomes a member: its addresses and its vote are added in one call",
			event:     add(p1A.DeepCopy()),
			wantVotes: map[string]egressVoteView{"ns/p1": voteA("p1", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p1Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantDesired: map[string]EgressConfig{memberIdentity: consensusA},
			wantFinalizer: "kept",
		},
		{
			name:          "a pod with invalid annotations is a member with an invalid vote, which the engine reports",
			event:         add(membershipPod("p1", memberIdentity, invalidAnnotations())),
			wantVotes:     map[string]egressVoteView{"ns/p1": expectedVote("p1", memberIdentity, nil, true)},
			wantAddresses: map[string][]string{memberIdentity: {p1Address}}, wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{"InvalidEgressConfig ns/p1"}, wantFinalizer: "kept",
		},
		{
			name:    "a pod joining with another configuration: the engine reports the conflict",
			members: []*v1.Pod{p1A},
			event:   add(membershipPod("p2", memberIdentity, annotationsB())),
			wantVotes: map[string]egressVoteView{"ns/p1": voteA("p1", memberIdentity),
				"ns/p2": expectedVote("p2", memberIdentity, &cfgB, true)},
			wantAddresses: map[string][]string{memberIdentity: {p1Address, p2Address}}, wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{"EgressConfigConflict ns/p2"}, wantFinalizer: "kept",
		},
		{
			name:      "the identity is the lower-cased label value",
			event:     add(membershipPod("p1", "Team-EGRESS", annotationsA())),
			wantVotes: map[string]egressVoteView{"ns/p1": voteA("p1", "team-egress")}, wantAddresses: map[string][]string{"team-egress": {p1Address}},
			wantStates: map[string]ResourceState{"team-egress": StateNotStarted}, wantFinalizer: "kept",
		},
		{
			name:          "a pod without an accepted address is no member: nothing is registered and it gets no finalizer",
			api:           []*v1.Pod{podWith(p1A, func(p *v1.Pod) { p.Finalizers = nil })},
			event:         add(podWith(lostAddresses(p1A), func(p *v1.Pod) { p.Finalizers = nil })),
			wantAddresses: map[string][]string{memberIdentity: nil}, wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{"ServiceGatewayNoNodeLocation ns/p1"}, wantFinalizer: "absent",
		},
		{
			name:          "annotation-only update: the vote is replaced",
			members:       []*v1.Pod{p1A},
			event:         update(p1A, withAnnotations(p1A.DeepCopy(), annotationsB())),
			wantVotes:     map[string]egressVoteView{"ns/p1": expectedVote("p1", memberIdentity, &cfgB, true)},
			wantAddresses: map[string][]string{memberIdentity: {p1Address}},
			wantStates:    map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantDesired: map[string]EgressConfig{memberIdentity: consensusB},
			wantFinalizer: "kept",
		},
		{
			name:    "annotation-only update to invalid annotations: an invalid vote, which the engine reports",
			members: []*v1.Pod{p1A, p2A},
			event:   update(p1A, withAnnotations(p1A.DeepCopy(), invalidAnnotations())),
			wantVotes: map[string]egressVoteView{"ns/p1": expectedVote("p1", memberIdentity, nil, true),
				"ns/p2": voteA("p2", memberIdentity)},
			wantAddresses: map[string][]string{memberIdentity: {p1Address, p2Address}},
			wantStates:    map[string]ResourceState{memberIdentity: StateCreated}, wantDesired: map[string]EgressConfig{memberIdentity: consensusApplied},
			wantEvents: []string{"InvalidEgressConfig ns/p1"}, wantFinalizer: "kept",
		},
		{
			name:      "IP change keeping accepted addresses: the pod is re-registered and keeps its vote",
			members:   []*v1.Pod{p1A},
			event:     update(p1A, podWith(p1A, func(p *v1.Pod) { withPodIPs(p, "10.244.1.9") })),
			wantVotes: map[string]egressVoteView{"ns/p1": voteA("p1", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {"10.244.1.9@" + memberNode}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantFinalizer: "kept",
		},
		{
			name:          "IP change leaving no accepted address, sole pod: the live pod leaves and the identity is deleted",
			members:       []*v1.Pod{p1A},
			event:         update(p1A, lostAddresses(p1A)),
			wantAddresses: map[string][]string{memberIdentity: nil}, wantStates: map[string]ResourceState{memberIdentity: StateDeletionPending},
			wantFinalizer: "kept",
		},
		{
			name:      "IP change leaving no accepted address, one of two pods: the identity stays",
			members:   []*v1.Pod{p1A, p2A},
			event:     update(p1A, lostAddresses(p1A)),
			wantVotes: map[string]egressVoteView{"ns/p2": voteA("p2", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantFinalizer: "kept",
		},
		{
			name:          "label move to a valid identity: the addresses and the vote move",
			members:       []*v1.Pod{p1A},
			event:         update(p1A, relabel(p1A, otherIdentity)),
			wantVotes:     map[string]egressVoteView{"ns/p1": voteA("p1", otherIdentity)},
			wantAddresses: map[string][]string{memberIdentity: nil, otherIdentity: {p1Address}},
			wantStates:    map[string]ResourceState{memberIdentity: StateDeletionPending, otherIdentity: StateNotStarted},
			wantFinalizer: "kept",
		},
		{
			name:          "label move to an identity that is no valid name: the pod is not accepted there and its vote is withdrawn",
			members:       []*v1.Pod{p1A},
			event:         update(p1A, relabel(p1A, "../evil")),
			wantAddresses: map[string][]string{memberIdentity: nil}, wantStates: map[string]ResourceState{memberIdentity: StateDeletionPending},
			wantEvents: []string{"ServiceGatewayInvalidEgressLabel ns/p1"}, wantFinalizer: "kept",
		},
		{
			name:          "label move with no accepted address left: the vote is withdrawn",
			members:       []*v1.Pod{p1A},
			event:         update(p1A, lostAddresses(relabel(p1A, otherIdentity))),
			wantAddresses: map[string][]string{memberIdentity: nil, otherIdentity: nil},
			wantStates:    map[string]ResourceState{memberIdentity: StateDeletionPending, otherIdentity: noEgressOperation},
			wantEvents:    []string{"ServiceGatewayNoNodeLocation ns/p1"}, wantFinalizer: "kept",
		},
		{
			name:          "label move to an identity colliding with an inbound service: the vote is withdrawn",
			setup:         func(dt *DiffTracker) { trackInboundService(dt, collidingIdentity) },
			members:       []*v1.Pod{p1A},
			event:         update(p1A, relabel(p1A, collidingIdentity)),
			wantAddresses: map[string][]string{memberIdentity: nil, collidingIdentity: nil},
			wantStates:    map[string]ResourceState{memberIdentity: StateDeletionPending, collidingIdentity: StateCreated},
			wantFinalizer: "kept",
		},
		{
			name:    "label move of a pod replaced meanwhile: the add is skipped and the replaced pod's vote withdrawn",
			members: []*v1.Pod{p1A},
			beforeEvent: func(t *testing.T, kube *fake.Clientset) {
				assert.NoError(t, kube.CoreV1().Pods("ns").Delete(context.Background(), "p1", metav1.DeleteOptions{}))
				_, err := kube.CoreV1().Pods("ns").Create(context.Background(),
					podWith(p1A, func(p *v1.Pod) { p.UID = "p1-replacement" }), metav1.CreateOptions{})
				assert.NoError(t, err)
			},
			event:         update(p1A, relabel(p1A, otherIdentity)),
			wantAddresses: map[string][]string{memberIdentity: nil, otherIdentity: nil},
			wantStates:    map[string]ResourceState{memberIdentity: StateDeletionPending, otherIdentity: noEgressOperation},
		},
		{
			name:          "label value removed: the pod is removed",
			members:       []*v1.Pod{p1A},
			event:         update(p1A, relabel(p1A, "")),
			wantAddresses: map[string][]string{memberIdentity: nil}, wantStates: map[string]ResourceState{memberIdentity: StateDeletionPending},
			wantRecord: true, wantFinalizer: "kept",
		},
		{
			name:          "label key removed from a live pod, deleted with its last labelled state: drained",
			members:       []*v1.Pod{p1A},
			event:         remove(p1A.DeepCopy()),
			wantAddresses: map[string][]string{memberIdentity: nil}, wantStates: map[string]ResourceState{memberIdentity: StateDeletionPending},
			wantRecord: true, wantFinalizer: "kept",
		},
		{
			name:          "label key removed from a live pod, deleted without the label: drained from the identity its vote names",
			members:       []*v1.Pod{p1A},
			event:         remove(podWith(p1A, func(p *v1.Pod) { delete(p.Labels, label) })),
			wantAddresses: map[string][]string{memberIdentity: nil}, wantStates: map[string]ResourceState{memberIdentity: StateDeletionPending},
			wantRecord: true, wantFinalizer: "kept",
		},
		{
			name:      "label value emptied unseen, then deleted: drained from the identity its vote names",
			members:   []*v1.Pod{p1A, p2A},
			event:     remove(podWith(relabel(p1A, ""), func(p *v1.Pod) { p.DeletionTimestamp = &now })),
			wantVotes: map[string]egressVoteView{"ns/p2": voteA("p2", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantRecord: true, wantFinalizer: "absent",
		},
		{
			name:      "unlabelled delete with our finalizer of a pod that is no member: its finalizer is released at once",
			members:   []*v1.Pod{p2A},
			event:     remove(podWith(relabel(p1A, ""), func(p *v1.Pod) { p.DeletionTimestamp = &now })),
			wantVotes: map[string]egressVoteView{"ns/p2": voteA("p2", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantFinalizer: "absent",
		},
		{
			name:      "DeletedFinalStateUnknown: the pod is removed from its identity",
			members:   []*v1.Pod{p1A, p2A},
			event:     remove(cache.DeletedFinalStateUnknown{Key: "ns/p1", Obj: p1A.DeepCopy()}),
			wantVotes: map[string]egressVoteView{"ns/p2": voteA("p2", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantRecord: true, wantFinalizer: "absent",
		},
		{
			name:      "pod terminating: removed, its finalizer held until its addresses drain",
			members:   []*v1.Pod{p1A, p2A},
			event:     update(p1A, podWith(p1A, func(p *v1.Pod) { p.DeletionTimestamp = &now })),
			wantVotes: map[string]egressVoteView{"ns/p2": voteA("p2", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantRecord: true, wantFinalizer: "absent",
		},
		{
			name:      "pod regaining validity with its addresses: added again with its vote",
			event:     update(podWith(p1A, func(p *v1.Pod) { p.Status.Phase = v1.PodUnknown }), p1A.DeepCopy()),
			wantVotes: map[string]egressVoteView{"ns/p1": voteA("p1", memberIdentity)}, wantAddresses: map[string][]string{memberIdentity: {p1Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantFinalizer: "kept",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := tc.api
			if api == nil {
				api = []*v1.Pod{p1A, p2A}
			}
			dt, kube, recorder := newMembershipTracker(t, api...)
			if tc.setup != nil {
				tc.setup(dt)
			}
			for _, member := range tc.members {
				assert.True(t, dt.podInformerAddPod(member.DeepCopy()), "member %s/%s", member.Namespace, member.Name)
			}
			recorder.take()
			if tc.beforeEvent != nil {
				tc.beforeEvent(t, kube)
			}

			tc.event(dt)

			votes := map[string]egressVoteView{}
			for _, podKey := range []string{"ns/p1", "ns/p2"} {
				if vote, ok := registeredVoteOf(dt, podKey); ok {
					votes[podKey] = vote
				}
			}
			assert.Equal(t, orEmpty(tc.wantVotes), votes, "votes")
			for identity, want := range tc.wantAddresses {
				assert.Equal(t, want, egressAddressesOf(dt, identity), "addresses of %s", identity)
			}
			for identity, want := range tc.wantStates {
				assert.Equal(t, want, egressStateOf(dt, identity), "operation of %s", identity)
			}
			for identity, want := range tc.wantDesired {
				assert.Equal(t, want, desiredEgressOf(dt, identity), "desired configuration of %s", identity)
			}
			assert.Equal(t, tc.wantRecord, dt.HasPendingPodDeletion("ns", "p1", "p1-uid"), "finalizer-removal record of ns/p1")
			assert.Equal(t, tc.wantEvents, recorder.take(), "events")
			assertEgressInvariants(t, dt)

			if tc.wantFinalizer != "" {
				dt.CheckPendingPodDeletions(context.Background())
				assert.Equal(t, tc.wantFinalizer == "kept", apiHasFinalizer(t, kube, p1A), "cleanup finalizer of ns/p1")
			}
		})
	}
}

// TestPodInformer_LivePodLeavingItsIdentity pins the sole member pod whose addresses change to ones without a node
// location of their family (1 -> 0): its identity is deleted as after any last pod, the live pod keeps its
// finalizer, and its later deletion releases it through the removal path, whether the identity's deletion was
// dispatched or not yet, or already completed.
func TestPodInformer_LivePodLeavingItsIdentity(t *testing.T) {
	now := metav1.Now()
	member := membershipPod("p1", memberIdentity, annotationsA())
	left := podWith(member, func(p *v1.Pod) { withPodIPs(p, "fd00::1"); onIPv4OnlyNode(p) })
	terminating := podWith(left, func(p *v1.Pod) { p.DeletionTimestamp = &now })

	cases := []struct {
		name string
		// deletion moves the identity's deletion on before the pod is deleted.
		deletion func(dt *DiffTracker)
	}{
		{name: "the pod is deleted before the identity's deletion is dispatched", deletion: func(*DiffTracker) {}},
		{name: "the pod is deleted while the identity's deletion is in progress", deletion: func(dt *DiffTracker) {
			dt.pendingServiceOps[memberIdentity].State = StateDeletionInProgress
		}},
		{name: "the pod is deleted after the identity was deleted", deletion: func(dt *DiffTracker) {
			delete(dt.pendingServiceOps, memberIdentity)
			delete(dt.pendingServiceDeletions, memberIdentity)
			dt.NRPResources.NATGateways.Delete(memberIdentity)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt, kube, _ := newMembershipTracker(t, member)
			assert.True(t, dt.podInformerAddPod(member.DeepCopy()))

			dt.reconcileEgressPodUpdate(member, left)

			assert.Equal(t, StateDeletionPending, egressStateOf(dt, memberIdentity), "the identity is deleted after its last pod")
			assert.Empty(t, votersOf(dt), "the pod is no member")
			assert.Empty(t, egressAddressesOf(dt, memberIdentity), "the pod's addresses are drained")
			assert.False(t, dt.HasPendingPodDeletion("ns", "p1", ""), "a live pod gets no finalizer-removal record")
			dt.CheckPendingPodDeletions(context.Background())
			assert.NoError(t, dt.RemoveLastPodFinalizers(context.Background(), memberIdentity))
			assert.True(t, apiHasFinalizer(t, kube, member), "the live pod keeps its finalizer")
			assertEgressInvariants(t, dt)

			dt.mu.Lock()
			tc.deletion(dt)
			dt.mu.Unlock()
			dt.reconcileEgressPodUpdate(left, terminating)

			assert.False(t, apiHasFinalizer(t, kube, member), "the pod's deletion releases its finalizer")
			assertEgressInvariants(t, dt)
		})
	}
}

// TestPodInformer_PartialDualStackMemberLosingFamilies pins a dual-stack member losing the node location of one
// family and then its last accepted address (2 -> 1 -> 0): with one family left it stays a member with its vote,
// without the identity being scheduled for deletion; with none left it leaves the identity as a live pod; and when an
// accepted address comes back before the deletion was dispatched, the identity is revived.
func TestPodInformer_PartialDualStackMemberLosingFamilies(t *testing.T) {
	dualStack := membershipPod("p1", memberIdentity, annotationsA(), "10.244.1.1", "fd00::1")
	ipv4Left := podWith(dualStack, onIPv4OnlyNode)
	noneLeft := podWith(ipv4Left, func(p *v1.Pod) { withPodIPs(p, "fd00::1") })
	back := podWith(noneLeft, func(p *v1.Pod) { withPodIPs(p, "10.244.1.1") })

	dt, kube, _ := newMembershipTracker(t, dualStack)
	assert.True(t, dt.podInformerAddPod(dualStack.DeepCopy()))
	assert.Equal(t, []string{"10.244.1.1@" + memberNode, "fd00::1@" + membershipNodeV6}, egressAddressesOf(dt, memberIdentity))

	steps := []struct {
		name          string
		oldPod        *v1.Pod
		newPod        *v1.Pod
		wantAddresses []string
		wantMember    bool
		wantDeleted   bool
	}{
		{name: "2 -> 1: the IPv4 family stays", oldPod: dualStack, newPod: ipv4Left,
			wantAddresses: []string{"10.244.1.1@" + memberNode}, wantMember: true},
		{name: "1 -> 0: the live pod leaves the identity", oldPod: ipv4Left, newPod: noneLeft, wantDeleted: true},
		{name: "0 -> 1 before the deletion is dispatched: the identity is revived", oldPod: noneLeft, newPod: back,
			wantAddresses: []string{"10.244.1.1@" + memberNode}, wantMember: true},
	}
	for _, step := range steps {
		dt.reconcileEgressPodUpdate(step.oldPod, step.newPod)

		assert.Equal(t, step.wantAddresses, egressAddressesOf(dt, memberIdentity), "%s: addresses", step.name)
		vote, voted := registeredVoteOf(dt, "ns/p1")
		assert.Equal(t, step.wantMember, voted, "%s: membership", step.name)
		if voted {
			assert.Equal(t, expectedVote("p1", memberIdentity, &consensusA, true), vote, "%s: vote", step.name)
		}
		state := egressStateOf(dt, memberIdentity)
		assert.Equal(t, step.wantDeleted, state == StateDeletionPending || state == StateDeletionInProgress, "%s: operation %s", step.name, state)
		assert.False(t, dt.HasPendingPodDeletion("ns", "p1", ""), "%s: a live pod gets no finalizer-removal record", step.name)
		assert.True(t, apiHasFinalizer(t, kube, dualStack), "%s: the live pod keeps its finalizer", step.name)
		assertEgressInvariants(t, dt)
	}
}

// TestPodInformer_VotesParsedForTheIdentitysFamilies pins the parse context of a vote: the IP families of the
// identity's NAT Gateway, decided when its operation was created, else the cluster's, which a new operation and the
// re-creation of an identity being deleted take, so every member of an identity votes in the same terms, when it joins
// and when its annotations change, and pods without annotations agree on its default.
func TestPodInformer_VotesParsedForTheIdentitysFamilies(t *testing.T) {
	ipv4Default := defaultEgressConfig(egressParseContext{})
	dualStackDefault := defaultEgressConfig(egressParseContext{WantsIPv6: true})

	cases := []struct {
		name string
		// operation tracks the identity's operation; nil: none.
		operation func(dt *DiffTracker)
		// ipv6 is set when the votes are parsed for a NAT Gateway that serves IPv6.
		ipv6      bool
		wantState ResourceState
	}{
		{
			name: "an identity created IPv4-only on a cluster that now has IPv6 keeps IPv4 terms",
			operation: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, ipv4Default).Config.OutboundConfig.IPFamilies = ipv4Families()
			},
			wantState: StateCreated,
		},
		{
			name: "a dual-stack identity",
			operation: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, dualStackDefault).Config.OutboundConfig.IPFamilies = dualStackFamilies()
			},
			ipv6: true, wantState: StateCreated,
		},
		{
			name: "an identity known only from NRP, deleted after its last pod, takes the cluster's families as its re-creation",
			operation: func(dt *DiffTracker) {
				dt.mu.Lock()
				defer dt.mu.Unlock()
				dt.markOutboundServiceForDeletionLocked(memberIdentity)
			},
			ipv6: true, wantState: StateDeletionPending,
		},
		{
			name: "an identity created IPv4-only whose deletion is in progress takes the cluster's families, as its re-creation",
			operation: func(dt *DiffTracker) {
				op := trackManagedEgressIdentity(dt, memberIdentity, ipv4Default)
				op.Config.OutboundConfig.IPFamilies = ipv4Families()
				op.State = StateDeletionInProgress
			},
			ipv6: true, wantState: StateDeletionInProgress,
		},
		{name: "a new identity takes the cluster's families", ipv6: true, wantState: noEgressOperation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pc := egressParseContext{WantsIPv6: tc.ipv6}
			pod := membershipPod("p1", memberIdentity, nil)
			dt := seededDT(t, gomock.NewController(t), fake.NewSimpleClientset(pod.DeepCopy()),
				K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(), Nodes: map[string]Node{}},
				NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(memberIdentity), Locations: map[string]NRPLocation{}})
			dt.seedClusterAddressFamilies(map[string][]string{"node": {memberNode, membershipNodeV6}})
			if tc.operation != nil {
				tc.operation(dt)
			}

			assert.True(t, dt.podInformerAddPod(pod.DeepCopy()))

			vote, ok := registeredVoteOf(dt, "ns/p1")
			assert.True(t, ok)
			assert.Equal(t, defaultEgressConfig(pc), vote.Config)
			assert.False(t, vote.HasAnnotations)
			if tc.wantState != StateDeletionPending {
				assert.Equal(t, tc.wantState, egressStateOf(dt, memberIdentity), "a pod without annotations agrees with the applied default")
			}

			dt.reconcileEgressPodUpdate(pod, withAnnotations(pod.DeepCopy(), annotationsA()))

			want, _, err := parseEgressConfig(annotationsA(), pc)
			assert.NoError(t, err)
			vote, _ = registeredVoteOf(dt, "ns/p1")
			assert.Equal(t, want, vote.Config, "a change of the annotations alone is parsed in the same terms")
		})
	}
}

// TestPodInformer_PodJoiningADeletionVotesForTheReCreation: a pod joining an identity whose deletion is in progress
// waits for its re-creation, which takes the cluster's IP families, not those of the NAT Gateway being deleted. Its
// vote is parsed for the re-creation's families, so the re-created identity is created with its agreed default.
func TestPodInformer_PodJoiningADeletionVotesForTheReCreation(t *testing.T) {
	pod := membershipPod("p1", memberIdentity, nil)
	dt := seededDT(t, gomock.NewController(t), fake.NewSimpleClientset(pod.DeepCopy()),
		K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(), Nodes: map[string]Node{}},
		NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(), Locations: map[string]NRPLocation{}})
	dt.seedClusterAddressFamilies(map[string][]string{"node": {memberNode, membershipNodeV6}})
	trackManagedEgressIdentity(dt, memberIdentity, defaultEgressConfig(egressParseContext{})).State = StateDeletionInProgress
	assert.True(t, dt.podInformerAddPod(pod.DeepCopy()))

	dt.OnServiceCreationComplete(memberIdentity, true, nil)
	dispatchWithoutRunning(t, dt)

	dt.mu.Lock()
	defer dt.mu.Unlock()
	op := dt.pendingServiceOps[memberIdentity]
	if !assert.NotNil(t, op) || !assert.Equal(t, StateCreationInProgress, op.State, "control: the identity is re-created") {
		return
	}
	assert.Equal(t, dualStackFamilies(), op.Config.OutboundConfig.IPFamilies)
	dualStackDefault := defaultEgressConfig(dt.egressParseContextFor(dualStackFamilies()))
	agreed, ok := dt.egress.votes.agreed(memberIdentity)
	assert.True(t, ok)
	assert.Equal(t, dualStackDefault, agreed, "the vote is cast for the re-created identity")
	assert.Equal(t, dualStackDefault, outboundEgress(op.InFlightConfig.OutboundConfig), "the creation configuration")
}

// ipv6ArrivingNodeLister is a Node lister whose cache starts exposing IPv6 right after its first List: that List
// answers with the nodes it listed and returns once released, after the test added an IPv6 node.
type ipv6ArrivingNodeLister struct {
	corelisters.NodeLister
	listed, release chan struct{}
	once            sync.Once
}

func (l *ipv6ArrivingNodeLister) List(selector labels.Selector) ([]*v1.Node, error) {
	nodes, err := l.NodeLister.List(selector)
	l.once.Do(func() {
		close(l.listed)
		<-l.release
	})
	return nodes, err
}

// TestPodInformerAddPod_VoteParsedForTheFamiliesOfItsOperation: the Node cache starts exposing IPv6 while a pod
// creates its identity, right after the identity's IP families were first looked up. The pod's vote is parsed in
// the critical section that registers it, once the operation decided its families, so it is parsed for those and
// the operation is created with it, whichever families the cluster has by then.
func TestPodInformerAddPod_VoteParsedForTheFamiliesOfItsOperation(t *testing.T) {
	node := func(name, ip string) *v1.Node {
		return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: ip}}}}
	}
	pod := membershipPod("p1", memberIdentity, nil)
	dt := newProviderDiffTracker(t, gomock.NewController(t), fake.NewSimpleClientset(pod.DeepCopy()))
	nodes := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	assert.NoError(t, nodes.Add(node("ipv4", memberNode)))
	lister := &ipv6ArrivingNodeLister{NodeLister: corelisters.NewNodeLister(nodes), listed: make(chan struct{}), release: make(chan struct{})}
	dt.SetNodeLister(lister)

	added := make(chan bool)
	go func() { added <- dt.podInformerAddPod(pod.DeepCopy()) }()
	<-lister.listed
	assert.NoError(t, nodes.Add(node("ipv6", membershipNodeV6)))
	close(lister.release)
	assert.True(t, <-added)

	assert.Equal(t, dualStackFamilies(), dt.outboundIPFamilies(), "control: the cluster exposes IPv6 now")
	dispatchWithoutRunning(t, dt)
	dt.mu.Lock()
	defer dt.mu.Unlock()
	op := dt.pendingServiceOps[memberIdentity]
	if !assert.NotNil(t, op) || !assert.NotNil(t, op.InFlightConfig, "control: the creation is dispatched") {
		return
	}
	want := defaultEgressConfig(dt.egressParseContextFor(op.Config.OutboundConfig.IPFamilies))
	vote, _ := dt.egress.votes.get("ns/p1")
	assert.Equal(t, want, vote.Config, "the vote is parsed for the families of its operation, %v", op.Config.OutboundConfig.IPFamilies)
	assert.Equal(t, want, outboundEgress(op.InFlightConfig.OutboundConfig), "the operation is created with it")
}

// TestParseEgressVote pins the vote the pod informer casts for a pod: parsed for the IP families of its parse context
// from the pod's egress annotations, which it keeps alone (none when the pod has none), so that the engine parses it
// again for other families exactly as the informer parses the pod for them.
func TestParseEgressVote(t *testing.T) {
	ipv4 := egressParseContext{NetworkSubscription: "sub", NodeResourceGroup: "mc-rg"}
	dual := egressParseContext{WantsIPv6: true, NetworkSubscription: "sub", NodeResourceGroup: "mc-rg"}
	unrelated := map[string]string{"example.com/team": "a", "kubernetes.azure.com/other": "b", consts.PodLabelServiceEgressGateway: "c"}
	with := func(annotations ...map[string]string) map[string]string {
		merged := map[string]string{}
		for _, a := range annotations {
			maps.Copy(merged, a)
		}
		return merged
	}
	ipv6Settings := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"}
	unknown := map[string]string{consts.PodAnnotationServiceEgressGatewayPrefix + "bogus": "1"}

	cases := []struct {
		name            string
		annotations     map[string]string
		wantAnnotations map[string]string
	}{
		{name: "no annotations"},
		{name: "no egress annotation", annotations: unrelated},
		{name: "egress annotations among others", annotations: with(unrelated, annotationsA()), wantAnnotations: annotationsA()},
		{name: "IPv6 settings", annotations: with(unrelated, ipv6Settings), wantAnnotations: ipv6Settings},
		{name: "an unknown egress annotation, which makes the vote invalid", annotations: with(unrelated, unknown, annotationsA()),
			wantAnnotations: with(unknown, annotationsA())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := withAnnotations(membershipPod("p1", memberIdentity, nil), tc.annotations)
			for _, pc := range []egressParseContext{ipv4, dual} {
				vote := parseEgressVote(pod, pc)

				assert.Equal(t, tc.wantAnnotations, vote.Annotations, "IPv6 %t: the egress annotations alone are kept", pc.WantsIPv6)
				assert.Equal(t, pc.WantsIPv6, vote.ParsedForIPv6)
				cfg, hasAnnotations, err := parseEgressConfig(pod.Annotations, pc)
				assert.Equal(t, hasAnnotations, vote.HasAnnotations)
				if err != nil {
					assert.Equal(t, err.Error(), vote.Invalid)
				} else {
					assert.Empty(t, vote.Invalid)
					assert.Equal(t, cfg, vote.Config)
				}
				for _, other := range []egressParseContext{ipv4, dual} {
					assert.Equal(t, parseEgressVote(pod, other), vote.reparsed(other), "IPv6 %t, parsed again for IPv6 %t",
						pc.WantsIPv6, other.WantsIPv6)
				}
			}
		})
	}
}

// ================================================================================================
// Startup admission reconciled by the pod informer (4.6): the informer starts after startup admitted the members of
// its own listing, and reconciles what became of them meanwhile
// ================================================================================================

// newAdmittedTracker returns a real engine on a fake API server holding api, whose egress identity memberIdentity is a
// created managed one, in NRP, with the IPv4 default configuration applied, once startup admitted the members among
// listed as InitializeFromCluster does (processK8sEgresses, seedEgressVotes), and the recorder of the events it emits.
func newAdmittedTracker(t *testing.T, listed, api []*v1.Pod) (*DiffTracker, *fake.Clientset, *podEventsRecorder) {
	t.Helper()
	objects := func(pods []*v1.Pod) []runtime.Object {
		out := make([]runtime.Object, 0, len(pods))
		for _, pod := range pods {
			out = append(out, pod.DeepCopy())
		}
		return out
	}
	k8s := newK8sStateForSeeders()
	listing, err := processK8sEgresses(context.Background(), fake.NewSimpleClientset(objects(listed)...), &k8s)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	kube := fake.NewSimpleClientset(objects(api)...)
	dt := seededDT(t, gomock.NewController(t), kube, k8s,
		NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(), Locations: map[string]NRPLocation{}})
	trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
	dt.seedEgressVotes(listing)
	recorder := &podEventsRecorder{}
	dt.SetEventRecorder(recorder)
	return dt, kube, recorder
}

// registeredVotesOf returns the registered votes of the pods podKeys, by pod key; a pod without a vote is not listed.
func registeredVotesOf(dt *DiffTracker, podKeys ...string) map[string]egressVoteView {
	votes := map[string]egressVoteView{}
	for _, podKey := range podKeys {
		if vote, ok := registeredVoteOf(dt, podKey); ok {
			votes[podKey] = vote
		}
	}
	return votes
}

// TestReconcileEgressPodAdd_ReconcilesThePodStartupAdmitted pins the first ADD of a pod that startup admitted as a
// member (4.6): the informer, which starts after startup, reports the pod as it is now, and what became of it meanwhile
// is applied as the update from the pod as startup admitted it. A member that is no longer one (evicted, completed with
// or without its addresses, terminating, label value emptied, no accepted address left) leaves the identity it was
// admitted to with its vote and the addresses it was admitted with, and that identity is evaluated again; a member
// moved to another identity leaves it too, keeping its finalizer, and joins the other one where it is a member; a
// member whose address changed keeps only its new one; a member replaced by a same-name pod is removed before the
// replacement, a new pod, is added, also when the replacement is gone again and is not added: no delete will report
// the admitted pod. An unchanged member keeps its vote, raises no event and gets its cleanup finalizer.
func TestReconcileEgressPodAdd_ReconcilesThePodStartupAdmitted(t *testing.T) {
	now := metav1.Now()
	p1A := membershipPod("p1", memberIdentity, annotationsA())
	p2B := membershipPod("p2", memberIdentity, annotationsB())
	p1IP := memberIP(memberPod("p1"))
	p1Address := p1IP + "@" + memberNode
	p2Address := memberIP(memberPod("p2")) + "@" + memberNode
	cfgA, cfgB := consensusA, consensusB
	voteA := expectedVote("p1", memberIdentity, &cfgA, true)
	voteB := expectedVote("p2", memberIdentity, &cfgB, true)
	relabel := func(identity string) func(*v1.Pod) {
		return func(p *v1.Pod) { p.Labels[consts.PodLabelServiceEgressGateway] = identity }
	}
	withoutFinalizer := podWith(p1A, func(p *v1.Pod) { p.Finalizers = nil })
	replacement := podWith(p1A, func(p *v1.Pod) { p.UID += "-replacement"; withPodIPs(p, "10.244.1.7") })
	replacementVote := voteA
	replacementVote.PodUID = replacement.UID
	// left is the identity once ns/p1 left it: ns/p2 alone votes B, which is applied.
	left := struct {
		votes     map[string]egressVoteView
		addresses map[string][]string
		states    map[string]ResourceState
	}{
		votes:     map[string]egressVoteView{"ns/p2": voteB},
		addresses: map[string][]string{memberIdentity: {p2Address}},
		states:    map[string]ResourceState{memberIdentity: StateUpdateInProgress},
	}

	cases := []struct {
		name     string
		admitted *v1.Pod // ns/p1 as startup admitted it; nil: p1A
		listed   *v1.Pod // ns/p1 as the informer first lists it, as the API server holds it
		gone     bool    // the API server no longer holds ns/p1 when its ADD is handled

		wantVotes     map[string]egressVoteView
		wantAddresses map[string][]string // identity -> addresses, live or buffered
		wantStates    map[string]ResourceState
		wantDesired   EgressConfig // of memberIdentity
		wantDrained   []string     // the addresses of the finalizer-removal record of the admitted ns/p1
		wantEvents    []string
		wantFinalizer bool // ns/p1 of the API server, once the drains are checked
	}{
		{
			name:      "an unchanged member keeps its vote and its address and raises no event",
			listed:    p1A,
			wantVotes: map[string]egressVoteView{"ns/p1": voteA, "ns/p2": voteB}, wantAddresses: map[string][]string{memberIdentity: {p1Address, p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated}, wantDesired: consensusApplied, wantFinalizer: true,
		},
		{
			name:     "a member created while the CCM was down gets its cleanup finalizer and raises no event",
			admitted: withoutFinalizer, listed: withoutFinalizer,
			wantVotes: map[string]egressVoteView{"ns/p1": voteA, "ns/p2": voteB}, wantAddresses: map[string][]string{memberIdentity: {p1Address, p2Address}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated}, wantDesired: consensusApplied, wantFinalizer: true,
		},
		{
			// Admitted without the cleanup finalizer (created while the CCM was down): the new vote is cast by the add,
			// which also gives the pod its finalizer.
			name:          "a member whose egress annotations changed casts its new vote",
			admitted:      withoutFinalizer,
			listed:        withAnnotations(withoutFinalizer.DeepCopy(), annotationsB()),
			wantVotes:     map[string]egressVoteView{"ns/p1": expectedVote("p1", memberIdentity, &cfgB, true), "ns/p2": voteB},
			wantAddresses: map[string][]string{memberIdentity: {p1Address, p2Address}},
			wantStates:    map[string]ResourceState{memberIdentity: StateUpdateInProgress}, wantDesired: consensusB, wantFinalizer: true,
		},
		{
			name:      "an evicted member leaves its identity, and the vote of the member left is applied",
			listed:    podWith(p1A, func(p *v1.Pod) { p.Status.Phase = v1.PodFailed }),
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantDrained: []string{p1IP},
		},
		{
			name: "a completed member whose addresses were cleared leaves with the addresses it was admitted with",
			listed: podWith(p1A, func(p *v1.Pod) {
				p.Status.Phase, p.Status.PodIP, p.Status.PodIPs = v1.PodSucceeded, "", nil
			}),
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantDrained: []string{p1IP},
		},
		{
			name:      "a terminating member leaves its identity",
			listed:    podWith(p1A, func(p *v1.Pod) { p.DeletionTimestamp = &now }),
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantDrained: []string{p1IP},
		},
		{
			name:      "a member whose egress label value was emptied leaves its identity",
			listed:    podWith(p1A, relabel("")),
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantDrained: []string{p1IP},
		},
		{
			name:      "a member left without an accepted address leaves its identity as a live pod and keeps its finalizer",
			listed:    podWith(p1A, func(p *v1.Pod) { withPodIPs(p, "fd00::1"); onIPv4OnlyNode(p) }),
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantFinalizer: true,
		},
		{
			name:          "a member moved to another identity joins it and leaves the one it was admitted to",
			listed:        podWith(p1A, relabel(otherIdentity)),
			wantVotes:     map[string]egressVoteView{"ns/p1": expectedVote("p1", otherIdentity, &cfgA, true), "ns/p2": voteB},
			wantAddresses: map[string][]string{memberIdentity: {p2Address}, otherIdentity: {p1Address}},
			wantStates:    map[string]ResourceState{memberIdentity: StateUpdateInProgress, otherIdentity: StateNotStarted},
			wantDesired:   consensusB, wantFinalizer: true,
		},
		{
			name:      "a member moved to an identity it is no member of leaves the one it was admitted to and keeps its finalizer",
			listed:    podWith(p1A, relabel("../evil")),
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantEvents: []string{"ServiceGatewayInvalidEgressLabel ns/p1"}, wantFinalizer: true,
		},
		{
			name:          "a member whose address changed is registered with its new address only",
			listed:        podWith(p1A, func(p *v1.Pod) { withPodIPs(p, "10.244.1.9") }),
			wantVotes:     map[string]egressVoteView{"ns/p1": voteA, "ns/p2": voteB},
			wantAddresses: map[string][]string{memberIdentity: {p2Address, "10.244.1.9@" + memberNode}},
			wantStates:    map[string]ResourceState{memberIdentity: StateCreated}, wantDesired: consensusApplied, wantFinalizer: true,
		},
		{
			name:          "a member replaced by a same-name pod is removed before the replacement, a new pod, is added",
			listed:        replacement,
			wantVotes:     map[string]egressVoteView{"ns/p1": replacementVote, "ns/p2": voteB},
			wantAddresses: map[string][]string{memberIdentity: {p2Address, "10.244.1.7@" + memberNode}},
			wantStates:    map[string]ResourceState{memberIdentity: StateCreated}, wantDesired: consensusApplied,
			wantDrained: []string{p1IP}, wantEvents: []string{"EgressConfigConflict ns/p1"}, wantFinalizer: true,
		},
		{
			name:   "a member replaced by a same-name pod that is gone again when added is removed all the same",
			listed: replacement, gone: true,
			wantVotes: left.votes, wantAddresses: left.addresses, wantStates: left.states, wantDesired: consensusB,
			wantDrained: []string{p1IP},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admitted := tc.admitted
			if admitted == nil {
				admitted = p1A
			}
			api := []*v1.Pod{tc.listed, p2B}
			if tc.gone {
				api = api[1:]
			}
			dt, kube, recorder := newAdmittedTracker(t, []*v1.Pod{admitted, p2B}, api)

			dt.reconcileEgressPodAdd(tc.listed.DeepCopy())
			dt.reconcileEgressPodAdd(p2B.DeepCopy())

			assert.Equal(t, tc.wantVotes, registeredVotesOf(dt, "ns/p1", "ns/p2"), "votes")
			for identity, want := range tc.wantAddresses {
				assert.Equal(t, want, egressAddressesOf(dt, identity), "addresses of %s", identity)
			}
			for identity, want := range tc.wantStates {
				assert.Equal(t, want, egressStateOf(dt, identity), "operation of %s", identity)
			}
			dt.mu.Lock()
			desired := desiredEgressOf(dt, memberIdentity)
			var drained []string
			if record, ok := dt.pendingPodDeletions["ns/p1"]; ok && record.UID == string(admitted.UID) {
				drained = record.Addresses
			}
			dt.mu.Unlock()
			assert.Equal(t, tc.wantDesired, desired, "desired configuration of %s", memberIdentity)
			assert.Equal(t, tc.wantDrained, drained, "finalizer-removal record of the admitted ns/p1")
			assert.Equal(t, tc.wantEvents, recorder.take(), "events")
			assertEgressInvariants(t, dt)

			dt.CheckPendingPodDeletions(context.Background())
			if !tc.gone {
				assert.Equal(t, tc.wantFinalizer, apiHasFinalizer(t, kube, tc.listed), "cleanup finalizer of ns/p1")
			}
		})
	}
}

// TestSetUpPodInformer_RemovesThePodsStartupAdmittedThatItDoesNotList pins the reconciliation of startup admission once
// the informer's store holds its listing (4.6): a member that startup admitted and that the informer does not list, as
// it was deleted or left the informer's selection before the informer started, leaves the identity it was admitted to,
// with its vote and the addresses it was admitted with, each only where it still holds it, so the vote of the members
// left is applied, or the identity is deleted after its last member. A same-name replacement and a new pod holding its
// address are members and keep their votes and addresses. A pod out of the selection has its cleanup finalizer released
// once its address drained, also when a new pod takes the address after its removal. Unchanged members raise no event,
// and the admitted pods are forgotten.
func TestSetUpPodInformer_RemovesThePodsStartupAdmittedThatItDoesNotList(t *testing.T) {
	p1A := membershipPod("p1", memberIdentity, annotationsA())
	p2B := membershipPod("p2", memberIdentity, annotationsB())
	p1IP := memberIP(memberPod("p1"))
	p1Address := p1IP + "@" + memberNode
	p2Address := memberIP(memberPod("p2")) + "@" + memberNode
	cfgA, cfgB := consensusA, consensusB
	voteA := expectedVote("p1", memberIdentity, &cfgA, true)
	voteB := expectedVote("p2", memberIdentity, &cfgB, true)
	unselected := func(pod *v1.Pod) *v1.Pod {
		return podWith(pod, func(p *v1.Pod) { delete(p.Labels, consts.PodLabelServiceEgressGateway) })
	}
	replacement := podWith(membershipPod("p1", memberIdentity, annotationsB()), func(p *v1.Pod) {
		p.UID += "-replacement"
		withPodIPs(p, "10.244.1.7")
	})
	replacementVote := expectedVote("p1", memberIdentity, &cfgB, true)
	replacementVote.PodUID = replacement.UID
	// p9 is a new pod that got the address of ns/p1.
	p9 := membershipPod("p9", memberIdentity, annotationsB(), p1IP)
	completed := podWith(p1A, func(p *v1.Pod) { p.Status.Phase = v1.PodSucceeded })
	applied := consensusApplied

	cases := []struct {
		name string
		api  []*v1.Pod // what the API server holds and the informer lists of it; startup admitted p1A and p2B

		wantVotes     map[string]egressVoteView
		wantAddresses []string // of memberIdentity
		wantState     ResourceState
		wantDesired   *EgressConfig // nil: not checked
		wantEvents    []string
		// the addresses of the finalizer-removal record of the admitted ns/p1; nil: not checked
		wantDrained   []string
		wantFinalizer bool // ns/p1 of the API server, once the drains are checked, when it holds one
	}{
		{
			name: "unchanged members keep their votes and their addresses and raise no event",
			api:  []*v1.Pod{p1A, p2B}, wantVotes: map[string]egressVoteView{"ns/p1": voteA, "ns/p2": voteB},
			wantAddresses: []string{p1Address, p2Address}, wantState: StateCreated, wantDesired: &applied, wantFinalizer: true,
		},
		{
			name: "a member deleted before the informer started leaves, and the vote of the member left is applied",
			api:  []*v1.Pod{p2B}, wantVotes: map[string]egressVoteView{"ns/p2": voteB},
			wantAddresses: []string{p2Address}, wantState: StateUpdateInProgress, wantDesired: &cfgB, wantDrained: []string{p1IP},
		},
		{
			name:      "members all deleted before the informer started leave, and their identity is deleted after the last",
			wantVotes: map[string]egressVoteView{}, wantState: StateDeletionPending,
		},
		{
			name: "a member replaced by a same-name pod leaves; the replacement, with another UID, stays",
			api:  []*v1.Pod{replacement, p2B}, wantVotes: map[string]egressVoteView{"ns/p1": replacementVote, "ns/p2": voteB},
			wantAddresses: []string{p2Address, "10.244.1.7@" + memberNode}, wantState: StateUpdateInProgress, wantDesired: &cfgB,
			wantFinalizer: true,
		},
		{
			// ns/p9 joins once ns/p1 left: it agrees with ns/p2.
			name: "a deleted member whose address a new pod holds leaves; the new pod keeps the address",
			api:  []*v1.Pod{p9, p2B}, wantVotes: map[string]egressVoteView{"ns/p9": expectedVote("p9", memberIdentity, &cfgB, true), "ns/p2": voteB},
			wantAddresses: []string{p1Address, p2Address}, wantState: StateUpdateInProgress, wantDesired: &cfgB,
			wantDrained: []string{p1IP},
		},
		{
			name: "a member out of the informer's selection leaves, and its cleanup finalizer is released once drained",
			api:  []*v1.Pod{unselected(p1A), p2B}, wantVotes: map[string]egressVoteView{"ns/p2": voteB},
			wantAddresses: []string{p2Address}, wantState: StateUpdateInProgress, wantDesired: &cfgB, wantDrained: []string{p1IP},
		},
		{
			name:          "a completed member out of the selection leaves before a new pod takes its address, and its cleanup finalizer is released once drained",
			api:           []*v1.Pod{unselected(completed), p9, p2B},
			wantVotes:     map[string]egressVoteView{"ns/p9": expectedVote("p9", memberIdentity, &cfgB, true), "ns/p2": voteB},
			wantAddresses: []string{p1Address, p2Address}, wantState: StateUpdateInProgress, wantDesired: &cfgB,
			wantDrained: []string{p1IP},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt, kube, recorder := newAdmittedTracker(t, []*v1.Pod{p1A, p2B}, tc.api)
			// The handler's API reads (AddPodFinalizer) wait until SetUpPodInformer returned, so that the removal of the
			// unlisted pods always comes before the ADDs of the members: no timing decides the events.
			release := make(chan struct{})
			kube.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				<-release
				return false, nil, nil
			})
			stop := make(chan struct{})
			defer close(stop)

			err := dt.SetUpPodInformer(stop)
			close(release)
			if !assert.NoError(t, err) {
				return
			}
			// The handler processes the listing after SetUpPodInformer returned: wait until it reached the end state.
			var events []string
			assert.Eventually(t, func() bool {
				events = append(events, recorder.take()...)
				dt.mu.Lock()
				forgotten := len(dt.egress.admitted) == 0
				dt.mu.Unlock()
				return forgotten && reflect.DeepEqual(tc.wantVotes, registeredVotesOf(dt, "ns/p1", "ns/p2", "ns/p9")) &&
					slices.Equal(tc.wantAddresses, egressAddressesOf(dt, memberIdentity)) && slices.Equal(tc.wantEvents, events)
			}, 10*time.Second, 10*time.Millisecond, "the handler processes the informer's listing")

			assert.Equal(t, tc.wantVotes, registeredVotesOf(dt, "ns/p1", "ns/p2", "ns/p9"), "votes")
			assert.Equal(t, tc.wantAddresses, egressAddressesOf(dt, memberIdentity), "addresses")
			assert.Equal(t, tc.wantState, egressStateOf(dt, memberIdentity), "operation")
			dt.mu.Lock()
			if tc.wantDesired != nil {
				assert.Equal(t, *tc.wantDesired, desiredEgressOf(dt, memberIdentity), "desired configuration")
			}
			assert.Empty(t, dt.egress.admitted, "the admitted pods are forgotten")
			var drained []string
			if record, ok := dt.pendingPodDeletions["ns/p1"]; ok && record.UID == string(p1A.UID) {
				drained = record.Addresses
			}
			dt.mu.Unlock()
			if tc.wantDrained != nil {
				assert.Equal(t, tc.wantDrained, drained, "the removal of the admitted ns/p1 waits for its address to drain")
			}
			assert.Equal(t, tc.wantEvents, append(events, recorder.take()...), "events")
			assertEgressInvariants(t, dt)

			dt.CheckPendingPodDeletions(context.Background())
			if got, err := kube.CoreV1().Pods("ns").Get(context.Background(), "p1", metav1.GetOptions{}); err == nil {
				assert.Equal(t, tc.wantFinalizer, hasPodFinalizer(got), "cleanup finalizer of ns/p1")
			}
		})
	}
}

// TestSetUpPodInformer_DoesNotWaitForTheHandler pins that startup does not wait for the informer's handler, whose ADDs
// each read the pod from the API server (AddPodFinalizer), and that a member startup admitted that the informer's store
// holds is left to its own ADD, however late: it is never taken for unlisted. Once its ADD came through, the unchanged
// members keep their votes and addresses, raise no event and are forgotten.
func TestSetUpPodInformer_DoesNotWaitForTheHandler(t *testing.T) {
	p1A := membershipPod("p1", memberIdentity, annotationsA())
	p2B := membershipPod("p2", memberIdentity, annotationsB())
	cfgA, cfgB := consensusA, consensusB
	dt, kube, recorder := newAdmittedTracker(t, []*v1.Pod{p1A, p2B}, []*v1.Pod{p1A, p2B})
	// The handler's API reads are held until the test releases them; the informer's store syncs regardless.
	release := make(chan struct{})
	kube.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		<-release
		return false, nil, nil
	})
	stop := make(chan struct{})
	defer close(stop)

	returned := make(chan error, 1)
	go func() { returned <- dt.SetUpPodInformer(stop) }()
	select {
	case err := <-returned:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("SetUpPodInformer waited for the handler's API reads")
	}

	wantVotes := map[string]egressVoteView{
		"ns/p1": expectedVote("p1", memberIdentity, &cfgA, true),
		"ns/p2": expectedVote("p2", memberIdentity, &cfgB, true),
	}
	wantAddresses := []string{memberIP(memberPod("p1")) + "@" + memberNode, memberIP(memberPod("p2")) + "@" + memberNode}
	assert.Equal(t, wantVotes, registeredVotesOf(dt, "ns/p1", "ns/p2"), "the listed members keep their votes while their ADDs wait")
	assert.Equal(t, wantAddresses, egressAddressesOf(dt, memberIdentity), "and their addresses")
	assert.False(t, dt.HasPendingPodDeletion("ns", "p1", ""), "ns/p1 is not removed")
	assert.False(t, dt.HasPendingPodDeletion("ns", "p2", ""), "ns/p2 is not removed")

	close(release)
	assert.Eventually(t, func() bool {
		dt.mu.Lock()
		defer dt.mu.Unlock()
		return len(dt.egress.admitted) == 0
	}, 10*time.Second, 10*time.Millisecond, "the listed members are reconciled by their ADDs and forgotten")
	assert.Equal(t, wantVotes, registeredVotesOf(dt, "ns/p1", "ns/p2"), "votes after the ADDs")
	assert.Equal(t, wantAddresses, egressAddressesOf(dt, memberIdentity), "addresses after the ADDs")
	assert.Equal(t, StateCreated, egressStateOf(dt, memberIdentity))
	assert.Empty(t, recorder.take(), "unchanged members raise no event")
}

// TestEgressPodListOptions pins the requests of the pod informer (egressPodListOptions): each selects the pods with the
// egress label key, and its first list, which the reflector makes with resourceVersion "0" and the API server may serve
// from a stale watch cache, is made consistent, so that the informer's store is never older than startup's listing
// (4.6). Any other resource version, and every other option, is kept as the reflector set it.
func TestEgressPodListOptions(t *testing.T) {
	timeout := ptr.To[int64](300)
	cases := []struct {
		name                string
		options             metav1.ListOptions // as the reflector sets them
		wantResourceVersion string             // "": a consistent read
	}{
		{
			name:    "the first list, from resourceVersion 0, is made consistent",
			options: metav1.ListOptions{ResourceVersion: "0", Limit: 500},
		},
		{
			name:    "a consistent list, once a resource version was unavailable, is kept",
			options: metav1.ListOptions{Limit: 500},
		},
		{
			name:                "a relist from the last resource version seen is kept",
			options:             metav1.ListOptions{ResourceVersion: "123"},
			wantResourceVersion: "123",
		},
		{
			name:    "a continued list is kept",
			options: metav1.ListOptions{Continue: "next", Limit: 500},
		},
		{
			name:                "a watch from the resource version of the list is kept",
			options:             metav1.ListOptions{ResourceVersion: "123", AllowWatchBookmarks: true, TimeoutSeconds: timeout},
			wantResourceVersion: "123",
		},
		{
			name: "a watch list, which starts from a consistent snapshot, is kept",
			options: metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
				SendInitialEvents: ptr.To(true), AllowWatchBookmarks: true, TimeoutSeconds: timeout},
		},
		{
			name: "a watch list from the last resource version seen is kept",
			options: metav1.ListOptions{ResourceVersion: "123", ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
				SendInitialEvents: ptr.To(true), AllowWatchBookmarks: true, TimeoutSeconds: timeout},
			wantResourceVersion: "123",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := *tc.options.DeepCopy()
			egressPodListOptions(&got)

			want := *tc.options.DeepCopy()
			want.LabelSelector = consts.PodLabelServiceEgressGateway
			want.ResourceVersion = tc.wantResourceVersion
			assert.Equal(t, want, got)
		})
	}
}

// TestSetUpPodInformer_ListsConsistently pins that the first list of the pod informer, which the reflector makes with
// resourceVersion "0", reaches the API server as a consistent list of the pods with the egress label key
// (egressPodListOptions), so that the informer's store is never older than startup's listing (4.6).
func TestSetUpPodInformer_ListsConsistently(t *testing.T) {
	dt, kube, _ := newAdmittedTracker(t, nil, nil)
	kube.ClearActions()
	stop := make(chan struct{})
	defer close(stop)

	if !assert.NoError(t, dt.SetUpPodInformer(stop)) {
		return
	}

	var lists []metav1.ListOptions
	for _, action := range kube.Actions() {
		if list, ok := action.(k8stesting.ListActionImpl); ok && list.Matches("list", "pods") {
			lists = append(lists, list.ListOptions)
		}
	}
	if assert.NotEmpty(t, lists, "the pod informer lists the pods") {
		assert.Equal(t, consts.PodLabelServiceEgressGateway, lists[0].LabelSelector, "label selector of its first list")
		assert.Empty(t, lists[0].ResourceVersion, "resource version of its first list: a consistent read")
	}
}

// TestRemoveUnlistedEgressPods pins the removal of the members startup admitted that the informer's store does not
// hold, by itself: a dual-stack member leaves its identity with both its addresses, and the removal of its finalizer
// waits for both to drain; a member out of the selection whose address a new pod holds has nothing to drain: its
// finalizer-only record, of the pod as admitted, triggers the LocationsUpdater, which alone checks the pending
// deletions, and the check releases its cleanup finalizer. A member the store holds is left to its ADD.
func TestRemoveUnlistedEgressPods(t *testing.T) {
	p1IP := memberIP(memberPod("p1"))
	p2B := membershipPod("p2", memberIdentity, annotationsB())
	p2Address := memberIP(memberPod("p2")) + "@" + memberNode
	onlyP2 := func(podKey string) bool { return podKey == "ns/p2" }

	t.Run("a dual-stack member leaves with both its addresses", func(t *testing.T) {
		p1 := membershipPod("p1", memberIdentity, annotationsA(), p1IP, "fd00::1")
		dt, _, _ := newAdmittedTracker(t, []*v1.Pod{p1, p2B}, nil)

		dt.removeUnlistedEgressPods(onlyP2)

		assert.Equal(t, []string{p2Address}, egressAddressesOf(dt, memberIdentity), "addresses")
		dt.mu.Lock()
		defer dt.mu.Unlock()
		if record := dt.pendingPodDeletions["ns/p1"]; assert.NotNil(t, record, "finalizer-removal record of ns/p1") {
			assert.ElementsMatch(t, []string{p1IP, "fd00::1"}, record.Addresses, "it waits for both addresses to drain")
		}
		assert.Equal(t, []string{"ns/p2"}, slices.Collect(maps.Keys(dt.egress.admitted)), "ns/p2 is left to its ADD")
	})

	t.Run("a member with nothing to drain has its cleanup finalizer released by its finalizer-only record", func(t *testing.T) {
		p1A := membershipPod("p1", memberIdentity, annotationsA())
		// ns/p1 left the informer's selection, and ns/p9, a new pod, holds its address.
		unselected := podWith(p1A, func(p *v1.Pod) { delete(p.Labels, consts.PodLabelServiceEgressGateway) })
		p9 := membershipPod("p9", memberIdentity, annotationsB(), p1IP)
		dt, kube, _ := newAdmittedTracker(t, []*v1.Pod{p1A, p2B}, []*v1.Pod{unselected, p9, p2B})
		dt.reconcileEgressPodAdd(p9)
		drainTrigger(dt.locationsUpdaterTrigger)

		dt.removeUnlistedEgressPods(onlyP2)

		dt.mu.Lock()
		record := dt.pendingPodDeletions["ns/p1"]
		dt.mu.Unlock()
		if assert.NotNil(t, record, "finalizer-removal record of ns/p1") {
			want := PendingPodDeletion{Namespace: "ns", Name: "p1", UID: string(p1A.UID), ServiceUID: memberIdentity,
				Timestamp: record.Timestamp}
			assert.Equal(t, want, *record, "a ready record of ns/p1 as admitted, with nothing to drain")
		}
		assert.Len(t, dt.locationsUpdaterTrigger, 1, "the LocationsUpdater checks the record")

		dt.CheckPendingPodDeletions(context.Background())
		assert.False(t, apiHasFinalizer(t, kube, unselected), "the check releases the cleanup finalizer of ns/p1")
	})
}

// TestReconcileEgressPodAdd_UnchangedDualStackMemberKeepsItsAddresses pins that the first ADD of an unchanged dual-stack
// member startup admitted is no change: both its addresses stay registered throughout, also while the ADD reads the pod
// from the API server (a re-registration would drain them meanwhile), and no event is raised.
func TestReconcileEgressPodAdd_UnchangedDualStackMemberKeepsItsAddresses(t *testing.T) {
	p1 := membershipPod("p1", memberIdentity, annotationsA(), memberIP(memberPod("p1")), "fd00::1")
	p2B := membershipPod("p2", memberIdentity, annotationsB())
	dt, kube, recorder := newAdmittedTracker(t, []*v1.Pod{p1, p2B}, []*v1.Pod{p1, p2B})
	admitted := egressAddressesOf(dt, memberIdentity)
	if !assert.Len(t, admitted, 3, "control: both addresses of ns/p1 and the address of ns/p2 are admitted") {
		return
	}
	var duringRead []string
	kube.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() == "p1" {
			duringRead = egressAddressesOf(dt, memberIdentity)
		}
		return false, nil, nil
	})

	dt.reconcileEgressPodAdd(p1)

	assert.Equal(t, admitted, duringRead, "the addresses while the ADD reads the pod")
	assert.Equal(t, admitted, egressAddressesOf(dt, memberIdentity), "the addresses after the ADD")
	assert.Empty(t, recorder.take(), "no event")
}
