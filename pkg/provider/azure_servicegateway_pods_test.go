package provider

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// mockDiffTracker tracks calls to AddPod and DeletePod for testing
type mockDiffTracker struct {
	addPodCalls    []addPodCall
	deletePodCalls []deletePodCall
}

type addPodCall struct {
	serviceUID string
	podKey     string
	location   string
	address    string
}

type deletePodCall struct {
	serviceUID string
	location   string
	addresses  []string
	namespace  string
	name       string
	uid        string
}

func (m *mockDiffTracker) AddPod(serviceUID, podKey, location, address string) {
	m.addPodCalls = append(m.addPodCalls, addPodCall{
		serviceUID: serviceUID,
		podKey:     podKey,
		location:   location,
		address:    address,
	})
}

func (m *mockDiffTracker) DeletePod(serviceUID, location string, addresses []string, namespace, name, uid string) difftracker.DeletePodResult {
	m.deletePodCalls = append(m.deletePodCalls, deletePodCall{
		serviceUID: serviceUID,
		location:   location,
		addresses:  addresses,
		namespace:  namespace,
		name:       name,
		uid:        uid,
	})
	// Mock always returns non-last pod (tests can adjust if needed)
	return difftracker.DeletePodResult{IsLastPod: false}
}

func (m *mockDiffTracker) reset() {
	m.addPodCalls = nil
	m.deletePodCalls = nil
}

// Helper to create a pod with specific attributes
func newTestPod(namespace, name, egressLabel, hostIP, podIP string, phase v1.PodPhase, deletionTimestamp *metav1.Time) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Status: v1.PodStatus{
			HostIP: hostIP,
			PodIP:  podIP,
			Phase:  phase,
		},
	}

	if egressLabel != "" {
		pod.Labels = map[string]string{
			consts.PodLabelServiceEgressGateway: egressLabel,
		}
	}

	if deletionTimestamp != nil {
		pod.DeletionTimestamp = deletionTimestamp
	}

	return pod
}

// withPodIPs sets a pod's Status.PodIPs (and keeps Status.PodIP aligned with PodIPs[0], per the
// Kubernetes API), so tests can exercise dual-stack egress pods.
func withPodIPs(pod *v1.Pod, ips ...string) *v1.Pod {
	pod.Status.PodIPs = nil
	for _, ip := range ips {
		pod.Status.PodIPs = append(pod.Status.PodIPs, v1.PodIP{IP: ip})
	}
	if len(ips) > 0 {
		pod.Status.PodIP = ips[0]
	}
	return pod
}

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
			mock := &mockDiffTracker{}
			// Create a wrapper struct that embeds Cloud but uses our mock for testing
			az := &testCloudWithMockDiffTracker{
				mock: mock,
			}

			az.podInformerAddPod(tt.pod)

			if len(mock.addPodCalls) != tt.expectedCalls {
				t.Errorf("Expected %d AddPod calls, got %d", tt.expectedCalls, len(mock.addPodCalls))
			}

			if tt.expectAddPod && len(mock.addPodCalls) > 0 {
				call := mock.addPodCalls[0]
				if tt.expectedPodKey != "" && call.podKey != tt.expectedPodKey {
					t.Errorf("Expected podKey %s, got %s", tt.expectedPodKey, call.podKey)
				}
				if tt.expectedEgress != "" && call.serviceUID != tt.expectedEgress {
					t.Errorf("Expected serviceUID %s, got %s", tt.expectedEgress, call.serviceUID)
				}
				if tt.expectedHostIP != "" && call.location != tt.expectedHostIP {
					t.Errorf("Expected location (HostIP) %s, got %s", tt.expectedHostIP, call.location)
				}
				if tt.expectedPodIP != "" && call.address != tt.expectedPodIP {
					t.Errorf("Expected address (PodIP) %s, got %s", tt.expectedPodIP, call.address)
				}
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
	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = seededProviderDiffTracker(t, az, kube,
		difftracker.K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(egress), Nodes: map[string]difftracker.Node{}},
		difftracker.NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(egress), Locations: map[string]difftracker.NRPLocation{}})

	az.podInformerAddPod(pod)
	v4Pods := az.diffTracker.K8sResources.Nodes[v4Node].Pods
	v6Pods := az.diffTracker.K8sResources.Nodes[v6Node].Pods
	assert.Contains(t, v4Pods, v4Pod, "the IPv4 PodIP must register under the IPv4 node location")
	assert.Contains(t, v6Pods, v6Pod, "the IPv6 PodIP must register under the IPv6 node location")
	assert.NotContains(t, v4Pods, v6Pod, "the IPv6 PodIP must NOT be filed under the IPv4 node location (NRP rejects mixed-family locations)")

	az.podInformerRemovePod(pod)
	assert.NotContains(t, az.diffTracker.K8sResources.Nodes[v4Node].Pods, v4Pod, "the IPv4 family must drain on remove")
	assert.NotContains(t, az.diffTracker.K8sResources.Nodes[v6Node].Pods, v6Pod, "the IPv6 family must drain on remove")
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

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = newProviderDiffTracker(t, az, kube)
	rec := record.NewFakeRecorder(10)
	az.eventRecorder = rec

	az.podInformerAddPod(pod)

	// The pod must still be registered with the engine despite the finalizer add failure.
	assert.True(t, az.diffTracker.IsServiceTracked("egress-svc"),
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

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = newProviderDiffTracker(t, az, kube)
	rec := record.NewFakeRecorder(10)
	az.eventRecorder = rec

	az.podInformerAddPod(pod)

	assert.False(t, az.diffTracker.IsServiceTracked("../hijacked-nat"),
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

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = newProviderDiffTracker(t, az, kube)

	az.podInformerAddPod(stalePod)

	assert.False(t, az.diffTracker.IsServiceTracked(egress),
		"a stale event for a UID-replaced pod must not register the stale address")
	if node, ok := az.diffTracker.K8sResources.Nodes["10.0.0.1"]; ok {
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

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = newProviderDiffTracker(t, az, kube)
	rec := record.NewFakeRecorder(10)
	az.eventRecorder = rec

	az.podInformerAddPod(pod)

	assert.False(t, az.diffTracker.IsServiceTracked("egress-svc"),
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
		expectedCalls   int
		expectedEgress  string
		expectedHostIP  string
		expectedPodIP   string
	}{
		{
			name:            "Valid pod with egress label and IPs should trigger DeletePod",
			pod:             newTestPod("default", "test-pod", "egress-gateway-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: true,
			expectedCalls:   1,
			expectedEgress:  "egress-gateway-a",
			expectedHostIP:  "10.0.0.1",
			expectedPodIP:   "10.0.1.1",
		},
		{
			name:            "Pod without egress label should be skipped",
			pod:             newTestPod("default", "no-label", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: false,
			expectedCalls:   0,
		},
		{
			name:            "Pod without HostIP should be skipped with warning",
			pod:             newTestPod("default", "no-hostip", "egress-a", "", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: false,
			expectedCalls:   0,
		},
		{
			name:            "Pod without PodIP should be skipped with warning",
			pod:             newTestPod("default", "no-podip", "egress-a", "10.0.0.1", "", v1.PodRunning, nil),
			expectDeletePod: false,
			expectedCalls:   0,
		},
		{
			name:            "Pod in any phase with IPs should trigger DeletePod (phase doesn't matter for removal)",
			pod:             newTestPod("default", "failed-pod", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			expectDeletePod: true,
			expectedCalls:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockDiffTracker{}
			az := &testCloudWithMockDiffTracker{
				mock: mock,
			}

			az.podInformerRemovePod(tt.pod)

			if len(mock.deletePodCalls) != tt.expectedCalls {
				t.Errorf("Expected %d DeletePod calls, got %d", tt.expectedCalls, len(mock.deletePodCalls))
			}

			if tt.expectDeletePod && len(mock.deletePodCalls) > 0 {
				call := mock.deletePodCalls[0]
				if tt.expectedEgress != "" && call.serviceUID != tt.expectedEgress {
					t.Errorf("Expected serviceUID %s, got %s", tt.expectedEgress, call.serviceUID)
				}
				if tt.expectedHostIP != "" && call.location != tt.expectedHostIP {
					t.Errorf("Expected location (HostIP) %s, got %s", tt.expectedHostIP, call.location)
				}
				if tt.expectedPodIP != "" && (len(call.addresses) == 0 || call.addresses[0] != tt.expectedPodIP) {
					t.Errorf("Expected address (PodIP) %s, got %v", tt.expectedPodIP, call.addresses)
				}
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
			Finalizers: []string{difftracker.ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{HostIP: "10.0.0.1", PodIP: "10.244.0.1"},
	}
	kubeClient := fake.NewSimpleClientset(pod)

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient) // empty engine state -> pod is untracked

	az.podInformerRemovePod(pod)

	got, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "egress-stale", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, difftracker.ServiceGatewayPodCleanupFinalizer,
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
			Finalizers: []string{difftracker.ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{}, // no HostIP/PodIP
	}
	kubeClient := fake.NewSimpleClientset(pod)

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	az.podInformerRemovePod(pod)

	got, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "egress-noip", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, difftracker.ServiceGatewayPodCleanupFinalizer,
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
	seedState := func() difftracker.K8sState {
		return difftracker.K8sState{
			Services: utilsets.NewString(),
			Egresses: utilsets.NewString(egress),
			Nodes: map[string]difftracker.Node{
				hostIP: {Pods: map[string]difftracker.Pod{
					addrA: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
					addrB: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
				}},
			},
		}
	}
	// addrA is not registered in NRP (already drained), so a non-last pending record would strip at once.
	drainedNRP := func() difftracker.NRPState {
		return difftracker.NRPState{
			LoadBalancers: utilsets.NewString(),
			NATGateways:   utilsets.NewString(egress),
			Locations:     make(map[string]difftracker.NRPLocation),
		}
	}
	podA := func(ip string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "pod-a",
				Namespace:  "default",
				UID:        types.UID("uid-a"),
				Labels:     map[string]string{consts.PodLabelServiceEgressGateway: egress},
				Finalizers: []string{difftracker.ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{HostIP: hostIP, PodIP: ip, PodIPs: []v1.PodIP{{IP: ip}}, Phase: v1.PodRunning},
		}
	}

	t.Run("drainForReplace leaves the live pod's finalizer intact", func(t *testing.T) {
		kube := fake.NewSimpleClientset(podA(addrA))
		az := GetTestCloudWithContainerLoadBalancer(ctrl)
		az.KubeClient = kube
		az.diffTracker = seededProviderDiffTracker(t, az, kube, seedState(), drainedNRP())

		// A same-service address change (addrA -> addrC) drains addrA with empty namespace/name, so
		// no finalizer-deletion record is enqueued for the still-live pod.
		az.podInformerDrainForReplace(podA(addrA), podA("10.244.0.3"))
		az.diffTracker.CheckPendingPodDeletions(context.Background())

		got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, got.Finalizers, difftracker.ServiceGatewayPodCleanupFinalizer,
			"a live re-registration drain must not enqueue a strippable record, so the finalizer must survive")
	})

	t.Run("contrast: the record-enqueuing removal path would strip it", func(t *testing.T) {
		kube := fake.NewSimpleClientset(podA(addrA))
		az := GetTestCloudWithContainerLoadBalancer(ctrl)
		az.KubeClient = kube
		az.diffTracker = seededProviderDiffTracker(t, az, kube, seedState(), drainedNRP())

		az.podInformerRemovePod(podA(addrA)) // enqueues a non-last drain-gated record
		az.diffTracker.CheckPendingPodDeletions(context.Background())

		got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, got.Finalizers, difftracker.ServiceGatewayPodCleanupFinalizer,
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
	k8s := difftracker.K8sState{
		Services: utilsets.NewString(),
		Egresses: utilsets.NewString(egress),
		Nodes: map[string]difftracker.Node{
			hostIP: {Pods: map[string]difftracker.Pod{
				v4: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
			}},
		},
	}
	nrp := difftracker.NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(egress),
		Locations:     make(map[string]difftracker.NRPLocation),
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
				Finalizers: []string{difftracker.ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{HostIP: hostIP, PodIP: ips[0], PodIPs: podIPs, Phase: v1.PodRunning},
		}
	}

	kube := fake.NewSimpleClientset(pod(v4, v6))
	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = seededProviderDiffTracker(t, az, kube, k8s, nrp)

	az.podInformerDrainForReplace(pod(v4), pod(v4, v6))

	// v4 must still be the sole live address: draining it now is the last-pod case. Had the full old
	// set been drained, v4 would already be gone and this would be a no-op (IsLastPod=false).
	res := az.diffTracker.DeletePod(egress, hostIP, []string{v4}, "default", "pod-a", "uid-a")
	assert.True(t, res.IsLastPod,
		"a dual-stack gain must keep the shared v4 registered, so the sole pod's service is never emptied/torn down mid-replace")
	assert.True(t, res.Enqueued)
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
	k8s := difftracker.K8sState{
		Services: utilsets.NewString(),
		Egresses: utilsets.NewString(egress),
		Nodes: map[string]difftracker.Node{
			hostIP: {Pods: map[string]difftracker.Pod{
				v4:    {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
				other: {InboundIdentities: utilsets.NewString(), PublicOutboundIdentity: egress},
			}},
		},
	}
	nrp := difftracker.NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(egress),
		Locations:     make(map[string]difftracker.NRPLocation),
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
				Finalizers: []string{difftracker.ServiceGatewayPodCleanupFinalizer},
			},
			Status: v1.PodStatus{HostIP: hostIP, HostIPs: []v1.HostIP{{IP: hostIP}, {IP: v6Node}}, PodIP: ips[0], PodIPs: podIPs, Phase: v1.PodRunning},
		}
	}

	oldPod, newPod := pod(v4), pod(v4, v6)
	kube := fake.NewSimpleClientset(newPod)
	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kube
	az.diffTracker = seededProviderDiffTracker(t, az, kube, k8s, nrp)

	az.reconcileEgressPodUpdate(oldPod, newPod)
	az.diffTracker.CheckPendingPodDeletions(context.Background())

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "pod-a", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.Finalizers, difftracker.ServiceGatewayPodCleanupFinalizer,
		"a live re-registration must leave the pod finalized")
	assert.True(t, az.diffTracker.IsServiceTracked(egress), "the egress service must remain tracked")

	// Both IP families must be registered under their SAME-FAMILY node location: the dual-stack gain
	// added v6 under the node's IPv6 IP and kept the shared v4 under the IPv4 node IP.
	v4Pods := az.diffTracker.K8sResources.Nodes[hostIP].Pods
	v6Pods := az.diffTracker.K8sResources.Nodes[v6Node].Pods
	if assert.Contains(t, v4Pods, v4, "the primary family must remain registered under the IPv4 node location") {
		assert.Equal(t, egress, v4Pods[v4].PublicOutboundIdentity)
	}
	if assert.Contains(t, v6Pods, v6, "the gained secondary family must be registered under the IPv6 node location") {
		assert.Equal(t, egress, v6Pods[v6].PublicOutboundIdentity)
	}
	assert.NotContains(t, v4Pods, v6, "the IPv6 address must not be filed under the IPv4 node location")
}

// TestEgressPodUpdateActions verifies the pure UPDATE decision function directly: for each pod
// transition it must report whether the pod has to be removed from its old gateway and/or (re-)added
// to its current one. Driving egressPodUpdateActions itself (rather than a mock informer) keeps this
// a genuine guard - the routing that consumes these decisions is covered by the real-Cloud tests
// (TestPodInformerDrainForReplace_*, TestReconcileEgressPodUpdate_*).
func TestEgressPodUpdateActions(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name       string
		oldPod     *v1.Pod
		newPod     *v1.Pod
		wantRemove bool
		wantAdd    bool
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
			name:   "Pod transitions from Failed to Running (was not previously tracked)",
			oldPod: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			newPod: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			needsRemove, needsAdd, _ := egressPodUpdateActions(tt.oldPod, tt.newPod)
			assert.Equal(t, tt.wantRemove, needsRemove, "needsRemove")
			assert.Equal(t, tt.wantAdd, needsAdd, "needsAdd")
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
		needsRemove, needsAdd, _ := egressPodUpdateActions(dsPod(v6NodeOld), dsPod(v6NodeNew))
		assert.True(t, needsRemove)
		assert.True(t, needsAdd)
	})
	t.Run("secondary-family node IP appearing re-registers", func(t *testing.T) {
		needsRemove, needsAdd, _ := egressPodUpdateActions(dsPod(""), dsPod(v6NodeNew))
		assert.True(t, needsRemove)
		assert.True(t, needsAdd)
	})
}
func TestPodInformerDeleteFunc(t *testing.T) {
	tests := []struct {
		name            string
		obj             interface{}
		expectDeletePod bool
		expectedCalls   int
		shouldError     bool
	}{
		{
			name:            "Direct pod object",
			obj:             newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectDeletePod: true,
			expectedCalls:   1,
		},
		{
			name: "Tombstone with valid pod",
			obj: cache.DeletedFinalStateUnknown{
				Key: "default/test",
				Obj: newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			},
			expectDeletePod: true,
			expectedCalls:   1,
		},
		{
			name: "Tombstone with invalid object type",
			obj: cache.DeletedFinalStateUnknown{
				Key: "default/test",
				Obj: "not-a-pod",
			},
			expectDeletePod: false,
			expectedCalls:   0,
			shouldError:     true,
		},
		{
			name:            "Invalid object type (not pod or tombstone)",
			obj:             "invalid-type",
			expectDeletePod: false,
			expectedCalls:   0,
			shouldError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockDiffTracker{}
			az := &testCloudWithMockDiffTracker{
				mock: mock,
			}

			// Simulate DeleteFunc logic
			var pod *v1.Pod
			switch v := tt.obj.(type) {
			case *v1.Pod:
				pod = v
			case cache.DeletedFinalStateUnknown:
				var ok bool
				pod, ok = v.Obj.(*v1.Pod)
				if !ok {
					// This would log an error in real code
					if !tt.shouldError {
						t.Errorf("Expected valid pod in tombstone but conversion failed")
					}
					return
				}
			default:
				// This would log an error in real code
				if !tt.shouldError {
					t.Errorf("Expected valid pod object but got %T", v)
				}
				return
			}

			if pod != nil {
				az.podInformerRemovePod(pod)
			}

			if len(mock.deletePodCalls) != tt.expectedCalls {
				t.Errorf("Expected %d DeletePod calls, got %d", tt.expectedCalls, len(mock.deletePodCalls))
			}
		})
	}
}

// testCloudWithMockDiffTracker wraps Cloud methods but uses mock diffTracker
type testCloudWithMockDiffTracker struct {
	mock *mockDiffTracker
}

// podInformerAddPod mimics the real implementation but uses our mock
func (tc *testCloudWithMockDiffTracker) podInformerAddPod(pod *v1.Pod) {
	// Validate pod has egress label
	if pod.Labels == nil || pod.Labels[consts.PodLabelServiceEgressGateway] == "" {
		return
	}

	// Skip pods that are being deleted
	if pod.DeletionTimestamp != nil {
		return
	}

	// Only process pods in Running or Pending phase
	if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending {
		return
	}

	// Validate pod has required IPs
	if pod.Status.HostIP == "" || len(difftracker.PodEgressAddresses(pod)) == 0 {
		return
	}

	egressName := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])
	if !difftracker.IsValidEgressIdentity(egressName) {
		return
	}
	podKey := pod.Namespace + "/" + pod.Name

	// Call mock instead of real diffTracker, once per pod IP (mirrors the real dual-stack loop)
	for _, podIP := range difftracker.PodEgressAddresses(pod) {
		tc.mock.AddPod(egressName, podKey, pod.Status.HostIP, podIP)
	}
}

// podInformerRemovePod mimics the real implementation but uses our mock
func (tc *testCloudWithMockDiffTracker) podInformerRemovePod(pod *v1.Pod) {
	// Validate pod has egress label
	if pod.Labels == nil || pod.Labels[consts.PodLabelServiceEgressGateway] == "" {
		return
	}

	// Need IPs to identify which location/address to remove
	if pod.Status.HostIP == "" || len(difftracker.PodEgressAddresses(pod)) == 0 {
		return
	}

	egressName := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])

	// Call mock instead of real diffTracker
	tc.mock.DeletePod(egressName, pod.Status.HostIP, difftracker.PodEgressAddresses(pod), pod.Namespace, pod.Name, string(pod.UID))
}
