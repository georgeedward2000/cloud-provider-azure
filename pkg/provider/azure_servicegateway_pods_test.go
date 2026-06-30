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
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
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
	address    string
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

func (m *mockDiffTracker) DeletePod(serviceUID, location, address, namespace, name, uid string) difftracker.DeletePodResult {
	m.deletePodCalls = append(m.deletePodCalls, deletePodCall{
		serviceUID: serviceUID,
		location:   location,
		address:    address,
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
				if tt.expectedPodIP != "" && call.address != tt.expectedPodIP {
					t.Errorf("Expected address (PodIP) %s, got %s", tt.expectedPodIP, call.address)
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

// TestPodInformerUpdateFunc tests the UpdateFunc logic in the informer
func TestPodInformerUpdateFunc(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name                string
		oldPod              *v1.Pod
		newPod              *v1.Pod
		expectedAddCalls    int
		expectedDeleteCalls int
		description         string
	}{
		{
			name:                "Label change from A to B with IPs",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectedAddCalls:    1,
			expectedDeleteCalls: 1,
			description:         "Should remove from A and add to B",
		},
		{
			name:                "Pod gets IPs AND label changes (race condition test)",
			oldPod:              newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod:              newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectedAddCalls:    1,
			expectedDeleteCalls: 0,
			description:         "Should only add to B (no remove since pod never had IPs in A)",
		},
		{
			name:                "Pod just gets IPs (no label change)",
			oldPod:              newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectedAddCalls:    1,
			expectedDeleteCalls: 0,
			description:         "Should add to A",
		},
		{
			name:                "IP change (pod moved to different node)",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.2", "10.0.1.2", v1.PodRunning, nil),
			expectedAddCalls:    1,
			expectedDeleteCalls: 1,
			description:         "Should remove old IPs and add new IPs",
		},
		{
			name:                "Pod loses IPs (edge case)",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "", "", v1.PodRunning, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should remove from A",
		},
		{
			name:                "Label removed (pod no longer egress)",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should remove from A",
		},
		{
			name:                "Pod transitions to Failed phase",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should remove from A when pod fails",
		},
		{
			name:                "Pod transitions to Succeeded phase",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodSucceeded, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should remove from A when pod succeeds",
		},
		{
			name:                "Pod gets DeletionTimestamp",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, &now),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should remove from A when pod starts terminating",
		},
		{
			name:                "Pod gets DeletionTimestamp AND label changes",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-b", "10.0.0.1", "10.0.1.1", v1.PodRunning, &now),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should remove from A but not add to B (terminating pod rejected by AddPod)",
		},
		{
			name:                "No relevant changes (same state)",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 0,
			description:         "Should do nothing",
		},
		{
			name:                "Pod in Pending without IPs stays in Pending without IPs",
			oldPod:              newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			newPod:              newTestPod("default", "test", "egress-a", "", "", v1.PodPending, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 0,
			description:         "Should do nothing (waiting for IPs)",
		},
		{
			name:                "IP change on terminating pod",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.2", "10.0.1.2", v1.PodRunning, &now),
			expectedAddCalls:    0,
			expectedDeleteCalls: 1,
			description:         "Should only remove (terminating pod not re-added)",
		},
		{
			name:                "Pod transitions from Failed to Running (recovery scenario)",
			oldPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodFailed, nil),
			newPod:              newTestPod("default", "test", "egress-a", "10.0.0.1", "10.0.1.1", v1.PodRunning, nil),
			expectedAddCalls:    0,
			expectedDeleteCalls: 0,
			description:         "Should do nothing (pod wasn't previously valid, so wasn't tracked). Note: This is an edge case where pod state transitions from invalid to valid - in practice, we rely on initial AddFunc when pod first becomes valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockDiffTracker{}
			az := &testCloudWithMockDiffTracker{
				mock: mock,
			}

			// Simulate UpdateFunc logic
			// Extract egress gateway names from labels
			var prevEgressGatewayName, currEgressGatewayName string
			if tt.oldPod.Labels != nil {
				prevEgressGatewayName = tt.oldPod.Labels[consts.PodLabelServiceEgressGateway]
			}
			if tt.newPod.Labels != nil {
				currEgressGatewayName = tt.newPod.Labels[consts.PodLabelServiceEgressGateway]
			}

			// Detect relevant changes
			labelChanged := prevEgressGatewayName != currEgressGatewayName
			oldHadIPs := tt.oldPod.Status.HostIP != "" && tt.oldPod.Status.PodIP != ""
			newHasIPs := tt.newPod.Status.HostIP != "" && tt.newPod.Status.PodIP != ""
			ipsChanged := tt.oldPod.Status.HostIP != tt.newPod.Status.HostIP || tt.oldPod.Status.PodIP != tt.newPod.Status.PodIP

			oldWasValid := tt.oldPod.DeletionTimestamp == nil &&
				(tt.oldPod.Status.Phase == v1.PodRunning || tt.oldPod.Status.Phase == v1.PodPending)
			newIsValid := tt.newPod.DeletionTimestamp == nil &&
				(tt.newPod.Status.Phase == v1.PodRunning || tt.newPod.Status.Phase == v1.PodPending)

			var needsRemove, needsAdd bool

			if labelChanged {
				needsRemove = prevEgressGatewayName != "" && oldHadIPs
				needsAdd = currEgressGatewayName != "" && newHasIPs && newIsValid
			} else if currEgressGatewayName != "" {
				if oldWasValid && !newIsValid && oldHadIPs {
					needsRemove = true
				} else if !oldHadIPs && newHasIPs && newIsValid {
					needsAdd = true
				} else if oldHadIPs && !newHasIPs {
					needsRemove = true
				} else if oldHadIPs && newHasIPs && ipsChanged && newIsValid {
					needsRemove = true
					needsAdd = true
				} else if oldHadIPs && newHasIPs && !newIsValid {
					needsRemove = true
				}
			}

			if needsRemove {
				az.podInformerRemovePod(tt.oldPod)
			}
			if needsAdd {
				az.podInformerAddPod(tt.newPod)
			}

			if len(mock.addPodCalls) != tt.expectedAddCalls {
				t.Errorf("%s: Expected %d AddPod calls, got %d", tt.description, tt.expectedAddCalls, len(mock.addPodCalls))
			}

			if len(mock.deletePodCalls) != tt.expectedDeleteCalls {
				t.Errorf("%s: Expected %d DeletePod calls, got %d", tt.description, tt.expectedDeleteCalls, len(mock.deletePodCalls))
			}
		})
	}
}

// TestPodInformerDeleteFunc tests the DeleteFunc logic including tombstone handling
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
	if pod.Status.HostIP == "" || pod.Status.PodIP == "" {
		return
	}

	egressName := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])
	if !difftracker.IsValidEgressIdentity(egressName) {
		return
	}
	podKey := pod.Namespace + "/" + pod.Name

	// Call mock instead of real diffTracker
	tc.mock.AddPod(egressName, podKey, pod.Status.HostIP, pod.Status.PodIP)
}

// podInformerRemovePod mimics the real implementation but uses our mock
func (tc *testCloudWithMockDiffTracker) podInformerRemovePod(pod *v1.Pod) {
	// Validate pod has egress label
	if pod.Labels == nil || pod.Labels[consts.PodLabelServiceEgressGateway] == "" {
		return
	}

	// Need IPs to identify which location/address to remove
	if pod.Status.HostIP == "" || pod.Status.PodIP == "" {
		return
	}

	egressName := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])

	// Call mock instead of real diffTracker
	tc.mock.DeletePod(egressName, pod.Status.HostIP, pod.Status.PodIP, pod.Namespace, pod.Name, string(pod.UID))
}
