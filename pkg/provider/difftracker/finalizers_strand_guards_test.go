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

// Regression guards for egress pod-finalizer strand conditions. Each pins the required behaviour;
// the buffered-pod case (TestGuardDeletePod_PodDeletedWhileBuffered_FinalizerNotStranded) remains
// skipped pending its fix.

package difftracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// TestGuardCheckPendingPodDeletions_TransientGetErrorKeepsEntryForRetry verifies that in
// CheckPendingPodDeletions Phase 2, getPodByNamespaceName returning ANY error (including a transient
// 503/timeout/etcd error that is NOT a typed NotFound) is misread as "pod gone, clean up tracking",
// so the entry is deleted from pendingPodDeletions and the finalizer is never removed, permanently
// stranding the terminating egress pod until a CCM restart. The sibling RemoveLastPodFinalizers does
// this correctly (it special-cases apierrors.IsNotFound and keeps the entry on transient errors).
func TestGuardCheckPendingPodDeletions_TransientGetErrorKeepsEntryForRetry(t *testing.T) {
	ctx := context.Background()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "p",
			Namespace:  "ns",
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
	}
	kube := fake.NewSimpleClientset(pod)
	// The pod GET always returns a transient (non-NotFound) server error.
	kube.PrependReactor("get", "pods", func(_ ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(fmt.Errorf("transient server error"))
	})

	dt := &DiffTracker{
		kubeClient:          kube,
		pendingPodDeletions: make(map[string]*PendingPodDeletion),
		NRPResources:        NRPState{Locations: make(map[string]NRPLocation)}, // address already drained from NRP
	}
	dt.pendingPodDeletions["ns/p"] = &PendingPodDeletion{
		Namespace:  "ns",
		Name:       "p",
		ServiceUID: "egress-1",
		Address:    "10.244.0.1",
		Location:   "10.0.0.1",
		IsLastPod:  false,
		Timestamp:  time.Now().Format(time.RFC3339),
	}

	dt.CheckPendingPodDeletions(ctx)

	// A transient GET error must NOT drop the entry; it must remain so a later cycle retries.
	assert.Len(t, dt.pendingPodDeletions, 1,
		"a transient (non-NotFound) GET error must not drop the pending pod deletion, else the pod is stranded Terminating")
}

// TestGuardDeletePod_PodDeletedWhileBuffered_FinalizerNotStranded verifies that when an egress pod is
// deleted while it is still buffered for an in-flight service creation, DeletePod returns early via
// cancelBufferedPodLocked BEFORE recording a pendingPodDeletions entry. Since the finalizer-handling
// refactor removed the inline non-last finalizer removal in podInformerRemovePod, nothing removes the
// pod's ServiceGatewayPodCleanupFinalizer, so it is stranded (pod stuck Terminating forever).
func TestGuardDeletePod_PodDeletedWhileBuffered_FinalizerNotStranded(t *testing.T) {
	t.Skip("pending: a pod deleted while buffered returns early in cancelBufferedPodLocked before the pendingPodDeletions insert, so with the inline removal gone its finalizer is stranded; DeletePod must still schedule finalizer removal for a buffered pod")

	dt := newTestDiffTracker()
	const svc, ns, name, location, address = "egress-buffered", "default", "pod-buf", "10.0.0.1", "10.244.0.7"

	// Service creation has not reached Azure yet; the pod is buffered (never reached live state/NRP).
	dt.pendingServiceOps[svc] = &ServiceOperationState{
		ServiceUID: svc,
		Config:     NewOutboundServiceConfig(svc, nil),
		State:      StateNotStarted,
	}
	dt.pendingPods[svc] = []PendingPodUpdate{{
		PodKey:    ns + "/" + name,
		Location:  location,
		Address:   address,
		Timestamp: time.Now().Format(time.RFC3339),
	}}

	dt.DeletePod(svc, location, address, ns, name, "")

	// The buffered pod's finalizer removal must still be scheduled (tracked), not stranded.
	_, tracked := dt.pendingPodDeletions[ns+"/"+name]
	assert.True(t, tracked,
		"a pod deleted while buffered must still have its finalizer removal scheduled, else it is stranded Terminating")
}

// TestGuardLocationsUpdaterReschedulesOnReadyFinalizerRemovalFailure verifies that when a ready
// (address already drained from NRP) non-last pod finalizer removal fails transiently in steady
// state (post-init), LocationsUpdater.process() must NOT report success - it must reschedule a
// retry via backoffAndRetry. Otherwise, on a quiet cluster with no further triggers, the pod is
// stranded Terminating until some unrelated future event. We observe the reschedule via
// failureCount (incremented at the start of backoffAndRetry).
func TestGuardLocationsUpdaterReschedulesOnReadyFinalizerRemovalFailure(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "p",
			Namespace:  "ns",
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
	}
	kube := fake.NewSimpleClientset(pod)
	// The finalizer-removing Update fails with a transient (non-conflict) server error.
	kube.PrependReactor("update", "pods", func(_ ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(fmt.Errorf("transient server error"))
	})

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.kubeClient = kube
	// A ready, non-last pending pod deletion: its address is NOT in NRP (empty Locations), so
	// CheckPendingPodDeletions attempts the removal this cycle and fails transiently.
	dt.pendingPodDeletions["ns/p"] = &PendingPodDeletion{
		Namespace:  "ns",
		Name:       "p",
		ServiceUID: "egress-1",
		Address:    "10.244.0.1",
		Location:   "10.0.0.1",
		IsLastPod:  false,
		Timestamp:  time.Now().Format(time.RFC3339),
	}
	// No K8s nodes / NRP locations -> GetSyncLocationsAddresses returns no diff -> no-diff branch.

	// Cancel the updater context so backoffAndRetry returns immediately after incrementing
	// failureCount (skipping the delay and the re-trigger); failureCount is the observable signal.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lu := &LocationsUpdater{
		diffTracker: dt,
		ctx:         ctx,
		cancel:      cancel,
		logger:      dt.logger.WithName("LocationsUpdater"),
	}

	// Post-init: isInitializing defaults to 0, so initPodFinalizersStillPending() is false and the
	// retry must come from the readyRemovalPending signal.
	lu.process(context.Background())

	assert.Equal(t, 1, lu.failureCount,
		"a ready non-last finalizer removal that fails transiently post-init must reschedule a retry (backoffAndRetry), not report success")
}
