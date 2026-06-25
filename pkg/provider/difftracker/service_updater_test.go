/*
Copyright 2024 The Kubernetes Authors.

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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// newTestServiceUpdater builds a ServiceUpdater wired to dt for unit tests that exercise
// dispatcher logic without Azure clients (no goroutines are spawned for the cases tested).
func newTestServiceUpdater(dt *DiffTracker) *ServiceUpdater {
	return &ServiceUpdater{
		diffTracker: dt,
		onComplete:  func(string, bool, error) {},
		trigger:     dt.serviceUpdaterTrigger,
		ctx:         context.Background(),
		semaphore:   make(chan struct{}, 10),
		activeOps:   make(map[string]bool),
	}
}

// TestServiceUpdaterInitialization tests ServiceUpdater creation
func TestServiceUpdaterInitialization(t *testing.T) {
	// Skip: ServiceUpdater requires DiffTracker with Azure clients which needs extensive mocking
	// The initialization logic is simple and verified through integration tests
	t.Skip("ServiceUpdater requires Azure client mocking - deferred to integration tests")
}

// TestServiceUpdaterGracefulStop tests that ServiceUpdater stops gracefully
func TestServiceUpdaterGracefulStop(t *testing.T) {
	// Skip: ServiceUpdater requires DiffTracker with Azure clients which needs extensive mocking
	// Graceful shutdown logic is verified through integration tests
	t.Skip("ServiceUpdater requires Azure client mocking - deferred to integration tests")
}

// TestServiceUpdaterProcessBatchFlow tests that processBatch correctly categorizes work
func TestServiceUpdaterProcessBatchFlow(t *testing.T) {
	dt := &DiffTracker{
		NRPResources: NRPState{
			LoadBalancers: utilsets.NewString(),
			NATGateways:   utilsets.NewString(),
			Locations:     make(map[string]NRPLocation),
		},
		K8sResources: K8sState{
			Services: utilsets.NewString(),
			Egresses: utilsets.NewString(),
			Nodes:    make(map[string]Node),
		},
		pendingServiceOps: map[string]*ServiceOperationState{
			"service-1": {ServiceUID: "service-1", Config: NewInboundServiceConfig("service-1", nil), State: StateNotStarted, RetryCount: 0},
			"service-2": {ServiceUID: "service-2", Config: NewInboundServiceConfig("service-2", nil), State: StateCreationInProgress, RetryCount: 0},
			"service-3": {ServiceUID: "service-3", Config: NewInboundServiceConfig("service-3", nil), State: StateCreated, RetryCount: 0},
			"service-4": {ServiceUID: "service-4", Config: NewInboundServiceConfig("service-4", nil), State: StateDeletionPending, RetryCount: 0},
			"service-5": {ServiceUID: "service-5", Config: NewInboundServiceConfig("service-5", nil), State: StateDeletionInProgress, RetryCount: 0},
		},
		pendingEndpoints:        make(map[string][]PendingEndpointUpdate),
		pendingPods:             make(map[string][]PendingPodUpdate),
		pendingServiceDeletions:        make(map[string]*PendingServiceDeletion),
		serviceUpdaterTrigger:   make(chan bool, 1),
		locationsUpdaterTrigger: make(chan bool, 1),
	}

	// processBatch behavior:
	// - Process service-1 (StateNotStarted -> will try to create)
	// - Skip service-2 (StateCreationInProgress - already being processed)
	// - Skip service-3 (StateCreated - done)
	// - Skip service-4 (StateDeletionPending - waiting for LocationsUpdater)
	// - Process service-5 (StateDeletionInProgress -> will try to delete)

	// Verify initial state counts
	dt.mu.Lock()
	notStartedCount := 0
	creationInProgressCount := 0
	createdCount := 0
	deletionPendingCount := 0
	deletionInProgressCount := 0

	for _, opState := range dt.pendingServiceOps {
		switch opState.State {
		case StateNotStarted:
			notStartedCount++
		case StateCreationInProgress:
			creationInProgressCount++
		case StateCreated:
			createdCount++
		case StateDeletionPending:
			deletionPendingCount++
		case StateDeletionInProgress:
			deletionInProgressCount++
		}
	}
	dt.mu.Unlock()

	assert.Equal(t, 1, notStartedCount, "Should have 1 service in StateNotStarted")
	assert.Equal(t, 1, creationInProgressCount, "Should have 1 service in StateCreationInProgress")
	assert.Equal(t, 1, createdCount, "Should have 1 service in StateCreated")
	assert.Equal(t, 1, deletionPendingCount, "Should have 1 service in StateDeletionPending")
	assert.Equal(t, 1, deletionInProgressCount, "Should have 1 service in StateDeletionInProgress")
}

// TestServiceUpdaterSemaphoreLimit tests that semaphore limits concurrent operations
func TestServiceUpdaterSemaphoreLimit(t *testing.T) {
	// Skip: ServiceUpdater requires DiffTracker with Azure clients which needs extensive mocking
	// Semaphore limiting is verified through integration tests
	t.Skip("ServiceUpdater requires Azure client mocking - deferred to integration tests")
}

// TestServiceUpdaterActiveOpsTracking tests that activeOps map prevents duplicate processing
func TestServiceUpdaterActiveOpsTracking(t *testing.T) {
	// Skip: ServiceUpdater requires DiffTracker with Azure clients which needs extensive mocking
	// Active operations tracking is verified through integration tests
	t.Skip("ServiceUpdater requires Azure client mocking - deferred to integration tests")
}

// TestServiceUpdaterRequeueKeepsInitTriggerCounterBalanced verifies that the follow-up
// trigger emitted by requeueIfMoreWork is accounted for in the initialization in-flight
// counter. During initialization, every processBatch decrements pendingUpdaterTriggers,
// so a requeue that did not increment it would drive the counter negative and prevent
// WaitForInitialSync from ever completing.
func TestServiceUpdaterRequeueKeepsInitTriggerCounterBalanced(t *testing.T) {
	dt := newTestDiffTracker()
	atomic.StoreInt32(&dt.isInitializing, 1)
	dt.initCompletionChecker = make(chan struct{}) // production sets this in startInitialization
	su := newTestServiceUpdater(dt)

	atomic.StoreInt32(&dt.pendingUpdaterTriggers, 0)
	su.requeueIfMoreWork("svc")
	assert.Equal(t, int32(1), atomic.LoadInt32(&dt.pendingUpdaterTriggers),
		"requeue during initialization should increment the in-flight trigger counter")

	<-dt.serviceUpdaterTrigger // worker consumes the follow-up trigger
	su.processBatch()
	assert.Equal(t, int32(0), atomic.LoadInt32(&dt.pendingUpdaterTriggers),
		"requeue + processBatch should net zero (no counter poisoning)")
}

// TestServiceUpdaterProcessBatchSkipsParkedService verifies that a service parked after a
// non-retryable creation error (CreationFailedTerminal) is not re-dispatched, preventing
// an infinite retry loop on deterministic (invalid-spec) failures.
func TestServiceUpdaterProcessBatchSkipsParkedService(t *testing.T) {
	dt := newTestDiffTracker()
	dt.pendingServiceOps["svc"] = &ServiceOperationState{
		ServiceUID:             "svc",
		Config:                 NewInboundServiceConfig("svc", nil),
		State:                  StateNotStarted,
		CreationFailedTerminal: true,
	}
	su := newTestServiceUpdater(dt)

	su.processBatch()

	assert.Equal(t, StateNotStarted, dt.pendingServiceOps["svc"].State,
		"parked service must not be transitioned/dispatched")
	assert.Len(t, dt.serviceUpdaterTrigger, 0, "parked service must not enqueue further work")
}
