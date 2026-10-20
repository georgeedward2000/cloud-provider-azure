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
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	servicehelper "k8s.io/cloud-provider/service/helpers"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/loadbalancerclient/mock_loadbalancerclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/natgatewayclient/mock_natgatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/publicipaddressclient/mock_publicipaddressclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/publicipprefixclient/mock_publicipprefixclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/servicegatewayclient/mock_servicegatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
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

// TestGuardServiceUpdater_BackoffAndTerminalCeiling verifies that a transient (non-terminal) create
// failure must schedule a backoff (NextRetryAt in the future, advancing per attempt); the dispatcher
// must skip the op while it is in backoff (no immediate re-dispatch hot-loop); and after
// maxServiceRetries the op is parked (RetriesExhausted) and no longer dispatched.
func TestGuardServiceUpdater_BackoffAndTerminalCeiling(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "svc-backoff"
	transientErr := errors.New("transient ARM 429")

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:      StateCreationInProgress,
	}

	// failOnce puts the op back in-flight (as the dispatcher would) and signals a transient
	// failure, returning the scheduled backoff delay.
	failOnce := func() time.Duration {
		op := dt.pendingServiceOps[uid]
		op.State = StateCreationInProgress
		snap := op.Config
		op.InFlightConfig = &snap
		before := time.Now()
		dt.OnServiceCreationComplete(uid, false, transientErr)
		return dt.pendingServiceOps[uid].NextRetryAt.Sub(before)
	}

	// First transient failure: RetryCount advances, NextRetryAt is set in the future.
	d1 := failOnce()
	assert.Equal(t, 1, dt.pendingServiceOps[uid].RetryCount)
	assert.Greater(t, d1, time.Duration(0), "a transient failure must schedule a future retry")
	assert.Equal(t, StateNotStarted, dt.pendingServiceOps[uid].State)

	// The dispatcher must SKIP the op while it is in backoff (now < NextRetryAt): not dispatched
	// and activeOps released - i.e. no immediate re-dispatch hot-loop.
	su := newTestServiceUpdater(dt)
	su.processBatch()
	assert.Equal(t, StateNotStarted, dt.pendingServiceOps[uid].State, "op in backoff must not be dispatched")
	su.mu.Lock()
	_, active := su.activeOps[uid]
	su.mu.Unlock()
	assert.False(t, active, "activeOps must be released for a backoff-skipped op")

	// Second failure: the backoff grows per attempt.
	d2 := failOnce()
	assert.Equal(t, 2, dt.pendingServiceOps[uid].RetryCount)
	assert.Greater(t, d2, d1, "backoff must grow per attempt")

	// Terminal ceiling: at maxServiceRetries the op is parked and no longer dispatched.
	op := dt.pendingServiceOps[uid]
	op.RetryCount = maxServiceRetries
	op.NextRetryAt = time.Time{} // exercise the ceiling, not the backoff window
	su.processBatch()
	assert.True(t, dt.pendingServiceOps[uid].RetriesExhausted, "op must park after exhausting the retry budget")
	assert.Equal(t, StateNotStarted, dt.pendingServiceOps[uid].State, "parked op must not be dispatched")
}

// TestCreateInboundService_TransientServiceLookupErrorDoesNotCreatePIP verifies that a transient
// (non-NotFound) error when looking up the Service in Step 0 aborts the create and reports a
// retryable failure, rather than proceeding to create the PIP/LB/SGW with no K8s cleanup-finalizer
// anchor. A genuine NotFound is handled separately (the service is gone).
func TestCreateInboundService_TransientServiceLookupErrorDoesNotCreatePIP(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// The Service List fails transiently, so getServiceByUID returns a generic wrapped error
	// (not a typed NotFound).
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("list", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("transient apiserver error")
	})

	// The PIP must never be created on this path; Times(0) fails the test if it is.
	pip := mock_publicipaddressclient.NewMockInterface(ctrl)
	pip.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	factory := mock_azclient.NewMockClientFactory(ctrl)
	factory.EXPECT().GetPublicIPAddressClient().Return(pip).AnyTimes()

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.kubeClient = kube
	dt.networkClientFactory = factory

	var gotSuccess *bool
	var gotErr error
	su := newTestServiceUpdater(dt)
	su.onComplete = func(_ string, ok bool, err error) {
		v := ok
		gotSuccess = &v
		gotErr = err
	}

	su.createInboundService("uid-x", makeInboundConfig(80), "corr-x")

	if assert.NotNil(t, gotSuccess, "onComplete must be called") {
		assert.False(t, *gotSuccess, "a transient service-lookup error must fail the op for retry")
	}
	assert.Error(t, gotErr, "the transient error must be propagated")
}

// TestCreateInboundService_ServiceGoneNotFoundAbortsWithoutCreatingResources verifies that when the
// K8s Service is gone (getServiceByUID returns a typed NotFound), createInboundService must abort -
// it must NOT fall through to create the PIP/LB/SGW (which would be orphaned with no Service object
// to ever clean them up). It drops tracking and does NOT call onComplete (which would loop on
// NotFound or falsely report Created); a still-live Service is re-added on the next resync.
func TestCreateInboundService_ServiceGoneNotFoundAbortsWithoutCreatingResources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Empty kube (no Services): the List succeeds but no UID matches, so getServiceByUID returns a
	// typed NotFound.
	kube := fake.NewSimpleClientset()

	// The PIP must never be created on the abort path.
	pip := mock_publicipaddressclient.NewMockInterface(ctrl)
	pip.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	factory := mock_azclient.NewMockClientFactory(ctrl)
	factory.EXPECT().GetPublicIPAddressClient().Return(pip).AnyTimes()

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.kubeClient = kube
	dt.networkClientFactory = factory
	uid := "uid-gone"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:      StateCreationInProgress,
	}

	completeCalled := false
	su := newTestServiceUpdater(dt)
	su.onComplete = func(string, bool, error) { completeCalled = true }

	su.createInboundService(uid, makeInboundConfig(80), "corr-gone")

	dt.mu.Lock()
	_, tracked := dt.pendingServiceOps[uid]
	dt.mu.Unlock()
	assert.False(t, tracked, "a NotFound (service gone) create must drop tracking, not orphan resources")
	assert.False(t, completeCalled, "the abort path must not call onComplete")
}

// TestServiceUpdaterWorker_RecoversFromPanic verifies that a panic inside a worker operation is
// recovered (so the CCM process survives) and reported as a failed op via onComplete, rather than
// crashing the whole process.
func TestServiceUpdaterWorker_RecoversFromPanic(t *testing.T) {
	// A panicking fake client: the Service List panics, so createInboundService Step 0 panics.
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("list", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		panic("simulated apiserver client panic")
	})

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.kubeClient = kube

	var gotSuccess *bool
	var gotErr error
	su := newTestServiceUpdater(dt)
	su.onComplete = func(_ string, ok bool, err error) {
		v := ok
		gotSuccess = &v
		gotErr = err
	}

	su.wg.Add(1)
	assert.NotPanics(t, func() {
		su.runWorker("uid-panic", func() {
			su.createInboundService("uid-panic", makeInboundConfig(80), "corr-panic")
		})
	}, "a panic in a worker operation must be recovered, not propagated")
	su.wg.Wait()

	if assert.NotNil(t, gotSuccess, "onComplete must be called after a recovered panic") {
		assert.False(t, *gotSuccess, "a panicking op must be reported as a failed operation")
	}
	if assert.Error(t, gotErr) {
		assert.Contains(t, gotErr.Error(), "panic", "the failure must carry the panic info")
	}
}

// TestServiceUpdaterProcessBatchFlow asserts how processBatch categorises each pending operation:
// which states it promotes and dispatches, and which it leaves untouched.
//
// The state transitions asserted below are made synchronously by processBatch while it holds the
// lock, before any worker goroutine is spawned, and the completion callback used here records
// results without mutating engine state. The Azure clients are permissive because the workers'
// outcome is not what is under test.
func TestServiceUpdaterProcessBatchFlow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := newOutboundMocks(ctrl)
	mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
	m.factory.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()
	m.expectNoDisassociation()
	m.pip.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&armnetwork.PublicIPAddress{Name: ptr.To("pip")}, nil).AnyTimes()
	m.pip.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockLB.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockLB.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	m.sgw.EXPECT().UpdateServices(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	m.nat.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// The Services must exist: a dispatched operation looks its Service up by UID, and an operation
	// whose Service is gone is dropped from tracking rather than dispatched.
	uids := []string{"not-started", "creation-in-progress", "created", "deletion-pending", "deletion-in-progress", "parked"}
	objects := make([]runtime.Object, 0, len(uids))
	for _, uid := range uids {
		objects = append(objects, &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: uid, Namespace: "default", UID: types.UID(uid),
				Finalizers: []string{ServiceGatewayServiceCleanupFinalizer},
			},
			Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
		})
	}

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.networkClientFactory = m.factory
	dt.kubeClient = fake.NewSimpleClientset(objects...)

	newOp := func(uid string, state ResourceState) *ServiceOperationState {
		return &ServiceOperationState{ServiceUID: uid, Config: NewInboundServiceConfig(uid, nil), State: state}
	}
	dt.pendingServiceOps = map[string]*ServiceOperationState{
		"not-started":          newOp("not-started", StateNotStarted),
		"creation-in-progress": newOp("creation-in-progress", StateCreationInProgress),
		"created":              newOp("created", StateCreated),
		"deletion-pending":     newOp("deletion-pending", StateDeletionPending),
		"deletion-in-progress": newOp("deletion-in-progress", StateDeletionInProgress),
		"parked":               newOp("parked", StateNotStarted),
	}
	dt.pendingServiceOps["parked"].CreationFailedTerminal = true

	updater := outboundUpdater(dt, &outboundCompletion{})
	updater.processBatch()
	updater.wg.Wait()

	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Promoted and dispatched.
	assert.Equal(t, StateCreationInProgress, dt.pendingServiceOps["not-started"].State,
		"an unstarted operation must be promoted to CreationInProgress and dispatched")
	assert.NotNil(t, dt.pendingServiceOps["not-started"].InFlightConfig,
		"the dispatched config must be snapshotted as in-flight")

	// Left untouched.
	assert.Equal(t, StateCreationInProgress, dt.pendingServiceOps["creation-in-progress"].State,
		"a creation already in flight must not be dispatched again")
	assert.Nil(t, dt.pendingServiceOps["creation-in-progress"].InFlightConfig,
		"a skipped operation must not have a config snapshotted for it")
	assert.Equal(t, StateCreated, dt.pendingServiceOps["created"].State,
		"a completed service must not be re-dispatched")
	assert.Equal(t, StateDeletionPending, dt.pendingServiceOps["deletion-pending"].State,
		"a deletion still waiting for its locations to drain must not be dispatched")
	assert.Equal(t, StateNotStarted, dt.pendingServiceOps["parked"].State,
		"an operation parked after a terminal failure must not be re-dispatched")
	assert.Nil(t, dt.pendingServiceOps["parked"].InFlightConfig,
		"a parked operation must not have a config snapshotted for it")
}

// TestProcessBatch_RunsTheWorkerOfTheDispatchedKind pins the routing of each kind of run processBatch dispatches, inbound
// and outbound: the worker of that kind runs, as its Azure writes show, and completes the operation.
func TestProcessBatch_RunsTheWorkerOfTheDispatchedKind(t *testing.T) {
	const inboundUID = "11111111-1111-1111-1111-111111111111"
	const egress = egressCleanupTestID
	putPIP, deletePIP := "PUT publicIPAddresses/"+PublicIPName(inboundUID), "DELETE publicIPAddresses/"+PublicIPName(inboundUID)
	putLB, deleteLB := "PUT loadBalancers/"+inboundUID, "DELETE loadBalancers/"+inboundUID
	dflt := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})

	// inbound returns an inbound operation in state, whose Service exists, on mocked clients that record the writes.
	inbound := func(state ResourceState) func(t *testing.T) (*DiffTracker, func() []string) {
		return func(t *testing.T) (*DiffTracker, func() []string) {
			ctrl := gomock.NewController(t)
			m := newOutboundMocks(ctrl)
			lb := mock_loadbalancerclient.NewMockInterface(ctrl)
			m.factory.EXPECT().GetLoadBalancerClient().Return(lb).AnyTimes()
			var mu sync.Mutex
			var writes []string
			record := func(call string) {
				mu.Lock()
				defer mu.Unlock()
				writes = append(writes, call)
			}
			m.pip.EXPECT().CreateOrUpdate(gomock.Any(), "rg", PublicIPName(inboundUID), gomock.Any()).DoAndReturn(
				func(context.Context, string, string, armnetwork.PublicIPAddress) (*armnetwork.PublicIPAddress, error) {
					record(putPIP)
					return &armnetwork.PublicIPAddress{Properties: &armnetwork.PublicIPAddressPropertiesFormat{IPAddress: ptr.To("10.1.2.3")}}, nil
				}).AnyTimes()
			m.pip.EXPECT().Delete(gomock.Any(), "rg", PublicIPName(inboundUID)).DoAndReturn(func(context.Context, string, string) error {
				record(deletePIP)
				return nil
			}).AnyTimes()
			lb.EXPECT().CreateOrUpdate(gomock.Any(), "rg", inboundUID, gomock.Any()).DoAndReturn(
				func(context.Context, string, string, armnetwork.LoadBalancer) (*armnetwork.LoadBalancer, error) {
					record(putLB)
					return nil, nil
				}).AnyTimes()
			lb.EXPECT().Delete(gomock.Any(), "rg", inboundUID).DoAndReturn(func(context.Context, string, string) error {
				record(deleteLB)
				return nil
			}).AnyTimes()
			m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).DoAndReturn(
				func(context.Context, string, string, armnetwork.ServiceGatewayUpdateServicesRequest) error {
					record(cleanupCallRegister)
					return nil
				}).AnyTimes()

			dt := newTestDiffTracker()
			dt.config = testConfig()
			dt.networkClientFactory = m.factory
			dt.kubeClient = fake.NewSimpleClientset(&v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default", UID: inboundUID,
					Finalizers: []string{ServiceGatewayServiceCleanupFinalizer, servicehelper.LoadBalancerCleanupFinalizer}},
				Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
			})
			dt.pendingServiceOps[inboundUID] = &ServiceOperationState{
				ServiceUID: inboundUID, Config: NewInboundServiceConfig(inboundUID, makeInboundConfig(80)), State: state,
			}
			return dt, func() []string {
				mu.Lock()
				defer mu.Unlock()
				return append([]string(nil), writes...)
			}
		}
	}
	// updating returns the egress identity in mode being updated to desired, its managed NAT Gateway created with the
	// default configuration and its registration referencing natID.
	updating := func(mode egressMode, desired EgressConfig, natID string) func(t *testing.T) (*DiffTracker, func() []string) {
		return func(t *testing.T) (*DiffTracker, func() []string) {
			dt := newTestDiffTracker()
			dt.config = testConfig()
			dt.egress.mode[egress] = mode
			dt.pendingServiceOps[egress] = &ServiceOperationState{ServiceUID: egress, State: StateUpdateInProgress,
				Config: NewOutboundServiceConfig(egress, &OutboundConfig{IPFamilies: ipv4Families(), Egress: desired})}
			c := newFakeARM(t, dt)
			c.seedApplied(dflt, false)
			c.register(egress, natID)
			return dt, c.writes
		}
	}

	cases := []struct {
		name        string
		setup       func(t *testing.T) (*DiffTracker, func() []string)
		wantWrites  []string
		wantOutcome *egressOutcome // nil: an inbound run completes without one
	}{
		{name: "inbound create", setup: inbound(StateNotStarted), wantWrites: []string{putPIP, putLB, cleanupCallRegister}},
		{name: "inbound update", setup: inbound(StateUpdateInProgress), wantWrites: []string{putLB}},
		{name: "inbound delete", setup: inbound(StateDeletionInProgress), wantWrites: []string{cleanupCallRegister, deleteLB, cleanupCallRegister, deletePIP}},
		{name: "outbound create", setup: func(t *testing.T) (*DiffTracker, func() []string) {
			dt, c := newOutboundCreation(t)
			dt.pendingServiceOps[egress].State = StateNotStarted
			return dt, c.writes
		}, wantWrites: []string{"PUT publicIPAddresses/egress-a-pip", cleanupCallPutNAT, cleanupCallRegister},
			wantOutcome: &egressOutcome{Mode: egressModeManaged, Applied: &dflt, NATWritten: true, Swept: true}},
		{name: "outbound update of a managed identity", setup: updating(egressModeManaged, two, natGatewayID(egress, testConfig())),
			wantWrites:  []string{"PUT publicIPAddresses/egress-a-pip-2", cleanupCallPutNAT},
			wantOutcome: &egressOutcome{Mode: egressModeManaged, Applied: &two, NATWritten: true, Swept: true}},
		{name: "outbound update of a BYO identity", setup: updating(egressModeBYO, egressUnknown, byoNATGatewayARMID(egress)),
			wantWrites:  []string{cleanupCallPutNAT, cleanupCallDeleteNAT, "DELETE publicIPAddresses/egress-a-pip"},
			wantOutcome: &egressOutcome{Mode: egressModeBYO, Swept: true}},
		{name: "outbound delete", setup: func(t *testing.T) (*DiffTracker, func() []string) {
			dt, c := newOutboundCloud(t, fake.NewSimpleClientset())
			return dt, c.writes
		}, wantWrites: []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister, cleanupCallDeleteNAT, "DELETE publicIPAddresses/egress-a-pip"},
			wantOutcome: &egressOutcome{Mode: egressModeUnknown, Swept: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt, writes := tc.setup(t)
			got := &outboundCompletion{}
			updater := outboundUpdater(dt, got)
			updater.processBatch()
			updater.wg.Wait()

			called, success, err := got.result()
			assert.True(t, called, "the run must complete the operation")
			assert.True(t, success, "%v", err)
			assert.Equal(t, tc.wantOutcome, got.lastOutcome())
			assert.Equal(t, tc.wantWrites, writes())
		})
	}
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

// TestCreateInboundService_StatusUpdateFailureRetriesInsteadOfFalseSuccess drives createInboundService
// with all Azure steps succeeding but the Service-status patch (Step 5) returning a transient non-409
// error. Because the load balancer would otherwise appear permanently pending, the op must report
// failure so the existing retry path re-runs (the Azure resources are idempotent), rather than
// reporting success and moving to StateCreated with an empty Ingress.
func TestCreateInboundService_StatusUpdateFailureRetriesInsteadOfFalseSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Pre-load the K8s Service WITH the SGW + LB finalizers so addServiceGatewayFinalizer
	// short-circuits on the Get (no Patch needed) — this isolates the Patch reactor below to
	// the Step 5 status patch only.
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc-status", Namespace: "default", UID: "uid-status",
			Finalizers: []string{ServiceGatewayServiceCleanupFinalizer, servicehelper.LoadBalancerCleanupFinalizer},
		},
		Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
	}
	kube := fake.NewSimpleClientset(svc)
	// Force every Service patch to fail with a generic (non-409, non-NotFound) transient error.
	// retry.RetryOnConflict only retries on Conflict, so this propagates as a hard error.
	kube.PrependReactor("patch", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("transient apiserver patch error")
	})

	f := mock_azclient.NewMockClientFactory(ctrl)
	mockPIP := mock_publicipaddressclient.NewMockInterface(ctrl)
	mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
	mockSGW := mock_servicegatewayclient.NewMockInterface(ctrl)
	f.EXPECT().GetPublicIPAddressClient().Return(mockPIP).AnyTimes()
	f.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()
	f.EXPECT().GetServiceGatewayClient().Return(mockSGW).AnyTimes()

	// PIP returns a populated response so pipIPAddress is non-empty and Step 5 actually runs.
	mockPIP.EXPECT().CreateOrUpdate(gomock.Any(), "rg", "uid-status-pip", gomock.Any()).Return(
		&armnetwork.PublicIPAddress{
			Name: ptr.To("uid-status-pip"),
			Properties: &armnetwork.PublicIPAddressPropertiesFormat{
				IPAddress: ptr.To("10.1.2.3"),
			},
		}, nil)
	mockLB.EXPECT().CreateOrUpdate(gomock.Any(), "rg", "uid-status", gomock.Any()).Return(nil, nil)
	mockSGW.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil)

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.kubeClient = kube
	dt.networkClientFactory = f
	uid := "uid-status"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:      StateCreationInProgress,
		InFlightConfig: func() *ServiceConfig {
			c := NewInboundServiceConfig(uid, makeInboundConfig(80))
			return &c
		}(),
	}

	var gotSuccess *bool
	var gotErr error
	su := newTestServiceUpdater(dt)
	su.onComplete = func(serviceUID string, ok bool, err error) {
		b := ok
		gotSuccess = &b
		gotErr = err
		// Route through the engine completion to drive the StateCreated transition.
		dt.OnServiceCreationComplete(serviceUID, ok, err)
	}

	su.createInboundService(uid, makeInboundConfig(80), "corr-status")

	if assert.NotNil(t, gotSuccess, "onComplete must be called") {
		assert.False(t, *gotSuccess, "a status-patch failure must fail the op, not report success")
	}
	if assert.Error(t, gotErr, "the status-patch failure must be propagated") {
		assert.Contains(t, gotErr.Error(), "failed to update service status with external IP")
	}

	op := dt.pendingServiceOps[uid]
	if assert.NotNil(t, op, "op must remain tracked") {
		assert.Equal(t, StateNotStarted, op.State, "a status-patch failure must reset the op for retry, not promote it to StateCreated")
		assert.Equal(t, 1, op.RetryCount, "a status-patch failure must schedule a retry")
	}

	// The status patch failed, so Ingress stays empty for this attempt; the scheduled retry repopulates it.
	got, err := kube.CoreV1().Services("default").Get(context.Background(), "svc-status", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Empty(t, got.Status.LoadBalancer.Ingress)
}

// TestCreateInboundService_PopulatesIngressOnSuccess confirms the success path writes the allocated
// public IP into Service.Status.LoadBalancer.Ingress and promotes the op to StateCreated, so a later
// retry caused by a transient status failure eventually surfaces the external IP.
func TestCreateInboundService_PopulatesIngressOnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc-status-ok", Namespace: "default", UID: "uid-status-ok",
			Finalizers: []string{ServiceGatewayServiceCleanupFinalizer, servicehelper.LoadBalancerCleanupFinalizer},
		},
		Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
	}
	kube := fake.NewSimpleClientset(svc)

	f := mock_azclient.NewMockClientFactory(ctrl)
	mockPIP := mock_publicipaddressclient.NewMockInterface(ctrl)
	mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
	mockSGW := mock_servicegatewayclient.NewMockInterface(ctrl)
	f.EXPECT().GetPublicIPAddressClient().Return(mockPIP).AnyTimes()
	f.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()
	f.EXPECT().GetServiceGatewayClient().Return(mockSGW).AnyTimes()

	mockPIP.EXPECT().CreateOrUpdate(gomock.Any(), "rg", "uid-status-ok-pip", gomock.Any()).Return(
		&armnetwork.PublicIPAddress{
			Name:       ptr.To("uid-status-ok-pip"),
			Properties: &armnetwork.PublicIPAddressPropertiesFormat{IPAddress: ptr.To("10.1.2.3")},
		}, nil)
	mockLB.EXPECT().CreateOrUpdate(gomock.Any(), "rg", "uid-status-ok", gomock.Any()).Return(nil, nil)
	mockSGW.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil)

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.kubeClient = kube
	dt.networkClientFactory = f
	uid := "uid-status-ok"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:      StateCreationInProgress,
		InFlightConfig: func() *ServiceConfig {
			c := NewInboundServiceConfig(uid, makeInboundConfig(80))
			return &c
		}(),
	}

	var gotSuccess *bool
	su := newTestServiceUpdater(dt)
	su.onComplete = func(serviceUID string, ok bool, err error) {
		b := ok
		gotSuccess = &b
		dt.OnServiceCreationComplete(serviceUID, ok, err)
	}

	su.createInboundService(uid, makeInboundConfig(80), "corr-status-ok")

	if assert.NotNil(t, gotSuccess, "onComplete must be called") {
		assert.True(t, *gotSuccess, "a create with a successful status patch must report success")
	}
	op := dt.pendingServiceOps[uid]
	if assert.NotNil(t, op, "op must remain tracked") {
		assert.Equal(t, StateCreated, op.State, "a successful create must promote the op to StateCreated")
	}

	got, err := kube.CoreV1().Services("default").Get(context.Background(), "svc-status-ok", metav1.GetOptions{})
	assert.NoError(t, err)
	if assert.Len(t, got.Status.LoadBalancer.Ingress, 1, "the allocated IP must be written to the Service status") {
		assert.Equal(t, "10.1.2.3", got.Status.LoadBalancer.Ingress[0].IP)
	}
}

// TestCreateInboundServiceClearsBuffersWhenServiceGone verifies that aborting createInboundService
// because the Service no longer exists also drops the endpoints and pods buffered for its in-flight
// creation, so they do not leak until the next restart.
func TestCreateInboundServiceClearsBuffersWhenServiceGone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const uid = "11111111-1111-1111-1111-111111111111"
	dt := newTestDiffTracker()
	dt.kubeClient = fake.NewSimpleClientset() // empty: getServiceByUID returns a typed NotFound
	dt.networkClientFactory = mock_azclient.NewMockClientFactory(ctrl)

	dt.pendingServiceOps[uid] = &ServiceOperationState{ServiceUID: uid, State: StateCreationInProgress}
	dt.pendingEndpoints[uid] = []PendingEndpointUpdate{{PodIPToNodeIP: map[string]string{"10.244.0.1": "10.0.0.1"}}}
	dt.pendingPods[uid] = []PendingPodUpdate{{PodKey: "ns/p", Location: "10.0.0.1", Address: "10.244.0.1"}}

	su := NewServiceUpdater(context.Background(), dt, func(string, bool, error) {}, func(string, bool, error, *egressOutcome) {}, dt.GetServiceUpdaterTrigger())
	su.createInboundService(uid, &InboundConfig{}, "corr")

	dt.mu.Lock()
	defer dt.mu.Unlock()
	if _, ok := dt.pendingServiceOps[uid]; ok {
		t.Fatalf("aborted create must drop the service operation")
	}
	if _, ok := dt.pendingEndpoints[uid]; ok {
		t.Fatalf("aborted create must drop buffered endpoints")
	}
	if _, ok := dt.pendingPods[uid]; ok {
		t.Fatalf("aborted create must drop buffered pods")
	}
}

// ---------------------------------------------------------------------------------------------
// Outbound (egress) lifecycle.
//
// deleteOutboundService is the most destructive operation in the feature: it disassociates the NAT
// Gateway from the ServiceGateway, unregisters it from NRP, deletes the NAT Gateway and its Public
// IP, and only then releases the last-pod finalizers holding egress pods - and therefore node
// drains and namespace deletions - open. Every failing step must report failure and retain NRP
// state so the operation is retried instead of leaking the Azure resource.
// ---------------------------------------------------------------------------------------------

// outboundMocks bundles the clients deleteOutboundService/createOutboundService drive.
type outboundMocks struct {
	factory *mock_azclient.MockClientFactory
	sgw     *mock_servicegatewayclient.MockInterface
	nat     *mock_natgatewayclient.MockInterface
	pip     *mock_publicipaddressclient.MockInterface
	prefix  *mock_publicipprefixclient.MockInterface
}

func newOutboundMocks(ctrl *gomock.Controller) *outboundMocks {
	m := &outboundMocks{
		factory: mock_azclient.NewMockClientFactory(ctrl),
		sgw:     mock_servicegatewayclient.NewMockInterface(ctrl),
		nat:     mock_natgatewayclient.NewMockInterface(ctrl),
		pip:     mock_publicipaddressclient.NewMockInterface(ctrl),
		prefix:  mock_publicipprefixclient.NewMockInterface(ctrl),
	}
	m.factory.EXPECT().GetServiceGatewayClient().Return(m.sgw).AnyTimes()
	m.factory.EXPECT().GetNatGatewayClient().Return(m.nat).AnyTimes()
	m.factory.EXPECT().GetPublicIPAddressClient().Return(m.pip).AnyTimes()
	m.factory.EXPECT().GetPublicIPPrefixClient().Return(m.prefix).AnyTimes()
	return m
}

// expectNoReservedResources makes the reserved Public IP and prefix names of an egress identity absent, as the
// reads of a preflight or a sweep find them, unless an expectation declared before answers a name.
func (m *outboundMocks) expectNoReservedResources() {
	m.pip.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, notFoundError()).AnyTimes()
	m.prefix.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, notFoundError()).AnyTimes()
}

// expectManagedEgressCreate answers the calls a managed creation of uid with the default configuration makes once
// its NAT Gateway is resolved (design 4.4): the preflight reads existing (nil: no NAT Gateway) and no Public IP, the
// Public IP and NAT Gateway PUTs return what they were sent, the identity is registered once, and the sweep finds
// no reserved Public IP or prefix. The Service Gateway services are read through the caller's expectations.
func (m *outboundMocks) expectManagedEgressCreate(uid string, existing *armnetwork.NatGateway) {
	if existing != nil {
		m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(existing, nil).Times(1)
	} else {
		m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, notFoundError()).Times(1)
	}
	m.expectNoReservedResources()
	m.pip.EXPECT().CreateOrUpdate(gomock.Any(), "rg", PublicIPName(uid), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, pip armnetwork.PublicIPAddress) (*armnetwork.PublicIPAddress, error) {
			return &pip, nil
		}).Times(1)
	m.nat.EXPECT().CreateOrUpdate(gomock.Any(), "rg", uid, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, nat armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
			return &nat, nil
		}).Times(1)
	m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).Times(1)
}

// expectNoDisassociation makes Step 1 of deleteOutboundService a clean no-op: the ServiceGateway
// reports no matching service and the NAT Gateway is already gone.
func (m *outboundMocks) expectNoDisassociation() {
	m.sgw.EXPECT().GetServices(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*armnetwork.ServiceGatewayService{}, nil).AnyTimes()
	m.nat.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, notFoundError()).AnyTimes()
}

func newOutboundDiffTracker(uid string, m *outboundMocks, kube *fake.Clientset) *DiffTracker {
	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.networkClientFactory = m.factory
	if kube != nil {
		dt.kubeClient = kube
	}
	dt.NRPResources.NATGateways = utilsets.NewString(uid)
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid, Config: NewOutboundServiceConfig(uid, nil), State: StateDeletionInProgress,
	}
	return dt
}

// outboundCompletion records the completion callback. It is mutex-guarded because processBatch can
// dispatch several operations concurrently, so more than one worker may report into it.
type outboundCompletion struct {
	mu      sync.Mutex
	called  bool
	success bool
	err     error
	// outcome is the outcome an outbound run completed with; nil for a completion without one.
	outcome *egressOutcome
}

func (c *outboundCompletion) record(success bool, err error, outcome *egressOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.called, c.success, c.err, c.outcome = true, success, err, outcome
}

func (c *outboundCompletion) result() (called, success bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.called, c.success, c.err
}

// lastOutcome returns the outcome of the last completion; nil when it had none.
func (c *outboundCompletion) lastOutcome() *egressOutcome {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outcome
}

func outboundUpdater(dt *DiffTracker, got *outboundCompletion) *ServiceUpdater {
	return &ServiceUpdater{
		diffTracker: dt,
		onComplete: func(_ string, success bool, err error) {
			got.record(success, err, nil)
		},
		onOutboundComplete: func(_ string, success bool, err error, outcome *egressOutcome) {
			got.record(success, err, outcome)
		},
		trigger:   dt.serviceUpdaterTrigger,
		ctx:       context.Background(),
		semaphore: make(chan struct{}, 10),
		activeOps: make(map[string]bool),
	}
}

// newOutboundCloud returns a tracker whose egress identity egressCleanupTestID is being deleted, as
// newOutboundDiffTracker leaves one, and the fake ARM it calls (fakeARM): the identity's
// managed NAT Gateway was created with the default configuration and is registered.
func newOutboundCloud(t *testing.T, kube *fake.Clientset) (*DiffTracker, *fakeARM) {
	t.Helper()
	dt := newTestDiffTracker()
	dt.config = testConfig()
	if kube != nil {
		dt.kubeClient = kube
	}
	dt.NRPResources.NATGateways = utilsets.NewString(egressCleanupTestID)
	dt.pendingServiceOps[egressCleanupTestID] = &ServiceOperationState{
		ServiceUID: egressCleanupTestID, Config: NewOutboundServiceConfig(egressCleanupTestID, nil), State: StateDeletionInProgress,
	}
	c := newFakeARM(t, dt)
	c.seedApplied(defaultEgressConfig(egressParseContext{}), false)
	c.register(egressCleanupTestID, natGatewayID(egressCleanupTestID, dt.config))
	return dt, c
}

// TestGuardDeleteOutboundService_RemovesLastPodFinalizerOnlyAfterNATGatewayDeletion pins the
// ordering that the whole egress teardown design rests on: the last pod's cleanup finalizer must
// not be released until the NAT Gateway is actually gone from Azure.
//
// Releasing it first hands the pod (and its IP) back to Kubernetes while NRP still routes that IP
// through a live NAT Gateway, which is exactly the stranding the finalizer exists to prevent.
//
// Every other last-pod test hand-calls RemoveLastPodFinalizers, so none of them observes the
// ordering; this drives the real deleteOutboundService and asserts, from inside the NAT delete
// call itself, that the finalizer is still attached at that moment.
func TestGuardDeleteOutboundService_RemovesLastPodFinalizerOnlyAfterNATGatewayDeletion(t *testing.T) {
	const (
		uid     = egressCleanupTestID
		podNS   = "default"
		podName = "egress-last-pod"
		podUID  = "pod-uid-1"
		podKey  = podNS + "/" + podName
	)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       podName,
			Namespace:  podNS,
			UID:        types.UID(podUID),
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
	}
	kube := fake.NewSimpleClientset(pod)
	dt, c := newOutboundCloud(t, kube)

	// Captured at the instant the NAT Gateway delete is served.
	var finalizerHeldDuringNATDelete bool
	c.hook(cleanupCallDeleteNAT, func(*fakeARM) {
		live, err := kube.CoreV1().Pods(podNS).Get(context.Background(), podName, metav1.GetOptions{})
		if err == nil {
			finalizerHeldDuringNATDelete = hasPodFinalizer(live)
		}
	})

	dt.pendingPodDeletions[podKey] = &PendingPodDeletion{
		Namespace:  podNS,
		Name:       podName,
		UID:        podUID,
		ServiceUID: uid,
		Addresses:  []string{"10.244.0.5"},
		IsLastPod:  true,
	}

	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

	called, success, completionErr := got.result()
	assert.True(t, called)
	assert.True(t, success, "a fully successful teardown must report success: %v", completionErr)

	assert.Len(t, c.requestsOf(cleanupCallDeleteNAT), 1, "control: the NAT Gateway was deleted")
	c.update(func(*fakeARM) {
		assert.True(t, finalizerHeldDuringNATDelete,
			"the last pod must still carry its cleanup finalizer while the NAT Gateway is being deleted")
	})

	live, err := kube.CoreV1().Pods(podNS).Get(context.Background(), podName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.False(t, hasPodFinalizer(live),
		"the last pod's finalizer must be released once the NAT Gateway is gone")
}

// TestGuardDeleteOutboundService_FinalizerRemovalFailureReportsFailure pins the other half of the
// contract: if the finalizer sweep cannot complete, the delete must be reported as failed so it is
// retried (the NAT/PIP deletes are idempotent on 404). Reporting success would drop the operation
// while the pod stays stuck Terminating forever.
func TestGuardDeleteOutboundService_FinalizerRemovalFailureReportsFailure(t *testing.T) {
	const (
		uid     = egressCleanupTestID
		podNS   = "default"
		podName = "egress-last-pod"
		podUID  = "pod-uid-2"
	)

	kube := fake.NewSimpleClientset(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       podName,
			Namespace:  podNS,
			UID:        types.UID(podUID),
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
	})
	// Every attempt to strip the finalizer fails, exhausting the retry budget.
	kube.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver down")
	})

	dt, c := newOutboundCloud(t, kube)
	dt.pendingPodDeletions[podNS+"/"+podName] = &PendingPodDeletion{
		Namespace:  podNS,
		Name:       podName,
		UID:        podUID,
		ServiceUID: uid,
		Addresses:  []string{"10.244.0.5"},
		IsLastPod:  true,
	}

	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

	called, success, completionErr := got.result()
	assert.True(t, called)
	assert.False(t, success,
		"a delete whose last-pod finalizer sweep failed must be reported as failed so it retries")
	assert.Error(t, completionErr)
	assert.Nil(t, c.nat(dt.config.ResourceGroup, uid), "control: the Azure teardown itself succeeded")
}

// TestServiceUpdaterDeleteOutboundService_HappyPath asserts the exact Azure teardown sequence and
// that NRP state is only cleared once every step succeeded: the unlink of both sides and the
// unregistration, then the NAT Gateway, read right before its conditional DELETE, then the sweep of
// every reserved Public IP and prefix name.
func TestServiceUpdaterDeleteOutboundService_HappyPath(t *testing.T) {
	const uid = egressCleanupTestID

	dt, c := newOutboundCloud(t, fake.NewSimpleClientset())
	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

	called, success, completionErr := got.result()
	assert.True(t, called)
	assert.True(t, success, "a fully successful teardown must report success: %v", completionErr)
	assert.False(t, dt.NRPResources.NATGateways.Has(uid),
		"NRP NAT Gateway state must be cleared after a successful delete")
	assert.Equal(t, []string{
		cleanupCallGetNAT, cleanupCallGetServices,
		cleanupCallGetServices, cleanupCallRegister, cleanupCallGetNAT, cleanupCallPutNAT,
		cleanupCallRegister,
		cleanupCallGetNAT, cleanupCallDeleteNAT,
	}, c.sequence())
	assert.Equal(t, cleanupReservedReads(true), c.reservedCalls(http.MethodGet))
	assert.Equal(t, cleanupDeletes("egress-a-pip"), c.reservedCalls(http.MethodDelete))
	assert.Nil(t, c.nat(dt.config.ResourceGroup, uid))
}

// TestServiceUpdaterDeleteOutboundService_StepFailuresRetain covers every failing Azure step. Each
// must report failure so the operation is retried, and must NOT clear the NRP entry - clearing it
// would make the retried delete a no-op and leak the Azure resource while the pod finalizer stays.
func TestServiceUpdaterDeleteOutboundService_StepFailuresRetain(t *testing.T) {
	const uid = egressCleanupTestID
	refused := armReplyError(http.StatusConflict, "Conflict")

	for _, tc := range []struct {
		name   string
		faults []string
	}{
		{name: "ServiceGateway unregister fails", faults: []string{cleanupCallRegister}},
		{name: "NAT Gateway delete fails", faults: []string{cleanupCallDeleteNAT}},
		{name: "Public IP delete fails", faults: []string{"DELETE publicIPAddresses/egress-a-pip"}},
		{name: "every step fails", faults: []string{cleanupCallRegister, cleanupCallDeleteNAT, "DELETE publicIPAddresses/egress-a-pip"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, c := newOutboundCloud(t, fake.NewSimpleClientset())
			// Registered without a NAT Gateway, so the unregistration is the only registration write.
			c.register(uid, "")
			for _, call := range tc.faults {
				c.fault(call, refused)
			}
			got := &outboundCompletion{}
			outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

			called, success, completionErr := got.result()
			assert.True(t, called)
			assert.False(t, success, "a failed teardown step must report failure so it is retried")
			assert.Error(t, completionErr)
			assert.True(t, dt.NRPResources.NATGateways.Has(uid),
				"NRP state must be retained on failure, otherwise the retry is a no-op and Azure leaks")
		})
	}
}

// TestServiceUpdaterDeleteOutboundService_ToleratesAlreadyDeleted asserts crash-after-delete
// convergence: an already-absent NAT Gateway and Public IP are a successful teardown, not a
// permanent failure that would strand the egress pod finalizer.
func TestServiceUpdaterDeleteOutboundService_ToleratesAlreadyDeleted(t *testing.T) {
	const uid = egressCleanupTestID

	dt, c := newOutboundCloud(t, fake.NewSimpleClientset())
	c.update(func(c *fakeARM) {
		delete(c.nats, fakeARMKey(dt.config.ResourceGroup, uid))
		delete(c.pips, fakeARMKey(dt.config.ResourceGroup, PublicIPName(uid)))
		delete(c.services, uid)
	})
	c.fault(cleanupCallRegister, armReplyError(http.StatusNotFound, "NotFound"))
	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

	called, success, completionErr := got.result()
	assert.True(t, called)
	assert.True(t, success, "404 on every resource means the teardown is already complete: %v", completionErr)
	assert.False(t, dt.NRPResources.NATGateways.Has(uid))
	assert.Empty(t, c.requestsOf(cleanupCallDeleteNAT), "an absent NAT Gateway needs no DELETE")
}

// TestServiceUpdaterDeleteOutboundService_EachCallHasABudgetOfItsOwn pins design 4.4 for the managed deletion: every
// Azure call, its ownership check and its Service Gateway calls included, has a budget of its own taken from the
// updater's context, never one the run shares. Here every answer takes most of a budget, so no two calls fit into one,
// and the deletion still completes. A call Azure never answers ends with its own budget, so the run ends.
func TestServiceUpdaterDeleteOutboundService_EachCallHasABudgetOfItsOwn(t *testing.T) {
	const uid = egressCleanupTestID
	setEgressCallTimeoutForTest(t, 600*time.Millisecond)

	t.Run("every answer takes most of a budget", func(t *testing.T) {
		setEgressConcurrencyForTest(t, len(reservedManaged(true)))
		dt, c := newOutboundCloud(t, fake.NewSimpleClientset())
		c.update(func(c *fakeARM) { c.delay = 350 * time.Millisecond })
		got := &outboundCompletion{}
		outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

		_, success, err := got.result()
		assert.True(t, success, "%v", err)
		assert.Nil(t, c.nat(dt.config.ResourceGroup, uid), "control: the NAT Gateway is deleted")
		assert.Len(t, c.writes(), 5, "control: the unlink of both sides, the unregistration and two DELETEs")
	})

	for _, call := range []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallRegister} {
		t.Run(call+" never answered", func(t *testing.T) {
			dt, c := newOutboundCloud(t, fake.NewSimpleClientset())
			gate := newArmGate()
			t.Cleanup(gate.open)
			c.fault(call, func(_ http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-gate.ch:
				}
			})
			got := &outboundCompletion{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				outboundUpdater(dt, got).deleteOutboundService(uid, "corr")
			}()
			select {
			case <-done:
				_, success, err := got.result()
				assert.False(t, success)
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(10 * time.Second):
				t.Errorf("the call did not end within its budget")
				gate.open()
				<-done
			}
		})
	}
}

// newOutboundCreation returns a tracker creating egress identity egressCleanupTestID and the stateful
// fake ARM it calls, which holds nothing of the identity yet.
func newOutboundCreation(t *testing.T) (*DiffTracker, *fakeARM) {
	t.Helper()
	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.pendingServiceOps[egressCleanupTestID] = &ServiceOperationState{
		ServiceUID: egressCleanupTestID, Config: NewOutboundServiceConfig(egressCleanupTestID, &OutboundConfig{}), State: StateCreationInProgress,
	}
	return dt, newFakeARM(t, dt)
}

// dispatchedCreation returns the outbound config the dispatch (processBatch) starts the creation of an egress identity
// without members with: its IP families and, as its creation configuration, their default one.
func dispatchedCreation(dt *DiffTracker, families ...string) *OutboundConfig {
	return &OutboundConfig{IPFamilies: families, Egress: defaultEgressConfig(dt.egressParseContextFor(families))}
}

// TestServiceUpdaterCreateOutboundService_HappyPath asserts the provisioning order (PIP, then NAT
// Gateway, then ServiceGateway registration) and that NRP state is recorded only on success.
func TestServiceUpdaterCreateOutboundService_HappyPath(t *testing.T) {
	const uid = egressCleanupTestID

	dt, c := newOutboundCreation(t)
	got := &outboundCompletion{}
	outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt), "corr", "ns", "pod")

	called, success, completionErr := got.result()
	assert.True(t, called)
	assert.True(t, success, "a fully successful create must report success: %v", completionErr)
	assert.True(t, dt.NRPResources.NATGateways.Has(uid),
		"NRP NAT Gateway state must be recorded after a successful create")
	assert.Equal(t, []string{"PUT publicIPAddresses/egress-a-pip", cleanupCallPutNAT, cleanupCallRegister}, c.writes())
}

// TestServiceUpdaterCreateOutboundService_StepFailuresDoNotRecordNRPState covers each failing
// provisioning step. Recording NRP state after a partial create would make the tracker believe NRP
// holds a service it does not, and the diff would never re-create it.
func TestServiceUpdaterCreateOutboundService_StepFailuresDoNotRecordNRPState(t *testing.T) {
	const uid = egressCleanupTestID

	for _, tc := range []struct {
		name  string
		fault string
	}{
		{name: "Public IP create fails", fault: "PUT publicIPAddresses/egress-a-pip"},
		{name: "NAT Gateway create fails", fault: cleanupCallPutNAT},
		{name: "ServiceGateway registration fails", fault: cleanupCallRegister},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, c := newOutboundCreation(t)
			c.fault(tc.fault, armReplyError(http.StatusConflict, "Conflict"))
			got := &outboundCompletion{}
			outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt), "corr", "ns", "pod")

			called, success, completionErr := got.result()
			assert.True(t, called)
			assert.False(t, success, "a failed create step must report failure so it is retried")
			assert.Error(t, completionErr)
			assert.False(t, dt.NRPResources.NATGateways.Has(uid),
				"a partially created outbound service must not be recorded as present in NRP")
		})
	}
}

// TestServiceUpdaterUpdateInboundService covers the path that applies a spec change to a live
// LoadBalancer. It re-PUTs only the LoadBalancer: the Public IP allocation is independent of the
// rules, and the ServiceGateway registration references the backend pool by an ID that is stable
// across port edits.
func TestServiceUpdaterUpdateInboundService(t *testing.T) {
	const uid = "11111111-1111-1111-1111-111111111111"

	validConfig := func() *InboundConfig {
		return &InboundConfig{
			FrontendPorts: []PortMapping{{Port: 80, Protocol: "TCP"}},
			BackendPorts:  []PortMapping{{Port: 8080, Protocol: "TCP"}},
		}
	}

	t.Run("re-PUTs the LoadBalancer and reports success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := newOutboundMocks(ctrl)
		mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
		m.factory.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()
		mockLB.EXPECT().CreateOrUpdate(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, nil).Times(1)
		// A port-only update must not touch the Public IP or re-register with the ServiceGateway.
		m.pip.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
		m.sgw.EXPECT().UpdateServices(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		dt := newTestDiffTracker()
		dt.config = testConfig()
		dt.networkClientFactory = m.factory
		got := &outboundCompletion{}
		outboundUpdater(dt, got).updateInboundService(uid, validConfig(), "corr")

		called, success, completionErr := got.result()
		assert.True(t, called)
		assert.True(t, success, "a successful LoadBalancer update must report success: %v", completionErr)
	})

	t.Run("transient ARM failure is retryable, not terminal", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := newOutboundMocks(ctrl)
		mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
		m.factory.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()
		mockLB.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("boom")).AnyTimes()

		dt := newTestDiffTracker()
		dt.config = testConfig()
		dt.networkClientFactory = m.factory
		got := &outboundCompletion{}
		outboundUpdater(dt, got).updateInboundService(uid, validConfig(), "corr")

		called, success, completionErr := got.result()
		assert.True(t, called)
		assert.False(t, success)
		assert.False(t, isTerminalError(completionErr),
			"an ARM failure must stay retryable so the update is re-attempted")
	})

	t.Run("unsupported spec parks instead of retrying forever", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := newOutboundMocks(ctrl)
		mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
		m.factory.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()
		// The build fails first, so the LoadBalancer must never be PUT with an unsupported spec.
		mockLB.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		dualStack := validConfig()
		dualStack.IPFamilies = []string{"IPv4", "IPv6"}

		dt := newTestDiffTracker()
		dt.config = testConfig()
		dt.networkClientFactory = m.factory
		got := &outboundCompletion{}
		outboundUpdater(dt, got).updateInboundService(uid, dualStack, "corr")

		called, success, completionErr := got.result()
		assert.True(t, called)
		assert.False(t, success)
		assert.True(t, isTerminalError(completionErr),
			"a deterministic spec failure must be terminal so the engine parks instead of looping")
	})
}

// TestServiceUpdaterUpdateInboundService_PortRemovalDropsOnlyThatRule pins the ARM payload for a
// port removal, which is the only way a Service can lose a load-balancing rule: Kubernetes rejects
// a type=LoadBalancer Service with an empty spec.ports, so a Service can never transition to
// "no ports at all" and the rule set can only shrink to a smaller non-empty set.
//
// updateInboundService rebuilds the LoadBalancer from the new config and PUTs it whole, so the
// rules it sends are the complete desired set and the removed port's rule disappears by omission.
// The sibling subtests above assert only that CreateOrUpdate happened, with gomock.Any() for the
// body; that still passes if the stale rule is carried into the payload, which would leave the
// removed port reachable on the frontend IP. This asserts the body itself.
func TestServiceUpdaterUpdateInboundService_PortRemovalDropsOnlyThatRule(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const uid = "22222222-2222-2222-2222-222222222222"

	m := newOutboundMocks(ctrl)
	mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
	m.factory.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()

	var puts []armnetwork.LoadBalancer
	mockLB.EXPECT().CreateOrUpdate(gomock.Any(), "rg", uid, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, lb armnetwork.LoadBalancer) (*armnetwork.LoadBalancer, error) {
			puts = append(puts, lb)
			return nil, nil
		}).Times(1)

	// Dropping a port changes only the rule list. Re-allocating the Public IP would move the
	// Service's external address, and re-registering with the ServiceGateway is unnecessary
	// because the backend pool ID the registration points at does not change.
	m.pip.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	m.sgw.EXPECT().UpdateServices(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.networkClientFactory = m.factory

	// The Service previously published 80->8080 and 443->8443; 443 has been removed from spec.ports.
	remaining := &InboundConfig{
		FrontendPorts: []PortMapping{{Port: 80, Protocol: "TCP"}},
		BackendPorts:  []PortMapping{{Port: 8080, Protocol: "TCP"}},
	}

	got := &outboundCompletion{}
	outboundUpdater(dt, got).updateInboundService(uid, remaining, "corr-port-removal")

	called, success, completionErr := got.result()
	assert.True(t, called)
	assert.True(t, success, "removing a port must report success: %v", completionErr)

	if assert.Len(t, puts, 1, "the update must PUT the LoadBalancer exactly once") &&
		assert.NotNil(t, puts[0].Properties) {
		rules := puts[0].Properties.LoadBalancingRules
		if assert.Len(t, rules, 1, "the PUT must carry only the surviving rule, so the removed one is deleted by omission") {
			assert.Equal(t, "rule-tcp-80", *rules[0].Name)
			assert.Equal(t, int32(80), *rules[0].Properties.FrontendPort)
			assert.Equal(t, int32(8080), *rules[0].Properties.BackendPort)
		}
		for _, rule := range rules {
			assert.NotEqual(t, "rule-tcp-443", *rule.Name,
				"the removed port's rule must not survive in the payload sent to Azure")
		}
		// The frontend and backend pool are the anchors the surviving rule and the ServiceGateway
		// registration reference; a port edit that dropped either would tear down the data path.
		assert.Len(t, puts[0].Properties.FrontendIPConfigurations, 1,
			"a port removal must keep the frontend IP configuration")
		assert.Len(t, puts[0].Properties.BackendAddressPools, 1,
			"a port removal must keep the backend address pool")
	}
}

// newRetryTimerTestUpdater builds an updater whose fired timer is observable as a buffered token.
func newRetryTimerTestUpdater(t *testing.T) (*ServiceUpdater, *DiffTracker) {
	t.Helper()
	dt := newTestDiffTracker()
	ctx, cancel := context.WithCancel(context.Background())
	s := &ServiceUpdater{
		diffTracker: dt,
		ctx:         ctx,
		cancel:      cancel,
		activeOps:   make(map[string]bool),
		retryTimers: make(map[string]*time.Timer),
		logger:      dt.logger,
	}
	return s, dt
}

// TestServiceUpdaterStop_CancelsPendingRetryTimers pins that a parked operation's self-arm cannot
// outlive the updater. Those timers are not tracked by wg nor bound to ctx, so unless Stop cancels
// them each keeps the whole DiffTracker reachable and then fires into a stopped updater. The
// still-running case is the control: the timer is the only thing that re-arms a parked service.
func TestServiceUpdaterStop_CancelsPendingRetryTimers(t *testing.T) {
	stopped, stoppedDT := newRetryTimerTestUpdater(t)
	stopped.scheduleRetry("svc", 60*time.Millisecond)
	stopped.Stop()

	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, stoppedDT.serviceUpdaterTrigger,
		"a retry timer must not push a trigger token into a stopped updater")
	assert.Empty(t, len(stopped.retryTimers), "Stop must release the retained timers")

	// Control: an updater that is still running must still be re-armed by the same mechanism.
	running, runningDT := newRetryTimerTestUpdater(t)
	defer running.Stop()
	running.scheduleRetry("svc", 60*time.Millisecond)

	assert.Eventually(t, func() bool {
		return len(runningDT.serviceUpdaterTrigger) > 0
	}, 2*time.Second, 20*time.Millisecond,
		"control: the self-arm is the only driver for a parked service and must still fire")
}

// TestServiceUpdaterScheduleRetry_ReplacesPendingTimer pins that repeated backoff passes for the
// same service do not accumulate timers, each holding the DiffTracker until it fires.
func TestServiceUpdaterScheduleRetry_ReplacesPendingTimer(t *testing.T) {
	s, _ := newRetryTimerTestUpdater(t)
	defer s.Stop()

	for i := 0; i < 5; i++ {
		s.scheduleRetry("svc", time.Hour)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Len(t, s.retryTimers, 1, "re-arming the same service must replace its pending revisit, not add one")
}

// TestGuardDeleteOutboundService_NATDeleteFailureRetainsLastPodFinalizer pins the failure half of
// the drain-before-release ordering.
//
// The happy path is guarded by RemovesLastPodFinalizerOnlyAfterNATGatewayDeletion, but nothing
// covered the case where the NAT Gateway delete FAILS. The finalizer must stay attached: releasing
// it lets the pod disappear while its NAT Gateway is still live in Azure, which is exactly the
// traffic-blackhole the finalizer exists to prevent. Structurally the sweep sits inside the
// lastErr == nil branch, but without this test that guard can be deleted with no signal.
func TestGuardDeleteOutboundService_NATDeleteFailureRetainsLastPodFinalizer(t *testing.T) {
	const (
		uid     = egressCleanupTestID
		podNS   = "default"
		podName = "egress-last-pod"
		podUID  = "pod-uid-nat-fail"
	)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       podName,
			Namespace:  podNS,
			UID:        types.UID(podUID),
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
	}
	kube := fake.NewSimpleClientset(pod)

	dt, c := newOutboundCloud(t, kube)
	// The NAT Gateway teardown fails, so the Azure resource is still live afterwards.
	c.fault(cleanupCallDeleteNAT, armReplyError(http.StatusConflict, "Conflict"))
	dt.pendingPodDeletions[podNS+"/"+podName] = &PendingPodDeletion{
		Namespace:  podNS,
		Name:       podName,
		UID:        podUID,
		ServiceUID: uid,
		Addresses:  []string{"10.244.0.5"},
		IsLastPod:  true,
	}

	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

	called, success, _ := got.result()
	assert.True(t, called)
	assert.False(t, success, "a delete whose NAT Gateway teardown failed must be reported as failed")
	assert.NotNil(t, c.nat(dt.config.ResourceGroup, uid), "control: the NAT Gateway is still live")

	live, err := kube.CoreV1().Pods(podNS).Get(context.Background(), podName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.True(t, hasPodFinalizer(live),
		"the last pod must keep its cleanup finalizer while the NAT Gateway is still live in Azure")

	_, stillPending := dt.pendingPodDeletions[podNS+"/"+podName]
	assert.True(t, stillPending,
		"the last-pod entry must survive so the retried delete can release the finalizer")
}

// TestGuardDeleteOutboundService_FinalizerReleasedOnRetryAfterNATFailure pins the other direction of
// the same invariant: the pod must not be stranded Terminating forever.
//
// Holding the finalizer through a failed teardown is only correct if a later successful attempt
// actually releases it. Without this, a fix that permanently held the finalizer on any prior failure
// would look correct to the test above while blocking node drain and namespace deletion indefinitely.
func TestGuardDeleteOutboundService_FinalizerReleasedOnRetryAfterNATFailure(t *testing.T) {
	const (
		uid     = egressCleanupTestID
		podNS   = "default"
		podName = "egress-last-pod"
		podUID  = "pod-uid-nat-retry"
	)

	kube := fake.NewSimpleClientset(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       podName,
			Namespace:  podNS,
			UID:        types.UID(podUID),
			Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
		},
	})

	dt, c := newOutboundCloud(t, kube)
	// First attempt fails, second succeeds.
	c.fault(cleanupCallDeleteNAT, armReplyError(http.StatusConflict, "Conflict"))
	entry := &PendingPodDeletion{
		Namespace:  podNS,
		Name:       podName,
		UID:        podUID,
		ServiceUID: uid,
		Addresses:  []string{"10.244.0.5"},
		IsLastPod:  true,
	}
	dt.pendingPodDeletions[podNS+"/"+podName] = entry

	updater := outboundUpdater(dt, &outboundCompletion{})
	updater.deleteOutboundService(uid, "corr-1")

	live, err := kube.CoreV1().Pods(podNS).Get(context.Background(), podName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.True(t, hasPodFinalizer(live), "precondition: the failed attempt must retain the finalizer")

	// The engine clears the write-ahead mark when it records a completion; this test calls the worker directly.
	c.setMutationNoted(false)
	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr-2")

	_, success, completionErr := got.result()
	assert.True(t, success, "the retried delete must succeed: %v", completionErr)
	assert.Len(t, c.requestsOf(cleanupCallDeleteNAT), 2, "control: the NAT Gateway DELETE was retried")

	live, err = kube.CoreV1().Pods(podNS).Get(context.Background(), podName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.False(t, hasPodFinalizer(live),
		"a successful retry must release the finalizer, or the pod stays Terminating forever")
}

// TestRefreshOperationGauges_ReflectsPodDrivenAndAbortedOperations pins that the periodic recompute
// converges on the truth for operations the four event-driven refresh points never see.
//
// Outbound services are created only by pod events, so a gauge refreshed solely from
// AddService/UpdateService/DeleteService/OnServiceCreationComplete never counts the egress fleet.
// The abort path is the mirror: it untracks the operation, so a stale count has nothing left to
// bring it back down and pages forever on an idle cluster.
func TestRefreshOperationGauges_ReflectsPodDrivenAndAbortedOperations(t *testing.T) {
	RegisterMetrics()
	pendingServiceOperations.Reset()

	s, dt := newRetryTimerTestUpdater(t)
	defer s.Stop()

	// A pod-driven outbound operation: created by AddPod, which does not refresh the gauges.
	dt.AddPodWithUID("team-egress", "team/pod", "pod-uid", "10.0.0.1", "10.244.0.5")

	dt.mu.Lock()
	tracked := len(dt.pendingServiceOps)
	dt.mu.Unlock()
	assert.Equal(t, 1, tracked, "the pod must have created an outbound operation")

	s.refreshOperationGauges()
	after, err := testutil.GetGaugeMetricValue(pendingServiceOperations.WithLabelValues("not_started", "outbound"))
	assert.NoError(t, err)
	assert.Equal(t, float64(1), after, "the recompute must count a pod-driven outbound operation")

	// Aborting untracks it; the recompute must drop the count rather than leave a phantom.
	dt.mu.Lock()
	delete(dt.pendingServiceOps, "team-egress")
	dt.mu.Unlock()

	s.refreshOperationGauges()
	cleared, err := testutil.GetGaugeMetricValue(pendingServiceOperations.WithLabelValues("not_started", "outbound"))
	assert.NoError(t, err)
	assert.Zero(t, cleared, "an untracked operation must not leave a phantom count")
}

// TestCreateInboundService_StatusPatchFailureKeepsLoadBalancerLive pins that a failed Kubernetes
// status write does not un-provision a service Azure and NRP have already accepted.
//
// The status write touches only Kubernetes, so failing it leaves the PIP, the LoadBalancer and the
// ServiceGateway registration in place. Recording the registration only after that write would
// demote the operation to StateNotStarted with no LoadBalancer tracked, and isServiceReadyToSync
// then withholds the service's endpoints - leaving a public IP with no backends until the patch
// eventually succeeds. The successful write is the control.
func TestCreateInboundService_StatusPatchFailureKeepsLoadBalancerLive(t *testing.T) {
	const uid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	run := func(t *testing.T, statusPatchFails bool) *DiffTracker {
		t.Helper()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: types.UID(uid)},
			Spec: v1.ServiceSpec{
				Type:  v1.ServiceTypeLoadBalancer,
				Ports: []v1.ServicePort{{Port: 80, Protocol: v1.ProtocolTCP}},
			},
		}
		kube := fake.NewSimpleClientset(svc)
		if statusPatchFails {
			// Only the status patch fails; the finalizer add earlier in the create uses update and
			// must still succeed so the run reaches the ServiceGateway registration.
			kube.PrependReactor("patch", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("apiserver unavailable")
			})
		}

		m := newOutboundMocks(ctrl)
		mockLB := mock_loadbalancerclient.NewMockInterface(ctrl)
		m.factory.EXPECT().GetLoadBalancerClient().Return(mockLB).AnyTimes()

		// PIP and LoadBalancer create succeed, and the ServiceGateway registration succeeds, so the
		// service is genuinely live in Azure before the status write is attempted.
		m.pip.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&armnetwork.PublicIPAddress{
				Name:       ptr.To(PublicIPName(uid)),
				Properties: &armnetwork.PublicIPAddressPropertiesFormat{IPAddress: ptr.To("20.30.40.50")},
			}, nil).AnyTimes()
		mockLB.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&armnetwork.LoadBalancer{Name: ptr.To(uid)}, nil).AnyTimes()
		m.sgw.EXPECT().UpdateServices(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).AnyTimes()

		dt := newTestDiffTracker()
		dt.config = testConfig()
		dt.networkClientFactory = m.factory
		dt.kubeClient = kube

		s := NewServiceUpdater(context.Background(), dt, func(string, bool, error) {}, func(string, bool, error, *egressOutcome) {}, dt.serviceUpdaterTrigger)
		s.createInboundService(uid, makeInboundConfig(80), "corr")
		return dt
	}

	failed := run(t, true)
	failed.mu.Lock()
	liveAfterFailure := failed.NRPResources.LoadBalancers.Has(uid)
	readyAfterFailure := failed.isServiceReadyToSync(uid, true)
	failed.mu.Unlock()
	assert.True(t, liveAfterFailure,
		"the LoadBalancer is registered in NRP, so a failed Kubernetes status write must not un-track it")
	assert.True(t, readyAfterFailure,
		"its endpoints must still publish, or the public IP serves nothing")

	ok := run(t, false)
	ok.mu.Lock()
	liveAfterSuccess := ok.NRPResources.LoadBalancers.Has(uid)
	ok.mu.Unlock()
	assert.True(t, liveAfterSuccess, "control: a successful create tracks the LoadBalancer")
}

// TestDeleteOutboundService_CountsSwallowedDisassociationFailure pins that a delete sub-step which
// fails and is continued past is still counted.
//
// The deletion proceeds deliberately, because the later steps free the resources, and it is recorded
// as a successful delete. Logging that at V(4) left a sustained NRP failure invisible in both metrics
// and production logs while every deleted service kept a stale ServiceGateway association. The
// successful disassociation is the control.
func TestDeleteOutboundService_CountsSwallowedDisassociationFailure(t *testing.T) {
	const uid = "egress-uid"

	run := func(t *testing.T, disassociationFails bool) float64 {
		t.Helper()
		RegisterMetrics()
		deleteSubstepFailuresTotal.Reset()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		m := newOutboundMocks(ctrl)

		if disassociationFails {
			m.sgw.EXPECT().GetServices(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, errors.New("NRP unavailable")).AnyTimes()
			m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, notFoundError()).AnyTimes()
		} else {
			m.expectNoDisassociation()
		}
		m.sgw.EXPECT().UpdateServices(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		m.nat.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		m.pip.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		m.expectNoReservedResources()

		dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
		s := NewServiceUpdater(context.Background(), dt, func(string, bool, error) {}, func(string, bool, error, *egressOutcome) {}, dt.serviceUpdaterTrigger)
		s.deleteOutboundService(uid, "corr")

		got, err := testutil.GetCounterMetricValue(deleteSubstepFailuresTotal.WithLabelValues(deleteStepDisassociateNAT))
		assert.NoError(t, err)
		return got
	}

	assert.Equal(t, float64(1), run(t, true),
		"a swallowed disassociation failure must be counted, or a sustained NRP failure is invisible")
	assert.Zero(t, run(t, false), "control: a successful disassociation counts nothing")
}

const byoNATGatewayRG = "vnet-rg"

func byoNATConfig() Config {
	c := testConfig()
	c.VNetResourceGroup = byoNATGatewayRG
	return c
}

func byoNATGatewayARMID(name string) string {
	return "/subscriptions/sub/resourceGroups/" + byoNATGatewayRG + "/providers/Microsoft.Network/natGateways/" + name
}

func byoNATGateway(name string) *armnetwork.NatGateway {
	return &armnetwork.NatGateway{
		ID:   ptr.To(byoNATGatewayARMID(name)),
		Name: ptr.To(name),
		SKU:  &armnetwork.NatGatewaySKU{Name: ptr.To(armnetwork.NatGatewaySKUNameStandardV2)},
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			PublicIPAddresses: []*armnetwork.SubResource{{ID: ptr.To("pip-v4")}},
			ProvisioningState: ptr.To(armnetwork.ProvisioningStateSucceeded),
		},
	}
}

func drainEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func TestServiceUpdaterCreateOutboundService_BYONATGateway(t *testing.T) {
	const uid = "byo-egress"
	cfg := byoNATConfig()
	serviceGatewayID := cfg.ServiceGatewayResourceID()
	forbidden := &azcore.ResponseError{StatusCode: http.StatusForbidden}
	boom := errors.New("ARM failure")

	for _, tc := range []struct {
		name          string
		config        Config
		families      []string
		nodeRGExists  bool
		nodeRGForeign bool
		nodeRGErr     error
		byo           func() *armnetwork.NatGateway
		byoErr        error
		services      []*armnetwork.ServiceGatewayService
		servicesErr   error
		linkErr       error
		registerErr   error
		wantManaged   bool
		wantLink      bool
		wantRegister  bool
		wantSuccess   bool
		wantRecorded  bool
		wantEvent     string
	}{
		{name: "links and registers a BYO NAT Gateway", wantRecorded: true, byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			wantLink: true, wantRegister: true, wantSuccess: true, wantEvent: egressNATGatewayLinkedReason},
		{name: "already linked gateway is not written again", wantRecorded: true, byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.Properties.ServiceGateway = &armnetwork.SubResource{ID: ptr.To(strings.ToUpper(serviceGatewayID))}
			return n
		}, wantRegister: true, wantSuccess: true, wantEvent: egressNATGatewayLinkedReason},
		{name: "linked gateway that is not Succeeded is written again", wantRecorded: true, byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.Properties.ServiceGateway = &armnetwork.SubResource{ID: ptr.To(serviceGatewayID)}
			n.Properties.ProvisioningState = ptr.To(armnetwork.ProvisioningStateFailed)
			return n
		}, wantLink: true, wantRegister: true, wantSuccess: true, wantEvent: egressNATGatewayLinkedReason},
		{name: "dual-stack gateway serves IPv6", wantRecorded: true, families: []string{"IPv4", "IPv6"}, byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.Properties.PublicIPPrefixesV6 = []*armnetwork.SubResource{{ID: ptr.To("prefix-v6")}}
			return n
		}, wantLink: true, wantRegister: true, wantSuccess: true, wantEvent: egressNATGatewayLinkedReason},
		{name: "cluster resource group NAT Gateway wins", nodeRGExists: true, wantManaged: true, wantSuccess: true},
		{name: "a transient cluster resource group lookup error is retried without an event", nodeRGErr: boom},
		{name: "cluster resource group NAT Gateway not created by this controller is rejected", nodeRGForeign: true,
			wantEvent: egressNATGatewayRejectedReason},
		{name: "no BYO NAT Gateway falls back to managed", byoErr: notFoundError(), wantManaged: true, wantSuccess: true},
		{name: "unreadable VNet resource group falls back to managed", byoErr: forbidden,
			wantManaged: true, wantSuccess: true, wantEvent: egressNATGatewayUnreadableReason},
		{name: "BYO lookup failure is retried", byoErr: boom},
		{name: "wrong SKU is rejected", byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.SKU.Name = ptr.To(armnetwork.NatGatewaySKUNameStandard)
			return n
		}, wantEvent: egressNATGatewayRejectedReason},
		{name: "gateway linked to another Service Gateway is rejected", byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.Properties.ServiceGateway = &armnetwork.SubResource{ID: ptr.To("/subscriptions/sub/resourceGroups/other/providers/Microsoft.Network/serviceGateways/sgw")}
			return n
		}, wantEvent: egressNATGatewayRejectedReason},
		{name: "gateway without IPv6 addresses is rejected on a dual-stack cluster", families: []string{"IPv4", "IPv6"},
			byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) }, wantEvent: egressNATGatewayRejectedReason},
		{name: "gateway without addresses is rejected", byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.Properties.PublicIPAddresses = nil
			return n
		}, wantEvent: egressNATGatewayRejectedReason},
		{name: "default outbound gateway is rejected", byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			services: []*armnetwork.ServiceGatewayService{{Name: ptr.To("default-natgw"), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
				IsDefault: ptr.To(true), PublicNatGatewayID: ptr.To(strings.ToLower(byoNATGatewayARMID(uid)))}}},
			wantEvent: egressNATGatewayRejectedReason},
		{name: "a gateway returned without a valid resource ID is retried, nothing linked", byo: func() *armnetwork.NatGateway {
			n := byoNATGateway(uid)
			n.ID = ptr.To("not-an-arm-id")
			return n
		}},
		{name: "its own earlier registration does not block a retry", wantRecorded: true, byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			services: []*armnetwork.ServiceGatewayService{{Name: ptr.To(uid), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{PublicNatGatewayID: ptr.To(byoNATGatewayARMID(uid))}}},
			wantLink: true, wantRegister: true, wantSuccess: true, wantEvent: egressNATGatewayLinkedReason},
		{name: "a transient link failure is retried without an event", wantRecorded: true, byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			linkErr: boom, wantLink: true},
		{name: "a lock refusing the link is retried without an event", wantRecorded: true, byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			linkErr: &azcore.ResponseError{StatusCode: http.StatusConflict, ErrorCode: "ScopeLocked"}, wantLink: true},
		{name: "unreadable Service Gateway services are retried, nothing linked", byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			servicesErr: boom},
		{name: "link denied is retried with a warning", wantRecorded: true, byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			linkErr: forbidden, wantLink: true, wantEvent: egressNATGatewayRejectedReason},
		{name: "registration failure is retried", wantRecorded: true, byo: func() *armnetwork.NatGateway { return byoNATGateway(uid) },
			wantLink: true, registerErr: boom, wantRegister: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newOutboundMocks(ctrl)

			if tc.nodeRGExists {
				m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(&armnetwork.NatGateway{Name: ptr.To(uid), Tags: egressIdentityTags(uid)}, nil).Times(1)
			} else if tc.nodeRGErr != nil {
				m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, tc.nodeRGErr).Times(1)
			} else if tc.nodeRGForeign {
				m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(&armnetwork.NatGateway{Name: ptr.To(uid)}, nil).Times(1)
			} else {
				m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, notFoundError()).Times(1)
				var gateway *armnetwork.NatGateway
				if tc.byo != nil {
					gateway = tc.byo()
				}
				m.nat.EXPECT().Get(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(gateway, tc.byoErr).Times(1)
			}
			m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(tc.services, tc.servicesErr).AnyTimes()

			linkCalls := 0
			m.nat.EXPECT().CreateOrUpdate(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).
				DoAndReturn(func(_ context.Context, _, _ string, n armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
					linkCalls++
					assert.Equal(t, serviceGatewayID, derefString(n.Properties.ServiceGateway.ID))
					assert.Equal(t, "pip-v4", derefString(n.Properties.PublicIPAddresses[0].ID), "the owner's addresses must be preserved")
					return &n, tc.linkErr
				}).AnyTimes()
			registered := 0
			if tc.wantManaged {
				var existing *armnetwork.NatGateway
				if tc.nodeRGExists {
					existing = &armnetwork.NatGateway{Name: ptr.To(uid), Tags: egressIdentityTags(uid)}
				}
				m.expectManagedEgressCreate(uid, existing)
			} else {
				m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).
					DoAndReturn(func(_ context.Context, _, _ string, req armnetwork.ServiceGatewayUpdateServicesRequest) error {
						registered++
						assert.Equal(t, byoNATGatewayARMID(uid), derefString(req.ServiceRequests[0].Service.Properties.PublicNatGatewayID))
						return tc.registerErr
					}).AnyTimes()
			}

			dt := newTestDiffTracker()
			dt.config = byoNATConfig()
			dt.networkClientFactory = m.factory
			dt.kubeClient = fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod", UID: "pod-uid"}})
			rec := &captureRecorder{FakeRecorder: *record.NewFakeRecorder(10)}
			dt.eventRecorder = rec
			got := &outboundCompletion{}
			outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt, tc.families...), "corr", "ns", "pod")
			for _, object := range rec.objects {
				assert.Equal(t, types.UID("pod-uid"), object.(*v1.Pod).UID, "events must be recorded on the triggering pod")
			}

			called, success, err := got.result()
			assert.True(t, called)
			assert.Equal(t, tc.wantSuccess, success, "completion error: %v", err)
			assert.False(t, isTerminalError(err), "a BYO NAT Gateway that cannot be used yet is tried again: %v", err)
			assert.Equal(t, tc.wantSuccess, dt.NRPResources.NATGateways.Has(uid))
			// The outcome reports which NAT Gateway the identity resolved to: none when the resolution failed.
			wantMode := egressModeUnknown
			switch {
			case tc.wantManaged:
				wantMode = egressModeManaged
			case tc.byo != nil:
				wantMode = egressModeBYO
			}
			if outcome := got.lastOutcome(); assert.NotNil(t, outcome, "every outbound completion reports an outcome") {
				assert.Equal(t, wantMode, outcome.Mode)
				assert.Nil(t, outcome.Rejected, "a NAT Gateway the identity cannot use is no rejection of its configuration")
			}
			assert.Equal(t, tc.wantLink, linkCalls > 0, "link calls")
			if !tc.wantManaged {
				assert.Equal(t, tc.wantRegister, registered > 0, "registration calls")
			}
			if tc.wantRecorded {
				assert.Equal(t, byoNATGatewayARMID(uid), dt.byoNATGatewayID(uid), "a linked gateway must be recorded so a delete unlinks it")
			} else {
				assert.Empty(t, dt.byoNATGatewayID(uid), "a gateway that was never linked must not be unlinked on delete")
			}
			events := drainEvents(&rec.FakeRecorder)
			if tc.wantEvent == "" {
				assert.Empty(t, events)
			} else if assert.Len(t, events, 1) {
				assert.Contains(t, events[0], tc.wantEvent)
				wantType := v1.EventTypeWarning
				if tc.wantEvent != egressNATGatewayRejectedReason {
					wantType = v1.EventTypeNormal
				}
				assert.True(t, strings.HasPrefix(events[0], wantType), "event %q should be %s", events[0], wantType)
				if errors.Is(tc.linkErr, forbidden) {
					assert.Contains(t, events[0], "access denied", "the e2e fail-fast relies on this wording")
				}
			}
		})
	}
}

func TestServiceUpdaterCreateOutboundService_UnreadableBYOAppliesTheAgreedConfiguration(t *testing.T) {
	const uid = "byo-egress"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newOutboundMocks(ctrl)
	// The lookup in the cluster resource group, then the preflight of the managed creation.
	m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, notFoundError()).Times(2)
	m.nat.EXPECT().Get(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(nil, &azcore.ResponseError{StatusCode: http.StatusForbidden}).Times(1)
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, nil).AnyTimes()
	m.expectNoReservedResources()
	m.pip.EXPECT().CreateOrUpdate(gomock.Any(), "rg", PublicIPName(uid), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, pip armnetwork.PublicIPAddress) (*armnetwork.PublicIPAddress, error) {
			return &pip, nil
		}).Times(1)
	var idleTimeout int32
	m.nat.EXPECT().CreateOrUpdate(gomock.Any(), "rg", uid, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, nat armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
			idleTimeout = ptr.Deref(nat.Properties.IdleTimeoutInMinutes, 0)
			return &nat, nil
		}).Times(1)
	m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).Times(1)

	dt := newTestDiffTracker()
	dt.config = byoNATConfig()
	dt.networkClientFactory = m.factory
	dt.kubeClient = fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod", UID: "pod-uid"}})
	rec := &captureRecorder{FakeRecorder: *record.NewFakeRecorder(10)}
	dt.eventRecorder = rec
	config := dispatchedCreation(dt)
	config.Egress.IdleTimeoutMinutes = 7
	got := &outboundCompletion{}
	outboundUpdater(dt, got).createOutboundService(uid, config, "corr", "ns", "pod")

	_, success, err := got.result()
	assert.True(t, success, "completion error: %v", err)
	assert.Equal(t, int32(7), idleTimeout, "the managed NAT Gateway must be created with the agreed configuration, not the default")
	if outcome := got.lastOutcome(); assert.NotNil(t, outcome) && assert.NotNil(t, outcome.Applied) {
		assert.Equal(t, egressModeManaged, outcome.Mode)
		assert.Equal(t, config.Egress, *outcome.Applied)
	}
	if events := drainEvents(&rec.FakeRecorder); assert.Len(t, events, 1) {
		assert.Contains(t, events[0], egressNATGatewayUnreadableReason, "the pod must be told the VNet resource group cannot be read")
	}
}

func TestServiceUpdaterCreateOutboundService_KeepsLinkedBYONATGateway(t *testing.T) {
	const uid = "byo-linked"
	forbidden := &azcore.ResponseError{StatusCode: http.StatusForbidden}

	for _, tc := range []struct {
		name         string
		linked       func() (*armnetwork.NatGateway, error)
		wantManaged  bool
		wantSuccess  bool
		wantRecorded bool
	}{
		{name: "reuses the linked gateway without resolving again", linked: func() (*armnetwork.NatGateway, error) { return byoNATGateway(uid), nil },
			wantSuccess: true, wantRecorded: true},
		{name: "a gateway its owner deleted is forgotten and resolution starts over", linked: func() (*armnetwork.NatGateway, error) { return nil, notFoundError() },
			wantManaged: true, wantSuccess: true},
		{name: "an unusable linked gateway is rejected but stays recorded for the unlink", linked: func() (*armnetwork.NatGateway, error) {
			n := byoNATGateway(uid)
			n.Properties.PublicIPAddresses = nil
			return n, nil
		}, wantRecorded: true},
		{name: "an unreadable linked gateway is retried, never replaced by a managed one", linked: func() (*armnetwork.NatGateway, error) { return nil, forbidden },
			wantRecorded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newOutboundMocks(ctrl)
			linked, linkedErr := tc.linked()
			m.nat.EXPECT().Get(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(linked, linkedErr).Times(1)
			m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, nil).AnyTimes()
			m.nat.EXPECT().CreateOrUpdate(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(nil, nil).AnyTimes()
			if tc.wantManaged {
				m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, notFoundError()).Times(1)
				m.nat.EXPECT().Get(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(nil, notFoundError()).Times(1)
				m.expectManagedEgressCreate(uid, nil)
			}
			m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).AnyTimes()

			dt := newTestDiffTracker()
			dt.config = byoNATConfig()
			dt.networkClientFactory = m.factory
			dt.setBYONATGatewayID(uid, byoNATGatewayARMID(uid))
			got := &outboundCompletion{}
			outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt), "corr", "ns", "pod")

			_, success, err := got.result()
			assert.Equal(t, tc.wantSuccess, success, "completion error: %v", err)
			if tc.wantRecorded {
				assert.Equal(t, byoNATGatewayARMID(uid), dt.byoNATGatewayID(uid))
			} else {
				assert.Empty(t, dt.byoNATGatewayID(uid))
			}
		})
	}
}

// A NAT Gateway in the cluster resource group that this controller did not create (for example the
// default outbound service's, which may carry any name) must never be taken over, whatever the VNet layout.
func TestServiceUpdaterCreateOutboundService_RejectsUnmanagedClusterNATGateway(t *testing.T) {
	const uid = "custom-default"
	for name, vnetRG := range map[string]string{"VNet in the cluster resource group": "", "VNet in its own resource group": byoNATGatewayRG} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newOutboundMocks(ctrl) // no expectations: any Azure call fails the test

			dt := newTestDiffTracker()
			dt.config = testConfig()
			dt.config.VNetResourceGroup = vnetRG
			dt.networkClientFactory = m.factory
			dt.NRPResources.UnmanagedNATGateways = utilsets.NewString("Custom-Default")
			dt.kubeClient = fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod", UID: "pod-uid"}})
			rec := &captureRecorder{FakeRecorder: *record.NewFakeRecorder(10)}
			dt.eventRecorder = rec
			got := &outboundCompletion{}
			outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt), "corr", "ns", "pod")

			_, success, err := got.result()
			assert.False(t, success)
			assert.Error(t, err)
			assert.False(t, dt.NRPResources.NATGateways.Has(uid))
			events := drainEvents(&rec.FakeRecorder)
			if assert.Len(t, events, 1) {
				assert.Contains(t, events[0], egressNATGatewayRejectedReason)
				assert.True(t, strings.HasPrefix(events[0], v1.EventTypeWarning), events[0])
				assert.Equal(t, types.UID("pod-uid"), rec.objects[0].(*v1.Pod).UID, "the event must be on the triggering pod")
			}
		})
	}
}

func TestServiceUpdaterCreateOutboundService_NoBYOLookupWithoutSeparateVNetResourceGroup(t *testing.T) {
	const uid = "egress-same-rg"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newOutboundMocks(ctrl)
	m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(nil, notFoundError()).Times(1) // the cluster resource group only
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, nil).AnyTimes()
	m.expectManagedEgressCreate(uid, nil)

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.config.VNetResourceGroup = "RG"
	dt.networkClientFactory = m.factory
	got := &outboundCompletion{}
	outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt), "corr", "ns", "pod")

	_, success, err := got.result()
	assert.True(t, success, "%v", err)
}

func TestServiceUpdaterDeleteOutboundService_BYONATGateway(t *testing.T) {
	const uid = "byo-egress"
	cfg := byoNATConfig()
	serviceGatewayID := cfg.ServiceGatewayResourceID()
	boom := errors.New("ARM failure")

	for _, tc := range []struct {
		name           string
		linkedTo       string
		natGetErr      error
		servicesErr    error
		unregisterErr  error
		linkErr        error
		sharedWith     string
		wantNATCleared bool
		wantUnregister bool
		wantSuccess    bool
	}{
		{name: "unlinks and unregisters without deleting", linkedTo: strings.ToUpper(serviceGatewayID), wantNATCleared: true, wantUnregister: true, wantSuccess: true},
		{name: "gateway linked elsewhere is left alone", linkedTo: "/subscriptions/sub/resourceGroups/x/providers/Microsoft.Network/serviceGateways/other",
			wantUnregister: true, wantSuccess: true},
		{name: "gateway already gone", natGetErr: notFoundError(), wantUnregister: true, wantSuccess: true},
		{name: "gateway shared with the default outbound service keeps its link", linkedTo: serviceGatewayID, sharedWith: "default-natgw",
			wantUnregister: true, wantSuccess: true},
		{name: "unlink failure is retried before unregistering", natGetErr: boom},
		{name: "access denied while unlinking does not block the delete", natGetErr: &azcore.ResponseError{StatusCode: http.StatusForbidden},
			wantUnregister: true, wantSuccess: true},
		{name: "write access denied while unlinking does not block the delete", linkedTo: serviceGatewayID, linkErr: &azcore.ResponseError{StatusCode: http.StatusForbidden},
			wantNATCleared: true, wantUnregister: true, wantSuccess: true},
		{name: "a resource lock while unlinking does not block the delete", linkedTo: serviceGatewayID, linkErr: &azcore.ResponseError{StatusCode: http.StatusConflict, ErrorCode: "ScopeLocked"},
			wantNATCleared: true, wantUnregister: true, wantSuccess: true},
		{name: "a conflict while unlinking is retried", linkedTo: serviceGatewayID, linkErr: &azcore.ResponseError{StatusCode: http.StatusConflict, ErrorCode: "AnotherOperationInProgress"},
			wantNATCleared: true},
		{name: "unregister failure is retried", linkedTo: serviceGatewayID, wantNATCleared: true, unregisterErr: boom, wantUnregister: true},
		{name: "already unregistered", linkedTo: serviceGatewayID, wantNATCleared: true, unregisterErr: notFoundError(), wantUnregister: true, wantSuccess: true},
		{name: "Service Gateway gone: still unlinked and released", linkedTo: serviceGatewayID, servicesErr: notFoundError(), wantNATCleared: true,
			unregisterErr: notFoundError(), wantUnregister: true, wantSuccess: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newOutboundMocks(ctrl)

			// The identity's own service still references the gateway when the unlink starts; it must
			// not count as another user.
			services := []*armnetwork.ServiceGatewayService{
				{Name: ptr.To("11111111-1111-1111-1111-111111111111"), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{ServiceType: ptr.To(armnetwork.ServiceTypeInbound)}},
				{Name: ptr.To(uid), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
					ServiceType: ptr.To(armnetwork.ServiceTypeOutbound), PublicNatGatewayID: ptr.To(byoNATGatewayARMID(uid))}},
			}
			if tc.sharedWith != "" {
				services = append(services, &armnetwork.ServiceGatewayService{Name: ptr.To(tc.sharedWith), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
					IsDefault: ptr.To(true), PublicNatGatewayID: ptr.To(strings.ToUpper(byoNATGatewayARMID(uid)))}})
			}
			if tc.servicesErr != nil {
				services = nil
			}
			m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(services, tc.servicesErr).Times(1)
			var natGateway *armnetwork.NatGateway
			if tc.natGetErr == nil {
				natGateway = byoNATGateway(uid)
				natGateway.Properties.ServiceGateway = &armnetwork.SubResource{ID: ptr.To(tc.linkedTo)}
			}
			natReads := 1
			if tc.sharedWith != "" {
				natReads = 0
			}
			m.nat.EXPECT().Get(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(natGateway, tc.natGetErr).Times(natReads)
			cleared := false
			m.nat.EXPECT().CreateOrUpdate(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).
				DoAndReturn(func(_ context.Context, _, _ string, n armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
					cleared = true
					return &n, tc.linkErr
				}).AnyTimes()
			unregistered := false
			m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).
				DoAndReturn(func(_ context.Context, _, _ string, req armnetwork.ServiceGatewayUpdateServicesRequest) error {
					if !ptr.Deref(req.ServiceRequests[0].IsDelete, false) {
						return nil // Step 1 of the unlink: clearing the service's NAT Gateway reference.
					}
					unregistered = true
					return tc.unregisterErr
				}).AnyTimes()

			dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
			dt.config = byoNATConfig()
			dt.setBYONATGatewayID(uid, byoNATGatewayARMID(uid))
			got := &outboundCompletion{}
			outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

			_, success, err := got.result()
			assert.Equal(t, tc.wantSuccess, success, "completion error: %v", err)
			if outcome := got.lastOutcome(); assert.NotNil(t, outcome, "every outbound completion reports an outcome") {
				assert.Equal(t, egressOutcome{}, *outcome, "a BYO deletion owing no residue reports nothing")
			}
			assert.Equal(t, tc.wantNATCleared, cleared, "NAT Gateway link cleared")
			assert.Equal(t, tc.wantUnregister, unregistered, "unregistered")
			assert.Equal(t, !tc.wantSuccess, dt.NRPResources.NATGateways.Has(uid))
			if tc.wantSuccess {
				assert.Empty(t, dt.byoNATGatewayID(uid))
			} else {
				assert.Equal(t, byoNATGatewayARMID(uid), dt.byoNATGatewayID(uid), "a failed delete must keep the BYO gateway so the retry unlinks it")
			}
		})
	}
}

// A delete that unlinked the gateway but could not release the last pod's finalizer is retried; the
// retry must still take the unlink-only path, never the managed path that deletes by name.
func TestServiceUpdaterDeleteOutboundService_BYONATGatewayKeptUntilFinalizersReleased(t *testing.T) {
	const (
		uid     = "byo-finalizer"
		podNS   = "default"
		podName = "egress-last-pod"
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newOutboundMocks(ctrl)
	m.expectNoDisassociation()
	m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).Times(1)

	kube := fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: podName, Namespace: podNS, UID: "pod-uid", Finalizers: []string{ServiceGatewayPodCleanupFinalizer},
	}})
	kube.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver down")
	})
	dt := newOutboundDiffTracker(uid, m, kube)
	dt.config = byoNATConfig()
	dt.setBYONATGatewayID(uid, byoNATGatewayARMID(uid))
	dt.pendingPodDeletions[podNS+"/"+podName] = &PendingPodDeletion{
		Namespace: podNS, Name: podName, UID: "pod-uid", ServiceUID: uid, Addresses: []string{"10.244.0.5"}, IsLastPod: true,
	}
	got := &outboundCompletion{}
	outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

	_, success, _ := got.result()
	assert.False(t, success, "a failed finalizer release must be retried")
	assert.Equal(t, byoNATGatewayARMID(uid), dt.byoNATGatewayID(uid), "the retry must still take the unlink-only path")
}

// A pod labelled with the name of a NAT Gateway this identity does not own (the default outbound
// service's, which may carry any name, or any other one this controller did not create) must never
// make this controller delete that gateway or its addresses.
func TestServiceUpdaterDeleteOutboundService_KeepsNATGatewayNotOwnedByIdentity(t *testing.T) {
	const uid = "custom-default"
	usedByDefault := []*armnetwork.ServiceGatewayService{{
		Name: ptr.To("default-natgw"),
		Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
			IsDefault: ptr.To(true), PublicNatGatewayID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/natGateways/" + uid)},
	}}
	// Looks created by this controller (untagged, only its own "<name>-pip"), so only the "used by
	// another service" check keeps it.
	ownAddressesOnly := &armnetwork.NatGateway{Name: ptr.To(uid), Properties: &armnetwork.NatGatewayPropertiesFormat{PublicIPAddresses: []*armnetwork.SubResource{
		{ID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/publicIPAddresses/" + PublicIPName(uid))}}}}
	tagged := &armnetwork.NatGateway{Name: ptr.To(uid), Tags: egressIdentityTags(uid), Properties: &armnetwork.NatGatewayPropertiesFormat{}}
	// Linked to our Service Gateway (for example mid-attach by the RP), yet not ours: never written.
	foreign := &armnetwork.NatGateway{Name: ptr.To(uid), Properties: &armnetwork.NatGatewayPropertiesFormat{
		ServiceGateway: &armnetwork.SubResource{ID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/serviceGateways/sgw")}}}
	// Created by this controller before tagging, then given a prefix by its user: no longer ours.
	userModified := &armnetwork.NatGateway{Name: ptr.To(uid), Properties: &armnetwork.NatGatewayPropertiesFormat{
		PublicIPAddresses: ownAddressesOnly.Properties.PublicIPAddresses,
		PublicIPPrefixes:  []*armnetwork.SubResource{{ID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/publicIPPrefixes/user")}}}}

	for _, tc := range []struct {
		name        string
		services    []*armnetwork.ServiceGatewayService
		servicesErr error
		gateway     *armnetwork.NatGateway
		unmanaged   bool
		rejected    bool
		wantRetry   bool
	}{
		{name: "created by this controller but used by the default outbound service", services: usedByDefault, gateway: ownAddressesOnly},
		{name: "tagged as ours but used by the default outbound service", services: usedByDefault, gateway: tagged},
		{name: "tagged as ours but its users unreadable: retried, nothing deleted", gateway: tagged, servicesErr: errors.New("NRP unavailable"), wantRetry: true},
		{name: "created after start-up by someone else", gateway: foreign},
		{name: "recorded at start-up as not ours, creation rejected: nothing in Azure is touched", unmanaged: true, rejected: true},
		{name: "recorded at start-up as not ours but registered: only its registration is removed", unmanaged: true, gateway: userModified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newOutboundMocks(ctrl)
			unregistered := 0
			if !tc.rejected {
				m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(tc.gateway, nil).AnyTimes()
				m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(tc.services, tc.servicesErr).AnyTimes()
				m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).
					DoAndReturn(func(_ context.Context, _, _ string, req armnetwork.ServiceGatewayUpdateServicesRequest) error {
						unregistered++
						assert.True(t, ptr.Deref(req.ServiceRequests[0].IsDelete, false), "only the identity's own registration may be removed")
						return nil
					}).AnyTimes()
			}
			// No NAT Gateway write and no deletes: any such call fails the test.

			dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
			if tc.unmanaged {
				dt.NRPResources.UnmanagedNATGateways = utilsets.NewString(uid)
			}
			if tc.rejected {
				dt.NRPResources.NATGateways.Delete(uid)
			}
			got := &outboundCompletion{}
			outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

			_, success, err := got.result()
			assert.Equal(t, !tc.wantRetry, success, "%v", err)
			assert.Equal(t, tc.wantRetry, dt.NRPResources.NATGateways.Has(uid))
			wantUnregistered := 1
			if tc.rejected || tc.wantRetry {
				wantUnregistered = 0
			}
			assert.Equal(t, wantUnregistered, unregistered, "the identity's own registration must still be removed")
		})
	}
}

// A NAT Gateway created before tagging is recognised by its own "<name>-pip" addresses, which an
// unlink does not change, so a delete retried after the unlink (or after the Service Gateway is gone)
// still removes it; one whose users cannot be read is never deleted. The "used by another service" case is covered by
// TestServiceUpdaterDeleteOutboundService_KeepsNATGatewayNotOwnedByIdentity.
func TestServiceUpdaterDeleteOutboundService_LegacyUntaggedNATGateway(t *testing.T) {
	const uid = "legacy-egress"
	// Untagged, only its own "<name>-pip": created by this controller before tagging.
	legacy := &armnetwork.NatGateway{Name: ptr.To(uid), Etag: ptr.To(`W/"legacy"`), Properties: &armnetwork.NatGatewayPropertiesFormat{PublicIPAddresses: []*armnetwork.SubResource{
		{ID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/publicIPAddresses/" + PublicIPName(uid))}}}}

	for _, tc := range []struct {
		name        string
		services    []*armnetwork.ServiceGatewayService
		servicesErr error
		wantDeleted bool
		wantSuccess bool
	}{
		{name: "retried after the unlink: still deleted", wantDeleted: true, wantSuccess: true},
		{name: "its own service still registered: deleted", services: []*armnetwork.ServiceGatewayService{{Name: ptr.To(uid), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
			PublicNatGatewayID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/natGateways/" + uid)}}}, wantDeleted: true, wantSuccess: true},
		{name: "users unreadable: retried, nothing deleted", servicesErr: errors.New("NRP unavailable")},
		{name: "Service Gateway gone: still deleted", servicesErr: &azcore.ResponseError{StatusCode: http.StatusNotFound}, wantDeleted: true, wantSuccess: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := newOutboundMocks(ctrl)
			m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(legacy, nil).AnyTimes()
			m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(tc.services, tc.servicesErr).AnyTimes()
			m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).AnyTimes()
			if tc.wantDeleted {
				m.nat.EXPECT().Delete(gomock.Any(), "rg", uid).Return(nil).Times(1)
				// The sweep once the NAT Gateway is gone: the untagged base Public IP is the controller's by its name.
				m.pip.EXPECT().Get(gomock.Any(), "rg", PublicIPName(uid), gomock.Any()).
					Return(&armnetwork.PublicIPAddress{Name: ptr.To(PublicIPName(uid))}, nil).AnyTimes()
				m.expectNoReservedResources()
				m.pip.EXPECT().Delete(gomock.Any(), "rg", PublicIPName(uid)).Return(nil).Times(1)
			}

			dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
			got := &outboundCompletion{}
			outboundUpdater(dt, got).deleteOutboundService(uid, "corr")

			_, success, err := got.result()
			assert.Equal(t, tc.wantSuccess, success, "%v", err)
		})
	}
}

// Creating an identity on an untagged NAT Gateway that another service (the default outbound
// service) uses must be rejected rather than overwrite that gateway's addresses.
func TestServiceUpdaterCreateOutboundService_RejectsLegacyLookingNATGatewayOfAnotherService(t *testing.T) {
	const uid = "custom-default"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := newOutboundMocks(ctrl)
	m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(&armnetwork.NatGateway{Name: ptr.To(uid), Properties: &armnetwork.NatGatewayPropertiesFormat{
		PublicIPAddresses: []*armnetwork.SubResource{{ID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/publicIPAddresses/" + PublicIPName(uid))}}}}, nil)
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return([]*armnetwork.ServiceGatewayService{{Name: ptr.To("default-natgw"), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
		IsDefault: ptr.To(true), PublicNatGatewayID: ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/natGateways/" + uid)}}}, nil)
	// No writes: any PUT or registration fails the test.

	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.networkClientFactory = m.factory
	rec := record.NewFakeRecorder(10)
	dt.eventRecorder = rec
	got := &outboundCompletion{}
	outboundUpdater(dt, got).createOutboundService(uid, dispatchedCreation(dt), "corr", "ns", "pod")

	_, success, err := got.result()
	assert.False(t, success)
	assert.ErrorIs(t, err, errNATGatewayNotOwned)
	events := drainEvents(rec)
	if assert.Len(t, events, 1) {
		assert.Contains(t, events[0], egressNATGatewayRejectedReason)
	}
}

// A BYO unlink left behind because of missing access or a lock is counted like a swallowed managed
// disassociation failure, so a sustained problem is visible.
func TestDeleteBYOOutboundService_CountsSwallowedUnlinkFailure(t *testing.T) {
	const uid = "byo-metric"
	run := func(t *testing.T, readErr error) float64 {
		t.Helper()
		RegisterMetrics()
		deleteSubstepFailuresTotal.Reset()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		m := newOutboundMocks(ctrl)
		m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, nil)
		m.nat.EXPECT().Get(gomock.Any(), byoNATGatewayRG, uid, gomock.Any()).Return(nil, readErr)
		m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).AnyTimes()

		dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
		dt.config = byoNATConfig()
		dt.setBYONATGatewayID(uid, byoNATGatewayARMID(uid))
		NewServiceUpdater(context.Background(), dt, func(string, bool, error) {}, func(string, bool, error, *egressOutcome) {}, dt.serviceUpdaterTrigger).deleteOutboundService(uid, "corr")

		got, err := testutil.GetCounterMetricValue(deleteSubstepFailuresTotal.WithLabelValues(deleteStepDisassociateNAT))
		assert.NoError(t, err)
		return got
	}

	assert.Equal(t, float64(1), run(t, &azcore.ResponseError{StatusCode: http.StatusForbidden}), "a left-behind link must be counted")
	assert.Zero(t, run(t, errors.New("transient")), "control: a retried unlink failure is not a left-behind link")
	assert.Zero(t, run(t, notFoundError()), "control: an already-gone gateway counts nothing")
}

// captureRecorder keeps the objects events were recorded on, and passes the events on to the
// embedded FakeRecorder.
type captureRecorder struct {
	record.FakeRecorder
	objects []runtime.Object
}

func (r *captureRecorder) Event(object runtime.Object, eventType, reason, message string) {
	r.objects = append(r.objects, object)
	r.FakeRecorder.Event(object, eventType, reason, message)
}

// Events are recorded on the live pod, so they carry its UID and appear in `kubectl describe pod`.
func TestRecordPodEventUsesTheLivePod(t *testing.T) {
	dt := newTestDiffTracker()
	dt.kubeClient = fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod", UID: "pod-uid"}})
	rec := &captureRecorder{FakeRecorder: *record.NewFakeRecorder(10)}
	dt.eventRecorder = rec

	dt.recordPodEvent(context.Background(), "ns", "pod", v1.EventTypeNormal, egressNATGatewayLinkedReason, "linked")
	dt.recordPodEvent(context.Background(), "ns", "gone", v1.EventTypeNormal, egressNATGatewayLinkedReason, "linked")
	dt.recordPodEvent(context.Background(), "ns", "", v1.EventTypeNormal, egressNATGatewayLinkedReason, "no pod")

	if assert.Len(t, rec.objects, 2, "an event without a pod name is dropped") {
		assert.Equal(t, types.UID("pod-uid"), rec.objects[0].(*v1.Pod).UID)
		assert.Equal(t, "gone", rec.objects[1].(*v1.Pod).Name, "a pod that cannot be read still gets the event, by name")
	}
}

// With network resources in their own subscription, a cluster-resource-group NAT Gateway ID carries
// that subscription: another service using it must still be recognised, on delete and on unlink.
func TestOutboundServiceWithSeparateNetworkSubscription(t *testing.T) {
	const uid = "custom-default"
	defaultUsing := func(natGatewayID string) []*armnetwork.ServiceGatewayService {
		return []*armnetwork.ServiceGatewayService{{Name: ptr.To("default-natgw"), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
			IsDefault: ptr.To(true), PublicNatGatewayID: ptr.To(natGatewayID)}}}
	}
	netConfig := func(c Config) Config {
		c.NetworkResourceSubscriptionID = "netsub"
		return c
	}

	t.Run("managed delete keeps a gateway the default uses", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		m := newOutboundMocks(ctrl)
		m.nat.EXPECT().Get(gomock.Any(), "rg", uid, gomock.Any()).Return(&armnetwork.NatGateway{Name: ptr.To(uid), Tags: egressIdentityTags(uid)}, nil).AnyTimes()
		m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").
			Return(defaultUsing("/subscriptions/netsub/resourceGroups/rg/providers/Microsoft.Network/natGateways/"+uid), nil).AnyTimes()
		m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).AnyTimes()
		// No NAT Gateway write and no deletes.

		dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
		dt.config = netConfig(dt.config)
		got := &outboundCompletion{}
		outboundUpdater(dt, got).deleteOutboundService(uid, "corr")
		_, success, err := got.result()
		assert.True(t, success, "%v", err)
	})

	t.Run("BYO unlink keeps a gateway the default uses", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		m := newOutboundMocks(ctrl)
		byoID := "/subscriptions/netsub/resourceGroups/" + byoNATGatewayRG + "/providers/Microsoft.Network/natGateways/" + uid
		m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(defaultUsing(byoID), nil).Times(1)
		m.sgw.EXPECT().UpdateServices(gomock.Any(), "rg", "sgw", gomock.Any()).Return(nil).Times(1)
		// No NAT Gateway read or write: the gateway stays linked for the default.

		dt := newOutboundDiffTracker(uid, m, fake.NewSimpleClientset())
		dt.config = netConfig(byoNATConfig())
		dt.setBYONATGatewayID(uid, byoID)
		got := &outboundCompletion{}
		outboundUpdater(dt, got).deleteOutboundService(uid, "corr")
		_, success, err := got.result()
		assert.True(t, success, "%v", err)
	})
}

// TestProcessBatch_OutboundSnapshotIsACopy pins that the in-flight snapshot taken at dispatch does not
// share the desired outbound config. Completion compares the two to find a desired change that landed
// while the run was in flight, and records the snapshot as applied.
func TestProcessBatch_OutboundSnapshotIsACopy(t *testing.T) {
	const uid = "egress-dispatch-copy"
	dispatched := EgressConfig{IdleTimeoutMinutes: 10, IPv4: egressFamilyConfig{ManagedPublicIPs: 2}}

	cases := []struct {
		name      string
		state     ResourceState
		wantState ResourceState
	}{
		{"create", StateNotStarted, StateCreationInProgress},
		{"update", StateUpdateInProgress, StateUpdateInProgress},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			// The members agree on it, so a creation is dispatched with it too.
			castEgressVote(dt, uid, "ns/pod", &dispatched, consensusEpoch)
			op := &ServiceOperationState{
				ServiceUID: uid,
				Config:     NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: []string{"IPv4"}, Egress: dispatched}),
				State:      tc.state,
			}
			dt.pendingServiceOps[uid] = op

			// The worker is dispatched but never runs: its context is done and the semaphore never frees.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			su := newTestServiceUpdater(dt)
			su.ctx = ctx
			su.semaphore = make(chan struct{})
			su.processBatch()
			su.wg.Wait()

			dt.mu.Lock()
			defer dt.mu.Unlock()
			assert.Equal(t, tc.wantState, op.State)
			if !assert.NotNil(t, op.InFlightConfig) || !assert.NotNil(t, op.InFlightConfig.OutboundConfig) {
				return
			}

			// An in-place edit no code may make.
			op.Config.OutboundConfig.IPFamilies[0] = "IPv6"
			op.Config.OutboundConfig.Egress = egressUnknown

			assert.Equal(t, []string{"IPv4"}, op.InFlightConfig.OutboundConfig.IPFamilies)
			assert.Equal(t, dispatched, op.InFlightConfig.OutboundConfig.Egress)
		})
	}
}

// TestServiceUpdaterCompleteOutbound pins where an outbound run reports its completion: to onOutboundComplete, with
// its outcome, never to onComplete. NewServiceUpdater refuses an updater without onOutboundComplete.
func TestServiceUpdaterCompleteOutbound(t *testing.T) {
	type completion struct {
		uid     string
		success bool
		err     error
		outcome *egressOutcome
	}
	outcome := &egressOutcome{Mode: egressModeManaged, Applied: &EgressConfig{IdleTimeoutMinutes: 4}, Swept: true}
	runErr := errors.New("run failed")

	var plain, withOutcome []completion
	su := newTestServiceUpdater(newTestDiffTracker())
	su.onComplete = func(uid string, success bool, err error) {
		plain = append(plain, completion{uid, success, err, nil})
	}
	su.onOutboundComplete = func(uid string, success bool, err error, o *egressOutcome) {
		withOutcome = append(withOutcome, completion{uid, success, err, o})
	}

	su.completeOutbound("egress", false, runErr, outcome)

	assert.Empty(t, plain)
	assert.Equal(t, []completion{{"egress", false, runErr, outcome}}, withOutcome)

	dt := newTestDiffTracker()
	assert.PanicsWithValue(t, "ServiceUpdater: onOutboundComplete callback must not be nil", func() {
		NewServiceUpdater(context.Background(), dt, func(string, bool, error) {}, nil, dt.serviceUpdaterTrigger)
	})
}

// stopRetryTimers stops the revisits scheduled by an updater built as a struct literal, so a test leaves
// no timer behind.
func stopRetryTimers(su *ServiceUpdater) {
	su.mu.Lock()
	defer su.mu.Unlock()
	for uid, timer := range su.retryTimers {
		timer.Stop()
		delete(su.retryTimers, uid)
	}
}

// TestRetryGate_EgressQuietPeriod pins that no outbound run of an identity is dispatched during its quiet
// period, whatever the run (create retry, update, observe-and-clean, delete), and waiting is not a failed
// attempt: the retry budget, backoff and park are left as they are, the slot is released and a revisit is
// scheduled. An ended quiet period and an inbound operation are not held.
func TestRetryGate_EgressQuietPeriod(t *testing.T) {
	const uid = "egress-quiet-gate"

	cases := []struct {
		name             string
		inbound          bool
		state            ResourceState
		desired          EgressConfig
		retryCount       int
		retriesExhausted bool
		nextRetryIn      time.Duration // 0: no backoff recorded
		quietFor         time.Duration
		wantHeld         bool
	}{
		{name: "create retry", state: StateNotStarted, desired: consensusA, retryCount: 2, nextRetryIn: -time.Second,
			quietFor: time.Hour, wantHeld: true},
		{name: "update", state: StateUpdateInProgress, desired: consensusA, retryCount: 2, nextRetryIn: -time.Second,
			quietFor: time.Hour, wantHeld: true},
		{name: "observe-and-clean", state: StateUpdateInProgress, desired: egressUnknown, quietFor: time.Hour, wantHeld: true},
		{name: "delete", state: StateDeletionInProgress, desired: consensusA, retryCount: 1, nextRetryIn: -time.Second,
			quietFor: time.Hour, wantHeld: true},
		{name: "retry budget used up: the wait does not park it", state: StateUpdateInProgress, desired: consensusA,
			retryCount: maxServiceRetries, quietFor: time.Hour, wantHeld: true},
		{name: "parked past its cooldown: the wait does not re-arm it", state: StateUpdateInProgress, desired: consensusA,
			retryCount: maxServiceRetries, retriesExhausted: true, nextRetryIn: -time.Second, quietFor: time.Hour, wantHeld: true},
		{name: "quiet period over", state: StateNotStarted, desired: consensusA, retryCount: 2, nextRetryIn: -time.Second,
			quietFor: -time.Second},
		{name: "inbound operation", inbound: true, state: StateNotStarted, quietFor: time.Hour},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			su := newTestServiceUpdater(dt)
			defer stopRetryTimers(su)
			config := NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: tc.desired})
			if tc.inbound {
				config = NewInboundServiceConfig(uid, makeInboundConfig(80))
			}
			op := &ServiceOperationState{
				ServiceUID:       uid,
				Config:           config,
				State:            tc.state,
				RetryCount:       tc.retryCount,
				RetriesExhausted: tc.retriesExhausted,
				Egress:           egressOperationState{QuietUntil: time.Now().Add(tc.quietFor)},
			}
			if tc.nextRetryIn != 0 {
				op.NextRetryAt = time.Now().Add(tc.nextRetryIn)
			}
			dt.pendingServiceOps[uid] = op
			before := *op
			su.activeOps[uid] = true

			dt.mu.Lock()
			held := su.retryGate(uid, op)
			dt.mu.Unlock()

			assert.Equal(t, tc.wantHeld, held)
			su.mu.Lock()
			active := su.activeOps[uid]
			_, revisit := su.retryTimers[uid]
			su.mu.Unlock()
			if !tc.wantHeld {
				assert.True(t, active, "a dispatched operation keeps its slot")
				assert.False(t, revisit)
				return
			}
			assert.False(t, active, "a held operation must release its slot")
			assert.True(t, revisit, "a held operation must be revisited")
			assert.Equal(t, before.State, op.State)
			assert.Equal(t, before.RetryCount, op.RetryCount, "waiting must not count a retry")
			assert.Equal(t, before.RetriesExhausted, op.RetriesExhausted, "waiting must not park or re-arm the operation")
			assert.Equal(t, before.NextRetryAt, op.NextRetryAt, "waiting must not change the backoff")
			assert.Equal(t, before.Egress.QuietUntil, op.Egress.QuietUntil)
		})
	}
}

// TestRetryGate_EgressQuietPeriodRevisitsAtItsEnd pins that a run held by the quiet period is revisited
// when the period ends, not before, and is then dispatched.
func TestRetryGate_EgressQuietPeriodRevisitsAtItsEnd(t *testing.T) {
	const uid = "egress-quiet-revisit"
	const quietFor = 400 * time.Millisecond
	su, dt := newRetryTimerTestUpdater(t)
	defer su.Stop()
	op := &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA}),
		State:      StateUpdateInProgress,
		Egress:     egressOperationState{QuietUntil: time.Now().Add(quietFor)},
	}
	dt.pendingServiceOps[uid] = op
	su.activeOps[uid] = true

	dt.mu.Lock()
	assert.True(t, su.retryGate(uid, op), "the run must wait for the end of the quiet period")
	dt.mu.Unlock()

	assert.Never(t, func() bool { return len(dt.serviceUpdaterTrigger) > 0 }, quietFor/2, 10*time.Millisecond,
		"the revisit must not come before the quiet period ends")
	assert.Eventually(t, func() bool { return len(dt.serviceUpdaterTrigger) > 0 }, 5*time.Second, 10*time.Millisecond,
		"the revisit must come when the quiet period ends")
	assert.False(t, time.Now().Before(op.Egress.QuietUntil), "the revisit came before the quiet period ended")

	su.mu.Lock()
	su.activeOps[uid] = true
	su.mu.Unlock()
	dt.mu.Lock()
	assert.False(t, su.retryGate(uid, op), "the run must be dispatched once the quiet period is over")
	dt.mu.Unlock()
}

// TestProcessBatch_EgressQuietPeriodHoldsOutboundRuns pins that the dispatcher starts no outbound run
// during the quiet period, for every kind of run, and starts it once the period is over.
func TestProcessBatch_EgressQuietPeriodHoldsOutboundRuns(t *testing.T) {
	const uid = "egress-quiet-dispatch"

	cases := []struct {
		name      string
		state     ResourceState
		quiet     bool
		wantState ResourceState
	}{
		{"create held", StateNotStarted, true, StateNotStarted},
		{"create dispatched after the quiet period", StateNotStarted, false, StateCreationInProgress},
		{"update held", StateUpdateInProgress, true, StateUpdateInProgress},
		{"update dispatched after the quiet period", StateUpdateInProgress, false, StateUpdateInProgress},
		{"delete held", StateDeletionInProgress, true, StateDeletionInProgress},
		{"delete dispatched after the quiet period", StateDeletionInProgress, false, StateDeletionInProgress},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			quietUntil := time.Now().Add(-time.Second)
			if tc.quiet {
				quietUntil = time.Now().Add(time.Hour)
			}
			op := &ServiceOperationState{
				ServiceUID: uid,
				Config:     NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA}),
				State:      tc.state,
				RetryCount: 1,
				Egress:     egressOperationState{QuietUntil: quietUntil},
			}
			dt.pendingServiceOps[uid] = op

			// A dispatched worker never runs: its context is done and the semaphore never frees.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			su := newTestServiceUpdater(dt)
			su.ctx = ctx
			su.semaphore = make(chan struct{})
			defer stopRetryTimers(su)
			su.processBatch()
			su.wg.Wait()

			dt.mu.Lock()
			defer dt.mu.Unlock()
			assert.Equal(t, !tc.quiet, !op.OperationStartedAt.IsZero(), "dispatched")
			assert.Equal(t, tc.wantState, op.State)
			assert.Equal(t, !tc.quiet && tc.state != StateDeletionInProgress, op.InFlightConfig != nil, "in-flight snapshot")
			assert.Equal(t, 1, op.RetryCount, "neither waiting nor dispatching counts a retry")
			su.mu.Lock()
			_, revisit := su.retryTimers[uid]
			su.mu.Unlock()
			assert.Equal(t, tc.quiet, revisit, "a held run must be revisited")
		})
	}
}

// TestDeleteService_EgressQuietPeriodSurvivesTheRetryReset pins that a delete arriving during the quiet
// period gets a clean retry budget, as fresh intent does, but is not dispatched before the period ends.
func TestDeleteService_EgressQuietPeriodSurvivesTheRetryReset(t *testing.T) {
	const uid = "egress-quiet-delete"
	dt := newTestDiffTracker()
	quietUntil := time.Now().Add(time.Hour)
	applied := NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA})
	op := &ServiceOperationState{
		ServiceUID:        uid,
		Config:            applied.snapshot(),
		LastAppliedConfig: &applied,
		State:             StateUpdateInProgress,
		RetryCount:        4,
		NextRetryAt:       time.Now().Add(-time.Second),
		Egress:            egressOperationState{QuietUntil: quietUntil},
	}
	dt.pendingServiceOps[uid] = op

	dt.DeleteService(uid, false, false)

	assert.Equal(t, StateDeletionInProgress, op.State, "control: with no locations the delete is ready to dispatch")
	assert.Zero(t, op.RetryCount, "control: a delete is fresh intent with a clean retry budget")
	assert.Equal(t, quietUntil, op.Egress.QuietUntil, "the retry reset must keep the quiet period")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	su := newTestServiceUpdater(dt)
	su.ctx = ctx
	su.semaphore = make(chan struct{})
	defer stopRetryTimers(su)
	su.processBatch()
	su.wg.Wait()

	dt.mu.Lock()
	defer dt.mu.Unlock()
	assert.True(t, op.OperationStartedAt.IsZero(), "the delete must wait for the end of the quiet period")
	assert.Equal(t, StateDeletionInProgress, op.State)
}

// TestProcessBatch_RecordsTheObservationGeneration pins that the dispatcher records the observation generation
// with every outbound run it dispatches (creation, update, deletion), so the run's completion can tell an
// obligation raised while it was in flight from one it settled. A run held back records nothing.
func TestProcessBatch_RecordsTheObservationGeneration(t *testing.T) {
	const uid = "egress-dispatch-gen"

	cases := []struct {
		name           string
		state          ResourceState
		quiet          bool
		wantDispatched bool
	}{
		{"creation", StateNotStarted, false, true},
		{"update", StateUpdateInProgress, false, true},
		{"deletion", StateDeletionInProgress, false, true},
		{"held by the quiet period", StateUpdateInProgress, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := &ServiceOperationState{
				ServiceUID: uid,
				Config:     NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA}),
				State:      tc.state,
				Egress:     egressOperationState{ObservationGen: 5, DispatchedObservationGen: 2},
			}
			if tc.quiet {
				op.Egress.QuietUntil = time.Now().Add(time.Hour)
			}
			dt.pendingServiceOps[uid] = op

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			su := newTestServiceUpdater(dt)
			su.ctx = ctx
			su.semaphore = make(chan struct{})
			defer stopRetryTimers(su)
			su.processBatch()
			su.wg.Wait()

			dt.mu.Lock()
			defer dt.mu.Unlock()
			if !assert.Equal(t, tc.wantDispatched, !op.OperationStartedAt.IsZero(), "control: dispatched") {
				return
			}
			if !tc.wantDispatched {
				assert.Equal(t, uint64(2), op.Egress.DispatchedObservationGen, "a run held back records nothing")
				return
			}
			assert.Equal(t, uint64(5), op.Egress.DispatchedObservationGen)
			op.Egress.requireObservation()
			assert.Equal(t, uint64(6), op.Egress.ObservationGen)
			assert.Equal(t, uint64(5), op.Egress.DispatchedObservationGen, "an obligation raised later must not move the dispatched generation")
		})
	}
}
