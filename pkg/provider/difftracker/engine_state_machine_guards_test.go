/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// State transition tests for the DiffTracker engine.

package difftracker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// ================================================================================================
// LEGAL TRANSITIONS — UpdateService dispatch
// ================================================================================================

// UpdateService should dispatch correctly across known states.
func TestGuardStateTransitions_UpdateServiceDispatch(t *testing.T) {
	type row struct {
		name        string
		startState  ResourceState
		nrpHasLB    bool
		afterState  ResourceState
		mustTrigger bool
	}
	tests := []row{
		{
			name:        "StateNotStarted -> kept (config overwritten, no trigger)",
			startState:  StateNotStarted,
			nrpHasLB:    true,
			afterState:  StateNotStarted,
			mustTrigger: false,
		},
		{
			name:        "StateCreationInProgress -> kept (config overwritten, no trigger)",
			startState:  StateCreationInProgress,
			nrpHasLB:    true,
			afterState:  StateCreationInProgress,
			mustTrigger: false,
		},
		{
			name:        "StateCreated -> StateUpdateInProgress + trigger (config changed)",
			startState:  StateCreated,
			nrpHasLB:    true,
			afterState:  StateUpdateInProgress,
			mustTrigger: true,
		},
		{
			name:        "StateUpdateInProgress -> kept (config overwritten, no new trigger)",
			startState:  StateUpdateInProgress,
			nrpHasLB:    true,
			afterState:  StateUpdateInProgress,
			mustTrigger: false,
		},
		{
			name:        "StateDeletionPending -> kept; update ignored",
			startState:  StateDeletionPending,
			nrpHasLB:    true,
			afterState:  StateDeletionPending,
			mustTrigger: false,
		},
		{
			name:        "StateDeletionInProgress -> kept; update ignored",
			startState:  StateDeletionInProgress,
			nrpHasLB:    true,
			afterState:  StateDeletionInProgress,
			mustTrigger: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			uid := "svc-statetx"
			oldCfg := NewInboundServiceConfig(uid, makeInboundConfig(80))
			dt.pendingServiceOps[uid] = &ServiceOperationState{
				ServiceUID:        uid,
				Config:            oldCfg,
				State:             tc.startState,
				LastAppliedConfig: &oldCfg,
			}
			if tc.nrpHasLB {
				dt.NRPResources.LoadBalancers.Insert(uid)
			}

			newCfg := NewInboundServiceConfig(uid, makeInboundConfig(8080))
			dt.UpdateService(newCfg)

			op := dt.pendingServiceOps[uid]
			assert.Equal(t, tc.afterState, op.State, "post-state mismatch")
			if tc.mustTrigger {
				assert.Len(t, dt.serviceUpdaterTrigger, 1, "expected ServiceUpdater trigger")
			} else {
				assert.Len(t, dt.serviceUpdaterTrigger, 0, "did NOT expect ServiceUpdater trigger")
			}
		})
	}
}

// ================================================================================================
// LEGAL TRANSITIONS — DeleteService dispatch
// ================================================================================================

// DeleteService should dispatch correctly across known states with locations.
func TestGuardStateTransitions_DeleteServiceDispatch(t *testing.T) {
	type row struct {
		name          string
		startState    ResourceState
		afterState    ResourceState
		mustBePending bool
	}
	tests := []row{
		{"StateNotStarted -> DeletionPending", StateNotStarted, StateDeletionPending, true},
		{"StateCreationInProgress -> DeletionPending", StateCreationInProgress, StateDeletionPending, true},
		{"StateCreated -> DeletionPending", StateCreated, StateDeletionPending, true},
		{"StateUpdateInProgress -> DeletionPending (preserves InFlightConfig)", StateUpdateInProgress, StateDeletionPending, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			uid := "svc-deltx"
			inflight := NewInboundServiceConfig(uid, makeInboundConfig(80))
			dt.pendingServiceOps[uid] = &ServiceOperationState{
				ServiceUID:     uid,
				Config:         inflight,
				InFlightConfig: &inflight,
				State:          tc.startState,
			}
			dt.NRPResources.LoadBalancers.Insert(uid)
			dt.NRPResources.Locations["loc"] = NRPLocation{
				Addresses: map[string]NRPAddress{
					"10.0.0.1": {Services: utilsets.NewString(uid)},
				},
			}
			// Seed a live K8s state pod so removeServiceFromK8sStateLocked does
			// not also empty the location and short-circuit to DeletionInProgress.
			node := newNode()
			pod := newPod()
			pod.InboundIdentities.Insert(uid)
			node.Pods["10.0.0.1"] = pod
			dt.K8sResources.Nodes["loc"] = node

			dt.DeleteService(uid, true, false)

			op := dt.pendingServiceOps[uid]
			assert.Equal(t, tc.afterState, op.State)
			if tc.startState == StateUpdateInProgress {
				assert.NotNil(t, op.InFlightConfig, "DeleteService during update must preserve InFlightConfig so OnServiceCreationComplete can route to deletion")
			}
			if tc.mustBePending {
				_, pending := dt.pendingServiceDeletions[uid]
				assert.True(t, pending, "service must be queued in pendingServiceDeletions")
			}
		})
	}
}

// ================================================================================================
// ILLEGAL / OUT-OF-ORDER TRANSITIONS — must be safe no-ops
// ================================================================================================

// Unknown states in UpdateService should be handled without panic.
func TestGuardStateTransitions_UpdateService_UnknownStateNoPanic(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "svc-bad-state"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:      ResourceState(99), // illegal
	}

	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(8080)))
	op := dt.pendingServiceOps[uid]
	assert.Equal(t, ResourceState(99), op.State, "unknown state must not be mutated")
	assert.Len(t, dt.serviceUpdaterTrigger, 0, "unknown state must NOT fire trigger")
}

// Unknown states in DeleteService should be handled without panic.
func TestGuardStateTransitions_DeleteService_UnknownStateNoPanic(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "svc-bad-state-delete"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, nil),
		State:      ResourceState(99),
	}

	dt.DeleteService(uid, true, false)
	op := dt.pendingServiceOps[uid]
	assert.Equal(t, ResourceState(99), op.State, "unknown state must not be mutated")
	_, pending := dt.pendingServiceDeletions[uid]
	assert.False(t, pending, "unknown state must NOT enqueue pending deletion")
}

// Unknown states in AddPod should be a no-op.
func TestGuardStateTransitions_AddPod_UnknownStateIsNoOp(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "egress-bad-state"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewOutboundServiceConfig(uid, nil),
		State:      ResourceState(99),
	}
	dt.AddPod(uid, "ns/p", "loc1", "10.0.0.1")
	assert.Empty(t, dt.pendingPods[uid], "unknown state must not buffer pod")
	select {
	case <-dt.locationsUpdaterTrigger:
		t.Fatal("unknown state must not fire LocationsUpdater")
	default:
	}
}

// Duplicate AddService should not reset state.
func TestGuardStateTransitions_DoubleCreate_DoesNotResetState(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "svc-double-add"
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:      StateCreationInProgress,
		RetryCount: 3,
	}

	dt.AddService(NewInboundServiceConfig(uid, makeInboundConfig(8080)))

	op := dt.pendingServiceOps[uid]
	assert.Equal(t, 3, op.RetryCount, "RetryCount must not be reset by duplicate AddService")
	assert.Equal(t, StateCreationInProgress, op.State, "state must not change on duplicate AddService")
	assert.True(t, op.Config.InboundConfig.Equals(makeInboundConfig(80)),
		"original Config must not be overwritten by duplicate AddService (use UpdateService for that)")
	select {
	case <-dt.serviceUpdaterTrigger:
		t.Fatal("duplicate AddService must NOT fire trigger")
	default:
	}
}

// UpdateService during deletion should buffer recreate intent.
func TestGuardStateTransitions_CreateAfterDelete_BuffersRecreate(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "svc-recreate-during-delete"
	cfg := NewInboundServiceConfig(uid, makeInboundConfig(80))
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     cfg,
		State:      StateDeletionInProgress,
	}

	newCfg := NewInboundServiceConfig(uid, makeInboundConfig(8080))
	dt.UpdateService(newCfg)

	op := dt.pendingServiceOps[uid]
	assert.Equal(t, StateDeletionInProgress, op.State, "in-flight deletion still wins (no mid-deletion race)")
	assert.True(t, op.RecreateAfterDeletion,
		"recreate intent must be buffered, not dropped")
	assert.True(t, op.Config.InboundConfig.Equals(makeInboundConfig(8080)),
		"buffered recreate must capture the latest desired config")
}

// AddPod during deletion pending should revive the service.
func TestGuardStateTransitions_AddPodDuringDeletionPending_RevivesService(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "egress-revive"
	dt.NRPResources.NATGateways.Insert(uid)
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewOutboundServiceConfig(uid, nil),
		State:      StateDeletionPending,
	}
	dt.pendingServiceDeletions[uid] = &PendingServiceDeletion{ServiceUID: uid}

	dt.AddPod(uid, "ns/p", "10.0.0.1", "10.244.0.1")

	op := dt.pendingServiceOps[uid]
	assert.Equal(t, StateCreated, op.State, "AddPod during DeletionPending must revive the service")
	_, stillPending := dt.pendingServiceDeletions[uid]
	assert.False(t, stillPending, "pending deletion must be cancelled on revive")
}

// AddPod during deletion in progress should be buffered.
func TestGuardStateTransitions_AddPodDuringDeletionInProgress_Buffers(t *testing.T) {
	dt := newTestDiffTracker()
	uid := "egress-recreate"
	dt.NRPResources.NATGateways.Insert(uid)
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid,
		Config:     NewOutboundServiceConfig(uid, nil),
		State:      StateDeletionInProgress,
	}

	dt.AddPod(uid, "ns/p", "10.0.0.1", "10.244.0.1")
	assert.Len(t, dt.pendingPods[uid], 1, "late AddPod during DeletionInProgress must be buffered, not dropped")
	op := dt.pendingServiceOps[uid]
	assert.Equal(t, StateDeletionInProgress, op.State, "service must stay in DeletionInProgress until delete finishes")
}
