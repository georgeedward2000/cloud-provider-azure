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

// Guards for transient-failure park recovery. retryGate parks a service operation after
// maxServiceRetries transient failures (RetriesExhausted). A parked op is re-armed by a fresh
// external intent (a spec-changing UpdateService or a DeleteService) or, for a stable Service, by
// an ordinary resync once the park cooldown has elapsed. Without recovery a service whose budget
// was exhausted during a sustained-but-transient ARM/NRP outage stays stranded until a CCM restart;
// the delete case is the most damaging, since a parked delete never runs, leaking the Azure load
// balancer and public IP and leaving the Service stuck Terminating.

package difftracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// gatedByRetry reports whether the dispatcher would skip the operation this pass. It mirrors the
// processBatch dispatch decision without spawning a worker goroutine.
func gatedByRetry(su *ServiceUpdater, dt *DiffTracker, uid string) bool {
	su.mu.Lock()
	su.activeOps[uid] = true
	su.mu.Unlock()
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return su.retryGate(uid, dt.pendingServiceOps[uid])
}

// TestGuardRetriesExhaustedPark_RecoveredBySpecChange verifies that a spec-changing UpdateService
// (the path EnsureLoadBalancer resync takes for a tracked service) clears a transient-exhausted
// park so the operation is dispatched again, while an unchanged-spec resync leaves the park intact
// so a normal resync does not defeat the backoff.
func TestGuardRetriesExhaustedPark_RecoveredBySpecChange(t *testing.T) {
	dt := newTestDiffTracker()
	su := newTestServiceUpdater(dt)
	uid := "svc-parked"

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:       uid,
		Config:           NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:            StateNotStarted,
		RetryCount:       maxServiceRetries,
		RetriesExhausted: true,
		NextRetryAt:      time.Now().Add(time.Hour),
	}

	// An unchanged-spec resync must NOT reset the park (it would defeat the backoff under a
	// normal resync storm).
	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(80)))
	op := dt.pendingServiceOps[uid]
	assert.Equal(t, maxServiceRetries, op.RetryCount, "unchanged-spec resync must not reset the retry budget")
	assert.True(t, op.RetriesExhausted, "unchanged-spec resync must leave the park intact")
	assert.True(t, gatedByRetry(su, dt, uid), "the still-parked op must remain gated")

	// A spec change is fresh intent: the park must be cleared and the op dispatchable again.
	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(8080)))
	op = dt.pendingServiceOps[uid]
	assert.Equal(t, 0, op.RetryCount, "a spec-changing resync must reset the retry budget")
	assert.False(t, op.RetriesExhausted, "a spec-changing resync must clear the park")
	assert.True(t, op.NextRetryAt.IsZero(), "a spec-changing resync must clear the backoff deadline")
	assert.False(t, gatedByRetry(su, dt, uid), "the recovered op must no longer be gated by retryGate")
}

// TestGuardRetriesExhaustedPark_RecoveredByDelete verifies that deleting a service whose create
// budget was exhausted gives the delete a clean retry budget instead of inheriting the parked
// create budget. Without the reset the delete is parked too: deleteInboundService never runs and
// the Azure load balancer and public IP are leaked while the Service stays Terminating.
func TestGuardRetriesExhaustedPark_RecoveredByDelete(t *testing.T) {
	dt := newTestDiffTracker()
	su := newTestServiceUpdater(dt)
	uid := "svc-parked-delete"

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:       uid,
		Config:           NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:            StateNotStarted,
		RetryCount:       maxServiceRetries,
		RetriesExhausted: true,
		NextRetryAt:      time.Now().Add(time.Hour),
	}

	dt.DeleteService(uid, true, false)

	op := dt.pendingServiceOps[uid]
	assert.Equal(t, 0, op.RetryCount, "a delete must reset the inherited create retry budget")
	assert.False(t, op.RetriesExhausted, "a delete must clear the inherited park")
	assert.True(t, op.NextRetryAt.IsZero(), "a delete must clear the inherited backoff deadline")
	assert.False(t, gatedByRetry(su, dt, uid), "the delete must be dispatchable, not gated by retryGate")
}

// TestGuardRetriesExhaustedPark_CreateRecoversAfterCooldown verifies that a create which exhausted
// its retry budget during a transient outage is re-armed by an ordinary same-spec resync once the
// park cooldown has elapsed, so a stable Service eventually gets its load balancer and public IP
// without a spec edit, a delete, or a CCM restart. While the cooldown is still pending the park is
// held instead, to avoid a per-resync retry storm.
func TestGuardRetriesExhaustedPark_CreateRecoversAfterCooldown(t *testing.T) {
	dt := newTestDiffTracker()
	su := newTestServiceUpdater(dt)
	uid := "svc-parked-create-stable-spec"

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:       uid,
		Config:           NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:            StateNotStarted,
		RetryCount:       maxServiceRetries,
		RetriesExhausted: true,
		NextRetryAt:      time.Now().Add(-time.Hour), // cooldown elapsed
	}

	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(80))) // ordinary same-spec resync

	op := dt.pendingServiceOps[uid]
	assert.False(t, op.RetriesExhausted, "a same-spec resync after the cooldown must clear the park")
	assert.Equal(t, 0, op.RetryCount, "the retry budget must be reset so the create can be dispatched")
	assert.False(t, gatedByRetry(su, dt, uid), "the recovered create must be dispatchable")
}

// TestGuardRetriesExhaustedPark_UpdateInProgressRecoversOnSpecChange verifies that an op which
// exhausted its retry budget while updating is re-armed by a genuine spec change. A parked op has no
// in-flight worker to pick up the new config, so the spec change must clear the park and re-dispatch
// rather than be silently dropped.
func TestGuardRetriesExhaustedPark_UpdateInProgressRecoversOnSpecChange(t *testing.T) {
	dt := newTestDiffTracker()
	su := newTestServiceUpdater(dt)
	uid := "svc-parked-update"

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:       uid,
		Config:           NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:            StateUpdateInProgress,
		RetryCount:       maxServiceRetries,
		RetriesExhausted: true,
		NextRetryAt:      time.Now().Add(-time.Hour),
	}

	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(8080))) // genuine spec change

	op := dt.pendingServiceOps[uid]
	assert.False(t, op.RetriesExhausted, "a spec change must clear a parked updating op")
	assert.Equal(t, 0, op.RetryCount, "a spec change must reset the parked update retry budget")
	assert.False(t, gatedByRetry(su, dt, uid), "the recovered update must be dispatchable")
}

// TestGuardRetriesExhaustedPark_UpdateRecoversAfterCooldown verifies that an op which exhausted its
// retry budget while updating is re-armed by an ordinary same-spec resync once the park cooldown has
// elapsed, so a stable Service applies the pending update instead of serving stale config until a
// CCM restart. While the cooldown is still pending the park is held, to avoid a per-resync retry storm.
func TestGuardRetriesExhaustedPark_UpdateRecoversAfterCooldown(t *testing.T) {
	dt := newTestDiffTracker()
	su := newTestServiceUpdater(dt)
	uid := "svc-parked-update-stable-spec"

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:       uid,
		Config:           NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:            StateUpdateInProgress,
		RetryCount:       maxServiceRetries,
		RetriesExhausted: true,
		NextRetryAt:      time.Now().Add(time.Hour), // still within cooldown
	}

	// While the cooldown is pending, a same-spec resync must leave the park intact.
	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(80)))
	op := dt.pendingServiceOps[uid]
	assert.True(t, op.RetriesExhausted, "a same-spec resync within the cooldown must leave the park intact")
	assert.True(t, gatedByRetry(su, dt, uid), "the still-parked update must remain gated")

	// Once the cooldown has elapsed, a same-spec resync must re-arm it.
	op.NextRetryAt = time.Now().Add(-time.Hour)
	dt.UpdateService(NewInboundServiceConfig(uid, makeInboundConfig(80)))
	op = dt.pendingServiceOps[uid]
	assert.False(t, op.RetriesExhausted, "a same-spec resync after the cooldown must clear the park")
	assert.Equal(t, 0, op.RetryCount, "the retry budget must be reset so the update can be dispatched")
	assert.False(t, gatedByRetry(su, dt, uid), "the recovered update must be dispatchable")
}

// TestGuardRetriesExhaustedPark_ParkedDeleteRecoversOnRedelete verifies that a delete which itself
// exhausted its retry budget (parked in StateDeletionInProgress) is re-armed by a repeated delete,
// so the deletion eventually runs instead of leaking the Azure load balancer and public IP and
// leaving the Service stuck Terminating.
func TestGuardRetriesExhaustedPark_ParkedDeleteRecoversOnRedelete(t *testing.T) {
	dt := newTestDiffTracker()
	su := newTestServiceUpdater(dt)
	uid := "svc-parked-delete-inflight"

	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:       uid,
		Config:           NewInboundServiceConfig(uid, makeInboundConfig(80)),
		State:            StateDeletionInProgress,
		RetryCount:       maxServiceRetries,
		RetriesExhausted: true,
		NextRetryAt:      time.Now().Add(-time.Hour),
	}

	dt.DeleteService(uid, true, false) // repeated delete while the finalizer is still present

	op := dt.pendingServiceOps[uid]
	assert.False(t, op.RetriesExhausted, "a repeated delete must clear a parked deletion")
	assert.Equal(t, 0, op.RetryCount, "a repeated delete must reset the parked delete retry budget")
	assert.False(t, gatedByRetry(su, dt, uid), "the recovered delete must be dispatchable")
}
