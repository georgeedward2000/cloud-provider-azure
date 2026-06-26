/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Metrics exactness tests.
//
// These tests verify two cross-cutting metric invariants:
//
//   * service_operation_total — INCREMENT EXACTLY ONCE per logical
//     operation (create / update / delete). A regression that double-records
//     (e.g. once in the dispatch, once in the completion handler) would
//     silently corrupt service-level SLOs.
//
//   * pendingServiceDeletions gauge — NEVER NEGATIVE and tracks
//     len(dt.pendingServiceDeletions) exactly. A negative value means
//     the gauge was decremented without a matching enqueue.

package difftracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/component-base/metrics/testutil"
)

// TestGuardMetrics_ServiceOperationTotal_IncrementsOnce verifies that
// recordServiceOperation increments serviceOperationTotal exactly once per
// call (and once only). A regression that double-fires the counter would
// fail this test.
func TestGuardMetrics_ServiceOperationTotal_IncrementsOnce(t *testing.T) {
	RegisterMetrics()
	serviceOperationTotal.Reset()

	recordServiceOperation("create", true, time.Now(), nil, "", false)

	got, err := testutil.GetCounterMetricValue(
		serviceOperationTotal.WithLabelValues("create", "inbound", "success", "", "false"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, got, "single recordServiceOperation call must increment counter by exactly 1")

	// Second call must produce exactly 2 (monotonic counter), not 3+ (no
	// hidden double-increment).
	recordServiceOperation("create", true, time.Now(), nil, "", false)
	got, _ = testutil.GetCounterMetricValue(
		serviceOperationTotal.WithLabelValues("create", "inbound", "success", "", "false"),
	)
	assert.Equal(t, 2.0, got, "two recordServiceOperation calls must increment counter by exactly 2")
}

// TestGuardMetrics_ServiceOperationTotal_SeparatesErrorAndSuccess verifies that
// success and error are emitted as DISTINCT label series (so error rate is
// observable) and that an error path does NOT also increment the success
// series. Regression: a future refactor that fires both labels would
// corrupt success/failure ratios.
func TestGuardMetrics_ServiceOperationTotal_SeparatesErrorAndSuccess(t *testing.T) {
	RegisterMetrics()
	serviceOperationTotal.Reset()

	recordServiceOperation("delete", false, time.Now(), assertErrorForMetrics{}, "throttled", false)
	successCount, _ := testutil.GetCounterMetricValue(
		serviceOperationTotal.WithLabelValues("delete", "outbound", "success", "throttled", "false"),
	)
	errorCount, _ := testutil.GetCounterMetricValue(
		serviceOperationTotal.WithLabelValues("delete", "outbound", "error", "throttled", "false"),
	)
	assert.Equal(t, 0.0, successCount, "error path MUST NOT emit success series")
	assert.Equal(t, 1.0, errorCount, "error path MUST emit error series exactly once")
}

// TestGuardMetrics_PendingServiceDeletionsGauge_NeverNegative verifies the
// never-negative invariant: the pendingServiceDeletions gauge is computed
// from len(dt.pendingServiceDeletions), which is always >= 0. Two calls in
// a row, after we mutate the underlying map, must both yield non-negative
// values that exactly reflect the map size.
func TestGuardMetrics_PendingServiceDeletionsGauge_NeverNegative(t *testing.T) {
	RegisterMetrics()
	dt := newTestDiffTracker()

	// Empty → gauge must be 0 (NOT negative, NOT NaN).
	updatePendingServiceDeletionsMetric(dt)
	v, err := testutil.GetGaugeMetricValue(pendingServiceDeletions)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, v, 0.0, "pendingServiceDeletions gauge must never be negative (empty map case)")
	assert.Equal(t, 0.0, v, "empty pendingServiceDeletions must report 0")

	// Add two pending deletions → gauge must be 2.
	dt.pendingServiceDeletions["a"] = &PendingServiceDeletion{ServiceUID: "a"}
	dt.pendingServiceDeletions["b"] = &PendingServiceDeletion{ServiceUID: "b"}
	updatePendingServiceDeletionsMetric(dt)
	v, _ = testutil.GetGaugeMetricValue(pendingServiceDeletions)
	assert.Equal(t, 2.0, v, "gauge must exactly equal len(pendingServiceDeletions)")
	assert.GreaterOrEqual(t, v, 0.0, "never negative")

	// Drain back to zero → gauge MUST go back to 0, never below.
	delete(dt.pendingServiceDeletions, "a")
	delete(dt.pendingServiceDeletions, "b")
	updatePendingServiceDeletionsMetric(dt)
	v, _ = testutil.GetGaugeMetricValue(pendingServiceDeletions)
	assert.Equal(t, 0.0, v, "draining map must drive gauge to 0")
	assert.GreaterOrEqual(t, v, 0.0, "never negative on drain")
}

// TestGuardMetrics_PendingServiceOperationsGauge_NeverNegative verifies that
// the per-state pendingServiceOperations gauge is non-negative and exactly
// reflects the count of pendingServiceOps in each known state.
func TestGuardMetrics_PendingServiceOperationsGauge_NeverNegative(t *testing.T) {
	RegisterMetrics()
	pendingServiceOperations.Reset()
	dt := newTestDiffTracker()

	dt.pendingServiceOps["a"] = &ServiceOperationState{
		ServiceUID: "a",
		Config:     NewInboundServiceConfig("a", nil),
		State:      StateCreationInProgress,
	}
	dt.pendingServiceOps["b"] = &ServiceOperationState{
		ServiceUID: "b",
		Config:     NewInboundServiceConfig("b", nil),
		State:      StateCreated,
	}
	dt.pendingServiceOps["c"] = &ServiceOperationState{
		ServiceUID: "c",
		Config:     NewOutboundServiceConfig("c", nil),
		State:      StateDeletionPending,
	}

	updatePendingServiceOperationsMetric(dt)

	v, _ := testutil.GetGaugeMetricValue(pendingServiceOperations.WithLabelValues("creation_in_progress", "inbound"))
	assert.Equal(t, 1.0, v)
	assert.GreaterOrEqual(t, v, 0.0)
	v, _ = testutil.GetGaugeMetricValue(pendingServiceOperations.WithLabelValues("created", "inbound"))
	assert.Equal(t, 1.0, v)
	v, _ = testutil.GetGaugeMetricValue(pendingServiceOperations.WithLabelValues("deletion_pending", "outbound"))
	assert.Equal(t, 1.0, v)
	// Untouched buckets must read 0 (not NaN, not negative).
	v, _ = testutil.GetGaugeMetricValue(pendingServiceOperations.WithLabelValues("deletion_in_progress", "outbound"))
	assert.Equal(t, 0.0, v)
	assert.GreaterOrEqual(t, v, 0.0)
}

// assertErrorForMetrics is a tiny error type used only by the error-series
// assertion above (we don't want to depend on a specific Azure-SDK error).
type assertErrorForMetrics struct{}

func (assertErrorForMetrics) Error() string { return "metrics-test-error" }
