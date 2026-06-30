package difftracker

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/stretchr/testify/assert"
	"k8s.io/component-base/metrics/testutil"
)

// TestResourceStateToString tests the resourceStateToString function
func TestResourceStateToString(t *testing.T) {
	tests := []struct {
		name     string
		state    ResourceState
		expected string
	}{
		{"StateNotStarted", StateNotStarted, "not_started"},
		{"StateCreationInProgress", StateCreationInProgress, "creation_in_progress"},
		{"StateCreated", StateCreated, "created"},
		{"StateDeletionPending", StateDeletionPending, "deletion_pending"},
		{"StateDeletionInProgress", StateDeletionInProgress, "deletion_in_progress"},
		{"Unknown state", ResourceState(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := resourceStateToString(tt.state)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractAzureErrorInfo tests the extractAzureErrorInfo function
func TestExtractAzureErrorInfo(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "nil error",
			err:            nil,
			expectedStatus: 0,
			expectedCode:   "",
		},
		{
			name: "quota exceeded",
			err: &azcore.ResponseError{
				StatusCode: http.StatusForbidden,
				ErrorCode:  "QuotaExceeded",
			},
			expectedStatus: http.StatusForbidden,
			expectedCode:   "quota_exceeded",
		},
		{
			name: "throttled 429",
			err: &azcore.ResponseError{
				StatusCode: http.StatusTooManyRequests,
				ErrorCode:  "TooManyRequests",
			},
			expectedStatus: http.StatusTooManyRequests,
			expectedCode:   "throttled",
		},
		{
			name: "conflict 409",
			err: &azcore.ResponseError{
				StatusCode: http.StatusConflict,
				ErrorCode:  "Conflict",
			},
			expectedStatus: http.StatusConflict,
			expectedCode:   "conflict",
		},
		{
			name: "not found 404",
			err: &azcore.ResponseError{
				StatusCode: http.StatusNotFound,
				ErrorCode:  "ResourceNotFound",
			},
			expectedStatus: http.StatusNotFound,
			expectedCode:   "not_found",
		},
		{
			name: "internal error 500",
			err: &azcore.ResponseError{
				StatusCode: http.StatusInternalServerError,
				ErrorCode:  "InternalServerError",
			},
			expectedStatus: http.StatusInternalServerError,
			expectedCode:   "internal_error",
		},
		{
			name: "internal error 503",
			err: &azcore.ResponseError{
				StatusCode: http.StatusServiceUnavailable,
				ErrorCode:  "ServiceUnavailable",
			},
			expectedStatus: http.StatusServiceUnavailable,
			expectedCode:   "internal_error",
		},
		{
			name: "unknown Azure error",
			err: &azcore.ResponseError{
				StatusCode: http.StatusBadRequest,
				ErrorCode:  "SomeUnknownError",
			},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "unknown",
		},
		{
			name:           "context deadline exceeded",
			err:            fmt.Errorf("operation failed: %w", errors.New("context deadline exceeded")),
			expectedStatus: 0,
			expectedCode:   "timeout",
		},
		{
			name:           "wrapped Azure error",
			err:            fmt.Errorf("failed to create LB: %w", &azcore.ResponseError{StatusCode: 429, ErrorCode: "TooManyRequests"}),
			expectedStatus: 429,
			expectedCode:   "throttled",
		},
		{
			name:           "generic error",
			err:            errors.New("something went wrong"),
			expectedStatus: 0,
			expectedCode:   "unknown",
		},
		{
			name: "PublicIPCountLimitReached as quota_exceeded",
			err: &azcore.ResponseError{
				StatusCode: http.StatusBadRequest,
				ErrorCode:  "PublicIPCountLimitReached",
			},
			expectedStatus: http.StatusBadRequest,
			expectedCode:   "quota_exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, code := extractAzureErrorInfo(tt.err)
			assert.Equal(t, tt.expectedStatus, status, "HTTP status mismatch")
			assert.Equal(t, tt.expectedCode, code, "error code mismatch")
		})
	}
}

// TestServiceConfigValidate tests ServiceConfig.Validate()
func TestServiceConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		config      ServiceConfig
		shouldError bool
		errorMsg    string
	}{
		{
			name: "valid inbound config",
			config: ServiceConfig{
				UID:       "test-uid",
				IsInbound: true,
			},
			shouldError: false,
		},
		{
			name: "valid outbound config",
			config: ServiceConfig{
				UID:       "test-uid",
				IsInbound: false,
			},
			shouldError: false,
		},
		{
			name: "empty UID",
			config: ServiceConfig{
				UID:       "",
				IsInbound: true,
			},
			shouldError: true,
			errorMsg:    "service UID cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.shouldError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestUpdatePendingOperationOldestAgeMetric_IncludesUpdateInProgress verifies that the oldest
// pending-operation age gauge is emitted for the update_in_progress state, so a stuck update is
// observable to its alert.
func TestUpdatePendingOperationOldestAgeMetric_IncludesUpdateInProgress(t *testing.T) {
	RegisterMetrics()
	dt := newTestDiffTracker()
	dt.pendingServiceOps["svc"] = &ServiceOperationState{
		ServiceUID: "svc",
		Config:     NewInboundServiceConfig("svc", nil),
		State:      StateUpdateInProgress,
		CreatedAt:  time.Now().Add(-time.Hour),
	}

	updatePendingOperationOldestAgeMetric(dt)

	v, err := testutil.GetGaugeMetricValue(pendingOperationOldestAgeSeconds.WithLabelValues("update_in_progress", "inbound"))
	assert.NoError(t, err)
	assert.Greater(t, v, 3000.0, "the update_in_progress oldest-age series must be emitted")
}

// TestOnServiceCreationComplete_PreEmptDoesNotRecordOperationMetric verifies the pre-empt branch:
// when a delete arrived while a create/update was in flight, OnServiceCreationComplete routes the
// completed in-flight operation to the deletion flow and does not increment the service-operation
// counter for the pre-empted create/update (the subsequent delete records its own metric).
func TestOnServiceCreationComplete_PreEmptDoesNotRecordOperationMetric(t *testing.T) {
	RegisterMetrics()
	serviceOperationTotal.Reset()

	dt := newTestDiffTracker()
	uid := "svc-preempt-metric"
	// Pre-empt fixture: a concurrent DeleteService changed the state while a create was in flight
	// (InFlightConfig != nil). StateDeletionPending is one of the pre-empt triggers.
	cfg := NewInboundServiceConfig(uid, makeInboundConfig(80))
	inflight := cfg
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID:     uid,
		Config:         cfg,
		InFlightConfig: &inflight,
		State:          StateDeletionPending,
	}
	dt.pendingServiceDeletions[uid] = &PendingServiceDeletion{ServiceUID: uid, IsInbound: true}

	// The in-flight create completes successfully. The pre-empt branch fires.
	dt.OnServiceCreationComplete(uid, true, nil)

	// The pre-empt branch took the path (InFlightConfig is cleared and state moved off pending).
	op := dt.pendingServiceOps[uid]
	if assert.NotNil(t, op, "op must remain tracked through the pre-empt") {
		assert.Nil(t, op.InFlightConfig, "pre-empt must clear InFlightConfig")
	}

	// The "create" metric for this service-type+success must NOT have been incremented.
	got, err := testutil.GetCounterMetricValue(
		serviceOperationTotal.WithLabelValues("create", "inbound", "success", "", "false"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, got,
		"the pre-empt path does not record the create operation, so the counter stays 0")

	// The error series is also untouched: no metric is recorded on this path at all.
	gotErr, err := testutil.GetCounterMetricValue(
		serviceOperationTotal.WithLabelValues("create", "inbound", "error", "", "false"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, gotErr,
		"no metric is recorded on the pre-empt path (the error series also stays 0)")
}
