package difftracker

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"

	"sigs.k8s.io/cloud-provider-azure/pkg/metrics"
)

// Bounded backoff bounds for retrying a failed NRP location sync.
const (
	locationsRetryBaseDelay = 1 * time.Second
	locationsRetryMaxDelay  = 30 * time.Second
)

// LocationsUpdater syncs location and address changes to NRP Service Gateway
type LocationsUpdater struct {
	diffTracker *DiffTracker
	ctx         context.Context
	cancel      context.CancelFunc

	// failureCount is the number of consecutive failed NRP syncs, used to compute the
	// retry backoff. Accessed only from the single Run goroutine (process), so no lock.
	failureCount int

	logger logr.Logger
}

// NewLocationsUpdater creates a new LocationsUpdater
func NewLocationsUpdater(ctx context.Context, diffTracker *DiffTracker) *LocationsUpdater {
	if diffTracker == nil {
		panic("LocationsUpdater: diffTracker must not be nil")
	}
	if diffTracker.networkClientFactory == nil {
		panic("LocationsUpdater: diffTracker.networkClientFactory must not be nil")
	}
	childCtx, cancel := context.WithCancel(ctx)
	return &LocationsUpdater{
		diffTracker: diffTracker,
		ctx:         childCtx,
		cancel:      cancel,
		logger:      diffTracker.logger.WithName("LocationsUpdater"),
	}
}

// Run is the main loop that processes location update requests
func (lu *LocationsUpdater) Run() {
	lu.logger.V(2).Info("Started LocationsUpdater")

	for {
		select {
		case <-lu.ctx.Done():
			lu.logger.V(2).Info("Context cancelled, stopping LocationsUpdater")
			return

		case <-lu.diffTracker.locationsUpdaterTrigger:
			lu.logger.V(4).Info("Triggered LocationsUpdater")
			lu.process(lu.ctx)
		}
	}
}

// Stop gracefully shuts down the LocationsUpdater
func (lu *LocationsUpdater) Stop() {
	lu.logger.V(2).Info("Stopping LocationsUpdater")
	lu.cancel()
	lu.logger.V(2).Info("Stopped LocationsUpdater")
}

// process computes location/address diff and syncs to NRP
func (lu *LocationsUpdater) process(ctx context.Context) {
	mc := metrics.NewMetricContext("locations", "LocationsUpdater.process",
		lu.diffTracker.config.ResourceGroup, lu.diffTracker.config.SubscriptionID, "sync")
	isOperationSucceeded := false
	var numLocations, numAddresses int

	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded,
			"num_locations", numLocations,
			"num_addresses", numAddresses)

		// On failure, schedule a bounded-backoff retry so a transient NRP/ARM error does
		// not leave the computed diff unsynced until some unrelated future trigger. This
		// runs BEFORE the in-flight trigger counter is decremented below, so initialization
		// stays blocked (WaitForInitialSync) until a sync actually succeeds. On success,
		// reset the backoff. The retry wait is cancellable via the updater context.
		if isOperationSucceeded {
			lu.failureCount = 0
		} else {
			lu.backoffAndRetry()
		}

		// Decrement in-flight trigger counter and check initialization completion
		lu.diffTracker.mu.Lock()
		shouldCheck := atomic.LoadInt32(&lu.diffTracker.isInitializing) == 1
		lu.diffTracker.mu.Unlock()

		if shouldCheck {
			atomic.AddInt32(&lu.diffTracker.pendingUpdaterTriggers, -1)
			lu.diffTracker.checkInitializationComplete()
		}
	}()
	startTime := time.Now()

	// Get locations and addresses diff from DiffTracker
	locationData := lu.diffTracker.GetSyncLocationsAddresses()

	if len(locationData.Locations) == 0 {
		lu.logger.V(4).Info("No location changes to sync")
		// Even with no location diff, recovered pending service/pod deletions must
		// still be processed so their finalizers are not left pending.
		lu.diffTracker.CheckPendingServiceDeletions()
		lu.diffTracker.CheckPendingPodDeletions(ctx)
		if lu.initPodFinalizersStillPending() {
			// During init, retry instead of reporting success: CheckPendingPodDeletions
			// swallows transient errors, and init completion requires pendingPodDeletions==0.
			lu.logger.V(4).Info("Pod finalizer removal incomplete, retrying")
			return
		}
		isOperationSucceeded = true
		return
	}

	// Calculate metrics dimensions
	numLocations = len(locationData.Locations)
	for _, loc := range locationData.Locations {
		numAddresses += len(loc.Addresses)
	}

	// Convert to DTO format for NRP API
	locationsDTO := MapLocationDataToDTO(locationData)

	// Call NRP Service Gateway API to update locations/addresses
	err := lu.diffTracker.updateNRPSGWAddressLocations(ctx, lu.diffTracker.config.ServiceGatewayResourceName, locationsDTO)
	if err != nil {
		lu.logger.V(4).Info("Could not sync locations to NRP", "err", err, "attempt", lu.failureCount+1)
		// Leave isOperationSucceeded=false so the deferred backoffAndRetry re-triggers a
		// sync; the diff is recomputed fresh on the next pass, so no state is lost.
		return
	}

	duration := time.Since(startTime)
	lu.logger.V(2).Info("Synced locations to NRP", "locations", numLocations, "addresses", numAddresses, "duration", duration)

	// Update location and address metrics
	updateLocationsAndAddressesMetric(numLocations, numAddresses)

	// Update NRPResources to reflect the sync
	lu.diffTracker.UpdateLocationsAddresses(locationData)

	// Check pending deletions after location sync
	// Services waiting for their locations to clear can now be deleted
	lu.diffTracker.CheckPendingServiceDeletions()

	// Check pending pod deletions after location sync
	// This handles pods recovered during restart (via recoverStuckFinalizers)
	// whose addresses need to be synced out of NRP before removing their finalizers.
	// Note: During normal operation, non-last pod finalizers are removed immediately
	// in podInformerRemovePod(), not here.
	lu.diffTracker.CheckPendingPodDeletions(ctx)

	if lu.initPodFinalizersStillPending() {
		// During init, a transient finalizer-removal failure must retry rather than
		// report success, which would hang WaitForInitialSync.
		lu.logger.V(4).Info("Pod finalizer removal incomplete after sync, retrying")
		return
	}

	isOperationSucceeded = true
}

// initPodFinalizersStillPending reports whether init is in progress and recovered pod
// deletions remain. Init completion requires pendingPodDeletions==0, and
// CheckPendingPodDeletions swallows transient errors, so process() uses this to keep
// retrying during init. Returns false post-init (steady-state behavior unchanged).
func (lu *LocationsUpdater) initPodFinalizersStillPending() bool {
	dt := lu.diffTracker
	if atomic.LoadInt32(&dt.isInitializing) != 1 {
		return false
	}
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return len(dt.pendingPodDeletions) > 0
}

// backoffAndRetry waits a bounded, jittered delay and then re-triggers the LocationsUpdater
// so a failed NRP/ARM sync is retried instead of stalling until an unrelated future trigger.
// It must be called from process() BEFORE the in-flight trigger counter is decremented, so
// initialization stays blocked until a sync actually succeeds. The wait is cancellable via
// the updater context (shutdown), and a concurrently buffered trigger simply shortcuts it.
func (lu *LocationsUpdater) backoffAndRetry() {
	lu.failureCount++
	delay := locationsRetryBaseDelay << min(lu.failureCount-1, 5)
	if delay <= 0 || delay > locationsRetryMaxDelay {
		delay = locationsRetryMaxDelay
	}
	// Add up to ~20% jitter to avoid synchronized retries across controllers.
	delay += time.Duration(rand.Int63n(int64(delay)/5 + 1))

	lu.logger.V(4).Info("Scheduled NRP location sync retry", "delay", delay, "attempt", lu.failureCount)

	select {
	case <-lu.ctx.Done():
		return
	case <-time.After(delay):
	}
	lu.diffTracker.triggerLocationsUpdater()
}
