package difftracker

import (
	"context"
	"math/rand"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
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
	}
}

// Run is the main loop that processes location update requests
func (lu *LocationsUpdater) Run() {
	klog.Infof("LocationsUpdater: Starting")

	for {
		select {
		case <-lu.ctx.Done():
			klog.Infof("LocationsUpdater: Context cancelled, stopping")
			return

		case <-lu.diffTracker.locationsUpdaterTrigger:
			klog.V(4).Infof("LocationsUpdater: Triggered by channel")
			lu.process(lu.ctx)
		}
	}
}

// Stop gracefully shuts down the LocationsUpdater
func (lu *LocationsUpdater) Stop() {
	klog.Infof("LocationsUpdater: stopping")
	lu.cancel()
	klog.Infof("LocationsUpdater: stopped")
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
	klog.V(2).Infof("LocationsUpdater: Starting location sync")

	// Get locations and addresses diff from DiffTracker
	locationData := lu.diffTracker.GetSyncLocationsAddresses()

	if len(locationData.Locations) == 0 {
		klog.V(4).Infof("LocationsUpdater: No changes to sync")
		// Even with no location diff, recovered pending service/pod deletions must
		// still be processed so their finalizers are not left pending.
		lu.diffTracker.CheckPendingServiceDeletions()
		lu.diffTracker.CheckPendingPodDeletions(ctx)
		isOperationSucceeded = true
		return
	}

	// Calculate metrics dimensions
	numLocations = len(locationData.Locations)
	for _, loc := range locationData.Locations {
		numAddresses += len(loc.Addresses)
	}

	klog.V(2).Infof("LocationsUpdater: Syncing %d locations with %d total addresses", numLocations, numAddresses)

	// Convert to DTO format for NRP API
	locationsDTO := MapLocationDataToDTO(locationData)

	// Call NRP Service Gateway API to update locations/addresses
	err := lu.diffTracker.updateNRPSGWAddressLocations(ctx, lu.diffTracker.config.ServiceGatewayResourceName, locationsDTO)
	if err != nil {
		klog.Errorf("LocationsUpdater: Failed to update locations in NRP: %v", err)
		// Leave isOperationSucceeded=false so the deferred backoffAndRetry re-triggers a
		// sync; the diff is recomputed fresh on the next pass, so no state is lost.
		return
	}

	duration := time.Since(startTime)
	klog.V(2).Infof("LocationsUpdater: Successfully synced locations to NRP in %v", duration)

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

	isOperationSucceeded = true
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

	klog.V(2).Infof("LocationsUpdater: NRP sync failed; retrying in %v (consecutive failure #%d)", delay, lu.failureCount)

	select {
	case <-lu.ctx.Done():
		return
	case <-time.After(delay):
	}
	lu.diffTracker.triggerLocationsUpdater()
}
