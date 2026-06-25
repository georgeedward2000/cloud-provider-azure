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
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/servicegatewayclient/mock_servicegatewayclient"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// TestLocationsUpdaterRetriesAfterSyncFailure verifies that a failed NRP location sync is
// retried automatically (with backoff) rather than left unsynced until an unrelated future
// trigger.
func TestLocationsUpdaterRetriesAfterSyncFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var calls int32
	mockFactory := mock_azclient.NewMockClientFactory(ctrl)
	mockSGW := mock_servicegatewayclient.NewMockInterface(ctrl)
	mockFactory.EXPECT().GetServiceGatewayClient().Return(mockSGW).AnyTimes()
	mockSGW.EXPECT().UpdateAddressLocations(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ armnetwork.ServiceGatewayUpdateAddressLocationsRequest) error {
			if atomic.AddInt32(&calls, 1) == 1 {
				return errors.New("service gateway unavailable")
			}
			return nil
		}).AnyTimes()

	dt := newTestDiffTracker()
	dt.networkClientFactory = mockFactory
	dt.config = testConfig()

	pod := newPod()
	pod.InboundIdentities = utilsets.NewString("svc")
	node := newNode()
	node.Pods["10.0.0.1"] = pod
	dt.K8sResources.Nodes["node-1"] = node
	dt.NRPResources.LoadBalancers.Insert("svc")
	dt.pendingServiceOps["svc"] = &ServiceOperationState{ServiceUID: "svc", State: StateCreated}

	lu := NewLocationsUpdater(context.Background(), dt)
	stopped := make(chan struct{})
	go func() {
		lu.Run()
		close(stopped)
	}()
	defer func() {
		lu.Stop()
		<-stopped // wait for the Run goroutine to fully exit before the test returns
	}()

	dt.triggerLocationsUpdater()

	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&calls) >= 2
	}, 5*time.Second, 20*time.Millisecond, "a failed location sync should be retried automatically")
}
