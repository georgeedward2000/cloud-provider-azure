package provider

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/servicegatewayclient/mock_servicegatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

func newProviderDiffTracker(t *testing.T, az *Cloud, kubeClient kubernetes.Interface) *difftracker.DiffTracker {
	t.Helper()

	dt, err := difftracker.New(
		log.Noop(),
		difftracker.K8sState{
			Services: utilsets.NewString(),
			Egresses: utilsets.NewString(),
			Nodes:    make(map[string]difftracker.Node),
		},
		difftracker.NRPState{
			LoadBalancers: utilsets.NewString(),
			NATGateways:   utilsets.NewString(),
			Locations:     make(map[string]difftracker.NRPLocation),
		},
		difftracker.Config{
			SubscriptionID:             az.SubscriptionID,
			ResourceGroup:              az.ResourceGroup,
			Location:                   az.Location,
			VNetName:                   az.VnetName,
			VNetResourceGroup:          az.VnetResourceGroup,
			ServiceGatewayResourceName: consts.DefaultServiceGatewayResourceName,
			ServiceGatewayID:           az.GetServiceGatewayID(),
		},
		az.NetworkClientFactory,
		kubeClient,
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return dt
}

func newSGWCloudWithServiceAndRecorder(t *testing.T, ctrl *gomock.Controller, svc v1.Service) (*Cloud, *record.FakeRecorder) {
	t.Helper()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	kubeClient := fake.NewSimpleClientset(&svc)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)
	rec := record.NewFakeRecorder(10)
	az.eventRecorder = rec

	return az, rec
}

func assertNoEvent(t *testing.T, rec *record.FakeRecorder) {
	t.Helper()

	select {
	case ev := <-rec.Events:
		t.Fatalf("expected no warning event, got: %s", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCreateServiceGateway_UsesConfiguredVNetRGAndSubnetDefault(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.SubscriptionID = "sub"
	az.ResourceGroup = "cluster-rg"
	az.VnetResourceGroup = "network-rg"
	az.VnetName = "byovnet"
	az.SubnetName = "" // exercise the aks-subnet default

	sgwClient := mock_servicegatewayclient.NewMockInterface(ctrl)
	az.NetworkClientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetServiceGatewayClient().Return(sgwClient).AnyTimes()

	var captured armnetwork.ServiceGateway
	sgwClient.EXPECT().CreateOrUpdate(gomock.Any(), az.ResourceGroup, "sgw", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, p armnetwork.ServiceGateway) (*armnetwork.ServiceGateway, error) {
			captured = p
			return &armnetwork.ServiceGateway{}, nil
		}).Times(1)

	err := az.createServiceGateway(context.Background(), "sgw")
	assert.NoError(t, err)

	wantVNetID := "/subscriptions/sub/resourceGroups/network-rg/providers/Microsoft.Network/virtualNetworks/byovnet"
	assert.Equal(t, wantVNetID, ptr.Deref(captured.Properties.VirtualNetwork.ID, ""),
		"VNet ID must use VnetResourceGroup, not the cluster ResourceGroup")
	assert.Equal(t, wantVNetID+"/subnets/aks-subnet", ptr.Deref(captured.Properties.RouteTargetAddress.Subnet.ID, ""),
		"subnet ID must use VnetResourceGroup and default to aks-subnet when SubnetName is unset")
}
