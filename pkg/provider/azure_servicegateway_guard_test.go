package provider

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	discovery_v1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/subnetclient/mock_subnetclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/privatelinkservice"
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

func TestPodIPWithoutServiceGateway_RejectsDualStack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.LoadBalancerBackendPoolConfigurationType = consts.LoadBalancerBackendPoolConfigurationTypePodIP
	az.LoadBalancerSKU = "standardV2"
	az.ServiceGatewayEnabled = false

	svc := getTestServiceDualStack("podip-dualstack", v1.ProtocolTCP, nil, 80)
	_, _, err := az.getExpectedLBRules(&svc, "frontend-v4", "backend-v4", "lb", consts.IPVersionIPv4)
	assert.Error(t, err, "PodIP backend pool without ServiceGateway must be rejected")
}

func TestPodIPWithoutServiceGateway_RejectsNamedTargetPort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.LoadBalancerBackendPoolConfigurationType = consts.LoadBalancerBackendPoolConfigurationTypePodIP
	az.LoadBalancerSKU = "standardV2"
	az.ServiceGatewayEnabled = false

	svc := getTestServiceWithNamedTargetPorts("podip-named-target-port", v1.ProtocolTCP, nil, false, 8080, "http")
	_, _, err := az.getExpectedLBRules(&svc, "frontend-v4", "backend-v4", "lb", consts.IPVersionIPv4)
	assert.Error(t, err, "PodIP backend pool without ServiceGateway must be rejected")
}

func TestPodIPWithoutServiceGateway_RejectsIntTargetPort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.LoadBalancerBackendPoolConfigurationType = consts.LoadBalancerBackendPoolConfigurationTypePodIP
	az.LoadBalancerSKU = "standardV2"
	az.ServiceGatewayEnabled = false

	svc := getTestServiceWithIntTargetPorts("podip-udp-target-port", v1.ProtocolUDP, nil, true, 8080, 1234)
	_, _, err := az.getExpectedLBRules(&svc, "frontend-ipv6", "backend-ipv6", "lb", consts.IPVersionIPv6)
	assert.Error(t, err, "PodIP backend pool without ServiceGateway must be rejected")
}

func TestServiceGatewayInternalAnnotation_IsRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getInternalTestService("servicegateway-internal", 80)
	kubeClient := fake.NewSimpleClientset(&svc)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	assert.Error(t, err, "internal load balancer must be rejected when ServiceGateway is enabled")
	assert.Nil(t, status, "rejected internal service must not receive an ingress status")
	assert.False(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)), "rejected internal service must not be tracked")
}

func TestAttachServiceGatewayToSubnet_UsesConfiguredVNetRGAndSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.ResourceGroup = "cluster-rg"
	az.VnetResourceGroup = "network-rg"
	az.VnetName = "byovnet"
	az.SubnetName = "custom-subnet"

	subnetClient := az.NetworkClientFactory.GetSubnetClient().(*mock_subnetclient.MockInterface)
	subnetClient.EXPECT().Get(gomock.Any(), az.VnetResourceGroup, az.VnetName, az.SubnetName, nil).Return(&armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{},
	}, nil).Times(1)
	subnetClient.EXPECT().CreateOrUpdate(gomock.Any(), az.VnetResourceGroup, az.VnetName, az.SubnetName, gomock.Any()).Return(&armnetwork.Subnet{}, nil).Times(1)

	err := az.attachServiceGatewayToSubnet(context.Background())
	assert.NoError(t, err)
}

func TestCLBWiring_CorrectedFixtureEnsureTracksService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getTestService("clb-wiring-ensure", v1.ProtocolTCP, nil, false, 80)

	kubeClient := fake.NewSimpleClientset(&svc)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	if !assert.NoError(t, err) || !assert.NotNil(t, status) {
		t.FailNow()
	}
	assert.True(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)))
}

func TestCLBWiring_CorrectedFixtureDeleteReturnsNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getTestService("clb-wiring-delete", v1.ProtocolTCP, nil, false, 80)

	kubeClient := fake.NewSimpleClientset(&svc)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	_, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	err = az.EnsureLoadBalancerDeleted(context.Background(), testClusterName, &svc)
	assert.NoError(t, err)
}

func TestCLBWiring_CorrectedFixtureEndpointSliceInformerPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getTestService("clb-wiring-eps", v1.ProtocolTCP, nil, false, 80)
	svc.Namespace = "test"
	serviceUID := svc.UID

	existingEPS := getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps-guard", "test", svc.Name, serviceUID, []string{"10.0.0.1"}, "node1")
	updatedEPS := getTestEndpointSliceWithAddressesAndServiceOwnerReference("eps-guard", "test", svc.Name, serviceUID, []string{"10.0.0.2"}, "node2")
	updatedEPS.ResourceVersion = "2"
	updatedEPS.AddressType = discovery_v1.AddressTypeIPv4

	kubeClient := fake.NewSimpleClientset(&svc, existingEPS)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	az.nodePrivateIPs = map[string]*utilsets.IgnoreCaseSet{
		"node1": utilsets.NewString("192.168.0.1"),
		"node2": utilsets.NewString("192.168.0.2"),
	}

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	az.serviceLister = informerFactory.Core().V1().Services().Lister()
	az.setUpEndpointSlicesInformer(informerFactory)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)

	_, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = kubeClient.DiscoveryV1().EndpointSlices("test").Update(context.Background(), updatedEPS, metav1.UpdateOptions{})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	time.Sleep(200 * time.Millisecond)

	assert.True(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)))
}

func TestPodIPWithoutServiceGateway_EnsureLoadBalancerRejects(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.LoadBalancerBackendPoolConfigurationType = consts.LoadBalancerBackendPoolConfigurationTypePodIP
	az.LoadBalancerSKU = "standardV2"
	az.ServiceGatewayEnabled = false

	mockLBBackendPool := az.LoadBalancerBackendPool.(*MockBackendPool)
	mockLBBackendPool.EXPECT().ReconcileBackendPools(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ *v1.Service, lb *armnetwork.LoadBalancer) (bool, bool, *armnetwork.LoadBalancer, error) {
		return false, false, lb, nil
	}).AnyTimes()
	mockLBBackendPool.EXPECT().EnsureHostsInPool(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockLBBackendPool.EXPECT().GetBackendPrivateIPs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	clusterResources, expectedInterfaces, expectedVirtualMachines := getClusterResources(az, 8, 4)
	setMockEnv(az, expectedInterfaces, expectedVirtualMachines, 5)

	svc := getTestServiceWithIntTargetPorts("service1", v1.ProtocolTCP, nil, false, 8080, 1234)
	expectedLBs := make([]*armnetwork.LoadBalancer, 0)
	setMockLBs(az, &expectedLBs, "service", 1, 1, false)

	mockPLSRepo := privatelinkservice.NewMockRepository(ctrl)
	mockPLSRepo.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&armnetwork.PrivateLinkService{ID: ptr.To(consts.PrivateLinkServiceNotExistID)}, nil).AnyTimes()
	az.plsRepo = mockPLSRepo

	// PodIP backend pools are only supported with ServiceGateway; without it EnsureLoadBalancer must
	// reject the service rather than program a load balancer that misroutes traffic to an empty pool.
	status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, clusterResources.nodes)
	assert.Error(t, err, "PodIP backend pool without ServiceGateway must be rejected")
	assert.Nil(t, status)
	assert.True(t, az.IsLBBackendPoolTypePodIP())
	assert.False(t, az.ServiceGatewayEnabled)
}

func TestServiceGatewayUnsupportedInputs_Events(t *testing.T) {
	t.Run("dual-stack service is rejected with a warning event", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := getTestServiceDualStack("sgw-dualstack", v1.ProtocolTCP, nil, 80)
		az, rec := newSGWCloudWithServiceAndRecorder(t, ctrl, svc)

		status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
		assert.Error(t, err)
		assert.Nil(t, status)
		assert.False(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)),
			"a rejected dual-stack service must not be tracked")

		select {
		case ev := <-rec.Events:
			assert.Contains(t, ev, "UnsupportedDualStack")
		case <-time.After(time.Second):
			t.Fatal("expected UnsupportedDualStack warning event")
		}
	})

	t.Run("named targetPort service is rejected with a warning event", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := getTestServiceWithNamedTargetPorts("sgw-named-port", v1.ProtocolTCP, nil, false, 8080, "http")
		az, rec := newSGWCloudWithServiceAndRecorder(t, ctrl, svc)

		status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
		assert.Error(t, err)
		assert.Nil(t, status)
		assert.False(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)),
			"a rejected named-targetPort service must not be tracked")

		select {
		case ev := <-rec.Events:
			assert.Contains(t, ev, "UnsupportedNamedTargetPort")
		case <-time.After(time.Second):
			t.Fatal("expected UnsupportedNamedTargetPort warning event")
		}
	})

	t.Run("internal service emits UnsupportedInternalLoadBalancer warning", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := getInternalTestService("sgw-internal-warning", 80)
		az, rec := newSGWCloudWithServiceAndRecorder(t, ctrl, svc)

		status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
		assert.Error(t, err)
		assert.Nil(t, status)

		select {
		case ev := <-rec.Events:
			assert.Contains(t, ev, "UnsupportedInternalLoadBalancer")
		case <-time.After(time.Second):
			t.Fatal("expected UnsupportedInternalLoadBalancer warning event")
		}
	})

	t.Run("supported single-stack numeric-port service is accepted without a warning", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		svc := getTestService("sgw-supported", v1.ProtocolTCP, nil, false, 80)
		az, rec := newSGWCloudWithServiceAndRecorder(t, ctrl, svc)

		status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
		assert.NoError(t, err)
		assert.NotNil(t, status)
		assert.True(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)))
		assertNoEvent(t, rec)
	})
}

func TestExtractInboundConfigFromService_DropsAffinityAndIdleTimeout(t *testing.T) {
	svc := getTestService("extract-inbound", v1.ProtocolTCP, map[string]string{consts.ServiceAnnotationLoadBalancerIdleTimeout: "30"}, false, 80)
	svc.Spec.SessionAffinity = v1.ServiceAffinityClientIP

	config := difftracker.ExtractInboundConfigFromService(&svc)
	if !assert.NotNil(t, config) {
		t.FailNow()
	}

	// The extractor does not currently map SessionAffinity or the idle-timeout annotation, so both stay nil.
	assert.Nil(t, config.SessionPersistence)
	assert.Nil(t, config.IdleTimeoutMinutes)
}

func TestServiceGateway_EnsureAndDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	svc := getTestService("sgw-ensure-delete", v1.ProtocolTCP, nil, false, 80)
	az, _ := newSGWCloudWithServiceAndRecorder(t, ctrl, svc)

	status, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.NotNil(t, status)
	assert.True(t, az.diffTracker.IsServiceTracked(getServiceUID(&svc)))

	err = az.EnsureLoadBalancerDeleted(context.Background(), testClusterName, &svc)
	assert.NoError(t, err)
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
