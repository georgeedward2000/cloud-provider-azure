package provider

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	discovery_v1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/loadbalancerclient/mock_loadbalancerclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/publicipaddressclient/mock_publicipaddressclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/securitygroupclient/mock_securitygroupclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/servicegatewayclient/mock_servicegatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/subnetclient/mock_subnetclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/difftracker"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

func newProviderDiffTracker(t *testing.T, az *Cloud, kubeClient kubernetes.Interface) *difftracker.DiffTracker {
	t.Helper()

	dt, err := difftracker.New(
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

func securityRuleHasDestinationPrefix(rule *armnetwork.SecurityRule, wantedPrefix string) bool {
	if rule == nil || rule.Properties == nil {
		return false
	}
	if rule.Properties.DestinationAddressPrefix != nil && *rule.Properties.DestinationAddressPrefix == wantedPrefix {
		return true
	}
	for _, p := range rule.Properties.DestinationAddressPrefixes {
		if p != nil && *p == wantedPrefix {
			return true
		}
	}
	return false
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
	assert.Error(t, err, "dual-stack PodIP services must be rejected")
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
	assert.Error(t, err, "named targetPort with PodIP backend must be rejected")
}

func TestPodIPWithoutServiceGateway_ProgramsBackendTargetPortAndProtocol(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.LoadBalancerBackendPoolConfigurationType = consts.LoadBalancerBackendPoolConfigurationTypePodIP
	az.LoadBalancerSKU = "standardV2"
	az.ServiceGatewayEnabled = false

	svc := getTestServiceWithIntTargetPorts("podip-udp-target-port", v1.ProtocolUDP, nil, true, 8080, 1234)
	probes, rules, err := az.getExpectedLBRules(&svc, "frontend-ipv6", "backend-ipv6", "lb", consts.IPVersionIPv6)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Len(t, rules, 1) {
		t.FailNow()
	}
	if !assert.NotNil(t, rules[0].Properties) {
		t.FailNow()
	}

	props := rules[0].Properties
	if !assert.NotNil(t, props.BackendPort) ||
		!assert.NotNil(t, props.Protocol) ||
		!assert.NotNil(t, props.FrontendIPConfiguration) ||
		!assert.NotNil(t, props.FrontendIPConfiguration.ID) ||
		!assert.NotNil(t, props.EnableFloatingIP) {
		t.FailNow()
	}

	assert.Equal(t, int32(1234), *props.BackendPort, "backend port must be programmed from targetPort")
	assert.Equal(t, armnetwork.TransportProtocolUDP, *props.Protocol, "UDP service must program UDP LB rule")
	assert.Equal(t, "frontend-ipv6", *props.FrontendIPConfiguration.ID, "rule must be bound to requested IP-family frontend")
	assert.False(t, *props.EnableFloatingIP, "PodIP backend should not enable floating IP")
	assert.Empty(t, probes, "PodIP backend path should not create health probes")
}

func TestServiceGatewayInternalAnnotation_DoesNotProgramPublicFrontend(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getInternalTestService("servicegateway-internal", 80)
	kubeClient := fake.NewSimpleClientset(&svc)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	mockFactory := az.NetworkClientFactory.(*mock_azclient.MockClientFactory)
	mockSGWClient := mock_servicegatewayclient.NewMockInterface(ctrl)
	mockFactory.EXPECT().GetServiceGatewayClient().Return(mockSGWClient).AnyTimes()
	mockSGWClient.EXPECT().UpdateServices(gomock.Any(), az.ResourceGroup, consts.DefaultServiceGatewayResourceName, gomock.Any()).Return(nil).Times(1)

	pipClient := az.NetworkClientFactory.GetPublicIPAddressClient().(*mock_publicipaddressclient.MockInterface)
	pipClient.EXPECT().CreateOrUpdate(gomock.Any(), az.ResourceGroup, gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, pip armnetwork.PublicIPAddress) (*armnetwork.PublicIPAddress, error) {
		if pip.Properties == nil {
			pip.Properties = &armnetwork.PublicIPAddressPropertiesFormat{}
		}
		pip.Properties.IPAddress = to.Ptr("20.0.0.10")
		return &pip, nil
	}).Times(1)

	lbClient := az.NetworkClientFactory.GetLoadBalancerClient().(*mock_loadbalancerclient.MockInterface)
	lbCreated := make(chan armnetwork.LoadBalancer, 1)
	lbClient.EXPECT().CreateOrUpdate(gomock.Any(), az.ResourceGroup, gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, lb armnetwork.LoadBalancer) (*armnetwork.LoadBalancer, error) {
		lbCreated <- lb
		return &lb, nil
	}).Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	updater := difftracker.NewServiceUpdater(ctx, az.diffTracker, az.diffTracker.OnServiceCreationComplete, az.diffTracker.GetServiceUpdaterTrigger())
	go updater.Run()
	t.Cleanup(func() {
		cancel()
		updater.Stop()
	})

	_, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	var lb armnetwork.LoadBalancer
	select {
	case lb = <-lbCreated:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for LB create")
	}

	if !assert.NotNil(t, lb.Properties) ||
		!assert.NotEmpty(t, lb.Properties.FrontendIPConfigurations) ||
		!assert.NotNil(t, lb.Properties.FrontendIPConfigurations[0].Properties) {
		t.FailNow()
	}

	assert.Nil(t, lb.Properties.FrontendIPConfigurations[0].Properties.PublicIPAddress, "internal annotation must not be silently programmed as public frontend")
	if lb.Properties.Scope != nil {
		assert.NotEqual(t, armnetwork.LoadBalancerScopePublic, *lb.Properties.Scope, "internal annotation must not produce public LB scope")
	}
}

func TestServiceGatewayEnsureLoadBalancer_ReconcilesNSGForSourceRanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	svc := getTestService("servicegateway-source-ranges", v1.ProtocolTCP, nil, false, 80)
	svc.Spec.LoadBalancerSourceRanges = []string{"10.20.0.0/24"}

	sgClient := az.NetworkClientFactory.GetSecurityGroupClient().(*mock_securitygroupclient.MockInterface)
	sgClient.EXPECT().Get(gomock.Any(), az.ResourceGroup, az.SecurityGroupName).Return(&armnetwork.SecurityGroup{
		Name: to.Ptr(az.SecurityGroupName),
		Properties: &armnetwork.SecurityGroupPropertiesFormat{
			SecurityRules: []*armnetwork.SecurityRule{},
		},
	}, nil).Times(1)
	sgClient.EXPECT().CreateOrUpdate(gomock.Any(), az.ResourceGroup, az.SecurityGroupName, gomock.Any()).Return(&armnetwork.SecurityGroup{}, nil).Times(1)

	_, err := az.EnsureLoadBalancer(context.Background(), testClusterName, &svc, nil)
	assert.NoError(t, err)
}

func TestServiceGatewayReconcileSecurityGroup_RetainsPodCIDRRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloudWithContainerLoadBalancerAndPrefixCidr(ctrl, false)
	svc := getTestService("servicegateway-podcidr-rules", v1.ProtocolTCP, nil, false, 80)
	wantedPodCIDR := az.PodCidrsIPv4[0].String()

	sgClient := az.NetworkClientFactory.GetSecurityGroupClient().(*mock_securitygroupclient.MockInterface)
	sgClient.EXPECT().Get(gomock.Any(), az.ResourceGroup, az.SecurityGroupName).Return(&armnetwork.SecurityGroup{
		Name: to.Ptr(az.SecurityGroupName),
		Properties: &armnetwork.SecurityGroupPropertiesFormat{
			SecurityRules: []*armnetwork.SecurityRule{},
		},
	}, nil).Times(1)
	sgClient.EXPECT().CreateOrUpdate(gomock.Any(), az.ResourceGroup, az.SecurityGroupName, gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, sg armnetwork.SecurityGroup) (*armnetwork.SecurityGroup, error) {
		if !assert.NotNil(t, sg.Properties) {
			t.FailNow()
		}
		found := false
		for _, rule := range sg.Properties.SecurityRules {
			if securityRuleHasDestinationPrefix(rule, wantedPodCIDR) {
				found = true
				break
			}
		}
		assert.True(t, found, "expected a retained security rule targeting PodCIDR %s", wantedPodCIDR)
		return &sg, nil
	}).Times(1)

	_, err := az.reconcileSecurityGroup(context.Background(), "test-cluster", &svc, "lb1", []string{"20.0.0.1"}, true)
	assert.NoError(t, err)
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
