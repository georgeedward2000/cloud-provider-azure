package difftracker

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

func TestNewIgnoreCaseSetFromSlice_Duplicates(t *testing.T) {
	items := []string{"service1", "SERVICE1", "service1", "service2"}
	set := newIgnoreCaseSetFromSlice(items)

	// Should deduplicate case-insensitively
	assert.Equal(t, 2, set.Len())
	assert.True(t, set.Has("service1"))
	assert.True(t, set.Has("service2"))
}

func TestExtractInboundConfigFromService_MixedTargetPorts(t *testing.T) {
	service := createTestService("mixed-service", []servicePort{
		{name: "http", port: 80, targetPort: intstr.FromInt(8080), protocol: "TCP"},
		{name: "https", port: 443, targetPort: intstr.IntOrString{}, protocol: "TCP"},       // Unset
		{name: "dns", port: 53, targetPort: intstr.FromString("dns-port"), protocol: "UDP"}, // Named
	})

	config := ExtractInboundConfigFromService(service)

	assert.NotNil(t, config)
	assert.Len(t, config.FrontendPorts, 3)
	assert.Len(t, config.BackendPorts, 3)

	// HTTP: should use TargetPort
	assert.Equal(t, int32(80), config.FrontendPorts[0].Port)
	assert.Equal(t, int32(8080), config.BackendPorts[0].Port)

	// HTTPS: should fall back to Port
	assert.Equal(t, int32(443), config.FrontendPorts[1].Port)
	assert.Equal(t, int32(443), config.BackendPorts[1].Port)

	// DNS: named port should fall back to Port
	assert.Equal(t, int32(53), config.FrontendPorts[2].Port)
	assert.Equal(t, int32(53), config.BackendPorts[2].Port)
	assert.Equal(t, "UDP", config.FrontendPorts[2].Protocol)
}

func TestBuildInboundServiceResources_MismatchedPortCounts(t *testing.T) {
	// Frontend has more ports than backend (edge case)
	config := &InboundConfig{
		FrontendPorts: []PortMapping{
			{Port: 80, Protocol: "TCP"},
			{Port: 443, Protocol: "TCP"},
			{Port: 8080, Protocol: "TCP"},
		},
		BackendPorts: []PortMapping{
			{Port: 8000, Protocol: "TCP"},
			{Port: 8443, Protocol: "TCP"},
		},
	}

	dtConfig := Config{
		SubscriptionID:             "test-sub",
		ResourceGroup:              "test-rg",
		Location:                   "eastus",
		ServiceGatewayResourceName: "test-sgw",
		ServiceGatewayID:           "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Network/serviceGateways/test-sgw",
	}

	_, lb, _, err := buildInboundServiceResources("test-service", config, dtConfig)
	assert.NoError(t, err)

	// Should create 3 rules
	assert.Len(t, lb.Properties.LoadBalancingRules, 3)

	// First two should use backend ports
	assert.Equal(t, int32(80), *lb.Properties.LoadBalancingRules[0].Properties.FrontendPort)
	assert.Equal(t, int32(8000), *lb.Properties.LoadBalancingRules[0].Properties.BackendPort)

	assert.Equal(t, int32(443), *lb.Properties.LoadBalancingRules[1].Properties.FrontendPort)
	assert.Equal(t, int32(8443), *lb.Properties.LoadBalancingRules[1].Properties.BackendPort)

	// Third should fall back to frontend port
	assert.Equal(t, int32(8080), *lb.Properties.LoadBalancingRules[2].Properties.FrontendPort)
	assert.Equal(t, int32(8080), *lb.Properties.LoadBalancingRules[2].Properties.BackendPort)
}

func TestBuildInboundServiceResources_EmptyConfig(t *testing.T) {
	config := &InboundConfig{
		FrontendPorts: []PortMapping{},
		BackendPorts:  []PortMapping{},
	}

	dtConfig := Config{
		SubscriptionID:             "test-sub",
		ResourceGroup:              "test-rg",
		Location:                   "eastus",
		ServiceGatewayResourceName: "test-sgw",
		ServiceGatewayID:           "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Network/serviceGateways/test-sgw",
	}

	_, lb, _, err := buildInboundServiceResources("test-service", config, dtConfig)
	assert.NoError(t, err)

	// Should create LB with no rules (empty config is valid)
	assert.Empty(t, lb.Properties.LoadBalancingRules)
	assert.Len(t, lb.Properties.BackendAddressPools, 1)
}

func TestBuildInboundServiceResources_LongServiceUID(t *testing.T) {
	// Test with very long service UID
	longUID := "very-long-service-uid-that-exceeds-normal-length-abcdef123456789012345678901234567890"

	config := &InboundConfig{
		FrontendPorts: []PortMapping{{Port: 80, Protocol: "TCP"}},
		BackendPorts:  []PortMapping{{Port: 8080, Protocol: "TCP"}},
	}

	dtConfig := Config{
		SubscriptionID:             "test-sub",
		ResourceGroup:              "test-rg",
		Location:                   "eastus",
		ServiceGatewayResourceName: "test-sgw",
		ServiceGatewayID:           "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Network/serviceGateways/test-sgw",
	}

	pip, lb, _, err := buildInboundServiceResources(longUID, config, dtConfig)
	assert.NoError(t, err)

	// Should handle long UIDs without truncation
	assert.Equal(t, longUID, *lb.Name)
	assert.Equal(t, longUID+"-pip", *pip.Name)
	assert.Equal(t, longUID, *lb.Properties.BackendAddressPools[0].Name)
}

func TestBuildOutboundServiceResources_NilConfig(t *testing.T) {
	// OutboundConfig is currently not used but test nil handling
	dtConfig := Config{
		SubscriptionID:             "test-sub",
		ResourceGroup:              "test-rg",
		Location:                   "westus",
		ServiceGatewayResourceName: "test-sgw",
		ServiceGatewayID:           "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Network/serviceGateways/test-sgw",
	}

	pip, natGw, servicesDTO := buildOutboundServiceResources("egress-123", nil, dtConfig)

	// Should create resources even with nil config
	assert.NotNil(t, pip)
	assert.NotNil(t, natGw)
	assert.NotNil(t, servicesDTO)
	assert.Equal(t, "egress-123-pip", *pip.Name)
	assert.Equal(t, "egress-123", *natGw.Name)
}

func TestBuildInboundServiceResources_MultipleConfigs(t *testing.T) {
	// Test that multiple invocations produce independent resources
	config1 := &InboundConfig{
		FrontendPorts: []PortMapping{{Port: 80, Protocol: "TCP"}},
		BackendPorts:  []PortMapping{{Port: 8080, Protocol: "TCP"}},
	}

	config2 := &InboundConfig{
		FrontendPorts: []PortMapping{{Port: 443, Protocol: "TCP"}},
		BackendPorts:  []PortMapping{{Port: 8443, Protocol: "TCP"}},
	}

	dtConfig := Config{
		SubscriptionID:             "test-sub",
		ResourceGroup:              "test-rg",
		Location:                   "eastus",
		ServiceGatewayResourceName: "test-sgw",
		ServiceGatewayID:           "/subscriptions/test-sub/resourceGroups/test-rg/providers/Microsoft.Network/serviceGateways/test-sgw",
	}

	pip1, lb1, _, err := buildInboundServiceResources("service-1", config1, dtConfig)
	assert.NoError(t, err)
	pip2, lb2, _, err := buildInboundServiceResources("service-2", config2, dtConfig)
	assert.NoError(t, err)

	// Should produce different resources
	assert.NotEqual(t, *pip1.Name, *pip2.Name)
	assert.NotEqual(t, *lb1.Name, *lb2.Name)

	// Each should have correct config
	assert.Equal(t, int32(80), *lb1.Properties.LoadBalancingRules[0].Properties.FrontendPort)
	assert.Equal(t, int32(443), *lb2.Properties.LoadBalancingRules[0].Properties.FrontendPort)
}

func TestConfigValidation_AllFieldsRequired(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config",
			config: Config{
				SubscriptionID:             "sub-123",
				ResourceGroup:              "rg-456",
				Location:                   "eastus",
				VNetName:                   "test-vnet",
				ServiceGatewayResourceName: "sgw-789",
				ServiceGatewayID:           "/subscriptions/sub-123/resourceGroups/rg-456/providers/Microsoft.Network/serviceGateways/sgw-789",
			},
			expectError: false,
		},
		{
			name: "missing subscription",
			config: Config{
				ResourceGroup:              "rg-456",
				Location:                   "eastus",
				ServiceGatewayResourceName: "sgw-789",
				ServiceGatewayID:           "/subscriptions/sub-123/resourceGroups/rg-456/providers/Microsoft.Network/serviceGateways/sgw-789",
			},
			expectError: true,
		},
		{
			name: "missing resource group",
			config: Config{
				SubscriptionID:             "sub-123",
				Location:                   "eastus",
				ServiceGatewayResourceName: "sgw-789",
				ServiceGatewayID:           "/subscriptions/sub-123/resourceGroups/rg-456/providers/Microsoft.Network/serviceGateways/sgw-789",
			},
			expectError: true,
		},
		{
			name: "missing location",
			config: Config{
				SubscriptionID:             "sub-123",
				ResourceGroup:              "rg-456",
				ServiceGatewayResourceName: "sgw-789",
				ServiceGatewayID:           "/subscriptions/sub-123/resourceGroups/rg-456/providers/Microsoft.Network/serviceGateways/sgw-789",
			},
			expectError: true,
		},
		{
			name: "missing ServiceGatewayResourceName",
			config: Config{
				SubscriptionID:   "sub-123",
				ResourceGroup:    "rg-456",
				Location:         "eastus",
				ServiceGatewayID: "/subscriptions/sub-123/resourceGroups/rg-456/providers/Microsoft.Network/serviceGateways/sgw-789",
			},
			expectError: true,
		},
		{
			name: "missing ServiceGatewayID",
			config: Config{
				SubscriptionID:             "sub-123",
				ResourceGroup:              "rg-456",
				Location:                   "eastus",
				ServiceGatewayResourceName: "sgw-789",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewIgnoreCaseSetFromSlice_PreservesOrder(t *testing.T) {
	// Order shouldn't matter for set membership
	items1 := []string{"a", "b", "c"}
	items2 := []string{"c", "b", "a"}

	set1 := newIgnoreCaseSetFromSlice(items1)
	set2 := newIgnoreCaseSetFromSlice(items2)

	// Both should contain same elements
	assert.Equal(t, set1.Len(), set2.Len())
	for _, item := range items1 {
		assert.True(t, set1.Has(item))
		assert.True(t, set2.Has(item))
	}
}

// Helper types and functions for tests

type servicePort struct {
	name       string
	port       int32
	targetPort intstr.IntOrString
	protocol   string
}

func createTestService(name string, ports []servicePort) *v1.Service {
	v1Ports := make([]v1.ServicePort, len(ports))
	for i, p := range ports {
		protocol := v1.ProtocolTCP
		if p.protocol == "UDP" {
			protocol = v1.ProtocolUDP
		}
		v1Ports[i] = v1.ServicePort{
			Name:       p.name,
			Port:       p.port,
			TargetPort: p.targetPort,
			Protocol:   protocol,
		}
	}

	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: v1.ServiceSpec{
			Ports: v1Ports,
		},
	}
}

// newK8sStateForSeeders returns an empty K8sState suitable for the processK8s* seeders.
func newK8sStateForSeeders(trackedServices ...string) K8sState {
	return K8sState{
		Services: utilsets.NewString(trackedServices...),
		Egresses: utilsets.NewString(),
		Nodes:    make(map[string]Node),
	}
}

// newServiceOwnedEndpointSlice builds an EndpointSlice owned by the given Service UID;
// extractServiceUIDFromEndpointSlice resolves ownership via the Service OwnerReference.
func newServiceOwnedEndpointSlice(name, namespace, svcUID string, addrType discoveryv1.AddressType, endpoints []discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Service", Name: name, UID: types.UID(svcUID)},
			},
		},
		AddressType: addrType,
		Endpoints:   endpoints,
	}
}

// newEgressPod builds an egress-labeled pod for the processK8sEgresses seeder.
func newEgressPod(name, namespace, egressVal, nodeName, podIP string, phase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{consts.PodLabelServiceEgressGateway: egressVal},
		},
		Spec:   v1.PodSpec{NodeName: nodeName},
		Status: v1.PodStatus{Phase: phase, PodIP: podIP},
	}
}

// podIPTracked reports whether any node in K8s state tracks a pod at the given IP.
func podIPTracked(k8s *K8sState, podIP string) bool {
	for _, node := range k8s.Nodes {
		if _, ok := node.Pods[podIP]; ok {
			return true
		}
	}
	return false
}

// TestProcessK8sEndpoints_SkipsNotReadyEndpoints verifies the cold-start seeder excludes
// EndpointSlice endpoints whose Conditions.Ready==false (a nil Ready is treated as ready),
// matching the runtime informer filter in azure_local_services.go. Without this, a CCM restart
// would import not-ready pod IPs as LoadBalancer backends that the runtime diff can never remove.
func TestProcessK8sEndpoints_SkipsNotReadyEndpoints(t *testing.T) {
	const (
		svcUID     = "svc-unready"
		nodeName   = "node-1"
		nodeIP     = "10.0.0.5"
		readyIP    = "10.244.0.10"
		notReadyIP = "10.244.0.11"
	)

	k8s := newK8sStateForSeeders(svcUID)
	nodeNameToIPMap := map[string]string{nodeName: nodeIP}

	eps := newServiceOwnedEndpointSlice("eps-1", "default", svcUID, discoveryv1.AddressTypeIPv4, []discoveryv1.Endpoint{
		{
			Addresses:  []string{readyIP},
			NodeName:   ptr.To(nodeName),
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
		},
		{
			Addresses:  []string{notReadyIP},
			NodeName:   ptr.To(nodeName),
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)},
		},
	})
	kube := fake.NewSimpleClientset(eps)

	_, err := processK8sEndpoints(context.Background(), kube, &k8s, nodeNameToIPMap)
	assert.NoError(t, err)

	assert.True(t, podIPTracked(&k8s, readyIP),
		"a ready endpoint must be imported into K8s state at init")
	assert.False(t, podIPTracked(&k8s, notReadyIP),
		"an endpoint with Conditions.Ready==false must not be imported into K8s state at init")
}

// TestInitOutboundRefCount_NotNegativeOnServiceUIDEgressLabelCollision verifies the outbound
// ref-counter is seeded solely from real egress pods (in New()) and never goes negative when an
// inbound LoadBalancer service UID happens to equal a pod egress label. A negative seed would
// trip DeletePod's `counter <= 0` guard and strand the pod (and its NAT gateway).
func TestInitOutboundRefCount_NotNegativeOnServiceUIDEgressLabelCollision(t *testing.T) {
	const (
		collideID = "collide-id" // serves as BOTH the inbound svc UID and the egress label
		nodeName  = "node-1"
		nodeIP    = "10.0.0.21"
		podIP     = "10.244.3.3"
	)

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-collide",
			Namespace: "default",
			UID:       types.UID(collideID),
		},
		Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
	}
	pod := newEgressPod("pod-collide", "default", collideID, nodeName, podIP, v1.PodRunning)
	kube := fake.NewSimpleClientset(svc, pod)

	k8s := newK8sStateForSeeders()
	nodeNameToIPMap := map[string]string{nodeName: nodeIP}

	// Build the cold-start K8s state: inbound LB service + colliding egress pod.
	_, _, err := processK8sServices(context.Background(), kube, &k8s)
	assert.NoError(t, err)
	_, err = processK8sEgresses(context.Background(), kube, &k8s, nodeNameToIPMap)
	assert.NoError(t, err)

	// New() seeds outboundIdentityPodRefCount from the egress pods now present in k8s state.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	nrp := NRPState{
		LoadBalancers: utilsets.NewString(),
		NATGateways:   utilsets.NewString(),
		Locations:     map[string]NRPLocation{},
	}
	cfg := Config{
		SubscriptionID:             "sub",
		ResourceGroup:              "rg",
		Location:                   "loc",
		ServiceGatewayResourceName: "sgw",
		ServiceGatewayID:           "/sgw/id",
		VNetName:                   "vnet",
	}
	dt, err := New(logr.Discard(), k8s, nrp, cfg, mock_azclient.NewMockClientFactory(ctrl), kube)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	v, ok := dt.outboundIdentityPodRefCount.Load(strings.ToLower(collideID))
	assert.True(t, ok, "the colliding egress identity must be seeded")
	assert.GreaterOrEqual(t, v.(int), 0,
		"an inbound-UID/egress-label collision must not seed a negative outbound ref-count (got %v)", v)
	assert.Equal(t, 1, v.(int),
		"the outbound ref-count must equal the egress pod count (1)")

	// A correct (non-negative) counter must allow DeletePod to complete.
	res := dt.DeletePod(collideID, nodeIP, podIP, "default", "pod-collide")
	assert.True(t, res.IsLastPod,
		"with exactly one live egress pod the ref-count must be 1 (last pod)")
	assert.False(t, podIPTracked(&dt.K8sResources, podIP),
		"DeletePod must remove the egress pod")
}
