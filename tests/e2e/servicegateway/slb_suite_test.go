/*
Copyright 2025 The Kubernetes Authors.

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

package servicegateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

// Environment variable names for SLB configuration
const (
	slbTestLabel = "SLB"

	// Environment variable names
	envServiceGatewayName       = "AZURE_SERVICE_GATEWAY_NAME"
	envServiceGatewayAPIVersion = "AZURE_SERVICE_GATEWAY_API_VERSION"

	// Default values.
	// NOTE: as of commit 33dcfb27 the SGW resource name is hardcoded in the cloud
	// provider to consts.DefaultServiceGatewayResourceName ("servicegateway").
	// Override via AZURE_SERVICE_GATEWAY_NAME only if your cluster predates that change.
	defaultServiceGatewayName = "servicegateway"
	defaultAPIVersion         = "2025-01-01"
)

// Package-level variables populated from environment or AzureTestClient
var (
	subscriptionID     string
	resourceGroupName  string
	serviceGatewayName string
	apiVersion         string
)

// AzurePublicIP represents a Public IP resource in Azure
type AzurePublicIP struct {
	Name      string            `json:"name"`
	IPAddress string            `json:"ipAddress"`
	Tags      map[string]string `json:"tags"`
	ID        string            `json:"id"`
	Location  string            `json:"location"`
}

// AzureLoadBalancer represents a Load Balancer resource in Azure
type AzureLoadBalancer struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Location string `json:"location"`
	SKU      struct {
		Name string `json:"name"`
	} `json:"sku"`
	// Azure CLI returns these at root level, not under "properties"
	FrontendIPConfigurations []struct {
		Name string `json:"name"`
		// publicIPAddress is at root level in Azure CLI JSON
		PublicIPAddress struct {
			ID string `json:"id"`
		} `json:"publicIPAddress"`
	} `json:"frontendIPConfigurations"`
	LoadBalancingRules []struct {
		Name string `json:"name"`
	} `json:"loadBalancingRules"`
	BackendAddressPools []struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	} `json:"backendAddressPools"`
}

// ServiceGatewayServicesResponse represents the response from Service Gateway services API
type ServiceGatewayServicesResponse struct {
	Value []ServiceGatewayService `json:"value"`
}

// ServiceGatewayService represents a service in the Service Gateway
type ServiceGatewayService struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Etag       string `json:"etag"`
	Properties struct {
		ProvisioningState        string `json:"provisioningState"`
		ServiceType              string `json:"serviceType"`
		IsDefault                bool   `json:"isDefault,omitempty"`
		PublicNatGatewayID       string `json:"publicNatGatewayId,omitempty"`
		LoadBalancerBackendPools []struct {
			ID string `json:"id"`
		} `json:"loadBalancerBackendPools"`
	} `json:"properties"`
}

// ServiceGatewayAddressLocationsResponse represents the response from Service Gateway address locations API
type ServiceGatewayAddressLocationsResponse struct {
	Value []ServiceGatewayAddressLocation `json:"value"`
}

// ServiceGatewayAddressLocation represents an address location in the Service Gateway
type ServiceGatewayAddressLocation struct {
	AddressLocation     string    `json:"addressLocation"`
	AddressUpdateAction string    `json:"addressUpdateAction"`
	Addresses           []Address `json:"addresses"`
}

// Address represents an IP address and its associated services
type Address struct {
	Address  string   `json:"address"`
	Services []string `json:"services"`
}

// Helper functions for SLB tests

// ensureSLBConfigInitialized ensures the SLB config is initialized.
// This should be called before using any SLB config variables.
func ensureSLBConfigInitialized() {
	// Try to get subscription and resource group from environment first
	if subscriptionID == "" {
		subscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
	}
	if resourceGroupName == "" {
		resourceGroupName = os.Getenv("AZURE_RESOURCE_GROUP")
	}

	// Initialize subscription and resource group from AzureTestClient if not set
	if subscriptionID == "" || resourceGroupName == "" {
		tc, err := utils.CreateAzureTestClient()
		if err == nil {
			if subscriptionID == "" {
				subscriptionID = tc.GetSubscriptionID()
			}
			if resourceGroupName == "" {
				resourceGroupName = tc.GetResourceGroup()
			}
		} else {
			utils.Logf("Warning: Could not create AzureTestClient: %v", err)
		}
	}

	// Initialize Service Gateway config from environment with defaults
	if serviceGatewayName == "" {
		serviceGatewayName = os.Getenv(envServiceGatewayName)
		if serviceGatewayName == "" {
			serviceGatewayName = defaultServiceGatewayName
		}
	}
	if apiVersion == "" {
		apiVersion = os.Getenv(envServiceGatewayAPIVersion)
		if apiVersion == "" {
			apiVersion = defaultAPIVersion
		}
	}

	utils.Logf("SLB Config: SubscriptionID=%s, ResourceGroup=%s, ServiceGateway=%s, APIVersion=%s",
		subscriptionID, resourceGroupName, serviceGatewayName, apiVersion)
}

// buildServiceGatewayURL constructs the Service Gateway API URL for a given path
func buildServiceGatewayURL(path string) string {
	ensureSLBConfigInitialized()
	return fmt.Sprintf(
		"https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/serviceGateways/%s/%s?api-version=%s",
		subscriptionID, resourceGroupName, serviceGatewayName, path, apiVersion,
	)
}

// azTransientRetries is how many times runAz re-issues a command that failed for a reason the
// Azure control plane is expected to recover from on its own.
const azTransientRetries = 4

// azRetryBackoff is the pause between those attempts.
const azRetryBackoff = 10 * time.Second

// isTransientAzureFailure reports whether an `az` invocation failed for a reason that is worth
// retrying rather than failing the spec.
//
// A 503 from ARM arrives as an HTML error page rather than JSON, and throttling (429) and gateway
// timeouts behave the same way. These say nothing about the cloud provider under test, but because
// most callers issue a single un-polled query, one of them is otherwise enough to fail a spec that
// has already run for minutes — and, in a suite this long, to cost a whole run.
func isTransientAzureFailure(output string) bool {
	for _, marker := range []string{
		"Service Unavailable",
		"ServiceUnavailable",
		"(503)",
		"Gateway Timeout",
		"GatewayTimeout", // the error code, as SDK commands print it: (GatewayTimeout) ...
		"(504)",
		"TooManyRequests",
		"Too Many Requests", // the reason, as az rest prints it whatever the error code (SubscriptionRequestsThrottled, ...)
		"(429)",
		"temporarily unavailable",
		"connection reset by peer",
		"Connection reset by peer", // Python's ConnectionResetError, also while reading a response ("Connection broken: ...")
		// Connection-level failures of az's own HTTP client, as ('Connection aborted.', RemoteDisconnected(...)).
		"Connection aborted",
		"RemoteDisconnected",
		"Please run 'az login'",
		"AADSTS700082", // expired refresh token; the next attempt re-authenticates
		// An ARM or resource provider 500, as az rest prints it (Internal Server Error({"error":{"code":"InternalServerError"...)
		// and as SDK commands print it ((InternalServerError) ...).
		"InternalServerError",
	} {
		if strings.Contains(output, marker) {
			return true
		}
	}
	return false
}

func TestIsTransientAzureFailure(t *testing.T) {
	for _, tc := range []struct {
		output    string
		transient bool
	}{
		{"ERROR: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))", true},
		{"ERROR: ('Connection aborted.', BrokenPipeError(32, 'Broken pipe'))", true},
		{"ERROR: ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))", true},
		{"ERROR: Service Unavailable(<HTML><HEAD><TITLE>Service Unavailable</TITLE></HEAD><BODY><h2>Service Unavailable</h2>" +
			"<hr><p>HTTP Error 503. The service is unavailable.</p></BODY></HTML>)", true},
		{"ERROR: (ServiceUnavailable) The service is unavailable. Please retry the request later.\nCode: ServiceUnavailable\n" +
			"Message: The service is unavailable. Please retry the request later.", true},
		{"ERROR: Operation returned an invalid status 'Gateway Timeout'", true},
		{"ERROR: (TooManyRequests) The request is being throttled. Please retry after 10 seconds.\nCode: TooManyRequests\n" +
			"Message: The request is being throttled. Please retry after 10 seconds.", true},
		{"ERROR: HTTPSConnectionPool(host='management.azure.com', port=443): Max retries exceeded with url: /subscriptions " +
			"(Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x7f3c2a1b5e50>: Failed to establish a new " +
			"connection: [Errno 11] Resource temporarily unavailable'))", true},
		{"ERROR: (InternalServerError) read tcp 10.224.0.4:52422->10.0.0.1:443: read: connection reset by peer\n" +
			"Code: InternalServerError\nMessage: read tcp 10.224.0.4:52422->10.0.0.1:443: read: connection reset by peer", true},
		{"ERROR: Please run 'az login' to setup account.", true},
		{"ERROR: AADSTS700082: The refresh token has expired due to inactivity. The token was issued on 2026-07-01T10:00:00Z " +
			"and was inactive for 90.00:00:00.", true},
		{"ERROR: (503)", true},
		{"ERROR: (504)", true},
		{"ERROR: (429)", true},
		{"ERROR: (GatewayTimeout) The gateway did not receive a response from 'Microsoft.Network' within the specified time period.\n" +
			"Code: GatewayTimeout\nMessage: The gateway did not receive a response from 'Microsoft.Network' within the specified time period.", true},
		{`ERROR: Too Many Requests({"error":{"code":"SubscriptionRequestsThrottled","message":"Number of 'read' requests for subscription ` +
			`'00000000-0000-0000-0000-000000000000' actor '11111111-1111-1111-1111-111111111111' exceeded. Please try again after '5' seconds ` +
			`after additional tokens are available. Refer to https://aka.ms/arm-throttling for additional information."}})`, true},
		{`ERROR: ("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))`, true},
		{`ERROR: Internal Server Error({"error":{"code":"InternalServerError","message":"Encountered internal server error. ` +
			`Diagnostic information: timestamp '20261020T094905Z', subscription id '00000000-0000-0000-0000-000000000000'."}})`, true},
		{"ERROR: (InternalServerError) Encountered internal server error.\nCode: InternalServerError\n" +
			"Message: Encountered internal server error.", true},
		{"ERROR: (ResourceNotFound) The Resource 'Microsoft.Network/publicIPAddresses/x' under resource group 'rg' was not found.", false},
	} {
		if got := isTransientAzureFailure(tc.output); got != tc.transient {
			t.Errorf("isTransientAzureFailure(%q) = %v, want %v", tc.output, got, tc.transient)
		}
	}
}

// runAz runs an `az` command and returns its stdout, retrying transient control-plane failures.
//
// It deliberately uses Output() rather than CombinedOutput(): `az` writes warnings (extension
// notices, deprecation and MSAL messages) to stderr, and folding those into stdout corrupts the
// buffer that callers hand to json.Unmarshal, turning a perfectly healthy query into a parse
// error. Stderr is still reported, but only as part of the error message.
func runAz(args ...string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= azTransientRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(azRetryBackoff)
		}

		cmd := exec.Command("az", args...)
		stdout, err := cmd.Output()
		if err == nil {
			return stdout, nil
		}

		stderr := ""
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr = string(exitErr.Stderr)
		}
		lastErr = fmt.Errorf("az %s: %w (stderr: %s)", strings.Join(args, " "), err, strings.TrimSpace(stderr))

		if !isTransientAzureFailure(stderr) && !isTransientAzureFailure(string(stdout)) {
			// Hand back the diagnostic text on the failure path. Several callers deliberately
			// expect the command to fail — "the resource is gone" is asserted by running `az
			// ... show` and matching ResourceNotFound — and `az` writes that to stderr. Returning
			// only stdout here would give them an empty buffer and turn a correct deletion into
			// an "unexpected error" with no message.
			return append(stdout, stderr...), lastErr
		}
		utils.Logf("Transient Azure failure (attempt %d/%d), retrying: %v", attempt+1, azTransientRetries+1, lastErr)
	}
	return nil, fmt.Errorf("az command still failing after %d attempts: %w", azTransientRetries+1, lastErr)
}

// queryServiceGatewayServices queries all services in the Service Gateway
func queryServiceGatewayServices() (ServiceGatewayServicesResponse, error) {
	url := buildServiceGatewayURL("services")
	output, err := runAz("rest", "--method", "get", "--url", url)
	if err != nil {
		return ServiceGatewayServicesResponse{}, fmt.Errorf("failed to query Service Gateway services: %w", err)
	}

	var response ServiceGatewayServicesResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return ServiceGatewayServicesResponse{}, fmt.Errorf("failed to parse Service Gateway services response: %w", err)
	}

	return response, nil
}

// queryServiceGatewayAddressLocations queries all address locations in the Service Gateway
func queryServiceGatewayAddressLocations() (ServiceGatewayAddressLocationsResponse, error) {
	url := buildServiceGatewayURL("addressLocations")
	output, err := runAz("rest", "--method", "get", "--url", url)
	if err != nil {
		return ServiceGatewayAddressLocationsResponse{}, fmt.Errorf("failed to query Service Gateway address locations: %w", err)
	}

	var response ServiceGatewayAddressLocationsResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return ServiceGatewayAddressLocationsResponse{}, fmt.Errorf("failed to parse Service Gateway address locations response: %w", err)
	}

	return response, nil
}

// verifyServiceGatewayCleanup verifies that only the default outbound service remains in the Service Gateway
func verifyServiceGatewayCleanup() {
	utils.Logf("Verifying Service Gateway services only contain default outbound service")

	sgResponse, err := queryServiceGatewayServices()
	Expect(err).NotTo(HaveOccurred(), "Should be able to query Service Gateway services after cleanup")

	utils.Logf("Found %d service(s) in Service Gateway after cleanup", len(sgResponse.Value))
	foundDefault := false
	for i := range sgResponse.Value {
		svc := &sgResponse.Value[i]
		utils.Logf("  Service: %s (Type: %s)", svc.Name, svc.Properties.ServiceType)

		if svc.Name != "default-natgw" {
			Fail(fmt.Sprintf("Unexpected service '%s' still exists in Service Gateway after cleanup", svc.Name))
		}
		foundDefault = true
		Expect(svc.Properties.ServiceType).To(Equal("Outbound"), "Service should be the default outbound service")
	}
	// The loop above only rejects *extra* services, so it passes vacuously on an empty response -
	// which is not cleanup succeeding but the cluster's default egress path having been destroyed.
	// The default NAT gateway is not created by these tests and must outlive every one of them.
	Expect(foundDefault).To(BeTrue(),
		"the default outbound service 'default-natgw' is missing from the Service Gateway; cleanup must leave it in place")
	utils.Logf("  ✓ Only default outbound service remains in Service Gateway")
}

// verifyAddressLocationsCleanup verifies that no addresses reference any services in the Service Gateway
func verifyAddressLocationsCleanup() {
	utils.Logf("Verifying Service Gateway address locations are empty")

	alResponse, err := queryServiceGatewayAddressLocations()
	Expect(err).NotTo(HaveOccurred(), "Should be able to query Service Gateway address locations after cleanup")

	utils.Logf("Found %d address location(s) in Service Gateway after cleanup", len(alResponse.Value))
	for _, location := range alResponse.Value {
		utils.Logf("  Address Location: %s with %d addresses", location.AddressLocation, len(location.Addresses))

		for _, addr := range location.Addresses {
			if len(addr.Services) > 0 {
				Fail(fmt.Sprintf("Address %s in location %s still has %d service reference(s) after cleanup",
					addr.Address, location.AddressLocation, len(addr.Services)))
			}
		}
	}
	utils.Logf("  ✓ No addresses reference any services in Service Gateway")
}

// verifyNATGatewayCleanup verifies that test-created NAT Gateways are cleaned up
func verifyNATGatewayCleanup(egressNames []string) {
	if len(egressNames) == 0 {
		return // No egress gateways to verify
	}

	utils.Logf("Verifying NAT Gateway cleanup for %d egress gateway(s)", len(egressNames))

	sgResponse, err := queryServiceGatewayServices()
	Expect(err).NotTo(HaveOccurred(), "Should be able to query Service Gateway services")

	for _, egressName := range egressNames {
		found := false
		for _, svc := range sgResponse.Value {
			if svc.Properties.ServiceType == "Outbound" && svc.Name == egressName {
				found = true
				Fail(fmt.Sprintf("Outbound service '%s' still exists in Service Gateway after cleanup", egressName))
			}
		}
		if !found {
			utils.Logf("  ✓ Outbound service '%s' cleaned up", egressName)
		}
	}
}

// serviceGatewayCleanupErr returns nil only when the Service Gateway contains just the
// default outbound service (i.e. all test services have been cleaned up). It is the
// error-returning core used to poll for cleanup via Eventually, so a spec waits exactly
// as long as Azure actually needs instead of a fixed, conservative sleep.
func serviceGatewayCleanupErr() error {
	sgResponse, err := queryServiceGatewayServices()
	if err != nil {
		return fmt.Errorf("query Service Gateway services: %w", err)
	}
	foundDefault := false
	for i := range sgResponse.Value {
		svc := &sgResponse.Value[i]
		if svc.Name != "default-natgw" {
			return fmt.Errorf("unexpected service %q (type %s) still exists in Service Gateway after cleanup", svc.Name, svc.Properties.ServiceType)
		}
		foundDefault = true
	}
	// Without this an empty Service Gateway satisfies the poll immediately: the loop finds no
	// unexpected service because it finds no service at all. Cleanup must remove the test's
	// services and leave the cluster-wide default outbound service untouched.
	if !foundDefault {
		return fmt.Errorf("the default outbound service %q is missing from the Service Gateway; cleanup must leave it in place", "default-natgw")
	}
	return nil
}

// addressLocationsCleanupErr returns nil only when no address in the Service Gateway still
// references a service. Error-returning core for polling address-location cleanup.
func addressLocationsCleanupErr() error {
	alResponse, err := queryServiceGatewayAddressLocations()
	if err != nil {
		return fmt.Errorf("query Service Gateway address locations: %w", err)
	}
	for _, location := range alResponse.Value {
		for _, addr := range location.Addresses {
			if len(addr.Services) > 0 {
				return fmt.Errorf("address %s in location %s still has %d service reference(s) after cleanup",
					addr.Address, location.AddressLocation, len(addr.Services))
			}
		}
	}
	return nil
}

// natGatewayCleanupErr returns nil only when none of the named egress (outbound) services
// remain in the Service Gateway. Error-returning core for polling NAT Gateway cleanup.
func natGatewayCleanupErr(egressNames []string) error {
	if len(egressNames) == 0 {
		return nil
	}
	sgResponse, err := queryServiceGatewayServices()
	if err != nil {
		return fmt.Errorf("query Service Gateway services: %w", err)
	}
	for _, egressName := range egressNames {
		for _, svc := range sgResponse.Value {
			if svc.Properties.ServiceType == "Outbound" && svc.Name == egressName {
				return fmt.Errorf("outbound service %q still exists in Service Gateway after cleanup", egressName)
			}
		}
	}
	// Deregistering from the ServiceGateway says nothing about ARM. Without this a CCM that
	// unregisters but never deletes the resources passes every egress teardown while leaking a NAT
	// Gateway and, on a dual-stack cluster, both of its Public IPs.
	for _, egressName := range egressNames {
		if err := azureEgressResourcesAbsentErr(egressName); err != nil {
			return err
		}
	}
	return nil
}

// azurePublicIPAbsentErr returns nil once no Public IP exists for the given service UID. It is the
// positive form of "nothing was provisioned", usable on its own rather than as a side effect of
// verifyAzureResources failing for some unrelated reason.
func azurePublicIPAbsentErr(serviceUID string) error {
	return azurePublicIPNamedAbsentErr(fmt.Sprintf("%s-pip", serviceUID))
}

// azurePublicIPNamedAbsentErr returns nil once the exactly-named Public IP is gone from Azure.
func azurePublicIPNamedAbsentErr(publicIPName string) error {
	pipOutput, err := runAz("network", "public-ip", "list",
		"--resource-group", resourceGroupName,
		"--output", "json")
	if err != nil {
		return fmt.Errorf("failed to query Azure for Public IPs: %w", err)
	}

	var publicIPs []AzurePublicIP
	if err := json.Unmarshal(pipOutput, &publicIPs); err != nil {
		return fmt.Errorf("failed to parse Public IPs response: %w", err)
	}
	for i := range publicIPs {
		if publicIPs[i].Name == publicIPName {
			return fmt.Errorf("public IP %s exists for a service that should not have been provisioned", publicIPName)
		}
	}
	return nil
}

// azureLoadBalancerAbsentErr returns nil once the service's Azure Load Balancer is gone.
//
// Deletion specs previously asserted only that the Service Gateway registration disappeared, which
// says nothing about the underlying ARM resources: a CCM that deregisters the service but never
// issues the LB/PIP deletes passes every such assertion while leaking billable Azure resources on
// each service deletion. Absence is decided from the LIST output rather than a `show` exit code so
// a transient query failure is reported as an error instead of being mistaken for "deleted".
func azureLoadBalancerAbsentErr(serviceUID string) error {
	lbOutput, err := runAz("network", "lb", "list",
		"--resource-group", resourceGroupName,
		"--output", "json")
	if err != nil {
		return fmt.Errorf("failed to query Azure for Load Balancers: %w", err)
	}

	var loadBalancers []AzureLoadBalancer
	if err := json.Unmarshal(lbOutput, &loadBalancers); err != nil {
		return fmt.Errorf("failed to parse Load Balancers response: %w", err)
	}
	for i := range loadBalancers {
		if loadBalancers[i].Name == serviceUID {
			return fmt.Errorf("load balancer %s still exists in Azure after deletion (leaked)", serviceUID)
		}
	}
	return nil
}

// azureInboundResourcesAbsentErr returns nil once BOTH the service's Load Balancer and its Public
// IP are gone from Azure. This is the resource-level counterpart to serviceDeletedErr, which only
// checks the ServiceGateway registration.
func azureInboundResourcesAbsentErr(serviceUID string) error {
	if err := azureLoadBalancerAbsentErr(serviceUID); err != nil {
		return err
	}
	return azurePublicIPAbsentErr(serviceUID)
}

// azureEgressResourcesAbsentErr returns nil once the egress identity's NAT Gateway and its Public
// IP are gone from Azure.
//
// The NAT Gateway is named after the egress identity. Its IPv4 Public IP follows the same
// <name>-pip convention as the inbound path, and a dual-stack cluster adds <name>-pip-v6.
// Without this check a CCM that removes the Outbound registration from the ServiceGateway but
// never deletes the ARM resources passes every egress teardown spec while leaking a NAT Gateway
// and a Public IP per egress identity.
func azureEgressResourcesAbsentErr(egressName string) error {
	natOutput, err := runAz("network", "nat", "gateway", "show",
		"--resource-group", resourceGroupName,
		"--name", egressName,
		"--output", "json")
	if err == nil {
		return fmt.Errorf("NAT Gateway %s still exists in Azure after teardown (leaked)", egressName)
	}
	text := string(natOutput)
	if !strings.Contains(text, "not found") && !strings.Contains(text, "NotFound") {
		return fmt.Errorf("could not determine whether NAT Gateway %s was deleted: %s", egressName, text)
	}
	if err := azurePublicIPAbsentErr(egressName); err != nil {
		return err
	}
	// A dual-stack cluster also gets an IPv6 address, named "<name>-pip-v6". It is provisioned by
	// the same create and removed by the same delete, so a teardown that misses it leaks a second
	// billable address per egress identity.
	return azurePublicIPNamedAbsentErr(fmt.Sprintf("%s-pip-v6", egressName))
}

// egressIdentityTagKey is the tag the controller sets, to the identity, on the NAT Gateway and on
// every Public IP and Public IP prefix it manages for an egress identity.
const egressIdentityTagKey = "k8s-azure-egress-identity"

// egressManagedPublicIPName returns the name of the n-th (from 1) managed Public IP of egress
// identity id in one IP family: "<id>-pip" or "<id>-pip-v6" for the first, the names used before
// egress configuration existed, then "<id>-pip-2" or "<id>-pip-v6-2" and so on.
func egressManagedPublicIPName(id string, ipv6 bool, n int) string {
	name := id + "-pip"
	if ipv6 {
		name += "-v6"
	}
	if n > 1 {
		name = fmt.Sprintf("%s-%d", name, n)
	}
	return name
}

// egressManagedPrefixName returns the name of the managed Public IP prefix of egress identity id
// with the given length, "<id>-prefix-31" or "<id>-prefix-v6-127". The length is part of the name
// so that a length change can create the new prefix while the old one is still attached.
func egressManagedPrefixName(id string, ipv6 bool, length int) string {
	if ipv6 {
		return fmt.Sprintf("%s-prefix-v6-%d", id, length)
	}
	return fmt.Sprintf("%s-prefix-%d", id, length)
}

// egressReservedNames returns every name egress identity id reserves for the Public IPs and the
// Public IP prefixes the controller may manage for it, in both IP families.
func egressReservedNames(id string) (publicIPs, prefixes []string) {
	for _, ipv6 := range []bool{false, true} {
		for n := 1; n <= consts.ServiceEgressGatewayPublicIPCountMax; n++ {
			publicIPs = append(publicIPs, egressManagedPublicIPName(id, ipv6, n))
		}
	}
	for length := consts.ServiceEgressGatewayPublicIPPrefixLengthMin; length <= consts.ServiceEgressGatewayPublicIPPrefixLengthMax; length++ {
		prefixes = append(prefixes, egressManagedPrefixName(id, false, length))
	}
	for length := consts.ServiceEgressGatewayPublicIPPrefixLengthIPv6Min; length <= consts.ServiceEgressGatewayPublicIPPrefixLengthIPv6Max; length++ {
		prefixes = append(prefixes, egressManagedPrefixName(id, true, length))
	}
	return publicIPs, prefixes
}

// armSubResource is a reference to another ARM resource.
type armSubResource struct {
	ID string `json:"id"`
}

// azureNetworkResource is a NAT Gateway, Public IP or Public IP prefix as ARM returns it. The three
// types share the struct; each fills the properties it has.
//
// It is read with `az rest` (armURL, at byoResourceAPIVersion), in ARM's shape: `az network ... show` prints the
// properties flattened, outside `properties`, and its own API version leaves out a NAT Gateway's ServiceGateway link.
type azureNetworkResource struct {
	ID   string            `json:"id"`
	Name string            `json:"name"`
	Etag string            `json:"etag"`
	Tags map[string]string `json:"tags"`
	SKU  struct {
		Name string `json:"name"`
	} `json:"sku"`
	Properties struct {
		ProvisioningState string `json:"provisioningState"`
		ResourceGUID      string `json:"resourceGuid"`

		// NAT Gateway.
		IdleTimeoutInMinutes *int             `json:"idleTimeoutInMinutes"`
		PublicIPAddresses    []armSubResource `json:"publicIpAddresses"`
		PublicIPAddressesV6  []armSubResource `json:"publicIpAddressesV6"`
		PublicIPPrefixes     []armSubResource `json:"publicIpPrefixes"`
		PublicIPPrefixesV6   []armSubResource `json:"publicIpPrefixesV6"`
		ServiceGateway       *armSubResource  `json:"serviceGateway"`

		// Public IP and Public IP prefix.
		IPAddress        string          `json:"ipAddress"`
		IPPrefix         string          `json:"ipPrefix"`
		PrefixLength     int             `json:"prefixLength"`
		AddressVersion   string          `json:"publicIPAddressVersion"`
		AllocationMethod string          `json:"publicIPAllocationMethod"`
		NatGateway       *armSubResource `json:"natGateway"`
	} `json:"properties"`
}

// getAzureNetworkResource reads a network resource by ID; found is false when it does not exist.
func getAzureNetworkResource(resourceID string) (resource azureNetworkResource, found bool, err error) {
	out, err := runAz("rest", "--method", "get", "--url", armURL(resourceID))
	if err != nil {
		if strings.Contains(string(out), "NotFound") {
			return azureNetworkResource{}, false, nil
		}
		return azureNetworkResource{}, false, fmt.Errorf("reading %s: %w", resourceID, err)
	}
	if err := json.Unmarshal(out, &resource); err != nil {
		return azureNetworkResource{}, false, fmt.Errorf("parsing %s: %w", resourceID, err)
	}
	return resource, true, nil
}

// listAzureNetworkResources lists the network resources of one type of the cluster resource group;
// collection is the type's segment in a resource ID, such as "publicIPPrefixes".
func listAzureNetworkResources(collection string) ([]azureNetworkResource, error) {
	url := fmt.Sprintf("https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/%s?api-version=%s",
		subscriptionID, resourceGroupName, collection, byoResourceAPIVersion)
	var resources []azureNetworkResource
	for url != "" {
		out, err := runAz("rest", "--method", "get", "--url", url)
		if err != nil {
			return nil, fmt.Errorf("listing %s: %w", collection, err)
		}
		var page struct {
			Value    []azureNetworkResource `json:"value"`
			NextLink string                 `json:"nextLink"`
		}
		if err := json.Unmarshal(out, &page); err != nil {
			return nil, fmt.Errorf("parsing the list of %s: %w", collection, err)
		}
		resources = append(resources, page.Value...)
		url = page.NextLink
	}
	return resources, nil
}

// tagValue returns the value of tag key, whose name compares case-insensitively as in Azure, or "".
func tagValue(tags map[string]string, key string) string {
	for k, v := range tags {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// containsFold reports whether names contains name, ignoring case as Azure does for resource names.
func containsFold(names []string, name string) bool {
	for _, n := range names {
		if strings.EqualFold(n, name) {
			return true
		}
	}
	return false
}

// sameNamesFold reports whether got and want hold the same names, ignoring case and order.
func sameNamesFold(got, want []string) bool {
	fold := func(names []string) []string {
		folded := make([]string, 0, len(names))
		for _, name := range names {
			folded = append(folded, strings.ToLower(name))
		}
		sort.Strings(folded)
		return folded
	}
	return slices.Equal(fold(got), fold(want))
}

// egressReservedResourcesErr returns nil when the Public IPs and Public IP prefixes of the cluster
// resource group that have a reserved name of egress identity egressName are exactly wantPublicIPs
// and wantPrefixes (names). Every reserved name counts, owned or not: a spec that creates one itself
// removes it before the AfterEach, so a surplus is a managed resource the controller failed to delete.
func egressReservedResourcesErr(egressName string, wantPublicIPs, wantPrefixes []string) error {
	reservedPublicIPs, reservedPrefixes := egressReservedNames(egressName)
	for _, kind := range []struct {
		collection     string
		reserved, want []string
	}{
		{"publicIPAddresses", reservedPublicIPs, wantPublicIPs},
		{"publicIPPrefixes", reservedPrefixes, wantPrefixes},
	} {
		resources, err := listAzureNetworkResources(kind.collection)
		if err != nil {
			return err
		}
		var got []string
		for i := range resources {
			if containsFold(kind.reserved, resources[i].Name) {
				got = append(got, resources[i].Name)
			}
		}
		if !sameNamesFold(got, kind.want) {
			sort.Strings(got)
			return fmt.Errorf("egress identity %s has managed %s %v, want %v", egressName, kind.collection, got, kind.want)
		}
	}
	return nil
}

// egressIdentityGoneErr returns nil once egress identity egressName is unregistered from the Service
// Gateway with no address left, and its NAT Gateway and every managed Public IP and Public IP prefix
// it may have had (any reserved name, numbered Public IPs and prefixes included) are deleted, except
// the numbered Public IPs and the prefixes kept (names), which are left at their reserved names. Its
// base Public IPs ("<id>-pip", "<id>-pip-v6") must be gone in any case (natGatewayCleanupErr).
func egressIdentityGoneErr(egressName string, kept ...string) error {
	if err := natGatewayCleanupErr([]string{egressName}); err != nil {
		return err
	}
	if got, err := countRegisteredEndpoints(egressName); err != nil {
		return err
	} else if got > 0 {
		return fmt.Errorf("egress %s still has %d registered address(es)", egressName, got)
	}
	reservedPublicIPs, _ := egressReservedNames(egressName)
	var keptPublicIPs, keptPrefixes []string
	for _, name := range kept {
		if containsFold(reservedPublicIPs, name) {
			keptPublicIPs = append(keptPublicIPs, name)
		} else {
			keptPrefixes = append(keptPrefixes, name)
		}
	}
	return egressReservedResourcesErr(egressName, keptPublicIPs, keptPrefixes)
}

// eventuallyEgressIdentityGone waits until egressIdentityGoneErr holds.
func eventuallyEgressIdentityGone(egressName string) {
	Eventually(func() error {
		return egressIdentityGoneErr(egressName)
	}, 8*time.Minute, defaultPollInterval).Should(Succeed(),
		"egress identity %s must be unregistered and its NAT Gateway, Public IPs and prefixes deleted", egressName)
}

// egressManagedResourcesCleanedUpErr returns nil once the cluster resource group holds no numbered
// managed Public IP ("<id>-pip-2".."<id>-pip-16", "<id>-pip-v6-2".."<id>-pip-v6-16") and no managed
// Public IP prefix ("<id>-prefix-28".."<id>-prefix-31", "<id>-prefix-v6-124".."<id>-prefix-v6-127")
// of any egress identity.
//
// These names exist only for egress identities with a configuration. The per-identity check
// (egressIdentityGoneErr) covers only the identities a spec configured; this global check covers every
// identity, in every spec's cleanup. Like egressIPv6PublicIPsCleanedUpErr it is global, but only a
// resource carrying the ownership tag of the identity its name belongs to counts, since the controller
// manages these names only with it.
func egressManagedResourcesCleanedUpErr() error {
	var leaked []string
	for _, collection := range []string{"publicIPAddresses", "publicIPPrefixes"} {
		resources, err := listAzureNetworkResources(collection)
		if err != nil {
			return err
		}
		for i := range resources {
			if isEgressManagedLeftover(resources[i], collection == "publicIPPrefixes") {
				leaked = append(leaked, resources[i].Name)
			}
		}
	}
	if len(leaked) > 0 {
		sort.Strings(leaked)
		return fmt.Errorf("managed egress Public IPs or Public IP prefixes still exist after cleanup (leaked): %v", leaked)
	}
	return nil
}

// isEgressManagedLeftover reports whether a Public IP, or a Public IP prefix when prefix is set, is a
// numbered managed Public IP or a managed prefix of an egress identity: it carries the identity's
// ownership tag under one of the identity's reserved names other than the base Public IP names
// "<id>-pip" and "<id>-pip-v6", which the existing checks cover.
func isEgressManagedLeftover(resource azureNetworkResource, prefix bool) bool {
	identity := tagValue(resource.Tags, egressIdentityTagKey)
	if identity == "" {
		return false
	}
	publicIPs, prefixes := egressReservedNames(identity)
	if prefix {
		return containsFold(prefixes, resource.Name)
	}
	return containsFold(publicIPs, resource.Name) &&
		!strings.EqualFold(resource.Name, egressManagedPublicIPName(identity, false, 1)) &&
		!strings.EqualFold(resource.Name, egressManagedPublicIPName(identity, true, 1))
}

func TestIsEgressManagedLeftover(t *testing.T) {
	owned := map[string]string{egressIdentityTagKey: "egi"}
	for _, tc := range []struct {
		name     string
		resource string
		tags     map[string]string
		prefix   bool
		want     bool
	}{
		{name: "numbered IPv4 Public IP", resource: "egi-pip-2", tags: owned, want: true},
		{name: "last numbered IPv6 Public IP", resource: "egi-pip-v6-16", tags: owned, want: true},
		{name: "IPv4 prefix", resource: "egi-prefix-31", tags: owned, prefix: true, want: true},
		{name: "IPv6 prefix", resource: "egi-prefix-v6-124", tags: owned, prefix: true, want: true},
		{name: "shortest IPv4 prefix", resource: "egi-prefix-28", tags: owned, prefix: true, want: true},
		{name: "longest IPv6 prefix", resource: "egi-prefix-v6-127", tags: owned, prefix: true, want: true},
		{name: "names and tag key in another case", resource: "EGI-PIP-3", tags: map[string]string{"K8S-Azure-Egress-Identity": "egi"}, want: true},
		{name: "base IPv4 Public IP", resource: "egi-pip", tags: owned},
		{name: "base IPv6 Public IP", resource: "egi-pip-v6", tags: owned},
		{name: "base IPv4 Public IP in another case", resource: "EGI-PIP", tags: owned},
		{name: "base IPv6 Public IP in another case", resource: "EGI-Pip-V6", tags: owned},
		{name: "no ownership tag", resource: "egi-pip-2"},
		{name: "another identity's tag", resource: "egi-pip-2", tags: map[string]string{egressIdentityTagKey: "other"}},
		{name: "beyond the Public IP count", resource: "egi-pip-17", tags: owned},
		{name: "prefix length out of range", resource: "egi-prefix-27", tags: owned, prefix: true},
		{name: "prefix name as a Public IP", resource: "egi-prefix-31", tags: owned},
		{name: "Public IP name as a prefix", resource: "egi-pip-2", tags: owned, prefix: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isEgressManagedLeftover(azureNetworkResource{Name: tc.resource, Tags: tc.tags}, tc.prefix); got != tc.want {
				t.Errorf("isEgressManagedLeftover(%q, %v, prefix=%v) = %v; want %v", tc.resource, tc.tags, tc.prefix, got, tc.want)
			}
		})
	}
	// The first Public IP of each family keeps the name it had before configuration existed, and is reserved too.
	publicIPs, _ := egressReservedNames("egi")
	for ipv6, want := range map[bool]string{false: "egi-pip", true: "egi-pip-v6"} {
		if got := egressManagedPublicIPName("egi", ipv6, 1); got != want || !containsFold(publicIPs, got) {
			t.Errorf("egressManagedPublicIPName(egi, ipv6=%v, 1) = %q, reserved %v; want %q, reserved", ipv6, got, containsFold(publicIPs, got), want)
		}
	}
}

// verifyAzureResources verifies Public IP, Load Balancer, and Service Gateway for a given service
func verifyAzureResources(serviceUID string) error {
	publicIPName := fmt.Sprintf("%s-pip", serviceUID)
	loadBalancerName := serviceUID

	// Verify Public IP in Azure
	pipOutput, err := runAz("network", "public-ip", "list",
		"--resource-group", resourceGroupName,
		"--output", "json")
	if err != nil {
		return fmt.Errorf("failed to query Azure for Public IPs: %w", err)
	}

	var publicIPs []AzurePublicIP
	if err := json.Unmarshal(pipOutput, &publicIPs); err != nil {
		return fmt.Errorf("failed to parse Public IP JSON: %w", err)
	}

	var servicePublicIP *AzurePublicIP
	for i := range publicIPs {
		if publicIPs[i].Name == publicIPName {
			servicePublicIP = &publicIPs[i]
			break
		}
	}
	if servicePublicIP == nil {
		return fmt.Errorf("public IP not found: %s", publicIPName)
	}

	// Verify Load Balancer in Azure
	lbOutput, err := runAz("network", "lb", "show",
		"--resource-group", resourceGroupName,
		"--name", loadBalancerName,
		"--output", "json")
	if err != nil {
		return fmt.Errorf("failed to query Azure for Load Balancer: %w", err)
	}

	var serviceLB AzureLoadBalancer
	if err := json.Unmarshal(lbOutput, &serviceLB); err != nil {
		return fmt.Errorf("failed to parse Load Balancer JSON: %w", err)
	}

	if serviceLB.SKU.Name != "Service" {
		return fmt.Errorf("load Balancer SKU should be 'Service', got '%s'", serviceLB.SKU.Name)
	}

	// Verify Load Balancer has backend address pools
	if len(serviceLB.BackendAddressPools) == 0 {
		return fmt.Errorf("load Balancer %s has no backend address pools", loadBalancerName)
	}

	// Verify Service Gateway has this service
	sgResponse, err := queryServiceGatewayServices()
	if err != nil {
		return fmt.Errorf("failed to query Service Gateway services: %w", err)
	}

	var foundService bool
	for _, sgSvc := range sgResponse.Value {
		if sgSvc.Name == serviceUID {
			foundService = true
			break
		}
	}
	if !foundService {
		return fmt.Errorf("service %s not found in Service Gateway", serviceUID)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Shared polling helpers
//
// These wrap the common "wait until Azure and the Service Gateway converge"
// checks behind error-returning predicates (suitable for Eventually) and thin
// Eventually wrappers. They let specs poll for convergence instead of sleeping a
// fixed, conservative amount of time, which is both faster and far less flaky.
//
// Predicates (return nil on success) are composable inside a caller's own
// Eventually loop (e.g. when polling several services at once); the eventually*
// wrappers are convenience one-liners for the common single-resource case.
// ---------------------------------------------------------------------------

// defaultPollInterval is the polling cadence used by the eventually* wrappers.
const defaultPollInterval = 10 * time.Second

// countRegisteredEndpoints returns how many distinct addresses the Service Gateway has registered
// for the given service/egress identifier. It works for both inbound services and outbound egress,
// since both are referenced by identifier in an address's Services list.
//
// Two properties matter for callers:
//
//   - It counts ADDRESSES, not pods. A single-stack pod contributes one address, so a pod count and
//     an address count coincide; a dual-stack pod registers one address per IP family and therefore
//     contributes two. Callers that want to express an expectation in pods on a dual-stack cluster
//     must use expectedAddressesForPods rather than the raw pod count.
//   - Each address is counted once even if it appears under more than one node location (which
//     happens transiently while a node's IP changes) and locations that are not a live view of
//     state are skipped, so a tombstoned entry cannot inflate the total.
func countRegisteredEndpoints(serviceID string) (int, error) {
	alResponse, err := queryServiceGatewayAddressLocations()
	if err != nil {
		return 0, fmt.Errorf("query Service Gateway address locations: %w", err)
	}
	seen := make(map[string]struct{})
	for _, location := range alResponse.Value {
		// A delete-shaped location describes addresses being removed, not addresses in service.
		if strings.EqualFold(location.AddressUpdateAction, "Delete") {
			continue
		}
		for _, addr := range location.Addresses {
			for _, svc := range addr.Services {
				if svc == serviceID {
					seen[addr.Address] = struct{}{}
					break
				}
			}
		}
	}
	return len(seen), nil
}

// registeredAddressesFor returns the exact set of addresses the Service Gateway has registered for
// the given service/egress identifier.
//
// Prefer it over countRegisteredEndpoints whenever a spec removes backends. A count cannot tell a
// correct removal apart from a stale address that leaked paired with a live one that was wrongly
// dropped — both leave the total unchanged — which is precisely the regression the removal paths
// are most likely to have.
func registeredAddressesFor(serviceID string) (map[string]struct{}, error) {
	alResponse, err := queryServiceGatewayAddressLocations()
	if err != nil {
		return nil, fmt.Errorf("query Service Gateway address locations: %w", err)
	}
	addrs := make(map[string]struct{})
	for _, location := range alResponse.Value {
		if strings.EqualFold(location.AddressUpdateAction, "Delete") {
			continue
		}
		for _, addr := range location.Addresses {
			for _, svc := range addr.Services {
				if svc == serviceID {
					addrs[addr.Address] = struct{}{}
					break
				}
			}
		}
	}
	return addrs, nil
}

// registeredAddressesMatchErr returns nil once the Service Gateway has registered exactly want for
// the service: no missing address and, just as importantly, no extra one left behind.
func registeredAddressesMatchErr(serviceID string, want map[string]struct{}) error {
	got, err := registeredAddressesFor(serviceID)
	if err != nil {
		return err
	}
	var missing, unexpected []string
	for addr := range want {
		if _, ok := got[addr]; !ok {
			missing = append(missing, addr)
		}
	}
	for addr := range got {
		if _, ok := want[addr]; !ok {
			unexpected = append(unexpected, addr)
		}
	}
	if len(missing) > 0 || len(unexpected) > 0 {
		sort.Strings(missing)
		sort.Strings(unexpected)
		return fmt.Errorf("registered addresses for %s do not match: missing %v, unexpectedly still registered %v",
			serviceID, missing, unexpected)
	}
	return nil
}

// podIPSet collects every IP of the given pods, which is the address set they should register.
// podIPSet collects the pod IPs of the given pods, skipping any pod that is already terminating.
// A pod keeps its IP in a List response until it is fully gone, so counting it after a scale-down
// yields one address more than the workload actually has and makes an exact-set comparison against
// the Service Gateway fail against a set the data path has correctly stopped using.
func podIPSet(pods []v1.Pod) map[string]struct{} {
	set := make(map[string]struct{})
	for i := range pods {
		if pods[i].DeletionTimestamp != nil {
			continue
		}
		for _, ip := range pods[i].Status.PodIPs {
			if ip.IP != "" {
				set[ip.IP] = struct{}{}
			}
		}
	}
	return set
}

// serviceReconciledErr returns nil once the inbound service's Azure resources exist
// (PIP, LB with SKU=Service and a backend pool, and a Service Gateway entry) and,
// when wantEndpoints >= 0, exactly wantEndpoints pod IPs are registered for it. Pass
// a negative wantEndpoints to skip the endpoint-count assertion.
func serviceReconciledErr(serviceUID string, wantEndpoints int) error {
	if err := verifyAzureResources(serviceUID); err != nil {
		return err
	}
	if wantEndpoints < 0 {
		return nil
	}
	got, err := countRegisteredEndpoints(serviceUID)
	if err != nil {
		return err
	}
	if got != wantEndpoints {
		return fmt.Errorf("service %s has %d registered endpoints, want %d", serviceUID, got, wantEndpoints)
	}
	return nil
}

// serviceReconciledMatchErr is the set-based sibling of serviceReconciledErr: it verifies the
// inbound service's Azure resources exist AND that exactly wantAddrs are registered for it.
//
// Prefer this whenever the expected pod IPs are knowable. A count cannot distinguish the right N
// addresses from the wrong N: registering a node IP, a stale pod's IP, or another service's pod
// leaves the count correct while traffic goes to the wrong place, and on a scale-down it cannot
// tell a drained survivor from a drained victim.
func serviceReconciledMatchErr(serviceUID string, wantAddrs map[string]struct{}) error {
	if err := verifyAzureResources(serviceUID); err != nil {
		return err
	}
	return registeredAddressesMatchErr(serviceUID, wantAddrs)
}

// egressRegisteredMatchErr is the set-based sibling of egressRegisteredErr: the egress service must
// exist with a NAT Gateway, and exactly wantAddrs must be registered for it.
func egressRegisteredMatchErr(egressName string, wantAddrs map[string]struct{}) error {
	if err := egressRegisteredErr(egressName, -1); err != nil {
		return err
	}
	return registeredAddressesMatchErr(egressName, wantAddrs)
}

// serviceDeletedErr returns nil once the inbound service is gone from the Service Gateway, no
// address location still references it, AND its Azure Load Balancer and Public IP are deleted.
//
// The Azure-resource check is the important half: without it a CCM that unregisters the service
// but never issues the ARM deletes satisfies every deletion spec in the suite while leaking a
// billable Load Balancer and Public IP per deleted service. Every caller already wraps this in
// Eventually/Consistently, so the extra convergence time is absorbed there.
func serviceDeletedErr(serviceUID string) error {
	sgResponse, err := queryServiceGatewayServices()
	if err != nil {
		return fmt.Errorf("query Service Gateway services: %w", err)
	}
	for _, svc := range sgResponse.Value {
		if svc.Name == serviceUID {
			return fmt.Errorf("service %s still registered in Service Gateway", serviceUID)
		}
	}
	got, err := countRegisteredEndpoints(serviceUID)
	if err != nil {
		return err
	}
	if got > 0 {
		return fmt.Errorf("service %s still has %d registered endpoint(s)", serviceUID, got)
	}
	return azureInboundResourcesAbsentErr(serviceUID)
}

// egressRegisteredErr returns nil once the named egress (outbound) service exists in
// the Service Gateway with a NAT Gateway and, when wantPods >= 0, exactly wantPods pod
// IPs registered for it. Pass a negative wantPods to skip the pod-count assertion.
func egressRegisteredErr(egressName string, wantPods int) error {
	sgResponse, err := queryServiceGatewayServices()
	if err != nil {
		return fmt.Errorf("query Service Gateway services: %w", err)
	}
	found := false
	for _, svc := range sgResponse.Value {
		if svc.Properties.ServiceType == "Outbound" && svc.Name == egressName {
			found = true
			if svc.Properties.PublicNatGatewayID == "" {
				return fmt.Errorf("egress %s has no NAT Gateway ID yet", egressName)
			}
			break
		}
	}
	if !found {
		return fmt.Errorf("egress %s not registered in Service Gateway yet", egressName)
	}
	if wantPods < 0 {
		return nil
	}
	got, err := countRegisteredEndpoints(egressName)
	if err != nil {
		return err
	}
	if got != wantPods {
		return fmt.Errorf("egress %s has %d registered pod(s), want %d", egressName, got, wantPods)
	}
	return nil
}

// eventuallyServiceReconciled polls until the inbound service is fully reconciled in
// Azure and the Service Gateway. Pass a negative wantEndpoints to skip the count check.
func eventuallyServiceReconciled(serviceUID string, wantEndpoints int, timeout time.Duration) {
	Eventually(func() error {
		return serviceReconciledErr(serviceUID, wantEndpoints)
	}, timeout, defaultPollInterval).Should(Succeed(),
		"service %s should be reconciled in Azure and the Service Gateway", serviceUID)
}

// eventuallyServiceDeleted polls until the inbound service is fully removed from the
// Service Gateway and its address locations.
func eventuallyServiceDeleted(serviceUID string, timeout time.Duration) {
	Eventually(func() error {
		return serviceDeletedErr(serviceUID)
	}, timeout, defaultPollInterval).Should(Succeed(),
		"service %s should be removed from the Service Gateway", serviceUID)
}

// eventuallyEgressRegistered polls until the egress service is reconciled with its NAT
// Gateway and registered pods. Pass a negative wantPods to skip the count check.
func eventuallyEgressRegistered(egressName string, wantPods int, timeout time.Duration) {
	Eventually(func() error {
		return egressRegisteredErr(egressName, wantPods)
	}, timeout, defaultPollInterval).Should(Succeed(),
		"egress %s should be registered with %d pod(s) in the Service Gateway", egressName, wantPods)
}

// eventuallyAzureCleanup polls until the Service Gateway and its address locations are
// free of all test services (only the default outbound service remains). Use it in
// AfterEach in place of a fixed post-delete sleep.
func eventuallyAzureCleanup(timeout time.Duration) {
	Eventually(func() error {
		if err := serviceGatewayCleanupErr(); err != nil {
			return err
		}
		if err := addressLocationsCleanupErr(); err != nil {
			return err
		}
		if err := egressIPv6PublicIPsCleanedUpErr(); err != nil {
			return err
		}
		return egressManagedResourcesCleanedUpErr()
	}, timeout, defaultPollInterval).Should(Succeed(),
		"Service Gateway, address locations and egress Public IPs and prefixes should be free of test resources after cleanup")
}

// egressIPv6PublicIPsCleanedUpErr returns nil once no IPv6 egress Public IP remains in Azure.
//
// The "-pip-v6" suffix is created only by this controller, for a dual-stack egress NAT Gateway, so
// any survivor is a leak. This is a global check like serviceGatewayCleanupErr: the other cleanup
// assertions only read the ServiceGateway registration, which says nothing about ARM, so without
// this a CCM that deregisters an egress identity but never deletes its IPv6 address passes every
// egress spec while leaking a billable Public IP per identity.
func egressIPv6PublicIPsCleanedUpErr() error {
	pipOutput, err := runAz("network", "public-ip", "list",
		"--resource-group", resourceGroupName,
		"--output", "json")
	if err != nil {
		return fmt.Errorf("failed to query Azure for Public IPs: %w", err)
	}

	var publicIPs []AzurePublicIP
	if err := json.Unmarshal(pipOutput, &publicIPs); err != nil {
		return fmt.Errorf("failed to parse Public IPs response: %w", err)
	}
	leaked := []string{}
	for i := range publicIPs {
		if strings.HasSuffix(publicIPs[i].Name, "-pip-v6") {
			leaked = append(leaked, publicIPs[i].Name)
		}
	}
	if len(leaked) > 0 {
		return fmt.Errorf("IPv6 egress Public IPs still exist after cleanup (leaked): %v", leaked)
	}
	return nil
}
