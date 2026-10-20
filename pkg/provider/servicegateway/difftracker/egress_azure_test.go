/*
Copyright 2026 The Kubernetes Authors.

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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/ratelimit"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/retryrepectthrottled"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
)

const (
	egressAzureTestIdentity = "egress-a"
	egressAzureTestEtag     = `W/"00000000-0000-0000-0000-000000000001"`

	egressAzureTestRG             = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network"
	egressAzureTestNATPath        = egressAzureTestRG + "/natGateways/egress-a"
	egressAzureTestPIPPath        = egressAzureTestRG + "/publicIPAddresses/egress-a-pip"
	egressAzureTestPrefixPath     = egressAzureTestRG + "/publicIPPrefixes/egress-a-prefix-31"
	egressAzureTestServicesPath   = egressAzureTestRG + "/serviceGateways/sgw/services"
	egressAzureTestRegisterPath   = egressAzureTestRG + "/serviceGateways/sgw/updateServices"
	egressAzureTestOperationPath  = "/operations/1"
	egressAzureTestNATID          = egressAzureTestRG + "/natGateways/egress-a"
	egressAzureTestSucceededNAT   = `{"name":"egress-a","properties":{"provisioningState":"Succeeded"}}`
	egressAzureTestAcceptedResult = `{"properties":{"provisioningState":"Updating"}}`
)

// fakeARMRequest is a request the fake Azure Resource Manager received.
type fakeARMRequest struct {
	Method string
	Path   string
	// Call is "METHOD kind/name[/action]", prefixed with the resource group when it is not the cluster's.
	Call    string
	IfMatch string
	Header  http.Header
	Body    []byte
}

// String returns the call of the request.
func (r fakeARMRequest) String() string { return r.Call }

// fakeARM is the stateful fake of the Azure Resource Manager the egress tests call: NAT Gateways, Public IPs and
// prefixes by resource group, and the Service Gateway's services. Its tracker calls it through the real azclient
// clients, built by azclient.NewClientFactory with the production client options (azclient retry policy and retriable
// statuses, throttling policy, etag policy of the Public IP and prefix clients), so every call runs through the real
// azcore pipelines and pollers; only the retry waits are shortened.
//
// It answers as NRP does where the runs depend on it: a read returns the stored resource or 404; a NAT Gateway PUT
// stores the request with a fresh etag and provisioning state Succeeded and keeps a link the request leaves out (only
// JSON null clears it); a Public IP or prefix PUT stores the request with a fresh etag and provisioning state Succeeded;
// a DELETE of an absent resource succeeds; a NAT Gateway DELETE honours If-Match (412); resources in use are refused: a
// NAT Gateway linked to a Service Gateway, a Public IP or prefix a NAT Gateway references (400).
//
// A script answers every request in place of the stored state (the call layer tests); a failure answers every request
// of a call, a fault the next requests of a call; a hook changes the state once a call was served, before its answer is
// sent; a delay holds every answer back. Every write is checked against the write-ahead mark of the test identity's
// operation in the tracker (Egress.MutationStarted, set by noteEgressMutationStarted): with requireMarks, a write sent
// before the operation was marked fails the test, unless unnotedAllowed accepts its call (the writes of a BYO
// identity's link and registration are not managed writes); with consumeMarks, every write records the mark in marks
// and clears it, so that marking again before a later write is seen.
type fakeARM struct {
	t  *testing.T
	dt *DiffTracker

	mu               sync.Mutex
	nats             map[string]*armnetwork.NatGateway // by lower-cased "resourceGroup/name"
	pips             map[string]*armnetwork.PublicIPAddress
	prefixes         map[string]*armnetwork.PublicIPPrefix
	services         map[string]*armnetwork.ServiceGatewayService // by lower-cased name
	noServiceGateway bool
	etags            int
	requests         []fakeARMRequest
	script           http.HandlerFunc
	failures         map[string]http.HandlerFunc   // by lower-cased call
	faults           map[string][]http.HandlerFunc // by call
	hooks            map[string]func(f *fakeARM)
	delay            time.Duration
	inFlight, peak   map[string]int // by method
	requireMarks     bool
	unnoted          []string
	unnotedAllowed   func(call string) bool
	consumeMarks     bool
	marks            []bool // guarded by dt.mu, so it is in the order in which the writes took the mark
}

// newFakeARM returns a fake ARM that requires marks, and sets dt up to call it. Writes are checked against the
// operation of the test identity egressAzureTestIdentity in dt.
func newFakeARM(t *testing.T, dt *DiffTracker) *fakeARM {
	t.Helper()
	f := &fakeARM{
		t:            t,
		dt:           dt,
		nats:         map[string]*armnetwork.NatGateway{},
		pips:         map[string]*armnetwork.PublicIPAddress{},
		prefixes:     map[string]*armnetwork.PublicIPPrefix{},
		services:     map[string]*armnetwork.ServiceGatewayService{},
		failures:     map[string]http.HandlerFunc{},
		faults:       map[string][]http.HandlerFunc{},
		hooks:        map[string]func(f *fakeARM){},
		inFlight:     map[string]int{},
		peak:         map[string]int{},
		requireMarks: true,
	}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	endpoint := cloud.Configuration{
		ActiveDirectoryAuthorityHost: server.URL,
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Audience: "https://management.azure.com", Endpoint: server.URL},
		},
	}
	factory, err := azclient.NewClientFactory(&azclient.ClientFactoryConfig{SubscriptionID: "sub"}, nil, endpoint, fakeCredential{},
		func(options *arm.ClientOptions) {
			options.Transport = server.Client()
			options.Retry.RetryDelay = time.Millisecond
			options.Retry.MaxRetryDelay = 10 * time.Millisecond
		})
	if err != nil {
		t.Fatalf("building the client factory: %v", err)
	}
	dt.networkClientFactory = factory
	t.Cleanup(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		assert.Empty(t, f.unnoted, "writes sent before the run called noteEgressMutationStarted")
	})
	return f
}

// newScriptedARM returns a tracker of the test cluster that calls a fake ARM whose script is handler, without an
// operation and without mark checks: the call layer tests.
func newScriptedARM(t *testing.T, handler http.HandlerFunc) (*DiffTracker, *fakeARM) {
	t.Helper()
	dt := &DiffTracker{config: testConfig(), logger: log.Noop()}
	f := newFakeARM(t, dt)
	f.update(func(f *fakeARM) { f.script, f.requireMarks = handler, false })
	return dt, f
}

// takeMark returns whether the operation of the test identity is marked as writing, records it in marks, and clears
// the mark when consume is set. Takes dt.mu.
func (f *fakeARM) takeMark(consume bool) bool {
	f.dt.mu.Lock()
	defer f.dt.mu.Unlock()
	marked := false
	if op := f.dt.pendingServiceOps[egressAzureTestIdentity]; op != nil {
		marked = op.Egress.MutationStarted
		if consume {
			op.Egress.MutationStarted = false
		}
	}
	f.marks = append(f.marks, marked)
	return marked
}

func (f *fakeARM) mutationNoted() bool {
	f.dt.mu.Lock()
	defer f.dt.mu.Unlock()
	op := f.dt.pendingServiceOps[egressAzureTestIdentity]
	return op != nil && op.Egress.MutationStarted
}

func (f *fakeARM) setMutationNoted(noted bool) {
	f.dt.mu.Lock()
	defer f.dt.mu.Unlock()
	f.dt.pendingServiceOps[egressAzureTestIdentity].Egress.MutationStarted = noted
}

func fakeARMID(rg, kind, name string) string {
	return "/subscriptions/sub/resourceGroups/" + rg + "/providers/Microsoft.Network/" + kind + "/" + name
}

func fakeARMKey(rg, name string) string { return strings.ToLower(rg + "/" + name) }

// fakeARMPath splits /subscriptions/{s}/resourceGroups/{rg}/providers/Microsoft.Network/{kind}/{name}[/{action}].
func fakeARMPath(path string) (rg, kind, name, action string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 8 {
		return "", "", path, ""
	}
	return parts[3], parts[6], parts[7], strings.Join(parts[8:], "/")
}

// fakeARMCall returns the call of a request with method on path, which may be the ID of the resource.
func fakeARMCall(method, path string) string {
	rg, kind, name, action := fakeARMPath(path)
	call := kind + "/" + name
	if action != "" {
		call += "/" + action
	}
	if !strings.EqualFold(rg, testConfig().ResourceGroup) {
		call = rg + "/" + call
	}
	return method + " " + call
}

func fakeARMJSON(t *testing.T, v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		t.Errorf("marshalling %T: %v", v, err)
	}
	return data
}

func fakeARMError(status int, code string) (int, []byte) {
	return status, []byte(`{"error":{"code":"` + code + `","message":"` + code + ` from the fake ARM"}}`)
}

func (f *fakeARM) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	call := fakeARMCall(r.Method, r.URL.Path)
	write := r.Method != http.MethodGet
	f.mu.Lock()
	consume := f.consumeMarks
	f.mu.Unlock()
	marked := write && f.takeMark(consume)

	f.mu.Lock()
	f.requests = append(f.requests, fakeARMRequest{Method: r.Method, Path: r.URL.Path, Call: call, IfMatch: r.Header.Get("If-Match"),
		Header: r.Header.Clone(), Body: body})
	if write {
		if f.requireMarks && !marked && (f.unnotedAllowed == nil || !f.unnotedAllowed(call)) {
			f.unnoted = append(f.unnoted, call)
		}
	}
	f.inFlight[r.Method]++
	f.peak[r.Method] = max(f.peak[r.Method], f.inFlight[r.Method])
	delay := f.delay
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight[r.Method]--
		f.mu.Unlock()
	}()
	time.Sleep(delay)

	f.mu.Lock()
	answer := f.script
	if failure := f.failures[strings.ToLower(call)]; answer == nil && failure != nil {
		answer = failure
	}
	if faults := f.faults[call]; answer == nil && len(faults) > 0 {
		f.faults[call] = faults[1:]
		answer = faults[0]
	}
	if answer != nil {
		f.mu.Unlock()
		answer(w, r)
		return
	}
	status, data := f.route(r.Method, r.URL.Path, r.Header.Get("If-Match"), body)
	if hook := f.hooks[call]; hook != nil {
		delete(f.hooks, call)
		hook(f)
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data) //nolint:gosec // G705: a fake ARM answering with the test's own JSON
}

func (f *fakeARM) route(method, path, ifMatch string, body []byte) (int, []byte) {
	rg, kind, name, action := fakeARMPath(path)
	key := fakeARMKey(rg, name)
	switch {
	case kind == "natGateways" && action == "":
		return f.serveNAT(method, rg, name, ifMatch, body)
	case kind == "publicIPAddresses" && action == "":
		current := f.pips[key]
		switch {
		case method == http.MethodGet && current == nil:
			return fakeARMError(http.StatusNotFound, "ResourceNotFound")
		case method == http.MethodGet:
			return http.StatusOK, fakeARMJSON(f.t, current)
		case method == http.MethodDelete && current == nil:
			return http.StatusNoContent, nil
		case method == http.MethodDelete && f.referencedLocked(derefString(current.ID)):
			return fakeARMError(http.StatusBadRequest, "PublicIPAddressInUse")
		case method == http.MethodDelete:
			delete(f.pips, key)
			return http.StatusOK, []byte(`{}`)
		case method == http.MethodPut:
			var pip armnetwork.PublicIPAddress
			if json.Unmarshal(body, &pip) != nil {
				return fakeARMError(http.StatusBadRequest, "InvalidRequestFormat")
			}
			if pip.Properties == nil {
				pip.Properties = &armnetwork.PublicIPAddressPropertiesFormat{}
			}
			pip.ID, pip.Name, pip.Etag = to.Ptr(fakeARMID(rg, kind, name)), to.Ptr(name), f.nextEtagLocked()
			pip.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			f.pips[key] = &pip
			return http.StatusOK, fakeARMJSON(f.t, pip)
		}
	case kind == "publicIPPrefixes" && action == "":
		current := f.prefixes[key]
		switch {
		case method == http.MethodGet && current == nil:
			return fakeARMError(http.StatusNotFound, "ResourceNotFound")
		case method == http.MethodGet:
			return http.StatusOK, fakeARMJSON(f.t, current)
		case method == http.MethodDelete && current == nil:
			return http.StatusNoContent, nil
		case method == http.MethodDelete && f.referencedLocked(derefString(current.ID)):
			return fakeARMError(http.StatusBadRequest, "PublicIpPrefixInUse")
		case method == http.MethodDelete:
			delete(f.prefixes, key)
			return http.StatusOK, []byte(`{}`)
		case method == http.MethodPut:
			var prefix armnetwork.PublicIPPrefix
			if json.Unmarshal(body, &prefix) != nil {
				return fakeARMError(http.StatusBadRequest, "InvalidRequestFormat")
			}
			if prefix.Properties == nil {
				prefix.Properties = &armnetwork.PublicIPPrefixPropertiesFormat{}
			}
			prefix.ID, prefix.Name, prefix.Etag = to.Ptr(fakeARMID(rg, kind, name)), to.Ptr(name), f.nextEtagLocked()
			prefix.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			f.prefixes[key] = &prefix
			return http.StatusOK, fakeARMJSON(f.t, prefix)
		}
	case kind == "serviceGateways" && f.noServiceGateway:
		return fakeARMError(http.StatusNotFound, "ResourceNotFound")
	case kind == "serviceGateways" && action == "services" && method == http.MethodGet:
		var value []*armnetwork.ServiceGatewayService
		for _, name := range slices.Sorted(maps.Keys(f.services)) {
			value = append(value, f.services[name])
		}
		return http.StatusOK, fakeARMJSON(f.t, map[string]any{"value": value})
	case kind == "serviceGateways" && action == "updateServices" && method == http.MethodPost:
		var request armnetwork.ServiceGatewayUpdateServicesRequest
		if err := json.Unmarshal(body, &request); err != nil {
			return fakeARMError(http.StatusBadRequest, "InvalidRequestFormat")
		}
		for _, sr := range request.ServiceRequests {
			if sr == nil || sr.Service == nil || sr.Service.Name == nil {
				continue
			}
			if sr.IsDelete != nil && *sr.IsDelete {
				delete(f.services, strings.ToLower(*sr.Service.Name))
				continue
			}
			f.services[strings.ToLower(*sr.Service.Name)] = sr.Service
		}
		return http.StatusOK, []byte(`{}`)
	}
	return fakeARMError(http.StatusInternalServerError, "UnexpectedRequest")
}

func (f *fakeARM) serveNAT(method, rg, name, ifMatch string, body []byte) (int, []byte) {
	key := fakeARMKey(rg, name)
	current := f.nats[key]
	switch method {
	case http.MethodGet:
		if current == nil {
			return fakeARMError(http.StatusNotFound, "ResourceNotFound")
		}
		return http.StatusOK, fakeARMJSON(f.t, current)
	case http.MethodPut:
		var nat armnetwork.NatGateway
		var sent struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if json.Unmarshal(body, &nat) != nil || json.Unmarshal(body, &sent) != nil {
			return fakeARMError(http.StatusBadRequest, "InvalidRequestFormat")
		}
		if nat.Properties == nil {
			nat.Properties = &armnetwork.NatGatewayPropertiesFormat{}
		}
		if _, ok := sent.Properties["serviceGateway"]; !ok && current != nil && current.Properties != nil {
			nat.Properties.ServiceGateway = current.Properties.ServiceGateway
		}
		f.storeNATLocked(rg, name, &nat)
		return http.StatusOK, fakeARMJSON(f.t, nat)
	case http.MethodDelete:
		switch {
		case current == nil:
			return http.StatusNoContent, nil
		case ifMatch != "" && ifMatch != derefString(current.Etag):
			return fakeARMError(http.StatusPreconditionFailed, "PreconditionFailed")
		case natGatewayServiceGatewayID(current) != "":
			return fakeARMError(http.StatusConflict, "InUseNatGatewayCannotBeDeleted")
		}
		delete(f.nats, key)
		return http.StatusOK, []byte(`{}`)
	}
	return fakeARMError(http.StatusInternalServerError, "UnexpectedRequest")
}

// referencedLocked reports whether a NAT Gateway of the fake references the resource id.
func (f *fakeARM) referencedLocked(id string) bool {
	for _, nat := range f.nats {
		if nat.Properties == nil {
			continue
		}
		p := nat.Properties
		for _, ref := range slices.Concat(p.PublicIPAddresses, p.PublicIPAddressesV6, p.PublicIPPrefixes, p.PublicIPPrefixesV6) {
			if ref != nil && strings.EqualFold(derefString(ref.ID), id) {
				return true
			}
		}
	}
	return false
}

func (f *fakeARM) nextEtagLocked() *string {
	f.etags++
	return to.Ptr(fmt.Sprintf(`W/"%08d"`, f.etags))
}

func (f *fakeARM) storeNATLocked(rg, name string, nat *armnetwork.NatGateway) {
	nat.ID = to.Ptr(fakeARMID(rg, "natGateways", name))
	nat.Name = to.Ptr(name)
	nat.Type = to.Ptr("Microsoft.Network/natGateways")
	nat.Etag = f.nextEtagLocked()
	nat.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
	f.nats[fakeARMKey(rg, name)] = nat
}

// update changes the state of the fake.
func (f *fakeARM) update(fn func(f *fakeARM)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// natLocked returns the stored NAT Gateway, to be changed in place.
func (f *fakeARM) natLocked(rg, name string) *armnetwork.NatGateway {
	nat := f.nats[fakeARMKey(rg, name)]
	if nat == nil {
		f.t.Fatalf("no NAT Gateway %s/%s", rg, name)
	}
	return nat
}

// seed stores the resource with ID id exactly as given, as Azure returns it.
func (f *fakeARM) seed(t *testing.T, id string, resource any) {
	t.Helper()
	rg, kind, name, _ := fakeARMPath(id)
	data := fakeARMJSON(t, resource)
	var err error
	f.update(func(f *fakeARM) {
		key := fakeARMKey(rg, name)
		switch strings.ToLower(kind) {
		case "natgateways":
			var nat armnetwork.NatGateway
			err = json.Unmarshal(data, &nat)
			f.nats[key] = &nat
		case "publicipaddresses":
			var pip armnetwork.PublicIPAddress
			err = json.Unmarshal(data, &pip)
			f.pips[key] = &pip
		case "publicipprefixes":
			var prefix armnetwork.PublicIPPrefix
			err = json.Unmarshal(data, &prefix)
			f.prefixes[key] = &prefix
		default:
			err = fmt.Errorf("the fake ARM holds no %s", kind)
		}
	})
	if err != nil {
		t.Fatalf("seeding %s: %v", id, err)
	}
}

// stored decodes the resource with ID id, as the fake holds it, into into.
func (f *fakeARM) stored(t *testing.T, id string, into any) {
	t.Helper()
	rg, kind, name, _ := fakeARMPath(id)
	var data []byte
	f.update(func(f *fakeARM) {
		key := fakeARMKey(rg, name)
		switch strings.ToLower(kind) {
		case "natgateways":
			if nat := f.nats[key]; nat != nil {
				data = fakeARMJSON(t, nat)
			}
		case "publicipaddresses":
			if pip := f.pips[key]; pip != nil {
				data = fakeARMJSON(t, pip)
			}
		case "publicipprefixes":
			if prefix := f.prefixes[key]; prefix != nil {
				data = fakeARMJSON(t, prefix)
			}
		}
	})
	if data == nil {
		t.Fatalf("%s is not stored", id)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("decoding %s: %v", id, err)
	}
}

// seedNAT stores a NAT Gateway as Azure returns it: with its ID, type, a fresh etag, and provisioning state Succeeded
// unless it has one.
func (f *fakeARM) seedNAT(rg string, nat armnetwork.NatGateway) {
	f.update(func(f *fakeARM) {
		if nat.Properties == nil {
			nat.Properties = &armnetwork.NatGatewayPropertiesFormat{}
		}
		state := nat.Properties.ProvisioningState
		f.storeNATLocked(rg, derefString(nat.Name), &nat)
		if state != nil {
			nat.Properties.ProvisioningState = state
		}
	})
}

// seedPublicIP stores a Public IP of the cluster resource group, in provisioning state Succeeded unless it has one.
func (f *fakeARM) seedPublicIP(pip armnetwork.PublicIPAddress) {
	f.update(func(f *fakeARM) {
		name := derefString(pip.Name)
		pip.ID = to.Ptr(publicIPAddressID(name, f.dt.config))
		pip.Etag = f.nextEtagLocked()
		if pip.Properties == nil {
			pip.Properties = &armnetwork.PublicIPAddressPropertiesFormat{}
		}
		if pip.Properties.ProvisioningState == nil {
			pip.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
		}
		f.pips[fakeARMKey(f.dt.config.ResourceGroup, name)] = &pip
	})
}

// seedPrefix stores a Public IP prefix of the cluster resource group, in provisioning state Succeeded unless it has
// one.
func (f *fakeARM) seedPrefix(prefix armnetwork.PublicIPPrefix) {
	f.update(func(f *fakeARM) {
		name := derefString(prefix.Name)
		prefix.ID = to.Ptr(publicIPPrefixID(name, f.dt.config))
		prefix.Etag = f.nextEtagLocked()
		if prefix.Properties == nil {
			prefix.Properties = &armnetwork.PublicIPPrefixPropertiesFormat{}
		}
		if prefix.Properties.ProvisioningState == nil {
			prefix.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
		}
		f.prefixes[fakeARMKey(f.dt.config.ResourceGroup, name)] = &prefix
	})
}

// seedApplied stores what applying cfg to the test identity creates: its NAT Gateway, linked to the cluster's Service
// Gateway, and its managed Public IPs and prefixes, as the projections write them.
func (f *fakeARM) seedApplied(cfg EgressConfig, ipv6 bool) {
	f.t.Helper()
	dt := f.dt.config
	nat, err := natRequest(nil, egressAzureTestIdentity, cfg, ipv6, dt)
	if err != nil {
		f.t.Fatal(err)
	}
	f.seedNAT(dt.ResourceGroup, nat)
	for _, v6 := range egressServedFamilies(ipv6) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			if m.Kind == egressManagedPrefix {
				prefix, err := managedPrefixRequest(nil, egressAzureTestIdentity, v6, m.N, cfg, dt)
				if err != nil {
					f.t.Fatal(err)
				}
				f.seedPrefix(prefix)
				continue
			}
			pip, err := managedPublicIPRequest(nil, egressAzureTestIdentity, v6, m.N, cfg, dt)
			if err != nil {
				f.t.Fatal(err)
			}
			f.seedPublicIP(pip)
		}
	}
}

// register records a Service Gateway service that uses natID ("" for none).
func (f *fakeARM) register(service, natID string) {
	f.update(func(f *fakeARM) {
		registration := &armnetwork.ServiceGatewayService{
			Name:       to.Ptr(service),
			Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{ServiceType: to.Ptr(armnetwork.ServiceTypeOutbound)},
		}
		if natID != "" {
			registration.Properties.PublicNatGatewayID = to.Ptr(natID)
		}
		f.services[strings.ToLower(service)] = registration
	})
}

// registration returns the NAT Gateway the stored registration of a service references and whether it is registered.
func (f *fakeARM) registration(service string) (natID string, registered bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	registration := f.services[strings.ToLower(service)]
	if registration == nil {
		return "", false
	}
	if registration.Properties != nil {
		natID = derefString(registration.Properties.PublicNatGatewayID)
	}
	return natID, true
}

// fault answers the next requests of call with answers, one each.
func (f *fakeARM) fault(call string, answers ...http.HandlerFunc) {
	f.update(func(f *fakeARM) { f.faults[call] = append(f.faults[call], answers...) })
}

// failEvery answers every request with method for the resource with ID id with h.
func (f *fakeARM) failEvery(method, id string, h http.HandlerFunc) {
	f.update(func(f *fakeARM) { f.failures[strings.ToLower(fakeARMCall(method, id))] = h })
}

func (f *fakeARM) hook(call string, fn func(f *fakeARM)) {
	f.update(func(f *fakeARM) { f.hooks[call] = fn })
}

// nat returns a copy of a stored NAT Gateway, or nil.
func (f *fakeARM) nat(rg, name string) *armnetwork.NatGateway {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored := f.nats[fakeARMKey(rg, name)]
	if stored == nil {
		return nil
	}
	var nat armnetwork.NatGateway
	if err := json.Unmarshal(fakeARMJSON(f.t, stored), &nat); err != nil {
		f.t.Fatal(err)
	}
	return &nat
}

// resourceNames returns the names of the Public IPs and prefixes of the cluster resource group, sorted.
func (f *fakeARM) resourceNames() (pips, prefixes []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, pip := range f.pips {
		pips = append(pips, derefString(pip.Name))
	}
	for _, prefix := range f.prefixes {
		prefixes = append(prefixes, derefString(prefix.Name))
	}
	slices.Sort(pips)
	slices.Sort(prefixes)
	return pips, prefixes
}

func (f *fakeARM) received() []fakeARMRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeARM) resetRequests() {
	f.update(func(f *fakeARM) { f.requests = nil })
}

// calls returns the calls received, in order.
func (f *fakeARM) calls() []string {
	var calls []string
	for _, req := range f.received() {
		calls = append(calls, req.Call)
	}
	return calls
}

// writeRequests returns the PUT, POST and DELETE requests received, in order.
func (f *fakeARM) writeRequests() []fakeARMRequest {
	return slices.DeleteFunc(f.received(), func(req fakeARMRequest) bool { return req.Method == http.MethodGet })
}

// writes returns the PUT, POST and DELETE calls received, in order.
func (f *fakeARM) writes() []string {
	return slices.DeleteFunc(f.calls(), func(call string) bool { return strings.HasPrefix(call, http.MethodGet+" ") })
}

// requestsOf returns the requests of one call, in order.
func (f *fakeARM) requestsOf(call string) []fakeARMRequest {
	return slices.DeleteFunc(f.received(), func(req fakeARMRequest) bool { return req.Call != call })
}

// count returns how many requests with method were received for a resource of the cluster resource group ("kind/name").
func (f *fakeARM) count(method, resource string) int {
	n := 0
	for _, req := range f.received() {
		if strings.EqualFold(req.Call, method+" "+resource) {
			n++
		}
	}
	return n
}

// writesPollsReads returns how many writes (PUT, POST, DELETE), LRO polls and other reads (GET) the fake received.
func (f *fakeARM) writesPollsReads() (writes, polls, reads int) {
	for _, req := range f.received() {
		switch {
		case req.Path == egressAzureTestOperationPath:
			polls++
		case req.Method == http.MethodGet:
			reads++
		default:
			writes++
		}
	}
	return writes, polls, reads
}

// peakInFlight returns the most requests with method the fake answered at once.
func (f *fakeARM) peakInFlight(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak[method]
}

// isReservedCall reports whether a call reads or deletes a Public IP or prefix of the cluster resource group: the calls
// of a bounded phase, made in any order.
func isReservedCall(call string) bool {
	_, rest, _ := strings.Cut(call, " ")
	return strings.HasPrefix(rest, "publicIPAddresses/") || strings.HasPrefix(rest, "publicIPPrefixes/")
}

// sequence returns the calls that are not on Public IPs or prefixes of the cluster resource group, in order.
func (f *fakeARM) sequence() []string {
	return slices.DeleteFunc(f.calls(), isReservedCall)
}

// reservedCalls returns the calls on Public IPs and prefixes of the cluster resource group with the given method,
// sorted.
func (f *fakeARM) reservedCalls(method string) []string {
	var calls []string
	for _, call := range f.calls() {
		if isReservedCall(call) && strings.HasPrefix(call, method+" ") {
			calls = append(calls, call)
		}
	}
	slices.Sort(calls)
	return calls
}

func armReply(status int, body string, header ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(header); i += 2 {
			w.Header().Set(header[i], header[i+1])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body) //nolint:gosec // G705: a fake ARM answering with the test's own JSON
	}
}

func armReplyError(status int, code string) http.HandlerFunc {
	return armReply(status, fmt.Sprintf(`{"error":{"code":%q,"message":"%s from the fake ARM"}}`, code, code))
}

// armReplyAccepted answers a write as NRP does a long-running one: accepted, with its status at
// egressAzureTestOperationPath.
func armReplyAccepted(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		armReply(status, egressAzureTestAcceptedResult, "Azure-AsyncOperation", "http://"+r.Host+egressAzureTestOperationPath)(w, r)
	}
}

func armReplyOperation(status string) http.HandlerFunc {
	return armReply(http.StatusOK, fmt.Sprintf(`{"status":%q}`, status))
}

// armDropConnection closes the connection without an answer: a transport error for the client.
func armDropConnection(w http.ResponseWriter, _ *http.Request) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "cannot drop the connection", http.StatusInternalServerError)
		return
	}
	if conn, _, err := hijacker.Hijack(); err == nil {
		_ = conn.Close()
	}
}

// armFirstThen answers the first request with first and every later one with rest.
func armFirstThen(first, rest http.HandlerFunc) http.HandlerFunc {
	var calls atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			first(w, r)
			return
		}
		rest(w, r)
	}
}

// armAfter answers after a delay, or not at all when the client gives up first.
func armAfter(delay time.Duration, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
			h(w, r)
		case <-r.Context().Done():
		}
	}
}

// armRetryWouldConflict is what a retry of a write would be answered with if one were sent: the conflict an applied
// first attempt causes, which is a refusal and would hide the unknown outcome of the first attempt.
var armRetryWouldConflict = armReplyError(http.StatusConflict, "Conflict")

// armScript routes the requests of one call: the write, the LRO polls, and the other reads (GETs, including the
// final GET of a PUT). An unexpected request is answered 500.
type armScript struct{ write, poll, read http.HandlerFunc }

func (s armScript) serve(w http.ResponseWriter, r *http.Request) {
	handler := s.write
	switch {
	case r.URL.Path == egressAzureTestOperationPath:
		handler = s.poll
	case r.Method == http.MethodGet:
		handler = s.read
	}
	if handler == nil {
		handler = armReplyError(http.StatusInternalServerError, "UnexpectedRequest")
	}
	handler(w, r)
}

// armGate releases blocked handlers once.
type armGate struct {
	ch   chan struct{}
	once sync.Once
}

func newArmGate() *armGate { return &armGate{ch: make(chan struct{})} }

func (g *armGate) open() { g.once.Do(func() { close(g.ch) }) }

func setEgressCallTimeoutForTest(t *testing.T, d time.Duration) {
	old := nrpOperationTimeout.Load()
	nrpOperationTimeout.Store(int64(d))
	t.Cleanup(func() { nrpOperationTimeout.Store(old) })
}

func setEgressConcurrencyForTest(t *testing.T, n int) {
	old := egressPublicIPConcurrency.Load()
	egressPublicIPConcurrency.Store(int64(n))
	t.Cleanup(func() { egressPublicIPConcurrency.Store(old) })
}

func isThrottleError(err error) bool {
	var throttle *retryrepectthrottled.ThrottleError
	return errors.As(err, &throttle)
}

// egressCallCase is one call of the egress call layer, with the path it reads or writes.
type egressCallCase struct {
	name  string
	write bool
	path  string
	// ifMatch is the precondition the write request must carry.
	ifMatch string
	invoke  func(ctx context.Context, dt *DiffTracker) error
}

func egressCallCases() []egressCallCase {
	nat := armnetwork.NatGateway{Name: to.Ptr(egressAzureTestIdentity), Location: to.Ptr("eastus"), Etag: to.Ptr(egressAzureTestEtag)}
	pip := armnetwork.PublicIPAddress{Name: to.Ptr("egress-a-pip"), Location: to.Ptr("eastus"), Etag: to.Ptr(egressAzureTestEtag)}
	prefix := armnetwork.PublicIPPrefix{Name: to.Ptr("egress-a-prefix-31"), Location: to.Ptr("eastus"), Etag: to.Ptr(egressAzureTestEtag)}
	registration := armnetwork.ServiceGatewayService{Name: to.Ptr(egressAzureTestIdentity), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
		ServiceType: to.Ptr(armnetwork.ServiceTypeOutbound), PublicNatGatewayID: to.Ptr(egressAzureTestNATID)}}
	return []egressCallCase{
		{name: "get NAT Gateway", path: egressAzureTestNATPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, _, err := dt.getEgressNAT(ctx, egressAzureTestIdentity)
			return err
		}},
		{name: "read NAT Gateway", path: egressAzureTestNATPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.readEgressNAT(ctx, egressAzureTestIdentity)
			return err
		}},
		{name: "get Public IP", path: egressAzureTestPIPPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, _, err := dt.getEgressPublicIP(ctx, "rg", "egress-a-pip")
			return err
		}},
		{name: "get Public IP prefix", path: egressAzureTestPrefixPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, _, err := dt.getEgressPrefix(ctx, "rg", "egress-a-prefix-31")
			return err
		}},
		{name: "get services", path: egressAzureTestServicesPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.getEgressServices(ctx)
			return err
		}},
		{name: "put NAT Gateway", write: true, path: egressAzureTestNATPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressNAT(ctx, nat)
			return err
		}},
		{name: "put NAT Gateway link", write: true, path: egressAzureTestNATPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressNATLink(ctx, &nat)
			return err
		}},
		{name: "delete NAT Gateway", write: true, path: egressAzureTestNATPath, ifMatch: egressAzureTestEtag, invoke: func(ctx context.Context, dt *DiffTracker) error {
			return dt.deleteEgressNAT(ctx, egressAzureTestIdentity, egressAzureTestEtag)
		}},
		{name: "put Public IP", write: true, path: egressAzureTestPIPPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressPublicIP(ctx, pip)
			return err
		}},
		{name: "delete Public IP", write: true, path: egressAzureTestPIPPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			return dt.deleteEgressPublicIP(ctx, "egress-a-pip")
		}},
		{name: "put Public IP prefix", write: true, path: egressAzureTestPrefixPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressPrefix(ctx, prefix)
			return err
		}},
		{name: "delete Public IP prefix", write: true, path: egressAzureTestPrefixPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			return dt.deleteEgressPrefix(ctx, "egress-a-prefix-31")
		}},
		{name: "register", write: true, path: egressAzureTestRegisterPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			return dt.registerEgressIdentity(ctx, egressAzureTestIdentity)
		}},
		{name: "clear the NAT Gateway of the registration", write: true, path: egressAzureTestRegisterPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			return dt.unlinkEgressRegistration(ctx, registration)
		}},
		{name: "unregister", write: true, path: egressAzureTestRegisterPath, invoke: func(ctx context.Context, dt *DiffTracker) error {
			return dt.unregisterEgressIdentity(ctx, egressAzureTestIdentity)
		}},
	}
}

func TestEgressSettled(t *testing.T) {
	state := func(s armnetwork.ProvisioningState) *armnetwork.ProvisioningState { return &s }
	for _, tc := range []struct {
		name               string
		state              *armnetwork.ProvisioningState
		settled, succeeded bool
	}{
		{name: "no state", state: nil, settled: true},
		{name: "Succeeded", state: state(armnetwork.ProvisioningStateSucceeded), settled: true, succeeded: true},
		{name: "Failed", state: state(armnetwork.ProvisioningStateFailed), settled: true},
		{name: "Canceled", state: state(armnetwork.ProvisioningStateCanceled), settled: true},
		{name: "Creating", state: state(armnetwork.ProvisioningStateCreating)},
		{name: "Updating", state: state(armnetwork.ProvisioningStateUpdating)},
		{name: "Deleting", state: state(armnetwork.ProvisioningStateDeleting)},
		{name: "updating in another case", state: state("updating")},
		{name: "succeeded in another case", state: state("succeeded"), settled: true, succeeded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.settled, egressSettled(tc.state), "settled")
			assert.Equal(t, tc.succeeded, provisioningSucceeded(tc.state), "succeeded")
		})
	}
	for _, value := range armnetwork.PossibleProvisioningStateValues() {
		s := state(value)
		assert.False(t, provisioningSucceeded(s) && !egressSettled(s), "a state that satisfies a configuration must be settled: %s", value)
	}
}

func TestClassifyEgressWrite(t *testing.T) {
	responseError := func(status int, method string, code string) *azcore.ResponseError {
		req, err := http.NewRequest(method, "https://management.azure.com"+egressAzureTestNATPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		return &azcore.ResponseError{StatusCode: status, ErrorCode: code, RawResponse: &http.Response{StatusCode: status, Request: req}}
	}
	for _, tc := range []struct {
		name      string
		err       error
		want      egressWriteOutcome
		status    int
		errorCode string
	}{
		{name: "success", err: nil, want: egressWriteSucceeded},
		{name: "PUT 400", err: responseError(http.StatusBadRequest, http.MethodPut, "InvalidRequestFormat"), want: egressWriteRefused, status: 400, errorCode: "unknown"},
		{name: "PUT 400 quota", err: responseError(http.StatusBadRequest, http.MethodPut, "PublicIPCountLimitReached"), want: egressWriteRefused, status: 400, errorCode: "quota_exceeded"},
		{name: "PUT 403", err: responseError(http.StatusForbidden, http.MethodPut, "AuthorizationFailed"), want: egressWriteRefused, status: 403, errorCode: "unknown"},
		{name: "PUT 404", err: responseError(http.StatusNotFound, http.MethodPut, "ResourceGroupNotFound"), want: egressWriteRefused, status: 404, errorCode: "not_found"},
		{name: "PUT 409", err: responseError(http.StatusConflict, http.MethodPut, "AnotherOperationInProgress"), want: egressWriteRefused, status: 409, errorCode: "conflict"},
		{name: "DELETE 412", err: responseError(http.StatusPreconditionFailed, http.MethodDelete, "PreconditionFailed"), want: egressWriteRefused, status: 412, errorCode: "unknown"},
		{name: "POST 400 wrapped", err: fmt.Errorf("registering: %w", responseError(http.StatusBadRequest, http.MethodPost, "InvalidRequest")), want: egressWriteRefused, status: 400, errorCode: "unknown"},
		// The production pipeline turns a 429 into a bare ThrottleError (below); a 429 the write itself got as a
		// response error is a refusal like any other 4xx.
		{name: "PUT 429 response error", err: responseError(http.StatusTooManyRequests, http.MethodPut, "TooManyRequests"), want: egressWriteRefused, status: 429, errorCode: "throttled"},
		{name: "PUT 408", err: responseError(http.StatusRequestTimeout, http.MethodPut, ""), want: egressWriteUnknown},
		{name: "PUT 500", err: responseError(http.StatusInternalServerError, http.MethodPut, ""), want: egressWriteUnknown},
		{name: "PUT 503", err: responseError(http.StatusServiceUnavailable, http.MethodPut, ""), want: egressWriteUnknown},
		{name: "PUT 202", err: responseError(http.StatusAccepted, http.MethodPut, ""), want: egressWriteUnknown},
		{name: "POST 200 naming an error code", err: responseError(http.StatusOK, http.MethodPost, "OperationFailed"), want: egressWriteUnknown},
		{name: "poll GET 404", err: responseError(http.StatusNotFound, http.MethodGet, "NotFound"), want: egressWriteUnknown},
		{name: "poll GET 403", err: responseError(http.StatusForbidden, http.MethodGet, "AuthorizationFailed"), want: egressWriteUnknown},
		{name: "PATCH 409", err: responseError(http.StatusConflict, http.MethodPatch, "Conflict"), want: egressWriteUnknown},
		{name: "no response", err: &azcore.ResponseError{StatusCode: http.StatusConflict, ErrorCode: "Conflict"}, want: egressWriteUnknown},
		{name: "no request", err: &azcore.ResponseError{StatusCode: http.StatusConflict, RawResponse: &http.Response{StatusCode: http.StatusConflict}}, want: egressWriteUnknown},
		{name: "bare ThrottleError", err: &retryrepectthrottled.ThrottleError{RetryAfter: time.Now()}, want: egressWriteUnknown},
		{name: "wrapped ThrottleError", err: fmt.Errorf("creating: %w", &retryrepectthrottled.ThrottleError{}), want: egressWriteUnknown},
		{name: "deadline", err: fmt.Errorf("creating: %w", context.DeadlineExceeded), want: egressWriteUnknown},
		{name: "cancellation", err: context.Canceled, want: egressWriteUnknown},
		{name: "transport", err: errors.New(`Put "https://management.azure.com/x": EOF`), want: egressWriteUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyEgressWrite(tc.err)
			assert.Equal(t, tc.want, got.Outcome)
			assert.Equal(t, tc.status, got.StatusCode)
			assert.Equal(t, tc.errorCode, got.ErrorCode)
			assert.Equal(t, tc.err, got.Err)
		})
	}
	assert.Equal(t, egressWriteUnknown, egressWriteResult{}.Outcome, "an unset result is never conclusive")
}

// TestClassifyEgressSyncWrite: a synchronous write (the Service Gateway's UpdateServices) that the azclient policies
// throttled was not applied, so it is a refusal, with 429, which the backoff retries: a ThrottleError, raised for a 429
// answer or for a block before sending, as no poll follows the write, or the client-side rate limit. Every other error
// is classified as for any write; a ThrottleError of a write that polls stays commit unknown (TestClassifyEgressWrite).
func TestClassifyEgressSyncWrite(t *testing.T) {
	post := func(status int, code string) *azcore.ResponseError {
		req, err := http.NewRequest(http.MethodPost, "https://management.azure.com"+egressAzureTestRegisterPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		return &azcore.ResponseError{StatusCode: status, ErrorCode: code, RawResponse: &http.Response{StatusCode: status, Request: req}}
	}
	for _, tc := range []struct {
		name      string
		err       error
		want      egressWriteOutcome
		status    int
		throttled bool
	}{
		{name: "bare ThrottleError", err: &retryrepectthrottled.ThrottleError{RetryAfter: time.Now()}, want: egressWriteRefused, status: 429, throttled: true},
		{name: "wrapped ThrottleError", err: fmt.Errorf("updating Service Gateway services: %w", &retryrepectthrottled.ThrottleError{}),
			want: egressWriteRefused, status: 429, throttled: true},
		{name: "client-side rate limit", err: fmt.Errorf("updating Service Gateway services: %w", ratelimit.ErrRateLimitReached),
			want: egressWriteRefused, status: 429, throttled: true},
		{name: "success", want: egressWriteSucceeded},
		{name: "POST 409", err: post(http.StatusConflict, "Conflict"), want: egressWriteRefused, status: 409},
		{name: "POST 404", err: post(http.StatusNotFound, "NotFound"), want: egressWriteRefused, status: 404},
		{name: "POST 500", err: post(http.StatusInternalServerError, ""), want: egressWriteUnknown},
		{name: "POST 200 naming an error code", err: post(http.StatusOK, "OperationFailed"), want: egressWriteUnknown},
		{name: "deadline", err: fmt.Errorf("updating Service Gateway services: %w", context.DeadlineExceeded), want: egressWriteUnknown},
		{name: "transport", err: errors.New(`Post "https://management.azure.com/x": EOF`), want: egressWriteUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyEgressSyncWrite(tc.err)
			assert.Equal(t, tc.want, got.Outcome)
			assert.Equal(t, tc.status, got.StatusCode)
			assert.Equal(t, tc.err, got.Err)
			if tc.throttled {
				assert.Equal(t, egressWriteUnknown, classifyEgressWrite(tc.err).Outcome, "control: commit unknown for a write that polls")
			} else {
				assert.Equal(t, classifyEgressWrite(tc.err), got, "classified as any write")
			}
		})
	}
}

// TestEgressWriteClassificationThroughRealPipelines drives NAT Gateway PUTs through the production pipeline. Each
// write that is retried would meet the conflict its applied first attempt causes (armRetryWouldConflict), so a retry
// hiding an unanswered attempt shows as a refusal and as a second request.
func TestEgressWriteClassificationThroughRealPipelines(t *testing.T) {
	succeeded := armReplyOperation("Succeeded")
	resource := armReply(http.StatusOK, egressAzureTestSucceededNAT)
	for _, tc := range []struct {
		name                 string
		script               armScript
		want                 egressWriteOutcome
		status               int
		throttled            bool
		writes, polls, reads int
	}{
		{name: "initial 400 is a refusal", script: armScript{write: armReplyError(http.StatusBadRequest, "InvalidRequestFormat")}, want: egressWriteRefused, status: 400, writes: 1},
		{name: "initial 403 is a refusal", script: armScript{write: armReplyError(http.StatusForbidden, "AuthorizationFailed")}, want: egressWriteRefused, status: 403, writes: 1},
		{name: "initial 404 is a refusal", script: armScript{write: armReplyError(http.StatusNotFound, "ResourceGroupNotFound")}, want: egressWriteRefused, status: 404, writes: 1},
		{name: "initial 409 is a refusal", script: armScript{write: armReplyError(http.StatusConflict, "AnotherOperationInProgress")}, want: egressWriteRefused, status: 409, writes: 1},
		{name: "initial 412 is a refusal", script: armScript{write: armReplyError(http.StatusPreconditionFailed, "PreconditionFailed")}, want: egressWriteRefused, status: 412, writes: 1},
		// The azclient throttling policy turns the 429 into a bare ThrottleError, which carries no request.
		{name: "initial 429 surfaces as a ThrottleError: commit unknown", script: armScript{write: armReplyError(http.StatusTooManyRequests, "TooManyRequests")}, want: egressWriteUnknown, throttled: true, writes: 1},
		{name: "initial 408 is commit unknown and not retried", script: armScript{write: armFirstThen(armReplyError(http.StatusRequestTimeout, "RequestTimeout"), armRetryWouldConflict)}, want: egressWriteUnknown, writes: 1},
		{name: "initial 500 is commit unknown and not retried", script: armScript{write: armFirstThen(armReplyError(http.StatusInternalServerError, "InternalServerError"), armRetryWouldConflict)}, want: egressWriteUnknown, writes: 1},
		{name: "initial 503 is commit unknown and not retried", script: armScript{write: armFirstThen(armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"), armRetryWouldConflict)}, want: egressWriteUnknown, writes: 1},
		{name: "transport error is commit unknown and not retried", script: armScript{write: armFirstThen(armDropConnection, armRetryWouldConflict)}, want: egressWriteUnknown, writes: 1},
		{name: "poll 404 is commit unknown", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: armReplyError(http.StatusNotFound, "NotFound")}, want: egressWriteUnknown, writes: 1, polls: 1},
		{name: "poll 403 is commit unknown", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: armReplyError(http.StatusForbidden, "AuthorizationFailed")}, want: egressWriteUnknown, writes: 1, polls: 1},
		{name: "poll 408 is commit unknown and not retried", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: armFirstThen(armReplyError(http.StatusRequestTimeout, "RequestTimeout"), succeeded), read: resource}, want: egressWriteUnknown, writes: 1, polls: 1},
		{name: "poll 429 surfaces as a ThrottleError: commit unknown", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: armReplyError(http.StatusTooManyRequests, "TooManyRequests")}, want: egressWriteUnknown, throttled: true, writes: 1, polls: 1},
		{name: "failed operation is commit unknown", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: armReplyOperation("Failed")}, want: egressWriteUnknown, writes: 1, polls: 1},
		{name: "final GET 404 is commit unknown", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: succeeded, read: armReplyError(http.StatusNotFound, "NotFound")}, want: egressWriteUnknown, writes: 1, polls: 1, reads: 1},
		{name: "final GET 500 is commit unknown and not retried", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: succeeded, read: armFirstThen(armReplyError(http.StatusInternalServerError, "InternalServerError"), resource)}, want: egressWriteUnknown, writes: 1, polls: 1, reads: 1},
		{name: "synchronous success", script: armScript{write: resource}, want: egressWriteSucceeded, writes: 1},
		{name: "asynchronous success", script: armScript{write: armReplyAccepted(http.StatusCreated), poll: succeeded, read: resource}, want: egressWriteSucceeded, writes: 1, polls: 1, reads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, fake := newScriptedARM(t, tc.script.serve)
			_, err := dt.putEgressNAT(context.Background(), armnetwork.NatGateway{Name: to.Ptr(egressAzureTestIdentity), Location: to.Ptr("eastus")})

			got := classifyEgressWrite(err)
			assert.Equal(t, tc.want, got.Outcome, "err: %v", err)
			assert.Equal(t, tc.status, got.StatusCode)
			assert.Equal(t, tc.throttled, isThrottleError(err), "bare ThrottleError: %v", err)
			writes, polls, reads := fake.writesPollsReads()
			assert.Equal(t, []int{tc.writes, tc.polls, tc.reads}, []int{writes, polls, reads}, "writes, polls and reads sent")
		})
	}
}

func TestEgressDeleteClassificationThroughRealPipelines(t *testing.T) {
	deletes := map[string]func(context.Context, *DiffTracker) error{
		"NAT Gateway": func(ctx context.Context, dt *DiffTracker) error {
			return dt.deleteEgressNAT(ctx, egressAzureTestIdentity, egressAzureTestEtag)
		},
		"Public IP": func(ctx context.Context, dt *DiffTracker) error { return dt.deleteEgressPublicIP(ctx, "egress-a-pip") },
		"Public IP prefix": func(ctx context.Context, dt *DiffTracker) error {
			return dt.deleteEgressPrefix(ctx, "egress-a-prefix-31")
		},
	}
	type deleteCase struct {
		name          string
		script        armScript
		want          egressWriteOutcome
		status        int
		writes, polls int
	}
	// cases returns new handlers for every delete: armFirstThen counts the requests it answers.
	cases := func() []deleteCase {
		return []deleteCase{
			{name: "deleted synchronously", script: armScript{write: armReply(http.StatusOK, `{}`)}, want: egressWriteSucceeded, writes: 1},
			{name: "already gone", script: armScript{write: armReply(http.StatusNoContent, ``)}, want: egressWriteSucceeded, writes: 1},
			{name: "deleted asynchronously, without a final GET", script: armScript{write: armReplyAccepted(http.StatusAccepted), poll: armReplyOperation("Succeeded")}, want: egressWriteSucceeded, writes: 1, polls: 1},
			{name: "DELETE 404: absent", script: armScript{write: armReplyError(http.StatusNotFound, "NotFound")}, want: egressWriteSucceeded, writes: 1},
			{name: "DELETE 409 is a refusal", script: armScript{write: armReplyError(http.StatusConflict, "InUseNatGatewayCannotBeDeleted")}, want: egressWriteRefused, status: 409, writes: 1},
			{name: "DELETE 412 is a refusal", script: armScript{write: armReplyError(http.StatusPreconditionFailed, "PreconditionFailed")}, want: egressWriteRefused, status: 412, writes: 1},
			{name: "DELETE 503 is commit unknown and not retried", script: armScript{write: armFirstThen(armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"), armRetryWouldConflict)}, want: egressWriteUnknown, writes: 1},
			{name: "transport error is commit unknown and not retried", script: armScript{write: armFirstThen(armDropConnection, armRetryWouldConflict)}, want: egressWriteUnknown, writes: 1},
			{name: "poll 404 is commit unknown, not absent", script: armScript{write: armReplyAccepted(http.StatusAccepted), poll: armReplyError(http.StatusNotFound, "NotFound")}, want: egressWriteUnknown, writes: 1, polls: 1},
			{name: "poll 429 is commit unknown", script: armScript{write: armReplyAccepted(http.StatusAccepted), poll: armReplyError(http.StatusTooManyRequests, "TooManyRequests")}, want: egressWriteUnknown, writes: 1, polls: 1},
		}
	}
	for kind, del := range deletes {
		for _, tc := range cases() {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				dt, fake := newScriptedARM(t, tc.script.serve)
				err := del(context.Background(), dt)

				got := classifyEgressWrite(err)
				assert.Equal(t, tc.want, got.Outcome, "err: %v", err)
				assert.Equal(t, tc.status, got.StatusCode)
				writes, polls, reads := fake.writesPollsReads()
				assert.Equal(t, []int{tc.writes, tc.polls, 0}, []int{writes, polls, reads}, "writes, polls and reads sent")
			})
		}
	}
}

// TestEgressRegistrationClassificationThroughRealPipelines drives the Service Gateway's writes through the production
// pipeline. They are synchronous, so classifyEgressSyncWrite classifies them: a 429 is a refusal, not commit unknown.
func TestEgressRegistrationClassificationThroughRealPipelines(t *testing.T) {
	registration := armnetwork.ServiceGatewayService{Name: to.Ptr(egressAzureTestIdentity), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
		ServiceType: to.Ptr(armnetwork.ServiceTypeOutbound), PublicNatGatewayID: to.Ptr(egressAzureTestNATID)}}
	posts := map[string]func(context.Context, *DiffTracker) error{
		"register": func(ctx context.Context, dt *DiffTracker) error {
			return dt.registerEgressIdentity(ctx, egressAzureTestIdentity)
		},
		"clear the NAT Gateway of the registration": func(ctx context.Context, dt *DiffTracker) error {
			return dt.unlinkEgressRegistration(ctx, registration)
		},
		"unregister": func(ctx context.Context, dt *DiffTracker) error {
			return dt.unregisterEgressIdentity(ctx, egressAzureTestIdentity)
		},
	}
	type postCase struct {
		name      string
		write     http.HandlerFunc
		want      egressWriteOutcome
		status    int
		throttled bool
	}
	// cases returns new handlers for every write: armFirstThen counts the requests it answers.
	cases := func() []postCase {
		return []postCase{
			{name: "200 is a success", write: armReply(http.StatusOK, `{}`), want: egressWriteSucceeded},
			{name: "200 naming an error code is commit unknown", write: armReply(http.StatusOK, `{}`, "x-ms-error-code", "OperationFailed"), want: egressWriteUnknown},
			{name: "202 is commit unknown", write: armReply(http.StatusAccepted, `{}`), want: egressWriteUnknown},
			{name: "400 is a refusal", write: armReplyError(http.StatusBadRequest, "InvalidRequest"), want: egressWriteRefused, status: 400},
			{name: "404 is a refusal", write: armReplyError(http.StatusNotFound, "ResourceNotFound"), want: egressWriteRefused, status: 404},
			{name: "409 is a refusal", write: armReplyError(http.StatusConflict, "Conflict"), want: egressWriteRefused, status: 409},
			{name: "429 surfaces as a ThrottleError: a refusal", write: armReplyError(http.StatusTooManyRequests, "TooManyRequests"),
				want: egressWriteRefused, status: 429, throttled: true},
			{name: "503 is commit unknown and not retried", write: armFirstThen(armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"), armRetryWouldConflict), want: egressWriteUnknown},
			{name: "transport error is commit unknown and not retried", write: armFirstThen(armDropConnection, armRetryWouldConflict), want: egressWriteUnknown},
		}
	}
	for kind, post := range posts {
		for _, tc := range cases() {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				dt, fake := newScriptedARM(t, armScript{write: tc.write}.serve)
				err := post(context.Background(), dt)

				got := classifyEgressSyncWrite(err)
				assert.Equal(t, tc.want, got.Outcome, "err: %v", err)
				assert.Equal(t, tc.status, got.StatusCode)
				assert.Equal(t, tc.throttled, isThrottleError(err), "bare ThrottleError: %v", err)
				writes, polls, reads := fake.writesPollsReads()
				assert.Equal(t, []int{1, 0, 0}, []int{writes, polls, reads}, "writes, polls and reads sent")
			})
		}
	}
}

// TestEgressThrottledSyncWriteIsRefusedBeforeItIsSent: a Service Gateway write answered 429 with Retry-After, and the
// next one, which the azclient throttling policy blocks before it is sent, are both refusals: nothing was applied.
func TestEgressThrottledSyncWriteIsRefusedBeforeItIsSent(t *testing.T) {
	dt, fake := newScriptedARM(t, armScript{write: armReply(http.StatusTooManyRequests, `{}`, "Retry-After", "60")}.serve)

	err := dt.registerEgressIdentity(context.Background(), egressAzureTestIdentity)
	assert.True(t, isThrottleError(err), "the 429 on the write: %v", err)
	assert.Equal(t, egressWriteRefused, classifyEgressSyncWrite(err).Outcome)

	err = dt.unregisterEgressIdentity(context.Background(), egressAzureTestIdentity)
	assert.True(t, isThrottleError(err), "the write blocked before sending: %v", err)
	assert.Equal(t, egressWriteRefused, classifyEgressSyncWrite(err).Outcome)
	writes, _, _ := fake.writesPollsReads()
	assert.Equal(t, 1, writes, "the second write was blocked before it was sent")
}

// TestEgressThrottledWriteIsCommitUnknownBeforeItIsSent pins the third source of a bare ThrottleError: after a 429
// with Retry-After, the azclient throttling policy blocks the next write of the client locally.
func TestEgressThrottledWriteIsCommitUnknownBeforeItIsSent(t *testing.T) {
	dt, fake := newScriptedARM(t, armScript{write: armReply(http.StatusTooManyRequests, `{}`, "Retry-After", "60")}.serve)
	pip := armnetwork.PublicIPAddress{Name: to.Ptr("egress-a-pip"), Location: to.Ptr("eastus")}

	_, err := dt.putEgressPublicIP(context.Background(), pip)
	assert.True(t, isThrottleError(err), "the 429 on the write: %v", err)
	assert.Equal(t, egressWriteUnknown, classifyEgressWrite(err).Outcome)

	_, err = dt.putEgressPublicIP(context.Background(), pip)
	assert.True(t, isThrottleError(err), "the write blocked before sending: %v", err)
	assert.Equal(t, egressWriteUnknown, classifyEgressWrite(err).Outcome)
	writes, _, _ := fake.writesPollsReads()
	assert.Equal(t, 1, writes, "the second write was blocked before it was sent")
}

// TestEgressWritesAreSingleAttempt: no write is retried by the SDK, so the only attempt is the classified one.
func TestEgressWritesAreSingleAttempt(t *testing.T) {
	for _, call := range egressCallCases() {
		if !call.write {
			continue
		}
		for name, first := range map[string]http.HandlerFunc{
			"503":             armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"),
			"408":             armReplyError(http.StatusRequestTimeout, "RequestTimeout"),
			"transport error": armDropConnection,
		} {
			t.Run(call.name+"/"+name, func(t *testing.T) {
				dt, fake := newScriptedARM(t, armFirstThen(first, armRetryWouldConflict))
				err := call.invoke(context.Background(), dt)

				assert.Equal(t, egressWriteUnknown, classifyEgressWrite(err).Outcome, "err: %v", err)
				requests := fake.received()
				if assert.Len(t, requests, 1, "only one attempt is sent") {
					assert.Equal(t, call.path, requests[0].Path)
				}
			})
		}
	}
}

// TestEgressReadsKeepTheClientRetries: reads are not single-attempt; the client's retry policy still applies.
func TestEgressReadsKeepTheClientRetries(t *testing.T) {
	for _, call := range egressCallCases() {
		if call.write {
			continue
		}
		t.Run(call.name, func(t *testing.T) {
			dt, fake := newScriptedARM(t, armFirstThen(armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"), armReply(http.StatusOK, `{}`)))
			assert.NoError(t, call.invoke(context.Background(), dt))

			requests := fake.received()
			if assert.Len(t, requests, 2, "the read was retried after the 503") {
				for _, req := range requests {
					assert.Equal(t, http.MethodGet, req.Method)
					assert.Equal(t, call.path, req.Path)
				}
			}
		})
	}
}

// TestEgressCallsEachHaveABudget: every call ends within its own getNRPOperationTimeout() budget, even when Azure
// never answers; a write that runs out of it is commit unknown.
func TestEgressCallsEachHaveABudget(t *testing.T) {
	setEgressCallTimeoutForTest(t, 200*time.Millisecond)
	for _, call := range egressCallCases() {
		t.Run(call.name, func(t *testing.T) {
			gate := newArmGate()
			dt, _ := newScriptedARM(t, func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-gate.ch:
					armReplyError(http.StatusInternalServerError, "Released")(w, r)
				}
			})
			t.Cleanup(gate.open)

			done := make(chan error, 1)
			go func() { done <- call.invoke(context.Background(), dt) }()
			select {
			case err := <-done:
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				if call.write {
					assert.Equal(t, egressWriteUnknown, classifyEgressWrite(err).Outcome)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("the call did not end within its budget")
				gate.open()
				<-done
			}
		})
	}
}

// TestEgressPhaseCallBudgetsStartWithEachCall: the calls of a phase run one after the other here, together longer
// than one budget; each still has a full budget of its own.
func TestEgressPhaseCallBudgetsStartWithEachCall(t *testing.T) {
	setEgressCallTimeoutForTest(t, time.Second)
	setEgressConcurrencyForTest(t, 1)
	dt, fake := newScriptedARM(t, armAfter(600*time.Millisecond, armReply(http.StatusOK, `{}`)))
	names := []string{"egress-a-pip", "egress-a-pip-2", "egress-a-pip-3"}

	start := time.Now()
	results := runEgressPhase(context.Background(), names, func(ctx context.Context, name string) error {
		_, err := dt.putEgressPublicIP(ctx, armnetwork.PublicIPAddress{Name: to.Ptr(name), Location: to.Ptr("eastus")})
		return err
	})

	assert.Greater(t, time.Since(start), getNRPOperationTimeout(), "the phase took longer than one budget")
	outcome := egressPhaseOutcome(results)
	assert.Equal(t, egressWriteSucceeded, outcome.Outcome, "a slow call shortened the budget of another: %v", outcome.Err)
	writes, _, _ := fake.writesPollsReads()
	assert.Equal(t, len(names), writes)
}

// TestEgressOnlyTheNATDeleteIsConditional: the NAT Gateway DELETE carries If-Match with the etag it is given; no other
// request does, and PUTs send no etag even when the request holds one (the Public IP and prefix clients would turn it
// into If-Match).
func TestEgressOnlyTheNATDeleteIsConditional(t *testing.T) {
	for _, call := range egressCallCases() {
		t.Run(call.name, func(t *testing.T) {
			dt, fake := newScriptedARM(t, armScript{
				write: func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodDelete {
						armReplyAccepted(http.StatusAccepted)(w, r)
						return
					}
					armReply(http.StatusOK, `{}`)(w, r)
				},
				poll: armReplyOperation("Succeeded"),
				read: armReply(http.StatusOK, `{}`),
			}.serve)
			assert.NoError(t, call.invoke(context.Background(), dt))

			requests := fake.received()
			assert.NotEmpty(t, requests)
			for _, req := range requests {
				ifMatch := req.Header.Get("If-Match")
				switch {
				case req.Path == call.path && req.Method != http.MethodGet:
					assert.Equal(t, call.ifMatch, ifMatch, "If-Match of the %s request", req.Method)
				case req.Path == egressAzureTestOperationPath:
					// The azclient wrapper polls with the context of the call, so a conditional DELETE's header
					// reaches its polls too, where NRP ignores it (live, design 4.4).
					assert.Equal(t, call.ifMatch, ifMatch, "If-Match of the poll")
				default:
					assert.Empty(t, ifMatch, "If-Match of the %s %s request", req.Method, req.Path)
				}
				if req.Method == http.MethodPut {
					var body map[string]any
					assert.NoError(t, json.Unmarshal(req.Body, &body))
					assert.NotContains(t, body, "etag", "a PUT carries no precondition")
				}
				if req.Method == http.MethodDelete {
					assert.False(t, slices.ContainsFunc(requests, func(r fakeARMRequest) bool {
						return r.Method == http.MethodGet && r.Path == call.path
					}), "a DELETE poller makes no final GET of the resource")
				}
			}
		})
	}
}

func TestDeleteEgressNATNeedsAnEtag(t *testing.T) {
	dt, fake := newScriptedARM(t, armReply(http.StatusOK, `{}`))
	assert.Error(t, dt.deleteEgressNAT(context.Background(), egressAzureTestIdentity, ""))
	assert.Empty(t, fake.received(), "an unconditional NAT Gateway DELETE is never sent")
}

func TestEgressPutsSendTheRequestToTheClusterResourceGroup(t *testing.T) {
	existingNAT := &armnetwork.NatGateway{
		Name:     to.Ptr(egressAzureTestIdentity),
		ID:       to.Ptr(egressAzureTestNATID),
		Location: to.Ptr("eastus"),
		Etag:     to.Ptr(egressAzureTestEtag),
		SKU:      &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandardV2)},
		Tags:     map[string]*string{"owner": to.Ptr("team")},
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			ProvisioningState:    to.Ptr(armnetwork.ProvisioningStateSucceeded),
			IdleTimeoutInMinutes: to.Ptr[int32](10),
			PublicIPAddresses:    []*armnetwork.SubResource{{ID: to.Ptr(egressAzureTestRG + "/publicIPAddresses/egress-a-pip")}},
			Subnets:              []*armnetwork.SubResource{{ID: to.Ptr("/subnet")}},
		},
	}
	pip := armnetwork.PublicIPAddress{
		Name: to.Ptr("egress-a-pip-2"), Location: to.Ptr("eastus"), Etag: to.Ptr(egressAzureTestEtag),
		SKU:        &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandardV2)},
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic)},
	}
	prefix := armnetwork.PublicIPPrefix{
		Name: to.Ptr("egress-a-prefix-30"), Location: to.Ptr("eastus"), Etag: to.Ptr(egressAzureTestEtag),
		Properties: &armnetwork.PublicIPPrefixPropertiesFormat{PrefixLength: to.Ptr[int32](30)},
	}
	withoutEtag := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		delete(m, "etag")
		b, err = json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	for _, tc := range []struct {
		name string
		path string
		want string
		put  func(context.Context, *DiffTracker) error
	}{
		{name: "NAT Gateway", path: egressAzureTestNATPath, want: withoutEtag(existingNAT), put: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressNAT(ctx, *existingNAT)
			return err
		}},
		{name: "NAT Gateway link", path: egressAzureTestNATPath, want: withoutEtag(natLinkRequest(existingNAT, testConfig())), put: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressNATLink(ctx, existingNAT)
			return err
		}},
		{name: "Public IP", path: egressAzureTestRG + "/publicIPAddresses/egress-a-pip-2", want: withoutEtag(pip), put: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressPublicIP(ctx, pip)
			return err
		}},
		{name: "Public IP prefix", path: egressAzureTestRG + "/publicIPPrefixes/egress-a-prefix-30", want: withoutEtag(prefix), put: func(ctx context.Context, dt *DiffTracker) error {
			_, err := dt.putEgressPrefix(ctx, prefix)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, fake := newScriptedARM(t, armReply(http.StatusOK, `{}`))
			assert.NoError(t, tc.put(context.Background(), dt))

			requests := fake.received()
			if assert.Len(t, requests, 1) {
				assert.Equal(t, http.MethodPut, requests[0].Method)
				assert.Equal(t, tc.path, requests[0].Path)
				assert.JSONEq(t, tc.want, string(requests[0].Body))
			}
		})
	}
	assert.Equal(t, egressAzureTestEtag, derefString(pip.Etag), "the caller's request is not changed")

	cfg := testConfig()
	link := natLinkRequest(existingNAT, cfg)
	if assert.NotNil(t, link.Properties.ServiceGateway) {
		assert.Equal(t, cfg.ServiceGatewayResourceID(), derefString(link.Properties.ServiceGateway.ID), "the link PUT links the NAT Gateway")
	}
}

func TestEgressPutsReturnTheWrittenResource(t *testing.T) {
	dt, _ := newScriptedARM(t, armScript{
		write: armReplyAccepted(http.StatusCreated),
		poll:  armReplyOperation("Succeeded"),
		read:  armReply(http.StatusOK, `{"name":"egress-a","etag":"W/\"2\"","properties":{"provisioningState":"Succeeded","idleTimeoutInMinutes":30}}`),
	}.serve)
	written, err := dt.putEgressNAT(context.Background(), armnetwork.NatGateway{Name: to.Ptr(egressAzureTestIdentity)})
	if assert.NoError(t, err) && assert.NotNil(t, written) && assert.NotNil(t, written.Properties) {
		assert.Equal(t, int32(30), *written.Properties.IdleTimeoutInMinutes, "the NAT Gateway as written, from the final GET")
		assert.True(t, provisioningSucceeded(written.Properties.ProvisioningState))
	}
}

func TestEgressWritesRefuseEmptyNames(t *testing.T) {
	dt, fake := newScriptedARM(t, armReply(http.StatusOK, `{}`))
	ctx := context.Background()
	_, err := dt.putEgressNAT(ctx, armnetwork.NatGateway{})
	assert.Error(t, err)
	_, err = dt.putEgressNATLink(ctx, nil)
	assert.Error(t, err)
	assert.Error(t, dt.deleteEgressNAT(ctx, "", egressAzureTestEtag))
	_, err = dt.putEgressPublicIP(ctx, armnetwork.PublicIPAddress{})
	assert.Error(t, err)
	assert.Error(t, dt.deleteEgressPublicIP(ctx, ""))
	_, err = dt.putEgressPrefix(ctx, armnetwork.PublicIPPrefix{})
	assert.Error(t, err)
	assert.Error(t, dt.deleteEgressPrefix(ctx, ""))
	assert.Empty(t, fake.received())
}

func TestEgressReadsReturnSettledAbsence(t *testing.T) {
	ctx := context.Background()
	t.Run("absent", func(t *testing.T) {
		dt, fake := newScriptedARM(t, armReplyError(http.StatusNotFound, "NotFound"))
		nat, exists, err := dt.getEgressNAT(ctx, egressAzureTestIdentity)
		assert.NoError(t, err)
		assert.False(t, exists)
		assert.Nil(t, nat)
		pip, exists, err := dt.getEgressPublicIP(ctx, "user-rg", "user-pip")
		assert.NoError(t, err)
		assert.False(t, exists)
		assert.Nil(t, pip)
		prefix, exists, err := dt.getEgressPrefix(ctx, "rg", "egress-a-prefix-31")
		assert.NoError(t, err)
		assert.False(t, exists)
		assert.Nil(t, prefix)
		services, err := dt.getEgressServices(ctx)
		assert.NoError(t, err, "a missing Service Gateway has no registrations")
		assert.Empty(t, services)

		var paths []string
		for _, req := range fake.received() {
			paths = append(paths, req.Path)
		}
		assert.Equal(t, []string{egressAzureTestNATPath, "/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/publicIPAddresses/user-pip",
			egressAzureTestPrefixPath, egressAzureTestServicesPath}, paths, "a 404 is not retried")
	})
	t.Run("present", func(t *testing.T) {
		dt, _ := newScriptedARM(t, armReply(http.StatusOK, `{"name":"x","etag":"W/\"7\"","properties":{"provisioningState":"Updating"}}`))
		nat, exists, err := dt.getEgressNAT(ctx, egressAzureTestIdentity)
		assert.NoError(t, err)
		if assert.True(t, exists) {
			assert.Equal(t, `W/"7"`, derefString(nat.Etag))
			assert.False(t, egressSettled(nat.Properties.ProvisioningState))
		}
		pip, exists, err := dt.getEgressPublicIP(ctx, "rg", "egress-a-pip")
		assert.NoError(t, err)
		if assert.True(t, exists) {
			assert.False(t, egressSettled(pip.Properties.ProvisioningState))
		}
		prefix, exists, err := dt.getEgressPrefix(ctx, "rg", "egress-a-prefix-31")
		assert.NoError(t, err)
		if assert.True(t, exists) {
			assert.False(t, egressSettled(prefix.Properties.ProvisioningState))
		}
	})
	t.Run("error", func(t *testing.T) {
		dt, _ := newScriptedARM(t, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		_, exists, err := dt.getEgressNAT(ctx, egressAzureTestIdentity)
		assert.Error(t, err)
		assert.False(t, exists)
		_, err = dt.getEgressServices(ctx)
		assert.Error(t, err)
	})
}

// TestEgressRegistrationOf: the registration of an identity is found among the services of every page, by its name in
// any case.
func TestEgressRegistrationOf(t *testing.T) {
	page2 := egressAzureTestRG + "/serviceGateways/sgw/services/page2"
	for _, tc := range []struct {
		name         string
		page1, page2 string
		natGatewayID string
		registered   bool
	}{
		{
			name:         "registered with its NAT Gateway",
			page1:        `{"value":[{"name":"inbound-svc","properties":{"serviceType":"Inbound"}},{"name":"EGRESS-A","properties":{"serviceType":"Outbound","publicNatGatewayId":"` + egressAzureTestNATID + `"}}]}`,
			natGatewayID: egressAzureTestNATID, registered: true,
		},
		{
			name:       "registered without a NAT Gateway",
			page1:      `{"value":[{"name":"egress-a","properties":{"serviceType":"Outbound"}}]}`,
			registered: true,
		},
		{
			name:  "not registered",
			page1: `{"value":[{"name":"egress-b","properties":{"serviceType":"Outbound","publicNatGatewayId":"` + egressAzureTestRG + `/natGateways/egress-b"}}]}`,
		},
		{
			name:         "registered on a later page",
			page1:        `{"value":[{"name":"egress-b","properties":{"serviceType":"Outbound"}}],"nextLink":"NEXT"}`,
			page2:        `{"value":[{"name":"egress-a","properties":{"serviceType":"Outbound","publicNatGatewayId":"` + egressAzureTestNATID + `"}}]}`,
			natGatewayID: egressAzureTestNATID, registered: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, _ := newScriptedARM(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == page2 {
					armReply(http.StatusOK, tc.page2)(w, r)
					return
				}
				armReply(http.StatusOK, strings.ReplaceAll(tc.page1, "NEXT", "http://"+r.Host+page2))(w, r)
			})
			services, err := dt.getEgressServices(context.Background())
			assert.NoError(t, err)
			registration := egressRegistrationOf(services, egressAzureTestIdentity)
			assert.Equal(t, tc.registered, registration != nil)
			if registration != nil {
				assert.Equal(t, tc.natGatewayID, derefString(registration.Properties.PublicNatGatewayID))
			}
		})
	}
}

// TestRegisterEgressIdentitySendsTheCreationRequest: the registration a link repair sends again is the one the
// identity's creation sent before configuration existed, with its managed NAT Gateway in the network subscription.
func TestRegisterEgressIdentitySendsTheCreationRequest(t *testing.T) {
	separateNetwork := testConfig()
	separateNetwork.NetworkResourceSubscriptionID = "netsub"
	for name, cfg := range map[string]Config{"same subscription": testConfig(), "separate network subscription": separateNetwork} {
		t.Run(name, func(t *testing.T) {
			creationDTO := MapLoadBalancerAndNATGatewayUpdatesToServicesDataDTO(SyncServicesReturnType{},
				SyncServicesReturnType{Additions: newIgnoreCaseSetFromSlice([]string{egressAzureTestIdentity})},
				cfg.networkResourceSubscriptionID(), cfg.ResourceGroup)
			serviceRequests, err := convertServiceDTOsToServiceRequests(creationDTO.Services, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(armnetwork.ServiceGatewayUpdateServicesRequest{
				Action:          convertServicesUpdateActionToARM(creationDTO.Action),
				ServiceRequests: serviceRequests,
			})
			if err != nil {
				t.Fatal(err)
			}

			dt, fake := newScriptedARM(t, armReply(http.StatusOK, `{}`))
			dt.config = cfg
			assert.NoError(t, dt.registerEgressIdentity(context.Background(), egressAzureTestIdentity))

			requests := fake.received()
			if assert.Len(t, requests, 1) {
				assert.Equal(t, http.MethodPost, requests[0].Method)
				assert.Equal(t, egressAzureTestRegisterPath, requests[0].Path)
				assert.JSONEq(t, string(want), string(requests[0].Body))
				assert.Contains(t, string(requests[0].Body), natGatewayID(egressAzureTestIdentity, cfg), "registered with its managed NAT Gateway")
			}
		})
	}
	assert.Equal(t, egressAzureTestNATID, natGatewayID(egressAzureTestIdentity, testConfig()), "control: the managed NAT Gateway of the test cluster")
}

// TestUnlinkEgressRegistrationClearsOnlyItsNATGateway: the NAT Gateway reference of a registration is cleared with
// the partial update the managed deletion always sent: the registration as read with an explicit null NAT Gateway (an
// absent field would change nothing). The registration the caller read is left as it was.
func TestUnlinkEgressRegistrationClearsOnlyItsNATGateway(t *testing.T) {
	registration := armnetwork.ServiceGatewayService{Name: to.Ptr(egressAzureTestIdentity), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
		ServiceType: to.Ptr(armnetwork.ServiceTypeOutbound), PublicNatGatewayID: to.Ptr(egressAzureTestNATID)}}
	dt, fake := newScriptedARM(t, armReply(http.StatusOK, `{}`))
	assert.NoError(t, dt.unlinkEgressRegistration(context.Background(), registration))

	requests := fake.received()
	if assert.Len(t, requests, 1) {
		assert.Equal(t, http.MethodPost, requests[0].Method)
		assert.Equal(t, egressAzureTestRegisterPath, requests[0].Path)
		assert.JSONEq(t, `{"action":"PartialUpdate","serviceRequests":[{"isDelete":false,"service":{"name":"egress-a",`+
			`"properties":{"publicNatGatewayId":null,"serviceType":"Outbound"}}}]}`, string(requests[0].Body))
	}
	assert.Equal(t, egressAzureTestNATID, derefString(registration.Properties.PublicNatGatewayID), "the registration as read is not changed")
}

// TestUnregisterEgressIdentitySendsTheRemoval: the registration of an identity is removed with the request its
// deletion always sent.
func TestUnregisterEgressIdentitySendsTheRemoval(t *testing.T) {
	cfg := testConfig()
	removal := buildServiceGatewayRemovalDTO(egressAzureTestIdentity, false, cfg)
	serviceRequests, err := convertServiceDTOsToServiceRequests(removal.Services, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(armnetwork.ServiceGatewayUpdateServicesRequest{
		Action:          convertServicesUpdateActionToARM(removal.Action),
		ServiceRequests: serviceRequests,
	})
	if err != nil {
		t.Fatal(err)
	}

	dt, fake := newScriptedARM(t, armReply(http.StatusOK, `{}`))
	assert.NoError(t, dt.unregisterEgressIdentity(context.Background(), egressAzureTestIdentity))

	requests := fake.received()
	if assert.Len(t, requests, 1) {
		assert.Equal(t, http.MethodPost, requests[0].Method)
		assert.Equal(t, egressAzureTestRegisterPath, requests[0].Path)
		assert.JSONEq(t, string(want), string(requests[0].Body))
		assert.Contains(t, string(requests[0].Body), `"isDelete":true`, "a removal")
	}
}

func TestRunEgressPhaseBoundsTheCallsInFlight(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		items int
		want  int
	}{
		{name: "default bound", items: 20, want: defaultEgressPublicIPConcurrency},
		{name: "lowered bound", limit: 3, items: 10, want: 3},
		{name: "fewer items than the bound", items: 5, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.limit > 0 {
				setEgressConcurrencyForTest(t, tc.limit)
			}
			var inFlight, peak, started atomic.Int32
			release := newArmGate()
			t.Cleanup(release.open)
			done := make(chan []egressCallResult, 1)
			go func() {
				done <- runEgressPhase(context.Background(), make([]struct{}, tc.items), func(context.Context, struct{}) error {
					n := inFlight.Add(1)
					defer inFlight.Add(-1)
					for {
						p := peak.Load()
						if n <= p || peak.CompareAndSwap(p, n) {
							break
						}
					}
					started.Add(1)
					<-release.ch
					return nil
				})
			}()

			deadline := time.Now().Add(10 * time.Second)
			for started.Load() < int32(tc.want) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
			assert.Equal(t, int32(tc.want), started.Load(), "calls started while %d are in flight", tc.want)
			release.open()

			results := <-done
			assert.Len(t, results, tc.items)
			assert.Equal(t, int32(tc.want), peak.Load(), "most calls in flight at once")
		})
	}
}

func TestRunEgressPhaseWaitsForEveryCallAndReturnsEveryResult(t *testing.T) {
	setEgressConcurrencyForTest(t, 2)
	refusal := &azcore.ResponseError{StatusCode: http.StatusConflict, RawResponse: &http.Response{
		StatusCode: http.StatusConflict, Request: &http.Request{Method: http.MethodPut},
	}}
	errs := []error{nil, refusal, errors.New("transport"), nil, context.DeadlineExceeded}
	want := []egressWriteOutcome{egressWriteSucceeded, egressWriteRefused, egressWriteUnknown, egressWriteSucceeded, egressWriteUnknown}
	var slowEnded atomic.Bool

	results := runEgressPhase(context.Background(), []int{0, 1, 2, 3, 4}, func(_ context.Context, i int) error {
		if i == 0 {
			time.Sleep(200 * time.Millisecond)
			slowEnded.Store(true)
		}
		return errs[i]
	})

	assert.True(t, slowEnded.Load(), "the phase returned before its slow call ended")
	if assert.Len(t, results, len(errs)) {
		for i, result := range results {
			assert.Equal(t, i, result.Index)
			assert.Equal(t, want[i], result.Outcome, "result %d", i)
			assert.Equal(t, errs[i], result.Err, "result %d", i)
		}
	}
	assert.Equal(t, 2, egressPhaseOutcome(results).Index, "the first commit-unknown call")
	assert.Empty(t, runEgressPhase(context.Background(), nil, func(context.Context, int) error { return nil }))
}

func TestRunEgressPhaseRaisesAPanicAfterEveryCallEnded(t *testing.T) {
	var slowEnded atomic.Bool
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		runEgressPhase(context.Background(), []int{0, 1}, func(_ context.Context, i int) error {
			if i == 0 {
				panic("boom")
			}
			time.Sleep(200 * time.Millisecond)
			slowEnded.Store(true)
			return nil
		})
	}()
	if assert.NotNil(t, recovered, "the panic reaches the caller") {
		assert.Contains(t, fmt.Sprint(recovered), "boom")
	}
	assert.True(t, slowEnded.Load(), "the panic was raised before every call ended")
}

func TestEgressPhaseOutcome(t *testing.T) {
	result := func(index int, outcome egressWriteOutcome, status int) egressCallResult {
		return egressCallResult{Index: index, egressWriteResult: egressWriteResult{Outcome: outcome, StatusCode: status}}
	}
	success := result(-1, egressWriteSucceeded, 0)
	for _, tc := range []struct {
		name    string
		results []egressCallResult
		want    egressCallResult
	}{
		{name: "no calls", want: success},
		{name: "all succeeded", results: []egressCallResult{result(0, egressWriteSucceeded, 0), result(1, egressWriteSucceeded, 0)}, want: success},
		{name: "a refusal", results: []egressCallResult{result(0, egressWriteSucceeded, 0), result(1, egressWriteRefused, 409)}, want: result(1, egressWriteRefused, 409)},
		{name: "the first refusal", results: []egressCallResult{result(0, egressWriteRefused, 412), result(1, egressWriteRefused, 409)}, want: result(0, egressWriteRefused, 412)},
		{name: "commit unknown after a refusal", results: []egressCallResult{result(0, egressWriteRefused, 409), result(1, egressWriteUnknown, 0)}, want: result(1, egressWriteUnknown, 0)},
		{name: "commit unknown before a refusal", results: []egressCallResult{result(0, egressWriteUnknown, 0), result(1, egressWriteRefused, 409)}, want: result(0, egressWriteUnknown, 0)},
		{name: "commit unknown and success", results: []egressCallResult{result(0, egressWriteSucceeded, 0), result(1, egressWriteUnknown, 0)}, want: result(1, egressWriteUnknown, 0)},
		{name: "the first commit unknown", results: []egressCallResult{result(0, egressWriteRefused, 409), result(1, egressWriteUnknown, 0), result(2, egressWriteUnknown, 0)}, want: result(1, egressWriteUnknown, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, egressPhaseOutcome(tc.results))
		})
	}
}

// TestEgressPhaseThroughRealPipelines: a phase waits for its slow calls, and commit unknown dominates a refusal
// whichever ends first.
func TestEgressPhaseThroughRealPipelines(t *testing.T) {
	ok := armReply(http.StatusOK, `{}`)
	conflict := armReplyError(http.StatusConflict, "AnotherOperationInProgress")
	for _, tc := range []struct {
		name          string
		first, second http.HandlerFunc
		want          egressWriteOutcome
		index, status int
	}{
		{name: "refused, then slow commit unknown", first: conflict, second: armAfter(150*time.Millisecond, armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable")), want: egressWriteUnknown, index: 1},
		{name: "slow refusal after commit unknown", first: armAfter(150*time.Millisecond, conflict), second: armDropConnection, want: egressWriteUnknown, index: 1},
		{name: "refused and slow success", first: conflict, second: armAfter(150*time.Millisecond, ok), want: egressWriteRefused, index: 0, status: 409},
		{name: "commit unknown and success", first: armReplyError(http.StatusInternalServerError, "InternalServerError"), second: ok, want: egressWriteUnknown, index: 0},
		{name: "all succeeded", first: ok, second: armAfter(150*time.Millisecond, ok), want: egressWriteSucceeded, index: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, fake := newScriptedARM(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/egress-a-pip") {
					tc.first(w, r)
					return
				}
				tc.second(w, r)
			})
			results := runEgressPhase(context.Background(), []string{"egress-a-pip", "egress-a-pip-2"}, func(ctx context.Context, name string) error {
				_, err := dt.putEgressPublicIP(ctx, armnetwork.PublicIPAddress{Name: to.Ptr(name), Location: to.Ptr("eastus")})
				return err
			})

			assert.Len(t, results, 2)
			outcome := egressPhaseOutcome(results)
			assert.Equal(t, tc.want, outcome.Outcome, "err: %v", outcome.Err)
			assert.Equal(t, tc.index, outcome.Index)
			assert.Equal(t, tc.status, outcome.StatusCode)
			writes, _, _ := fake.writesPollsReads()
			assert.Equal(t, 2, writes, "each write sent once")
		})
	}
}
