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
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/ratelimit"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/retryrepectthrottled"
	"sigs.k8s.io/cloud-provider-azure/pkg/util/errutils"
)

// egressReadContext returns the context of one Azure read of a managed egress run. The call gets its own
// getNRPOperationTimeout() budget, taken when it starts, so a slow call never shortens another one; the client's
// retries apply.
func egressReadContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, getNRPOperationTimeout())
}

// egressWriteContext returns the context of one Azure write (PUT, POST or DELETE) of a managed egress run: its own
// budget, and no SDK retry for any request of the call. The one attempt sent is then the one classifyEgressWrite
// classifies, so an attempt Azure did not answer can never hide behind the answer to a retry; the engine's backoff
// retries the run instead. The LRO polls and the final GET of a PUT share the call's context, so they are not retried
// either: their failure leaves the write commit unknown.
func egressWriteContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := egressReadContext(parent)
	// A negative MaxRetries means no retry (azcore runtime/policy_retry.go setDefaults).
	return policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1}), cancel
}

// egressSettled reports whether a provisioning state read from Azure is settled: not Creating, Updating or Deleting.
// Succeeded, Failed and Canceled are settled, and so is a missing state. A run decides only from settled reads.
func egressSettled(state *armnetwork.ProvisioningState) bool {
	if state == nil {
		return true
	}
	for _, unsettled := range []armnetwork.ProvisioningState{
		armnetwork.ProvisioningStateCreating, armnetwork.ProvisioningStateUpdating, armnetwork.ProvisioningStateDeleting,
	} {
		if strings.EqualFold(string(*state), string(unsettled)) {
			return false
		}
	}
	return true
}

// egressWriteOutcome is how one Azure write of a managed egress run ended.
type egressWriteOutcome int

const (
	// egressWriteUnknown means Azure may still apply the write (commit unknown). It is the zero value, so a result
	// that was never filled in is not taken for a conclusive one.
	egressWriteUnknown egressWriteOutcome = iota
	// egressWriteSucceeded means Azure returned success: the write is committed.
	egressWriteSucceeded
	// egressWriteRefused means Azure refused the write request itself: nothing was applied.
	egressWriteRefused
)

func (o egressWriteOutcome) String() string {
	switch o {
	case egressWriteUnknown:
		return "CommitUnknown"
	case egressWriteSucceeded:
		return "Succeeded"
	case egressWriteRefused:
		return "Refused"
	default:
		return fmt.Sprintf("egressWriteOutcome(%d)", int(o))
	}
}

// egressWriteResult is the classified end of one Azure write.
type egressWriteResult struct {
	Outcome egressWriteOutcome
	// StatusCode and ErrorCode describe a refusal: the HTTP status Azure answered the write request with and its
	// canonical error code (extractAzureErrorInfo). They are zero for the other outcomes.
	StatusCode int
	ErrorCode  string
	// Err is the error of the call; nil on success.
	Err error
}

// classifyEgressWrite classifies the error of one Azure write. A nil error is a conclusive success. A write is
// conclusively refused only when Azure answered the write request itself (PUT, POST or DELETE) with a 4xx other than
// 408. Everything else is commit unknown: the failure of an LRO poll or of the final GET of a PUT (GET requests,
// returned by the azclient wrappers like the failure of the write itself), a response error without its response or
// request, a bare ThrottleError of the azclient throttling policy (a 429 on the write or on a poll, or a local block
// before sending, which cannot be told apart; classifyEgressSyncWrite tells them apart for a synchronous write), 408,
// 5xx and other statuses, transport errors, deadlines and cancellations.
func classifyEgressWrite(err error) egressWriteResult {
	if err == nil {
		return egressWriteResult{Outcome: egressWriteSucceeded}
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && writeRequestRefused(respErr) {
		_, errorCode := extractAzureErrorInfo(err)
		return egressWriteResult{Outcome: egressWriteRefused, StatusCode: respErr.StatusCode, ErrorCode: errorCode, Err: err}
	}
	return egressWriteResult{Outcome: egressWriteUnknown, Err: err}
}

// classifyEgressSyncWrite classifies the error of one synchronous Azure write: a single request with no long-running
// operation behind it, as the Service Gateway's UpdateServices is (armnetwork v12: no poll, no final read). It is
// classifyEgressWrite, except for a write the azclient policies throttled: the throttling policy's ThrottleError (a
// 429 answer to the write, or a block before sending it; no poll can follow) and the client-side rate limit (a block
// before sending) mean that nothing was applied. Such a write is refused, with 429, and the run's backoff retries it.
func classifyEgressSyncWrite(err error) egressWriteResult {
	var throttle *retryrepectthrottled.ThrottleError
	if errors.As(err, &throttle) || errors.Is(err, ratelimit.ErrRateLimitReached) {
		return egressWriteResult{Outcome: egressWriteRefused, StatusCode: http.StatusTooManyRequests, Err: err}
	}
	return classifyEgressWrite(err)
}

// writeRequestRefused reports whether a response error is Azure's conclusive refusal of a write request.
func writeRequestRefused(respErr *azcore.ResponseError) bool {
	if respErr.RawResponse == nil || respErr.RawResponse.Request == nil {
		return false
	}
	switch respErr.RawResponse.Request.Method {
	case http.MethodPut, http.MethodPost, http.MethodDelete:
	default:
		return false
	}
	return respErr.StatusCode >= http.StatusBadRequest && respErr.StatusCode < http.StatusInternalServerError &&
		respErr.StatusCode != http.StatusRequestTimeout
}

// egressCallResult is the classified result of one call of a phase.
type egressCallResult struct {
	// Index is the position of the call's item in the phase; -1 for the success of a whole phase.
	Index int
	egressWriteResult
}

// runEgressPhase calls fn for every item, at most getEgressPublicIPConcurrency() calls at a time, waits for every call
// it started and returns the classified result of each call, in item order. The budget of a call is taken by the
// wrapper it uses when the call starts, so waiting for a slot or for a slow call never shortens it. A panic in a call
// is raised again on the caller's goroutine once every call has ended, so the ServiceUpdater worker recovers it as
// any other and no call outlives the phase.
func runEgressPhase[T any](ctx context.Context, items []T, fn func(context.Context, T) error) []egressCallResult {
	results := make([]egressCallResult, len(items))
	slots := make(chan struct{}, max(1, getEgressPublicIPConcurrency()))
	var (
		wg        sync.WaitGroup
		panicOnce sync.Once
		panicErr  error
	)
	for i, item := range items {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("panic in egress Azure call %d: %v\n%s", i, r, debug.Stack())
					panicOnce.Do(func() { panicErr = err })
				}
			}()
			results[i] = egressCallResult{Index: i, egressWriteResult: classifyEgressWrite(fn(ctx, item))}
		}()
	}
	wg.Wait()
	if panicErr != nil {
		panic(panicErr)
	}
	return results
}

// egressPhaseOutcome folds the results of a write phase: the first commit-unknown result if there is one (it
// dominates: the run ends commit unknown), else the first refusal, else success. In a read phase only Err matters: a
// failed read is never a refusal and never means that anything was written.
func egressPhaseOutcome(results []egressCallResult) egressCallResult {
	refused := -1
	for i := range results {
		switch results[i].Outcome {
		case egressWriteUnknown:
			return results[i]
		case egressWriteRefused:
			if refused < 0 {
				refused = i
			}
		}
	}
	if refused >= 0 {
		return results[refused]
	}
	return egressCallResult{Index: -1, egressWriteResult: egressWriteResult{Outcome: egressWriteSucceeded}}
}

// egressRead returns a read resource, or exists false for a resource that is absent (404), a settled read.
func egressRead[T any](resource *T, err error, kind, resourceGroup, name string) (*T, bool, error) {
	if err != nil {
		if found, cerr := errutils.CheckResourceExistsFromAzcoreError(err); cerr == nil && !found {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("getting %s %q in resource group %q: %w", kind, name, resourceGroup, err)
	}
	return resource, true, nil
}

// egressDeleted returns the error of a DELETE, nil when the resource is gone: deleted, or the DELETE request was
// refused with 404. A 404 of an LRO poll stays an error (commit unknown).
func egressDeleted(err error, kind, name string) error {
	if err == nil {
		return nil
	}
	if result := classifyEgressWrite(err); result.Outcome == egressWriteRefused && result.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("deleting %s %q: %w", kind, name, err)
}

// getEgressNAT reads the NAT Gateway of an egress identity in the cluster resource group.
func (dt *DiffTracker) getEgressNAT(ctx context.Context, name string) (*armnetwork.NatGateway, bool, error) {
	ctx, cancel := egressReadContext(ctx)
	defer cancel()
	return dt.getNatGateway(ctx, dt.config.ResourceGroup, name)
}

// egressNATRead is a read of the NAT Gateway of an egress identity in the cluster resource group, with what a managed
// egress run decides from it.
type egressNATRead struct {
	// NAT is the NAT Gateway as read; nil when it does not exist.
	NAT *armnetwork.NatGateway
	// Exists is set when the NAT Gateway exists.
	Exists bool
	// Settled is set when the read is settled (natReadSettled): the NAT Gateway does not exist (404), or is not being
	// created, updated or deleted.
	Settled bool
	// Ours is set when the NAT Gateway exists and is the controller's (ownsNATGateway).
	Ours bool
	// Etag is the etag of the read, the precondition (If-Match) of a NAT Gateway DELETE decided from it.
	Etag string
}

// readEgressNAT reads the NAT Gateway of egress identity id (getEgressNAT) and tells whether the read is settled and
// the NAT Gateway the controller's. A failed read is only an error: nothing is decided from it.
func (dt *DiffTracker) readEgressNAT(ctx context.Context, id string) (egressNATRead, error) {
	nat, exists, err := dt.getEgressNAT(ctx, id)
	switch {
	case err != nil:
		return egressNATRead{}, err
	case !exists:
		return egressNATRead{Settled: true}, nil
	}
	read := egressNATRead{NAT: nat, Exists: true, Settled: natReadSettled(nat), Ours: ownsNATGateway(nat, dt.config)}
	if nat != nil {
		read.Etag = derefString(nat.Etag)
	}
	return read, nil
}

// putEgressNAT creates or updates a NAT Gateway in the cluster resource group with one attempt and returns it as
// written. PUTs carry no precondition: an etag in the request is dropped.
func (dt *DiffTracker) putEgressNAT(ctx context.Context, nat armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
	name := derefString(nat.Name)
	if name == "" {
		return nil, errors.New("putEgressNAT: NAT Gateway name is empty")
	}
	nat.Etag = nil
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	dt.logger.V(2).Info("Creating or updating NAT Gateway", "natGateway", name, "resourceGroup", dt.config.ResourceGroup)
	written, err := dt.networkClientFactory.GetNatGatewayClient().CreateOrUpdate(ctx, dt.config.ResourceGroup, name, nat)
	if err != nil {
		return nil, fmt.Errorf("creating or updating NAT Gateway %q: %w", name, err)
	}
	return written, nil
}

// putEgressNATLink links a settled NAT Gateway to the cluster's Service Gateway with a PUT that changes nothing else
// (natLinkRequest).
func (dt *DiffTracker) putEgressNATLink(ctx context.Context, existing *armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
	return dt.putEgressNAT(ctx, natLinkRequest(existing, dt.config))
}

// deleteEgressNAT deletes a NAT Gateway of the cluster resource group with one attempt, on condition that it still
// has the etag of the settled read the deletion was decided from (If-Match), so a late DELETE can never remove a NAT
// Gateway created again since: Azure refuses it with 412. The header reaches the LRO polls as well, where NRP ignores
// it; a DELETE poller makes no final GET of the resource. It returns nil once the NAT Gateway is gone, also when it
// did not exist.
func (dt *DiffTracker) deleteEgressNAT(ctx context.Context, name, etag string) error {
	if name == "" {
		return errors.New("deleteEgressNAT: NAT Gateway name is empty")
	}
	if etag == "" {
		return fmt.Errorf("deleting NAT Gateway %q: no etag to make the delete conditional", name)
	}
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	ctx = policy.WithHTTPHeader(ctx, http.Header{"If-Match": []string{etag}})
	dt.logger.V(2).Info("Deleting NAT Gateway", "natGateway", name, "resourceGroup", dt.config.ResourceGroup, "ifMatch", etag)
	return egressDeleted(dt.networkClientFactory.GetNatGatewayClient().Delete(ctx, dt.config.ResourceGroup, name), "NAT Gateway", name)
}

// getEgressPublicIP reads a Public IP address: a managed one in the cluster resource group or a user one.
func (dt *DiffTracker) getEgressPublicIP(ctx context.Context, resourceGroup, name string) (*armnetwork.PublicIPAddress, bool, error) {
	ctx, cancel := egressReadContext(ctx)
	defer cancel()
	pip, err := dt.networkClientFactory.GetPublicIPAddressClient().Get(ctx, resourceGroup, name, nil)
	return egressRead(pip, err, "Public IP", resourceGroup, name)
}

// putEgressPublicIP creates or updates a Public IP address in the cluster resource group with one attempt and returns
// it as written. PUTs carry no precondition: an etag in the request is dropped, so the client's etag policy adds no
// If-Match.
func (dt *DiffTracker) putEgressPublicIP(ctx context.Context, pip armnetwork.PublicIPAddress) (*armnetwork.PublicIPAddress, error) {
	name := derefString(pip.Name)
	if name == "" {
		return nil, errors.New("putEgressPublicIP: Public IP name is empty")
	}
	pip.Etag = nil
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	dt.logger.V(2).Info("Creating or updating public IP", "pip", name, "resourceGroup", dt.config.ResourceGroup)
	written, err := dt.networkClientFactory.GetPublicIPAddressClient().CreateOrUpdate(ctx, dt.config.ResourceGroup, name, pip)
	if err != nil {
		return nil, fmt.Errorf("creating or updating public IP %q: %w", name, err)
	}
	return written, nil
}

// deleteEgressPublicIP deletes a Public IP address of the cluster resource group with one attempt. It returns nil once
// the Public IP is gone, also when it did not exist.
func (dt *DiffTracker) deleteEgressPublicIP(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("deleteEgressPublicIP: Public IP name is empty")
	}
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	dt.logger.V(2).Info("Deleting public IP", "pip", name, "resourceGroup", dt.config.ResourceGroup)
	return egressDeleted(dt.networkClientFactory.GetPublicIPAddressClient().Delete(ctx, dt.config.ResourceGroup, name), "public IP", name)
}

// getEgressPrefix reads a Public IP prefix: a managed one in the cluster resource group or a user one.
func (dt *DiffTracker) getEgressPrefix(ctx context.Context, resourceGroup, name string) (*armnetwork.PublicIPPrefix, bool, error) {
	ctx, cancel := egressReadContext(ctx)
	defer cancel()
	prefix, err := dt.networkClientFactory.GetPublicIPPrefixClient().Get(ctx, resourceGroup, name, nil)
	return egressRead(prefix, err, "Public IP prefix", resourceGroup, name)
}

// putEgressPrefix creates or updates a Public IP prefix in the cluster resource group with one attempt and returns it
// as written. PUTs carry no precondition: an etag in the request is dropped, so the client's etag policy adds no
// If-Match.
func (dt *DiffTracker) putEgressPrefix(ctx context.Context, prefix armnetwork.PublicIPPrefix) (*armnetwork.PublicIPPrefix, error) {
	name := derefString(prefix.Name)
	if name == "" {
		return nil, errors.New("putEgressPrefix: Public IP prefix name is empty")
	}
	prefix.Etag = nil
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	dt.logger.V(2).Info("Creating or updating Public IP prefix", "prefix", name, "resourceGroup", dt.config.ResourceGroup)
	written, err := dt.networkClientFactory.GetPublicIPPrefixClient().CreateOrUpdate(ctx, dt.config.ResourceGroup, name, prefix)
	if err != nil {
		return nil, fmt.Errorf("creating or updating Public IP prefix %q: %w", name, err)
	}
	return written, nil
}

// deleteEgressPrefix deletes a Public IP prefix of the cluster resource group with one attempt. It returns nil once the
// prefix is gone, also when it did not exist.
func (dt *DiffTracker) deleteEgressPrefix(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("deleteEgressPrefix: Public IP prefix name is empty")
	}
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	dt.logger.V(2).Info("Deleting Public IP prefix", "prefix", name, "resourceGroup", dt.config.ResourceGroup)
	return egressDeleted(dt.networkClientFactory.GetPublicIPPrefixClient().Delete(ctx, dt.config.ResourceGroup, name), "Public IP prefix", name)
}

// getEgressServices reads the services registered with the Service Gateway. A Service Gateway that does not exist has
// none.
func (dt *DiffTracker) getEgressServices(ctx context.Context) ([]*armnetwork.ServiceGatewayService, error) {
	ctx, cancel := egressReadContext(ctx)
	defer cancel()
	services, err := dt.networkClientFactory.GetServiceGatewayClient().GetServices(ctx, dt.config.ResourceGroup, dt.config.ServiceGatewayResourceName)
	if err != nil {
		if found, cerr := errutils.CheckResourceExistsFromAzcoreError(err); cerr == nil && !found {
			return nil, nil
		}
		return nil, fmt.Errorf("getting Service Gateway services: %w", err)
	}
	return services, nil
}

// egressRegistrationOf returns the registration of an egress identity among services; nil when it is not registered.
func egressRegistrationOf(services []*armnetwork.ServiceGatewayService, identity string) *armnetwork.ServiceGatewayService {
	for _, service := range services {
		if service != nil && strings.EqualFold(derefString(service.Name), identity) {
			return service
		}
	}
	return nil
}

// registerEgressIdentity registers an egress identity with the Service Gateway as an outbound service that uses its
// managed NAT Gateway, once. It is idempotent. The write is synchronous: classifyEgressSyncWrite classifies its error.
func (dt *DiffTracker) registerEgressIdentity(ctx context.Context, identity string) error {
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	servicesDTO := MapLoadBalancerAndNATGatewayUpdatesToServicesDataDTO(
		SyncServicesReturnType{},
		SyncServicesReturnType{Additions: newIgnoreCaseSetFromSlice([]string{identity})},
		dt.config.networkResourceSubscriptionID(),
		dt.config.ResourceGroup,
	)
	return dt.updateNRPSGWServices(ctx, dt.config.ServiceGatewayResourceName, servicesDTO)
}

// unlinkEgressRegistration clears the NAT Gateway reference of a Service Gateway registration, as read, with one
// partial update that changes nothing else. The write is synchronous: classifyEgressSyncWrite classifies its error.
func (dt *DiffTracker) unlinkEgressRegistration(ctx context.Context, registration armnetwork.ServiceGatewayService) error {
	var props armnetwork.ServiceGatewayServicePropertiesFormat
	if registration.Properties != nil {
		props = *registration.Properties
	}
	// azcore.NullValue, not a plain nil: the generated marshaller drops a typed-nil pointer instead of emitting JSON
	// null, and ARM reads an absent field as "leave unchanged".
	props.PublicNatGatewayID = azcore.NullValue[*string]()
	registration.Properties = &props
	request := armnetwork.ServiceGatewayUpdateServicesRequest{
		Action:          ptr.To(armnetwork.ServiceUpdateActionPartialUpdate),
		ServiceRequests: []*armnetwork.ServiceGatewayServiceRequest{{IsDelete: ptr.To(false), Service: &registration}},
	}
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	name := derefString(registration.Name)
	dt.logger.V(2).Info("Clearing the NAT Gateway of a Service Gateway service", "service", name, "serviceGateway", dt.config.ServiceGatewayResourceName)
	if err := dt.updateNRPServices(ctx, dt.config.ServiceGatewayResourceName, request); err != nil {
		return fmt.Errorf("clearing the NAT Gateway of Service Gateway service %q: %w", name, err)
	}
	return nil
}

// unregisterEgressIdentity removes the Service Gateway registration of an egress identity, once. The write is
// synchronous: classifyEgressSyncWrite classifies its error.
func (dt *DiffTracker) unregisterEgressIdentity(ctx context.Context, identity string) error {
	ctx, cancel := egressWriteContext(ctx)
	defer cancel()
	return dt.updateNRPSGWServices(ctx, dt.config.ServiceGatewayResourceName, buildServiceGatewayRemovalDTO(identity, false, dt.config))
}
