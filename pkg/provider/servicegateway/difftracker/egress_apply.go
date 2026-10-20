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
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

// egressApplyRun applies egress configurations to the NAT Gateway of one identity in one ServiceUpdater run.
type egressApplyRun struct {
	dt       *DiffTracker
	identity string
	// ipv6 reports whether the identity's NAT Gateway serves IPv6.
	ipv6 bool
	// wrote is set once the run marked the identity's operation as writing to Azure, right before its first write.
	wrote bool
}

// applyEgressConfig applies cfg to the NAT Gateway of an egress identity, whose NAT Gateway serves IPv6 when ipv6 is
// set (design 4.4, steps 1-3): the preflight reads and checks, then no write when the settled NAT Gateway already
// satisfies cfg; else the PUTs of the managed Public IPs and prefixes cfg asks for that are missing or differ and,
// once all of them succeeded, the PUT of the NAT Gateway, the commit point. Every call has a budget of its own. It
// never registers the identity, repairs links, sweeps or completes the operation: the caller does, from the result.
func (dt *DiffTracker) applyEgressConfig(ctx context.Context, identity string, cfg EgressConfig, ipv6 bool) egressRunResult {
	run := &egressApplyRun{dt: dt, identity: identity, ipv6: ipv6}
	return run.apply(ctx, cfg)
}

// applyEgressCreation applies the creation configuration of an egress identity and, when it is rejected, the
// fallback (the default) in the same run, so that the identity gets a working NAT Gateway (I6). Each apply makes its
// own calls, each with a budget of its own. When the fallback is applied, the result has Applied set to it and
// Rejected to the rejection of the creation configuration. When it is not, the other fields tell how the fallback
// ended, and a rejected fallback is an error. A creation that ended commit unknown, busy or with an error is not
// rejected, so it is never followed by the fallback.
func (dt *DiffTracker) applyEgressCreation(ctx context.Context, identity string, creation, fallback EgressConfig, ipv6 bool) egressRunResult {
	run := &egressApplyRun{dt: dt, identity: identity, ipv6: ipv6}
	result := run.apply(ctx, creation)
	if result.Rejected != nil && creation != fallback {
		rejected := result.Rejected
		dt.logger.V(2).Info("Applying the default egress configuration after the creation configuration was rejected",
			"identity", identity, "rejected", creation.Summary(), "reason", rejected.Reason)
		result = run.apply(ctx, fallback)
		if result.Rejected != nil {
			result.Err = fmt.Errorf("egress identity %q: the default configuration was rejected as well: %s", identity, result.Rejected.Reason)
		}
		result.Rejected = rejected
	}
	return result
}

// apply applies cfg once and logs how it ended.
func (r *egressApplyRun) apply(ctx context.Context, cfg EgressConfig) egressRunResult {
	result := r.applyOnce(ctx, cfg)
	logger := r.dt.logger.WithValues("identity", r.identity, "configuration", cfg.Summary())
	switch {
	case result.Applied != nil:
		logger.V(2).Info("Applied egress configuration", "wrote", r.wrote)
	case result.Rejected != nil:
		logger.V(2).Info("Rejected egress configuration", "reason", result.Rejected.Reason, "retryable", result.Rejected.Retryable)
	case result.CommitUnknown:
		logger.V(2).Info("Could not tell whether Azure applied an egress write", "err", result.Err)
	case result.Busy:
		logger.V(4).Info("Postponed egress configuration: an Azure resource was not settled", "err", result.Err)
	default:
		logger.V(4).Info("Could not apply egress configuration", "err", result.Err)
	}
	return result
}

// applyOnce is design 4.4, steps 1-3, for cfg: the preflight, then the checks (a deterministic problem rejects cfg
// terminally even when a mutable one exists), then no write when the settled NAT Gateway satisfies cfg; else the PUTs
// of the managed resources that differ, in one phase, and the NAT Gateway PUT once all of them succeeded.
func (r *egressApplyRun) applyOnce(ctx context.Context, cfg EgressConfig) egressRunResult {
	pf, end := r.preflight(ctx, cfg)
	if end != nil {
		return *end
	}
	problems := r.check(cfg, pf)
	satisfied := natSatisfies(pf.nat, pf.managedResources(), r.identity, cfg, r.ipv6, r.dt.config)
	var (
		nat    armnetwork.NatGateway
		writes []egressApplyWrite
	)
	if !satisfied {
		nat, writes = r.requests(cfg, pf, &problems)
	}
	if rejection := problems.rejection(cfg); rejection != nil {
		return egressRunResult{Rejected: rejection, NAT: pf.nat}
	}
	if satisfied {
		return egressRunResult{Applied: &cfg, NAT: pf.nat, Services: pf.services}
	}

	r.noteMutationStarted()
	results := runEgressPhase(ctx, writes, func(ctx context.Context, w egressApplyWrite) error { return w.put(ctx, r.dt) })
	if outcome := egressPhaseOutcome(results); outcome.Outcome != egressWriteSucceeded {
		return r.writeFailed(cfg, writes[outcome.Index].what, outcome.egressWriteResult, pf.nat)
	}
	written, err := r.dt.putEgressNAT(ctx, nat)
	if outcome := classifyEgressWrite(err); outcome.Outcome != egressWriteSucceeded {
		return r.writeFailed(cfg, fmt.Sprintf("NAT Gateway %q", r.identity), outcome, pf.nat)
	}
	return egressRunResult{Applied: &cfg, NATWritten: true, NAT: written, Services: pf.services}
}

// noteMutationStarted marks the identity's operation as writing to Azure (noteEgressMutationStarted), once per run,
// right before the run's first write.
func (r *egressApplyRun) noteMutationStarted() {
	if !r.wrote {
		r.dt.noteEgressMutationStarted(r.identity)
		r.wrote = true
	}
}

// egressApplyPreflight holds the settled reads an apply decides from.
type egressApplyPreflight struct {
	// nat is the identity's NAT Gateway; nil when it does not exist.
	nat *armnetwork.NatGateway
	// services are the services registered with the Service Gateway.
	services []*armnetwork.ServiceGatewayService
	// users are the user Public IPs and prefixes the configuration references, and managed the managed Public IPs and
	// prefixes it asks for, in the served families, IPv4 first.
	users   []*egressApplyUserRead
	managed []*egressApplyManagedRead
}

// natProvisioningState returns the provisioning state of the NAT Gateway; nil when it does not exist.
func (pf egressApplyPreflight) natProvisioningState() *armnetwork.ProvisioningState {
	if pf.nat == nil || pf.nat.Properties == nil {
		return nil
	}
	return pf.nat.Properties.ProvisioningState
}

// managedResources returns the managed resources the configuration asks for that exist.
func (pf egressApplyPreflight) managedResources() egressManagedResources {
	var (
		pips     []*armnetwork.PublicIPAddress
		prefixes []*armnetwork.PublicIPPrefix
	)
	for _, m := range pf.managed {
		if m.pip != nil {
			pips = append(pips, m.pip)
		}
		if m.prefix != nil {
			prefixes = append(prefixes, m.prefix)
		}
	}
	return newEgressManagedResources(pips, prefixes)
}

// preflight reads, in one bounded phase, what applying cfg depends on: the identity's NAT Gateway, the services
// registered with the Service Gateway, the user Public IPs and prefixes cfg references and the managed Public IPs and
// prefixes it asks for, in the served families. It returns end when the reads cannot be decided from, in this order: a
// failed read is a plain retryable error (nothing was written, so it is never commit unknown); an unsettled read
// (Creating, Updating, Deleting) makes the run busy; a NAT Gateway of the identity's name that the controller does not
// own, or that another Service Gateway service uses, is never written, a plain error. A NAT Gateway that does not
// exist yet has no other user.
func (r *egressApplyRun) preflight(ctx context.Context, cfg EgressConfig) (egressApplyPreflight, *egressRunResult) {
	dt := r.dt
	var (
		pf          egressApplyPreflight
		natRead     egressNATRead
		natErr      error
		servicesErr error
	)
	for _, v6 := range egressServedFamilies(r.ipv6) {
		family := cfg.familyConfig(v6)
		for _, id := range egressIDList(family.PublicIPIDs) {
			pf.users = append(pf.users, &egressApplyUserRead{id: id, ipv6: v6})
		}
		for _, id := range egressIDList(family.PublicIPPrefixIDs) {
			pf.users = append(pf.users, &egressApplyUserRead{id: id, ipv6: v6, isPrefix: true})
		}
		for _, m := range family.managed(v6) {
			pf.managed = append(pf.managed, &egressApplyManagedRead{managed: m, name: m.name(r.identity)})
		}
	}
	reads := []func(context.Context){
		func(ctx context.Context) { natRead, natErr = dt.readEgressNAT(ctx, r.identity) },
		func(ctx context.Context) { pf.services, servicesErr = dt.getEgressServices(ctx) },
	}
	for _, u := range pf.users {
		reads = append(reads, func(ctx context.Context) { u.read(ctx, dt) })
	}
	for _, m := range pf.managed {
		reads = append(reads, func(ctx context.Context) { m.read(ctx, dt) })
	}
	runEgressPhase(ctx, reads, func(ctx context.Context, read func(context.Context)) error {
		read(ctx)
		return nil
	})
	pf.nat = natRead.NAT

	readErrs := []error{natErr, servicesErr}
	for _, u := range pf.users {
		if !u.forbidden() {
			readErrs = append(readErrs, u.err)
		}
	}
	for _, m := range pf.managed {
		readErrs = append(readErrs, m.err)
	}
	for _, err := range readErrs {
		if err != nil {
			return pf, &egressRunResult{Err: err}
		}
	}

	busy := func(what string, state *armnetwork.ProvisioningState) *egressRunResult {
		return &egressRunResult{Busy: true, Err: fmt.Errorf("%s is %s", what, ptr.Deref(state, ""))}
	}
	if !natRead.Settled {
		return pf, busy(fmt.Sprintf("NAT Gateway %q", r.identity), pf.natProvisioningState())
	}
	for _, u := range pf.users {
		if state := u.provisioningState(); !egressSettled(state) {
			return pf, busy(u.what(), state)
		}
	}
	for _, m := range pf.managed {
		if state := m.provisioningState(); !egressSettled(state) {
			return pf, busy(m.what(), state)
		}
	}

	if natRead.Exists && !natRead.Ours {
		return pf, &egressRunResult{Err: fmt.Errorf("NAT Gateway %q: %w", r.identity, errNATGatewayNotOwned)}
	}
	if user := otherServiceUsingNATGateway(pf.services, r.identity, natGatewayID(r.identity, dt.config)); natRead.Exists && user != "" {
		return pf, &egressRunResult{Err: fmt.Errorf("NAT Gateway %q is used by Service Gateway service %q", r.identity, user)}
	}
	return pf, nil
}

// check returns the problems of applying cfg over the preflight reads (design 2.3): deterministic ones (SKU, location
// or IP family of a user resource, the exact address count of a family) and mutable ones, which the user or Azure can
// clear without a configuration change (a user resource that is missing, unreadable, not provisioned or in use
// elsewhere, and a desired numbered Public IP or prefix that exists without the ownership tag, which is never
// overwritten).
func (r *egressApplyRun) check(cfg EgressConfig, pf egressApplyPreflight) egressApplyProblems {
	var problems egressApplyProblems
	ownNAT := natGatewayID(r.identity, r.dt.config)
	for _, u := range pf.users {
		u.check(&problems, ownNAT, r.dt.config)
	}
	r.checkAddresses(&problems, cfg, pf.users)
	for _, m := range pf.managed {
		if m.exists() && !ownsManagedName(r.identity, m.name, m.tags()) {
			problems.mutable("%s exists without the ownership tag of egress identity %q and is never overwritten", m.what(), r.identity)
		}
	}
	return problems
}

// checkAddresses adds a deterministic problem for each served family whose exact number of public addresses is 0 or
// more than a NAT Gateway accepts: the managed Public IPs, the size of the managed prefix, the user Public IPs and the
// size of each user prefix as read (parsing could only count egressUserPrefixMinAddresses per user prefix). A user
// prefix that could not be read (missing or access denied: a mutable problem) still counts as parsing counted it, in
// the family it is listed for, so that neither limit is decided before its size is known.
func (r *egressApplyRun) checkAddresses(problems *egressApplyProblems, cfg EgressConfig, users []*egressApplyUserRead) {
	for _, v6 := range egressServedFamilies(r.ipv6) {
		family := cfg.familyConfig(v6)
		bits := egressFamilyAnnotationsOf(v6).bits
		addresses := family.ManagedPublicIPs + len(egressIDList(family.PublicIPIDs))
		if family.ManagedPrefixLength > 0 {
			addresses += egressPrefixAddresses(bits, family.ManagedPrefixLength)
		}
		for _, u := range users {
			if !u.isPrefix || u.ipv6 != v6 {
				continue
			}
			if _, read := u.resource(); !read {
				addresses += egressUserPrefixMinAddresses
			} else if length := u.prefixLength(); length != nil && u.familyMatches() {
				addresses += egressPrefixAddresses(bits, int(*length))
			}
		}
		switch {
		case addresses == 0:
			problems.deterministic("%s: no public address", ipVersion(v6))
		case addresses > consts.ServiceEgressGatewayMaxAddressesPerFamily:
			problems.deterministic("%s: %d public addresses, more than the %d a NAT Gateway accepts (managed Public IPs + managed "+
				"prefix size + user Public IPs + user prefix sizes)", ipVersion(v6), addresses, consts.ServiceEgressGatewayMaxAddressesPerFamily)
		}
	}
}

// egressPrefixAddresses returns the number of addresses of a prefix of the given length in an IP family of bits bits.
// It saturates far above any NAT Gateway limit, so that adding sizes never overflows.
func egressPrefixAddresses(bits, length int) int {
	return 1 << max(0, min(bits-length, 30))
}

// egressApplyUserRead is a user Public IP or Public IP prefix a configuration references, as the preflight read it.
type egressApplyUserRead struct {
	// id is the canonical ARM ID of the resource.
	id string
	// ipv6 is the IP family it is listed for.
	ipv6     bool
	isPrefix bool
	// pip or prefix is the resource as read; both are nil when it does not exist or was not read.
	pip    *armnetwork.PublicIPAddress
	prefix *armnetwork.PublicIPPrefix
	// invalid is set when id is not a resource ID, and err when the read failed.
	invalid error
	err     error
}

func (u *egressApplyUserRead) read(ctx context.Context, dt *DiffTracker) {
	parsed, err := arm.ParseResourceID(u.id)
	if err != nil {
		u.invalid = err
		return
	}
	if u.isPrefix {
		u.prefix, _, u.err = dt.getEgressPrefix(ctx, parsed.ResourceGroupName, parsed.Name)
		return
	}
	u.pip, _, u.err = dt.getEgressPublicIP(ctx, parsed.ResourceGroupName, parsed.Name)
}

func (u *egressApplyUserRead) what() string {
	if u.isPrefix {
		return fmt.Sprintf("user Public IP prefix %q", u.id)
	}
	return fmt.Sprintf("user Public IP %q", u.id)
}

// forbidden reports whether reading the resource was refused for lack of access, which the user can grant.
func (u *egressApplyUserRead) forbidden() bool {
	status, _ := extractAzureErrorInfo(u.err)
	return status == http.StatusForbidden
}

// egressApplyUserResource is what the checks look at in a user Public IP or prefix that exists.
type egressApplyUserResource struct {
	sku, wantSKU string
	location     *string
	version      armnetwork.IPVersion
	state        *armnetwork.ProvisioningState
	// length is the length of a prefix.
	length *int32
}

// resource returns what the checks look at in the resource; ok is false when it does not exist.
func (u *egressApplyUserRead) resource() (r egressApplyUserResource, ok bool) {
	r.version = armnetwork.IPVersionIPv4
	switch {
	case u.prefix != nil:
		r.wantSKU, r.location = string(armnetwork.PublicIPPrefixSKUNameStandardV2), u.prefix.Location
		if u.prefix.SKU != nil {
			r.sku = string(ptr.Deref(u.prefix.SKU.Name, ""))
		}
		if props := u.prefix.Properties; props != nil {
			r.version = ptr.Deref(props.PublicIPAddressVersion, r.version)
			r.state, r.length = props.ProvisioningState, props.PrefixLength
		}
		return r, true
	case u.pip != nil:
		r.wantSKU, r.location = string(armnetwork.PublicIPAddressSKUNameStandardV2), u.pip.Location
		if u.pip.SKU != nil {
			r.sku = string(ptr.Deref(u.pip.SKU.Name, ""))
		}
		if props := u.pip.Properties; props != nil {
			r.version = ptr.Deref(props.PublicIPAddressVersion, r.version)
			r.state = props.ProvisioningState
		}
		return r, true
	}
	return r, false
}

// egressUsesElsewhere describes each use of a Public IP or prefix, as read, other than by the NAT Gateway ownNAT; none
// when nothing else uses it. Each keeps a NAT Gateway from using the resource and Azure from deleting it.
func egressUsesElsewhere(pip *armnetwork.PublicIPAddress, prefix *armnetwork.PublicIPPrefix, ownNAT string) []string {
	var (
		nat  *armnetwork.NatGateway
		uses []string
	)
	switch {
	case prefix != nil && prefix.Properties != nil:
		props := prefix.Properties
		nat = props.NatGateway
		if props.LoadBalancerFrontendIPConfiguration != nil {
			uses = append(uses, fmt.Sprintf("is in use by load balancer frontend %q", derefString(props.LoadBalancerFrontendIPConfiguration.ID)))
		}
		if len(props.PublicIPAddresses) > 0 {
			// A NAT Gateway uses every address of a prefix.
			uses = append(uses, fmt.Sprintf("is in use by %d Public IPs allocated from it", len(props.PublicIPAddresses)))
		}
	case pip != nil && pip.Properties != nil:
		nat = pip.Properties.NatGateway
		if config := pip.Properties.IPConfiguration; config != nil {
			uses = append(uses, fmt.Sprintf("is in use by IP configuration %q", derefString(config.ID)))
		}
	}
	if nat != nil && canonicalResourceID(derefString(nat.ID)) != canonicalResourceID(ownNAT) {
		uses = append([]string{fmt.Sprintf("is attached to NAT Gateway %q", derefString(nat.ID))}, uses...)
	}
	return uses
}

// provisioningState returns the provisioning state of the resource; nil when it does not exist.
func (u *egressApplyUserRead) provisioningState() *armnetwork.ProvisioningState {
	r, _ := u.resource()
	return r.state
}

// familyMatches reports whether the resource has the IP family it is listed for.
func (u *egressApplyUserRead) familyMatches() bool {
	r, _ := u.resource()
	return strings.EqualFold(string(r.version), string(ipVersion(u.ipv6)))
}

// prefixLength returns the length of a prefix as read; nil for a Public IP or a prefix that does not exist.
func (u *egressApplyUserRead) prefixLength() *int32 {
	r, _ := u.resource()
	return r.length
}

// check adds the problems that keep the NAT Gateway whose ID is ownNAT from using the resource (design 2.3). Use by
// that NAT Gateway itself is fine.
func (u *egressApplyUserRead) check(problems *egressApplyProblems, ownNAT string, dt Config) {
	what := u.what()
	r, exists := u.resource()
	switch {
	case u.invalid != nil:
		problems.deterministic("%s is not a resource ID: %v", what, u.invalid)
		return
	case u.err != nil:
		// The preflight ends on any other failed read before the checks.
		problems.mutable("%s cannot be read: access denied", what)
		return
	case !exists:
		problems.mutable("%s does not exist", what)
		return
	}
	if !strings.EqualFold(r.sku, r.wantSKU) {
		problems.deterministic("%s has SKU %q, not %s", what, r.sku, r.wantSKU)
	}
	if !sameLocation(r.location, dt.Location) {
		problems.deterministic("%s is in location %q, not %q", what, derefString(r.location), dt.Location)
	}
	if !u.familyMatches() {
		problems.deterministic("%s is %s, but it is listed for %s", what, r.version, ipVersion(u.ipv6))
	}
	if !provisioningSucceeded(r.state) {
		problems.mutable("%s has provisioning state %q", what, ptr.Deref(r.state, ""))
	}
	for _, use := range egressUsesElsewhere(u.pip, u.prefix, ownNAT) {
		problems.mutable("%s %s", what, use)
	}
}

// egressApplyManagedRead is a managed Public IP or prefix a configuration asks for, as the preflight read it.
type egressApplyManagedRead struct {
	managed egressManagedName
	name    string
	// pip or prefix is the resource as read; both are nil when it does not exist.
	pip    *armnetwork.PublicIPAddress
	prefix *armnetwork.PublicIPPrefix
	err    error
}

func (m *egressApplyManagedRead) read(ctx context.Context, dt *DiffTracker) {
	if m.managed.Kind == egressManagedPrefix {
		m.prefix, _, m.err = dt.getEgressPrefix(ctx, dt.config.ResourceGroup, m.name)
		return
	}
	m.pip, _, m.err = dt.getEgressPublicIP(ctx, dt.config.ResourceGroup, m.name)
}

func (m *egressApplyManagedRead) what() string {
	if m.managed.Kind == egressManagedPrefix {
		return fmt.Sprintf("Public IP prefix %q", m.name)
	}
	return fmt.Sprintf("Public IP %q", m.name)
}

func (m *egressApplyManagedRead) exists() bool {
	return m.pip != nil || m.prefix != nil
}

func (m *egressApplyManagedRead) tags() map[string]*string {
	switch {
	case m.pip != nil:
		return m.pip.Tags
	case m.prefix != nil:
		return m.prefix.Tags
	}
	return nil
}

// provisioningState returns the provisioning state of the resource; nil when it does not exist.
func (m *egressApplyManagedRead) provisioningState() *armnetwork.ProvisioningState {
	switch {
	case m.pip != nil && m.pip.Properties != nil:
		return m.pip.Properties.ProvisioningState
	case m.prefix != nil && m.prefix.Properties != nil:
		return m.prefix.Properties.ProvisioningState
	}
	return nil
}

// egressApplyProblems collects why a configuration cannot be applied.
type egressApplyProblems struct{ terminal, retryable []string }

// deterministic adds a problem that only another configuration avoids.
func (p *egressApplyProblems) deterministic(format string, args ...any) {
	p.terminal = append(p.terminal, fmt.Sprintf(format, args...))
}

// mutable adds a problem that the user or Azure can clear without a configuration change.
func (p *egressApplyProblems) mutable(format string, args ...any) {
	p.retryable = append(p.retryable, fmt.Sprintf(format, args...))
}

// rejection returns the rejection of cfg for the problems, or nil when there is none: terminal, for the deterministic
// problems, when there is one; else retryable.
func (p *egressApplyProblems) rejection(cfg EgressConfig) *egressRejection {
	switch {
	case len(p.terminal) > 0:
		return &egressRejection{Config: cfg, Reason: strings.Join(p.terminal, "; ")}
	case len(p.retryable) > 0:
		return &egressRejection{Config: cfg, Reason: strings.Join(p.retryable, "; "), Retryable: true}
	}
	return nil
}

// egressApplyWrite is the PUT of a managed Public IP or prefix.
type egressApplyWrite struct {
	what   string
	pip    *armnetwork.PublicIPAddress
	prefix *armnetwork.PublicIPPrefix
}

func (w egressApplyWrite) put(ctx context.Context, dt *DiffTracker) error {
	if w.prefix != nil {
		_, err := dt.putEgressPrefix(ctx, *w.prefix)
		return err
	}
	_, err := dt.putEgressPublicIP(ctx, *w.pip)
	return err
}

// requests returns the writes that apply cfg over the preflight reads: the NAT Gateway, and the managed Public IPs
// and prefixes cfg asks for that are missing or differ. Each is the projection of cfg over the resource as read, so
// what the controller does not set is kept. The managed resources are judged without the legacy ownership rule
// (legacyNAT false): the NAT Gateway PUT adds the ownership tag, which ends the legacy ownership of an untagged base
// Public IP, so the base Public IP gets the tag in the same run. A resource of a reserved name that is not the
// identity's is never written, so it gets no request: check rejected cfg retryably for it. A request that cannot be
// built (more merged tags than Azure accepts on a resource, a configuration out of range) is a deterministic problem.
func (r *egressApplyRun) requests(cfg EgressConfig, pf egressApplyPreflight, problems *egressApplyProblems) (armnetwork.NatGateway, []egressApplyWrite) {
	dt := r.dt.config
	nat, err := natRequest(pf.nat, r.identity, cfg, r.ipv6, dt)
	if err != nil {
		problems.deterministic("%v", err)
	}
	var writes []egressApplyWrite
	for _, m := range pf.managed {
		if m.exists() && !ownsManagedName(r.identity, m.name, m.tags()) {
			continue
		}
		v6, n := m.managed.IPv6, m.managed.N
		if m.managed.Kind == egressManagedPrefix {
			if managedPrefixMismatch(m.prefix, r.identity, v6, n, cfg, dt) == "" {
				continue
			}
			request, err := managedPrefixRequest(m.prefix, r.identity, v6, n, cfg, dt)
			if err != nil {
				problems.deterministic("%v", err)
				continue
			}
			writes = append(writes, egressApplyWrite{what: m.what(), prefix: &request})
			continue
		}
		if managedPublicIPMismatch(m.pip, r.identity, v6, n, cfg, dt, false) == "" {
			continue
		}
		request, err := managedPublicIPRequest(m.pip, r.identity, v6, n, cfg, dt)
		if err != nil {
			problems.deterministic("%v", err)
			continue
		}
		writes = append(writes, egressApplyWrite{what: m.what(), pip: &request})
	}
	return nat, writes
}

// writeFailed is how an apply ends when a write of it did not succeed. A write whose outcome is unknown ends it
// commit unknown. A refusal rejects cfg retryably for a quota (egressQuotaErrorCode) and for 403 and 404 (access
// to, or existence of, a resource cfg references, which the user can fix), terminally for any other 400, and is a
// plain retryable error otherwise (409, and statuses the controller does not expect). nat is the settled NAT Gateway
// of the preflight, which a refused or unsent NAT Gateway PUT left as it was.
func (r *egressApplyRun) writeFailed(cfg EgressConfig, what string, write egressWriteResult, nat *armnetwork.NatGateway) egressRunResult {
	if write.Outcome == egressWriteUnknown {
		return egressRunResult{CommitUnknown: true, Err: write.Err}
	}
	code, message := egressAzureError(write.Err)
	reason := fmt.Sprintf("Azure refused %s: HTTP %d", what, write.StatusCode)
	if code != "" {
		reason += " " + code
	}
	if message != "" {
		reason += ": " + message
	}
	switch {
	case egressQuotaErrorCode(code):
		return egressRunResult{Rejected: &egressRejection{Config: cfg, Reason: reason, Retryable: true}, NAT: nat}
	case write.StatusCode == http.StatusBadRequest:
		return egressRunResult{Rejected: &egressRejection{Config: cfg, Reason: reason}, NAT: nat}
	case write.StatusCode == http.StatusForbidden, write.StatusCode == http.StatusNotFound:
		return egressRunResult{Rejected: &egressRejection{Config: cfg, Reason: reason, Retryable: true}, NAT: nat}
	}
	return egressRunResult{Err: write.Err, NAT: nat}
}

// egressQuotaErrorCode reports whether an Azure error code refuses a write for a quota, which a quota increase or
// freed capacity clears without a configuration change. These are ARM's QuotaExceeded and NRP's subscription count
// limits, which NRP names "<resource>CountLimitReached": PublicIPCountLimitReached and MaxPublicIpCountLimitReached
// (also counted as quota_exceeded by extractAzureErrorInfo), PublicIPPrefixCountLimitReached,
// StandardSkuPublicIpCountLimitReached and IPv4StandardSkuPublicIpCountLimitReached (the Public IP quota failures of
// this repository's e2e jobs, .agents/skills/unblock-dependabot-pr/references/act-patterns.md), and the same limits
// of other resources.
func egressQuotaErrorCode(code string) bool {
	return strings.EqualFold(code, "QuotaExceeded") || strings.HasSuffix(strings.ToLower(code), "countlimitreached")
}

// egressAzureError returns the error code and message Azure answered a request with, when err is Azure's answer.
func egressAzureError(err error) (code, message string) {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return "", ""
	}
	if respErr.RawResponse != nil {
		if body, readErr := runtime.Payload(respErr.RawResponse); readErr == nil {
			var payload struct {
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal(body, &payload) == nil {
				message = payload.Error.Message
			}
		}
	}
	return respErr.ErrorCode, message
}
