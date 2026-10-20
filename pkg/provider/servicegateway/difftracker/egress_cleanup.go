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

// Observe-and-clean, link repair, sweep, deletion and BYO residue cleanup of the NAT Gateways of egress identities.
//
// Every Azure call goes through the egress call layer (egress_azure.go): each read and write has its own budget,
// writes are single attempts classified by classifyEgressWrite (classifyEgressSyncWrite for the synchronous writes of
// the Service Gateway's registrations), and the NAT Gateway DELETE is conditional on the etag of a settled read.
// Decisions are taken from settled reads only. dt.noteEgressMutationStarted is called before the first write of a run,
// so a run that panics after it is known to have written. The functions only report what they found and did; the
// ServiceUpdater completes the identity's operation with it.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"k8s.io/utils/ptr"
)

// egressReservedRead is the read of one reserved managed name of an egress identity in the cluster resource group.
type egressReservedRead struct {
	managed egressManagedName
	name    string
	pip     *armnetwork.PublicIPAddress
	prefix  *armnetwork.PublicIPPrefix
	exists  bool
	err     error
}

// id returns the resource ID of the reserved name in the cluster resource group.
func (r *egressReservedRead) id(dt Config) string {
	if r.managed.Kind == egressManagedPrefix {
		return publicIPPrefixID(r.name, dt)
	}
	return publicIPAddressID(r.name, dt)
}

func (r *egressReservedRead) tags() map[string]*string {
	switch {
	case r.pip != nil:
		return r.pip.Tags
	case r.prefix != nil:
		return r.prefix.Tags
	default:
		return nil
	}
}

// settled reports whether a successful read is settled: the name is absent, or its resource is not being created,
// updated or deleted.
func (r *egressReservedRead) settled() bool {
	switch {
	case r.pip != nil && r.pip.Properties != nil:
		return egressSettled(r.pip.Properties.ProvisioningState)
	case r.prefix != nil && r.prefix.Properties != nil:
		return egressSettled(r.prefix.Properties.ProvisioningState)
	default:
		return true
	}
}

// natReadSettled reports whether a NAT Gateway read is settled: not being created, updated or deleted.
func natReadSettled(nat *armnetwork.NatGateway) bool {
	return nat == nil || nat.Properties == nil || egressSettled(nat.Properties.ProvisioningState)
}

// natNeedsLink reports whether a NAT Gateway is not linked to the cluster's Service Gateway.
func natNeedsLink(nat *armnetwork.NatGateway, dt Config) bool {
	return canonicalResourceID(natGatewayServiceGatewayID(nat)) != canonicalResourceID(dt.ServiceGatewayResourceID())
}

// sweepReferencedIDs returns the canonical IDs (canonicalResourceID) of the Public IPs and prefixes the NAT Gateways
// reference, in either IP family. Nil NAT Gateways reference nothing.
func sweepReferencedIDs(nats ...*armnetwork.NatGateway) map[string]bool {
	referenced := map[string]bool{}
	for _, nat := range nats {
		if nat == nil || nat.Properties == nil {
			continue
		}
		props := nat.Properties
		for _, refs := range [][]*armnetwork.SubResource{props.PublicIPAddresses, props.PublicIPAddressesV6, props.PublicIPPrefixes, props.PublicIPPrefixesV6} {
			for _, ref := range refs {
				if ref != nil && ref.ID != nil {
					referenced[canonicalResourceID(*ref.ID)] = true
				}
			}
		}
	}
	return referenced
}

// managedFromReservedReads returns the existing resources of the reads, by reserved name, for natSatisfies.
func managedFromReservedReads(reads []*egressReservedRead) egressManagedResources {
	managed := newEgressManagedResources(nil, nil)
	for _, r := range reads {
		switch {
		case r.pip != nil:
			managed.publicIPs[strings.ToLower(r.name)] = r.pip
		case r.prefix != nil:
			managed.prefixes[strings.ToLower(r.name)] = r.prefix
		}
	}
	return managed
}

// observedEgressConfig returns the first of candidates, in order, that a settled NAT Gateway satisfies, else
// egressUnknown; a nil candidate is skipped.
func observedEgressConfig(nat *armnetwork.NatGateway, managed egressManagedResources, id string, ipv6 bool, dt Config,
	candidates ...*EgressConfig) EgressConfig {
	for _, candidate := range candidates {
		if candidate != nil && natSatisfies(nat, managed, id, *candidate, ipv6, dt) {
			return *candidate
		}
	}
	return egressUnknown
}

// readEgressReserved reads every reserved managed name of egress identity id in the served IP families (IPv4, and
// IPv6 when ipv6 is set) in the cluster resource group, in one bounded phase (runEgressPhase).
func (dt *DiffTracker) readEgressReserved(ctx context.Context, id string, ipv6 bool) []*egressReservedRead {
	reserved := reservedManaged(ipv6)
	reads := make([]*egressReservedRead, len(reserved))
	for i, m := range reserved {
		reads[i] = &egressReservedRead{managed: m, name: m.name(id)}
	}
	// Each call fills its own read; only the read errors matter, so the classified results are not needed.
	runEgressPhase(ctx, reads, func(ctx context.Context, r *egressReservedRead) error {
		if r.managed.Kind == egressManagedPrefix {
			r.prefix, r.exists, r.err = dt.getEgressPrefix(ctx, dt.config.ResourceGroup, r.name)
		} else {
			r.pip, r.exists, r.err = dt.getEgressPublicIP(ctx, dt.config.ResourceGroup, r.name)
		}
		return r.err
	})
	return reads
}

// sweepEgressReserved deletes the leftovers among the reserved managed names of egress identity id in the served IP
// families (IPv4, and IPv6 when ipv6 is set): every existing name that is the controller's (ownsManagedName) and that
// nat, the identity's settled NAT Gateway, does not reference; every such name when nat is nil, which a deletion
// passes once the NAT Gateway is gone. Names that are not the controller's are never written or deleted, and a name is
// only deleted from a settled read. It reads every name in one bounded phase, then deletes in another.
//
// swept is set when every name was read settled and every DELETE ended in success, 404, or a refusal with 400 of a
// resource that its read shows in use elsewhere (egressUsesElsewhere: by another NAT Gateway, an IP configuration, a
// load balancer frontend or Public IPs allocated from a prefix), which only its user can end, so it is not retried. Any
// other refusal leaves it unset, so the next run retries the DELETE: also a 400 while the read shows no use, or one by
// the identity's own NAT Gateway, which may not have released the resource yet. unknown is set when a DELETE may
// still be applied. err joins the errors of the failed reads and of the other refused or unknown DELETEs.
func (dt *DiffTracker) sweepEgressReserved(ctx context.Context, id string, ipv6 bool, nat *armnetwork.NatGateway) (swept, unknown bool, err error) {
	return dt.sweepEgressReads(ctx, id, dt.readEgressReserved(ctx, id, ipv6), nat)
}

// sweepEgressReads is sweepEgressReserved from reads already taken. A name referenced by any of nats is kept.
func (dt *DiffTracker) sweepEgressReads(ctx context.Context, id string, reads []*egressReservedRead, nats ...*armnetwork.NatGateway) (swept, unknown bool, err error) {
	referenced := sweepReferencedIDs(nats...)
	swept = true
	var errs []error
	var leftovers []*egressReservedRead
	for _, r := range reads {
		switch {
		case r.err != nil:
			swept = false
			errs = append(errs, r.err)
		case !r.exists:
		case !r.settled():
			swept = false
			dt.logger.V(4).Info("Kept reserved egress resource until it settles", "egressIdentity", id, "resource", r.name)
		case !ownsManagedName(id, r.name, r.tags()):
		case referenced[canonicalResourceID(r.id(dt.config))]:
		default:
			leftovers = append(leftovers, r)
		}
	}
	if len(leftovers) == 0 {
		return swept, false, errors.Join(errs...)
	}

	dt.noteEgressMutationStarted(id)
	results := runEgressPhase(ctx, leftovers, func(ctx context.Context, r *egressReservedRead) error {
		if r.managed.Kind == egressManagedPrefix {
			return dt.deleteEgressPrefix(ctx, r.name)
		}
		return dt.deleteEgressPublicIP(ctx, r.name)
	})
	ownNAT := natGatewayID(id, dt.config)
	var wrote bool
	for i, result := range results {
		r := leftovers[i]
		switch uses := egressUsesElsewhere(r.pip, r.prefix, ownNAT); {
		case result.Outcome == egressWriteSucceeded:
			wrote = true
		case result.Outcome == egressWriteUnknown:
			wrote, unknown, swept = true, true, false
			errs = append(errs, result.Err)
		case result.Outcome == egressWriteRefused && result.StatusCode == http.StatusBadRequest && len(uses) > 0:
			// Only the user of the resource can end that use: the DELETE is not retried.
			dt.logger.V(2).Info("Kept reserved egress resource in use elsewhere", "egressIdentity", id, "resource", r.name,
				"errorCode", result.ErrorCode, "uses", uses)
		default:
			swept = false
			errs = append(errs, result.Err)
		}
	}
	logger := dt.logger.V(4)
	if wrote {
		logger = dt.logger.V(2)
	}
	logger.Info("Swept reserved egress resources", "egressIdentity", id, "leftovers", len(leftovers), "swept", swept, "commitUnknown", unknown)
	return swept, unknown, errors.Join(errs...)
}

// repairEgressLinks restores both sides of the link between egress identity id and its settled NAT Gateway nat,
// whatever the identity's configuration, from services, the services registered with the Service Gateway as the run
// read them before its first write. NAT Gateway side: one not linked to the cluster's Service Gateway gets the link
// back with a PUT that changes nothing else (natLinkRequest) and carries no precondition. Service Gateway side: when
// the identity's registration does not reference the NAT Gateway, the identity is registered again with the request
// its creation sends. Each write is a single attempt, classified; the repair stops at the first write that does not
// succeed. A NAT Gateway that is unsettled, not the controller's (ownsNATGateway) or used by another Service Gateway
// service is never written: the repair then fails without any call. The result reports the NAT Gateway as the repair
// left it (as the link PUT returned it, else as given) and why the links are not both in place (Err), which a write
// with an unknown outcome also gives (CommitUnknown).
func (dt *DiffTracker) repairEgressLinks(ctx context.Context, id string, nat *armnetwork.NatGateway, services []*armnetwork.ServiceGatewayService) egressRunResult {
	result := egressRunResult{NAT: nat}
	natID := natGatewayID(id, dt.config)
	switch {
	case nat == nil:
		result.Err = fmt.Errorf("repairing the links of egress identity %q: no NAT Gateway", id)
		return result
	case !natReadSettled(nat):
		result.Err = fmt.Errorf("repairing the links of egress identity %q: NAT Gateway is %s", id, ptr.Deref(nat.Properties.ProvisioningState, ""))
		return result
	case !ownsNATGateway(nat, dt.config):
		result.Err = fmt.Errorf("repairing the links of egress identity %q: NAT Gateway %q is not owned by this controller", id, derefString(nat.Name))
		return result
	}
	if user := otherServiceUsingNATGateway(services, id, natID); user != "" {
		result.Err = fmt.Errorf("repairing the links of egress identity %q: NAT Gateway %q is used by Service Gateway service %q", id, derefString(nat.Name), user)
		return result
	}

	if natNeedsLink(nat, dt.config) {
		dt.noteEgressMutationStarted(id)
		written, err := dt.putEgressNATLink(ctx, nat)
		switch classifyEgressWrite(err).Outcome {
		case egressWriteUnknown:
			result.CommitUnknown, result.Err = true, err
			return result
		case egressWriteRefused:
			result.Err = err
			return result
		}
		if written != nil {
			result.NAT = written
		}
		dt.logger.V(2).Info("Linked NAT Gateway to Service Gateway", "egressIdentity", id, "natGateway", derefString(nat.Name))
	}

	// Not registered, or registered without a NAT Gateway, reads as "": never the NAT Gateway's ID.
	registeredNAT := ""
	if registration := egressRegistrationOf(services, id); registration != nil && registration.Properties != nil {
		registeredNAT = derefString(registration.Properties.PublicNatGatewayID)
	}
	if canonicalResourceID(registeredNAT) == canonicalResourceID(natID) {
		return result
	}
	dt.noteEgressMutationStarted(id)
	err := dt.registerEgressIdentity(ctx, id)
	switch classifyEgressSyncWrite(err).Outcome {
	case egressWriteUnknown:
		result.CommitUnknown = true
		result.Err = fmt.Errorf("registering egress identity %q with its NAT Gateway: %w", id, err)
		return result
	case egressWriteRefused:
		result.Err = fmt.Errorf("registering egress identity %q with its NAT Gateway: %w", id, err)
		return result
	}
	dt.logger.V(2).Info("Registered egress identity with its NAT Gateway", "egressIdentity", id, "previousNATGateway", registeredNAT)
	return result
}

// observeEgress is an observe-and-clean run of egress identity id, whose NAT Gateway serves IPv6 when ipv6 is set:
// it reads the NAT Gateway, the reserved managed names of the served families and the services registered with the
// Service Gateway, repairs the links (repairEgressLinks), finds the first of candidates, in order, that the NAT
// Gateway satisfies (observedEgressConfig; observeOutboundService passes egressObservationTargets and the default),
// else egressUnknown, and sweeps the reserved names against it. It changes no configuration: the link repair and the
// sweep's DELETEs are its only writes.
//
// Every read is taken before any write. The run is busy, and writes nothing, while the NAT Gateway or a reserved
// resource it references is unsettled; a failed read of either, or of the services, ends it before any write too. A
// NAT Gateway that does not exist is reported and nothing else is done: re-creating a NAT Gateway deleted out of band
// is out of scope, so the run reports nothing owed (Swept) and the identity is not observed again and again. A NAT
// Gateway that is not the controller's is neither written nor swept against. A link repair that does not succeed ends
// the run before the sweep.
func (dt *DiffTracker) observeEgress(ctx context.Context, id string, ipv6 bool, candidates ...*EgressConfig) egressRunResult {
	read, err := dt.readEgressNAT(ctx, id)
	switch {
	case err != nil:
		return egressRunResult{Err: err}
	case !read.Exists:
		dt.logger.V(2).Info("Completed the observation of an egress identity without a NAT Gateway; nothing was swept", "egressIdentity", id)
		return egressRunResult{NATGone: true, Swept: true}
	case !read.Settled:
		return egressRunResult{Busy: true}
	case !read.Ours:
		return egressRunResult{Applied: ptr.To(egressUnknown),
			Err: fmt.Errorf("observing egress identity %q: NAT Gateway %q is not owned by this controller", id, derefString(read.NAT.Name))}
	}
	nat := read.NAT
	reads := dt.readEgressReserved(ctx, id, ipv6)
	referenced := sweepReferencedIDs(nat)
	for _, r := range reads {
		if !referenced[canonicalResourceID(r.id(dt.config))] {
			continue
		}
		if r.err != nil {
			return egressRunResult{Err: r.err}
		}
		if !r.settled() {
			return egressRunResult{Busy: true}
		}
	}
	services, err := dt.getEgressServices(ctx)
	if err != nil {
		return egressRunResult{Err: err}
	}

	var result egressRunResult
	link := dt.repairEgressLinks(ctx, id, nat, services)
	if link.CommitUnknown {
		result.Applied, result.CommitUnknown, result.Err = ptr.To(egressUnknown), true, link.Err
		return result
	}
	applied := observedEgressConfig(link.NAT, managedFromReservedReads(reads), id, ipv6, dt.config, candidates...)
	result.Applied = &applied
	if link.Err != nil {
		result.Err = link.Err
		return result
	}
	// The link PUT changes no reference; what either version of the NAT Gateway references is kept.
	swept, unknown, err := dt.sweepEgressReads(ctx, id, reads, nat, link.NAT)
	result.Swept, result.Err = swept, err
	if unknown {
		result.Applied, result.CommitUnknown = ptr.To(egressUnknown), true
	}
	return result
}

// unlinkManagedEgress is the unlink step of deleting managed egress identity id, whose NAT Gateway the controller may
// delete (ownsClusterNATGateway), before its registration is removed. Service Gateway side: the registrations are read
// and, when the identity's references a NAT Gateway, that reference is cleared with a partial update that changes
// nothing else. NAT Gateway side, unless another Service Gateway service uses it: the NAT Gateway, read settled, is
// unlinked from the cluster's Service Gateway with a PUT without precondition (natUnlinkRequest) when it is linked to
// it; one that is no longer the controller's is left alone. Each write is a single attempt, classified, and the unlink
// stops at the first read or write that does not succeed. The result reports a write whose outcome is unknown
// (CommitUnknown), an unsettled NAT Gateway (Busy), and why the unlink is not done (Err).
func (dt *DiffTracker) unlinkManagedEgress(ctx context.Context, id string) egressRunResult {
	var result egressRunResult
	services, err := dt.getEgressServices(ctx)
	if err != nil {
		result.Err = err
		return result
	}
	if registration := egressRegistrationOf(services, id); registration != nil && registration.Properties != nil &&
		registration.Properties.PublicNatGatewayID != nil {
		dt.noteEgressMutationStarted(id)
		err := dt.unlinkEgressRegistration(ctx, *registration)
		switch classifyEgressSyncWrite(err).Outcome {
		case egressWriteUnknown:
			result.CommitUnknown, result.Err = true, err
			return result
		case egressWriteRefused:
			result.Err = err
			return result
		}
		dt.logger.V(2).Info("Cleared the NAT Gateway of the Service Gateway registration of egress identity", "egressIdentity", id)
	}

	if user := otherServiceUsingNATGateway(services, id, natGatewayID(id, dt.config)); user != "" {
		dt.logger.V(2).Info("Kept NAT Gateway linked to Service Gateway because another service uses it", "egressIdentity", id, "service", user)
		return result
	}
	read, err := dt.readEgressNAT(ctx, id)
	switch {
	case err != nil:
		result.Err = err
		return result
	case !read.Exists:
		return result
	case !read.Settled:
		result.Busy = true
		result.Err = fmt.Errorf("NAT Gateway %q is %s", id, ptr.Deref(read.NAT.Properties.ProvisioningState, ""))
		return result
	case !read.Ours:
		dt.logger.V(2).Info("Kept NAT Gateway not owned by this controller", "egressIdentity", id)
		return result
	case natNeedsLink(read.NAT, dt.config):
		return result
	}
	dt.noteEgressMutationStarted(id)
	_, err = dt.putEgressNAT(ctx, natUnlinkRequest(read.NAT))
	switch classifyEgressWrite(err).Outcome {
	case egressWriteUnknown:
		result.CommitUnknown, result.Err = true, err
		return result
	case egressWriteRefused:
		result.Err = err
		return result
	}
	dt.logger.V(2).Info("Unlinked NAT Gateway of egress identity from Service Gateway", "egressIdentity", id)
	return result
}

// deleteManagedEgress deletes the managed NAT Gateway of egress identity id, then every reserved managed name of both
// IP families that is the controller's: the NAT Gateway and Public IP steps of deleting a managed identity, after the
// unlink and unregister steps, for a NAT Gateway the controller may delete (ownsClusterNATGateway, checked before the
// unlink). The NAT Gateway is read right before the DELETE, which is conditional on that settled read's etag
// (If-Match): the unlink just before changed the etag, and a NAT Gateway written since is never deleted. The reserved
// names of both families are swept once it is gone, whatever families the identity served. A NAT Gateway that is no
// longer the controller's is left alone with its resources (NotApplicable). The deletion is complete (Complete) only
// once the NAT Gateway is gone and the reserved names are swept.
func (dt *DiffTracker) deleteManagedEgress(ctx context.Context, id string) egressRunResult {
	var result egressRunResult
	read, err := dt.readEgressNAT(ctx, id)
	switch {
	case err != nil:
		result.Err = err
		return result
	case !read.Exists:
	case !read.Settled:
		result.Busy = true
		return result
	case !read.Ours:
		dt.keepUnownedEgressNAT(id, &result)
		return result
	default:
		if !dt.deleteSettledEgressNAT(ctx, id, read.Etag, &result) {
			return result
		}
	}
	result.NATGone = true
	dt.sweepAfterEgressNATGone(ctx, id, &result)
	return result
}

// cleanupBYOResidue removes what BYO egress identity id has left of a managed past: the same-name NAT Gateway in the
// cluster resource group and the reserved managed names of both IP families that are the controller's. It never reads
// or writes the identity's BYO NAT Gateway, which is in another resource group, and never writes a Service Gateway
// registration.
//
// A NAT Gateway that exists is only removed when it is the controller's (ownsNATGateway) and no Service Gateway
// service uses it (a fresh read of the registrations), the semantics of ownsClusterNATGateway, except that the
// identity's own registration counts as a use too: a BYO identity's registration references its BYO NAT Gateway, and
// one that references this NAT Gateway still sends its egress through it. Otherwise the NAT Gateway and its resources
// are left alone (NotApplicable). A removed NAT Gateway is unlinked on its own side if it is linked
// (natUnlinkRequest, a PUT without precondition), read again (settled), and deleted on condition of that read's etag.
// The reserved names are swept once it is gone. The cleanup is complete (Complete) when the NAT Gateway is gone or not
// the controller's, and the reserved names are swept.
func (dt *DiffTracker) cleanupBYOResidue(ctx context.Context, id string) egressRunResult {
	var result egressRunResult
	read, err := dt.readEgressNAT(ctx, id)
	if err != nil {
		result.Err = err
		return result
	}
	if read.Exists {
		if !dt.byoResidueRemovable(ctx, id, read, &result) {
			return result
		}
		if natGatewayServiceGatewayID(read.NAT) != "" {
			var proceed bool
			if read, proceed = dt.unlinkBYOResidueNAT(ctx, id, read.NAT, &result); !proceed {
				return result
			}
		}
		if read.Exists && !dt.deleteSettledEgressNAT(ctx, id, read.Etag, &result) {
			return result
		}
	}
	result.NATGone = true
	dt.sweepAfterEgressNATGone(ctx, id, &result)
	return result
}

// unlinkBYOResidueNAT unlinks the removable NAT Gateway of BYO egress identity id, read as nat, on its own side with a
// PUT without precondition, then reads it again: the unlink changed the etag the DELETE is conditional on. It returns
// the fresh read and whether the cleanup may go on with it; otherwise result records why not: a write that did not
// succeed, a failed or unsettled read, or a NAT Gateway that is no longer the controller's.
func (dt *DiffTracker) unlinkBYOResidueNAT(ctx context.Context, id string, nat *armnetwork.NatGateway, result *egressRunResult) (egressNATRead, bool) {
	dt.noteEgressMutationStarted(id)
	_, err := dt.putEgressNAT(ctx, natUnlinkRequest(nat))
	switch classifyEgressWrite(err).Outcome {
	case egressWriteUnknown:
		result.CommitUnknown, result.Err = true, err
		return egressNATRead{}, false
	case egressWriteRefused:
		result.Err = err
		return egressNATRead{}, false
	}
	dt.logger.V(2).Info("Unlinked NAT Gateway left by a managed past of a BYO egress identity", "egressIdentity", id)

	fresh, err := dt.readEgressNAT(ctx, id)
	switch {
	case err != nil:
		result.Err = err
		return egressNATRead{}, false
	case !fresh.Exists:
		return fresh, true
	case !fresh.Settled:
		result.Busy = true
		return egressNATRead{}, false
	case !fresh.Ours:
		dt.keepUnownedEgressNAT(id, result)
		return egressNATRead{}, false
	}
	return fresh, true
}

// byoResidueRemovable reports whether the existing same-name NAT Gateway of BYO egress identity id, as read, may be
// removed: it is settled, the controller's, and used by no Service Gateway service. Otherwise it records in result why
// not: busy, a failed read, or not applicable.
func (dt *DiffTracker) byoResidueRemovable(ctx context.Context, id string, read egressNATRead, result *egressRunResult) bool {
	if !read.Settled {
		result.Busy = true
		return false
	}
	if !read.Ours {
		dt.keepUnownedEgressNAT(id, result)
		return false
	}
	used, user, err := dt.egressNATUser(ctx, natGatewayID(id, dt.config))
	if err != nil {
		result.Err = err
		return false
	}
	if used {
		dt.logger.V(2).Info("Kept NAT Gateway used by a Service Gateway service", "egressIdentity", id, "service", user)
		result.NotApplicable, result.Swept = true, true
		return false
	}
	return true
}

// keepUnownedEgressNAT records in result that the NAT Gateway of egress identity id is not owned by this controller:
// the run leaves it alone with its resources, so nothing is left that it may delete.
func (dt *DiffTracker) keepUnownedEgressNAT(id string, result *egressRunResult) {
	dt.logger.V(2).Info("Kept NAT Gateway not owned by this controller", "egressIdentity", id)
	result.NotApplicable, result.Swept = true, true
}

// egressNATUser reports whether a Service Gateway service, any identity's own included, uses the NAT Gateway natID,
// and the name of the first that does; none does when the Service Gateway does not exist. It reads the registrations
// afresh.
func (dt *DiffTracker) egressNATUser(ctx context.Context, natID string) (used bool, user string, err error) {
	services, err := dt.getEgressServices(ctx)
	if err != nil {
		return false, "", err
	}
	want := canonicalResourceID(natID)
	for _, service := range services {
		if service == nil || service.Properties == nil {
			continue
		}
		if canonicalResourceID(derefString(service.Properties.PublicNatGatewayID)) == want {
			return true, derefString(service.Name), nil
		}
	}
	return false, "", nil
}

// deleteSettledEgressNAT deletes the NAT Gateway of egress identity id on condition that it is still as read by a
// settled read taken right before, whose etag is etag (If-Match), with a single attempt. It reports whether the NAT
// Gateway is gone, and records the run's failure in result.
func (dt *DiffTracker) deleteSettledEgressNAT(ctx context.Context, id, etag string, result *egressRunResult) bool {
	if etag == "" {
		result.Err = fmt.Errorf("deleting NAT Gateway %q: its read has no etag to make the delete conditional", id)
		return false
	}
	dt.noteEgressMutationStarted(id)
	err := dt.deleteEgressNAT(ctx, id, etag)
	switch classifyEgressWrite(err).Outcome {
	case egressWriteUnknown:
		result.CommitUnknown, result.Err = true, err
		return false
	case egressWriteRefused:
		result.Err = err
		return false
	}
	dt.logger.V(2).Info("Deleted NAT Gateway of egress identity", "egressIdentity", id)
	return true
}

// sweepAfterEgressNATGone sweeps every reserved managed name of both IP families of egress identity id that is the
// controller's, once its NAT Gateway is gone, and records the outcome in result.
func (dt *DiffTracker) sweepAfterEgressNATGone(ctx context.Context, id string, result *egressRunResult) {
	swept, unknown, err := dt.sweepEgressReserved(ctx, id, true, nil)
	result.Swept = swept
	result.CommitUnknown = unknown
	result.Err = err
}
