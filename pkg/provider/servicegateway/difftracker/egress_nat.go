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
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

const (
	publicIPPrefixNameSuffix   = "-prefix"
	publicIPPrefixNameSuffixV6 = "-prefix-v6"
)

// egressManagedKind is the kind of resource a reserved managed name of an egress identity holds.
type egressManagedKind int

const (
	egressManagedPublicIP egressManagedKind = iota + 1
	egressManagedPrefix
)

// egressManagedName is a name an egress identity reserves for a Public IP or Public IP prefix the controller
// manages, apart from the identity: Public IPs "<id>-pip", "<id>-pip-2".."<id>-pip-16" and "<id>-pip-v6",
// "<id>-pip-v6-2".."<id>-pip-v6-16"; prefixes "<id>-prefix-28".."<id>-prefix-31" and
// "<id>-prefix-v6-124".."<id>-prefix-v6-127". The length in a prefix name lets a length change create the new
// prefix while the old one is still attached.
type egressManagedName struct {
	Kind egressManagedKind
	IPv6 bool
	// N is the number of a Public IP or the length of a prefix.
	N int
}

func (m egressManagedName) name(id string) string {
	if m.Kind == egressManagedPrefix {
		return managedPrefixName(id, m.IPv6, m.N)
	}
	return managedPublicIPName(id, m.IPv6, m.N)
}

// reserved reports whether m is one of the reserved names: its number or length is in range for its kind and family.
func (m egressManagedName) reserved() bool {
	switch m.Kind {
	case egressManagedPublicIP:
		return m.N >= 1 && m.N <= consts.ServiceEgressGatewayPublicIPCountMax
	case egressManagedPrefix:
		family := egressFamilyAnnotationsOf(m.IPv6)
		return m.N >= family.minPrefixLength && m.N <= family.maxPrefixLength
	default:
		return false
	}
}

// basePublicIP reports whether m is the first Public IP of its family, the one named before configuration existed.
func (m egressManagedName) basePublicIP() bool {
	return m.Kind == egressManagedPublicIP && m.N == 1
}

// managedPublicIPName returns the name of the n-th managed Public IP of an egress identity in one IP family. The
// first keeps the name it had before configuration existed (PublicIPName, PublicIPNameV6).
func managedPublicIPName(id string, ipv6 bool, n int) string {
	name := PublicIPName(id)
	if ipv6 {
		name = PublicIPNameV6(id)
	}
	if n == 1 {
		return name
	}
	return name + "-" + strconv.Itoa(n)
}

// managedPrefixName returns the name of the managed Public IP prefix of an egress identity of the given length.
func managedPrefixName(id string, ipv6 bool, length int) string {
	suffix := publicIPPrefixNameSuffix
	if ipv6 {
		suffix = publicIPPrefixNameSuffixV6
	}
	return id + suffix + "-" + strconv.Itoa(length)
}

func egressFamilyAnnotationsOf(ipv6 bool) egressFamilyAnnotations {
	if ipv6 {
		return egressIPv6Annotations
	}
	return egressIPv4Annotations
}

// egressServedFamilies returns the IP families of a NAT Gateway, as IPv6 flags: IPv4 always, IPv6 when it serves IPv6.
func egressServedFamilies(ipv6 bool) []bool {
	if ipv6 {
		return []bool{false, true}
	}
	return []bool{false}
}

// familyConfig returns the configuration of one IP family.
func (c EgressConfig) familyConfig(ipv6 bool) egressFamilyConfig {
	if ipv6 {
		return c.IPv6
	}
	return c.IPv4
}

// servedFamilyConfig returns the configuration of one IP family of a NAT Gateway: none for IPv6 when the NAT Gateway
// does not serve it.
func (c EgressConfig) servedFamilyConfig(v6, servesIPv6 bool) egressFamilyConfig {
	if v6 && !servesIPv6 {
		return egressFamilyConfig{}
	}
	return c.familyConfig(v6)
}

// reservedManaged returns the reserved managed names of the served families: 20 per family.
func reservedManaged(ipv6 bool) []egressManagedName {
	var names []egressManagedName
	for _, v6 := range egressServedFamilies(ipv6) {
		for n := 1; n <= consts.ServiceEgressGatewayPublicIPCountMax; n++ {
			names = append(names, egressManagedName{Kind: egressManagedPublicIP, IPv6: v6, N: n})
		}
		family := egressFamilyAnnotationsOf(v6)
		for length := family.minPrefixLength; length <= family.maxPrefixLength; length++ {
			names = append(names, egressManagedName{Kind: egressManagedPrefix, IPv6: v6, N: length})
		}
	}
	return names
}

// parseManagedName returns which reserved managed name of egress identity id name is, in either family. Names
// compare case-insensitively, as in Azure.
func parseManagedName(id, name string) (egressManagedName, bool) {
	for _, m := range reservedManaged(true) {
		if strings.EqualFold(m.name(id), name) {
			return m, true
		}
	}
	return egressManagedName{}, false
}

// managed returns the managed resources a family configuration asks for: Public IPs 1..count, then the prefix.
func (f egressFamilyConfig) managed(ipv6 bool) []egressManagedName {
	var names []egressManagedName
	for n := 1; n <= f.ManagedPublicIPs; n++ {
		names = append(names, egressManagedName{Kind: egressManagedPublicIP, IPv6: ipv6, N: n})
	}
	if f.ManagedPrefixLength > 0 {
		names = append(names, egressManagedName{Kind: egressManagedPrefix, IPv6: ipv6, N: f.ManagedPrefixLength})
	}
	return names
}

// references returns the IDs of the resources a family configuration asks the NAT Gateway to reference: managed
// Public IPs, then user Public IPs; the managed prefix, then user prefixes.
func (f egressFamilyConfig) references(id string, ipv6 bool, dt Config) (publicIPs, prefixes []string) {
	for _, m := range f.managed(ipv6) {
		if m.Kind == egressManagedPrefix {
			prefixes = append(prefixes, publicIPPrefixID(m.name(id), dt))
		} else {
			publicIPs = append(publicIPs, publicIPAddressID(m.name(id), dt))
		}
	}
	publicIPs = append(publicIPs, egressIDList(f.PublicIPIDs)...)
	prefixes = append(prefixes, egressIDList(f.PublicIPPrefixIDs)...)
	return publicIPs, prefixes
}

// egressIDList returns the IDs of a comma-joined canonical list.
func egressIDList(ids string) []string {
	if ids == "" {
		return nil
	}
	return strings.Split(ids, ",")
}

func natGatewayID(name string, dt Config) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/natGateways/%s",
		dt.networkResourceSubscriptionID(), dt.ResourceGroup, name)
}

func publicIPAddressID(name string, dt Config) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPAddresses/%s",
		dt.networkResourceSubscriptionID(), dt.ResourceGroup, name)
}

func publicIPPrefixID(name string, dt Config) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPPrefixes/%s",
		dt.networkResourceSubscriptionID(), dt.ResourceGroup, name)
}

// hasEgressIdentityTag reports whether tags carry the ownership tag of egress identity id. Like ownsNATGateway, it
// reads the key the controller writes; identities compare case-insensitively.
func hasEgressIdentityTag(tags map[string]*string, id string) bool {
	value, ok := tags[EgressIdentityTagKey]
	return ok && value != nil && strings.EqualFold(*value, id)
}

// ownsManagedName reports whether the controller may write and delete the existing resource that has reserved
// managed name name of egress identity id. A base Public IP is the controller's by its name, as before configuration
// existed (every "<id>-pip" and "<id>-pip-v6" of the node resource group is); every other reserved name is only with
// the ownership tag, so a resource someone else created under such a name is never touched.
func ownsManagedName(id, name string, tags map[string]*string) bool {
	m, ok := parseManagedName(id, name)
	return ok && (m.basePublicIP() || hasEgressIdentityTag(tags, id))
}

// managedOwnershipRecorded reports whether an existing managed resource of egress identity id is recorded as the
// identity's, as a satisfied configuration requires: it carries the ownership tag or, for a base Public IP without
// one, legacyNAT holds: it is referenced by the identity's NAT Gateway in the shape created before the controller
// tagged its resources (legacyEgressNAT). Numbered Public IPs and prefixes need the tag.
func managedOwnershipRecorded(id, name string, tags map[string]*string, legacyNAT bool) bool {
	m, ok := parseManagedName(id, name)
	if !ok {
		return false
	}
	if hasEgressIdentityTag(tags, id) {
		return true
	}
	_, tagged := tags[EgressIdentityTagKey]
	return m.basePublicIP() && !tagged && legacyNAT
}

// legacyEgressNAT reports whether nat is the NAT Gateway of egress identity id as created before the controller
// tagged its resources: without the ownership tag and in the default shape, referencing exactly the base Public IP
// of each served family and no prefix (a shape the legacy rule of ownsNATGateway owns).
func legacyEgressNAT(nat *armnetwork.NatGateway, id string, ipv6 bool, dt Config) bool {
	if nat == nil || nat.Properties == nil || !strings.EqualFold(derefString(nat.Name), id) {
		return false
	}
	if _, tagged := nat.Tags[EgressIdentityTagKey]; tagged {
		return false
	}
	var wantIPv6 []string
	if ipv6 {
		wantIPv6 = []string{publicIPAddressID(PublicIPNameV6(id), dt)}
	}
	props := nat.Properties
	return sameResourceIDs(props.PublicIPAddresses, []string{publicIPAddressID(PublicIPName(id), dt)}) &&
		sameResourceIDs(props.PublicIPAddressesV6, wantIPv6) &&
		len(props.PublicIPPrefixes) == 0 && len(props.PublicIPPrefixesV6) == 0
}

// egressTag is a configured tag.
type egressTag struct{ key, value string }

// egressTagList returns the tags of a canonical "k=v,..." list.
func egressTagList(canonical string) []egressTag {
	if canonical == "" {
		return nil
	}
	entries := strings.Split(canonical, consts.TagsDelimiter)
	tags := make([]egressTag, 0, len(entries))
	for _, entry := range entries {
		key, value, _ := strings.Cut(entry, consts.TagKeyValueDelimiter)
		tags = append(tags, egressTag{key: key, value: value})
	}
	return tags
}

// mergeTags returns the tags to write on a resource the controller manages for egress identity id: the existing tags,
// the configured tags (canonical "k=v,...") added or updated, and the ownership tag. Tags are only added, never
// removed, so tags set by others and tags a configuration no longer lists stay. A configured key updates an existing
// key that differs only in case and keeps its spelling, since Azure tag keys are case-insensitive; the ownership tag
// always gets the spelling the controller reads, replacing any other. It fails when the result has more tags than
// Azure accepts on a resource.
func mergeTags(existing map[string]*string, configured, id string) (map[string]*string, error) {
	merged := make(map[string]*string, len(existing)+1)
	keys := map[string]string{}
	for _, key := range slices.Sorted(maps.Keys(existing)) {
		if strings.EqualFold(key, EgressIdentityTagKey) {
			continue
		}
		merged[key] = clonePtr(existing[key])
		keys[strings.ToLower(key)] = key
	}
	for _, tag := range egressTagList(configured) {
		key := tag.key
		if existingKey, ok := keys[strings.ToLower(key)]; ok {
			key = existingKey
		}
		merged[key] = to.Ptr(tag.value)
	}
	merged[EgressIdentityTagKey] = to.Ptr(id)
	if len(merged) > consts.ServiceEgressGatewayMaxTags {
		return nil, fmt.Errorf("%d tags, more than the %d Azure accepts on a resource", len(merged), consts.ServiceEgressGatewayMaxTags)
	}
	return merged, nil
}

// missingEgressTags returns the configured tags (canonical "k=v,...") that tags lacks, in the same form, or "". Keys
// compare case-insensitively, as in Azure, and values exactly.
func missingEgressTags(tags map[string]*string, configured string) string {
	var missing []string
	for _, tag := range egressTagList(configured) {
		present := false
		for key, value := range tags {
			if strings.EqualFold(key, tag.key) && value != nil && *value == tag.value {
				present = true
				break
			}
		}
		if !present {
			missing = append(missing, tag.key+consts.TagKeyValueDelimiter+tag.value)
		}
	}
	return strings.Join(missing, consts.TagsDelimiter)
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneTags(tags map[string]*string) map[string]*string {
	if tags == nil {
		return nil
	}
	out := make(map[string]*string, len(tags))
	for key, value := range tags {
		out[key] = clonePtr(value)
	}
	return out
}

func cloneEach[T any](s []*T) []*T {
	if s == nil {
		return nil
	}
	out := make([]*T, len(s))
	for i, p := range s {
		out[i] = clonePtr(p)
	}
	return out
}

// natReferences returns references to ids. An empty list is nil, as in a NAT Gateway created before configuration
// existed, unless the existing NAT Gateway has references there: then it is empty, so the PUT removes them explicitly.
func natReferences(ids []string, existing []*armnetwork.SubResource) []*armnetwork.SubResource {
	if len(ids) == 0 {
		if len(existing) > 0 {
			return []*armnetwork.SubResource{}
		}
		return nil
	}
	refs := make([]*armnetwork.SubResource, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, &armnetwork.SubResource{ID: to.Ptr(id)})
	}
	return refs
}

// natWritable returns the writable fields of a NAT Gateway as read: what a PUT built from the read sends back
// unchanged. The name and ID are kept as well, as every request of the controller carries them. Read-only fields
// (etag, type, provisioning state, resource GUID, subnets) are never copied, and the result shares no memory with
// nat. A nil nat gives an empty NAT Gateway.
func natWritable(nat *armnetwork.NatGateway) armnetwork.NatGateway {
	writable := armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{}}
	if nat == nil {
		return writable
	}
	writable.Name = clonePtr(nat.Name)
	writable.ID = clonePtr(nat.ID)
	writable.Location = clonePtr(nat.Location)
	if nat.SKU != nil {
		writable.SKU = &armnetwork.NatGatewaySKU{Name: clonePtr(nat.SKU.Name)}
	}
	writable.Tags = cloneTags(nat.Tags)
	writable.Zones = cloneEach(nat.Zones)
	if props := nat.Properties; props != nil {
		writable.Properties = &armnetwork.NatGatewayPropertiesFormat{
			IdleTimeoutInMinutes: clonePtr(props.IdleTimeoutInMinutes),
			Nat64:                clonePtr(props.Nat64),
			PublicIPAddresses:    cloneSubResources(props.PublicIPAddresses),
			PublicIPAddressesV6:  cloneSubResources(props.PublicIPAddressesV6),
			PublicIPPrefixes:     cloneSubResources(props.PublicIPPrefixes),
			PublicIPPrefixesV6:   cloneSubResources(props.PublicIPPrefixesV6),
			ServiceGateway:       cloneSubResource(props.ServiceGateway),
			SourceVirtualNetwork: cloneSubResource(props.SourceVirtualNetwork),
		}
	}
	return writable
}

func cloneSubResource(ref *armnetwork.SubResource) *armnetwork.SubResource {
	if ref == nil {
		return nil
	}
	return &armnetwork.SubResource{ID: clonePtr(ref.ID)}
}

func cloneSubResources(refs []*armnetwork.SubResource) []*armnetwork.SubResource {
	if refs == nil {
		return nil
	}
	out := make([]*armnetwork.SubResource, len(refs))
	for i, ref := range refs {
		out[i] = cloneSubResource(ref)
	}
	return out
}

// natRequest returns the NAT Gateway of egress identity id as cfg configures it, to be written whole (PUT). It starts
// from the writable fields of existing (natWritable), so the ones the controller does not set are kept (zones, NAT64,
// source virtual network), and sets the name, ID, StandardV2 SKU, location, Service Gateway link, tags (mergeTags),
// idle timeout, and the Public IPs and prefixes of each family: the managed ones cfg asks for, then the user ones.
// IPv6 references are made only when the NAT Gateway serves IPv6, and an idle timeout that is and was the default is
// left unset. From scratch (existing nil) and for the default configuration this is the NAT Gateway created before
// configuration existed.
func natRequest(existing *armnetwork.NatGateway, id string, cfg EgressConfig, ipv6 bool, dt Config) (armnetwork.NatGateway, error) {
	if cfg.IdleTimeoutMinutes < consts.ServiceEgressGatewayIdleTimeoutMinMinutes ||
		cfg.IdleTimeoutMinutes > consts.ServiceEgressGatewayIdleTimeoutMaxMinutes {
		return armnetwork.NatGateway{}, fmt.Errorf("egress NAT Gateway %q: idle timeout %d is not from %d to %d minutes", id,
			cfg.IdleTimeoutMinutes, consts.ServiceEgressGatewayIdleTimeoutMinMinutes, consts.ServiceEgressGatewayIdleTimeoutMaxMinutes)
	}
	for _, v6 := range egressServedFamilies(ipv6) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			if !m.reserved() {
				return armnetwork.NatGateway{}, fmt.Errorf("egress NAT Gateway %q: managed resource %q is not a reserved name", id, m.name(id))
			}
		}
	}
	request := natWritable(existing)
	tags, err := mergeTags(request.Tags, cfg.Tags, id)
	if err != nil {
		return armnetwork.NatGateway{}, fmt.Errorf("egress NAT Gateway %q: %w", id, err)
	}

	request.Name = to.Ptr(id)
	request.ID = to.Ptr(natGatewayID(id, dt))
	request.SKU = &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandardV2)}
	request.Location = to.Ptr(dt.Location)
	request.Tags = tags
	props := request.Properties
	props.ServiceGateway = &armnetwork.SubResource{ID: to.Ptr(dt.ServiceGatewayResourceID())}
	if cfg.IdleTimeoutMinutes != consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes ||
		ptr.Deref(props.IdleTimeoutInMinutes, consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes) != consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes {
		props.IdleTimeoutInMinutes = to.Ptr(cfg.IdleTimeoutMinutes)
	} else {
		props.IdleTimeoutInMinutes = nil
	}
	publicIPs, prefixes := cfg.IPv4.references(id, false, dt)
	props.PublicIPAddresses = natReferences(publicIPs, props.PublicIPAddresses)
	props.PublicIPPrefixes = natReferences(prefixes, props.PublicIPPrefixes)
	publicIPs, prefixes = cfg.servedFamilyConfig(true, ipv6).references(id, true, dt)
	props.PublicIPAddressesV6 = natReferences(publicIPs, props.PublicIPAddressesV6)
	props.PublicIPPrefixesV6 = natReferences(prefixes, props.PublicIPPrefixesV6)
	return request, nil
}

// natLinkRequest returns a settled NAT Gateway as read and linked to the cluster's Service Gateway, to be written
// whole (PUT). Nothing else changes, so repairing the link never changes the configuration: every writable field is
// kept as read and read-only fields are not copied (natWritable). A nil existing gives a request without a name,
// which the NAT Gateway client refuses to write.
func natLinkRequest(existing *armnetwork.NatGateway, dt Config) armnetwork.NatGateway {
	request := natWritable(existing)
	request.Properties.ServiceGateway = &armnetwork.SubResource{ID: to.Ptr(dt.ServiceGatewayResourceID())}
	return request
}

// natUnlinkRequest returns a settled NAT Gateway as read and unlinked from its Service Gateway, to be written whole
// (PUT): the BYO residue cleanup unlinks a NAT Gateway on its own side only before deleting it, and leaves the Service
// Gateway's registrations alone. Nothing else changes: every writable field is kept as read and read-only fields are
// not copied (natWritable). The link is azcore's null value, which marshals as JSON null: Azure keeps a field a PUT
// leaves out, so only null clears it. That value is shared, so the request's link must not be modified. A nil existing
// gives a request without a name, which the NAT Gateway client refuses to write.
func natUnlinkRequest(existing *armnetwork.NatGateway) armnetwork.NatGateway {
	request := natWritable(existing)
	request.Properties.ServiceGateway = azcore.NullValue[*armnetwork.SubResource]()
	return request
}

func ipVersion(ipv6 bool) armnetwork.IPVersion {
	if ipv6 {
		return armnetwork.IPVersionIPv6
	}
	return armnetwork.IPVersionIPv4
}

// managedPublicIPRequest returns the n-th managed Public IP of one IP family of egress identity id as cfg configures
// it, to be written whole (PUT). The controller sets the name, ID, StandardV2 SKU, location, tags (mergeTags), static
// allocation and IP version; from scratch (existing nil) this is the Public IP created before configuration existed.
// Otherwise the writable fields of existing the controller does not set are kept (zones, extended location, SKU tier,
// DNS and DDoS settings, IP tags, idle timeout, delete option, migration phase, the prefix it was allocated from, and
// linked addresses as references); read-only and derived fields (etag, type, IP address, IP configuration, the NAT
// Gateway it is attached to, provisioning state, resource GUID, upgrade flag) are never copied.
func managedPublicIPRequest(existing *armnetwork.PublicIPAddress, id string, ipv6 bool, n int, cfg EgressConfig, dt Config) (armnetwork.PublicIPAddress, error) {
	m := egressManagedName{Kind: egressManagedPublicIP, IPv6: ipv6, N: n}
	name := m.name(id)
	if !m.reserved() {
		return armnetwork.PublicIPAddress{}, fmt.Errorf("egress Public IP %q: not a reserved name", name)
	}
	var current armnetwork.PublicIPAddress
	if existing != nil {
		current = *existing
	}
	tags, err := mergeTags(current.Tags, cfg.Tags, id)
	if err != nil {
		return armnetwork.PublicIPAddress{}, fmt.Errorf("egress Public IP %q: %w", name, err)
	}

	request := armnetwork.PublicIPAddress{
		Name:             to.Ptr(name),
		ID:               to.Ptr(publicIPAddressID(name, dt)),
		SKU:              &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandardV2)},
		Location:         to.Ptr(dt.Location),
		Tags:             tags,
		Zones:            cloneEach(current.Zones),
		ExtendedLocation: clonePtr(current.ExtendedLocation),
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{
			PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic),
			PublicIPAddressVersion:   to.Ptr(ipVersion(ipv6)),
		},
	}
	if current.SKU != nil {
		request.SKU.Tier = clonePtr(current.SKU.Tier)
	}
	if props := current.Properties; props != nil {
		request.Properties.DNSSettings = clonePtr(props.DNSSettings)
		request.Properties.DdosSettings = clonePtr(props.DdosSettings)
		request.Properties.IPTags = cloneEach(props.IPTags)
		request.Properties.IdleTimeoutInMinutes = clonePtr(props.IdleTimeoutInMinutes)
		request.Properties.DeleteOption = clonePtr(props.DeleteOption)
		request.Properties.MigrationPhase = clonePtr(props.MigrationPhase)
		request.Properties.PublicIPPrefix = cloneSubResource(props.PublicIPPrefix)
		request.Properties.LinkedPublicIPAddress = publicIPReference(props.LinkedPublicIPAddress)
		request.Properties.ServicePublicIPAddress = publicIPReference(props.ServicePublicIPAddress)
	}
	return request, nil
}

// publicIPReference returns a reference to the Public IP pip, without the fields of pip itself.
func publicIPReference(pip *armnetwork.PublicIPAddress) *armnetwork.PublicIPAddress {
	if pip == nil || pip.ID == nil {
		return nil
	}
	return &armnetwork.PublicIPAddress{ID: to.Ptr(*pip.ID)}
}

// managedPrefixRequest returns the managed Public IP prefix of the given length of one IP family of egress identity
// id as cfg configures it, to be written whole (PUT). The controller sets the name, ID, StandardV2 SKU, location, tags
// (mergeTags), prefix length and IP version. With an existing prefix the writable fields the controller does not set
// are kept (zones, extended location, SKU tier, IP tags, custom IP prefix); read-only and derived fields (etag, type,
// allocated prefix, NAT Gateway, load balancer frontend, Public IPs, provisioning state, resource GUID, upgrade flag)
// are never copied.
func managedPrefixRequest(existing *armnetwork.PublicIPPrefix, id string, ipv6 bool, length int, cfg EgressConfig, dt Config) (armnetwork.PublicIPPrefix, error) {
	m := egressManagedName{Kind: egressManagedPrefix, IPv6: ipv6, N: length}
	name := m.name(id)
	if !m.reserved() {
		return armnetwork.PublicIPPrefix{}, fmt.Errorf("egress Public IP prefix %q: not a reserved name", name)
	}
	var current armnetwork.PublicIPPrefix
	if existing != nil {
		current = *existing
	}
	tags, err := mergeTags(current.Tags, cfg.Tags, id)
	if err != nil {
		return armnetwork.PublicIPPrefix{}, fmt.Errorf("egress Public IP prefix %q: %w", name, err)
	}

	request := armnetwork.PublicIPPrefix{
		Name:             to.Ptr(name),
		ID:               to.Ptr(publicIPPrefixID(name, dt)),
		SKU:              &armnetwork.PublicIPPrefixSKU{Name: to.Ptr(armnetwork.PublicIPPrefixSKUNameStandardV2)},
		Location:         to.Ptr(dt.Location),
		Tags:             tags,
		Zones:            cloneEach(current.Zones),
		ExtendedLocation: clonePtr(current.ExtendedLocation),
		Properties: &armnetwork.PublicIPPrefixPropertiesFormat{
			PrefixLength:           to.Ptr(int32(length)),
			PublicIPAddressVersion: to.Ptr(ipVersion(ipv6)),
		},
	}
	if current.SKU != nil {
		request.SKU.Tier = clonePtr(current.SKU.Tier)
	}
	if props := current.Properties; props != nil {
		request.Properties.IPTags = cloneEach(props.IPTags)
		request.Properties.CustomIPPrefix = cloneSubResource(props.CustomIPPrefix)
	}
	return request, nil
}

// egressManagedResources holds settled reads of the managed Public IPs and prefixes of egress identities, by name
// (case-insensitively). A name without an entry does not exist. The zero value holds nothing.
type egressManagedResources struct {
	publicIPs map[string]*armnetwork.PublicIPAddress
	prefixes  map[string]*armnetwork.PublicIPPrefix
}

func newEgressManagedResources(publicIPs []*armnetwork.PublicIPAddress, prefixes []*armnetwork.PublicIPPrefix) egressManagedResources {
	r := egressManagedResources{
		publicIPs: make(map[string]*armnetwork.PublicIPAddress, len(publicIPs)),
		prefixes:  make(map[string]*armnetwork.PublicIPPrefix, len(prefixes)),
	}
	for _, pip := range publicIPs {
		if pip != nil && pip.Name != nil {
			r.publicIPs[strings.ToLower(*pip.Name)] = pip
		}
	}
	for _, prefix := range prefixes {
		if prefix != nil && prefix.Name != nil {
			r.prefixes[strings.ToLower(*prefix.Name)] = prefix
		}
	}
	return r
}

func (r egressManagedResources) publicIP(name string) *armnetwork.PublicIPAddress {
	return r.publicIPs[strings.ToLower(name)]
}

func (r egressManagedResources) prefix(name string) *armnetwork.PublicIPPrefix {
	return r.prefixes[strings.ToLower(name)]
}

// natSatisfies reports whether the NAT Gateway of egress identity id, and the managed resources it references, are
// as cfg configures them on every field the projections control, so that applying cfg needs no write. managed holds
// settled reads of the identity's managed resources.
func natSatisfies(nat *armnetwork.NatGateway, managed egressManagedResources, id string, cfg EgressConfig, ipv6 bool, dt Config) bool {
	return natMismatch(nat, managed, id, cfg, ipv6, dt) == ""
}

// natMismatch returns the first way the NAT Gateway of egress identity id, or a managed resource it references,
// differs from cfg, or "" when it satisfies cfg. The NAT Gateway must have succeeded provisioning, have the StandardV2
// SKU and the cluster's location, be linked to the cluster's Service Gateway, be owned by the identity (ownership tag
// or ownsNATGateway's legacy rule), carry the configured tags and the idle timeout (unset means the default), and
// reference exactly the managed resources and user resources cfg asks for in each family (IDs compare
// case-insensitively). Each referenced managed resource must satisfy managedPublicIPMismatch or managedPrefixMismatch.
func natMismatch(nat *armnetwork.NatGateway, managed egressManagedResources, id string, cfg EgressConfig, ipv6 bool, dt Config) string {
	if nat == nil {
		return "the NAT Gateway does not exist"
	}
	var props armnetwork.NatGatewayPropertiesFormat
	if nat.Properties != nil {
		props = *nat.Properties
	}
	var sku string
	if nat.SKU != nil {
		sku = string(ptr.Deref(nat.SKU.Name, ""))
	}
	idle := ptr.Deref(props.IdleTimeoutInMinutes, consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes)
	switch {
	case !strings.EqualFold(derefString(nat.Name), id):
		return fmt.Sprintf("the NAT Gateway is named %q", derefString(nat.Name))
	case !provisioningSucceeded(props.ProvisioningState):
		return fmt.Sprintf("the NAT Gateway's provisioning state is %q", ptr.Deref(props.ProvisioningState, ""))
	case !strings.EqualFold(sku, string(armnetwork.NatGatewaySKUNameStandardV2)):
		return fmt.Sprintf("the NAT Gateway's SKU is %q", sku)
	case !sameLocation(nat.Location, dt.Location):
		return fmt.Sprintf("the NAT Gateway's location is %q", derefString(nat.Location))
	case natGatewayServiceGatewayID(nat) == "":
		return "the NAT Gateway is not linked to a Service Gateway"
	case canonicalResourceID(natGatewayServiceGatewayID(nat)) != canonicalResourceID(dt.ServiceGatewayResourceID()):
		return fmt.Sprintf("the NAT Gateway is linked to Service Gateway %q", natGatewayServiceGatewayID(nat))
	case !ownsNATGateway(nat, dt):
		return "the NAT Gateway does not carry the identity's ownership tag"
	case missingEgressTags(nat.Tags, cfg.Tags) != "":
		return fmt.Sprintf("the NAT Gateway lacks tags %s", missingEgressTags(nat.Tags, cfg.Tags))
	case idle != cfg.IdleTimeoutMinutes:
		return fmt.Sprintf("the NAT Gateway's idle timeout is %d minutes", idle)
	}

	for _, v6 := range egressServedFamilies(true) {
		wantPublicIPs, wantPrefixes := cfg.servedFamilyConfig(v6, ipv6).references(id, v6, dt)
		publicIPs, prefixes := props.PublicIPAddresses, props.PublicIPPrefixes
		if v6 {
			publicIPs, prefixes = props.PublicIPAddressesV6, props.PublicIPPrefixesV6
		}
		if !sameResourceIDs(publicIPs, wantPublicIPs) {
			return fmt.Sprintf("the NAT Gateway's %s Public IPs are %v, not %v", ipVersion(v6), referenceNames(publicIPs), resourceNames(wantPublicIPs))
		}
		if !sameResourceIDs(prefixes, wantPrefixes) {
			return fmt.Sprintf("the NAT Gateway's %s Public IP prefixes are %v, not %v", ipVersion(v6), referenceNames(prefixes), resourceNames(wantPrefixes))
		}
	}
	legacy := legacyEgressNAT(nat, id, ipv6, dt)
	for _, v6 := range egressServedFamilies(ipv6) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			var problem string
			if m.Kind == egressManagedPrefix {
				problem = managedPrefixMismatch(managed.prefix(m.name(id)), id, v6, m.N, cfg, dt)
			} else {
				problem = managedPublicIPMismatch(managed.publicIP(m.name(id)), id, v6, m.N, cfg, dt, legacy)
			}
			if problem != "" {
				return problem
			}
		}
	}
	return ""
}

// managedPublicIPMismatch returns the first way an existing managed Public IP differs from the n-th Public IP of one
// IP family as cfg configures it, or "": it must exist, have succeeded provisioning, have the StandardV2 SKU, the
// cluster's location, static allocation and the family's IP version, be recorded as the identity's
// (managedOwnershipRecorded with legacyNAT) and carry the configured tags.
func managedPublicIPMismatch(pip *armnetwork.PublicIPAddress, id string, ipv6 bool, n int, cfg EgressConfig, dt Config, legacyNAT bool) string {
	name := managedPublicIPName(id, ipv6, n)
	if pip == nil {
		return fmt.Sprintf("Public IP %q does not exist", name)
	}
	var props armnetwork.PublicIPAddressPropertiesFormat
	if pip.Properties != nil {
		props = *pip.Properties
	}
	var sku string
	if pip.SKU != nil {
		sku = string(ptr.Deref(pip.SKU.Name, ""))
	}
	allocation := ptr.Deref(props.PublicIPAllocationMethod, "")
	version := ptr.Deref(props.PublicIPAddressVersion, armnetwork.IPVersionIPv4)
	switch {
	case !provisioningSucceeded(props.ProvisioningState):
		return fmt.Sprintf("Public IP %q has provisioning state %q", name, ptr.Deref(props.ProvisioningState, ""))
	case !strings.EqualFold(sku, string(armnetwork.PublicIPAddressSKUNameStandardV2)):
		return fmt.Sprintf("Public IP %q has SKU %q", name, sku)
	case !sameLocation(pip.Location, dt.Location):
		return fmt.Sprintf("Public IP %q is in location %q", name, derefString(pip.Location))
	case !strings.EqualFold(string(allocation), string(armnetwork.IPAllocationMethodStatic)):
		return fmt.Sprintf("Public IP %q has allocation method %q", name, allocation)
	case !strings.EqualFold(string(version), string(ipVersion(ipv6))):
		return fmt.Sprintf("Public IP %q has IP version %q", name, version)
	case !managedOwnershipRecorded(id, name, pip.Tags, legacyNAT):
		return fmt.Sprintf("Public IP %q does not carry the identity's ownership tag", name)
	case missingEgressTags(pip.Tags, cfg.Tags) != "":
		return fmt.Sprintf("Public IP %q lacks tags %s", name, missingEgressTags(pip.Tags, cfg.Tags))
	}
	return ""
}

// managedPrefixMismatch returns the first way an existing managed Public IP prefix differs from the prefix of the
// given length of one IP family as cfg configures it, or "": it must exist, have succeeded provisioning, have the
// StandardV2 SKU, the cluster's location, the length and the family's IP version, carry the ownership tag and the
// configured tags.
func managedPrefixMismatch(prefix *armnetwork.PublicIPPrefix, id string, ipv6 bool, length int, cfg EgressConfig, dt Config) string {
	name := managedPrefixName(id, ipv6, length)
	if prefix == nil {
		return fmt.Sprintf("Public IP prefix %q does not exist", name)
	}
	var props armnetwork.PublicIPPrefixPropertiesFormat
	if prefix.Properties != nil {
		props = *prefix.Properties
	}
	var sku string
	if prefix.SKU != nil {
		sku = string(ptr.Deref(prefix.SKU.Name, ""))
	}
	version := ptr.Deref(props.PublicIPAddressVersion, armnetwork.IPVersionIPv4)
	switch {
	case !provisioningSucceeded(props.ProvisioningState):
		return fmt.Sprintf("Public IP prefix %q has provisioning state %q", name, ptr.Deref(props.ProvisioningState, ""))
	case !strings.EqualFold(sku, string(armnetwork.PublicIPPrefixSKUNameStandardV2)):
		return fmt.Sprintf("Public IP prefix %q has SKU %q", name, sku)
	case !sameLocation(prefix.Location, dt.Location):
		return fmt.Sprintf("Public IP prefix %q is in location %q", name, derefString(prefix.Location))
	case props.PrefixLength == nil || int(*props.PrefixLength) != length:
		return fmt.Sprintf("Public IP prefix %q has length %d", name, ptr.Deref(props.PrefixLength, 0))
	case !strings.EqualFold(string(version), string(ipVersion(ipv6))):
		return fmt.Sprintf("Public IP prefix %q has IP version %q", name, version)
	case !hasEgressIdentityTag(prefix.Tags, id):
		return fmt.Sprintf("Public IP prefix %q does not carry the identity's ownership tag", name)
	case missingEgressTags(prefix.Tags, cfg.Tags) != "":
		return fmt.Sprintf("Public IP prefix %q lacks tags %s", name, missingEgressTags(prefix.Tags, cfg.Tags))
	}
	return ""
}

// provisioningSucceeded reports whether a provisioning state is Succeeded, the only state that satisfies a
// configuration: Failed and Canceled are settled but are re-applied, and the others are not settled.
func provisioningSucceeded(state *armnetwork.ProvisioningState) bool {
	return state != nil && strings.EqualFold(string(*state), string(armnetwork.ProvisioningStateSucceeded))
}

// sameLocation reports whether location is the Azure location want; Azure writes locations in lower case without
// spaces.
func sameLocation(location *string, want string) bool {
	normalize := func(s string) string { return strings.ToLower(strings.ReplaceAll(s, " ", "")) }
	return location != nil && normalize(*location) == normalize(want)
}

// canonicalResourceID returns an ARM resource ID in the canonical form of user resource IDs, lower-cased as parsed,
// so that IDs naming the same resource compare equal.
func canonicalResourceID(id string) string {
	if parsed, err := arm.ParseResourceID(id); err == nil {
		return strings.ToLower(parsed.String())
	}
	return strings.ToLower(id)
}

// sameResourceIDs reports whether refs reference exactly the resources ids names, each once, in any order.
func sameResourceIDs(refs []*armnetwork.SubResource, ids []string) bool {
	have := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			return false
		}
		have = append(have, canonicalResourceID(derefString(ref.ID)))
	}
	want := make([]string, 0, len(ids))
	for _, id := range ids {
		want = append(want, canonicalResourceID(id))
	}
	slices.Sort(have)
	slices.Sort(want)
	return slices.Equal(have, want)
}

// referenceNames returns the names of the referenced resources, for messages.
func referenceNames(refs []*armnetwork.SubResource) []string {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref != nil {
			ids = append(ids, derefString(ref.ID))
		}
	}
	return resourceNames(ids)
}

// resourceNames returns the names of the resources with the given IDs, for messages.
func resourceNames(ids []string) []string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		names = append(names, id[strings.LastIndex(id, "/")+1:])
	}
	return names
}
