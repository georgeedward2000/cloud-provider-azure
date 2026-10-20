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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

// EgressConfig is the NAT Gateway configuration of an egress identity. It is a comparable value in
// canonical form, so two spellings of one configuration are equal and it can key a map.
type EgressConfig struct {
	IdleTimeoutMinutes int32
	IPv4, IPv6         egressFamilyConfig
	// Tags is "k=v,..." sorted by lower-cased key.
	Tags string
}

// egressFamilyConfig is the public address configuration of one IP family of a NAT Gateway.
type egressFamilyConfig struct {
	ManagedPublicIPs int
	// ManagedPrefixLength is the length of the managed Public IP prefix; 0 means none.
	ManagedPrefixLength int
	// PublicIPIDs and PublicIPPrefixIDs are user resource IDs: lower-cased, unique, sorted and comma-joined.
	PublicIPIDs       string
	PublicIPPrefixIDs string
}

// egressUnknown is the applied configuration of a NAT Gateway whose configuration is not known. Parsing
// never produces it (its idle timeout is out of range), so it differs from every configuration a pod asks for.
var egressUnknown = EgressConfig{IdleTimeoutMinutes: -1}

const (
	publicIPAddressResourceType = "Microsoft.Network/publicIPAddresses"
	publicIPPrefixResourceType  = "Microsoft.Network/publicIPPrefixes"

	// maxConfiguredEgressTags leaves room for the ownership tag on every resource the tags are added to.
	maxConfiguredEgressTags = consts.ServiceEgressGatewayMaxTags - 1

	// egressUserPrefixMinAddresses is the number of addresses a user Public IP prefix counts for until its size is
	// read: that of the smallest prefix of either IP family (/31, /127).
	egressUserPrefixMinAddresses = 2
)

// reservedEgressTagKeys are the tag keys whose values the controller owns on the resources configured tags
// are added to: the egress ownership tag, and the tags from which the LoadBalancer code infers the Service
// that owns a Public IP of the cluster.
var reservedEgressTagKeys = []string{
	EgressIdentityTagKey,
	consts.ClusterNameKey, consts.LegacyClusterNameKey,
	consts.ServiceTagKey, consts.LegacyServiceTagKey,
	consts.ServiceUsingDNSKey, consts.LegacyServiceUsingDNSKey,
}

// egressParseContext is what parsing needs to know about the cluster and the egress identity.
type egressParseContext struct {
	// WantsIPv6 reports whether the identity's NAT Gateway serves IPv6.
	WantsIPv6 bool
	// NetworkSubscription holds the cluster's network resources; user resources must be in it.
	NetworkSubscription string
	// NodeResourceGroup is managed by the cluster; user resources must not be in it.
	NodeResourceGroup string
}

// egressFamilyAnnotations are the annotations of one IP family and the managed prefix lengths it accepts.
type egressFamilyAnnotations struct {
	family                                      string
	count, prefixLength, publicIPIDs, prefixIDs string
	minPrefixLength, maxPrefixLength, bits      int
}

var (
	egressIPv4Annotations = egressFamilyAnnotations{
		family:          string(armnetwork.IPVersionIPv4),
		count:           consts.PodAnnotationServiceEgressGatewayPublicIPCount,
		prefixLength:    consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength,
		publicIPIDs:     consts.PodAnnotationServiceEgressGatewayPublicIPIDs,
		prefixIDs:       consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs,
		minPrefixLength: consts.ServiceEgressGatewayPublicIPPrefixLengthMin,
		maxPrefixLength: consts.ServiceEgressGatewayPublicIPPrefixLengthMax,
		bits:            32,
	}
	egressIPv6Annotations = egressFamilyAnnotations{
		family:          string(armnetwork.IPVersionIPv6),
		count:           consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6,
		prefixLength:    consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6,
		publicIPIDs:     consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6,
		prefixIDs:       consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6,
		minPrefixLength: consts.ServiceEgressGatewayPublicIPPrefixLengthIPv6Min,
		maxPrefixLength: consts.ServiceEgressGatewayPublicIPPrefixLengthIPv6Max,
		bits:            128,
	}
)

func (a egressFamilyAnnotations) keys() []string {
	return []string{a.count, a.prefixLength, a.publicIPIDs, a.prefixIDs}
}

func isEgressAnnotation(key string) bool {
	return key == consts.PodAnnotationServiceEgressGatewayIdleTimeout || key == consts.PodAnnotationServiceEgressGatewayTags ||
		slices.Contains(egressIPv4Annotations.keys(), key) || slices.Contains(egressIPv6Annotations.keys(), key)
}

// defaultEgressConfig is the configuration of a pod without egress annotations: the default idle timeout
// and one managed Public IP per IP family the identity uses, as before configuration existed.
func defaultEgressConfig(pc egressParseContext) EgressConfig {
	cfg := EgressConfig{
		IdleTimeoutMinutes: consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes,
		IPv4:               egressFamilyConfig{ManagedPublicIPs: 1},
	}
	if pc.WantsIPv6 {
		cfg.IPv6.ManagedPublicIPs = 1
	}
	return cfg
}

// parseEgressConfig returns the configuration a pod's annotations vote for. hasAnnotations reports whether
// any annotation with the egress prefix is set, even an invalid one or one with the default value. err lists
// every problem, problems of a whole IP family first and then by annotation, and cfg is then the zero value.
// Only checks that need no Azure call are made; whether user resources exist and fit is checked when the
// configuration is applied.
func parseEgressConfig(ann map[string]string, pc egressParseContext) (cfg EgressConfig, hasAnnotations bool, err error) {
	p := egressAnnotationParser{pc: pc, values: map[string]string{}, idKeys: map[string]string{}}
	for key, value := range ann {
		if strings.HasPrefix(key, consts.PodAnnotationServiceEgressGatewayPrefix) {
			p.values[key] = strings.TrimSpace(value)
		}
	}
	cfg = defaultEgressConfig(pc)
	if len(p.values) == 0 {
		return cfg, false, nil
	}

	for key := range p.values {
		if !isEgressAnnotation(key) {
			p.report(key, "unknown annotation")
		}
	}
	if idle, ok := p.integer(consts.PodAnnotationServiceEgressGatewayIdleTimeout,
		consts.ServiceEgressGatewayIdleTimeoutMinMinutes, consts.ServiceEgressGatewayIdleTimeoutMaxMinutes); ok {
		cfg.IdleTimeoutMinutes = int32(idle)
	}
	cfg.IPv4 = p.family(egressIPv4Annotations, true, cfg.IPv4)
	cfg.IPv6 = p.family(egressIPv6Annotations, pc.WantsIPv6, cfg.IPv6)
	cfg.Tags = p.tags(consts.PodAnnotationServiceEgressGatewayTags)

	if err = p.err(); err != nil {
		return EgressConfig{}, true, err
	}
	return cfg, true, nil
}

// egressAnnotationParser collects every problem of a pod's egress annotations.
type egressAnnotationParser struct {
	pc egressParseContext
	// values are the trimmed values of the annotations with the egress prefix.
	values map[string]string
	// idKeys maps every user resource ID accepted so far to its annotation.
	idKeys   map[string]string
	problems []egressProblem
}

// egressProblem is a problem with an annotation, or with an IP family as a whole.
type egressProblem struct{ key, problem string }

func (p *egressAnnotationParser) report(key, problem string) {
	p.problems = append(p.problems, egressProblem{key: key, problem: problem})
}

func (p *egressAnnotationParser) err() error {
	if len(p.problems) == 0 {
		return nil
	}
	slices.SortStableFunc(p.problems, func(a, b egressProblem) int { return strings.Compare(a.key, b.key) })
	messages := make([]string, 0, len(p.problems))
	for _, problem := range p.problems {
		messages = append(messages, problem.key+": "+problem.problem)
	}
	return errors.New(strings.Join(messages, "; "))
}

// value returns the value of an annotation and whether it is set. An empty value is reported.
func (p *egressAnnotationParser) value(key string) (string, bool) {
	value, set := p.values[key]
	if set && value == "" {
		p.report(key, "empty value")
		return "", false
	}
	return value, set
}

// integer returns the value of an annotation that must be an integer from lo to hi, and whether it is set
// and valid.
func (p *egressAnnotationParser) integer(key string, lo, hi int) (int, bool) {
	value, set := p.value(key)
	if !set {
		return 0, false
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < lo || n > hi {
		p.report(key, fmt.Sprintf("%q is not an integer from %d to %d", value, lo, hi))
		return 0, false
	}
	return n, true
}

// family returns the configuration of one IP family. A family the identity does not use accepts no
// annotation. Every family the identity uses needs at least one public address and accepts at most
// consts.ServiceEgressGatewayMaxAddressesPerFamily, counting each user prefix at its smallest size
// (egressUserPrefixMinAddresses).
func (p *egressAnnotationParser) family(a egressFamilyAnnotations, served bool, def egressFamilyConfig) egressFamilyConfig {
	if !served {
		for _, key := range a.keys() {
			if _, set := p.values[key]; set {
				p.report(key, fmt.Sprintf("this egress identity does not use %s", a.family))
			}
		}
		return def
	}

	before := len(p.problems)
	count, countSet := p.integer(a.count, consts.ServiceEgressGatewayPublicIPCountMin, consts.ServiceEgressGatewayPublicIPCountMax)
	length, _ := p.integer(a.prefixLength, a.minPrefixLength, a.maxPrefixLength)
	publicIPs := p.resourceIDs(a.publicIPIDs, publicIPAddressResourceType, "Public IP address")
	prefixes := p.resourceIDs(a.prefixIDs, publicIPPrefixResourceType, "Public IP prefix")
	if len(p.problems) > before {
		return def
	}

	cfg := egressFamilyConfig{
		ManagedPublicIPs:    def.ManagedPublicIPs,
		ManagedPrefixLength: length,
		PublicIPIDs:         strings.Join(publicIPs, ","),
		PublicIPPrefixIDs:   strings.Join(prefixes, ","),
	}
	switch {
	case countSet:
		cfg.ManagedPublicIPs = count
	case length > 0 || len(publicIPs) > 0 || len(prefixes) > 0:
		cfg.ManagedPublicIPs = 0
	}

	addresses := cfg.ManagedPublicIPs + len(publicIPs) + egressUserPrefixMinAddresses*len(prefixes)
	if length > 0 {
		addresses += 1 << (a.bits - length)
	}
	switch {
	case addresses == 0:
		p.report(a.family, "no public address (the Public IP count is 0 and no prefix or user Public IP is set)")
	case addresses > consts.ServiceEgressGatewayMaxAddressesPerFamily:
		p.report(a.family, fmt.Sprintf("%d public addresses, more than %d (managed Public IPs + managed prefix size + "+
			"user Public IPs + 2 per user prefix)", addresses, consts.ServiceEgressGatewayMaxAddressesPerFamily))
	}
	return cfg
}

// resourceIDs returns the user resources listed in an annotation as canonical IDs: lower-cased and sorted.
// Every ID must be of resourceType, in the network subscription, outside the node resource group, and
// listed only once across all annotations.
func (p *egressAnnotationParser) resourceIDs(key, resourceType, kind string) []string {
	value, set := p.value(key)
	if !set {
		return nil
	}
	var ids []string
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		id, problem := p.userResourceID(entry, resourceType, kind)
		if other, listed := p.idKeys[id]; listed && problem == "" {
			problem = fmt.Sprintf("%q is also listed in %s", entry, other)
			if other == key {
				problem = fmt.Sprintf("%q is listed more than once", entry)
			}
		}
		if problem != "" {
			p.report(key, problem)
			continue
		}
		p.idKeys[id] = key
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// userResourceID returns the canonical ID of a user resource, or the problem with it.
func (p *egressAnnotationParser) userResourceID(entry, resourceType, kind string) (id, problem string) {
	if entry == "" {
		return "", "empty entry in the list"
	}
	parsed, err := arm.ParseResourceID(entry)
	if err != nil || !strings.EqualFold(parsed.ResourceType.String(), resourceType) || parsed.Parent == nil ||
		!strings.EqualFold(parsed.Parent.ResourceType.String(), arm.ResourceGroupResourceType.String()) {
		return "", fmt.Sprintf("%q is not a %s ID", entry, kind)
	}
	if !strings.EqualFold(parsed.SubscriptionID, p.pc.NetworkSubscription) {
		return "", fmt.Sprintf("%q is not in subscription %q", entry, p.pc.NetworkSubscription)
	}
	if strings.EqualFold(parsed.ResourceGroupName, p.pc.NodeResourceGroup) {
		return "", fmt.Sprintf("%q is in the node resource group %q, which the cluster manages", entry, p.pc.NodeResourceGroup)
	}
	return strings.ToLower(parsed.String()), ""
}

// tags returns the tags of an annotation in canonical form: "key=value" joined by commas and ordered by
// lower-cased key. Keys keep the user's spelling, which is what Azure shows; as in Azure, two keys that differ
// only in case are the same key.
func (p *egressAnnotationParser) tags(key string) string {
	value, set := p.value(key)
	if !set {
		return ""
	}
	entries := strings.Split(value, consts.TagsDelimiter)
	if len(entries) > maxConfiguredEgressTags {
		p.report(key, fmt.Sprintf("%d tags, more than %d (Azure accepts %d and the cluster adds its own)",
			len(entries), maxConfiguredEgressTags, consts.ServiceEgressGatewayMaxTags))
	}
	tags := map[string]string{}
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		k, v, found := strings.Cut(entry, consts.TagKeyValueDelimiter)
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		_, duplicate := tags[strings.ToLower(k)]
		var problem string
		switch {
		case entry == "":
			problem = "empty entry in the list"
		case !found || strings.Contains(v, consts.TagKeyValueDelimiter):
			problem = fmt.Sprintf("%q is not key=value", entry)
		case k == "":
			problem = fmt.Sprintf("%q has an empty key", entry)
		case utf8.RuneCountInString(k) > consts.ServiceEgressGatewayMaxTagKeyLength:
			problem = fmt.Sprintf("key %q is longer than %d characters", k, consts.ServiceEgressGatewayMaxTagKeyLength)
		case strings.ContainsAny(k, consts.ServiceEgressGatewayTagKeyInvalidCharacters):
			problem = fmt.Sprintf("key %q contains one of %s", k, consts.ServiceEgressGatewayTagKeyInvalidCharacters)
		case slices.ContainsFunc(reservedEgressTagKeys, func(reserved string) bool { return strings.EqualFold(k, reserved) }):
			problem = fmt.Sprintf("key %q is reserved for the cluster", k)
		case duplicate:
			problem = fmt.Sprintf("key %q is set more than once (keys are case-insensitive)", k)
		case utf8.RuneCountInString(v) > consts.ServiceEgressGatewayMaxTagValueLength:
			problem = fmt.Sprintf("value of %q is longer than %d characters", k, consts.ServiceEgressGatewayMaxTagValueLength)
		case strings.EqualFold(v, "null"):
			problem = fmt.Sprintf("value of %q is %q, which Azure reserves", k, v)
		}
		if problem != "" {
			p.report(key, problem)
			continue
		}
		tags[strings.ToLower(k)] = k + consts.TagKeyValueDelimiter + v
	}
	canonical := make([]string, 0, len(tags))
	for _, lowerKey := range slices.Sorted(maps.Keys(tags)) {
		canonical = append(canonical, tags[lowerKey])
	}
	return strings.Join(canonical, consts.TagsDelimiter)
}

// Summary describes the configuration in one deterministic line for events, for example
// "idle timeout 10m; IPv4: 2 managed IPs, managed prefix /31, user IPs (pip-a); tags: env=prod".
func (c EgressConfig) Summary() string {
	if c == egressUnknown {
		return "unknown"
	}
	parts := []string{fmt.Sprintf("idle timeout %dm", c.IdleTimeoutMinutes)}
	if family := c.IPv4.summary(); family != "" {
		parts = append(parts, string(armnetwork.IPVersionIPv4)+": "+family)
	}
	if family := c.IPv6.summary(); family != "" {
		parts = append(parts, string(armnetwork.IPVersionIPv6)+": "+family)
	}
	if c.Tags != "" {
		parts = append(parts, "tags: "+c.Tags)
	}
	return strings.Join(parts, "; ")
}

func (f egressFamilyConfig) summary() string {
	var parts []string
	switch {
	case f.ManagedPublicIPs == 1:
		parts = append(parts, "1 managed IP")
	case f.ManagedPublicIPs > 1:
		parts = append(parts, fmt.Sprintf("%d managed IPs", f.ManagedPublicIPs))
	}
	if f.ManagedPrefixLength > 0 {
		parts = append(parts, fmt.Sprintf("managed prefix /%d", f.ManagedPrefixLength))
	}
	if f.PublicIPIDs != "" {
		parts = append(parts, "user IPs ("+egressResourceNames(f.PublicIPIDs)+")")
	}
	if f.PublicIPPrefixIDs != "" {
		parts = append(parts, "user prefixes ("+egressResourceNames(f.PublicIPPrefixIDs)+")")
	}
	return strings.Join(parts, ", ")
}

// egressResourceNames returns the names of comma-joined resource IDs.
func egressResourceNames(ids string) string {
	names := strings.Split(ids, ",")
	for i, id := range names {
		names[i] = id[strings.LastIndex(id, "/")+1:]
	}
	return strings.Join(names, ", ")
}
