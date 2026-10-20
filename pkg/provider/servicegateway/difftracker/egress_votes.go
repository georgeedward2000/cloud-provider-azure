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
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// egressVote is the NAT Gateway configuration an egress member pod asks for its identity.
type egressVote struct {
	Identity string
	Config   EgressConfig
	// Invalid is the parse error of the pod's annotations; empty for a valid vote. That a BYO identity
	// accepts no configuration is derived from its mode, never stored here.
	Invalid        string
	HasAnnotations bool
	// ParsedForIPv6 is set when the vote was parsed for an identity whose NAT Gateway serves IPv6.
	ParsedForIPv6 bool
	PodUID        types.UID
	PodCreated    time.Time
	// Annotations are the egress annotations the vote was parsed from, nil when the pod has none: the vote is
	// parsed again from them for other IP families (reparsed).
	Annotations map[string]string
}

// egressVoteOf returns the vote that egress annotations, a pod's annotations with the egress prefix, cast for an
// identity whose parse context is pc: an invalid vote carries the parse error. The engine fills in the identity, the
// pod UID and its creation time.
func egressVoteOf(annotations map[string]string, pc egressParseContext) egressVote {
	vote := egressVote{ParsedForIPv6: pc.WantsIPv6, Annotations: annotations}
	cfg, hasAnnotations, err := parseEgressConfig(annotations, pc)
	vote.HasAnnotations = hasAnnotations
	if err != nil {
		vote.Invalid = err.Error()
		return vote
	}
	vote.Config = cfg
	return vote
}

// reparsed returns the vote the annotations of v cast for an identity whose parse context is pc, for the same pod and
// identity.
func (v egressVote) reparsed(pc egressParseContext) egressVote {
	vote := egressVoteOf(v.Annotations, pc)
	vote.Identity, vote.PodUID, vote.PodCreated = v.Identity, v.PodUID, v.PodCreated
	return vote
}

// egressVotes holds the vote of every egress member pod, keyed by "namespace/name". byPod is the source
// of truth; byIdentity indexes its keys by identity, so that reading the members of one identity costs
// only its members, and tally, invalid and ipv6 count its votes per identity. None holds an empty set or a
// zero count. Only set and remove mutate it. Guarded by dt.mu.
type egressVotes struct {
	byPod      map[string]egressVote
	byIdentity map[string]map[string]struct{}  // identity -> keys of its members
	tally      map[string]map[EgressConfig]int // identity -> valid configuration -> votes
	invalid    map[string]int                  // identity -> invalid votes
	ipv6       map[string]int                  // identity -> votes parsed for IPv6
}

func newEgressVotes() *egressVotes {
	return &egressVotes{
		byPod:      make(map[string]egressVote),
		byIdentity: make(map[string]map[string]struct{}),
		tally:      make(map[string]map[EgressConfig]int),
		invalid:    make(map[string]int),
		ipv6:       make(map[string]int),
	}
}

// set records vote as the vote of podKey, replacing its previous vote, also one for another identity.
// It returns the identity of the replaced vote, empty when the pod had none.
func (v *egressVotes) set(podKey string, vote egressVote) (oldIdentity string) {
	if old, ok := v.byPod[podKey]; ok {
		v.unindex(podKey, old)
		oldIdentity = old.Identity
	}
	v.byPod[podKey] = vote
	v.index(podKey, vote)
	return oldIdentity
}

// remove deletes the vote of podKey only when the pod with uid cast it for identity, so a late event of
// a replaced pod or of the pod's previous identity cannot remove the current vote. It reports whether
// a vote was removed.
func (v *egressVotes) remove(podKey string, uid types.UID, identity string) bool {
	vote, ok := v.byPod[podKey]
	if !ok || vote.PodUID != uid || vote.Identity != identity {
		return false
	}
	delete(v.byPod, podKey)
	v.unindex(podKey, vote)
	return true
}

// index records podKey, whose vote is vote, as a member of the vote's identity and counts the vote.
func (v *egressVotes) index(podKey string, vote egressVote) {
	members := v.byIdentity[vote.Identity]
	if members == nil {
		members = make(map[string]struct{})
		v.byIdentity[vote.Identity] = members
	}
	members[podKey] = struct{}{}
	v.count(vote, 1)
}

// unindex undoes index, deleting the member set it leaves empty.
func (v *egressVotes) unindex(podKey string, vote egressVote) {
	members := v.byIdentity[vote.Identity]
	delete(members, podKey)
	if len(members) == 0 {
		delete(v.byIdentity, vote.Identity)
	}
	v.count(vote, -1)
}

// count adds delta to the counters of vote, deleting the counters that drop to zero.
func (v *egressVotes) count(vote egressVote, delta int) {
	if vote.ParsedForIPv6 {
		addVotes(v.ipv6, vote.Identity, delta)
	}
	if vote.Invalid != "" {
		addVotes(v.invalid, vote.Identity, delta)
		return
	}
	configs := v.tally[vote.Identity]
	if configs == nil {
		configs = make(map[EgressConfig]int)
		v.tally[vote.Identity] = configs
	}
	configs[vote.Config] += delta
	if configs[vote.Config] == 0 {
		delete(configs, vote.Config)
		if len(configs) == 0 {
			delete(v.tally, vote.Identity)
		}
	}
}

// addVotes adds delta to the counter of identity in counters, deleting it when it drops to zero.
func addVotes(counters map[string]int, identity string, delta int) {
	counters[identity] += delta
	if counters[identity] == 0 {
		delete(counters, identity)
	}
}

// parsedFor reports whether every vote of identity was parsed for an identity whose NAT Gateway serves IPv6 when
// ipv6 is set, else for one whose NAT Gateway does not.
func (v *egressVotes) parsedFor(identity string, ipv6 bool) bool {
	if ipv6 {
		return v.ipv6[identity] == len(v.byIdentity[identity])
	}
	return v.ipv6[identity] == 0
}

// get returns the vote of podKey.
func (v *egressVotes) get(podKey string) (egressVote, bool) {
	vote, ok := v.byPod[podKey]
	return vote, ok
}

// agreed returns the configuration every member of identity votes for. It is false when the identity
// has no members, an invalid vote or votes for more than one configuration.
func (v *egressVotes) agreed(identity string) (EgressConfig, bool) {
	configs := v.tally[identity]
	if v.invalid[identity] > 0 || len(configs) != 1 {
		return EgressConfig{}, false
	}
	for cfg := range configs {
		return cfg, true
	}
	return EgressConfig{}, false
}

// oldest returns the member of identity created first, the lowest pod key among those created at the
// same time. Invalid votes are members too.
func (v *egressVotes) oldest(identity string) (podKey string, vote egressVote, ok bool) {
	for key := range v.byIdentity[identity] {
		candidate := v.byPod[key]
		if !ok || candidate.PodCreated.Before(vote.PodCreated) ||
			(candidate.PodCreated.Equal(vote.PodCreated) && key < podKey) {
			podKey, vote, ok = key, candidate, true
		}
	}
	return podKey, vote, ok
}

// podKeys returns the sorted keys of the members of identity.
func (v *egressVotes) podKeys(identity string) []string {
	return slices.Sorted(maps.Keys(v.byIdentity[identity]))
}
