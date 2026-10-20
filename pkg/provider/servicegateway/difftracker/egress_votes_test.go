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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

var (
	votesEpoch     = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	voteCfgDefault = EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1}}
	voteCfgA       = EgressConfig{IdleTimeoutMinutes: 30, IPv4: egressFamilyConfig{ManagedPublicIPs: 2}}
	voteCfgB       = EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPrefixLength: 31}, Tags: "team=a"}
)

func validVote(identity string, cfg EgressConfig, uid types.UID) egressVote {
	return egressVote{Identity: identity, Config: cfg, HasAnnotations: true, PodUID: uid, PodCreated: votesEpoch}
}

func invalidVote(identity string, uid types.UID) egressVote {
	return egressVote{
		Identity: identity, Invalid: "idle-timeout out of range", HasAnnotations: true, PodUID: uid, PodCreated: votesEpoch,
	}
}

// votesOp is a registry mutation with its expected result: a set when vote is non-nil, else a remove.
type votesOp struct {
	pod         string
	vote        *egressVote
	wantOld     string
	uid         types.UID
	identity    string
	wantRemoved bool
}

func setOp(pod string, vote egressVote, wantOld string) votesOp {
	return votesOp{pod: pod, vote: &vote, wantOld: wantOld}
}

func removeOp(pod string, uid types.UID, identity string, wantRemoved bool) votesOp {
	return votesOp{pod: pod, uid: uid, identity: identity, wantRemoved: wantRemoved}
}

// applyVotesOps runs ops in order and checks the result of each and the counters after each.
func applyVotesOps(t *testing.T, v *egressVotes, ops []votesOp) {
	t.Helper()
	for i, op := range ops {
		if op.vote != nil {
			assert.Equal(t, op.wantOld, v.set(op.pod, *op.vote), "op %d: set(%s) returns the replaced vote's identity", i, op.pod)
		} else {
			assert.Equal(t, op.wantRemoved, v.remove(op.pod, op.uid, op.identity), "op %d: remove(%s, %s, %s)", i, op.pod, op.uid, op.identity)
		}
		assertRecounted(t, v)
	}
}

// recount rebuilds the member index, tally, invalid and ipv6 from byPod, the source of truth.
func recount(byPod map[string]egressVote) (map[string]map[string]struct{}, map[string]map[EgressConfig]int, map[string]int, map[string]int) {
	members := map[string]map[string]struct{}{}
	tally := map[string]map[EgressConfig]int{}
	invalid := map[string]int{}
	ipv6 := map[string]int{}
	for podKey, vote := range byPod {
		if members[vote.Identity] == nil {
			members[vote.Identity] = map[string]struct{}{}
		}
		members[vote.Identity][podKey] = struct{}{}
		if vote.ParsedForIPv6 {
			ipv6[vote.Identity]++
		}
		if vote.Invalid != "" {
			invalid[vote.Identity]++
			continue
		}
		if tally[vote.Identity] == nil {
			tally[vote.Identity] = map[EgressConfig]int{}
		}
		tally[vote.Identity][vote.Config]++
	}
	return members, tally, invalid, ipv6
}

// assertRecounted asserts invariant I2: the member index, tally, invalid and ipv6 equal a recount of byPod, so no
// empty set, zero count or empty map is left behind either.
func assertRecounted(t *testing.T, v *egressVotes) {
	t.Helper()
	members, tally, invalid, ipv6 := recount(v.byPod)
	assert.Equal(t, members, v.byIdentity, "the member index must equal a recount of byPod")
	assert.Equal(t, tally, v.tally, "tally must equal a recount of byPod")
	assert.Equal(t, invalid, v.invalid, "invalid must equal a recount of byPod")
	assert.Equal(t, ipv6, v.ipv6, "ipv6 must equal a recount of byPod")
}

func orEmpty[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return map[K]V{}
	}
	return m
}

// TestEgressVotes_SetAndRemove pins how set and remove keep the votes and their counters exact, and
// that remove ignores a late event of a replaced pod (other UID) or of the pod's previous identity.
func TestEgressVotes_SetAndRemove(t *testing.T) {
	cases := []struct {
		name        string
		ops         []votesOp
		wantByPod   map[string]egressVote
		wantTally   map[string]map[EgressConfig]int
		wantInvalid map[string]int
	}{
		{
			name:      "insert a valid vote",
			ops:       []votesOp{setOp("ns/p1", validVote("a", voteCfgA, "u1"), "")},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgA, "u1")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgA: 1}},
		},
		{
			name:        "insert an invalid vote",
			ops:         []votesOp{setOp("ns/p1", invalidVote("a", "u1"), "")},
			wantByPod:   map[string]egressVote{"ns/p1": invalidVote("a", "u1")},
			wantInvalid: map[string]int{"a": 1},
		},
		{
			name: "insert next to other votes",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgB, "u2"), ""),
				setOp("ns/p3", validVote("a", voteCfgA, "u3"), ""),
				setOp("ns/p4", invalidVote("a", "u4"), ""),
				setOp("ns/p5", validVote("b", voteCfgA, "u5"), ""),
			},
			wantByPod: map[string]egressVote{
				"ns/p1": validVote("a", voteCfgA, "u1"),
				"ns/p2": validVote("a", voteCfgB, "u2"),
				"ns/p3": validVote("a", voteCfgA, "u3"),
				"ns/p4": invalidVote("a", "u4"),
				"ns/p5": validVote("b", voteCfgA, "u5"),
			},
			wantTally:   map[string]map[EgressConfig]int{"a": {voteCfgA: 2, voteCfgB: 1}, "b": {voteCfgA: 1}},
			wantInvalid: map[string]int{"a": 1},
		},
		{
			name: "set the same vote again",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgA, "u1")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgA: 1}},
		},
		{
			name: "replace with another configuration",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p1", validVote("a", voteCfgB, "u1"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgB, "u1")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgB: 1}},
		},
		{
			name: "replace a valid vote with an invalid one",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgA, "u2"), ""),
				setOp("ns/p1", invalidVote("a", "u1"), "a"),
			},
			wantByPod:   map[string]egressVote{"ns/p1": invalidVote("a", "u1"), "ns/p2": validVote("a", voteCfgA, "u2")},
			wantTally:   map[string]map[EgressConfig]int{"a": {voteCfgA: 1}},
			wantInvalid: map[string]int{"a": 1},
		},
		{
			name: "replace an invalid vote with a valid one",
			ops: []votesOp{
				setOp("ns/p1", invalidVote("a", "u1"), ""),
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgA, "u1")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgA: 1}},
		},
		{
			name: "replace an invalid vote with another invalid one",
			ops: []votesOp{
				setOp("ns/p1", invalidVote("a", "u1"), ""),
				setOp("ns/p1", egressVote{Identity: "a", Invalid: "tags: duplicate key", PodUID: "u1"}, "a"),
			},
			wantByPod:   map[string]egressVote{"ns/p1": {Identity: "a", Invalid: "tags: duplicate key", PodUID: "u1"}},
			wantInvalid: map[string]int{"a": 1},
		},
		{
			name: "move a valid vote to another identity",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgA, "u2"), ""),
				setOp("ns/p1", validVote("b", voteCfgA, "u1"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("b", voteCfgA, "u1"), "ns/p2": validVote("a", voteCfgA, "u2")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgA: 1}, "b": {voteCfgA: 1}},
		},
		{
			name: "move the last vote of an identity",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p1", validVote("b", voteCfgB, "u1"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("b", voteCfgB, "u1")},
			wantTally: map[string]map[EgressConfig]int{"b": {voteCfgB: 1}},
		},
		{
			name: "move an invalid vote to another identity",
			ops: []votesOp{
				setOp("ns/p1", invalidVote("a", "u1"), ""),
				setOp("ns/p2", invalidVote("a", "u2"), ""),
				setOp("ns/p1", invalidVote("b", "u1"), "a"),
			},
			wantByPod:   map[string]egressVote{"ns/p1": invalidVote("b", "u1"), "ns/p2": invalidVote("a", "u2")},
			wantInvalid: map[string]int{"a": 1, "b": 1},
		},
		{
			name: "move an invalid vote to a valid one of another identity",
			ops: []votesOp{
				setOp("ns/p1", invalidVote("a", "u1"), ""),
				setOp("ns/p1", validVote("b", voteCfgA, "u1"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("b", voteCfgA, "u1")},
			wantTally: map[string]map[EgressConfig]int{"b": {voteCfgA: 1}},
		},
		{
			name: "a same-name replacement takes over the vote",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p1", validVote("a", voteCfgB, "u2"), "a"),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgB, "u2")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgB: 1}},
		},
		{
			name: "remove with the UID of a replaced pod is a no-op",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p1", validVote("a", voteCfgB, "u2"), "a"),
				removeOp("ns/p1", "u1", "a", false),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgB, "u2")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgB: 1}},
		},
		{
			name: "remove with the previous identity of a moved pod is a no-op",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p1", validVote("b", voteCfgA, "u1"), "a"),
				removeOp("ns/p1", "u1", "a", false),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("b", voteCfgA, "u1")},
			wantTally: map[string]map[EgressConfig]int{"b": {voteCfgA: 1}},
		},
		{
			name: "remove a pod without a vote",
			ops: []votesOp{
				removeOp("ns/p1", "u1", "a", false),
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				removeOp("ns/p2", "u1", "a", false),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgA, "u1")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgA: 1}},
		},
		{
			name: "remove one of several votes",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgB, "u2"), ""),
				setOp("ns/p3", invalidVote("a", "u3"), ""),
				removeOp("ns/p1", "u1", "a", true),
			},
			wantByPod:   map[string]egressVote{"ns/p2": validVote("a", voteCfgB, "u2"), "ns/p3": invalidVote("a", "u3")},
			wantTally:   map[string]map[EgressConfig]int{"a": {voteCfgB: 1}},
			wantInvalid: map[string]int{"a": 1},
		},
		{
			name: "remove the last valid vote",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				removeOp("ns/p1", "u1", "a", true),
			},
		},
		{
			name: "remove the last invalid vote",
			ops: []votesOp{
				setOp("ns/p1", invalidVote("a", "u1"), ""),
				removeOp("ns/p1", "u1", "a", true),
			},
		},
		{
			name: "remove twice, then vote again",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				removeOp("ns/p1", "u1", "a", true),
				removeOp("ns/p1", "u1", "a", false),
				setOp("ns/p1", validVote("a", voteCfgB, "u1"), ""),
			},
			wantByPod: map[string]egressVote{"ns/p1": validVote("a", voteCfgB, "u1")},
			wantTally: map[string]map[EgressConfig]int{"a": {voteCfgB: 1}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := newEgressVotes()
			applyVotesOps(t, v, tc.ops)
			assert.Equal(t, orEmpty(tc.wantByPod), v.byPod)
			assert.Equal(t, orEmpty(tc.wantTally), v.tally)
			assert.Equal(t, orEmpty(tc.wantInvalid), v.invalid)
		})
	}
}

// TestEgressVotes_Agreed pins consensus: every member votes validly for one configuration, the default
// included. An invalid vote, a dissenting vote or no member at all means no agreement.
func TestEgressVotes_Agreed(t *testing.T) {
	unannotatedDefault := egressVote{Identity: "a", Config: voteCfgDefault, PodUID: "u1", PodCreated: votesEpoch}

	cases := []struct {
		name   string
		ops    []votesOp
		want   EgressConfig
		wantOK bool
	}{
		{name: "no members"},
		{
			name: "members of another identity only",
			ops:  []votesOp{setOp("ns/p1", validVote("b", voteCfgA, "u1"), "")},
		},
		{
			name:   "one member",
			ops:    []votesOp{setOp("ns/p1", validVote("a", voteCfgA, "u1"), "")},
			want:   voteCfgA,
			wantOK: true,
		},
		{
			name: "every member votes for one configuration",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgA, "u2"), ""),
				setOp("ns/p3", validVote("a", voteCfgA, "u3"), ""),
				setOp("ns/p4", validVote("b", voteCfgB, "u4"), ""),
			},
			want:   voteCfgA,
			wantOK: true,
		},
		{
			name: "two configurations",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgB, "u2"), ""),
				setOp("ns/p3", validVote("a", voteCfgA, "u3"), ""),
			},
		},
		{
			name: "an invalid vote",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgA, "u2"), ""),
				setOp("ns/p3", invalidVote("a", "u3"), ""),
			},
		},
		{
			name: "an invalid vote carrying the configuration of the others",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", egressVote{Identity: "a", Config: voteCfgA, Invalid: "tags: duplicate key", PodUID: "u2"}, ""),
			},
		},
		{
			name: "invalid votes only",
			ops:  []votesOp{setOp("ns/p1", invalidVote("a", "u1"), "")},
		},
		{
			name: "default votes with and without annotations",
			ops: []votesOp{
				setOp("ns/p1", unannotatedDefault, ""),
				setOp("ns/p2", validVote("a", voteCfgDefault, "u2"), ""),
			},
			want:   voteCfgDefault,
			wantOK: true,
		},
		{
			name: "a pod without annotations votes for the default",
			ops: []votesOp{
				setOp("ns/p1", unannotatedDefault, ""),
				setOp("ns/p2", validVote("a", voteCfgA, "u2"), ""),
			},
		},
		{
			name: "the dissenter is removed",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgB, "u2"), ""),
				removeOp("ns/p2", "u2", "a", true),
			},
			want:   voteCfgA,
			wantOK: true,
		},
		{
			name: "the dissenter moves to another identity",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", validVote("a", voteCfgB, "u2"), ""),
				setOp("ns/p2", validVote("b", voteCfgB, "u2"), "a"),
			},
			want:   voteCfgA,
			wantOK: true,
		},
		{
			name: "the invalid vote is fixed",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				setOp("ns/p2", invalidVote("a", "u2"), ""),
				setOp("ns/p2", validVote("a", voteCfgA, "u2"), "a"),
			},
			want:   voteCfgA,
			wantOK: true,
		},
		{
			name: "the last member is removed",
			ops: []votesOp{
				setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
				removeOp("ns/p1", "u1", "a", true),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := newEgressVotes()
			applyVotesOps(t, v, tc.ops)
			got, ok := v.agreed("a")
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
			assertRecounted(t, v)
		})
	}
}

// TestEgressVotes_Oldest pins the seed fallback: the member created first, and among members created at
// the same time the lowest pod key, so every process picks the same pod.
func TestEgressVotes_Oldest(t *testing.T) {
	at := func(identity string, uid types.UID, seconds int) egressVote {
		vote := validVote(identity, voteCfgA, uid)
		vote.PodCreated = votesEpoch.Add(time.Duration(seconds) * time.Second)
		return vote
	}
	invalidAt := func(identity string, uid types.UID, seconds int) egressVote {
		vote := invalidVote(identity, uid)
		vote.PodCreated = votesEpoch.Add(time.Duration(seconds) * time.Second)
		return vote
	}

	cases := []struct {
		name    string
		ops     []votesOp
		wantPod string
	}{
		{name: "no members"},
		{
			name: "members of another identity only",
			ops:  []votesOp{setOp("ns/p1", at("b", "u1", 0), "")},
		},
		{
			name: "the member created first, whatever its key",
			ops: []votesOp{
				setOp("ns/a", at("a", "u1", 5), ""),
				setOp("ns/z", at("a", "u2", 1), ""),
				setOp("ns/m", at("a", "u3", 3), ""),
			},
			wantPod: "ns/z",
		},
		{
			name: "the lowest key among members created at the same time",
			ops: []votesOp{
				setOp("ns/h", at("a", "u1", 0), ""),
				setOp("ns/c", at("a", "u2", 0), ""),
				setOp("ns/f", at("a", "u3", 0), ""),
				setOp("ns/b", at("a", "u4", 0), ""),
				setOp("ns/g", at("a", "u5", 0), ""),
				setOp("ns/d", at("a", "u6", 0), ""),
				setOp("ns/e", at("a", "u7", 0), ""),
				setOp("ns/i", at("a", "u8", 0), ""),
				setOp("ns/a", at("a", "u9", 1), ""),
			},
			wantPod: "ns/b",
		},
		{
			name: "an older member of another identity is ignored",
			ops: []votesOp{
				setOp("ns/a", at("b", "u1", 0), ""),
				setOp("ns/b", at("a", "u2", 5), ""),
			},
			wantPod: "ns/b",
		},
		{
			name: "the oldest is removed",
			ops: []votesOp{
				setOp("ns/a", at("a", "u1", 0), ""),
				setOp("ns/c", at("a", "u2", 5), ""),
				setOp("ns/b", at("a", "u3", 5), ""),
				removeOp("ns/a", "u1", "a", true),
			},
			wantPod: "ns/b",
		},
		{
			name: "the oldest moves to another identity",
			ops: []votesOp{
				setOp("ns/a", at("a", "u1", 0), ""),
				setOp("ns/b", at("a", "u2", 5), ""),
				setOp("ns/a", at("b", "u1", 0), "a"),
			},
			wantPod: "ns/b",
		},
		{
			name: "a same-name replacement of the oldest is younger",
			ops: []votesOp{
				setOp("ns/a", at("a", "u1", 0), ""),
				setOp("ns/b", at("a", "u2", 5), ""),
				setOp("ns/a", at("a", "u3", 9), "a"),
			},
			wantPod: "ns/b",
		},
		{
			name: "an invalid vote can be the oldest",
			ops: []votesOp{
				setOp("ns/b", invalidAt("a", "u1", 0), ""),
				setOp("ns/a", at("a", "u2", 5), ""),
			},
			wantPod: "ns/b",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := newEgressVotes()
			applyVotesOps(t, v, tc.ops)
			// Map iteration order changes between calls, so a missing tie-break shows up here.
			for range 32 {
				pod, vote, ok := v.oldest("a")
				if !assert.Equal(t, tc.wantPod, pod) || !assert.Equal(t, tc.wantPod != "", ok) ||
					!assert.Equal(t, v.byPod[tc.wantPod], vote) {
					break
				}
			}
			assertRecounted(t, v)
		})
	}
}

// members returns the number of votes for identity, valid or not, from the counters (tally and invalid).
func (v *egressVotes) members(identity string) int {
	n := v.invalid[identity]
	for _, votes := range v.tally[identity] {
		n += votes
	}
	return n
}

// TestEgressVotes_ReadHelpers pins get, members (valid and invalid votes) and podKeys (sorted).
func TestEgressVotes_ReadHelpers(t *testing.T) {
	v := newEgressVotes()
	applyVotesOps(t, v, []votesOp{
		setOp("ns/c", validVote("a", voteCfgA, "u1"), ""),
		setOp("ns/a", invalidVote("a", "u2"), ""),
		setOp("ns/b", validVote("a", voteCfgB, "u3"), ""),
		setOp("ns/d", validVote("b", voteCfgA, "u4"), ""),
		setOp("ns/e", validVote("a", voteCfgA, "u5"), ""),
		setOp("ns/e", validVote("b", voteCfgA, "u5"), "a"),
		setOp("ns/f", validVote("a", voteCfgA, "u6"), ""),
		removeOp("ns/f", "u6", "a", true),
	})

	identities := []struct {
		identity    string
		wantMembers int
		wantPodKeys []string
	}{
		{"a", 3, []string{"ns/a", "ns/b", "ns/c"}},
		{"b", 2, []string{"ns/d", "ns/e"}},
		{"missing", 0, nil},
	}
	for _, tc := range identities {
		t.Run("identity "+tc.identity, func(t *testing.T) {
			assert.Equal(t, tc.wantMembers, v.members(tc.identity))
			assert.Equal(t, tc.wantPodKeys, v.podKeys(tc.identity))
		})
	}

	pods := []struct {
		pod    string
		want   egressVote
		wantOK bool
	}{
		{"ns/a", invalidVote("a", "u2"), true},
		{"ns/e", validVote("b", voteCfgA, "u5"), true},
		{"ns/f", egressVote{}, false},
		{"ns/missing", egressVote{}, false},
	}
	for _, tc := range pods {
		t.Run("pod "+tc.pod, func(t *testing.T) {
			got, ok := v.get(tc.pod)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
	assertRecounted(t, v)
}

// TestEgressVotes_MembersIndex pins the index of the members of each identity: sets, removes and a move to another
// identity keep it equal to the votes, and an identity left without members leaves it, so it holds no memory.
func TestEgressVotes_MembersIndex(t *testing.T) {
	v := newEgressVotes()
	applyVotesOps(t, v, []votesOp{
		setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
		setOp("ns/p2", invalidVote("a", "u2"), ""),
		setOp("ns/p3", validVote("b", voteCfgB, "u3"), ""),
	})
	assert.Equal(t, map[string]map[string]struct{}{"a": {"ns/p1": {}, "ns/p2": {}}, "b": {"ns/p3": {}}}, v.byIdentity)

	applyVotesOps(t, v, []votesOp{
		setOp("ns/p1", validVote("b", voteCfgA, "u1"), "a"),
		removeOp("ns/p2", "u2", "a", true),
	})
	assert.Equal(t, map[string]map[string]struct{}{"b": {"ns/p1": {}, "ns/p3": {}}}, v.byIdentity, "a is gone with its last member")

	applyVotesOps(t, v, []votesOp{removeOp("ns/p1", "u1", "b", true), removeOp("ns/p3", "u3", "b", true)})
	assert.Empty(t, v.byIdentity)
}

// TestEgressVotes_ParsedFor pins how the registry tells whether every vote of an identity was parsed for the IP
// families of a NAT Gateway that serves IPv6, or for those of one that does not, from a count of the votes parsed for
// IPv6 that sets, moves and removes keep exact: an identity without votes is both, one with both kinds neither.
func TestEgressVotes_ParsedFor(t *testing.T) {
	forIPv6 := func(vote egressVote) egressVote {
		vote.ParsedForIPv6 = true
		return vote
	}
	steps := []struct {
		name     string
		op       votesOp
		wantIPv6 map[string]bool // identity -> parsedFor(identity, true)
		wantIPv4 map[string]bool // identity -> parsedFor(identity, false)
	}{
		{name: "an IPv4 vote", op: setOp("ns/p1", validVote("a", voteCfgA, "u1"), ""),
			wantIPv6: map[string]bool{"a": false, "b": true}, wantIPv4: map[string]bool{"a": true, "b": true}},
		{name: "an IPv6 vote mixes the families", op: setOp("ns/p2", forIPv6(invalidVote("a", "u2")), ""),
			wantIPv6: map[string]bool{"a": false}, wantIPv4: map[string]bool{"a": false}},
		{name: "the IPv4 vote parsed again for IPv6", op: setOp("ns/p1", forIPv6(validVote("a", voteCfgA, "u1")), "a"),
			wantIPv6: map[string]bool{"a": true}, wantIPv4: map[string]bool{"a": false}},
		{name: "an IPv6 vote moves away", op: setOp("ns/p2", validVote("b", voteCfgA, "u2"), "a"),
			wantIPv6: map[string]bool{"a": true, "b": false}, wantIPv4: map[string]bool{"a": false, "b": true}},
		{name: "the last IPv6 vote leaves", op: removeOp("ns/p1", "u1", "a", true),
			wantIPv6: map[string]bool{"a": true}, wantIPv4: map[string]bool{"a": true}},
	}
	v := newEgressVotes()
	for _, step := range steps {
		applyVotesOps(t, v, []votesOp{step.op})
		for identity, want := range step.wantIPv6 {
			assert.Equal(t, want, v.parsedFor(identity, true), "%s: parsedFor(%s, IPv6)", step.name, identity)
		}
		for identity, want := range step.wantIPv4 {
			assert.Equal(t, want, v.parsedFor(identity, false), "%s: parsedFor(%s, IPv4)", step.name, identity)
		}
	}
	assert.Empty(t, v.ipv6, "no count is left behind")
}

// TestEgressVote_Reparsed pins the vote of a pod parsed again for other IP families: the vote its egress annotations
// cast for them, for the same pod and identity, with the annotations it was parsed from.
func TestEgressVote_Reparsed(t *testing.T) {
	ipv4 := egressParseContext{NetworkSubscription: "sub", NodeResourceGroup: "mc-rg"}
	dual := egressParseContext{WantsIPv6: true, NetworkSubscription: "sub", NodeResourceGroup: "mc-rg"}
	annotations := map[string]map[string]string{
		"none": nil,
		"IPv4 settings": {
			consts.PodAnnotationServiceEgressGatewayIdleTimeout:   "10",
			consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
		},
		"IPv6 settings, valid only for IPv6": {consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"},
		"invalid":                            {consts.PodAnnotationServiceEgressGatewayIdleTimeout: "ten"},
	}
	for name, ann := range annotations {
		for _, from := range []egressParseContext{ipv4, dual} {
			for _, to := range []egressParseContext{ipv4, dual} {
				vote := egressVoteOf(ann, from)
				vote.Identity, vote.PodUID, vote.PodCreated = "a", "u1", votesEpoch
				want := egressVoteOf(ann, to)
				want.Identity, want.PodUID, want.PodCreated = "a", "u1", votesEpoch
				assert.Equal(t, want, vote.reparsed(to), "%s, parsed for IPv6 %t, again for IPv6 %t", name, from.WantsIPv6, to.WantsIPv6)
			}
		}
	}

	ann := annotations["IPv6 settings, valid only for IPv6"]
	assert.Equal(t, egressVote{Invalid: consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6 + ": this egress identity does not use IPv6",
		HasAnnotations: true, Annotations: ann}, egressVoteOf(ann, ipv4))
	assert.Equal(t, egressVote{Config: EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1},
		IPv6: egressFamilyConfig{ManagedPublicIPs: 2}}, HasAnnotations: true, ParsedForIPv6: true, Annotations: ann}, egressVoteOf(ann, dual))
}

// TestEgressVotes_AnnotationsLeaveWithTheVote pins that the annotations a vote was parsed from are held by its vote
// alone: the vote that replaces it, also for another identity, holds its own, and once the vote is removed nothing of
// it is left in the registry.
func TestEgressVotes_AnnotationsLeaveWithTheVote(t *testing.T) {
	v := newEgressVotes()
	first := validVote("a", voteCfgA, "u1")
	first.ParsedForIPv6, first.Annotations = true, map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: "30"}
	second := validVote("b", voteCfgB, "u1")
	second.Annotations = map[string]string{consts.PodAnnotationServiceEgressGatewayTags: "team=a"}

	applyVotesOps(t, v, []votesOp{setOp("ns/p1", first, ""), setOp("ns/p1", second, "a")})
	vote, _ := v.get("ns/p1")
	assert.Equal(t, second.Annotations, vote.Annotations, "the replacing vote holds its own annotations")

	applyVotesOps(t, v, []votesOp{removeOp("ns/p1", "u1", "b", true)})
	assert.Empty(t, v.byPod)
	assert.Empty(t, v.byIdentity)
	assert.Empty(t, v.tally)
	assert.Empty(t, v.invalid)
	assert.Empty(t, v.ipv6)
}

// TestEgressVotes_ReadsOnlyTheMembersOfTheIdentity pins that oldest and podKeys cost the members of the identity, not
// every vote: among many identities, they never see a vote that is not indexed under the identity, which a scan of
// every vote would.
func TestEgressVotes_ReadsOnlyTheMembersOfTheIdentity(t *testing.T) {
	v := newEgressVotes()
	for i := range 1000 {
		for j := range 5 {
			vote := validVote(fmt.Sprintf("id-%d", i), voteCfgA, types.UID(fmt.Sprintf("u-%d-%d", i, j)))
			vote.PodCreated = votesEpoch.Add(time.Duration(j) * time.Second)
			v.set(fmt.Sprintf("ns/p-%d-%d", i, j), vote)
		}
	}
	hidden := validVote("id-7", voteCfgA, "hidden")
	hidden.PodCreated = votesEpoch.Add(-time.Hour)
	v.byPod["ns/hidden"] = hidden

	pod, vote, ok := v.oldest("id-7")
	assert.True(t, ok)
	assert.Equal(t, "ns/p-7-0", pod)
	assert.Equal(t, v.byPod["ns/p-7-0"], vote)
	assert.Equal(t, []string{"ns/p-7-0", "ns/p-7-1", "ns/p-7-2", "ns/p-7-3", "ns/p-7-4"}, v.podKeys("id-7"))
}

// TestEgressVotes_RandomOperationsMatchModel drives the registry with a fixed pseudo-random sequence of
// sets and removes over a few pods, identities, configurations, UIDs and creation times, and compares it
// after every operation with a plain map of the votes and brute-force answers.
func TestEgressVotes_RandomOperationsMatchModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 1013)) //nolint:gosec // G404: a fixed, reproducible operation sequence
	// pods is sorted, so the oracles below meet the members of an identity in key order.
	pods := []string{"ns/p0", "ns/p1", "ns/p2", "ns/p3", "ns/p4"}
	identities := []string{"a", "b", "c"}
	configs := []EgressConfig{voteCfgDefault, voteCfgA, voteCfgB}
	uids := []types.UID{"u0", "u1", "u2"}

	v := newEgressVotes()
	model := map[string]egressVote{}
	seen := map[string]int{}
	for i := range 4000 {
		pod := pods[rng.IntN(len(pods))]
		stored, hasVote := model[pod]
		var op string
		if rng.IntN(3) > 0 {
			vote := egressVote{
				Identity:      identities[rng.IntN(len(identities))],
				Config:        configs[rng.IntN(len(configs))],
				PodUID:        uids[rng.IntN(len(uids))],
				PodCreated:    votesEpoch.Add(time.Duration(rng.IntN(3)) * time.Second),
				ParsedForIPv6: rng.IntN(2) == 0,
			}
			if rng.IntN(4) == 0 {
				vote.Invalid = "invalid"
			}
			op = fmt.Sprintf("set(%s, %+v)", pod, vote)
			if hasVote && stored.Identity != vote.Identity {
				seen["move"]++
			}
			model[pod] = vote
			assert.Equal(t, stored.Identity, v.set(pod, vote), op)
		} else {
			uid, identity := uids[rng.IntN(len(uids))], identities[rng.IntN(len(identities))]
			if hasVote && rng.IntN(2) == 0 {
				uid, identity = stored.PodUID, stored.Identity
			}
			op = fmt.Sprintf("remove(%s, %s, %s)", pod, uid, identity)
			want := hasVote && stored.PodUID == uid && stored.Identity == identity
			switch {
			case want:
				seen["remove"]++
				delete(model, pod)
			case hasVote:
				seen["stale remove"]++
			}
			assert.Equal(t, want, v.remove(pod, uid, identity), op)
		}

		for _, identity := range identities {
			var keys []string
			wantOldest, invalid, ipv6 := "", 0, 0
			for _, key := range pods {
				vote, ok := model[key]
				if !ok || vote.Identity != identity {
					continue
				}
				keys = append(keys, key)
				if wantOldest == "" || vote.PodCreated.Before(model[wantOldest].PodCreated) {
					wantOldest = key
				}
				if vote.Invalid != "" {
					invalid++
				}
				if vote.ParsedForIPv6 {
					ipv6++
				}
			}
			want, wantOK := EgressConfig{}, len(keys) > 0
			for _, key := range keys {
				if model[key].Invalid != "" || model[key].Config != model[keys[0]].Config {
					want, wantOK = EgressConfig{}, false
					break
				}
				want = model[key].Config
			}
			switch {
			case invalid > 0:
				seen["invalid blocks"]++
			case len(keys) > 0 && !wantOK:
				seen["dissent blocks"]++
			case len(keys) > 1:
				seen["agreed by several"]++
			}
			for _, key := range keys {
				if key != wantOldest && model[key].PodCreated.Equal(model[wantOldest].PodCreated) {
					seen["oldest by key"]++
					break
				}
			}
			if ipv6 > 0 && ipv6 < len(keys) {
				seen["families mixed"]++
			}

			got, ok := v.agreed(identity)
			assert.Equal(t, wantOK, ok, "agreed(%s)", identity)
			assert.Equal(t, want, got, "agreed(%s)", identity)
			assert.Equal(t, ipv6 == len(keys), v.parsedFor(identity, true), "parsedFor(%s, IPv6)", identity)
			assert.Equal(t, ipv6 == 0, v.parsedFor(identity, false), "parsedFor(%s, IPv4)", identity)
			oldestPod, oldestVote, ok := v.oldest(identity)
			assert.Equal(t, wantOldest, oldestPod, "oldest(%s)", identity)
			assert.Equal(t, wantOldest != "", ok, "oldest(%s)", identity)
			assert.Equal(t, model[wantOldest], oldestVote, "oldest(%s)", identity)
			assert.Equal(t, len(keys), v.members(identity), "members(%s)", identity)
			assert.Equal(t, keys, v.podKeys(identity), "podKeys(%s)", identity)
		}
		for _, key := range pods {
			vote, ok := v.get(key)
			_, wantOK := model[key]
			assert.Equal(t, wantOK, ok, "get(%s)", key)
			assert.Equal(t, model[key], vote, "get(%s)", key)
		}
		assert.Equal(t, model, v.byPod)
		assertRecounted(t, v)
		if t.Failed() {
			t.Fatalf("diverged from the model at operation %d: %s", i, op)
		}
	}

	for _, kind := range []string{"move", "remove", "stale remove", "invalid blocks", "dissent blocks", "agreed by several", "oldest by key",
		"families mixed"} {
		assert.Positive(t, seen[kind], "the sequence must exercise %q", kind)
	}
}
