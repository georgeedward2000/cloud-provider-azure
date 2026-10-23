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

	"k8s.io/apimachinery/pkg/types"
)

var errEgressConfigConflict = errors.New("egress pods do not agree on the configuration")
var errInvalidEgressVotes = errors.New("one or more egress pods has an invalid configuration")

type egressPodVote struct {
	UID      types.UID
	Identity string
	Config   egressConfig
	Invalid  bool
}

type egressIdentityVotes struct {
	configurations map[egressConfig]int
	pods           int
	invalid        int
}

// byIdentity holds identity -> canonical configuration -> pod count. byPod makes replacement,
// relabel and duplicate events update those counters exactly once. Guarded by DiffTracker.mu.
type egressConfigVotes struct {
	byPod      map[string]egressPodVote
	byIdentity map[string]*egressIdentityVotes
}

func (v *egressConfigVotes) set(key string, vote egressPodVote) (oldIdentity string, changed bool) {
	if old, exists := v.byPod[key]; exists {
		if old == vote {
			return old.Identity, false
		}
		oldIdentity = old.Identity
		v.count(old, -1)
		delete(v.byPod, key)
	}
	if vote.Identity != "" {
		if v.byPod == nil {
			v.byPod = make(map[string]egressPodVote)
		}
		v.byPod[key] = vote
		v.count(vote, 1)
	}
	v.prune(oldIdentity)
	return oldIdentity, oldIdentity != "" || vote.Identity != ""
}

func (v *egressConfigVotes) remove(key string, uid types.UID) (identity string, changed bool) {
	old, exists := v.byPod[key]
	if !exists || old.UID != uid {
		return "", false
	}
	delete(v.byPod, key)
	v.count(old, -1)
	v.prune(old.Identity)
	return old.Identity, true
}

func (v *egressConfigVotes) count(vote egressPodVote, delta int) {
	group := v.byIdentity[vote.Identity]
	if group == nil {
		group = &egressIdentityVotes{configurations: make(map[egressConfig]int)}
		if v.byIdentity == nil {
			v.byIdentity = make(map[string]*egressIdentityVotes)
		}
		v.byIdentity[vote.Identity] = group
	}
	group.pods += delta
	if vote.Invalid {
		group.invalid += delta
	} else {
		group.configurations[vote.Config] += delta
		if group.configurations[vote.Config] == 0 {
			delete(group.configurations, vote.Config)
		}
	}
}

func (v *egressConfigVotes) prune(identity string) {
	if group := v.byIdentity[identity]; group != nil && group.pods == 0 {
		delete(v.byIdentity, identity)
	}
	if len(v.byPod) == 0 {
		v.byPod, v.byIdentity = nil, nil
	}
}

func (v *egressConfigVotes) agreed(identity string) (egressConfig, error) {
	group := v.byIdentity[identity]
	if group == nil || group.pods == 0 {
		return egressConfig{}, nil
	}
	if group.invalid != 0 {
		return egressConfig{}, errInvalidEgressVotes
	}
	if len(group.configurations) != 1 {
		return egressConfig{}, errEgressConfigConflict
	}
	for config, count := range group.configurations {
		if count != group.pods {
			return egressConfig{}, fmt.Errorf("egress vote counters for %q do not match its pod membership", identity)
		}
		return config, nil
	}
	return egressConfig{}, fmt.Errorf("egress vote counters for %q have no configuration", identity)
}
