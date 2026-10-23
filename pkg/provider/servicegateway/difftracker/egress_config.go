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
	"strconv"
	"strings"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

type egressConfig struct {
	IdleTimeoutMinutes int32
}

func parseEgressConfig(annotations map[string]string) (egressConfig, error) {
	minutes, err := parseEgressIdleTimeout(annotations)
	return egressConfig{IdleTimeoutMinutes: minutes}, err
}

func parseEgressIdleTimeout(annotations map[string]string) (int32, error) {
	var unknown string
	for key := range annotations {
		if isEgressConfigAnnotation(key) && key != consts.PodAnnotationServiceEgressGatewayIdleTimeout {
			if unknown == "" || key < unknown {
				unknown = key
			}
		}
	}
	if unknown != "" {
		return 0, fmt.Errorf("unknown egress configuration annotation %q", unknown)
	}
	raw, present := annotations[consts.PodAnnotationServiceEgressGatewayIdleTimeout]
	if !present {
		return consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes, nil
	}
	minutes, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || minutes < int64(consts.ServiceEgressGatewayIdleTimeoutMinMinutes) ||
		minutes > int64(consts.ServiceEgressGatewayIdleTimeoutMaxMinutes) {
		return 0, fmt.Errorf("%s must be an integer from %d to %d minutes, got %q",
			consts.PodAnnotationServiceEgressGatewayIdleTimeout,
			consts.ServiceEgressGatewayIdleTimeoutMinMinutes,
			consts.ServiceEgressGatewayIdleTimeoutMaxMinutes, raw)
	}
	return int32(minutes), nil
}

func isEgressConfigAnnotation(key string) bool {
	return strings.HasPrefix(key, consts.PodLabelServiceEgressGateway+"-") &&
		key != consts.PodAnnotationServiceEgressGatewayResourceGroup
}
