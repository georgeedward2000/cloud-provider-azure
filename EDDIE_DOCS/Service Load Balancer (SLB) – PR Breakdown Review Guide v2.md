> **Classified as Microsoft Confidential**

# Service Load Balancer (SLB) – PR Review Guide

**Author:** enechitoaia

> **Currency note (updated on move to the v9 umbrella):** This document now lives in and
> describes the **`eddie/dev/clb-umbrella-v9`** worktree (`/home/enechitoaia/cpa-umbrella-v9`).
> All engine code lives under **`pkg/provider/servicegateway/difftracker/`**. Two large
> forward-looking sections below — the **#10068 reconcile checklist** and the **armnetwork
> v6→v9 SDK drift** — were written against the older private-v6 umbrella and are now **largely
> resolved in v9**; each carries an inline **STATUS (v9)** banner. The PR-sequence tables (CP1–CP8)
> and per-component reference are still the plan of record; live PR/merge statuses may have moved
> since this doc was last synced.

## Introduction

This document provides an overview of the Pull Request breakdown for the
**Service Load Balancer (SLB)** feature in Azure Cloud Provider. The SLB
introduces an **asynchronous, non-blocking architecture** that replaces the
synchronous `EnsureLoadBalancer()` pattern for managing Kubernetes
LoadBalancer services and NAT Gateways for pod egress.

## Key Architecture Components

| Component | Description |
| --- | --- |
| **DiffTracker** | Core state manager holding K8s and NRP state representations |
| **Engine** | API layer with a 6-state machine for service lifecycle |
| **ServiceUpdater** | Background worker for Azure resource creation/deletion |
| **LocationsUpdater** | Background worker syncing pod IP locations to NRP |
| **Finalizers** | Service and Pod finalizer management for deletion protection |

## Service State Machine

```
StateNotStarted → StateCreationInProgress → StateCreated → StateDeletionPending → StateDeletionInProgress
                                          ↘ StateUpdateInProgress ↗
```

`StateUpdateInProgress` handles in-place config drift on an already-created service. Two terminal
flags on `ServiceOperationState` park a service off the happy path without blocking init:
`CreationFailedTerminal` (non-retryable creation failure) and `RetriesExhausted` (retried
`maxServiceRetries` times); both self-heal on the next relevant change.

## PR Dependency Graph

_(Dependency diagram in the source PDF; each PR depends on the previous one as noted in the "Depends on" fields below.)_

---

# Compressed PR Plan (~11 PRs, dependency-verified)

> **Why:** the original plan below lists 16 PRs. Small, tightly-coupled layers can be
> merged without hurting reviewability, while large components set a practical floor.
> This collapses **16 → 11 PRs** (2 merged, 1 in review, 8 to go).
>
> **⚠️ This sequence was validated against the real symbol-level dependencies in the
> `eddie/dev/clb-difftracker-engine` umbrella branch** (not just the doc), because a
> wrong order yields a **non-compiling PR** and merges are irreversible. The naive
> "one-file-per-PR" layering is **incorrect** — see _Dependency facts_ below.
>
> **Caveat on LOC & boundaries:** PR boundaries are **not whole files**. Shared files
> (`types.go`, the `DiffTracker` struct, `k8s_state_updates.go`) **grow incrementally**
> across PRs — earlier PRs ship a trimmed struct and later PRs add fields/methods. The
> ~LOC column is the **end-state** file size and therefore *overstates* the per-PR delta
> for files that grow. **e2e starts at CP6.**
>
> **External name:** the **External name** column is the outward-facing PR label (F1 = **1st PR**,
> CP1 = **2nd PR**, CP2 = **3rd PR**, … CP8 = **9th PR**). It is distinct from the **Bundles (old)**
> column's `PR N`, which references the original 16-PR plan.

| New PR | External name | Bundles (old) | Files | ~LOC (end-state) | Depends on | e2e |
| --- | --- | --- | --- | --- | --- | --- |
| **F0** Foundation: NAT GW client | Foundation | PR 0 | azclient ServiceGatewayClient | — | — | ✅ merged (#10066) |
| **F1** Foundation: First CLB batch | 1st PR | PR 1 | config, constants, clients | — | F0 | ✅ merged (#9775) |
| **CP1** DiffTracker Core *(trimmed struct; grows later)* | 2nd PR | PR 2 + 3 + 4 + sync | `types`, `config`, `util`, `difftracker`, `k8s_state_updates`, `nrp_state_updates`, `sync_operations` | ~1,700 | F1 | 🔄 in review (#10068) |
| **CP2** Azure Operations Layer | 3rd PR | PR 5 + resource_helpers | `azure_operations`, `resource_helpers` | ~760 | CP1 | unit (mocked clients) |
| **CP3** Finalizers + Metrics *(leaves — moved EARLIER)* | 4th PR | PR 9 + 10 | `finalizers`, `metrics` | ~900 | CP1 | unit |
| **CP4** Async Engine + Workers *(MERGED — mutually dependent)* | 5th PR | PR 6 + 7 + 8 | `engine`, `service_updater`, `locations_updater` | ~1,900 | CP2, CP3 | unit |
| **CP5** Initialization | 6th PR | PR 11 | `initialization` | ~1,665 | CP4 | unit *(may split: init-from-cluster vs sync/recovery)* |
| **CP6** Provider Integration + ServiceGateway | 7th PR | PR 12 | `azure.go` wiring, `azure_servicegateway*`, `azure_natgateway_repo`, `azure_publicip_repo` | ~700 | CP5 | **★ first e2e — SGW create/attach + cleanup** |
| **CP7** LoadBalancer Integration | 8th PR | PR 13 | `azure_loadbalancer`, `azure_loadbalancer_backendpool`, `azure_standard`, healthprobe | ~1,200 | CP6 | **e2e — `type=LoadBalancer` → Azure LB + IP** |
| **CP8** Informers (EndpointSlice + Pod egress) | 9th PR | PR 14 + 15 | EndpointSlice informer, `azure_servicegateway_pods` | ~450 | CP7 | **e2e — reachability + egress / NAT GW** |

## Dependency facts (verified in umbrella — these drive the ordering)

1. **`ServiceUpdater` → finalizers:** `service_updater.go` calls `removeServiceGatewayFinalizer` (defined in `finalizers.go`). → **Finalizers must land *before* the workers**, not after.
2. **`engine` → metrics:** `engine.go` calls `recordServiceOperation` / `updatePending…Metric` (17 call sites). → **Metrics must land *before* the engine**, not after.
3. **workers ⇄ engine cycle:** both `service_updater.go` and `locations_updater.go` call `engine.checkInitializationComplete`, while `engine.go` owns the `serviceUpdater`/`locationsUpdater` fields + triggers. → **CP4 cannot be split** into "workers" then "engine"; they ship together.
4. **core struct binds later layers:** the umbrella `DiffTracker` struct embeds `*ServiceUpdater`, `*LocationsUpdater`, `pendingServiceOps`, and init channels. → CP1 ships a **trimmed** struct and CP3–CP5 **grow** it; this is why boundaries are not whole files.
5. **leaves (no back-deps, safe early):** `azure_operations`+`resource_helpers`, `finalizers`, `metrics` depend only on CP1/the struct.

## What changed vs. the naive plan (and why)

- **Finalizers + Metrics moved earlier (now CP3, before the engine)** — fact #1 and #2: shipping them after the engine/workers would not compile.
- **Workers + Engine merged into one PR (CP4, ~1,900 LOC)** — fact #3: they mutually reference, so they can't be separate PRs without introducing throwaway stubs/interfaces. This is the **floor**; merging further (e.g. + initialization) would exceed ~3,500 LOC and hurt reviewability.
- **Other safe merges kept:** CP1 (= PR 2+3+4+sync, the shape of #10068) and CP8 (= PR 14+15, same informer plumbing → reachability + egress e2e).
- **Kept separate (size floor):** CP5 Initialization (~1,665) and CP7 backendpool (~1,024) are large enough to stand alone.

## ⚠️ Umbrella ↔ #10068 drift (must propagate on every PR cut from umbrella)

> **STATUS (v9): ✅ applied in the umbrella.** The v9 tree already uses the standardized names
> (`K8sState`/`NRPState`, unexported `outboundIdentityPodRefCount`) with **zero** occurrences of the
> old names, and carries the concurrency/enum fixes (`deepEqualLocked` and the single-lock
> `GetSyncOperations`). This section is retained as historical rationale for the renames.

The umbrella branch still uses the **pre-#10068** names; #10068 standardized them. Every later PR cut from the umbrella must carry these renames (see also _Follow-up Items_ at the end):
- `K8s_State` / `NRP_State` → **`K8sState` / `NRPState`**
- `LocalServiceNameToNRPServiceMap` → **`outboundIdentityPodRefCount`** (now unexported)
- plus the other #10068 fixes (counter robustness, `DeepEqual`/`GetSyncOperations` locking, enum bounds-checks, logging verbosity).

## e2e rollout

Unit tests on every PR; integration tests with mocked Azure clients from **CP2/CP4**; first true **e2e at CP6** (ServiceGateway create/attach + cleanup), then enriched through **CP7** (inbound LB + IP) and **CP8** (reachability, egress, scale, deletion/finalizers, crash-recovery).

---

# PR Descriptions (original 16-PR plan — component reference)

> The sections below are the granular, per-component reference for the original plan.
> Use them to see exactly which files/methods land in each **CP** above (per the
> "Bundles (old)" column).

## Phase 1: Foundation

### PR 0: NAT Gateway Client

- **Status:** Add ServiceGatewayClient to Azure client factory by georgeedward2000 — Pull Request **#10066** · kubernetes-sigs/cloud-provider-azure — ✅ **Merged**

Azure SDK client module for NAT Gateway CRUD operations.

### PR 1: First CLB Batch of Changes

- **Status:** PR **#9775** — ✅ **Merged**

Foundation PR with configuration, clients, and NSG enhancements.

| Component | Changes |
| --- | --- |
| Security Group | Not in scope anymore |
| Configuration | `ServiceGatewayEnabled`, `IsLBBackendPoolTypePodIP()`, `UseServiceLoadBalancer()` |
| Constants | `LoadBalancerSKUService` |
| ServiceGateway Client | We have a different PR opened for this one — full client with `GetAddressLocations`, `GetServices`, `UpdateAddressLocations`, `UpdateServices` |

## Phase 2: DiffTracker Core

### PR 2: DiffTracker Types & Utilities

- **Depends on:** CLB 2nd PR — Add DiffTracker core types, state management, and sync operations by georgeedward2000 — Pull Request **#10068** · kubernetes-sigs/cloud-provider-azure — 🔄 **In review**

Core types and utility functions for the DiffTracker package.

| File | Content |
| --- | --- |
| `types.go` | `ResourceState` enum (6 states), `K8sResources`, `NRPResources`, `ServiceOperationState` |
| `config.go` | `Config` struct with subscription, resource group, location, service gateway settings |
| `util.go` | DTO mappers (K8s ↔ NRP), IP family helpers, resource name generation, diff computation |

### PR 3: State Update Methods

- **Depends on:** PR 2

Methods for tracking K8s and NRP state changes.

| File | Methods |
| --- | --- |
| `k8s_state_updates.go` | `UpdateK8sService()`, `DeleteK8sService()`, `UpdateK8sEndpoints()`, `UpdateK8sPod()`, `DeleteK8sPod()` |
| `nrp_state_updates.go` | `UpdateNRPService()`, `DeleteNRPService()`, `UpdateNRPLocations()` |
| `resource_helpers.go` | `CreatePublicIP()`, `DeletePublicIP()`, `CreateLoadBalancer()`, `DeleteLoadBalancer()`, `CreateNATGateway()`, `DeleteNATGateway()` |

### PR 4: DiffTracker Core

- **Depends on:** PR 3

Main DiffTracker struct and state management.

```go
type DiffTracker struct {
    k8sResources       *K8sResources
    nrpResources       *NRPResources
    operationStates    map[string]*ServiceOperationState
    serviceUpdaterCh   chan struct{}
    locationsUpdaterCh chan struct{}
}
```

## Phase 3: Background Workers

### PR 5: Azure Operations Repository

- **Depends on:** PR 4

Azure API operations layer wrapping ServiceGateway and resource clients.

| Method | Description |
| --- | --- |
| `UpdateServices()` | Register/unregister services with ServiceGateway |
| `UpdateAddressLocations()` | Sync pod IPs to NRP |
| `GetServices()` / `GetAddressLocations()` | Query current NRP state |
| `CreateOrUpdateLoadBalancer()` / `DeleteLoadBalancer()` | LB management |
| `CreateOrUpdatePublicIP()` / `DeletePublicIP()` | PIP management |

### PR 6: ServiceUpdater

- **Depends on:** PR 5

Background worker for Azure resource lifecycle.

| Responsibility | Description |
| --- | --- |
| Creation Flow | PIP → LB/NAT Gateway → Register with ServiceGateway |
| Deletion Flow | Cleanup locations → Delete resources → Unregister |
| Retry Logic | Max 3 attempts with backoff |
| Callback | `OnServiceCreationComplete()` |

### PR 7: LocationsUpdater

- **Depends on:** PR 6

Background worker for pod IP location sync.

| Responsibility | Description |
| --- | --- |
| Sync | Compute diff between K8s and NRP locations |
| Update | Call `UpdateAddressLocations()` |
| Deletion Check | `CheckPendingDeletions()` after each sync |

## Phase 4: Engine & Supporting Features

### PR 8: Engine API Layer

- **Depends on:** PR 7

Main Engine API coordinating all components.

| API | Description |
| --- | --- |
| `AddService(config)` | Start service creation |
| `UpdateEndpoints(serviceUID, old, new)` | Handle endpoint changes |
| `DeleteService(serviceUID, isInbound)` | Start service deletion |
| `AddPod(serviceUID, podKey, location, address)` | Add pod to egress |
| `DeletePod(...)` | Remove pod from egress |

**Buffering:** Endpoints/pods arriving during creation are buffered and applied after resource creation completes.

### PR 9: Finalizers

- **Depends on:** PR 8

Finalizer management for deletion protection.

| Method | Description |
| --- | --- |
| `AddServiceFinalizer()` / `RemoveServiceFinalizer()` | Protect services during SLB deletion |
| `AddPodFinalizer()` / `RemovePodFinalizer()` | Protect pods during NAT Gateway cleanup |
| `RemoveLastPodFinalizers()` | Bulk removal after NAT Gateway deletion |

### PR 10: Sync Operations & Metrics

- **Depends on:** PR 9

Startup synchronization and observability.

| Component | Content |
| --- | --- |
| `sync_operations.go` | `ComputeSyncOperations()`, `ApplySyncOperations()`, orphan detection |
| `metrics.go` | `slb_service_state`, `slb_service_operations_total`, `slb_service_operation_duration_seconds`, `slb_azure_api_calls_total` |

## Phase 5: Initialization

### PR 11: DiffTracker Initialization

- **Depends on:** PR 10

Initialize DiffTracker from cluster and NRP state.

| Responsibility | Description |
| --- | --- |
| K8s Query | Fetch Services, EndpointSlices, Pods |
| NRP Query | Fetch ServiceGateway services and locations |
| Sync | Compute and apply initial sync operations |
| Workers | Start ServiceUpdater and LocationsUpdater goroutines |
| Recovery | Handle CCM crash recovery scenarios |

## Phase 6: Provider Integration

### PR 12: Core Provider Integration

- **Depends on:** PR 11

Integrate DiffTracker with Cloud provider and add repository layers.

| File | Changes |
| --- | --- |
| `azure.go` | Add `diffTracker` field, initialize in `InitializeCloudFromConfig`, ServiceGateway creation |
| `azure_fakes.go` | Test helpers for ServiceGateway mode |
| `azure_servicegateway.go` | `GetServiceGatewayID()` |
| `azure_servicegateway_init.go` | `existsServiceGateway()`, `createServiceGateway()`, `attachServiceGatewayToSubnet()` |
| `azure_servicegateway_repo.go` | `GetServiceGateway()`, `GetServices()`, `UpdateAddressLocations()`, `UpdateServices()` |
| `azure_natgateway_repo.go` | `GetNATGateway()`, `CreateOrUpdateNATGateway()`, `DeleteNATGateway()` |
| `azure_publicip_repo.go` | `CreateOrUpdatePublicIPForSLB()`, `DeletePublicIPForSLB()` |

### PR 13: Load Balancer Integration

- **Depends on:** PR 12

Integrate DiffTracker with `EnsureLoadBalancer()`.

| File | Changes |
| --- | --- |
| `azure_loadbalancer.go` | Skip flipped service reconciliation in SLB mode, call `Engine.AddService()`, return existing status during async creation |
| `azure_loadbalancer_backendpool.go` | `backendPoolTypePodIP` struct, `newBackendPoolTypePodIP()` |
| `azure_loadbalancer_healthprobe.go` | Update `keepSharedProbe()` for ServiceGateway mode |
| `azure_standard.go` | `newBackendPoolTypePodIP()` backend pool type |

### PR 14: EndpointSlice Informer Enhancement

- **Depends on:** PR 13

Enhance EndpointSlice informer for SLB.

| Handler | SLB Behavior |
| --- | --- |
| `AddFunc` | Call `diffTracker.UpdateEndpoints(serviceUID, nil, newAddresses)` |
| `UpdateFunc` | Call `diffTracker.UpdateEndpoints(serviceUID, oldAddresses, newAddresses)` |
| `DeleteFunc` | Call `diffTracker.UpdateEndpoints(serviceUID, oldAddresses, nil)` |

### PR 15: Pod Informer for Egress

- **Depends on:** PR 14

Pod informer for egress label detection.

| Feature | Description |
| --- | --- |
| Label Selector | `kubernetes.azure.com/service-egress-gateway` |
| `AddFunc` | Validate pod, call `Engine.AddPod()` |
| `UpdateFunc` | Handle label changes, phase transitions, DeletionTimestamp |
| `DeleteFunc` | Call `Engine.DeletePod()`, handle tombstones |
| Finalizers | Add/remove pod finalizers for deletion protection |

---

# Summary

**Compressed plan (16 → 11 PRs, dependency-verified):**

| New PR | External name | Description | Status / e2e |
| --- | --- | --- | --- |
| F0–F1 | Foundation / 1st PR | Foundation: NAT GW client, config, constants, clients | ✅ merged (#10066, #9775) |
| CP1 | 2nd PR | DiffTracker core: types, state updates, struct, sync/diff | 🔄 in review (#10068) |
| CP2 | 3rd PR | Azure operations layer (+ resource helpers) | unit |
| CP3 | 4th PR | Finalizers + metrics *(leaves — before the engine)* | unit |
| CP4 | 5th PR | Async Engine + Workers (engine + ServiceUpdater + LocationsUpdater) *(merged)* | unit |
| CP5 | 6th PR | Initialization (from cluster + NRP; recovery) | unit *(may split)* |
| CP6 | 7th PR | Provider integration + ServiceGateway | ★ first e2e |
| CP7 | 8th PR | LoadBalancer integration | e2e (inbound LB) |
| CP8 | 9th PR | Informers: EndpointSlice + Pod egress | e2e (reachability + egress) |

**Original phases (reference for the per-component sections above):**

| Phase | Old PRs | Maps to |
| --- | --- | --- |
| Phase 1: Foundation | 0–1 | F0–F1 |
| Phase 2: DiffTracker Core | 2–4 | CP1 |
| Phase 3: Background Workers | 5–7 | CP2 (azure ops) + CP4 (workers) |
| Phase 4: Engine & Features | 8–10 | CP4 (engine) + CP3 (finalizers, metrics) |
| Phase 5: Initialization | 11 | CP5 |
| Phase 6: Provider Integration | 12–15 | CP6 + CP7 + CP8 |

---

# Follow-up Items from PR #10068 Review (carry into later PRs)

> **STATUS (v9): ✅ mostly reconciled in the umbrella.** On the v9 tree the field renames are done
> (`outboundIdentityPodRefCount`, unexported), the concurrency refactor is in (`deepEqualLocked` +
> single-lock `GetSyncOperations`), and `removeServiceFromK8sStateLocked` is now **wired** (public
> lock-acquiring wrapper + callers on the service-deletion path). Still outstanding: the two #10068
> **test artifacts** (`coverage_test.go` and `TestUpdateK8sPodRemoveUsesStoredIdentity`) were **not**
> carried over verbatim — confirm equivalent coverage exists before relying on this checklist.

> **Context:** PR #10068 (DiffTracker Types & Utilities — "PR 2") was reviewed by `nilo19`
> and several changes were made **only on the PR branch**
> `enechitoaia/clb-difftracker-engine-2`. We are **NOT** updating the umbrella / engine
> branch (`eddie/dev/clb-difftracker-engine`) now. These items must be reconciled **later,
> when we pull / rebase the engine branch on top of the merged PR #10068.** This section is
> the checklist for that rebase and for raising the following PRs (3+).

## ⚠️ Breaking / must-reconcile on rebase

- **Field rename:** `DiffTracker.LocalServiceNameToNRPServiceMap` → **`outboundIdentityPodRefCount`**,
  and it was **unexported** (lowercase). It is a *reference counter* keyed by lowercased
  `PublicOutboundIdentity`, value = number of pods using that **outbound/egress** identity.
  It is **outbound-only**; inbound (LoadBalancer) services are not counted here.
  - Engine branch still references the **old, exported** name in ~7 files:
    `engine.go`, `initialization.go`, `k8s_state_updates.go` (its own copy), and the
    **`provider`-package** tests `azure_loadbalancer_test.go` (≈ lines 888, 897) and
    `azure_local_services_test.go` (≈ lines 857–858).
  - **Export tension:** the two `provider`-package tests access the field **cross-package**
    (`az.diffTracker.LocalServiceNameToNRPServiceMap`). Since it is now unexported, on rebase
    we must either (a) keep it exported (`OutboundIdentityPodRefCount`), (b) add an exported
    accessor / test helper, or (c) move those assertions into the `difftracker` package.

## 🔁 Fixes made in PR #10068 that the engine branch's copies need

- **Pod counter robustness** (`k8s_state_updates.go`): `removePod` now returns the **stored**
  pod identity, and the `REMOVE` path decrements that authoritative identity (skipping empty),
  rather than trusting `input.PublicOutboundIdentity`. Engine branch's copy should carry this.
  Regression test: `TestUpdateK8sPodRemoveUsesStoredIdentity`.
- **Concurrency** (`util.go` / `sync_operations.go`): `DeepEqual` now takes `dt.mu`, and
  `GetSyncOperations` takes the lock **once** and calls lock-free `*Locked` variants
  (`deepEqualLocked`, `getSyncLoadBalancerServicesLocked`, `getSyncNRPNATGatewaysLocked`,
  `getSyncLocationsAddressesLocked`) for a single consistent snapshot. The engine branch has
  the same un-fixed structure — apply the same refactor.
- **Enum `String()` bounds-check** (`util.go`): `Operation`, `UpdateAction`, `SyncStatus`
  `String()` now guard against out-of-range index instead of panicking. Same pattern exists
  in the engine branch `util.go`.
- **Logging verbosity** (`sync_operations.go`, `util.go`): bare `klog.Infof` → `klog.V(2)`
  (summaries) / `klog.V(4)` (per-item). Sweep the engine branch for remaining bare `Infof`.
- **Micro:** `findLocationData` now uses a direct map lookup (was an O(n) scan); set param
  `Services` renamed to `nrpServices` in `GetServicesToSync`.

## 🪝 Wiring owed by later PRs

- `removeServiceFromK8sStateLocked` is present but `//nolint:unused` in PR #10068. The engine
  PR must add a **public lock-acquiring wrapper** and the **caller** (service-deletion path).
- `isServiceReady` currently checks NRP only. Engine PR extends it to first check
  `pendingServiceOps` (`StateCreated` / `StateUpdateInProgress`) then fall back to NRP.
  (Already implemented on the engine branch — keep on rebase.)

## 🧷 Intentionally kept (do not "fix" in later PRs)

- `IgnoreCaseSet.MarshalJSON` is currently **unused** but kept as foundational (will be
  consumed by debug logging / state snapshots in later PRs). **Deferred** in the review reply,
  not removed.
- Names kept on purpose, with clarifying comments: `Config.Location` (Azure region, mirrors
  `az.Location`), `NRPState.LoadBalancers` / `NATGateways` (hold service UIDs, not Azure
  resource names), `isServiceReady`, and the `UpdateAction` enum (maps to the ServiceGateway
  ARM `AddressUpdateAction`).

## ✅ Test coverage

- `difftracker` package coverage raised to ~95% via a new `coverage_test.go` plus targeted
  cases. Keep/merge `coverage_test.go` when rebasing.

---

# ⚠️ Azure SDK Drift: private armnetwork/v6 → public v9 (affects every ServiceGateway PR)

> **STATUS (v9): ✅ resolved in this umbrella.** The v9 tree builds against the **public
> `armnetwork/v9 v9.0.0`** with **no `my-vendor` `replace`** directive. The symbol mapping below is
> fully applied — the public symbols (`ServiceUpdateAction`, `armnetwork.UpdateAction`,
> `AddressUpdateAction`, `armnetwork.ServiceType`, `SubResource{ID:…}`) are in use with **zero**
> occurrences of the old private-v6 names, and the Service SKU is emitted as
> `armnetwork.LoadBalancerSKUName(consts.LoadBalancerSKUNameService)`. This section is retained as a
> reference for any PR still being cut from the older private-v6 umbrella. (Note: the v9 vendored SGW
> client also carries a separate `HasStatusCode` fix so `updateServices`/`updateAddressLocations`
> accept HTTP **200** in addition to 202/204, while `deleteOperation` stays at 202/204 — not part of
> this symbol mapping.)

> **Discovered while prepping CP2.** The **umbrella branch builds against a PRIVATE
> `armnetwork/v6` fork** wired in via a `replace` directive
> (`replace github.com/Azure/azure-sdk-for-go/.../armnetwork/v6 => ./my-vendor/...`),
> whereas **CP1 / upstream master use the PUBLIC `armnetwork/v9`**. The public v9 SDK
> *does* have ServiceGateway support, but the types are **named and shaped differently**.
> Every PR that ports ServiceGateway/Azure code from the umbrella (CP2 onward) must apply
> these adaptations, or it won't compile on top of CP1/master.

## Symbol mapping (umbrella v6 → public v9)

| Umbrella (private v6) | Public v9 |
| --- | --- |
| `armnetwork.ServiceGatewayUpdateServicesRequestAction` | `armnetwork.ServiceUpdateAction` |
| `armnetwork.ServiceGatewayUpdateAddressLocationsRequestAction` | `armnetwork.UpdateAction` |
| `armnetwork.ServiceGatewayAddressLocationAddressUpdateAction*` | `armnetwork.AddressUpdateAction*` |
| `armnetwork.ServiceGatewayServicePropertiesFormatServiceType[Inbound/Outbound]` | `armnetwork.ServiceType[Inbound/Outbound]` |
| `…RequestActionFullUpdate/PartialUpdate` (suffix) | suffix preserved (e.g. `ServiceUpdateActionFullUpdate`) |
| `armnetwork.LoadBalancerSKUNameService` (custom SKU const) | not in v9 → `armnetwork.LoadBalancerSKUName(consts.LoadBalancerSKUService)` |

## Shape differences (require code changes, not just renames)

- `NatGatewayPropertiesFormat.ServiceGateway` is typed **`*SubResource`** in v9 — replace
  `&armnetwork.ServiceGateway{ID: …}` with `&armnetwork.SubResource{ID: …}`.
- `ServiceGatewayAddressLocation` uses field **`AddressLocation *string`** (already matches
  the umbrella usage, but confirm on every port).

## ServiceGateway client dependency (root module roll-up)

- The SGW client (#10066) is **merged into the `pkg/azclient` submodule** and wired into
  `ClientFactory.GetServiceGatewayClient()` — confirmed on upstream master.
- BUT the **root module** must bump its vendored `pkg/azclient` to pick it up. First version
  containing the client: **`azclient v0.20.8`** (CP1 currently pins `v0.20.2`).
- **The first consumer carries the bump.** CP2 is that consumer; ship the bump as a **separate
  `build(deps): bump azclient` PR** (mechanical, ~86 vendor files) that lands **before** CP2,
  so CP2's own diff stays source-only. Do **not** put it in CP1 (CP1 never calls the client).

## CP2 local-prep status (worktree `/home/enechitoaia/cpa-cp2`)

Already prepped and green (build + vet + `-race`), stacked as:
`CP1 (#10068) → azclient-bump-sgw (bump PR) → cp2-azure-ops (CP2 PR)`.
CP2 = `azure_operations.go`, `resource_helpers.go`, `dto_mappers.go`, the deferred DTO/config
types in `types.go`, `consts.NatGatewayIDTemplate`, and ported tests. **Held** pending CP1 merge.
