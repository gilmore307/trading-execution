# Runtime Model Lifecycle

Runtime model lifecycle is execution-owned.

`trading-evaluation` admits candidates through `promotion_readiness_record`.
`trading-execution` then runs the current active model for trading and runs
eligible promoted model groups as realtime shadow candidates during market hours.

Shadow is not Replay. Replay uses a fixed historical window and frozen historical
data to judge whether a training output is meaningful enough for promotion
readiness. Shadow uses realtime market data during live market hours to compare
already-promoted model groups and choose which one should be active in production.
`execution_shadow_cycle_selection` must not be called during promotion Replay.

## C08 Model Group Shadow Comparison

Shadow is the C08 realtime component, not an after-hours replay mechanism and
not part of promotion Replay.

`execution_shadow_runtime_component` describes `C08 Model Group Shadow
Comparison`. C08 runs during market hours over realtime snapshots. It feeds the
current active model group and eligible promoted shadow model groups the same
point-in-time inputs, records comparable shadow evidence, and later contributes
mature evidence to `execution_shadow_cycle_selection`.

C08 never has trading authority. Only the current active model can route
decisions into C01-C06 live trading. Shadow model decisions remain evidence only
until a later `execution_active_model_config_write` changes the active pointer.
C08 does not write active pointers, construct orders, call brokers, or mutate
accounts.

C08 is hardware-capacity gated. It should run all eligible promoted model groups
only when realtime latency, market-data ingestion, broker gates, and account
freshness remain inside budget. When capacity is constrained, execution must
throttle, sample, or rank the model groups rather than letting shadow comparison
compete with the active trading path.

The side-effect-free local simulator is:

```bash
PYTHONPATH=src python3 scripts/execution/simulate_c08_capacity.py
```

The simulator emits `execution_c08_capacity_simulation`: requested model groups,
admitted model groups, throttled model groups, p95 latency estimate, CPU/memory
limits, and reason codes. Its result is an estimate, not an activation decision.
Production admission still depends on real runtime telemetry.

During future live runtime, historical model tasks are paused by manager policy.
C08 capacity should be measured after that pause because historical training
load is not part of the live budget.

## Cycle

1. Keep one active model as the trading authority.
2. Run C08 with the accepted six-slot roster: one active model group, three
   stable wingmen, and two rotating challengers.
3. Run realtime C07 failure/deviation watch during market hours for active and
   shadow decisions with open theses, open positions, material path deviations,
   new event evidence, or unexplained model drift.
4. Run after-close or off-hours settlement-attribution cycles for matured live
   and shadow decisions when failures, deviations, overblocks, underblocks, or
   unexplained residuals are present.
5. Aggregate matured live/shadow decision effectiveness and attribution evidence
   for the cycle.
6. Re-rank weekly. The best overall candidate becomes or remains active.
7. Keep the next three accepted candidates as stable wingmen when no probation
   candidate is present.
8. Keep two rotating challenger slots for lower-ranked but still eligible
   promoted model groups.
9. If one elimination-probation candidate needs a final realtime check, it uses
   one stable wingman slot for that cycle. The roster becomes one active, two
   stable wingmen, one probation wingman, and two rotating challengers.
10. If the probation wingman remains weak and evidence coverage is valid, it
   enters expedited elimination review. The review still checks data quality and
   coverage, but does not reopen the full model-value debate.
11. Retire a model from runtime promoted eligibility only after accepted
   elimination review. Historical artifacts and promotion history remain
   auditable.

## Performance Boundary

Running many realtime strategies can compete with the active model for runtime capacity. The active model remains primary. Realtime candidates may be sampled, throttled, or moved to off-hours replay/backtest when capacity is constrained.

## Selection Contract

`execution_shadow_cycle_selection` records the post-cycle roster decision:

- previous active model;
- selected active model;
- stable wingmen;
- probation wingman, when present;
- rotating challengers;
- complete realtime candidate refs for all admitted C08 non-active slots;
- shadow-only candidates;
- eliminate candidates and reason evidence;
- weekly re-rank cadence and roster policy;
- probation exit policy;
- live/shadow realtime failure-watch refs when present;
- live/shadow settlement-attribution refs when present;
- untrained event-risk review refs when present;
- whether an active switch is recommended.

The contract records selection only. It does not write active config pointers, construct orders, submit broker calls, or mutate accounts.

If an agent is used to review the cycle, it must use the fixed `runtime-model-lifecycle-review` skill. The comparison packet must be blinded: the agent sees anonymous model labels and does not know which label is current active, newly promoted, old, incumbent, champion, challenger, or latest. Execution code maps labels back to model refs only after the review.

## Active Pointer Write Gate

`execution_active_model_config_write` is the separate execution-owned pointer mutation record. It may be built only after a valid `execution_shadow_cycle_selection`.

Required checks:

- selection id is present and valid;
- embedded shadow-cycle selection payload validates and its digest matches the write record;
- selected active model is still the intended winner;
- expected previous active model matches the selection's previous active model;
- new active config ref is present;
- rollback ref is present;
- write window ref is present, normally a closed-market or explicitly accepted switch window.

This record is the audit surface for changing the active model config pointer. It still does not construct orders, submit broker calls, or mutate accounts.

## Realtime Runtime Readiness

`execution_realtime_trading_runtime_status` is the execution-owned readiness surface for an always-on realtime trading process.

The runtime checks the active model pointer at:

```text
storage/04_execution_artifacts/runtime/active_model/latest_active_model_config_write.json
```

Current states:

- `waiting_for_promoted_model` when no active pointer exists;
- `blocked_invalid_active_model_pointer` when the pointer file is malformed or fails `execution_active_model_config_write` validation;
- `ready_for_active_model_pointer_requires_activation_gate` when a valid pointer exists but model activation has not been enabled for the runtime;
- `ready_for_model_inference_requires_order_construction_gate` when model activation is allowed but order-intent construction is still gated;
- `ready_for_order_intent_construction_not_submission` when order-intent construction can be attempted after a decision record, risk cap, and construction approval;
- `blocked_broker_submit_interface_not_implemented` if a caller asks for broker execution before a reviewed submit adapter exists.

This status connects realtime monitor receipts, Trading Economics recent refresh receipts, model-decision input snapshots, active-pointer writes, risk-cap validation, and order-intent construction. It does not perform provider calls, model calls, broker calls, order submission, or account mutation.

The status reports implemented capability separately from connected runtime inputs. A capability can be available in code while `interfaces_connected` remains false until the current status run is given a valid active pointer or the relevant realtime monitor, model-input, risk-cap, order-approval, or calendar refresh ref.

The checked-in host watcher is:

```text
deploy/systemd/trading-execution-realtime-runtime-check.path
```

It refreshes the status artifact when the active model pointer changes. Dashboard clients should consume the storage-hosted read-model WebSocket route:

```text
/ws/read-models/execution_realtime_trading_runtime_status/latest
```

## Elimination

Elimination is evidence-based and not purely quantitative. Acceptable reasons can include repeated unstable tail loss, repeated poor live/shadow decision effectiveness, excessive turnover/cost sensitivity, operational incompatibility, or clear degradation versus the active model. A single weak cycle normally marks a candidate for elimination review; repeated cycles can retire it.
