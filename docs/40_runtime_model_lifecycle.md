# Runtime Model Lifecycle

Runtime model lifecycle is execution-owned.

`trading-evaluation` admits candidates through `promotion_readiness_record`. `trading-execution` then runs the current active model for trading and runs promoted-but-not-active models as shadow candidates during market hours.

## Cycle

1. Keep one active model as the trading authority.
2. Run promoted-but-not-active models as shadow candidates during the same market-hours window.
3. Aggregate matured live/shadow decision effectiveness for the cycle, normally about one month.
4. Select the best overall candidate as the next active model.
5. Keep ranks 2-4 as realtime candidates for ongoing comparison.
6. Keep lower-ranked candidates shadow-only unless elimination evidence is sufficient.
7. Mark weak candidates as eliminate candidates when there is a clear reason, not merely a low score.
8. Retire a model only after repeated eliminate-candidate cycles or another accepted elimination policy.

## Performance Boundary

Running many realtime strategies can compete with the active model for runtime capacity. The active model remains primary. Realtime candidates may be sampled, throttled, or moved to off-hours replay/backtest when capacity is constrained.

## Selection Contract

`execution_shadow_cycle_selection` records the post-cycle roster decision:

- previous active model;
- selected active model;
- ranks 2-4 realtime candidates;
- shadow-only candidates;
- eliminate candidates and reason evidence;
- whether an active switch is recommended.

The contract records selection only. It does not write active config pointers, construct orders, submit broker calls, or mutate accounts.

If an agent is used to review the cycle, it must use the fixed `runtime-model-lifecycle-review` skill. The comparison packet must be blinded: the agent sees anonymous model labels and does not know which label is current active, newly promoted, old, incumbent, champion, challenger, or latest. Execution code maps labels back to model refs only after the review.

## Active Pointer Write Gate

`execution_active_model_config_write` is the separate execution-owned pointer mutation record. It may be built only after a valid `execution_shadow_cycle_selection`.

Required checks:

- selection id is present and valid;
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

The checked-in host watcher is:

```text
deploy/systemd/trading-execution-realtime-runtime-check.path
```

It refreshes the status artifact when the active model pointer changes. Dashboard clients should consume the storage-hosted read-model WebSocket route:

```text
/ws/read-models/execution_realtime_trading_runtime_status/latest
```

The old minute timer is not the primary runtime status channel.

## Elimination

Elimination is evidence-based and not purely quantitative. Acceptable reasons can include repeated unstable tail loss, repeated poor live/shadow decision effectiveness, excessive turnover/cost sensitivity, operational incompatibility, or clear degradation versus the active model. A single weak cycle normally marks a candidate for elimination review; repeated cycles can retire it.
