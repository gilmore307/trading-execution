# Tasks

## Active Tasks

- Keep the execution realtime trading runtime check available while the first promoted model is still pending.
- Support `trading-evaluation` as it freezes the first replay dataset and starts producing real Replay decision rows.

Execution-side realtime data, monitoring, model-input handoff, runtime component, active-pointer readiness, and order-intent surfaces are accepted. The runtime may run continuously, but no active model pointer means `waiting_for_promoted_model`. The checked-in realtime monitor loop service is plan-only by default and requires a reviewed host override before read-only provider observation.

The active route is side-effect-controlled:

- Realtime provider observation requires `realtime_live_observe_approval` and an explicit execute flag.
- Model activation requires a valid active-model config pointer and runtime activation gate.
- `trade_risk_cap` validation is mandatory before executable order intent construction.
- Broker-shaped order intents require `execution_order_construction_approval`.
- Broker submission, live fills, reconciliation, account mutation, and position mutation remain closed.
- Replay uses the same runtime component graph through side-effect-free adapters; it must not submit broker requests or mutate account, order, or position state.
- Runtime decisions carry exactly one account sleeve and must not net risk or positions across the crypto and equity/options accounts.

## Historical-Training Todo Status

- No execution tasks are required for no-broker historical training or the current promote-first model phase.
- Realtime data/monitoring and runtime readiness may stay online before a promoted model exists; the runtime must wait on a valid active model config pointer.
- Broker/order/fill/account work remains blocked until explicit execution acceptance.

## Not Current Historical-Training Scope

These items are intentionally outside the current promote-first model phase and must not be treated as active execution work items:

- broker submit adapters;
- broker adapters;
- order placement;
- paper/live mode enablement;
- fill, position, reconciliation, or account mutation artifacts;
- execution-owned storage/request/manifest wiring beyond accepted monitor, active-pointer, risk-cap, and order-intent boundaries.

## Current Accepted Surfaces

- Realtime monitor smoke/loop receipts and read-only provider-observe approval.
- Realtime capture, feature snapshot, and model-decision input handoff builders.
- Runtime active/shadow model roster selection and active-pointer write records.
- Live/Replay shared runtime component graph and side-effect-free decision builders.
- Independent `crypto_spot_account` and `equity_options_account` sleeves.
- OKX broker-order intent construction after approval and risk-cap validation, without submission.
- Capability catalogs for realtime data and broker/exchange posture.
- Calendar discovery, including approved Nasdaq future EPS-consensus baseline snapshots.

## Nasdaq EPS baseline snapshot route

Execution now supports the manager-prepared Nasdaq future earnings EPS-consensus baseline snapshot mode inside `calendar_discovery`.

When a task key sets:

```json
{
  "params": {
    "calendar_source": "nasdaq_earnings_calendar",
    "baseline_capture_mode": "future_pre_event_eps_consensus_snapshot"
  }
}
```

`calendar_discovery` still writes `saved/release_calendar.csv` and additionally writes `saved/earnings_guidance_expectation_baseline.csv` containing only clean pre-event EPS forecast rows. Rows are skipped if source data contains actual EPS or surprise fields, or if the capture clock is not before `release_time`.

This route does not activate models, construct orders, place orders, mutate broker/account state, or decide beat/miss. It is an EPS-consensus baseline capture route only; revenue consensus and guidance expectation baselines remain outside this execution slice.
