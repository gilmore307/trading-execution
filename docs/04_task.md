# Tasks

## Active Tasks

- Keep the execution realtime trading runtime check available while the first promoted model is still pending.
- Implement the second dry-run runtime component contracts: `option_reexpression_decision`, `failure_explanation_packet`, and `simulated_fill_event`.
- Wire `trading-evaluation` Replay settlement to call the execution runtime decision builders directly once the second-batch contracts exist.

Execution-side realtime data and monitoring scaffolds are accepted and connected to a runtime readiness surface. The runtime may run continuously, but no active model pointer means `waiting_for_promoted_model`. The checked-in realtime monitor loop service is plan-only by default and requires a reviewed host override before read-only provider observation. It must not activate models, construct orders, submit broker calls, or mutate accounts without the separate accepted gates.

The accepted `trade_risk_cap` validator remains mandatory for future execution-facing decision records but must not be treated as permission to construct or place orders.

The accepted component graph in `docs/50_runtime_components.md` is now the
runtime shape for both live trading and Replay. The implemented first-batch
decision records stay side-effect-free and carry a single account sleeve. The
remaining implementation should preserve that rule: do not submit broker calls,
mutate account/position state, or net risk and positions across the crypto and
equity/options accounts.

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

## Recently Accepted

- Accepted that realtime monitoring runtime control is execution-owned and isolated from manager-owned historical modeling; manager may consume receipts/evidence but must not control provider monitoring processes.
- Added concrete realtime live-observe fixture scaffolds for Alpaca, ThetaData, OKX, calendar/event refs, read-only execution account/restriction context refs, and derived model context refs.
- Completed the realtime monitor handoff envelope: monitor universe filtering stays scoped to Layer 1/2 ETF rows by default, while smoke/loop receipts now default downstream `realtime_feature_snapshot` and `execution_model_decision_input_snapshot` coverage to the complete Layer 1-10 matrix without model activation, broker calls, order construction, or account mutation.
- Added the first execution-owned realtime monitor smoke and bounded loop: `scripts/execution/run_realtime_monitor_smoke.py` loads the reviewed 44-symbol Layer 1/2 ETF universe and can perform bounded read-only Alpaca snapshot observations after explicit approval and execute flag; `scripts/execution/run_realtime_monitor_loop.py` repeats that smoke under execution-owned runtime control and writes per-cycle plus loop receipts.
- Added the execution realtime trading runtime readiness surface: `scripts/execution/run_realtime_trading_runtime_check.py` reports whether the system is waiting for a promoted active model pointer, ready for activation review, ready for order-intent construction, or blocked by a broker-submit gate. The check performs no provider, model, broker, order-submit, or account mutation work.
- Added systemd templates for event-triggered runtime readiness refresh and storage-hosted WebSocket consumption without activating models or submitting orders.
- Added the first formal realtime provider-observe path: `realtime_live_observe_approval` plus `scripts/execution/execute_live_observe.py` can perform bounded read-only OKX/Alpaca/ThetaData market-data observations after explicit approval and execute flag.
- Added the first formal order-construction path: `execution_order_construction_approval` plus `scripts/execution/build_broker_order_intent.py --construct-order` builds an OKX-shaped order intent after `trade_risk_cap` validation; broker submission and account mutation remain separate gates.
- Added side-effect-free realtime feature and model-decision input scaffold: `realtime_feature_snapshot`, `execution_model_decision_input_snapshot`, builders, validators, CLIs, and fixture/shadow tests. These prepare direct handoff into historical-model decision routing without activating models.
- Added execution-owned runtime lifecycle boundary: active model remains trading authority, promoted-but-not-active models run as shadow candidates, mature market-hours evidence selects the next active model, ranks 2-4 stay realtime candidates, and repeated elimination evidence can retire weak candidates.
- Added side-effect-free execution capability catalogs: `execution_realtime_data_interface`, `execution_broker_interface`, and `execution_capability_catalog`.
- Added the live/Replay shared runtime component graph: `opportunity_risk_allocation_engine`, `entry_decision_engine`, `position_lifecycle_controller`, `option_reexpression_review`, `failure_explanation_component`, `order_intent_builder`, and `execution_gate_adapter`. Layer 10 is only called by the failure explanation component after observed model/trade failure.
- Added independent account sleeves to the runtime graph: `crypto_spot_account` with fixed `BTC`/`ETH`/`SOL` spot candidates, and `equity_options_account` for stocks/ETFs/options with option re-expression enabled.
- Implemented and tested the first dry-run runtime component contracts: `target_allocation_snapshot`, `entry_decision`, `position_lifecycle_decision`, and broker-neutral `execution_order_intent`.
- Kept crypto and equity/options runtime decisions separated by account sleeve. Crypto starts from the fixed `BTC`/`ETH`/`SOL` candidate pool; equity/options uses the reviewed stock and optionable-underlying candidate process.
- Accepted the first interface split: realtime market data may share canonical providers with historical data, but it uses distinct realtime transports; broker mutation is a separate authority from market-data access.
- Accepted OKX as crypto execution venue candidate because an official API exists; live mutation remains disabled.
- Deferred Firstrade equity/options automation because no official trading API is accepted.
- Closed the prior execution-preparation phase in `docs/11_execution_acceptance.md`: repository boundary, calendar-discovery ownership, mandatory pre-order `trade_risk_cap` invariant, broker-agnostic risk-cap validator, and package/source/test layout are accepted. No broker adapter, order construction, order placement, fill handling, account mutation, model activation, provider call, or manager dispatch is enabled by this closeout.
- Added component-facing validation entrypoint `scripts/execution/validate_trade_risk_cap.py` and tests. This provides the integration route for unified decision records before any future broker/paper order-construction path.
- Accepted current package/source/test layout and default verification commands: `PYTHONPATH=src python3 -m unittest discover -s tests` and `python3 -m compileall -q src scripts`.
- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.

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
