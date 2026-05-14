# Task

## Active Tasks

- None for the current promote-first model phase.

Execution-side realtime data and monitoring scaffolds are accepted but parked until at least one model has an approved/promotable version. Do not continue realtime monitor hardening, realtime feed expansion, live/shadow integration, or execution adapter work during the current focus window unless explicitly reopened.

The accepted `trade_risk_cap` validator remains mandatory for future execution-facing decision records but must not be treated as permission to construct or place orders.

## Historical-Training Todo Status

- No execution tasks are required for no-broker historical training or the current promote-first model phase.
- Realtime data/monitoring is parked until a model has an approved/promotable version.
- Broker/order/fill/account work remains blocked until explicit execution acceptance.

## Not Current Historical-Training Scope

These items are intentionally outside the current promote-first model phase and must not be treated as active execution work items:

- realtime data/monitoring hardening or feed expansion;
- live/shadow integration expansion;
- broker adapters;
- order construction or placement;
- paper/live mode enablement;
- fill, position, reconciliation, or account mutation artifacts;
- execution-owned storage/request/manifest wiring beyond the accepted risk-cap validation boundary.

## Recently Accepted

- Accepted that realtime monitoring runtime control is execution-owned and isolated from manager-owned historical modeling; manager may consume receipts/evidence but must not control provider monitoring processes.
- Added concrete realtime live-observe fixture scaffolds for Alpaca, ThetaData, OKX, calendar/event refs, read-only execution account/restriction context refs, and derived model context refs.
- Added the first execution-owned realtime monitor smoke and bounded loop: `scripts/execution/run_realtime_monitor_smoke.py` loads the reviewed 44-symbol Layer 1/2 ETF universe and can perform bounded read-only Alpaca snapshot observations after explicit approval and execute flag; `scripts/execution/run_realtime_monitor_loop.py` repeats that smoke under execution-owned runtime control and writes per-cycle plus loop receipts.
- Added the first formal realtime provider-observe path: `realtime_live_observe_approval` plus `scripts/execution/execute_live_observe.py` can perform bounded read-only OKX/Alpaca/ThetaData market-data observations after explicit approval and execute flag.
- Added the first formal order-construction path: `execution_order_construction_approval` plus `scripts/execution/build_broker_order_intent.py --construct-order` builds an OKX-shaped order intent after `trade_risk_cap` validation; broker submission and account mutation remain separate gates.
- Added side-effect-free realtime feature and model-decision input scaffold: `realtime_feature_snapshot`, `execution_model_decision_input_snapshot`, builders, validators, CLIs, and fixture/shadow tests. These prepare direct handoff into historical-model decision routing without activating models.
- Added side-effect-free execution capability catalogs: `execution_realtime_data_interface`, `execution_broker_interface`, and `execution_capability_catalog`.
- Accepted the first interface split: realtime market data may share canonical providers with historical data, but it uses distinct realtime transports; broker mutation is a separate authority from market-data access.
- Accepted OKX as crypto execution venue candidate because an official API exists; live mutation remains disabled.
- Deferred Firstrade equity/options automation because no official trading API is accepted.
- Closed the prior execution-preparation phase in `docs/08_execution_closeout.md`: repository boundary, calendar-discovery ownership, mandatory pre-order `trade_risk_cap` invariant, broker-agnostic risk-cap validator, and package/source/test layout are accepted. No broker adapter, order construction, order placement, fill handling, account mutation, model activation, provider call, or manager dispatch is enabled by this closeout.
- Added component-facing validation entrypoint `scripts/execution/validate_trade_risk_cap.py` and tests. This provides the integration route for unified decision records before any future broker/paper order-construction path.
- Accepted current package/source/test layout and default verification commands: `PYTHONPATH=src python3 -m unittest discover -s tests` and `python3 -m compileall -q src scripts`.
- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.
