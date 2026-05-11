# Task

## Active Tasks

- Define the initial execution interface boundary without enabling mutation: realtime market-data interfaces are cataloged separately from historical backfill endpoints, model input coverage is explicit for Layers 1-8, append-only capture requirements are defined for forward/shadow validation, dry-run/fixture subscription planning and capture validation are available, realtime feature/model-decision input handoff envelopes can feed fixture/shadow historical-model decision routing, and broker/exchange interfaces are cataloged separately from market-data observation.
- Start OKX crypto execution on the safe path only: catalog and future adapter scaffold are allowed, but live order mutation remains disabled until explicit mode, approval, idempotency, credential, risk-cap, and receipt gates exist.
- Keep Firstrade equity/options execution deferred because no official trading API is accepted. Do not implement reverse-engineered login, browser trading, or unofficial order automation.

The accepted `trade_risk_cap` validator remains mandatory for future execution-facing decision records but must not be treated as permission to construct or place orders.

## Historical-Training Todo Status

- No execution tasks are required for no-broker historical training.
- Broker/order/fill/account work remains blocked until explicit execution acceptance.

## Not Current Historical-Training Scope

These items are intentionally outside the current no-broker historical-training run and must not be treated as active execution work items:

- broker adapters;
- order construction or placement;
- paper/live mode enablement;
- fill, position, reconciliation, or account mutation artifacts;
- execution-owned storage/request/manifest wiring beyond the accepted risk-cap validation boundary.

## Recently Accepted

- Added side-effect-free realtime feature and model-decision input scaffold: `realtime_feature_snapshot_v1`, `execution_model_decision_input_snapshot_v1`, builders, validators, CLIs, and fixture/shadow tests. These prepare direct handoff into historical-model decision routing without activating models.
- Added side-effect-free execution capability catalogs: `execution_realtime_data_interface_v1`, `execution_broker_interface_v1`, and `execution_capability_catalog_v1`.
- Accepted the first interface split: realtime market data may share canonical providers with historical data, but it uses distinct realtime transports; broker mutation is a separate authority from market-data access.
- Accepted OKX as crypto execution venue candidate because an official API exists; live mutation remains disabled.
- Deferred Firstrade equity/options automation because no official trading API is accepted.
- Closed the prior execution-preparation phase in `docs/08_execution_closeout.md`: repository boundary, calendar-discovery ownership, mandatory pre-order `trade_risk_cap` invariant, broker-agnostic risk-cap validator, and package/source/test layout are accepted. No broker adapter, order construction, order placement, fill handling, account mutation, model activation, provider call, or manager dispatch is enabled by this closeout.
- Added component-facing validation entrypoint `scripts/execution/validate_trade_risk_cap.py` and tests. This provides the integration route for unified decision records before any future broker/paper order-construction path.
- Accepted current package/source/test layout and default verification commands: `PYTHONPATH=src python3 -m unittest discover -s tests` and `python3 -m compileall -q src scripts`.
- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.
