# Task

## Active Tasks

- None for the historical-data training preparation boundary.

`trading-execution` is intentionally inactive while the current work is historical data acquisition, offline feature/model training, and evidence generation. The accepted `trade_risk_cap` validator remains available for future execution-facing decision records but must not be treated as permission to construct or place orders.

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

- Closed the current execution-preparation phase in `docs/08_execution_closeout.md`: repository boundary, calendar-discovery ownership, mandatory pre-order `trade_risk_cap` invariant, broker-agnostic risk-cap validator, and package/source/test layout are accepted. No broker adapter, order construction, order placement, fill handling, account mutation, model activation, provider call, or manager dispatch is enabled by this closeout.
- Added component-facing validation entrypoint `scripts/execution/validate_trade_risk_cap.py` and tests. This provides the integration route for unified decision records before any future broker/paper order-construction path.
- Accepted current package/source/test layout and default verification commands: `PYTHONPATH=src python3 -m unittest discover -s tests` and `python3 -m compileall -q src scripts`.
- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.
