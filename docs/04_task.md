# Task

## Active Tasks

- None.

## Queued Tasks

- None for the current execution-preparation closeout phase.

## Deferred Beyond Current Closeout

- First broker/order-construction slice on top of the accepted `trade_risk_cap` pre-order validation invariant.
- Paper-trading and live-trading mode boundaries with explicit approval and audit evidence.
- Order/fill/position/reconciliation artifact contracts.
- Exact artifact/manifest/ready-signal/request contract interactions.
- Exact storage path/reference requirements.
- Any future global fields, helper surfaces, templates, or type values that must be registered in `trading-manager`.

These are execution production-phase tasks, not blockers for this closeout.

## Recently Accepted

- Closed the current execution-preparation phase in `docs/08_execution_closeout.md`: repository boundary, calendar-discovery ownership, mandatory pre-order `trade_risk_cap` invariant, broker-agnostic risk-cap validator, and package/source/test layout are accepted. No broker adapter, order construction, order placement, fill handling, account mutation, model activation, provider call, or manager dispatch is enabled by this closeout.

- Added component-facing validation entrypoint `scripts/execution/validate_trade_risk_cap.py` and tests. This provides the integration route for unified decision records before any future broker/paper order-construction path.
- Accepted current package/source/test layout and default verification commands: `PYTHONPATH=src python3 -m unittest discover -s tests` and `python3 -m compileall -q src scripts`.
- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.
