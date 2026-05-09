# Task

## Active Tasks

- None.

## Queued Tasks

- Build the first broker/order-construction slice on top of the accepted `trade_risk_cap` pre-order validation invariant.
- Identify any future global fields, helper surfaces, templates, or type values that must be registered in `trading-manager`.

## Open Gaps

- Exact broker/order-construction implementation slice after the risk-cap validator.
- Exact artifact/manifest/ready-signal/request contract interactions.
- Exact storage path/reference requirements.

## Recently Accepted

- Added component-facing validation entrypoint `scripts/execution/validate_trade_risk_cap.py` and tests. This provides the integration route for unified decision records before any future broker/paper order-construction path.
- Accepted current package/source/test layout and default verification commands: `PYTHONPATH=src python3 -m unittest discover -s tests` and `python3 -m compileall -q src scripts`.
- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.
