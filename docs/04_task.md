# Task

## Active Tasks

- None.

## Queued Tasks

- Build the first broker/order-construction slice on top of the accepted `trade_risk_cap` pre-order validation invariant.
- Define broader package/source/test layout after the broker/order-construction slice is accepted.
- Define fixture policy and default test commands.
- Identify any global fields, helper surfaces, templates, or type values that must be registered in `trading-manager`.

## Open Gaps

- Exact broker/order-construction implementation slice after the risk-cap validator.
- Exact broader source/package layout.
- Exact fixture and test policy.
- Exact artifact/manifest/ready-signal/request contract interactions.
- Exact storage path/reference requirements.

## Recently Accepted

- Added accepted `trade_risk_cap` pre-order invariant and broker-agnostic validator. Missing or invalid caps force `reject_order` before order construction/placement.
- Created initial `trading-execution` docs spine and repository boundary.
- Added initial `.gitignore` for local environments, generated outputs, logs, and secrets.
