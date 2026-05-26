# broker

Execution-side broker/exchange interface catalog.

This package records broker posture without constructing orders, calling broker APIs, opening account streams, or mutating account state.

Key file:

- `contracts.py` — `status_broker_interface` catalog for OKX and Firstrade plus combined `status_capability_catalog`.
- `order_construction.py` — approval-gated OKX broker-order intent construction without broker submission.

Current boundary:

- OKX crypto adapter scaffolding is allowed, but live order mutation is disabled.
- Firstrade equity/options automation is deferred because no official trading API is accepted.
- Broker-order intents are constructed only after `trade_order_construction_approval` and `trade_risk_cap` validation.
