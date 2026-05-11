# broker

Execution-side broker/exchange interface catalog.

This package records broker posture without constructing orders, calling broker APIs, opening account streams, or mutating account state.

Key file:

- `contracts.py` — `execution_broker_interface_v1` catalog for OKX and Firstrade plus combined `execution_capability_catalog_v1`.

Current boundary:

- OKX crypto adapter scaffolding is allowed, but live order mutation is disabled.
- Firstrade equity/options automation is deferred because no official trading API is accepted.
