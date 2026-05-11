# market_data

Execution-side realtime market-data interface catalog.

This package records reviewed realtime interfaces for providers that may also exist as historical data sources. It does not open sockets, call providers, write runtime data, or define historical source-of-truth behavior.

Key file:

- `contracts.py` — `execution_realtime_data_interface_v1` catalog for OKX, Alpaca, and ThetaData realtime routes; `execution_realtime_input_coverage_v1` coverage rows for Layers 1-8; and `realtime_capture_contract_v1` for append-only forward/shadow validation evidence.
