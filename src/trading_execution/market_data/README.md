# market_data

Execution-side realtime market-data interface catalog.

This package records reviewed realtime interfaces for providers that may also exist as historical data sources. It does not open sockets, call providers, write runtime data, or define historical source-of-truth behavior.

Key file:

- `contracts.py` — `execution_realtime_data_interface_v1` catalog for OKX, Alpaca, and ThetaData realtime routes; `execution_realtime_input_coverage_v1` coverage rows for Layers 1-8; and `realtime_capture_contract_v1` for append-only forward/shadow validation evidence.
- `adapters.py` — `execution_realtime_subscription_plan_v1` planning helpers for dry-run, fixture-replay, and approval-blocked live-observe routes.
- `live_observe.py` — concrete Alpaca/ThetaData/OKX/calendar/account/model-context fixture adapter plans, capture-fixture rows, and realtime shadow fixture bundles; still no provider calls or mutations.
- `capture.py` — `realtime_capture_validation_v1` checks for candidate append-only capture rows.
- `features.py` — `realtime_feature_snapshot_v1` and `execution_model_decision_input_snapshot_v1` builders/validators that bridge realtime capture refs into historical-model decision input envelopes without activating models or opening streams.
