# market_data

Execution-side realtime market-data interface catalog.

This package records reviewed realtime interfaces for providers that may also exist as historical data sources. It does not open sockets, call providers, write runtime data, or define historical source-of-truth behavior.

Key file:

- `contracts.py` — `execution_realtime_data_interface` catalog for OKX, Alpaca, and ThetaData realtime routes; `execution_realtime_input_coverage` coverage rows for Layers 1-10; and `realtime_capture_contract` for append-only forward/shadow validation evidence.
- `adapters.py` — `execution_realtime_subscription_plan` planning helpers for dry-run, fixture-replay, and approval-blocked live-observe routes.
- `live_observe.py` — concrete Alpaca/ThetaData/OKX/calendar/account/model-context fixture adapter plans, capture-fixture rows, and realtime shadow fixture bundles.
- `live_approval.py` — `realtime_live_observe_approval` validation for bounded formal read-only provider observation.
- `live_provider.py` — approved read-only provider observation path for OKX/Alpaca/ThetaData HTTP probes; still no model activation, order construction, or account mutation.
- `capture.py` — `realtime_capture_validation` checks for candidate append-only capture rows.
- `features.py` — `realtime_feature_snapshot` and `execution_model_decision_input_snapshot` builders/validators that bridge realtime capture refs into historical-model decision input envelopes without activating models or opening streams.
- `realtime_monitor.py` — execution-owned bounded ETF live-observe smoke/loop runners, cycle summaries, and credential-free receipts for the realtime monitoring runtime slice.
- `effectiveness.py` — lightweight `realtime_model_decision_effectiveness` aggregation for matured shadow/live decision outcomes; creates no historical dataset rows and performs no activation, persistence, broker call, or account mutation.
