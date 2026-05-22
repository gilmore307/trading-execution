# tests

First-party tests for `trading-execution`.

- `test_calendar_discovery_pipeline.py` covers execution-owned future calendar discovery helpers, Nasdaq earnings-calendar parsing, future pre-event Nasdaq EPS baseline output, and rejection of baseline rows contaminated by actual EPS or surprise fields.
- `test_trade_risk_cap.py` and `test_trade_risk_cap_cli.py` cover mandatory pre-order `trade_risk_cap` validation.
- `test_execution_capabilities.py` covers the side-effect-free realtime data and broker capability catalogs, including OKX enabled-for-scaffold/no-mutation posture and Firstrade deferred posture.
- `test_realtime_market_data_scaffold.py` covers realtime subscription planning, concrete Alpaca/ThetaData/OKX/calendar/account/model-context live-observe fixture planning, live-observe blocking, execution-account placeholder routing, capture validation, realtime feature/model-decision input handoff validation, and CLI smoke checks.
- `test_order_construction.py` covers approved broker-order intent construction without broker submission.
- `test_model_lifecycle.py` and `test_realtime_trading_runtime.py` cover shadow-cycle selection, active-pointer write records, and realtime runtime readiness status.
- `test_runtime_components.py` and `test_runtime_decisions.py` cover the shared live/Replay runtime component graph, account sleeves, decision builders, order intents, and Replay-only simulated fills.

Tests must not call live providers, open broker sessions, place orders, or require secrets.
