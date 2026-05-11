# tests

First-party tests for `trading-execution`.

- `test_calendar_discovery_pipeline.py` covers execution-owned future calendar discovery helpers.
- `test_trade_risk_cap.py` and `test_trade_risk_cap_cli.py` cover mandatory pre-order `trade_risk_cap` validation.
- `test_execution_capabilities.py` covers the side-effect-free realtime data and broker capability catalogs, including OKX enabled-for-scaffold/no-mutation posture and Firstrade deferred posture.

Tests must not call live providers, open broker sessions, place orders, or require secrets.
