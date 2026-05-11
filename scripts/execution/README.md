# execution scripts

Executable component helpers for execution-runtime inspection and validation.

- `list_execution_capabilities.py` prints the reviewed capability catalog without external calls or mutation.
- `validate_trade_risk_cap.py` validates a proposed decision record before any future order construction path.

Scripts may import `src/trading_execution`; `src/` must not import scripts.
