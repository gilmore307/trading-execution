# execution scripts

Executable component helpers for execution-runtime inspection and validation.

- `list_execution_capabilities.py` prints the reviewed capability catalog without external calls or mutation.
- `plan_realtime_capture.py` builds side-effect-free realtime subscription/capture plans; it opens no provider streams.
- `validate_realtime_capture.py` validates a candidate `realtime_capture_contract_v1` row without persistence or mutation.
- `validate_trade_risk_cap.py` validates a proposed decision record before any future order construction path.

Scripts may import `src/trading_execution`; `src/` must not import scripts.
