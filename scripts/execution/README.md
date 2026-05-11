# execution scripts

Executable component helpers for execution-runtime inspection and validation.

- `list_execution_capabilities.py` prints the reviewed capability catalog without external calls or mutation.
- `plan_realtime_capture.py` builds side-effect-free realtime subscription/capture plans; it opens no provider streams.
- `plan_live_observe_adapters.py` builds concrete provider/account/event live-observe adapter fixture plans without opening streams or resolving secrets.
- `execute_live_observe.py` executes reviewed read-only realtime provider observation only when given `realtime_live_observe_approval_v1` plus `--execute-live-observe`; it still forbids model activation, order construction, and account mutation.
- `validate_realtime_capture.py` validates a candidate `realtime_capture_contract_v1` row without persistence or mutation.
- `build_realtime_feature_snapshot.py` builds a `realtime_feature_snapshot_v1` envelope from realtime capture refs and historical model/config refs.
- `build_realtime_model_input.py` builds an `execution_model_decision_input_snapshot_v1` handoff envelope for fixture/shadow historical-model decision routing.
- `build_realtime_shadow_fixture.py` builds the execution-side adapter/capture/feature/model-input shadow fixture bundle for the full realtime handoff path.
- `validate_realtime_model_input.py` validates realtime feature or model decision input snapshots without activating models.
- `validate_trade_risk_cap.py` validates a proposed decision record before any future order construction path.
- `build_broker_order_intent.py` constructs an approved OKX broker order intent after `trade_risk_cap` validation; it does not submit the order or mutate accounts.

Scripts may import `src/trading_execution`; `src/` must not import scripts.
- `run_realtime_monitor_smoke.py` runs the execution-owned read-only Alpaca ETF realtime monitor smoke and writes a summary receipt; it requires `--execute-live-observe` for provider calls and performs no model activation, order construction, broker submission, or account mutation.
