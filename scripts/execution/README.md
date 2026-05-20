# execution scripts

Executable component helpers for execution-runtime inspection and validation.

- `list_execution_capabilities.py` prints the reviewed capability catalog without external calls or mutation.
- `plan_realtime_capture.py` builds side-effect-free realtime subscription/capture plans; it opens no provider streams.
- `plan_live_observe_adapters.py` builds concrete provider/account/event live-observe adapter fixture plans without opening streams or resolving secrets.
- `execute_live_observe.py` executes reviewed read-only realtime provider observation only when given `realtime_live_observe_approval` plus `--execute-live-observe`; it still forbids model activation, order construction, and account mutation.
- `validate_realtime_capture.py` validates a candidate `realtime_capture_contract` row without persistence or mutation.
- `build_realtime_feature_snapshot.py` builds a `realtime_feature_snapshot` envelope from realtime capture refs and historical model/config refs.
- `build_realtime_model_input.py` builds an `execution_model_decision_input_snapshot` handoff envelope for fixture/shadow historical-model decision routing.
- `build_realtime_shadow_fixture.py` builds the execution-side adapter/capture/feature/model-input shadow fixture bundle for the full realtime handoff path.
- `validate_realtime_model_input.py` validates realtime feature or model decision input snapshots without activating models.
- `validate_trade_risk_cap.py` validates a proposed decision record before any future order construction path.
- `build_broker_order_intent.py` constructs an approved OKX broker order intent after `trade_risk_cap` validation; it does not submit the order or mutate accounts.

Scripts may import `src/trading_execution`; `src/` must not import scripts.
- `run_realtime_monitor_smoke.py` runs the execution-owned read-only Alpaca ETF realtime monitor smoke and writes a summary receipt; it requires `--execute-live-observe` for provider calls, defaults the observed universe to Layer 1/2 ETF rows, defaults the downstream handoff envelope to complete Layer 1-9 coverage, and performs no model activation, order construction, broker submission, or account mutation.
- `run_realtime_monitor_loop.py` runs repeated realtime monitor smoke cycles, writes per-cycle receipts plus `loop_receipt.json`, and keeps model activation/order/broker/account mutation disabled. `--universe-model-layer` controls universe CSV filtering; `--model-layer` controls downstream handoff coverage.
- `run_realtime_trading_runtime_check.py` builds the live-trading runtime readiness status. It waits when no promoted active model pointer exists, reports which interfaces are connected, and performs no provider, model, broker, order-submit, or account mutation work.
- `aggregate_realtime_decision_effectiveness.py` aggregates matured realtime/shadow decision records into `realtime_model_decision_effectiveness` without creating historical dataset rows, activating models, persisting state, or mutating broker/account state.
- `build_shadow_cycle_selection.py` builds `execution_shadow_cycle_selection` from ranked active/shadow cycle review rows; it recommends active, realtime-candidate, shadow-only, and eliminate-candidate roles without writing active pointers or mutating broker/account state.
- `build_active_model_config_write.py` builds `execution_active_model_config_write` from a valid shadow-cycle selection; it records the active pointer write and rollback ref without mutating broker/account state.
