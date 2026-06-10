# execution scripts

Executable component helpers for execution-runtime inspection and validation.

Scripts may import `src/trading_execution`; `src/` must not import scripts.

- `list_execution_capabilities.py` prints the reviewed capability catalog without external calls or mutation.
- `plan_realtime_capture.py` builds side-effect-free realtime subscription/capture plans; it opens no provider streams.
- `plan_live_observe_adapters.py` builds concrete provider/account/event live-observe adapter fixture plans without opening streams or resolving secrets.
- `execute_live_observe.py` executes reviewed read-only realtime provider observation only when given `realtime_live_observe_approval` plus `--execute-live-observe`; it still forbids model activation, order construction, and account mutation.
- `validate_realtime_capture.py` validates a candidate `realtime_capture_contract` row without persistence or mutation.
- `build_realtime_feature_snapshot.py` builds a `realtime_feature_snapshot` envelope from realtime capture refs and historical model/config refs.
- `build_realtime_model_input.py` builds a C-runtime-component-routed `realtime_model_decision_input_snapshot` handoff envelope for fixture/shadow historical-model decision routing.
- `build_realtime_shadow_fixture.py` builds the execution-side adapter/capture/feature/model-input shadow fixture bundle for the full realtime handoff path.
- `validate_realtime_model_input.py` validates realtime feature or model decision input snapshots without activating models.
- `validate_trade_risk_cap.py` validates a proposed decision record before broker-order intent construction.
- `build_broker_order_intent.py` constructs an approved OKX broker order intent after `trade_risk_cap` validation; it does not submit broker requests or mutate accounts.
- `run_realtime_monitor_smoke.py` runs the execution-owned read-only Alpaca ETF realtime monitor smoke and writes a summary receipt; it requires `--execute-live-observe` for provider calls, defaults the observed universe to Layer 1/2 ETF rows, emits downstream C-runtime component handoff refs, and performs no model activation, order construction, broker submission, or account mutation.
- `run_realtime_monitor_loop.py` runs repeated realtime monitor smoke cycles, writes per-cycle receipts plus `loop_receipt.json`, and keeps model activation/order/broker/account mutation disabled. `--universe-model-layer` controls universe CSV filtering; `--model-layer` controls feature snapshot coverage, while the downstream decision handoff is C-runtime-component routed.
- `run_realtime_trading_runtime_check.py` builds the live-trading runtime readiness status. It waits when no promoted active model pointer exists, separates implemented capabilities from currently connected runtime input refs, and performs no provider, model, broker, order-submit, or account mutation work.
- `aggregate_realtime_decision_effectiveness.py` aggregates matured realtime/shadow decision records into `performance_model_decision_effectiveness` without creating historical dataset rows, activating models, persisting state, or mutating broker/account state.
- `build_shadow_cycle_selection.py` builds `c08_shadow_cycle_selection` from ranked active/shadow cycle review rows; it recommends active, realtime-candidate, shadow-only, and eliminate-candidate roles and carries active-pointer audit fields without mutating broker/account state.
- `simulate_c08_capacity.py` estimates how many realtime model groups C08 can run without degrading C01-C06 latency, market-data ingestion, broker gates, or account-state freshness. It is test-only evidence and performs no provider calls, model activation, broker calls, or account mutation.
