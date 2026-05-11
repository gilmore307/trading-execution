# execution scripts

Executable component helpers for execution-runtime inspection and validation.

- `list_execution_capabilities.py` prints the reviewed capability catalog without external calls or mutation.
- `plan_realtime_capture.py` builds side-effect-free realtime subscription/capture plans; it opens no provider streams.
- `plan_live_observe_adapters.py` builds concrete provider/account/event live-observe adapter fixture plans without opening streams or resolving secrets.
- `validate_realtime_capture.py` validates a candidate `realtime_capture_contract_v1` row without persistence or mutation.
- `build_realtime_feature_snapshot.py` builds a `realtime_feature_snapshot_v1` envelope from realtime capture refs and historical model/config refs.
- `build_realtime_model_input.py` builds an `execution_model_decision_input_snapshot_v1` handoff envelope for fixture/shadow historical-model decision routing.
- `build_realtime_shadow_fixture.py` builds the execution-side adapter/capture/feature/model-input shadow fixture bundle for the full realtime handoff path.
- `validate_realtime_model_input.py` validates realtime feature or model decision input snapshots without activating models.
- `validate_trade_risk_cap.py` validates a proposed decision record before any future order construction path.

Scripts may import `src/trading_execution`; `src/` must not import scripts.
