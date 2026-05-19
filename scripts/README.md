# scripts

Executable execution-runtime helpers live here.

## Capability inspection

- `execution/list_execution_capabilities.py` prints the side-effect-free execution capability catalog: realtime data interfaces, broker interfaces, and mutation-disabled status.

## Risk-cap validation

- `execution/validate_trade_risk_cap.py` validates a proposed unified decision record before any broker/paper order construction.

Example:

```bash
PYTHONPATH=src python3 scripts/execution/list_execution_capabilities.py
PYTHONPATH=src python3 scripts/execution/validate_trade_risk_cap.py decision_record.json
```

The command exits non-zero when the `trade_risk_cap` is missing or invalid. It does not create orders, mutate broker state, or call external providers.

## Runtime model lifecycle

- `execution/build_shadow_cycle_selection.py` builds an `execution_shadow_cycle_selection` record from ranked market-hours active/shadow review rows. The record recommends active, realtime-candidate, shadow-only, and eliminate-candidate roster roles without writing active pointers, constructing orders, calling brokers, or mutating accounts.
- `execution/build_active_model_config_write.py` builds an `execution_active_model_config_write` record from a valid shadow-cycle selection. It records the active pointer write, expected previous active ref, new active config ref, rollback ref, and write window without constructing orders, calling brokers, or mutating accounts.
