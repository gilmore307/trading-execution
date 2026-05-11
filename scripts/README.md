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
