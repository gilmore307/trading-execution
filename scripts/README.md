# scripts

Executable execution-runtime helpers live here.

## Capability inspection

- `execution/list_execution_capabilities.py` prints the side-effect-free execution capability catalog: realtime data interfaces, broker interfaces, and mutation-disabled status.

## Risk-cap validation

- `execution/validate_trade_risk_cap.py` validates a proposed unified decision record before broker-order intent construction.

Example:

```bash
PYTHONPATH=src python3 scripts/execution/list_execution_capabilities.py
PYTHONPATH=src python3 scripts/execution/validate_trade_risk_cap.py decision_record.json
```

The command exits non-zero when the `trade_risk_cap` is missing or invalid. It does not create orders, mutate broker state, or call external providers.

## Runtime model lifecycle

- `execution/build_shadow_cycle_selection.py` builds a `c08_shadow_cycle_selection` record from ranked market-hours active/shadow review rows. The record recommends active, realtime-candidate, shadow-only, and eliminate-candidate roster roles and carries active-pointer audit fields without constructing orders, calling brokers, or mutating accounts.
- `execution/simulate_c08_capacity.py` estimates how many realtime model groups C08 can admit under CPU, memory, and p95 latency budgets. It is side-effect-free, test-only evidence and assumes historical model tasks are paused during live runtime.
- `execution/build_broker_order_intent.py` constructs an approved OKX broker-order intent after risk-cap validation. It does not submit broker requests or mutate accounts.
