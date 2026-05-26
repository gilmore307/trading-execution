# Runtime Data Outputs

Status: accepted output charter
Date: 2026-05-26

## Purpose

This document owns the runtime data-output charter for live execution. It
decides which live outputs are SQL tables, which are SQL rows plus storage
artifacts, and which broker/account surfaces remain future-gated.

Realtime execution outputs are not historical training data. They are
point-in-time execution evidence. Downstream evaluation, runtime model lifecycle
review, and model maintenance may consume them through SQL rows and artifact
refs, but realtime capture does not replace the historical backfill pipeline.

## Charter

- Durable queryable runtime state is SQL-backed under `trading_execution`.
- Large payloads use SQL index rows plus artifact refs; SQL owns ids, timing,
  status, digests, and refs, while the artifact owns the full payload.
- Smoke, monitor, live-observe, and capacity receipts use one SQL run row plus
  a receipt artifact when the receipt is too large for the row.
- Runtime status and capability tables use `status_*`.
- Realtime input, snapshot, subscription, and monitor tables use `realtime_*`.
- Component-owned physical SQL tables use `cNN_*` only when the table is the
  component's own decision output.
- Risk, order-construction, and broker-shaped intent rows use `trade_*`.
- Live/shadow results, model effectiveness, attribution, and lifecycle review
  evidence use `performance_*`.
- C08 shadow outputs are evidence only. They do not route orders into C01-C06,
  call brokers, or mutate accounts. C08's component output is the cycle
  selection table; model-group runtime evidence belongs to `performance_*` rows.
- Broker submission, broker order state, fills, account mutation, position
  mutation, and reconciliation are future-gated SQL surfaces until reviewed
  submit/reconcile gates exist.
- Runtime outputs must live outside Git-tracked source paths.

## Current SQL Tables

Runtime status and capability:

- `trading_execution.status_realtime_trading_runtime`
- `trading_execution.status_capability_catalog`
- `trading_execution.status_realtime_data_interface`
- `trading_execution.status_broker_interface`

Realtime capture and model-input handoff:

- `trading_execution.realtime_capture_contract`
- `trading_execution.realtime_feature_snapshot`
- `trading_execution.realtime_model_decision_input_snapshot`
- `trading_execution.realtime_input_coverage`
- `trading_execution.realtime_subscription_plan`
- `trading_execution.realtime_live_observe_result`
- `trading_execution.realtime_monitor_smoke_receipt`
- `trading_execution.realtime_monitor_loop_receipt`

`realtime_feature_snapshot`, `realtime_model_decision_input_snapshot`, and
provider capture payloads are SQL-indexed artifact surfaces when payload is
large. The SQL rows store refs and digests; storage artifacts hold full payloads.

Runtime component decisions:

- `trading_execution.c01_intake_snapshot`
- `trading_execution.c02_entry_decision`
- `trading_execution.c03_position_lifecycle_decision`
- `trading_execution.c04_option_reexpression_decision`
- `trading_execution.c05_order_intent`
- `trading_execution.c06_execution_gate_result`
- `trading_execution.c07_failure_explanation_packet`

Runtime model lifecycle and C08 shadow comparison:

- `trading_execution.c08_shadow_cycle_selection`

`c08_shadow_cycle_selection` is C08's durable component-owned output. It includes
the selected active model, previous active model, stable wingmen, probation
wingman when present, rotating challengers, rollback ref, write window ref, and
active-pointer write audit fields. There is no separate active-config-write SQL
table in the current output model.

C08 capacity simulation is a test artifact, not a durable SQL table. If it must
be retained for a run, keep it as a receipt artifact referenced by the relevant
test or review run.

Order construction and risk-gated intent:

- `trading_execution.trade_risk_cap`
- `trading_execution.trade_order_construction_approval`
- `trading_execution.trade_broker_order_intent`
- `trading_execution.trade_broker_order_intent_result`

Realtime effectiveness and attribution:

- `trading_execution.performance_model_runtime_evidence`
- `trading_execution.performance_model_decision_effectiveness`
- `trading_execution.performance_model_decision_effectiveness_row`
- `trading_execution.performance_c07_failure_attribution`
- `trading_execution.performance_runtime_model_lifecycle_review`

`performance_model_runtime_evidence` stores all active, wingman, challenger,
probation, and shadow-only model groups together. Rows must include model-group
identity fields, including `model_group_ref`, `model_group_role`, and
`model_group_run_ref`, so runtime comparison does not require one table per
role.

## Future-Gated SQL Tables

These table names are reserved for the future live broker/account path. They are
not active runtime outputs until the matching gates exist:

- `trading_execution.broker_order_submission`
- `trading_execution.broker_order_state`
- `trading_execution.broker_fill`
- `trading_execution.account_state_snapshot`
- `trading_execution.position_state_snapshot`
- `trading_execution.execution_reconciliation_result`

## Non-SQL Payloads

Durable non-SQL payloads are storage artifacts, not source files. Accepted
examples include:

- full provider capture payloads;
- complete realtime feature payloads;
- complete model-decision input payloads;
- monitor loop receipts;
- C08 capacity simulation test detail;
- chart-ready runtime review reports.

Every durable artifact must be reachable from a SQL row or accepted storage
manifest. Unindexed runtime files are temporary and must not become the system of
record.
