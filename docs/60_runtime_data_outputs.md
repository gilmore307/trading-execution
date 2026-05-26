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
- Component-owned physical SQL tables use `cNN` prefixes. Runtime lifecycle and
  data-ingestion surfaces that are not owned by one component do not invent a
  fake component number.
- C08 shadow outputs are evidence only. They do not route orders into C01-C06,
  write active pointers, call brokers, or mutate accounts.
- Broker submission, broker order state, fills, account mutation, position
  mutation, and reconciliation are future-gated SQL surfaces until reviewed
  submit/reconcile gates exist.
- Runtime outputs must live outside Git-tracked source paths.

## Current SQL Tables

Runtime status and capability:

- `trading_execution.execution_realtime_trading_runtime_status`
- `trading_execution.execution_capability_catalog`
- `trading_execution.execution_realtime_data_interface`
- `trading_execution.execution_broker_interface`

Realtime capture and model-input handoff:

- `trading_execution.realtime_capture_contract`
- `trading_execution.realtime_feature_snapshot`
- `trading_execution.execution_model_decision_input_snapshot`
- `trading_execution.execution_realtime_input_coverage`
- `trading_execution.execution_realtime_subscription_plan`
- `trading_execution.execution_realtime_live_observe_result`
- `trading_execution.execution_realtime_monitor_smoke_receipt`
- `trading_execution.execution_realtime_monitor_loop_receipt`

`realtime_feature_snapshot`, `execution_model_decision_input_snapshot`, and
provider capture payloads are SQL-indexed artifact surfaces when the payload is
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

- `trading_execution.c08_shadow_model_runtime_evidence`
- `trading_execution.c08_shadow_cycle_selection`
- `trading_execution.c08_capacity_simulation`
- `trading_execution.execution_active_model_config_write`

Order construction and risk-gated intent:

- `trading_execution.trade_risk_cap`
- `trading_execution.execution_order_construction_approval`
- `trading_execution.execution_broker_order_intent`
- `trading_execution.execution_broker_order_intent_result`

Realtime effectiveness and attribution:

- `trading_execution.realtime_model_decision_effectiveness`
- `trading_execution.realtime_model_decision_effectiveness_row`
- `trading_execution.c07_failure_attribution`
- `trading_execution.runtime_model_lifecycle_review_result`

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
- C08 capacity simulation detail;
- chart-ready runtime review reports.

Every durable artifact must be reachable from a SQL row or accepted storage
manifest. Unindexed runtime files are temporary and must not become the system of
record.
