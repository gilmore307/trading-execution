# Docs

This directory is the authoritative documentation spine for `trading-execution`.

## Files

- `00_scope.md` — repository boundary, in-scope work, out-of-scope work, and owner intent.
- `01_context.md` — why the repository exists, related systems, environment assumptions, and dependencies.
- `02_architecture.md` — component workflow, handoffs, and operating sequence.
- `03_contracts.md` — acceptance gates, verification commands, evidence requirements, and rejection reasons.
- `04_task.md` — active task state, blocked work, and current accepted surfaces.
- `05_decision.md` — ratified repository decisions.
- `06_memory.md` — durable local continuity that does not fit narrower docs.
- `10_trade_risk_cap.md` — mandatory pre-order hard risk-cap invariant and validation surface.
- `11_execution_acceptance.md` — prior execution-preparation phase closeout receipt.
- `20_realtime_data.md` — realtime market-data interface boundary for execution.
- `30_broker_interfaces.md` — broker/exchange interface posture for OKX and Firstrade.
- `40_runtime_model_lifecycle.md` — runtime active/shadow model lifecycle and selection boundary.
- `50_runtime_components.md` — live/Replay shared runtime component graph and decision contracts.

Do not place generated data, artifacts, notebooks, logs, credentials, or implementation outputs in this directory.
