# trading-execution

`trading-execution` is the live and paper execution runtime repository for the trading system.

It consumes externally promoted decisions and owns broker/exchange execution workflows, orders, positions, reconciliation, execution artifacts, and safety controls.

It does not own component responsibilities outside that boundary, global contracts, shared registry authority, generated runtime artifacts committed to Git, or secrets.

## Top-Level Structure

```text
docs/        Repository scope, context, workflow, acceptance, task, decisions, and local memory.
src/         Importable execution-runtime implementation code.
tests/       First-party tests.
```

`src/` owns importable/reusable code. Add `scripts/` only for executable maintenance or operational entrypoints; `scripts/` may import `src/`, but `src/` must not import `scripts/`.

## Docs Spine

```text
docs/
  00_scope.md
  01_context.md
  02_workflow.md
  03_acceptance.md
  04_task.md
  05_decision.md
  06_memory.md
```

## Current Implementation

- `trading_execution.calendar_discovery` owns future macro release-calendar discovery and explicitly approved market calendars such as Nasdaq earnings dates for live/realtime acquisition scheduling. Historical macro values and source evidence remain in `trading-data`.

## Platform Dependencies

- `trading-main` owns global contracts, registry, shared helpers, templates, and platform guidance.
- `trading-storage` owns durable storage layout and retention unless this repository is `trading-storage` itself.
- `trading-main` owns control-plane orchestration and lifecycle routing.

Any new global helper, reusable template, shared field, status, type, config key, or vocabulary discovered here must be routed back to `trading-main` before other repositories depend on it.
