# trading-execution

`trading-execution` is the live and paper execution runtime repository for the trading system.

It consumes externally promoted decisions and owns broker/exchange execution workflows, orders, positions, reconciliation, execution artifacts, and safety controls.

It does not own component responsibilities outside that boundary, global contracts, shared registry authority, generated runtime artifacts committed to Git, or secrets.

## Top-Level Structure

```text
docs/        Repository scope, context, workflow, acceptance, task, decisions, and local memory.
scripts/     Executable execution-runtime validation helpers.
src/         Importable execution-runtime implementation code.
tests/       First-party tests.
```

`src/` owns importable/reusable code. `scripts/` owns executable maintenance or operational entrypoints; `scripts/` may import `src/`, but `src/` must not import `scripts/`.

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
  07_trade_risk_cap.md
  08_execution_closeout.md
  09_realtime_data.md
  10_broker_interfaces.md
```

## Verification

```bash
PYTHONPATH=src python3 -m unittest discover -s tests
python3 -m compileall -q src scripts
```

## Current Implementation

- `trading_execution.calendar_discovery` owns future macro release-calendar discovery and explicitly approved market calendars such as Nasdaq earnings dates for live/realtime acquisition scheduling. Historical macro values and source evidence remain in `trading-data`.
- `trading_execution.risk_cap` owns broker-agnostic pre-order validation for mandatory `trade_risk_cap` payloads. Missing or invalid caps force order rejection before paper/live mutation.
- `trading_execution.market_data` owns the side-effect-free realtime data interface catalog, model input coverage matrix, adapter subscription planner, concrete live-observe fixture adapter plans, capture fixture builder, capture validator, realtime feature snapshot builder, and model-decision input handoff envelope. Realtime acquisition may use the same canonical data providers as historical backfill, but through distinct realtime transports such as WebSocket streams or realtime HTTP snapshots.
- `trading_execution.broker` owns the side-effect-free broker/exchange interface catalog. OKX is accepted for crypto adapter scaffolding with live mutation disabled; Firstrade equity/options execution is deferred because no official trading API is accepted.
- `docs/08_execution_closeout.md` records the prior execution-preparation closeout; `docs/09_realtime_data.md` and `docs/10_broker_interfaces.md` open the next execution design slice without enabling broker adapters, order construction, order placement, fills, positions, or account mutation.

## Platform Dependencies

- `trading-manager` owns global contracts, registry, shared helpers, templates, and platform guidance.
- `trading-storage` owns durable storage layout and retention unless this repository is `trading-storage` itself.
- `trading-manager` owns control-plane orchestration and lifecycle routing.

Any new global helper, reusable template, shared field, status, type, config key, or vocabulary discovered here must be routed back to `trading-manager` before other repositories depend on it.

### Nasdaq EPS baseline snapshots

`calendar_discovery` can emit a side-effect-free `saved/earnings_guidance_expectation_baseline.csv` when manager-prepared future Nasdaq earnings-calendar tasks set `baseline_capture_mode = future_pre_event_eps_consensus_snapshot`. The output uses pre-event `epsForecast` rows only and excludes actual EPS / surprise fields.
