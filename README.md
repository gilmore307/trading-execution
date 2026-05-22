# trading-execution

`trading-execution` is the live and paper execution runtime repository for the trading system.

It consumes externally promoted/readiness decisions and owns runtime model selection, broker/exchange execution workflows, orders, positions, reconciliation, execution artifacts, and safety controls.

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
  02_architecture.md
  03_contracts.md
  04_task.md
  05_decision.md
  06_memory.md
  10_trade_risk_cap.md
  11_execution_acceptance.md
  20_realtime_data.md
  30_broker_interfaces.md
  40_runtime_model_lifecycle.md
  50_runtime_components.md
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
- `trading_execution.model_lifecycle` owns the execution-side post-shadow-cycle model roster selection contract. It records active, realtime-candidate, shadow-only, and eliminate-candidate recommendations without writing active pointers, constructing orders, submitting broker calls, or mutating accounts.
- `trading_execution.runtime.components` owns the live/Replay shared runtime component graph. The graph is task-centric: opportunity/risk allocation, entry, position lifecycle, option re-expression, failure explanation, order intent, and execution gate/adapters. It also defines independent `crypto_spot_account` and `equity_options_account` sleeves; crypto starts from the fixed `BTC`/`ETH`/`SOL` candidate pool.
- `trading_execution.broker` owns the side-effect-free broker/exchange interface catalog. OKX is accepted for crypto adapter scaffolding with live mutation disabled; Firstrade equity/options execution is deferred because no official trading API is accepted.
- `docs/11_execution_acceptance.md` records the prior execution-preparation closeout; `docs/20_realtime_data.md` and `docs/30_broker_interfaces.md` open the next execution design slice without enabling broker adapters, order construction, order placement, fills, positions, or account mutation.

## Platform Dependencies

- `trading-manager` owns global contracts, registry, shared helpers, templates, and platform guidance.
- `trading-storage` owns durable storage layout and retention unless this repository is `trading-storage` itself.
- `trading-manager` owns control-plane orchestration and lifecycle routing.

Any new global helper, reusable template, shared field, status, type, config key, or vocabulary discovered here must be routed back to `trading-manager` before other repositories depend on it.

### Nasdaq EPS baseline snapshots

`calendar_discovery` can emit a side-effect-free `saved/earnings_guidance_expectation_baseline.csv` when manager-prepared future Nasdaq earnings-calendar tasks set `baseline_capture_mode = future_pre_event_eps_consensus_snapshot`. The output uses pre-event `epsForecast` rows only and excludes actual EPS / surprise fields.
