# Architecture

## Module Map

| Docs band | Implementation surface | Purpose |
|---|---|---|
| `10_*` | `src/trading_execution/risk_cap/` | Trade risk-cap and execution acceptance boundary. |
| `20_*` | `src/trading_execution/market_data/`, `scripts/execution/` | Realtime market-data observation interfaces. |
| `30_*` | `src/trading_execution/broker/` | Broker interface contracts and non-mutation gates. |
| `40_*` | `src/trading_execution/model_lifecycle.py` | Active/shadow runtime model roster selection. |
| `50_*` | `src/trading_execution/runtime/` | Live/Replay runtime component graph and trading lifecycle contracts. |

## Purpose

This file defines the intended component workflow for `trading-execution`.

## Primary Flow

```text
promotion readiness -> active/shadow model roster -> realtime context snapshot -> execution plan -> safety checks -> paper/live adapter -> orders/fills/positions -> reconcile -> manifest/alert
```

The trading runtime is component-centric, not layer-centric. Training may remain
organized by model layer, but live trading and Replay run the same task-level
component graph:

```text
clock + market adapter + account adapter + frozen model bundle
  -> Account Sleeve Split
  -> C01 Intake
      -> candidate_entry_pool -> C02 Entry
      -> open_position_pool   -> C03 Lifecycle
  -> C04 Option Review
  -> C05 Order Intent
  -> C06 Execution Gate

observed model/trade failure
  -> C07 Failure Review
```

Live mode uses live clock, realtime market data, live account snapshots, and a
broker execution gate. Replay uses historical clock, historical market snapshots,
a simulated account adapter, and a simulated execution/fill adapter. Replay
adapters are side-effect-free: they must not submit broker requests or mutate
account, order, or position state. The components and decision contracts must
remain identical across both modes.

C07 is also part of live operation. It has two timing modes:

- realtime watch during market hours for active and shadow decisions that have
  an open thesis, open position, material path deviation, new event evidence, or
  unexplained model drift;
- settlement attribution after the regular session closes, or in another
  accepted off-hours window, once decision outcomes, fills, open-position state,
  and relevant event evidence have matured.

Realtime watch may surface early failure warnings or preliminary attribution so
the runtime can avoid preventable loss. It does not revise intraday decisions
directly, submit orders, mutate positions, or switch active model pointers. Any
protective reduce, exit, block, or review action must still route through the
normal C03, C05, and C06 decision gates. Settlement attribution produces the
final evidence for model feedback, runtime lifecycle review, and Layer 4/Layer
10 event-family work.

The account adapter exposes two independent sleeves: `crypto_spot_account` and
`equity_options_account`. Runtime components must preserve that split through
intake, entry, lifecycle, option re-expression, and order intent records. C01 is
the runtime intake boundary: it receives model/candidate-policy targets and
current open positions, then emits a `candidate_entry_pool` for C02 and an
`open_position_pool` for C03. C02 and C03 are sibling branches, not a linear
dependency. Crypto candidates are fixed to `BTC`, `ETH`, and `SOL`;
equity/options candidates come from the eligible equity/ETF/optionable-underlying
universe.

## Operating Principles

- Execution is safety-sensitive and must distinguish dry-run, paper, and live behavior.
- Live external actions require explicit safeguards and should not be hidden inside generic tests.
- Execution consumes promotion readiness records; it must not train models or judge offline replay promotion.
- Evaluation-owned Replay calls the execution runtime component graph rather than reimplementing trading decisions.
- Crypto and equity/options use separate account sleeves; execution must not net buying power, collateral, positions, or risk budget across them.
- Execution owns runtime active/shadow roster selection after live/shadow evidence matures.
- Realtime data acquisition for execution is a separate interface layer from historical backfill even when the provider/source is the same.
- Broker/exchange mutation is separate from market-data observation; market-data access must never imply order-placement authority.
- Shared fields, statuses, type values, helpers, and reusable templates must come from `trading-manager`.
- Runtime outputs must be written outside Git-tracked source paths.
- Cross-repository handoffs should use accepted request, artifact, manifest, and ready-signal contracts.

## Collaboration Boundary

`trading-execution` collaborates with other trading repositories through explicit contracts, not direct mutation of their local state.

Upstream inputs and downstream outputs should be described by artifact references, manifests, ready signals, requests, or accepted storage contracts.

`trading-evaluation` owns Replay contracts, datasets, settlement, metrics, and
promotion readiness. It does not own the trading decision logic used during
Replay. The accepted route is:

```text
trading-evaluation replay runner -> trading-execution runtime component graph -> replay decision/fill logs -> trading-evaluation settlement
```

## Current Execution Surface

The active route is a side-effect-controlled runtime surface:

- `execution_realtime_data_interface` records reviewed realtime market-data interfaces for OKX, Alpaca, and ThetaData.
- `execution_broker_interface` records broker/exchange posture for OKX and Firstrade.
- `execution_capability_catalog` combines those catalogs for inspection.
- `execution_realtime_subscription_plan`, `realtime_live_observe_approval`, and `execution_realtime_live_observe_result` support bounded read-only provider observation after explicit approval.
- `realtime_feature_snapshot` and `execution_model_decision_input_snapshot` prepare point-in-time realtime handoff inputs without activating models.
- `execution_shadow_cycle_selection` and `execution_active_model_config_write` record execution-owned active/shadow roster decisions and active-pointer writes.
- `execution_runtime_component_graph` and the runtime decision builders provide the shared live/Replay trading component graph.
- `execution_order_construction_approval` and `execution_broker_order_intent` support approved broker-shaped order-intent construction without submission.

Live broker submission, live fills, live position/account mutation, and reconciliation remain closed until separate reviewed gates exist. Replay remains simulated and side-effect-free.

## Not Current Historical-Training Scope

Execution implementation remains outside the no-broker historical-training run. Historical training must not depend on live broker adapters, paper/live mutation, or execution-owned runtime outputs beyond accepted artifacts and references.
