# Contracts

## Acceptance Summary

`trading-execution` is accepted when it provides a clear, testable component boundary for its role in the trading system.

Acceptance focuses on:

- repository boundary clarity;
- workflow clarity;
- compatibility with `trading-manager` contracts and registry rules;
- compatibility with `trading-storage` where durable artifacts are involved;
- absence of committed generated outputs, logs, notebooks, credentials, and secrets;
- evidence-backed tests once code exists.

## Acceptance Rules

### For Documentation Changes

Documentation changes are acceptable when they:

- update the narrowest authoritative file;
- preserve separation between scope, context, workflow, acceptance, task, decision, and memory;
- route global helper, template, field, status, type, and shared vocabulary changes to `trading-manager`;
- mark unresolved contract and runtime-storage questions as open gaps;
- avoid pretending implementation choices are settled before evidence exists.

### For Implementation Changes

Implementation changes are acceptable only when they:

- stay inside this repository's component boundary;
- avoid committing generated data, artifacts, logs, notebooks, credentials, or secrets;
- include meaningful tests for the behavior introduced;
- avoid external side effects in default tests unless explicitly guarded;
- use accepted contracts for cross-repository handoffs;
- reject missing or invalid `trade_risk_cap` payloads before any order construction, paper execution, live execution, or broker/account mutation;
- keep runtime active/shadow model selection separate from offline promotion eligibility and from broker/account mutation;
- route new shared names through `trading-manager/scripts/`.

## Runtime Component Contracts

The live and Replay runtime component graph is accepted as the execution-owned
decision surface. `trading-evaluation` may orchestrate Replay runs and judge
results, but it must call this graph instead of duplicating trading decisions.

First-batch contracts:

- `execution_intake_snapshot`
- `entry_decision`
- `position_lifecycle_decision`
- `execution_order_intent`
- `execution_gate_result`

Second-batch contracts:

- `option_reexpression_decision`
- `failure_explanation_packet`
- `simulated_fill_event`

Graph/catalog contracts:

- `execution_runtime_component`
- `execution_runtime_component_graph`
- `execution_runtime_component_graph_validation`
- `execution_account_sleeve`

The first-batch contracts are sufficient to build the initial dry-run lifecycle:
intake the runtime candidate and open-position pools, decide entries from the
candidate pool, manage existing positions from the open-position pool, and emit
a broker-neutral order intent plus a C06 execution gate result. They are
implemented as side-effect-free runtime builders and validators in
`trading_execution.runtime`: they may emit decision records for live or Replay,
but they do not call providers, submit broker requests, construct broker-specific
payloads, or mutate account, order, or position state.

The second-batch contracts add option roll review, post-failure Layer 10
explanation, and Replay fill simulation. They are also implemented as
side-effect-free runtime builders and validators. `simulated_fill_event` is
Replay-only evidence and never represents a real broker/account fill or account,
order, or position mutation.

Layer 10 is only called by `component_07_failure_review` after observed model
or trade failure. In live operation, C07 normally runs after the regular session
closes or in another accepted off-hours attribution window. Normal entry
decisions use Layer 4 for forward event risk.

Runtime decisions must be scoped to one independent account sleeve:
`crypto_spot_account` or `equity_options_account`. The crypto sleeve is limited
to `BTC`, `ETH`, and `SOL` spot candidates. The equity/options sleeve uses the
reviewed stock/ETF/optionable-underlying candidate process and owns option
re-expression. Cross-account collateral, buying-power substitution, and position
netting are not accepted.

`execution_intake_snapshot` is C01-owned. It emits a `candidate_entry_pool` for
C02 and an `open_position_pool` for C03. C02 and C03 are sibling branches:
candidate entries do not flow through C03, and existing open positions do not
need C02 re-entry approval before lifecycle review.

`execution_order_intent` is broker-neutral and C05-owned. It must contain the
complete position-management result for the proposed operation: final quantity,
target post-trade position when available, quantity source, sizing reason codes,
broker-neutral price/order policy, target position-scaling capacity evidence,
and a valid `trade_risk_cap`. Missing or invalid sizing/cap evidence produces a
blocked intent and never implies order submission permission.

Target position-scaling capacity is based on target-allocated buying power and
estimated unit/contract cost, and is evaluated in C05 after option-expression
and risk-cap evidence are available. C03 must not inspect option contract cost
or target-level buying-power capacity. If the target allocation can afford fewer
than the minimum advanced-management unit count, C05 records
`single_allocation_no_advanced_scaling` and blocks tactical add/reduce order
intent construction. Protective stops, exits, and risk reductions are still
allowed; capacity rules only suppress tactical scaling.

`execution_gate_result` is C06-owned. It records whether the C05 intent is
rejected, approved for Replay simulation, or approved for live broker submission.
It must verify the order quantity matches the C05 `sizing_plan`, preserve the
broker-neutral order unchanged, apply final hard-block checks, and require an
approved agent final review before live submission. C06 must not recalculate,
increase, reduce, or otherwise alter the C05 quantity or order policy.

`simulated_fill_event` must cite both the source `execution_order_intent` and
the approving `execution_gate_result`. Replay fill simulation is not valid from
an intent alone.

## Verification Commands

Current checks:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests
python3 -m compileall -q src scripts
git diff --check
git status --short
```

Future execution slices must add appropriate lint/type checks, schema validation, and artifact/manifest/ready-signal validation as applicable.

## Required Review Evidence

Every accepted change should provide:

- changed files;
- boundary impact;
- contract impact;
- registry impact;
- storage impact;
- test/verification output;
- confirmation that no generated outputs, logs, notebooks, credentials, or secrets were committed;
- unresolved gaps routed to `docs/04_task.md`.

## Rejection Reasons

A change must be rejected or returned if it:

- takes over another component repository responsibility.
- commits generated outputs, logs, notebooks, or credentials.
- invents shared fields/statuses/types without trading-manager registry review.
- stores secret values.
- writes artifacts to undocumented paths.
- claims acceptance without test or inspection evidence.
- constructs or places an order when `trade_risk_cap` is missing, malformed, unsupported, stale, or impossible to enforce.
- duplicates global contract definitions locally instead of referencing trading-manager.
- treats an offline replay promotion decision as automatic active model switching.
