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

- `target_allocation_snapshot`
- `entry_decision`
- `position_lifecycle_decision`
- `execution_order_intent`

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
select target/risk, decide entry, manage an existing position, and emit a
broker-neutral order intent. The second-batch contracts add option roll review,
post-failure Layer 10 explanation, and Replay fill simulation.

Layer 10 is only called by `failure_explanation_component` after observed model
or trade failure. Normal entry decisions use Layer 4 for forward event risk.

Runtime decisions must be scoped to one independent account sleeve:
`crypto_spot_account` or `equity_options_account`. The crypto sleeve is limited
to `BTC`, `ETH`, and `SOL` spot candidates. The equity/options sleeve uses the
reviewed stock/ETF/optionable-underlying candidate process and owns option
re-expression. Cross-account collateral, buying-power substitution, and position
netting are not accepted.

## Verification Commands

Current checks:

```bash
PYTHONPATH=src python3 -m unittest discover tests
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
