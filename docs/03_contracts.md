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
