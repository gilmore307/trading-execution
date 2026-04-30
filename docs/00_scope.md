# Scope

## Purpose

`trading-execution` is the live and paper execution runtime repository for the trading system.

It consumes externally promoted decisions and owns broker/exchange execution workflows, orders, positions, reconciliation, execution artifacts, and safety controls.

This repository exists to keep that responsibility explicit, testable, and separate from neighboring trading repositories.

## In Scope

- paper/live execution runtime code once approved.
- broker/exchange adapter boundaries.
- order, fill, position, reconciliation, and execution artifact handling.
- execution safety checks and dry-run/paper/live mode boundaries.
- execution-local tests and simulation fixtures.

## Out of Scope

- strategy selection or model training.
- market data fetching as source of truth.
- cross-repository promotion decisions.
- dashboard rendering.
- shared storage policy.
- secrets, API keys, or brokerage credentials in Git.
- Defining global artifact, manifest, ready-signal, request, field, status, or type contracts outside `trading-manager`.
- Storing generated data, artifacts, logs, notebooks, credentials, or secrets in Git.

## Owner Intent

`trading-execution` should become a disciplined component repository with clear contracts, evidence-backed acceptance, and no hidden ownership drift.

The repository should prefer explicit interfaces, fixture-backed tests, and narrow responsibility boundaries over quick scripts that blur component roles.

## Boundary Rules

- Component-local implementation belongs here only when it matches this repository's role.
- Global contracts, registry entries, shared helpers, and reusable templates belong in `trading-manager`.
- Durable storage layout and retention belong in `trading-storage` unless this repository is defining that storage contract.
- Scheduling, retries, lifecycle routing, and promotion decisions belong in the `trading-manager` control plane unless explicitly delegated by contract.
- Generated artifacts and runtime outputs are not source files.
- Secrets and credentials must stay outside the repository.
- Shared helpers, templates, fields, statuses, and type values discovered here must be recorded through `trading-manager` before cross-repository use.

## Out-of-Scope Signals

A request should be rejected or re-scoped if it asks `trading-execution` to:

- take over another component repository responsibility.
- commit generated runtime outputs or secrets.
- define global contracts without routing them through trading-manager.
- invent shared fields/statuses/types without registry review.
- bypass accepted storage or manager lifecycle boundaries.
