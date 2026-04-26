# Workflow

## Purpose

This file defines the intended component workflow for `trading-execution`.

## Primary Flow

```text
promoted decision -> execution plan -> safety checks -> paper/live adapter -> orders/fills/positions -> reconcile -> manifest/alert
```

## Operating Principles

- Execution is safety-sensitive and must distinguish dry-run, paper, and live behavior.
- Live external actions require explicit safeguards and should not be hidden inside generic tests.
- Execution consumes promoted decisions; it must not train models or choose strategies by itself.
- Shared fields, statuses, type values, helpers, and reusable templates must come from `trading-main`.
- Runtime outputs must be written outside Git-tracked source paths.
- Cross-repository handoffs should use accepted request, artifact, manifest, and ready-signal contracts.

## Collaboration Boundary

`trading-execution` collaborates with other trading repositories through explicit contracts, not direct mutation of their local state.

Upstream inputs and downstream outputs should be described by artifact references, manifests, ready signals, requests, or accepted storage contracts.

## Open Gaps

- Exact first implementation slice.
- Exact request shape consumed or produced by this repository.
- Exact artifact, manifest, and ready-signal schema interactions.
- Exact shared storage paths and references.
- Exact test harness and fixture policy.
- Exact package/source layout once implementation begins.
