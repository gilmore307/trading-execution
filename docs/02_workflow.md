# Workflow

## Purpose

This file defines the intended component workflow for `trading-execution`.

## Primary Flow

```text
promoted decision -> realtime context snapshot -> execution plan -> safety checks -> paper/live adapter -> orders/fills/positions -> reconcile -> manifest/alert
```

## Operating Principles

- Execution is safety-sensitive and must distinguish dry-run, paper, and live behavior.
- Live external actions require explicit safeguards and should not be hidden inside generic tests.
- Execution consumes promoted decisions; it must not train models or choose strategies by itself.
- Realtime data acquisition for execution is a separate interface layer from historical backfill even when the provider/source is the same.
- Broker/exchange mutation is separate from market-data observation; market-data access must never imply order-placement authority.
- Shared fields, statuses, type values, helpers, and reusable templates must come from `trading-manager`.
- Runtime outputs must be written outside Git-tracked source paths.
- Cross-repository handoffs should use accepted request, artifact, manifest, and ready-signal contracts.

## Collaboration Boundary

`trading-execution` collaborates with other trading repositories through explicit contracts, not direct mutation of their local state.

Upstream inputs and downstream outputs should be described by artifact references, manifests, ready signals, requests, or accepted storage contracts.

## Current Execution Slice

The current slice opens execution development with side-effect-free catalogs only:

- `execution_realtime_data_interface` records reviewed realtime market-data interfaces for OKX, Alpaca, and ThetaData.
- `execution_broker_interface` records broker/exchange posture for OKX and Firstrade.
- `execution_capability_catalog` combines those catalogs for inspection.

No order construction, order placement, broker call, provider stream, fill handling, account mutation, or model activation is enabled by this slice.

## Not Current Historical-Training Scope

Execution implementation remains outside the no-broker historical-training run. The exact first live broker/order slice, request shape, artifact/manifest/ready-signal schema interactions, shared storage references, test harness, fixture policy, and package layout require explicit acceptance before mutation is enabled.
