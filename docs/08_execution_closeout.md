# Execution Closeout

## Status

The current `trading-execution` execution-preparation phase is closed.

This closeout covers the execution-owned surfaces needed before the next component production phase:

- repository boundary and docs spine;
- calendar-discovery ownership for future realtime acquisition triggers;
- mandatory pre-order `trade_risk_cap` invariant;
- broker-agnostic risk-cap validation helper;
- package/source/test layout for future execution runtime work.

## Accepted Execution-Owned Shape

`trading-execution` owns broker/exchange execution workflows and safety-sensitive mutation boundaries. It does not choose strategies, promote models, create market data truth, or bypass manager review.

The accepted current safety gate is:

```text
unified decision record
  -> trade_risk_cap present and valid
  -> validate_trade_risk_cap.py
  -> order construction may be considered only after validation succeeds
```

The first implementation helper is:

```text
src/trading_execution/risk_cap.py
scripts/execution/validate_trade_risk_cap.py
```

## Boundaries Preserved

This closeout does not enable or claim:

- broker adapter implementation;
- paper or live order placement;
- fills, positions, reconciliation, or account mutation;
- model promotion or production activation;
- provider data calls;
- manager dispatch;
- dashboard-triggered actions.

Broker/order/fill/account lifecycle remains future execution production work and requires explicit acceptance before implementation.

## Not Current Historical-Training Scope

There are no active execution work items for the current no-broker historical-training preparation boundary. Future execution work should begin only when a reviewed decision/handoff consumer requires it:

- first broker/order-construction slice after `trade_risk_cap` validation;
- paper-trading mode boundary and audit evidence;
- live-trading mode boundary and explicit approval gate;
- order/fill/position/reconciliation artifact contracts;
- execution-owned receipt/manifest/ready-signal integration with manager/storage;
- broker credential alias policy and adapter-specific safeguards.

These are not blockers for current historical training.

## Acceptance Evidence

The closeout is acceptable only while these gates pass:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests
python3 -m compileall -q src scripts
git diff --check
```

No command in this closeout performs provider calls, manager dispatch, model activation, broker order construction, order placement, fill handling, or account mutation.
