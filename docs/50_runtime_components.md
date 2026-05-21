# Runtime Components

`trading-execution` owns the task-level runtime component graph used by both live
trading and Replay. Models are inputs to these components; they are not the
runtime organizing unit.

## Hard Rule

Live and Replay must use the same components, same decision logic, same model
calls, same risk constraints, and same decision contracts. They differ only by
adapter profile:

| Mode | Clock | Market | Account | Execution |
|---|---|---|---|---|
| Live | live clock | realtime market adapter | live account adapter | broker execution gate |
| Replay | historical clock | historical market snapshot adapter | simulated account adapter | simulated execution gate / fill simulator |

Any trading component that cannot run in both modes is not an accepted runtime
component.

## Component Graph

```text
Opportunity & Risk Allocation Engine
  -> Entry Decision Engine
  -> Position Lifecycle Controller
  -> Option Re-Expression Review
  -> Failure Explanation Component when observed failure exists
  -> Order Intent Builder
  -> Execution Gate / Adapter
```

### Opportunity & Risk Allocation Engine

Owns `target_allocation_snapshot`.

Purpose: select the current target pool and pre-allocate risk budget from market,
sector, target-state, account, and existing-position evidence.

Model inputs:

- Layer 1 market regime.
- Layer 2 sector context.
- Layer 3 target state.
- Layer 6 dynamic risk policy.

It does not call Layer 8, Layer 9, or Layer 10.

### Entry Decision Engine

Owns `entry_decision`.

Purpose: decide whether an allocated target should open an underlying or option
position, remain watch-only, defer, or be blocked.

Model inputs:

- Layer 3 target state.
- Layer 4 event failure risk.
- Layer 5 alpha confidence.
- Layer 6 dynamic risk policy.
- Layer 8 underlying action.
- Layer 9 option expression.

It does not call Layer 10. Pre-entry event risk is handled by Layer 4.

### Position Lifecycle Controller

Owns `position_lifecycle_decision`.

Purpose: manage open positions by deciding hold, add, reduce, exit, stop,
take-profit, or flatten-review actions from current thesis and risk state.

Model inputs:

- Layer 4 event failure risk.
- Layer 5 alpha confidence.
- Layer 6 dynamic risk policy.
- Layer 7 position projection.
- Layer 8 underlying action.

It does not call Layer 10 during normal lifecycle decisions. Observed model or
trade failure routes to the Failure Explanation Component.

### Option Re-Expression Review

Owns `option_reexpression_decision`.

Purpose: periodically review held option contracts for moneyness, greeks, DTE,
spread, liquidity, IV, payoff efficiency, and roll cost.

Model inputs:

- Layer 6 dynamic risk policy.
- Layer 8 underlying action.
- Layer 9 option expression.

Roll decisions require a material improvement after roll-cost penalty and must
respect roll-count, liquidity, and risk-budget limits.

### Failure Explanation Component

Owns `failure_explanation_packet`.

Purpose: after model or trade behavior has already failed or deviated, link the
failure evidence to possible unscreened events and produce Layer 4 feedback
candidates.

Model input:

- Layer 10 event risk governor.

Layer 10 is not a pre-entry veto. It is a post-failure explanation model:

```text
observed model/trade failure -> possible event causes -> Layer 4 feedback candidate
```

Layer 4 remains the forward event-risk model used in entry and position
lifecycle decisions:

```text
accepted event evidence -> model/trade risk
```

### Order Intent Builder

Owns `execution_order_intent`.

Purpose: convert accepted entry, lifecycle, or option re-expression decisions
into broker-neutral execution order intents.

It calls no models and performs no broker/account mutation.

### Execution Gate / Adapter

Owns the boundary where `execution_order_intent` becomes either a live broker
request or a Replay simulated fill event.

Live broker mutation remains disabled unless a reviewed execution gate enables
it. Replay never mutates a real broker or account.

## First Implementation Batch

Implement and validate these contracts first:

- `target_allocation_snapshot`
- `entry_decision`
- `position_lifecycle_decision`
- `execution_order_intent`

Then add:

- `option_reexpression_decision`
- `failure_explanation_packet`
- `simulated_fill_event`

