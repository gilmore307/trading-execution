# Trade Risk Cap

Status: accepted pre-order safety invariant
Date: 2026-05-07

## Purpose

Every executable trade must carry a hard `trade_risk_cap` before order construction or placement. The cap converts model-side stop/invalidation thesis and option premium-risk constraints into an execution-side rejection gate.

This document does not approve live trading. It defines the minimum invariant any future paper/live execution implementation must enforce.

The high-risk options account is managed by underlying thesis, not by a fixed
option P/L stop. For options-first trades, execution follows the model-provided
underlying stop and thesis invalidation levels. Fixed loss percentages may size
risk budget and gate catastrophic account exposure, but they must not replace
the model stop line for ordinary position management.

## Boundary

Model layers may emit offline risk thesis fields:

- M04: `stop_loss_price`, `thesis_invalidation_price`, `time_stop_minutes`.
- M05: `premium_stop_pct`, `premium_time_stop_minutes`, `planned_max_premium_at_risk_usd`, `max_loss_is_premium_paid_flag`.

Those fields are not broker orders. `trading-execution` owns pre-order validation and any later broker-native or synthetic enforcement.

## Required `trade_risk_cap` fields

| Field | Requirement |
|---|---|
| `max_loss_usd` | positive hard maximum loss estimate for the proposed trade |
| `max_loss_pct` | positive normalized maximum loss estimate |
| `time_stop_at` | ISO timestamp by which the trade thesis must be rechecked/exited/rejected by policy |
| `cap_enforcement_mode` | one accepted mode: `broker_native_stop`, `risk_monitor_synthetic_stop`, or `long_option_premium_defined_risk` |
| `cap_failure_action` | must be `reject_order` |

Direct underlying trades also require:

```text
model_invalidation_price
hard_stop_price
```

Long-option premium-defined trades require:

```text
planned_max_premium_at_risk_usd
max_loss_is_premium_paid_flag = true
```

For long-option trades, premium-at-risk defines the maximum capital committed
to the option expression. It is not an automatic exit trigger based on option
mark-to-market loss. Exit, roll, reduce, or hold decisions are driven by the
underlying thesis path: model invalidation, model stop, time stop, event-risk
changes, liquidity/spread deterioration, or account-level catastrophic gates.

## Stop-source policy

Execution must not substitute a fixed percentage stop for a model stop.

Accepted stop hierarchy:

1. `model_invalidation_price` from the model thesis identifies where the
   underlying thesis is no longer valid.
2. `hard_stop_price` is the enforceable execution protection derived from that
   thesis and broker/synthetic monitor constraints.
3. `max_loss_usd` and `max_loss_pct` size the trade and account budget; they do
   not define a standalone fixed stop.

If the model thesis does not provide an enforceable stop or invalidation line,
the order must be rejected rather than filled with a default fixed stop.

## Rejection rule

If the cap is missing, malformed, unsupported, stale, or impossible to enforce, the execution system must reject order construction/placement.

```text
missing_or_invalid_trade_risk_cap -> reject_order
```

Warn-only behavior is not accepted.

## Implementation hook

`src/trading_execution/risk_cap/validator.py` owns the current reusable pre-order validator. It is intentionally small and broker-agnostic; later broker adapters must call equivalent validation before constructing any paper/live order.

`scripts/execution/validate_trade_risk_cap.py` is the component-facing validation entrypoint for a unified decision record JSON. It exits non-zero when the cap is missing or invalid. It does not create orders, mutate broker state, or call external providers.
