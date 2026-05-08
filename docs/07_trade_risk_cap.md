# Trade Risk Cap

Status: accepted pre-order safety invariant
Date: 2026-05-07

## Purpose

Every executable trade must carry a hard `trade_risk_cap` before order construction or placement. The cap converts model-side stop/invalidation thesis and option premium-risk constraints into an execution-side rejection gate.

This document does not approve live trading. It defines the minimum invariant any future paper/live execution implementation must enforce.

## Boundary

Model layers may emit offline risk thesis fields:

- Layer 7: `stop_loss_price`, `thesis_invalidation_price`, `time_stop_minutes`.
- Layer 8: `premium_stop_pct`, `premium_time_stop_minutes`, `planned_max_premium_at_risk_usd`, `max_loss_is_premium_paid_flag`.

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

## Rejection rule

If the cap is missing, malformed, unsupported, stale, or impossible to enforce, the execution system must reject order construction/placement.

```text
missing_or_invalid_trade_risk_cap -> reject_order
```

Warn-only behavior is not accepted.

## Implementation hook

`src/trading_execution/risk_cap/validator.py` owns the current reusable pre-order validator. It is intentionally small and broker-agnostic; later broker adapters must call equivalent validation before constructing any paper/live order.
