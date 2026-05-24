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

## Account Sleeves

Runtime decisions are scoped to independent account sleeves. A component may
read aggregate portfolio caps, but it must not net positions, collateral,
buying power, or risk budget across sleeves when producing trade decisions.

Accepted sleeves:

- `crypto_spot_account`
  - account contract: `crypto_account_state_snapshot`
  - risk-budget contract: `crypto_risk_budget_snapshot`
  - allowed asset class: `crypto_spot`
  - candidate pool: fixed to `BTC`, `ETH`, and `SOL`
  - OKX spot instrument refs: `BTC-USDT`, `ETH-USDT`, and `SOL-USDT`
  - option re-expression is disabled
- `equity_options_account`
  - account contract: `equity_options_account_state_snapshot`
  - risk-budget contract: `equity_options_risk_budget_snapshot`
  - allowed asset classes: `us_equity`, `us_etf`, and `us_option`
  - candidate pool: model-selected from the reviewed equity watchlist and
    optionable underlyings
  - option re-expression is enabled

Every `execution_intake_snapshot`, `entry_decision`,
`position_lifecycle_decision`, `option_reexpression_decision`, and
`execution_order_intent` must carry exactly one account sleeve. Cross-account
collateral, cross-account buying-power substitution, and cross-account position
netting are not accepted.

## Component Graph

```text
C01 Intake
  -> C02 Entry
  -> C03 Lifecycle
  -> C04 Option Review
  -> C05 Failure Review when observed failure exists
  -> C06 Order Intent
  -> C07 Execution Gate
```

The short numbered names are the intraday process order. The stable
`component_id` values follow the same physical naming pattern as model ids:
`component_01_*` through `component_07_*`.

| Step | Short name | Stable `component_id` | Owns |
|---|---|---|---|
| `C01` | Intake | `component_01_intake` | `execution_intake_snapshot` |
| `C02` | Entry | `component_02_entry` | `entry_decision` |
| `C03` | Lifecycle | `component_03_lifecycle` | `position_lifecycle_decision` |
| `C04` | Option Review | `component_04_option_review` | `option_reexpression_decision` |
| `C05` | Failure Review | `component_05_failure_review` | `failure_explanation_packet` |
| `C06` | Order Intent | `component_06_order_intent` | `execution_order_intent` |
| `C07` | Execution Gate | `component_07_execution_gate` | `broker_order_request` / `simulated_fill_event` |

### C01 Intake

Owns `execution_intake_snapshot`.

Purpose: read account balance state, current holdings, watch targets, and the
strong-sector opportunity mix for one account sleeve before downstream entry and
lifecycle components make trading decisions.

Model inputs:

- Layer 1 market regime.
- Layer 2 sector context.
- Layer 3 target state.

It does not call Layer 6, Layer 8, Layer 9, or Layer 10. C01 does not size
positions, allocate risk budget, decide whether a thesis deserves a trade, or
manage exits.

Live application scenario:

- At each live decision minute, C01 reads account sleeve state, available
  balance, current open positions, the market universe, watch targets, and the
  latest M01/M02/M03 outputs.
- For `crypto_spot_account`, it keeps the fixed crypto pool limited to `BTC`,
  `ETH`, and `SOL`, blocking other crypto symbols before later components see
  them.
- For `equity_options_account`, it keeps only eligible equity, ETF, or
  optionable-underlying watch targets and leaves option expression to C02/C04.
- It builds `sector_opportunity_mix` from sectors whose M02 strength exceeds the
  accepted strong-sector threshold, then subtracts the sector mix already held
  in the account sleeve. It does not force a top-three list.
- The mix is dynamic: if the desired opportunity mix is `software 35%`,
  `semiconductors 35%`, and `healthcare 30%`, but current positions already
  fill `semiconductors 35%`, the remaining C01 opportunity map stops asking C02
  to focus on semiconductors until that exposure falls below its target mix.
- The mix is an opportunity map for C02, not a final position-weight, order
  quantity, or risk allocation instruction.
- If the account has available balance, C01 may pass watch targets forward. It
  does not block targets for concentration, position size, stop distance, or
  risk-budget reasons; those decisions belong to downstream components.
- The output `execution_intake_snapshot` is the minute's account-and-watchlist
  entrance record. It authorizes downstream evaluation, not order construction,
  position sizing, risk management, or broker mutation.

### C02 Entry

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

### C03 Lifecycle

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

### C04 Option Review

Owns `option_reexpression_decision`.

Purpose: periodically review held option contracts for moneyness, greeks, DTE,
spread, liquidity, IV, payoff efficiency, and roll cost.

This component runs only for `equity_options_account`. Crypto spot positions do
not use option re-expression.

For the high-risk options account, option review is underlying-thesis driven.
Large option mark-to-market drawdowns are tolerated when the underlying path
still respects the model-provided stop and thesis invalidation. C04 may roll or
repair expression when DTE, delta, spread, liquidity, IV, or payoff efficiency
deteriorates, but it must not exit solely because option premium crossed a
fixed loss percentage.

Model inputs:

- Layer 6 dynamic risk policy.
- Layer 8 underlying action.
- Layer 9 option expression.

Roll decisions require a material improvement after roll-cost penalty and must
respect roll-count, liquidity, and risk-budget limits.

### C05 Failure Review

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

### C06 Order Intent

Owns `execution_order_intent`.

Purpose: convert accepted entry, lifecycle, or option re-expression decisions
into broker-neutral execution order intents.

It calls no models and performs no broker/account mutation.

### C07 Execution Gate

Owns the boundary where `execution_order_intent` becomes either a live broker
request or a Replay simulated fill event.

Live broker mutation remains disabled unless a reviewed execution gate enables
it. Replay uses simulated adapters only; it must not submit broker requests or
mutate account, order, or position state.

## Implementation Status

Implemented and tested runtime dry-run contracts:

- `execution_intake_snapshot`
- `entry_decision`
- `position_lifecycle_decision`
- `execution_order_intent`
- `option_reexpression_decision`
- `failure_explanation_packet`
- `simulated_fill_event`
