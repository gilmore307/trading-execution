# Runtime Components

`trading-execution` owns the task-level runtime component graph used by both live
trading and Replay. Models are inputs to these components; they are not the
runtime organizing unit.

C08 Model Group Shadow Comparison is a market-hours runtime comparison component
for already-promoted model groups. It runs the active model group and eligible
shadow model groups on realtime snapshots, records evidence, and feeds mature
evidence to `execution_shadow_cycle_selection`. C08 is not used by Replay and
does not route shadow decisions into C01-C06 live trading authority. It must be
capacity-gated so model comparison does not degrade the active trading path.

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
  -> candidate_entry_pool -> C02 Entry
  -> open_position_pool   -> C03 Lifecycle

C02/C03 accepted underlying intents
  -> C04 Option Review
  -> C05 Order Intent
  -> C06 Execution Gate

observed model/trade failure
  -> C07 Failure Review
```

The short numbered names are the runtime component catalog order. The live and
Replay execution path branches after C01: new opportunity candidates go through
C02, while existing positions bypass C02 and go through C03. Both branches then
converge at C04 expression review before C05 order intent and C06 execution
gate review. The stable
`component_id` values follow the same physical naming pattern as model ids:
`component_01_*` through `component_07_*`.

| Step | Short name | Stable `component_id` | Owns |
|---|---|---|---|
| `C01` | Intake | `component_01_intake` | `execution_intake_snapshot` |
| `C02` | Entry | `component_02_entry` | `entry_decision` |
| `C03` | Lifecycle | `component_03_lifecycle` | `position_lifecycle_decision` |
| `C04` | Option Review | `component_04_option_review` | `option_reexpression_decision` |
| `C05` | Order Intent | `component_05_order_intent` | `execution_order_intent` |
| `C06` | Execution Gate | `component_06_execution_gate` | `execution_gate_result` / `broker_order_request` / `simulated_fill_event` |
| `C07` | Failure Review | `component_07_failure_review` | `failure_explanation_packet` |

### C01 Intake

Owns `execution_intake_snapshot`.

Purpose: read account balance state, current holdings, target candidates, and
the strong-sector opportunity mix for one account sleeve, then split the minute
into the entry-candidate and open-position branches.

C01 emits two downstream pools:

- `candidate_entry_pool`: new-opportunity targets routed to C02 Entry.
- `open_position_pool`: existing positions routed to C03 Lifecycle.

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
- The equity/options watch target pool is the union of remaining strong-sector
  targets, recent high-trading-volume targets, recent abnormal-volume targets,
  and recent catalyst targets such as earnings beats or material news. A filled
  sector only removes the strong-sector opportunity reason; a target from that
  sector can still enter through high volume, abnormal volume, or catalyst
  evidence.
- `recent_high_trading_volume` means a reviewed high-volume flag or an absolute
  volume/dollar-volume score or percentile at or above `0.80`.
- `recent_abnormal_volume` means a reviewed abnormal-volume flag, relative or
  abnormal volume score at or above `0.80`, relative volume at or above `2.0x`,
  or volume z-score at or above `2.0`.
- The mix is an opportunity map for C02, not a final position-weight, order
  quantity, or risk allocation instruction.
- If the account has available balance, C01 may pass watch targets forward. It
  does not block targets for concentration, position size, stop distance, or
  risk-budget reasons; those decisions belong to downstream components.
- The output `execution_intake_snapshot` is the minute's account-and-watchlist
  entrance record. It includes both `candidate_entry_pool` and
  `open_position_pool`; this authorizes downstream evaluation, not order
  construction, position sizing, risk management, or broker mutation.

### C02 Entry

Owns `entry_decision`.

Purpose: decide which targets in the C01 `candidate_entry_pool` have a suitable
underlying entry thesis for continued review. C02 does not choose option versus
stock expression, choose contracts, size positions, check account balance, or
build orders.

Model inputs:

- Layer 3 target state.
- Layer 4 event failure risk.
- Layer 5 alpha confidence.
- Layer 6 dynamic risk policy.
- Layer 8 underlying action.

It does not call Layer 9 or Layer 10. Option versus underlying expression is a
C04 decision; pre-entry event risk is handled by Layer 4.

Live application scenario:

- C02 consumes only `execution_intake_snapshot.candidate_entry_pool` emitted by
  C01. The compatibility alias `watch_targets` may carry the same rows, but the
  accepted route is the candidate entry pool. A target outside C01 is rejected
  as a defensive contract error, not actively discovered by C02.
- C02 emits `entry_thesis_status` as `suitable`, `deferred`, or `rejected`.
  Only `suitable` proceeds to C04.
- A suitable thesis must include a long or short underlying direction, entry
  zone, target or take-profit zone, model invalidation price, hard stop price,
  expected horizon when available, and an `entry_suitability_score`.
- `deferred` is used when the thesis may be valid but the entry zone or
  target/take-profit setup is not currently actionable, including when current
  price is outside the entry zone.
- `rejected` is used for event-risk blocks, dynamic-risk new-entry blocks,
  insufficient alpha, missing direction, missing model invalidation, missing
  hard stop, or invalid C01 source target.
- `entry_decision` is not a direct order source for C05. It is an underlying
  thesis handoff for C04 expression review.

### C03 Lifecycle

Owns `position_lifecycle_decision`.

Purpose: manage already-open positions by deciding hold, add, reduce, exit,
stop, take-profit, or flatten-review actions from the current underlying thesis
and risk state.

C03 is an underlying-thesis lifecycle layer. It does not select new targets,
decide new-entry suitability, select option contracts, size positions, build
orders, or execute broker/account mutations. For option positions, it evaluates
the position's underlying exposure; C04 owns the later translation into option
or stock expression.

C03 consumes C01 `open_position_pool` records and the current model evidence for
those already-open positions. It is a sibling of C02, not downstream of C02.

Model inputs:

- Layer 4 event failure risk.
- Layer 5 alpha confidence.
- Layer 6 dynamic risk policy.
- Layer 7 position projection.
- Layer 8 underlying action.

It does not call Layer 10 during normal lifecycle decisions. Observed model or
trade failure routes to the Failure Explanation Component.

Live application scenario:

- All lifecycle operations are computed in underlying terms first. C03 decides
  whether the underlying thesis should hold, add exposure, reduce exposure,
  stop, exit, or take profit. C04 translates that underlying action into option
  expression, roll, repair, stock fallback, or no expression.
- Ordinary high-risk options-account exits are not driven by fixed option P/L
  loss percentages. C03 follows the model-provided underlying hard stop and
  thesis invalidation lines. Option premium at risk limits capital committed;
  it is not an automatic mark-to-market exit trigger.
- Every non-hold lifecycle action must carry explicit reason codes and model
  evidence. C03 does not use fee, PDT, day-trade, or churn formulas to
  override the thesis decision; those facts are execution-review context for
  C06.
- Risk-based add/reduce, staged entry/exit, and thesis-aware high-sell/low-buy
  style exposure adjustments are valid only when carried by trained model
  evidence from M07/M08. C03 should not invent tactical trades from raw price
  movement.
- Add signals must respect the upstream sector/opportunity mix and portfolio
  exposure constraints carried through C01/M07 context. If the target's sector
  or exposure bucket is already filled, C03 keeps `hold` rather than adding
  more exposure.
- `reduce` is reserved for material risk reduction or thesis deterioration.
  It is not a reaction to every small price wiggle.
- `add` requires a still-valid thesis, stronger alpha, acceptable projected
  path after add, and compliance with the current sector/opportunity mix.

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

### C05 Order Intent

Owns `execution_order_intent`.

Purpose: convert accepted entry, lifecycle, or option re-expression decisions
into complete broker-neutral execution order intents.

C05 owns all position-management content needed for the order:

- final order quantity;
- target post-trade position or exposure when available;
- add/reduce/exit/roll sizing reason codes;
- target-allocated buying power and estimated unit cost when available;
- whether advanced tranche management is allowed for the target;
- premium/capital-at-risk packaging through `trade_risk_cap`;
- broker-neutral order type, limit/stop reference, and time-in-force policy;
- source decision refs from C02, C03, and C04.

It calls no models and performs no broker/account mutation. It must not submit
orders. Once C05 emits an executable `execution_order_intent`, C06 may reject or
submit/simulate it, but must not recalculate or modify the quantity.

If target-allocated buying power can only afford fewer than the configured
advanced-management unit count, C05 records
`single_allocation_no_advanced_scaling` in `sizing_plan` and blocks tactical
add/reduce order-intent construction. C03 must not make this capacity decision:
it has not yet resolved final option expression cost and does not own final
sizing. When the same target allocation can afford several contracts, C05
records `advanced_tranche_management_allowed` and may construct the current
tranche from model-backed staged entry, add, reduce, or staged exit evidence.

### C06 Execution Gate

Owns `execution_gate_result`, the boundary where `execution_order_intent`
becomes either a live broker request candidate or a Replay simulated fill event.

Live broker mutation remains disabled unless a reviewed execution gate enables
it. Replay uses simulated adapters only; it must not submit broker requests or
mutate account, order, or position state.

Agent final review is a hard live-submission boundary. Any open, add, reduce,
exit, stop, take-profit, roll, or stock-fallback order must present its C02/C03
or C04 reason evidence to C06 and receive an approved agent review before a
live broker order can be submitted. C06 only validates the C05 order intent,
checks final missed-event review, C07 untrained-event risk review evidence when
present, and broker/regulatory hard blocks, and then rejects the intent,
approves it for live submission, or approves it for Replay simulation. C06 does
not own position management, sizing, target exposure, or order-policy
calculation.

The C06 gate result must prove:

- the source `execution_order_intent` is ready for execution gate review;
- the broker-neutral order quantity equals the C05 `sizing_plan` quantity;
- `execution_gate_may_change_quantity` is false;
- hard-block checks do not reject the order;
- live mode has an approved `agent_final_review_ref`;
- Replay simulated fills cite the approving `execution_gate_result`.

### C07 Failure Review

Owns `failure_explanation_packet`.

Purpose: after model or trade behavior has already failed or deviated, link the
failure evidence to possible unscreened events and produce Layer 4 feedback
candidates.

Live timing: C07 is valid in live operation in two modes:

- realtime watch during market hours;
- settlement attribution after the regular session closes or in another accepted
  off-hours attribution window.

Realtime watch may inspect active and shadow decisions with an open thesis, open
position, material path deviation, new event evidence, or unexplained model
drift. When the event or anomaly has not been trained through Layer 10 and
accepted into Layer 4, C07 must mark the evidence as untrained. It may estimate
a provisional risk value from model-failure severity, path deviation, event
proximity, exposure at risk, and evidence quality, but that value is review
evidence only. It is not an alpha score, not a trained Layer 4 risk score, and
not an order instruction.

Realtime watch emits early warning or preliminary attribution evidence so
C03/C05/C06 and the trading-review agent can review protective reduce, exit,
block, or human-review paths before losses compound.

Settlement attribution may inspect matured active and shadow decisions, fills,
missed opportunities, open-position outcomes, overblocks, underblocks,
option-expression drag, and event/co-event evidence.

C07 must not mutate intraday C02-C06 decisions, submit orders, change position
state, or switch active model pointers.

For untrained event-risk evidence, `failure_explanation_packet` must carry:

- `event_training_status=untrained_or_not_accepted`;
- model-failure severity evidence;
- provisional risk estimate and confidence;
- evidence-quality and freshness refs;
- affected thesis, position, and candidate refs;
- recommended review route such as `protective_review`, `reduce_review`,
  `exit_review`, `block_new_entry_review`, or `human_review`.

The trading-review agent owns the final judgment on whether that provisional
risk warrants blocking, reducing, exiting, or escalation.

Model input:

- Layer 10 event risk governor.

Layer 10 is not a standalone pre-entry veto. It is a residual event-risk
explanation and deviation-watch model. If the observed event was not trained and
accepted through Layer 10/Layer 4, C07 may only produce provisional untrained
risk evidence:

```text
active/shadow thesis or observed model/trade failure
  -> possible untrained event cause or model-invalidating context
  -> provisional risk estimate from failure severity
  -> trading-review agent judgment
  -> C03/C05/C06 protective review evidence
  -> later Layer 10/Layer 4 feedback candidate
```

Layer 4 remains the forward event-risk model used in entry and position
lifecycle decisions:

```text
accepted event evidence -> model/trade risk
```

## Implementation Status

Implemented and tested runtime dry-run contracts:

- `execution_intake_snapshot`
- `entry_decision`
- `position_lifecycle_decision`
- `execution_order_intent`
- `execution_gate_result`
- `option_reexpression_decision`
- `failure_explanation_packet`
- `simulated_fill_event`
