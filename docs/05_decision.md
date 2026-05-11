# Decision


## D001 - Execution consumes promoted decisions only

Date: 2026-04-25

### Context

The trading platform needs `trading-execution` to have a clear owner boundary before implementation begins.

### Decision

Execution must not independently choose strategies, train models, or bypass manager-controlled promotion.

### Rationale

A narrow component boundary prevents hidden coupling and keeps cross-repository work reviewable.

### Consequences

- Implementation work must stay inside the accepted component role.
- Shared names and contracts must route through `trading-main`.
- Generated outputs and secrets must stay out of Git.


## D002 - Broker/exchange operations are safety-sensitive

Date: 2026-04-25

### Context

The trading platform needs `trading-execution` to have a clear owner boundary before implementation begins.

### Decision

Live order placement and account-affecting actions require explicit mode boundaries, safeguards, and evidence.

### Rationale

A narrow component boundary prevents hidden coupling and keeps cross-repository work reviewable.

### Consequences

- Implementation work must stay inside the accepted component role.
- Shared names and contracts must route through `trading-main`.
- Generated outputs and secrets must stay out of Git.


## D003 - Credentials stay outside the repository

Date: 2026-04-25

### Context

The trading platform needs `trading-execution` to have a clear owner boundary before implementation begins.

### Decision

Broker/exchange credentials and tokens must be stored as secret aliases or external secret material, never in Git.

### Rationale

A narrow component boundary prevents hidden coupling and keeps cross-repository work reviewable.

### Consequences

- Implementation work must stay inside the accepted component role.
- Shared names and contracts must route through `trading-main`.
- Generated outputs and secrets must stay out of Git.


## D002 - Calendar discovery belongs to execution

Future macro release calendars are realtime acquisition triggers, not historical data payloads. `trading-execution` owns `calendar_discovery` code for discovering official release-calendar URLs, fetching official pages/feeds, and producing release-event rows for scheduling. `trading-data` remains focused on historical data retrieval and cleaning.

## D004 - Every executable trade requires a hard trade risk cap

Date: 2026-05-07
Status: Accepted

### Context

Layer 7 emits offline direct-underlying stop/invalidation thesis fields and Layer 8 emits long-option premium-risk fields, but neither model layer owns broker enforcement.

### Decision

`trading-execution` must reject order construction and order placement unless the decision record contains a valid `trade_risk_cap`.

The cap must include positive `max_loss_usd`, positive `max_loss_pct`, ISO `time_stop_at`, accepted `cap_enforcement_mode`, and `cap_failure_action = reject_order`. Direct underlying trades also require `model_invalidation_price` and `hard_stop_price`. Long-option premium-defined trades require positive premium-at-risk evidence and `max_loss_is_premium_paid_flag = true`.

### Consequences

- Warn-only or best-effort cap handling is not accepted.
- Broker adapters must call equivalent pre-order validation before any paper/live mutation.
- Missing, malformed, unsupported, stale, or unenforceable caps are hard rejects.

## D005 - Current execution-preparation phase is closed

Date: 2026-05-09
Status: Accepted

### Context

`trading-execution` now has a clear repository boundary, accepted calendar-discovery ownership, mandatory `trade_risk_cap` pre-order invariant, and a broker-agnostic validation helper.

### Decision

Close the current execution-preparation phase. `docs/08_execution_closeout.md` is the authoritative closeout receipt.

No active execution-preparation tasks remain. Future execution work is deferred until a concrete reviewed decision/handoff consumer requires it: broker/order-construction, paper/live mode boundaries, order/fill/position/reconciliation artifacts, manager/storage integration, and broker-specific credential/adapter safeguards.

### Consequences

- `trading-execution` remains the owner of broker/exchange runtime and safety-sensitive mutations.
- This closeout does not enable broker adapters, order construction, order placement, fills, positions, account mutation, provider calls, model activation, or manager dispatch.
- New execution implementation must start from the hard `trade_risk_cap` validation invariant and an explicit acceptance gate.

## D006 - Realtime market data uses reviewed realtime interfaces

Date: 2026-05-11
Status: Accepted

### Context

Execution needs current market observations for monitoring, risk checks, and order routing. Chentong clarified that realtime data should generally use the same data sources as historical data, but not necessarily the same interfaces.

### Decision

Realtime market-data acquisition is an execution-facing interface boundary separate from historical backfill. OKX, Alpaca, and ThetaData may share canonical provider/source identities with historical data, but execution must use separately reviewed realtime transports such as public WebSocket channels, realtime HTTP snapshots, or terminal WebSocket streams.

### Consequences

- Historical cleaned data remains owned by `trading-data`.
- Execution realtime streams must not become historical source-of-truth backfills.
- Market-data access does not imply broker/order authority.
- Adapters start from side-effect-free catalogs and fixture tests before any live stream is enabled.

## D007 - OKX is the first crypto execution venue candidate

Date: 2026-05-11
Status: Accepted

### Context

Chentong selected OKX for crypto trading. OKX has official REST and WebSocket APIs, including public market-data channels and authenticated private trading/account channels.

### Decision

OKX is accepted as the first crypto broker/exchange interface candidate. Initial development may add catalogs, schemas, dry-run validation, signing fixtures, and simulated order lifecycle, but live order mutation remains disabled until explicit execution-mode, approval, idempotency, credential, risk-cap, and receipt gates are implemented.

### Consequences

- OKX credentials resolve only through external secret aliases.
- Any future order adapter must call `trade_risk_cap` validation before order construction/placement.
- Public market-data routes and private order/account routes remain separate.
- No live OKX order call is enabled by the initial catalog work.

## D008 - Firstrade equity/options execution is deferred

Date: 2026-05-11
Status: Accepted

### Context

Chentong selected Firstrade for US stocks and options, but Firstrade does not appear to provide an accepted official trading API. Web review found reverse-engineered community clients, which are not enough for this system.

### Decision

Firstrade remains the intended equity/options broker, but automated execution is deferred until an official or explicitly reviewed compliant trading interface exists.

### Consequences

- Do not implement reverse-engineered Firstrade login, browser trading, scraped order tickets, or unofficial order automation.
- Equity/options broker mutation remains unavailable in `trading-execution` for now.
- The catalog may record Firstrade as deferred so future work does not accidentally treat it as an active adapter.
