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

## D009 - Realtime coverage matrix and capture contract are side-effect-free

Date: 2026-05-11
Status: Accepted

### Context

Realtime inputs should cover the model stack's live inference needs and later support forward/shadow validation, but cataloging those needs must not accidentally enable provider streams, model activation, or broker mutation.

### Decision

`trading-execution` records `execution_realtime_input_coverage_v1` rows for Layers 1-8 and a `realtime_capture_contract_v1` for append-only validation evidence. The matrix separates complete routes from partial/gap routes, especially proxy coverage for Layer 1, event routes for Layer 4, account-state routes for Layer 6, restriction/account routes for Layer 7, and ThetaData terminal requirements for Layer 8.

### Consequences

- Realtime coverage gaps remain visible instead of being hidden behind generic "connected" language.
- Future adapters must emit point-in-time capture facts before realtime rows can become `forward_holdout` or `shadow_monitoring` evidence.
- Catalog inspection performs no provider calls, opens no streams, activates no models, and mutates no broker/account state.

## D010 - Realtime adapter scaffold starts with planning and capture validation

Date: 2026-05-11
Status: Accepted

### Context

The realtime data layer needs to become executable without collapsing safety boundaries. Opening WebSocket streams, resolving credentials, or using account state requires separate live-observe approvals, but the system can already validate request shape, route coverage, and capture evidence locally.

### Decision

`trading-execution` adds side-effect-free realtime subscription planning and capture validation. `execution_realtime_subscription_plan_v1` supports `dry_run`, `fixture_replay`, and approval-blocked `live_observe` plan rows. `realtime_capture_validation_v1` checks candidate capture rows against `realtime_capture_contract_v1`.

### Consequences

- Adapter scaffolds are now connected across OKX, Alpaca, ThetaData, derived model context, calendar discovery, and execution account-state placeholders.
- Plan/validation helpers report zero provider calls, zero broker calls, and no model activation.
- Future live-observe adapters must reuse these contracts and add explicit approval, secret, reconnect/backoff, manifest, artifact, and ready-signal handling before any external stream is opened.

## D012 - Formal realtime provider observation is gated and read-only

Date: 2026-05-11
Status: Accepted

### Context

Fixture-only realtime paths are not sufficient for formal integration. Execution needs a real provider-observation path, but provider calls, model activation, manager persistence, and broker/account mutation have different safety profiles and must not be enabled by one implicit switch.

### Decision

`trading-execution` accepts `realtime_live_observe_approval_v1` as the first formal live-integration gate. With a valid approval and explicit `--execute-live-observe`, `scripts/execution/execute_live_observe.py` may perform bounded read-only market-data observations for reviewed OKX, Alpaca, and ThetaData routes and emit `execution_realtime_live_observe_result_v1`, realtime capture rows, feature snapshots, and model-input snapshots.

This approval is only for realtime market-data observation. It must explicitly keep model activation, broker execution, broker order construction, and account mutation disabled.

### Consequences

- Formal provider observation is no longer fixture-only; approved read-only provider calls are supported.
- A plan-only invocation still performs zero provider calls.
- Model activation, production configuration activation, broker execution, and account mutation require separate reviewed gates. Order construction has its own separate approval gate under `execution_order_construction_approval_v1`.
- Manager visibility can consume the produced artifacts, but execution does not persist manager decisions.

## D011 - Realtime feature snapshots bridge into historical-model decision inputs

Date: 2026-05-11
Status: Accepted

### Context

Raw realtime captures are not model inputs. The model stack was designed and validated around point-in-time feature/model-output contracts, so live observations need a deterministic bridge that preserves historical feature parity and frozen model/data refs before any decision handoff.

### Decision

`trading-execution` owns `realtime_feature_snapshot_v1` and `execution_model_decision_input_snapshot_v1` as side-effect-free handoff envelopes. The feature snapshot converts realtime capture refs into Layer 1-8 feature refs with `feature_time`, `available_time`, `tradeable_time`, historical feature parity refs, frozen model config refs, and historical dataset snapshot refs. The decision input snapshot packages those layer refs for fixture/shadow historical-model decision routing.

### Consequences

- Realtime data can now be prepared into the shape expected by historical model decision paths without opening streams or activating models.
- Feature generation remains parity-bound to historical `trading-data` / `trading-model` definitions; realtime builders must not silently invent divergent live-only semantics.
- The handoff still does not authorize provider streams, model activation, production decision activation, order construction, broker mutation, or account mutation.

## D013 - Approved order-intent construction is separate from broker submission

Date: 2026-05-11
Status: Accepted

### Context

Formal integration needs to progress beyond validating risk caps, but constructing an order payload and submitting it to a broker are not the same safety boundary.

### Decision

`trading-execution` accepts `execution_order_construction_approval_v1` as the first broker-facing order-construction gate. With a valid approval, a valid `trade_risk_cap`, and explicit `--construct-order`, `scripts/execution/build_broker_order_intent.py` may construct an OKX-shaped `execution_broker_order_intent_v1`.

The resulting intent is `constructed_not_submitted`. It carries an idempotency key and broker payload, but performs zero broker calls and zero account mutation.

### Consequences

- Order construction is no longer only theoretical; approved order intents can be built.
- Broker submission, fills, position/account mutation, and reconciliation remain separate gates.
- Missing or invalid `trade_risk_cap` still blocks construction.

## D014 - Realtime monitoring runtime is execution-owned

Date: 2026-05-11
Status: Accepted

### Context

The platform now has two very different operating loops: manager-owned historical modeling, and live market monitoring for execution/risk context. Chentong clarified that realtime monitoring must be isolated from the historical modeling system and must not be controlled by `trading-manager`.

### Decision

`trading-execution` owns the realtime monitoring runtime. This includes live observe processes, provider stream/session lifecycle, subscriptions, throttling, heartbeat/reconnect/backoff, runtime health, and monitoring-specific capacity policy.

`trading-manager` may consume append-only realtime receipts, coverage summaries, shadow handoff artifacts, and mature validation evidence. It must not start, stop, schedule, throttle, reconnect, or otherwise control realtime provider monitoring processes. Manager-owned historical schedulers may reserve capacity for realtime systems and back off during protected windows, but they are not the realtime control plane.

### Consequences

- Realtime monitors can continue operating even when historical modeling is paused, backlogged, or restarting.
- Historical modeling cannot accidentally disable or starve live monitoring by owning its runtime loop.
- Shared names/contracts may still be registered through `trading-manager`, but registration and receipt consumption do not imply runtime control.
- Production model activation, order construction, broker submission, and account mutation remain separate reviewed gates.
