# Realtime Market Data Interfaces

Status: accepted initial boundary catalog  
Date: 2026-05-11

## Purpose

Execution needs realtime market observations, but realtime acquisition is not the same interface as historical backfill. The canonical provider/source can be the same while the transport, latency expectation, subscription model, and entitlement behavior differ.

This document defines the first execution-facing realtime data boundary. It does not open sockets, call providers, place orders, or make realtime data the source of historical truth.

## Boundary

- Historical backfill and cleaned historical datasets remain owned by `trading-data`.
- Realtime market observations needed for execution monitoring, risk checks, and order routing belong under `trading-execution` once activated.
- The realtime monitoring runtime is execution-owned: live observe processes, provider stream/session lifecycle, subscriptions, throttling, heartbeat/reconnect/backoff, and runtime health are not controlled by `trading-manager`.
- Realtime observations may feed manager/model shadow or forward-validation evidence only as append-only point-in-time capture with frozen model/config refs; they do not replace the initial historical validation/test split ladder.
- Shared source names, interface terms, and cross-repository contracts must be registered through `trading-manager` before other repos depend on them, but registration/receipt visibility does not make manager the live monitoring controller.
- Runtime observations must be written outside Git-tracked source paths.
- Secrets and provider credentials stay outside the repository.

## Reviewed interfaces

| Source | Historical relationship | Realtime interface | Execution use | Status |
|---|---|---|---|---|
| OKX | Same canonical source as historical OKX crypto data | Public WebSocket market data plus public REST snapshots | Crypto realtime market data for execution/risk context | Adapter scaffold allowed; no live socket enabled yet |
| Alpaca | Same canonical source as historical Alpaca equity/ETF data | Market-data WebSocket plus HTTP API | Equity/ETF/options realtime observations; broker execution may still route elsewhere | Reviewed source; adapter not started |
| ThetaData | Same canonical source as historical ThetaData options data | Local Theta Terminal WebSocket streams | Options quote/trade stream for option execution context | Reviewed source; adapter not started |

## Source notes from official docs checks

- OKX official API docs describe REST and WebSocket APIs; public WebSocket channels do not require authentication and include tickers, K-Line/candlestick, order book, mark price, and related market-data channels. Private account/order channels require login.
- Alpaca official docs describe Market Data API access through both HTTP and WebSocket protocols for realtime and historical market data.
- ThetaData official streaming docs describe local Theta Terminal WebSocket access; the terminal and entitlement are required.


## Model input coverage matrix

Realtime coverage is tracked by `execution_realtime_input_coverage` rows in `src/trading_execution/market_data/contracts.py`. These rows are requirements and gap markers; they do not enable streams.

| Layer | Model output | Realtime input groups | Primary sources | Status |
|---:|---|---|---|---|
| 1 | `market_context_state` | market/ETF quotes, bars, liquidity; volatility/rates/credit/dollar/commodity proxies; crypto risk-appetite proxies | Alpaca, OKX | Partial route defined; proxy/feed gap review still required |
| 2 | `sector_context_state` | sector/industry ETF quotes, bars, liquidity, relative strength, breadth/dispersion proxies | Alpaca | Route defined; adapter not started |
| 3 | `target_context_state` | target quote/trade/bar/snapshot, liquidity/spread, Layer 1/2 context refs | Alpaca, OKX | Route defined; adapter not started |
| 4 | `event_failure_risk_vector` | reviewed event/strategy-failure conditioning refs and freshness/quality diagnostics | derived governance/model context | Contract defined; no direct provider route |
| 5 | `alpha_confidence_vector` | current Layer 1-4 state stack and freshness/quality diagnostics | derived model context | Contract defined; no direct provider route |
| 6 | `dynamic_risk_policy_state` | market/systemic event context, alpha confidence context, portfolio capacity, risk-budget state | derived model context, execution account state, Alpaca, OKX | Context contract only; broker/account route deferred |
| 7 | `position_projection_vector` | current/pending position, exposure, risk budget, current cost/liquidity context, Layer 6 policy ref | execution account state, Alpaca, OKX, ThetaData | Context contract only; broker/account route deferred |
| 8 | `underlying_action_plan` | underlying quote/liquidity/spread, restrictions/halt/borrow state, Layer 7 projection ref | Alpaca, OKX, execution account state | Partial route defined; restriction/account route deferred |
| 9 | `option_expression_plan` | underlying quote, option-chain snapshot, option quote/trade stream, IV/Greeks, OI/latest interest | ThetaData, Alpaca | Route defined; adapter not started; terminal required |
| 10 | `event_context_vector` / `event_risk_intervention` | news/event arrivals, earnings/macro triggers, abnormal equity activity, option activity events attached to the Layer 8 thesis | Alpaca, ThetaData, calendar discovery | Partial route defined; event adapter review required |

The matrix intentionally exposes gaps. A partial row is not a failure; it prevents us from pretending that realtime coverage is complete before a provider, account-state, or restriction route is accepted.


## Runtime ownership

Realtime monitoring is isolated from manager-owned historical modeling. `trading-execution` owns the live monitor control loop: process lifecycle, provider subscriptions, stream/session health, throttling, reconnect/backoff, and monitoring-specific runtime capacity. `trading-manager` may consume append-only receipts, coverage summaries, shadow handoff artifacts, and mature decision-effectiveness metrics, but it must not start, stop, schedule, throttle, reconnect, or otherwise control realtime provider monitoring.

This separation lets live monitoring continue while historical training is paused, restarting, backlogged, or running under market-hours protection. Manager-side schedulers may reserve capacity for realtime systems and back off when live monitoring needs priority; they do not become the realtime runtime owner.

## Realtime capture contract

`realtime_capture_contract` is the append-only evidence shape for realtime observation capture. Required facts include observation time, provider available time, tradeable time, source/interface, instrument ref, normalized payload ref, frozen model/config refs, model output refs, label maturity time, outcome label refs, and manager/storage handoff refs.

The realtime monitor does not create historical test/holdout/training rows by default. Historical backfill will eventually cover the same calendar period through the historical pipeline. Realtime capture should stay light enough to support online model decision-effectiveness metrics: decision id, model/config refs, decision/output ref, evaluation horizon, matured outcome label/ref, correctness status, and aggregate accuracy/hit-rate/error metrics. The contract forbids provider-stream activation by catalog inspection, historical snapshot rewrites, model refit before reviewed snapshot boundaries, model activation, broker order construction, broker order mutation, and account mutation.

## Realtime model effectiveness metrics

`realtime_model_decision_effectiveness` is the accepted monitoring surface for model quality in live/shadow operation. It summarizes whether the model's decisions were correct after the relevant outcome horizon matures. These metrics may inform promotion review, drift review, trust reduction, and retraining planning, but they are not historical test-set rows and should not force the realtime monitor to run the historical dataset-processing ladder. `scripts/execution/aggregate_realtime_decision_effectiveness.py` builds this aggregate from matured decision records without provider calls, model activation, persistence, broker/order construction, or account mutation.

After a market-hours cycle matures, runtime roster selection is handled by `execution_shadow_cycle_selection` in `docs/40_runtime_model_lifecycle.md`. Decision-effectiveness metrics feed that review, but the realtime monitor itself still does not switch active pointers.

## Adapter scaffold

The adapter scaffold now has two safe layers:

1. generic subscription planning via `execution_realtime_subscription_plan`;
2. concrete provider/source live-observe fixture planning via `execution_realtime_live_observe_adapter_plan`.

Concrete fixture routes currently cover Alpaca equity/ETF quote/trade/bar/snapshot refs, ThetaData option quote/trade/IV/Greeks/OI refs, OKX crypto ticker/trade/candle/snapshot refs, calendar/event refs, read-only execution account/restriction context refs, and derived model context refs. These are still fixture/shadow routes: they do not open sockets or perform provider/broker calls.

The generic adapter scaffold is planning/validation only:

```bash
PYTHONPATH=src python3 scripts/execution/plan_realtime_capture.py \
  --mode dry_run \
  --source alpaca \
  --model-layer layer_03_target_state_vector \
  --instrument-ref AAPL

PYTHONPATH=src python3 scripts/execution/validate_realtime_capture.py capture.json
```

`dry_run` and `fixture_replay` plans are ready without provider calls. `live_observe` plans remain blocked unless a reviewed `realtime_live_observe_approval` is supplied.

## Execution-owned realtime monitor smoke

`execution_realtime_monitor_smoke_receipt` is the first execution-owned runtime smoke for the 44-symbol ETF monitoring universe. It loads the reviewed Layer 1/2 ETF universe from `trading-storage/main/shared/layer_01_02_market_context_etf_universe.csv`, builds a bounded `realtime_live_observe_approval`, and runs read-only Alpaca snapshot observations only when `--execute-live-observe` is supplied. The universe filter remains Layer 1/2 by default, while the downstream realtime feature/model-decision handoff envelope defaults to the complete Layer 1-9 model input coverage matrix.

The smoke writes a receipt containing request, approval, result, and summary rows. The summary intentionally excludes credentials and provider payload details; it reports provider calls, observation counts, provider status counts, capture counts, and the invariant flags for broker calls, model activation, order construction, and account mutation.

```bash
PYTHONPATH=src python3 scripts/execution/run_realtime_monitor_smoke.py \
  --execute-live-observe \
  --output-path /root/projects/trading-storage/storage/04_execution_artifacts/runtime/realtime_monitor/latest_smoke.json
```

`execution_realtime_monitor_loop_receipt` is the bounded runtime-loop receipt for supervised monitor operation. `scripts/execution/run_realtime_monitor_loop.py` runs repeated smoke cycles, writes per-cycle receipts plus `loop_receipt.json`, preserves reconnect/backoff observability through cycle status/delay fields, and keeps the same hard invariants: no model activation, no order construction/submission, no broker mutation, and no account mutation. Use `--universe-model-layer` to change which rows from the universe CSV are observed, and `--model-layer` only when intentionally narrowing the downstream handoff envelope.

This is still not a production model-decision executor. Decision-effectiveness aggregation is the lightweight quality surface for later shadow/live model review; historical dataset construction remains owned by the historical backfill/promotion pipeline.

The checked-in systemd template is `deploy/systemd/trading-execution-realtime-monitor-loop.service`. It runs one supervised cycle per service start and lets systemd restart it on the configured cadence. By default it omits `--execute-live-observe`, so it writes plan/blocked receipts without provider calls. A host override may set `TRADING_EXECUTION_REALTIME_MONITOR_EXECUTE_LIVE_OBSERVE=1` only after read-only provider observation is reviewed for the runtime window.

## Formal live-observe execution

The first formal realtime integration path is read-only provider observation, not trading. `src/trading_execution/market_data/live_approval.py` validates `realtime_live_observe_approval`; `src/trading_execution/market_data/live_provider.py` executes approved read-only observations and emits `execution_realtime_live_observe_result`.

A valid approval must bound sources, instruments, expiry, and `max_provider_calls`; set `approval_scope=realtime_market_data_observe_only`; set `execute_live_observe_allowed=true`; and keep all mutation/activation flags false:

- `model_activation_allowed=false`
- `broker_execution_allowed=false`
- `broker_order_construction_allowed=false`
- `account_mutation_allowed=false`

Execute only with the explicit flag:

```bash
PYTHONPATH=src python3 scripts/execution/execute_live_observe.py \
  --request live_observe_request.json \
  --approval realtime_live_observe_approval.json \
  --execute-live-observe
```

Supported direct provider observe routes in this first formal slice:

- OKX public REST ticker snapshot for approved crypto instruments.
- Alpaca equity snapshot using `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` environment variables when injected by a service, or the registered source secret JSON at `/root/secrets/alpaca.json` for local OpenClaw-managed runs. Unless `alpaca_data_base_url` / `ALPACA_DATA_BASE_URL` is explicitly supplied, realtime observe defaults to `https://data.alpaca.markets` so broker/trading endpoints in shared secrets are not mistaken for market-data endpoints.
- ThetaData reviewed URL-template HTTP probe when the request supplies `thetadata_url_template`.

The result may contain provider market-data calls and realtime capture rows, then package feature/model-input snapshots for downstream shadow routing. It still does not activate models, persist manager decisions, construct orders, execute broker calls, or mutate accounts.

Concrete fixture planning:

```bash
PYTHONPATH=src python3 scripts/execution/plan_live_observe_adapters.py \
  --mode fixture_replay \
  --instrument-ref AAPL

PYTHONPATH=src python3 scripts/execution/build_realtime_shadow_fixture.py \
  --request-id rtshadow_example \
  --mode fixture_replay \
  --instrument-ref AAPL \
  --decision-time 2026-05-11T13:30:00+00:00 \
  --available-time 2026-05-11T13:30:01+00:00 \
  --tradeable-time 2026-05-11T13:30:02+00:00 \
  --historical-dataset-snapshot-ref trading-model://snapshots/historical/reviewed \
  --frozen-model-config-ref trading-model://configs/frozen/reviewed
```

The shadow fixture bundle contains adapter plans, validated capture-fixture rows, a realtime feature snapshot, and an execution-side model decision input snapshot. It performs zero provider calls, zero model activation, zero broker calls, zero order construction, and zero account mutation.

## Realtime feature and model-decision handoff

Realtime capture is still too raw for the model stack. The accepted handoff chain is:

```text
realtime_capture_contract
  -> realtime_feature_snapshot
  -> execution_model_decision_input_snapshot
  -> historical-model decision stack fixture/shadow route
```

`realtime_feature_snapshot` preserves the same point-in-time timing discipline as historical features: `feature_time <= available_time <= tradeable_time`, plus historical feature parity refs, frozen model/config refs, dataset snapshot refs, source capture refs, and per-layer feature refs. It is not a new training substrate by itself.

`execution_model_decision_input_snapshot` packages all Layer 1-8 feature refs into the shape needed by the historical model decision stack. It is intentionally fixture/shadow-ready only: it does not activate a model, construct an order, mutate an account, or authorize provider streams.

Example:

```bash
PYTHONPATH=src python3 scripts/execution/build_realtime_feature_snapshot.py \
  --decision-time 2026-05-11T13:30:00+00:00 \
  --available-time 2026-05-11T13:30:01+00:00 \
  --tradeable-time 2026-05-11T13:30:02+00:00 \
  --instrument-ref AAPL \
  --historical-dataset-snapshot-ref trading-model://snapshots/historical/unit \
  --frozen-model-config-ref trading-model://configs/frozen/unit \
  --source-capture-ref capture://alpaca/aapl/unit > feature_snapshot.json

PYTHONPATH=src python3 scripts/execution/build_realtime_model_input.py \
  --feature-snapshot feature_snapshot.json > decision_input.json

PYTHONPATH=src python3 scripts/execution/validate_realtime_model_input.py decision_input.json
```

This makes the bridge to historical model data decision routing explicit while keeping live inference/model activation behind later reviewed gates.

## Implementation hook

`src/trading_execution/market_data/contracts.py` owns the side-effect-free `execution_realtime_data_interface`, `execution_realtime_input_coverage`, and `realtime_capture_contract` catalogs. `adapters.py` owns `execution_realtime_subscription_plan` planning; `live_observe.py` owns concrete provider/account/event fixture adapter plans, capture-fixture rows, and execution-side realtime shadow fixture bundles; `capture.py` owns `realtime_capture_validation`; `features.py` owns `realtime_feature_snapshot` and `execution_model_decision_input_snapshot` builders/validators.

The current implementation slice is catalog/contract/fixture handoff only. Later adapters must add:

1. explicit mode (`dry_run`, `paper`, `live_observe`, or equivalent accepted values);
2. entitlement/secret alias resolution without leaking secrets;
3. subscription normalization;
4. heartbeat/reconnect/backoff policy;
5. artifact/manifest/ready-signal emission through accepted manager/storage contracts;
6. tests using fixtures or local fakes, not live provider calls by default.
