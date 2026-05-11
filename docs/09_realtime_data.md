# Realtime Market Data Interfaces

Status: accepted initial boundary catalog  
Date: 2026-05-11

## Purpose

Execution needs realtime market observations, but realtime acquisition is not the same interface as historical backfill. The canonical provider/source can be the same while the transport, latency expectation, subscription model, and entitlement behavior differ.

This document defines the first execution-facing realtime data boundary. It does not open sockets, call providers, place orders, or make realtime data the source of historical truth.

## Boundary

- Historical backfill and cleaned historical datasets remain owned by `trading-data`.
- Realtime market observations needed for execution monitoring, risk checks, and order routing belong under `trading-execution` once activated.
- Realtime observations may feed manager/model shadow or forward-validation evidence only as append-only point-in-time capture with frozen model/config refs; they do not replace the initial historical validation/test split ladder.
- Shared source names, interface terms, and cross-repository contracts must be registered through `trading-manager` before other repos depend on them.
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

Realtime coverage is tracked by `execution_realtime_input_coverage_v1` rows in `src/trading_execution/market_data/contracts.py`. These rows are requirements and gap markers; they do not enable streams.

| Layer | Model output | Realtime input groups | Primary sources | Status |
|---:|---|---|---|---|
| 1 | `market_context_state` | market/ETF quotes, bars, liquidity; volatility/rates/credit/dollar/commodity proxies; crypto risk-appetite proxies | Alpaca, OKX | Partial route defined; proxy/feed gap review still required |
| 2 | `sector_context_state` | sector/industry ETF quotes, bars, liquidity, relative strength, breadth/dispersion proxies | Alpaca | Route defined; adapter not started |
| 3 | `target_context_state` | target quote/trade/bar/snapshot, liquidity/spread, Layer 1/2 context refs | Alpaca, OKX | Route defined; adapter not started |
| 4 | `event_context_vector` | news/event arrivals, earnings/macro triggers, abnormal equity activity, option activity events | Alpaca, ThetaData, calendar discovery | Partial route defined; event adapter review required |
| 5 | `alpha_confidence_vector` | current Layer 1-4 state stack and freshness/quality diagnostics | derived model context | Contract defined; no direct provider route |
| 6 | `position_projection_vector` | current/pending position, exposure, risk budget, current cost/liquidity context | execution account state, Alpaca, OKX, ThetaData | Context contract only; broker/account route deferred |
| 7 | `underlying_action_plan` | underlying quote/liquidity/spread, restrictions/halt/borrow state, Layer 6 projection ref | Alpaca, OKX, execution account state | Partial route defined; restriction/account route deferred |
| 8 | `option_expression_plan` | underlying quote, option-chain snapshot, option quote/trade stream, IV/Greeks, OI/latest interest | ThetaData, Alpaca | Route defined; adapter not started; terminal required |

The matrix intentionally exposes gaps. A partial row is not a failure; it prevents us from pretending that realtime coverage is complete before a provider, account-state, or restriction route is accepted.

## Realtime capture contract

`realtime_capture_contract_v1` is the append-only evidence shape for future realtime forward-validation and shadow-monitoring capture. Required facts include observation time, provider available time, tradeable time, source/interface, instrument ref, normalized payload ref, frozen model/config refs, model output refs, dataset role, label maturity time, outcome label refs, and manager/storage handoff refs.

Accepted dataset roles are `forward_holdout` and `shadow_monitoring`. The contract forbids provider-stream activation by catalog inspection, historical snapshot rewrites, model refit before reviewed snapshot boundaries, model activation, broker order construction, broker order mutation, and account mutation.

## Adapter scaffold

The adapter scaffold now has two safe layers:

1. generic subscription planning via `execution_realtime_subscription_plan_v1`;
2. concrete provider/source live-observe fixture planning via `execution_realtime_live_observe_adapter_plan_v1`.

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

`dry_run` and `fixture_replay` plans are ready without provider calls. `live_observe` plans remain blocked unless a reviewed `realtime_live_observe_approval_v1` is supplied.

## Formal live-observe execution

The first formal realtime integration path is read-only provider observation, not trading. `src/trading_execution/market_data/live_approval.py` validates `realtime_live_observe_approval_v1`; `src/trading_execution/market_data/live_provider.py` executes approved read-only observations and emits `execution_realtime_live_observe_result_v1`.

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
- Alpaca equity snapshot using `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` environment variables.
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
realtime_capture_contract_v1
  -> realtime_feature_snapshot_v1
  -> execution_model_decision_input_snapshot_v1
  -> historical-model decision stack fixture/shadow route
```

`realtime_feature_snapshot_v1` preserves the same point-in-time timing discipline as historical features: `feature_time <= available_time <= tradeable_time`, plus historical feature parity refs, frozen model/config refs, dataset snapshot refs, source capture refs, and per-layer feature refs. It is not a new training substrate by itself.

`execution_model_decision_input_snapshot_v1` packages all Layer 1-8 feature refs into the shape needed by the historical model decision stack. It is intentionally fixture/shadow-ready only: it does not activate a model, construct an order, mutate an account, or authorize provider streams.

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

`src/trading_execution/market_data/contracts.py` owns the side-effect-free `execution_realtime_data_interface_v1`, `execution_realtime_input_coverage_v1`, and `realtime_capture_contract_v1` catalogs. `adapters.py` owns `execution_realtime_subscription_plan_v1` planning; `live_observe.py` owns concrete provider/account/event fixture adapter plans, capture-fixture rows, and execution-side realtime shadow fixture bundles; `capture.py` owns `realtime_capture_validation_v1`; `features.py` owns `realtime_feature_snapshot_v1` and `execution_model_decision_input_snapshot_v1` builders/validators.

The current implementation slice is catalog/contract/fixture handoff only. Later adapters must add:

1. explicit mode (`dry_run`, `paper`, `live_observe`, or equivalent accepted values);
2. entitlement/secret alias resolution without leaking secrets;
3. subscription normalization;
4. heartbeat/reconnect/backoff policy;
5. artifact/manifest/ready-signal emission through accepted manager/storage contracts;
6. tests using fixtures or local fakes, not live provider calls by default.
