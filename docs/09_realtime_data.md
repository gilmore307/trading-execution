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

## Implementation hook

`src/trading_execution/market_data/contracts.py` owns the side-effect-free `execution_realtime_data_interface_v1`, `execution_realtime_input_coverage_v1`, and `realtime_capture_contract_v1` catalogs.

The first implementation slice is catalog/contract only. Later adapters must add:

1. explicit mode (`dry_run`, `paper`, `live_observe`, or equivalent accepted values);
2. entitlement/secret alias resolution without leaking secrets;
3. subscription normalization;
4. heartbeat/reconnect/backoff policy;
5. artifact/manifest/ready-signal emission through accepted manager/storage contracts;
6. tests using fixtures or local fakes, not live provider calls by default.
