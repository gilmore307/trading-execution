# Realtime Market Data Interfaces

Status: accepted initial boundary catalog  
Date: 2026-05-11

## Purpose

Execution needs realtime market observations, but realtime acquisition is not the same interface as historical backfill. The canonical provider/source can be the same while the transport, latency expectation, subscription model, and entitlement behavior differ.

This document defines the first execution-facing realtime data boundary. It does not open sockets, call providers, place orders, or make realtime data the source of historical truth.

## Boundary

- Historical backfill and cleaned historical datasets remain owned by `trading-data`.
- Realtime market observations needed for execution monitoring, risk checks, and order routing belong under `trading-execution` once activated.
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

## Implementation hook

`src/trading_execution/market_data/contracts.py` owns the side-effect-free `execution_realtime_data_interface_v1` catalog.

The first implementation slice is catalog/contract only. Later adapters must add:

1. explicit mode (`dry_run`, `paper`, `live_observe`, or equivalent accepted values);
2. entitlement/secret alias resolution without leaking secrets;
3. subscription normalization;
4. heartbeat/reconnect/backoff policy;
5. artifact/manifest/ready-signal emission through accepted manager/storage contracts;
6. tests using fixtures or local fakes, not live provider calls by default.
