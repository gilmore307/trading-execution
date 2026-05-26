# Broker and Exchange Interfaces

Status: accepted initial boundary catalog  
Date: 2026-05-11

## Purpose

Execution owns broker/exchange mutation, but broker interfaces are not market-data interfaces. They must be reviewed separately because they can place orders, change positions, and mutate accounts.

This document records the first accepted broker posture:

- OKX is the crypto execution venue because it has an official API.
- Alpaca paper trading is the accepted simulated broker route for US equities, ETFs, and options.
- Firstrade remains deferred because no official trading API is accepted.

## Hard boundary

No broker adapter may construct or place a paper/live order unless all of these are true:

1. the input is an externally promoted/approved decision, not a model draft;
2. `trade_risk_cap` validates successfully;
3. execution mode is explicit;
4. an idempotency key/order-intent id is present;
5. broker-specific credentials resolve through external secret aliases only;
6. order mutation is explicitly enabled by a reviewed decision path;
7. the adapter emits order/fill/position/reconcile artifacts through accepted manager/storage contracts.

The current catalog does not enable live trading. It permits approved order-intent construction without broker submission.

## Formal order-intent construction

`src/trading_execution/broker/order_construction.py` owns `trade_order_construction_approval` and `trade_broker_order_intent`.

`build_broker_order_intent.py` can construct an OKX-shaped order intent only when all of the following pass:

1. reviewed `trade_order_construction_approval` with `approval_scope=broker_order_construction_only`;
2. `construct_order_allowed=true`;
3. `broker_execution_allowed=false` and `account_mutation_allowed=false`;
4. approved instrument, side, order type, and broker;
5. valid `trade_risk_cap` on the decision record.

The resulting intent is `constructed_not_submitted`: it contains the broker-shaped payload and idempotency key, but performs zero broker calls and zero account mutation.

## Reviewed broker interfaces

| Broker/exchange | Asset classes | Official API status | Current status | Mutation enabled |
|---|---|---|---|---|
| OKX | Crypto spot / crypto derivatives | Official API available | Adapter scaffold allowed; live order mutation disabled | No |
| Alpaca paper | US equities / ETFs / options | Official paper Trading API available | Paper adapter allowed behind explicit paper gates; live-money mutation disabled | Paper only |
| Firstrade | US equities / ETFs / options | No official trading API accepted | Deferred; do not automate reverse-engineered login or order flow | No |

## OKX notes from official docs checks

OKX official docs describe authenticated private REST requests with `OK-ACCESS-KEY`, `OK-ACCESS-SIGN`, `OK-ACCESS-TIMESTAMP`, and `OK-ACCESS-PASSPHRASE`. They also describe WebSocket login for private channels and trading/order operations. Public market-data channels are separate from private account/order channels.

Accepted initial OKX development path:

1. catalog and safety contracts;
2. approved order-intent construction without submission;
3. request signing unit tests with fixed fixtures only;
4. paper/simulated order lifecycle;
5. live order mutation only after explicit activation gate.

## Alpaca paper trading posture

Alpaca official documentation says paper trading uses separate paper credentials and the paper API endpoint, normally `https://paper-api.alpaca.markets`; the Trading API spec is shared between paper and live accounts. Alpaca's options documentation also states that options trading is enabled by default in the paper environment, and options orders use the same Orders API with options-specific validations.

Accepted paper path:

1. use only the external `alpaca` secret alias and paper endpoint configuration;
2. require explicit `paper_trading_mode_explicit`;
3. require a promoted or shadow decision ref, valid `trade_risk_cap`, final agent/operator approval, and idempotency key before paper order mutation;
4. emit order/fill/position/reconcile artifacts through accepted manager/storage contracts;
5. keep live-money Alpaca order submission disabled until a separate live activation decision exists.

Paper trading is still broker/order/account mutation. It is allowed only inside the reviewed paper route; server-error repair and ordinary runtime smoke must not submit paper orders as a side effect.

## Firstrade posture

Current web review found reverse-engineered community clients but no accepted official Firstrade trading API. That is not enough for this system.

Do not implement:

- reverse-engineered login;
- browser-driven trading;
- scraped order tickets;
- unofficial order automation;
- credential replay flows.

Firstrade can stay in the broker catalog as `deferred_no_official_trading_api` until an official or explicitly reviewed compliant interface exists.

## Implementation hook

`src/trading_execution/broker/contracts.py` owns the side-effect-free `status_broker_interface` catalog and combined `status_capability_catalog`.

`src/trading_execution/broker/order_construction.py` owns gated order-intent construction. `scripts/execution/build_broker_order_intent.py --construct-order` constructs a broker-shaped intent only after approval and risk-cap validation; it does not submit the order.

`scripts/execution/list_execution_capabilities.py` prints the reviewed catalog without external calls, provider calls, broker calls, order construction, or account mutation.
