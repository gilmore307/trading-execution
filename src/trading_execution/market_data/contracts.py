"""Realtime market-data interface catalog for execution runtime.

The catalog is intentionally descriptive and side-effect free. It records which
existing historical data sources have distinct realtime interfaces that execution
may consume later, without opening sockets, calling providers, or storing market
data.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class RealtimeDataInterface:
    """Reviewed realtime interface candidate for an existing data source."""

    contract_type: str
    source_id: str
    canonical_historical_source_id: str
    execution_use: str
    asset_classes: tuple[str, ...]
    realtime_interfaces: tuple[str, ...]
    auth_requirement: str
    implementation_status: str
    official_docs_url: str | None
    boundary_note: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["asset_classes"] = list(self.asset_classes)
        row["realtime_interfaces"] = list(self.realtime_interfaces)
        return row


def realtime_data_interfaces() -> tuple[RealtimeDataInterface, ...]:
    """Return the reviewed execution realtime-data interface catalog."""

    return (
        RealtimeDataInterface(
            contract_type="execution_realtime_data_interface_v1",
            source_id="okx",
            canonical_historical_source_id="okx",
            execution_use="crypto_realtime_market_data",
            asset_classes=("crypto_spot", "crypto_derivative"),
            realtime_interfaces=("okx_public_websocket", "okx_public_rest_snapshot"),
            auth_requirement="public_market_data_without_login_private_account_streams_require_login",
            implementation_status="adapter_scaffold_allowed_no_live_socket_enabled",
            official_docs_url="https://www.okx.com/docs-v5/en/",
            boundary_note=(
                "Same canonical provider as historical OKX data, but realtime execution should use OKX public "
                "WebSocket/REST market-data interfaces rather than historical backfill endpoints."
            ),
        ),
        RealtimeDataInterface(
            contract_type="execution_realtime_data_interface_v1",
            source_id="alpaca",
            canonical_historical_source_id="alpaca",
            execution_use="equity_etf_realtime_market_data",
            asset_classes=("us_equity", "us_etf", "us_option", "crypto"),
            realtime_interfaces=("alpaca_market_data_websocket", "alpaca_market_data_http"),
            auth_requirement="api_key_required_for_streaming_market_data",
            implementation_status="source_reviewed_adapter_not_started",
            official_docs_url="https://docs.alpaca.markets/docs/streaming-market-data",
            boundary_note=(
                "Alpaca can remain a data provider for realtime equity/ETF/option observations even when "
                "broker execution for equities/options is routed elsewhere."
            ),
        ),
        RealtimeDataInterface(
            contract_type="execution_realtime_data_interface_v1",
            source_id="thetadata",
            canonical_historical_source_id="thetadata",
            execution_use="option_realtime_quote_trade_stream",
            asset_classes=("us_option",),
            realtime_interfaces=("thetadata_terminal_websocket" ,),
            auth_requirement="local_theta_terminal_and_entitlement_required",
            implementation_status="source_reviewed_adapter_not_started",
            official_docs_url="https://docs.thetadata.us/Streaming/Getting-Started.html",
            boundary_note=(
                "ThetaData realtime options data is terminal/WebSocket oriented and should not be conflated "
                "with the historical REST endpoints used for backfill."
            ),
        ),
    )


__all__ = ["RealtimeDataInterface", "realtime_data_interfaces"]
