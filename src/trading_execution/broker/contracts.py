"""Broker/exchange execution interface catalog.

This module records accepted broker-interface posture without constructing
orders or calling broker APIs. Live mutation remains disabled until a later
adapter implements explicit mode gates and risk-cap validation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class BrokerInterface:
    """Reviewed broker/exchange execution route."""

    contract_type: str
    broker_id: str
    execution_use: str
    asset_classes: tuple[str, ...]
    official_api_status: str
    interface_kind: str
    credential_alias: str | None
    implementation_status: str
    order_mutation_enabled: bool
    required_pre_order_gates: tuple[str, ...]
    official_docs_url: str | None
    boundary_note: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["asset_classes"] = list(self.asset_classes)
        row["required_pre_order_gates"] = list(self.required_pre_order_gates)
        return row


def broker_interfaces() -> tuple[BrokerInterface, ...]:
    """Return the reviewed broker/exchange execution interface catalog."""

    return (
        BrokerInterface(
            contract_type="execution_broker_interface",
            broker_id="okx",
            execution_use="crypto_order_execution",
            asset_classes=("crypto_spot", "crypto_derivative"),
            official_api_status="official_api_available",
            interface_kind="okx_rest_private_and_websocket_private_trade_api",
            credential_alias="okx",
            implementation_status="adapter_scaffold_allowed_live_order_mutation_disabled",
            order_mutation_enabled=False,
            required_pre_order_gates=(
                "promoted_decision_ref",
                "trade_risk_cap_valid",
                "execution_mode_explicit",
                "idempotency_key",
                "operator_or_agent_execution_approval",
            ),
            official_docs_url="https://www.okx.com/docs-v5/en/",
            boundary_note=(
                "OKX is the accepted crypto execution venue because it has an official API. The first adapter "
                "may validate/simulate orders, but live order placement must stay disabled until explicit mode "
                "and approval gates are implemented."
            ),
        ),
        BrokerInterface(
            contract_type="execution_broker_interface",
            broker_id="alpaca_paper",
            execution_use="us_equity_etf_option_paper_order_execution",
            asset_classes=("us_equity", "us_etf", "us_option"),
            official_api_status="official_paper_trading_api_available",
            interface_kind="alpaca_trading_api_paper_endpoint",
            credential_alias="alpaca",
            implementation_status="paper_adapter_allowed_live_money_order_mutation_disabled",
            order_mutation_enabled=True,
            required_pre_order_gates=(
                "paper_trading_mode_explicit",
                "promoted_or_shadow_decision_ref",
                "trade_risk_cap_valid",
                "agent_final_review_approved",
                "idempotency_key",
            ),
            official_docs_url="https://docs.alpaca.markets/v1.4.2/docs/paper-trading",
            boundary_note=(
                "Alpaca paper trading is the accepted simulated broker route for US equities, ETFs, and options. "
                "It must use paper credentials and the paper endpoint only; live-money Alpaca order submission remains disabled."
            ),
        ),
        BrokerInterface(
            contract_type="execution_broker_interface",
            broker_id="firstrade",
            execution_use="us_equity_and_option_order_execution",
            asset_classes=("us_equity", "us_etf", "us_option"),
            official_api_status="no_official_api_found",
            interface_kind="deferred_no_official_trading_api",
            credential_alias=None,
            implementation_status="deferred_do_not_automate_reverse_engineered_login_or_order_flow",
            order_mutation_enabled=False,
            required_pre_order_gates=("official_or_reviewed_interface_required",),
            official_docs_url=None,
            boundary_note=(
                "Firstrade is the intended equity/options broker, but no official trading API is accepted. "
                "Do not implement reverse-engineered login, browser trading, or unofficial order automation here."
            ),
        ),
    )


def build_execution_capability_catalog() -> dict[str, Any]:
    """Return a combined side-effect-free execution capability catalog."""

    from trading_execution.market_data import (
        model_decision_input_snapshot_contract,
        realtime_capture_contract,
        realtime_data_interfaces,
        realtime_feature_snapshot_contract,
        realtime_input_coverage_matrix,
    )

    return {
        "contract_type": "execution_capability_catalog",
        "realtime_data_interfaces": [interface.summary_row() for interface in realtime_data_interfaces()],
        "realtime_input_coverage_matrix": [coverage.summary_row() for coverage in realtime_input_coverage_matrix()],
        "realtime_capture_contract": realtime_capture_contract().summary_row(),
        "realtime_feature_snapshot_contract": realtime_feature_snapshot_contract(),
        "model_decision_input_snapshot_contract": model_decision_input_snapshot_contract(),
        "broker_interfaces": [interface.summary_row() for interface in broker_interfaces()],
        "order_mutation_enabled": False,
        "paper_order_mutation_enabled": any(interface.broker_id == "alpaca_paper" and interface.order_mutation_enabled for interface in broker_interfaces()),
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
    }


__all__ = ["BrokerInterface", "broker_interfaces", "build_execution_capability_catalog"]
