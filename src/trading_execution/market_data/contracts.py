"""Realtime market-data interface and validation coverage catalogs.

The catalog is intentionally descriptive and side-effect free. It records which
existing historical data sources have distinct realtime interfaces that execution
may consume later, and how realtime observations should cover model input and
forward-validation needs, without opening sockets, calling providers, or storing
market data.
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


@dataclass(frozen=True)
class RealtimeModelInputCoverage:
    """Model-layer realtime input coverage requirement."""

    contract_type: str
    model_layer: str
    model_id: str
    model_output: str
    live_input_surface: str
    realtime_input_groups: tuple[str, ...]
    primary_realtime_sources: tuple[str, ...]
    required_capture_fields: tuple[str, ...]
    coverage_status: str
    validation_role: str
    boundary_note: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["realtime_input_groups"] = list(self.realtime_input_groups)
        row["primary_realtime_sources"] = list(self.primary_realtime_sources)
        row["required_capture_fields"] = list(self.required_capture_fields)
        return row


@dataclass(frozen=True)
class RealtimeCaptureContract:
    """Append-only realtime capture contract for forward/shadow validation."""

    contract_type: str
    contract_id: str
    required_fields: tuple[str, ...]
    accepted_dataset_roles: tuple[str, ...]
    forbidden_actions: tuple[str, ...]
    label_maturity_rule: str
    storage_boundary: str
    manager_handoff_refs: tuple[str, ...]
    boundary_note: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["required_fields"] = list(self.required_fields)
        row["accepted_dataset_roles"] = list(self.accepted_dataset_roles)
        row["forbidden_actions"] = list(self.forbidden_actions)
        row["manager_handoff_refs"] = list(self.manager_handoff_refs)
        return row


def realtime_data_interfaces() -> tuple[RealtimeDataInterface, ...]:
    """Return the reviewed execution realtime-data interface catalog."""

    return (
        RealtimeDataInterface(
            contract_type="execution_realtime_data_interface",
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
            contract_type="execution_realtime_data_interface",
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
            contract_type="execution_realtime_data_interface",
            source_id="thetadata",
            canonical_historical_source_id="thetadata",
            execution_use="option_realtime_quote_trade_stream",
            asset_classes=("us_option",),
            realtime_interfaces=("thetadata_terminal_websocket",),
            auth_requirement="local_theta_terminal_and_entitlement_required",
            implementation_status="source_reviewed_adapter_not_started",
            official_docs_url="https://docs.thetadata.us/Streaming/Getting-Started.html",
            boundary_note=(
                "ThetaData realtime options data is terminal/WebSocket oriented and should not be conflated "
                "with the historical REST endpoints used for backfill."
            ),
        ),
    )


def _common_capture_fields() -> tuple[str, ...]:
    return (
        "observation_time",
        "provider_available_time",
        "tradeable_time",
        "source_id",
        "realtime_interface",
        "asset_class",
        "instrument_ref",
        "normalized_payload_ref",
        "run_manifest_ref",
        "artifact_ref",
        "ready_signal_ref",
    )


def realtime_input_coverage_matrix() -> tuple[RealtimeModelInputCoverage, ...]:
    """Return required realtime coverage by model layer.

    Coverage rows are requirements and boundary markers, not adapter enablement.
    Status values intentionally distinguish reviewed interface coverage from
    gaps that still require future adapters, provider policy, or broker/account
    context.
    """

    common = _common_capture_fields()
    validation_role = "live_inference_input_and_forward_validation_after_label_maturity"
    return (
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_01_market_regime",
            model_id="model_01_market_regime",
            model_output="market_context_state",
            live_input_surface="current broad-market context",
            realtime_input_groups=(
                "market_etf_quotes_bars_and_liquidity",
                "volatility_rates_credit_dollar_commodity_proxy_observations",
                "crypto_risk_appetite_proxy_observations",
            ),
            primary_realtime_sources=("alpaca", "okx"),
            required_capture_fields=common,
            coverage_status="partial_route_defined_adapter_not_started_proxy_gap_review_required",
            validation_role=validation_role,
            boundary_note=(
                "Alpaca can cover market/ETF proxy observations and OKX can cover crypto risk-appetite proxies. "
                "Native realtime rates, credit, volatility, dollar, and commodity feeds remain a reviewed gap unless "
                "represented by accepted point-in-time ETF/proxy routes."
            ),
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_02_sector_context",
            model_id="model_02_sector_context",
            model_output="sector_context_state",
            live_input_surface="current sector/industry ETF context",
            realtime_input_groups=(
                "sector_industry_etf_quotes_bars_and_liquidity",
                "relative_strength_inputs",
                "breadth_and_dispersion_proxy_inputs",
            ),
            primary_realtime_sources=("alpaca",),
            required_capture_fields=common,
            coverage_status="route_defined_adapter_not_started",
            validation_role=validation_role,
            boundary_note="Sector context realtime coverage starts from the reviewed Alpaca ETF universe and derived relative-strength inputs.",
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_03_target_state_vector",
            model_id="model_03_target_state_vector",
            model_output="target_context_state",
            live_input_surface="current anonymous target-local tape/liquidity context plus Layer 1/2 state",
            realtime_input_groups=(
                "target_quote_trade_bar_snapshot",
                "target_liquidity_and_spread",
                "market_and_sector_context_refs",
            ),
            primary_realtime_sources=("alpaca", "okx"),
            required_capture_fields=common + ("upstream_context_ref",),
            coverage_status="route_defined_adapter_not_started",
            validation_role=validation_role,
            boundary_note="Target realtime rows must preserve identity-safe model features while retaining audit/routing metadata outside fitting features.",
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_04_alpha_confidence",
            model_id="model_04_alpha_confidence",
            model_output="alpha_confidence_vector",
            live_input_surface="current Layer 1-3 state stack plus Layer 8 event-risk context",
            realtime_input_groups=("market_sector_target_event_context_refs", "freshness_and_quality_diagnostics"),
            primary_realtime_sources=("derived_model_context",),
            required_capture_fields=common + ("upstream_context_ref", "model_output_ref"),
            coverage_status="derived_context_contract_defined_no_direct_provider_route",
            validation_role=validation_role,
            boundary_note="Conceptual Layer 4 does not need a distinct realtime provider feed; it needs fresh Layer 1-3 state plus reviewed Layer 8 event-risk context and label-safe future outcomes for validation.",
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_05_position_projection",
            model_id="model_05_position_projection",
            model_output="position_projection_vector",
            live_input_surface="current/pending position, exposure, cost, and risk-budget context plus Layer 4 alpha",
            realtime_input_groups=(
                "current_position_exposure",
                "pending_order_state",
                "portfolio_risk_budget",
                "current_cost_liquidity_context",
            ),
            primary_realtime_sources=("execution_account_state", "alpaca", "okx", "thetadata"),
            required_capture_fields=common + ("account_context_ref", "pending_order_context_ref", "upstream_context_ref"),
            coverage_status="context_contract_only_broker_account_route_deferred",
            validation_role=validation_role,
            boundary_note="Conceptual Layer 5 position projection cannot be complete until broker/account state contracts exist; market-data pieces remain separate from order mutation.",
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_06_underlying_action",
            model_id="model_06_underlying_action",
            model_output="underlying_action_plan",
            live_input_surface="current underlying action feasibility context plus Layer 5 projection",
            realtime_input_groups=(
                "underlying_quote_liquidity_and_spread",
                "trading_halt_or_restriction_state",
                "borrow_or_short_availability_when_applicable",
                "upstream_position_projection_ref",
            ),
            primary_realtime_sources=("alpaca", "okx", "execution_account_state"),
            required_capture_fields=common + ("restriction_context_ref", "upstream_context_ref"),
            coverage_status="partial_route_defined_restriction_and_account_route_deferred",
            validation_role=validation_role,
            boundary_note="Conceptual Layer 6 feasibility needs current market data plus execution/account restrictions; it still emits a plan, not an order.",
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_07_option_expression",
            model_id="model_07_option_expression",
            model_output="option_expression_plan",
            live_input_surface="current option-chain/expression feasibility context plus Layer 6 thesis",
            realtime_input_groups=(
                "underlying_quote_ref",
                "option_chain_snapshot",
                "option_quote_trade_stream",
                "implied_volatility_and_greeks",
                "open_interest_or_latest_available_interest",
            ),
            primary_realtime_sources=("thetadata", "alpaca"),
            required_capture_fields=common + ("option_contract_ref", "underlying_context_ref", "upstream_context_ref"),
            coverage_status="route_defined_adapter_not_started_terminal_required",
            validation_role=validation_role,
            boundary_note="Conceptual Layer 7 realtime coverage centers on ThetaData option-chain/quote/trade context plus current underlying observations.",
        ),
        RealtimeModelInputCoverage(
            contract_type="execution_realtime_input_coverage",
            model_layer="layer_08_event_risk_governor",
            model_id="model_08_event_risk_governor",
            model_output="event_context_vector",
            live_input_surface="current target/market event context attached to Layer 1-3 state",
            realtime_input_groups=(
                "equity_news_and_event_arrivals",
                "earnings_and_macro_calendar_triggers",
                "abnormal_equity_activity",
                "option_activity_events",
            ),
            primary_realtime_sources=("alpaca", "thetadata", "calendar_discovery"),
            required_capture_fields=common + ("event_time", "event_source_ref", "upstream_context_ref"),
            coverage_status="partial_route_defined_event_adapter_review_required",
            validation_role=validation_role,
            boundary_note="EventRiskGovernor needs news/calendar/activity capture; some routes are trigger/catalog contracts before realtime adapters exist.",
        ),
    )


def realtime_capture_contract() -> RealtimeCaptureContract:
    """Return the append-only capture contract for realtime validation evidence."""

    return RealtimeCaptureContract(
        contract_type="realtime_capture_contract",
        contract_id="execution_realtime_capture_contract",
        required_fields=(
            "capture_id",
            "observation_time",
            "provider_available_time",
            "tradeable_time",
            "source_id",
            "realtime_interface",
            "asset_class",
            "instrument_ref",
            "normalized_payload_ref",
            "frozen_model_config_ref",
            "model_output_ref",
            "dataset_snapshot_ref",
            "dataset_role",
            "label_maturity_time",
            "outcome_label_ref",
            "ingestion_commit_ref",
            "run_manifest_ref",
            "artifact_ref",
            "ready_signal_ref",
        ),
        accepted_dataset_roles=("forward_holdout", "shadow_monitoring"),
        forbidden_actions=(
            "provider_stream_activation",
            "historical_snapshot_rewrite",
            "model_refit_before_reviewed_snapshot_boundary",
            "model_activation",
            "broker_order_construction",
            "broker_order_mutation",
            "account_mutation",
        ),
        label_maturity_rule="outcome labels may attach only after the reviewed horizon has elapsed from tradeable_time",
        storage_boundary="runtime observations stay outside Git and hand off by manager/storage refs, not inline payloads",
        manager_handoff_refs=("manager_request", "run_manifest", "artifact_ref", "ready_signal"),
        boundary_note=(
            "The contract defines what a future adapter must emit for validation. It performs no provider calls, "
            "does not open streams, and does not authorize broker mutation."
        ),
    )


__all__ = [
    "RealtimeCaptureContract",
    "RealtimeDataInterface",
    "RealtimeModelInputCoverage",
    "realtime_capture_contract",
    "realtime_data_interfaces",
    "realtime_input_coverage_matrix",
]
