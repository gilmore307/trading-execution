"""Runtime component graph shared by live trading and Replay.

The graph is deliberately component-oriented. Models provide point-in-time
decision inputs, but execution owns the task-level trading lifecycle that turns
those inputs into decision records and broker-neutral order intents.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

RUNTIME_COMPONENT_CONTRACT = "execution_runtime_component"
RUNTIME_COMPONENT_GRAPH_CONTRACT = "execution_runtime_component_graph"

EXECUTION_INTAKE_SNAPSHOT_CONTRACT = "execution_intake_snapshot"
ENTRY_DECISION_CONTRACT = "entry_decision"
POSITION_LIFECYCLE_DECISION_CONTRACT = "position_lifecycle_decision"
OPTION_REEXPRESSION_DECISION_CONTRACT = "option_reexpression_decision"
FAILURE_EXPLANATION_PACKET_CONTRACT = "failure_explanation_packet"
EXECUTION_ORDER_INTENT_CONTRACT = "execution_order_intent"
SIMULATED_FILL_EVENT_CONTRACT = "simulated_fill_event"
ACCOUNT_SLEEVE_CONTRACT = "execution_account_sleeve"

CRYPTO_SPOT_ACCOUNT_SLEEVE = "crypto_spot_account"
EQUITY_OPTIONS_ACCOUNT_SLEEVE = "equity_options_account"
CRYPTO_CANDIDATE_SYMBOLS = ("BTC", "ETH", "SOL")
CRYPTO_SPOT_INSTRUMENT_REFS = ("BTC-USDT", "ETH-USDT", "SOL-USDT")

RuntimeMode = Literal["live", "replay"]


@dataclass(frozen=True)
class RuntimeAccountSleeve:
    """One independently funded execution account sleeve."""

    sleeve_id: str
    sleeve_label: str
    account_state_contract: str
    risk_budget_contract: str
    allowed_asset_classes: tuple[str, ...]
    candidate_pool_policy: str
    candidate_symbols: tuple[str, ...]
    candidate_instrument_refs: tuple[str, ...]
    option_reexpression_enabled: bool
    broker_mutation_allowed: bool = False
    account_mutation_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["contract_type"] = ACCOUNT_SLEEVE_CONTRACT
        row["allowed_asset_classes"] = list(self.allowed_asset_classes)
        row["candidate_symbols"] = list(self.candidate_symbols)
        row["candidate_instrument_refs"] = list(self.candidate_instrument_refs)
        return row


@dataclass(frozen=True)
class RuntimeComponent:
    """One task-level trading component in the execution runtime graph."""

    component_step: str
    component_name: str
    component_id: str
    component_label: str
    purpose: str
    input_contracts: tuple[str, ...]
    output_contracts: tuple[str, ...]
    called_model_layers: tuple[str, ...]
    layer_10_policy: str
    account_sleeves: tuple[str, ...] = (CRYPTO_SPOT_ACCOUNT_SLEEVE, EQUITY_OPTIONS_ACCOUNT_SLEEVE)
    broker_mutation_allowed: bool = False
    account_mutation_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["contract_type"] = RUNTIME_COMPONENT_CONTRACT
        row["input_contracts"] = list(self.input_contracts)
        row["output_contracts"] = list(self.output_contracts)
        row["called_model_layers"] = list(self.called_model_layers)
        row["account_sleeves"] = list(self.account_sleeves)
        return row


def runtime_account_sleeves() -> tuple[RuntimeAccountSleeve, ...]:
    """Return the independent execution account sleeves."""

    return (
        RuntimeAccountSleeve(
            sleeve_id=CRYPTO_SPOT_ACCOUNT_SLEEVE,
            sleeve_label="Crypto Spot Account",
            account_state_contract="crypto_account_state_snapshot",
            risk_budget_contract="crypto_risk_budget_snapshot",
            allowed_asset_classes=("crypto_spot",),
            candidate_pool_policy="fixed_three_asset_crypto_pool",
            candidate_symbols=CRYPTO_CANDIDATE_SYMBOLS,
            candidate_instrument_refs=CRYPTO_SPOT_INSTRUMENT_REFS,
            option_reexpression_enabled=False,
        ),
        RuntimeAccountSleeve(
            sleeve_id=EQUITY_OPTIONS_ACCOUNT_SLEEVE,
            sleeve_label="Equity / Options Account",
            account_state_contract="equity_options_account_state_snapshot",
            risk_budget_contract="equity_options_risk_budget_snapshot",
            allowed_asset_classes=("us_equity", "us_etf", "us_option"),
            candidate_pool_policy="model_selected_from_reviewed_equity_watchlist_and_optionable_underlyings",
            candidate_symbols=(),
            candidate_instrument_refs=(),
            option_reexpression_enabled=True,
        ),
    )


def runtime_components() -> tuple[RuntimeComponent, ...]:
    """Return the accepted live/Replay runtime component graph."""

    return (
        RuntimeComponent(
            component_step="C01",
            component_name="Intake",
            component_id="component_01_intake",
            component_label="C01 Intake",
            purpose=(
                "Read account balance state, current holdings, and watch targets for one account sleeve "
                "before downstream entry and lifecycle components make trading decisions."
            ),
            input_contracts=(
                "market_universe_snapshot",
                "account_sleeve_state_snapshot",
                "position_state_snapshot",
                "market_context_state",
                "sector_context_state",
                "target_context_state",
            ),
            output_contracts=(EXECUTION_INTAKE_SNAPSHOT_CONTRACT,),
            called_model_layers=(
                "layer_01_market_regime",
                "layer_02_sector_context",
                "layer_03_target_state_vector",
            ),
            layer_10_policy="not_called",
        ),
        RuntimeComponent(
            component_step="C02",
            component_name="Entry",
            component_id="component_02_entry",
            component_label="C02 Entry",
            purpose=(
                "Decide whether an allocated target should open an underlying or "
                "option position, remain watch-only, defer, or be blocked."
            ),
            input_contracts=(
                EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
                "account_sleeve_state_snapshot",
                "account_sleeve_risk_budget_snapshot",
                "position_state_snapshot",
                "target_context_state",
                "event_failure_risk_vector",
                "alpha_confidence_vector",
                "dynamic_risk_policy_state",
                "underlying_action_plan",
                "option_expression_plan",
            ),
            output_contracts=(ENTRY_DECISION_CONTRACT,),
            called_model_layers=(
                "layer_03_target_state_vector",
                "layer_04_event_failure_risk",
                "layer_05_alpha_confidence",
                "layer_06_dynamic_risk_policy",
                "layer_08_underlying_action",
                "layer_09_option_expression",
            ),
            layer_10_policy="not_called; pre-entry event risk is handled by layer_04_event_failure_risk",
        ),
        RuntimeComponent(
            component_step="C03",
            component_name="Lifecycle",
            component_id="component_03_lifecycle",
            component_label="C03 Lifecycle",
            purpose=(
                "Manage open positions by deciding hold, add, reduce, exit, stop, "
                "take-profit, or flatten-review actions from current thesis and risk state."
            ),
            input_contracts=(
                "position_state_snapshot",
                "account_sleeve_state_snapshot",
                "account_sleeve_risk_budget_snapshot",
                "market_context_state",
                "entry_decision",
                "event_failure_risk_vector",
                "alpha_confidence_vector",
                "dynamic_risk_policy_state",
                "position_projection_vector",
                "underlying_action_plan",
            ),
            output_contracts=(POSITION_LIFECYCLE_DECISION_CONTRACT,),
            called_model_layers=(
                "layer_04_event_failure_risk",
                "layer_05_alpha_confidence",
                "layer_06_dynamic_risk_policy",
                "layer_07_position_projection",
                "layer_08_underlying_action",
            ),
            layer_10_policy="trigger_component_05_failure_review_only_after_observed_failure_or_abnormal_deviation",
        ),
        RuntimeComponent(
            component_step="C04",
            component_name="Option Review",
            component_id="component_04_option_review",
            component_label="C04 Option Review",
            purpose=(
                "Periodically review held option contracts for moneyness, greeks, "
                "DTE, spread, liquidity, IV, payoff efficiency, and roll cost."
            ),
            input_contracts=(
                "option_position_state_snapshot",
                "underlying_action_plan",
                "option_expression_plan",
                "dynamic_risk_policy_state",
            ),
            output_contracts=(OPTION_REEXPRESSION_DECISION_CONTRACT,),
            called_model_layers=(
                "layer_06_dynamic_risk_policy",
                "layer_08_underlying_action",
                "layer_09_option_expression",
            ),
            account_sleeves=(EQUITY_OPTIONS_ACCOUNT_SLEEVE,),
            layer_10_policy="not_called; abnormal option or model behavior routes to component_05_failure_review",
        ),
        RuntimeComponent(
            component_step="C05",
            component_name="Failure Review",
            component_id="component_05_failure_review",
            component_label="C05 Failure Review",
            purpose=(
                "When model or trade behavior has already failed or deviated, link "
                "the failure evidence to possible unscreened events and produce Layer 4 feedback candidates."
            ),
            input_contracts=(
                "model_failure_observation",
                "trade_failure_observation",
                "actual_vs_expected_performance",
                "unscreened_event_evidence",
                "event_failure_risk_vector",
            ),
            output_contracts=(FAILURE_EXPLANATION_PACKET_CONTRACT,),
            called_model_layers=("layer_10_event_risk_governor",),
            layer_10_policy="called_only_after_observed_model_or_trade_failure",
        ),
        RuntimeComponent(
            component_step="C06",
            component_name="Order Intent",
            component_id="component_06_order_intent",
            component_label="C06 Order Intent",
            purpose=(
                "Convert accepted entry, lifecycle, or option re-expression decisions "
                "into broker-neutral execution order intents."
            ),
            input_contracts=(
                ENTRY_DECISION_CONTRACT,
                POSITION_LIFECYCLE_DECISION_CONTRACT,
                OPTION_REEXPRESSION_DECISION_CONTRACT,
                "account_sleeve_state_snapshot",
                "execution_policy_snapshot",
            ),
            output_contracts=(EXECUTION_ORDER_INTENT_CONTRACT,),
            called_model_layers=(),
            layer_10_policy="not_called",
        ),
        RuntimeComponent(
            component_step="C07",
            component_name="Execution Gate",
            component_id="component_07_execution_gate",
            component_label="C07 Execution Gate",
            purpose=(
                "Apply final execution gates to broker-neutral order intents. Live mode "
                "routes to reviewed broker adapters; Replay mode routes to the fill simulator."
            ),
            input_contracts=(EXECUTION_ORDER_INTENT_CONTRACT, "trade_risk_cap"),
            output_contracts=("broker_order_request", SIMULATED_FILL_EVENT_CONTRACT),
            called_model_layers=(),
            layer_10_policy="not_called",
            broker_mutation_allowed=False,
            account_mutation_allowed=False,
        ),
    )


def build_runtime_component_graph(*, mode: RuntimeMode) -> dict[str, Any]:
    """Build the accepted execution component graph for live or replay mode."""

    if mode not in {"live", "replay"}:
        raise ValueError("mode must be live or replay")

    adapter_profile = (
        {
            "clock": "live_clock",
            "market_data": "live_market_data_adapter",
            "account": "live_account_adapter",
            "execution": "broker_execution_gate",
            "fill": "broker_fill_events",
        }
        if mode == "live"
        else {
            "clock": "historical_clock",
            "market_data": "historical_market_snapshot_adapter",
            "account": "simulated_account_adapter",
            "execution": "simulated_execution_gate",
            "fill": "fill_simulator",
        }
    )

    return {
        "contract_type": RUNTIME_COMPONENT_GRAPH_CONTRACT,
        "mode": mode,
        "component_graph_policy": "same_components_live_and_replay_different_adapters",
        "adapter_profile": adapter_profile,
        "account_sleeve_policy": "separate_crypto_and_equity_options_accounts_no_cross_account_netting",
        "account_sleeves": [sleeve.to_dict() for sleeve in runtime_account_sleeves()],
        "component_order": [component.component_id for component in runtime_components()],
        "component_sequence": [
            {
                "component_step": component.component_step,
                "component_name": component.component_name,
                "component_id": component.component_id,
            }
            for component in runtime_components()
        ],
        "components": [component.to_dict() for component in runtime_components()],
        "required_first_batch_contracts": [
            EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
            ENTRY_DECISION_CONTRACT,
            POSITION_LIFECYCLE_DECISION_CONTRACT,
            EXECUTION_ORDER_INTENT_CONTRACT,
        ],
        "required_second_batch_contracts": [
            OPTION_REEXPRESSION_DECISION_CONTRACT,
            FAILURE_EXPLANATION_PACKET_CONTRACT,
            SIMULATED_FILL_EVENT_CONTRACT,
        ],
        "side_effect_policy": {
            "components_construct_broker_neutral_decisions": True,
            "live_broker_mutation_requires_execution_gate": True,
            "replay_broker_mutation_allowed": False,
            "replay_account_mutation_allowed": False,
            "replay_order_state_mutation_allowed": False,
            "replay_position_state_mutation_allowed": False,
            "replay_uses_simulated_fills": True,
            "cross_account_collateral_or_position_netting_allowed": False,
        },
    }


def validate_same_component_graph(live_graph: dict[str, Any], replay_graph: dict[str, Any]) -> dict[str, Any]:
    """Validate that live and replay use the same component ids and contracts."""

    errors: list[str] = []
    live_components = live_graph.get("components")
    replay_components = replay_graph.get("components")
    live_sleeves = live_graph.get("account_sleeves")
    replay_sleeves = replay_graph.get("account_sleeves")
    if not isinstance(live_components, list) or not isinstance(replay_components, list):
        errors.append("components must be lists")
    else:
        live_shape = [
            (
                row.get("component_step"),
                row.get("component_name"),
                row.get("component_id"),
                tuple(row.get("input_contracts") or ()),
                tuple(row.get("output_contracts") or ()),
            )
            for row in live_components
        ]
        replay_shape = [
            (
                row.get("component_step"),
                row.get("component_name"),
                row.get("component_id"),
                tuple(row.get("input_contracts") or ()),
                tuple(row.get("output_contracts") or ()),
            )
            for row in replay_components
        ]
        if live_shape != replay_shape:
            errors.append("live and replay component shapes must match")

    if not isinstance(live_sleeves, list) or not isinstance(replay_sleeves, list):
        errors.append("account_sleeves must be lists")
    else:
        live_sleeve_shape = [
            (
                row.get("sleeve_id"),
                row.get("account_state_contract"),
                row.get("risk_budget_contract"),
                tuple(row.get("allowed_asset_classes") or ()),
                tuple(row.get("candidate_symbols") or ()),
            )
            for row in live_sleeves
        ]
        replay_sleeve_shape = [
            (
                row.get("sleeve_id"),
                row.get("account_state_contract"),
                row.get("risk_budget_contract"),
                tuple(row.get("allowed_asset_classes") or ()),
                tuple(row.get("candidate_symbols") or ()),
            )
            for row in replay_sleeves
        ]
        if live_sleeve_shape != replay_sleeve_shape:
            errors.append("live and replay account sleeve shapes must match")

    return {
        "contract_type": "execution_runtime_component_graph_validation",
        "validation_status": "passed" if not errors else "failed",
        "errors": errors,
    }
