"""Runtime component graph shared by live trading and replay.

The graph is deliberately component-oriented. Models provide point-in-time
decision inputs, but execution owns the task-level trading lifecycle that turns
those inputs into decision records and broker-neutral order intents.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

RUNTIME_COMPONENT_CONTRACT = "execution_runtime_component"
RUNTIME_COMPONENT_GRAPH_CONTRACT = "execution_runtime_component_graph"

TARGET_ALLOCATION_SNAPSHOT_CONTRACT = "target_allocation_snapshot"
ENTRY_DECISION_CONTRACT = "entry_decision"
POSITION_LIFECYCLE_DECISION_CONTRACT = "position_lifecycle_decision"
OPTION_REEXPRESSION_DECISION_CONTRACT = "option_reexpression_decision"
FAILURE_EXPLANATION_PACKET_CONTRACT = "failure_explanation_packet"
EXECUTION_ORDER_INTENT_CONTRACT = "execution_order_intent"
SIMULATED_FILL_EVENT_CONTRACT = "simulated_fill_event"

RuntimeMode = Literal["live", "replay"]


@dataclass(frozen=True)
class RuntimeComponent:
    """One task-level trading component in the execution runtime graph."""

    component_id: str
    component_label: str
    purpose: str
    input_contracts: tuple[str, ...]
    output_contracts: tuple[str, ...]
    called_model_layers: tuple[str, ...]
    layer_10_policy: str
    broker_mutation_allowed: bool = False
    account_mutation_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["contract_type"] = RUNTIME_COMPONENT_CONTRACT
        row["input_contracts"] = list(self.input_contracts)
        row["output_contracts"] = list(self.output_contracts)
        row["called_model_layers"] = list(self.called_model_layers)
        return row


def runtime_components() -> tuple[RuntimeComponent, ...]:
    """Return the accepted live/replay runtime component graph."""

    return (
        RuntimeComponent(
            component_id="opportunity_risk_allocation_engine",
            component_label="Opportunity & Risk Allocation Engine",
            purpose=(
                "Select the current target pool and pre-allocate risk budget from "
                "market, sector, target-state, account, and existing-position evidence."
            ),
            input_contracts=(
                "market_universe_snapshot",
                "account_state_snapshot",
                "position_state_snapshot",
                "market_context_state",
                "sector_context_state",
                "target_context_state",
                "dynamic_risk_policy_state",
            ),
            output_contracts=(TARGET_ALLOCATION_SNAPSHOT_CONTRACT,),
            called_model_layers=(
                "layer_01_market_regime",
                "layer_02_sector_context",
                "layer_03_target_state_vector",
                "layer_06_dynamic_risk_policy",
            ),
            layer_10_policy="not_called",
        ),
        RuntimeComponent(
            component_id="entry_decision_engine",
            component_label="Entry Decision Engine",
            purpose=(
                "Decide whether an allocated target should open an underlying or "
                "option position, remain watch-only, defer, or be blocked."
            ),
            input_contracts=(
                TARGET_ALLOCATION_SNAPSHOT_CONTRACT,
                "account_state_snapshot",
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
            component_id="position_lifecycle_controller",
            component_label="Position Lifecycle Controller",
            purpose=(
                "Manage open positions by deciding hold, add, reduce, exit, stop, "
                "take-profit, or flatten-review actions from current thesis and risk state."
            ),
            input_contracts=(
                "position_state_snapshot",
                "account_state_snapshot",
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
            layer_10_policy="trigger_failure_explanation_component_only_after_observed_failure_or_abnormal_deviation",
        ),
        RuntimeComponent(
            component_id="option_reexpression_review",
            component_label="Option Re-Expression Review",
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
            layer_10_policy="not_called; abnormal option or model behavior routes to failure_explanation_component",
        ),
        RuntimeComponent(
            component_id="failure_explanation_component",
            component_label="Failure Explanation Component",
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
            component_id="order_intent_builder",
            component_label="Order Intent Builder",
            purpose=(
                "Convert accepted entry, lifecycle, or option re-expression decisions "
                "into broker-neutral execution order intents."
            ),
            input_contracts=(
                ENTRY_DECISION_CONTRACT,
                POSITION_LIFECYCLE_DECISION_CONTRACT,
                OPTION_REEXPRESSION_DECISION_CONTRACT,
                "account_state_snapshot",
                "execution_policy_snapshot",
            ),
            output_contracts=(EXECUTION_ORDER_INTENT_CONTRACT,),
            called_model_layers=(),
            layer_10_policy="not_called",
        ),
        RuntimeComponent(
            component_id="execution_gate_adapter",
            component_label="Execution Gate / Adapter",
            purpose=(
                "Apply final execution gates to broker-neutral order intents. Live mode "
                "routes to reviewed broker adapters; replay mode routes to the fill simulator."
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
        "component_order": [component.component_id for component in runtime_components()],
        "components": [component.to_dict() for component in runtime_components()],
        "required_first_batch_contracts": [
            TARGET_ALLOCATION_SNAPSHOT_CONTRACT,
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
            "replay_uses_simulated_fills": True,
        },
    }


def validate_same_component_graph(live_graph: dict[str, Any], replay_graph: dict[str, Any]) -> dict[str, Any]:
    """Validate that live and replay use the same component ids and contracts."""

    errors: list[str] = []
    live_components = live_graph.get("components")
    replay_components = replay_graph.get("components")
    if not isinstance(live_components, list) or not isinstance(replay_components, list):
        errors.append("components must be lists")
    else:
        live_shape = [
            (row.get("component_id"), tuple(row.get("input_contracts") or ()), tuple(row.get("output_contracts") or ()))
            for row in live_components
        ]
        replay_shape = [
            (row.get("component_id"), tuple(row.get("input_contracts") or ()), tuple(row.get("output_contracts") or ()))
            for row in replay_components
        ]
        if live_shape != replay_shape:
            errors.append("live and replay component shapes must match")

    return {
        "contract_type": "execution_runtime_component_graph_validation",
        "validation_status": "passed" if not errors else "failed",
        "errors": errors,
    }

