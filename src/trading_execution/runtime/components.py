"""Runtime component graph shared by live trading and Replay.

The graph is deliberately component-oriented. Models provide point-in-time
decision inputs, but execution owns the task-level trading lifecycle that turns
those inputs into decision records and broker-neutral order intents.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, Literal

RUNTIME_COMPONENT_CONTRACT = "execution_runtime_component"
RUNTIME_COMPONENT_GRAPH_CONTRACT = "execution_runtime_component_graph"
RUNTIME_COMPONENT_MANIFEST_CONTRACT = "execution_runtime_component_manifest"
RUNTIME_COMPONENT_MANIFEST_VERSION = "2026-07-06"

EXECUTION_INTAKE_SNAPSHOT_CONTRACT = "execution_intake_snapshot"
ENTRY_DECISION_CONTRACT = "entry_decision"
POSITION_LIFECYCLE_DECISION_CONTRACT = "position_lifecycle_decision"
EXPRESSION_DECISION_CONTRACT = "expression_decision"
FAILURE_EXPLANATION_PACKET_CONTRACT = "failure_explanation_packet"
EXECUTION_ORDER_INTENT_CONTRACT = "execution_order_intent"
EXECUTION_GATE_RESULT_CONTRACT = "execution_gate_result"
SIMULATED_FILL_EVENT_CONTRACT = "simulated_fill_event"
ACCOUNT_SLEEVE_CONTRACT = "execution_account_sleeve"

CRYPTO_LEVERAGE_ACCOUNT_SLEEVE = "crypto_leverage_account"
EQUITY_OPTIONS_ACCOUNT_SLEEVE = "equity_options_account"
CRYPTO_CANDIDATE_SYMBOLS = ("BTC", "ETH", "SOL")
CRYPTO_UNDERLYING_INSTRUMENT_REFS = ("BTC-USDT", "ETH-USDT", "SOL-USDT")
CRYPTO_LEVERAGE_INSTRUMENT_REFS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
CRYPTO_LEVERAGE_MIN_MULTIPLE = 2
CRYPTO_LEVERAGE_MAX_MULTIPLE = 50
CRYPTO_LEVERAGE_STARTING_CAPITAL_USD = 5000.0
EXPRESSION_REVIEW_COMPONENT_ID = "component_04_expression_review"
RUNTIME_COMPONENT_ORDER = (
    "component_01_intake",
    "component_02_entry",
    "component_03_lifecycle",
    EXPRESSION_REVIEW_COMPONENT_ID,
    "component_05_order_intent",
    "component_06_execution_gate",
    "component_07_failure_review",
)
REQUIRED_RUNTIME_COMPONENT_ORDER = (
    "component_01_intake",
    "component_02_entry",
    "component_03_lifecycle",
    "component_05_order_intent",
    "component_06_execution_gate",
)
OPTIONAL_RUNTIME_COMPONENT_ORDER = (
    EXPRESSION_REVIEW_COMPONENT_ID,
    "component_07_failure_review",
)

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
    expression_review_enabled: bool
    starting_capital_usd: float | None = None
    leverage_policy: dict[str, Any] | None = None
    broker_mutation_allowed: bool = False
    account_mutation_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["contract_type"] = ACCOUNT_SLEEVE_CONTRACT
        row["allowed_asset_classes"] = list(self.allowed_asset_classes)
        row["candidate_symbols"] = list(self.candidate_symbols)
        row["candidate_instrument_refs"] = list(self.candidate_instrument_refs)
        row["leverage_policy"] = dict(self.leverage_policy or {})
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
    required_model_surfaces: tuple[str, ...]
    optional_model_surfaces: tuple[str, ...]
    live_invocation_policy: str
    replay_invocation_policy: str
    skip_degrade_policy: str
    forbidden_recomputations: tuple[str, ...]
    account_sleeves: tuple[str, ...] = (CRYPTO_LEVERAGE_ACCOUNT_SLEEVE, EQUITY_OPTIONS_ACCOUNT_SLEEVE)
    broker_mutation_allowed: bool = False
    account_mutation_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["contract_type"] = RUNTIME_COMPONENT_CONTRACT
        row["input_contracts"] = list(self.input_contracts)
        row["output_contracts"] = list(self.output_contracts)
        row["required_model_surfaces"] = list(self.required_model_surfaces)
        row["optional_model_surfaces"] = list(self.optional_model_surfaces)
        row["forbidden_recomputations"] = list(self.forbidden_recomputations)
        row["account_sleeves"] = list(self.account_sleeves)
        return row


def runtime_account_sleeves() -> tuple[RuntimeAccountSleeve, ...]:
    """Return the independent execution account sleeves."""

    return (
        RuntimeAccountSleeve(
            sleeve_id=CRYPTO_LEVERAGE_ACCOUNT_SLEEVE,
            sleeve_label="Crypto Leverage Account",
            account_state_contract="crypto_account_state_snapshot",
            risk_budget_contract="crypto_risk_budget_snapshot",
            allowed_asset_classes=("crypto_underlying", "crypto_perp"),
            candidate_pool_policy="fixed_three_asset_crypto_leverage_pool",
            candidate_symbols=CRYPTO_CANDIDATE_SYMBOLS,
            candidate_instrument_refs=CRYPTO_LEVERAGE_INSTRUMENT_REFS,
            expression_review_enabled=True,
            starting_capital_usd=CRYPTO_LEVERAGE_STARTING_CAPITAL_USD,
            leverage_policy={
                "owner_component": EXPRESSION_REVIEW_COMPONENT_ID,
                "min_leverage": CRYPTO_LEVERAGE_MIN_MULTIPLE,
                "max_leverage": CRYPTO_LEVERAGE_MAX_MULTIPLE,
                "default_margin_mode": "isolated",
                "order_intent_owner": "component_05_order_intent",
                "hard_gate_owner": "component_06_execution_gate",
            },
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
            expression_review_enabled=True,
        ),
    )


def runtime_components() -> tuple[RuntimeComponent, ...]:
    """Return the accepted use-case runtime components for live and Replay."""

    no_model_recompute = (
        "model_surface_estimation",
        "model_training",
        "model_promotion",
        "broker_or_account_mutation",
    )

    return (
        RuntimeComponent(
            component_step="C01",
            component_name="Intake",
            component_id="component_01_intake",
            component_label="C01 Intake",
            purpose=(
                "Read account balance state, current holdings, target candidates, and remaining sector "
                "opportunity mix for one account sleeve, then split the minute into a candidate entry "
                "pool for C02 and an open-position pool for C03."
            ),
            input_contracts=(
                "market_universe_snapshot",
                "account_sleeve_state_snapshot",
                "position_state_snapshot",
                "background_context_state",
                "target_context_state",
            ),
            output_contracts=(EXECUTION_INTAKE_SNAPSHOT_CONTRACT,),
            required_model_surfaces=("model_01_background_context", "model_02_target_state"),
            optional_model_surfaces=(),
            live_invocation_policy="required_each_decision_minute_for_account_sleeve_intake",
            replay_invocation_policy="required_for_each_replay_minute_with_constructed_intake_inputs",
            skip_degrade_policy="block_downstream_entry_and_lifecycle_when_required_intake_surfaces_are_missing",
            forbidden_recomputations=no_model_recompute + ("entry_thesis_decision", "position_lifecycle_decision"),
        ),
        RuntimeComponent(
            component_step="C02",
            component_name="Entry",
            component_id="component_02_entry",
            component_label="C02 Entry",
            purpose=(
                "Evaluate each target in the C01 candidate entry pool and decide which targets have "
                "a suitable underlying entry thesis for continued expression review."
            ),
            input_contracts=(
                EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
                "event_state_vector",
                "thesis_distribution_surface",
                "direct_underlying_intent",
            ),
            output_contracts=(ENTRY_DECISION_CONTRACT,),
            required_model_surfaces=("model_03_event_state", "model_04_unified_decision"),
            optional_model_surfaces=(),
            live_invocation_policy="required_for_candidates_in_candidate_entry_pool",
            replay_invocation_policy="required_for_replay_candidate_entry_pool_rows",
            skip_degrade_policy="emit_deferred_or_rejected_entry_decision_when_required_surfaces_are_missing_or_stale",
            forbidden_recomputations=no_model_recompute + ("option_expression_selection", "final_order_sizing"),
        ),
        RuntimeComponent(
            component_step="C03",
            component_name="Lifecycle",
            component_id="component_03_lifecycle",
            component_label="C03 Lifecycle",
            purpose=(
                "Manage already-open positions in underlying-thesis terms, deciding hold, "
                "reduce, exit, stop, take-profit, or flatten-review actions with "
                "model-stop, reason-evidence, and portfolio constraint checks."
            ),
            input_contracts=(
                "position_state_snapshot",
                "account_sleeve_state_snapshot",
                "account_sleeve_risk_budget_snapshot",
                "event_state_vector",
                "thesis_distribution_surface",
                "direct_underlying_intent",
            ),
            output_contracts=(POSITION_LIFECYCLE_DECISION_CONTRACT,),
            required_model_surfaces=("model_03_event_state", "model_04_unified_decision"),
            optional_model_surfaces=(),
            live_invocation_policy="required_for_open_positions_in_open_position_pool",
            replay_invocation_policy="required_for_replay_open_position_pool_rows",
            skip_degrade_policy="emit_hold_or_review_required_lifecycle_decision_when_required_surfaces_are_missing_or_stale",
            forbidden_recomputations=no_model_recompute + ("new_entry_discovery", "option_expression_selection", "final_order_sizing"),
        ),
        RuntimeComponent(
            component_step="C04",
            component_name="Expression Review",
            component_id=EXPRESSION_REVIEW_COMPONENT_ID,
            component_label="C04 Expression Review",
            purpose=(
                "Periodically review held option contracts for moneyness, greeks, "
                "DTE, spread, liquidity, IV, payoff efficiency, and roll cost, or translate accepted "
                "C02/C03 underlying intents into the current option, direct-underlying fallback, or crypto leveraged expression."
            ),
            input_contracts=(
                "option_position_state_snapshot",
                "entry_decision",
                "position_lifecycle_decision",
                "expression_probability_surface",
                "option_expression_plan",
            ),
            output_contracts=(EXPRESSION_DECISION_CONTRACT,),
            required_model_surfaces=(),
            optional_model_surfaces=("model_05_option_expression",),
            live_invocation_policy="conditional_for_optionable_routes_crypto_leverage_routes_held_options_or_expression_required_underlying_intents",
            replay_invocation_policy="conditional_but_replay_records_direct_underlying_crypto_leverage_or_not_option_applicable_state",
            skip_degrade_policy="emit_direct_underlying_crypto_leverage_or_not_option_applicable_expression_without_fabricating_option_selection",
            forbidden_recomputations=no_model_recompute + ("direct_underlying_decision", "final_order_sizing"),
            account_sleeves=(CRYPTO_LEVERAGE_ACCOUNT_SLEEVE, EQUITY_OPTIONS_ACCOUNT_SLEEVE),
        ),
        RuntimeComponent(
            component_step="C05",
            component_name="Order Intent",
            component_id="component_05_order_intent",
            component_label="C05 Order Intent",
            purpose=(
                "Convert accepted entry, lifecycle, or expression review decisions into complete "
                "broker-neutral execution order intents, including final quantity, target post-trade "
                "position, risk-cap packaging, and price/order policy."
            ),
            input_contracts=(
                ENTRY_DECISION_CONTRACT,
                POSITION_LIFECYCLE_DECISION_CONTRACT,
                EXPRESSION_DECISION_CONTRACT,
                "account_sleeve_state_snapshot",
                "account_sleeve_risk_budget_snapshot",
                "position_sizing_context",
                "trade_risk_cap",
                "execution_policy_snapshot",
            ),
            output_contracts=(EXECUTION_ORDER_INTENT_CONTRACT,),
            required_model_surfaces=(),
            optional_model_surfaces=(),
            live_invocation_policy="required_after_accepted_entry_lifecycle_or_expression_decision",
            replay_invocation_policy="required_after_replay_accepted_entry_lifecycle_or_expression_decision",
            skip_degrade_policy="block_order_intent_when_source_decision_or_sizing_context_is_missing",
            forbidden_recomputations=no_model_recompute + ("model_decision_rewrite", "execution_gate_result"),
        ),
        RuntimeComponent(
            component_step="C06",
            component_name="Execution Gate",
            component_id="component_06_execution_gate",
            component_label="C06 Execution Gate",
            purpose=(
                "Apply final execution gates to broker-neutral order intents. Live mode "
                "routes to reviewed broker adapters; Replay mode routes to the fill simulator."
            ),
            input_contracts=(
                EXECUTION_ORDER_INTENT_CONTRACT,
                "agent_final_review",
                "execution_hard_block_checks",
            ),
            output_contracts=(EXECUTION_GATE_RESULT_CONTRACT, "broker_order_request", SIMULATED_FILL_EVENT_CONTRACT),
            required_model_surfaces=(),
            optional_model_surfaces=(),
            live_invocation_policy="required_before_live_broker_submission_candidate_after_review_gates",
            replay_invocation_policy="required_before_replay_fill_simulation",
            skip_degrade_policy="reject_or_hold_execution_when_gate_inputs_or_required_reviews_are_missing",
            forbidden_recomputations=no_model_recompute + ("order_quantity_recalculation", "strategy_decision_rewrite"),
            broker_mutation_allowed=False,
            account_mutation_allowed=False,
        ),
        RuntimeComponent(
            component_step="C07",
            component_name="Failure Review",
            component_id="component_07_failure_review",
            component_label="C07 Failure Review",
            purpose=(
                "When model or trade behavior has already failed or deviated, link "
                "the failure evidence to possible unscreened events and produce future model feedback candidates."
            ),
            input_contracts=(
                "model_failure_observation",
                "trade_failure_observation",
                "actual_vs_expected_performance",
                "unscreened_event_evidence",
            ),
            output_contracts=(FAILURE_EXPLANATION_PACKET_CONTRACT,),
            required_model_surfaces=(),
            optional_model_surfaces=(),
            live_invocation_policy="conditional_after_observed_model_or_trade_failure_deviation_or_residual_event_evidence",
            replay_invocation_policy="conditional_after_replay_failure_deviation_or_settlement_attribution_evidence",
            skip_degrade_policy="emit_unattributed_or_review_required_failure_packet_when_residual_event_context_is_missing",
            forbidden_recomputations=no_model_recompute + ("same_fold_upstream_feature_mutation", "live_order_instruction"),
        ),
    )


def runtime_use_case_graphs() -> tuple[dict[str, Any], ...]:
    """Return use-case execution graphs built from the runtime components."""

    return (
        {
            "use_case_id": "candidate_entry_execution",
            "description": "New opportunity candidates from C01 are evaluated by C02 before expression, sizing, and execution gate review.",
            "source_component_id": "component_01_intake",
            "source_pool": "candidate_entry_pool",
            "component_ids": [
                "component_01_intake",
                "component_02_entry",
                EXPRESSION_REVIEW_COMPONENT_ID,
                "component_05_order_intent",
                "component_06_execution_gate",
            ],
        },
        {
            "use_case_id": "open_position_lifecycle_execution",
            "description": "Existing positions from C01 bypass C02 and are managed by C03 before expression, sizing, and execution gate review.",
            "source_component_id": "component_01_intake",
            "source_pool": "open_position_pool",
            "component_ids": [
                "component_01_intake",
                "component_03_lifecycle",
                EXPRESSION_REVIEW_COMPONENT_ID,
                "component_05_order_intent",
                "component_06_execution_gate",
            ],
        },
        {
            "use_case_id": "direct_underlying_execution",
            "description": "Accepted direct-underlying and crypto leveraged intents pass through C04 expression review without requiring listed-option model output.",
            "source_component_id": EXPRESSION_REVIEW_COMPONENT_ID,
            "source_pool": "accepted_underlying_intents",
            "component_ids": [EXPRESSION_REVIEW_COMPONENT_ID, "component_05_order_intent", "component_06_execution_gate"],
        },
        {
            "use_case_id": "option_or_crypto_expression_execution",
            "description": "Optionable routes, held options, and crypto leveraged routes use C04 expression review with M05 expression evidence when available.",
            "source_component_id": EXPRESSION_REVIEW_COMPONENT_ID,
            "source_pool": "optionable_crypto_or_held_option_intents",
            "component_ids": [EXPRESSION_REVIEW_COMPONENT_ID, "component_05_order_intent", "component_06_execution_gate"],
        },
        {
            "use_case_id": "failure_diagnosis",
            "description": "Observed model or trade failures route to C07 for post-failure explanation and future model feedback evidence.",
            "source_component_id": "observed_model_or_trade_failure",
            "source_pool": "failure_observations",
            "component_ids": ["component_07_failure_review"],
        },
    )


def _adapter_profile(*, mode: RuntimeMode) -> dict[str, str]:
    if mode == "live":
        return {
            "clock": "live_clock",
            "market_data": "live_market_data_adapter",
            "account": "live_account_adapter",
            "execution": "broker_execution_gate",
            "fill": "broker_fill_events",
        }
    if mode == "replay":
        return {
            "clock": "historical_clock",
            "market_data": "historical_market_snapshot_adapter",
            "account": "simulated_account_adapter",
            "execution": "simulated_execution_gate",
            "fill": "fill_simulator",
        }
    raise ValueError("mode must be live or replay")


def _side_effect_policy() -> dict[str, bool]:
    return {
        "components_construct_broker_neutral_decisions": True,
        "live_broker_mutation_requires_execution_gate": True,
        "replay_broker_mutation_allowed": False,
        "replay_account_mutation_allowed": False,
        "replay_order_state_mutation_allowed": False,
        "replay_position_state_mutation_allowed": False,
        "replay_uses_simulated_fills": True,
        "cross_account_collateral_or_position_netting_allowed": False,
    }


def _manifest_checksum(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()[:16]


def runtime_component_manifest() -> dict[str, Any]:
    """Return the execution-owned component manifest used by handoff snapshots."""

    payload: dict[str, Any] = {
        "contract_type": RUNTIME_COMPONENT_MANIFEST_CONTRACT,
        "manifest_version": RUNTIME_COMPONENT_MANIFEST_VERSION,
        "component_graph_policy": "same_use_case_components_live_and_replay_different_adapters",
        "component_order": list(RUNTIME_COMPONENT_ORDER),
        "required_component_order": list(REQUIRED_RUNTIME_COMPONENT_ORDER),
        "optional_component_order": list(OPTIONAL_RUNTIME_COMPONENT_ORDER),
        "adapter_profiles": {
            "live": _adapter_profile(mode="live"),
            "replay": _adapter_profile(mode="replay"),
        },
        "account_sleeves": [sleeve.to_dict() for sleeve in runtime_account_sleeves()],
        "use_case_graphs": list(runtime_use_case_graphs()),
        "components": [component.to_dict() for component in runtime_components()],
        "side_effect_policy": _side_effect_policy(),
    }
    payload["manifest_checksum"] = _manifest_checksum(payload)
    return payload


def build_runtime_component_graph(*, mode: RuntimeMode) -> dict[str, Any]:
    """Build the accepted execution component graph for live or replay mode."""

    if mode not in {"live", "replay"}:
        raise ValueError("mode must be live or replay")
    manifest = runtime_component_manifest()

    return {
        "contract_type": RUNTIME_COMPONENT_GRAPH_CONTRACT,
        "mode": mode,
        "component_graph_policy": manifest["component_graph_policy"],
        "manifest_version": manifest["manifest_version"],
        "manifest_checksum": manifest["manifest_checksum"],
        "adapter_profile": _adapter_profile(mode=mode),
        "account_sleeve_policy": "separate_crypto_leverage_and_equity_options_accounts_no_cross_account_netting",
        "account_sleeves": manifest["account_sleeves"],
        "component_order": manifest["component_order"],
        "component_sequence": [
            {
                "component_step": component.component_step,
                "component_name": component.component_name,
                "component_id": component.component_id,
            }
            for component in runtime_components()
        ],
        "use_case_graphs": manifest["use_case_graphs"],
        "components": manifest["components"],
        "required_first_batch_contracts": [
            EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
            ENTRY_DECISION_CONTRACT,
            POSITION_LIFECYCLE_DECISION_CONTRACT,
            EXECUTION_ORDER_INTENT_CONTRACT,
            EXECUTION_GATE_RESULT_CONTRACT,
        ],
        "required_second_batch_contracts": [
            EXPRESSION_DECISION_CONTRACT,
            FAILURE_EXPLANATION_PACKET_CONTRACT,
            SIMULATED_FILL_EVENT_CONTRACT,
        ],
        "side_effect_policy": manifest["side_effect_policy"],
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
