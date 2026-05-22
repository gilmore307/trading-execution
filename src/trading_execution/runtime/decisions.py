"""Side-effect-free runtime decision record builders.

These builders are shared by live trading and Replay. They consume point-in-time
model/account/market evidence and emit decision records only. They do not call
providers, construct broker-specific payloads, submit orders, or mutate account
state.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
import hashlib
import json
from typing import Any, Literal

from trading_execution.risk_cap.validator import validate_trade_risk_cap

from .components import (
    CRYPTO_CANDIDATE_SYMBOLS,
    CRYPTO_SPOT_ACCOUNT_SLEEVE,
    CRYPTO_SPOT_INSTRUMENT_REFS,
    ENTRY_DECISION_CONTRACT,
    EQUITY_OPTIONS_ACCOUNT_SLEEVE,
    EXECUTION_ORDER_INTENT_CONTRACT,
    POSITION_LIFECYCLE_DECISION_CONTRACT,
    TARGET_ALLOCATION_SNAPSHOT_CONTRACT,
    RuntimeAccountSleeve,
    runtime_account_sleeves,
)

DecisionStatus = Literal["accepted", "blocked", "deferred", "watch_only", "monitor_only"]

_ALLOWED_SLEEVES = {sleeve.sleeve_id: sleeve for sleeve in runtime_account_sleeves()}
_CRYPTO_INSTRUMENT_BY_SYMBOL = dict(zip(CRYPTO_CANDIDATE_SYMBOLS, CRYPTO_SPOT_INSTRUMENT_REFS, strict=True))
_EXECUTABLE_ENTRY_ACTIONS = {"open_underlying", "open_option"}
_EXECUTABLE_LIFECYCLE_ACTIONS = {"add", "reduce", "exit", "stop", "take_profit"}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(row) for key, row in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(row) for row in value]
    if isinstance(value, set):
        return sorted(_jsonable(row) for row in value)
    return value


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(_jsonable(payload), sort_keys=True, separators=(",", ":"), default=str).encode()
    return f"{prefix}-{hashlib.sha256(encoded).hexdigest()[:16]}"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        rows = value.get("targets") or value.get("rows") or value.get("candidate_targets") or ()
    else:
        rows = value
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray)):
        return [row for row in rows if isinstance(row, Mapping)]
    return []


def _sleeve(account_sleeve_id: str) -> RuntimeAccountSleeve:
    try:
        return _ALLOWED_SLEEVES[account_sleeve_id]
    except KeyError as exc:
        raise ValueError(f"unsupported account_sleeve_id: {account_sleeve_id}") from exc


def _target_ref(row: Mapping[str, Any]) -> str | None:
    for key in ("target_ref", "symbol", "ticker", "instrument_ref", "underlying_symbol"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return None


def _instrument_ref(row: Mapping[str, Any], target_ref: str) -> str:
    value = row.get("instrument_ref") or row.get("venue_symbol") or row.get("contract_ref")
    if isinstance(value, str) and value.strip():
        return value.strip().upper()
    return _CRYPTO_INSTRUMENT_BY_SYMBOL.get(target_ref, target_ref)


def _number(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _bool_flag(mapping: Mapping[str, Any], *keys: str) -> bool:
    return any(mapping.get(key) is True for key in keys)


def _risk_level(mapping: Mapping[str, Any]) -> str:
    value = mapping.get("risk_level") or mapping.get("event_failure_risk_level") or mapping.get("policy_risk_level")
    return str(value or "").strip().lower()


def _selected_targets(snapshot: Mapping[str, Any]) -> set[str]:
    rows = snapshot.get("selected_targets")
    if not isinstance(rows, list):
        return set()
    values: set[str] = set()
    for row in rows:
        if isinstance(row, Mapping):
            ref = _target_ref(row)
            if ref:
                values.add(ref)
    return values


def _candidate_rows_for_sleeve(
    *,
    sleeve: RuntimeAccountSleeve,
    market_universe: Any,
    target_context_rows: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = _as_rows(target_context_rows) or _as_rows(market_universe)
    selected: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []

    if sleeve.sleeve_id == CRYPTO_SPOT_ACCOUNT_SLEEVE:
        allowed = set(CRYPTO_CANDIDATE_SYMBOLS)
        input_refs = {_target_ref(row) for row in rows} if rows else allowed
        for symbol in CRYPTO_CANDIDATE_SYMBOLS:
            if symbol in input_refs:
                selected.append(
                    {
                        "target_ref": symbol,
                        "instrument_ref": _CRYPTO_INSTRUMENT_BY_SYMBOL[symbol],
                        "asset_class": "crypto_spot",
                    }
                )
        for ref in sorted(row_ref for row_ref in input_refs if row_ref and row_ref not in allowed):
            blocked.append({"target_ref": ref, "reason_codes": ["outside_fixed_crypto_candidate_pool"]})
        return selected, blocked

    for row in rows:
        ref = _target_ref(row)
        if not ref:
            blocked.append({"target_ref": None, "reason_codes": ["missing_target_ref"]})
            continue
        asset_class = str(row.get("asset_class") or row.get("instrument_type") or "us_equity")
        if asset_class not in sleeve.allowed_asset_classes:
            blocked.append({"target_ref": ref, "reason_codes": ["asset_class_not_allowed_for_account_sleeve"]})
            continue
        selected.append(
            {
                "target_ref": ref,
                "instrument_ref": _instrument_ref(row, ref),
                "asset_class": asset_class,
            }
        )
    return selected, blocked


def build_target_allocation_snapshot(
    *,
    account_sleeve_id: str,
    market_universe: Any = None,
    account_sleeve_state: Mapping[str, Any] | None = None,
    account_sleeve_risk_budget: Mapping[str, Any] | None = None,
    position_state: Any = None,
    market_context_state: Mapping[str, Any] | None = None,
    sector_context_state: Mapping[str, Any] | None = None,
    target_context_rows: Any = None,
    dynamic_risk_policy_state: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build the target/risk allocation snapshot for one account sleeve."""

    sleeve = _sleeve(account_sleeve_id)
    generated_at_utc = generated_at_utc or _utc_now_iso()
    selected, blocked = _candidate_rows_for_sleeve(
        sleeve=sleeve,
        market_universe=market_universe,
        target_context_rows=target_context_rows,
    )
    positions = _as_rows(position_state)
    body = {
        "contract_type": TARGET_ALLOCATION_SNAPSHOT_CONTRACT,
        "account_sleeve_id": sleeve.sleeve_id,
        "candidate_pool_policy": sleeve.candidate_pool_policy,
        "generated_at_utc": generated_at_utc,
        "selected_targets": selected,
        "blocked_targets": blocked,
        "risk_budget": dict(_as_mapping(account_sleeve_risk_budget)),
        "account_state_ref": _as_mapping(account_sleeve_state).get("account_state_ref"),
        "open_position_refs": [
            row.get("position_ref") or row.get("instrument_ref") or row.get("target_ref")
            for row in positions
            if row.get("position_ref") or row.get("instrument_ref") or row.get("target_ref")
        ],
        "model_layer_refs": {
            "market_context_state": _as_mapping(market_context_state).get("model_ref"),
            "sector_context_state": _as_mapping(sector_context_state).get("model_ref"),
            "dynamic_risk_policy_state": _as_mapping(dynamic_risk_policy_state).get("model_ref"),
        },
        "safety": _safety_flags(),
    }
    body["allocation_snapshot_id"] = _stable_id("tas", body)
    return body


def build_entry_decision(
    *,
    target_allocation_snapshot: Mapping[str, Any],
    target_ref: str,
    account_sleeve_state: Mapping[str, Any] | None = None,
    account_sleeve_risk_budget: Mapping[str, Any] | None = None,
    position_state: Any = None,
    target_context_state: Mapping[str, Any] | None = None,
    event_failure_risk_vector: Mapping[str, Any] | None = None,
    alpha_confidence_vector: Mapping[str, Any] | None = None,
    dynamic_risk_policy_state: Mapping[str, Any] | None = None,
    underlying_action_plan: Mapping[str, Any] | None = None,
    option_expression_plan: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Decide whether a selected target should open a position."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    target_ref = target_ref.strip().upper()
    sleeve_id = str(target_allocation_snapshot.get("account_sleeve_id") or "")
    sleeve = _sleeve(sleeve_id)
    selected = _selected_targets(target_allocation_snapshot)
    event_risk = _as_mapping(event_failure_risk_vector)
    alpha = _as_mapping(alpha_confidence_vector)
    policy = _as_mapping(dynamic_risk_policy_state)
    option_plan = _as_mapping(option_expression_plan)
    underlying_plan = _as_mapping(underlying_action_plan)

    reasons: list[str] = []
    status: DecisionStatus = "accepted"
    action = "open_underlying"
    instrument_ref = target_ref
    asset_class = "crypto_spot" if sleeve_id == CRYPTO_SPOT_ACCOUNT_SLEEVE else "us_equity"

    if target_ref not in selected:
        status = "blocked"
        action = "block_entry"
        reasons.append("target_not_in_allocation_snapshot")

    if _bool_flag(event_risk, "block_new_entries", "halt_new_entries") or _risk_level(event_risk) in {"high", "critical"}:
        status = "blocked"
        action = "block_entry"
        reasons.append("event_failure_risk_blocks_new_entry")

    if _bool_flag(policy, "block_new_entries", "account_risk_cap_reached"):
        status = "blocked"
        action = "block_entry"
        reasons.append("dynamic_risk_policy_blocks_new_entry")

    alpha_score = _number(alpha.get("alpha_confidence_score", alpha.get("score")), default=0.0)
    minimum_alpha = _number(policy.get("minimum_entry_alpha_confidence"), default=0.55)
    if status == "accepted" and alpha_score < minimum_alpha:
        status = "watch_only"
        action = "watch_only"
        reasons.append("alpha_confidence_below_entry_threshold")

    preferred_expression = str(
        option_plan.get("preferred_expression")
        or option_plan.get("expression_type")
        or underlying_plan.get("preferred_expression")
        or "underlying"
    )
    if status == "accepted" and preferred_expression in {"option", "long_call", "long_put", "option_contract"}:
        if not sleeve.option_reexpression_enabled:
            status = "blocked"
            action = "block_entry"
            reasons.append("options_not_allowed_for_account_sleeve")
        else:
            action = "open_option"
            instrument_ref = str(option_plan.get("instrument_ref") or option_plan.get("contract_ref") or target_ref).upper()
            asset_class = "us_option"
    elif status == "accepted" and sleeve_id == CRYPTO_SPOT_ACCOUNT_SLEEVE:
        instrument_ref = _CRYPTO_INSTRUMENT_BY_SYMBOL.get(target_ref, target_ref)

    body = {
        "contract_type": ENTRY_DECISION_CONTRACT,
        "entry_decision_id": None,
        "account_sleeve_id": sleeve_id,
        "source_allocation_snapshot_id": target_allocation_snapshot.get("allocation_snapshot_id"),
        "generated_at_utc": generated_at_utc,
        "target_ref": target_ref,
        "instrument_ref": instrument_ref,
        "asset_class": asset_class,
        "decision_status": status,
        "decision_action": action,
        "reason_codes": reasons,
        "alpha_confidence_score": alpha_score,
        "minimum_alpha_confidence": minimum_alpha,
        "position_refs_considered": [
            row.get("position_ref") or row.get("instrument_ref")
            for row in _as_rows(position_state)
            if row.get("position_ref") or row.get("instrument_ref")
        ],
        "model_layer_refs": {
            "target_context_state": _as_mapping(target_context_state).get("model_ref"),
            "event_failure_risk_vector": event_risk.get("model_ref"),
            "alpha_confidence_vector": alpha.get("model_ref"),
            "dynamic_risk_policy_state": policy.get("model_ref"),
            "underlying_action_plan": underlying_plan.get("model_ref"),
            "option_expression_plan": option_plan.get("model_ref"),
        },
        "account_sleeve_risk_budget": dict(_as_mapping(account_sleeve_risk_budget)),
        "account_state_ref": _as_mapping(account_sleeve_state).get("account_state_ref"),
        "safety": _safety_flags(),
    }
    body["entry_decision_id"] = _stable_id("ed", body)
    return body


def build_position_lifecycle_decision(
    *,
    position_state: Mapping[str, Any],
    account_sleeve_state: Mapping[str, Any] | None = None,
    account_sleeve_risk_budget: Mapping[str, Any] | None = None,
    market_context_state: Mapping[str, Any] | None = None,
    entry_decision: Mapping[str, Any] | None = None,
    event_failure_risk_vector: Mapping[str, Any] | None = None,
    alpha_confidence_vector: Mapping[str, Any] | None = None,
    dynamic_risk_policy_state: Mapping[str, Any] | None = None,
    position_projection_vector: Mapping[str, Any] | None = None,
    underlying_action_plan: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Manage an existing position without submitting any account mutation."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    position = _as_mapping(position_state)
    sleeve_id = str(position.get("account_sleeve_id") or _as_mapping(entry_decision).get("account_sleeve_id") or "")
    _sleeve(sleeve_id)
    event_risk = _as_mapping(event_failure_risk_vector)
    alpha = _as_mapping(alpha_confidence_vector)
    policy = _as_mapping(dynamic_risk_policy_state)
    projection = _as_mapping(position_projection_vector)

    reasons: list[str] = []
    status: DecisionStatus = "monitor_only"
    action = "hold"

    quantity = _number(position.get("quantity"), default=0.0)
    if quantity <= 0:
        reasons.append("no_open_position")
    else:
        status = "accepted"
        unrealized_loss_pct = abs(_number(position.get("unrealized_loss_pct"), default=0.0))
        max_loss_pct = _number(_as_mapping(account_sleeve_risk_budget).get("max_position_loss_pct"), default=0.0)
        if max_loss_pct > 0 and unrealized_loss_pct >= max_loss_pct:
            action = "stop"
            reasons.append("max_position_loss_pct_reached")
        elif _bool_flag(event_risk, "flatten_positions", "halt_exposure") or _risk_level(event_risk) == "critical":
            action = "exit"
            reasons.append("event_failure_risk_requires_exit")
        elif _risk_level(event_risk) == "high":
            action = "reduce"
            reasons.append("event_failure_risk_requires_reduction")
        else:
            alpha_score = _number(alpha.get("alpha_confidence_score", alpha.get("score")), default=0.0)
            add_threshold = _number(policy.get("minimum_add_alpha_confidence"), default=0.70)
            reduce_threshold = _number(policy.get("minimum_hold_alpha_confidence"), default=0.45)
            if alpha_score < reduce_threshold:
                action = "reduce"
                reasons.append("alpha_confidence_below_hold_threshold")
            elif alpha_score >= add_threshold and _bool_flag(projection, "add_allowed", "position_can_add"):
                action = "add"
                reasons.append("alpha_confidence_supports_add")
            else:
                action = "hold"
                reasons.append("position_thesis_still_valid")

    body = {
        "contract_type": POSITION_LIFECYCLE_DECISION_CONTRACT,
        "position_lifecycle_decision_id": None,
        "account_sleeve_id": sleeve_id,
        "position_ref": position.get("position_ref"),
        "target_ref": str(position.get("target_ref") or position.get("symbol") or "").upper(),
        "instrument_ref": str(position.get("instrument_ref") or position.get("target_ref") or "").upper(),
        "generated_at_utc": generated_at_utc,
        "decision_status": status,
        "decision_action": action,
        "reason_codes": reasons,
        "source_entry_decision_id": _as_mapping(entry_decision).get("entry_decision_id"),
        "model_layer_refs": {
            "market_context_state": _as_mapping(market_context_state).get("model_ref"),
            "event_failure_risk_vector": event_risk.get("model_ref"),
            "alpha_confidence_vector": alpha.get("model_ref"),
            "dynamic_risk_policy_state": policy.get("model_ref"),
            "position_projection_vector": projection.get("model_ref"),
            "underlying_action_plan": _as_mapping(underlying_action_plan).get("model_ref"),
        },
        "account_state_ref": _as_mapping(account_sleeve_state).get("account_state_ref"),
        "account_sleeve_risk_budget": dict(_as_mapping(account_sleeve_risk_budget)),
        "safety": _safety_flags(),
    }
    body["position_lifecycle_decision_id"] = _stable_id("pld", body)
    return body


def build_execution_order_intent(
    *,
    decision_record: Mapping[str, Any],
    trade_risk_cap: Mapping[str, Any],
    execution_policy_snapshot: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Convert an accepted decision into a broker-neutral order intent."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    decision = dict(decision_record)
    decision["trade_risk_cap"] = dict(trade_risk_cap)
    cap_validation = validate_trade_risk_cap(decision)
    sleeve_id = str(decision_record.get("account_sleeve_id") or "")
    sleeve = _sleeve(sleeve_id)
    action = str(decision_record.get("decision_action") or "")
    contract_type = decision_record.get("contract_type")
    status = str(decision_record.get("decision_status") or "")
    instrument_ref = str(decision_record.get("instrument_ref") or "").upper()
    reasons: list[str] = []
    intent_status = "no_order_intent_required"

    if status != "accepted":
        reasons.append("source_decision_not_accepted")
    if contract_type == ENTRY_DECISION_CONTRACT and action not in _EXECUTABLE_ENTRY_ACTIONS:
        reasons.append("entry_action_not_executable")
    if contract_type == POSITION_LIFECYCLE_DECISION_CONTRACT and action not in _EXECUTABLE_LIFECYCLE_ACTIONS:
        reasons.append("lifecycle_action_not_executable")
    if contract_type not in {ENTRY_DECISION_CONTRACT, POSITION_LIFECYCLE_DECISION_CONTRACT}:
        reasons.append("unsupported_source_decision_contract")
    if sleeve.sleeve_id == CRYPTO_SPOT_ACCOUNT_SLEEVE and instrument_ref not in CRYPTO_SPOT_INSTRUMENT_REFS:
        reasons.append("crypto_order_instrument_not_in_fixed_spot_pool")
    if not cap_validation["valid"]:
        reasons.extend(cap_validation["reason_codes"])

    quantity = _number(decision_record.get("proposed_quantity", trade_risk_cap.get("planned_quantity")), default=0.0)
    if quantity <= 0 and not reasons:
        reasons.append("missing_positive_order_quantity")

    if not reasons:
        intent_status = "ready_for_execution_gate_not_submitted"
    elif "source_decision_not_accepted" not in reasons and "entry_action_not_executable" not in reasons and "lifecycle_action_not_executable" not in reasons:
        intent_status = "blocked_order_intent"

    side = _order_side(decision_record)
    body = {
        "contract_type": EXECUTION_ORDER_INTENT_CONTRACT,
        "execution_order_intent_id": None,
        "account_sleeve_id": sleeve.sleeve_id,
        "generated_at_utc": generated_at_utc,
        "intent_status": intent_status,
        "reason_codes": reasons,
        "source_decision_contract": contract_type,
        "source_decision_id": decision_record.get("entry_decision_id")
        or decision_record.get("position_lifecycle_decision_id")
        or decision_record.get("decision_id"),
        "decision_action": action,
        "instrument_ref": instrument_ref,
        "asset_class": decision_record.get("asset_class"),
        "broker_neutral_order": {
            "instrument_ref": instrument_ref,
            "side": side,
            "order_type": _as_mapping(execution_policy_snapshot).get("default_order_type", "limit"),
            "quantity": quantity if quantity > 0 else None,
            "limit_price": decision_record.get("limit_price") or trade_risk_cap.get("planned_limit_price"),
            "time_in_force": _as_mapping(execution_policy_snapshot).get("time_in_force", "day"),
        },
        "trade_risk_cap": dict(trade_risk_cap),
        "risk_cap_validation": cap_validation,
        "execution_policy_ref": _as_mapping(execution_policy_snapshot).get("execution_policy_ref"),
        "safety": _safety_flags(),
    }
    body["execution_order_intent_id"] = _stable_id("eoi", body)
    return body


def validate_target_allocation_snapshot(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=TARGET_ALLOCATION_SNAPSHOT_CONTRACT,
        required_fields=("allocation_snapshot_id", "account_sleeve_id", "selected_targets", "safety"),
    )


def validate_entry_decision(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=ENTRY_DECISION_CONTRACT,
        required_fields=(
            "entry_decision_id",
            "account_sleeve_id",
            "target_ref",
            "instrument_ref",
            "decision_status",
            "decision_action",
            "safety",
        ),
    )


def validate_position_lifecycle_decision(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=POSITION_LIFECYCLE_DECISION_CONTRACT,
        required_fields=(
            "position_lifecycle_decision_id",
            "account_sleeve_id",
            "decision_status",
            "decision_action",
            "safety",
        ),
    )


def validate_execution_order_intent(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=EXECUTION_ORDER_INTENT_CONTRACT,
        required_fields=(
            "execution_order_intent_id",
            "account_sleeve_id",
            "intent_status",
            "broker_neutral_order",
            "trade_risk_cap",
            "risk_cap_validation",
            "safety",
        ),
    )


def _validate_record(
    record: Mapping[str, Any],
    *,
    contract_type: str,
    required_fields: tuple[str, ...],
) -> dict[str, Any]:
    errors: list[str] = []
    if record.get("contract_type") != contract_type:
        errors.append("unexpected_contract_type")
    for field in required_fields:
        if field not in record or record.get(field) in (None, ""):
            errors.append(f"missing_{field}")
    sleeve_id = str(record.get("account_sleeve_id") or "")
    if sleeve_id not in _ALLOWED_SLEEVES:
        errors.append("unsupported_account_sleeve_id")
    safety = _as_mapping(record.get("safety"))
    if safety.get("provider_calls_performed") != 0:
        errors.append("provider_calls_must_be_zero")
    if safety.get("broker_calls_performed") != 0:
        errors.append("broker_calls_must_be_zero")
    if safety.get("account_mutation_performed") is not False:
        errors.append("account_mutation_must_be_false")
    if safety.get("broker_mutation_performed") is not False:
        errors.append("broker_mutation_must_be_false")
    return {
        "validated_contract_type": contract_type,
        "validation_status": "passed" if not errors else "failed",
        "errors": errors,
    }


def _order_side(decision_record: Mapping[str, Any]) -> str:
    explicit = decision_record.get("order_side") or decision_record.get("side")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()
    action = str(decision_record.get("decision_action") or "")
    position_side = str(decision_record.get("position_side") or "long").lower()
    if action in {"reduce", "exit", "stop", "take_profit"}:
        return "buy" if position_side == "short" else "sell"
    return "sell" if position_side == "short" else "buy"


def _safety_flags() -> dict[str, Any]:
    return {
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "account_mutation_performed": False,
        "broker_mutation_performed": False,
        "cross_account_netting_performed": False,
    }
