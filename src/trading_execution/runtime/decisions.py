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
    FAILURE_EXPLANATION_PACKET_CONTRACT,
    OPTION_REEXPRESSION_DECISION_CONTRACT,
    POSITION_LIFECYCLE_DECISION_CONTRACT,
    SIMULATED_FILL_EVENT_CONTRACT,
    EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
    RuntimeAccountSleeve,
    runtime_account_sleeves,
)

DecisionStatus = Literal["accepted", "blocked", "deferred", "watch_only", "monitor_only"]

_ALLOWED_SLEEVES = {sleeve.sleeve_id: sleeve for sleeve in runtime_account_sleeves()}
_CRYPTO_INSTRUMENT_BY_SYMBOL = dict(zip(CRYPTO_CANDIDATE_SYMBOLS, CRYPTO_SPOT_INSTRUMENT_REFS, strict=True))
_EXECUTABLE_ENTRY_ACTIONS = {"open_underlying", "open_option"}
_EXECUTABLE_LIFECYCLE_ACTIONS = {"add", "reduce", "exit", "stop", "take_profit"}
_EXECUTABLE_OPTION_REEXPRESSION_ACTIONS = {"roll_option", "exit_option", "reduce_option"}


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


def _watch_targets(snapshot: Mapping[str, Any]) -> set[str]:
    rows = snapshot.get("watch_targets")
    if not isinstance(rows, list):
        return set()
    values: set[str] = set()
    for row in rows:
        if isinstance(row, Mapping):
            ref = _target_ref(row)
            if ref:
                values.add(ref)
    return values


def _account_balance_status(account_sleeve_state: Mapping[str, Any]) -> dict[str, Any]:
    balance_value = account_sleeve_state.get(
        "available_cash_usd",
        account_sleeve_state.get("buying_power_usd", account_sleeve_state.get("cash_usd")),
    )
    balance = _number(balance_value, default=0.0)
    return {
        "available_balance_usd": balance,
        "new_position_balance_status": "has_balance" if balance > 0 else "no_available_balance",
    }


def _sector_ref(row: Mapping[str, Any]) -> str | None:
    value = row.get("sector_ref") or row.get("sector") or row.get("industry_ref") or row.get("theme_ref")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _target_sector_refs(target_context_rows: Any) -> dict[str, str]:
    refs: dict[str, str] = {}
    for row in _as_rows(target_context_rows):
        sector_ref = _sector_ref(row)
        target_ref = _target_ref(row)
        if sector_ref and target_ref:
            refs[target_ref] = sector_ref
        instrument_ref = row.get("instrument_ref") or row.get("contract_ref")
        if sector_ref and isinstance(instrument_ref, str) and instrument_ref.strip():
            refs[instrument_ref.strip().upper()] = sector_ref
    return refs


def _position_weight(row: Mapping[str, Any]) -> float:
    for key in ("sector_weight", "portfolio_weight", "position_weight", "allocation_weight", "notional_weight"):
        value = _number(row.get(key), default=-1.0)
        if value >= 0:
            return value
    return -1.0


def _current_sector_mix(position_state: Any, target_sector_refs: Mapping[str, str]) -> dict[str, float]:
    rows = _as_rows(position_state)
    weighted: dict[str, float] = {}
    unweighted: dict[str, float] = {}
    notional_rows: list[tuple[str, float]] = []

    for row in rows:
        sector_ref = _sector_ref(row)
        if not sector_ref:
            target_ref = _target_ref(row)
            sector_ref = target_sector_refs.get(target_ref or "")
        if not sector_ref:
            continue
        weight = _position_weight(row)
        if weight >= 0:
            weighted[sector_ref] = weighted.get(sector_ref, 0.0) + weight
            continue
        notional = _number(
            row.get("market_value_usd", row.get("notional_usd", row.get("gross_exposure_usd"))),
            default=0.0,
        )
        if notional > 0:
            notional_rows.append((sector_ref, notional))
        else:
            unweighted[sector_ref] = unweighted.get(sector_ref, 0.0) + 1.0

    if weighted:
        return weighted
    total_notional = sum(value for _, value in notional_rows)
    if total_notional > 0:
        current: dict[str, float] = {}
        for sector_ref, value in notional_rows:
            current[sector_ref] = current.get(sector_ref, 0.0) + value / total_notional
        return current
    total_count = sum(unweighted.values())
    if total_count > 0:
        return {sector_ref: count / total_count for sector_ref, count in unweighted.items()}
    return {}


def _sector_opportunity_rows(
    sector_context_state: Mapping[str, Any],
    *,
    position_state: Any = None,
    target_context_rows: Any = None,
) -> list[dict[str, Any]]:
    threshold = _number(sector_context_state.get("strong_sector_threshold"), default=0.70)
    raw_rows = (
        sector_context_state.get("sector_scores")
        or sector_context_state.get("sector_context_rows")
        or sector_context_state.get("sectors")
        or ()
    )
    rows = _as_rows({"rows": raw_rows})
    strong: list[dict[str, Any]] = []
    for row in rows:
        sector_ref = _sector_ref(row)
        if not sector_ref:
            continue
        strength = _number(
            row.get("opportunity_strength_score", row.get("sector_strength_score", row.get("strength_score"))),
            default=0.0,
        )
        if strength < threshold:
            continue
        strong.append({"sector_ref": sector_ref, "opportunity_strength_score": strength})

    total_strength = sum(row["opportunity_strength_score"] for row in strong)
    if total_strength <= 0:
        return []
    target_rows = [
        {
            "sector_ref": row["sector_ref"],
            "opportunity_strength_score": row["opportunity_strength_score"],
            "target_mix_weight": row["opportunity_strength_score"] / total_strength,
        }
        for row in strong
    ]
    current_mix = _current_sector_mix(position_state, _target_sector_refs(target_context_rows))
    residual_rows = [
        {
            **row,
            "current_mix_weight": current_mix.get(row["sector_ref"], 0.0),
            "remaining_mix_weight": max(row["target_mix_weight"] - current_mix.get(row["sector_ref"], 0.0), 0.0),
        }
        for row in target_rows
    ]
    return residual_rows


def _sector_opportunity_mix(
    sector_context_state: Mapping[str, Any],
    *,
    position_state: Any = None,
    target_context_rows: Any = None,
) -> list[dict[str, Any]]:
    residual_rows = _sector_opportunity_rows(
        sector_context_state,
        position_state=position_state,
        target_context_rows=target_context_rows,
    )
    total_remaining = sum(row["remaining_mix_weight"] for row in residual_rows)
    if total_remaining <= 0:
        return []
    return [
        {
            "sector_ref": row["sector_ref"],
            "opportunity_strength_score": round(row["opportunity_strength_score"], 6),
            "target_mix_weight": round(row["target_mix_weight"], 6),
            "current_mix_weight": round(row["current_mix_weight"], 6),
            "remaining_mix_weight": round(row["remaining_mix_weight"], 6),
            "opportunity_mix_weight": round(row["remaining_mix_weight"] / total_remaining, 6),
        }
        for row in sorted(residual_rows, key=lambda item: (-item["remaining_mix_weight"], item["sector_ref"]))
        if row["remaining_mix_weight"] > 0
    ]


def _filled_sector_refs(sector_opportunity_rows: Sequence[Mapping[str, Any]]) -> set[str]:
    refs: set[str] = set()
    for row in sector_opportunity_rows:
        sector_ref = row.get("sector_ref")
        if isinstance(sector_ref, str) and _number(row.get("remaining_mix_weight"), default=0.0) <= 0:
            refs.add(sector_ref)
    return refs


def _pool_score(row: Mapping[str, Any], *keys: str) -> float:
    return max((_number(row.get(key), default=0.0) for key in keys), default=0.0)


def _recent_high_volume(row: Mapping[str, Any]) -> bool:
    if _bool_flag(row, "recent_high_volume", "high_trading_volume", "unusual_volume"):
        return True
    if _pool_score(row, "volume_score", "relative_volume_score", "liquidity_score") >= 0.70:
        return True
    return _number(row.get("relative_volume"), default=0.0) >= 2.0


def _recent_news_catalyst(row: Mapping[str, Any]) -> bool:
    if _bool_flag(row, "recent_news_catalyst", "news_catalyst", "earnings_catalyst", "earnings_beat"):
        return True
    if _pool_score(row, "news_catalyst_score", "catalyst_score", "earnings_surprise_score") >= 0.70:
        return True
    catalyst_type = row.get("catalyst_type") or row.get("news_catalyst_type")
    return isinstance(catalyst_type, str) and bool(catalyst_type.strip())


def _candidate_reasons(row: Mapping[str, Any], residual_sector_refs: set[str]) -> list[str]:
    reasons: list[str] = []
    sector_ref = _sector_ref(row)
    if sector_ref in residual_sector_refs:
        reasons.append("remaining_strong_sector_opportunity")
    if _recent_high_volume(row):
        reasons.append("recent_high_trading_volume")
    if _recent_news_catalyst(row):
        reasons.append("recent_news_catalyst")
    return reasons


def _candidate_rows_for_sleeve(
    *,
    sleeve: RuntimeAccountSleeve,
    market_universe: Any,
    target_context_rows: Any,
    sector_opportunity_mix: Sequence[Mapping[str, Any]] = (),
    filled_sector_refs: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = _as_rows(target_context_rows) or _as_rows(market_universe)
    selected: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    residual_sector_refs = {
        row["sector_ref"]
        for row in sector_opportunity_mix
        if isinstance(row.get("sector_ref"), str) and _number(row.get("remaining_mix_weight"), default=0.0) > 0
    }
    filled_refs = filled_sector_refs or set()
    pool_filter_enabled = bool(residual_sector_refs or filled_refs) or any(
        _recent_high_volume(row) or _recent_news_catalyst(row) for row in rows
    )

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
        sector_ref = _sector_ref(row)
        if sector_ref in filled_refs:
            blocked.append({"target_ref": ref, "reason_codes": ["sector_opportunity_already_filled"]})
            continue
        candidate_reasons = _candidate_reasons(row, residual_sector_refs)
        if pool_filter_enabled and not candidate_reasons:
            blocked.append({"target_ref": ref, "reason_codes": ["not_in_c01_candidate_source_pool"]})
            continue
        selected_row = {
            "target_ref": ref,
            "instrument_ref": _instrument_ref(row, ref),
            "asset_class": asset_class,
        }
        if sector_ref:
            selected_row["sector_ref"] = sector_ref
        if candidate_reasons:
            selected_row["candidate_reasons"] = candidate_reasons
        selected.append(
            selected_row
        )
    return selected, blocked


def build_execution_intake_snapshot(
    *,
    account_sleeve_id: str,
    market_universe: Any = None,
    account_sleeve_state: Mapping[str, Any] | None = None,
    position_state: Any = None,
    market_context_state: Mapping[str, Any] | None = None,
    sector_context_state: Mapping[str, Any] | None = None,
    target_context_rows: Any = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build the account and watch-target intake snapshot for one account sleeve."""

    sleeve = _sleeve(account_sleeve_id)
    account_state = _as_mapping(account_sleeve_state)
    sector_state = _as_mapping(sector_context_state)
    balance_status = _account_balance_status(account_state)
    generated_at_utc = generated_at_utc or _utc_now_iso()
    sector_opportunity_rows = _sector_opportunity_rows(
        sector_state,
        position_state=position_state,
        target_context_rows=target_context_rows,
    )
    sector_opportunity_mix = _sector_opportunity_mix(
        sector_state,
        position_state=position_state,
        target_context_rows=target_context_rows,
    )
    watch_targets, blocked = _candidate_rows_for_sleeve(
        sleeve=sleeve,
        market_universe=market_universe,
        target_context_rows=target_context_rows,
        sector_opportunity_mix=sector_opportunity_mix,
        filled_sector_refs=_filled_sector_refs(sector_opportunity_rows),
    )
    positions = _as_rows(position_state)
    body = {
        "contract_type": EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
        "account_sleeve_id": sleeve.sleeve_id,
        "candidate_pool_policy": sleeve.candidate_pool_policy,
        "generated_at_utc": generated_at_utc,
        "watch_targets": watch_targets,
        "blocked_targets": blocked,
        "sector_opportunity_mix": sector_opportunity_mix,
        "available_balance_usd": balance_status["available_balance_usd"],
        "new_position_balance_status": balance_status["new_position_balance_status"],
        "account_state_ref": account_state.get("account_state_ref"),
        "open_position_refs": [
            row.get("position_ref") or row.get("instrument_ref") or row.get("target_ref")
            for row in positions
            if row.get("position_ref") or row.get("instrument_ref") or row.get("target_ref")
        ],
        "model_layer_refs": {
            "market_context_state": _as_mapping(market_context_state).get("model_ref"),
            "sector_context_state": sector_state.get("model_ref"),
        },
        "safety": _safety_flags(),
    }
    body["intake_snapshot_id"] = _stable_id("eis", body)
    return body


def build_entry_decision(
    *,
    execution_intake_snapshot: Mapping[str, Any],
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
    sleeve_id = str(execution_intake_snapshot.get("account_sleeve_id") or "")
    sleeve = _sleeve(sleeve_id)
    selected = _watch_targets(execution_intake_snapshot)
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
        reasons.append("target_not_in_execution_intake_snapshot")

    if execution_intake_snapshot.get("new_position_balance_status") == "no_available_balance":
        status = "blocked"
        action = "block_entry"
        reasons.append("no_available_account_balance")

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
        "source_intake_snapshot_id": execution_intake_snapshot.get("intake_snapshot_id"),
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
    if contract_type == OPTION_REEXPRESSION_DECISION_CONTRACT and action not in _EXECUTABLE_OPTION_REEXPRESSION_ACTIONS:
        reasons.append("option_reexpression_action_not_executable")
    if contract_type not in {
        ENTRY_DECISION_CONTRACT,
        POSITION_LIFECYCLE_DECISION_CONTRACT,
        OPTION_REEXPRESSION_DECISION_CONTRACT,
    }:
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
        or decision_record.get("option_reexpression_decision_id")
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


def build_option_reexpression_decision(
    *,
    option_position_state: Mapping[str, Any],
    underlying_action_plan: Mapping[str, Any] | None = None,
    option_expression_plan: Mapping[str, Any] | None = None,
    dynamic_risk_policy_state: Mapping[str, Any] | None = None,
    candidate_option_contracts: Any = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Review a held option and decide whether it should be rolled or held."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    position = _as_mapping(option_position_state)
    sleeve_id = str(position.get("account_sleeve_id") or "")
    sleeve = _sleeve(sleeve_id)
    if sleeve.sleeve_id != EQUITY_OPTIONS_ACCOUNT_SLEEVE:
        raise ValueError("option re-expression is only allowed for equity_options_account")

    policy = _as_mapping(dynamic_risk_policy_state)
    current_score = _number(position.get("contract_quality_score"), default=0.0)
    min_improvement = _number(policy.get("minimum_roll_quality_improvement"), default=0.15)
    max_roll_cost_pct = _number(policy.get("max_roll_cost_pct"), default=0.20)
    best_candidate = _best_option_candidate(candidate_option_contracts)
    reasons: list[str] = []
    status: DecisionStatus = "monitor_only"
    action = "hold_option"

    if _number(position.get("quantity"), default=0.0) <= 0:
        reasons.append("no_open_option_position")
    elif _bool_flag(policy, "force_exit_options", "halt_option_exposure"):
        status = "accepted"
        action = "exit_option"
        reasons.append("dynamic_risk_policy_requires_option_exit")
    elif best_candidate:
        improvement = _number(best_candidate.get("contract_quality_score"), default=0.0) - current_score
        roll_cost_pct = _number(best_candidate.get("roll_cost_pct"), default=0.0)
        if improvement >= min_improvement and roll_cost_pct <= max_roll_cost_pct:
            status = "accepted"
            action = "roll_option"
            reasons.append("candidate_option_materially_better_after_roll_cost")
        elif roll_cost_pct > max_roll_cost_pct:
            reasons.append("roll_cost_above_policy_limit")
        else:
            reasons.append("candidate_option_not_materially_better")
    else:
        reasons.append("no_candidate_option_contract")

    body = {
        "contract_type": OPTION_REEXPRESSION_DECISION_CONTRACT,
        "option_reexpression_decision_id": None,
        "account_sleeve_id": sleeve.sleeve_id,
        "position_ref": position.get("position_ref"),
        "target_ref": str(position.get("underlying_symbol") or position.get("target_ref") or "").upper(),
        "current_instrument_ref": str(position.get("instrument_ref") or "").upper(),
        "replacement_instrument_ref": str(best_candidate.get("instrument_ref") or "").upper() if best_candidate else "",
        "instrument_ref": str(best_candidate.get("instrument_ref") or position.get("instrument_ref") or "").upper()
        if best_candidate
        else str(position.get("instrument_ref") or "").upper(),
        "generated_at_utc": generated_at_utc,
        "decision_status": status,
        "decision_action": action,
        "reason_codes": reasons,
        "current_contract_quality_score": current_score,
        "candidate_contract": dict(best_candidate) if best_candidate else {},
        "model_layer_refs": {
            "underlying_action_plan": _as_mapping(underlying_action_plan).get("model_ref"),
            "option_expression_plan": _as_mapping(option_expression_plan).get("model_ref"),
            "dynamic_risk_policy_state": policy.get("model_ref"),
        },
        "safety": _safety_flags(),
    }
    body["option_reexpression_decision_id"] = _stable_id("ord", body)
    return body


def build_failure_explanation_packet(
    *,
    failure_observation: Mapping[str, Any],
    unscreened_event_evidence: Any,
    event_failure_risk_vector: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Link an observed model/trade failure to possible earlier events."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    failure = _as_mapping(failure_observation)
    observed_at = str(failure.get("observed_at_utc") or failure.get("failure_time_utc") or "")
    event_rows = _as_rows(unscreened_event_evidence)
    backward_events: list[dict[str, Any]] = []
    ignored_events: list[dict[str, Any]] = []
    for row in event_rows:
        event_time = str(row.get("event_time_utc") or row.get("observed_at_utc") or "")
        event_ref = str(row.get("event_ref") or row.get("event_id") or "")
        if observed_at and event_time and event_time > observed_at:
            ignored_events.append({"event_ref": event_ref, "reason_codes": ["event_after_failure_time"]})
            continue
        backward_events.append(
            {
                "event_ref": event_ref,
                "event_time_utc": event_time,
                "event_family": row.get("event_family") or row.get("family") or "",
                "severity_score": _number(row.get("severity_score"), default=0.0),
                "match_score": _number(row.get("match_score", row.get("event_match_score")), default=0.0),
            }
        )
    ranked = sorted(
        backward_events,
        key=lambda row: (row["match_score"], row["severity_score"], row["event_time_utc"]),
        reverse=True,
    )[:5]
    body = {
        "contract_type": FAILURE_EXPLANATION_PACKET_CONTRACT,
        "failure_explanation_packet_id": None,
        "account_sleeve_id": failure.get("account_sleeve_id"),
        "observed_failure_ref": failure.get("failure_ref") or failure.get("observation_ref"),
        "observed_at_utc": observed_at,
        "generated_at_utc": generated_at_utc,
        "explanation_status": "candidate_causes_found" if ranked else "no_backward_event_match",
        "ranked_possible_causes": ranked,
        "ignored_events": ignored_events,
        "layer_4_feedback_candidates": [
            {
                "event_ref": row["event_ref"],
                "event_family": row["event_family"],
                "reason": "possible_backward_cause_of_observed_model_or_trade_failure",
            }
            for row in ranked
            if row["event_ref"]
        ],
        "model_layer_refs": {
            "event_failure_risk_vector": _as_mapping(event_failure_risk_vector).get("model_ref"),
            "layer_10_event_risk_governor": failure.get("layer_10_model_ref"),
        },
        "safety": _safety_flags(),
    }
    body["failure_explanation_packet_id"] = _stable_id("fep", body)
    return body


def build_simulated_fill_event(
    *,
    execution_order_intent: Mapping[str, Any],
    replay_fill_policy: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build a Replay-only simulated fill event from a broker-neutral intent."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    intent = _as_mapping(execution_order_intent)
    policy = _as_mapping(replay_fill_policy)
    market = _as_mapping(market_snapshot)
    order = _as_mapping(intent.get("broker_neutral_order"))
    reasons: list[str] = []
    fill_status = "simulated_rejected"

    if intent.get("contract_type") != EXECUTION_ORDER_INTENT_CONTRACT:
        reasons.append("source_is_not_execution_order_intent")
    if intent.get("intent_status") != "ready_for_execution_gate_not_submitted":
        reasons.append("source_order_intent_not_ready")

    quantity = _number(order.get("quantity"), default=0.0)
    if quantity <= 0:
        reasons.append("missing_positive_order_quantity")

    limit_price = _number(order.get("limit_price"), default=0.0)
    reference_price = _number(market.get("reference_price", market.get("close_price")), default=limit_price)
    slippage_bps = _number(policy.get("slippage_bps"), default=0.0)
    fee_bps = _number(policy.get("fee_bps"), default=0.0)
    side = str(order.get("side") or "buy").lower()
    signed_slippage = 1 if side == "buy" else -1
    simulated_price = reference_price * (1 + signed_slippage * slippage_bps / 10000) if reference_price > 0 else 0.0
    fee_usd = abs(simulated_price * quantity * fee_bps / 10000)
    if simulated_price <= 0:
        reasons.append("missing_positive_reference_price")
    if not reasons:
        fill_status = "simulated_filled"

    body = {
        "contract_type": SIMULATED_FILL_EVENT_CONTRACT,
        "simulated_fill_event_id": None,
        "account_sleeve_id": intent.get("account_sleeve_id"),
        "source_order_intent_id": intent.get("execution_order_intent_id"),
        "generated_at_utc": generated_at_utc,
        "fill_status": fill_status,
        "reason_codes": reasons,
        "instrument_ref": order.get("instrument_ref"),
        "side": side,
        "quantity": quantity if quantity > 0 else None,
        "simulated_fill_price": simulated_price if simulated_price > 0 else None,
        "simulated_fee_usd": fee_usd if simulated_price > 0 and quantity > 0 else None,
        "replay_fill_policy_ref": policy.get("replay_fill_policy_ref"),
        "market_snapshot_ref": market.get("market_snapshot_ref"),
        "safety": _safety_flags(),
    }
    body["simulated_fill_event_id"] = _stable_id("sfe", body)
    return body


def validate_execution_intake_snapshot(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
        required_fields=("intake_snapshot_id", "account_sleeve_id", "watch_targets", "safety"),
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


def validate_option_reexpression_decision(record: Mapping[str, Any]) -> dict[str, Any]:
    validation = _validate_record(
        record,
        contract_type=OPTION_REEXPRESSION_DECISION_CONTRACT,
        required_fields=(
            "option_reexpression_decision_id",
            "account_sleeve_id",
            "current_instrument_ref",
            "decision_status",
            "decision_action",
            "safety",
        ),
    )
    if record.get("account_sleeve_id") != EQUITY_OPTIONS_ACCOUNT_SLEEVE:
        validation["errors"].append("option_reexpression_requires_equity_options_account")
        validation["validation_status"] = "failed"
    return validation


def validate_failure_explanation_packet(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=FAILURE_EXPLANATION_PACKET_CONTRACT,
        required_fields=(
            "failure_explanation_packet_id",
            "observed_failure_ref",
            "observed_at_utc",
            "ranked_possible_causes",
            "safety",
        ),
    )


def validate_simulated_fill_event(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=SIMULATED_FILL_EVENT_CONTRACT,
        required_fields=(
            "simulated_fill_event_id",
            "account_sleeve_id",
            "source_order_intent_id",
            "fill_status",
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


def _best_option_candidate(candidate_option_contracts: Any) -> Mapping[str, Any]:
    rows = _as_rows(candidate_option_contracts)
    if not rows:
        return {}
    return max(rows, key=lambda row: _number(row.get("contract_quality_score"), default=0.0))


def _safety_flags() -> dict[str, Any]:
    return {
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "account_mutation_performed": False,
        "broker_mutation_performed": False,
        "cross_account_netting_performed": False,
    }
