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
    CRYPTO_LEVERAGE_ACCOUNT_SLEEVE,
    CRYPTO_LEVERAGE_INSTRUMENT_REFS,
    CRYPTO_LEVERAGE_MAX_MULTIPLE,
    CRYPTO_LEVERAGE_MIN_MULTIPLE,
    CRYPTO_UNDERLYING_INSTRUMENT_REFS,
    ENTRY_DECISION_CONTRACT,
    EQUITY_OPTIONS_ACCOUNT_SLEEVE,
    EXECUTION_GATE_RESULT_CONTRACT,
    EXECUTION_ORDER_INTENT_CONTRACT,
    FAILURE_EXPLANATION_PACKET_CONTRACT,
    EXPRESSION_DECISION_CONTRACT,
    POSITION_LIFECYCLE_DECISION_CONTRACT,
    SIMULATED_FILL_EVENT_CONTRACT,
    EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
    RuntimeAccountSleeve,
    runtime_account_sleeves,
)

DecisionStatus = Literal["accepted", "blocked", "deferred", "watch_only", "monitor_only", "suitable", "rejected"]

_ALLOWED_SLEEVES = {sleeve.sleeve_id: sleeve for sleeve in runtime_account_sleeves()}
_CRYPTO_UNDERLYING_BY_SYMBOL = dict(zip(CRYPTO_CANDIDATE_SYMBOLS, CRYPTO_UNDERLYING_INSTRUMENT_REFS, strict=True))
_CRYPTO_LEVERAGE_BY_SYMBOL = dict(zip(CRYPTO_CANDIDATE_SYMBOLS, CRYPTO_LEVERAGE_INSTRUMENT_REFS, strict=True))
_EXECUTABLE_ENTRY_ACTIONS = {"open_long"}
_EXECUTABLE_LIFECYCLE_ACTIONS = {"reduce", "exit", "stop", "take_profit"}
_EXECUTABLE_EXPRESSION_ACTIONS = {"open_long", "open_short", "roll_option", "exit_option", "reduce_option"}
_HIGH_VOLUME_SCORE_THRESHOLD = 0.80
_ABNORMAL_RELATIVE_VOLUME_THRESHOLD = 2.0
_ABNORMAL_VOLUME_Z_SCORE_THRESHOLD = 2.0
_APPROVED_AGENT_REVIEW_STATUSES = {"approved", "approve", "passed", "pass"}


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
    return _CRYPTO_UNDERLYING_BY_SYMBOL.get(target_ref, target_ref)


def _number(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _bool_flag(mapping: Mapping[str, Any], *keys: str) -> bool:
    return any(mapping.get(key) is True for key in keys)


def _agent_review_approved(review: Mapping[str, Any]) -> bool:
    status = str(
        review.get("review_status")
        or review.get("review_decision")
        or review.get("decision")
        or review.get("status")
        or ""
    ).strip().lower()
    return review.get("approved") is True or status in _APPROVED_AGENT_REVIEW_STATUSES


def _risk_level(mapping: Mapping[str, Any]) -> str:
    value = mapping.get("risk_level") or mapping.get("residual_event_risk_level") or mapping.get("governance_risk_level")
    return str(value or "").strip().lower()


def _first_number(mapping: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _number(mapping.get(key), default=-1.0)
        if value >= 0:
            return value
    return None


def _candidate_entry_targets(snapshot: Mapping[str, Any]) -> set[str]:
    rows = snapshot.get("candidate_entry_pool")
    if not isinstance(rows, list):
        return set()
    values: set[str] = set()
    for row in rows:
        if isinstance(row, Mapping):
            ref = _target_ref(row)
            if ref:
                values.add(ref)
    return values


def _entry_direction(unified_decision: Mapping[str, Any]) -> str | None:
    value = str(
        unified_decision.get("entry_direction")
        or unified_decision.get("underlying_path_direction")
        or unified_decision.get("direction_thesis")
        or unified_decision.get("resolved_action_side")
        or unified_decision.get("action_side")
        or unified_decision.get("planned_side")
        or unified_decision.get("decision_direction")
        or unified_decision.get("direction")
        or ""
    ).strip().lower()
    if value in {"long", "buy", "open_long", "bullish"}:
        return "long"
    if value in {"short", "sell_short", "open_short", "bearish"}:
        return "short"
    return None


def _position_direction(
    position: Mapping[str, Any],
    entry_decision: Mapping[str, Any],
    unified_decision: Mapping[str, Any],
) -> str:
    for source in (position, entry_decision, unified_decision):
        value = str(
            source.get("position_side")
            or source.get("underlying_direction")
            or source.get("entry_direction")
            or source.get("side")
            or source.get("direction")
            or ""
        ).strip().lower()
        if value in {"long", "buy", "bullish", "call", "open_long"}:
            return "long"
        if value in {"short", "sell_short", "bearish", "put", "open_short"}:
            return "short"
    return "long"


def _planned_lifecycle_action(unified_decision: Mapping[str, Any]) -> str | None:
    value = str(
        unified_decision.get("lifecycle_action")
        or unified_decision.get("position_action")
        or unified_decision.get("planned_action")
        or unified_decision.get("decision_action")
        or unified_decision.get("action")
        or unified_decision.get("recommended_action")
        or ""
    ).strip().lower()
    if value in {"hold", "maintain", "no_change"}:
        return "hold"
    if value in {"add", "add_exposure"}:
        return "hold"
    if value in {"reduce", "trim", "decrease", "decrease_exposure"}:
        return "reduce"
    if value in {"exit", "close", "close_position", "flatten"}:
        return "exit"
    if value in {"stop", "stop_out", "hard_stop"}:
        return "stop"
    if value in {"take_profit", "profit_take", "take-profit"}:
        return "take_profit"
    return None


def _m04_thesis_handoff(
    *,
    thesis_distribution_surface: Mapping[str, Any] | None = None,
    direct_underlying_intent: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the M04-derived operational handoff without treating it as a model."""

    handoff: dict[str, Any] = {}
    intent = _as_mapping(direct_underlying_intent)
    nested = _as_mapping(intent.get("handoff_to_model_05"))
    handoff.update(nested)
    handoff.update(intent)
    surface = _as_mapping(thesis_distribution_surface)
    if surface:
        handoff.setdefault("thesis_distribution_surface_ref", surface.get("surface_ref") or surface.get("model_ref"))
        dominant = _as_mapping(surface.get("dominant_horizon_distribution"))
        if dominant:
            handoff.setdefault("dominant_horizon", dominant.get("horizon"))
            handoff.setdefault("direction_thesis", dominant.get("direction_thesis"))
            handoff.setdefault("direction_thesis_score", dominant.get("direction_thesis_score"))
            handoff.setdefault("direction_certainty_score", dominant.get("direction_certainty_score"))
    if "expected_target_price" in handoff:
        handoff.setdefault("target_price", handoff.get("expected_target_price"))
    if "expected_entry_price" in handoff:
        handoff.setdefault("entry_price", handoff.get("expected_entry_price"))
    if "reference_price" in handoff:
        handoff.setdefault("entry_price", handoff.get("reference_price"))
        handoff.setdefault("current_price", handoff.get("reference_price"))
    if "stop_loss_price" in handoff:
        handoff.setdefault("hard_stop_price", handoff.get("stop_loss_price"))
    if "thesis_invalidation_price" in handoff:
        handoff.setdefault("model_invalidation_price", handoff.get("thesis_invalidation_price"))
        handoff.setdefault("hard_stop_price", handoff.get("thesis_invalidation_price"))
    if "underlying_action_confidence_score" in handoff:
        handoff.setdefault("unified_decision_confidence_score", handoff.get("underlying_action_confidence_score"))
    if "action_confidence_score" in handoff:
        handoff.setdefault("unified_decision_confidence_score", handoff.get("action_confidence_score"))
    return handoff


def _crypto_leverage_multiple(
    *,
    expression_probability_surface: Mapping[str, Any],
    source_intent: Mapping[str, Any],
    crypto_leverage_policy: Mapping[str, Any],
) -> tuple[int, dict[str, Any]]:
    """Compute component-owned crypto leverage from surface quality and hard policy caps."""

    selected = _as_mapping(expression_probability_surface.get("selected_expression"))
    risk = _as_mapping(expression_probability_surface.get("risk_summary"))
    confidence = max(
        0.0,
        min(
            1.0,
            _number(
                selected.get(
                    "confidence_score",
                    expression_probability_surface.get(
                        "expression_confidence_score",
                        source_intent.get("unified_decision_confidence_score"),
                    ),
                ),
                default=0.5,
            ),
        ),
    )
    quality = max(
        0.0,
        min(
            1.0,
            _number(selected.get("surface_quality_score", expression_probability_surface.get("surface_quality_score")), default=0.75),
        ),
    )
    downside_tail = max(
        0.0,
        min(
            1.0,
            _number(
                risk.get(
                    "downside_tail_probability",
                    expression_probability_surface.get("downside_tail_probability", expression_probability_surface.get("left_tail_probability")),
                ),
                default=0.20,
            ),
        ),
    )
    volatility = max(
        0.0,
        min(
            1.0,
            _number(risk.get("volatility_score", expression_probability_surface.get("volatility_score")), default=0.50),
        ),
    )
    min_leverage = int(_number(crypto_leverage_policy.get("min_leverage"), default=CRYPTO_LEVERAGE_MIN_MULTIPLE))
    max_leverage = int(_number(crypto_leverage_policy.get("max_leverage"), default=CRYPTO_LEVERAGE_MAX_MULTIPLE))
    min_leverage = max(CRYPTO_LEVERAGE_MIN_MULTIPLE, min_leverage)
    max_leverage = min(CRYPTO_LEVERAGE_MAX_MULTIPLE, max(min_leverage, max_leverage))
    min_liquidation_buffer_pct = max(
        0.01,
        _number(crypto_leverage_policy.get("min_liquidation_buffer_pct"), default=0.04),
    )
    liquidation_cap = max(min_leverage, int(1.0 / min_liquidation_buffer_pct))
    max_leverage = min(max_leverage, liquidation_cap)
    raw = min_leverage + (max_leverage - min_leverage) * confidence * quality * (1.0 - downside_tail) * (1.0 - 0.5 * volatility)
    leverage = max(min_leverage, min(max_leverage, int(raw)))
    plan = {
        "owner_component": "component_04_expression_review",
        "formula": "min_plus_surface_quality_confidence_tail_volatility_clip",
        "leverage_multiple": leverage,
        "min_leverage": min_leverage,
        "max_leverage": max_leverage,
        "hard_max_leverage": CRYPTO_LEVERAGE_MAX_MULTIPLE,
        "margin_mode": str(crypto_leverage_policy.get("margin_mode") or crypto_leverage_policy.get("default_margin_mode") or "isolated"),
        "min_liquidation_buffer_pct": min_liquidation_buffer_pct,
        "confidence_score": round(confidence, 6),
        "surface_quality_score": round(quality, 6),
        "downside_tail_probability": round(downside_tail, 6),
        "volatility_score": round(volatility, 6),
    }
    return leverage, plan


def _price_reaches_downside(price: float | None, level: float | None, direction: str) -> bool:
    if price is None or level is None:
        return False
    return price <= level if direction == "long" else price >= level


def _price_reaches_upside(price: float | None, level: float | None, direction: str) -> bool:
    if price is None or level is None:
        return False
    return price >= level if direction == "long" else price <= level


def _first_positive_number(*sources: Mapping[str, Any], keys: tuple[str, ...]) -> float | None:
    for source in sources:
        for key in keys:
            value = _number(source.get(key), default=0.0)
            if value > 0:
                return value
    return None


def _target_position_scaling_capacity(
    *sources: Mapping[str, Any],
    default_min_advanced_units: int = 3,
) -> dict[str, Any]:
    nested_sources: list[Mapping[str, Any]] = []
    for source in sources:
        nested_sources.append(source)
        for key in (
            "position_scaling_capacity_state",
            "target_position_scaling_capacity",
            "target_capacity_state",
            "position_scaling_capacity",
        ):
            nested = source.get(key)
            if isinstance(nested, Mapping):
                nested_sources.append(nested)

    min_units_value = _first_positive_number(
        *nested_sources,
        keys=(
            "min_advanced_position_management_units",
            "minimum_advanced_position_management_units",
            "min_tranche_contract_count",
            "min_scaling_units",
        ),
    )
    min_units = max(int(min_units_value or default_min_advanced_units), 1)
    allocated = _first_positive_number(
        *nested_sources,
        keys=(
            "target_allocated_buying_power_usd",
            "allocated_buying_power_usd",
            "remaining_target_buying_power_usd",
            "target_remaining_buying_power_usd",
            "available_buying_power_usd",
            "available_balance_usd",
        ),
    )
    unit_cost = _first_positive_number(
        *nested_sources,
        keys=(
            "estimated_unit_cost_usd",
            "contract_unit_cost_usd",
            "estimated_contract_cost_usd",
            "planned_contract_cost_usd",
            "planned_premium_per_contract_usd",
            "premium_per_contract_usd",
            "contract_premium_usd",
        ),
    )
    if unit_cost is None:
        premium = _first_positive_number(*nested_sources, keys=("planned_limit_price", "limit_price", "mark_price"))
        multiplier = _first_positive_number(*nested_sources, keys=("contract_multiplier", "multiplier"))
        if premium is not None:
            unit_cost = premium * (multiplier or 1.0)

    affordable_units = None
    if allocated is not None and unit_cost is not None and unit_cost > 0:
        affordable_units = int(allocated // unit_cost)

    explicit_allowed = None
    for source in nested_sources:
        if source.get("advanced_position_management_allowed") is True:
            explicit_allowed = True
            break
        if source.get("advanced_position_management_allowed") is False:
            explicit_allowed = False
            break

    if explicit_allowed is not None:
        advanced_allowed = explicit_allowed
    elif affordable_units is None:
        advanced_allowed = None
    else:
        advanced_allowed = affordable_units >= min_units

    reason_codes: list[str] = []
    if affordable_units is None:
        mode = "position_scaling_capacity_unknown"
        reason_codes.append("target_position_scaling_capacity_unknown")
    elif advanced_allowed:
        mode = "advanced_tranche_management_allowed"
        reason_codes.append("target_buying_power_supports_multiple_contract_tranches")
    else:
        mode = "single_allocation_no_advanced_scaling"
        reason_codes.append("insufficient_target_buying_power_for_advanced_position_management")

    return {
        "target_allocated_buying_power_usd": allocated,
        "estimated_unit_cost_usd": unit_cost,
        "affordable_unit_count": affordable_units,
        "min_advanced_position_management_units": min_units,
        "advanced_position_management_allowed": advanced_allowed,
        "position_scaling_mode": mode,
        "reason_codes": reason_codes,
    }


def _zone_from_fields(mapping: Mapping[str, Any], zone_key: str, low_keys: tuple[str, ...], high_keys: tuple[str, ...]) -> dict[str, float] | None:
    zone = mapping.get(zone_key)
    if isinstance(zone, Mapping):
        low = _first_number(zone, "low", "min", "lower", "from")
        high = _first_number(zone, "high", "max", "upper", "to")
    elif isinstance(zone, Sequence) and not isinstance(zone, (str, bytes, bytearray)) and len(zone) >= 2:
        low = _number(zone[0], default=-1.0)
        high = _number(zone[1], default=-1.0)
        low = low if low >= 0 else None
        high = high if high >= 0 else None
    else:
        low = _first_number(mapping, *low_keys)
        high = _first_number(mapping, *high_keys)
    if low is None and high is None:
        single = _first_number(mapping, f"{zone_key}_price", "entry_price", "planned_entry_price", "limit_price")
        if single is not None:
            low = single
            high = single
    if low is None or high is None:
        return None
    return {"low": min(low, high), "high": max(low, high)}


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


def _recent_high_trading_volume(row: Mapping[str, Any]) -> bool:
    if _bool_flag(row, "recent_high_volume", "high_trading_volume"):
        return True
    if (
        _pool_score(
            row,
            "volume_score",
            "trading_volume_score",
            "dollar_volume_score",
            "volume_percentile",
            "dollar_volume_percentile",
        )
        >= _HIGH_VOLUME_SCORE_THRESHOLD
    ):
        return True
    return False


def _recent_abnormal_volume(row: Mapping[str, Any]) -> bool:
    if _bool_flag(row, "recent_abnormal_volume", "abnormal_volume", "unusual_volume"):
        return True
    if _pool_score(row, "relative_volume_score", "abnormal_volume_score", "volume_surge_score") >= _HIGH_VOLUME_SCORE_THRESHOLD:
        return True
    if _number(row.get("relative_volume"), default=0.0) >= _ABNORMAL_RELATIVE_VOLUME_THRESHOLD:
        return True
    return _number(row.get("volume_z_score"), default=0.0) >= _ABNORMAL_VOLUME_Z_SCORE_THRESHOLD


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
    if _recent_high_trading_volume(row):
        reasons.append("recent_high_trading_volume")
    if _recent_abnormal_volume(row):
        reasons.append("recent_abnormal_volume")
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
        _recent_high_trading_volume(row) or _recent_abnormal_volume(row) or _recent_news_catalyst(row) for row in rows
    )

    if sleeve.sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE:
        allowed = set(CRYPTO_CANDIDATE_SYMBOLS)
        input_refs = {_target_ref(row) for row in rows} if rows else allowed
        for symbol in CRYPTO_CANDIDATE_SYMBOLS:
            if symbol in input_refs:
                selected.append(
                    {
                        "target_ref": symbol,
                        "instrument_ref": _CRYPTO_UNDERLYING_BY_SYMBOL[symbol],
                        "asset_class": "crypto_underlying",
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
        candidate_reasons = _candidate_reasons(row, residual_sector_refs)
        if pool_filter_enabled and not candidate_reasons:
            reason = "sector_opportunity_already_filled" if sector_ref in filled_refs else "not_in_c01_candidate_source_pool"
            blocked.append({"target_ref": ref, "reason_codes": [reason]})
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
    """Build the account, entry-candidate, and open-position intake snapshot for one account sleeve."""

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
    candidate_entry_pool, blocked = _candidate_rows_for_sleeve(
        sleeve=sleeve,
        market_universe=market_universe,
        target_context_rows=target_context_rows,
        sector_opportunity_mix=sector_opportunity_mix,
        filled_sector_refs=_filled_sector_refs(sector_opportunity_rows),
    )
    positions = _as_rows(position_state)
    open_position_pool = [
        {
            "position_ref": row.get("position_ref"),
            "target_ref": _target_ref(row),
            "instrument_ref": row.get("instrument_ref"),
            "quantity": row.get("quantity"),
            "sector_ref": _sector_ref(row),
        }
        for row in positions
        if row.get("position_ref") or row.get("instrument_ref") or row.get("target_ref")
    ]
    body = {
        "contract_type": EXECUTION_INTAKE_SNAPSHOT_CONTRACT,
        "account_sleeve_id": sleeve.sleeve_id,
        "candidate_pool_policy": sleeve.candidate_pool_policy,
        "generated_at_utc": generated_at_utc,
        "candidate_entry_pool": candidate_entry_pool,
        "open_position_pool": open_position_pool,
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


def build_target_allocation_snapshot(
    *,
    account_sleeve_id: str,
    market_universe: Any = None,
    account_sleeve_state: Mapping[str, Any] | None = None,
    account_sleeve_risk_budget: Mapping[str, Any] | None = None,
    position_state: Any = None,
    target_context_rows: Any = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build the C01-compatible replay allocation snapshot expected by evaluation."""

    intake = build_execution_intake_snapshot(
        account_sleeve_id=account_sleeve_id,
        market_universe=market_universe,
        account_sleeve_state=account_sleeve_state,
        position_state=position_state,
        target_context_rows=target_context_rows,
        generated_at_utc=generated_at_utc,
    )
    body = dict(intake)
    body["contract_type"] = "target_allocation_snapshot"
    body["target_allocation_snapshot_id"] = _stable_id("tas", body)
    return body


def build_entry_decision(
    *,
    execution_intake_snapshot: Mapping[str, Any],
    target_ref: str,
    account_sleeve_state: Mapping[str, Any] | None = None,
    account_sleeve_risk_budget: Mapping[str, Any] | None = None,
    position_state: Any = None,
    target_context_state: Mapping[str, Any] | None = None,
    event_state_vector: Mapping[str, Any] | None = None,
    thesis_distribution_surface: Mapping[str, Any] | None = None,
    direct_underlying_intent: Mapping[str, Any] | None = None,
    event_risk_control: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Decide whether a C01 target has a suitable M04 direct-underlying entry thesis."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    target_ref = target_ref.strip().upper()
    sleeve_id = str(execution_intake_snapshot.get("account_sleeve_id") or "")
    _sleeve(sleeve_id)
    selected = _candidate_entry_targets(execution_intake_snapshot)
    event_state = _as_mapping(event_state_vector)
    thesis_surface = _as_mapping(thesis_distribution_surface)
    direct_intent = _as_mapping(direct_underlying_intent)
    thesis_handoff = _m04_thesis_handoff(
        thesis_distribution_surface=thesis_surface,
        direct_underlying_intent=direct_intent,
    )
    residual_governance = _as_mapping(event_risk_control)
    target_state = _as_mapping(target_context_state)

    reasons: list[str] = []
    status: DecisionStatus = "suitable"
    action = "continue_to_expression_review"
    asset_class = "crypto_underlying" if sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE else "us_equity"

    if target_ref not in selected:
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("target_not_in_execution_intake_snapshot")

    if _bool_flag(residual_governance, "block_new_entries", "halt_new_entries") or _risk_level(residual_governance) in {"high", "critical"}:
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("component_event_risk_control_blocks_new_entry")

    if _bool_flag(thesis_handoff, "block_new_entries", "account_risk_cap_reached"):
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("m04_thesis_distribution_blocks_new_entry")

    confidence_score = _number(
        thesis_handoff.get(
            "unified_decision_confidence_score",
            thesis_handoff.get("entry_thesis_score", thesis_handoff.get("score")),
        ),
        default=0.0,
    )
    minimum_confidence = _number(thesis_handoff.get("minimum_entry_confidence"), default=0.55)
    if status == "suitable" and confidence_score < minimum_confidence:
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("m04_thesis_confidence_below_entry_threshold")

    direction = _entry_direction(thesis_handoff)
    if status == "suitable" and direction is None:
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("missing_m04_entry_direction")

    entry_zone = _zone_from_fields(
        thesis_handoff,
        "entry_zone",
        ("entry_price_min", "entry_lower_price", "entry_low", "entry_min"),
        ("entry_price_max", "entry_upper_price", "entry_high", "entry_max"),
    )
    if entry_zone is None:
        single_entry = _first_number(thesis_handoff, "entry_price", "planned_entry_price", "limit_price", "reference_price")
        if single_entry is not None:
            entry_zone = {"low": single_entry, "high": single_entry}

    target_price = _first_number(
        thesis_handoff,
        "target_price",
        "expected_target_price",
        "take_profit_price",
        "planned_target_price",
        "price_target",
    )
    take_profit_zone = _zone_from_fields(
        thesis_handoff,
        "take_profit_zone",
        ("take_profit_price_min", "target_price_min", "take_profit_low"),
        ("take_profit_price_max", "target_price_max", "take_profit_high"),
    )
    model_invalidation_price = _first_number(
        thesis_handoff,
        "model_invalidation_price",
        "thesis_invalidation_price",
        "invalidation_price",
    )
    hard_stop_price = _first_number(thesis_handoff, "hard_stop_price", "stop_loss_price", "stop_price", "planned_stop_price")
    expected_horizon = thesis_handoff.get("expected_horizon") or thesis_handoff.get("dominant_horizon") or thesis_handoff.get("horizon")

    if status == "suitable" and entry_zone is None:
        status = "deferred"
        action = "defer_entry_thesis"
        reasons.append("missing_m04_entry_zone")
    if status == "suitable" and target_price is None and take_profit_zone is None:
        status = "deferred"
        action = "defer_entry_thesis"
        reasons.append("missing_m04_take_profit_or_target")
    if status == "suitable" and model_invalidation_price is None:
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("missing_model_invalidation_price")
    if status == "suitable" and hard_stop_price is None:
        status = "rejected"
        action = "reject_entry_thesis"
        reasons.append("missing_hard_stop_price")

    current_price = _first_number(target_state, "current_price", "last_price", "mark_price")
    if current_price is None:
        current_price = _first_number(thesis_handoff, "current_price", "reference_price", "last_price")
    if status == "suitable" and current_price is not None and entry_zone is not None:
        if current_price < entry_zone["low"] or current_price > entry_zone["high"]:
            status = "deferred"
            action = "defer_entry_thesis"
            reasons.append("current_price_outside_entry_zone")

    suitability_score = max(
        0.0,
        min(
            1.0,
            (
                confidence_score
                + _pool_score(thesis_handoff, "unified_decision_score", "entry_thesis_score", "setup_quality_score")
            )
            / 2,
        ),
    )

    body = {
        "contract_type": ENTRY_DECISION_CONTRACT,
        "entry_decision_id": None,
        "account_sleeve_id": sleeve_id,
        "source_intake_snapshot_id": execution_intake_snapshot.get("intake_snapshot_id"),
        "generated_at_utc": generated_at_utc,
        "target_ref": target_ref,
        "instrument_ref": _CRYPTO_UNDERLYING_BY_SYMBOL.get(target_ref, target_ref)
        if sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE
        else target_ref,
        "asset_class": asset_class,
        "decision_status": status,
        "decision_action": action,
        "entry_thesis_status": status,
        "entry_direction": direction,
        "entry_zone": entry_zone,
        "target_price": target_price,
        "take_profit_zone": take_profit_zone,
        "model_invalidation_price": model_invalidation_price,
        "hard_stop_price": hard_stop_price,
        "expected_horizon": expected_horizon,
        "entry_suitability_score": round(suitability_score, 6),
        "reason_codes": reasons,
        "unified_decision_confidence_score": confidence_score,
        "minimum_entry_confidence": minimum_confidence,
        "position_refs_considered": [
            row.get("position_ref") or row.get("instrument_ref")
            for row in _as_rows(position_state)
            if row.get("position_ref") or row.get("instrument_ref")
        ],
        "model_layer_refs": {
            "target_context_state": _as_mapping(target_context_state).get("model_ref"),
            "event_state_vector": event_state.get("model_ref"),
            "thesis_distribution_surface": thesis_surface.get("surface_ref") or thesis_surface.get("model_ref"),
            "direct_underlying_intent": direct_intent.get("model_ref") or direct_intent.get("intent_ref"),
            "event_risk_control": residual_governance.get("model_ref"),
        },
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
    event_state_vector: Mapping[str, Any] | None = None,
    thesis_distribution_surface: Mapping[str, Any] | None = None,
    direct_underlying_intent: Mapping[str, Any] | None = None,
    event_risk_control: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Manage an existing position without submitting any account mutation."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    position = _as_mapping(position_state)
    sleeve_id = str(position.get("account_sleeve_id") or _as_mapping(entry_decision).get("account_sleeve_id") or "")
    _sleeve(sleeve_id)
    event_state = _as_mapping(event_state_vector)
    thesis_surface = _as_mapping(thesis_distribution_surface)
    direct_intent = _as_mapping(direct_underlying_intent)
    thesis_handoff = _m04_thesis_handoff(
        thesis_distribution_surface=thesis_surface,
        direct_underlying_intent=direct_intent,
    )
    residual_governance = _as_mapping(event_risk_control)
    entry = _as_mapping(entry_decision)
    market = _as_mapping(market_context_state)
    risk_budget = _as_mapping(account_sleeve_risk_budget)
    account = _as_mapping(account_sleeve_state)
    position_side = _position_direction(position, entry, thesis_handoff)
    current_underlying_price = _first_number(
        position,
        "current_underlying_price",
        "underlying_price",
        "current_price",
        "last_price",
        "reference_price",
    )
    if current_underlying_price is None:
        current_underlying_price = _first_number(market, "current_underlying_price", "current_price", "last_price", "reference_price")
    if current_underlying_price is None:
        current_underlying_price = _first_number(
            thesis_handoff,
            "current_underlying_price",
            "underlying_price",
            "current_price",
            "last_price",
            "reference_price",
        )
    model_invalidation_price = (
        _first_number(thesis_handoff, "model_invalidation_price", "thesis_invalidation_price", "invalidation_price")
        or _first_number(entry, "model_invalidation_price", "thesis_invalidation_price", "invalidation_price")
        or _first_number(position, "model_invalidation_price", "thesis_invalidation_price", "invalidation_price")
    )
    hard_stop_price = (
        _first_number(thesis_handoff, "hard_stop_price", "stop_loss_price", "stop_price")
        or _first_number(entry, "hard_stop_price", "stop_loss_price", "stop_price")
        or _first_number(position, "hard_stop_price", "stop_loss_price", "stop_price")
    )
    target_price = (
        _first_number(thesis_handoff, "take_profit_price", "target_price", "expected_target_price", "target_price_high", "target_price_low")
        or _first_number(entry, "take_profit_price", "target_price", "target_price_high", "target_price_low")
        or _first_number(position, "take_profit_price", "target_price", "target_price_high", "target_price_low")
    )
    planned_action = _planned_lifecycle_action(thesis_handoff)
    reasons: list[str] = []
    status: DecisionStatus = "monitor_only"
    action = "hold"

    quantity = _number(position.get("quantity"), default=0.0)
    if quantity <= 0:
        reasons.append("no_open_position")
    else:
        status = "accepted"
        if _price_reaches_downside(current_underlying_price, hard_stop_price, position_side):
            action = "stop"
            reasons.append("model_underlying_hard_stop_reached")
        elif _price_reaches_downside(current_underlying_price, model_invalidation_price, position_side):
            action = "stop"
            reasons.append("model_underlying_invalidation_reached")
        elif _bool_flag(residual_governance, "flatten_positions", "halt_exposure") or _risk_level(residual_governance) == "critical":
            action = "exit"
            reasons.append("component_event_risk_control_requires_exit")
        elif planned_action in {"stop", "exit"}:
            action = planned_action
            reasons.append(f"m04_thesis_distribution_requires_{planned_action}")
        elif planned_action == "take_profit" or _price_reaches_upside(current_underlying_price, target_price, position_side):
            action = "take_profit"
            reasons.append("m04_thesis_target_or_take_profit_reached")
        elif _risk_level(residual_governance) == "high":
            action = "reduce"
            reasons.append("component_event_risk_control_requires_reduction")
        elif planned_action in {"reduce", "hold"}:
            action = planned_action
            reasons.append(f"m04_thesis_distribution_supports_{planned_action}")
        else:
            confidence_score = _number(
                thesis_handoff.get(
                    "unified_decision_confidence_score",
                    thesis_handoff.get("hold_thesis_score", thesis_handoff.get("score")),
                ),
                default=0.0,
            )
            hold_threshold = _number(thesis_handoff.get("minimum_hold_confidence"), default=0.45)
            if confidence_score < hold_threshold:
                action = "reduce"
                reasons.append("m04_thesis_confidence_below_hold_threshold")
            else:
                action = "hold"
                reasons.append("position_thesis_still_valid")
        if risk_budget.get("max_position_loss_pct") is not None or position.get("unrealized_loss_pct") is not None:
            reasons.append("fixed_percentage_loss_not_lifecycle_stop")

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
        "position_side": position_side,
        "current_underlying_price": current_underlying_price,
        "model_invalidation_price": model_invalidation_price,
        "hard_stop_price": hard_stop_price,
        "target_price": target_price,
        "reason_codes": reasons,
        "source_entry_decision_id": entry.get("entry_decision_id"),
        "portfolio_constraint_checks": {
            "add_blocked": True,
            "reason_codes": ["tactical_add_disabled_full_allocation_policy"],
        },
        "model_layer_refs": {
            "market_context_state": market.get("model_ref"),
            "event_state_vector": event_state.get("model_ref"),
            "thesis_distribution_surface": thesis_surface.get("surface_ref") or thesis_surface.get("model_ref"),
            "direct_underlying_intent": direct_intent.get("model_ref") or direct_intent.get("intent_ref"),
            "event_risk_control": residual_governance.get("model_ref"),
        },
        "account_state_ref": account.get("account_state_ref"),
        "account_sleeve_risk_budget": dict(risk_budget),
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
    """Convert an accepted decision into a complete broker-neutral order intent."""

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
    if contract_type == EXPRESSION_DECISION_CONTRACT and action not in _EXECUTABLE_EXPRESSION_ACTIONS:
        reasons.append("expression_action_not_executable")
    if contract_type not in {
        ENTRY_DECISION_CONTRACT,
        POSITION_LIFECYCLE_DECISION_CONTRACT,
        EXPRESSION_DECISION_CONTRACT,
    }:
        reasons.append("unsupported_source_decision_contract")
    if sleeve.sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE and instrument_ref not in CRYPTO_LEVERAGE_INSTRUMENT_REFS:
        reasons.append("crypto_order_instrument_not_in_fixed_leverage_pool")
    leverage_plan = _as_mapping(decision_record.get("leverage_plan"))
    leverage_multiple = _number(leverage_plan.get("leverage_multiple"), default=0.0)
    crypto_opening_expression = (
        sleeve.sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE
        and contract_type == EXPRESSION_DECISION_CONTRACT
        and action in {"open_long", "open_short"}
    )
    if crypto_opening_expression:
        if leverage_multiple < CRYPTO_LEVERAGE_MIN_MULTIPLE or leverage_multiple > CRYPTO_LEVERAGE_MAX_MULTIPLE:
            reasons.append("crypto_leverage_plan_missing_or_outside_policy_bounds")
        if str(leverage_plan.get("margin_mode") or "").lower() != "isolated":
            reasons.append("crypto_leverage_requires_isolated_margin_mode")
    if not cap_validation["valid"]:
        reasons.extend(cap_validation["reason_codes"])

    quantity_source = "decision_record.proposed_quantity"
    quantity = _number(decision_record.get("proposed_quantity"), default=0.0)
    if quantity <= 0:
        quantity_source = "trade_risk_cap.planned_quantity"
        quantity = _number(trade_risk_cap.get("planned_quantity"), default=0.0)
    if quantity <= 0 and not reasons:
        reasons.append("missing_positive_order_quantity")
    current_position_quantity = _number(decision_record.get("current_position_quantity"), default=0.0)
    target_position_quantity = _number(
        decision_record.get("target_position_quantity", trade_risk_cap.get("planned_target_position_quantity")),
        default=None,
    )
    scaling_capacity = _target_position_scaling_capacity(decision_record, trade_risk_cap, _as_mapping(execution_policy_snapshot))
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
        or decision_record.get("expression_decision_id")
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
            "leverage": leverage_multiple if leverage_multiple > 0 else None,
            "margin_mode": leverage_plan.get("margin_mode"),
        },
        "sizing_plan": {
            "position_management_owner": "component_05_order_intent",
            "quantity": quantity if quantity > 0 else None,
            "quantity_source": quantity_source,
            "current_position_quantity": current_position_quantity,
            "target_position_quantity": target_position_quantity,
            "planned_exposure_change": decision_record.get("planned_exposure_change")
            or trade_risk_cap.get("planned_exposure_change"),
            "target_position_scaling_capacity": scaling_capacity,
            "leverage_plan": dict(leverage_plan),
            "sizing_reason_codes": list(
                decision_record.get("sizing_reason_codes")
                or trade_risk_cap.get("sizing_reason_codes")
                or decision_record.get("reason_codes")
                or ()
            ),
            "execution_gate_may_change_quantity": False,
        },
        "trade_risk_cap": dict(trade_risk_cap),
        "risk_cap_validation": cap_validation,
        "execution_policy_ref": _as_mapping(execution_policy_snapshot).get("execution_policy_ref"),
        "required_execution_gate_reviews": {
            "agent_final_review_required": _as_mapping(execution_policy_snapshot).get("agent_final_review_required") is True,
            "agent_final_review_status": _as_mapping(execution_policy_snapshot).get(
                "agent_final_review_status",
                "not_required_for_automatic_gate",
            ),
            "agent_final_review_ref": _as_mapping(execution_policy_snapshot).get("agent_final_review_ref"),
            "review_scope": _as_mapping(execution_policy_snapshot).get(
                "review_scope",
                "automatic_gate_default_agent_review_only_for_live_broker_or_high_risk_manual_mode",
            ),
        },
        "safety": _safety_flags(),
    }
    body["execution_order_intent_id"] = _stable_id("eoi", body)
    return body


def build_expression_decision(
    *,
    option_position_state: Mapping[str, Any] | None = None,
    entry_decision: Mapping[str, Any] | None = None,
    position_lifecycle_decision: Mapping[str, Any] | None = None,
    expression_probability_surface: Mapping[str, Any] | None = None,
    option_expression_plan: Mapping[str, Any] | None = None,
    crypto_leverage_policy: Mapping[str, Any] | None = None,
    event_risk_control: Mapping[str, Any] | None = None,
    candidate_option_contracts: Any = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Translate an accepted underlying intent or review a held option expression."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    position = _as_mapping(option_position_state)
    entry = _as_mapping(entry_decision)
    lifecycle = _as_mapping(position_lifecycle_decision)
    source_intent = entry or lifecycle
    sleeve_id = str(position.get("account_sleeve_id") or source_intent.get("account_sleeve_id") or "")
    sleeve = _sleeve(sleeve_id)

    expression_surface = _as_mapping(expression_probability_surface)
    residual_governance = _as_mapping(event_risk_control)
    current_score = _number(position.get("contract_quality_score"), default=0.0)
    expression_plan = _as_mapping(option_expression_plan)
    selected_expression_candidate = _selected_expression_candidate(expression_surface)
    selected_contract = _selected_contract_from_expression_inputs(
        expression_surface=expression_surface,
        expression_plan=expression_plan,
    )
    min_improvement = _number(expression_plan.get("minimum_roll_quality_improvement"), default=0.15)
    max_roll_cost_pct = _number(expression_plan.get("max_roll_cost_pct"), default=0.20)
    best_candidate = _best_option_candidate(candidate_option_contracts)
    reasons: list[str] = []
    status: DecisionStatus = "monitor_only"
    action = "hold_expression"
    asset_class = str(source_intent.get("asset_class") or position.get("asset_class") or "us_equity")
    instrument_ref = str(position.get("instrument_ref") or source_intent.get("instrument_ref") or "").upper()
    replacement_instrument_ref = ""
    leverage_plan: dict[str, Any] = {}

    if sleeve.sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE:
        source_status = str(source_intent.get("decision_status") or "")
        direction = str(source_intent.get("entry_direction") or source_intent.get("position_side") or "long").lower()
        target_ref = str(source_intent.get("target_ref") or position.get("target_ref") or "").upper()
        if source_status != "suitable":
            reasons.append("source_underlying_intent_not_suitable")
        elif target_ref not in _CRYPTO_LEVERAGE_BY_SYMBOL:
            reasons.append("crypto_target_not_in_fixed_leverage_pool")
        else:
            status = "accepted"
            action = "open_short" if direction == "short" else "open_long"
            asset_class = "crypto_perp"
            instrument_ref = _CRYPTO_LEVERAGE_BY_SYMBOL[target_ref]
            leverage_multiple, leverage_plan = _crypto_leverage_multiple(
                expression_probability_surface=expression_surface,
                source_intent=source_intent,
                crypto_leverage_policy=_as_mapping(crypto_leverage_policy) or _as_mapping(sleeve.leverage_policy),
            )
            reasons.append("component_04_crypto_leverage_expression_selected")
            reasons.append(f"crypto_leverage_{leverage_multiple}x_selected_by_policy_formula")
    elif _number(position.get("quantity"), default=0.0) <= 0:
        route = str(expression_plan.get("asset_expression_route") or "")
        selected_ref = str(selected_contract.get("contract_ref") or selected_contract.get("option_symbol") or "").upper()
        selected_expression_type = str(
            selected_expression_candidate.get("expression_type")
            or expression_surface.get("resolved_expression_type")
            or ""
        )
        source_status = str(source_intent.get("decision_status") or "")
        if source_status != "suitable":
            reasons.append("source_underlying_intent_not_suitable")
        elif selected_ref:
            status = "accepted"
            action = "open_long" if str(source_intent.get("entry_direction") or source_intent.get("position_side") or "long").lower() != "short" else "open_short"
            asset_class = "us_option"
            instrument_ref = selected_ref
            reasons.append("m05_expression_probability_surface_selected_option_contract")
        elif route == "direct_underlying_fallback" or selected_expression_type in {
            "underlying_equity",
            "underlying_only_expression",
            "no_option_expression",
        }:
            fallback_scope = str(expression_plan.get("option_fallback_scope_status") or "")
            all_options_unsuitable = _bool_flag(
                expression_plan,
                "all_candidate_option_expressions_unsuitable",
                "no_suitable_option_expression_in_batch",
            ) or _surface_all_option_candidates_unsuitable(expression_surface)
            if fallback_scope == "no_suitable_option_expression_in_current_candidate_batch" or all_options_unsuitable:
                status = "accepted"
                action = "open_long" if str(source_intent.get("entry_direction") or source_intent.get("position_side") or "long").lower() != "short" else "open_short"
                asset_class = str(source_intent.get("asset_class") or "us_equity")
                instrument_ref = str(source_intent.get("instrument_ref") or source_intent.get("target_ref") or "").upper()
                reasons.append("m05_expression_probability_surface_direct_underlying_fallback")
            else:
                reasons.append("direct_underlying_fallback_requires_batch_no_suitable_options")
        else:
            reasons.append("no_open_option_position_or_valid_expression_selection")
    elif _bool_flag(residual_governance, "force_exit_options", "halt_option_exposure") or _risk_level(residual_governance) == "critical":
        status = "accepted"
        action = "exit_option"
        reasons.append("component_event_risk_control_requires_option_exit")
    elif best_candidate:
        improvement = _number(best_candidate.get("contract_quality_score"), default=0.0) - current_score
        roll_cost_pct = _number(best_candidate.get("roll_cost_pct"), default=0.0)
        if improvement >= min_improvement and roll_cost_pct <= max_roll_cost_pct:
            status = "accepted"
            action = "roll_option"
            replacement_instrument_ref = str(best_candidate.get("instrument_ref") or "").upper()
            instrument_ref = replacement_instrument_ref or instrument_ref
            asset_class = "us_option"
            reasons.append("candidate_option_materially_better_after_roll_cost")
        elif roll_cost_pct > max_roll_cost_pct:
            reasons.append("roll_cost_above_policy_limit")
        else:
            reasons.append("candidate_option_not_materially_better")
    else:
        reasons.append("no_candidate_option_contract")

    body = {
        "contract_type": EXPRESSION_DECISION_CONTRACT,
        "expression_decision_id": None,
        "account_sleeve_id": sleeve.sleeve_id,
        "position_ref": position.get("position_ref"),
        "source_entry_decision_id": entry.get("entry_decision_id"),
        "source_position_lifecycle_decision_id": lifecycle.get("position_lifecycle_decision_id"),
        "target_ref": str(
            position.get("underlying_symbol")
            or position.get("target_ref")
            or entry.get("target_ref")
            or lifecycle.get("target_ref")
            or ""
        ).upper(),
        "current_instrument_ref": str(position.get("instrument_ref") or "").upper(),
        "replacement_instrument_ref": replacement_instrument_ref,
        "instrument_ref": instrument_ref,
        "asset_class": asset_class,
        "generated_at_utc": generated_at_utc,
        "decision_status": status,
        "decision_action": action,
        "reason_codes": reasons,
        "expression_type": "crypto_perp" if sleeve.sleeve_id == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE else asset_class,
        "leverage_plan": leverage_plan,
        "current_contract_quality_score": current_score,
        "candidate_contract": dict(best_candidate) if best_candidate else {},
        "model_layer_refs": {
            "expression_probability_surface": expression_surface.get("surface_ref") or expression_surface.get("model_ref"),
            "option_expression_plan": expression_plan.get("model_ref"),
            "event_risk_control": residual_governance.get("model_ref"),
        },
        "safety": _safety_flags(),
    }
    body["expression_decision_id"] = _stable_id("ord", body)
    return body


def _selected_expression_candidate(expression_probability_surface: Mapping[str, Any]) -> Mapping[str, Any]:
    surface_selected = _as_mapping(expression_probability_surface.get("selected_candidate"))
    if surface_selected:
        return surface_selected
    selected_id = str(expression_probability_surface.get("selected_candidate_id") or "")
    candidates = _as_rows(expression_probability_surface.get("candidate_probability_functions"))
    for candidate in candidates:
        if selected_id and str(candidate.get("candidate_id") or "") == selected_id:
            return candidate
        if _bool_flag(candidate, "selected"):
            return candidate
    return {}


def _selected_contract_from_expression_inputs(
    *,
    expression_surface: Mapping[str, Any],
    expression_plan: Mapping[str, Any],
) -> Mapping[str, Any]:
    selected_contract = _as_mapping(expression_plan.get("selected_contract"))
    if selected_contract:
        return selected_contract
    candidate = _selected_expression_candidate(expression_surface)
    if str(candidate.get("expression_type") or "") != "option_contract":
        return {}
    instrument_ref = str(candidate.get("instrument_ref") or "").upper()
    if not instrument_ref:
        return {}
    return {
        "contract_ref": instrument_ref,
        "option_right": candidate.get("option_right"),
        "probability_function": dict(_as_mapping(candidate.get("probability_function"))),
    }


def _surface_all_option_candidates_unsuitable(expression_probability_surface: Mapping[str, Any]) -> bool:
    summary = _as_mapping(expression_probability_surface.get("option_suitability_summary"))
    if "all_option_candidates_unsuitable" in summary:
        return bool(summary.get("all_option_candidates_unsuitable"))
    candidates = _as_rows(expression_probability_surface.get("candidate_probability_functions"))
    option_candidates = [candidate for candidate in candidates if candidate.get("expression_type") == "option_contract"]
    if not option_candidates:
        return False
    return all(candidate.get("eligibility_status") != "eligible" for candidate in option_candidates)


def build_execution_gate_result(
    *,
    execution_order_intent: Mapping[str, Any],
    mode: Literal["live", "paper", "replay"],
    agent_final_review: Mapping[str, Any] | None = None,
    execution_hard_block_checks: Mapping[str, Any] | None = None,
    broker_submit_enabled: bool = False,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Apply C06 execution gates without changing the C05 order intent."""

    if mode not in {"live", "paper", "replay"}:
        raise ValueError("mode must be live, paper, or replay")

    generated_at_utc = generated_at_utc or _utc_now_iso()
    intent = _as_mapping(execution_order_intent)
    order = _as_mapping(intent.get("broker_neutral_order"))
    sizing = _as_mapping(intent.get("sizing_plan"))
    required_reviews = _as_mapping(intent.get("required_execution_gate_reviews"))
    review = _as_mapping(agent_final_review)
    hard_blocks = _as_mapping(execution_hard_block_checks)
    reasons: list[str] = []

    if intent.get("contract_type") != EXECUTION_ORDER_INTENT_CONTRACT:
        reasons.append("source_is_not_execution_order_intent")
    if intent.get("intent_status") != "ready_for_execution_gate_not_submitted":
        reasons.append("source_order_intent_not_ready")

    order_quantity = _number(order.get("quantity"), default=0.0)
    sizing_quantity = _number(sizing.get("quantity"), default=0.0)
    if order_quantity <= 0:
        reasons.append("missing_positive_order_quantity")
    if sizing_quantity <= 0:
        reasons.append("missing_positive_sizing_plan_quantity")
    if order_quantity > 0 and sizing_quantity > 0 and abs(order_quantity - sizing_quantity) > 1e-9:
        reasons.append("c06_quantity_mismatch_with_c05_sizing_plan")
    if sizing.get("execution_gate_may_change_quantity") is not False:
        reasons.append("execution_gate_quantity_change_not_allowed")
    if intent.get("account_sleeve_id") == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE and (
        order.get("leverage") is not None or _as_mapping(sizing.get("leverage_plan"))
    ):
        leverage = _number(order.get("leverage", _as_mapping(sizing.get("leverage_plan")).get("leverage_multiple")), default=0.0)
        margin_mode = str(order.get("margin_mode") or _as_mapping(sizing.get("leverage_plan")).get("margin_mode") or "").lower()
        liquidation_buffer = _number(_as_mapping(sizing.get("leverage_plan")).get("min_liquidation_buffer_pct"), default=0.0)
        if leverage < CRYPTO_LEVERAGE_MIN_MULTIPLE:
            reasons.append("crypto_leverage_below_minimum_policy")
        if leverage > CRYPTO_LEVERAGE_MAX_MULTIPLE:
            reasons.append("crypto_leverage_above_maximum_policy")
        if margin_mode != "isolated":
            reasons.append("crypto_leverage_requires_isolated_margin_mode")
        if liquidation_buffer <= 0:
            reasons.append("crypto_leverage_missing_liquidation_buffer")

    hard_block_keys = (
        "broker_regulatory_hard_block",
        "missed_event_hard_block",
        "halt_hard_block",
        "risk_cap_hard_block",
        "buying_power_hard_block",
    )
    for key in hard_block_keys:
        if hard_blocks.get(key) is True:
            reasons.append(key)
    for reason in hard_blocks.get("reason_codes") or ():
        if isinstance(reason, str) and reason and reason not in reasons:
            reasons.append(reason)

    review_status = str(
        review.get("review_status")
        or review.get("review_decision")
        or review.get("decision")
        or review.get("status")
        or ("approved" if review.get("approved") is True else "")
        or "not_required_for_replay"
    )
    review_ref = review.get("agent_final_review_ref") or review.get("review_ref") or review.get("review_id")
    if mode == "live":
        reasons.append("live_broker_execution_disabled")
        if broker_submit_enabled:
            reasons.append("use_paper_mode_for_alpaca_simulated_trading")
    agent_review_required = required_reviews.get("agent_final_review_required") is True or hard_blocks.get("agent_final_review_required") is True
    if mode in {"live", "paper"} and agent_review_required:
        if not _agent_review_approved(review):
            reasons.append("agent_final_review_not_approved")
        if not review_ref:
            reasons.append("missing_agent_final_review_ref")
    if mode in {"live", "paper"}:
        if not broker_submit_enabled:
            reasons.append("broker_submit_disabled")

    if reasons:
        gate_status = "rejected_execution_gate"
        gate_action = "reject"
    elif mode == "replay":
        gate_status = "approved_for_simulated_fill"
        gate_action = "simulate_fill"
    elif mode == "paper":
        gate_status = "approved_for_paper_broker_submission"
        gate_action = "submit_paper_broker_order"
    else:
        gate_status = "approved_for_broker_submission"
        gate_action = "approve_broker_submission"

    body = {
        "contract_type": EXECUTION_GATE_RESULT_CONTRACT,
        "execution_gate_result_id": None,
        "account_sleeve_id": intent.get("account_sleeve_id"),
        "source_order_intent_id": intent.get("execution_order_intent_id"),
        "generated_at_utc": generated_at_utc,
        "mode": mode,
        "execution_gate_status": gate_status,
        "execution_action": gate_action,
        "reason_codes": reasons,
        "broker_neutral_order": dict(order),
        "source_order_quantity": order_quantity if order_quantity > 0 else None,
        "sizing_plan_quantity": sizing_quantity if sizing_quantity > 0 else None,
        "quantity_unchanged_by_execution_gate": (
            order_quantity > 0
            and sizing_quantity > 0
            and abs(order_quantity - sizing_quantity) <= 1e-9
            and sizing.get("execution_gate_may_change_quantity") is False
        ),
        "agent_final_review_status": review_status,
        "agent_final_review_ref": review_ref,
        "execution_hard_block_checks": dict(hard_blocks),
        "safety": _safety_flags(),
    }
    body["execution_gate_result_id"] = _stable_id("egr", body)
    return body


def build_failure_explanation_packet(
    *,
    failure_observation: Mapping[str, Any],
    unscreened_event_evidence: Any,
    event_risk_control: Mapping[str, Any] | None = None,
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
        "residual_event_feedback_candidates": [
            {
                "event_ref": row["event_ref"],
                "event_family": row["event_family"],
                "reason": "possible_backward_cause_of_observed_model_or_trade_failure",
            }
            for row in ranked
            if row["event_ref"]
        ],
        "model_layer_refs": {
            "event_risk_control": _as_mapping(event_risk_control).get("model_ref")
            or failure.get("event_risk_control_ref"),
        },
        "safety": _safety_flags(),
    }
    body["failure_explanation_packet_id"] = _stable_id("fep", body)
    return body


def build_simulated_fill_event(
    *,
    execution_order_intent: Mapping[str, Any],
    execution_gate_result: Mapping[str, Any],
    replay_fill_policy: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build a Replay-only fill event after C06 approves simulation."""

    generated_at_utc = generated_at_utc or _utc_now_iso()
    intent = _as_mapping(execution_order_intent)
    gate = _as_mapping(execution_gate_result)
    policy = _as_mapping(replay_fill_policy)
    market = _as_mapping(market_snapshot)
    order = _as_mapping(intent.get("broker_neutral_order"))
    reasons: list[str] = []
    fill_status = "simulated_rejected"

    if intent.get("contract_type") != EXECUTION_ORDER_INTENT_CONTRACT:
        reasons.append("source_is_not_execution_order_intent")
    if intent.get("intent_status") != "ready_for_execution_gate_not_submitted":
        reasons.append("source_order_intent_not_ready")
    if gate.get("contract_type") != EXECUTION_GATE_RESULT_CONTRACT:
        reasons.append("source_is_not_execution_gate_result")
    if gate.get("source_order_intent_id") != intent.get("execution_order_intent_id"):
        reasons.append("execution_gate_result_does_not_match_order_intent")
    if gate.get("execution_gate_status") != "approved_for_simulated_fill":
        reasons.append("execution_gate_not_approved_for_simulated_fill")
    if gate.get("execution_action") != "simulate_fill":
        reasons.append("execution_gate_action_not_simulate_fill")

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
        "source_execution_gate_result_id": gate.get("execution_gate_result_id"),
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
        required_fields=("intake_snapshot_id", "account_sleeve_id", "candidate_entry_pool", "safety"),
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
            "entry_thesis_status",
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
            "sizing_plan",
            "trade_risk_cap",
            "risk_cap_validation",
            "safety",
        ),
    )


def validate_target_allocation_snapshot(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type="target_allocation_snapshot",
        required_fields=(
            "target_allocation_snapshot_id",
            "account_sleeve_id",
            "candidate_entry_pool",
            "safety",
        ),
    )


def validate_execution_gate_result(record: Mapping[str, Any]) -> dict[str, Any]:
    return _validate_record(
        record,
        contract_type=EXECUTION_GATE_RESULT_CONTRACT,
        required_fields=(
            "execution_gate_result_id",
            "account_sleeve_id",
            "source_order_intent_id",
            "mode",
            "execution_gate_status",
            "execution_action",
            "safety",
        ),
    )


def validate_expression_decision(record: Mapping[str, Any]) -> dict[str, Any]:
    validation = _validate_record(
        record,
        contract_type=EXPRESSION_DECISION_CONTRACT,
        required_fields=(
            "expression_decision_id",
            "account_sleeve_id",
            "decision_status",
            "decision_action",
            "safety",
        ),
    )
    if record.get("account_sleeve_id") not in {CRYPTO_LEVERAGE_ACCOUNT_SLEEVE, EQUITY_OPTIONS_ACCOUNT_SLEEVE}:
        validation["errors"].append("expression_requires_supported_expression_account")
        validation["validation_status"] = "failed"
    if record.get("decision_status") == "accepted" and record.get("decision_action") in _EXECUTABLE_EXPRESSION_ACTIONS:
        if not record.get("instrument_ref"):
            validation["errors"].append("accepted_expression_requires_instrument_ref")
            validation["validation_status"] = "failed"
        if record.get("account_sleeve_id") == CRYPTO_LEVERAGE_ACCOUNT_SLEEVE:
            leverage_plan = _as_mapping(record.get("leverage_plan"))
            leverage = _number(leverage_plan.get("leverage_multiple"), default=0.0)
            if leverage < CRYPTO_LEVERAGE_MIN_MULTIPLE or leverage > CRYPTO_LEVERAGE_MAX_MULTIPLE:
                validation["errors"].append("accepted_crypto_expression_requires_valid_leverage_plan")
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
            "source_execution_gate_result_id",
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
