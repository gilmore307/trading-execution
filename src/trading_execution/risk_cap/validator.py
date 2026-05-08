"""Validate mandatory hard risk caps before an executable order exists."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

ALLOWED_ENFORCEMENT_MODES = {
    "broker_native_stop",
    "risk_monitor_synthetic_stop",
    "long_option_premium_defined_risk",
}

REQUIRED_TRADE_RISK_CAP_FIELDS = (
    "max_loss_usd",
    "max_loss_pct",
    "time_stop_at",
    "cap_enforcement_mode",
    "cap_failure_action",
)


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def _parseable_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    try:
        datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def validate_trade_risk_cap(decision_record: Mapping[str, Any]) -> dict[str, Any]:
    """Validate that a decision record has a hard trade-risk cap.

    The validator is intentionally pre-order. Missing or invalid caps must make
    the caller reject order construction/placement instead of trying to infer a
    stop from model confidence after the fact.
    """
    cap = decision_record.get("trade_risk_cap")
    if not isinstance(cap, Mapping):
        return {
            "valid": False,
            "reject_order": True,
            "reason_codes": ["missing_trade_risk_cap"],
        }

    reasons: list[str] = []
    for field in REQUIRED_TRADE_RISK_CAP_FIELDS:
        if not _present(cap.get(field)):
            reasons.append(f"missing_{field}")

    mode = str(cap.get("cap_enforcement_mode") or "").strip()
    if mode and mode not in ALLOWED_ENFORCEMENT_MODES:
        reasons.append("unsupported_cap_enforcement_mode")

    if cap.get("cap_failure_action") != "reject_order":
        reasons.append("cap_failure_action_must_reject_order")

    if not _positive_number(cap.get("max_loss_usd")):
        reasons.append("max_loss_usd_must_be_positive")
    if not _positive_number(cap.get("max_loss_pct")):
        reasons.append("max_loss_pct_must_be_positive")

    if _present(cap.get("time_stop_at")) and not _parseable_timestamp(cap.get("time_stop_at")):
        reasons.append("time_stop_at_must_be_iso_timestamp")

    if mode == "long_option_premium_defined_risk":
        if cap.get("max_loss_is_premium_paid_flag") is not True:
            reasons.append("premium_defined_risk_requires_premium_paid_flag")
        if not _positive_number(cap.get("planned_max_premium_at_risk_usd", cap.get("max_loss_usd"))):
            reasons.append("premium_defined_risk_requires_positive_premium_at_risk")
    else:
        has_model_invalidation = _present(cap.get("model_invalidation_price"))
        has_hard_stop = _present(cap.get("hard_stop_price"))
        if not has_model_invalidation:
            reasons.append("missing_model_invalidation_price")
        if not has_hard_stop:
            reasons.append("missing_hard_stop_price")

    return {
        "valid": not reasons,
        "reject_order": bool(reasons),
        "reason_codes": reasons,
        "cap_enforcement_mode": mode or None,
    }
