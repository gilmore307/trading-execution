"""Reviewed approval contract for realtime live-observe execution.

This module is the boundary between fixture rehearsal and formal realtime
provider observation. It permits read-only provider observation only when a
reviewed approval artifact is present, bounded, unexpired, and explicitly
forbids model activation, order construction, broker execution, and account
mutation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

LIVE_OBSERVE_APPROVAL_CONTRACT = "realtime_live_observe_approval"
APPROVAL_SCOPE = "realtime_market_data_observe_only"
APPROVED_SOURCES = (
    "alpaca",
    "thetadata",
    "okx",
    "calendar_discovery",
    "execution_account_state",
    "derived_model_context",
)


@dataclass(frozen=True)
class RealtimeLiveObserveApprovalValidation:
    """Validation result for a realtime live-observe approval artifact."""

    contract_type: str
    approval_id: str | None
    valid: bool
    missing_fields: tuple[str, ...]
    invalid_fields: tuple[str, ...]
    unknown_sources: tuple[str, ...]
    unapproved_sources: tuple[str, ...]
    unapproved_instruments: tuple[str, ...]
    expired: bool
    provider_call_budget_remaining: int
    execute_live_observe_allowed: bool
    model_activation_allowed: bool
    broker_execution_allowed: bool
    broker_order_construction_allowed: bool
    account_mutation_allowed: bool

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        for key in ("missing_fields", "invalid_fields", "unknown_sources", "unapproved_sources", "unapproved_instruments"):
            row[key] = list(row[key])
        return row


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [str(item) for item in value]
    return []


def _approved_instruments(approval: Mapping[str, Any]) -> set[str]:
    values = set(_list(approval.get("approved_instrument_refs")))
    values.update(_list(approval.get("approved_symbols")))
    return values


def validate_live_observe_approval(
    approval: Mapping[str, Any],
    *,
    requested_sources: Sequence[str] | None = None,
    requested_instrument_refs: Sequence[str] | None = None,
    requested_provider_calls: int = 1,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate approval for formal read-only realtime observation."""

    required = (
        "contract_type",
        "approval_id",
        "approval_scope",
        "approved_sources",
        "approved_instrument_refs",
        "approved_at_utc",
        "expires_at_utc",
        "max_provider_calls",
    )
    missing = [field for field in required if approval.get(field) in (None, "", [], {})]
    invalid: list[str] = []
    if approval.get("contract_type") != LIVE_OBSERVE_APPROVAL_CONTRACT:
        invalid.append("contract_type")
    if approval.get("approval_scope") != APPROVAL_SCOPE:
        invalid.append("approval_scope")

    approved_sources = set(_list(approval.get("approved_sources")))
    requested_source_set = set(requested_sources or approved_sources)
    unknown_sources = sorted((approved_sources | requested_source_set) - set(APPROVED_SOURCES))
    unapproved_sources = sorted(requested_source_set - approved_sources)

    approved_instruments = _approved_instruments(approval)
    requested_instrument_set = set(requested_instrument_refs or approved_instruments)
    wildcard_instruments = "*" in approved_instruments
    unapproved_instruments = sorted(set() if wildcard_instruments else requested_instrument_set - approved_instruments)

    max_provider_calls = approval.get("max_provider_calls")
    try:
        max_provider_calls_int = int(max_provider_calls)
    except (TypeError, ValueError):
        max_provider_calls_int = -1
        invalid.append("max_provider_calls")
    if max_provider_calls_int < 1:
        invalid.append("max_provider_calls")
    if requested_provider_calls < 0:
        invalid.append("requested_provider_calls")
    budget_remaining = max_provider_calls_int - max(0, requested_provider_calls)
    if budget_remaining < 0:
        invalid.append("max_provider_calls_exceeded")

    expires_at = _parse_time(approval.get("expires_at_utc"))
    if expires_at is None:
        invalid.append("expires_at_utc")
        expired = True
    else:
        expired = (now or _now()) > expires_at

    allowed = bool(approval.get("execute_live_observe_allowed"))
    model_activation_allowed = bool(approval.get("model_activation_allowed"))
    broker_execution_allowed = bool(approval.get("broker_execution_allowed"))
    order_allowed = bool(approval.get("broker_order_construction_allowed"))
    account_mutation_allowed = bool(approval.get("account_mutation_allowed"))
    if not allowed:
        invalid.append("execute_live_observe_allowed")
    if model_activation_allowed:
        invalid.append("model_activation_allowed_must_be_false")
    if broker_execution_allowed:
        invalid.append("broker_execution_allowed_must_be_false")
    if order_allowed:
        invalid.append("broker_order_construction_allowed_must_be_false")
    if account_mutation_allowed:
        invalid.append("account_mutation_allowed_must_be_false")

    valid = not missing and not invalid and not unknown_sources and not unapproved_sources and not unapproved_instruments and not expired
    return RealtimeLiveObserveApprovalValidation(
        contract_type="realtime_live_observe_approval_validation",
        approval_id=str(approval.get("approval_id")) if approval.get("approval_id") else None,
        valid=valid,
        missing_fields=tuple(missing),
        invalid_fields=tuple(sorted(set(invalid))),
        unknown_sources=tuple(unknown_sources),
        unapproved_sources=tuple(unapproved_sources),
        unapproved_instruments=tuple(unapproved_instruments),
        expired=expired,
        provider_call_budget_remaining=budget_remaining,
        execute_live_observe_allowed=allowed,
        model_activation_allowed=model_activation_allowed,
        broker_execution_allowed=broker_execution_allowed,
        broker_order_construction_allowed=order_allowed,
        account_mutation_allowed=account_mutation_allowed,
    ).summary_row()


__all__ = [
    "APPROVAL_SCOPE",
    "APPROVED_SOURCES",
    "LIVE_OBSERVE_APPROVAL_CONTRACT",
    "RealtimeLiveObserveApprovalValidation",
    "validate_live_observe_approval",
]
