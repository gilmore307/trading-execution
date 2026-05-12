"""Realtime capture validation for forward/shadow evidence.

Validation is intentionally local and side-effect free. It checks whether a
candidate capture row has the fields and guardrails required by the reviewed
`realtime_capture_contract` before any future persistence or promotion
workflow consumes it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from .contracts import realtime_capture_contract


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def validate_realtime_capture(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a candidate realtime capture row without side effects."""

    contract = realtime_capture_contract()
    required_fields = set(contract.required_fields)
    present_fields = {key for key, value in candidate.items() if value not in (None, "", [], {})}
    missing_fields = sorted(required_fields - present_fields)
    dataset_role = candidate.get("dataset_role")
    dataset_role_valid = dataset_role in contract.accepted_dataset_roles
    requested_actions = set(candidate.get("requested_actions") or [])
    forbidden_actions_present = sorted(requested_actions.intersection(contract.forbidden_actions))

    tradeable_time = _parse_time(candidate.get("tradeable_time"))
    label_maturity_time = _parse_time(candidate.get("label_maturity_time"))
    observation_time = _parse_time(candidate.get("observation_time"))
    provider_available_time = _parse_time(candidate.get("provider_available_time"))
    invalid_time_fields = sorted(
        field
        for field, parsed in {
            "observation_time": observation_time,
            "provider_available_time": provider_available_time,
            "tradeable_time": tradeable_time,
            "label_maturity_time": label_maturity_time,
        }.items()
        if field in candidate and parsed is None
    )
    label_mature_after_tradeable = bool(
        tradeable_time is not None and label_maturity_time is not None and label_maturity_time >= tradeable_time
    )

    valid = (
        not missing_fields
        and dataset_role_valid
        and not forbidden_actions_present
        and not invalid_time_fields
        and label_mature_after_tradeable
    )
    return {
        "contract_type": "realtime_capture_validation",
        "capture_id": candidate.get("capture_id"),
        "valid": valid,
        "missing_fields": missing_fields,
        "dataset_role_valid": dataset_role_valid,
        "accepted_dataset_roles": list(contract.accepted_dataset_roles),
        "forbidden_actions_present": forbidden_actions_present,
        "invalid_time_fields": invalid_time_fields,
        "label_mature_after_tradeable": label_mature_after_tradeable,
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "boundary_note": "Validation performs no provider calls, broker calls, model activation, or persistence.",
    }


__all__ = ["validate_realtime_capture"]
