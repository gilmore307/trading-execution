"""Realtime model decision-effectiveness aggregation.

This module keeps live/shadow model-quality evidence intentionally light. It
summarizes matured decision outcomes for monitoring and review without creating
historical training/test rows, activating models, writing persistence, or
mutating broker/account state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence

FORBIDDEN_EFFECTIVENESS_ACTIONS = (
    "historical_dataset_row_creation",
    "historical_snapshot_rewrite",
    "model_refit",
    "model_activation",
    "broker_order_construction",
    "broker_order_submission",
    "broker_mutation",
    "account_mutation",
)

_CORRECT_STATUSES = {"correct", "hit", "true_positive", "true_negative"}
_INCORRECT_STATUSES = {"incorrect", "miss", "false_positive", "false_negative"}
_VALID_STATUSES = _CORRECT_STATUSES | _INCORRECT_STATUSES | {"unknown", "unmatured"}


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _coerce_records(records: Sequence[Mapping[str, Any]] | None) -> list[Mapping[str, Any]]:
    if records is None:
        return []
    if isinstance(records, (str, bytes, bytearray)):
        raise ValueError("decision records must be a list of objects")
    normalized: list[Mapping[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"decision_records[{index}] must be an object")
        normalized.append(record)
    return normalized


def realtime_decision_effectiveness_contract() -> dict[str, Any]:
    """Return the realtime decision-effectiveness monitoring contract."""

    return {
        "contract_type": "realtime_model_decision_effectiveness_contract",
        "required_decision_record_fields": [
            "decision_id",
            "model_id",
            "model_layer",
            "instrument_ref",
            "decision_time",
            "evaluation_horizon_seconds",
            "matured_outcome_ref",
            "correctness_status",
        ],
        "accepted_correctness_statuses": sorted(_VALID_STATUSES),
        "forbidden_actions": list(FORBIDDEN_EFFECTIVENESS_ACTIONS),
        "boundary_note": (
            "Aggregates matured live/shadow decision outcomes for monitoring only. It does not create historical "
            "dataset rows, refit or activate models, construct orders, submit broker calls, persist state, or mutate accounts."
        ),
    }


def build_realtime_decision_effectiveness(
    decision_records: Sequence[Mapping[str, Any]] | None,
    *,
    effectiveness_id: str | None = None,
    model_id: str | None = None,
    model_layer: str | None = None,
    evaluation_window_ref: str | None = None,
    generated_at: str | None = None,
    requested_actions: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Aggregate mature realtime/shadow decision outcomes without side effects."""

    records = _coerce_records(decision_records)
    requested_action_set = {str(action) for action in (requested_actions or [])}
    forbidden_actions_present = sorted(requested_action_set.intersection(FORBIDDEN_EFFECTIVENESS_ACTIONS))
    generated_at_value = generated_at or _now_iso()
    filtered_records = [
        record
        for record in records
        if (model_id is None or str(record.get("model_id")) == model_id)
        and (model_layer is None or str(record.get("model_layer")) == model_layer)
    ]

    total = len(filtered_records)
    matured = 0
    correct = 0
    incorrect = 0
    unknown = 0
    status_counts: dict[str, int] = {}
    model_counts: dict[str, int] = {}
    layer_counts: dict[str, int] = {}
    instrument_counts: dict[str, int] = {}
    record_errors: list[str] = []
    row_summaries: list[dict[str, Any]] = []
    required = realtime_decision_effectiveness_contract()["required_decision_record_fields"]

    for index, record in enumerate(filtered_records):
        missing = [field for field in required if record.get(field) in (None, "", [], {})]
        if missing:
            record_errors.append(f"decision_records[{index}] missing fields: {', '.join(missing)}")
        status = str(record.get("correctness_status") or "unknown").strip().lower()
        if status not in _VALID_STATUSES:
            record_errors.append(f"decision_records[{index}] invalid correctness_status: {status}")
            status = "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        decision_model_id = str(record.get("model_id") or "unknown")
        decision_layer = str(record.get("model_layer") or "unknown")
        instrument = str(record.get("instrument_ref") or "unknown")
        model_counts[decision_model_id] = model_counts.get(decision_model_id, 0) + 1
        layer_counts[decision_layer] = layer_counts.get(decision_layer, 0) + 1
        instrument_counts[instrument] = instrument_counts.get(instrument, 0) + 1
        is_matured = status != "unmatured" and bool(record.get("matured_outcome_ref"))
        if is_matured:
            matured += 1
        if status in _CORRECT_STATUSES:
            correct += 1
        elif status in _INCORRECT_STATUSES:
            incorrect += 1
        else:
            unknown += 1
        row_summaries.append(
            {
                "contract_type": "realtime_model_decision_effectiveness_row",
                "decision_id": record.get("decision_id"),
                "model_id": decision_model_id,
                "model_layer": decision_layer,
                "instrument_ref": instrument,
                "evaluation_horizon_seconds": record.get("evaluation_horizon_seconds"),
                "matured_outcome_ref": record.get("matured_outcome_ref"),
                "correctness_status": status,
            }
        )

    denominator = correct + incorrect
    accuracy = (correct / denominator) if denominator else None
    hit_rate = (correct / matured) if matured else None
    readiness = "ready_for_monitoring_review" if total and matured and not record_errors and not forbidden_actions_present else "blocked_missing_matured_decision_evidence"
    return {
        "contract_type": "realtime_model_decision_effectiveness",
        "effectiveness_id": effectiveness_id
        or _stable_id(
            "rteff",
            {
                "generated_at": generated_at_value,
                "model_id": model_id or "all",
                "model_layer": model_layer or "all",
                "record_count": total,
                "evaluation_window_ref": evaluation_window_ref or "adhoc",
            },
        ),
        "generated_at": generated_at_value,
        "evaluation_window_ref": evaluation_window_ref or "adhoc_realtime_shadow_window",
        "model_id_filter": model_id,
        "model_layer_filter": model_layer,
        "decision_count": total,
        "matured_decision_count": matured,
        "correct_decision_count": correct,
        "incorrect_decision_count": incorrect,
        "unknown_decision_count": unknown,
        "accuracy": accuracy,
        "hit_rate": hit_rate,
        "status_counts": status_counts,
        "model_counts": model_counts,
        "model_layer_counts": layer_counts,
        "instrument_counts": instrument_counts,
        "decision_rows": row_summaries,
        "record_errors": record_errors,
        "forbidden_actions_present": forbidden_actions_present,
        "readiness_status": readiness,
        "historical_dataset_rows_created": 0,
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "broker_order_construction_performed": False,
        "account_mutation_performed": False,
        "persistence_performed": False,
        "boundary_note": (
            "Monitoring aggregate only. Historical dataset construction remains with the historical pipeline; "
            "this helper performs no model activation, order construction, broker call, persistence, or account mutation."
        ),
    }


def validate_realtime_decision_effectiveness(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a realtime decision-effectiveness aggregate."""

    required = {
        "contract_type",
        "effectiveness_id",
        "generated_at",
        "decision_count",
        "matured_decision_count",
        "correct_decision_count",
        "incorrect_decision_count",
        "readiness_status",
        "decision_rows",
    }
    present = {key for key, value in candidate.items() if value not in (None, "", [], {})}
    missing_fields = sorted(required - present)
    forbidden_actions_present = sorted(set(candidate.get("forbidden_actions_present") or []))
    mutation_flags = {
        "historical_dataset_rows_created": int(candidate.get("historical_dataset_rows_created") or 0),
        "provider_calls_performed": int(candidate.get("provider_calls_performed") or 0),
        "broker_calls_performed": int(candidate.get("broker_calls_performed") or 0),
        "model_activation_performed": bool(candidate.get("model_activation_performed")),
        "broker_order_construction_performed": bool(candidate.get("broker_order_construction_performed")),
        "account_mutation_performed": bool(candidate.get("account_mutation_performed")),
        "persistence_performed": bool(candidate.get("persistence_performed")),
    }
    mutation_detected = any(value for value in mutation_flags.values())
    valid = not missing_fields and not forbidden_actions_present and not mutation_detected and not candidate.get("record_errors")
    return {
        "contract_type": "realtime_model_decision_effectiveness_validation",
        "effectiveness_id": candidate.get("effectiveness_id"),
        "valid": valid,
        "missing_fields": missing_fields,
        "forbidden_actions_present": forbidden_actions_present,
        "mutation_flags": mutation_flags,
        "mutation_detected": mutation_detected,
        "record_errors": list(candidate.get("record_errors") or []),
    }


__all__ = [
    "FORBIDDEN_EFFECTIVENESS_ACTIONS",
    "build_realtime_decision_effectiveness",
    "realtime_decision_effectiveness_contract",
    "validate_realtime_decision_effectiveness",
]
