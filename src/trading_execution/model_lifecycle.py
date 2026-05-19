"""Execution-owned runtime model lifecycle selection contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

SHADOW_CYCLE_SELECTION_CONTRACT = "execution_shadow_cycle_selection"
ACTIVE_MODEL_CONFIG_WRITE_CONTRACT = "execution_active_model_config_write"
REQUIRED_REVIEW_FIELDS = (
    "candidate_model_ref",
    "promotion_readiness_ref",
    "overall_rank",
    "review_status",
)
ELIMINATION_STATUSES = {"eliminate_candidate", "eliminate"}
PASSING_STATUSES = {"active_candidate", "realtime_candidate", "shadow_continue", "incumbent_active"}


@dataclass(frozen=True)
class RuntimeSelectionValidation:
    """Validation result for execution runtime model selection."""

    contract_type: str
    validation_status: str
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_type": self.contract_type,
            "validation_status": self.validation_status,
            "errors": list(self.errors),
        }


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_id(prefix: str, *parts: object) -> str:
    payload = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"


def _coerce_reviews(candidate_reviews: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(candidate_reviews, (str, bytes, bytearray)) or not isinstance(candidate_reviews, Sequence):
        raise ValueError("candidate_reviews must be a sequence of objects")
    reviews: list[dict[str, Any]] = []
    for index, review in enumerate(candidate_reviews):
        if not isinstance(review, Mapping):
            raise ValueError(f"candidate_reviews[{index}] must be an object")
        normalized = dict(review)
        for field in REQUIRED_REVIEW_FIELDS:
            if normalized.get(field) in (None, "", [], {}):
                raise ValueError(f"candidate_reviews[{index}].{field} is required")
        try:
            normalized["overall_rank"] = int(normalized["overall_rank"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"candidate_reviews[{index}].overall_rank must be an integer") from exc
        reviews.append(normalized)
    return reviews


def build_shadow_cycle_selection(
    *,
    cycle_ref: str,
    current_active_model_ref: str,
    candidate_reviews: Sequence[Mapping[str, Any]],
    cycle_duration_days: int = 30,
    selection_id: str | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build the execution-owned post-shadow runtime model selection record."""

    if not cycle_ref:
        raise ValueError("cycle_ref is required")
    if not current_active_model_ref:
        raise ValueError("current_active_model_ref is required")
    if cycle_duration_days < 1:
        raise ValueError("cycle_duration_days must be positive")

    reviews = sorted(_coerce_reviews(candidate_reviews), key=lambda row: (row["overall_rank"], str(row["candidate_model_ref"])))
    eliminated = [
        row
        for row in reviews
        if str(row.get("review_status")).lower() in ELIMINATION_STATUSES or bool(row.get("eliminate_candidate"))
    ]
    for row in eliminated:
        if not (row.get("elimination_reason") or row.get("elimination_reason_refs")):
            raise ValueError(f"eliminate candidate {row['candidate_model_ref']} requires sufficient reason evidence")

    eligible_ranked = [
        row
        for row in reviews
        if row not in eliminated and str(row.get("review_status")).lower() in PASSING_STATUSES
    ]
    if not eligible_ranked:
        raise ValueError("at least one non-eliminated runtime candidate is required")

    active_row = eligible_ranked[0]
    active_model_ref = str(active_row["candidate_model_ref"])
    realtime_rows = eligible_ranked[1:4]
    shadow_rows = eligible_ranked[4:]
    generated = generated_at_utc or _now_utc()
    selection = {
        "contract_type": SHADOW_CYCLE_SELECTION_CONTRACT,
        "selection_id": selection_id or _stable_id("execsel", cycle_ref, current_active_model_ref, active_model_ref, generated),
        "cycle_ref": cycle_ref,
        "cycle_duration_days": cycle_duration_days,
        "generated_at_utc": generated,
        "previous_active_model_ref": current_active_model_ref,
        "selected_active_model_ref": active_model_ref,
        "active_model_switch_recommended": active_model_ref != current_active_model_ref,
        "realtime_candidate_refs": [str(row["candidate_model_ref"]) for row in realtime_rows],
        "shadow_only_candidate_refs": [str(row["candidate_model_ref"]) for row in shadow_rows],
        "eliminate_candidate_refs": [str(row["candidate_model_ref"]) for row in eliminated],
        "candidate_review_rows": reviews,
        "selection_basis": (
            "Best overall market-hours shadow-cycle review becomes active. Ranks 2-4 remain realtime candidates; "
            "lower-ranked candidates continue shadow-only unless sufficient elimination evidence is present."
        ),
        "active_model_config_write_performed": False,
        "broker_order_construction_performed": False,
        "broker_execution_performed": False,
        "account_mutation_performed": False,
    }
    validation = validate_shadow_cycle_selection(selection)
    if validation.validation_status != "passed":
        raise ValueError("; ".join(validation.errors))
    return selection


def validate_shadow_cycle_selection(payload: Mapping[str, Any]) -> RuntimeSelectionValidation:
    errors: list[str] = []
    required = (
        "contract_type",
        "selection_id",
        "cycle_ref",
        "cycle_duration_days",
        "generated_at_utc",
        "previous_active_model_ref",
        "selected_active_model_ref",
        "realtime_candidate_refs",
        "shadow_only_candidate_refs",
        "eliminate_candidate_refs",
        "candidate_review_rows",
    )
    for field in required:
        if payload.get(field) in (None, ""):
            errors.append(f"{field} is required")
    if payload.get("contract_type") != SHADOW_CYCLE_SELECTION_CONTRACT:
        errors.append(f"contract_type must be {SHADOW_CYCLE_SELECTION_CONTRACT}")
    if int(payload.get("cycle_duration_days") or 0) < 1:
        errors.append("cycle_duration_days must be positive")
    realtime_refs = payload.get("realtime_candidate_refs")
    if not isinstance(realtime_refs, list):
        errors.append("realtime_candidate_refs must be a list")
    elif len(realtime_refs) > 3:
        errors.append("realtime_candidate_refs may contain only ranks 2-4")
    for field in ("shadow_only_candidate_refs", "eliminate_candidate_refs", "candidate_review_rows"):
        if not isinstance(payload.get(field), list):
            errors.append(f"{field} must be a list")
    for field in (
        "active_model_config_write_performed",
        "broker_order_construction_performed",
        "broker_execution_performed",
        "account_mutation_performed",
    ):
        if payload.get(field) is not False:
            errors.append(f"{field} must be false")
    return RuntimeSelectionValidation(
        contract_type="execution_shadow_cycle_selection_validation",
        validation_status="passed" if not errors else "failed",
        errors=tuple(errors),
    )


def build_active_model_config_write(
    *,
    shadow_cycle_selection: Mapping[str, Any],
    expected_previous_active_model_ref: str,
    new_active_config_ref: str,
    rollback_ref: str,
    write_window_ref: str,
    write_id: str | None = None,
    written_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build an audited active-model pointer write record from a valid selection."""

    selection_validation = validate_shadow_cycle_selection(shadow_cycle_selection)
    if selection_validation.validation_status != "passed":
        raise ValueError("; ".join(selection_validation.errors))
    selected_active = str(shadow_cycle_selection["selected_active_model_ref"])
    previous_active = str(shadow_cycle_selection["previous_active_model_ref"])
    if expected_previous_active_model_ref != previous_active:
        raise ValueError("expected_previous_active_model_ref must match selection previous_active_model_ref")
    for field, value in {
        "new_active_config_ref": new_active_config_ref,
        "rollback_ref": rollback_ref,
        "write_window_ref": write_window_ref,
    }.items():
        if not value:
            raise ValueError(f"{field} is required")
    written = written_at_utc or _now_utc()
    record = {
        "contract_type": ACTIVE_MODEL_CONFIG_WRITE_CONTRACT,
        "active_model_config_write_id": write_id
        or _stable_id(
            "activewrite",
            shadow_cycle_selection["selection_id"],
            previous_active,
            selected_active,
            new_active_config_ref,
            rollback_ref,
        ),
        "shadow_cycle_selection_ref": shadow_cycle_selection["selection_id"],
        "previous_active_model_ref": previous_active,
        "selected_active_model_ref": selected_active,
        "expected_previous_active_model_ref": expected_previous_active_model_ref,
        "new_active_config_ref": new_active_config_ref,
        "rollback_ref": rollback_ref,
        "write_window_ref": write_window_ref,
        "written_at_utc": written,
        "active_pointer_write_performed": True,
        "rollback_available": True,
        "broker_order_construction_performed": False,
        "broker_execution_performed": False,
        "account_mutation_performed": False,
    }
    validation = validate_active_model_config_write(record)
    if validation.validation_status != "passed":
        raise ValueError("; ".join(validation.errors))
    return record


def validate_active_model_config_write(payload: Mapping[str, Any]) -> RuntimeSelectionValidation:
    errors: list[str] = []
    required = (
        "contract_type",
        "active_model_config_write_id",
        "shadow_cycle_selection_ref",
        "previous_active_model_ref",
        "selected_active_model_ref",
        "expected_previous_active_model_ref",
        "new_active_config_ref",
        "rollback_ref",
        "write_window_ref",
        "written_at_utc",
    )
    for field in required:
        if payload.get(field) in (None, ""):
            errors.append(f"{field} is required")
    if payload.get("contract_type") != ACTIVE_MODEL_CONFIG_WRITE_CONTRACT:
        errors.append(f"contract_type must be {ACTIVE_MODEL_CONFIG_WRITE_CONTRACT}")
    if payload.get("expected_previous_active_model_ref") != payload.get("previous_active_model_ref"):
        errors.append("expected_previous_active_model_ref must match previous_active_model_ref")
    if payload.get("active_pointer_write_performed") is not True:
        errors.append("active_pointer_write_performed must be true")
    if payload.get("rollback_available") is not True:
        errors.append("rollback_available must be true")
    for field in ("broker_order_construction_performed", "broker_execution_performed", "account_mutation_performed"):
        if payload.get(field) is not False:
            errors.append(f"{field} must be false")
    return RuntimeSelectionValidation(
        contract_type="execution_active_model_config_write_validation",
        validation_status="passed" if not errors else "failed",
        errors=tuple(errors),
    )


__all__ = [
    "SHADOW_CYCLE_SELECTION_CONTRACT",
    "ACTIVE_MODEL_CONFIG_WRITE_CONTRACT",
    "RuntimeSelectionValidation",
    "build_active_model_config_write",
    "build_shadow_cycle_selection",
    "validate_active_model_config_write",
    "validate_shadow_cycle_selection",
]
