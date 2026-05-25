"""Execution-owned runtime model lifecycle selection contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

SHADOW_CYCLE_SELECTION_CONTRACT = "execution_shadow_cycle_selection"
ACTIVE_MODEL_CONFIG_WRITE_CONTRACT = "execution_active_model_config_write"
SHADOW_RUNTIME_COMPONENT_CONTRACT = "execution_shadow_runtime_component"
SHADOW_MODEL_RUNTIME_EVIDENCE_CONTRACT = "execution_shadow_model_runtime_evidence"
REQUIRED_REVIEW_FIELDS = (
    "candidate_model_ref",
    "promotion_readiness_ref",
    "overall_rank",
    "review_status",
)
ELIMINATION_STATUSES = {"eliminate_candidate", "eliminate"}
PASSING_STATUSES = {"active_candidate", "realtime_candidate", "shadow_continue", "incumbent_active"}
ACCEPTED_REVIEW_STATUSES = PASSING_STATUSES | ELIMINATION_STATUSES


@dataclass(frozen=True)
class ShadowRuntimeComponent:
    """Intraday component for live/shadow model comparison evidence."""

    component_step: str
    component_name: str
    component_id: str
    purpose: str
    runtime_data_mode: str
    replay_allowed: bool
    input_contracts: tuple[str, ...]
    output_contracts: tuple[str, ...]
    active_trading_authority_policy: str
    broker_mutation_allowed: bool = False
    account_mutation_allowed: bool = False
    active_pointer_write_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_type": SHADOW_RUNTIME_COMPONENT_CONTRACT,
            "component_step": self.component_step,
            "component_name": self.component_name,
            "component_id": self.component_id,
            "purpose": self.purpose,
            "runtime_data_mode": self.runtime_data_mode,
            "replay_allowed": self.replay_allowed,
            "input_contracts": list(self.input_contracts),
            "output_contracts": list(self.output_contracts),
            "active_trading_authority_policy": self.active_trading_authority_policy,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "account_mutation_allowed": self.account_mutation_allowed,
            "active_pointer_write_allowed": self.active_pointer_write_allowed,
        }


def shadow_runtime_component() -> ShadowRuntimeComponent:
    """Return the accepted intraday Shadow component.

    Shadow is separate from the live/Replay C01-C07 trading component graph. It
    runs only during live market-hours evidence collection for promoted models.
    """

    return ShadowRuntimeComponent(
        component_step="S01",
        component_name="Shadow Model Comparison",
        component_id="shadow_01_model_comparison",
        purpose=(
            "Run the active model and promoted-but-not-active shadow models over "
            "the same realtime market-hours snapshots, collect comparable "
            "decision-effectiveness evidence, and feed mature evidence into "
            "execution_shadow_cycle_selection."
        ),
        runtime_data_mode="realtime_market_hours_only",
        replay_allowed=False,
        input_contracts=(
            "promotion_readiness_record",
            ACTIVE_MODEL_CONFIG_WRITE_CONTRACT,
            "realtime_feature_snapshot",
            "execution_model_decision_input_snapshot",
            "realtime_model_decision_effectiveness",
        ),
        output_contracts=(
            SHADOW_MODEL_RUNTIME_EVIDENCE_CONTRACT,
            SHADOW_CYCLE_SELECTION_CONTRACT,
        ),
        active_trading_authority_policy=(
            "Only the current active model may route decisions into live C01-C06 "
            "trading authority. Shadow model decisions are evidence only until "
            "a later execution_active_model_config_write gate changes the active pointer."
        ),
    )


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


def _sha256_payload(payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


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


def _review_rows_by_ref(rows: Sequence[Mapping[str, Any]], errors: list[str]) -> dict[str, Mapping[str, Any]]:
    by_ref: dict[str, Mapping[str, Any]] = {}
    ranks: set[int] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            errors.append(f"candidate_review_rows[{index}] must be an object")
            continue
        for field in REQUIRED_REVIEW_FIELDS:
            if row.get(field) in (None, "", [], {}):
                errors.append(f"candidate_review_rows[{index}].{field} is required")
        ref = str(row.get("candidate_model_ref") or "").strip()
        if ref:
            if ref in by_ref:
                errors.append(f"candidate_review_rows duplicate candidate_model_ref: {ref}")
            by_ref[ref] = row
        try:
            rank = int(row.get("overall_rank"))
        except (TypeError, ValueError):
            errors.append(f"candidate_review_rows[{index}].overall_rank must be an integer")
            continue
        if rank < 1:
            errors.append(f"candidate_review_rows[{index}].overall_rank must be positive")
        if rank in ranks:
            errors.append(f"candidate_review_rows duplicate overall_rank: {rank}")
        ranks.add(rank)
        status = str(row.get("review_status") or "").lower()
        if status not in ACCEPTED_REVIEW_STATUSES:
            errors.append(f"candidate_review_rows[{index}].review_status is not accepted")
        if status in ELIMINATION_STATUSES and not (row.get("elimination_reason") or row.get("elimination_reason_refs")):
            errors.append(f"candidate_review_rows[{index}] eliminate candidate requires reason evidence")
    return by_ref


def _review_rank(row: Mapping[str, Any]) -> int:
    try:
        return int(row.get("overall_rank"))
    except (TypeError, ValueError):
        return 0


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
    if not isinstance(payload, Mapping):
        return RuntimeSelectionValidation(
            contract_type="execution_shadow_cycle_selection_validation",
            validation_status="failed",
            errors=("payload must be an object",),
        )
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
    try:
        cycle_duration_days = int(payload.get("cycle_duration_days") or 0)
    except (TypeError, ValueError):
        cycle_duration_days = 0
        errors.append("cycle_duration_days must be positive")
    if cycle_duration_days < 1 and "cycle_duration_days must be positive" not in errors:
        errors.append("cycle_duration_days must be positive")
    realtime_refs = payload.get("realtime_candidate_refs")
    if not isinstance(realtime_refs, list):
        errors.append("realtime_candidate_refs must be a list")
    elif len(realtime_refs) > 3:
        errors.append("realtime_candidate_refs may contain only ranks 2-4")
    for field in ("shadow_only_candidate_refs", "eliminate_candidate_refs", "candidate_review_rows"):
        if not isinstance(payload.get(field), list):
            errors.append(f"{field} must be a list")
    reviews = payload.get("candidate_review_rows")
    review_rows = reviews if isinstance(reviews, list) else []
    if not review_rows:
        errors.append("candidate_review_rows must be non-empty")
    rows_by_ref = _review_rows_by_ref(review_rows, errors)
    selected_ref = str(payload.get("selected_active_model_ref") or "")
    eligible_rows = [
        row
        for row in review_rows
        if isinstance(row, Mapping)
        and str(row.get("review_status") or "").lower() in PASSING_STATUSES
        and str(row.get("candidate_model_ref") or "")
    ]
    eligible_rows.sort(key=lambda row: (_review_rank(row), str(row.get("candidate_model_ref") or "")))
    expected_selected = str(eligible_rows[0].get("candidate_model_ref")) if eligible_rows else ""
    if eligible_rows and selected_ref != expected_selected:
        errors.append("selected_active_model_ref must match the top-ranked non-eliminated review row")
    elif not eligible_rows:
        errors.append("at least one non-eliminated runtime candidate is required")
    realtime_list = realtime_refs if isinstance(realtime_refs, list) else []
    shadow_list = payload.get("shadow_only_candidate_refs") if isinstance(payload.get("shadow_only_candidate_refs"), list) else []
    eliminate_list = payload.get("eliminate_candidate_refs") if isinstance(payload.get("eliminate_candidate_refs"), list) else []
    expected_realtime = [str(row["candidate_model_ref"]) for row in eligible_rows[1:4]]
    expected_shadow = [str(row["candidate_model_ref"]) for row in eligible_rows[4:]]
    expected_eliminate = [
        str(row["candidate_model_ref"])
        for row in review_rows
        if isinstance(row, Mapping) and str(row.get("review_status") or "").lower() in ELIMINATION_STATUSES
    ]
    if realtime_list != expected_realtime:
        errors.append("realtime_candidate_refs must match ranks 2-4 from eligible review rows")
    if shadow_list != expected_shadow:
        errors.append("shadow_only_candidate_refs must match eligible review rows after rank 4")
    if eliminate_list != expected_eliminate:
        errors.append("eliminate_candidate_refs must match eliminated review rows")
    roster_refs = [selected_ref, *map(str, realtime_list), *map(str, shadow_list), *map(str, eliminate_list)]
    if len([ref for ref in roster_refs if ref]) != len(set(ref for ref in roster_refs if ref)):
        errors.append("roster model refs must be unique across active, realtime, shadow, and eliminate sets")
    unknown_roster_refs = sorted(set(ref for ref in roster_refs if ref) - set(rows_by_ref))
    if unknown_roster_refs:
        errors.append(f"roster refs missing candidate review rows: {', '.join(unknown_roster_refs)}")
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
        "shadow_cycle_selection_digest": _sha256_payload(shadow_cycle_selection),
        "shadow_cycle_selection": dict(shadow_cycle_selection),
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
    if not isinstance(payload, Mapping):
        return RuntimeSelectionValidation(
            contract_type="execution_active_model_config_write_validation",
            validation_status="failed",
            errors=("payload must be an object",),
        )
    required = (
        "contract_type",
        "active_model_config_write_id",
        "shadow_cycle_selection_ref",
        "shadow_cycle_selection_digest",
        "shadow_cycle_selection",
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
    embedded_selection = payload.get("shadow_cycle_selection")
    if not isinstance(embedded_selection, Mapping):
        errors.append("shadow_cycle_selection must be embedded for pointer validation")
    else:
        selection_validation = validate_shadow_cycle_selection(embedded_selection)
        if selection_validation.validation_status != "passed":
            errors.extend(f"shadow_cycle_selection.{error}" for error in selection_validation.errors)
        if payload.get("shadow_cycle_selection_ref") != embedded_selection.get("selection_id"):
            errors.append("shadow_cycle_selection_ref must match embedded selection_id")
        if payload.get("previous_active_model_ref") != embedded_selection.get("previous_active_model_ref"):
            errors.append("previous_active_model_ref must match embedded selection previous_active_model_ref")
        if payload.get("selected_active_model_ref") != embedded_selection.get("selected_active_model_ref"):
            errors.append("selected_active_model_ref must match embedded selection selected_active_model_ref")
        if payload.get("shadow_cycle_selection_digest") != _sha256_payload(embedded_selection):
            errors.append("shadow_cycle_selection_digest must match embedded selection payload")
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
