"""Realtime feature and model-decision input scaffolds.

The helpers in this module bridge realtime capture into the historical model
input world without performing live provider calls, model activation, storage
writes, or broker/account mutation. They prepare deterministic fixture/shadow
handoff objects that later adapters can populate with real observations after
separate approval gates exist.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence

from trading_execution.runtime.components import (
    REQUIRED_RUNTIME_COMPONENT_ORDER,
    RUNTIME_COMPONENT_ORDER,
    runtime_component_manifest,
)

from .contracts import realtime_capture_contract, realtime_input_coverage_matrix

MODEL_LAYER_ORDER = tuple(row.model_layer for row in realtime_input_coverage_matrix())
ACCEPTED_DATASET_ROLES = realtime_capture_contract().accepted_dataset_roles
FORBIDDEN_REALTIME_DECISION_ACTIONS = realtime_capture_contract().forbidden_actions + (
    "live_model_inference_activation",
    "production_decision_activation",
)

_HISTORICAL_FEATURE_PARITY_REFS = {
    "model_01_background_context": "trading-model://src/models/model_01_background_context",
    "model_02_target_state": "trading-model://src/models/model_02_target_state",
    "model_03_event_state": "trading-model://src/models/model_03_event_state",
    "model_04_unified_decision": "trading-model://src/models/model_04_unified_decision",
    "model_05_option_expression": "trading-data://src/data_feature/m05_option_expression_feature_generation",
    "model_06_residual_event_governance": "trading-data://src/data_feature/m06_residual_event_governance_feature_generation",
}


@dataclass(frozen=True)
class RealtimeFeatureSnapshotRow:
    """One model-layer feature row prepared from realtime capture refs."""

    contract_type: str
    snapshot_id: str
    model_layer: str
    model_id: str
    model_output: str
    instrument_ref: str
    feature_time: str
    available_time: str
    tradeable_time: str
    realtime_input_groups: tuple[str, ...]
    source_capture_refs: tuple[str, ...]
    upstream_context_refs: tuple[str, ...]
    calendar_context_refs: tuple[str, ...]
    feature_ref: str
    historical_feature_parity_ref: str
    coverage_status: str
    freshness_status: str
    quality_status: str
    decision_readiness_status: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["realtime_input_groups"] = list(self.realtime_input_groups)
        row["source_capture_refs"] = list(self.source_capture_refs)
        row["upstream_context_refs"] = list(self.upstream_context_refs)
        row["calendar_context_refs"] = list(self.calendar_context_refs)
        return row


@dataclass(frozen=True)
class ModelDecisionComponentInput:
    """One C-runtime component input ref for a historical-model decision handoff."""

    contract_type: str
    decision_input_snapshot_id: str
    component_id: str
    component_step: str
    component_name: str
    required_model_surfaces: tuple[str, ...]
    optional_model_surfaces: tuple[str, ...]
    feature_ref: str
    upstream_context_refs: tuple[str, ...]
    frozen_model_config_ref: str
    historical_dataset_snapshot_ref: str
    realtime_feature_snapshot_ref: str
    decision_handoff_status: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["required_model_surfaces"] = list(self.required_model_surfaces)
        row["optional_model_surfaces"] = list(self.optional_model_surfaces)
        row["upstream_context_refs"] = list(self.upstream_context_refs)
        return row


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _manifest_checksum(payload: Mapping[str, Any]) -> str:
    body = dict(payload)
    body.pop("manifest_checksum", None)
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()[:16]


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _iso(value: Any, *, fallback: str | None = None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if fallback:
        return fallback
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _coerce_string_list(value: Any, *, default: Sequence[str] = ()) -> tuple[str, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return tuple(str(item) for item in value)
    raise ValueError("expected string or list of strings")


def _capture_refs(value: Any) -> tuple[str, ...]:
    if value is None:
        return ("fixture://realtime-capture/reviewed-runtime-universe",)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        refs: list[str] = []
        for item in value:
            if isinstance(item, str):
                refs.append(item)
            elif isinstance(item, Mapping):
                ref = item.get("capture_id") or item.get("artifact_ref") or item.get("normalized_payload_ref")
                if not ref:
                    raise ValueError("capture objects require capture_id, artifact_ref, or normalized_payload_ref")
                refs.append(str(ref))
            else:
                raise ValueError("capture refs must contain strings or objects")
        return tuple(refs) or ("fixture://realtime-capture/reviewed-runtime-universe",)
    raise ValueError("capture refs must be a string or list")


def _dict_string_map(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("expected object mapping model layers to refs")
    return {str(key): str(val) for key, val in value.items() if val not in (None, "")}


def _context_ref_map(value: Any) -> dict[str, tuple[str, ...]]:
    if value is None:
        return {}
    if isinstance(value, str):
        return {"*": (value,)}
    if isinstance(value, Mapping):
        refs: dict[str, tuple[str, ...]] = {}
        for key, val in value.items():
            if val in (None, "", []):
                continue
            refs[str(key)] = _coerce_string_list(val)
        return refs
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return {"*": tuple(str(item) for item in value if item not in (None, ""))}
    raise ValueError("context refs must be a string, list, or object mapping model layers to refs")


def _requested_layers(value: Any) -> tuple[str, ...]:
    layers = _coerce_string_list(value, default=MODEL_LAYER_ORDER)
    unknown = sorted(set(layers) - set(MODEL_LAYER_ORDER))
    if unknown:
        raise ValueError(f"unknown model layers: {', '.join(unknown)}")
    return tuple(layer for layer in MODEL_LAYER_ORDER if layer in layers)


def realtime_feature_snapshot_contract() -> dict[str, Any]:
    """Return the side-effect-free realtime feature snapshot contract."""

    return {
        "contract_type": "realtime_feature_snapshot_contract",
        "required_fields": [
            "snapshot_id",
            "decision_time",
            "feature_time",
            "available_time",
            "tradeable_time",
            "instrument_ref",
            "dataset_role",
            "historical_dataset_snapshot_ref",
            "frozen_model_config_ref",
            "feature_rows",
        ],
        "required_layer_rows": list(MODEL_LAYER_ORDER),
        "optional_fields": [
            "calendar_context_refs",
            "feature_rows[].calendar_context_refs",
        ],
        "accepted_dataset_roles": list(ACCEPTED_DATASET_ROLES),
        "forbidden_actions": list(FORBIDDEN_REALTIME_DECISION_ACTIONS),
        "boundary_note": (
            "Realtime features must preserve historical feature parity refs, point-in-time timing fields, "
            "and model/config snapshot refs. The contract performs no provider calls, model activation, "
            "persistence, or broker/account mutation."
        ),
    }


def model_decision_input_snapshot_contract() -> dict[str, Any]:
    """Return the model-decision handoff contract for realtime snapshots."""

    return {
        "contract_type": "execution_model_decision_input_snapshot_contract",
        "required_fields": [
            "decision_input_snapshot_id",
            "decision_time",
            "instrument_ref",
            "dataset_role",
            "historical_dataset_snapshot_ref",
            "frozen_model_config_ref",
            "realtime_feature_snapshot_ref",
            "runtime_component_manifest",
            "component_input_refs",
        ],
        "required_component_inputs": list(REQUIRED_RUNTIME_COMPONENT_ORDER),
        "runtime_component_manifest_contract": runtime_component_manifest()["contract_type"],
        "runtime_component_manifest_version": runtime_component_manifest()["manifest_version"],
        "forbidden_actions": list(FORBIDDEN_REALTIME_DECISION_ACTIONS),
        "boundary_note": (
            "The handoff object is the execution-side bridge from realtime feature snapshots to historical "
            "model data decision inputs routed by execution runtime component. It is a fixture/shadow-ready envelope and does not activate a model "
            "or authorize orders."
        ),
    }


def build_realtime_feature_snapshot(request: Mapping[str, Any]) -> dict[str, Any]:
    """Build a realtime feature snapshot from refs without side effects."""

    decision_time = _iso(request.get("decision_time"))
    feature_time = _iso(request.get("feature_time"), fallback=decision_time)
    available_time = _iso(request.get("available_time"), fallback=feature_time)
    tradeable_time = _iso(request.get("tradeable_time"), fallback=available_time)
    instrument_ref = str(request.get("instrument_ref") or "reviewed-runtime-universe").strip()
    dataset_role = str(request.get("dataset_role") or "shadow_monitoring")
    requested_layers = _requested_layers(request.get("model_layers"))
    snapshot_id = str(request.get("snapshot_id") or _stable_id("rtfeat", {"instrument_ref": instrument_ref, "decision_time": decision_time, "layers": requested_layers}))
    historical_dataset_snapshot_ref = str(
        request.get("historical_dataset_snapshot_ref")
        or request.get("dataset_snapshot_ref")
        or "trading-model://historical-dataset-snapshot/review-required"
    )
    frozen_model_config_ref = str(request.get("frozen_model_config_ref") or "trading-model://frozen-model-config/review-required")
    feature_refs = _dict_string_map(request.get("feature_refs"))
    upstream_refs = _dict_string_map(request.get("upstream_context_refs"))
    calendar_refs = _context_ref_map(request.get("calendar_context_refs"))
    source_capture_refs = _capture_refs(request.get("source_capture_refs") or request.get("captures"))
    allow_placeholder_context_refs = bool(request.get("allow_placeholder_context_refs", False))

    rows: list[RealtimeFeatureSnapshotRow] = []
    missing_context_ref_layers: list[str] = []
    placeholder_context_layers: list[str] = []
    coverage_rows = {row.model_layer: row for row in realtime_input_coverage_matrix()}
    for layer in requested_layers:
        coverage = coverage_rows[layer]
        default_feature_ref = f"realtime-feature://{snapshot_id}/{layer}"
        upstream_ref = upstream_refs.get(layer)
        if not upstream_ref and allow_placeholder_context_refs and layer not in ("model_01_background_context",):
            upstream_ref = f"placeholder://upstream-context/{snapshot_id}/{layer}"
            placeholder_context_layers.append(layer)
        if not upstream_ref and layer not in ("model_01_background_context",):
            missing_context_ref_layers.append(layer)
        layer_calendar_refs = calendar_refs.get(layer) or calendar_refs.get("*") or ()
        row = RealtimeFeatureSnapshotRow(
            contract_type="realtime_feature_snapshot_row",
            snapshot_id=snapshot_id,
            model_layer=layer,
            model_id=coverage.model_id,
            model_output=coverage.model_output,
            instrument_ref=instrument_ref,
            feature_time=feature_time,
            available_time=available_time,
            tradeable_time=tradeable_time,
            realtime_input_groups=coverage.realtime_input_groups,
            source_capture_refs=source_capture_refs,
            upstream_context_refs=(upstream_ref,) if upstream_ref else (),
            calendar_context_refs=layer_calendar_refs,
            feature_ref=feature_refs.get(layer, default_feature_ref),
            historical_feature_parity_ref=_HISTORICAL_FEATURE_PARITY_REFS[layer],
            coverage_status=coverage.coverage_status,
            freshness_status=str(request.get("freshness_status") or "fixture_or_shadow_ready_not_live_observed"),
            quality_status=str(request.get("quality_status") or "requires_runtime_quality_metrics_before_live_activation"),
            decision_readiness_status="ready_for_model_decision_input_snapshot",
        )
        rows.append(row)

    missing_layers = sorted(set(MODEL_LAYER_ORDER) - {row.model_layer for row in rows})
    requested_actions = set(request.get("requested_actions") or [])
    forbidden_actions_present = sorted(requested_actions.intersection(FORBIDDEN_REALTIME_DECISION_ACTIONS))
    valid_role = dataset_role in ACCEPTED_DATASET_ROLES
    ready = not missing_layers and not forbidden_actions_present and valid_role and not missing_context_ref_layers

    return {
        "contract_type": "realtime_feature_snapshot",
        "snapshot_id": snapshot_id,
        "decision_time": decision_time,
        "feature_time": feature_time,
        "available_time": available_time,
        "tradeable_time": tradeable_time,
        "instrument_ref": instrument_ref,
        "dataset_role": dataset_role,
        "historical_dataset_snapshot_ref": historical_dataset_snapshot_ref,
        "frozen_model_config_ref": frozen_model_config_ref,
        "feature_rows": [row.summary_row() for row in rows],
        "calendar_context_refs": sorted({ref for row in rows for ref in row.calendar_context_refs}),
        "missing_model_layers": missing_layers,
        "missing_context_ref_layers": missing_context_ref_layers,
        "placeholder_context_layers": placeholder_context_layers,
        "forbidden_actions_present": forbidden_actions_present,
        "readiness_status": "ready_for_fixture_or_shadow_model_decision_input" if ready else "blocked_missing_realtime_feature_requirements",
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "persistence_performed": False,
        "boundary_note": (
            "Prepared from refs only. No provider stream/HTTP call, secret lookup, model activation, persistence, "
            "broker order construction, or account mutation was performed."
        ),
    }


def validate_realtime_feature_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a realtime feature snapshot for decision handoff readiness."""

    required = set(realtime_feature_snapshot_contract()["required_fields"])
    present = {key for key, value in snapshot.items() if value not in (None, "", [], {})}
    missing_fields = sorted(required - present)
    dataset_role_valid = snapshot.get("dataset_role") in ACCEPTED_DATASET_ROLES
    requested_actions = set(snapshot.get("requested_actions") or [])
    forbidden_actions_present = sorted(requested_actions.intersection(FORBIDDEN_REALTIME_DECISION_ACTIONS))
    invalid_time_fields = sorted(
        field
        for field in ("decision_time", "feature_time", "available_time", "tradeable_time")
        if field in snapshot and _parse_time(snapshot.get(field)) is None
    )
    feature_time = _parse_time(snapshot.get("feature_time"))
    available_time = _parse_time(snapshot.get("available_time"))
    tradeable_time = _parse_time(snapshot.get("tradeable_time"))
    no_future_leakage_timing = bool(
        feature_time is not None
        and available_time is not None
        and tradeable_time is not None
        and feature_time <= available_time <= tradeable_time
    )

    rows = snapshot.get("feature_rows") or []
    layer_set = {str(row.get("model_layer")) for row in rows if isinstance(row, Mapping)} if isinstance(rows, Sequence) else set()
    missing_layer_rows = sorted(set(MODEL_LAYER_ORDER) - layer_set)
    missing_context_ref_layers = sorted(str(layer) for layer in snapshot.get("missing_context_ref_layers") or [])
    row_errors: list[str] = []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        row_errors.append("feature_rows must be a list")
    else:
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                row_errors.append(f"feature_rows[{index}] must be an object")
                continue
            for field in ("model_layer", "model_id", "model_output", "feature_ref", "historical_feature_parity_ref"):
                if not row.get(field):
                    row_errors.append(f"feature_rows[{index}].{field} missing")
            calendar_row_refs = row.get("calendar_context_refs", [])
            if not isinstance(calendar_row_refs, Sequence) or isinstance(calendar_row_refs, (str, bytes, bytearray)):
                row_errors.append(f"feature_rows[{index}].calendar_context_refs must be a list")

    valid = (
        not missing_fields
        and dataset_role_valid
        and not forbidden_actions_present
        and not invalid_time_fields
        and no_future_leakage_timing
        and not missing_layer_rows
        and not missing_context_ref_layers
        and not row_errors
    )
    return {
        "contract_type": "realtime_feature_snapshot_validation",
        "snapshot_id": snapshot.get("snapshot_id"),
        "valid": valid,
        "missing_fields": missing_fields,
        "dataset_role_valid": dataset_role_valid,
        "forbidden_actions_present": forbidden_actions_present,
        "invalid_time_fields": invalid_time_fields,
        "no_future_leakage_timing": no_future_leakage_timing,
        "missing_layer_rows": missing_layer_rows,
        "missing_context_ref_layers": missing_context_ref_layers,
        "row_errors": row_errors,
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
    }


def build_model_decision_input_snapshot(request: Mapping[str, Any]) -> dict[str, Any]:
    """Build a model-decision input snapshot from a realtime feature snapshot."""

    feature_snapshot = request.get("feature_snapshot")
    if isinstance(feature_snapshot, Mapping):
        snapshot = dict(feature_snapshot)
    else:
        snapshot = build_realtime_feature_snapshot(request)

    validation = validate_realtime_feature_snapshot(snapshot)
    snapshot_id = str(snapshot.get("snapshot_id") or "rtfeat_missing")
    decision_time = str(snapshot.get("decision_time") or request.get("decision_time") or "")
    instrument_ref = str(snapshot.get("instrument_ref") or request.get("instrument_ref") or "reviewed-runtime-universe")
    decision_input_snapshot_id = str(
        request.get("decision_input_snapshot_id")
        or _stable_id("rtdecision", {"snapshot_id": snapshot_id, "decision_time": decision_time, "instrument_ref": instrument_ref})
    )
    frozen_model_config_ref = str(snapshot.get("frozen_model_config_ref") or request.get("frozen_model_config_ref") or "")
    historical_dataset_snapshot_ref = str(
        snapshot.get("historical_dataset_snapshot_ref") or request.get("historical_dataset_snapshot_ref") or ""
    )
    realtime_feature_snapshot_ref = str(request.get("realtime_feature_snapshot_ref") or f"realtime-feature-snapshot://{snapshot_id}")
    manifest = runtime_component_manifest()
    components_by_id = {str(row["component_id"]): row for row in manifest["components"]}

    component_inputs: list[ModelDecisionComponentInput] = []
    for component_id in RUNTIME_COMPONENT_ORDER:
        metadata = components_by_id[component_id]
        component_inputs.append(
            ModelDecisionComponentInput(
                contract_type="execution_model_decision_component_input",
                decision_input_snapshot_id=decision_input_snapshot_id,
                component_id=component_id,
                component_step=metadata["component_step"],
                component_name=metadata["component_name"],
                required_model_surfaces=metadata["required_model_surfaces"],
                optional_model_surfaces=metadata["optional_model_surfaces"],
                feature_ref=f"realtime-feature://{snapshot_id}/{component_id}",
                upstream_context_refs=(realtime_feature_snapshot_ref,),
                frozen_model_config_ref=frozen_model_config_ref,
                historical_dataset_snapshot_ref=historical_dataset_snapshot_ref,
                realtime_feature_snapshot_ref=realtime_feature_snapshot_ref,
                decision_handoff_status="ready_for_historical_model_decision_input",
            )
        )

    missing_component_inputs = sorted(set(REQUIRED_RUNTIME_COMPONENT_ORDER) - {row.component_id for row in component_inputs})
    requested_actions = set(request.get("requested_actions") or [])
    forbidden_actions_present = sorted(requested_actions.intersection(FORBIDDEN_REALTIME_DECISION_ACTIONS))
    ready = validation["valid"] and not missing_component_inputs and not forbidden_actions_present

    return {
        "contract_type": "execution_model_decision_input_snapshot",
        "decision_input_snapshot_id": decision_input_snapshot_id,
        "decision_time": decision_time,
        "instrument_ref": instrument_ref,
        "dataset_role": snapshot.get("dataset_role"),
        "historical_dataset_snapshot_ref": historical_dataset_snapshot_ref,
        "frozen_model_config_ref": frozen_model_config_ref,
        "realtime_feature_snapshot_ref": realtime_feature_snapshot_ref,
        "runtime_component_manifest": manifest,
        "component_input_refs": [row.summary_row() for row in component_inputs],
        "feature_snapshot_validation": validation,
        "missing_component_inputs": missing_component_inputs,
        "forbidden_actions_present": forbidden_actions_present,
        "readiness_status": "ready_for_historical_model_decision_handoff" if ready else "blocked_missing_model_decision_input_requirements",
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "broker_order_construction_performed": False,
        "account_mutation_performed": False,
        "boundary_note": (
            "This is a decision input envelope only. It may be used for fixture/shadow handoff to the historical "
            "model data decision stack, but it does not run or activate a model and cannot construct orders."
        ),
    }


def validate_model_decision_input_snapshot(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a realtime-to-model decision input handoff envelope."""

    required = set(model_decision_input_snapshot_contract()["required_fields"])
    present = {key for key, value in candidate.items() if value not in (None, "", [], {})}
    missing_fields = sorted(required - present)
    requested_actions = set(candidate.get("requested_actions") or [])
    forbidden_actions_present = sorted(requested_actions.intersection(FORBIDDEN_REALTIME_DECISION_ACTIONS))
    invalid_time_fields = ["decision_time"] if candidate.get("decision_time") and _parse_time(candidate.get("decision_time")) is None else []
    rows = candidate.get("component_input_refs") or []
    component_set = {str(row.get("component_id")) for row in rows if isinstance(row, Mapping)} if isinstance(rows, Sequence) else set()
    missing_component_inputs = sorted(set(REQUIRED_RUNTIME_COMPONENT_ORDER) - component_set)
    expected_manifest = runtime_component_manifest()
    manifest = candidate.get("runtime_component_manifest") or {}
    manifest_errors: list[str] = []
    if not isinstance(manifest, Mapping):
        manifest_errors.append("runtime_component_manifest must be an object")
    else:
        if manifest.get("contract_type") != expected_manifest["contract_type"]:
            manifest_errors.append("runtime_component_manifest contract_type invalid")
        if manifest.get("manifest_version") != expected_manifest["manifest_version"]:
            manifest_errors.append("runtime_component_manifest manifest_version invalid")
        if manifest.get("manifest_checksum") != expected_manifest["manifest_checksum"]:
            manifest_errors.append("runtime_component_manifest manifest_checksum invalid")
        if manifest.get("manifest_checksum") != _manifest_checksum(manifest):
            manifest_errors.append("runtime_component_manifest body checksum invalid")
        if dict(manifest) != expected_manifest:
            manifest_errors.append("runtime_component_manifest body does not match current execution manifest")
        if tuple(str(component_id) for component_id in manifest.get("component_order") or ()) != tuple(RUNTIME_COMPONENT_ORDER):
            manifest_errors.append("runtime_component_manifest component_order invalid")
        if (
            tuple(str(component_id) for component_id in manifest.get("required_component_order") or ())
            != tuple(REQUIRED_RUNTIME_COMPONENT_ORDER)
        ):
            manifest_errors.append("runtime_component_manifest required_component_order invalid")
    feature_validation = candidate.get("feature_snapshot_validation") or {}
    feature_snapshot_valid = bool(feature_validation.get("valid")) if isinstance(feature_validation, Mapping) else False
    row_errors: list[str] = []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        row_errors.append("component_input_refs must be a list")
    else:
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                row_errors.append(f"component_input_refs[{index}] must be an object")
                continue
            for field in ("component_id", "component_step", "component_name", "feature_ref"):
                if not row.get(field):
                    row_errors.append(f"component_input_refs[{index}].{field} missing")

    valid = (
        not missing_fields
        and not forbidden_actions_present
        and not invalid_time_fields
        and not missing_component_inputs
        and not manifest_errors
        and feature_snapshot_valid
        and not row_errors
    )
    return {
        "contract_type": "execution_model_decision_input_validation",
        "decision_input_snapshot_id": candidate.get("decision_input_snapshot_id"),
        "valid": valid,
        "missing_fields": missing_fields,
        "forbidden_actions_present": forbidden_actions_present,
        "invalid_time_fields": invalid_time_fields,
        "missing_component_inputs": missing_component_inputs,
        "runtime_component_manifest_errors": manifest_errors,
        "feature_snapshot_valid": feature_snapshot_valid,
        "row_errors": row_errors,
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "broker_order_construction_performed": False,
        "account_mutation_performed": False,
    }


__all__ = [
    "ACCEPTED_DATASET_ROLES",
    "FORBIDDEN_REALTIME_DECISION_ACTIONS",
    "MODEL_LAYER_ORDER",
    "ModelDecisionComponentInput",
    "RealtimeFeatureSnapshotRow",
    "REQUIRED_RUNTIME_COMPONENT_ORDER",
    "RUNTIME_COMPONENT_ORDER",
    "build_model_decision_input_snapshot",
    "build_realtime_feature_snapshot",
    "model_decision_input_snapshot_contract",
    "realtime_feature_snapshot_contract",
    "validate_model_decision_input_snapshot",
    "validate_realtime_feature_snapshot",
]
