"""Realtime trading runtime readiness orchestration.

The runtime surface wires execution interfaces without submitting orders. It can
run continuously before the first promoted model exists: no active model pointer
means the runtime reports a waiting state while realtime data monitors and
external maintenance loops keep their own receipts current.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_execution.model_lifecycle import validate_active_model_config_write
from trading_execution.storage_paths import execution_storage_root

REALTIME_TRADING_RUNTIME_STATUS_CONTRACT = "execution_realtime_trading_runtime_status"
DEFAULT_ACTIVE_MODEL_CONFIG_PATH = (
    execution_storage_root() / "runtime" / "active_model" / "latest_active_model_config_write.json"
)


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(payload, dict):
        return None, "active model config payload must be a JSON object"
    return payload, None


def _active_pointer_state(active_model_config_path: Path) -> dict[str, Any]:
    payload, error = _read_json(active_model_config_path)
    if payload is None:
        if error:
            return {
                "active_model_pointer_status": "invalid_active_model_pointer",
                "active_model_config_path": str(active_model_config_path),
                "active_model_config_present": active_model_config_path.exists(),
                "active_model_config_validation": {
                    "contract_type": "execution_active_model_config_write_validation",
                    "validation_status": "failed",
                    "errors": [error],
                },
                "selected_active_model_ref": None,
                "new_active_config_ref": None,
            }
        return {
            "active_model_pointer_status": "missing_active_model_pointer",
            "active_model_config_path": str(active_model_config_path),
            "active_model_config_present": False,
            "active_model_config_validation": None,
            "selected_active_model_ref": None,
            "new_active_config_ref": None,
        }

    validation = validate_active_model_config_write(payload).to_dict()
    valid = validation["validation_status"] == "passed"
    return {
        "active_model_pointer_status": "valid_active_model_pointer" if valid else "invalid_active_model_pointer",
        "active_model_config_path": str(active_model_config_path),
        "active_model_config_present": True,
        "active_model_config_validation": validation,
        "selected_active_model_ref": payload.get("selected_active_model_ref") if valid else None,
        "new_active_config_ref": payload.get("new_active_config_ref") if valid else None,
        "rollback_ref": payload.get("rollback_ref") if valid else None,
        "write_window_ref": payload.get("write_window_ref") if valid else None,
    }


def build_realtime_trading_runtime_status(
    *,
    active_model_config_path: str | Path = DEFAULT_ACTIVE_MODEL_CONFIG_PATH,
    realtime_monitor_loop_ref: str | None = None,
    te_calendar_refresh_ref: str | None = None,
    allow_model_activation: bool = False,
    allow_order_construction: bool = False,
    allow_broker_execution: bool = False,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Build a side-effect-free runtime status record for live trading readiness."""

    pointer = _active_pointer_state(Path(active_model_config_path))
    pointer_status = pointer["active_model_pointer_status"]
    active_ready = pointer_status == "valid_active_model_pointer"

    if pointer_status == "missing_active_model_pointer":
        runtime_status = "waiting_for_promoted_model"
        next_gate = "write_active_model_config_after_promotion"
    elif pointer_status == "invalid_active_model_pointer":
        runtime_status = "blocked_invalid_active_model_pointer"
        next_gate = "repair_active_model_config_write"
    elif allow_broker_execution:
        runtime_status = "blocked_broker_submit_interface_not_implemented"
        next_gate = "implement_and_review_broker_submit_adapter"
    elif not allow_model_activation:
        runtime_status = "ready_for_active_model_pointer_requires_activation_gate"
        next_gate = "enable_model_activation_after_runtime_review"
    elif not allow_order_construction:
        runtime_status = "ready_for_model_inference_requires_order_construction_gate"
        next_gate = "provide_order_construction_approval_and_risk_cap"
    else:
        runtime_status = "ready_for_order_intent_construction_not_submission"
        next_gate = "construct_order_intent_after_decision_and_approval"

    return {
        "contract_type": REALTIME_TRADING_RUNTIME_STATUS_CONTRACT,
        "generated_at_utc": generated_at_utc or _now_utc(),
        "runtime_status": runtime_status,
        "next_gate": next_gate,
        "active_model_pointer": pointer,
        "interfaces_connected": {
            "realtime_monitor_loop": bool(realtime_monitor_loop_ref),
            "model_decision_input_snapshot": True,
            "active_model_config_write": True,
            "trade_risk_cap_validation": True,
            "broker_order_intent_construction": True,
            "broker_submit_adapter": False,
            "account_mutation_adapter": False,
            "trading_economics_recent_calendar_refresh": bool(te_calendar_refresh_ref),
        },
        "runtime_refs": {
            "realtime_monitor_loop_ref": realtime_monitor_loop_ref,
            "te_calendar_refresh_ref": te_calendar_refresh_ref,
        },
        "allowed_actions": {
            "live_provider_observation_allowed_by_this_record": False,
            "model_activation_allowed": bool(allow_model_activation and active_ready),
            "broker_order_construction_allowed": bool(allow_model_activation and allow_order_construction and active_ready),
            "broker_execution_allowed": False,
            "account_mutation_allowed": False,
        },
        "required_runtime_inputs": [
            "bounded realtime observe approval before live provider calls",
            "valid active model config write from promotion review",
            "point-in-time realtime model decision input snapshot",
            "model decision record with risk cap fields",
            "execution order construction approval before order-intent construction",
        ],
        "provider_calls_performed": 0,
        "model_activation_performed": False,
        "broker_order_construction_performed": False,
        "broker_calls_performed": 0,
        "account_mutation_performed": False,
        "boundary_note": (
            "This record wires execution runtime readiness only. It performs no provider call, model call, "
            "broker call, order submission, or account mutation."
        ),
    }


def run_realtime_trading_runtime_check(
    *,
    output_dir: str | Path | None = None,
    active_model_config_path: str | Path = DEFAULT_ACTIVE_MODEL_CONFIG_PATH,
    realtime_monitor_loop_ref: str | None = None,
    te_calendar_refresh_ref: str | None = None,
    allow_model_activation: bool = False,
    allow_order_construction: bool = False,
    allow_broker_execution: bool = False,
) -> dict[str, Any]:
    """Build and optionally persist a realtime trading runtime status record."""

    status = build_realtime_trading_runtime_status(
        active_model_config_path=active_model_config_path,
        realtime_monitor_loop_ref=realtime_monitor_loop_ref,
        te_calendar_refresh_ref=te_calendar_refresh_ref,
        allow_model_activation=allow_model_activation,
        allow_order_construction=allow_order_construction,
        allow_broker_execution=allow_broker_execution,
    )
    if output_dir is not None:
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        (path / "runtime_status.json").write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return status
