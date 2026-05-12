"""Formal read-only realtime provider observation.

The entrypoint here is intentionally narrow: it may perform provider market-data
HTTP observations only after a valid ``realtime_live_observe_approval`` and
an explicit execute flag. It never activates models, persists manager decisions,
constructs orders, or mutates accounts.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib import error, parse, request

from .capture import validate_realtime_capture
from .features import build_model_decision_input_snapshot, build_realtime_feature_snapshot
from .live_approval import validate_live_observe_approval
from .live_observe import _CAPTURE_INTERFACE_BY_SOURCE, _SOURCE_ASSET_CLASS, build_live_observe_adapter_plan

Transport = Callable[[str, Mapping[str, str]], Mapping[str, Any]]


@dataclass(frozen=True)
class RealtimeLiveObservation:
    """One read-only provider observation result."""

    contract_type: str
    observation_id: str
    request_id: str
    approval_id: str
    source_id: str
    instrument_ref: str
    observation_time: str
    provider_available_time: str
    tradeable_time: str
    provider_endpoint: str
    provider_status: str
    normalized_payload_ref: str
    normalized_payload: Mapping[str, Any]
    provider_calls_performed: int
    broker_calls_performed: int
    model_activation_performed: bool
    broker_order_construction_performed: bool
    account_mutation_performed: bool

    def summary_row(self) -> dict[str, Any]:
        return asdict(self)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _plus_seconds(value: str, seconds: int) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (parsed + timedelta(seconds=seconds)).isoformat()


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _http_json_transport(url: str, headers: Mapping[str, str]) -> Mapping[str, Any]:
    req = request.Request(url, headers=dict(headers), method="GET")
    try:
        with request.urlopen(req, timeout=15) as response:  # noqa: S310 - explicit approved provider market-data call.
            body = response.read().decode("utf-8")
            payload = json.loads(body) if body else {}
            if not isinstance(payload, Mapping):
                return {"payload": payload}
            return dict(payload)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        return {"error": "http_error", "status_code": exc.code, "body": body[:1000]}
    except (error.URLError, TimeoutError, OSError) as exc:
        return {"error": "transport_error", "message": str(exc)}


def _instrument_values(instruments: Sequence[Any]) -> list[str]:
    values: list[str] = []
    for item in instruments:
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, Mapping):
            values.append(str(item.get("instrument_ref") or item.get("symbol") or "").strip())
    return [value for value in values if value]


def _alpaca_headers(env: Mapping[str, str]) -> dict[str, str]:
    key, secret, _endpoint = _alpaca_secret_values(env)
    if not key or not secret:
        raise ValueError(
            "Alpaca live observe requires APCA_API_KEY_ID/APCA_API_SECRET_KEY environment variables "
            "or the registered Alpaca source secret JSON"
        )
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def _alpaca_secret_values(env: Mapping[str, str]) -> tuple[str | None, str | None, str | None]:
    """Resolve Alpaca market-data credentials without exposing secret values.

    The shared trading environment is anchored by ``trading-manager``. Runtime
    callers may inject standard Alpaca environment variables, but local
    OpenClaw-managed runs should also be able to use the registered source
    secret JSON shape under ``/root/secrets/alpaca.json``.
    """

    key = env.get("APCA_API_KEY_ID") or env.get("ALPACA_API_KEY_ID") or env.get("APCA_API_KEY")
    secret = env.get("APCA_API_SECRET_KEY") or env.get("ALPACA_API_SECRET_KEY") or env.get("APCA_API_SECRET")
    endpoint = env.get("ALPACA_DATA_BASE_URL") or env.get("APCA_DATA_BASE_URL")
    if key and secret:
        return key, secret, endpoint

    secret_path = Path(env.get("ALPACA_SECRET_FILE") or "/root/secrets/alpaca.json")
    if not secret_path.exists():
        return key, secret, endpoint
    try:
        payload = json.loads(secret_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Alpaca source secret JSON could not be loaded") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("Alpaca source secret JSON must be an object")

    key = key or _optional_secret_text(payload.get("api_key"))
    secret = secret or _optional_secret_text(payload.get("secret_key"))
    endpoint = endpoint or _optional_secret_text(payload.get("endpoint"))
    return key, secret, endpoint


def _optional_secret_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _alpaca_data_base_url(request_payload: Mapping[str, Any], env: Mapping[str, str]) -> str:
    explicit = request_payload.get("alpaca_data_base_url") or env.get("ALPACA_DATA_BASE_URL") or env.get("APCA_DATA_BASE_URL")
    if explicit:
        return str(explicit).rstrip("/")
    _key, _secret, endpoint = _alpaca_secret_values(env)
    if endpoint and "data.alpaca" in endpoint:
        return endpoint.rstrip("/")
    return "https://data.alpaca.markets"


def _provider_request(source_id: str, instrument_ref: str, request_payload: Mapping[str, Any], env: Mapping[str, str]) -> tuple[str, dict[str, str]]:
    if source_id == "okx":
        inst_id = request_payload.get("okx_inst_id") or instrument_ref
        query = parse.urlencode({"instId": inst_id})
        return f"https://www.okx.com/api/v5/market/ticker?{query}", {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 OpenClaw formal-live-observe/1.0",
        }
    if source_id == "alpaca":
        feed = request_payload.get("alpaca_feed") or "iex"
        base_url = _alpaca_data_base_url(request_payload, env)
        symbol = parse.quote(instrument_ref, safe="")
        query = parse.urlencode({"feed": feed})
        return f"{base_url}/v2/stocks/{symbol}/snapshot?{query}", _alpaca_headers(env)
    if source_id == "thetadata":
        template = request_payload.get("thetadata_url_template")
        if not template:
            raise ValueError("ThetaData live observe requires reviewed thetadata_url_template in the request payload")
        return str(template).format(symbol=parse.quote(instrument_ref, safe=""), instrument_ref=parse.quote(instrument_ref, safe="")), {}
    raise ValueError(f"source {source_id} does not have a direct provider HTTP observe route")


def execute_live_observe(
    request_payload: Mapping[str, Any],
    *,
    approval: Mapping[str, Any],
    execute_live_observe: bool = False,
    transport: Transport | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Execute approved read-only provider observations and return capture refs.

    ``execute_live_observe`` must be true. Tests and plan-only callers should
    use false and inspect the validation/blocked result.
    """

    request_id = str(request_payload.get("request_id") or "rtlive_observe")
    sources = [str(item) for item in (request_payload.get("sources") or request_payload.get("source_ids") or approval.get("approved_sources") or [])]
    instruments = _instrument_values(request_payload.get("instruments") or request_payload.get("instrument_refs") or approval.get("approved_instrument_refs") or [])
    provider_sources = [source for source in sources if source in {"alpaca", "thetadata", "okx"}]
    estimated_calls = len(provider_sources) * max(1, len(instruments))
    validation = validate_live_observe_approval(
        approval,
        requested_sources=sources,
        requested_instrument_refs=instruments,
        requested_provider_calls=estimated_calls,
    )
    if not execute_live_observe or not validation["valid"]:
        return {
            "contract_type": "execution_realtime_live_observe_result",
            "request_id": request_id,
            "approval_validation": validation,
            "observations": [],
            "captures": [],
            "feature_snapshot": None,
            "decision_input_snapshot": None,
            "provider_calls_performed": 0,
            "broker_calls_performed": 0,
            "model_activation_performed": False,
            "broker_order_construction_performed": False,
            "account_mutation_performed": False,
            "live_observe_status": "blocked" if not validation["valid"] else "ready_requires_execute_live_observe_flag",
        }

    transport = transport or _http_json_transport
    env = env or os.environ
    observation_time = str(request_payload.get("observation_time") or request_payload.get("decision_time") or _now_iso())
    provider_available_time = str(request_payload.get("provider_available_time") or _plus_seconds(observation_time, 0))
    tradeable_time = str(request_payload.get("tradeable_time") or _plus_seconds(provider_available_time, 0))
    observations: list[dict[str, Any]] = []
    captures: list[dict[str, Any]] = []
    provider_calls = 0
    plan_set = build_live_observe_adapter_plan({**dict(request_payload), "mode": "live_observe", "allow_live_streams": True, "live_stream_approval_ref": approval.get("approval_id")})
    adapter_layers = {str(row["source_id"]): list(row.get("model_layers") or []) for row in plan_set.get("adapter_plans", []) if isinstance(row, Mapping)}

    for source_id in provider_sources:
        for instrument_ref in instruments:
            endpoint, headers = _provider_request(source_id, instrument_ref, request_payload, env)
            payload = transport(endpoint, headers)
            provider_calls += 1
            provider_status = "observed" if not payload.get("error") else "provider_error"
            observation_id = _stable_id("rtobs", {"request_id": request_id, "source_id": source_id, "instrument_ref": instrument_ref, "time": observation_time})
            normalized_payload_ref = f"memory://realtime-live-observe/{request_id}/{observation_id}/payload"
            observation = RealtimeLiveObservation(
                contract_type="realtime_live_observation",
                observation_id=observation_id,
                request_id=request_id,
                approval_id=str(approval.get("approval_id")),
                source_id=source_id,
                instrument_ref=instrument_ref,
                observation_time=observation_time,
                provider_available_time=provider_available_time,
                tradeable_time=tradeable_time,
                provider_endpoint=endpoint.split("?")[0],
                provider_status=provider_status,
                normalized_payload_ref=normalized_payload_ref,
                normalized_payload={"source_id": source_id, "instrument_ref": instrument_ref, "payload": payload},
                provider_calls_performed=1,
                broker_calls_performed=0,
                model_activation_performed=False,
                broker_order_construction_performed=False,
                account_mutation_performed=False,
            ).summary_row()
            observations.append(observation)
            for layer in adapter_layers.get(source_id, []):
                capture_id = _stable_id("rtcap", {"observation_id": observation_id, "layer": layer})
                row = {
                    "contract_type": "realtime_capture_row",
                    "capture_id": capture_id,
                    "request_id": request_id,
                    "model_layer": layer,
                    "observation_time": observation_time,
                    "provider_available_time": provider_available_time,
                    "tradeable_time": tradeable_time,
                    "source_id": source_id,
                    "realtime_interface": _CAPTURE_INTERFACE_BY_SOURCE.get(source_id, f"{source_id}.live_observe"),
                    "asset_class": _SOURCE_ASSET_CLASS.get(source_id, "unspecified"),
                    "instrument_ref": instrument_ref,
                    "normalized_payload_ref": normalized_payload_ref,
                    "frozen_model_config_ref": str(request_payload.get("frozen_model_config_ref") or "trading-model://frozen-model-config/review-required"),
                    "model_output_ref": f"shadow://realtime-live-observe/{request_id}/{instrument_ref}/{layer}/model_output_ref_pending",
                    "dataset_snapshot_ref": str(request_payload.get("historical_dataset_snapshot_ref") or request_payload.get("dataset_snapshot_ref") or "trading-model://historical-dataset-snapshot/review-required"),
                    "dataset_role": str(request_payload.get("dataset_role") or "shadow_monitoring"),
                    "label_maturity_time": str(request_payload.get("label_maturity_time") or _plus_seconds(tradeable_time, int(request_payload.get("label_horizon_seconds") or 3600))),
                    "outcome_label_ref": f"label://realtime-live-observe/{request_id}/{instrument_ref}/{layer}/pending_maturity",
                    "ingestion_commit_ref": str(request_payload.get("ingestion_commit_ref") or "git://trading-execution/live-observe-runtime"),
                    "run_manifest_ref": f"artifact://trading-execution/{request_id}/run_manifest",
                    "artifact_ref": f"artifact://trading-execution/{request_id}/{capture_id}",
                    "ready_signal_ref": f"ready://trading-execution/{request_id}/{capture_id}",
                    "requested_actions": [],
                    "provider_calls_performed": 1,
                    "broker_calls_performed": 0,
                    "model_activation_performed": False,
                    "account_mutation_performed": False,
                }
                row["capture_validation"] = validate_realtime_capture(row)
                captures.append(row)

    source_capture_refs = [row["capture_id"] for row in captures]
    feature_snapshot = build_realtime_feature_snapshot({**dict(request_payload), "source_capture_refs": source_capture_refs}) if source_capture_refs else None
    decision_input = build_model_decision_input_snapshot({"feature_snapshot": feature_snapshot}) if feature_snapshot else None
    return {
        "contract_type": "execution_realtime_live_observe_result",
        "request_id": request_id,
        "approval_validation": validation,
        "adapter_plan_set": plan_set,
        "observations": observations,
        "captures": captures,
        "feature_snapshot": feature_snapshot,
        "decision_input_snapshot": decision_input,
        "provider_calls_performed": provider_calls,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "broker_order_construction_performed": False,
        "account_mutation_performed": False,
        "live_observe_status": "observed" if observations else "no_provider_observations",
    }


__all__ = ["RealtimeLiveObservation", "execute_live_observe"]
