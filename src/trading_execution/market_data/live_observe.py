"""Side-effect-free live-observe adapter and fixture scaffolds.

The functions here make provider-specific realtime routes concrete enough for
fixture/shadow rehearsal, while still refusing to open sockets, call HTTP APIs,
resolve secrets, activate models, construct orders, or mutate accounts.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence

from .adapters import ALLOWED_MODES, RealtimeInstrumentRequest, build_realtime_subscription_plan
from .features import build_model_decision_input_snapshot, build_realtime_feature_snapshot
from .contracts import realtime_input_coverage_matrix

LIVE_OBSERVE_SOURCES = (
    "alpaca",
    "thetadata",
    "okx",
    "calendar_discovery",
    "execution_account_state",
    "derived_model_context",
)

_PROVIDER_INTERFACE_HINTS = {
    "alpaca": (
        "alpaca_market_data_websocket.quote",
        "alpaca_market_data_websocket.trade",
        "alpaca_market_data_websocket.bar",
        "alpaca_market_data_http.snapshot",
        "alpaca_market_data_http.news_or_corporate_event_ref",
    ),
    "thetadata": (
        "thetadata_terminal_websocket.option_quote",
        "thetadata_terminal_websocket.option_trade",
        "thetadata_terminal_websocket.option_greeks_iv",
        "thetadata_terminal_websocket.option_open_interest_or_latest_snapshot",
    ),
    "okx": (
        "okx_public_websocket.tickers",
        "okx_public_websocket.trades",
        "okx_public_websocket.candles",
        "okx_public_rest_snapshot",
    ),
    "calendar_discovery": (
        "calendar_discovery.event_catalog_ref",
        "calendar_discovery.earnings_macro_trigger_ref",
    ),
    "execution_account_state": (
        "execution_account_state.position_snapshot_ref",
        "execution_account_state.pending_order_snapshot_ref",
        "execution_account_state.restriction_snapshot_ref",
        "execution_account_state.risk_budget_ref",
    ),
    "derived_model_context": (
        "derived_model_context.layer_state_stack_ref",
        "derived_model_context.freshness_quality_diagnostics_ref",
    ),
}

_SOURCE_ASSET_CLASS = {
    "alpaca": "us_equity",
    "thetadata": "us_option",
    "okx": "crypto_spot",
    "calendar_discovery": "event_context",
    "execution_account_state": "account_context_ref",
    "derived_model_context": "model_context_ref",
}

_SOURCE_APPROVAL_GATES = {
    "alpaca": ("live_stream_approval_ref", "alpaca_market_data_secret_alias_review", "runtime_adapter_acceptance"),
    "thetadata": ("live_stream_approval_ref", "theta_terminal_ready_signal", "theta_entitlement_review", "runtime_adapter_acceptance"),
    "okx": ("live_stream_approval_ref", "okx_public_market_data_policy_review", "runtime_adapter_acceptance"),
    "calendar_discovery": ("event_adapter_policy_review", "runtime_adapter_acceptance"),
    "execution_account_state": ("read_only_account_context_policy_review", "broker_account_no_mutation_invariant", "runtime_adapter_acceptance"),
    "derived_model_context": ("frozen_model_output_ref_review", "runtime_adapter_acceptance"),
}

_CAPTURE_INTERFACE_BY_SOURCE = {
    "alpaca": "alpaca_market_data_websocket",
    "thetadata": "thetadata_terminal_websocket",
    "okx": "okx_public_websocket",
    "calendar_discovery": "calendar_discovery.event_catalog_ref",
    "execution_account_state": "execution_account_state.read_only_context_ref",
    "derived_model_context": "derived_model_context.layer_state_stack_ref",
}


@dataclass(frozen=True)
class RealtimeLiveObserveAdapterPlan:
    """Concrete provider/source adapter route that remains non-executing."""

    contract_type: str
    adapter_id: str
    request_id: str
    mode: str
    source_id: str
    adapter_family: str
    intended_interfaces: tuple[str, ...]
    model_layers: tuple[str, ...]
    instruments: tuple[RealtimeInstrumentRequest, ...]
    fixture_capture_supported: bool
    live_observe_status: str
    required_gate_refs: tuple[str, ...]
    provider_calls_performed: int
    broker_calls_performed: int
    model_activation_performed: bool
    account_mutation_performed: bool
    boundary_note: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["intended_interfaces"] = list(self.intended_interfaces)
        row["model_layers"] = list(self.model_layers)
        row["instruments"] = [instrument.summary_row() for instrument in self.instruments]
        row["required_gate_refs"] = list(self.required_gate_refs)
        return row


def _stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _iso(value: Any, *, fallback: str | None = None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if fallback:
        return fallback
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _plus_minutes(value: str, minutes: int) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (parsed + timedelta(minutes=minutes)).isoformat()


def _coerce_string_list(value: Any, *, default: Sequence[str]) -> tuple[str, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        result = tuple(str(item) for item in value)
        return result or tuple(default)
    raise ValueError("expected string or list of strings")


def _coerce_instruments(payload: Mapping[str, Any]) -> tuple[RealtimeInstrumentRequest, ...]:
    raw = payload.get("instruments") or payload.get("instrument_refs") or [payload.get("instrument_ref") or "reviewed-runtime-universe"]
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise ValueError("instruments must be a list or string")
    instruments: list[RealtimeInstrumentRequest] = []
    for item in raw:
        if isinstance(item, str):
            instruments.append(RealtimeInstrumentRequest(instrument_ref=item, asset_class="unspecified", symbol=item))
        elif isinstance(item, Mapping):
            instrument_ref = str(item.get("instrument_ref") or item.get("symbol") or "").strip()
            if not instrument_ref:
                raise ValueError("instrument objects require instrument_ref or symbol")
            instruments.append(
                RealtimeInstrumentRequest(
                    instrument_ref=instrument_ref,
                    asset_class=str(item.get("asset_class") or "unspecified"),
                    symbol=str(item["symbol"]) if item.get("symbol") is not None else None,
                )
            )
        else:
            raise ValueError("instruments must contain strings or objects")
    return tuple(instruments)


def _coverage_layers_for_source(source_id: str, requested_layers: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        row.model_layer
        for row in realtime_input_coverage_matrix()
        if row.model_layer in requested_layers and source_id in row.primary_realtime_sources
    )


def build_live_observe_adapter_plan(request: Mapping[str, Any]) -> dict[str, Any]:
    """Build concrete provider/source adapter plans without execution."""

    request_id = str(request.get("request_id") or "rtlive_fixture").strip()
    mode = str(request.get("mode") or "fixture_replay")
    if mode not in ALLOWED_MODES:
        raise ValueError(f"mode must be one of {', '.join(ALLOWED_MODES)}")
    all_layers = tuple(row.model_layer for row in realtime_input_coverage_matrix())
    requested_layers = _coerce_string_list(request.get("model_layers"), default=all_layers)
    unknown_layers = sorted(set(requested_layers) - set(all_layers))
    if unknown_layers:
        raise ValueError(f"unknown model layers: {', '.join(unknown_layers)}")
    requested_sources = _coerce_string_list(request.get("sources") or request.get("source_ids"), default=LIVE_OBSERVE_SOURCES)
    unknown_sources = sorted(set(requested_sources) - set(LIVE_OBSERVE_SOURCES))
    if unknown_sources:
        raise ValueError(f"unknown live-observe source ids: {', '.join(unknown_sources)}")
    instruments = _coerce_instruments(request)
    live_approval_ref = str(request.get("live_stream_approval_ref") or "").strip()
    allow_live_streams = bool(request.get("allow_live_streams"))

    adapter_plans: list[RealtimeLiveObserveAdapterPlan] = []
    for source_id in requested_sources:
        layers = _coverage_layers_for_source(source_id, requested_layers)
        if not layers:
            continue
        gate_refs = _SOURCE_APPROVAL_GATES[source_id]
        if mode == "live_observe" and not (allow_live_streams and live_approval_ref):
            status = "blocked_requires_live_stream_approval_ref"
            required_gate_refs = gate_refs
        elif mode == "live_observe":
            status = "planned_live_observe_not_executed"
            required_gate_refs = (live_approval_ref,) + tuple(ref for ref in gate_refs if ref != "live_stream_approval_ref")
        else:
            status = f"{mode}_adapter_fixture_ready_no_provider_calls"
            required_gate_refs = tuple(ref for ref in gate_refs if ref != "live_stream_approval_ref")
        adapter_plans.append(
            RealtimeLiveObserveAdapterPlan(
                contract_type="execution_realtime_live_observe_adapter_plan",
                adapter_id=_stable_id("rtadapter", {"request_id": request_id, "source_id": source_id, "layers": layers, "mode": mode}),
                request_id=request_id,
                mode=mode,
                source_id=source_id,
                adapter_family=f"{source_id}_live_observe_fixture_adapter",
                intended_interfaces=_PROVIDER_INTERFACE_HINTS[source_id],
                model_layers=layers,
                instruments=instruments,
                fixture_capture_supported=True,
                live_observe_status=status,
                required_gate_refs=required_gate_refs,
                provider_calls_performed=0,
                broker_calls_performed=0,
                model_activation_performed=False,
                account_mutation_performed=False,
                boundary_note=(
                    "Adapter plan is concrete enough for fixture/shadow rehearsal, but it opens no sockets, "
                    "performs no HTTP requests, resolves no secrets, activates no models, constructs no orders, "
                    "and mutates no account state."
                ),
            )
        )

    subscription_plan = build_realtime_subscription_plan(
        {
            "request_id": request_id,
            "mode": mode,
            "sources": list(requested_sources),
            "model_layers": list(requested_layers),
            "instruments": [instrument.summary_row() for instrument in instruments],
            "allow_live_streams": allow_live_streams,
            "live_stream_approval_ref": live_approval_ref,
        }
    )
    return {
        "contract_type": "execution_realtime_live_observe_adapter_plan_set",
        "request_id": request_id,
        "mode": mode,
        "requested_sources": list(requested_sources),
        "requested_model_layers": list(requested_layers),
        "adapter_plans": [plan.summary_row() for plan in adapter_plans],
        "subscription_plan": subscription_plan,
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "account_mutation_performed": False,
        "plan_status": "adapter_fixture_rows_present" if adapter_plans else "no_matching_live_observe_routes",
    }


def build_realtime_capture_fixture(request: Mapping[str, Any]) -> dict[str, Any]:
    """Build valid capture rows for fixture/shadow rehearsal."""

    adapter_plan_set = request.get("adapter_plan_set")
    if isinstance(adapter_plan_set, Mapping):
        plan_set = dict(adapter_plan_set)
    else:
        plan_set = build_live_observe_adapter_plan(request)
    request_id = str(plan_set.get("request_id") or request.get("request_id") or "rtlive_fixture")
    observation_time = _iso(request.get("observation_time") or request.get("decision_time"))
    provider_available_time = _iso(request.get("provider_available_time"), fallback=_plus_minutes(observation_time, 0))
    tradeable_time = _iso(request.get("tradeable_time"), fallback=_plus_minutes(provider_available_time, 0))
    label_maturity_time = _iso(request.get("label_maturity_time"), fallback=_plus_minutes(tradeable_time, int(request.get("label_horizon_minutes") or 60)))
    frozen_model_config_ref = str(request.get("frozen_model_config_ref") or "trading-model://frozen-model-config/review-required")
    dataset_snapshot_ref = str(request.get("historical_dataset_snapshot_ref") or request.get("dataset_snapshot_ref") or "trading-model://historical-dataset-snapshot/review-required")
    dataset_role = str(request.get("dataset_role") or "shadow_monitoring")
    ingestion_commit_ref = str(request.get("ingestion_commit_ref") or "git://trading-execution/fixture-not-persisted")
    coverage = {row.model_layer: row for row in realtime_input_coverage_matrix()}
    captures: list[dict[str, Any]] = []

    for adapter in plan_set.get("adapter_plans", []):
        if not isinstance(adapter, Mapping):
            continue
        source_id = str(adapter.get("source_id") or "")
        for instrument in adapter.get("instruments") or []:
            if not isinstance(instrument, Mapping):
                continue
            instrument_ref = str(instrument.get("instrument_ref") or instrument.get("symbol") or "reviewed-runtime-universe")
            asset_class = str(instrument.get("asset_class") or _SOURCE_ASSET_CLASS.get(source_id, "unspecified"))
            if asset_class == "unspecified":
                asset_class = _SOURCE_ASSET_CLASS.get(source_id, "unspecified")
            for layer in adapter.get("model_layers") or []:
                model_output = coverage[str(layer)].model_output
                capture_id = _stable_id(
                    "rtcap",
                    {"request_id": request_id, "source_id": source_id, "instrument_ref": instrument_ref, "model_layer": layer, "time": observation_time},
                )
                captures.append(
                    {
                        "contract_type": "realtime_capture_fixture_row",
                        "capture_id": capture_id,
                        "request_id": request_id,
                        "model_layer": layer,
                        "observation_time": observation_time,
                        "provider_available_time": provider_available_time,
                        "tradeable_time": tradeable_time,
                        "source_id": source_id,
                        "realtime_interface": _CAPTURE_INTERFACE_BY_SOURCE.get(source_id, f"{source_id}.fixture_ref"),
                        "asset_class": asset_class,
                        "instrument_ref": instrument_ref,
                        "normalized_payload_ref": f"fixture://realtime/{request_id}/{source_id}/{instrument_ref}/{layer}/normalized_payload",
                        "frozen_model_config_ref": frozen_model_config_ref,
                        "model_output_ref": f"fixture://realtime/{request_id}/{instrument_ref}/{layer}/{model_output}",
                        "dataset_snapshot_ref": dataset_snapshot_ref,
                        "dataset_role": dataset_role,
                        "label_maturity_time": label_maturity_time,
                        "outcome_label_ref": f"fixture://realtime/{request_id}/{instrument_ref}/{layer}/outcome_label_after_maturity",
                        "ingestion_commit_ref": ingestion_commit_ref,
                        "run_manifest_ref": f"fixture://realtime/{request_id}/run_manifest",
                        "artifact_ref": f"fixture://realtime/{request_id}/{capture_id}/artifact",
                        "ready_signal_ref": f"fixture://realtime/{request_id}/{capture_id}/ready_signal",
                        "provider_calls_performed": 0,
                        "broker_calls_performed": 0,
                        "model_activation_performed": False,
                        "account_mutation_performed": False,
                    }
                )
    return {
        "contract_type": "execution_realtime_capture_fixture_set",
        "request_id": request_id,
        "adapter_plan_set_ref": f"fixture://realtime/{request_id}/adapter_plan_set",
        "captures": captures,
        "capture_refs": [row["capture_id"] for row in captures],
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "account_mutation_performed": False,
        "fixture_status": "capture_fixture_rows_ready" if captures else "no_capture_fixture_rows",
    }


def build_realtime_shadow_fixture_bundle(request: Mapping[str, Any]) -> dict[str, Any]:
    """Build execution-side live-observe fixture, features, and model input."""

    plan_set = build_live_observe_adapter_plan(request)
    capture_fixture = build_realtime_capture_fixture({**dict(request), "adapter_plan_set": plan_set})
    source_capture_refs = capture_fixture.get("capture_refs") or ["fixture://realtime-capture/reviewed-runtime-universe"]
    feature_snapshot = build_realtime_feature_snapshot({**dict(request), "source_capture_refs": source_capture_refs})
    decision_input = build_model_decision_input_snapshot({"feature_snapshot": feature_snapshot})
    return {
        "contract_type": "execution_realtime_shadow_fixture_bundle",
        "request_id": plan_set["request_id"],
        "mode": plan_set["mode"],
        "adapter_plan_set": plan_set,
        "capture_fixture": capture_fixture,
        "feature_snapshot": feature_snapshot,
        "decision_input_snapshot": decision_input,
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "model_activation_performed": False,
        "broker_order_construction_performed": False,
        "account_mutation_performed": False,
        "bundle_status": "ready_for_model_route_plan" if decision_input.get("readiness_status") == "ready_for_historical_model_decision_handoff" else "blocked",
    }


__all__ = [
    "LIVE_OBSERVE_SOURCES",
    "RealtimeLiveObserveAdapterPlan",
    "build_live_observe_adapter_plan",
    "build_realtime_capture_fixture",
    "build_realtime_shadow_fixture_bundle",
]
