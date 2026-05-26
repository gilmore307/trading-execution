"""Side-effect-free realtime market-data adapter planning.

This module plans provider subscriptions against the reviewed realtime coverage
catalog. It does not open sockets, call HTTP APIs, resolve secrets, or write
runtime observations. Real provider adapters must pass through these planning
contracts before a later approved live-observe implementation can exist.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from .contracts import realtime_data_interfaces, realtime_input_coverage_matrix

ALLOWED_MODES = ("dry_run", "fixture_replay", "live_observe")


@dataclass(frozen=True)
class RealtimeInstrumentRequest:
    """Instrument requested for realtime observation planning."""

    instrument_ref: str
    asset_class: str
    symbol: str | None = None

    def summary_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RealtimeSubscriptionPlan:
    """Provider subscription plan row; side-effect-free."""

    contract_type: str
    request_id: str
    mode: str
    source_id: str
    realtime_interfaces: tuple[str, ...]
    model_layers: tuple[str, ...]
    instruments: tuple[RealtimeInstrumentRequest, ...]
    subscription_status: str
    requires_secret_alias: bool
    required_gate_refs: tuple[str, ...]
    provider_calls_performed: int
    broker_calls_performed: int
    boundary_note: str

    def summary_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["realtime_interfaces"] = list(self.realtime_interfaces)
        row["model_layers"] = list(self.model_layers)
        row["instruments"] = [instrument.summary_row() for instrument in self.instruments]
        row["required_gate_refs"] = list(self.required_gate_refs)
        return row


def _coerce_string_list(value: Any, *, default: Sequence[str]) -> tuple[str, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        result = tuple(str(item) for item in value)
        return result or tuple(default)
    raise ValueError("expected string or list of strings")


def _coerce_instruments(payload: Mapping[str, Any]) -> tuple[RealtimeInstrumentRequest, ...]:
    raw_instruments = payload.get("instruments") or payload.get("instrument_refs") or []
    instruments: list[RealtimeInstrumentRequest] = []
    if isinstance(raw_instruments, Sequence) and not isinstance(raw_instruments, (str, bytes)):
        for item in raw_instruments:
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
    elif isinstance(raw_instruments, str):
        instruments.append(RealtimeInstrumentRequest(instrument_ref=raw_instruments, asset_class="unspecified", symbol=raw_instruments))
    else:
        raise ValueError("instruments must be a list or string")

    if instruments:
        return tuple(instruments)
    return (
        RealtimeInstrumentRequest(instrument_ref="reviewed-runtime-universe", asset_class="mixed", symbol=None),
    )


def build_realtime_subscription_plan(request: Mapping[str, Any]) -> dict[str, Any]:
    """Build a side-effect-free realtime subscription plan.

    Parameters are intentionally generic so manager/task payloads can reuse the
    helper without adding a second request schema too early. The function returns
    a serializable plan and always reports zero provider/broker calls.
    """

    request_id = str(request.get("request_id") or "rtplan_dry_run").strip()
    mode = str(request.get("mode") or "dry_run")
    if mode not in ALLOWED_MODES:
        raise ValueError(f"mode must be one of {', '.join(ALLOWED_MODES)}")

    interfaces_by_source = {interface.source_id: interface for interface in realtime_data_interfaces()}
    coverage_rows = realtime_input_coverage_matrix()
    all_sources = tuple(interfaces_by_source)
    all_layers = tuple(row.model_layer for row in coverage_rows)
    requested_sources = _coerce_string_list(request.get("sources") or request.get("source_ids"), default=all_sources)
    requested_layers = _coerce_string_list(request.get("model_layers"), default=all_layers)
    instruments = _coerce_instruments(request)

    context_sources = {"derived_model_context", "derived_governance_context", "execution_account_state", "calendar_discovery"}
    unknown_sources = sorted(set(requested_sources) - set(all_sources) - context_sources)
    if unknown_sources:
        raise ValueError(f"unknown realtime source ids: {', '.join(unknown_sources)}")
    unknown_layers = sorted(set(requested_layers) - set(all_layers))
    if unknown_layers:
        raise ValueError(f"unknown model layers: {', '.join(unknown_layers)}")

    live_approval_ref = str(request.get("live_stream_approval_ref") or "").strip()
    allow_live_streams = bool(request.get("allow_live_streams"))
    plans: list[RealtimeSubscriptionPlan] = []
    for source_id in requested_sources:
        source_layers = tuple(
            row.model_layer
            for row in coverage_rows
            if row.model_layer in requested_layers and source_id in row.primary_realtime_sources
        )
        if not source_layers:
            continue

        interface = interfaces_by_source.get(source_id)
        realtime_interfaces = interface.realtime_interfaces if interface else (f"{source_id}_context_ref",)
        requires_secret_alias = interface.auth_requirement != "public_market_data_without_login_private_account_streams_require_login" if interface else False
        if mode == "live_observe" and not (allow_live_streams and live_approval_ref):
            status = "blocked_requires_live_stream_approval_ref"
            gates = ("live_stream_approval_ref", "runtime_adapter_acceptance", "secret_alias_review")
        elif mode == "live_observe":
            status = "planned_live_observe_not_executed"
            gates = (live_approval_ref, "runtime_adapter_acceptance")
        else:
            status = f"{mode}_plan_ready_no_provider_calls"
            gates = ("runtime_adapter_acceptance_before_live",)

        plans.append(
            RealtimeSubscriptionPlan(
                contract_type="execution_realtime_subscription_plan",
                request_id=request_id,
                mode=mode,
                source_id=source_id,
                realtime_interfaces=tuple(realtime_interfaces),
                model_layers=source_layers,
                instruments=instruments,
                subscription_status=status,
                requires_secret_alias=requires_secret_alias,
                required_gate_refs=tuple(gates),
                provider_calls_performed=0,
                broker_calls_performed=0,
                boundary_note=(
                    "Planning is side-effect-free. It opens no sockets, performs no HTTP requests, resolves no secrets, "
                    "activates no models, and mutates no broker/account state."
                ),
            )
        )

    return {
        "contract_type": "execution_realtime_subscription_plan_set",
        "request_id": request_id,
        "mode": mode,
        "requested_sources": list(requested_sources),
        "requested_model_layers": list(requested_layers),
        "subscription_plans": [plan.summary_row() for plan in plans],
        "provider_calls_performed": 0,
        "broker_calls_performed": 0,
        "plan_status": "blocked_or_ready_rows_present" if plans else "no_matching_realtime_routes",
    }


__all__ = [
    "ALLOWED_MODES",
    "RealtimeInstrumentRequest",
    "RealtimeSubscriptionPlan",
    "build_realtime_subscription_plan",
]
