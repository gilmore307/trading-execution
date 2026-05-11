"""Execution-owned realtime monitor smoke runner.

This module keeps the first live monitoring slice intentionally narrow:
bounded read-only provider observation for a reviewed ETF universe, plus a
small summary envelope. It does not activate models, construct orders, submit
broker calls, or mutate accounts.
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .live_provider import Transport, execute_live_observe

DEFAULT_REALTIME_MODEL_LAYERS = ("layer_01_market_regime", "layer_02_sector_context")
DEFAULT_UNIVERSE_PATH = "/root/projects/trading-storage/main/shared/market_regime_etf_universe.csv"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.isoformat()


def load_etf_universe(
    universe_path: str | Path = DEFAULT_UNIVERSE_PATH,
    *,
    model_layers: Sequence[str] | None = DEFAULT_REALTIME_MODEL_LAYERS,
    max_symbols: int | None = None,
) -> list[str]:
    """Load ETF symbols for realtime observe while preserving file order."""

    path = Path(universe_path)
    layers = set(model_layers or [])
    symbols: list[str] = []
    seen: set[str] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if layers and row.get("model_layer") not in layers:
                continue
            symbol = str(row.get("symbol") or row.get("ticker_symbol") or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            symbols.append(symbol)
            seen.add(symbol)
            if max_symbols is not None and len(symbols) >= max_symbols:
                break
    return symbols


def build_realtime_monitor_request(
    *,
    request_id: str,
    symbols: Sequence[str],
    source_id: str = "alpaca",
    model_layers: Sequence[str] = DEFAULT_REALTIME_MODEL_LAYERS,
    observation_time: str | None = None,
    label_horizon_seconds: int = 900,
) -> dict[str, Any]:
    """Build a read-only realtime monitor live-observe request."""

    observed_at = observation_time or _iso(_now())
    return {
        "contract_type": "execution_realtime_monitor_live_observe_request_v1",
        "request_id": request_id,
        "sources": [source_id],
        "model_layers": list(model_layers),
        "instrument_refs": list(symbols),
        "decision_time": observed_at,
        "observation_time": observed_at,
        "provider_available_time": observed_at,
        "tradeable_time": observed_at,
        "label_horizon_seconds": label_horizon_seconds,
        "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/review-required",
        "frozen_model_config_ref": "trading-model://configs/frozen/review-required",
        "dataset_role": "shadow_monitoring",
    }


def build_realtime_monitor_approval(
    *,
    approval_id: str,
    symbols: Sequence[str],
    source_id: str = "alpaca",
    approved_at_utc: str | None = None,
    expires_at_utc: str | None = None,
    max_provider_calls: int | None = None,
) -> dict[str, Any]:
    """Build the bounded read-only approval contract for monitor smoke."""

    approved_at = datetime.fromisoformat(approved_at_utc.replace("Z", "+00:00")) if approved_at_utc else _now()
    if approved_at.tzinfo is None:
        approved_at = approved_at.replace(tzinfo=timezone.utc)
    expires_at = (
        datetime.fromisoformat(expires_at_utc.replace("Z", "+00:00"))
        if expires_at_utc
        else approved_at + timedelta(minutes=30)
    )
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return {
        "contract_type": "realtime_live_observe_approval_v1",
        "approval_id": approval_id,
        "approval_scope": "realtime_market_data_observe_only",
        "approved_sources": [source_id],
        "approved_instrument_refs": list(symbols),
        "approved_at_utc": _iso(approved_at),
        "expires_at_utc": _iso(expires_at),
        "max_provider_calls": int(max_provider_calls if max_provider_calls is not None else len(symbols)),
        "execute_live_observe_allowed": True,
        "model_activation_allowed": False,
        "broker_execution_allowed": False,
        "broker_order_construction_allowed": False,
        "account_mutation_allowed": False,
    }


def summarize_live_observe_result(result: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize monitor output without including provider payload details."""

    observations = [row for row in result.get("observations", []) if isinstance(row, Mapping)]
    captures = [row for row in result.get("captures", []) if isinstance(row, Mapping)]
    provider_status_counts: dict[str, int] = {}
    instruments_observed: set[str] = set()
    for row in observations:
        status = str(row.get("provider_status") or "unknown")
        provider_status_counts[status] = provider_status_counts.get(status, 0) + 1
        instrument = str(row.get("instrument_ref") or "").strip()
        if instrument:
            instruments_observed.add(instrument)

    capture_valid_count = sum(1 for row in captures if isinstance(row.get("capture_validation"), Mapping) and row["capture_validation"].get("valid"))
    return {
        "contract_type": "execution_realtime_monitor_summary_v1",
        "request_id": result.get("request_id"),
        "live_observe_status": result.get("live_observe_status"),
        "provider_calls_performed": result.get("provider_calls_performed", 0),
        "broker_calls_performed": result.get("broker_calls_performed", 0),
        "model_activation_performed": bool(result.get("model_activation_performed")),
        "broker_order_construction_performed": bool(result.get("broker_order_construction_performed")),
        "account_mutation_performed": bool(result.get("account_mutation_performed")),
        "observation_count": len(observations),
        "capture_count": len(captures),
        "capture_valid_count": capture_valid_count,
        "instrument_count": len(instruments_observed),
        "provider_status_counts": provider_status_counts,
        "feature_snapshot_readiness": (result.get("feature_snapshot") or {}).get("readiness_status") if isinstance(result.get("feature_snapshot"), Mapping) else None,
        "decision_input_readiness": (result.get("decision_input_snapshot") or {}).get("readiness_status") if isinstance(result.get("decision_input_snapshot"), Mapping) else None,
    }


def run_realtime_monitor_smoke(
    *,
    request_id: str,
    approval_id: str,
    universe_path: str | Path = DEFAULT_UNIVERSE_PATH,
    source_id: str = "alpaca",
    model_layers: Sequence[str] = DEFAULT_REALTIME_MODEL_LAYERS,
    max_symbols: int | None = None,
    execute: bool = False,
    transport: Transport | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run or plan a bounded read-only realtime monitor smoke."""

    symbols = load_etf_universe(universe_path, model_layers=model_layers, max_symbols=max_symbols)
    request = build_realtime_monitor_request(
        request_id=request_id,
        symbols=symbols,
        source_id=source_id,
        model_layers=model_layers,
    )
    approval = build_realtime_monitor_approval(
        approval_id=approval_id,
        symbols=symbols,
        source_id=source_id,
    )
    result = execute_live_observe(
        request,
        approval=approval,
        execute_live_observe=execute,
        transport=transport,
        env=env,
    )
    summary = summarize_live_observe_result(result)
    return {
        "contract_type": "execution_realtime_monitor_smoke_receipt_v1",
        "request": request,
        "approval": approval,
        "result": result,
        "summary": summary,
    }


__all__ = [
    "DEFAULT_REALTIME_MODEL_LAYERS",
    "DEFAULT_UNIVERSE_PATH",
    "build_realtime_monitor_approval",
    "build_realtime_monitor_request",
    "load_etf_universe",
    "run_realtime_monitor_smoke",
    "summarize_live_observe_result",
]
