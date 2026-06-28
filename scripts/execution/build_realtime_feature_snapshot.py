#!/usr/bin/env python3
"""Build a side-effect-free realtime feature snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from trading_execution.market_data import build_realtime_feature_snapshot


def _context_refs(values: list[str] | None) -> dict[str, str]:
    refs: dict[str, str] = {}
    for value in values or []:
        if "=" not in value:
            raise ValueError("--upstream-context-ref must use MODEL_LAYER=REF")
        layer, ref = value.split("=", 1)
        layer = layer.strip()
        ref = ref.strip()
        if not layer or not ref:
            raise ValueError("--upstream-context-ref must use MODEL_LAYER=REF")
        refs[layer] = ref
    return refs


def _payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if args.request_json:
        payload.update(json.loads(Path(args.request_json).read_text(encoding="utf-8")))
    upstream_context_refs = _context_refs(args.upstream_context_ref)
    payload.update(
        {
            key: value
            for key, value in {
                "snapshot_id": args.snapshot_id,
                "decision_time": args.decision_time,
                "feature_time": args.feature_time,
                "available_time": args.available_time,
                "tradeable_time": args.tradeable_time,
                "instrument_ref": args.instrument_ref,
                "dataset_role": args.dataset_role,
                "historical_dataset_snapshot_ref": args.historical_dataset_snapshot_ref,
                "frozen_model_config_ref": args.frozen_model_config_ref,
                "model_layers": args.model_layer,
                "source_capture_refs": args.source_capture_ref,
                "upstream_context_refs": upstream_context_refs,
                "calendar_context_refs": args.calendar_context_ref,
            }.items()
            if value not in (None, [], "")
        }
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build realtime_feature_snapshot without external calls.")
    parser.add_argument("--request-json", help="Optional JSON request payload to merge before CLI overrides.")
    parser.add_argument("--snapshot-id")
    parser.add_argument("--decision-time", required=True)
    parser.add_argument("--feature-time")
    parser.add_argument("--available-time")
    parser.add_argument("--tradeable-time")
    parser.add_argument("--instrument-ref", required=True)
    parser.add_argument("--dataset-role", default="shadow_monitoring")
    parser.add_argument("--historical-dataset-snapshot-ref", required=True)
    parser.add_argument("--frozen-model-config-ref", required=True)
    parser.add_argument("--model-layer", action="append", dest="model_layer")
    parser.add_argument("--source-capture-ref", action="append", dest="source_capture_ref")
    parser.add_argument("--upstream-context-ref", action="append", dest="upstream_context_ref", help="Explicit upstream context ref as MODEL_LAYER=REF. Required for downstream model layers to be decision-ready.")
    parser.add_argument("--calendar-context-ref", action="append", dest="calendar_context_ref")
    args = parser.parse_args()

    print(json.dumps(build_realtime_feature_snapshot(_payload_from_args(args)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
