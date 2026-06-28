#!/usr/bin/env python3
"""Build a side-effect-free realtime feature snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from trading_execution.market_data import build_realtime_feature_snapshot


def _payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if args.request_json:
        payload.update(json.loads(Path(args.request_json).read_text(encoding="utf-8")))
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
                "calendar_context_refs": args.calendar_context_ref,
                "allow_placeholder_context_refs": args.allow_placeholder_context_refs,
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
    parser.add_argument("--calendar-context-ref", action="append", dest="calendar_context_ref")
    parser.add_argument("--allow-placeholder-context-refs", action="store_true", help="Allow fixture/shadow placeholder upstream refs for layers without reviewed context refs.")
    args = parser.parse_args()

    print(json.dumps(build_realtime_feature_snapshot(_payload_from_args(args)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
