#!/usr/bin/env python3
"""Build a realtime-to-historical-model decision input snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from trading_execution.market_data import build_model_decision_input_snapshot


def _payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if args.request_json:
        payload.update(json.loads(Path(args.request_json).read_text(encoding="utf-8")))
    if args.feature_snapshot:
        payload["feature_snapshot"] = json.loads(Path(args.feature_snapshot).read_text(encoding="utf-8"))
    payload.update(
        {
            key: value
            for key, value in {
                "decision_input_snapshot_id": args.decision_input_snapshot_id,
                "decision_time": args.decision_time,
                "instrument_ref": args.instrument_ref,
                "dataset_role": args.dataset_role,
                "historical_dataset_snapshot_ref": args.historical_dataset_snapshot_ref,
                "frozen_model_config_ref": args.frozen_model_config_ref,
                "realtime_feature_snapshot_ref": args.realtime_feature_snapshot_ref,
                "model_layers": args.model_layer,
                "source_capture_refs": args.source_capture_ref,
            }.items()
            if value not in (None, [], "")
        }
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build execution_model_decision_input_snapshot_v1 without activating models.")
    parser.add_argument("--request-json", help="Optional JSON request payload to merge before CLI overrides.")
    parser.add_argument("--feature-snapshot", help="Existing realtime_feature_snapshot_v1 JSON path.")
    parser.add_argument("--decision-input-snapshot-id")
    parser.add_argument("--decision-time")
    parser.add_argument("--instrument-ref")
    parser.add_argument("--dataset-role", default="shadow_monitoring")
    parser.add_argument("--historical-dataset-snapshot-ref")
    parser.add_argument("--frozen-model-config-ref")
    parser.add_argument("--realtime-feature-snapshot-ref")
    parser.add_argument("--model-layer", action="append", dest="model_layer")
    parser.add_argument("--source-capture-ref", action="append", dest="source_capture_ref")
    args = parser.parse_args()

    payload = _payload_from_args(args)
    if "feature_snapshot" not in payload:
        missing = [
            name
            for name in ("decision_time", "instrument_ref", "historical_dataset_snapshot_ref", "frozen_model_config_ref")
            if not payload.get(name)
        ]
        if missing:
            parser.error("missing required fields without --feature-snapshot: " + ", ".join(missing))
    print(json.dumps(build_model_decision_input_snapshot(payload), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
