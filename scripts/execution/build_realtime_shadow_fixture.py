#!/usr/bin/env python3
"""Build an execution-side realtime shadow fixture bundle without side effects."""

from __future__ import annotations

import argparse
import json
import sys

from trading_execution.market_data import build_realtime_shadow_fixture_bundle


def main() -> int:
    parser = argparse.ArgumentParser(description="Build realtime adapter/capture/feature/model-input fixture bundle.")
    parser.add_argument("--request-id", default="rtshadow_fixture")
    parser.add_argument("--mode", choices=("dry_run", "fixture_replay", "live_observe"), default="fixture_replay")
    parser.add_argument("--source", action="append", dest="sources")
    parser.add_argument("--model-layer", action="append", dest="model_layers")
    parser.add_argument("--instrument-ref", action="append", dest="instrument_refs")
    parser.add_argument("--decision-time", required=True)
    parser.add_argument("--available-time")
    parser.add_argument("--tradeable-time")
    parser.add_argument("--label-maturity-time")
    parser.add_argument("--dataset-role", default="shadow_monitoring")
    parser.add_argument("--historical-dataset-snapshot-ref", required=True)
    parser.add_argument("--frozen-model-config-ref", required=True)
    parser.add_argument("--live-stream-approval-ref")
    parser.add_argument("--allow-live-streams", action="store_true")
    parser.add_argument(
        "--output",
        choices=("bundle", "adapter_plan_set", "capture_fixture", "feature_snapshot", "decision_input_snapshot"),
        default="bundle",
    )
    args = parser.parse_args()
    bundle = build_realtime_shadow_fixture_bundle(
        {
            "request_id": args.request_id,
            "mode": args.mode,
            "sources": args.sources,
            "model_layers": args.model_layers,
            "instrument_refs": args.instrument_refs,
            "decision_time": args.decision_time,
            "available_time": args.available_time,
            "tradeable_time": args.tradeable_time,
            "label_maturity_time": args.label_maturity_time,
            "dataset_role": args.dataset_role,
            "historical_dataset_snapshot_ref": args.historical_dataset_snapshot_ref,
            "frozen_model_config_ref": args.frozen_model_config_ref,
            "live_stream_approval_ref": args.live_stream_approval_ref,
            "allow_live_streams": args.allow_live_streams,
        }
    )
    payload = bundle if args.output == "bundle" else bundle[args.output]
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
