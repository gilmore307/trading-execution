#!/usr/bin/env python3
"""Plan concrete realtime live-observe adapters without executing them."""

from __future__ import annotations

import argparse
import json
import sys

from trading_execution.market_data import build_live_observe_adapter_plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Build side-effect-free realtime live-observe adapter plans.")
    parser.add_argument("--request-id", default="rtlive_fixture")
    parser.add_argument("--mode", choices=("dry_run", "fixture_replay", "live_observe"), default="fixture_replay")
    parser.add_argument("--source", action="append", dest="sources")
    parser.add_argument("--model-layer", action="append", dest="model_layers")
    parser.add_argument("--instrument-ref", action="append", dest="instrument_refs")
    parser.add_argument("--live-stream-approval-ref")
    parser.add_argument("--allow-live-streams", action="store_true")
    args = parser.parse_args()
    payload = build_live_observe_adapter_plan(
        {
            "request_id": args.request_id,
            "mode": args.mode,
            "sources": args.sources,
            "model_layers": args.model_layers,
            "instrument_refs": args.instrument_refs,
            "live_stream_approval_ref": args.live_stream_approval_ref,
            "allow_live_streams": args.allow_live_streams,
        }
    )
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
