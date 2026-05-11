#!/usr/bin/env python3
"""Plan realtime capture subscriptions without opening provider streams."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from trading_execution.market_data import build_realtime_subscription_plan


def _load_request(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if args.request_json:
        loaded = json.loads(args.request_json.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise SystemExit("request JSON must be an object")
        payload.update(loaded)
    if args.request_id:
        payload["request_id"] = args.request_id
    if args.mode:
        payload["mode"] = args.mode
    if args.source:
        payload["sources"] = args.source
    if args.model_layer:
        payload["model_layers"] = args.model_layer
    if args.instrument_ref:
        payload["instrument_refs"] = args.instrument_ref
    if args.live_stream_approval_ref:
        payload["live_stream_approval_ref"] = args.live_stream_approval_ref
    if args.allow_live_streams:
        payload["allow_live_streams"] = True
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a side-effect-free realtime subscription/capture plan.")
    parser.add_argument("--request-json", type=Path, help="Optional JSON object with planning request fields.")
    parser.add_argument("--request-id", help="Plan request id.")
    parser.add_argument("--mode", choices=["dry_run", "fixture_replay", "live_observe"], default="dry_run")
    parser.add_argument("--source", action="append", help="Realtime source id; repeatable. Defaults to all reviewed sources.")
    parser.add_argument("--model-layer", action="append", help="Model layer id; repeatable. Defaults to all layers.")
    parser.add_argument("--instrument-ref", action="append", help="Instrument ref/symbol for planning; repeatable.")
    parser.add_argument("--live-stream-approval-ref", help="Reviewed approval ref for future live_observe planning.")
    parser.add_argument("--allow-live-streams", action="store_true", help="Allow live_observe plan rows when approval ref is supplied. Does not execute streams.")
    args = parser.parse_args(argv)
    plan = build_realtime_subscription_plan(_load_request(args))
    sys.stdout.write(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
