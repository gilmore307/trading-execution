#!/usr/bin/env python3
"""Run the execution-owned realtime monitor loop."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_execution.market_data.realtime_monitor import run_realtime_monitor_loop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-prefix", required=True)
    parser.add_argument("--approval-prefix", required=True)
    parser.add_argument("--universe-path", default=None)
    parser.add_argument("--source-id", default="alpaca")
    parser.add_argument("--universe-model-layer", action="append", dest="universe_model_layers")
    parser.add_argument("--model-layer", action="append", dest="model_layers")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=0.0)
    parser.add_argument("--execute-live-observe", action="store_true")
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    kwargs = {
        "request_prefix": args.request_prefix,
        "approval_prefix": args.approval_prefix,
        "source_id": args.source_id,
        "universe_model_layers": tuple(args.universe_model_layers) if args.universe_model_layers else None,
        "model_layers": tuple(args.model_layers) if args.model_layers else None,
        "max_symbols": args.max_symbols,
        "cycles": args.cycles,
        "interval_seconds": args.interval_seconds,
        "execute": args.execute_live_observe,
        "output_dir": Path(args.output_dir),
    }
    if args.universe_path:
        kwargs["universe_path"] = args.universe_path
    if kwargs["universe_model_layers"] is None:
        del kwargs["universe_model_layers"]
    if kwargs["model_layers"] is None:
        del kwargs["model_layers"]
    receipt = run_realtime_monitor_loop(**kwargs)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
