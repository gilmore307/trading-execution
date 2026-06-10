#!/usr/bin/env python3
"""Run a bounded execution-owned realtime monitor smoke."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from trading_execution.market_data.realtime_monitor import (
    DEFAULT_MONITOR_UNIVERSE_MODEL_LAYERS,
    DEFAULT_REALTIME_MODEL_LAYERS,
    DEFAULT_UNIVERSE_PATH,
    run_realtime_monitor_smoke,
)


def _default_id(prefix: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{stamp}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a bounded read-only realtime monitor smoke.")
    parser.add_argument("--universe-path", type=Path, default=Path(DEFAULT_UNIVERSE_PATH))
    parser.add_argument("--source", default="alpaca", choices=("alpaca", "okx", "thetadata"))
    parser.add_argument(
        "--universe-model-layer",
        action="append",
        dest="universe_model_layers",
        help="Universe CSV model_layer to include; repeatable. Defaults to Layer 1/2 ETF monitor universe.",
    )
    parser.add_argument(
        "--model-layer",
        action="append",
        dest="model_layers",
        help="Realtime feature snapshot model_layer to include; repeatable. Decision handoff remains C-runtime-component routed.",
    )
    parser.add_argument("--max-symbols", type=int, help="Limit symbols for a small smoke.")
    parser.add_argument("--request-id", default=None)
    parser.add_argument("--approval-id", default=None)
    parser.add_argument("--execute-live-observe", action="store_true", help="Actually perform approved read-only provider observations.")
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args()

    request_id = args.request_id or _default_id("rtmon_smoke")
    approval_id = args.approval_id or _default_id("rtla_rtmon_smoke")
    receipt = run_realtime_monitor_smoke(
        request_id=request_id,
        approval_id=approval_id,
        universe_path=args.universe_path,
        source_id=args.source,
        universe_model_layers=tuple(args.universe_model_layers or DEFAULT_MONITOR_UNIVERSE_MODEL_LAYERS),
        model_layers=tuple(args.model_layers or DEFAULT_REALTIME_MODEL_LAYERS),
        max_symbols=args.max_symbols,
        execute=args.execute_live_observe,
    )
    text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
