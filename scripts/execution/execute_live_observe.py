#!/usr/bin/env python3
"""Execute approved read-only realtime provider observation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from trading_execution.market_data.live_provider import execute_live_observe


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute reviewed realtime live-observe provider calls.")
    parser.add_argument("--request", type=Path, required=True, help="Live observe request JSON.")
    parser.add_argument("--approval", type=Path, required=True, help="realtime_live_observe_approval JSON.")
    parser.add_argument("--execute-live-observe", action="store_true", help="Actually perform approved read-only provider observations.")
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args()

    payload = execute_live_observe(
        _read_json(args.request),
        approval=_read_json(args.approval),
        execute_live_observe=args.execute_live_observe,
    )
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
