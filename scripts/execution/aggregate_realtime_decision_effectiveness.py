#!/usr/bin/env python3
"""Aggregate realtime/shadow decision-effectiveness records."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from trading_execution.market_data.effectiveness import build_realtime_decision_effectiveness


def _load_records(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        payload = json.loads(text)
        if not isinstance(payload, list):
            raise ValueError("JSON input must be a list of decision records")
        return payload
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"JSONL line {line_number} must be an object")
        rows.append(payload)
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records_path", type=Path)
    parser.add_argument("--effectiveness-id", default=None)
    parser.add_argument("--model-id", default=None)
    parser.add_argument("--model-layer", default=None)
    parser.add_argument("--evaluation-window-ref", default=None)
    parser.add_argument("--output-path", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    receipt = build_realtime_decision_effectiveness(
        _load_records(args.records_path),
        effectiveness_id=args.effectiveness_id,
        model_id=args.model_id,
        model_layer=args.model_layer,
        evaluation_window_ref=args.evaluation_window_ref,
    )
    text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
