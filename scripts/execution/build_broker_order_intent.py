#!/usr/bin/env python3
"""Build a gated broker order intent without submitting it."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from trading_execution.broker.order_construction import build_broker_order_intent


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an approved broker order intent without broker submission.")
    parser.add_argument("--decision-record", type=Path, required=True, help="Decision record JSON with trade_risk_cap.")
    parser.add_argument("--approval", type=Path, required=True, help="execution_order_construction_approval JSON.")
    parser.add_argument("--construct-order", action="store_true", help="Actually construct the broker order intent payload.")
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args()

    payload = build_broker_order_intent(
        _read_json(args.decision_record),
        approval=_read_json(args.approval),
        construct_order=args.construct_order,
    )
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0 if payload.get("order_construction_status") != "blocked_order_construction_validation_failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
