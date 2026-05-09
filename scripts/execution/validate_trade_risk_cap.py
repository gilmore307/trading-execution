#!/usr/bin/env python3
"""Validate mandatory trade_risk_cap before order construction."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Mapping

from trading_execution.risk_cap import validate_trade_risk_cap


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a decision_record.trade_risk_cap pre-order safety gate.")
    parser.add_argument("decision_record", type=Path, help="Unified decision record JSON containing trade_risk_cap.")
    args = parser.parse_args(argv)
    payload = json.loads(args.decision_record.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise SystemExit("decision record must be a JSON object")
    result = validate_trade_risk_cap(payload)
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result.get("valid") and not result.get("reject_order") else 2


if __name__ == "__main__":
    raise SystemExit(main())
