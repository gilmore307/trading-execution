#!/usr/bin/env python3
"""Validate a realtime capture evidence row without side effects."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Mapping

from trading_execution.market_data import validate_realtime_capture


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a realtime_capture_contract_v1 candidate row.")
    parser.add_argument("capture_json", type=Path, help="Candidate capture JSON object.")
    args = parser.parse_args(argv)
    payload = json.loads(args.capture_json.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise SystemExit("capture JSON must be an object")
    result = validate_realtime_capture(payload)
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result["valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
