#!/usr/bin/env python3
"""Validate realtime feature and model-decision input snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_execution.market_data import validate_model_decision_input_snapshot, validate_realtime_feature_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate realtime feature/model decision input JSON without side effects.")
    parser.add_argument("snapshot_json", help="Path to realtime_feature_snapshot or execution_model_decision_input_snapshot JSON.")
    args = parser.parse_args()

    payload = json.loads(Path(args.snapshot_json).read_text(encoding="utf-8"))
    contract_type = payload.get("contract_type")
    if contract_type == "realtime_feature_snapshot":
        result = validate_realtime_feature_snapshot(payload)
    elif contract_type == "execution_model_decision_input_snapshot":
        result = validate_model_decision_input_snapshot(payload)
    else:
        raise SystemExit(f"unsupported contract_type: {contract_type!r}")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("valid") else 1


if __name__ == "__main__":
    raise SystemExit(main())
