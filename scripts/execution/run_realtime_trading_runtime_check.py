#!/usr/bin/env python3
"""Build the execution realtime trading runtime readiness status."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_execution.runtime import DEFAULT_ACTIVE_MODEL_CONFIG_PATH, run_realtime_trading_runtime_check


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-model-config-path", default=str(DEFAULT_ACTIVE_MODEL_CONFIG_PATH))
    parser.add_argument("--realtime-monitor-loop-ref", default=None)
    parser.add_argument("--te-calendar-refresh-ref", default=None)
    parser.add_argument("--model-decision-input-snapshot-ref", default=None)
    parser.add_argument("--trade-risk-cap-validation-ref", default=None)
    parser.add_argument("--order-construction-approval-ref", default=None)
    parser.add_argument("--allow-model-activation", action="store_true")
    parser.add_argument("--allow-order-construction", action="store_true")
    parser.add_argument("--allow-broker-execution", action="store_true")
    parser.add_argument("--output-dir", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    status = run_realtime_trading_runtime_check(
        output_dir=Path(args.output_dir) if args.output_dir else None,
        active_model_config_path=Path(args.active_model_config_path),
        realtime_monitor_loop_ref=args.realtime_monitor_loop_ref,
        te_calendar_refresh_ref=args.te_calendar_refresh_ref,
        model_decision_input_snapshot_ref=args.model_decision_input_snapshot_ref,
        trade_risk_cap_validation_ref=args.trade_risk_cap_validation_ref,
        order_construction_approval_ref=args.order_construction_approval_ref,
        allow_model_activation=args.allow_model_activation,
        allow_order_construction=args.allow_order_construction,
        allow_broker_execution=args.allow_broker_execution,
    )
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
