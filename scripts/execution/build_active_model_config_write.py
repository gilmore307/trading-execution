#!/usr/bin/env python3
"""Build an audited execution active-model config pointer write record."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from trading_execution.model_lifecycle import build_active_model_config_write


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build execution_active_model_config_write from a shadow-cycle selection.")
    parser.add_argument("--shadow-cycle-selection-json", required=True, type=Path)
    parser.add_argument("--expected-previous-active-model-ref", required=True)
    parser.add_argument("--new-active-config-ref", required=True)
    parser.add_argument("--rollback-ref", required=True)
    parser.add_argument("--write-window-ref", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    selection = json.loads(args.shadow_cycle_selection_json.read_text(encoding="utf-8"))
    if not isinstance(selection, dict):
        raise SystemExit("shadow cycle selection must be a JSON object")
    record = build_active_model_config_write(
        shadow_cycle_selection=selection,
        expected_previous_active_model_ref=args.expected_previous_active_model_ref,
        new_active_config_ref=args.new_active_config_ref,
        rollback_ref=args.rollback_ref,
        write_window_ref=args.write_window_ref,
    )
    text = json.dumps(record, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
