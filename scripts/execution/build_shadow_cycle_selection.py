#!/usr/bin/env python3
"""Build an execution shadow-cycle model selection record."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from trading_execution.model_lifecycle import build_shadow_cycle_selection


def _read_reviews(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise SystemExit(f"review row {line_number} must be a JSON object")
        rows.append(row)
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build execution_shadow_cycle_selection from ranked shadow-cycle reviews.")
    parser.add_argument("--cycle-ref", required=True)
    parser.add_argument("--current-active-model-ref", required=True)
    parser.add_argument("--candidate-reviews-jsonl", required=True, type=Path)
    parser.add_argument("--cycle-duration-days", type=int, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    selection = build_shadow_cycle_selection(
        cycle_ref=args.cycle_ref,
        current_active_model_ref=args.current_active_model_ref,
        candidate_reviews=_read_reviews(args.candidate_reviews_jsonl),
        cycle_duration_days=args.cycle_duration_days,
    )
    text = json.dumps(selection, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
