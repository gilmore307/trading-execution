#!/usr/bin/env python3
"""Simulate C08 realtime model-group capacity without side effects."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from trading_execution.model_lifecycle import simulate_c08_capacity


def _available_memory_mb() -> int:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return 0
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        if line.startswith("MemAvailable:"):
            parts = line.split()
            if len(parts) >= 2:
                return int(parts[1]) // 1024
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simulate C08 realtime model-group capacity.")
    parser.add_argument("--requested-model-groups", type=int, default=8)
    parser.add_argument("--cpu-count", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--available-memory-mb", type=int, default=_available_memory_mb())
    parser.add_argument("--live-reserved-cpu-count", type=int, default=4)
    parser.add_argument("--per-group-worker-count", type=int, default=2)
    parser.add_argument("--reserved-memory-mb", type=int, default=4096)
    parser.add_argument("--per-group-memory-mb", type=int, default=1024)
    parser.add_argument("--realtime-tick-budget-ms", type=float, default=1000.0)
    parser.add_argument("--active-path-p95-ms", type=float, default=250.0)
    parser.add_argument("--per-group-p95-ms", type=float, default=120.0)
    parser.add_argument("--orchestration-overhead-ms", type=float, default=40.0)
    args = parser.parse_args(argv)

    simulation = simulate_c08_capacity(
        requested_model_group_count=args.requested_model_groups,
        cpu_count=args.cpu_count,
        available_memory_mb=args.available_memory_mb,
        realtime_tick_budget_ms=args.realtime_tick_budget_ms,
        active_path_p95_ms=args.active_path_p95_ms,
        per_group_p95_ms=args.per_group_p95_ms,
        orchestration_overhead_ms=args.orchestration_overhead_ms,
        live_reserved_cpu_count=args.live_reserved_cpu_count,
        per_group_worker_count=args.per_group_worker_count,
        reserved_memory_mb=args.reserved_memory_mb,
        per_group_memory_mb=args.per_group_memory_mb,
    )
    print(json.dumps(simulation.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
