#!/usr/bin/env python3
"""Print reviewed execution capability catalog without external calls."""

from __future__ import annotations

import json

from trading_execution.broker import build_execution_capability_catalog


def main() -> int:
    print(json.dumps(build_execution_capability_catalog(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
