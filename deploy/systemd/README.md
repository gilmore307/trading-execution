# systemd units

- `trading-execution-realtime-runtime-check.service` writes the current realtime trading runtime readiness status.
- `trading-execution-realtime-runtime-check.timer` refreshes that status while the runtime waits for a promoted active model pointer.
