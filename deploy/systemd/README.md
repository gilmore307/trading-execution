# systemd units

- `trading-execution-realtime-runtime-check.service` writes the current realtime trading runtime readiness status.
- `trading-execution-realtime-runtime-check.path` refreshes that status when the active model pointer changes. Dashboard consumers should use the storage-hosted read-model WebSocket stream for live updates.
- `trading-execution-realtime-monitor-loop.service` runs the execution-owned realtime monitor loop under systemd. It has an `ExecStartPre` active-model-config guard and must fail closed until a promoted active model group exists. It is plan-only unless a reviewed host override sets `TRADING_EXECUTION_REALTIME_MONITOR_EXECUTE_LIVE_OBSERVE=1`.
