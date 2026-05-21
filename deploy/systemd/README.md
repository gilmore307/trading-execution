# systemd units

- `trading-execution-realtime-runtime-check.service` writes the current realtime trading runtime readiness status.
- `trading-execution-realtime-runtime-check.path` refreshes that status when the active model pointer changes. Dashboard consumers should use the storage-hosted read-model WebSocket stream for live updates.
