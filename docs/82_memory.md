# Memory

## Durable Local Notes

- `trading-execution` owns its component boundary only; do not let it absorb neighboring repository responsibilities.
- Generated outputs, logs, notebooks, credentials, and secrets must stay out of Git.
- Shared fields, statuses, type values, helper surfaces, and reusable templates discovered here must be routed to `trading-manager` for registry/docs review.
- Durable storage layout and retention are owned by `trading-storage` unless this repository is defining those storage contracts.
- Component runtime dependencies should use the shared `trading-manager` environment policy unless an exception is accepted.
