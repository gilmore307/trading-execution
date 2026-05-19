# Runtime Model Lifecycle

Runtime model lifecycle is execution-owned.

`trading-evaluation` admits candidates through `promotion_readiness_record`. `trading-execution` then runs the current active model for trading and runs promoted-but-not-active models as shadow candidates during market hours.

## Cycle

1. Keep one active model as the trading authority.
2. Run promoted-but-not-active models as shadow candidates during the same market-hours window.
3. Aggregate matured live/shadow decision effectiveness for the cycle, normally about one month.
4. Select the best overall candidate as the next active model.
5. Keep ranks 2-4 as realtime candidates for ongoing comparison.
6. Keep lower-ranked candidates shadow-only unless elimination evidence is sufficient.
7. Mark weak candidates as eliminate candidates when there is a clear reason, not merely a low score.
8. Retire a model only after repeated eliminate-candidate cycles or another accepted elimination policy.

## Performance Boundary

Running many realtime strategies can compete with the active model for runtime capacity. The active model remains primary. Realtime candidates may be sampled, throttled, or moved to off-hours replay/backtest when capacity is constrained.

## Selection Contract

`execution_shadow_cycle_selection` records the post-cycle roster decision:

- previous active model;
- selected active model;
- ranks 2-4 realtime candidates;
- shadow-only candidates;
- eliminate candidates and reason evidence;
- whether an active switch is recommended.

The contract records selection only. It does not write active config pointers, construct orders, submit broker calls, or mutate accounts.

## Active Pointer Write Gate

`execution_active_model_config_write` is the separate execution-owned pointer mutation record. It may be built only after a valid `execution_shadow_cycle_selection`.

Required checks:

- selection id is present and valid;
- selected active model is still the intended winner;
- expected previous active model matches the selection's previous active model;
- new active config ref is present;
- rollback ref is present;
- write window ref is present, normally a closed-market or explicitly accepted switch window.

This record is the audit surface for changing the active model config pointer. It still does not construct orders, submit broker calls, or mutate accounts.

## Elimination

Elimination is evidence-based and not purely quantitative. Acceptable reasons can include repeated unstable tail loss, repeated poor live/shadow decision effectiveness, excessive turnover/cost sensitivity, operational incompatibility, or clear degradation versus the active model. A single weak cycle normally marks a candidate for elimination review; repeated cycles can retire it.
