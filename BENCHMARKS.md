# lazily Benchmark Results

Generated benchmark data for the [lazily](https://crates.io/crates/lazily) reactive primitives library.

## Benchmark Results

<!-- benchmark-results:start -->
Generated for package `lazily` version `0.51.0`.

Environment: `rustc 1.97.0 (2d8144b78 2026-07-07)` on `x86_64-unknown-linux-gnu`.

Refresh command:

```bash
python3 scripts/update-benchmark-results.py
```

Regression workflow:

```bash
cargo bench --features instrumentation,thread-safe -- --save-baseline before
# apply the performance patch
cargo bench --features instrumentation,thread-safe -- --baseline before
python3 scripts/update-benchmark-results.py --no-run
```

Regression budgets enforced by `python3 scripts/update-benchmark-results.py --check`:

| Profile | Max lock acquisitions | Site lock budgets |
|---|---:|---|
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | 700 | set_cell_invalidation<=260, dependency_edge<=16, get_refresh<=32, publish<=32 |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | 900 | other<=800, set_cell_invalidation<=16, dependency_edge<=64, get_refresh<=2, publish<=2 |
| thread_safe_contention_same_slot_write_read_16 | 1400 | get_refresh<=160, publish<=256, in_flight_wait<=700, set_cell_invalidation<=260 |
| thread_safe_contention_independent_slots_16 | 1100 | other<=450, get_refresh<=64, publish<=320, dependency_edge<=16, set_cell_invalidation<=300 |
| thread_safe_contention_read_mostly_waiters_16 | 256 | get_refresh<=128, publish<=64, in_flight_wait<=96 |
| thread_safe_contention_batched_write_bursts_16 | 950 | other<=800, get_refresh<=128, dependency_edge<=64, set_cell_invalidation<=16, publish<=64, in_flight_wait<=64 |
| thread_safe_effect_contention_queue_coalescing_16 | 2600 | other<=900, dependency_edge<=1600, set_cell_invalidation<=16, get_refresh<=64, publish<=0 |
| thread_safe_effect_contention_cleanup_execution_16 | 1300 | other<=450, dependency_edge<=700, set_cell_invalidation<=256, get_refresh<=0, publish<=0 |
| thread_safe_effect_contention_batch_flush_16 | 1500 | other<=1300, get_refresh<=32, dependency_edge<=96, set_cell_invalidation<=16, publish<=32 |

Budgets use deterministic lock acquisition counts instead of elapsed wait/hold time.

Synchronization strategy adoption gate:

| Strategy | Status | Required throughput evidence | Required p50/p95 latency evidence | Lock-site and safety gate |
|---|---|---|---|---|
| current_std_mutex_condvar | baseline | thread_safe_contention and thread_safe_effect_contention at 8/16 workers | p50/p95 latency for same-slot, read-mostly, batch, and effect-heavy cases | must stay within current lock-site budgets and Loom safety coverage |
| narrower_condvar_wakeups | adopted for per-slot recompute waiters | same-slot write/read and read-mostly waiter throughput at 8/16 workers | p50/p95 latency for waiter wakeup handoff and stale-completion retry | must not regress effect queue, cleanup, or batch flush budgets |
| parking_lot_style_parking | candidate only | same contention matrix measured against current_std_mutex_condvar | p50/p95 latency for parking/unparking under 8/16 workers | requires no worse lock-site budgets plus a deadlock/starvation model |
| targeted_cas | candidate only | fresh cached reads and independent-slot throughput at 8/16 workers | p50/p95 latency for revision validation fallback and publish races | requires unchanged effect/batch/disposal budgets plus Loom/Shuttle proof |

Candidates do not replace the current strategy before the same run reports throughput, p50/p95 latency, and lock-site budgets for the required 8/16-worker cases.

Required latency evidence uses Criterion sample per-iteration timing.

Watch-item A/B follow-up:

| Watch item | Baseline/current refs | Focused command | Controlled rerun result | Decision |
|---|---|---|---|---|
| cached ThreadSafeContext read latency | a8b6fc3 vs c917401 | `cargo bench --features instrumentation,thread-safe --bench context -- cached_reads/thread_safe_context` | 73.48 ns baseline vs 73.20 ns current on warm-cache repeat | no tuning; the archived 56.5 ns row did not reproduce under controlled A/B |
| effect cleanup contention at 16 workers | a8b6fc3 vs c917401 | `cargo bench --features instrumentation,thread-safe --bench context -- thread_safe_effect_contention/cleanup_execution/16` | 2.31 ms baseline vs 2.43 ms current on warm-cache repeat with overlapping CIs | keep watching; Criterion reported no statistically significant change |
| invalidation-frontier fast-path Arc cache (#lzfrontierarc) | 15d4206 vs this change (controlled --save-baseline before_opt A/B, same session) | `cargo bench --features instrumentation,thread-safe --bench context -- --baseline before_opt` | fan_out_lazy_dirty_epochs/16 -46.8% (p=0.00), fan_in_lazy_dirty_epochs/16 -22.6% (p=0.00), independent_slot_contention/16 -17.3% (p=0.00), independent_slots/16 -5.3% (p=0.37 n.s.) | adopted; the cached Arc reuses the BFS-time fast path in the marking pass, halving uninstrumented slot_fast_paths RwLock read acquisitions whose reader-count atomics dominate under 16-way contention. Deterministic state-mutex acquisition counts (the budget metric) are unchanged because slot_fast_paths is a separate uninstrumented lock; the evidence is the controlled wall-clock A/B. Microbench cases (cached_reads) correctly show no change as they do not touch the invalidation frontier. |
| Context slot clean-cache-hit fast path (#lzslotfastpath) | 8c64f33 vs this change (controlled --save-baseline before_slot A/B, same session) | `cargo bench --features instrumentation,thread-safe --bench context -- --baseline before_slot 'cached_reads|typed_cache_reads'` | typed_cache_reads/context_slot -58.9% (p=0.00), cached_reads/context -51.6% (p=0.00), typed_cache_reads/context_cell -2.1% (p=0.76 n.s.) | adopted; refresh_slot now early-returns when the slot holds a value and is neither dirty nor force-recompute, skipping the cycle-guard borrowMut + guard-drop borrowMut + dependencies Vec clone + per-dep is_slot_node borrows + clear_slot_dirty_flags borrowMut on the cache-hit path. Correctness rests on mark_slot_dirty always being called with force_recompute=true from invalidate_dependent_from_changed_value, so any upstream change sets dirty=true and bypasses the fast path. context_slot 11.8 -> 4.7 ns, now within ~1.5 ns of context_cell (3.0 ns); the previous downcast 'tax' framing was wrong (the cell also downcasts) - the real cost was refresh_slot's redundant work on clean reads. |

| Group | Case | p50 | p95 | Samples |
|---|---|---:|---:|---:|
| thread_safe_contention | same_slot_write_read / 8 | 2.591 ms | 2.789 ms | 10 |
| thread_safe_contention | same_slot_write_read / 16 | 6.534 ms | 7.057 ms | 10 |
| thread_safe_contention | independent_slots / 8 | 2.251 ms | 2.411 ms | 10 |
| thread_safe_contention | independent_slots / 16 | 5.361 ms | 5.715 ms | 10 |
| thread_safe_contention | read_mostly_waiters / 8 | 728.300 us | 825.511 us | 10 |
| thread_safe_contention | read_mostly_waiters / 16 | 987.461 us | 1.173 ms | 10 |
| thread_safe_contention | batched_write_bursts / 8 | 2.304 ms | 2.411 ms | 10 |
| thread_safe_contention | batched_write_bursts / 16 | 3.577 ms | 3.961 ms | 10 |
| thread_safe_effect_contention | queue_coalescing / 8 | 1.365 ms | 1.443 ms | 10 |
| thread_safe_effect_contention | queue_coalescing / 16 | 3.135 ms | 3.491 ms | 10 |
| thread_safe_effect_contention | cleanup_execution / 8 | 1.413 ms | 1.645 ms | 10 |
| thread_safe_effect_contention | cleanup_execution / 16 | 3.379 ms | 3.653 ms | 10 |
| thread_safe_effect_contention | batch_flush / 8 | 2.460 ms | 2.750 ms | 10 |
| thread_safe_effect_contention | batch_flush / 16 | 5.940 ms | 6.498 ms | 10 |
| thread_safe_graph_propagation | fan_out_eager_validation / 8 | 3.177 ms | 3.289 ms | 2 |
| thread_safe_graph_propagation | fan_out_eager_validation / 16 | 4.987 ms | 5.064 ms | 2 |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 8 | 1.901 ms | 1.969 ms | 2 |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 16 | 3.756 ms | 3.775 ms | 2 |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 8 | 2.683 ms | 2.748 ms | 2 |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 16 | 6.944 ms | 7.277 ms | 2 |
| thread_safe_graph_propagation | fan_in_batched_flush / 8 | 1.153 ms | 1.162 ms | 2 |
| thread_safe_graph_propagation | fan_in_batched_flush / 16 | 1.871 ms | 1.941 ms | 2 |

Criterion estimates are local mean wall-clock time per iteration.

| Group | Case | Mean | 95% CI |
|---|---|---:|---:|
| cached_reads | context | 2.398 ns | 2.332 ns - 2.481 ns |
| cached_reads | thread_safe_context | 58.574 ns | 58.183 ns - 59.067 ns |
| cold_first_get | context | 171.049 ns | 152.223 ns - 187.688 ns |
| cold_first_get | thread_safe_context | 1.033 us | 1.010 us - 1.057 us |
| dependency_fan_out | context / 32 | 2.796 us | 2.539 us - 3.037 us |
| dependency_fan_out | context / 256 | 22.716 us | 20.536 us - 24.881 us |
| dependency_fan_out | thread_safe_context / 32 | 22.288 us | 21.618 us - 22.937 us |
| dependency_fan_out | thread_safe_context / 256 | 176.479 us | 169.290 us - 183.514 us |
| set_cell_invalidation | high_fan_out / 512 | 152.828 us | 131.104 us - 169.461 us |
| set_cell_invalidation | same_slot_contention / 1 | 84.004 us | 82.820 us - 85.293 us |
| set_cell_invalidation | same_slot_contention / 2 | 193.403 us | 188.013 us - 198.034 us |
| set_cell_invalidation | same_slot_contention / 4 | 502.780 us | 483.776 us - 521.943 us |
| set_cell_invalidation | same_slot_contention / 8 | 1.417 ms | 1.344 ms - 1.492 ms |
| set_cell_invalidation | same_slot_contention / 16 | 3.181 ms | 2.926 ms - 3.401 ms |
| set_cell_invalidation | independent_slot_contention / 1 | 83.071 us | 81.932 us - 84.188 us |
| set_cell_invalidation | independent_slot_contention / 2 | 185.999 us | 180.763 us - 191.334 us |
| set_cell_invalidation | independent_slot_contention / 4 | 476.056 us | 460.931 us - 492.403 us |
| set_cell_invalidation | independent_slot_contention / 8 | 1.408 ms | 1.349 ms - 1.465 ms |
| set_cell_invalidation | independent_slot_contention / 16 | 3.230 ms | 3.075 ms - 3.383 ms |
| set_cell_invalidation | batched_write_bursts / 1 | 144.790 us | 142.787 us - 146.627 us |
| set_cell_invalidation | batched_write_bursts / 2 | 236.056 us | 229.842 us - 241.658 us |
| set_cell_invalidation | batched_write_bursts / 4 | 518.165 us | 494.736 us - 538.809 us |
| set_cell_invalidation | batched_write_bursts / 8 | 1.253 ms | 1.166 ms - 1.331 ms |
| set_cell_invalidation | batched_write_bursts / 16 | 2.934 ms | 2.837 ms - 3.030 ms |
| memo_equality_suppression | context | 1.688 us | 1.515 us - 1.853 us |
| memo_equality_suppression | thread_safe_context | 29.891 us | 28.479 us - 31.395 us |
| effect_flushing | context | 33.684 ns | 33.142 ns - 34.349 ns |
| effect_flushing | thread_safe_context | 974.846 ns | 951.859 ns - 999.896 ns |
| batch_storms | context / 64 | 2.141 us | 2.068 us - 2.235 us |
| batch_storms | thread_safe_context / 64 | 7.673 us | 7.628 us - 7.719 us |
| thread_safe_contention | same_slot_write_read / 1 | 141.560 us | 140.421 us - 142.650 us |
| thread_safe_contention | same_slot_write_read / 2 | 416.381 us | 397.293 us - 433.373 us |
| thread_safe_contention | same_slot_write_read / 4 | 1.005 ms | 946.433 us - 1.053 ms |
| thread_safe_contention | same_slot_write_read / 8 | 2.581 ms | 2.470 ms - 2.684 ms |
| thread_safe_contention | same_slot_write_read / 16 | 6.568 ms | 6.288 ms - 6.821 ms |
| thread_safe_contention | independent_slots / 1 | 141.799 us | 140.141 us - 143.719 us |
| thread_safe_contention | independent_slots / 2 | 305.660 us | 295.988 us - 316.652 us |
| thread_safe_contention | independent_slots / 4 | 810.093 us | 786.129 us - 835.041 us |
| thread_safe_contention | independent_slots / 8 | 2.276 ms | 2.215 ms - 2.333 ms |
| thread_safe_contention | independent_slots / 16 | 5.289 ms | 5.077 ms - 5.470 ms |
| thread_safe_contention | read_mostly_waiters / 1 | 141.961 us | 141.128 us - 142.746 us |
| thread_safe_contention | read_mostly_waiters / 2 | 172.883 us | 169.810 us - 176.968 us |
| thread_safe_contention | read_mostly_waiters / 4 | 273.239 us | 262.090 us - 290.480 us |
| thread_safe_contention | read_mostly_waiters / 8 | 729.499 us | 688.752 us - 769.359 us |
| thread_safe_contention | read_mostly_waiters / 16 | 1.016 ms | 972.421 us - 1.063 ms |
| thread_safe_contention | batched_write_bursts / 1 | 266.708 us | 249.387 us - 284.858 us |
| thread_safe_contention | batched_write_bursts / 2 | 604.174 us | 581.709 us - 626.619 us |
| thread_safe_contention | batched_write_bursts / 4 | 1.479 ms | 1.458 ms - 1.498 ms |
| thread_safe_contention | batched_write_bursts / 8 | 2.305 ms | 2.261 ms - 2.347 ms |
| thread_safe_contention | batched_write_bursts / 16 | 3.532 ms | 3.323 ms - 3.699 ms |
| thread_safe_effect_contention | queue_coalescing / 8 | 1.363 ms | 1.327 ms - 1.396 ms |
| thread_safe_effect_contention | queue_coalescing / 16 | 3.184 ms | 3.081 ms - 3.293 ms |
| thread_safe_effect_contention | cleanup_execution / 8 | 1.443 ms | 1.373 ms - 1.513 ms |
| thread_safe_effect_contention | cleanup_execution / 16 | 3.343 ms | 3.194 ms - 3.465 ms |
| thread_safe_effect_contention | batch_flush / 8 | 2.438 ms | 2.319 ms - 2.550 ms |
| thread_safe_effect_contention | batch_flush / 16 | 5.968 ms | 5.742 ms - 6.195 ms |
| thread_safe_graph_propagation | fan_out_eager_validation / 8 | 3.233 ms | 3.177 ms - 3.289 ms |
| thread_safe_graph_propagation | fan_out_eager_validation / 16 | 5.026 ms | 4.987 ms - 5.064 ms |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 8 | 1.935 ms | 1.901 ms - 1.969 ms |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 16 | 3.765 ms | 3.756 ms - 3.775 ms |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 8 | 2.715 ms | 2.683 ms - 2.748 ms |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 16 | 7.111 ms | 6.944 ms - 7.277 ms |
| thread_safe_graph_propagation | fan_in_batched_flush / 8 | 1.157 ms | 1.153 ms - 1.162 ms |
| thread_safe_graph_propagation | fan_in_batched_flush / 16 | 1.906 ms | 1.871 ms - 1.941 ms |
| profile_instrumentation | context_snapshot | 246.391 ns | 244.739 ns - 248.043 ns |
| profile_instrumentation | thread_safe_snapshot | 295.055 us | 294.592 us - 295.518 us |
| async_cached_resolve | async_context | 3.713 us | 3.645 us - 3.783 us |
| async_cached_resolve | sync_context_baseline | 62.400 ns | 60.792 ns - 64.559 ns |
| async_cached_resolve | sync_get | 11.869 ns | 11.838 ns - 11.899 ns |
| async_cached_resolve | thread_safe_context_baseline | 1.383 us | 1.376 us - 1.391 us |
| async_cold_resolve | async_context | 3.908 us | 3.791 us - 4.029 us |
| async_cold_resolve | sync_context_baseline | 170.382 ns | 149.584 ns - 189.489 ns |
| async_cold_resolve | thread_safe_context_baseline | 1.017 us | 1.002 us - 1.032 us |
| async_invalidation_throughput | async_context | 220.877 us | 215.452 us - 226.595 us |
| async_invalidation_throughput | sync_context_baseline | 2.866 us | 2.818 us - 2.914 us |
| async_invalidation_throughput | thread_safe_context_baseline | 58.109 us | 57.480 us - 58.947 us |
| async_cancellation_throughput | async_invalidate_in_flight | 79.944 us | 63.647 us - 94.985 us |
| async_concurrent_contention | async_context / 1 | 70.754 us | 69.888 us - 71.635 us |
| async_concurrent_contention | async_context / 4 | 339.650 us | 317.429 us - 360.105 us |
| async_concurrent_contention | async_context / 16 | 1.727 ms | 1.588 ms - 1.876 ms |
| async_concurrent_contention | thread_safe_context_baseline / 1 | 83.135 us | 81.953 us - 84.380 us |
| async_concurrent_contention | thread_safe_context_baseline / 4 | 668.680 us | 649.688 us - 682.519 us |
| async_concurrent_contention | thread_safe_context_baseline / 16 | 4.022 ms | 3.980 ms - 4.069 ms |
| async_effect_throughput | async_context | 188.196 ms | 188.096 ms - 188.286 ms |
| async_batch_throughput | async_context | 77.542 us | 74.143 us - 81.107 us |
| async_batch_throughput | sync_context_baseline | 7.370 us | 7.309 us - 7.436 us |
| queue_reactive_shell_overhead | raw_vecdeque_push_pop | 1.191 ns | 1.151 ns - 1.246 ns |
| queue_reactive_shell_overhead | subscribed_len_push_pop | 94.863 ns | 93.396 ns - 96.488 ns |
| queue_reactive_shell_overhead | unsubscribed_push_pop | 17.442 ns | 17.090 ns - 17.927 ns |

Instrumentation snapshots are single local profile runs captured by
`examples/instrumentation_profile.rs`.

| Profile | Alloc | Recomputes | Duplicate recomputes | Edges + | Edges - | Effect pushes | Max queue | Lock acquisitions | Lock wait | Lock hold | Sidecar frontiers | Sidecar dirty marks | Sidecar fallbacks | Dirty epochs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| context_memo_effect | 4 | 3 | 0 | 4 | 1 | 2 | 1 | 0 | 0.000 ns | 0.000 ns | 0 | 0 | 0 | 0 |
| context_fan_out_32 | 33 | 64 | 0 | 64 | 32 | 0 | 0 | 0 | 0.000 ns | 0.000 ns | 0 | 0 | 0 | 0 |
| context_batch_storm_64 | 65 | 0 | 0 | 128 | 64 | 2 | 1 | 0 | 0.000 ns | 0.000 ns | 0 | 0 | 0 | 0 |
| thread_safe_first_get_2 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 11 | 1.060 us | 15.770 us | 0 | 0 | 0 | 0 |
| thread_safe_set_cell_invalidation_high_fan_out_512 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | 80.000 ns | 530.654 us | 0 | 0 | 0 | 512 |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 56 | 1.540 us | 19.260 us | 0 | 0 | 0 | 16 |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 98 | 45.591 us | 25.960 us | 0 | 0 | 0 | 32 |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 164 | 685.736 us | 111.860 us | 0 | 0 | 0 | 64 |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 287 | 2.516 ms | 150.401 us | 0 | 0 | 0 | 128 |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 544 | 10.047 ms | 287.892 us | 0 | 0 | 0 | 256 |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 53 | 1.530 us | 14.770 us | 0 | 0 | 0 | 15 |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | 4 | 2 | 0 | 2 | 0 | 0 | 0 | 107 | 109.412 us | 34.170 us | 0 | 0 | 0 | 31 |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | 8 | 4 | 0 | 4 | 0 | 0 | 0 | 187 | 331.682 us | 64.060 us | 0 | 0 | 0 | 63 |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | 16 | 8 | 0 | 8 | 0 | 0 | 0 | 345 | 2.053 ms | 130.622 us | 0 | 0 | 0 | 127 |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | 32 | 16 | 0 | 16 | 0 | 0 | 0 | 674 | 9.096 ms | 284.524 us | 0 | 0 | 0 | 255 |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | 5 | 1 | 0 | 4 | 0 | 0 | 0 | 97 | 3.920 us | 51.521 us | 0 | 0 | 0 | 15 |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | 9 | 1 | 0 | 8 | 0 | 0 | 0 | 132 | 85.401 us | 74.851 us | 0 | 0 | 0 | 13 |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | 17 | 1 | 0 | 16 | 0 | 0 | 0 | 219 | 387.043 us | 126.201 us | 0 | 0 | 0 | 13 |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | 33 | 1 | 0 | 32 | 0 | 0 | 0 | 372 | 2.229 ms | 239.421 us | 0 | 0 | 0 | 5 |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | 65 | 1 | 0 | 64 | 0 | 0 | 0 | 730 | 9.140 ms | 457.393 us | 0 | 0 | 0 | 6 |
| thread_safe_contention_same_slot_write_read_1 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 72 | 1.950 us | 27.421 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_same_slot_write_read_2 | 2 | 25 | 0 | 1 | 0 | 0 | 0 | 140 | 32.860 us | 53.131 us | 0 | 0 | 0 | 32 |
| thread_safe_contention_same_slot_write_read_4 | 2 | 55 | 0 | 1 | 0 | 0 | 0 | 323 | 137.721 us | 184.620 us | 0 | 0 | 0 | 64 |
| thread_safe_contention_same_slot_write_read_8 | 2 | 102 | 0 | 1 | 0 | 0 | 0 | 631 | 823.501 us | 326.433 us | 0 | 0 | 0 | 128 |
| thread_safe_contention_same_slot_write_read_16 | 2 | 226 | 0 | 1 | 0 | 0 | 0 | 1322 | 2.057 ms | 752.397 us | 0 | 0 | 0 | 256 |
| thread_safe_contention_independent_slots_1 | 2 | 16 | 0 | 1 | 0 | 0 | 0 | 68 | 2.070 us | 26.190 us | 0 | 0 | 0 | 15 |
| thread_safe_contention_independent_slots_2 | 4 | 33 | 0 | 2 | 0 | 0 | 0 | 140 | 46.831 us | 55.681 us | 0 | 0 | 0 | 31 |
| thread_safe_contention_independent_slots_4 | 8 | 67 | 0 | 4 | 0 | 0 | 0 | 252 | 901.809 us | 150.470 us | 0 | 0 | 0 | 63 |
| thread_safe_contention_independent_slots_8 | 16 | 135 | 0 | 8 | 0 | 0 | 0 | 489 | 4.090 ms | 276.664 us | 0 | 0 | 0 | 127 |
| thread_safe_contention_independent_slots_16 | 32 | 271 | 0 | 16 | 0 | 0 | 0 | 953 | 19.018 ms | 561.265 us | 0 | 0 | 0 | 255 |
| thread_safe_contention_read_mostly_waiters_1 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 72 | 2.170 us | 26.350 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_2 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 72 | 1.870 us | 24.590 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_4 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 81 | 3.070 us | 30.980 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_8 | 2 | 18 | 0 | 1 | 0 | 0 | 0 | 106 | 19.510 us | 55.260 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_16 | 2 | 18 | 0 | 1 | 0 | 0 | 0 | 105 | 17.980 us | 76.250 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_batched_write_bursts_1 | 5 | 16 | 0 | 4 | 0 | 0 | 0 | 112 | 3.130 us | 55.540 us | 0 | 0 | 0 | 15 |
| thread_safe_contention_batched_write_bursts_2 | 9 | 21 | 0 | 8 | 0 | 0 | 0 | 189 | 82.221 us | 107.880 us | 0 | 0 | 0 | 21 |
| thread_safe_contention_batched_write_bursts_4 | 17 | 16 | 0 | 16 | 0 | 0 | 0 | 263 | 448.602 us | 167.320 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_batched_write_bursts_8 | 33 | 10 | 0 | 32 | 0 | 0 | 0 | 405 | 1.573 ms | 244.411 us | 0 | 0 | 0 | 9 |
| thread_safe_contention_batched_write_bursts_16 | 65 | 4 | 0 | 64 | 0 | 0 | 0 | 722 | 9.118 ms | 458.173 us | 0 | 0 | 0 | 3 |
| thread_safe_effect_contention_queue_coalescing_8 | 33 | 0 | 0 | 32 | 0 | 4 | 1 | 382 | 2.070 ms | 226.935 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_queue_coalescing_16 | 65 | 0 | 0 | 64 | 0 | 3 | 1 | 726 | 6.510 ms | 405.713 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_cleanup_execution_8 | 9 | 0 | 0 | 8 | 8 | 33 | 1 | 420 | 2.430 ms | 180.853 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_cleanup_execution_16 | 17 | 0 | 0 | 16 | 16 | 31 | 1 | 696 | 10.674 ms | 345.340 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_batch_flush_8 | 34 | 2 | 0 | 33 | 0 | 3 | 1 | 631 | 4.184 ms | 324.542 us | 0 | 0 | 0 | 1 |
| thread_safe_effect_contention_batch_flush_16 | 66 | 4 | 0 | 65 | 0 | 5 | 1 | 1250 | 15.404 ms | 564.945 us | 0 | 0 | 0 | 3 |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | 34 | 562 | 0 | 64 | 0 | 49 | 1 | 1165 | 20.256 ms | 4.358 ms | 0 | 0 | 0 | 4096 |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | 34 | 555 | 0 | 64 | 0 | 49 | 1 | 1414 | 80.461 ms | 7.312 ms | 0 | 0 | 0 | 8192 |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | 33 | 64 | 0 | 32 | 0 | 0 | 0 | 511 | 19.037 ms | 3.473 ms | 0 | 0 | 0 | 4064 |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | 33 | 64 | 0 | 32 | 0 | 0 | 0 | 776 | 78.660 ms | 6.430 ms | 0 | 0 | 0 | 8192 |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | 65 | 66 | 0 | 64 | 0 | 0 | 0 | 1462 | 14.182 ms | 682.584 us | 0 | 0 | 0 | 572 |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | 129 | 130 | 0 | 128 | 0 | 0 | 0 | 2812 | 42.823 ms | 1.139 ms | 0 | 0 | 0 | 1148 |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | 66 | 115 | 0 | 65 | 0 | 17 | 1 | 1111 | 3.568 ms | 537.084 us | 0 | 0 | 0 | 195 |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | 130 | 140 | 0 | 129 | 0 | 7 | 1 | 1447 | 8.420 ms | 675.165 us | 0 | 0 | 0 | 151 |

ThreadSafe lock attribution for contention profiles:

| Profile | Site | Lock acquisitions | Lock wait | Lock hold |
|---|---|---:|---:|---:|
| thread_safe_set_cell_invalidation_high_fan_out_512 | other | 2 | 60.000 ns | 310.000 ns |
| thread_safe_set_cell_invalidation_high_fan_out_512 | set_cell_invalidation | 1 | 20.000 ns | 530.344 us |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | other | 36 | 960.000 ns | 1.600 us |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | get_refresh | 2 | 60.000 ns | 190.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | dependency_edge | 1 | 30.000 ns | 460.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | set_cell_invalidation | 16 | 460.000 ns | 16.690 us |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | publish | 1 | 30.000 ns | 320.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | other | 62 | 31.431 us | 2.100 us |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | get_refresh | 2 | 50.000 ns | 160.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | dependency_edge | 1 | 20.000 ns | 270.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | set_cell_invalidation | 32 | 14.060 us | 23.220 us |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | publish | 1 | 30.000 ns | 210.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | other | 96 | 376.313 us | 7.230 us |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | get_refresh | 2 | 140.000 ns | 1.400 us |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | dependency_edge | 1 | 30.000 ns | 1.310 us |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | set_cell_invalidation | 64 | 309.223 us | 100.860 us |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | publish | 1 | 30.000 ns | 1.060 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | other | 155 | 1.178 ms | 7.570 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | get_refresh | 2 | 70.000 ns | 220.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | dependency_edge | 1 | 30.000 ns | 330.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | set_cell_invalidation | 128 | 1.338 ms | 142.001 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | publish | 1 | 20.000 ns | 280.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | other | 284 | 4.710 ms | 14.030 us |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | get_refresh | 2 | 70.000 ns | 240.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | dependency_edge | 1 | 30.000 ns | 290.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | set_cell_invalidation | 256 | 5.336 ms | 273.082 us |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | publish | 1 | 30.000 ns | 250.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | other | 34 | 930.000 ns | 1.900 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | get_refresh | 2 | 140.000 ns | 980.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | dependency_edge | 1 | 30.000 ns | 1.260 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | set_cell_invalidation | 15 | 410.000 ns | 9.610 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | publish | 1 | 20.000 ns | 1.020 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | other | 68 | 70.971 us | 3.060 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | get_refresh | 4 | 110.000 ns | 330.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | dependency_edge | 2 | 50.000 ns | 450.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | set_cell_invalidation | 31 | 38.221 us | 29.870 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | publish | 2 | 60.000 ns | 460.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | other | 108 | 161.101 us | 4.900 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | get_refresh | 8 | 220.000 ns | 630.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | dependency_edge | 4 | 110.000 ns | 900.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | set_cell_invalidation | 63 | 170.141 us | 56.880 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | publish | 4 | 110.000 ns | 750.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | other | 186 | 1.009 ms | 9.800 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | get_refresh | 16 | 550.000 ns | 2.160 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | dependency_edge | 8 | 210.000 ns | 3.330 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | set_cell_invalidation | 127 | 1.043 ms | 113.022 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | publish | 8 | 210.000 ns | 2.310 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | other | 355 | 3.892 ms | 18.620 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | get_refresh | 32 | 930.000 ns | 2.400 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | dependency_edge | 16 | 440.000 ns | 4.270 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | set_cell_invalidation | 255 | 5.202 ms | 256.064 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | publish | 16 | 410.000 ns | 3.170 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | other | 74 | 3.280 us | 17.100 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | get_refresh | 2 | 60.000 ns | 310.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | dependency_edge | 4 | 100.000 ns | 1.430 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | set_cell_invalidation | 16 | 460.000 ns | 32.231 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | publish | 1 | 20.000 ns | 450.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | other | 108 | 83.811 us | 36.020 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | get_refresh | 2 | 80.000 ns | 290.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | dependency_edge | 8 | 270.000 ns | 3.420 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | set_cell_invalidation | 13 | 1.210 us | 34.731 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | publish | 1 | 30.000 ns | 390.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | other | 187 | 386.043 us | 81.930 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | get_refresh | 2 | 60.000 ns | 520.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | dependency_edge | 16 | 430.000 ns | 5.270 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | set_cell_invalidation | 13 | 480.000 ns | 37.861 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | publish | 1 | 30.000 ns | 620.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | other | 332 | 2.228 ms | 201.991 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | get_refresh | 2 | 60.000 ns | 250.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | dependency_edge | 32 | 890.000 ns | 10.340 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | set_cell_invalidation | 5 | 160.000 ns | 26.540 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | publish | 1 | 20.000 ns | 300.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | other | 656 | 9.108 ms | 404.333 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | get_refresh | 2 | 50.000 ns | 230.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | dependency_edge | 64 | 1.750 us | 25.670 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | set_cell_invalidation | 7 | 30.131 us | 26.910 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | publish | 1 | 20.000 ns | 250.000 ns |
| thread_safe_contention_same_slot_write_read_1 | other | 36 | 970.000 ns | 1.820 us |
| thread_safe_contention_same_slot_write_read_1 | get_refresh | 2 | 60.000 ns | 350.000 ns |
| thread_safe_contention_same_slot_write_read_1 | dependency_edge | 1 | 30.000 ns | 470.000 ns |
| thread_safe_contention_same_slot_write_read_1 | set_cell_invalidation | 16 | 450.000 ns | 12.391 us |
| thread_safe_contention_same_slot_write_read_1 | publish | 17 | 440.000 ns | 12.390 us |
| thread_safe_contention_same_slot_write_read_2 | other | 64 | 20.350 us | 2.440 us |
| thread_safe_contention_same_slot_write_read_2 | get_refresh | 2 | 60.000 ns | 160.000 ns |
| thread_safe_contention_same_slot_write_read_2 | dependency_edge | 1 | 30.000 ns | 230.000 ns |
| thread_safe_contention_same_slot_write_read_2 | set_cell_invalidation | 32 | 6.880 us | 27.590 us |
| thread_safe_contention_same_slot_write_read_2 | publish | 25 | 5.540 us | 22.711 us |
| thread_safe_contention_same_slot_write_read_2 | in_flight_wait | 16 | 0.000 ns | 0.000 ns |
| thread_safe_contention_same_slot_write_read_4 | other | 124 | 59.420 us | 6.360 us |
| thread_safe_contention_same_slot_write_read_4 | get_refresh | 8 | 17.160 us | 3.470 us |
| thread_safe_contention_same_slot_write_read_4 | dependency_edge | 1 | 30.000 ns | 580.000 ns |
| thread_safe_contention_same_slot_write_read_4 | set_cell_invalidation | 64 | 50.311 us | 63.090 us |
| thread_safe_contention_same_slot_write_read_4 | publish | 55 | 10.800 us | 111.120 us |
| thread_safe_contention_same_slot_write_read_4 | in_flight_wait | 71 | 0.000 ns | 0.000 ns |
| thread_safe_contention_same_slot_write_read_8 | other | 234 | 422.261 us | 10.680 us |
| thread_safe_contention_same_slot_write_read_8 | get_refresh | 9 | 11.080 us | 2.600 us |
| thread_safe_contention_same_slot_write_read_8 | dependency_edge | 1 | 30.000 ns | 320.000 ns |
| thread_safe_contention_same_slot_write_read_8 | set_cell_invalidation | 128 | 312.910 us | 126.420 us |
| thread_safe_contention_same_slot_write_read_8 | publish | 102 | 77.220 us | 186.413 us |
| thread_safe_contention_same_slot_write_read_8 | in_flight_wait | 157 | 0.000 ns | 0.000 ns |
| thread_safe_contention_same_slot_write_read_16 | other | 468 | 871.433 us | 22.790 us |
| thread_safe_contention_same_slot_write_read_16 | get_refresh | 42 | 156.553 us | 24.250 us |
| thread_safe_contention_same_slot_write_read_16 | dependency_edge | 1 | 40.000 ns | 1.141 us |
| thread_safe_contention_same_slot_write_read_16 | set_cell_invalidation | 256 | 793.601 us | 277.302 us |
| thread_safe_contention_same_slot_write_read_16 | publish | 226 | 235.351 us | 426.914 us |
| thread_safe_contention_same_slot_write_read_16 | in_flight_wait | 329 | 0.000 ns | 0.000 ns |
| thread_safe_contention_independent_slots_1 | other | 34 | 970.000 ns | 2.150 us |
| thread_safe_contention_independent_slots_1 | get_refresh | 2 | 130.000 ns | 1.370 us |
| thread_safe_contention_independent_slots_1 | dependency_edge | 1 | 30.000 ns | 1.280 us |
| thread_safe_contention_independent_slots_1 | set_cell_invalidation | 15 | 510.000 ns | 10.720 us |
| thread_safe_contention_independent_slots_1 | publish | 16 | 430.000 ns | 10.670 us |
| thread_safe_contention_independent_slots_2 | other | 70 | 15.820 us | 2.780 us |
| thread_safe_contention_independent_slots_2 | get_refresh | 4 | 120.000 ns | 330.000 ns |
| thread_safe_contention_independent_slots_2 | dependency_edge | 2 | 50.000 ns | 500.000 ns |
| thread_safe_contention_independent_slots_2 | set_cell_invalidation | 31 | 13.370 us | 29.241 us |
| thread_safe_contention_independent_slots_2 | publish | 33 | 17.471 us | 22.830 us |
| thread_safe_contention_independent_slots_4 | other | 110 | 400.155 us | 7.400 us |
| thread_safe_contention_independent_slots_4 | get_refresh | 8 | 310.000 ns | 1.780 us |
| thread_safe_contention_independent_slots_4 | dependency_edge | 4 | 110.000 ns | 2.270 us |
| thread_safe_contention_independent_slots_4 | set_cell_invalidation | 63 | 260.172 us | 75.840 us |
| thread_safe_contention_independent_slots_4 | publish | 67 | 241.062 us | 63.180 us |
| thread_safe_contention_independent_slots_8 | other | 203 | 1.662 ms | 11.900 us |
| thread_safe_contention_independent_slots_8 | get_refresh | 16 | 520.000 ns | 2.370 us |
| thread_safe_contention_independent_slots_8 | dependency_edge | 8 | 231.000 ns | 4.020 us |
| thread_safe_contention_independent_slots_8 | set_cell_invalidation | 127 | 1.174 ms | 129.170 us |
| thread_safe_contention_independent_slots_8 | publish | 135 | 1.253 ms | 129.204 us |
| thread_safe_contention_independent_slots_16 | other | 379 | 6.667 ms | 20.370 us |
| thread_safe_contention_independent_slots_16 | get_refresh | 32 | 1.010 us | 3.080 us |
| thread_safe_contention_independent_slots_16 | dependency_edge | 16 | 420.000 ns | 5.880 us |
| thread_safe_contention_independent_slots_16 | set_cell_invalidation | 255 | 4.834 ms | 251.712 us |
| thread_safe_contention_independent_slots_16 | publish | 271 | 7.516 ms | 280.223 us |
| thread_safe_contention_read_mostly_waiters_1 | other | 36 | 1.000 us | 2.510 us |
| thread_safe_contention_read_mostly_waiters_1 | get_refresh | 2 | 100.000 ns | 350.000 ns |
| thread_safe_contention_read_mostly_waiters_1 | dependency_edge | 1 | 30.000 ns | 1.310 us |
| thread_safe_contention_read_mostly_waiters_1 | set_cell_invalidation | 16 | 580.000 ns | 11.640 us |
| thread_safe_contention_read_mostly_waiters_1 | publish | 17 | 460.000 ns | 10.540 us |
| thread_safe_contention_read_mostly_waiters_2 | other | 36 | 900.000 ns | 1.290 us |
| thread_safe_contention_read_mostly_waiters_2 | get_refresh | 2 | 60.000 ns | 160.000 ns |
| thread_safe_contention_read_mostly_waiters_2 | dependency_edge | 1 | 20.000 ns | 200.000 ns |
| thread_safe_contention_read_mostly_waiters_2 | set_cell_invalidation | 16 | 450.000 ns | 12.580 us |
| thread_safe_contention_read_mostly_waiters_2 | publish | 17 | 440.000 ns | 10.360 us |
| thread_safe_contention_read_mostly_waiters_4 | other | 36 | 960.000 ns | 1.220 us |
| thread_safe_contention_read_mostly_waiters_4 | get_refresh | 6 | 860.000 ns | 2.100 us |
| thread_safe_contention_read_mostly_waiters_4 | dependency_edge | 1 | 20.000 ns | 200.000 ns |
| thread_safe_contention_read_mostly_waiters_4 | set_cell_invalidation | 16 | 580.000 ns | 11.320 us |
| thread_safe_contention_read_mostly_waiters_4 | publish | 17 | 650.000 ns | 16.140 us |
| thread_safe_contention_read_mostly_waiters_4 | in_flight_wait | 5 | 0.000 ns | 0.000 ns |
| thread_safe_contention_read_mostly_waiters_8 | other | 36 | 3.150 us | 1.640 us |
| thread_safe_contention_read_mostly_waiters_8 | get_refresh | 18 | 14.490 us | 5.410 us |
| thread_safe_contention_read_mostly_waiters_8 | dependency_edge | 1 | 30.000 ns | 210.000 ns |
| thread_safe_contention_read_mostly_waiters_8 | set_cell_invalidation | 16 | 830.000 ns | 14.980 us |
| thread_safe_contention_read_mostly_waiters_8 | publish | 18 | 1.010 us | 33.020 us |
| thread_safe_contention_read_mostly_waiters_8 | in_flight_wait | 17 | 0.000 ns | 0.000 ns |
| thread_safe_contention_read_mostly_waiters_16 | other | 36 | 1.480 us | 1.580 us |
| thread_safe_contention_read_mostly_waiters_16 | get_refresh | 8 | 8.110 us | 3.120 us |
| thread_safe_contention_read_mostly_waiters_16 | dependency_edge | 1 | 30.000 ns | 270.000 ns |
| thread_safe_contention_read_mostly_waiters_16 | set_cell_invalidation | 16 | 1.290 us | 17.500 us |
| thread_safe_contention_read_mostly_waiters_16 | publish | 18 | 7.070 us | 53.780 us |
| thread_safe_contention_read_mostly_waiters_16 | in_flight_wait | 26 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_1 | other | 74 | 2.090 us | 15.220 us |
| thread_safe_contention_batched_write_bursts_1 | get_refresh | 2 | 60.000 ns | 230.000 ns |
| thread_safe_contention_batched_write_bursts_1 | dependency_edge | 4 | 100.000 ns | 1.480 us |
| thread_safe_contention_batched_write_bursts_1 | set_cell_invalidation | 16 | 450.000 ns | 26.060 us |
| thread_safe_contention_batched_write_bursts_1 | publish | 16 | 430.000 ns | 12.550 us |
| thread_safe_contention_batched_write_bursts_2 | other | 123 | 75.101 us | 33.570 us |
| thread_safe_contention_batched_write_bursts_2 | get_refresh | 2 | 60.000 ns | 150.000 ns |
| thread_safe_contention_batched_write_bursts_2 | dependency_edge | 8 | 210.000 ns | 2.580 us |
| thread_safe_contention_batched_write_bursts_2 | set_cell_invalidation | 21 | 1.640 us | 50.310 us |
| thread_safe_contention_batched_write_bursts_2 | publish | 21 | 5.210 us | 21.270 us |
| thread_safe_contention_batched_write_bursts_2 | in_flight_wait | 14 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_4 | other | 194 | 427.692 us | 81.750 us |
| thread_safe_contention_batched_write_bursts_4 | get_refresh | 2 | 60.000 ns | 160.000 ns |
| thread_safe_contention_batched_write_bursts_4 | dependency_edge | 16 | 430.000 ns | 4.990 us |
| thread_safe_contention_batched_write_bursts_4 | set_cell_invalidation | 16 | 1.670 us | 52.050 us |
| thread_safe_contention_batched_write_bursts_4 | publish | 16 | 18.750 us | 28.370 us |
| thread_safe_contention_batched_write_bursts_4 | in_flight_wait | 19 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_8 | other | 340 | 1.568 ms | 172.241 us |
| thread_safe_contention_batched_write_bursts_8 | get_refresh | 2 | 50.000 ns | 180.000 ns |
| thread_safe_contention_batched_write_bursts_8 | dependency_edge | 32 | 850.000 ns | 11.870 us |
| thread_safe_contention_batched_write_bursts_8 | set_cell_invalidation | 9 | 310.000 ns | 40.860 us |
| thread_safe_contention_batched_write_bursts_8 | publish | 10 | 3.400 us | 19.260 us |
| thread_safe_contention_batched_write_bursts_8 | in_flight_wait | 12 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_16 | other | 648 | 9.116 ms | 383.273 us |
| thread_safe_contention_batched_write_bursts_16 | get_refresh | 2 | 60.000 ns | 200.000 ns |
| thread_safe_contention_batched_write_bursts_16 | dependency_edge | 64 | 1.710 us | 26.810 us |
| thread_safe_contention_batched_write_bursts_16 | set_cell_invalidation | 3 | 80.000 ns | 32.930 us |
| thread_safe_contention_batched_write_bursts_16 | publish | 4 | 230.000 ns | 14.960 us |
| thread_safe_contention_batched_write_bursts_16 | in_flight_wait | 1 | 0.000 ns | 0.000 ns |
| thread_safe_effect_contention_queue_coalescing_8 | other | 344 | 2.069 ms | 194.983 us |
| thread_safe_effect_contention_queue_coalescing_8 | dependency_edge | 32 | 860.000 ns | 8.271 us |
| thread_safe_effect_contention_queue_coalescing_8 | set_cell_invalidation | 6 | 170.000 ns | 23.681 us |
| thread_safe_effect_contention_queue_coalescing_16 | other | 660 | 6.508 ms | 360.313 us |
| thread_safe_effect_contention_queue_coalescing_16 | dependency_edge | 64 | 1.740 us | 20.130 us |
| thread_safe_effect_contention_queue_coalescing_16 | set_cell_invalidation | 2 | 50.000 ns | 25.270 us |
| thread_safe_effect_contention_cleanup_execution_8 | other | 277 | 1.183 ms | 44.103 us |
| thread_safe_effect_contention_cleanup_execution_8 | dependency_edge | 16 | 420.000 ns | 6.440 us |
| thread_safe_effect_contention_cleanup_execution_8 | set_cell_invalidation | 127 | 1.247 ms | 130.310 us |
| thread_safe_effect_contention_cleanup_execution_16 | other | 409 | 4.931 ms | 62.380 us |
| thread_safe_effect_contention_cleanup_execution_16 | dependency_edge | 32 | 910.000 ns | 10.870 us |
| thread_safe_effect_contention_cleanup_execution_16 | set_cell_invalidation | 255 | 5.743 ms | 272.090 us |
| thread_safe_effect_contention_batch_flush_8 | other | 593 | 4.183 ms | 292.761 us |
| thread_safe_effect_contention_batch_flush_8 | get_refresh | 2 | 60.000 ns | 850.000 ns |
| thread_safe_effect_contention_batch_flush_8 | dependency_edge | 33 | 910.000 ns | 13.521 us |
| thread_safe_effect_contention_batch_flush_8 | set_cell_invalidation | 1 | 110.000 ns | 12.110 us |
| thread_safe_effect_contention_batch_flush_8 | publish | 2 | 70.000 ns | 5.300 us |
| thread_safe_effect_contention_batch_flush_16 | other | 1176 | 15.401 ms | 499.783 us |
| thread_safe_effect_contention_batch_flush_16 | get_refresh | 2 | 50.000 ns | 860.000 ns |
| thread_safe_effect_contention_batch_flush_16 | dependency_edge | 65 | 1.740 us | 29.651 us |
| thread_safe_effect_contention_batch_flush_16 | set_cell_invalidation | 3 | 600.000 ns | 23.751 us |
| thread_safe_effect_contention_batch_flush_16 | publish | 4 | 150.000 ns | 10.900 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | other | 347 | 2.286 ms | 140.831 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | get_refresh | 64 | 1.750 us | 5.700 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | dependency_edge | 64 | 1.670 us | 22.680 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | set_cell_invalidation | 128 | 14.963 ms | 3.514 ms |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | publish | 562 | 3.004 ms | 674.564 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | other | 475 | 13.891 ms | 110.863 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | get_refresh | 64 | 1.780 us | 5.080 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | dependency_edge | 64 | 1.670 us | 22.130 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | set_cell_invalidation | 256 | 63.069 ms | 6.715 ms |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | publish | 555 | 3.498 ms | 458.942 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | other | 224 | 5.082 ms | 16.230 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | get_refresh | 64 | 2.200 us | 5.270 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | dependency_edge | 32 | 860.000 ns | 12.950 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | set_cell_invalidation | 127 | 13.950 ms | 3.406 ms |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | publish | 64 | 1.730 us | 32.600 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | other | 360 | 12.839 ms | 16.511 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | get_refresh | 64 | 1.870 us | 5.190 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | dependency_edge | 32 | 850.000 ns | 12.020 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | set_cell_invalidation | 256 | 65.816 ms | 6.367 ms |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | publish | 64 | 1.730 us | 29.640 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | other | 756 | 7.194 ms | 49.721 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | get_refresh | 68 | 2.020 us | 8.720 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | dependency_edge | 64 | 1.670 us | 20.881 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | set_cell_invalidation | 508 | 6.983 ms | 560.632 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | publish | 66 | 1.740 us | 42.630 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | other | 1402 | 18.688 ms | 71.370 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | get_refresh | 132 | 3.690 us | 13.260 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | dependency_edge | 128 | 3.370 us | 41.300 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | set_cell_invalidation | 1020 | 24.125 ms | 933.054 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | publish | 130 | 3.450 us | 80.191 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | other | 446 | 3.356 ms | 217.102 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | get_refresh | 474 | 13.200 us | 56.211 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | dependency_edge | 65 | 1.780 us | 21.040 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | set_cell_invalidation | 11 | 380.000 ns | 139.640 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | publish | 115 | 197.002 us | 103.091 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | other | 798 | 8.395 ms | 388.533 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | get_refresh | 376 | 17.210 us | 42.680 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | dependency_edge | 129 | 3.370 us | 49.091 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | set_cell_invalidation | 4 | 990.000 ns | 101.271 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | publish | 140 | 3.830 us | 93.590 us |

<!-- benchmark-results:end -->

## Scale (≥1M cells) — `#lzscalebench`

The `scale` group in the generated section above is a rigorous criterion benchmark
over a spreadsheet-shaped graph of `N` input cells + `N` formula slots
(`formula[i] = input[i] + input[i-1]`). At the default `N = 1_000_000` that is
~2,000,000 reactive nodes. It is gated behind the `scale-bench` feature so a plain
`cargo bench` skips it; the benchmark generator enables the feature so the group is
tracked by `make benchmark-check`. Run it directly, or at a larger size:

```bash
cargo bench --features scale-bench --bench scale
LAZILY_SCALE_N=2000000 cargo bench --features scale-bench --bench scale
```

What the four cases show at `N = 1_000_000` (reference machine below): `build`
constructs 2M nodes (~0.12 s), `cold_full_recalc` computes every formula from cold
(~0.105 s), `full_recalc_invalidate_all` re-edits every input and recomputes the
whole sheet (~0.080 s), and `viewport_recalc` edits one input and reads only a
1,000-cell viewport — **~3.7 µs**, ~21,000× cheaper than a full recalc because the
lazy pull-based model leaves off-viewport formulas dirty and never recomputes them
(the property a viewport-rendered spreadsheet needs).
(`build`/`cold_full_recalc`/`full_recalc_invalidate_all` are unaffected by the
v0.22.2 `#lzslotfastpath` refresh fast path — they are cold/slow-path — so their
figures are retained from the original run; only `viewport_recalc`, which is
~998/1000 cache-hit reads, moved, by the controlled A/B below. The generated
`scale` rows in the table above reflect the latest single criterion run on this
host and drift with host load for the allocation-heavy `build`/`cold` cases; the
curated baseline here is the reference.)

Memory (not captured by criterion): building 2,000,000 nodes uses ~414 MiB RSS, i.e.
~216 B/node, so 1M populated formula cells land in the low hundreds of MiB.

### Spreadsheet cell-count context

How the two dominant spreadsheets bound a sheet:

| Spreadsheet | Documented limit | Cells |
|---|---|---:|
| **Google Sheets** | 10,000,000 cells per workbook (also 18,278 columns max) | **10,000,000** |
| **Microsoft Excel** | 1,048,576 rows × 16,384 columns per worksheet | **17,179,869,184** |

**Google Sheets (10M cells) — measured.** Modeled as 5,000,000 input cells + 5,000,000
formula cells (= 10M cells) by running the bench at `LAZILY_SCALE_N=5000000`. Criterion
median on the cross-language reference machine (AMD Ryzen 9 9950X3D), pinned to one core
(`taskset -c 4`) and run serially so nothing contends for L3 / memory bandwidth:

| case | mean | per cell |
|---|---:|---:|
| `build` (10M cells) | ~718 ms | ~72 ns |
| `cold_full_recalc` (5M) | ~544 ms | ~109 ns |
| `full_recalc_invalidate_all` (5M) | ~398 ms | ~80 ns |
| `viewport_recalc` (1k) | ~3.8 µs | ~4 ns |

So lazily backs a **full-capacity Google Sheets workbook**: build under a second, full
recompute ~0.5 s, and — crucially — viewport recalc stays ~3.8 µs **independent of sheet
size** (it was ~3.7 µs at 1M too), because the lazy pull-based model only recomputes the
cells you read. Reproduce: `LAZILY_SCALE_N=5000000 cargo bench --features scale-bench --bench scale`.
Across the three implementations lazily-rs holds the **cheapest viewport reads** (3.7–3.8 µs);
see the cross-language table in lazily-zig's `BENCHMARKS.md` for the full head-to-head.

Controlled A/B isolating the v0.22.2 `#lzslotfastpath` refresh fast path on
`viewport_recalc` (`--save-baseline pre_fix`, same session, toggling only
`src/context.rs` between `8c64f33` and `1390a6e`): **13.78 µs → 4.49 µs,
−64.1% (p=0.00)** at `N = 1_000_000`. Only ~2 of the 1,000 viewport cells recompute; the
other ~998 are cache-hit slot reads, each now ~7 ns cheaper because `refresh_slot`
early-returns on a clean hit instead of cloning the dependency `Vec` and walking deps.

**Microsoft Excel (17.18B grid) — sparse, not dense.** Excel's
1,048,576 × 16,384 = 17,179,869,184 is the *grid capacity*, not a populated-cell count.
Building all 17.18B cells densely would need ~7 TB at ~216 B/node — infeasible and
unrepresentative: real sheets populate a tiny fraction of the grid, and lazily's storage
is a **sparse arena** (`Vec<Option<Node>>` with a free-list) that only allocates cells you
actually create. The practical limit is therefore *populated* cells vs. available RAM, not
the 17.18B grid. With the flat per-node cost above (~216 B, ~70–100 ns/cell), capacity ≈
available RAM ÷ ~216 B — e.g. this 186 GB host could hold on the order of ~10⁸–10⁹
populated cells, far beyond any realistically-populated Excel sheet. The `scale` group's
linear scaling (1M → 10M held ~constant per-cell cost) is the evidence that the model
extrapolates rather than degrading at spreadsheet capacity.

### Cross-library comparison — `#lzscalecompare`

Head-to-head against [`leptos_reactive`](https://crates.io/crates/leptos_reactive)
(Leptos 0.6's fine-grained reactivity) on the **identical** spreadsheet graph
(`N` input signals + `N` formula memos, `formula[i] = input[i] + input[i-1]`), in
the same criterion harness on the same host. `leptos_reactive` is the fair
apples-to-apples pick: like lazily it is a **lazy, pull-based memo** system (a memo
recomputes only when read while dirty), so this isolates per-node runtime overhead
and the lazy-pull viewport property rather than comparing a pull model against an
eager push one. (JS signal libraries — Solid, MobX, Preact Signals — are a
different runtime and are excluded; the standard js-reactivity-benchmark / cellx
harnesses also measure small/medium graphs, not a 100k-node sheet.)

Measured at `N = 100_000` (200,000 nodes/library; leptos is far heavier per node,
so this size keeps its wall clock feasible — lazily's own 1M/10M numbers are above):

| case | lazily | leptos_reactive | ratio |
|---|---:|---:|---|
| `build` (200k nodes) | **8.58 ms** | 12.89 ms | lazily **1.5×** faster |
| `cold_full_recalc` (100k formulas) | **8.45 ms** | 30.06 ms | lazily **3.6×** faster |
| `full_recalc_invalidate_all` (100k) | **6.26 ms** | 17.29 ms | lazily **2.8×** faster |
| `viewport_recalc` (edit 1, read 1k) | **~4.5 µs** † | 8.22 µs | lazily **~1.8×** faster |

† lazily's `viewport_recalc` is post-v0.22.2 (`#lzslotfastpath`). Before that refresh
fast path it measured **11.52 µs** and leptos led ~1.4× (the original row this table
shipped with). The v0.22.2 controlled A/B on this case is
**13.78 µs → 4.49 µs, −64.1% (p=0.00)** (`--save-baseline pre_fix`, toggling only
`src/context.rs`). leptos_reactive is an unchanged external library so its 8.22 µs is
retained from the original same-host run; a fresh same-session re-measure under load gave
~10.5 µs, i.e. lazily leads by ~1.8–2.3× depending on leptos's run-to-run variance.

**Honest read:** lazily now leads all four cases — building the sheet (1.5×), computing
it cold (3.6×), recomputing the whole sheet after a full invalidation (2.8×), and the
cached-read-dominated viewport read (~1.8×) — driven by its sparse arena + lean
single-threaded `Context` versus leptos's runtime slotmap and subscriber bookkeeping, plus
the v0.22.2 `refresh_slot` clean-cache-hit fast path that removed the per-read
dependency-walk tax on the ~998/1000 viewport cells that are cache hits. The fairness
evidence is no longer "leptos wins a case" (it did, before v0.22.2, and that historical
result is documented in the footnote above) — it is that leptos's genuine 30 ms cold
recalc proves its memos truly recompute (this is not a straw-man comparison), and that
lazily's viewport lead is a recent code improvement, not an inherent property: the
pre-v0.22.2 code lost this case. The shared headline is unchanged: the lazy-pull property
both exhibit — a one-input edit + bounded-viewport read is **microseconds**, ~1000×
cheaper than a full recalc, *independent of total sheet size* — neither library
recomputes off-viewport formulas. The defensible claim is now "lazily has materially
higher throughput than a comparable native-Rust pull-based reactive system across both
whole-graph and incremental-viewport workloads," **not** a blanket "fastest reactive
library."

Reproduce (gated behind the `scale-compare` feature so the comparison dependency is
never pulled into normal builds / `make check`):

```bash
cargo bench --features scale-compare --bench scale_compare
LAZILY_SCALE_N=250000 cargo bench --features scale-compare --bench scale_compare
```

## Cross-language comparison (lazily-rs / lazily-cpp / lazily-zig)

Head-to-head on the same spreadsheet-shaped workload (`N` input cells + `N`
formula slots, `formula[i] = input[i] + input[i-1]`), measured on `x86_64`
Linux. lazily-rs uses criterion; lazily-cpp uses its `std::chrono` harness;
lazily-zig uses `clock_gettime(.MONOTONIC)` for the scale bench. Numbers are
the current published results from each repo's `BENCHMARKS.md`.

### Micro-benchmarks (single-threaded `Context` unless noted)

| Metric | lazily-rs | lazily-cpp | lazily-zig |
|---|---:|---:|---:|
| cached read (Context) | 5.7 ns | 23 ns | — † |
| cached read (ThreadSafeContext) | 68 ns | 22 ns | — † |
| cold first get (Context) | 129 ns | 97 ns | — † |
| cold first get (ThreadSafeContext) | 1.17 µs | 107 ns | — † |
| fan-out 256 (Context) | 58.4 µs | 1.12 µs | — † |
| fan-out 256 (ThreadSafeContext) | 182 µs | 1.68 µs | — |
| set_cell high_fan_out 512 | 139 µs | 3.26 µs | — † |
| memo equality suppression (Context) | 3.3 µs | 34 ns | — † |
| effect flushing (Context) | 90 ns | 87 ns | — |
| batch storms 64 (Context) | 3.1 µs | 1.55 µs | — |

† lazily-zig 0.17-dev removed `std.time.Timer`, so its reactive-core
micro-bench is **counter-based** (deterministic work-counts: allocations,
edges, recomputes — not wall-clock). The counters confirm the same zero-work
steady state (cached reads = 0 allocs / 0 recomputes) but are not directly
comparable on a wall-clock axis. See
[lazily-zig BENCHMARKS.md](https://github.com/lazily-hub/lazily-zig/blob/main/BENCHMARKS.md).

### Scale — 1M rows (~2M cells)

| Metric | lazily-rs | lazily-cpp | lazily-zig |
|---|---:|---:|---:|
| build (2N nodes) | 105 ms | 123 ms | 132 ms |
| cold full recalc | 106 ms | 36 ms | 381 ms |
| viewport recalc (edit 1, read 1k) | 4.5 µs | 35.1 µs | 6.4 µs |

### Scale — 10M cells (full Google Sheets workbook capacity)

| Metric | lazily-rs | lazily-cpp | lazily-zig |
|---|---:|---:|---:|
| build | 706 ms | 1.41 s | 1.13 s |
| cold full recalc | 518 ms | 415 ms | 2.26 s |
| viewport recalc | 4.1 µs | 43.8 µs | 6.6 µs |

**Honest read:** lazily-rs's monomorphized `Rc<T>` fast path leads the
spreadsheet-scale **build** wall clock (leanest per-node storage) and — after the
v0.22.2 `#lzslotfastpath` refresh fast path — delivers the **cheapest viewport
reads** of the three (4.5 µs @ 1M, 4.1 µs @ 10M, undercutting lazily-zig's
integer-keyed cache at 6.4/6.6 µs). lazily-cpp's v0.6.0 `SmallAny` inline value
storage (optimization B) + alloc-free batch bookkeeping (E) **flipped the
cold-recalc lead**: lazily-cpp cold full recalc is now ~3× faster than lazily-rs
at both 1M (36 vs 106 ms) and 10M (415 vs 518 ms), and its `batch_storms` now
edges out lazily-rs (1.55 vs 3.1 µs). lazily-cpp's type-erased `SmallFn` +
`SmallVec` node layout still wins the high-fan-out micro-benchmarks (fan-out 256,
set_cell 512, memo equality) by 16–49× over lazily-rs. The **shared headline**
across all three: they back a full-capacity Google Sheets workbook and all
exhibit the **lazy-pull viewport property** — a one-cell edit + bounded-viewport
read stays in the **microsecond** range, independent of sheet size, because
off-viewport formulas are left dirty and never recomputed (~2,000–60,000× cheaper
than a full recalc across the three runtimes).

## Phase 3 Wire-Format Optimizations (`#lzperfaudit`)

Three spec-ratified wire wins (`#lzspecfrontiersuppress`, `#lzspecbase64`,
`#lzspecintern`), measured by `benches/wire_optimizations.rs`. Run with:

```bash
cargo bench --features json-base64 --bench wire_optimizations
```

### `#lzspecfrontiersuppress` — optional CrdtSync frontier

Omitting the stamp frontier when unchanged cuts wire size and encode/decode cost:

| Variant | Wire size | Encode | Decode |
|---|---:|---:|---:|
| with frontier (8 peers) | 879 B | ~740 ns | ~1.6 µs |
| ops only (suppressed) | 514 B (**−42%**) | ~463 ns | ~1.0 µs |

### `#lzspecbase64` — base64 byte arrays vs JSON-u8 arrays

Under the `json-base64` capability flag, `Inline`/`Payload` bytes travel as base64
strings instead of JSON integer arrays:

| Payload | json-u8 wire | base64 wire | Savings | Decode (u8 → b64) |
|---:|---:|---:|---:|---|
| 64 B | 395 B | 228 B | **42%** | 911 ns → 710 ns |
| 1 KiB | 4,235 B | 1,508 B | **64%** | 36 µs → 25 µs |
| 16 KiB | 65,675 B | 21,988 B | **67%** | 89 µs → 65 µs |

### `#lzspecintern` — batch string-intern table

Deduplicating repeated `type_tag` strings into a sidecar intern table (256 nodes,
4 distinct tags):

| Variant | Wire size | Savings |
|---|---:|---:|
| inline tags | 15,729 B | — |
| interned | 14,890 B | **5%** |

Savings grow with the node-to-tag ratio (more nodes sharing fewer tags).

## Revision engine crossover (`#lzspecrevisionengine`)

The revision (pull) invalidation engine gives O(1) writes (no dependent cone
walk) at the cost of O(changed-subpath) reads. Observable values are provably
identical to push mode (`get_equiv_push`, lazily-formal `RevisionEngine.lean`).

Benchmark: 10 writes to a source cell with N dependent slots (construction +
priming included in each measurement). Run with:

```bash
cargo bench --bench revision_engine
```

| Fan-out | Push | Revision | Revision win |
|---:|---:|---:|---:|
| 1 | 194 ns | 127 ns | 1.5× |
| 16 | 1.19 µs | 822 ns | 1.4× |
| 128 | 10.9 µs | 8.75 µs | 1.25× |
| 1024 | 192 µs | 177 µs | 1.08× |

The write cost scales linearly with fan-out in push (O(N) dirty walk) but is
O(1) in revision (revision bump). The construction+priming overhead (same for
both) dilutes the pure write-cost gap; workloads with high write:read ratios
and large fan-out benefit most.

## Multi-Language

lazily is implemented across three languages with shared semantics:

| | [lazily-rs](https://crates.io/crates/lazily) | [lazily-zig](https://github.com/btakita/lazily-zig) | [lazily-py](https://github.com/btakita/lazily-py) |
|---|---|---|---|
| Context | Owned `Context` struct | Explicit allocator | Plain `dict` |
| Slot creation | `Box<dyn Fn>` closures | `comptime` function pointers | Lambdas |
| Cell equality | `PartialEq` trait | `std.meta.eql` | `!=` operator |
| Thread safety | Single-threaded `Context`; explicit `ThreadSafeContext` | Mutex by default | GIL |
| Storage | Unified generics | `.direct` / `.indirect` | Object identity |

## Related

- [lazily-zig](https://github.com/btakita/lazily-zig) — Zig implementation with FFI support
- [lazily-py](https://github.com/btakita/lazily-py) — Python implementation with context-as-dict
- [Blog post: Lazily — Reactive Primitives Done Right](https://briantakita.me/posts/lazily-reactive-signals)

## License

MIT
