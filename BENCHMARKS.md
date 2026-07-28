# lazily Benchmark Results

Generated benchmark data for the [lazily](https://crates.io/crates/lazily) reactive primitives library.

## Benchmark Results

<!-- benchmark-results:start -->
Generated for package `lazily` version `0.54.0`.

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
| thread_safe_contention | same_slot_write_read / 8 | 2.812 ms | 3.329 ms | 10 |
| thread_safe_contention | same_slot_write_read / 16 | 6.853 ms | 7.923 ms | 10 |
| thread_safe_contention | independent_slots / 8 | 2.461 ms | 2.849 ms | 10 |
| thread_safe_contention | independent_slots / 16 | 5.549 ms | 6.543 ms | 10 |
| thread_safe_contention | read_mostly_waiters / 8 | 603.801 us | 718.127 us | 10 |
| thread_safe_contention | read_mostly_waiters / 16 | 1.465 ms | 1.503 ms | 10 |
| thread_safe_contention | batched_write_bursts / 8 | 2.430 ms | 2.558 ms | 10 |
| thread_safe_contention | batched_write_bursts / 16 | 3.932 ms | 4.392 ms | 10 |
| thread_safe_effect_contention | queue_coalescing / 8 | 1.159 ms | 1.284 ms | 10 |
| thread_safe_effect_contention | queue_coalescing / 16 | 3.178 ms | 3.660 ms | 10 |
| thread_safe_effect_contention | cleanup_execution / 8 | 1.277 ms | 1.423 ms | 10 |
| thread_safe_effect_contention | cleanup_execution / 16 | 2.975 ms | 4.020 ms | 10 |
| thread_safe_effect_contention | batch_flush / 8 | 2.092 ms | 2.881 ms | 10 |
| thread_safe_effect_contention | batch_flush / 16 | 4.342 ms | 6.935 ms | 10 |
| thread_safe_graph_propagation | fan_out_eager_validation / 8 | 3.012 ms | 3.124 ms | 10 |
| thread_safe_graph_propagation | fan_out_eager_validation / 16 | 4.872 ms | 5.322 ms | 10 |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 8 | 1.741 ms | 1.861 ms | 10 |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 16 | 3.540 ms | 3.902 ms | 10 |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 8 | 2.888 ms | 4.082 ms | 10 |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 16 | 7.587 ms | 8.237 ms | 10 |
| thread_safe_graph_propagation | fan_in_batched_flush / 8 | 1.030 ms | 1.153 ms | 10 |
| thread_safe_graph_propagation | fan_in_batched_flush / 16 | 1.779 ms | 2.123 ms | 10 |

Criterion estimates are local mean wall-clock time per iteration.

| Group | Case | Mean | 95% CI |
|---|---|---:|---:|
| cached_reads | context | 2.336 ns | 2.313 ns - 2.363 ns |
| cached_reads | thread_safe_context | 58.633 ns | 57.688 ns - 59.689 ns |
| cold_first_get | context | 102.567 ns | 94.254 ns - 110.452 ns |
| cold_first_get | thread_safe_context | 1.105 us | 1.053 us - 1.163 us |
| dependency_fan_out | context / 32 | 2.290 us | 2.140 us - 2.445 us |
| dependency_fan_out | context / 256 | 17.442 us | 16.531 us - 18.338 us |
| dependency_fan_out | thread_safe_context / 32 | 19.216 us | 18.934 us - 19.585 us |
| dependency_fan_out | thread_safe_context / 256 | 149.046 us | 147.325 us - 150.915 us |
| set_cell_invalidation | high_fan_out / 512 | 104.624 us | 95.189 us - 113.806 us |
| set_cell_invalidation | same_slot_contention / 1 | 78.065 us | 75.748 us - 80.340 us |
| set_cell_invalidation | same_slot_contention / 2 | 165.389 us | 162.757 us - 168.326 us |
| set_cell_invalidation | same_slot_contention / 4 | 472.810 us | 460.139 us - 485.050 us |
| set_cell_invalidation | same_slot_contention / 8 | 1.266 ms | 1.187 ms - 1.340 ms |
| set_cell_invalidation | same_slot_contention / 16 | 2.752 ms | 2.628 ms - 2.884 ms |
| set_cell_invalidation | independent_slot_contention / 1 | 77.279 us | 76.071 us - 78.495 us |
| set_cell_invalidation | independent_slot_contention / 2 | 156.226 us | 152.965 us - 159.624 us |
| set_cell_invalidation | independent_slot_contention / 4 | 448.904 us | 433.647 us - 465.555 us |
| set_cell_invalidation | independent_slot_contention / 8 | 1.365 ms | 1.256 ms - 1.485 ms |
| set_cell_invalidation | independent_slot_contention / 16 | 2.732 ms | 2.488 ms - 2.991 ms |
| set_cell_invalidation | batched_write_bursts / 1 | 142.353 us | 141.151 us - 143.418 us |
| set_cell_invalidation | batched_write_bursts / 2 | 203.977 us | 201.505 us - 206.645 us |
| set_cell_invalidation | batched_write_bursts / 4 | 491.988 us | 482.168 us - 501.495 us |
| set_cell_invalidation | batched_write_bursts / 8 | 1.201 ms | 1.149 ms - 1.249 ms |
| set_cell_invalidation | batched_write_bursts / 16 | 3.145 ms | 3.021 ms - 3.292 ms |
| memo_equality_suppression | context | 1.269 us | 1.166 us - 1.368 us |
| memo_equality_suppression | thread_safe_context | 25.558 us | 24.982 us - 26.338 us |
| effect_flushing | context | 31.760 ns | 31.619 ns - 31.929 ns |
| effect_flushing | thread_safe_context | 912.667 ns | 901.415 ns - 924.679 ns |
| batch_storms | context / 64 | 1.999 us | 1.982 us - 2.021 us |
| batch_storms | thread_safe_context / 64 | 7.316 us | 7.277 us - 7.360 us |
| thread_safe_contention | same_slot_write_read / 1 | 130.742 us | 128.739 us - 132.731 us |
| thread_safe_contention | same_slot_write_read / 2 | 396.473 us | 383.073 us - 409.953 us |
| thread_safe_contention | same_slot_write_read / 4 | 971.636 us | 909.543 us - 1.031 ms |
| thread_safe_contention | same_slot_write_read / 8 | 2.714 ms | 2.462 ms - 2.950 ms |
| thread_safe_contention | same_slot_write_read / 16 | 7.027 ms | 6.709 ms - 7.353 ms |
| thread_safe_contention | independent_slots / 1 | 130.414 us | 127.419 us - 133.172 us |
| thread_safe_contention | independent_slots / 2 | 260.812 us | 254.009 us - 268.127 us |
| thread_safe_contention | independent_slots / 4 | 700.189 us | 668.411 us - 727.576 us |
| thread_safe_contention | independent_slots / 8 | 2.451 ms | 2.293 ms - 2.606 ms |
| thread_safe_contention | independent_slots / 16 | 5.523 ms | 5.050 ms - 5.960 ms |
| thread_safe_contention | read_mostly_waiters / 1 | 130.430 us | 128.775 us - 132.244 us |
| thread_safe_contention | read_mostly_waiters / 2 | 157.747 us | 154.031 us - 161.903 us |
| thread_safe_contention | read_mostly_waiters / 4 | 231.663 us | 230.369 us - 233.106 us |
| thread_safe_contention | read_mostly_waiters / 8 | 627.512 us | 586.493 us - 668.411 us |
| thread_safe_contention | read_mostly_waiters / 16 | 1.388 ms | 1.298 ms - 1.462 ms |
| thread_safe_contention | batched_write_bursts / 1 | 206.661 us | 205.032 us - 208.194 us |
| thread_safe_contention | batched_write_bursts / 2 | 545.496 us | 523.208 us - 570.543 us |
| thread_safe_contention | batched_write_bursts / 4 | 1.411 ms | 1.401 ms - 1.421 ms |
| thread_safe_contention | batched_write_bursts / 8 | 2.397 ms | 2.301 ms - 2.478 ms |
| thread_safe_contention | batched_write_bursts / 16 | 3.960 ms | 3.759 ms - 4.151 ms |
| thread_safe_effect_contention | queue_coalescing / 8 | 1.159 ms | 1.094 ms - 1.217 ms |
| thread_safe_effect_contention | queue_coalescing / 16 | 3.124 ms | 2.887 ms - 3.345 ms |
| thread_safe_effect_contention | cleanup_execution / 8 | 1.278 ms | 1.209 ms - 1.343 ms |
| thread_safe_effect_contention | cleanup_execution / 16 | 3.200 ms | 2.932 ms - 3.478 ms |
| thread_safe_effect_contention | batch_flush / 8 | 2.280 ms | 2.081 ms - 2.495 ms |
| thread_safe_effect_contention | batch_flush / 16 | 5.026 ms | 4.398 ms - 5.718 ms |
| thread_safe_graph_propagation | fan_out_eager_validation / 8 | 3.025 ms | 2.996 ms - 3.056 ms |
| thread_safe_graph_propagation | fan_out_eager_validation / 16 | 4.953 ms | 4.867 ms - 5.060 ms |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 8 | 1.753 ms | 1.726 ms - 1.784 ms |
| thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 16 | 3.567 ms | 3.476 ms - 3.666 ms |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 8 | 3.134 ms | 2.728 ms - 3.538 ms |
| thread_safe_graph_propagation | fan_in_lazy_dirty_epochs / 16 | 7.596 ms | 7.221 ms - 7.941 ms |
| thread_safe_graph_propagation | fan_in_batched_flush / 8 | 1.058 ms | 1.016 ms - 1.099 ms |
| thread_safe_graph_propagation | fan_in_batched_flush / 16 | 1.827 ms | 1.750 ms - 1.915 ms |
| profile_instrumentation | context_snapshot | 235.293 ns | 234.448 ns - 236.245 ns |
| profile_instrumentation | thread_safe_snapshot | 293.183 us | 291.255 us - 294.845 us |
| async_cached_resolve | async_context | 4.722 us | 4.423 us - 5.046 us |
| async_cached_resolve | sync_context_baseline | 68.269 ns | 65.310 ns - 71.694 ns |
| async_cached_resolve | sync_get | 12.818 ns | 12.575 ns - 13.066 ns |
| async_cached_resolve | thread_safe_context_baseline | 1.378 us | 1.354 us - 1.405 us |
| async_cold_resolve | async_context | 4.005 us | 3.834 us - 4.179 us |
| async_cold_resolve | sync_context_baseline | 100.095 ns | 93.421 ns - 105.453 ns |
| async_cold_resolve | thread_safe_context_baseline | 933.131 ns | 923.714 ns - 944.643 ns |
| async_invalidation_throughput | async_context | 276.614 us | 253.909 us - 303.372 us |
| async_invalidation_throughput | sync_context_baseline | 2.452 us | 2.444 us - 2.464 us |
| async_invalidation_throughput | thread_safe_context_baseline | 53.932 us | 53.844 us - 54.032 us |
| async_cancellation_throughput | async_invalidate_in_flight | 67.768 us | 54.110 us - 81.021 us |
| async_concurrent_contention | async_context / 1 | 71.438 us | 70.571 us - 72.275 us |
| async_concurrent_contention | async_context / 4 | 337.954 us | 299.588 us - 367.654 us |
| async_concurrent_contention | async_context / 16 | 1.942 ms | 1.793 ms - 2.100 ms |
| async_concurrent_contention | thread_safe_context_baseline / 1 | 79.512 us | 78.198 us - 80.677 us |
| async_concurrent_contention | thread_safe_context_baseline / 4 | 662.336 us | 651.978 us - 670.977 us |
| async_concurrent_contention | thread_safe_context_baseline / 16 | 3.675 ms | 3.623 ms - 3.710 ms |
| async_effect_throughput | async_context | 188.151 ms | 188.039 ms - 188.238 ms |
| async_batch_throughput | async_context | 71.850 us | 67.474 us - 76.897 us |
| async_batch_throughput | sync_context_baseline | 9.448 us | 8.598 us - 10.390 us |
| tokio_sync_cached_read | single_task | 1.433 us | 1.427 us - 1.438 us |
| tokio_sync_cached_read | spawn_read | 5.018 us | 4.698 us - 5.436 us |
| tokio_sync_cold_first_get | single_task | 1.421 us | 1.393 us - 1.455 us |
| tokio_sync_cold_first_get | spawn_compute | 5.195 us | 4.890 us - 5.505 us |
| tokio_sync_invalidation | single_task | 55.059 us | 54.737 us - 55.394 us |
| tokio_sync_concurrent_contention | same_slot_write_read / 1 | 60.281 us | 59.482 us - 61.148 us |
| tokio_sync_concurrent_contention | same_slot_write_read / 4 | 447.492 us | 414.606 us - 485.461 us |
| tokio_sync_concurrent_contention | same_slot_write_read / 16 | 4.202 ms | 4.075 ms - 4.332 ms |
| tokio_sync_concurrent_contention | independent_slots / 1 | 59.662 us | 59.075 us - 60.264 us |
| tokio_sync_concurrent_contention | independent_slots / 4 | 394.242 us | 363.367 us - 427.120 us |
| tokio_sync_concurrent_contention | independent_slots / 16 | 3.263 ms | 3.166 ms - 3.350 ms |
| tokio_sync_batch | spawn_batch | 46.970 us | 46.860 us - 47.088 us |
| tokio_sync_effect | single_task | 10.091 ms | 10.088 ms - 10.094 ms |
| scale | build | 65.284 ms | 64.821 ms - 65.809 ms |
| scale | cold_full_recalc | 43.326 ms | 43.255 ms - 43.391 ms |
| scale | full_recalc_invalidate_all | 54.419 ms | 53.734 ms - 55.098 ms |
| scale | viewport_recalc | 2.297 us | 2.260 us - 2.347 us |
| queue_reactive_shell_overhead | raw_vecdeque_push_pop | 1.264 ns | 1.215 ns - 1.322 ns |
| queue_reactive_shell_overhead | subscribed_len_push_pop | 93.316 ns | 89.570 ns - 99.791 ns |
| queue_reactive_shell_overhead | unsubscribed_push_pop | 16.785 ns | 16.715 ns - 16.865 ns |
| revision_write_cost | push / 1 | 212.485 ns | 208.751 ns - 216.685 ns |
| revision_write_cost | push / 16 | 1.051 us | 1.049 us - 1.054 us |
| revision_write_cost | push / 128 | 9.966 us | 9.913 us - 10.034 us |
| revision_write_cost | push / 1024 | 97.254 us | 94.787 us - 99.737 us |
| revision_write_cost | revision / 1 | 122.388 ns | 122.202 ns - 122.593 ns |
| revision_write_cost | revision / 16 | 788.027 ns | 786.220 ns - 789.847 ns |
| revision_write_cost | revision / 128 | 8.254 us | 8.228 us - 8.282 us |
| revision_write_cost | revision / 1024 | 72.269 us | 70.616 us - 74.098 us |
| revision_write_then_read | push / 1 | 105.101 ns | 104.658 ns - 105.598 ns |
| revision_write_then_read | push / 16 | 1.292 us | 1.289 us - 1.296 us |
| revision_write_then_read | push / 128 | 13.515 us | 13.465 us - 13.592 us |
| revision_write_then_read | push / 1024 | 109.586 us | 108.925 us - 110.419 us |
| revision_write_then_read | revision / 1 | 92.298 ns | 91.863 ns - 92.979 ns |
| revision_write_then_read | revision / 16 | 1.214 us | 1.211 us - 1.216 us |
| revision_write_then_read | revision / 128 | 12.944 us | 12.900 us - 12.997 us |
| revision_write_then_read | revision / 1024 | 106.959 us | 106.744 us - 107.188 us |
| typed_cache_reads | context_cell | 0.737 ns | 0.735 ns - 0.738 ns |
| typed_cache_reads | context_rc_cell | 4.884 ns | 4.872 ns - 4.895 ns |
| typed_cache_reads | context_rc_slot | 7.498 ns | 7.210 ns - 7.861 ns |
| typed_cache_reads | context_slot | 2.271 ns | 2.263 ns - 2.280 ns |
| typed_cache_reads | thread_safe_arc_slot | 64.431 ns | 64.001 ns - 65.137 ns |
| typed_cache_reads | thread_safe_arc_string_slot | 64.132 ns | 63.956 ns - 64.334 ns |
| typed_cache_reads | thread_safe_cell | 24.342 ns | 24.238 ns - 24.465 ns |
| typed_cache_reads | thread_safe_slot | 57.543 ns | 57.000 ns - 58.113 ns |
| typed_cache_reads | thread_safe_string_slot | 69.988 ns | 69.815 ns - 70.211 ns |

Instrumentation snapshots are single local profile runs captured by
`examples/instrumentation_profile.rs`.

| Profile | Alloc | Recomputes | Duplicate recomputes | Edges + | Edges - | Effect pushes | Max queue | Lock acquisitions | Lock wait | Lock hold | Sidecar frontiers | Sidecar dirty marks | Sidecar fallbacks | Dirty epochs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| context_memo_effect | 4 | 3 | 0 | 4 | 1 | 2 | 1 | 0 | 0.000 ns | 0.000 ns | 0 | 0 | 0 | 0 |
| context_fan_out_32 | 33 | 64 | 0 | 64 | 32 | 0 | 0 | 0 | 0.000 ns | 0.000 ns | 0 | 0 | 0 | 0 |
| context_batch_storm_64 | 65 | 0 | 0 | 128 | 64 | 2 | 1 | 0 | 0.000 ns | 0.000 ns | 0 | 0 | 0 | 0 |
| thread_safe_first_get_2 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 11 | 4.660 us | 15.940 us | 0 | 0 | 0 | 0 |
| thread_safe_set_cell_invalidation_high_fan_out_512 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | 70.000 ns | 508.394 us | 0 | 0 | 0 | 512 |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 56 | 1.440 us | 18.830 us | 0 | 0 | 0 | 16 |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 96 | 114.761 us | 52.190 us | 0 | 0 | 0 | 32 |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 172 | 560.403 us | 76.491 us | 0 | 0 | 0 | 64 |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 300 | 2.687 ms | 177.351 us | 0 | 0 | 0 | 128 |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 549 | 11.390 ms | 320.041 us | 0 | 0 | 0 | 256 |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 53 | 1.450 us | 12.220 us | 0 | 0 | 0 | 15 |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | 4 | 2 | 0 | 2 | 0 | 0 | 0 | 107 | 39.190 us | 23.890 us | 0 | 0 | 0 | 31 |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | 8 | 4 | 0 | 4 | 0 | 0 | 0 | 178 | 290.324 us | 50.760 us | 0 | 0 | 0 | 63 |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | 16 | 8 | 0 | 8 | 0 | 0 | 0 | 351 | 2.219 ms | 129.431 us | 0 | 0 | 0 | 127 |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | 32 | 16 | 0 | 16 | 0 | 0 | 0 | 678 | 9.647 ms | 269.073 us | 0 | 0 | 0 | 255 |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | 5 | 1 | 0 | 4 | 0 | 0 | 0 | 97 | 2.680 us | 47.010 us | 0 | 0 | 0 | 15 |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | 9 | 1 | 0 | 8 | 0 | 0 | 0 | 126 | 80.661 us | 71.180 us | 0 | 0 | 0 | 11 |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | 17 | 1 | 0 | 16 | 0 | 0 | 0 | 196 | 536.435 us | 129.921 us | 0 | 0 | 0 | 5 |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | 33 | 1 | 0 | 32 | 0 | 0 | 0 | 372 | 2.020 ms | 218.222 us | 0 | 0 | 0 | 5 |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | 65 | 1 | 0 | 64 | 0 | 0 | 0 | 712 | 8.814 ms | 429.075 us | 0 | 0 | 0 | 1 |
| thread_safe_contention_same_slot_write_read_1 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 72 | 1.920 us | 28.850 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_same_slot_write_read_2 | 2 | 21 | 0 | 1 | 0 | 0 | 0 | 138 | 31.710 us | 52.670 us | 0 | 0 | 0 | 32 |
| thread_safe_contention_same_slot_write_read_4 | 2 | 51 | 0 | 1 | 0 | 0 | 0 | 336 | 148.824 us | 114.141 us | 0 | 0 | 0 | 64 |
| thread_safe_contention_same_slot_write_read_8 | 2 | 116 | 0 | 1 | 0 | 0 | 0 | 659 | 233.610 us | 369.544 us | 0 | 0 | 0 | 128 |
| thread_safe_contention_same_slot_write_read_16 | 2 | 226 | 0 | 1 | 0 | 0 | 0 | 1301 | 1.289 ms | 591.265 us | 0 | 0 | 0 | 256 |
| thread_safe_contention_independent_slots_1 | 2 | 16 | 0 | 1 | 0 | 0 | 0 | 68 | 1.760 us | 22.791 us | 0 | 0 | 0 | 15 |
| thread_safe_contention_independent_slots_2 | 4 | 33 | 0 | 2 | 0 | 0 | 0 | 139 | 34.880 us | 46.740 us | 0 | 0 | 0 | 31 |
| thread_safe_contention_independent_slots_4 | 8 | 67 | 0 | 4 | 0 | 0 | 0 | 254 | 488.475 us | 100.050 us | 0 | 0 | 0 | 63 |
| thread_safe_contention_independent_slots_8 | 16 | 135 | 0 | 8 | 0 | 0 | 0 | 487 | 3.782 ms | 253.891 us | 0 | 0 | 0 | 127 |
| thread_safe_contention_independent_slots_16 | 32 | 271 | 0 | 16 | 0 | 0 | 0 | 942 | 18.712 ms | 548.815 us | 0 | 0 | 0 | 255 |
| thread_safe_contention_read_mostly_waiters_1 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 72 | 1.930 us | 25.910 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_2 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 75 | 3.570 us | 26.600 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_4 | 2 | 17 | 0 | 1 | 0 | 0 | 0 | 85 | 23.500 us | 35.450 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_8 | 2 | 18 | 0 | 1 | 0 | 0 | 0 | 110 | 40.411 us | 51.520 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_read_mostly_waiters_16 | 2 | 18 | 0 | 1 | 0 | 0 | 0 | 141 | 181.890 us | 71.861 us | 0 | 0 | 0 | 16 |
| thread_safe_contention_batched_write_bursts_1 | 5 | 16 | 0 | 4 | 0 | 0 | 0 | 112 | 3.110 us | 57.260 us | 0 | 0 | 0 | 15 |
| thread_safe_contention_batched_write_bursts_2 | 9 | 22 | 0 | 8 | 0 | 0 | 0 | 193 | 61.892 us | 93.041 us | 0 | 0 | 0 | 21 |
| thread_safe_contention_batched_write_bursts_4 | 17 | 40 | 0 | 16 | 0 | 0 | 0 | 392 | 347.523 us | 248.591 us | 0 | 0 | 0 | 39 |
| thread_safe_contention_batched_write_bursts_8 | 33 | 6 | 0 | 32 | 0 | 0 | 0 | 384 | 2.396 ms | 236.474 us | 0 | 0 | 0 | 5 |
| thread_safe_contention_batched_write_bursts_16 | 65 | 12 | 0 | 64 | 0 | 0 | 0 | 761 | 8.077 ms | 451.265 us | 0 | 0 | 0 | 11 |
| thread_safe_effect_contention_queue_coalescing_8 | 33 | 0 | 0 | 32 | 0 | 3 | 1 | 375 | 1.655 ms | 214.062 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_queue_coalescing_16 | 65 | 0 | 0 | 64 | 0 | 5 | 1 | 741 | 7.927 ms | 415.253 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_cleanup_execution_8 | 9 | 0 | 0 | 8 | 8 | 32 | 1 | 408 | 2.219 ms | 168.331 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_cleanup_execution_16 | 17 | 0 | 0 | 16 | 16 | 36 | 1 | 704 | 10.265 ms | 317.073 us | 0 | 0 | 0 | 0 |
| thread_safe_effect_contention_batch_flush_8 | 34 | 4 | 0 | 33 | 0 | 5 | 1 | 642 | 4.330 ms | 311.102 us | 0 | 0 | 0 | 3 |
| thread_safe_effect_contention_batch_flush_16 | 66 | 5 | 0 | 65 | 0 | 9 | 1 | 1263 | 13.865 ms | 560.432 us | 0 | 0 | 0 | 4 |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | 34 | 560 | 0 | 64 | 0 | 50 | 1 | 1167 | 17.536 ms | 3.801 ms | 0 | 0 | 0 | 4096 |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | 34 | 561 | 0 | 64 | 0 | 50 | 1 | 1424 | 72.138 ms | 6.855 ms | 0 | 0 | 0 | 8192 |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | 33 | 64 | 0 | 32 | 0 | 0 | 0 | 498 | 16.653 ms | 3.050 ms | 0 | 0 | 0 | 4096 |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | 33 | 64 | 0 | 32 | 0 | 0 | 0 | 767 | 73.114 ms | 6.013 ms | 0 | 0 | 0 | 8192 |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | 65 | 66 | 0 | 64 | 0 | 0 | 0 | 1445 | 6.660 ms | 513.015 us | 0 | 0 | 0 | 572 |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | 129 | 130 | 0 | 128 | 0 | 0 | 0 | 2789 | 30.138 ms | 1.018 ms | 0 | 0 | 0 | 1148 |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | 66 | 151 | 0 | 65 | 0 | 25 | 1 | 1409 | 2.606 ms | 578.384 us | 0 | 0 | 0 | 258 |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | 130 | 130 | 0 | 129 | 0 | 3 | 1 | 1183 | 5.961 ms | 570.577 us | 0 | 0 | 0 | 141 |

ThreadSafe lock attribution for contention profiles:

| Profile | Site | Lock acquisitions | Lock wait | Lock hold |
|---|---|---:|---:|---:|
| thread_safe_set_cell_invalidation_high_fan_out_512 | other | 2 | 40.000 ns | 370.000 ns |
| thread_safe_set_cell_invalidation_high_fan_out_512 | set_cell_invalidation | 1 | 30.000 ns | 508.024 us |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | other | 36 | 910.000 ns | 1.660 us |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | get_refresh | 2 | 60.000 ns | 170.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | dependency_edge | 1 | 30.000 ns | 440.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | set_cell_invalidation | 16 | 420.000 ns | 16.270 us |
| thread_safe_set_cell_invalidation_same_slot_contention_1 | publish | 1 | 20.000 ns | 290.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | other | 60 | 57.910 us | 3.380 us |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | get_refresh | 2 | 60.000 ns | 150.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | dependency_edge | 1 | 30.000 ns | 350.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | set_cell_invalidation | 32 | 56.731 us | 48.030 us |
| thread_safe_set_cell_invalidation_same_slot_contention_2 | publish | 1 | 30.000 ns | 280.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | other | 104 | 276.211 us | 5.560 us |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | get_refresh | 2 | 60.000 ns | 140.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | dependency_edge | 1 | 30.000 ns | 290.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | set_cell_invalidation | 64 | 284.082 us | 70.231 us |
| thread_safe_set_cell_invalidation_same_slot_contention_4 | publish | 1 | 20.000 ns | 270.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | other | 168 | 1.352 ms | 11.900 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | get_refresh | 2 | 260.000 ns | 2.700 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | dependency_edge | 1 | 60.000 ns | 2.630 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | set_cell_invalidation | 128 | 1.334 ms | 158.011 us |
| thread_safe_set_cell_invalidation_same_slot_contention_8 | publish | 1 | 50.000 ns | 2.110 us |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | other | 289 | 5.344 ms | 16.970 us |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | get_refresh | 2 | 70.000 ns | 470.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | dependency_edge | 1 | 30.000 ns | 630.000 ns |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | set_cell_invalidation | 256 | 6.046 ms | 301.401 us |
| thread_safe_set_cell_invalidation_same_slot_contention_16 | publish | 1 | 20.000 ns | 570.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | other | 34 | 900.000 ns | 1.310 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | get_refresh | 2 | 70.000 ns | 440.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | dependency_edge | 1 | 30.000 ns | 630.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | set_cell_invalidation | 15 | 420.000 ns | 9.400 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_1 | publish | 1 | 30.000 ns | 440.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | other | 68 | 22.950 us | 2.590 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | get_refresh | 4 | 100.000 ns | 310.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | dependency_edge | 2 | 60.000 ns | 560.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | set_cell_invalidation | 31 | 16.040 us | 19.910 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_2 | publish | 2 | 40.000 ns | 520.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | other | 99 | 152.892 us | 4.060 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | get_refresh | 8 | 211.000 ns | 620.000 ns |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | dependency_edge | 4 | 100.000 ns | 1.110 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | set_cell_invalidation | 63 | 137.011 us | 43.930 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_4 | publish | 4 | 110.000 ns | 1.040 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | other | 192 | 1.137 ms | 10.890 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | get_refresh | 16 | 540.000 ns | 2.000 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | dependency_edge | 8 | 220.000 ns | 4.090 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | set_cell_invalidation | 127 | 1.081 ms | 109.581 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_8 | publish | 8 | 220.000 ns | 2.870 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | other | 359 | 4.260 ms | 20.650 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | get_refresh | 32 | 990.000 ns | 3.380 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | dependency_edge | 16 | 420.000 ns | 6.900 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | set_cell_invalidation | 255 | 5.386 ms | 233.053 us |
| thread_safe_set_cell_invalidation_independent_slot_contention_16 | publish | 16 | 410.000 ns | 5.090 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | other | 74 | 2.040 us | 14.690 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | get_refresh | 2 | 130.000 ns | 990.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | dependency_edge | 4 | 100.000 ns | 2.240 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | set_cell_invalidation | 16 | 390.000 ns | 25.050 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_1 | publish | 1 | 20.000 ns | 4.040 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | other | 104 | 79.961 us | 34.330 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | get_refresh | 2 | 130.000 ns | 980.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | dependency_edge | 8 | 210.000 ns | 3.760 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | set_cell_invalidation | 11 | 330.000 ns | 31.060 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_2 | publish | 1 | 30.000 ns | 1.050 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | other | 172 | 535.855 us | 101.611 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | get_refresh | 2 | 40.000 ns | 140.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | dependency_edge | 16 | 380.000 ns | 5.420 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | set_cell_invalidation | 5 | 140.000 ns | 22.480 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_4 | publish | 1 | 20.000 ns | 270.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | other | 332 | 2.019 ms | 185.041 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | get_refresh | 2 | 50.000 ns | 300.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | dependency_edge | 32 | 780.000 ns | 12.600 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | set_cell_invalidation | 5 | 160.000 ns | 19.811 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_8 | publish | 1 | 20.000 ns | 470.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | other | 644 | 8.812 ms | 384.343 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | get_refresh | 2 | 50.000 ns | 350.000 ns |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | dependency_edge | 64 | 1.680 us | 29.101 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | set_cell_invalidation | 1 | 20.000 ns | 14.931 us |
| thread_safe_set_cell_invalidation_batched_write_bursts_16 | publish | 1 | 20.000 ns | 350.000 ns |
| thread_safe_contention_same_slot_write_read_1 | other | 36 | 910.000 ns | 1.470 us |
| thread_safe_contention_same_slot_write_read_1 | get_refresh | 2 | 60.000 ns | 320.000 ns |
| thread_safe_contention_same_slot_write_read_1 | dependency_edge | 1 | 30.000 ns | 460.000 ns |
| thread_safe_contention_same_slot_write_read_1 | set_cell_invalidation | 16 | 470.000 ns | 13.770 us |
| thread_safe_contention_same_slot_write_read_1 | publish | 17 | 450.000 ns | 12.830 us |
| thread_safe_contention_same_slot_write_read_2 | other | 66 | 21.090 us | 2.480 us |
| thread_safe_contention_same_slot_write_read_2 | get_refresh | 2 | 60.000 ns | 150.000 ns |
| thread_safe_contention_same_slot_write_read_2 | dependency_edge | 1 | 30.000 ns | 320.000 ns |
| thread_safe_contention_same_slot_write_read_2 | set_cell_invalidation | 32 | 9.740 us | 24.360 us |
| thread_safe_contention_same_slot_write_read_2 | publish | 21 | 790.000 ns | 25.360 us |
| thread_safe_contention_same_slot_write_read_2 | in_flight_wait | 16 | 0.000 ns | 0.000 ns |
| thread_safe_contention_same_slot_write_read_4 | other | 122 | 78.313 us | 4.630 us |
| thread_safe_contention_same_slot_write_read_4 | get_refresh | 21 | 12.240 us | 5.220 us |
| thread_safe_contention_same_slot_write_read_4 | dependency_edge | 1 | 30.000 ns | 270.000 ns |
| thread_safe_contention_same_slot_write_read_4 | set_cell_invalidation | 64 | 51.531 us | 50.551 us |
| thread_safe_contention_same_slot_write_read_4 | publish | 51 | 6.710 us | 53.470 us |
| thread_safe_contention_same_slot_write_read_4 | in_flight_wait | 77 | 0.000 ns | 0.000 ns |
| thread_safe_contention_same_slot_write_read_8 | other | 251 | 99.210 us | 8.990 us |
| thread_safe_contention_same_slot_write_read_8 | get_refresh | 6 | 1.060 us | 800.000 ns |
| thread_safe_contention_same_slot_write_read_8 | dependency_edge | 1 | 20.000 ns | 370.000 ns |
| thread_safe_contention_same_slot_write_read_8 | set_cell_invalidation | 128 | 89.220 us | 111.221 us |
| thread_safe_contention_same_slot_write_read_8 | publish | 116 | 44.100 us | 248.163 us |
| thread_safe_contention_same_slot_write_read_8 | in_flight_wait | 157 | 0.000 ns | 0.000 ns |
| thread_safe_contention_same_slot_write_read_16 | other | 488 | 478.632 us | 18.520 us |
| thread_safe_contention_same_slot_write_read_16 | get_refresh | 42 | 15.450 us | 8.500 us |
| thread_safe_contention_same_slot_write_read_16 | dependency_edge | 1 | 20.000 ns | 410.000 ns |
| thread_safe_contention_same_slot_write_read_16 | set_cell_invalidation | 256 | 676.484 us | 223.400 us |
| thread_safe_contention_same_slot_write_read_16 | publish | 226 | 118.911 us | 340.435 us |
| thread_safe_contention_same_slot_write_read_16 | in_flight_wait | 288 | 0.000 ns | 0.000 ns |
| thread_safe_contention_independent_slots_1 | other | 34 | 830.000 ns | 1.550 us |
| thread_safe_contention_independent_slots_1 | get_refresh | 2 | 60.000 ns | 270.000 ns |
| thread_safe_contention_independent_slots_1 | dependency_edge | 1 | 30.000 ns | 820.000 ns |
| thread_safe_contention_independent_slots_1 | set_cell_invalidation | 15 | 410.000 ns | 9.651 us |
| thread_safe_contention_independent_slots_1 | publish | 16 | 430.000 ns | 10.500 us |
| thread_safe_contention_independent_slots_2 | other | 69 | 19.530 us | 2.600 us |
| thread_safe_contention_independent_slots_2 | get_refresh | 4 | 120.000 ns | 290.000 ns |
| thread_safe_contention_independent_slots_2 | dependency_edge | 2 | 60.000 ns | 940.000 ns |
| thread_safe_contention_independent_slots_2 | set_cell_invalidation | 31 | 7.010 us | 21.110 us |
| thread_safe_contention_independent_slots_2 | publish | 33 | 8.160 us | 21.800 us |
| thread_safe_contention_independent_slots_4 | other | 112 | 197.463 us | 4.860 us |
| thread_safe_contention_independent_slots_4 | get_refresh | 8 | 220.000 ns | 690.000 ns |
| thread_safe_contention_independent_slots_4 | dependency_edge | 4 | 90.000 ns | 1.710 us |
| thread_safe_contention_independent_slots_4 | set_cell_invalidation | 63 | 183.671 us | 45.110 us |
| thread_safe_contention_independent_slots_4 | publish | 67 | 107.031 us | 47.680 us |
| thread_safe_contention_independent_slots_8 | other | 201 | 1.179 ms | 10.150 us |
| thread_safe_contention_independent_slots_8 | get_refresh | 16 | 450.000 ns | 1.570 us |
| thread_safe_contention_independent_slots_8 | dependency_edge | 8 | 220.000 ns | 3.260 us |
| thread_safe_contention_independent_slots_8 | set_cell_invalidation | 127 | 1.466 ms | 115.011 us |
| thread_safe_contention_independent_slots_8 | publish | 135 | 1.137 ms | 123.900 us |
| thread_safe_contention_independent_slots_16 | other | 368 | 6.186 ms | 21.270 us |
| thread_safe_contention_independent_slots_16 | get_refresh | 32 | 910.000 ns | 2.350 us |
| thread_safe_contention_independent_slots_16 | dependency_edge | 16 | 430.000 ns | 6.060 us |
| thread_safe_contention_independent_slots_16 | set_cell_invalidation | 255 | 6.200 ms | 253.442 us |
| thread_safe_contention_independent_slots_16 | publish | 271 | 6.324 ms | 265.693 us |
| thread_safe_contention_read_mostly_waiters_1 | other | 36 | 950.000 ns | 1.520 us |
| thread_safe_contention_read_mostly_waiters_1 | get_refresh | 2 | 50.000 ns | 300.000 ns |
| thread_safe_contention_read_mostly_waiters_1 | dependency_edge | 1 | 30.000 ns | 500.000 ns |
| thread_safe_contention_read_mostly_waiters_1 | set_cell_invalidation | 16 | 410.000 ns | 10.580 us |
| thread_safe_contention_read_mostly_waiters_1 | publish | 17 | 490.000 ns | 13.010 us |
| thread_safe_contention_read_mostly_waiters_2 | other | 36 | 980.000 ns | 1.130 us |
| thread_safe_contention_read_mostly_waiters_2 | get_refresh | 4 | 1.590 us | 1.410 us |
| thread_safe_contention_read_mostly_waiters_2 | dependency_edge | 1 | 20.000 ns | 320.000 ns |
| thread_safe_contention_read_mostly_waiters_2 | set_cell_invalidation | 16 | 480.000 ns | 11.060 us |
| thread_safe_contention_read_mostly_waiters_2 | publish | 17 | 500.000 ns | 12.680 us |
| thread_safe_contention_read_mostly_waiters_2 | in_flight_wait | 1 | 0.000 ns | 0.000 ns |
| thread_safe_contention_read_mostly_waiters_4 | other | 36 | 6.450 us | 1.390 us |
| thread_safe_contention_read_mostly_waiters_4 | get_refresh | 6 | 10.000 us | 2.260 us |
| thread_safe_contention_read_mostly_waiters_4 | dependency_edge | 1 | 20.000 ns | 540.000 ns |
| thread_safe_contention_read_mostly_waiters_4 | set_cell_invalidation | 16 | 470.000 ns | 11.540 us |
| thread_safe_contention_read_mostly_waiters_4 | publish | 17 | 6.560 us | 19.720 us |
| thread_safe_contention_read_mostly_waiters_4 | in_flight_wait | 9 | 0.000 ns | 0.000 ns |
| thread_safe_contention_read_mostly_waiters_8 | other | 36 | 5.240 us | 1.360 us |
| thread_safe_contention_read_mostly_waiters_8 | get_refresh | 17 | 33.370 us | 3.980 us |
| thread_safe_contention_read_mostly_waiters_8 | dependency_edge | 1 | 20.000 ns | 330.000 ns |
| thread_safe_contention_read_mostly_waiters_8 | set_cell_invalidation | 16 | 1.110 us | 12.460 us |
| thread_safe_contention_read_mostly_waiters_8 | publish | 18 | 671.000 ns | 33.390 us |
| thread_safe_contention_read_mostly_waiters_8 | in_flight_wait | 22 | 0.000 ns | 0.000 ns |
| thread_safe_contention_read_mostly_waiters_16 | other | 36 | 28.800 us | 1.681 us |
| thread_safe_contention_read_mostly_waiters_16 | get_refresh | 28 | 133.230 us | 15.500 us |
| thread_safe_contention_read_mostly_waiters_16 | dependency_edge | 1 | 30.000 ns | 320.000 ns |
| thread_safe_contention_read_mostly_waiters_16 | set_cell_invalidation | 16 | 1.410 us | 14.150 us |
| thread_safe_contention_read_mostly_waiters_16 | publish | 18 | 18.420 us | 40.210 us |
| thread_safe_contention_read_mostly_waiters_16 | in_flight_wait | 42 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_1 | other | 74 | 2.120 us | 15.370 us |
| thread_safe_contention_batched_write_bursts_1 | get_refresh | 2 | 60.000 ns | 180.000 ns |
| thread_safe_contention_batched_write_bursts_1 | dependency_edge | 4 | 100.000 ns | 1.520 us |
| thread_safe_contention_batched_write_bursts_1 | set_cell_invalidation | 16 | 450.000 ns | 27.270 us |
| thread_safe_contention_batched_write_bursts_1 | publish | 16 | 380.000 ns | 12.920 us |
| thread_safe_contention_batched_write_bursts_2 | other | 124 | 52.151 us | 27.750 us |
| thread_safe_contention_batched_write_bursts_2 | get_refresh | 2 | 60.000 ns | 140.000 ns |
| thread_safe_contention_batched_write_bursts_2 | dependency_edge | 8 | 190.000 ns | 2.820 us |
| thread_safe_contention_batched_write_bursts_2 | set_cell_invalidation | 21 | 1.580 us | 43.191 us |
| thread_safe_contention_batched_write_bursts_2 | publish | 22 | 7.911 us | 19.140 us |
| thread_safe_contention_batched_write_bursts_2 | in_flight_wait | 16 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_4 | other | 241 | 291.443 us | 68.700 us |
| thread_safe_contention_batched_write_bursts_4 | get_refresh | 4 | 8.160 us | 1.840 us |
| thread_safe_contention_batched_write_bursts_4 | dependency_edge | 16 | 420.000 ns | 6.320 us |
| thread_safe_contention_batched_write_bursts_4 | set_cell_invalidation | 40 | 14.540 us | 95.211 us |
| thread_safe_contention_batched_write_bursts_4 | publish | 40 | 32.960 us | 76.520 us |
| thread_safe_contention_batched_write_bursts_4 | in_flight_wait | 51 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_8 | other | 332 | 2.394 ms | 186.453 us |
| thread_safe_contention_batched_write_bursts_8 | get_refresh | 2 | 50.000 ns | 240.000 ns |
| thread_safe_contention_batched_write_bursts_8 | dependency_edge | 32 | 880.000 ns | 13.770 us |
| thread_safe_contention_batched_write_bursts_8 | set_cell_invalidation | 5 | 140.000 ns | 20.790 us |
| thread_safe_contention_batched_write_bursts_8 | publish | 6 | 190.000 ns | 15.221 us |
| thread_safe_contention_batched_write_bursts_8 | in_flight_wait | 7 | 0.000 ns | 0.000 ns |
| thread_safe_contention_batched_write_bursts_16 | other | 664 | 8.074 ms | 337.402 us |
| thread_safe_contention_batched_write_bursts_16 | get_refresh | 2 | 60.000 ns | 350.000 ns |
| thread_safe_contention_batched_write_bursts_16 | dependency_edge | 64 | 1.670 us | 31.071 us |
| thread_safe_contention_batched_write_bursts_16 | set_cell_invalidation | 11 | 340.000 ns | 41.181 us |
| thread_safe_contention_batched_write_bursts_16 | publish | 12 | 400.000 ns | 41.261 us |
| thread_safe_contention_batched_write_bursts_16 | in_flight_wait | 8 | 0.000 ns | 0.000 ns |
| thread_safe_effect_contention_queue_coalescing_8 | other | 341 | 1.654 ms | 187.412 us |
| thread_safe_effect_contention_queue_coalescing_8 | dependency_edge | 32 | 810.000 ns | 10.710 us |
| thread_safe_effect_contention_queue_coalescing_8 | set_cell_invalidation | 2 | 50.000 ns | 15.940 us |
| thread_safe_effect_contention_queue_coalescing_16 | other | 673 | 7.925 ms | 377.592 us |
| thread_safe_effect_contention_queue_coalescing_16 | dependency_edge | 64 | 1.660 us | 20.880 us |
| thread_safe_effect_contention_queue_coalescing_16 | set_cell_invalidation | 4 | 110.000 ns | 16.781 us |
| thread_safe_effect_contention_cleanup_execution_8 | other | 265 | 1.236 ms | 45.260 us |
| thread_safe_effect_contention_cleanup_execution_8 | dependency_edge | 16 | 440.000 ns | 7.530 us |
| thread_safe_effect_contention_cleanup_execution_8 | set_cell_invalidation | 127 | 983.136 us | 115.541 us |
| thread_safe_effect_contention_cleanup_execution_16 | other | 417 | 4.902 ms | 70.260 us |
| thread_safe_effect_contention_cleanup_execution_16 | dependency_edge | 32 | 810.000 ns | 10.690 us |
| thread_safe_effect_contention_cleanup_execution_16 | set_cell_invalidation | 255 | 5.361 ms | 236.123 us |
| thread_safe_effect_contention_batch_flush_8 | other | 600 | 4.329 ms | 271.202 us |
| thread_safe_effect_contention_batch_flush_8 | get_refresh | 2 | 60.000 ns | 460.000 ns |
| thread_safe_effect_contention_batch_flush_8 | dependency_edge | 33 | 890.000 ns | 14.010 us |
| thread_safe_effect_contention_batch_flush_8 | set_cell_invalidation | 3 | 90.000 ns | 17.960 us |
| thread_safe_effect_contention_batch_flush_8 | publish | 4 | 170.000 ns | 7.470 us |
| thread_safe_effect_contention_batch_flush_16 | other | 1187 | 13.863 ms | 487.692 us |
| thread_safe_effect_contention_batch_flush_16 | get_refresh | 2 | 60.000 ns | 450.000 ns |
| thread_safe_effect_contention_batch_flush_16 | dependency_edge | 65 | 1.700 us | 35.350 us |
| thread_safe_effect_contention_batch_flush_16 | set_cell_invalidation | 4 | 110.000 ns | 19.450 us |
| thread_safe_effect_contention_batch_flush_16 | publish | 5 | 140.000 ns | 17.490 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | other | 351 | 1.611 ms | 101.630 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | get_refresh | 64 | 1.730 us | 4.820 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | dependency_edge | 64 | 1.680 us | 24.630 us |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | set_cell_invalidation | 128 | 13.423 ms | 3.194 ms |
| thread_safe_graph_propagation_fan_out_eager_validation_8 | publish | 560 | 2.499 ms | 476.084 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | other | 479 | 16.908 ms | 99.181 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | get_refresh | 64 | 1.830 us | 4.810 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | dependency_edge | 64 | 1.720 us | 24.180 us |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | set_cell_invalidation | 256 | 49.780 ms | 6.258 ms |
| thread_safe_graph_propagation_fan_out_eager_validation_16 | publish | 561 | 5.446 ms | 468.495 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | other | 210 | 4.299 ms | 10.570 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | get_refresh | 64 | 1.840 us | 5.700 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | dependency_edge | 32 | 840.000 ns | 14.850 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | set_cell_invalidation | 128 | 12.349 ms | 2.981 ms |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_8 | publish | 64 | 1.810 us | 38.151 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | other | 351 | 16.035 ms | 12.310 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | get_refresh | 64 | 1.740 us | 4.550 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | dependency_edge | 32 | 840.000 ns | 13.250 us |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | set_cell_invalidation | 256 | 57.075 ms | 5.949 ms |
| thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16 | publish | 64 | 1.670 us | 33.990 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | other | 739 | 2.418 ms | 32.761 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | get_refresh | 68 | 1.950 us | 8.900 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | dependency_edge | 64 | 1.760 us | 23.670 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | set_cell_invalidation | 508 | 4.236 ms | 398.384 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_8 | publish | 66 | 1.750 us | 49.300 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | other | 1379 | 10.357 ms | 53.961 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | get_refresh | 132 | 3.720 us | 12.700 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | dependency_edge | 128 | 3.390 us | 50.900 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | set_cell_invalidation | 1020 | 19.770 ms | 810.545 us |
| thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16 | publish | 130 | 3.530 us | 89.680 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | other | 471 | 2.369 ms | 176.243 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | get_refresh | 706 | 26.721 us | 90.430 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | dependency_edge | 65 | 1.720 us | 23.620 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | set_cell_invalidation | 16 | 510.000 ns | 145.311 us |
| thread_safe_graph_propagation_fan_in_batched_flush_8 | publish | 151 | 207.472 us | 142.780 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | other | 788 | 5.938 ms | 335.656 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | get_refresh | 132 | 3.690 us | 13.750 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | dependency_edge | 129 | 3.390 us | 50.290 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | set_cell_invalidation | 4 | 3.090 us | 87.640 us |
| thread_safe_graph_propagation_fan_in_batched_flush_16 | publish | 130 | 12.840 us | 83.241 us |

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
