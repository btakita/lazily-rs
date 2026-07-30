//! Comprehensive spec-compliance tests for lazily-rs.
//!
//! Verifies every claim in SPEC.md across these categories:
//! 1. Context — creation, slot/cell allocation
//! 2. Slot semantics — lazy compute, caching, clearing, cascading, immutability
//! 3. Cell semantics — get, set, PartialEq guard, invalidation
//! 4. Dependency tracking — thread-local stack, auto-discovery
//! 5. Invalidation semantics — lazy recomputation, memo guard
//! 6. Effect system — auto-tracking, scheduling, cleanup, disposal
//! 7. Batch updates — deferred invalidation and effect flushing
//! 8. Edge cases — no deps, shared deps, deep chains, dynamic deps
//! 9. Threading contract — local contexts today, portable handles for future shared contexts

#[cfg(feature = "thread-safe")]
use lazily::ThreadSafeContext;
use lazily::{Computed, Context, Effect, Source};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
#[cfg(feature = "thread-safe")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "thread-safe")]
use std::sync::mpsc;
#[cfg(feature = "thread-safe")]
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
#[cfg(feature = "thread-safe")]
use std::time::Duration;

// ============================================================================
// 1. Context
// ============================================================================

mod context {
    use super::*;

    #[test]
    fn context_new_creates_empty_context() {
        let _ctx = Context::new();
    }

    #[test]
    fn context_default_creates_empty_context() {
        let _ctx = Context::default();
    }

    #[test]
    fn context_creates_slots_with_unique_handles() {
        let ctx = Context::new();
        let a = ctx.slot(|_| 1);
        let b = ctx.slot(|_| 2);
        // They should return different values (different slots).
        assert_eq!(ctx.get(&a), 1);
        assert_eq!(ctx.get(&b), 2);
    }

    #[test]
    fn context_creates_cells_with_unique_handles() {
        let ctx = Context::new();
        let a = ctx.source(10i32);
        let b = ctx.source(20i32);
        assert_eq!(ctx.get(&a), 10);
        assert_eq!(ctx.get(&b), 20);
    }

    #[test]
    fn context_handles_mixed_slots_and_cells() {
        let ctx = Context::new();
        let c = ctx.source(100i32);
        let s = ctx.slot(move |ctx| ctx.get(&c) + 1);
        assert_eq!(ctx.get(&c), 100);
        assert_eq!(ctx.get(&s), 101);
    }

    #[test]
    fn context_computed_alias_tracks_dependencies() {
        let ctx = Context::new();
        let c = ctx.source(2i32);
        let doubled = ctx.computed(move |ctx| ctx.get(&c) * 2);

        assert_eq!(ctx.get(&doubled), 4);
        assert!(ctx.is_set(&doubled));

        ctx.set(&c, 3);
        assert!(!ctx.is_set(&doubled));
        assert_eq!(ctx.get(&doubled), 6);
    }

    #[test]
    fn context_allocates_after_effect_disposal() {
        let ctx = Context::new();
        let root = ctx.source(1i32);
        let doubled = ctx.computed(move |ctx| ctx.get(&root) * 2);
        let effect = ctx.effect(move |ctx| {
            ctx.get(&doubled);
        });

        effect.dispose(&ctx);
        assert!(!effect.is_active(&ctx));

        let tripled = ctx.computed(move |ctx| ctx.get(&root) * 3);
        assert_eq!(ctx.get(&tripled), 3);

        ctx.set(&root, 2);
        assert_eq!(ctx.get(&tripled), 6);
    }

    // #lzrsgetarc: these cases pin the implementation to the shared-read
    // parity proved by Reactive.readShared_eq_readCell,
    // Reactive.trackedSharedRead_eq_trackedRead, and
    // Reactive.trackedSharedRead_registers_edge in lazily-formal.
    #[test]
    fn get_rc_returns_reference_counted_computed_value() {
        let ctx = Context::new();
        let computed = ctx.computed(|_| "hello".to_string());
        let rc1 = ctx.get_rc(&computed);
        let rc2 = ctx.get_rc(&computed);
        assert_eq!(&*rc1, "hello");
        assert_eq!(*rc1, ctx.get(&computed));
        assert_eq!(&*rc2, "hello");
        assert!(
            Rc::ptr_eq(&rc1, &rc2),
            "both Rc should point to the same allocation"
        );
    }

    #[test]
    fn get_rc_returns_reference_counted_source_value() {
        let ctx = Context::new();
        // Use a heap-backed value (32 bytes > the inline cap) so the shared-
        // allocation guarantee of get_rc is exercised: both Rc's must
        // alias. Small inline-eligible values have no shared box to refcount,
        // so get_rc materializes a fresh Rc for them (value-correctness for
        // that path is covered by get_rc_avoids_clone_for_non_clone_source).
        let cell = ctx.source([42u64; 4]);
        let rc1 = ctx.get_rc(&cell);
        let rc2 = ctx.get_rc(&cell);
        assert_eq!(*rc1, [42u64; 4]);
        assert_eq!(*rc1, ctx.get(&cell));
        assert!(
            Rc::ptr_eq(&rc1, &rc2),
            "both Rc should point to the same allocation"
        );
    }

    #[test]
    fn get_rc_avoids_clone_for_non_clone_computed() {
        #[derive(Debug, PartialEq)]
        struct NoClone(i32);

        let ctx = Context::new();
        let computed = ctx.computed(|_| NoClone(99));
        let rc = ctx.get_rc(&computed);
        assert_eq!(rc.0, 99);
    }

    #[test]
    fn get_rc_avoids_clone_for_non_clone_source() {
        #[derive(Debug, PartialEq)]
        struct NoClone(i32);

        let ctx = Context::new();
        let cell = ctx.source(NoClone(7));
        let rc = ctx.get_rc(&cell);
        assert_eq!(rc.0, 7);
    }

    #[test]
    fn get_rc_computed_tracks_dependencies() {
        let ctx = Context::new();
        let a = ctx.source(1i32);
        let b = ctx.computed(move |ctx| ctx.get(&a) + 10);
        let c = ctx.computed(move |ctx| *ctx.get_rc(&b) + 100);

        assert_eq!(*ctx.get_rc(&c), 111);
        ctx.set(&a, 2);
        assert_eq!(*ctx.get_rc(&c), 112);
    }

    #[test]
    fn get_rc_source_tracks_dependencies() {
        let ctx = Context::new();
        let a = ctx.source(1i32);
        let b = ctx.computed(move |ctx| *ctx.get_rc(&a) + 10);

        assert_eq!(*ctx.get_rc(&b), 11);
        ctx.set(&a, 5);
        assert_eq!(*ctx.get_rc(&b), 15);
    }
}

// ============================================================================
// 1b. Threading Contract
// ============================================================================

mod threading_contract {
    use super::*;

    const SPEC: &str = include_str!("../SPEC.md");

    fn assert_copy<T: Copy>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_spec_contains(fragment: &str) {
        assert!(
            SPEC.contains(fragment),
            "SPEC.md should document threading contract element: {fragment}"
        );
    }

    /// SPEC: Handles are lightweight ids. When their payload type is thread-safe,
    /// the handle may be copied between threads even though the current Context is
    /// not itself shareable.
    #[test]
    fn handles_are_copy_send_sync_ids() {
        assert_copy::<Computed<i32>>();
        assert_copy::<Source<i32>>();
        assert_copy::<Effect>();
        assert_send_sync::<Computed<i32>>();
        assert_send_sync::<Source<i32>>();
        assert_send_sync::<Effect>();
    }

    /// SPEC: The current `Context` is local to one thread, but multiple
    /// independent contexts may run on different OS threads because dependency
    /// tracking is thread-local.
    #[test]
    fn independent_contexts_can_run_on_separate_threads() {
        let threads: Vec<_> = (0..4)
            .map(|seed| {
                thread::spawn(move || {
                    let ctx = Context::new();
                    let cell = ctx.source(seed);
                    let doubled = ctx.computed(move |ctx| ctx.get(&cell) * 2);

                    assert_eq!(ctx.get(&doubled), seed * 2);
                    ctx.set(&cell, seed + 10);
                    ctx.get(&doubled)
                })
            })
            .collect();

        let values: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().expect("context thread should finish"))
            .collect();

        assert_eq!(values, vec![20, 22, 24, 26]);
    }

    /// SPEC: `ThreadSafeContext` shares a single graph across OS threads while
    /// keeping handles id-only and copyable.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_context_shares_slot_across_threads() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(21i32);
        let compute_count = Arc::new(AtomicUsize::new(0));
        let compute_count_for_slot = Arc::clone(&compute_count);
        let answer = ctx.computed(move |ctx| {
            compute_count_for_slot.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(10));
            ctx.get(&root) * 2
        });

        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let ctx = ctx.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    ctx.get(&answer)
                })
            })
            .collect();

        let values: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().expect("worker should finish"))
            .collect();

        assert_eq!(values, vec![42; 8]);
        assert_eq!(
            compute_count.load(Ordering::SeqCst),
            1,
            "contending first-get callers should share one computation"
        );
        assert!(ctx.is_set(&answer));
    }

    /// SPEC: changed values in one thread invalidate dependent slots read from
    /// another thread.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_context_invalidates_across_threads() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(1i32);
        let doubled = ctx.computed(move |ctx| ctx.get(&root) * 2);

        assert_eq!(ctx.get(&doubled), 2);

        let worker_ctx = ctx.clone();
        let worker = thread::spawn(move || {
            worker_ctx.set(&root, 5);
            worker_ctx.get(&doubled)
        });

        assert_eq!(worker.join().expect("worker should finish"), 10);
        assert_eq!(ctx.get(&doubled), 10);
    }

    /// SPEC: frontier invalidation coalesces duplicate slot paths but still
    /// upgrades a duplicated direct cell dependency to force recomputation.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_frontier_invalidation_preserves_direct_force_recompute() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0i32);
        let stable = ctx.computed(move |ctx| {
            ctx.get(&root);
            0i32
        });
        let compute_count = Arc::new(AtomicUsize::new(0));
        let compute_count_for_slot = Arc::clone(&compute_count);
        let mixed = ctx.computed(move |ctx| {
            compute_count_for_slot.fetch_add(1, Ordering::SeqCst);
            ctx.get(&stable);
            ctx.get(&root)
        });

        assert_eq!(ctx.get(&mixed), 0);

        let worker_ctx = ctx.clone();
        let worker = thread::spawn(move || {
            worker_ctx.set(&root, 1);
        });
        worker.join().expect("worker should finish");

        assert_eq!(
            ctx.get(&mixed),
            1,
            "direct cell reads must force recompute even when an equal memo path also reaches the slot"
        );
        assert_eq!(
            compute_count.load(Ordering::SeqCst),
            2,
            "slot should recompute once for the changed cell after duplicate frontier coalescing"
        );
    }

    /// SPEC: the graph lock is not held while user compute callbacks run, so
    /// callbacks may re-enter the same context through nested `get` calls.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_context_allows_reentrant_computation() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(1i32);
        let inner = ctx.computed(move |ctx| ctx.get(&root) + 1);
        let outer = ctx.computed(move |ctx| ctx.get(&inner) + 1);

        assert_eq!(ctx.get(&outer), 3);
        ctx.set(&root, 2);
        assert_eq!(ctx.get(&outer), 4);
    }

    /// SPEC: thread-safe effects track dependencies and rerun when a different
    /// thread mutates a dependency.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_effect_reruns_from_other_thread() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0i32);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_effect = Arc::clone(&seen);

        let effect = ctx.effect(move |ctx| {
            seen_for_effect
                .lock()
                .expect("seen lock should not be poisoned")
                .push(ctx.get(&root));
        });

        assert!(ctx.is_effect_active(&effect));
        assert_eq!(*seen.lock().expect("seen lock"), vec![0]);

        let worker_ctx = ctx.clone();
        let worker = thread::spawn(move || {
            worker_ctx.set(&root, 1);
        });
        worker.join().expect("worker should finish");

        assert_eq!(*seen.lock().expect("seen lock"), vec![0, 1]);
        ctx.dispose_effect(&effect);
        assert!(!ctx.is_effect_active(&effect));
    }

    /// SPEC: concurrent first access to one thread-safe slot returns the same
    /// value to all callers and leaves the slot cached.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_concurrent_first_get_contention() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(21i32);
        let compute_count = Arc::new(AtomicUsize::new(0));
        let compute_count_for_slot = Arc::clone(&compute_count);
        let answer = ctx.computed(move |ctx| {
            compute_count_for_slot.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(10));
            ctx.get(&root) * 2
        });

        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let ctx = ctx.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    ctx.get(&answer)
                })
            })
            .collect();

        for worker in threads {
            assert_eq!(worker.join().expect("worker should finish"), 42);
        }
        assert_eq!(ctx.get(&answer), 42);
        assert!(ctx.is_set(&answer));
        assert_eq!(
            compute_count.load(Ordering::SeqCst),
            1,
            "contending first-get callers should share one computation"
        );
    }

    /// SPEC: high-frequency concurrent cell writes do not corrupt graph state;
    /// the final slot read matches the final cell value.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_concurrent_set_cell_contention() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0usize);
        let doubled = ctx.computed(move |ctx| ctx.get(&root) * 2);

        assert_eq!(ctx.get(&doubled), 0);

        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|thread_id| {
                let ctx = ctx.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for step in 0..200 {
                        ctx.set(&root, thread_id * 1_000 + step);
                        assert_eq!(ctx.get(&doubled) % 2, 0);
                    }
                })
            })
            .collect();

        for worker in threads {
            worker.join().expect("worker should finish");
        }

        let final_value = ctx.get(&root);
        assert_eq!(ctx.get(&doubled), final_value * 2);
    }

    /// SPEC: if an upstream cell changes while a thread-safe slot callback is
    /// running, the stale callback result is discarded and recomputed before
    /// the getter returns.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_retries_slot_compute_invalidated_midflight() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0usize);
        let compute_runs = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(AtomicUsize::new(0));
        let compute_runs_for_slot = Arc::clone(&compute_runs);
        let gate_for_slot = Arc::clone(&gate);
        let derived = ctx.computed(move |ctx| {
            let run = compute_runs_for_slot.fetch_add(1, Ordering::SeqCst) + 1;
            let value = ctx.get(&root);
            if run == 1 {
                gate_for_slot.store(1, Ordering::SeqCst);
                while gate_for_slot.load(Ordering::SeqCst) == 1 {
                    thread::yield_now();
                }
            }
            value
        });

        let worker_ctx = ctx.clone();
        let worker = thread::spawn(move || worker_ctx.get(&derived));

        while gate.load(Ordering::SeqCst) != 1 {
            thread::yield_now();
        }
        ctx.set(&root, 1);
        gate.store(2, Ordering::SeqCst);

        assert_eq!(worker.join().expect("worker should finish"), 1);
        assert_eq!(ctx.get(&derived), 1);
        assert!(
            compute_runs.load(Ordering::SeqCst) >= 2,
            "midflight invalidation should force a retry"
        );
    }

    /// SPEC: a batch opened on another thread defers thread-safe effect reruns
    /// until the outermost batch exits.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_batch_flushes_after_cross_thread_exit() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0i32);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_effect = Arc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            seen_for_effect
                .lock()
                .expect("seen lock should not be poisoned")
                .push(ctx.get(&root));
        });

        assert_eq!(*seen.lock().expect("seen lock"), vec![0]);

        let worker_ctx = ctx.clone();
        let seen_for_worker = Arc::clone(&seen);
        let worker = thread::spawn(move || {
            worker_ctx.batch(|ctx| {
                ctx.set(&root, 1);
                ctx.set(&root, 2);
                assert_eq!(
                    *seen_for_worker.lock().expect("seen lock"),
                    vec![0],
                    "effect should not rerun before batch exit"
                );
            });
        });
        worker.join().expect("worker should finish");

        assert_eq!(*seen.lock().expect("seen lock"), vec![0, 2]);
    }

    /// SPEC: diamond invalidation from another thread schedules one effect
    /// rerun even when multiple dirty paths reach the same effect.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_effect_coalesces_diamond_invalidation_across_thread() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0i32);
        let left = ctx.computed(move |ctx| ctx.get(&root) + 1);
        let right = ctx.computed(move |ctx| ctx.get(&root) + 1);
        let sum = ctx.computed(move |ctx| ctx.get(&left) + ctx.get(&right));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_effect = Arc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            seen_for_effect
                .lock()
                .expect("seen lock should not be poisoned")
                .push(ctx.get(&sum));
        });

        assert_eq!(*seen.lock().expect("seen lock"), vec![2]);

        let worker_ctx = ctx.clone();
        let worker = thread::spawn(move || {
            worker_ctx.set(&root, 1);
        });
        worker.join().expect("worker should finish");

        assert_eq!(*seen.lock().expect("seen lock"), vec![2, 4]);
    }

    /// SPEC: thread-safe effect callbacks may re-enter the same context and
    /// schedule more work without deadlocking the active flush.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_effect_reentrant_write_does_not_deadlock() {
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let ctx = ThreadSafeContext::new();
            let root = ctx.source(0i32);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let seen_for_effect = Arc::clone(&seen);

            let effect = ctx.effect(move |ctx| {
                let current = ctx.get(&root);
                seen_for_effect
                    .lock()
                    .expect("seen lock should not be poisoned")
                    .push(current);
                if current == 0 {
                    ctx.set(&root, 1);
                }
            });

            ctx.dispose_effect(&effect);
            tx.send(seen.lock().expect("seen lock").clone())
                .expect("receiver should still be open");
        });

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2))
                .expect("effect rerun should not deadlock"),
            vec![0, 1]
        );
    }

    /// SPEC: clearing slots, clearing cell dependents, disposing effects, and
    /// setting cells from different threads do not leave graph state corrupted.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_clear_and_dispose_races_remain_consistent() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(1usize);
        let doubled = ctx.computed(move |ctx| ctx.get(&root) * 2);
        let runs = Arc::new(AtomicUsize::new(0));
        let runs_for_effect = Arc::clone(&runs);

        let effect = ctx.effect(move |ctx| {
            runs_for_effect.fetch_add(1, Ordering::SeqCst);
            ctx.get(&doubled);
        });

        assert_eq!(ctx.get(&doubled), 2);

        let barrier = Arc::new(Barrier::new(4));
        let dispose_thread = {
            let ctx = ctx.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                ctx.dispose_effect(&effect);
            })
        };
        let set_thread = {
            let ctx = ctx.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for value in 2..100 {
                    ctx.set(&root, value);
                }
            })
        };
        let clear_slot_thread = {
            let ctx = ctx.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    ctx.clear(&doubled);
                }
            })
        };
        let clear_cell_thread = {
            let ctx = ctx.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    ctx.clear_cell_dependents(&root);
                }
            })
        };

        dispose_thread.join().expect("dispose thread should finish");
        set_thread.join().expect("set thread should finish");
        clear_slot_thread
            .join()
            .expect("slot clear thread should finish");
        clear_cell_thread
            .join()
            .expect("cell clear thread should finish");

        assert!(!ctx.is_effect_active(&effect));
        let final_value = ctx.get(&root);
        assert_eq!(ctx.get(&doubled), final_value * 2);
        assert!(runs.load(Ordering::SeqCst) >= 1);
    }

    /// SPEC: dynamic thread-safe effect dependencies unsubscribe stale edges
    /// before later lazy invalidations can rerun the effect, and disposal clears
    /// pending cleanup/subscription state before racing writes continue.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_dynamic_effect_dependency_cleanup_survives_disposal() {
        let ctx = ThreadSafeContext::new();
        let choose_left = ctx.source(true);
        let left = ctx.source(1usize);
        let right = ctx.source(10usize);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let cleanup_runs = Arc::new(AtomicUsize::new(0));
        let seen_for_effect = Arc::clone(&seen);
        let cleanup_runs_for_effect = Arc::clone(&cleanup_runs);

        let effect = ctx.effect(move |ctx| {
            let value = if ctx.get(&choose_left) {
                ctx.get(&left)
            } else {
                ctx.get(&right)
            };
            seen_for_effect
                .lock()
                .expect("seen lock should not be poisoned")
                .push(value);
            let cleanup_runs = Arc::clone(&cleanup_runs_for_effect);
            move || {
                cleanup_runs.fetch_add(1, Ordering::SeqCst);
            }
        });

        assert_eq!(*seen.lock().expect("seen lock"), vec![1]);

        ctx.set(&choose_left, false);
        assert_eq!(*seen.lock().expect("seen lock"), vec![1, 10]);
        assert_eq!(
            cleanup_runs.load(Ordering::SeqCst),
            1,
            "switching dynamic dependencies should run the previous cleanup once"
        );

        ctx.set(&left, 2);
        assert_eq!(
            *seen.lock().expect("seen lock"),
            vec![1, 10],
            "stale left dependency should not rerun the effect after it switches to right"
        );

        ctx.set(&right, 11);
        assert_eq!(*seen.lock().expect("seen lock"), vec![1, 10, 11]);

        let dispose_ctx = ctx.clone();
        let dispose_thread = thread::spawn(move || {
            dispose_ctx.dispose_effect(&effect);
        });
        let writer_ctx = ctx.clone();
        let writer = thread::spawn(move || {
            writer_ctx.set(&right, 12);
            writer_ctx.set(&choose_left, true);
            writer_ctx.set(&left, 3);
        });

        dispose_thread.join().expect("dispose thread should finish");
        writer.join().expect("writer thread should finish");

        assert!(!ctx.is_effect_active(&effect));
        let after_dispose = seen.lock().expect("seen lock").clone();
        ctx.set(&right, 13);
        ctx.set(&left, 4);
        assert_eq!(
            *seen.lock().expect("seen lock"),
            after_dispose,
            "disposed effect should not rerun through stale dynamic dependency edges"
        );
        assert!(
            cleanup_runs.load(Ordering::SeqCst) >= 2,
            "disposal should run the latest pending cleanup"
        );
    }

    #[test]
    fn spec_documents_thread_safe_stress_harness() {
        for fragment in [
            "Thread-safe stress coverage",
            "`LowConcurrency` and `HighConcurrency`",
            "batched cell writes",
            "effect cleanup/rerun",
            "effect disposal racing",
            "concurrent cached reads",
            "stale publishes cannot",
            "tests/thread_safe_stress.rs",
        ] {
            assert_spec_contains(fragment);
        }
    }
}

// ============================================================================
// 1c. Benchmark Report Harness
// ============================================================================

mod benchmark_report_harness {
    const CARGO_TOML: &str = include_str!("../Cargo.toml");
    const BENCHMARKS_MD: &str = include_str!("../BENCHMARKS.md");
    const MAKEFILE: &str = include_str!("../Makefile");
    const REGRESSIONS_WORKFLOW: &str = include_str!("../.github/workflows/regressions.yml");
    const CI_WORKFLOW: &str = include_str!("../.github/workflows/ci.yml");
    const UPDATE_SCRIPT: &str = include_str!("../scripts/update-benchmark-results.py");

    fn package_version() -> &'static str {
        let mut in_package = false;

        for line in CARGO_TOML.lines() {
            let trimmed = line.trim();
            if trimmed == "[package]" {
                in_package = true;
                continue;
            }
            if in_package && trimmed.starts_with('[') {
                break;
            }
            if in_package && trimmed.starts_with("version") {
                return trimmed
                    .split_once('=')
                    .expect("version line should contain '='")
                    .1
                    .trim()
                    .trim_matches('"');
            }
        }

        panic!("package version should be present in Cargo.toml");
    }

    fn benchmark_section() -> &'static str {
        let start_marker = "<!-- benchmark-results:start -->";
        let end_marker = "<!-- benchmark-results:end -->";
        let start = BENCHMARKS_MD
            .find(start_marker)
            .expect("BENCHMARKS.md should contain benchmark results start marker");
        let end = BENCHMARKS_MD
            .find(end_marker)
            .expect("BENCHMARKS.md should contain benchmark results end marker");

        &BENCHMARKS_MD[start..end]
    }

    /// SPEC: README benchmark results are generated by a version-aware harness
    /// so release docs cannot silently drift from Cargo.toml.
    #[test]
    fn readme_benchmark_results_track_package_version() {
        let expected = format!(
            "Generated for package `lazily` version `{}`.",
            package_version()
        );

        assert!(
            benchmark_section().contains(&expected),
            "BENCHMARKS.md benchmark section should identify the current package version"
        );
    }

    /// SPEC: the README benchmark report publishes each required benchmark
    /// family and documents the refresh/check harness.
    #[test]
    fn readme_benchmark_results_cover_required_scenarios() {
        let section = benchmark_section();

        for expected in [
            "cached_reads",
            "cold_first_get",
            "dependency_fan_out",
            "set_cell_invalidation",
            "memo_equality_suppression",
            "effect_flushing",
            "batch_storms",
            "thread_safe_contention",
            "thread_safe_effect_contention",
            "thread_safe_graph_propagation",
            "profile_instrumentation",
        ] {
            assert!(
                section.contains(expected),
                "BENCHMARKS.md benchmark section should include {expected}"
            );
        }

        assert!(section.contains("python3 scripts/update-benchmark-results.py"));
        assert!(section.contains("cargo bench --features instrumentation"));
        assert!(section.contains("Instrumentation snapshots"));
        assert!(section.contains("Duplicate recomputes"));
        assert!(section.contains("ThreadSafe lock attribution"));
        assert!(section.contains("Regression budgets enforced by"));
        // #lzbenchbudgetheadroom: the budget table publishes the evidence each
        // ceiling is derived from — the observed range, the sample count, and
        // the classification — rather than a bare hand-typed maximum. A reader
        // who cannot see the spread cannot tell an enforced ceiling from one
        // that only ever fires on a busy machine.
        assert!(section.contains(
            "| Profile | Counter | Observed range | Samples | Classification | Enforced ceiling |"
        ));
        assert!(section.contains("deterministic"));
        assert!(section.contains("scheduling_sensitive"));
        assert!(
            section.contains("not enforced"),
            "the table must name the counters it does NOT enforce; an unnamed \
             blind spot reads as coverage"
        );
        assert!(section.contains("Budgets use lock acquisition counts"));
        assert!(section.contains("Synchronization strategy adoption gate"));
        assert!(section.contains("Required p50/p95 latency evidence"));
        assert!(section.contains("Required latency evidence uses Criterion sample"));
        assert!(section.contains("| Group | Case | p50 | p95 | Samples |"));
        assert!(section.contains("Sidecar frontiers"));
        assert!(section.contains("Dirty epochs"));
        assert!(section.contains("current_std_mutex_condvar"));
        assert!(section.contains("narrower_condvar_wakeups"));
        assert!(section.contains("parking_lot_style_parking"));
        assert!(section.contains("targeted_cas"));
        assert!(section.contains("get_refresh"));
        assert!(section.contains("set_cell_invalidation"));

        for expected in [
            "high_fan_out / 512",
            "same_slot_contention / 16",
            "independent_slot_contention / 16",
            "batched_write_bursts / 16",
            "same_slot_write_read / 16",
            "independent_slots / 16",
            "read_mostly_waiters / 16",
            "batched_write_bursts / 16",
            "queue_coalescing / 16",
            "cleanup_execution / 16",
            "batch_flush / 16",
            "fan_out_eager_validation / 16",
            "fan_out_lazy_dirty_epochs / 16",
            "fan_in_lazy_dirty_epochs / 16",
            "fan_in_batched_flush / 16",
        ] {
            assert!(
                section.contains(expected),
                "BENCHMARKS.md benchmark section should include contention matrix case {expected}"
            );
        }

        for expected in [
            "context_memo_effect",
            "context_fan_out_32",
            "context_batch_storm_64",
            "thread_safe_first_get_2",
            "thread_safe_set_cell_invalidation_high_fan_out_512",
            "thread_safe_set_cell_invalidation_same_slot_contention_16",
            "thread_safe_set_cell_invalidation_independent_slot_contention_16",
            "thread_safe_set_cell_invalidation_batched_write_bursts_16",
            "thread_safe_contention_same_slot_write_read_16",
            "thread_safe_contention_independent_slots_16",
            "thread_safe_contention_read_mostly_waiters_16",
            "thread_safe_contention_batched_write_bursts_16",
            "thread_safe_effect_contention_queue_coalescing_8",
            "thread_safe_effect_contention_queue_coalescing_16",
            "thread_safe_effect_contention_cleanup_execution_8",
            "thread_safe_effect_contention_cleanup_execution_16",
            "thread_safe_effect_contention_batch_flush_8",
            "thread_safe_effect_contention_batch_flush_16",
            "thread_safe_graph_propagation_fan_out_eager_validation_16",
            "thread_safe_graph_propagation_fan_out_lazy_dirty_epochs_16",
            "thread_safe_graph_propagation_fan_in_lazy_dirty_epochs_16",
            "thread_safe_graph_propagation_fan_in_batched_flush_16",
        ] {
            assert!(
                section.contains(expected),
                "BENCHMARKS.md benchmark section should include instrumentation row {expected}"
            );
        }

        assert!(CARGO_TOML.contains("name = \"instrumentation_profile\""));
        assert!(UPDATE_SCRIPT.contains("lazily-instrumentation-profile.csv"));
        assert!(UPDATE_SCRIPT.contains("SET_CELL_INVALIDATION_CASE_ORDER"));
        assert!(UPDATE_SCRIPT.contains("THREAD_SAFE_GRAPH_PROPAGATION_CASE_ORDER"));
        assert!(UPDATE_SCRIPT.contains("REGRESSION_BUDGETS"));
        assert!(UPDATE_SCRIPT.contains("regression_budget_failures"));
        assert!(UPDATE_SCRIPT.contains("instrumentation regression budget failure"));
        assert!(UPDATE_SCRIPT.contains("REQUIRED_LATENCY_CASES"));
        assert!(UPDATE_SCRIPT.contains("discover_latency_results"));
        assert!(UPDATE_SCRIPT.contains("required_latency_failures"));
        assert!(UPDATE_SCRIPT.contains("required latency evidence failure"));
        assert!(UPDATE_SCRIPT.contains("thread_safe_effect_contention"));
        assert!(UPDATE_SCRIPT.contains("thread_safe_effect_contention_queue_coalescing_16"));
        assert!(UPDATE_SCRIPT.contains("thread_safe_effect_contention_cleanup_execution_16"));
        assert!(UPDATE_SCRIPT.contains("thread_safe_effect_contention_batch_flush_16"));
        assert!(UPDATE_SCRIPT.contains("thread_safe_graph_propagation"));
        assert!(UPDATE_SCRIPT.contains("SYNC_STRATEGY_ADOPTION_GATE"));
        assert!(UPDATE_SCRIPT.contains("--check"));
        assert!(UPDATE_SCRIPT.contains("--budgets-only"));
        assert!(UPDATE_SCRIPT.contains("--refresh-profile"));

        // #vnmr: the gate is unconditionally enforcing. `--require-evidence` was
        // the opt-in spelling of "actually fail"; its absence used to mean a
        // pruned target dir exited 0. There is no opt-in any more, so the flag is
        // gone — and it must not come back, because a flag that turns enforcement
        // off is the escape hatch this gate exists without.
        assert!(
            !UPDATE_SCRIPT.contains("--require-evidence"),
            "budget enforcement must not be opt-in"
        );
        assert!(
            !UPDATE_SCRIPT.contains("NO BENCHMARK BUDGET WAS ENFORCED"),
            "the skip-and-exit-0 banner must not come back"
        );
        assert!(MAKEFILE.contains("scripts/update-benchmark-results.py --check --budgets-only"));
        // `make check` MEASURES before it gates, so the gate is satisfiable
        // honestly on a fresh clone rather than by skipping.
        assert!(MAKEFILE.contains("scripts/update-benchmark-results.py --record-evidence --quick"));
        assert!(
            MAKEFILE.contains("test-websocket benchmark-evidence benchmark-check"),
            "`check` must generate evidence immediately before enforcing budgets"
        );
        assert!(UPDATE_SCRIPT.contains("EXIT_EVIDENCE_ABSENT"));
        assert!(UPDATE_SCRIPT.contains("EXIT_EVIDENCE_STALE"));
        assert!(UPDATE_SCRIPT.contains("BENCHMARK EVIDENCE IS STALE"));

        assert!(REGRESSIONS_WORKFLOW.contains(
            "cargo +stable bench --locked --features \
             \"instrumentation,async,tokio,thread-safe\""
        ));
        assert!(REGRESSIONS_WORKFLOW.contains("run: make benchmark-evidence-record"));
        assert!(REGRESSIONS_WORKFLOW.contains("run: make benchmark-check"));
        assert!(REGRESSIONS_WORKFLOW.contains("runs-on: ubuntu-24.04"));
        // Per-push CI must MEASURE the budgets, not defer them to the nightly.
        assert!(CI_WORKFLOW.contains("run: make benchmark-evidence"));
        assert!(CI_WORKFLOW.contains("run: make benchmark-check"));
        assert!(UPDATE_SCRIPT.contains("benchmark-results:start"));
    }
}

// ============================================================================
// 1e. Storage Strategy Evaluation
// ============================================================================

mod storage_strategy_evaluation {
    const BENCHMARKS_MD: &str = include_str!("../BENCHMARKS.md");
    const README: &str = include_str!("../README.md");
    const SPEC: &str = include_str!("../SPEC.md");

    /// SPEC: sharded or versioned ThreadSafeContext storage is benchmark-gated
    /// by the contention matrix, not adopted directly from the read-mostly
    /// cached-value prototype.
    #[test]
    fn sharded_storage_evaluation_is_benchmark_gated() {
        for expected in [
            "Sharded/versioned storage evaluation",
            "read-mostly cached-value sidecar",
            "versioned optimistically",
            "atomic cache revision",
            "mid-read optimistic validation fallback",
            "Optimistic cached reads fall back",
            "per-slot recompute/value-publish sidecar",
            "per-slot dependency summary",
            "cell-only dirty refresh",
            "SlotId-partitioned recompute sidecar",
            "per-node dependent frontier sidecars",
            "cache revision acts as the dirty epoch",
            "explicit `InvalidationPlan`",
            "snapshot hard-clear frontiers",
            "snapshot and application under the context mutex",
            "thread-local batch frames",
            "local batch-frame prototype",
            "Effect-heavy contention profiles",
            "thread_safe_effect_contention",
            "thread_safe_graph_propagation",
            "effect queue coalescing",
            "cleanup execution",
            "nested batch flush behavior",
            "deterministic lock-site budgets",
            "Synchronization strategy comparison",
            "std::sync",
            "Condvar wakeups",
            "parking_lot",
            "style parking",
            "targeted CAS",
            "throughput plus p50/p95 latency",
            "Benchmark watch items from generated README deltas",
            "controlled A/B rerun before tuning",
            "clean worktrees or Criterion baselines",
            "statistically significant change",
            "waiter-counted handoff",
            "notify_one",
            "waiter-counted handoff wakeup draining",
            "fast-frontier fallback while dependency discovery is active",
            "explicit invalidation-plan safety envelope",
            "dirty same-slot contention",
            "Do not replace the single graph lock with sharded storage",
            "Do not treat versioned optimistic reads as an invalidation optimization",
            "same_slot_write_read",
            "independent_slots",
            "read_mostly_waiters",
            "batched_write_bursts",
        ] {
            assert!(
                SPEC.contains(expected),
                "SPEC should document storage evaluation evidence: {expected}"
            );
        }

        for expected in [
            "set_cell_invalidation | independent_slot_contention / 16",
            "set_cell_invalidation | batched_write_bursts / 16",
            "thread_safe_set_cell_invalidation_independent_slot_contention_16",
            "thread_safe_effect_contention_queue_coalescing_16",
            "thread_safe_effect_contention_cleanup_execution_16",
            "thread_safe_effect_contention_batch_flush_16",
            "Synchronization strategy adoption gate",
            "Required p50/p95 latency evidence",
            "Watch-item A/B follow-up",
            "cached ThreadSafeContext read latency",
            "a8b6fc3 vs c917401",
            "73.48 ns baseline vs 73.20 ns current",
            "effect cleanup contention at 16 workers",
            "2.31 ms baseline vs 2.43 ms current",
            "ThreadSafe lock attribution for contention profiles",
            "thread_safe_graph_propagation | fan_out_lazy_dirty_epochs / 16",
            "Sidecar frontiers",
            "Dirty epochs",
        ] {
            assert!(
                BENCHMARKS_MD.contains(expected),
                "BENCHMARKS.md benchmark report should expose storage evaluation evidence: {expected}"
            );
        }

        {
            let expected = "explicit frontier plan";
            assert!(
                README.contains(expected),
                "README should contain storage evaluation evidence: {expected}"
            );
        }
    }
}

// ============================================================================
// 1f. Benchmark Instrumentation
// ============================================================================

#[cfg(feature = "instrumentation")]
mod benchmark_instrumentation {
    use super::*;
    use lazily::ThreadSafeLockSite;
    use std::sync::atomic::AtomicBool;

    /// SPEC: The optional instrumentation feature exposes lightweight counters
    /// for benchmark diagnostics without changing the public reactive semantics.
    #[test]
    fn context_instrumentation_tracks_graph_work() {
        let ctx = Context::new();
        let root = ctx.source(0usize);
        let parity = ctx.computed(move |ctx| ctx.get(&root) % 2);
        let label = ctx.computed(move |ctx| ctx.get(&parity).wrapping_add(1));
        let _effect = ctx.effect(move |ctx| {
            ctx.get(&label);
        });

        let allocation_snapshot = ctx.instrumentation_snapshot();
        assert_eq!(
            allocation_snapshot.node_allocations, 4,
            "cell, memo slot, computed slot, and effect should allocate nodes"
        );

        ctx.reset_instrumentation();
        ctx.set(&root, 2);
        assert_eq!(ctx.get(&label), 1);

        let snapshot = ctx.instrumentation_snapshot();
        assert!(
            snapshot.slot_recomputes >= 1,
            "dirty memo slot should validate during effect flush or later get"
        );
        assert!(
            snapshot.dependency_edges_added >= 1,
            "recompute should re-add tracked dependency edges"
        );
        assert!(
            snapshot.dependency_edges_removed >= 1,
            "recompute should remove stale dependency edges before rediscovery"
        );
        assert_eq!(snapshot.effect_queue_pushes, 1);
        assert!(snapshot.max_effect_queue_depth >= 1);
    }

    /// SPEC: `ThreadSafeContext` instrumentation tracks graph work plus
    /// in-flight first-get deduplication and lock timing.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_instrumentation_tracks_dedup_and_locks() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(40usize);
        let gate = Arc::new(AtomicUsize::new(0));
        let compute_runs = Arc::new(AtomicUsize::new(0));
        let gate_for_slot = Arc::clone(&gate);
        let compute_runs_for_slot = Arc::clone(&compute_runs);
        let answer = ctx.computed(move |ctx| {
            let run = compute_runs_for_slot.fetch_add(1, Ordering::SeqCst) + 1;
            if run == 1 {
                gate_for_slot.store(1, Ordering::SeqCst);
                while gate_for_slot.load(Ordering::SeqCst) == 1 {
                    thread::yield_now();
                }
            }
            ctx.get(&root).wrapping_add(2)
        });

        let allocation_snapshot = ctx.instrumentation_snapshot();
        assert_eq!(
            allocation_snapshot.node_allocations, 2,
            "cell and computed slot should allocate nodes"
        );

        ctx.reset_instrumentation();

        let computing_ctx = ctx.clone();
        let computing_worker = thread::spawn(move || computing_ctx.get(&answer));
        while gate.load(Ordering::SeqCst) != 1 {
            thread::yield_now();
        }

        let waiting_ctx = ctx.clone();
        let waiting_worker = thread::spawn(move || waiting_ctx.get(&answer));
        for _ in 0..100_000 {
            if lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions > 0 {
                break;
            }
            thread::yield_now();
        }
        gate.store(2, Ordering::SeqCst);

        assert_eq!(computing_worker.join().expect("worker should finish"), 42);
        assert_eq!(waiting_worker.join().expect("worker should finish"), 42);

        ctx.set(&root, 41);
        assert_eq!(ctx.get(&answer), 43);

        let snapshot = ctx.instrumentation_snapshot();
        assert!(
            snapshot.slot_recomputes >= 2,
            "first get plus invalidation should recompute at least twice"
        );
        assert_eq!(
            snapshot.duplicate_speculative_recomputes, 0,
            "deduplication should prevent duplicate speculative publication races"
        );
        assert!(
            snapshot.dependency_edges_added >= 1,
            "published compute should track the cell dependency"
        );
        assert!(
            snapshot.lock_acquisitions > 0,
            "thread-safe operations should acquire the graph lock"
        );
        // v0.24.0+ (#lzstateinvalidation): all invalidation goes through the
        // single state-locked path — the former per-node sidecar frontiers were
        // removed. The vestigial sidecar counters are always 0; what we now
        // assert is that the state lock is taken once per changed-cell write and
        // that the dirty-epoch frontier still advances.
        assert_eq!(
            snapshot.sidecar_invalidation_frontiers, 0,
            "sidecar frontiers were removed in v0.24.0; invalidation is state-locked"
        );
        assert!(
            snapshot.dirty_epoch_advances >= 1,
            "changed-cell invalidation should advance the dirty epoch frontier"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::SetCellInvalidation).lock_acquisitions,
            1,
            "one state lock for the single changed-cell invalidation"
        );

        let profile = ctx.lock_profile_snapshot();
        let profiled_acquisitions = profile
            .iter()
            .map(|site| site.lock_acquisitions)
            .sum::<u64>();
        assert_eq!(
            profiled_acquisitions, snapshot.lock_acquisitions,
            "per-site lock acquisitions should sum to the aggregate counter"
        );

        for expected_site in [
            ThreadSafeLockSite::GetRefresh,
            ThreadSafeLockSite::DependencyEdge,
            ThreadSafeLockSite::Publish,
            ThreadSafeLockSite::InFlightWait,
        ] {
            let site = profile
                .iter()
                .find(|site| site.site == expected_site)
                .expect("lock site should be present");
            assert!(
                site.lock_acquisitions > 0,
                "{expected_site:?} should record lock acquisitions"
            );
        }
    }

    /// SPEC: a fresh cached thread-safe get uses the per-slot fast path instead
    /// of taking the graph lock or recursively refreshing unchanged dependencies.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_cached_get_bypasses_get_refresh_graph_lock() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(40usize);
        let answer = ctx.computed(move |ctx| ctx.get(&root).wrapping_add(2));

        assert_eq!(ctx.get(&answer), 42);
        ctx.reset_instrumentation();

        assert_eq!(ctx.get(&answer), 42);

        let snapshot = ctx.instrumentation_snapshot();
        assert_eq!(
            snapshot.slot_recomputes, 0,
            "fresh cached get should not recompute"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::GetRefresh).lock_acquisitions,
            0,
            "fresh cached get should clone from the per-slot fast path without a GetRefresh graph lock"
        );
    }

    /// SPEC: thread-safe effect reruns preserve unchanged dependency edges and
    /// skip redundant edge-registration locks.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_effect_rerun_preserves_unchanged_dependency_edges() {
        let ctx = ThreadSafeContext::new();
        let cells = [
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
        ];
        let runs = Arc::new(AtomicUsize::new(0));
        let sink = Arc::new(AtomicUsize::new(0));
        let effect_cells = cells;
        let runs_for_effect = Arc::clone(&runs);
        let sink_for_effect = Arc::clone(&sink);
        let _effect = ctx.effect(move |ctx| {
            runs_for_effect.fetch_add(1, Ordering::SeqCst);
            let total = effect_cells
                .iter()
                .fold(0usize, |sum, cell| sum.wrapping_add(ctx.get(cell)));
            sink_for_effect.store(total, Ordering::SeqCst);
        });

        assert_eq!(runs.load(Ordering::SeqCst), 1);
        ctx.reset_instrumentation();

        ctx.batch(|ctx| {
            for (index, cell) in cells.iter().enumerate() {
                ctx.set(cell, index + 1);
            }
        });

        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(sink.load(Ordering::SeqCst), 10);

        let snapshot = ctx.instrumentation_snapshot();
        assert_eq!(
            snapshot.dependency_edges_removed, 0,
            "stable effect dependencies should stay subscribed across rerun"
        );
        assert_eq!(
            snapshot.dependency_edges_added, 0,
            "stable effect dependencies should not be re-added during rerun"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::DependencyEdge).lock_acquisitions,
            0,
            "stable effect dependencies should not take dependency-edge graph locks"
        );
        assert_eq!(
            snapshot.effect_queue_pushes, 1,
            "batched invalidation should coalesce the shared effect queue push"
        );
    }

    /// SPEC: fresh cached thread-safe gets use a per-slot read guard, so
    /// independent readers can clone the cached value concurrently without
    /// entering write-side graph mutation.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_cached_get_allows_concurrent_fresh_readers() {
        #[derive(Debug)]
        struct BlockingClone {
            active_clones: Arc<AtomicUsize>,
            max_active_clones: Arc<AtomicUsize>,
            release: Arc<AtomicBool>,
        }

        impl Clone for BlockingClone {
            fn clone(&self) -> Self {
                let active = self.active_clones.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active_clones.fetch_max(active, Ordering::SeqCst);
                while !self.release.load(Ordering::SeqCst)
                    && self.max_active_clones.load(Ordering::SeqCst) < 2
                {
                    thread::yield_now();
                }
                self.active_clones.fetch_sub(1, Ordering::SeqCst);
                Self {
                    active_clones: Arc::clone(&self.active_clones),
                    max_active_clones: Arc::clone(&self.max_active_clones),
                    release: Arc::clone(&self.release),
                }
            }
        }

        let ctx = ThreadSafeContext::new();
        let active_clones = Arc::new(AtomicUsize::new(0));
        let max_active_clones = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(true));
        let value = ctx.slot({
            let active_clones = Arc::clone(&active_clones);
            let max_active_clones = Arc::clone(&max_active_clones);
            let release = Arc::clone(&release);
            move |_| BlockingClone {
                active_clones: Arc::clone(&active_clones),
                max_active_clones: Arc::clone(&max_active_clones),
                release: Arc::clone(&release),
            }
        });

        let _ = ctx.get(&value);
        active_clones.store(0, Ordering::SeqCst);
        max_active_clones.store(0, Ordering::SeqCst);
        release.store(false, Ordering::SeqCst);

        let first_ctx = ctx.clone();
        let first = thread::spawn(move || first_ctx.get(&value));
        let second_ctx = ctx.clone();
        let second = thread::spawn(move || second_ctx.get(&value));

        for _ in 0..100_000 {
            if max_active_clones.load(Ordering::SeqCst) >= 2 {
                break;
            }
            thread::yield_now();
        }
        release.store(true, Ordering::SeqCst);

        let _ = first.join().expect("first reader should finish");
        let _ = second.join().expect("second reader should finish");
        assert_eq!(
            max_active_clones.load(Ordering::SeqCst),
            2,
            "fresh readers should share the per-slot fast path instead of serializing behind the graph lock"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::DependencyEdge).lock_acquisitions,
            0,
            "untracked fresh reads should not mutate dependency edges"
        );
    }

    /// SPEC: the cached get fast path must still observe invalidation from
    /// another thread before returning, while cell-only dirty slots can bypass
    /// the graph-locked refresh decision.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_cached_get_revalidates_after_cross_thread_invalidation() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(40usize);
        let answer = ctx.computed(move |ctx| ctx.get(&root).wrapping_add(2));

        assert_eq!(ctx.get(&answer), 42);
        ctx.reset_instrumentation();

        let writer_ctx = ctx.clone();
        let writer = thread::spawn(move || {
            writer_ctx.set(&root, 41);
        });
        writer.join().expect("writer should finish");

        assert_eq!(ctx.get(&answer), 43);

        let snapshot = ctx.instrumentation_snapshot();
        assert_eq!(
            snapshot.slot_recomputes, 1,
            "invalidated cached get should recompute before returning"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::GetRefresh).lock_acquisitions,
            0,
            "cell-only invalidated cached get should recompute from the per-slot dependency summary"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::Publish).lock_acquisitions,
            1,
            "cell-only invalidated cached get should need only the final publish graph mutation"
        );
    }

    /// SPEC: in-flight thread-safe recompute waiters park instead of repeatedly
    /// reacquiring the graph lock while another thread owns the computation.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_in_flight_wait_parks_until_recompute_finishes() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(40usize);
        let gate = Arc::new(AtomicUsize::new(0));
        let compute_runs = Arc::new(AtomicUsize::new(0));
        let gate_for_slot = Arc::clone(&gate);
        let compute_runs_for_slot = Arc::clone(&compute_runs);
        let answer = ctx.computed(move |ctx| {
            let run = compute_runs_for_slot.fetch_add(1, Ordering::SeqCst) + 1;
            if run == 1 {
                gate_for_slot.store(1, Ordering::SeqCst);
                while gate_for_slot.load(Ordering::SeqCst) == 1 {
                    thread::yield_now();
                }
            }
            ctx.get(&root).wrapping_add(2)
        });

        let computing_ctx = ctx.clone();
        let computing_worker = thread::spawn(move || computing_ctx.get(&answer));
        while gate.load(Ordering::SeqCst) != 1 {
            thread::yield_now();
        }

        ctx.reset_instrumentation();

        let waiting_ctx = ctx.clone();
        let waiting_worker = thread::spawn(move || waiting_ctx.get(&answer));
        for _ in 0..100_000 {
            if lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions > 0 {
                break;
            }
            thread::yield_now();
        }
        assert!(
            lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions > 0,
            "waiter should enter the in-flight recompute path before the compute is released"
        );

        thread::sleep(Duration::from_millis(10));
        let parked_acquisitions =
            lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions;
        assert!(
            parked_acquisitions <= 4,
            "parked waiter should not spin-acquire the graph lock while compute is in flight; \
             saw {parked_acquisitions} acquisitions"
        );

        gate.store(2, Ordering::SeqCst);

        assert_eq!(computing_worker.join().expect("worker should finish"), 42);
        assert_eq!(waiting_worker.join().expect("worker should finish"), 42);
    }

    /// SPEC: dirty same-slot readers first check per-slot recompute state, so
    /// waiters park behind the in-flight owner without taking the graph
    /// `get_refresh` or `publish` locks.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_dirty_same_slot_waiters_bypass_graph_locks() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(40usize);
        let gate = Arc::new(AtomicUsize::new(0));
        let compute_runs = Arc::new(AtomicUsize::new(0));
        let gate_for_slot = Arc::clone(&gate);
        let compute_runs_for_slot = Arc::clone(&compute_runs);
        let answer = ctx.computed(move |ctx| {
            let run = compute_runs_for_slot.fetch_add(1, Ordering::SeqCst) + 1;
            if run == 2 {
                gate_for_slot.store(1, Ordering::SeqCst);
                while gate_for_slot.load(Ordering::SeqCst) == 1 {
                    thread::yield_now();
                }
            }
            ctx.get(&root).wrapping_add(2)
        });

        assert_eq!(ctx.get(&answer), 42);
        ctx.set(&root, 41);
        ctx.reset_instrumentation();

        let computing_ctx = ctx.clone();
        let computing_worker = thread::spawn(move || computing_ctx.get(&answer));
        while gate.load(Ordering::SeqCst) != 1 {
            thread::yield_now();
        }

        let waiter_count = 4;
        let waiters = (0..waiter_count)
            .map(|_| {
                let waiting_ctx = ctx.clone();
                thread::spawn(move || waiting_ctx.get(&answer))
            })
            .collect::<Vec<_>>();

        for _ in 0..100_000 {
            if lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions
                >= waiter_count as u64
            {
                break;
            }
            thread::yield_now();
        }

        assert!(
            lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions
                >= waiter_count as u64,
            "all same-slot waiters should enter the per-slot in-flight path"
        );
        thread::sleep(Duration::from_millis(10));

        let get_refresh_locks = lock_site(&ctx, ThreadSafeLockSite::GetRefresh);
        assert!(
            get_refresh_locks.lock_acquisitions <= 2,
            "dirty same-slot waiters should not acquire get_refresh graph locks; \
             saw {} acquisitions",
            get_refresh_locks.lock_acquisitions
        );

        let publish_locks = lock_site(&ctx, ThreadSafeLockSite::Publish);
        assert!(
            publish_locks.lock_acquisitions <= 1,
            "dirty same-slot waiters should not acquire publish graph locks before parking; \
             saw {} acquisitions",
            publish_locks.lock_acquisitions
        );

        gate.store(2, Ordering::SeqCst);

        assert_eq!(computing_worker.join().expect("worker should finish"), 43);
        for waiter in waiters {
            assert_eq!(waiter.join().expect("waiter should finish"), 43);
        }
        assert_eq!(
            compute_runs.load(Ordering::SeqCst),
            2,
            "dirty same-slot contention should share one recompute after invalidation"
        );
    }

    /// SPEC: independent changed-cell invalidations each take the state lock
    /// once (v0.24.0+, #lzstateinvalidation). The former per-node SlotId sidecar
    /// frontiers were removed in favor of a single state-locked DFS — the same
    /// model lazily-cpp uses (one recursive_mutex, raw-pointer inner loop).
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_independent_cell_invalidations_use_sharded_sidecars() {
        let ctx = ThreadSafeContext::new();
        let workers = 8usize;
        let iters = 16usize;
        let roots = (0..workers)
            .map(|worker| ctx.source(worker))
            .collect::<Vec<_>>();
        let values = roots
            .iter()
            .map(|root| {
                let root = *root;
                ctx.computed(move |ctx| ctx.get(&root).wrapping_add(1))
            })
            .collect::<Vec<_>>();

        for (worker, value) in values.iter().enumerate() {
            assert_eq!(ctx.get(value), worker + 1);
        }
        ctx.reset_instrumentation();

        let barrier = Arc::new(Barrier::new(workers));
        let threads = (0..workers)
            .map(|worker| {
                let ctx = ctx.clone();
                let barrier = Arc::clone(&barrier);
                let root = roots[worker];
                thread::spawn(move || {
                    barrier.wait();
                    for iter in 0..iters {
                        ctx.set(&root, worker.wrapping_mul(iters).wrapping_add(iter));
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().expect("independent setter should finish");
        }

        // Each changed-cell write acquires the state lock exactly once for its
        // invalidation DFS. The first write per worker (iter 0) sets the cell
        // from its initial value to worker*0+0=0 — only changed values count.
        let snapshot = ctx.instrumentation_snapshot();
        let set_cell_locks = lock_site(&ctx, ThreadSafeLockSite::SetCellInvalidation);
        assert!(
            set_cell_locks.lock_acquisitions > 0,
            "state-locked invalidation should record one lock per changed-cell write"
        );
        assert_eq!(
            snapshot.sidecar_invalidation_frontiers, 0,
            "sidecar frontiers were removed in v0.24.0; invalidation is state-locked"
        );
        assert!(
            snapshot.dirty_epoch_advances >= workers as u64,
            "dirty epoch advances should cover at least one per independent root"
        );

        for value in &values {
            assert!(
                !ctx.is_set(value),
                "state-locked invalidation should make cached slots stale before graph refresh"
            );
        }
        for (worker, value) in values.iter().enumerate() {
            assert_eq!(
                ctx.get(value),
                worker.wrapping_mul(iters).wrapping_add(iters - 1) + 1
            );
        }
    }

    /// SPEC: independent cell-only dirty slots use their per-slot dependency
    /// summaries to bypass the graph-locked refresh decision, then publish each
    /// recompute through one final graph mutation.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_independent_cell_only_refreshes_skip_get_refresh_locks() {
        let ctx = ThreadSafeContext::new();
        let workers = 8usize;
        let roots = (0..workers)
            .map(|worker| ctx.source(worker))
            .collect::<Vec<_>>();
        let values = roots
            .iter()
            .map(|root| {
                let root = *root;
                ctx.computed(move |ctx| ctx.get(&root).wrapping_add(1))
            })
            .collect::<Vec<_>>();

        for (worker, value) in values.iter().enumerate() {
            assert_eq!(ctx.get(value), worker + 1);
        }

        for (worker, root) in roots.iter().enumerate() {
            ctx.set(root, worker + 10);
        }
        ctx.reset_instrumentation();

        for (worker, value) in values.iter().enumerate() {
            assert_eq!(ctx.get(value), worker + 11);
        }

        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::GetRefresh).lock_acquisitions,
            0,
            "cell-only independent refreshes should use per-slot dependency summaries"
        );
        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::Publish).lock_acquisitions,
            workers as u64,
            "each independent slot should take only its final publish mutation"
        );
    }

    /// SPEC: same-thread `ThreadSafeContext` batches keep changed cells in a
    /// local batch frame and take one graph invalidation lock at batch exit.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_batch_queues_same_thread_writes_before_graph_flush() {
        let ctx = ThreadSafeContext::new();
        let cells = [
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
        ];
        let total = ctx.computed(move |ctx| {
            cells
                .iter()
                .fold(0usize, |sum, cell| sum.wrapping_add(ctx.get(cell)))
        });

        assert_eq!(ctx.get(&total), 0);
        ctx.reset_instrumentation();

        ctx.batch(|ctx| {
            for (offset, cell) in cells.iter().enumerate() {
                ctx.set(cell, offset + 1);
            }

            assert_eq!(
                lock_site(ctx, ThreadSafeLockSite::SetCellInvalidation).lock_acquisitions,
                0,
                "same-thread batch writes should queue without per-write graph invalidation locks"
            );
            assert!(
                ctx.is_set(&total),
                "dependent slot should remain cached until the batch exits"
            );
        });

        assert_eq!(
            lock_site(&ctx, ThreadSafeLockSite::SetCellInvalidation).lock_acquisitions,
            1,
            "batch exit should apply one coalesced graph invalidation flush"
        );
        assert!(
            !ctx.is_set(&total),
            "batch exit should make the dependent slot stale"
        );
        assert_eq!(ctx.get(&total), 10);
    }

    /// SPEC: in-flight recompute notifications are scoped to the slot that
    /// finished, so unrelated in-flight slot waiters stay parked.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_in_flight_waiters_are_scoped_to_finished_slot() {
        let ctx = ThreadSafeContext::new();
        let root_a = ctx.source(40usize);
        let root_b = ctx.source(100usize);
        let gate_a = Arc::new(AtomicUsize::new(0));
        let gate_b = Arc::new(AtomicUsize::new(0));
        let gate_a_for_slot = Arc::clone(&gate_a);
        let gate_b_for_slot = Arc::clone(&gate_b);
        let value_a = ctx.computed(move |ctx| {
            gate_a_for_slot.store(1, Ordering::SeqCst);
            while gate_a_for_slot.load(Ordering::SeqCst) == 1 {
                thread::yield_now();
            }
            ctx.get(&root_a).wrapping_add(2)
        });
        let value_b = ctx.computed(move |ctx| {
            gate_b_for_slot.store(1, Ordering::SeqCst);
            while gate_b_for_slot.load(Ordering::SeqCst) == 1 {
                thread::yield_now();
            }
            ctx.get(&root_b).wrapping_add(2)
        });

        let computing_a_ctx = ctx.clone();
        let computing_a = thread::spawn(move || computing_a_ctx.get(&value_a));
        let computing_b_ctx = ctx.clone();
        let computing_b = thread::spawn(move || computing_b_ctx.get(&value_b));
        while gate_a.load(Ordering::SeqCst) != 1 || gate_b.load(Ordering::SeqCst) != 1 {
            thread::yield_now();
        }

        ctx.reset_instrumentation();
        let waiter_a_done = Arc::new(AtomicUsize::new(0));
        let waiter_b_done = Arc::new(AtomicUsize::new(0));

        let waiter_a_ctx = ctx.clone();
        let waiter_a_done_for_thread = Arc::clone(&waiter_a_done);
        let waiter_a = thread::spawn(move || {
            waiter_a_done_for_thread.store(waiter_a_ctx.get(&value_a), Ordering::SeqCst);
        });
        let waiter_b_ctx = ctx.clone();
        let waiter_b_done_for_thread = Arc::clone(&waiter_b_done);
        let waiter_b = thread::spawn(move || {
            waiter_b_done_for_thread.store(waiter_b_ctx.get(&value_b), Ordering::SeqCst);
        });

        for _ in 0..100_000 {
            if lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions >= 2 {
                break;
            }
            thread::yield_now();
        }
        let parked_acquisitions =
            lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions;
        assert!(
            parked_acquisitions >= 2,
            "both slot waiters should enter the in-flight wait path before release"
        );

        gate_a.store(2, Ordering::SeqCst);
        while waiter_a_done.load(Ordering::SeqCst) == 0 {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(10));

        let after_a_release = lock_site(&ctx, ThreadSafeLockSite::InFlightWait).lock_acquisitions;
        assert_eq!(
            waiter_b_done.load(Ordering::SeqCst),
            0,
            "the second slot waiter should remain blocked while its compute is still in flight"
        );
        assert!(
            after_a_release <= parked_acquisitions + 1,
            "finishing slot A should only wake slot A's waiter; \
             saw {after_a_release} in-flight acquisitions from baseline {parked_acquisitions}"
        );

        gate_b.store(2, Ordering::SeqCst);

        assert_eq!(computing_a.join().expect("worker should finish"), 42);
        assert_eq!(waiter_a.join().expect("waiter should finish"), ());
        assert_eq!(waiter_a_done.load(Ordering::SeqCst), 42);
        assert_eq!(computing_b.join().expect("worker should finish"), 102);
        assert_eq!(waiter_b.join().expect("waiter should finish"), ());
        assert_eq!(waiter_b_done.load(Ordering::SeqCst), 102);
    }

    /// SPEC: Thread-safe recompute preserves unchanged dependency edges and skips
    /// redundant edge-registration locks for dependencies already subscribed.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_recompute_preserves_unchanged_dependency_edges() {
        let ctx = ThreadSafeContext::new();
        let cells = [
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
        ];
        let total = ctx.computed(move |ctx| {
            cells
                .iter()
                .fold(0usize, |sum, cell| sum.wrapping_add(ctx.get(cell)))
        });

        assert_eq!(ctx.get(&total), 0);
        ctx.reset_instrumentation();

        ctx.set(&cells[0], 1);
        assert_eq!(ctx.get(&total), 1);

        let snapshot = ctx.instrumentation_snapshot();
        assert_eq!(snapshot.dependency_edges_removed, 0);
        assert_eq!(snapshot.dependency_edges_added, 0);

        let dependency_edge_locks = lock_site(&ctx, ThreadSafeLockSite::DependencyEdge);
        assert_eq!(
            dependency_edge_locks.lock_acquisitions, 0,
            "unchanged dependencies should stay subscribed without redundant edge-registration locks"
        );

        let get_refresh_locks = lock_site(&ctx, ThreadSafeLockSite::GetRefresh);
        assert_eq!(
            get_refresh_locks.lock_acquisitions, 0,
            "forced recompute of a cell-only total should skip graph get_refresh locks; \
             saw {} get_refresh acquisitions",
            get_refresh_locks.lock_acquisitions
        );

        let publish_locks = lock_site(&ctx, ThreadSafeLockSite::Publish);
        assert_eq!(
            publish_locks.lock_acquisitions, 1,
            "forced recompute of a cell-only total should only take the final publish lock"
        );
    }

    /// SPEC: Thread-safe recompute diffs old and new dependency sets at publish
    /// so dynamic dependency changes add and remove only changed edges.
    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_recompute_diffs_dynamic_dependency_edges() {
        let ctx = ThreadSafeContext::new();
        let use_right = ctx.source(false);
        let left = ctx.source(1usize);
        let right = ctx.source(10usize);
        let selected = ctx.computed(move |ctx| {
            if ctx.get(&use_right) {
                ctx.get(&right)
            } else {
                ctx.get(&left)
            }
        });

        assert_eq!(ctx.get(&selected), 1);
        ctx.reset_instrumentation();

        ctx.set(&use_right, true);
        assert_eq!(ctx.get(&selected), 10);

        let snapshot = ctx.instrumentation_snapshot();
        assert_eq!(snapshot.dependency_edges_added, 1);
        assert_eq!(snapshot.dependency_edges_removed, 1);

        let dependency_edge_locks = lock_site(&ctx, ThreadSafeLockSite::DependencyEdge);
        assert_eq!(
            dependency_edge_locks.lock_acquisitions, 1,
            "only newly discovered dependencies should take edge-registration locks"
        );
    }

    fn lock_site(
        ctx: &ThreadSafeContext,
        site: ThreadSafeLockSite,
    ) -> lazily::ThreadSafeLockSiteSnapshot {
        ctx.lock_profile_snapshot()
            .into_iter()
            .find(|snapshot| snapshot.site == site)
            .expect("lock site should be present")
    }
}

// ============================================================================
// 2. Slot Semantics
// ============================================================================

mod slot_semantics {
    use super::*;

    /// SPEC: "First access calls the compute function, caches the result"
    #[test]
    fn unset_slot_computes_on_first_access() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let s = ctx.slot(|_| {
            COUNT.with(|c| c.set(c.get() + 1));
            42
        });

        // Before access, compute hasn't run.
        COUNT.with(|c| assert_eq!(c.get(), 0, "compute should not run before first access"));
        assert!(!ctx.is_set(&s), "slot should be unset before first access");

        // First access triggers compute.
        assert_eq!(ctx.get(&s), 42);
        COUNT.with(|c| {
            assert_eq!(
                c.get(),
                1,
                "compute should run exactly once on first access"
            )
        });
    }

    /// SPEC: Value cached after first access — no recompute on subsequent gets.
    #[test]
    fn value_cached_after_first_access() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let s = ctx.slot(|_| {
            COUNT.with(|c| c.set(c.get() + 1));
            99
        });

        // Access multiple times.
        for _ in 0..5 {
            assert_eq!(ctx.get(&s), 99);
        }
        COUNT.with(|c| {
            assert_eq!(
                c.get(),
                1,
                "compute should only run once despite 5 accesses"
            )
        });
        assert!(ctx.is_set(&s), "slot should be set after access");
    }

    /// SPEC: "slot.clear() removes the cached value"
    /// Since clear_slot is private, we test clearing via cell invalidation.
    #[test]
    fn clear_removes_cached_value() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.slot(move |ctx| ctx.get(&c) * 10);

        assert_eq!(ctx.get(&s), 10);
        assert!(ctx.is_set(&s));

        // Changing the cell clears the dependent slot.
        ctx.set(&c, 2);
        assert!(!ctx.is_set(&s), "slot should be cleared after cell change");
    }

    /// SPEC: Changed-cell invalidation keeps downstream cached until a changed
    /// intermediate value is proven.
    #[test]
    fn clear_cascades_to_dependents() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let a = ctx.slot(move |ctx| ctx.get(&c));
        let b = ctx.slot(move |ctx| ctx.get(&a) + 10);
        let d = ctx.slot(move |ctx| ctx.get(&b) + 100);

        // Compute all.
        assert_eq!(ctx.get(&d), 111);
        assert!(ctx.is_set(&a));
        assert!(ctx.is_set(&b));
        assert!(ctx.is_set(&d));

        // Change cell — slots become dirty while keeping cached values for
        // validation until access proves whether `a` changed.
        ctx.set(&c, 2);
        assert!(!ctx.is_set(&a), "a should be stale");
        assert!(!ctx.is_set(&b), "b should be dirty");
        assert!(!ctx.is_set(&d), "d should be dirty");
        assert_eq!(ctx.get(&d), 112);
    }

    /// SPEC: "Dependencies auto-discovered via tracking stack"
    #[test]
    fn dependencies_auto_discovered() {
        thread_local! {
            static B_COUNT: Cell<u32> = const { Cell::new(0) };
        }
        B_COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let a = ctx.slot(move |ctx| ctx.get(&c));
        let b = ctx.slot(move |ctx| {
            B_COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&a) * 2
        });

        assert_eq!(ctx.get(&b), 2);
        B_COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Changing cell should invalidate b (through a) automatically.
        ctx.set(&c, 5);
        assert_eq!(ctx.get(&b), 10);
        B_COUNT.with(|cnt| assert_eq!(cnt.get(), 2, "b should recompute after dependency changed"));
    }

    /// SPEC: "Immutable by default: Once set, a Slot's value doesn't change
    /// — only clear + recompute"
    #[test]
    fn slot_is_immutable_between_clears() {
        thread_local! {
            static COUNTER: Cell<u32> = const { Cell::new(0) };
        }
        COUNTER.with(|c| c.set(0));

        let ctx = Context::new();
        let s = ctx.slot(|_| {
            COUNTER.with(|c| {
                let v = c.get();
                c.set(v + 1);
                v
            })
        });

        // First access returns 0.
        assert_eq!(ctx.get(&s), 0);
        // Subsequent accesses return the cached value, not a new computation.
        assert_eq!(ctx.get(&s), 0);
        assert_eq!(ctx.get(&s), 0);
        COUNTER.with(|c| assert_eq!(c.get(), 1, "compute should only run once"));
    }
}

// ============================================================================
// 3. Cell Semantics
// ============================================================================

mod cell_semantics {
    use super::*;

    /// SPEC: "Cell::new(initial) — Create with initial value"
    #[test]
    fn cell_initial_value_accessible() {
        let ctx = Context::new();
        let c = ctx.source(42i32);
        assert_eq!(ctx.get(&c), 42);
    }

    /// SPEC: `cell.set(&ctx, value)` updates the cell value.
    #[test]
    fn cell_set_updates_value() {
        let ctx = Context::new();
        let c = ctx.source(0i32);
        ctx.set(&c, 100);
        assert_eq!(ctx.get(&c), 100);
    }

    /// SPEC: Set with same value (PartialEq) does NOT invalidate dependents.
    #[test]
    fn set_same_value_does_not_invalidate() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(5i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c) * 3
        });

        assert_eq!(ctx.get(&s), 15);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Set same value.
        ctx.set(&c, 5);
        assert!(
            ctx.is_set(&s),
            "slot should remain cached when cell value unchanged"
        );
        assert_eq!(ctx.get(&s), 15);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "no recomputation on same-value set"));
    }

    /// SPEC: Set with different value DOES invalidate dependents.
    #[test]
    fn set_different_value_invalidates_dependents() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c) + 100
        });

        assert_eq!(ctx.get(&s), 101);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Set different value.
        ctx.set(&c, 2);
        assert!(
            !ctx.is_set(&s),
            "slot should be cleared after cell value changed"
        );
        assert_eq!(ctx.get(&s), 102);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
    }

    /// SPEC: "Dependents cascade recursively"
    #[test]
    fn cell_invalidation_cascades_recursively() {
        thread_local! {
            static A_COUNT: Cell<u32> = const { Cell::new(0) };
            static B_COUNT: Cell<u32> = const { Cell::new(0) };
            static C_COUNT: Cell<u32> = const { Cell::new(0) };
        }
        A_COUNT.with(|c| c.set(0));
        B_COUNT.with(|c| c.set(0));
        C_COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let root = ctx.source(1i32);
        let a = ctx.slot(move |ctx| {
            A_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&root)
        });
        let b = ctx.slot(move |ctx| {
            B_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&a) * 2
        });
        let c = ctx.slot(move |ctx| {
            C_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&b) * 3
        });

        assert_eq!(ctx.get(&c), 6); // 1 * 2 * 3
        A_COUNT.with(|cnt| assert_eq!(cnt.get(), 1));
        B_COUNT.with(|cnt| assert_eq!(cnt.get(), 1));
        C_COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Change root — all three slots should invalidate and recompute on access.
        ctx.set(&root, 10);
        assert_eq!(ctx.get(&c), 60); // 10 * 2 * 3
        A_COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
        B_COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
        C_COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
    }

    /// Verify PartialEq is used for equality check with strings.
    #[test]
    fn partial_eq_guard_works_with_strings() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let name = ctx.source("alice".to_string());
        let greeting = ctx.slot(move |ctx| {
            COUNT.with(|c| c.set(c.get() + 1));
            format!("hi {}", ctx.get(&name))
        });

        assert_eq!(ctx.get(&greeting), "hi alice");

        // Same value (different allocation, same content).
        ctx.set(&name, "alice".to_string());
        assert!(
            ctx.is_set(&greeting),
            "should not invalidate on equal string"
        );
        COUNT.with(|c| assert_eq!(c.get(), 1));

        // Different value.
        ctx.set(&name, "bob".to_string());
        assert!(!ctx.is_set(&greeting));
        assert_eq!(ctx.get(&greeting), "hi bob");
        COUNT.with(|c| assert_eq!(c.get(), 2));
    }
}

// ============================================================================
// 4. Dependency Tracking
// ============================================================================

mod dependency_tracking {
    use super::*;

    /// SPEC: "Thread-local tracking stack" — nested slot access registers dependency.
    #[test]
    fn nested_slot_access_registers_dependency() {
        thread_local! {
            static INNER_COUNT: Cell<u32> = const { Cell::new(0) };
            static OUTER_COUNT: Cell<u32> = const { Cell::new(0) };
        }
        INNER_COUNT.with(|c| c.set(0));
        OUTER_COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let inner = ctx.slot(move |ctx| {
            INNER_COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c) * 10
        });
        let outer = ctx.slot(move |ctx| {
            OUTER_COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&inner) + 1
        });

        assert_eq!(ctx.get(&outer), 11);

        // Change cell — dependents become dirty until their dependency chain is
        // refreshed.
        ctx.set(&c, 5);
        assert!(!ctx.is_set(&inner));
        assert!(!ctx.is_set(&outer));

        assert_eq!(ctx.get(&outer), 51);
        INNER_COUNT.with(|c| assert_eq!(c.get(), 2));
        OUTER_COUNT.with(|c| assert_eq!(c.get(), 2));
    }

    /// SPEC: "Cell access during slot computation registers dependency"
    #[test]
    fn cell_access_during_computation_registers_dependency() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c1 = ctx.source(10i32);
        let c2 = ctx.source(20i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c1) + ctx.get(&c2)
        });

        assert_eq!(ctx.get(&s), 30);
        COUNT.with(|c| assert_eq!(c.get(), 1));

        // Changing c1 should invalidate s.
        ctx.set(&c1, 100);
        assert!(!ctx.is_set(&s));
        assert_eq!(ctx.get(&s), 120);
        COUNT.with(|c| assert_eq!(c.get(), 2));

        // Changing c2 should also invalidate s.
        ctx.set(&c2, 200);
        assert!(!ctx.is_set(&s));
        assert_eq!(ctx.get(&s), 300);
        COUNT.with(|c| assert_eq!(c.get(), 3));
    }

    /// SPEC: "Dynamic dependency graphs (dependencies can change on recomputation)"
    ///
    /// A slot conditionally reads from different cells based on a flag cell.
    /// When the flag changes, the dependency graph should update.
    #[test]
    fn dynamic_dependency_graph() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let flag = ctx.source(true);
        let a = ctx.source(10i32);
        let b = ctx.source(20i32);

        // When flag is true, depends on a. When false, depends on b.
        let s = ctx.slot(move |ctx| {
            COUNT.with(|c| c.set(c.get() + 1));
            if ctx.get(&flag) {
                ctx.get(&a)
            } else {
                ctx.get(&b)
            }
        });

        // flag=true → reads a=10.
        assert_eq!(ctx.get(&s), 10);
        COUNT.with(|c| assert_eq!(c.get(), 1));

        // Changing b should NOT invalidate s (s doesn't depend on b right now).
        ctx.set(&b, 99);
        assert!(
            ctx.is_set(&s),
            "s should still be cached since it doesn't depend on b"
        );
        COUNT.with(|c| assert_eq!(c.get(), 1));

        // Changing flag to false → s recomputes, now depends on b.
        ctx.set(&flag, false);
        assert!(!ctx.is_set(&s));
        assert_eq!(ctx.get(&s), 99); // b was set to 99
        COUNT.with(|c| assert_eq!(c.get(), 2));

        // Now changing a should NOT invalidate s (dynamic dep changed).
        ctx.set(&a, 999);
        assert!(
            ctx.is_set(&s),
            "s should still be cached since it no longer depends on a"
        );
        COUNT.with(|c| assert_eq!(c.get(), 2));

        // But changing b should invalidate s now.
        ctx.set(&b, 50);
        assert!(!ctx.is_set(&s));
        assert_eq!(ctx.get(&s), 50);
        COUNT.with(|c| assert_eq!(c.get(), 3));
    }
}

// ============================================================================
// 5. Invalidation Semantics
// ============================================================================

mod invalidation_semantics {
    use super::*;

    /// SPEC: `Source::set()` stores the new value and marks dependent slots dirty.
    #[test]
    fn cell_set_clears_dependents_not_self() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.slot(move |ctx| ctx.get(&c));

        assert_eq!(ctx.get(&s), 1);
        ctx.set(&c, 2);

        // Cell has new value immediately.
        assert_eq!(ctx.get(&c), 2);
        // Dependent slot is forced stale.
        assert!(!ctx.is_set(&s));
        // Recomputes with new value.
        assert_eq!(ctx.get(&s), 2);
    }

    /// SPEC: `ctx.set()` marks direct slot dependents stale without hard
    /// clearing downstream memoized values.
    #[test]
    fn slot_clear_cascades() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let a = ctx.slot(move |ctx| ctx.get(&c));
        let b = ctx.slot(move |ctx| ctx.get(&a) + 10);

        assert_eq!(ctx.get(&b), 11);
        assert!(ctx.is_set(&a));
        assert!(ctx.is_set(&b));

        // Changing the cell makes both slots dirty until access proves whether
        // `a` changed.
        ctx.set(&c, 2);
        assert!(!ctx.is_set(&a));
        assert!(!ctx.is_set(&b));
        assert_eq!(ctx.get(&b), 12);
    }

    /// SPEC: "Cleared slots recompute on next get() access" (lazy recomputation).
    #[test]
    fn lazy_recomputation_only_on_access() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c)
        });

        assert_eq!(ctx.get(&s), 1);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Invalidate.
        ctx.set(&c, 2);
        // Count should NOT have increased — no eager recompute.
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "should not recompute eagerly"));

        // Invalidate again without ever accessing.
        ctx.set(&c, 3);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "still should not recompute"));

        // Now access — should recompute once.
        assert_eq!(ctx.get(&s), 3);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 2, "should recompute on access"));
    }

    /// Multiple invalidations without access should only trigger one recompute.
    #[test]
    fn multiple_invalidations_single_recompute() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(0i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c)
        });

        assert_eq!(ctx.get(&s), 0);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Multiple set_cell calls without accessing s.
        ctx.set(&c, 1);
        ctx.set(&c, 2);
        ctx.set(&c, 3);
        ctx.set(&c, 4);
        ctx.set(&c, 5);

        // Only one recompute on access.
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "no recomputes during invalidation"));
        assert_eq!(ctx.get(&s), 5);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 2, "exactly one recompute on access"));
    }

    /// SPEC: If an intermediate slot recomputes equal, downstream dirty slots
    /// become fresh again without recomputing.
    #[test]
    fn equal_intermediate_slot_prevents_downstream_recompute() {
        let ctx = Context::new();
        let root = ctx.source(0i32);
        let parity_computes = Rc::new(RefCell::new(0));
        let parity_computes_for_slot = Rc::clone(&parity_computes);
        let parity = ctx.computed(move |ctx| {
            *parity_computes_for_slot.borrow_mut() += 1;
            ctx.get(&root) % 2
        });
        let downstream_computes = Rc::new(RefCell::new(0));
        let downstream_computes_for_slot = Rc::clone(&downstream_computes);
        let downstream = ctx.slot(move |ctx| {
            *downstream_computes_for_slot.borrow_mut() += 1;
            ctx.get(&parity) * 10
        });

        assert_eq!(ctx.get(&downstream), 0);
        assert_eq!(*parity_computes.borrow(), 1);
        assert_eq!(*downstream_computes.borrow(), 1);

        ctx.set(&root, 2);
        assert!(!ctx.is_set(&parity));
        assert!(!ctx.is_set(&downstream));

        assert_eq!(ctx.get(&downstream), 0);
        assert_eq!(
            *parity_computes.borrow(),
            2,
            "dirty intermediate slot should validate once"
        );
        assert_eq!(
            *downstream_computes.borrow(),
            1,
            "equal intermediate value should keep downstream cache"
        );
        assert!(ctx.is_set(&parity));
        assert!(ctx.is_set(&downstream));
        assert_eq!(
            ctx.get(&downstream),
            0,
            "freshened downstream cache should remain readable without recompute"
        );
        assert_eq!(
            *parity_computes.borrow(),
            2,
            "clean downstream read should not revalidate the unchanged memo"
        );
        assert_eq!(
            *downstream_computes.borrow(),
            1,
            "clean downstream read should keep the preserved cache"
        );

        ctx.set(&root, 3);
        assert_eq!(ctx.get(&downstream), 10);
        assert_eq!(*parity_computes.borrow(), 3);
        assert_eq!(
            *downstream_computes.borrow(),
            2,
            "changed intermediate value should recompute downstream"
        );
    }
}

// ============================================================================
// 6. Effect System
// ============================================================================

mod effect_system {
    use super::*;

    /// SPEC: Effects run immediately and track cell dependencies automatically.
    #[test]
    fn effect_runs_immediately_and_reruns_when_cell_changes() {
        let ctx = Context::new();
        let count = ctx.source(0i32);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_effect = Rc::clone(&seen);

        let effect = ctx.effect(move |ctx| {
            seen_for_effect.borrow_mut().push(ctx.get(&count));
        });

        assert!(effect.is_active(&ctx));
        assert_eq!(*seen.borrow(), vec![0], "effect should run on creation");

        ctx.set(&count, 1);
        assert_eq!(
            *seen.borrow(),
            vec![0, 1],
            "effect should rerun after dependency changes"
        );

        ctx.set(&count, 1);
        assert_eq!(
            *seen.borrow(),
            vec![0, 1],
            "same-value cell set should not schedule the effect"
        );
    }

    /// SPEC: Effects can depend on slots; slot invalidation schedules the effect once.
    #[test]
    fn effect_tracks_slot_dependencies_and_coalesces_scheduling() {
        let ctx = Context::new();
        let root = ctx.source(1i32);
        let left = ctx.slot(move |ctx| ctx.get(&root) + 1);
        let right = ctx.slot(move |ctx| ctx.get(&root) + 2);
        let sum = ctx.slot(move |ctx| ctx.get(&left) + ctx.get(&right));
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_effect = Rc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            seen_for_effect.borrow_mut().push(ctx.get(&sum));
        });

        assert_eq!(*seen.borrow(), vec![5]);

        ctx.set(&root, 10);
        assert_eq!(
            *seen.borrow(),
            vec![5, 23],
            "diamond invalidation should schedule one effect rerun"
        );
    }

    /// SPEC: Scheduled effects skip cleanup/rerun when slot dependencies
    /// validate to the same value.
    #[test]
    fn effect_skips_rerun_when_slot_dependency_recomputes_equal() {
        let ctx = Context::new();
        let root = ctx.source(0i32);
        let parity_computes = Rc::new(RefCell::new(0));
        let parity_computes_for_slot = Rc::clone(&parity_computes);
        let parity = ctx.computed(move |ctx| {
            *parity_computes_for_slot.borrow_mut() += 1;
            ctx.get(&root) % 2
        });
        let label_computes = Rc::new(RefCell::new(0));
        let label_computes_for_slot = Rc::clone(&label_computes);
        let label = ctx.slot(move |ctx| {
            *label_computes_for_slot.borrow_mut() += 1;
            ctx.get(&parity) * 10
        });
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_effect = Rc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            seen_for_effect.borrow_mut().push(ctx.get(&label));
        });

        assert_eq!(*seen.borrow(), vec![0]);
        assert_eq!(*parity_computes.borrow(), 1);
        assert_eq!(*label_computes.borrow(), 1);

        ctx.set(&root, 2);
        assert_eq!(
            *seen.borrow(),
            vec![0],
            "equal slot value should suppress the effect rerun"
        );
        assert_eq!(*parity_computes.borrow(), 2);
        assert_eq!(
            *label_computes.borrow(),
            1,
            "effect validation should not recompute unchanged downstream slot"
        );

        ctx.set(&root, 3);
        assert_eq!(*seen.borrow(), vec![0, 10]);
        assert_eq!(*parity_computes.borrow(), 3);
        assert_eq!(*label_computes.borrow(), 2);
    }

    /// SPEC: Cleanup runs before each rerun and when the effect is disposed.
    #[test]
    fn effect_cleanup_runs_before_rerun_and_on_dispose() {
        let ctx = Context::new();
        let value = ctx.source(0i32);
        let events = Rc::new(RefCell::new(Vec::new()));
        let events_for_effect = Rc::clone(&events);

        let effect = ctx.effect(move |ctx| {
            let current = ctx.get(&value);
            events_for_effect
                .borrow_mut()
                .push(format!("run:{current}"));
            let events_for_cleanup = Rc::clone(&events_for_effect);
            move || {
                events_for_cleanup
                    .borrow_mut()
                    .push(format!("cleanup:{current}"));
            }
        });

        assert_eq!(*events.borrow(), vec!["run:0"]);

        ctx.set(&value, 1);
        assert_eq!(
            *events.borrow(),
            vec!["run:0", "cleanup:0", "run:1"],
            "cleanup from the previous run should execute before rerun"
        );

        effect.dispose(&ctx);
        assert!(!effect.is_active(&ctx));
        assert_eq!(
            *events.borrow(),
            vec!["run:0", "cleanup:0", "run:1", "cleanup:1"],
            "dispose should run the latest cleanup"
        );

        ctx.set(&value, 2);
        assert_eq!(
            *events.borrow(),
            vec!["run:0", "cleanup:0", "run:1", "cleanup:1"],
            "disposed effects should not rerun"
        );
    }

    /// SPEC: Initial effect activation uses the scheduler, so invalidations
    /// triggered during the first run queue a follow-up run instead of
    /// recursively overwriting the latest cleanup.
    #[test]
    fn effect_initial_run_schedules_nested_invalidations_after_cleanup_is_stored() {
        let ctx = Context::new();
        let value = ctx.source(0i32);
        let events = Rc::new(RefCell::new(Vec::new()));
        let events_for_effect = Rc::clone(&events);

        let effect = ctx.effect(move |ctx| {
            let current = ctx.get(&value);
            events_for_effect
                .borrow_mut()
                .push(format!("run:{current}"));
            if current == 0 {
                ctx.set(&value, 1);
            }
            let events_for_cleanup = Rc::clone(&events_for_effect);
            move || {
                events_for_cleanup
                    .borrow_mut()
                    .push(format!("cleanup:{current}"));
            }
        });

        assert_eq!(
            *events.borrow(),
            vec!["run:0", "cleanup:0", "run:1"],
            "nested invalidation should run after the first cleanup is stored"
        );

        effect.dispose(&ctx);
        assert_eq!(
            *events.borrow(),
            vec!["run:0", "cleanup:0", "run:1", "cleanup:1"],
            "dispose should clean up the latest run, not the overwritten first run"
        );
    }

    /// SPEC: Effect dependencies are dynamic and re-discovered on each rerun.
    #[test]
    fn effect_dependencies_are_dynamic() {
        let ctx = Context::new();
        let flag = ctx.source(true);
        let a = ctx.source(10i32);
        let b = ctx.source(20i32);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_effect = Rc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            let value = if ctx.get(&flag) {
                ctx.get(&a)
            } else {
                ctx.get(&b)
            };
            seen_for_effect.borrow_mut().push(value);
        });

        assert_eq!(*seen.borrow(), vec![10]);

        ctx.set(&b, 99);
        assert_eq!(
            *seen.borrow(),
            vec![10],
            "inactive branch should not schedule the effect"
        );

        ctx.set(&flag, false);
        assert_eq!(*seen.borrow(), vec![10, 99]);

        ctx.set(&a, 100);
        assert_eq!(
            *seen.borrow(),
            vec![10, 99],
            "old branch dependency should be unsubscribed"
        );

        ctx.set(&b, 50);
        assert_eq!(*seen.borrow(), vec![10, 99, 50]);
    }
}

// ============================================================================
// 7. Batch Updates
// ============================================================================

mod batch_updates {
    use super::*;

    /// SPEC: Batches defer changed-cell invalidation until the callback exits.
    #[test]
    fn batch_defers_cell_invalidation_until_outermost_exit() {
        let ctx = Context::new();
        let value = ctx.source(0i32);
        let computes = Rc::new(RefCell::new(0));
        let computes_for_slot = Rc::clone(&computes);
        let doubled = ctx.slot(move |ctx| {
            *computes_for_slot.borrow_mut() += 1;
            ctx.get(&value) * 2
        });

        assert_eq!(ctx.get(&doubled), 0);
        assert_eq!(*computes.borrow(), 1);

        ctx.batch(|ctx| {
            ctx.set(&value, 1);
            ctx.set(&value, 2);

            assert_eq!(ctx.get(&value), 2);
            assert!(
                ctx.is_set(&doubled),
                "dependent slot should stay cached while the batch is open"
            );
            assert_eq!(
                ctx.get(&doubled),
                0,
                "dependent slot reads remain pre-batch until invalidation flushes"
            );
        });

        assert!(
            !ctx.is_set(&doubled),
            "batch exit should clear changed-cell dependents"
        );
        assert_eq!(ctx.get(&doubled), 4);
        assert_eq!(
            *computes.borrow(),
            2,
            "slot should recompute once after the batch"
        );
    }

    /// SPEC: Multiple updates in one batch schedule each dependent effect once.
    #[test]
    fn batch_coalesces_effect_reruns() {
        let ctx = Context::new();
        let value = ctx.source(0i32);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_effect = Rc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            seen_for_effect.borrow_mut().push(ctx.get(&value));
        });

        assert_eq!(*seen.borrow(), vec![0]);

        ctx.batch(|ctx| {
            ctx.set(&value, 1);
            ctx.set(&value, 2);
            ctx.set(&value, 3);
            assert_eq!(
                *seen.borrow(),
                vec![0],
                "effect should not rerun before batch exit"
            );
        });

        assert_eq!(
            *seen.borrow(),
            vec![0, 3],
            "effect should rerun once with the final batched value"
        );
    }

    /// SPEC: Nested batches flush only when the outermost batch completes.
    #[test]
    fn nested_batches_flush_only_at_outermost_exit() {
        let ctx = Context::new();
        let value = ctx.source(0i32);
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_for_effect = Rc::clone(&seen);

        let _effect = ctx.effect(move |ctx| {
            seen_for_effect.borrow_mut().push(ctx.get(&value));
        });

        ctx.batch(|ctx| {
            ctx.set(&value, 1);

            ctx.batch(|ctx| {
                ctx.set(&value, 2);
            });

            assert_eq!(
                *seen.borrow(),
                vec![0],
                "inner batch exit should not flush while outer batch is open"
            );
        });

        assert_eq!(*seen.borrow(), vec![0, 2]);
    }

    /// SPEC: Explicit slot clears inside a batch are deferred and flush effects
    /// after cleanup has been preserved.
    #[test]
    fn batch_defers_slot_clear_and_effect_cleanup() {
        let ctx = Context::new();
        let value = ctx.source(2i32);
        let doubled = ctx.slot(move |ctx| ctx.get(&value) * 2);
        let events = Rc::new(RefCell::new(Vec::new()));
        let events_for_effect = Rc::clone(&events);

        let _effect = ctx.effect(move |ctx| {
            let current = ctx.get(&doubled);
            events_for_effect
                .borrow_mut()
                .push(format!("run:{current}"));
            let events_for_cleanup = Rc::clone(&events_for_effect);
            move || {
                events_for_cleanup
                    .borrow_mut()
                    .push(format!("cleanup:{current}"));
            }
        });

        assert_eq!(*events.borrow(), vec!["run:4"]);

        ctx.batch(|ctx| {
            doubled.clear(ctx);
            assert!(
                ctx.is_set(&doubled),
                "slot.clear should defer cache clearing while batched"
            );
            assert_eq!(
                *events.borrow(),
                vec!["run:4"],
                "effect cleanup/rerun should wait for batch exit"
            );
        });

        assert_eq!(
            *events.borrow(),
            vec!["run:4", "cleanup:4", "run:4"],
            "batch exit should clear the slot and rerun dependents once"
        );
    }

    /// SPEC: Explicit cell dependent clears inside a batch are deferred.
    #[test]
    fn batch_defers_cell_clear_dependents() {
        let ctx = Context::new();
        let value = ctx.source(2i32);
        let computes = Rc::new(RefCell::new(0));
        let computes_for_slot = Rc::clone(&computes);
        let doubled = ctx.slot(move |ctx| {
            *computes_for_slot.borrow_mut() += 1;
            ctx.get(&value) * 2
        });

        assert_eq!(ctx.get(&doubled), 4);
        assert_eq!(*computes.borrow(), 1);

        ctx.batch(|ctx| {
            value.clear_dependents(ctx);
            assert!(
                ctx.is_set(&doubled),
                "cell.clear_dependents should defer dependent clearing while batched"
            );
            assert_eq!(ctx.get(&doubled), 4);
            assert_eq!(
                *computes.borrow(),
                1,
                "dependent slot should not recompute before batch exit"
            );
        });

        assert!(
            !ctx.is_set(&doubled),
            "batch exit should clear explicit cell dependents"
        );
        assert_eq!(ctx.get(&doubled), 4);
        assert_eq!(
            *computes.borrow(),
            2,
            "dependent slot should recompute once after batch exit"
        );
    }

    /// SPEC: Batched cell updates combined with explicit clears still hard-clear transitive slots.
    #[test]
    fn batch_cell_set_plus_clear_dependents_hard_clears_transitive_slots() {
        let ctx = Context::new();
        let value = ctx.source(2i32);
        let doubled = ctx.slot(move |ctx| ctx.get(&value) * 2);
        let label = ctx.slot(move |ctx| format!("value:{}", ctx.get(&doubled)));

        assert_eq!(ctx.get(&label), "value:4");
        assert!(ctx.is_set(&doubled));
        assert!(ctx.is_set(&label));

        ctx.batch(|ctx| {
            ctx.set(&value, 3);
            value.clear_dependents(ctx);
            assert!(ctx.is_set(&doubled));
            assert!(ctx.is_set(&label));
        });

        assert!(!ctx.is_set(&doubled));
        assert!(!ctx.is_set(&label));
        assert_eq!(ctx.get(&label), "value:6");
    }
}

// ============================================================================
// 8. Edge Cases
// ============================================================================

mod edge_cases {
    use super::*;

    /// Slot with no dependencies (pure constant).
    #[test]
    fn slot_with_no_dependencies() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let s = ctx.slot(|_| {
            COUNT.with(|c| c.set(c.get() + 1));
            "constant"
        });

        assert_eq!(ctx.get(&s), "constant");
        assert_eq!(ctx.get(&s), "constant");
        COUNT.with(|c| assert_eq!(c.get(), 1, "pure slot computes only once"));
    }

    /// Multiple slots depending on the same cell.
    #[test]
    fn multiple_slots_sharing_same_cell() {
        thread_local! {
            static A_COUNT: Cell<u32> = const { Cell::new(0) };
            static B_COUNT: Cell<u32> = const { Cell::new(0) };
            static C_COUNT: Cell<u32> = const { Cell::new(0) };
        }
        A_COUNT.with(|c| c.set(0));
        B_COUNT.with(|c| c.set(0));
        C_COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let base = ctx.source(10i32);

        let a = ctx.slot(move |ctx| {
            A_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&base) + 1
        });
        let b = ctx.slot(move |ctx| {
            B_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&base) + 2
        });
        let c = ctx.slot(move |ctx| {
            C_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&base) + 3
        });

        assert_eq!(ctx.get(&a), 11);
        assert_eq!(ctx.get(&b), 12);
        assert_eq!(ctx.get(&c), 13);

        // Change base — all three should invalidate.
        ctx.set(&base, 100);
        assert!(!ctx.is_set(&a));
        assert!(!ctx.is_set(&b));
        assert!(!ctx.is_set(&c));

        assert_eq!(ctx.get(&a), 101);
        assert_eq!(ctx.get(&b), 102);
        assert_eq!(ctx.get(&c), 103);

        A_COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
        B_COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
        C_COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
    }

    /// Deep dependency chain (6 levels: cell → s1 → s2 → s3 → s4 → s5).
    #[test]
    fn deep_dependency_chain() {
        thread_local! {
            static COUNTS: Cell<[u32; 5]> = const { Cell::new([0; 5]) };
        }
        COUNTS.with(|c| c.set([0; 5]));

        let ctx = Context::new();
        let root = ctx.source(1i32);

        let s1 = ctx.slot(move |ctx| {
            COUNTS.with(|c| {
                let mut v = c.get();
                v[0] += 1;
                c.set(v);
            });
            ctx.get(&root)
        });
        let s2 = ctx.slot(move |ctx| {
            COUNTS.with(|c| {
                let mut v = c.get();
                v[1] += 1;
                c.set(v);
            });
            ctx.get(&s1) + 1
        });
        let s3 = ctx.slot(move |ctx| {
            COUNTS.with(|c| {
                let mut v = c.get();
                v[2] += 1;
                c.set(v);
            });
            ctx.get(&s2) + 1
        });
        let s4 = ctx.slot(move |ctx| {
            COUNTS.with(|c| {
                let mut v = c.get();
                v[3] += 1;
                c.set(v);
            });
            ctx.get(&s3) + 1
        });
        let s5 = ctx.slot(move |ctx| {
            COUNTS.with(|c| {
                let mut v = c.get();
                v[4] += 1;
                c.set(v);
            });
            ctx.get(&s4) + 1
        });

        // root=1, s1=1, s2=2, s3=3, s4=4, s5=5
        assert_eq!(ctx.get(&s5), 5);
        COUNTS.with(|c| assert_eq!(c.get(), [1, 1, 1, 1, 1], "each slot computed once"));

        // Change root.
        ctx.set(&root, 100);
        // The chain is dirty until access proves each previous layer changed.
        assert!(!ctx.is_set(&s1));
        assert!(!ctx.is_set(&s2));
        assert!(!ctx.is_set(&s3));
        assert!(!ctx.is_set(&s4));
        assert!(!ctx.is_set(&s5));

        // root=100, s1=100, s2=101, s3=102, s4=103, s5=104
        assert_eq!(ctx.get(&s5), 104);
        COUNTS.with(|c| assert_eq!(c.get(), [2, 2, 2, 2, 2], "each slot recomputed once"));
    }

    /// Slot that reads from multiple cells.
    #[test]
    fn slot_reads_multiple_cells() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let a = ctx.source(1i32);
        let b = ctx.source(2i32);
        let c = ctx.source(3i32);

        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&a) + ctx.get(&b) + ctx.get(&c)
        });

        assert_eq!(ctx.get(&s), 6);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Change any one cell — slot invalidates.
        ctx.set(&b, 20);
        assert!(!ctx.is_set(&s));
        assert_eq!(ctx.get(&s), 24); // 1 + 20 + 3
        COUNT.with(|cnt| assert_eq!(cnt.get(), 2));
    }

    /// Re-access a slot after its dependency changed and was recomputed.
    /// Verifies the dependency graph is correctly re-established after recompute.
    #[test]
    fn re_access_after_recompute_re_establishes_deps() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c) * 10
        });

        // First cycle.
        assert_eq!(ctx.get(&s), 10);
        ctx.set(&c, 2);
        assert_eq!(ctx.get(&s), 20);

        // Second cycle — deps should still work.
        ctx.set(&c, 3);
        assert!(
            !ctx.is_set(&s),
            "dep should still be tracked after recompute"
        );
        assert_eq!(ctx.get(&s), 30);

        // Third cycle.
        ctx.set(&c, 4);
        assert_eq!(ctx.get(&s), 40);

        COUNT.with(|cnt| assert_eq!(cnt.get(), 4, "should compute exactly 4 times"));
    }

    /// Diamond dependency: cell → (a, b) → d. Changing cell marks both
    /// branches and their downstream dependent dirty.
    #[test]
    fn diamond_dependency_both_branches_cleared() {
        thread_local! {
            static D_COUNT: Cell<u32> = const { Cell::new(0) };
        }
        D_COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let root = ctx.source(1i32);
        let a = ctx.slot(move |ctx| ctx.get(&root) + 1);
        let b = ctx.slot(move |ctx| ctx.get(&root) + 2);
        let d = ctx.slot(move |ctx| {
            D_COUNT.with(|c| c.set(c.get() + 1));
            ctx.get(&a) + ctx.get(&b)
        });

        assert_eq!(ctx.get(&d), 5); // (1+1) + (1+2) = 5
        D_COUNT.with(|c| assert_eq!(c.get(), 1));

        ctx.set(&root, 10);
        assert!(!ctx.is_set(&a));
        assert!(!ctx.is_set(&b));
        assert!(!ctx.is_set(&d));

        assert_eq!(ctx.get(&d), 23); // (10+1) + (10+2) = 23
        D_COUNT.with(|c| assert_eq!(c.get(), 2));
    }

    /// Accessing only a leaf slot triggers computation of entire chain.
    #[test]
    fn accessing_leaf_triggers_full_chain_computation() {
        thread_local! {
            static A_COUNT: Cell<u32> = const { Cell::new(0) };
            static B_COUNT: Cell<u32> = const { Cell::new(0) };
        }
        A_COUNT.with(|c| c.set(0));
        B_COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let a = ctx.slot(move |ctx| {
            A_COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c)
        });
        let b = ctx.slot(move |ctx| {
            B_COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&a) + 10
        });

        // Only access b (never directly access a).
        assert_eq!(ctx.get(&b), 11);
        A_COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "a computed as part of b's chain"));
        B_COUNT.with(|cnt| assert_eq!(cnt.get(), 1));
    }

    /// Setting a cell multiple times before any slot access.
    #[test]
    fn set_cell_multiple_times_before_first_slot_access() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(0i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c)
        });

        // Set cell multiple times before ever accessing the slot.
        ctx.set(&c, 1);
        ctx.set(&c, 2);
        ctx.set(&c, 3);

        // First access should see latest value and compute only once.
        assert_eq!(ctx.get(&s), 3);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "should compute only once on first access"));
    }

    /// A cleared slot that is never re-accessed should not recompute.
    #[test]
    fn cleared_slot_never_reaccessed_does_not_recompute() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c)
        });

        assert_eq!(ctx.get(&s), 1);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        // Invalidate but never re-access.
        ctx.set(&c, 2);
        ctx.set(&c, 3);
        ctx.set(&c, 4);

        // Compute count should still be 1.
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1, "no recompute without access"));
    }

    /// Computed::clear removes cached value and cascades to dependents.
    #[test]
    fn slot_handle_clear_cascades() {
        let ctx = Context::new();
        let a = ctx.slot(|_| 42);
        let b = ctx.slot(move |ctx| ctx.get(&a) + 1);
        let c = ctx.slot(move |ctx| ctx.get(&b) + 1);

        assert_eq!(ctx.get(&c), 44);
        assert!(ctx.is_set(&a));
        assert!(ctx.is_set(&b));
        assert!(ctx.is_set(&c));

        a.clear(&ctx);
        assert!(!ctx.is_set(&a), "cleared slot should be unset");
        assert!(!ctx.is_set(&b), "dependent should cascade-clear");
        assert!(!ctx.is_set(&c), "transitive dependent should cascade-clear");

        assert_eq!(ctx.get(&c), 44);
    }

    /// Computed::clear on an already-cleared slot is a no-op.
    #[test]
    fn slot_handle_clear_idempotent() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let s = ctx.slot(|_| {
            COUNT.with(|c| c.set(c.get() + 1));
            42
        });

        assert_eq!(ctx.get(&s), 42);
        COUNT.with(|c| assert_eq!(c.get(), 1));

        s.clear(&ctx);
        s.clear(&ctx);
        s.clear(&ctx);

        assert_eq!(ctx.get(&s), 42);
        COUNT.with(|c| assert_eq!(c.get(), 2, "only one recompute after multiple clears"));
    }

    /// Computed::clear on a slot that was never accessed is a no-op.
    #[test]
    fn slot_handle_clear_on_unset_slot() {
        let ctx = Context::new();
        let s = ctx.slot(|_| 42);
        assert!(!ctx.is_set(&s));
        s.clear(&ctx);
        assert!(!ctx.is_set(&s));
        assert_eq!(ctx.get(&s), 42);
    }

    /// Source::clear_dependents clears downstream slots without changing the cell value.
    #[test]
    fn cell_handle_clear_dependents() {
        thread_local! {
            static COUNT: Cell<u32> = const { Cell::new(0) };
        }
        COUNT.with(|c| c.set(0));

        let ctx = Context::new();
        let c = ctx.source(10i32);
        let s = ctx.slot(move |ctx| {
            COUNT.with(|cnt| cnt.set(cnt.get() + 1));
            ctx.get(&c) * 2
        });

        assert_eq!(ctx.get(&s), 20);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 1));

        c.clear_dependents(&ctx);
        assert!(!ctx.is_set(&s), "slot should be cleared");
        assert_eq!(ctx.get(&c), 10, "cell value unchanged");

        assert_eq!(ctx.get(&s), 20);
        COUNT.with(|cnt| assert_eq!(cnt.get(), 2, "slot recomputed after clear_dependents"));
    }

    /// Source::clear_dependents cascades through transitive dependents.
    #[test]
    fn cell_handle_clear_dependents_cascades() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let a = ctx.slot(move |ctx| ctx.get(&c) + 1);
        let b = ctx.slot(move |ctx| ctx.get(&a) + 10);
        let d = ctx.slot(move |ctx| ctx.get(&b) + 100);

        assert_eq!(ctx.get(&d), 112);
        assert!(ctx.is_set(&a));
        assert!(ctx.is_set(&b));
        assert!(ctx.is_set(&d));

        c.clear_dependents(&ctx);
        assert!(!ctx.is_set(&a));
        assert!(!ctx.is_set(&b));
        assert!(!ctx.is_set(&d));
        assert_eq!(ctx.get(&c), 1, "cell value unchanged");

        assert_eq!(ctx.get(&d), 112);
    }

    /// Source::set updates the cell through its owning context.
    #[test]
    fn cell_handle_set_updates_and_invalidates_dependents() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let doubled = ctx.slot(move |ctx| ctx.get(&c) * 2);

        assert_eq!(ctx.get(&doubled), 2);
        c.set(&ctx, 21);

        assert!(!ctx.is_set(&doubled));
        assert_eq!(ctx.get(&c), 21);
        assert_eq!(ctx.get(&doubled), 42);
    }

    /// Slot handles are Copy — copies refer to the same underlying slot.
    #[test]
    fn slot_handle_copy_refers_to_same_slot() {
        let ctx = Context::new();
        let c = ctx.source(5i32);
        let s = ctx.slot(move |ctx| ctx.get(&c) * 2);
        let s_copy = s;

        assert_eq!(ctx.get(&s), 10);
        assert_eq!(ctx.get(&s_copy), 10);

        ctx.set(&c, 7);
        assert_eq!(ctx.get(&s), 14);
        assert_eq!(ctx.get(&s_copy), 14);
    }

    /// Cell handles are Copy — copies refer to the same underlying cell.
    #[test]
    fn cell_handle_copy_refers_to_same_cell() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let c_copy = c;

        ctx.set(&c, 42);
        assert_eq!(ctx.get(&c_copy), 42);
    }

    /// Slots can produce non-numeric types (Vec, struct, etc.).
    #[test]
    fn slot_with_vec_type() {
        let ctx = Context::new();
        let size = ctx.source(3usize);
        let v = ctx.slot(move |ctx| {
            let n = ctx.get(&size);
            (0..n).collect::<Vec<usize>>()
        });

        assert_eq!(ctx.get(&v), vec![0, 1, 2]);
        ctx.set(&size, 5);
        assert_eq!(ctx.get(&v), vec![0, 1, 2, 3, 4]);
    }
}

// ============================================================================
// Handle-centric get methods
// ============================================================================

mod handle_get_methods {
    use super::*;

    #[test]
    fn cell_handle_get_returns_initial_value() {
        let ctx = Context::new();
        let c = ctx.source(42i32);
        assert_eq!(c.get(&ctx), 42);
    }

    #[test]
    fn cell_handle_get_matches_context_get_cell() {
        let ctx = Context::new();
        let c = ctx.source(99i32);
        assert_eq!(c.get(&ctx), ctx.get(&c));
    }

    #[test]
    fn cell_handle_get_tracks_dependencies() {
        let ctx = Context::new();
        let c = ctx.source(10i32);
        let s = ctx.computed(move |ctx| c.get(ctx) * 2);
        assert_eq!(s.get(&ctx), 20);
        c.set(&ctx, 5);
        assert_eq!(s.get(&ctx), 10);
    }

    #[test]
    fn slot_handle_get_returns_computed_value() {
        let ctx = Context::new();
        let s = ctx.computed(|_| 7i32);
        assert_eq!(s.get(&ctx), 7);
    }

    #[test]
    fn slot_handle_get_matches_context_get() {
        let ctx = Context::new();
        let s = ctx.computed(|_| 123i32);
        assert_eq!(s.get(&ctx), ctx.get(&s));
    }

    #[test]
    fn slot_handle_get_lazy_recomputation() {
        let ctx = Context::new();
        let c = ctx.source(1i32);
        let s = ctx.computed(move |ctx| c.get(ctx) + 10);
        assert_eq!(s.get(&ctx), 11);
        c.set(&ctx, 5);
        assert_eq!(s.get(&ctx), 15);
    }

    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_cell_get_cell_still_works() {
        let ctx = ThreadSafeContext::new();
        let c = ctx.source(42i32);
        assert_eq!(ctx.get(&c), 42);
    }

    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_slot_get_still_works() {
        let ctx = ThreadSafeContext::new();
        let c = ctx.source(10i32);
        let s = ctx.computed(move |ctx| ctx.get(&c) * 2);
        assert_eq!(ctx.get(&s), 20);
    }
}

// ============================================================================
// 9. Cross-language channel compatibility
// ============================================================================

mod cross_language_channel_compatibility_spec {
    const README: &str = include_str!("../README.md");
    const SPEC: &str = include_str!("../SPEC.md");

    fn assert_spec_contains(fragment: &str) {
        assert!(
            SPEC.contains(fragment),
            "SPEC.md should document channel compatibility element: {fragment}"
        );
    }

    #[test]
    fn spec_documents_ffi_as_abi_adapter_not_a_second_graph_model() {
        for fragment in [
            "Cross-language channel compatibility (FFI / IPC / WebSocket / WebRTC data)",
            "Yes: lazily-rs has a viable FFI strategy",
            "adapter around the same transport-agnostic state plane",
            "should not expose the closure-based Rust `Context`",
            "No Rust references, trait objects, closures, or typed handles cross the boundary",
            "The `ffi` feature exports `extern \"C\"` functions",
            "The implemented channel is a local ABI adapter",
            "stores the decoded message",
            "re-encodes with the requested",
            "must be caught before crossing the C ABI",
            "`type_tag` + payload registry",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_one_message_plane_for_all_channels() {
        for fragment in [
            "one canonical message plane",
            "`IpcMessage::Snapshot` and `IpcMessage::Delta` are the graph-state payloads",
            "`NodeId`, `PeerId`, `RemoteOp`, `Snapshot`, `Delta`, and `DeltaOp`",
            "wire-facing contract",
            "typed handles remain local",
            "differ only in framing",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_channel_reliability_and_negotiation_requirements() {
        for fragment in [
            "Reliable ordered data channels carry the same serialized `IpcMessage`s",
            "`Delta`s need ordered reliable delivery or receiver-side gap detection and snapshot resync",
            "protocol id: `lazily-ipc`",
            "protocol major version",
            "maximum frame size and fragmentation support",
            "ordered/reliable delivery guarantee",
            "fail closed before applying",
            "Permission filtering happens before serialization on every channel",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn readme_summarizes_cross_channel_strategy() {
        for fragment in [
            "Cross-Channel Compatibility",
            "`IpcMessage::Snapshot` and `IpcMessage::Delta`",
            "C ABI adapter with opaque handles and owned byte buffers",
            "IPC, WebSocket frames, WebRTC data channels, and FFI byte buffers",
            "Enable the `ffi` feature for the C ABI adapter",
            "`LazilyFfiChannel`",
            "`LazilyFfiBytes`",
            "Transport code owns framing",
            "memory ownership, reliability, and back-pressure",
        ] {
            assert!(
                README.contains(fragment),
                "README.md should summarize channel compatibility element: {fragment}"
            );
        }
    }
}

// ============================================================================
// 10. AsyncContext Design Spec
// ============================================================================

mod async_context_design_spec {
    const SPEC: &str = include_str!("../SPEC.md");

    fn assert_spec_contains(fragment: &str) {
        assert!(
            SPEC.contains(fragment),
            "SPEC.md should document AsyncContext design element: {fragment}"
        );
    }

    #[test]
    fn spec_documents_async_context_overview() {
        for fragment in [
            "### AsyncContext",
            "explicit async context surface",
            "not an overload of",
            "`async` feature flag so downstream users",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_context_type_definitions() {
        for fragment in [
            "pub struct AsyncContext",
            "pub struct AsyncComputed<T>",
            "pub struct AsyncSource<T>",
            "pub struct AsyncEffectHandle",
            "pub struct AsyncComputeContext",
            "AsyncContextInner",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_context_api_surface() {
        for fragment in [
            "computed_async",
            "get_async",
            "memo_async",
            "effect_async",
            "dispose_async_effect",
            "fn source<T>(&self, value: T) -> AsyncSource<T>",
            "fn get<T>(&self, handle: &AsyncSource<T>) -> T",
            "fn set<T>(&self, handle: &AsyncSource<T>, value: T)",
            "fn batch<F, R>(&self, run: F) -> R",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_context_api_bounds() {
        for fragment in [
            "T: PartialEq + Clone + Send + Sync + 'static",
            "Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static",
            "Future<Output = T> + Send + 'static",
            "Future<Output = Option<C>> + Send + 'static",
            "FnOnce() -> CleanupFut + Send + 'static",
            "Future<Output = ()> + Send + 'static",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_slot_state_machine() {
        for fragment in [
            "AsyncSlotState",
            "Empty",
            "Computing",
            "Resolved",
            "Error",
            "JoinHandle",
            "revision",
            "State transitions",
            "Empty → Computing",
            "Computing → Resolved",
            "Computing → Error",
            "Computing → Computing",
            "Resolved → Computing",
            "Error → Computing",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_cancellation_contract() {
        for fragment in [
            "cancellation contract",
            "Waiter cancellation is safe",
            "dropping one `get_async` future does not",
            "Stale completion handling",
            "revision no longer matches and discards the result",
            "Explicit cancellation",
            "cancellation-safe",
            "Context disposal",
            "JoinHandle::abort()",
            "Effect cleanup futures",
            "Disposal removes pending reruns before awaiting cleanup",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_dependency_tracking() {
        for fragment in [
            "do not use thread-local tracking stacks",
            "AsyncComputeContext",
            "pub async fn get_async<T>(&self, handle: &AsyncComputed<T>) -> T",
            "pub fn get<T>(&self, handle: &AsyncSource<T>) -> T",
            "records the accessed slot as a dependency",
            "records the accessed cell as a dependency",
            "Async reads register the graph edge immediately",
            "publishes stale data",
            "HashSet<SlotId>",
            "survives executor thread migration",
            "suspension/resume across",
            "`.await` points",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_effects() {
        for fragment in [
            "Serialized reruns",
            "async effect reruns are serialized per effect",
            "Cleanup ordering",
            "cleanup future from the previous run completes before",
            "Auto-tracking",
            "Dependency invalidation",
            "schedules an async rerun after the current",
            "invalidation pass. The rerun is spawned on the runtime executor",
            "Effect disposal",
            "removes pending scheduled reruns",
            "unsubscribes dependency edges",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_batch_support() {
        for fragment in [
            "synchronous boundary",
            "Cell updates queue invalidation",
            "Async slots",
            "and effects are scheduled for rerun but do not execute inside the batch",
            "after the batch returns, on the runtime executor",
            "invalidations schedule async reruns only after the outermost batch exits",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_feature_flag() {
        for fragment in [
            "async = [\"dep:tokio\"]",
            "depends on Tokio",
            "separate from the `tokio` feature",
            "The `async` feature implies",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_implementation_notes() {
        for fragment in [
            "Async graph locks must never be held while polling user futures",
            "Nested async slot reads register dependencies",
            "separate `async` feature flag",
            "requires `Send + Sync + 'static` values, callbacks",
            "futures, and cleanup futures",
            "LocalAsyncContext",
            "LocalSet",
            "handles must not be interchangeable",
            "in-flight computation for the current slot revision",
            "at most one in-flight computation",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_stale_completion() {
        for fragment in [
            "stale completion",
            "dependency invalidation advances the slot",
            "recorded revision no longer matches",
            "discarded",
            "newly spawned future",
        ] {
            assert_spec_contains(fragment);
        }
    }

    #[test]
    fn spec_documents_async_race_stress_harness() {
        for fragment in [
            "Async race stress coverage",
            "`get_async` waiter cancellation",
            "in-flight completion after dependency invalidation",
            "replacement across awaited slot reads",
            "async effect cleanup-before-rerun",
            "tests/async_stress.rs",
        ] {
            assert_spec_contains(fragment);
        }
    }
}
