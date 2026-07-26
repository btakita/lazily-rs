#![cfg(feature = "async")]

use lazily::{AsyncComputed, AsyncContext, AsyncEffectHandle, AsyncSource};
use std::sync::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[tokio::test]
async fn async_context_public_api_cell_round_trip() {
    let ctx = AsyncContext::new();
    let cell: AsyncSource<i32> = ctx.source(42);
    assert_eq!(ctx.get(&cell), 42);
    ctx.set(&cell, 99);
    assert_eq!(ctx.get(&cell), 99);
}

#[tokio::test]
async fn async_context_computed_async_resolves() {
    let ctx = AsyncContext::new();
    let slot: AsyncComputed<i32> = ctx.computed_async(|_ctx| async { 7 });
    let val = ctx.get_async(&slot).await;
    assert_eq!(val, 7);
}

#[tokio::test]
async fn async_context_memo_async_deduplicates_equal() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(5i32);
    let invocations = Arc::new(AtomicU64::new(0));
    let inv_clone = invocations.clone();
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        let c = inv_clone.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            v % 2
        }
    });
    assert_eq!(ctx.get_async(&slot).await, 1);
    ctx.set(&cell, 7);
    assert_eq!(ctx.get_async(&slot).await, 1);
    assert_eq!(invocations.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn async_context_batch_defers_and_applies() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(1i32);
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v + 100 }
    });
    assert_eq!(ctx.get_async(&slot).await, 101);
    ctx.batch(|ctx| {
        ctx.set(&cell, 2);
        ctx.set(&cell, 3);
    });
    assert_eq!(ctx.get_async(&slot).await, 103);
}

#[tokio::test]
async fn async_context_effect_async_lifecycle() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(0i32);
    let observations = Arc::new(Mutex::new(Vec::new()));
    let obs_clone = observations.clone();
    let handle: AsyncEffectHandle = ctx.effect_async(move |ctx| {
        let v = ctx.get(&cell);
        let o = obs_clone.clone();
        async move {
            o.lock().unwrap().push(v);
            None::<fn()>
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(*observations.lock().unwrap(), vec![0]);
    ctx.set(&cell, 1);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(*observations.lock().unwrap(), vec![0, 1]);
    ctx.dispose_async_effect(&handle);
    ctx.set(&cell, 2);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(*observations.lock().unwrap(), vec![0, 1]);
}

#[tokio::test]
async fn async_context_effect_async_cleanup_before_rerun() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(10i32);
    let cleanup_ran = Arc::new(AtomicBool::new(false));
    let cleanup_clone = cleanup_ran.clone();
    ctx.effect_async(move |ctx| {
        let _v = ctx.get(&cell);
        let c = cleanup_clone.clone();
        async move {
            Some(move || {
                c.store(true, Ordering::Relaxed);
            })
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!cleanup_ran.load(Ordering::Relaxed));
    ctx.set(&cell, 20);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(cleanup_ran.load(Ordering::Relaxed));
}

#[tokio::test]
async fn async_context_effect_async_cleanup_runs_on_dispose() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(0i32);
    let cleanup_ran = Arc::new(AtomicBool::new(false));
    let cleanup_clone = cleanup_ran.clone();
    let handle: AsyncEffectHandle = ctx.effect_async(move |ctx| {
        let _v = ctx.get(&cell);
        let c = cleanup_clone.clone();
        async move {
            Some(move || {
                c.store(true, Ordering::Relaxed);
            })
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!cleanup_ran.load(Ordering::Relaxed));
    ctx.dispose_async_effect(&handle);
    assert!(
        cleanup_ran.load(Ordering::Relaxed),
        "dispose_async_effect must run the effect's stored cleanup (SPEC: 'Dispose async effect and await cleanup')"
    );
}

#[tokio::test]
async fn async_context_dispose_aborts_in_flight_effect_rerun() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(0i32);

    let park = Arc::new(tokio::sync::Notify::new());
    let a_cleanup = Arc::new(AtomicU64::new(0));
    let park_for_a = park.clone();
    let a_cleanup_for_a = a_cleanup.clone();
    let handle_a: AsyncEffectHandle = ctx.effect_async(move |ctx| {
        let _ = ctx.get(&cell);
        let p = park_for_a.clone();
        let c = a_cleanup_for_a.clone();
        async move {
            p.notified().await;
            Some(move || {
                c.fetch_add(1, Ordering::Relaxed);
            })
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    ctx.dispose_async_effect(&handle_a);

    let b_ran = Arc::new(AtomicU64::new(0));
    let b_ran_for_b = b_ran.clone();
    let handle_b: AsyncEffectHandle = ctx.effect_async(move |ctx| {
        let _ = ctx.get(&cell);
        let c = b_ran_for_b.clone();
        async move {
            Some(move || {
                c.fetch_add(1, Ordering::Relaxed);
            })
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    park.notify_one();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    ctx.dispose_async_effect(&handle_b);
    assert_eq!(
        b_ran.load(Ordering::Relaxed),
        1,
        "B's cleanup must run exactly once; A's aborted in-flight task must not have overwritten B's node"
    );
    assert_eq!(
        a_cleanup.load(Ordering::Relaxed),
        0,
        "A's aborted in-flight run must not commit a cleanup into the recycled-id node"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_context_concurrent_reads_dedup() {
    let ctx = Arc::new(AsyncContext::new());
    let compute_count = Arc::new(AtomicU64::new(0));
    let count_clone = compute_count.clone();
    let slot = ctx.computed_async(move |_| {
        let c = count_clone.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            42i32
        }
    });
    let ctx1 = ctx.clone();
    let ctx2 = ctx.clone();
    let h1 = tokio::spawn(async move { ctx1.get_async(&slot).await });
    let h2 = tokio::spawn(async move { ctx2.get_async(&slot).await });
    let (v1, v2) = tokio::join!(h1, h2);
    assert_eq!(v1.unwrap(), 42);
    assert_eq!(v2.unwrap(), 42);
    assert_eq!(compute_count.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_context_invalidation_aborts_in_flight_across_tasks() {
    let ctx = Arc::new(AsyncContext::new());
    let cell = ctx.source(1i32);
    let compute_count = Arc::new(AtomicU64::new(0));
    let count_clone = compute_count.clone();
    let slot = ctx.computed_async(move |ctx| {
        let _v = ctx.get(&cell);
        let c = count_clone.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            99i32
        }
    });
    let ctx_reader = ctx.clone();
    let reader = tokio::spawn(async move { ctx_reader.get_async(&slot).await });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    ctx.set(&cell, 2);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let val = ctx.get_async(&slot).await;
    assert_eq!(val, 99);
    let _ = reader.await;
}

#[tokio::test]
async fn async_context_chain_propagation() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(1i32);
    let a = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v * 10 }
    });
    let b = ctx.computed_async(move |ctx| {
        let ah = a;
        async move {
            let v = ctx.get_async(&ah).await;
            v + 5
        }
    });
    let c = ctx.computed_async(move |ctx| {
        let bh = b;
        async move {
            let v = ctx.get_async(&bh).await;
            v * 2
        }
    });
    assert_eq!(ctx.get_async(&c).await, 30);
    ctx.set(&cell, 2);
    assert_eq!(ctx.get_async(&c).await, 50);
}

#[tokio::test]
async fn async_context_memo_blocks_downstream_on_equal() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(2i32);
    let inner_invocations = Arc::new(AtomicU64::new(0));
    let inner_clone = inner_invocations.clone();
    let memo_slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        let c = inner_clone.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            v.abs()
        }
    });
    let outer_invocations = Arc::new(AtomicU64::new(0));
    let outer_clone = outer_invocations.clone();
    let derived = ctx.computed_async(move |ctx| {
        let mh = memo_slot;
        let c = outer_clone.clone();
        async move {
            let v = ctx.get_async(&mh).await;
            c.fetch_add(1, Ordering::Relaxed);
            v + 1
        }
    });
    assert_eq!(ctx.get_async(&derived).await, 3);
    ctx.set(&cell, -2);
    assert_eq!(ctx.get_async(&derived).await, 3);
    assert_eq!(inner_invocations.load(Ordering::Relaxed), 2);
    assert_eq!(
        outer_invocations.load(Ordering::Relaxed),
        2,
        "async memo does not suppress downstream propagation"
    );
}

#[tokio::test]
async fn async_context_dynamic_dependency_switch() {
    let ctx = AsyncContext::new();
    let cell_a = ctx.source(10i32);
    let cell_b = ctx.source(20i32);
    let flag = ctx.source(true);
    let slot = ctx.computed_async(move |ctx| {
        let f = ctx.get(&flag);
        let v = if f {
            ctx.get(&cell_a)
        } else {
            ctx.get(&cell_b)
        };
        async move { v }
    });
    assert_eq!(ctx.get_async(&slot).await, 10);
    ctx.set(&flag, false);
    assert_eq!(ctx.get_async(&slot).await, 20);
    ctx.set(&cell_a, 99);
    let val = ctx.get_async(&slot).await;
    assert_eq!(val, 20, "cell_a change should not propagate after switch");
}

#[tokio::test]
async fn async_context_cell_noop_set_no_invalidation() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(5i32);
    let compute_count = Arc::new(AtomicU64::new(0));
    let count_clone = compute_count.clone();
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        let c = count_clone.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            v * 2
        }
    });
    assert_eq!(ctx.get_async(&slot).await, 10);
    ctx.set(&cell, 5);
    assert_eq!(ctx.get_async(&slot).await, 10);
    assert_eq!(compute_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn async_context_batch_multiple_cells() {
    let ctx = AsyncContext::new();
    let cell_x = ctx.source(1i32);
    let cell_y = ctx.source(10i32);
    let slot = ctx.computed_async(move |ctx| {
        let x = ctx.get(&cell_x);
        let y = ctx.get(&cell_y);
        async move { x + y }
    });
    assert_eq!(ctx.get_async(&slot).await, 11);
    ctx.batch(|ctx| {
        ctx.set(&cell_x, 5);
        ctx.set(&cell_y, 50);
    });
    assert_eq!(ctx.get_async(&slot).await, 55);
}

#[tokio::test]
async fn async_context_dispose_effect_prevents_rerun() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(1i32);
    let run_count = Arc::new(AtomicU64::new(0));
    let count_clone = run_count.clone();
    let effect = ctx.effect_async(move |ctx| {
        let _v = ctx.get(&cell);
        let c = count_clone.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            None::<fn()>
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let after_create = run_count.load(Ordering::Relaxed);
    assert!(after_create >= 1);
    ctx.dispose_async_effect(&effect);
    for _ in 0..3 {
        ctx.set(&cell, 2);
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        run_count.load(Ordering::Relaxed),
        after_create,
        "no reruns after dispose"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_context_shared_across_tokio_tasks() {
    let ctx = Arc::new(AsyncContext::new());
    let cell = ctx.source(0i32);
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v * 3 }
    });
    let _ = ctx.get_async(&slot).await;
    let ctx1 = ctx.clone();
    let reader = tokio::spawn(async move { ctx1.get_async(&slot).await });
    assert_eq!(reader.await.unwrap(), 0);
    ctx.set(&cell, 7);
    let ctx2 = ctx.clone();
    let reader2 = tokio::spawn(async move { ctx2.get_async(&slot).await });
    assert_eq!(reader2.await.unwrap(), 21);
}

#[tokio::test]
async fn async_context_handles_are_copy() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(1i32);
    let cell_copy = cell;
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell_copy);
        async move { v + 1 }
    });
    let slot_copy = slot;
    assert_eq!(ctx.get_async(&slot_copy).await, 2);
    let effect = ctx.effect_async(move |ctx| {
        let _v = ctx.get(&cell);
        async { None::<fn()> }
    });
    let _effect_copy = effect;
    ctx.dispose_async_effect(&effect);
}

#[tokio::test]
async fn async_context_sync_get_returns_resolved_value() {
    let ctx = AsyncContext::new();
    let slot = ctx.computed_async(|_| async { 7i32 });
    assert!(ctx.get(&slot).is_none());
    let _ = ctx.get_async(&slot).await;
    assert_eq!(ctx.get(&slot), Some(7));
}

#[tokio::test]
async fn async_context_sync_get_invalidated_returns_none() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(1i32);
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v * 3 }
    });
    assert_eq!(ctx.get_async(&slot).await, 3);
    assert_eq!(ctx.get(&slot), Some(3));
    ctx.set(&cell, 5);
    assert!(ctx.get(&slot).is_none());
    assert_eq!(ctx.get_async(&slot).await, 15);
    assert_eq!(ctx.get(&slot), Some(15));
}

#[tokio::test]
async fn async_context_sync_get_with_chain() {
    let ctx = AsyncContext::new();
    let cell = ctx.source(2i32);
    let a = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v + 10 }
    });
    let b = ctx.computed_async(move |ctx| {
        let ah = a;
        async move { ctx.get_async(&ah).await * 2 }
    });
    assert_eq!(ctx.get_async(&b).await, 24);
    assert_eq!(ctx.get(&a), Some(12));
    assert_eq!(ctx.get(&b), Some(24));
    ctx.set(&cell, 5);
    assert!(ctx.get(&a).is_none());
    assert!(ctx.get(&b).is_none());
}

#[tokio::test]
async fn async_context_sync_get_across_tokio_tasks() {
    let ctx = Arc::new(AsyncContext::new());
    let cell = ctx.source(10i32);
    let slot = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v + 1 }
    });
    let _ = ctx.get_async(&slot).await;
    let ctx_c = ctx.clone();
    let reader = tokio::spawn(async move { ctx_c.get(&slot) });
    assert_eq!(reader.await.unwrap(), Some(11));
    ctx.set(&cell, 20);
    let ctx_c = ctx.clone();
    let reader2 = tokio::spawn(async move { ctx_c.get(&slot) });
    assert_eq!(reader2.await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_context_concurrent_set_and_get_async_never_panics_k03k() {
    // Regression for #k03k. Concurrent `set_cell` (invalidation) racing
    // `get_async` (re-resolve) previously panicked on a benign race:
    //   - `unreachable!("get() already checked Resolved")` when the slot
    //     transitioned `Computing -> Resolved` between the `get()` fast-path
    //     check and the re-lock inside `get_async`; and
    //   - `get_async: notifier dropped unexpectedly` when a superseded
    //     (stale-revision) or invalidated compute dropped its `watch` senders
    //     without a final `Resolved` send.
    // Both must now re-resolve from authoritative slot state, not panic.
    let ctx = Arc::new(AsyncContext::new());
    let cell = ctx.source(0usize);
    let slot: AsyncComputed<usize> = ctx.computed_async(move |ctx| {
        let v = ctx.get(&cell);
        async move { v.wrapping_add(1) }
    });
    let _ = ctx.get_async(&slot).await;

    let workers = 8usize;
    let iters = 250usize;
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let ctx_c = Arc::clone(&ctx);
        let cell_c = cell;
        let slot_c = slot;
        handles.push(tokio::spawn(async move {
            for i in 0..iters {
                ctx_c.set(&cell_c, w * iters + i);
                // Always resolves to (some observed cell + 1); never panics.
                let v = ctx_c.get_async(&slot_c).await;
                assert!(v >= 1, "computed value should be cell + 1");
            }
        }));
    }
    for h in handles {
        h.await.expect("worker task panicked (race in get_async)");
    }

    // After contention settles, a fresh write resolves deterministically.
    ctx.set(&cell, 4242);
    assert_eq!(ctx.get_async(&slot).await, 4243);
}

// -- Eager async Signal (#lzsignalparity) ---------------------------------
//
// The AsyncContext counterpart to the single-threaded/thread-safe `signal`
// suite in tests/signal.rs. Because resolution is asynchronous, eager
// materialization completes on the runtime shortly after the invalidating
// write rather than synchronously within it, so these tests yield before
// asserting the puller has driven the recompute.

use lazily::AsyncSignalHandle;
use std::time::Duration;

#[tokio::test]
async fn async_signal_materializes_eagerly_without_a_read() {
    let ctx = AsyncContext::new();
    let n = ctx.source(2i32);
    let computes = Arc::new(AtomicU64::new(0));
    let c = computes.clone();
    let sig: AsyncSignalHandle<i32> = ctx.signal_async(move |ctx| {
        let v = ctx.get(&n);
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            v * 2
        }
    });

    // The eager puller drives one compute on creation; no read happens first.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(computes.load(Ordering::Relaxed), 1);
    // A non-blocking snapshot already sees the materialized value.
    assert_eq!(ctx.get_signal(&sig), Some(4));
}

#[tokio::test]
async fn async_signal_recomputes_eagerly_without_a_read() {
    let ctx = AsyncContext::new();
    let n = ctx.source(1i32);
    let computes = Arc::new(AtomicU64::new(0));
    let c = computes.clone();
    let sig = ctx.signal_async(move |ctx| {
        let v = ctx.get(&n);
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            v + 10
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ctx.get_signal(&sig), Some(11));

    // Changing the input recomputes eagerly: no `get_async` is needed to drive
    // it — a later non-blocking snapshot already reflects the new value.
    ctx.set(&n, 5);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(computes.load(Ordering::Relaxed) >= 2);
    assert_eq!(ctx.get_signal(&sig), Some(15));
}

#[tokio::test]
async fn async_signal_value_is_glitch_free_but_propagation_is_not_suppressed() {
    // The async memo guard keeps the *value* correct on an equal recompute, but
    // (unlike the single-threaded/thread-safe graph) it does NOT suppress
    // downstream propagation — async invalidation force-reruns effect
    // dependents on every upstream change. This mirrors the documented
    // `async_context_memo_blocks_downstream_on_equal` behavior.
    let ctx = AsyncContext::new();
    let n = ctx.source(4i32);
    let parity = ctx.signal_async(move |ctx| {
        let v = ctx.get(&n);
        async move { v % 2 }
    });

    let observed = Arc::new(Mutex::new(Vec::<i32>::new()));
    let obs = observed.clone();
    let _watch = ctx.effect_async(move |ctx| {
        let fut = ctx.get_signal_async(&parity);
        let obs = obs.clone();
        async move {
            let v = fut.await;
            obs.lock().unwrap().push(v);
            None::<fn()>
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4 -> 6: parity value stays 0 (no observable glitch)...
    ctx.set(&n, 6);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ctx.get_signal_async(&parity).await, 0);

    // 6 -> 7: parity flips to 1.
    ctx.set(&n, 7);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ctx.get_signal_async(&parity).await, 1);

    // Every observed value is a real parity (0 or 1); the run-count is not
    // suppressed on the equal step, but no inconsistent value is ever seen.
    let observed = observed.lock().unwrap();
    assert!(
        observed.iter().all(|v| *v == 0 || *v == 1),
        "observed inconsistent value: {observed:?}"
    );
    assert_eq!(observed.first().copied(), Some(0));
    assert_eq!(observed.last().copied(), Some(1));
}

#[tokio::test]
async fn async_chained_signals_propagate_eagerly() {
    let ctx = AsyncContext::new();
    let n = ctx.source(1i32);
    let a = ctx.signal_async(move |ctx| {
        let v = ctx.get(&n);
        async move { v + 1 }
    });
    let b = ctx.signal_async(move |ctx| {
        let fut = ctx.get_signal_async(&a);
        async move { fut.await * 10 }
    });

    assert_eq!(ctx.get_signal_async(&a).await, 2);
    assert_eq!(ctx.get_signal_async(&b).await, 20);

    ctx.set(&n, 5);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Both updated eagerly; a non-blocking snapshot already reflects them.
    assert_eq!(ctx.get_signal(&a), Some(6));
    assert_eq!(ctx.get_signal(&b), Some(60));
}

#[tokio::test]
async fn async_signal_dispose_stops_eager_recomputation() {
    let ctx = AsyncContext::new();
    let n = ctx.source(1i32);
    let computes = Arc::new(AtomicU64::new(0));
    let c = computes.clone();
    let sig = ctx.signal_async(move |ctx| {
        let v = ctx.get(&n);
        let c = c.clone();
        async move {
            c.fetch_add(1, Ordering::Relaxed);
            v + 1
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(sig.is_active(&ctx));
    assert_eq!(computes.load(Ordering::Relaxed), 1);

    sig.dispose(&ctx);
    assert!(!sig.is_active(&ctx));

    // Eager puller gone: a cell change no longer drives recomputation...
    ctx.set(&n, 9);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(computes.load(Ordering::Relaxed), 1);

    // ...but the value is still resolvable lazily on demand.
    assert_eq!(sig.get_async(&ctx).await, 10);
    assert_eq!(computes.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn async_signal_get_async_awaits_up_to_date_value() {
    let ctx = AsyncContext::new();
    let n = ctx.source(3i32);
    let sig = ctx.signal_async(move |ctx| {
        let v = ctx.get(&n);
        async move { v * 2 }
    });
    assert_eq!(sig.get_async(&ctx).await, 6);
    ctx.set(&n, 4);
    assert_eq!(sig.get_async(&ctx).await, 8);
}

// -- #lzqfrs AsyncContext prerequisites ---------------------------------------
//
// AsyncQueueCell needs two things AsyncContext did not have: a synchronous-compute
// `computed`, and a multi-root `clear`. These pin both.

#[tokio::test]
async fn sync_computed_resolves_without_being_driven() {
    let ctx = AsyncContext::new();
    let c = ctx.computed(|_| 42u32);
    // The whole point: no settle, no drive, no await. `computed_async` would be
    // `None` here until its task ran.
    assert_eq!(
        ctx.get(&c),
        Some(42),
        "a synchronous compute has nothing to await"
    );
}

#[tokio::test]
async fn sync_computed_memoizes_until_cleared() {
    let ctx = AsyncContext::new();
    let runs = Arc::new(AtomicU64::new(0));
    let c = {
        let runs = Arc::clone(&runs);
        ctx.computed(move |_| runs.fetch_add(1, Ordering::SeqCst) + 1)
    };

    assert_eq!(ctx.get(&c), Some(1));
    assert_eq!(
        ctx.get(&c),
        Some(1),
        "second read must hit the memo, not recompute"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "exactly one compute so far");

    ctx.clear(&c);
    assert_eq!(
        ctx.get(&c),
        Some(2),
        "clear must drop the memo so the next read re-derives"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn sync_computed_registers_a_real_dependency_edge() {
    let ctx = AsyncContext::new();
    let src = ctx.source(1u32);
    let doubled = ctx.computed(move |cx| cx.get(&src) * 2);

    assert_eq!(ctx.get(&doubled), Some(2));
    // If the read inside the compute did not register an edge, this write would
    // leave the memo stale and the assertion below would still read 2. That is the
    // difference between a derived cell and a cached function call.
    ctx.set(&src, 5);
    assert_eq!(
        ctx.get(&doubled),
        Some(10),
        "writing the source must invalidate the derived cell; a stale 2 means no \
         dependency edge was registered"
    );
}

#[tokio::test]
async fn clear_slots_invalidates_every_root() {
    let ctx = AsyncContext::new();
    let counters: Vec<Arc<AtomicU64>> = (0..4).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let cells: Vec<_> = counters
        .iter()
        .map(|c| {
            let c = Arc::clone(c);
            ctx.computed(move |_| c.fetch_add(1, Ordering::SeqCst) + 1)
        })
        .collect();

    for cell in &cells {
        assert_eq!(ctx.get(cell), Some(1));
    }
    for c in &counters {
        assert_eq!(c.load(Ordering::SeqCst), 1, "each cell computed once");
    }

    // One call, four roots — this is the shape a queue op needs when several
    // reader kinds transition together.
    let ids: Vec<_> = cells.iter().map(|c| c.id()).collect();
    ctx.clear_slots(&ids);

    for cell in &cells {
        assert_eq!(
            ctx.get(cell),
            Some(2),
            "every root must have been invalidated"
        );
    }
    for c in &counters {
        assert_eq!(
            c.load(Ordering::SeqCst),
            2,
            "each cell re-derived exactly once"
        );
    }
}

#[tokio::test]
async fn clear_slots_ignores_an_empty_root_set() {
    let ctx = AsyncContext::new();
    let runs = Arc::new(AtomicU64::new(0));
    let c = {
        let runs = Arc::clone(&runs);
        ctx.computed(move |_| runs.fetch_add(1, Ordering::SeqCst) + 1)
    };
    assert_eq!(ctx.get(&c), Some(1));
    ctx.clear_slots(&[]);
    assert_eq!(
        ctx.get(&c),
        Some(1),
        "an empty clear must not invalidate anything"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

// A sync compute must record the edge in BOTH directions.
//
// Found by mutation: dropping the forward `dependencies` list left every other
// test green, because invalidation travels the REVERSE edge, which
// `AsyncComputeContext::get` registers on the source itself. The forward list is
// what disposal walks to detach those reverse edges — so without it the graph
// invalidates correctly and then leaks a dangling dependent on every dispose.
// `dependency_count` names the direction the other tests could not see.
#[tokio::test]
async fn sync_computed_records_the_edge_in_both_directions() {
    let ctx = AsyncContext::new();
    let src = ctx.source(1u32);
    let doubled = ctx.computed(move |cx| cx.get(&src) * 2);
    assert_eq!(ctx.get(&doubled), Some(2));

    assert_eq!(
        ctx.dependent_count(&src),
        1,
        "the source must know its dependent (reverse edge — this is what carries \
         invalidation)"
    );
    assert_eq!(
        ctx.dependency_count(&doubled),
        1,
        "the derived cell must record what it read (forward edge — this is what \
         disposal walks to detach the reverse edge)"
    );
}
