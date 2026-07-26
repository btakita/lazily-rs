//! `GraphModel` implementations for `lazily`'s three execution models.
//!
//! Each impl is responsible for its own compute-closure bounds and its own
//! read-error convention; the engine sees only `Result<i64, ()>`.

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::model::{
    COMPUTE_FAILED, Computes, GraphModel, Log, Merges, Poison, Ref, ScopeModel, count_computes,
    count_merge, log_push,
};
use lazily::Sum;

/// Run `f`, converting a panic into `Err(())` with the message suppressed.
///
/// Reading a disposed node panics in every model — that is the library's
/// expression of the corpus's `read_after_dispose`.
///
/// A `fail_next`-armed compute failure is deliberately NOT swallowed here: it is
/// re-raised for the engine's `catch_armed_failure` to convert. Collapsing both
/// into `Err(())` would make the engine latch the id as permanently unreadable,
/// which is correct for disposal and wrong for a failed compute — the next read
/// must re-run the body. Conflating them made the engine report the very defect
/// `failed_compute_is_never_cached.json` exists to catch.
pub fn quiet<R>(f: impl FnOnce() -> R) -> Result<R, ()> {
    let prev = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let out = panic::catch_unwind(AssertUnwindSafe(f));
    panic::set_hook(prev);
    match out {
        Ok(v) => Ok(v),
        Err(payload) => {
            if payload
                .downcast_ref::<String>()
                .is_some_and(|m| m.contains(COMPUTE_FAILED))
            {
                panic::resume_unwind(payload);
            }
            Err(())
        }
    }
}

// -- Context ----------------------------------------------------------------

mod basic {
    use super::*;
    use lazily::{Compute, ComputeOps, Computed, Context, Effect, Source, TeardownScope};

    pub struct BasicModel {
        pub ctx: Context,
        runs: Log,
        cleanups: Log,
        poison: Poison,
    }

    fn read_ref<C: ComputeOps>(ctx: &C, node: Ref<Context>) -> Result<i64, ()> {
        match node {
            Ref::Cell(h) => quiet(|| h.get(ctx)),
            Ref::Slot(h) => quiet(|| h.get(ctx)),
            Ref::Effect(_) => Err(()),
        }
    }

    /// A read from inside a callback: never unwinds, records the failure.
    fn tracked<C: ComputeOps>(ctx: &C, node: Ref<Context>, poison: &Poison) -> i64 {
        match read_ref(ctx, node) {
            Ok(v) => v,
            Err(()) => {
                poison.store(true, Ordering::SeqCst);
                0
            }
        }
    }

    fn compute(
        reads: &[Ref<Context>],
        offset: i64,
        poison: &Poison,
        computes: &Computes,
    ) -> impl Fn(&Compute) -> i64 + 'static {
        let reads = reads.to_vec();
        let poison = poison.clone();
        let computes = computes.clone();
        move |c: &Compute| {
            // Counted here, inside the body the runtime invokes — see
            // `Computes`. Counting at the construction site instead would make
            // a lazy memo indistinguishable from an eager signal.
            if count_computes(&computes) {
                panic!("{COMPUTE_FAILED}");
            }
            let mut acc = offset;
            for r in &reads {
                acc += tracked(c, *r, &poison);
            }
            acc
        }
    }

    fn effect_body(
        name: &str,
        reads: &[Ref<Context>],
        runs: &Log,
        cleanups: &Log,
        poison: &Poison,
    ) -> impl Fn(&Compute) -> Box<dyn FnOnce()> + 'static {
        let reads = reads.to_vec();
        let name = name.to_owned();
        let runs = runs.clone();
        let cleanups = cleanups.clone();
        let poison = poison.clone();
        move |c: &Compute| {
            for r in &reads {
                tracked(c, *r, &poison);
            }
            log_push(&runs, &name);
            let cleanups = cleanups.clone();
            let name = name.clone();
            Box::new(move || log_push(&cleanups, &name)) as Box<dyn FnOnce()>
        }
    }

    /// The feed effect (`#lzmergefeed`): read `reads` (tracked, so the effect —
    /// not the merge cell — owns the edge), fold their sum into `target` under
    /// `Sum`, and count the fold. The write acquires no dependency edge to
    /// `target`; it is an argument, not a dependency (§9.2.3).
    fn feed_body(
        reads: &[Ref<Context>],
        target: Source<i64>,
        poison: &Poison,
        merges: &Merges,
    ) -> impl Fn(&Compute) + 'static {
        let reads = reads.to_vec();
        let poison = poison.clone();
        let merges = merges.clone();
        move |c: &Compute| {
            let mut acc = 0i64;
            for r in &reads {
                acc += tracked(c, *r, &poison);
            }
            count_merge(&merges);
            // The reads above are tracked (the effect owns the edge); the merge
            // write is an argument, not a dependency — via the untracked escape.
            c.untracked().apply_merge::<i64, Sum>(&target, acc);
        }
    }

    /// The divergent effect (`#lzfeedbackdrain`): read `own` (tracked, so it is
    /// a dependency) and write an incremented value back into it. The write
    /// reschedules the effect through the scheduler — a scheduler-closed loop,
    /// not a graph cycle — which the bounded drain cuts short.
    ///
    /// Two wrinkles make this faithful to the fixture on `lazily`:
    ///
    /// - `0` is held as a fixed point (`v == 0` writes `0`), so at creation the
    ///   effect reads `counter = 0`, writes `0`, the `PartialEq` store guard
    ///   skips the invalidation, and the loop is *not* kicked yet (step-1
    ///   `drain_exhausted = false`). `lazily` registers the dependency edge the
    ///   instant `get_cell` runs, so a plain `n + 1` body would reschedule
    ///   itself mid-creation and exhaust before the external kick ever landed.
    /// - `wrapping_add` rather than `+`: the divergent loop runs to the drain
    ///   budget, and a checked `+` would panic on i64 overflow before the bound
    ///   fires. Divergence here means *never converging* (every step changes the
    ///   value and reschedules), not the value growing without bound.
    ///
    /// The external `set_cell(counter, 1)` moves off the fixed point and the
    /// loop diverges under `KeepLatest` — the step-2 exhaustion.
    fn diverge_body(own: Source<i64>) -> impl Fn(&Compute) + 'static {
        move |c: &Compute| {
            let v = c.get(&own);
            let next = if v == 0 { 0 } else { v.wrapping_add(1) };
            c.set(&own, next);
        }
    }

    pub struct BasicScope<'a>(TeardownScope<'a>, Log, Log, Poison);

    impl ScopeModel<BasicModel> for BasicScope<'_> {
        fn source(&self, value: i64) -> Source<i64> {
            self.0.source(value)
        }
        fn computed(
            &self,
            reads: &[Ref<Context>],
            offset: i64,
            computes: &Computes,
        ) -> Computed<i64> {
            self.0.computed(compute(reads, offset, &self.3, computes))
        }
        fn effect(&self, name: &str, reads: &[Ref<Context>]) -> Effect {
            self.0
                .effect(effect_body(name, reads, &self.1, &self.2, &self.3))
        }
        fn owned(&self) -> usize {
            self.0.len()
        }
        fn disarm(self) {
            self.0.disarm();
        }
    }

    impl GraphModel for BasicModel {
        type Graph = Context;
        type Scope<'a> = BasicScope<'a>;
        type Signal = Computed<i64>;

        const NAME: &'static str = "Context";

        fn create() -> Self {
            Self {
                ctx: Context::new(),
                runs: Log::default(),
                cleanups: Log::default(),
                poison: Arc::new(AtomicBool::new(false)),
            }
        }

        fn graph(&self) -> &Self::Graph {
            &self.ctx
        }

        fn source(&self, value: i64) -> Source<i64> {
            self.ctx.source(value)
        }
        fn computed(
            &self,
            reads: &[Ref<Self::Graph>],
            offset: i64,
            computes: &Computes,
        ) -> Computed<i64> {
            self.ctx
                .computed(compute(reads, offset, &self.poison, computes))
        }
        fn effect(&self, name: &str, reads: &[Ref<Self::Graph>]) -> Effect {
            self.ctx.effect(effect_body(
                name,
                reads,
                &self.runs,
                &self.cleanups,
                &self.poison,
            ))
        }
        fn signal(
            &self,
            reads: &[Ref<Self::Graph>],
            offset: i64,
            computes: &Computes,
        ) -> Computed<i64> {
            self.ctx
                .signal(compute(reads, offset, &self.poison, computes))
        }
        fn read_signal(&self, signal: &Self::Signal) -> Result<i64, ()> {
            quiet(|| self.ctx.get_signal(signal))
        }
        fn dispose_signal(&self, signal: &Self::Signal) {
            self.ctx.dispose_signal(signal);
        }
        fn batch(&self, writes: &[(Source<i64>, i64)], merges: &[(Source<i64>, i64)]) {
            self.ctx.batch(|c| {
                for (h, v) in writes {
                    c.set(h, *v);
                }
                for (h, v) in merges {
                    c.apply_merge::<i64, Sum>(h, *v);
                }
            });
        }
        fn merge(&self, cell: Source<i64>, op: i64) {
            self.ctx.apply_merge::<i64, Sum>(&cell, op);
        }
        fn feed_effect(
            &self,
            _name: &str,
            reads: &[Ref<Self::Graph>],
            target: Source<i64>,
            merges: &Merges,
        ) -> Effect {
            self.ctx
                .effect(feed_body(reads, target, &self.poison, merges))
        }
        fn diverge_effect(&self, _name: &str, own: Source<i64>) -> Effect {
            // Only the divergent fixture builds this, so lowering the drain
            // budget here keeps the exhausting loop fast without affecting any
            // other fixture's model.
            self.ctx.set_drain_budget(256);
            self.ctx.effect(diverge_body(own))
        }
        fn drain_exhausted(&self) -> bool {
            self.ctx.last_drain_exhaustion().is_some()
        }
        fn clear_drain(&self) {
            self.ctx.clear_drain_exhaustion();
        }
        fn read(&self, node: Ref<Self::Graph>) -> Result<i64, ()> {
            read_ref(&self.ctx, node)
        }
        fn set_cell(&self, cell: Source<i64>, value: i64) {
            self.ctx.set(&cell, value);
        }
        fn is_effect_active(&self, effect: Effect) -> bool {
            self.ctx.is_effect_active(&effect)
        }
        fn scope(&self) -> Self::Scope<'_> {
            BasicScope(
                self.ctx.scope(),
                self.runs.clone(),
                self.cleanups.clone(),
                self.poison.clone(),
            )
        }
        fn run_log(&self) -> &Log {
            &self.runs
        }
        fn cleanup_log(&self) -> &Log {
            &self.cleanups
        }
        fn poison(&self) -> &Poison {
            &self.poison
        }
    }
}

pub use basic::BasicModel;

// -- ThreadSafeContext ------------------------------------------------------

#[cfg(feature = "thread-safe")]
mod threadsafe {
    use super::*;
    use lazily::{
        Computed, Effect, Source, ThreadSafeContext, ThreadSafeSignalHandle,
        ThreadSafeTeardownScope,
    };

    pub struct ThreadSafeModel {
        pub ctx: ThreadSafeContext,
        runs: Log,
        cleanups: Log,
        poison: Poison,
    }

    fn read_ref(ctx: &ThreadSafeContext, node: Ref<ThreadSafeContext>) -> Result<i64, ()> {
        match node {
            Ref::Cell(h) => quiet(|| ctx.get(&h)),
            Ref::Slot(h) => quiet(|| ctx.get(&h)),
            Ref::Effect(_) => Err(()),
        }
    }

    fn tracked(ctx: &ThreadSafeContext, node: Ref<ThreadSafeContext>, poison: &Poison) -> i64 {
        match read_ref(ctx, node) {
            Ok(v) => v,
            Err(()) => {
                poison.store(true, Ordering::SeqCst);
                0
            }
        }
    }

    fn compute(
        reads: &[Ref<ThreadSafeContext>],
        offset: i64,
        poison: &Poison,
        computes: &Computes,
    ) -> impl Fn(&ThreadSafeContext) -> i64 + Send + Sync + 'static {
        let reads = reads.to_vec();
        let poison = poison.clone();
        let computes = computes.clone();
        move |c: &ThreadSafeContext| {
            if count_computes(&computes) {
                panic!("{COMPUTE_FAILED}");
            }
            let mut acc = offset;
            for r in &reads {
                acc += tracked(c, *r, &poison);
            }
            acc
        }
    }

    fn effect_body(
        name: &str,
        reads: &[Ref<ThreadSafeContext>],
        runs: &Log,
        cleanups: &Log,
        poison: &Poison,
    ) -> impl Fn(&ThreadSafeContext) -> Box<dyn FnOnce() + Send + Sync> + Send + Sync + 'static
    {
        let reads = reads.to_vec();
        let name = name.to_owned();
        let runs = runs.clone();
        let cleanups = cleanups.clone();
        let poison = poison.clone();
        move |c: &ThreadSafeContext| {
            for r in &reads {
                tracked(c, *r, &poison);
            }
            log_push(&runs, &name);
            let cleanups = cleanups.clone();
            let name = name.clone();
            Box::new(move || log_push(&cleanups, &name)) as Box<dyn FnOnce() + Send + Sync>
        }
    }

    /// Feed effect (`#lzmergefeed`), thread-safe flavour. See the basic module's
    /// `feed_body`; the only difference is the `Send + Sync` closure bound.
    fn feed_body(
        reads: &[Ref<ThreadSafeContext>],
        target: Source<i64>,
        poison: &Poison,
        merges: &Merges,
    ) -> impl Fn(&ThreadSafeContext) + Send + Sync + 'static {
        let reads = reads.to_vec();
        let poison = poison.clone();
        let merges = merges.clone();
        move |c: &ThreadSafeContext| {
            let mut acc = 0i64;
            for r in &reads {
                acc += tracked(c, *r, &poison);
            }
            count_merge(&merges);
            c.apply_merge::<i64, Sum>(&target, acc);
        }
    }

    /// Divergent effect (`#lzfeedbackdrain`), thread-safe flavour. The write
    /// reschedules the effect onto the outer drain, which the bounded
    /// `flush_effects` cuts short. Holds `0` as a fixed point and uses
    /// `wrapping_add` — see the basic module's `diverge_body` for why.
    fn diverge_body(own: Source<i64>) -> impl Fn(&ThreadSafeContext) + Send + Sync + 'static {
        move |c: &ThreadSafeContext| {
            let v = c.get(&own);
            let next = if v == 0 { 0 } else { v.wrapping_add(1) };
            c.set(&own, next);
        }
    }

    /// Owned, not borrowed — see `ThreadSafeContext::scope`. The GAT lifetime is
    /// simply unused here, which is the point: this scope is `Send` and can
    /// outlive the borrow that produced it.
    pub struct ThreadSafeScope(ThreadSafeTeardownScope, Log, Log, Poison);

    impl ScopeModel<ThreadSafeModel> for ThreadSafeScope {
        fn source(&self, value: i64) -> Source<i64> {
            self.0.source(value)
        }
        fn computed(
            &self,
            reads: &[Ref<ThreadSafeContext>],
            offset: i64,
            computes: &Computes,
        ) -> Computed<i64> {
            self.0.computed(compute(reads, offset, &self.3, computes))
        }
        fn effect(&self, name: &str, reads: &[Ref<ThreadSafeContext>]) -> Effect {
            self.0
                .effect(effect_body(name, reads, &self.1, &self.2, &self.3))
        }
        fn owned(&self) -> usize {
            self.0.len()
        }
        fn disarm(self) {
            self.0.disarm();
        }
    }

    impl GraphModel for ThreadSafeModel {
        type Graph = ThreadSafeContext;
        type Scope<'a> = ThreadSafeScope;
        type Signal = ThreadSafeSignalHandle<i64>;

        const NAME: &'static str = "ThreadSafeContext";

        fn create() -> Self {
            Self {
                ctx: ThreadSafeContext::new(),
                runs: Log::default(),
                cleanups: Log::default(),
                poison: Arc::new(AtomicBool::new(false)),
            }
        }

        fn graph(&self) -> &Self::Graph {
            &self.ctx
        }

        fn source(&self, value: i64) -> Source<i64> {
            self.ctx.source(value)
        }
        fn computed(
            &self,
            reads: &[Ref<Self::Graph>],
            offset: i64,
            computes: &Computes,
        ) -> Computed<i64> {
            self.ctx
                .computed(compute(reads, offset, &self.poison, computes))
        }
        fn effect(&self, name: &str, reads: &[Ref<Self::Graph>]) -> Effect {
            self.ctx.effect(effect_body(
                name,
                reads,
                &self.runs,
                &self.cleanups,
                &self.poison,
            ))
        }
        fn signal(
            &self,
            reads: &[Ref<Self::Graph>],
            offset: i64,
            computes: &Computes,
        ) -> ThreadSafeSignalHandle<i64> {
            self.ctx
                .signal(compute(reads, offset, &self.poison, computes))
        }
        fn read_signal(&self, signal: &Self::Signal) -> Result<i64, ()> {
            quiet(|| self.ctx.get_signal(signal))
        }
        fn dispose_signal(&self, signal: &Self::Signal) {
            self.ctx.dispose_signal(signal);
        }
        fn batch(&self, writes: &[(Source<i64>, i64)], merges: &[(Source<i64>, i64)]) {
            self.ctx.batch(|c| {
                for (h, v) in writes {
                    c.set(h, *v);
                }
                for (h, v) in merges {
                    c.apply_merge::<i64, Sum>(h, *v);
                }
            });
        }
        fn merge(&self, cell: Source<i64>, op: i64) {
            self.ctx.apply_merge::<i64, Sum>(&cell, op);
        }
        fn feed_effect(
            &self,
            _name: &str,
            reads: &[Ref<Self::Graph>],
            target: Source<i64>,
            merges: &Merges,
        ) -> Effect {
            self.ctx
                .effect(feed_body(reads, target, &self.poison, merges))
        }
        fn diverge_effect(&self, _name: &str, own: Source<i64>) -> Effect {
            self.ctx.set_drain_budget(256);
            self.ctx.effect(diverge_body(own))
        }
        fn drain_exhausted(&self) -> bool {
            self.ctx.last_drain_exhaustion().is_some()
        }
        fn clear_drain(&self) {
            self.ctx.clear_drain_exhaustion();
        }
        fn read(&self, node: Ref<Self::Graph>) -> Result<i64, ()> {
            read_ref(&self.ctx, node)
        }
        fn set_cell(&self, cell: Source<i64>, value: i64) {
            self.ctx.set(&cell, value);
        }
        fn is_effect_active(&self, effect: Effect) -> bool {
            self.ctx.is_effect_active(&effect)
        }
        fn scope(&self) -> Self::Scope<'_> {
            ThreadSafeScope(
                self.ctx.scope(),
                self.runs.clone(),
                self.cleanups.clone(),
                self.poison.clone(),
            )
        }
        fn run_log(&self) -> &Log {
            &self.runs
        }
        fn cleanup_log(&self) -> &Log {
            &self.cleanups
        }
        fn poison(&self) -> &Poison {
            &self.poison
        }
    }
}

#[cfg(feature = "thread-safe")]
pub use threadsafe::ThreadSafeModel;

// -- AsyncContext -----------------------------------------------------------

#[cfg(feature = "async")]
mod asynchronous {
    use super::*;
    use lazily::{
        AsyncComputeContext, AsyncComputed, AsyncContext, AsyncEffectHandle, AsyncSignalHandle,
        AsyncSource, AsyncTeardownScope,
    };

    /// The async model owns its runtime and blocks on it inside `read`, so the
    /// engine stays synchronous and the default-feature `Context` test never
    /// needs an executor.
    pub struct AsyncModel {
        pub ctx: AsyncContext,
        rt: tokio::runtime::Runtime,
        runs: Log,
        cleanups: Log,
        poison: Poison,
    }

    /// Computes receive an `AsyncComputeContext`, not an owned `AsyncContext` —
    /// it carries the node id and the generation captured at spawn, which is
    /// what makes dependency registration safe across an await.
    ///
    /// The `use<>` on the return type says the closure captures no lifetime: it
    /// clones everything it needs out of the arguments. Without it Rust 2024
    /// infers a capture of `&Computes` and rejects the temporary counter that
    /// `effect_body` passes.
    fn compute(
        reads: &[Ref<AsyncContext>],
        offset: i64,
        poison: &Poison,
        computes: &Computes,
    ) -> impl Fn(AsyncComputeContext) -> BoxFuture + Send + Sync + 'static + use<> {
        let reads = reads.to_vec();
        let poison = poison.clone();
        let computes = computes.clone();
        move |c: AsyncComputeContext| {
            let reads = reads.clone();
            let poison = poison.clone();
            let computes = computes.clone();
            Box::pin(async move {
                if count_computes(&computes) {
                    panic!("{COMPUTE_FAILED}");
                }
                let mut acc = offset;
                for r in &reads {
                    match read_in_compute(&c, *r).await {
                        Ok(v) => acc += v,
                        Err(()) => poison.store(true, Ordering::SeqCst),
                    }
                }
                acc
            })
        }
    }

    type BoxFuture = std::pin::Pin<Box<dyn std::future::Future<Output = i64> + Send>>;

    /// A read from inside a compute. Reading a disposed node panics, and
    /// `catch_unwind` cannot span an await point, so the panic is caught around
    /// the two halves separately: building the future, and driving it.
    async fn read_in_compute(c: &AsyncComputeContext, node: Ref<AsyncContext>) -> Result<i64, ()> {
        match node {
            Ref::Cell(h) => quiet(|| c.get(&h)),
            Ref::Slot(h) => match quiet(|| c.get_async(&h)) {
                Ok(fut) => {
                    let prev = std::panic::take_hook();
                    std::panic::set_hook(Box::new(|_| {}));
                    let out = tokio::spawn(fut).await;
                    std::panic::set_hook(prev);
                    out.map_err(|_| ())
                }
                Err(()) => Err(()),
            },
            Ref::Effect(_) => Err(()),
        }
    }

    fn effect_body(
        name: &str,
        reads: &[Ref<AsyncContext>],
        runs: &Log,
        cleanups: &Log,
        poison: &Poison,
    ) -> impl Fn(AsyncComputeContext) -> EffectFuture + Send + Sync + 'static {
        // A private counter: an effect body is not a node the corpus can name in
        // `computes_of`, so its runs must not land on any node's count.
        let uncounted = Computes::default();
        let body = compute(reads, 0, poison, &uncounted);
        let name = name.to_owned();
        let runs = runs.clone();
        let cleanups = cleanups.clone();
        move |c: AsyncComputeContext| {
            let fut = body(c);
            let name = name.clone();
            let runs = runs.clone();
            let cleanups = cleanups.clone();
            Box::pin(async move {
                let _ = fut.await;
                log_push(&runs, &name);
                let cleanup_name = name.clone();
                Some(Box::new(move || log_push(&cleanups, &cleanup_name))
                    as Box<dyn FnOnce() + Send>)
            })
        }
    }

    type EffectFuture = std::pin::Pin<
        Box<dyn std::future::Future<Output = Option<Box<dyn FnOnce() + Send>>> + Send>,
    >;

    /// The feed effect (`#lzmergefeed`), async flavour. Reads `reads` through the
    /// tracking compute context (so the effect owns the edge), then folds their
    /// sum into `target` under `Sum` **synchronously** via
    /// [`AsyncComputeContext::apply_merge`] — the fold is a cell op, so it is
    /// synchronous even here (§9.1); only the effect's *rerun* is scheduled on
    /// the executor. The merge cell acquires no dependency edge.
    fn async_feed_body(
        reads: &[Ref<AsyncContext>],
        target: AsyncSource<i64>,
        poison: &Poison,
        merges: &Merges,
    ) -> impl Fn(AsyncComputeContext) -> EffectFuture + Send + Sync + 'static {
        let reads = reads.to_vec();
        let poison = poison.clone();
        let merges = merges.clone();
        move |c: AsyncComputeContext| {
            let reads = reads.clone();
            let poison = poison.clone();
            let merges = merges.clone();
            Box::pin(async move {
                let mut acc = 0i64;
                for r in &reads {
                    match read_in_compute(&c, *r).await {
                        Ok(v) => acc += v,
                        Err(()) => poison.store(true, Ordering::SeqCst),
                    }
                }
                count_merge(&merges);
                c.apply_merge::<i64, Sum>(&target, acc);
                None
            })
        }
    }

    /// Carries a runtime `Handle` because effect registration spawns a task, and
    /// `tokio::spawn` panics outside a runtime context. Disposal only calls
    /// `JoinHandle::abort`, which does not need one.
    pub struct AsyncScope(AsyncTeardownScope, Log, Log, Poison, tokio::runtime::Handle);

    impl ScopeModel<AsyncModel> for AsyncScope {
        fn source(&self, value: i64) -> AsyncSource<i64> {
            let _guard = self.4.enter();
            self.0.source(value)
        }
        fn computed(
            &self,
            reads: &[Ref<AsyncContext>],
            offset: i64,
            computes: &Computes,
        ) -> AsyncComputed<i64> {
            let _guard = self.4.enter();
            self.0
                .computed_async(compute(reads, offset, &self.3, computes))
        }
        fn effect(&self, name: &str, reads: &[Ref<AsyncContext>]) -> AsyncEffectHandle {
            let _guard = self.4.enter();
            self.0
                .effect_async(effect_body(name, reads, &self.1, &self.2, &self.3))
        }
        fn owned(&self) -> usize {
            self.0.len()
        }
        fn disarm(self) {
            self.0.disarm();
        }
    }

    impl GraphModel for AsyncModel {
        type Graph = AsyncContext;
        type Scope<'a> = AsyncScope;
        type Signal = AsyncSignalHandle<i64>;

        const NAME: &'static str = "AsyncContext";

        fn create() -> Self {
            Self {
                ctx: AsyncContext::new(),
                rt: tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap(),
                runs: Log::default(),
                cleanups: Log::default(),
                poison: Arc::new(AtomicBool::new(false)),
            }
        }

        fn graph(&self) -> &Self::Graph {
            &self.ctx
        }

        fn source(&self, value: i64) -> AsyncSource<i64> {
            let _guard = self.rt.enter();
            self.ctx.source(value)
        }
        fn computed(
            &self,
            reads: &[Ref<Self::Graph>],
            offset: i64,
            computes: &Computes,
        ) -> AsyncComputed<i64> {
            let _guard = self.rt.enter();
            self.ctx
                .computed_async(compute(reads, offset, &self.poison, computes))
        }
        fn effect(&self, name: &str, reads: &[Ref<Self::Graph>]) -> AsyncEffectHandle {
            let _guard = self.rt.enter();
            self.ctx.effect_async(effect_body(
                name,
                reads,
                &self.runs,
                &self.cleanups,
                &self.poison,
            ))
        }
        fn signal(
            &self,
            reads: &[Ref<Self::Graph>],
            offset: i64,
            computes: &Computes,
        ) -> AsyncSignalHandle<i64> {
            let _guard = self.rt.enter();
            self.ctx
                .signal_async(compute(reads, offset, &self.poison, computes))
        }
        /// Awaits rather than snapshotting: `get_signal` returns `Option`, and
        /// treating an unresolved snapshot as a read would let a signal that
        /// never materialized pass as readable.
        fn read_signal(&self, signal: &Self::Signal) -> Result<i64, ()> {
            quiet(|| self.rt.block_on(self.ctx.get_signal_async(signal)))
        }
        fn dispose_signal(&self, signal: &Self::Signal) {
            self.ctx.dispose_signal(signal);
        }
        fn batch(&self, writes: &[(AsyncSource<i64>, i64)], merges: &[(AsyncSource<i64>, i64)]) {
            let _guard = self.rt.enter();
            self.ctx.batch(|c| {
                for (h, v) in writes {
                    c.set(h, *v);
                }
                for (h, v) in merges {
                    c.apply_merge::<i64, Sum>(h, *v);
                }
            });
        }
        fn merge(&self, cell: AsyncSource<i64>, op: i64) {
            let _guard = self.rt.enter();
            self.ctx.apply_merge::<i64, Sum>(&cell, op);
        }
        fn feed_effect(
            &self,
            _name: &str,
            reads: &[Ref<Self::Graph>],
            target: AsyncSource<i64>,
            merges: &Merges,
        ) -> AsyncEffectHandle {
            let _guard = self.rt.enter();
            self.ctx
                .effect_async(async_feed_body(reads, target, &self.poison, merges))
        }
        /// A NON-writing stand-in (`#lzfeedbackdrain`). `AsyncContext` has no
        /// bounded effect drain, and a real self-writing async effect would
        /// spawn reruns unboundedly (one per revision — the async scheduler
        /// coalesces per revision, but a self-write advances the revision every
        /// time), which would exhaust memory rather than fail an assertion. So
        /// the async model reads `own` without writing it: no loop, and
        /// `drain_exhausted` stays `false`. That gap is recorded in the runner's
        /// per-model divergence ledger rather than papered over — bounding
        /// divergent async feedback is future work beyond the merge algebra.
        fn diverge_effect(&self, name: &str, own: AsyncSource<i64>) -> AsyncEffectHandle {
            self.effect(name, &[Ref::Cell(own)])
        }
        fn read(&self, node: Ref<Self::Graph>) -> Result<i64, ()> {
            match node {
                Ref::Cell(h) => quiet(|| self.ctx.get(&h)),
                Ref::Slot(h) => quiet(|| self.rt.block_on(self.ctx.get_async(&h))),
                Ref::Effect(_) => Err(()),
            }
        }
        fn set_cell(&self, cell: AsyncSource<i64>, value: i64) {
            let _guard = self.rt.enter();
            self.ctx.set(&cell, value);
        }
        fn is_effect_active(&self, effect: AsyncEffectHandle) -> bool {
            self.ctx.is_async_effect_active(&effect)
        }
        fn scope(&self) -> Self::Scope<'_> {
            let _guard = self.rt.enter();
            AsyncScope(
                self.ctx.scope(),
                self.runs.clone(),
                self.cleanups.clone(),
                self.poison.clone(),
                self.rt.handle().clone(),
            )
        }
        fn run_log(&self) -> &Log {
            &self.runs
        }
        fn cleanup_log(&self) -> &Log {
            &self.cleanups
        }
        fn poison(&self) -> &Poison {
            &self.poison
        }

        /// Async effects are spawned tasks; let them run before the engine
        /// evaluates assertions that depend on an effect having executed.
        fn settle(&self) {
            self.rt.block_on(async {
                for _ in 0..32 {
                    tokio::task::yield_now().await;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                for _ in 0..32 {
                    tokio::task::yield_now().await;
                }
            });
        }
    }
}

#[cfg(feature = "async")]
pub use asynchronous::AsyncModel;
