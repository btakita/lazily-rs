//! `ThreadSafeIngressCell` — the `Send + Sync` flavor of [`IngressCell`]
//! (`#designimplementtransport`).
//!
//! Same flavor-neutral [`IngressCore`](crate::ingress_core), same four reader
//! kinds per keyed scope, same three receipt channels. This shell adds only the
//! reactivity, minted on *this* context's graph — because the family's claim is
//! that all three flavors obey ONE contract.
//!
//! **Lock discipline.** Three locks exist and they are always taken in this
//! order, never any other:
//!
//! 1. `core` — taken by an op, and released before anything else is touched.
//! 2. `scopes` — the per-key reader-handle table.
//! 3. the context — taken by `ctx.clear` / `ctx.get` / `ctx.computed`.
//!
//! A reader's compute closure runs *inside* the context lock and takes `core`,
//! which is context→core. That is the reverse of an op's core→context, so an op
//! that invalidated while still holding `core` would deadlock against a
//! concurrent reader. Every op below therefore scopes its `core` guard to a block
//! that ends before `scopes` or the context is reached. This is why
//! [`apply`](ThreadSafeIngressCell::apply) is a separate step taking an
//! already-computed [`IngressChange`] rather than something an op does inline.
//!
//! **Multi-root invalidation goes through `batch()`.** One admission can dirty a
//! scope's value, readiness, authority, and retry plus a receipt channel;
//! clearing them one at a time is one frontier walk each, and a concurrent reader
//! can interleave and see the new value with the old authority — precisely the
//! partial fan-out a generation handoff must never expose.
//! `ThreadSafeContext` has no `clear_slots`, but it does not need one:
//! `clear_slot` accumulates into `batched_slots` while `batch_depth > 0` and
//! `finish_batch` hands every collected root to a single `clear_frontier_locked`.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::cell::{Computed, Source};
use crate::ingress_core::{
    IngressAdmission, IngressAuthority, IngressChange, IngressConfigError, IngressCore,
    IngressEnvelope, IngressError, IngressPolicy, IngressReadiness, IngressReceipt,
    IngressReceiptChannel, IngressRetry, IngressSchedule, IngressTransport, IngressTransportKind,
    ReplayRequest, ScopeView,
};
use crate::merge::MergePolicy;
use crate::thread_safe::ThreadSafeContext;

/// The four reader kinds one keyed scope exposes.
struct ScopeReaders<T> {
    value: Computed<Option<T>>,
    readiness: Computed<IngressReadiness>,
    authority: Computed<Option<IngressAuthority>>,
    retry: Computed<Option<IngressRetry>>,
}

impl<T> Clone for ScopeReaders<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            readiness: self.readiness,
            authority: self.authority,
            retry: self.retry,
        }
    }
}

struct Inner<K, T, M> {
    core: Arc<Mutex<IngressCore<K, T, M>>>,
    scopes: Mutex<HashMap<K, ScopeReaders<T>>>,
    accepted: Computed<Vec<IngressReceipt<K>>>,
    dropped: Computed<Vec<IngressReceipt<K>>>,
    errors: Computed<Vec<IngressReceipt<K>>>,
    transport_kind: Source<IngressTransportKind>,
    poll_interval: Source<u64>,
    schedule: Computed<IngressSchedule>,
}

/// A `Send + Sync` keyed, lifecycle-scoped reactive ingress: one admission plane
/// per key, with readiness, authority, and retry as derives rather than calls.
pub struct ThreadSafeIngressCell<K, T, M> {
    inner: Arc<Inner<K, T, M>>,
}

impl<K, T, M> Clone for ThreadSafeIngressCell<K, T, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, T, M> ThreadSafeIngressCell<K, T, M>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: PartialEq + Clone + Send + Sync + 'static,
    M: MergePolicy<T> + Send + Sync + 'static,
{
    /// Build an ingress over `policy`, delivering as `kind`.
    pub fn new(
        ctx: &ThreadSafeContext,
        policy: IngressPolicy,
        kind: IngressTransportKind,
        poll_interval: u64,
    ) -> Result<Self, IngressConfigError> {
        let core = Arc::new(Mutex::new(IngressCore::<K, T, M>::new(policy)?));
        let accepted = Self::receipt_reader(ctx, &core, IngressReceiptChannel::Accepted);
        let dropped = Self::receipt_reader(ctx, &core, IngressReceiptChannel::Dropped);
        let errors = Self::receipt_reader(ctx, &core, IngressReceiptChannel::Error);
        let transport_kind = ctx.source(kind);
        let poll_interval = ctx.source(poll_interval);
        let schedule = ctx.computed(move |c| {
            IngressSchedule::for_kind(c.get(&transport_kind), c.get(&poll_interval))
        });
        Ok(Self {
            inner: Arc::new(Inner {
                core,
                scopes: Mutex::new(HashMap::new()),
                accepted,
                dropped,
                errors,
                transport_kind,
                poll_interval,
                schedule,
            }),
        })
    }

    fn receipt_reader(
        ctx: &ThreadSafeContext,
        core: &Arc<Mutex<IngressCore<K, T, M>>>,
        channel: IngressReceiptChannel,
    ) -> Computed<Vec<IngressReceipt<K>>> {
        let core = Arc::clone(core);
        ctx.computed(move |_| core.lock().expect("ingress core").receipts(channel))
    }

    /// Mint (or return) one scope's four readers.
    ///
    /// Holds `scopes` across the mint so two threads asking for the same key
    /// cannot orphan four graph nodes; safe because nothing ever acquires
    /// `scopes` while holding the context, and this method never touches `core`.
    fn ensure_readers(&self, ctx: &ThreadSafeContext, key: K) -> ScopeReaders<T> {
        let mut scopes = self.inner.scopes.lock().expect("ingress scopes");
        if let Some(readers) = scopes.get(&key) {
            return readers.clone();
        }
        let readers = ScopeReaders {
            value: {
                let core = Arc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.lock().expect("ingress core").peek(&key))
            },
            readiness: {
                let core = Arc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.lock().expect("ingress core").readiness(&key))
            },
            authority: {
                let core = Arc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.lock().expect("ingress core").authority(&key))
            },
            retry: {
                let core = Arc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.lock().expect("ingress core").retry(&key))
            },
        };
        scopes.insert(key, readers.clone());
        readers
    }

    /// Apply one core-reported invalidation set in a single frontier walk.
    ///
    /// Called only with `core` already released — see the lock-order note on this
    /// module.
    fn apply(&self, ctx: &ThreadSafeContext, change: IngressChange<K>) {
        if change.is_empty() {
            return;
        }
        let scoped: Vec<(ScopeReaders<T>, _)> = change
            .scopes
            .iter()
            .map(|(key, scope_change)| (self.ensure_readers(ctx, key.clone()), *scope_change))
            .collect();
        ctx.batch(|_| {
            for (readers, scope_change) in &scoped {
                if scope_change.value {
                    ctx.clear(&readers.value);
                }
                if scope_change.readiness {
                    ctx.clear(&readers.readiness);
                }
                if scope_change.authority {
                    ctx.clear(&readers.authority);
                }
                if scope_change.retry {
                    ctx.clear(&readers.retry);
                }
            }
            if change.accepted_receipts {
                ctx.clear(&self.inner.accepted);
            }
            if change.dropped_receipts {
                ctx.clear(&self.inner.dropped);
            }
            if change.error_receipts {
                ctx.clear(&self.inner.errors);
            }
        });
    }

    /// Open (or reopen) a keyed scope at `generation`.
    pub fn open(&self, ctx: &ThreadSafeContext, key: K, generation: u64) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.open(key, generation)
        };
        self.apply(ctx, change);
    }

    /// Admit one decoded envelope.
    pub fn admit(
        &self,
        ctx: &ThreadSafeContext,
        envelope: IngressEnvelope<K, T>,
    ) -> IngressAdmission {
        let (change, admission) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.admit(envelope)
        };
        self.apply(ctx, change);
        admission
    }

    /// Suspend a scope, retaining its watermark.
    pub fn suspend(&self, ctx: &ThreadSafeContext, key: &K) -> Option<ReplayRequest> {
        let (change, request) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.suspend(key)
        };
        self.apply(ctx, change);
        request
    }

    /// Reconnect a scope at `generation`, clearing its error streak.
    pub fn reconnect(&self, ctx: &ThreadSafeContext, key: &K, generation: u64) -> ReplayRequest {
        let (change, request) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.reconnect(key, generation)
        };
        self.apply(ctx, change);
        request
    }

    /// Close a scope.
    pub fn close(&self, ctx: &ThreadSafeContext, key: &K) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.close(key)
        };
        self.apply(ctx, change);
    }

    /// Record a transport/decode failure, deepening the scope's backoff.
    pub fn fail(&self, ctx: &ThreadSafeContext, key: &K, error: IngressError) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.fail(key, error)
        };
        self.apply(ctx, change);
    }

    /// Advance logical time.
    pub fn tick(&self, ctx: &ThreadSafeContext, now: u64) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.tick(now)
        };
        self.apply(ctx, change);
    }

    /// Drain a scope's coalesced window.
    pub fn drain(&self, ctx: &ThreadSafeContext, key: &K) -> Option<T> {
        let (change, value) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.drain(key)
        };
        self.apply(ctx, change);
        value
    }

    /// Admit everything `transport` has decoded, then ask it to replay any gap
    /// still open.
    pub fn pump<Tr>(&self, ctx: &ThreadSafeContext, transport: &mut Tr) -> Vec<IngressAdmission>
    where
        Tr: IngressTransport<K, T> + ?Sized,
    {
        let batch = transport.drain();
        let mut outcomes = Vec::with_capacity(batch.len());
        let mut touched: Vec<K> = Vec::new();
        for envelope in batch {
            let key = envelope.key.clone();
            outcomes.push(self.admit(ctx, envelope));
            if !touched.contains(&key) {
                touched.push(key);
            }
        }
        for key in touched {
            let gap = {
                let core = self.inner.core.lock().expect("ingress core");
                core.view(&key)
                    .filter(ScopeView::has_gap)
                    .map(|view| ReplayRequest {
                        generation: view.generation,
                        from_sequence: view.resume_from(),
                    })
            };
            if let Some(request) = gap {
                transport.request_replay(&key, request);
            }
        }
        outcomes
    }

    /// Reactive read: the coalesced window awaiting drain.
    pub fn value(&self, ctx: &ThreadSafeContext, key: &K) -> Option<T> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.value)
    }

    /// Reactive read: derived readiness.
    pub fn readiness(&self, ctx: &ThreadSafeContext, key: &K) -> IngressReadiness {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.readiness)
    }

    /// Reactive read: derived authority.
    pub fn authority(&self, ctx: &ThreadSafeContext, key: &K) -> Option<IngressAuthority> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.authority)
    }

    /// Reactive read: derived retry decision.
    pub fn retry(&self, ctx: &ThreadSafeContext, key: &K) -> Option<IngressRetry> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.retry)
    }

    /// Handle for the scope's value reader.
    pub fn value_handle(&self, ctx: &ThreadSafeContext, key: &K) -> Computed<Option<T>> {
        self.ensure_readers(ctx, key.clone()).value
    }

    /// Handle for the scope's readiness reader.
    pub fn readiness_handle(&self, ctx: &ThreadSafeContext, key: &K) -> Computed<IngressReadiness> {
        self.ensure_readers(ctx, key.clone()).readiness
    }

    /// Handle for the scope's authority reader.
    pub fn authority_handle(
        &self,
        ctx: &ThreadSafeContext,
        key: &K,
    ) -> Computed<Option<IngressAuthority>> {
        self.ensure_readers(ctx, key.clone()).authority
    }

    /// Handle for the scope's retry reader.
    pub fn retry_handle(&self, ctx: &ThreadSafeContext, key: &K) -> Computed<Option<IngressRetry>> {
        self.ensure_readers(ctx, key.clone()).retry
    }

    /// Reactive read: accepted receipts, oldest first.
    pub fn accepted(&self, ctx: &ThreadSafeContext) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.accepted)
    }

    /// Reactive read: dropped receipts, oldest first.
    pub fn dropped(&self, ctx: &ThreadSafeContext) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.dropped)
    }

    /// Reactive read: error receipts, oldest first.
    pub fn errors(&self, ctx: &ThreadSafeContext) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.errors)
    }

    /// Handle for the accepted-receipt reader.
    pub fn accepted_handle(&self) -> Computed<Vec<IngressReceipt<K>>> {
        self.inner.accepted
    }

    /// Handle for the dropped-receipt reader.
    pub fn dropped_handle(&self) -> Computed<Vec<IngressReceipt<K>>> {
        self.inner.dropped
    }

    /// Handle for the error-receipt reader.
    pub fn errors_handle(&self) -> Computed<Vec<IngressReceipt<K>>> {
        self.inner.errors
    }

    /// Reactive read: the derived delivery schedule.
    pub fn schedule(&self, ctx: &ThreadSafeContext) -> IngressSchedule {
        ctx.get(&self.inner.schedule)
    }

    /// Handle for the schedule reader.
    pub fn schedule_handle(&self) -> Computed<IngressSchedule> {
        self.inner.schedule
    }

    /// Retune the transport live.
    pub fn set_transport(&self, ctx: &ThreadSafeContext, kind: IngressTransportKind) {
        ctx.set(&self.inner.transport_kind, kind);
    }

    /// Retune the poll bound live.
    pub fn set_poll_interval(&self, ctx: &ThreadSafeContext, interval: u64) {
        ctx.set(&self.inner.poll_interval, interval);
    }

    /// Non-reactive projection of a scope.
    pub fn view(&self, key: &K) -> Option<ScopeView> {
        self.inner.core.lock().expect("ingress core").view(key)
    }

    /// The bounds in force.
    pub fn policy(&self) -> IngressPolicy {
        self.inner.core.lock().expect("ingress core").policy()
    }

    /// Every known scope key.
    pub fn scope_keys(&self) -> Vec<K> {
        self.inner.core.lock().expect("ingress core").scope_keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress_core::{InProcIngress, IngressDropReason, IngressReceiptOutcome};
    use crate::merge::Sum;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type Cell = ThreadSafeIngressCell<&'static str, u64, Sum>;

    fn cell(ctx: &ThreadSafeContext, policy: IngressPolicy) -> Cell {
        ThreadSafeIngressCell::new(ctx, policy, IngressTransportKind::EventChannel, 25)
            .expect("policy")
    }

    fn env(
        key: &'static str,
        generation: u64,
        sequence: u64,
        stamped_at: u64,
        payload: u64,
    ) -> IngressEnvelope<&'static str, u64> {
        IngressEnvelope::new(key, generation, sequence, stamped_at, payload)
    }

    #[test]
    fn delivery_is_visible_through_the_value_reader() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        assert_eq!(ingress.value(&ctx, &"a"), None);
        ingress.admit(&ctx, env("a", 1, 0, 0, 5));
        assert_eq!(ingress.value(&ctx, &"a"), Some(5));
        ingress.admit(&ctx, env("a", 1, 1, 0, 7));
        assert_eq!(ingress.value(&ctx, &"a"), Some(12));
        assert_eq!(ingress.drain(&ctx, &"a"), Some(12));
        assert_eq!(ingress.value(&ctx, &"a"), None);
    }

    #[test]
    fn readiness_authority_and_retry_are_derives() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(
            &ctx,
            IngressPolicy {
                freshness_horizon: 10,
                retry_base: 4,
                ..IngressPolicy::default()
            },
        );
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Unknown);
        ingress.open(&ctx, "a", 3);
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Warming);
        ingress.admit(&ctx, env("a", 3, 0, 5, 1));
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Ready);
        assert_eq!(
            ingress.authority(&ctx, &"a"),
            Some(IngressAuthority {
                generation: 3,
                delivered_through: Some(0),
                stamped_at: 5
            })
        );
        ingress.tick(&ctx, 100);
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Stale);

        ingress.fail(&ctx, &"a", IngressError::TransportClosed);
        assert_eq!(ingress.retry(&ctx, &"a").expect("retry").backoff, 4);
    }

    #[test]
    fn a_generation_handoff_lands_value_and_authority_in_one_frontier_walk() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 5));

        let value = ingress.value_handle(&ctx, &"a");
        let authority = ingress.authority_handle(&ctx, &"a");
        let runs = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let effect = {
            let runs = Arc::clone(&runs);
            let seen = Arc::clone(&seen);
            ctx.effect(move |c| {
                runs.fetch_add(1, Ordering::SeqCst);
                let v = c.get(&value);
                let a = c.get(&authority).map(|a| a.generation);
                seen.lock().expect("seen").push((v, a));
            })
        };
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        ingress.admit(&ctx, env("a", 2, 0, 0, 9));
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "one admission is one effect run, not one per reader kind"
        );
        assert_eq!(
            *seen.lock().expect("seen"),
            vec![(Some(5), Some(1)), (Some(9), Some(2))]
        );
        ctx.dispose_effect(&effect);
    }

    #[test]
    fn a_buffered_envelope_and_an_in_horizon_tick_invalidate_nothing() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(
            &ctx,
            IngressPolicy {
                freshness_horizon: 100,
                ..IngressPolicy::default()
            },
        );
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));

        let value = ingress.value_handle(&ctx, &"a");
        let readiness = ingress.readiness_handle(&ctx, &"a");
        let runs = Arc::new(AtomicUsize::new(0));
        let effect = {
            let runs = Arc::clone(&runs);
            ctx.effect(move |c| {
                runs.fetch_add(1, Ordering::SeqCst);
                let _ = c.get(&value);
                let _ = c.get(&readiness);
            })
        };
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        ingress.admit(&ctx, env("a", 1, 3, 0, 8));
        ingress.tick(&ctx, 50);
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        ingress.tick(&ctx, 500);
        assert_eq!(runs.load(Ordering::SeqCst), 2, "the horizon crossing shows");
        ctx.dispose_effect(&effect);
    }

    #[test]
    fn receipt_channels_are_independent_readers() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 2, 0, 0, 1));
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));
        ingress.fail(&ctx, &"a", IngressError::DecodeFailed);
        assert_eq!(ingress.accepted(&ctx).len(), 1);
        let dropped = ingress.dropped(&ctx);
        assert_eq!(dropped.len(), 1);
        assert_eq!(
            dropped[0].outcome,
            IngressReceiptOutcome::Dropped(IngressDropReason::StaleGeneration)
        );
        assert_eq!(ingress.errors(&ctx).len(), 1);
    }

    #[test]
    fn the_schedule_derives_from_the_transport_and_retunes_live() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        assert_eq!(ingress.schedule(&ctx).poll_interval, None);
        ingress.set_transport(&ctx, IngressTransportKind::BoundedPolling);
        assert_eq!(ingress.schedule(&ctx).poll_interval, Some(25));
        ingress.set_poll_interval(&ctx, 200);
        assert_eq!(ingress.schedule(&ctx).poll_interval, Some(200));
    }

    #[test]
    fn pump_admits_a_batch_and_requests_replay_for_a_surviving_gap() {
        let ctx = ThreadSafeContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        let mut transport = InProcIngress::new(IngressTransportKind::EventChannel);
        transport.push(env("a", 1, 0, 0, 1));
        transport.push(env("a", 1, 2, 0, 4));
        let outcomes = ingress.pump(&ctx, &mut transport);
        assert!(outcomes[0].is_delivered());
        assert_eq!(outcomes[1], IngressAdmission::Buffered { gap_from: 1 });
        assert_eq!(
            transport.replays(),
            &[(
                "a",
                ReplayRequest {
                    generation: 1,
                    from_sequence: 1
                }
            )]
        );
    }

    #[test]
    fn concurrent_producers_on_distinct_scopes_converge() {
        // Distinct keys are distinct admission planes, so N producer threads need
        // no coordination — and the shared receipt log still totals correctly.
        let ctx = Arc::new(ThreadSafeContext::new());
        let ingress: ThreadSafeIngressCell<u64, u64, Sum> = ThreadSafeIngressCell::new(
            &ctx,
            IngressPolicy::default(),
            IngressTransportKind::EventChannel,
            25,
        )
        .expect("policy");

        let mut handles = Vec::new();
        for key in 0u64..4 {
            let ctx = Arc::clone(&ctx);
            let ingress = ingress.clone();
            handles.push(std::thread::spawn(move || {
                for sequence in 0u64..8 {
                    ingress.admit(&ctx, IngressEnvelope::new(key, 1, sequence, 0, 1));
                }
            }));
        }
        for handle in handles {
            handle.join().expect("producer");
        }

        for key in 0u64..4 {
            assert_eq!(ingress.value(&ctx, &key), Some(8), "key {key}");
            assert_eq!(
                ingress.view(&key).expect("scope").delivered_through,
                Some(7)
            );
        }
        assert_eq!(ingress.accepted(&ctx).len(), 32);
    }

    /// The fan-out is atomic per OBSERVATION, and one observation is one graph
    /// read — not two.
    ///
    /// `#lzrstornfanout`. This gate used to spawn a reader that called `value()`
    /// and then `authority()` in a loop and rejected every pair but three. Those
    /// are two independent `ctx.get`s, each taking and releasing the context lock,
    /// so an admission landing between them is a plain read skew with nothing torn
    /// anywhere — and the shipped assertion rejected exactly the one skew its own
    /// read order can produce. It reddened CI roughly once in many runs.
    ///
    /// The reader here takes the pair through a SINGLE derived cell, so both reads
    /// happen in one compute under one context-lock hold. That is the boundary the
    /// batch actually guarantees, and it is guaranteed, not lucky: the first read
    /// is ordered before the admission by a barrier, so the pre-handoff state is
    /// always observed, and the post-handoff state is asserted after the join.
    #[test]
    fn a_single_observation_never_straddles_a_generation_handoff() {
        let ctx = Arc::new(ThreadSafeContext::new());
        let ingress: ThreadSafeIngressCell<&'static str, u64, Sum> = ThreadSafeIngressCell::new(
            &ctx,
            IngressPolicy::default(),
            IngressTransportKind::EventChannel,
            25,
        )
        .expect("policy");
        ingress.admit(&ctx, env("a", 1, 0, 0, 5));

        let value = ingress.value_handle(&ctx, &"a");
        let authority = ingress.authority_handle(&ctx, &"a");
        let pair = ctx.computed(move |c| (c.get(&value), c.get(&authority).map(|a| a.generation)));

        let first_read_taken = Arc::new(std::sync::Barrier::new(2));
        let saw_old = Arc::new(AtomicUsize::new(0));
        let reader = {
            let ctx = Arc::clone(&ctx);
            let first_read_taken = Arc::clone(&first_read_taken);
            let saw_old = Arc::clone(&saw_old);
            std::thread::spawn(move || {
                for iteration in 0..500 {
                    // Generation 2's baseline is 9; generation 1's window is 5.
                    match ctx.get(&pair) {
                        (Some(5), Some(1)) => {
                            saw_old.fetch_add(1, Ordering::SeqCst);
                        }
                        (Some(9), Some(2)) => {}
                        other => panic!("partial fan-out observed: {other:?}"),
                    }
                    if iteration == 0 {
                        first_read_taken.wait();
                    }
                }
            })
        };
        first_read_taken.wait();
        ingress.admit(&ctx, env("a", 2, 0, 0, 9));
        reader.join().expect("reader");

        assert!(
            saw_old.load(Ordering::SeqCst) >= 1,
            "the pre-handoff state must actually be observed — a gate that only \
             ever sees the settled state proves nothing"
        );
        assert_eq!(
            ctx.get(&pair),
            (Some(9), Some(2)),
            "the handoff must be visible once it has landed"
        );
    }

    /// The other half of `#lzrstornfanout`: two sequential reads are NOT a
    /// snapshot, and the skew goes whichever way the reads are ordered.
    ///
    /// Deterministic — the reader is parked between its two reads until the
    /// handoff has fully landed — so this is a standing statement of where the
    /// atomicity boundary is, not a race. It is the counter-example to re-adding
    /// a two-read racing assertion: BOTH mixed pairs are reachable this way,
    /// including `new value, old authority`, which no amount of batching in the
    /// shell can prevent.
    #[test]
    fn two_sequential_reads_are_not_a_snapshot_and_skew_either_way() {
        for authority_first in [false, true] {
            let ctx = Arc::new(ThreadSafeContext::new());
            let ingress: ThreadSafeIngressCell<&'static str, u64, Sum> =
                ThreadSafeIngressCell::new(
                    &ctx,
                    IngressPolicy::default(),
                    IngressTransportKind::EventChannel,
                    25,
                )
                .expect("policy");
            ingress.admit(&ctx, env("a", 1, 0, 0, 5));

            let first_read_taken = Arc::new(std::sync::Barrier::new(2));
            let handoff_landed = Arc::new(std::sync::Barrier::new(2));
            let reader = {
                let ctx = Arc::clone(&ctx);
                let ingress = ingress.clone();
                let first_read_taken = Arc::clone(&first_read_taken);
                let handoff_landed = Arc::clone(&handoff_landed);
                std::thread::spawn(move || {
                    if authority_first {
                        let generation = ingress.authority(&ctx, &"a").map(|a| a.generation);
                        first_read_taken.wait();
                        handoff_landed.wait();
                        (ingress.value(&ctx, &"a"), generation)
                    } else {
                        let value = ingress.value(&ctx, &"a");
                        first_read_taken.wait();
                        handoff_landed.wait();
                        (value, ingress.authority(&ctx, &"a").map(|a| a.generation))
                    }
                })
            };
            first_read_taken.wait();
            ingress.admit(&ctx, env("a", 2, 0, 0, 9));
            handoff_landed.wait();

            let observed = reader.join().expect("reader");
            let expected = if authority_first {
                // New value under the superseded fence — the pair the spec's
                // prose names, reachable with nothing torn in the shell.
                (Some(9), Some(1))
            } else {
                (Some(5), Some(2))
            };
            assert_eq!(
                observed, expected,
                "authority_first={authority_first}: two `ctx.get`s are two \
                 observations, so an admission between them skews the pair"
            );
        }
    }
}
