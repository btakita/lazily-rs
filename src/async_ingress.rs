//! `AsyncIngressCell` — the `AsyncContext` flavor of [`IngressCell`]
//! (`#designimplementtransport`).
//!
//! Completes the ingress triple. The shell is deliberately the same shape as the
//! thread-safe one, over the same flavor-neutral
//! [`IngressCore`](crate::ingress_core), because the family's claim is that the
//! three flavors obey ONE contract.
//!
//! **Admission is not async-coloured.** This is the finding, not an oversight.
//! Whether an envelope is admissible is a function of the scope's fence,
//! watermark, reorder buffer, and observed clock — state the graph does not own
//! and nothing has to await. Routing the readers through `computed_async` would
//! make every `value()` return `Option` and need a settle step to observe a value
//! already known synchronously, so the reader kinds here use
//! [`AsyncContext::computed`] (sync compute, async graph) and return plain values
//! exactly like the other two flavors. The *transport* is where awaiting belongs,
//! and the transport is outside the primitive by construction.
//!
//! Lock discipline is the thread-safe shell's, for the same reason: a reader's
//! compute takes the context lock and then `core`, so an op must release `core`
//! before invalidating or the two orders deadlock. Multi-root fan-out goes through
//! [`AsyncContext::clear_slots`], so one admission clears the scope's value,
//! readiness, authority, retry, and receipt channel in a single frontier walk.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext, AsyncSource};
use crate::context::SlotId;
use crate::ingress_core::{
    IngressAdmission, IngressAuthority, IngressChange, IngressConfigError, IngressCore,
    IngressEnvelope, IngressError, IngressPolicy, IngressReadiness, IngressReceipt,
    IngressReceiptChannel, IngressRetry, IngressSchedule, IngressTransport, IngressTransportKind,
    ReplayRequest, ScopeView,
};
use crate::merge::MergePolicy;

/// The four reader kinds one keyed scope exposes.
struct ScopeReaders<T> {
    value: AsyncComputed<Option<T>>,
    readiness: AsyncComputed<IngressReadiness>,
    authority: AsyncComputed<Option<IngressAuthority>>,
    retry: AsyncComputed<Option<IngressRetry>>,
}

impl<T> Clone for ScopeReaders<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ScopeReaders<T> {}

struct Inner<K, T, M> {
    core: Arc<Mutex<IngressCore<K, T, M>>>,
    scopes: Mutex<HashMap<K, ScopeReaders<T>>>,
    accepted: AsyncComputed<Vec<IngressReceipt<K>>>,
    dropped: AsyncComputed<Vec<IngressReceipt<K>>>,
    errors: AsyncComputed<Vec<IngressReceipt<K>>>,
    transport_kind: AsyncSource<IngressTransportKind>,
    poll_interval: AsyncSource<u64>,
    schedule: AsyncComputed<IngressSchedule>,
}

/// An `AsyncContext` keyed, lifecycle-scoped reactive ingress: one admission
/// plane per key, with readiness, authority, and retry as derives rather than
/// calls.
pub struct AsyncIngressCell<K, T, M> {
    inner: Arc<Inner<K, T, M>>,
}

impl<K, T, M> Clone for AsyncIngressCell<K, T, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, T, M> AsyncIngressCell<K, T, M>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: PartialEq + Clone + Send + Sync + 'static,
    M: MergePolicy<T> + Send + Sync + 'static,
{
    /// Build an ingress over `policy`, delivering as `kind`.
    pub fn new(
        ctx: &AsyncContext,
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
        ctx: &AsyncContext,
        core: &Arc<Mutex<IngressCore<K, T, M>>>,
        channel: IngressReceiptChannel,
    ) -> AsyncComputed<Vec<IngressReceipt<K>>> {
        let core = Arc::clone(core);
        ctx.computed(move |_| core.lock().expect("ingress core").receipts(channel))
    }

    /// Mint (or return) one scope's four readers. Holds `scopes` across the mint
    /// and never touches `core`, so it cannot invert the lock order.
    fn ensure_readers(&self, ctx: &AsyncContext, key: K) -> ScopeReaders<T> {
        let mut scopes = self.inner.scopes.lock().expect("ingress scopes");
        if let Some(readers) = scopes.get(&key) {
            return *readers;
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
        scopes.insert(key, readers);
        readers
    }

    /// Apply one core-reported invalidation set in a single frontier walk.
    fn apply(&self, ctx: &AsyncContext, change: IngressChange<K>) {
        if change.is_empty() {
            return;
        }
        let mut roots: Vec<SlotId> = Vec::new();
        for (key, scope_change) in &change.scopes {
            let readers = self.ensure_readers(ctx, key.clone());
            if scope_change.value {
                roots.push(readers.value.id());
            }
            if scope_change.readiness {
                roots.push(readers.readiness.id());
            }
            if scope_change.authority {
                roots.push(readers.authority.id());
            }
            if scope_change.retry {
                roots.push(readers.retry.id());
            }
        }
        if change.accepted_receipts {
            roots.push(self.inner.accepted.id());
        }
        if change.dropped_receipts {
            roots.push(self.inner.dropped.id());
        }
        if change.error_receipts {
            roots.push(self.inner.errors.id());
        }
        if !roots.is_empty() {
            ctx.clear_slots(&roots);
        }
    }

    /// Open (or reopen) a keyed scope at `generation`.
    pub fn open(&self, ctx: &AsyncContext, key: K, generation: u64) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.open(key, generation)
        };
        self.apply(ctx, change);
    }

    /// Admit one decoded envelope.
    pub fn admit(&self, ctx: &AsyncContext, envelope: IngressEnvelope<K, T>) -> IngressAdmission {
        let (change, admission) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.admit(envelope)
        };
        self.apply(ctx, change);
        admission
    }

    /// Suspend a scope, retaining its watermark.
    pub fn suspend(&self, ctx: &AsyncContext, key: &K) -> Option<ReplayRequest> {
        let (change, request) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.suspend(key)
        };
        self.apply(ctx, change);
        request
    }

    /// Reconnect a scope at `generation`, clearing its error streak.
    pub fn reconnect(&self, ctx: &AsyncContext, key: &K, generation: u64) -> ReplayRequest {
        let (change, request) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.reconnect(key, generation)
        };
        self.apply(ctx, change);
        request
    }

    /// Close a scope.
    pub fn close(&self, ctx: &AsyncContext, key: &K) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.close(key)
        };
        self.apply(ctx, change);
    }

    /// Record a transport/decode failure, deepening the scope's backoff.
    pub fn fail(&self, ctx: &AsyncContext, key: &K, error: IngressError) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.fail(key, error)
        };
        self.apply(ctx, change);
    }

    /// Advance logical time.
    pub fn tick(&self, ctx: &AsyncContext, now: u64) {
        let change = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.tick(now)
        };
        self.apply(ctx, change);
    }

    /// Drain a scope's coalesced window.
    pub fn drain(&self, ctx: &AsyncContext, key: &K) -> Option<T> {
        let (change, value) = {
            let mut core = self.inner.core.lock().expect("ingress core");
            core.drain(key)
        };
        self.apply(ctx, change);
        value
    }

    /// Admit everything `transport` has decoded, then ask it to replay any gap
    /// still open.
    pub fn pump<Tr>(&self, ctx: &AsyncContext, transport: &mut Tr) -> Vec<IngressAdmission>
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
    pub fn value(&self, ctx: &AsyncContext, key: &K) -> Option<T> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.value)
            .expect("sync compute resolves inline")
    }

    /// Reactive read: derived readiness.
    pub fn readiness(&self, ctx: &AsyncContext, key: &K) -> IngressReadiness {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.readiness)
            .expect("sync compute resolves inline")
    }

    /// Reactive read: derived authority.
    pub fn authority(&self, ctx: &AsyncContext, key: &K) -> Option<IngressAuthority> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.authority)
            .expect("sync compute resolves inline")
    }

    /// Reactive read: derived retry decision.
    pub fn retry(&self, ctx: &AsyncContext, key: &K) -> Option<IngressRetry> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.retry)
            .expect("sync compute resolves inline")
    }

    /// Handle for the scope's value reader.
    pub fn value_handle(&self, ctx: &AsyncContext, key: &K) -> AsyncComputed<Option<T>> {
        self.ensure_readers(ctx, key.clone()).value
    }

    /// Handle for the scope's readiness reader.
    pub fn readiness_handle(&self, ctx: &AsyncContext, key: &K) -> AsyncComputed<IngressReadiness> {
        self.ensure_readers(ctx, key.clone()).readiness
    }

    /// Handle for the scope's authority reader.
    pub fn authority_handle(
        &self,
        ctx: &AsyncContext,
        key: &K,
    ) -> AsyncComputed<Option<IngressAuthority>> {
        self.ensure_readers(ctx, key.clone()).authority
    }

    /// Handle for the scope's retry reader.
    pub fn retry_handle(&self, ctx: &AsyncContext, key: &K) -> AsyncComputed<Option<IngressRetry>> {
        self.ensure_readers(ctx, key.clone()).retry
    }

    /// Reactive read: accepted receipts, oldest first.
    pub fn accepted(&self, ctx: &AsyncContext) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.accepted)
            .expect("sync compute resolves inline")
    }

    /// Reactive read: dropped receipts, oldest first.
    pub fn dropped(&self, ctx: &AsyncContext) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.dropped)
            .expect("sync compute resolves inline")
    }

    /// Reactive read: error receipts, oldest first.
    pub fn errors(&self, ctx: &AsyncContext) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.errors)
            .expect("sync compute resolves inline")
    }

    /// Handle for the accepted-receipt reader.
    pub fn accepted_handle(&self) -> AsyncComputed<Vec<IngressReceipt<K>>> {
        self.inner.accepted
    }

    /// Handle for the dropped-receipt reader.
    pub fn dropped_handle(&self) -> AsyncComputed<Vec<IngressReceipt<K>>> {
        self.inner.dropped
    }

    /// Handle for the error-receipt reader.
    pub fn errors_handle(&self) -> AsyncComputed<Vec<IngressReceipt<K>>> {
        self.inner.errors
    }

    /// Reactive read: the derived delivery schedule.
    pub fn schedule(&self, ctx: &AsyncContext) -> IngressSchedule {
        ctx.get(&self.inner.schedule)
            .expect("sync compute resolves inline")
    }

    /// Handle for the schedule reader.
    pub fn schedule_handle(&self) -> AsyncComputed<IngressSchedule> {
        self.inner.schedule
    }

    /// Retune the transport live.
    pub fn set_transport(&self, ctx: &AsyncContext, kind: IngressTransportKind) {
        ctx.set(&self.inner.transport_kind, kind);
    }

    /// Retune the poll bound live.
    pub fn set_poll_interval(&self, ctx: &AsyncContext, interval: u64) {
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

    type Cell = AsyncIngressCell<&'static str, u64, Sum>;

    fn cell(ctx: &AsyncContext, policy: IngressPolicy) -> Cell {
        AsyncIngressCell::new(ctx, policy, IngressTransportKind::EventChannel, 25).expect("policy")
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
    fn readers_return_plain_values_with_no_settle_step() {
        // The whole point of the async shell's shape: nothing here is
        // async-coloured, so a value is observable without driving the graph.
        let ctx = AsyncContext::new();
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
        let ctx = AsyncContext::new();
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
    fn reorder_duplication_and_fencing_match_the_other_flavors() {
        let ctx = AsyncContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        assert_eq!(
            ingress.admit(&ctx, env("a", 1, 2, 0, 4)),
            IngressAdmission::Buffered { gap_from: 0 }
        );
        assert_eq!(
            ingress.admit(&ctx, env("a", 1, 2, 0, 4)),
            IngressAdmission::Dropped(IngressDropReason::DuplicateBuffered)
        );
        ingress.admit(&ctx, env("a", 1, 1, 0, 2));
        assert_eq!(
            ingress.admit(&ctx, env("a", 1, 0, 0, 1)),
            IngressAdmission::Conflated {
                delivered_through: 2
            }
        );
        assert_eq!(ingress.value(&ctx, &"a"), Some(7));
        assert_eq!(
            ingress.admit(&ctx, env("a", 1, 0, 0, 1)),
            IngressAdmission::Dropped(IngressDropReason::DuplicateSequence)
        );
    }

    #[test]
    fn a_generation_handoff_resets_the_baseline() {
        let ctx = AsyncContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 5));
        assert_eq!(
            ingress.admit(&ctx, env("a", 2, 0, 0, 9)),
            IngressAdmission::GenerationHandoff { from: 1, to: 2 }
        );
        assert_eq!(ingress.value(&ctx, &"a"), Some(9));
        assert_eq!(
            ingress.authority(&ctx, &"a").expect("authority").generation,
            2
        );
    }

    #[test]
    fn receipt_channels_are_independent_readers() {
        let ctx = AsyncContext::new();
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
        let ctx = AsyncContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        assert_eq!(ingress.schedule(&ctx).poll_interval, None);
        ingress.set_transport(&ctx, IngressTransportKind::BoundedPolling);
        assert_eq!(ingress.schedule(&ctx).poll_interval, Some(25));
        ingress.set_poll_interval(&ctx, 200);
        assert_eq!(ingress.schedule(&ctx).poll_interval, Some(200));
        ingress.set_transport(&ctx, IngressTransportKind::EventChannel);
        assert_eq!(ingress.schedule(&ctx).poll_interval, None);
    }

    #[test]
    fn pump_admits_a_batch_and_requests_replay_for_a_surviving_gap() {
        let ctx = AsyncContext::new();
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
    fn suspend_and_reconnect_report_the_gap() {
        let ctx = AsyncContext::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));
        ingress.admit(&ctx, env("a", 1, 1, 0, 1));
        assert_eq!(
            ingress.suspend(&ctx, &"a"),
            Some(ReplayRequest {
                generation: 1,
                from_sequence: 2
            })
        );
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Suspended);
        assert_eq!(ingress.reconnect(&ctx, &"a", 1).from_sequence, 2);
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Ready);
    }
}
