//! `IngressCell` — the single-threaded flavor of the transport-agnostic reactive
//! ingress family (`#designimplementtransport`).
//!
//! The admission algebra lives in the flavor-neutral
//! [`IngressCore`](crate::ingress_core); this shell adds only the reactivity —
//! four memoized `Computed`s per keyed scope plus three receipt readers, minted
//! on *this* context's graph.
//!
//! # Readiness, authority, and retry are derives, not refresh calls
//!
//! The point of the family: nothing here polls a connection to find out whether
//! it is healthy. [`readiness`](IngressCell::readiness),
//! [`authority`](IngressCell::authority), and [`retry`](IngressCell::retry) are
//! `Computed`s over scope state, so a consumer that reads readiness is a graph
//! dependent of exactly the transitions that can change it — and a transition
//! that cannot (a buffered out-of-order envelope, a tick inside the freshness
//! horizon) invalidates nothing. `IngressCore` returns the invalidation set for
//! every transition, and this shell clears precisely that set.
//!
//! # Why four reader kinds per scope and not one
//!
//! Collapsing them into one reader would make an error deepen a backoff *and*
//! re-render a value that did not change. The four boundaries are distinct in the
//! algebra ([`IngressScopeChange`](crate::ingress_core::IngressScopeChange)), so
//! they are distinct here.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

use crate::Context;
use crate::cell::{Computed, Source};
use crate::ingress_core::{
    IngressAdmission, IngressAuthority, IngressChange, IngressConfigError, IngressCore,
    IngressEnvelope, IngressError, IngressPolicy, IngressReadiness, IngressReceipt,
    IngressReceiptChannel, IngressRetry, IngressSchedule, IngressScopeChange, IngressTransport,
    IngressTransportKind, ReplayRequest, ScopeView,
};
use crate::merge::MergePolicy;

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
    core: Rc<RefCell<IngressCore<K, T, M>>>,
    scopes: RefCell<HashMap<K, ScopeReaders<T>>>,
    accepted: Computed<Vec<IngressReceipt<K>>>,
    dropped: Computed<Vec<IngressReceipt<K>>>,
    errors: Computed<Vec<IngressReceipt<K>>>,
    transport_kind: Source<IngressTransportKind>,
    poll_interval: Source<u64>,
    schedule: Computed<IngressSchedule>,
}

/// A keyed, lifecycle-scoped reactive ingress: one admission plane per key, with
/// readiness, authority, and retry as derives rather than calls.
pub struct IngressCell<K, T, M> {
    inner: Rc<Inner<K, T, M>>,
}

impl<K, T, M> Clone for IngressCell<K, T, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<K, T, M> IngressCell<K, T, M>
where
    K: Eq + Hash + Clone + 'static,
    T: PartialEq + Clone + 'static,
    M: MergePolicy<T> + 'static,
{
    /// Build an ingress over `policy`, delivering as `kind`.
    ///
    /// `poll_interval` is retained even for an event channel so a later
    /// [`set_transport`](Self::set_transport) to `BoundedPolling` has a bound to
    /// fall back to rather than inventing one.
    pub fn new(
        ctx: &Context,
        policy: IngressPolicy,
        kind: IngressTransportKind,
        poll_interval: u64,
    ) -> Result<Self, IngressConfigError> {
        let core = Rc::new(RefCell::new(IngressCore::<K, T, M>::new(policy)?));
        let accepted = Self::receipt_reader(ctx, &core, IngressReceiptChannel::Accepted);
        let dropped = Self::receipt_reader(ctx, &core, IngressReceiptChannel::Dropped);
        let errors = Self::receipt_reader(ctx, &core, IngressReceiptChannel::Error);
        let transport_kind = ctx.source(kind);
        let poll_interval = ctx.source(poll_interval);
        let schedule = ctx.computed(move |c| {
            IngressSchedule::for_kind(transport_kind.get(c), poll_interval.get(c))
        });
        Ok(Self {
            inner: Rc::new(Inner {
                core,
                scopes: RefCell::new(HashMap::new()),
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
        ctx: &Context,
        core: &Rc<RefCell<IngressCore<K, T, M>>>,
        channel: IngressReceiptChannel,
    ) -> Computed<Vec<IngressReceipt<K>>> {
        let core = Rc::clone(core);
        ctx.computed(move |_| core.borrow().receipts(channel))
    }

    /// Mint (or return) one scope's four readers. Idempotent, so a consumer may
    /// hold a handle for a key that has not opened yet.
    fn ensure_readers(&self, ctx: &Context, key: K) -> ScopeReaders<T> {
        if let Some(readers) = self.inner.scopes.borrow().get(&key) {
            return readers.clone();
        }
        let readers = ScopeReaders {
            value: {
                let core = Rc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.borrow().peek(&key))
            },
            readiness: {
                let core = Rc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.borrow().readiness(&key))
            },
            authority: {
                let core = Rc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.borrow().authority(&key))
            },
            retry: {
                let core = Rc::clone(&self.inner.core);
                let key = key.clone();
                ctx.computed(move |_| core.borrow().retry(&key))
            },
        };
        self.inner.scopes.borrow_mut().insert(key, readers.clone());
        readers
    }

    /// Apply one core-reported invalidation set. Every affected slot is cleared
    /// in a single frontier walk, so no reader observes a partial fan-out — a
    /// generation handoff must not be visible as "new value, old authority".
    fn apply(&self, ctx: &Context, change: IngressChange<K>) {
        if change.is_empty() {
            return;
        }
        let mut roots = Vec::new();
        for (key, scope_change) in &change.scopes {
            let readers = self.ensure_readers(ctx, key.clone());
            Self::collect_scope_roots(&mut roots, &readers, *scope_change);
        }
        if change.accepted_receipts {
            roots.push(self.inner.accepted.id);
        }
        if change.dropped_receipts {
            roots.push(self.inner.dropped.id);
        }
        if change.error_receipts {
            roots.push(self.inner.errors.id);
        }
        if !roots.is_empty() {
            ctx.clear_slots(&roots);
        }
    }

    fn collect_scope_roots(
        roots: &mut Vec<crate::context::SlotId>,
        readers: &ScopeReaders<T>,
        change: IngressScopeChange,
    ) {
        if change.value {
            roots.push(readers.value.id);
        }
        if change.readiness {
            roots.push(readers.readiness.id);
        }
        if change.authority {
            roots.push(readers.authority.id);
        }
        if change.retry {
            roots.push(readers.retry.id);
        }
    }

    /// Open (or reopen) a keyed scope at `generation`.
    pub fn open(&self, ctx: &Context, key: K, generation: u64) {
        let change = self.inner.core.borrow_mut().open(key, generation);
        self.apply(ctx, change);
    }

    /// Admit one decoded envelope.
    pub fn admit(&self, ctx: &Context, envelope: IngressEnvelope<K, T>) -> IngressAdmission {
        let (change, admission) = self.inner.core.borrow_mut().admit(envelope);
        self.apply(ctx, change);
        admission
    }

    /// Suspend a scope, retaining its watermark. Returns the replay request a
    /// reconnect will need.
    pub fn suspend(&self, ctx: &Context, key: &K) -> Option<ReplayRequest> {
        let (change, request) = self.inner.core.borrow_mut().suspend(key);
        self.apply(ctx, change);
        request
    }

    /// Reconnect a scope at `generation`, clearing its error streak.
    pub fn reconnect(&self, ctx: &Context, key: &K, generation: u64) -> ReplayRequest {
        let (change, request) = self.inner.core.borrow_mut().reconnect(key, generation);
        self.apply(ctx, change);
        request
    }

    /// Close a scope. It admits nothing and claims no authority until reopened.
    pub fn close(&self, ctx: &Context, key: &K) {
        let change = self.inner.core.borrow_mut().close(key);
        self.apply(ctx, change);
    }

    /// Record a transport/decode failure, deepening the scope's backoff.
    pub fn fail(&self, ctx: &Context, key: &K, error: IngressError) {
        let change = self.inner.core.borrow_mut().fail(key, error);
        self.apply(ctx, change);
    }

    /// Advance logical time. Only scopes that crossed the freshness horizon are
    /// invalidated.
    pub fn tick(&self, ctx: &Context, now: u64) {
        let change = self.inner.core.borrow_mut().tick(now);
        self.apply(ctx, change);
    }

    /// Drain a scope's coalesced window.
    pub fn drain(&self, ctx: &Context, key: &K) -> Option<T> {
        let (change, value) = self.inner.core.borrow_mut().drain(key);
        self.apply(ctx, change);
        value
    }

    /// Admit everything `transport` has decoded, then ask it to replay any gap
    /// still open. Returns the admission outcomes in arrival order.
    ///
    /// This is the only method that touches a transport, and it makes no decision
    /// of its own: the gap it replays is the one the algebra reports.
    pub fn pump<Tr>(&self, ctx: &Context, transport: &mut Tr) -> Vec<IngressAdmission>
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
            let gap = self
                .inner
                .core
                .borrow()
                .view(&key)
                .filter(ScopeView::has_gap)
                .map(|view| ReplayRequest {
                    generation: view.generation,
                    from_sequence: view.resume_from(),
                });
            if let Some(request) = gap {
                transport.request_replay(&key, request);
            }
        }
        outcomes
    }

    /// Reactive read: the coalesced window awaiting drain.
    pub fn value(&self, ctx: &Context, key: &K) -> Option<T> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.value)
    }

    /// Reactive read: derived readiness.
    pub fn readiness(&self, ctx: &Context, key: &K) -> IngressReadiness {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.readiness)
    }

    /// Reactive read: derived authority.
    pub fn authority(&self, ctx: &Context, key: &K) -> Option<IngressAuthority> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.authority)
    }

    /// Reactive read: derived retry decision.
    pub fn retry(&self, ctx: &Context, key: &K) -> Option<IngressRetry> {
        let readers = self.ensure_readers(ctx, key.clone());
        ctx.get(&readers.retry)
    }

    /// Handle for the scope's value reader, for composing further derives.
    pub fn value_handle(&self, ctx: &Context, key: &K) -> Computed<Option<T>> {
        self.ensure_readers(ctx, key.clone()).value
    }

    /// Handle for the scope's readiness reader.
    pub fn readiness_handle(&self, ctx: &Context, key: &K) -> Computed<IngressReadiness> {
        self.ensure_readers(ctx, key.clone()).readiness
    }

    /// Handle for the scope's authority reader.
    pub fn authority_handle(&self, ctx: &Context, key: &K) -> Computed<Option<IngressAuthority>> {
        self.ensure_readers(ctx, key.clone()).authority
    }

    /// Handle for the scope's retry reader.
    pub fn retry_handle(&self, ctx: &Context, key: &K) -> Computed<Option<IngressRetry>> {
        self.ensure_readers(ctx, key.clone()).retry
    }

    /// Reactive read: accepted receipts, oldest first.
    pub fn accepted(&self, ctx: &Context) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.accepted)
    }

    /// Reactive read: dropped receipts, oldest first.
    pub fn dropped(&self, ctx: &Context) -> Vec<IngressReceipt<K>> {
        ctx.get(&self.inner.dropped)
    }

    /// Reactive read: error receipts, oldest first.
    pub fn errors(&self, ctx: &Context) -> Vec<IngressReceipt<K>> {
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
    pub fn schedule(&self, ctx: &Context) -> IngressSchedule {
        ctx.get(&self.inner.schedule)
    }

    /// Handle for the schedule reader.
    pub fn schedule_handle(&self) -> Computed<IngressSchedule> {
        self.inner.schedule
    }

    /// Retune the transport live: falling back from an event channel to bounded
    /// polling is a cell write, so every schedule dependent reacts.
    pub fn set_transport(&self, ctx: &Context, kind: IngressTransportKind) {
        self.inner.transport_kind.set(ctx, kind);
    }

    /// Retune the poll bound live.
    pub fn set_poll_interval(&self, ctx: &Context, interval: u64) {
        self.inner.poll_interval.set(ctx, interval);
    }

    /// Non-reactive projection of a scope, for assertions and diagnostics.
    pub fn view(&self, key: &K) -> Option<ScopeView> {
        self.inner.core.borrow().view(key)
    }

    /// The bounds in force.
    pub fn policy(&self) -> IngressPolicy {
        self.inner.core.borrow().policy()
    }

    /// Every known scope key.
    pub fn scope_keys(&self) -> Vec<K> {
        self.inner.core.borrow().scope_keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress_core::{InProcIngress, IngressDropReason, IngressReceiptOutcome};
    use crate::merge::Sum;

    type Cell = IngressCell<&'static str, u64, Sum>;

    fn cell(ctx: &Context, policy: IngressPolicy) -> Cell {
        IngressCell::new(ctx, policy, IngressTransportKind::EventChannel, 25).expect("policy")
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
        let ctx = Context::new();
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
    fn readiness_and_authority_are_derives_of_the_same_transitions() {
        let ctx = Context::new();
        let ingress = cell(
            &ctx,
            IngressPolicy {
                freshness_horizon: 10,
                ..IngressPolicy::default()
            },
        );
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Unknown);
        assert_eq!(ingress.authority(&ctx, &"a"), None);

        ingress.open(&ctx, "a", 4);
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Warming);
        assert_eq!(
            ingress.authority(&ctx, &"a"),
            Some(IngressAuthority {
                generation: 4,
                delivered_through: None,
                stamped_at: 0
            })
        );

        ingress.admit(&ctx, env("a", 4, 0, 5, 1));
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Ready);
        assert_eq!(
            ingress.authority(&ctx, &"a"),
            Some(IngressAuthority {
                generation: 4,
                delivered_through: Some(0),
                stamped_at: 5
            })
        );

        ingress.tick(&ctx, 100);
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Stale);
    }

    #[test]
    fn a_buffered_envelope_reruns_no_effect() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.open(&ctx, "a", 1);

        let value = ingress.value_handle(&ctx, &"a");
        let runs = Rc::new(RefCell::new(0usize));
        let observed = Rc::new(RefCell::new(Vec::new()));
        let effect = {
            let runs = Rc::clone(&runs);
            let observed = Rc::clone(&observed);
            ctx.effect(move |c| {
                *runs.borrow_mut() += 1;
                observed.borrow_mut().push(c.get(&value));
            })
        };
        assert_eq!(*runs.borrow(), 1);

        // Out of order: nothing observable moved, so the value effect must not run.
        ingress.admit(&ctx, env("a", 1, 2, 0, 4));
        ingress.admit(&ctx, env("a", 1, 1, 0, 2));
        assert_eq!(*runs.borrow(), 1);

        // The delivery that closes the gap flushes all three as ONE value change.
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));
        assert_eq!(*runs.borrow(), 2);
        assert_eq!(*observed.borrow(), vec![None, Some(7)]);
        effect.dispose(&ctx);
    }

    #[test]
    fn a_tick_inside_the_horizon_reruns_no_readiness_effect() {
        let ctx = Context::new();
        let ingress = cell(
            &ctx,
            IngressPolicy {
                freshness_horizon: 100,
                ..IngressPolicy::default()
            },
        );
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));

        let readiness = ingress.readiness_handle(&ctx, &"a");
        let runs = Rc::new(RefCell::new(0usize));
        let effect = {
            let runs = Rc::clone(&runs);
            ctx.effect(move |c| {
                *runs.borrow_mut() += 1;
                let _ = c.get(&readiness);
            })
        };
        assert_eq!(*runs.borrow(), 1);

        ingress.tick(&ctx, 50);
        assert_eq!(*runs.borrow(), 1, "tick inside the horizon is not a change");
        ingress.tick(&ctx, 500);
        assert_eq!(*runs.borrow(), 2, "crossing the horizon is a change");
        effect.dispose(&ctx);
    }

    #[test]
    fn an_error_moves_retry_without_touching_the_value() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 9));

        let value = ingress.value_handle(&ctx, &"a");
        let value_runs = Rc::new(RefCell::new(0usize));
        let value_effect = {
            let runs = Rc::clone(&value_runs);
            ctx.effect(move |c| {
                *runs.borrow_mut() += 1;
                let _ = c.get(&value);
            })
        };
        ingress.fail(&ctx, &"a", IngressError::TransportClosed);
        assert_eq!(*value_runs.borrow(), 1);
        assert_eq!(ingress.retry(&ctx, &"a").expect("retry").attempt, 1);
        assert_eq!(ingress.value(&ctx, &"a"), Some(9));
        value_effect.dispose(&ctx);
    }

    #[test]
    fn receipt_channels_are_independent_readers() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 2, 0, 0, 1));
        assert_eq!(ingress.accepted(&ctx).len(), 1);
        assert!(ingress.dropped(&ctx).is_empty());
        assert!(ingress.errors(&ctx).is_empty());

        // A fenced zombie shows up only on the dropped channel.
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));
        assert_eq!(ingress.accepted(&ctx).len(), 1);
        let dropped = ingress.dropped(&ctx);
        assert_eq!(dropped.len(), 1);
        assert_eq!(
            dropped[0].outcome,
            IngressReceiptOutcome::Dropped(IngressDropReason::StaleGeneration)
        );

        ingress.fail(&ctx, &"a", IngressError::DecodeFailed);
        assert_eq!(ingress.errors(&ctx).len(), 1);
        assert_eq!(ingress.dropped(&ctx).len(), 1);
    }

    #[test]
    fn the_schedule_derives_from_the_transport_and_retunes_live() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        assert_eq!(ingress.schedule(&ctx).poll_interval, None);

        ingress.set_transport(&ctx, IngressTransportKind::BoundedPolling);
        assert_eq!(ingress.schedule(&ctx).poll_interval, Some(25));
        ingress.set_poll_interval(&ctx, 200);
        assert_eq!(ingress.schedule(&ctx).poll_interval, Some(200));

        ingress.set_transport(&ctx, IngressTransportKind::RpcTriggered);
        assert_eq!(ingress.schedule(&ctx).poll_interval, None);
    }

    #[test]
    fn pump_admits_a_batch_and_requests_replay_for_a_surviving_gap() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        let mut transport = InProcIngress::new(IngressTransportKind::EventChannel);
        transport.push(env("a", 1, 0, 0, 1));
        transport.push(env("a", 1, 2, 0, 4));

        let outcomes = ingress.pump(&ctx, &mut transport);
        assert_eq!(outcomes.len(), 2);
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

        // The replay closes the gap, and a second pump asks for nothing more.
        transport.push(env("a", 1, 1, 0, 2));
        ingress.pump(&ctx, &mut transport);
        assert_eq!(ingress.value(&ctx, &"a"), Some(7));
        assert_eq!(transport.replays().len(), 1);
    }

    #[test]
    fn a_polling_transport_cannot_serve_a_replay() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        let mut transport = InProcIngress::new(IngressTransportKind::BoundedPolling);
        transport.push(env("a", 1, 3, 0, 1));
        ingress.pump(&ctx, &mut transport);
        assert!(transport.replays().is_empty());
    }

    #[test]
    fn a_generation_handoff_never_shows_a_new_value_with_stale_authority() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 5));

        let value_handle = ingress.value_handle(&ctx, &"a");
        let authority_handle = ingress.authority_handle(&ctx, &"a");
        let seen = Rc::new(RefCell::new(Vec::new()));
        let effect = {
            let seen = Rc::clone(&seen);
            ctx.effect(move |c| {
                let value = c.get(&value_handle);
                let authority = c.get(&authority_handle);
                seen.borrow_mut()
                    .push((value, authority.map(|a| a.generation)));
            })
        };
        ingress.admit(&ctx, env("a", 2, 0, 0, 9));
        // Value and authority land together — one frontier walk, one effect run.
        assert_eq!(*seen.borrow(), vec![(Some(5), Some(1)), (Some(9), Some(2))]);
        effect.dispose(&ctx);
    }

    #[test]
    fn scopes_do_not_invalidate_each_other() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));

        let value = ingress.value_handle(&ctx, &"a");
        let runs = Rc::new(RefCell::new(0usize));
        let effect = {
            let runs = Rc::clone(&runs);
            ctx.effect(move |c| {
                *runs.borrow_mut() += 1;
                let _ = c.get(&value);
            })
        };
        assert_eq!(*runs.borrow(), 1);
        ingress.admit(&ctx, env("b", 1, 0, 0, 2));
        ingress.close(&ctx, &"b");
        assert_eq!(*runs.borrow(), 1);
        assert_eq!(ingress.value(&ctx, &"a"), Some(1));
        effect.dispose(&ctx);
    }

    #[test]
    fn suspend_and_reconnect_move_readiness_and_report_the_gap() {
        let ctx = Context::new();
        let ingress = cell(&ctx, IngressPolicy::default());
        ingress.admit(&ctx, env("a", 1, 0, 0, 1));
        ingress.admit(&ctx, env("a", 1, 1, 0, 1));

        let request = ingress.suspend(&ctx, &"a").expect("request");
        assert_eq!(
            request,
            ReplayRequest {
                generation: 1,
                from_sequence: 2
            }
        );
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Suspended);
        assert_eq!(ingress.value(&ctx, &"a"), Some(2));

        let request = ingress.reconnect(&ctx, &"a", 1);
        assert_eq!(request.from_sequence, 2);
        assert_eq!(ingress.readiness(&ctx, &"a"), IngressReadiness::Ready);
    }
}
