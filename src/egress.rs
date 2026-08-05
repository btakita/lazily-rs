//! Reactive egress shell (`#lzegress`).
//!
//! The four projections are Computeds over [`EgressCore`]. Transport I/O lives
//! in exactly one Effect per attachment; the core remains pure and reusable by
//! the thread-safe and async shells.

use std::cell::RefCell;
use std::rc::Rc;

use crate::cell::Computed;
use crate::context::Context;
use crate::effect::Effect;
use crate::egress_core::{
    EgressAck, EgressChange, EgressClaim, EgressConfigError, EgressCore, EgressEnvelope,
    EgressFailure, EgressPolicy, EgressReconnect, EgressRetry, EgressRetryAction,
};

/// The only transport seam egress owns. Durability and framing remain separate
/// adapters around the envelope.
pub trait EgressTransport<T> {
    /// `true` means the transport accepted the send attempt. Domain delivery is
    /// proven later by [`EgressCell::ack`].
    fn send(&mut self, envelope: &EgressEnvelope<T>) -> bool;
}

struct Readers<T> {
    pending: Computed<Vec<EgressEnvelope<T>>>,
    inflight: Computed<Vec<EgressEnvelope<T>>>,
    acked_through: Computed<Option<u64>>,
    retry: Computed<Option<EgressRetry>>,
}

impl<T> Clone for Readers<T> {
    fn clone(&self) -> Self {
        Self {
            pending: self.pending,
            inflight: self.inflight,
            acked_through: self.acked_through,
            retry: self.retry,
        }
    }
}

struct Inner<T> {
    core: Rc<RefCell<EgressCore<T>>>,
    readers: Readers<T>,
}

pub struct EgressCell<T> {
    inner: Rc<Inner<T>>,
}

impl<T> Clone for EgressCell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T> EgressCell<T>
where
    T: PartialEq + Clone + 'static,
{
    pub fn new(
        ctx: &Context,
        generation: u64,
        policy: EgressPolicy,
    ) -> Result<Self, EgressConfigError> {
        let core = Rc::new(RefCell::new(EgressCore::new(generation, policy)?));
        let readers = Readers {
            pending: {
                let core = Rc::clone(&core);
                ctx.computed(move |_| core.borrow().pending())
            },
            inflight: {
                let core = Rc::clone(&core);
                ctx.computed(move |_| core.borrow().inflight())
            },
            acked_through: {
                let core = Rc::clone(&core);
                ctx.computed(move |_| core.borrow().acked_through())
            },
            retry: {
                let core = Rc::clone(&core);
                ctx.computed(move |_| core.borrow().retry())
            },
        };
        Ok(Self {
            inner: Rc::new(Inner { core, readers }),
        })
    }

    fn apply(&self, ctx: &Context, change: EgressChange) {
        let mut roots = Vec::new();
        if change.pending {
            roots.push(self.inner.readers.pending.id);
        }
        if change.inflight {
            roots.push(self.inner.readers.inflight.id);
        }
        if change.acked_through {
            roots.push(self.inner.readers.acked_through.id);
        }
        if change.retry {
            roots.push(self.inner.readers.retry.id);
        }
        ctx.clear_slots(&roots);
    }

    pub fn generation(&self) -> u64 {
        self.inner.core.borrow().generation()
    }

    pub fn next_sequence(&self) -> u64 {
        self.inner.core.borrow().next_sequence()
    }

    pub fn enqueue(&self, ctx: &Context, payload: T) -> u64 {
        let (change, sequence) = self.inner.core.borrow_mut().enqueue(payload);
        self.apply(ctx, change);
        sequence
    }

    pub fn claim(&self, ctx: &Context, generation: u64) -> EgressClaim<T> {
        let (change, claim) = self.inner.core.borrow_mut().claim(generation);
        self.apply(ctx, change);
        claim
    }

    pub fn ack(&self, ctx: &Context, generation: u64, through: u64) -> EgressAck {
        let (change, ack) = self.inner.core.borrow_mut().ack(generation, through);
        self.apply(ctx, change);
        ack
    }

    pub fn fail(&self, ctx: &Context, generation: u64, sequence: u64) -> EgressFailure {
        let (change, failure) = self.inner.core.borrow_mut().fail(generation, sequence);
        self.apply(ctx, change);
        failure
    }

    pub fn retry_now(&self, ctx: &Context, generation: u64, sequence: u64) -> EgressRetryAction {
        let (change, action) = self.inner.core.borrow_mut().retry_now(generation, sequence);
        self.apply(ctx, change);
        action
    }

    pub fn reconnect(&self, ctx: &Context, generation: u64) -> EgressReconnect {
        let (change, reconnect) = self.inner.core.borrow_mut().reconnect(generation);
        self.apply(ctx, change);
        reconnect
    }

    pub fn pending(&self, ctx: &Context) -> Vec<EgressEnvelope<T>> {
        ctx.get(&self.inner.readers.pending)
    }

    pub fn inflight(&self, ctx: &Context) -> Vec<EgressEnvelope<T>> {
        ctx.get(&self.inner.readers.inflight)
    }

    pub fn acked_through(&self, ctx: &Context) -> Option<u64> {
        ctx.get(&self.inner.readers.acked_through)
    }

    pub fn retry(&self, ctx: &Context) -> Option<EgressRetry> {
        ctx.get(&self.inner.readers.retry)
    }

    pub fn pending_handle(&self) -> Computed<Vec<EgressEnvelope<T>>> {
        self.inner.readers.pending
    }

    pub fn inflight_handle(&self) -> Computed<Vec<EgressEnvelope<T>>> {
        self.inner.readers.inflight
    }

    pub fn acked_through_handle(&self) -> Computed<Option<u64>> {
        self.inner.readers.acked_through
    }

    pub fn retry_handle(&self) -> Computed<Option<EgressRetry>> {
        self.inner.readers.retry
    }

    /// Attach one transport Effect at the current producer generation.
    ///
    /// A reconnect does not mutate this Effect's captured generation. The old
    /// attachment therefore becomes inert at the core fence; callers attach one
    /// replacement Effect for the new transport incarnation.
    pub fn attach_transport<Tr>(&self, ctx: Rc<Context>, transport: Rc<RefCell<Tr>>) -> Effect
    where
        Tr: EgressTransport<T> + 'static,
    {
        let generation = self.generation();
        let pending = self.pending_handle();
        let inflight = self.inflight_handle();
        let cell = self.clone();
        let effect_ctx = Rc::clone(&ctx);
        ctx.effect(move |compute| {
            // `inflight` is a dependency even though the core remains the
            // authority. An acknowledgement reopens the send window and must
            // reactively re-run this same attachment Effect.
            inflight.get(compute);
            if pending.get(compute).is_empty() {
                return;
            }
            loop {
                let EgressClaim::Claimed(envelope) = cell.claim(&effect_ctx, generation) else {
                    break;
                };
                if !transport.borrow_mut().send(&envelope) {
                    cell.fail(&effect_ctx, generation, envelope.sequence);
                    break;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[derive(Default)]
    struct Transport {
        sent: Vec<u64>,
        fail_next: bool,
    }

    impl EgressTransport<i32> for Transport {
        fn send(&mut self, envelope: &EgressEnvelope<i32>) -> bool {
            if self.fail_next {
                self.fail_next = false;
                return false;
            }
            self.sent.push(envelope.sequence);
            true
        }
    }

    #[test]
    fn one_effect_projects_pending_to_transport_and_inflight() {
        let ctx = Rc::new(Context::new());
        let egress = EgressCell::new(&ctx, 1, EgressPolicy::default()).unwrap();
        let transport = Rc::new(RefCell::new(Transport::default()));
        let _effect = egress.attach_transport(Rc::clone(&ctx), Rc::clone(&transport));
        egress.enqueue(&ctx, 10);
        egress.enqueue(&ctx, 20);
        assert_eq!(transport.borrow().sent, vec![0, 1]);
        assert!(egress.pending(&ctx).is_empty());
        assert_eq!(egress.inflight(&ctx).len(), 2);
    }

    #[test]
    fn reconnect_makes_old_attachment_inert() {
        let ctx = Rc::new(Context::new());
        let egress = EgressCell::new(&ctx, 3, EgressPolicy::default()).unwrap();
        let old = Rc::new(RefCell::new(Transport::default()));
        let _old_effect = egress.attach_transport(Rc::clone(&ctx), Rc::clone(&old));
        egress.enqueue(&ctx, 10);
        egress.reconnect(&ctx, 4);
        egress.enqueue(&ctx, 20);
        assert_eq!(old.borrow().sent, vec![0]);
        let fresh = Rc::new(RefCell::new(Transport::default()));
        let _fresh_effect = egress.attach_transport(Rc::clone(&ctx), Rc::clone(&fresh));
        assert_eq!(fresh.borrow().sent, vec![0, 1]);
    }

    #[test]
    fn ack_reopens_the_window_without_a_request_ack_handshake() {
        let ctx = Rc::new(Context::new());
        let policy = EgressPolicy {
            inflight_limit: 1,
            ..EgressPolicy::default()
        };
        let egress = EgressCell::new(&ctx, 1, policy).unwrap();
        let transport = Rc::new(RefCell::new(Transport::default()));
        let _effect = egress.attach_transport(Rc::clone(&ctx), Rc::clone(&transport));
        egress.enqueue(&ctx, 10);
        egress.enqueue(&ctx, 20);
        assert_eq!(transport.borrow().sent, vec![0]);
        assert_eq!(egress.pending(&ctx).len(), 1);

        assert_eq!(egress.ack(&ctx, 1, 0), EgressAck::Advanced { through: 0 });
        assert_eq!(transport.borrow().sent, vec![0, 1]);
        assert!(egress.pending(&ctx).is_empty());
    }

    #[test]
    fn reader_kinds_invalidate_independently() {
        let ctx = Context::new();
        let egress = EgressCell::new(&ctx, 1, EgressPolicy::default()).unwrap();
        let pending_runs = Rc::new(Cell::new(0));
        let ack_runs = Rc::new(Cell::new(0));
        let pending_handle = egress.pending_handle();
        let ack_handle = egress.acked_through_handle();
        let _pending_effect = {
            let runs = Rc::clone(&pending_runs);
            ctx.effect(move |compute| {
                pending_handle.get(compute);
                runs.set(runs.get() + 1);
            })
        };
        let _ack_effect = {
            let runs = Rc::clone(&ack_runs);
            ctx.effect(move |compute| {
                ack_handle.get(compute);
                runs.set(runs.get() + 1);
            })
        };
        egress.enqueue(&ctx, 1);
        assert_eq!(pending_runs.get(), 2);
        assert_eq!(ack_runs.get(), 1);
    }
}
