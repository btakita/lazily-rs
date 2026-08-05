//! `Send + Sync` reactive egress shell (`#lzegress`).

use std::sync::{Arc, Mutex};

use crate::cell::Computed;
use crate::effect::Effect;
use crate::egress::EgressTransport;
use crate::egress_core::{
    EgressAck, EgressChange, EgressClaim, EgressConfigError, EgressCore, EgressEnvelope,
    EgressFailure, EgressPolicy, EgressReconnect, EgressRetry, EgressRetryAction,
};
use crate::thread_safe::ThreadSafeContext;

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
    core: Arc<Mutex<EgressCore<T>>>,
    readers: Readers<T>,
}

pub struct ThreadSafeEgressCell<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for ThreadSafeEgressCell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> ThreadSafeEgressCell<T>
where
    T: PartialEq + Clone + Send + Sync + 'static,
{
    pub fn new(
        ctx: &ThreadSafeContext,
        generation: u64,
        policy: EgressPolicy,
    ) -> Result<Self, EgressConfigError> {
        let core = Arc::new(Mutex::new(EgressCore::new(generation, policy)?));
        let readers = Readers {
            pending: {
                let core = Arc::clone(&core);
                ctx.computed(move |_| core.lock().expect("egress core").pending())
            },
            inflight: {
                let core = Arc::clone(&core);
                ctx.computed(move |_| core.lock().expect("egress core").inflight())
            },
            acked_through: {
                let core = Arc::clone(&core);
                ctx.computed(move |_| core.lock().expect("egress core").acked_through())
            },
            retry: {
                let core = Arc::clone(&core);
                ctx.computed(move |_| core.lock().expect("egress core").retry())
            },
        };
        Ok(Self {
            inner: Arc::new(Inner { core, readers }),
        })
    }

    fn apply(&self, ctx: &ThreadSafeContext, change: EgressChange) {
        if change.is_empty() {
            return;
        }
        ctx.batch(|ctx| {
            if change.pending {
                ctx.clear(&self.inner.readers.pending);
            }
            if change.inflight {
                ctx.clear(&self.inner.readers.inflight);
            }
            if change.acked_through {
                ctx.clear(&self.inner.readers.acked_through);
            }
            if change.retry {
                ctx.clear(&self.inner.readers.retry);
            }
        });
    }

    pub fn generation(&self) -> u64 {
        self.inner.core.lock().expect("egress core").generation()
    }

    pub fn next_sequence(&self) -> u64 {
        self.inner.core.lock().expect("egress core").next_sequence()
    }

    pub fn enqueue(&self, ctx: &ThreadSafeContext, payload: T) -> u64 {
        let (change, sequence) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .enqueue(payload);
        self.apply(ctx, change);
        sequence
    }

    pub fn claim(&self, ctx: &ThreadSafeContext, generation: u64) -> EgressClaim<T> {
        let (change, claim) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .claim(generation);
        self.apply(ctx, change);
        claim
    }

    pub fn ack(&self, ctx: &ThreadSafeContext, generation: u64, through: u64) -> EgressAck {
        let (change, ack) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .ack(generation, through);
        self.apply(ctx, change);
        ack
    }

    pub fn fail(&self, ctx: &ThreadSafeContext, generation: u64, sequence: u64) -> EgressFailure {
        let (change, failure) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .fail(generation, sequence);
        self.apply(ctx, change);
        failure
    }

    pub fn retry_now(
        &self,
        ctx: &ThreadSafeContext,
        generation: u64,
        sequence: u64,
    ) -> EgressRetryAction {
        let (change, action) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .retry_now(generation, sequence);
        self.apply(ctx, change);
        action
    }

    pub fn reconnect(&self, ctx: &ThreadSafeContext, generation: u64) -> EgressReconnect {
        let (change, reconnect) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .reconnect(generation);
        self.apply(ctx, change);
        reconnect
    }

    pub fn pending(&self, ctx: &ThreadSafeContext) -> Vec<EgressEnvelope<T>> {
        ctx.get(&self.inner.readers.pending)
    }

    pub fn inflight(&self, ctx: &ThreadSafeContext) -> Vec<EgressEnvelope<T>> {
        ctx.get(&self.inner.readers.inflight)
    }

    pub fn acked_through(&self, ctx: &ThreadSafeContext) -> Option<u64> {
        ctx.get(&self.inner.readers.acked_through)
    }

    pub fn retry(&self, ctx: &ThreadSafeContext) -> Option<EgressRetry> {
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

    pub fn attach_transport<Tr>(&self, ctx: &ThreadSafeContext, transport: Arc<Mutex<Tr>>) -> Effect
    where
        Tr: EgressTransport<T> + Send + 'static,
    {
        let generation = self.generation();
        let pending = self.inner.readers.pending;
        let inflight = self.inner.readers.inflight;
        let cell = self.clone();
        ctx.effect(move |ctx| {
            ctx.get(&inflight);
            if ctx.get(&pending).is_empty() {
                return;
            }
            loop {
                let EgressClaim::Claimed(envelope) = cell.claim(ctx, generation) else {
                    break;
                };
                if !transport.lock().expect("egress transport").send(&envelope) {
                    cell.fail(ctx, generation, envelope.sequence);
                    break;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Transport(Vec<u64>);

    impl EgressTransport<i32> for Transport {
        fn send(&mut self, envelope: &EgressEnvelope<i32>) -> bool {
            self.0.push(envelope.sequence);
            true
        }
    }

    #[test]
    fn attachment_drives_the_shared_projection() {
        let ctx = ThreadSafeContext::new();
        let egress = ThreadSafeEgressCell::new(&ctx, 1, EgressPolicy::default()).unwrap();
        let transport = Arc::new(Mutex::new(Transport(Vec::new())));
        let _effect = egress.attach_transport(&ctx, Arc::clone(&transport));
        egress.enqueue(&ctx, 7);
        assert_eq!(transport.lock().unwrap().0, vec![0]);
        assert_eq!(egress.inflight(&ctx).len(), 1);
    }
}
