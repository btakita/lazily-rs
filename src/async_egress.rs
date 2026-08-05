//! `AsyncContext` reactive egress shell (`#lzegress`).
//!
//! Delivery authority is synchronous; only the attached transport Effect is
//! async-coloured.

use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext, AsyncEffectHandle};
use crate::context::SlotId;
use crate::egress::EgressTransport;
use crate::egress_core::{
    EgressAck, EgressChange, EgressClaim, EgressConfigError, EgressCore, EgressEnvelope,
    EgressFailure, EgressPolicy, EgressReconnect, EgressRetry, EgressRetryAction,
};

struct Readers<T> {
    pending: AsyncComputed<Vec<EgressEnvelope<T>>>,
    inflight: AsyncComputed<Vec<EgressEnvelope<T>>>,
    acked_through: AsyncComputed<Option<u64>>,
    retry: AsyncComputed<Option<EgressRetry>>,
}

impl<T> Clone for Readers<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Readers<T> {}

struct Inner<T> {
    core: Arc<Mutex<EgressCore<T>>>,
    readers: Readers<T>,
}

pub struct AsyncEgressCell<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for AsyncEgressCell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> AsyncEgressCell<T>
where
    T: PartialEq + Clone + Send + Sync + 'static,
{
    pub fn new(
        ctx: &AsyncContext,
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

    fn apply(&self, ctx: &AsyncContext, change: EgressChange) {
        let mut roots: Vec<SlotId> = Vec::new();
        if change.pending {
            roots.push(self.inner.readers.pending.id());
        }
        if change.inflight {
            roots.push(self.inner.readers.inflight.id());
        }
        if change.acked_through {
            roots.push(self.inner.readers.acked_through.id());
        }
        if change.retry {
            roots.push(self.inner.readers.retry.id());
        }
        ctx.clear_slots(&roots);
    }

    pub fn generation(&self) -> u64 {
        self.inner.core.lock().expect("egress core").generation()
    }

    pub fn next_sequence(&self) -> u64 {
        self.inner.core.lock().expect("egress core").next_sequence()
    }

    pub fn enqueue(&self, ctx: &AsyncContext, payload: T) -> u64 {
        let (change, sequence) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .enqueue(payload);
        self.apply(ctx, change);
        sequence
    }

    pub fn claim(&self, ctx: &AsyncContext, generation: u64) -> EgressClaim<T> {
        let (change, claim) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .claim(generation);
        self.apply(ctx, change);
        claim
    }

    pub fn ack(&self, ctx: &AsyncContext, generation: u64, through: u64) -> EgressAck {
        let (change, ack) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .ack(generation, through);
        self.apply(ctx, change);
        ack
    }

    pub fn fail(&self, ctx: &AsyncContext, generation: u64, sequence: u64) -> EgressFailure {
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
        ctx: &AsyncContext,
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

    pub fn reconnect(&self, ctx: &AsyncContext, generation: u64) -> EgressReconnect {
        let (change, reconnect) = self
            .inner
            .core
            .lock()
            .expect("egress core")
            .reconnect(generation);
        self.apply(ctx, change);
        reconnect
    }

    pub fn pending(&self, ctx: &AsyncContext) -> Vec<EgressEnvelope<T>> {
        ctx.get(&self.inner.readers.pending)
            .expect("synchronous egress projection")
    }

    pub fn inflight(&self, ctx: &AsyncContext) -> Vec<EgressEnvelope<T>> {
        ctx.get(&self.inner.readers.inflight)
            .expect("synchronous egress projection")
    }

    pub fn acked_through(&self, ctx: &AsyncContext) -> Option<u64> {
        ctx.get(&self.inner.readers.acked_through)
            .expect("synchronous egress projection")
    }

    pub fn retry(&self, ctx: &AsyncContext) -> Option<EgressRetry> {
        ctx.get(&self.inner.readers.retry)
            .expect("synchronous egress projection")
    }

    pub fn pending_handle(&self) -> AsyncComputed<Vec<EgressEnvelope<T>>> {
        self.inner.readers.pending
    }

    pub fn inflight_handle(&self) -> AsyncComputed<Vec<EgressEnvelope<T>>> {
        self.inner.readers.inflight
    }

    pub fn acked_through_handle(&self) -> AsyncComputed<Option<u64>> {
        self.inner.readers.acked_through
    }

    pub fn retry_handle(&self) -> AsyncComputed<Option<EgressRetry>> {
        self.inner.readers.retry
    }

    pub fn attach_transport<Tr>(
        &self,
        ctx: &AsyncContext,
        transport: Arc<Mutex<Tr>>,
    ) -> AsyncEffectHandle
    where
        Tr: EgressTransport<T> + Send + 'static,
    {
        let generation = self.generation();
        let pending = self.inner.readers.pending;
        let inflight = self.inner.readers.inflight;
        let cell = self.clone();
        ctx.effect_async(move |compute| {
            compute.get(&inflight);
            let has_pending = !compute
                .get(&pending)
                .expect("synchronous egress projection")
                .is_empty();
            let effect_ctx = compute.owning_context();
            let cell = cell.clone();
            let transport = Arc::clone(&transport);
            async move {
                if has_pending {
                    loop {
                        let EgressClaim::Claimed(envelope) = cell.claim(&effect_ctx, generation)
                        else {
                            break;
                        };
                        if !transport.lock().expect("egress transport").send(&envelope) {
                            cell.fail(&effect_ctx, generation, envelope.sequence);
                            break;
                        }
                    }
                }
                None::<fn()>
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

    #[tokio::test]
    async fn attachment_drives_the_async_projection() {
        let ctx = AsyncContext::new();
        let egress = AsyncEgressCell::new(&ctx, 1, EgressPolicy::default()).unwrap();
        let transport = Arc::new(Mutex::new(Transport(Vec::new())));
        let _effect = egress.attach_transport(&ctx, Arc::clone(&transport));
        egress.enqueue(&ctx, 7);
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(transport.lock().unwrap().0, vec![0]);
        assert_eq!(egress.inflight(&ctx).len(), 1);
    }
}
