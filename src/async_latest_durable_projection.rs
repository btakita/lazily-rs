//! `AsyncContext` reactive latest-durable projection shell.
//!
//! The transition algebra is synchronous and deterministic; only the owning
//! graph is async-coloured.

use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext};
use crate::latest_durable_projection_core::{
    LatestDurableAck, LatestDurableChange, LatestDurableClaim, LatestDurableFailure,
    LatestDurableKeyState, LatestDurableProjectionCore, LatestDurableReconnect,
    LatestDurableSnapshot, LatestDurableUpsert,
};

struct Inner<K, V> {
    core: Arc<Mutex<LatestDurableProjectionCore<K, V>>>,
    state: AsyncComputed<LatestDurableSnapshot<K, V>>,
}

/// Async-graph reactive keyed latest-durable projection.
pub struct AsyncLatestDurableProjection<K, V> {
    inner: Arc<Inner<K, V>>,
}

impl<K, V> Clone for AsyncLatestDurableProjection<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> AsyncLatestDurableProjection<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + PartialEq + Send + Sync + 'static,
{
    pub fn new(ctx: &AsyncContext, generation: u64) -> Self {
        let core = Arc::new(Mutex::new(LatestDurableProjectionCore::new(generation)));
        let state = {
            let core = Arc::clone(&core);
            ctx.computed(move |_| {
                core.lock()
                    .expect("latest-durable projection core")
                    .snapshot()
            })
        };
        Self {
            inner: Arc::new(Inner { core, state }),
        }
    }

    fn apply(&self, ctx: &AsyncContext, change: LatestDurableChange) {
        if change.state {
            ctx.clear_slots(&[self.inner.state.id()]);
        }
    }

    pub fn generation(&self) -> u64 {
        self.inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .generation()
    }

    pub fn snapshot(&self, ctx: &AsyncContext) -> LatestDurableSnapshot<K, V> {
        ctx.get(&self.inner.state)
            .expect("synchronous latest-durable projection")
    }

    pub fn state(&self, key: &K) -> Option<LatestDurableKeyState<K, V>> {
        self.inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .state(key)
    }

    pub fn durable_through(&self, key: &K) -> Option<u64> {
        self.inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .durable_through(key)
    }

    pub fn pending_keys(&self) -> Vec<K> {
        self.inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .pending_keys()
    }

    pub fn state_handle(&self) -> AsyncComputed<LatestDurableSnapshot<K, V>> {
        self.inner.state
    }

    pub fn upsert_desired(
        &self,
        ctx: &AsyncContext,
        key: K,
        epoch: u64,
        value: V,
    ) -> LatestDurableUpsert {
        let (change, outcome) = self
            .inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .upsert_desired(key, epoch, value);
        self.apply(ctx, change);
        outcome
    }

    pub fn claim(&self, ctx: &AsyncContext, key: &K, generation: u64) -> LatestDurableClaim<K, V> {
        let (change, outcome) = self
            .inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .claim(key, generation);
        self.apply(ctx, change);
        outcome
    }

    pub fn ack_applied(
        &self,
        ctx: &AsyncContext,
        key: &K,
        generation: u64,
        epoch: u64,
    ) -> LatestDurableAck {
        let (change, outcome) = self
            .inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .ack_applied(key, generation, epoch);
        self.apply(ctx, change);
        outcome
    }

    pub fn fail_retryable(
        &self,
        ctx: &AsyncContext,
        key: &K,
        generation: u64,
        epoch: u64,
    ) -> LatestDurableFailure {
        let (change, outcome) = self
            .inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .fail_retryable(key, generation, epoch);
        self.apply(ctx, change);
        outcome
    }

    pub fn reconnect(&self, ctx: &AsyncContext, generation: u64) -> LatestDurableReconnect {
        let (change, outcome) = self
            .inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .reconnect(generation);
        self.apply(ctx, change);
        outcome
    }
}
