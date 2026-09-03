//! `Send + Sync` reactive latest-durable projection shell.

use std::sync::{Arc, Mutex};

use crate::cell::Computed;
use crate::latest_durable_projection_core::{
    LatestDurableAck, LatestDurableChange, LatestDurableClaim, LatestDurableFailure,
    LatestDurableKeyState, LatestDurableProjectionCore, LatestDurableReconnect,
    LatestDurableSnapshot, LatestDurableUpsert,
};
use crate::thread_safe::ThreadSafeContext;

struct Inner<K, V> {
    core: Arc<Mutex<LatestDurableProjectionCore<K, V>>>,
    state: Computed<LatestDurableSnapshot<K, V>>,
}

/// Thread-safe reactive keyed latest-durable projection.
pub struct ThreadSafeLatestDurableProjection<K, V> {
    inner: Arc<Inner<K, V>>,
}

impl<K, V> Clone for ThreadSafeLatestDurableProjection<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> ThreadSafeLatestDurableProjection<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + PartialEq + Send + Sync + 'static,
{
    pub fn new(ctx: &ThreadSafeContext, generation: u64) -> Self {
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

    fn apply(&self, ctx: &ThreadSafeContext, change: LatestDurableChange) {
        if change.state {
            ctx.clear(&self.inner.state);
        }
    }

    pub fn generation(&self) -> u64 {
        self.inner
            .core
            .lock()
            .expect("latest-durable projection core")
            .generation()
    }

    pub fn snapshot(&self, ctx: &ThreadSafeContext) -> LatestDurableSnapshot<K, V> {
        ctx.get(&self.inner.state)
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

    pub fn state_handle(&self) -> Computed<LatestDurableSnapshot<K, V>> {
        self.inner.state
    }

    pub fn upsert_desired(
        &self,
        ctx: &ThreadSafeContext,
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

    pub fn claim(
        &self,
        ctx: &ThreadSafeContext,
        key: &K,
        generation: u64,
    ) -> LatestDurableClaim<K, V> {
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
        ctx: &ThreadSafeContext,
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
        ctx: &ThreadSafeContext,
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

    pub fn reconnect(&self, ctx: &ThreadSafeContext, generation: u64) -> LatestDurableReconnect {
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
