//! Reactive shell for [`LatestDurableProjectionCore`](crate::LatestDurableProjectionCore).

use std::cell::RefCell;
use std::rc::Rc;

use crate::cell::Computed;
use crate::context::Context;
use crate::latest_durable_projection_core::{
    LatestDurableAck, LatestDurableChange, LatestDurableClaim, LatestDurableFailure,
    LatestDurableKeyState, LatestDurableProjectionCore, LatestDurableReconnect,
    LatestDurableSnapshot, LatestDurableUpsert,
};

struct Inner<K, V> {
    core: Rc<RefCell<LatestDurableProjectionCore<K, V>>>,
    state: Computed<LatestDurableSnapshot<K, V>>,
}

/// Single-threaded reactive keyed latest-durable projection.
pub struct LatestDurableProjection<K, V> {
    inner: Rc<Inner<K, V>>,
}

impl<K, V> Clone for LatestDurableProjection<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<K, V> LatestDurableProjection<K, V>
where
    K: Ord + Clone + 'static,
    V: Clone + PartialEq + 'static,
{
    pub fn new(ctx: &Context, generation: u64) -> Self {
        let core = Rc::new(RefCell::new(LatestDurableProjectionCore::new(generation)));
        let state = {
            let core = Rc::clone(&core);
            ctx.computed(move |_| core.borrow().snapshot())
        };
        Self {
            inner: Rc::new(Inner { core, state }),
        }
    }

    fn apply(&self, ctx: &Context, change: LatestDurableChange) {
        if change.state {
            ctx.clear_slots(&[self.inner.state.id]);
        }
    }

    pub fn generation(&self) -> u64 {
        self.inner.core.borrow().generation()
    }

    pub fn snapshot(&self, ctx: &Context) -> LatestDurableSnapshot<K, V> {
        ctx.get(&self.inner.state)
    }

    pub fn state(&self, key: &K) -> Option<LatestDurableKeyState<K, V>> {
        self.inner.core.borrow().state(key)
    }

    pub fn durable_through(&self, key: &K) -> Option<u64> {
        self.inner.core.borrow().durable_through(key)
    }

    pub fn pending_keys(&self) -> Vec<K> {
        self.inner.core.borrow().pending_keys()
    }

    pub fn state_handle(&self) -> Computed<LatestDurableSnapshot<K, V>> {
        self.inner.state
    }

    pub fn upsert_desired(
        &self,
        ctx: &Context,
        key: K,
        epoch: u64,
        value: V,
    ) -> LatestDurableUpsert {
        let (change, outcome) = self
            .inner
            .core
            .borrow_mut()
            .upsert_desired(key, epoch, value);
        self.apply(ctx, change);
        outcome
    }

    pub fn claim(&self, ctx: &Context, key: &K, generation: u64) -> LatestDurableClaim<K, V> {
        let (change, outcome) = self.inner.core.borrow_mut().claim(key, generation);
        self.apply(ctx, change);
        outcome
    }

    pub fn ack_applied(
        &self,
        ctx: &Context,
        key: &K,
        generation: u64,
        epoch: u64,
    ) -> LatestDurableAck {
        let (change, outcome) = self
            .inner
            .core
            .borrow_mut()
            .ack_applied(key, generation, epoch);
        self.apply(ctx, change);
        outcome
    }

    pub fn fail_retryable(
        &self,
        ctx: &Context,
        key: &K,
        generation: u64,
        epoch: u64,
    ) -> LatestDurableFailure {
        let (change, outcome) = self
            .inner
            .core
            .borrow_mut()
            .fail_retryable(key, generation, epoch);
        self.apply(ctx, change);
        outcome
    }

    pub fn reconnect(&self, ctx: &Context, generation: u64) -> LatestDurableReconnect {
        let (change, outcome) = self.inner.core.borrow_mut().reconnect(generation);
        self.apply(ctx, change);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn distinct_transitions_invalidate_the_reactive_snapshot() {
        let ctx = Context::new();
        let projection = LatestDurableProjection::new(&ctx, 1);
        let runs = Rc::new(Cell::new(0));
        let _effect = {
            let runs = Rc::clone(&runs);
            let state = projection.state_handle();
            ctx.effect(move |compute| {
                state.get(compute);
                runs.set(runs.get() + 1);
            })
        };

        projection.upsert_desired(&ctx, "doc", 1, "one");
        projection.upsert_desired(&ctx, "doc", 1, "one");
        assert_eq!(runs.get(), 2);
    }
}
