//! `AsyncTopicCell` — the `AsyncContext` flavor of [`TopicCell`]
//! (`#lazilythreadsafetopiccel`).
//!
//! Completes the topic triple. The shell is deliberately the same shape as the
//! thread-safe one, over the same flavor-neutral
//! [`TopicCore`](crate::topic_core), because the family's claim is that the three
//! flavors obey ONE contract.
//!
//! **A cursor is not async-coloured.** Subscription cursors are per-subscriber
//! and monotone, and a subscriber's unread suffix derives from a retained log
//! the graph does not own — there is nothing to await. Routing the reader
//! through `computed_async` would make every `read_stream()` return `Option` and
//! need a settle step to observe a value already known synchronously, so the
//! reader kinds here use `AsyncContext::computed` (sync compute, async graph)
//! and return plain values exactly like the other two flavors.
//!
//! Lock discipline is the thread-safe shell's, for the same reason: a reader's
//! compute takes the context lock and then `core`, so an op must release `core`
//! before invalidating or the two orders deadlock. Multi-root fan-out goes
//! through [`AsyncContext::clear_slots`], so a publish clears every affected
//! subscriber in one frontier walk.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext};
use crate::queue::{
    TopicDurability, TopicSnapshot, TopicSubscribeOutcome, TopicSubscriptionSnapshot,
};
use crate::topic_core::TopicCore;

struct Inner<T, I> {
    core: Arc<Mutex<TopicCore<T, I>>>,
    readers: Mutex<HashMap<I, AsyncComputed<Vec<T>>>>,
}

/// An `AsyncContext` broadcast topic with independent, non-destructive
/// per-subscriber cursors.
pub struct AsyncTopicCell<T, I = String> {
    inner: Arc<Inner<T, I>>,
}

impl<T, I> Clone for AsyncTopicCell<T, I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, I> AsyncTopicCell<T, I>
where
    T: PartialEq + Clone + Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Create an empty topic at absolute offset zero.
    pub fn new(_ctx: &AsyncContext) -> Self {
        Self {
            inner: Arc::new(Inner {
                core: Arc::new(Mutex::new(TopicCore::new())),
                readers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Restore an atomic topic snapshot, preserving durable cursors exactly.
    ///
    /// # Panics
    ///
    /// Panics if any cursor lies outside `base_offset..=end_offset`.
    pub fn from_snapshot(ctx: &AsyncContext, snapshot: TopicSnapshot<T, I>) -> Self {
        let topic = Self {
            inner: Arc::new(Inner {
                core: Arc::new(Mutex::new(TopicCore::from_snapshot(snapshot))),
                readers: Mutex::new(HashMap::new()),
            }),
        };
        let ids = topic
            .inner
            .core
            .lock()
            .expect("topic core")
            .subscription_ids();
        for id in ids {
            topic.ensure_reader(ctx, id);
        }
        topic
    }

    fn ensure_reader(&self, ctx: &AsyncContext, id: I) -> AsyncComputed<Vec<T>> {
        let mut readers = self.inner.readers.lock().expect("topic readers");
        if let Some(handle) = readers.get(&id) {
            return *handle;
        }
        let core = Arc::clone(&self.inner.core);
        let reader_id = id.clone();
        let handle =
            ctx.computed(move |_| core.lock().expect("topic core").read_suffix(&reader_id));
        readers.insert(id, handle);
        handle
    }

    /// Create a cursor at the current tail, or reconnect an existing durable
    /// identity without moving its cursor.
    pub fn subscribe(
        &self,
        ctx: &AsyncContext,
        id: I,
        durability: TopicDurability,
    ) -> TopicSubscribeOutcome {
        let (outcome, invalidate) = {
            let mut core = self.inner.core.lock().expect("topic core");
            core.subscribe(id.clone(), durability)
        };
        let reader = self.ensure_reader(ctx, id);
        if invalidate {
            ctx.clear(&reader);
        }
        outcome
    }

    /// Reconnect a durable identity, preserving its cursor.
    pub fn reconnect(&self, ctx: &AsyncContext, id: I) -> TopicSubscribeOutcome {
        self.subscribe(ctx, id, TopicDurability::Durable)
    }

    /// Disconnect a subscriber. Returns whether state changed.
    pub fn disconnect(&self, ctx: &AsyncContext, id: &I) -> bool {
        let remove_reader = {
            let mut core = self.inner.core.lock().expect("topic core");
            match core.disconnect(id) {
                Some(remove_reader) => remove_reader,
                None => return false,
            }
        };
        let reader = {
            let mut readers = self.inner.readers.lock().expect("topic readers");
            if remove_reader {
                readers.remove(id)
            } else {
                readers.get(id).copied()
            }
        };
        if let Some(reader) = reader {
            ctx.clear(&reader);
        }
        true
    }

    /// Append exactly one element, leaving every cursor unchanged. Every
    /// connected subscriber whose unread suffix grew is invalidated in ONE
    /// frontier walk.
    pub fn publish(&self, ctx: &AsyncContext, value: T) -> u64 {
        let (offset, connected) = {
            let mut core = self.inner.core.lock().expect("topic core");
            core.publish(value)
        };
        let roots: Vec<_> = {
            let readers = self.inner.readers.lock().expect("topic readers");
            connected
                .iter()
                .filter_map(|id| readers.get(id).map(|reader| reader.id()))
                .collect()
        };
        ctx.clear_slots(&roots);
        offset
    }

    /// Reactive suffix read for one connected subscriber.
    ///
    /// Returns a plain `Vec`, not an `Option`: the compute is synchronous, so
    /// the cell resolves inline on read.
    pub fn read_stream(&self, ctx: &AsyncContext, id: &I) -> Vec<T> {
        let reader = self
            .inner
            .readers
            .lock()
            .expect("topic readers")
            .get(id)
            .copied();
        reader
            .map(|reader| ctx.get(&reader).expect("sync compute resolves inline"))
            .unwrap_or_default()
    }

    /// Reactive read of the element at a subscriber's cursor.
    pub fn read(&self, ctx: &AsyncContext, id: &I) -> Option<T> {
        self.read_stream(ctx, id).into_iter().next()
    }

    /// Advance only the named connected cursor by one, returning the element it
    /// passed. A `None` invalidates nothing.
    pub fn advance(&self, ctx: &AsyncContext, id: &I) -> Option<T> {
        let value = {
            let mut core = self.inner.core.lock().expect("topic core");
            core.advance(id)?
        };
        let reader = self
            .inner
            .readers
            .lock()
            .expect("topic readers")
            .get(id)
            .copied();
        if let Some(reader) = reader {
            ctx.clear(&reader);
        }
        Some(value)
    }

    /// Safe GC. Cursors are absolute, so this invalidates no reader.
    pub fn gc(&self) -> usize {
        self.inner.core.lock().expect("topic core").gc()
    }

    pub fn base_offset(&self) -> u64 {
        self.inner.core.lock().expect("topic core").base_offset()
    }

    pub fn end_offset(&self) -> u64 {
        self.inner.core.lock().expect("topic core").end_offset()
    }

    /// Non-reactive retained-log snapshot, oldest first.
    pub fn elements(&self) -> Vec<T> {
        self.inner.core.lock().expect("topic core").elements()
    }

    /// Snapshot one subscription record.
    pub fn subscription(&self, id: &I) -> Option<TopicSubscriptionSnapshot> {
        self.inner.core.lock().expect("topic core").subscription(id)
    }

    /// Handle to the named subscriber's reactive unread suffix.
    pub fn reader_handle(&self, id: &I) -> Option<AsyncComputed<Vec<T>>> {
        self.inner
            .readers
            .lock()
            .expect("topic readers")
            .get(id)
            .copied()
    }

    /// Atomic durable/live-state snapshot suitable for restart and conformance.
    pub fn snapshot(&self) -> TopicSnapshot<T, I> {
        self.inner.core.lock().expect("topic core").snapshot()
    }
}
