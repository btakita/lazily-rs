//! `ThreadSafeTopicCell` — the `Send + Sync` flavor of [`TopicCell`]
//! (`#lazilythreadsafetopiccel`).
//!
//! Follows the reference pair (`ThreadSafeQueueCell` / `AsyncQueueCell`) exactly:
//! the cursor/retention algebra lives in the flavor-neutral
//! [`TopicCore`](crate::topic_core), and this shell adds only the reactivity —
//! one memoized `Computed` per stable subscriber, minted on *this* context's
//! graph. Reader kinds are derives, not polled counters.
//!
//! **Lock discipline.** Three locks exist and they are always taken in this
//! order, never any other:
//!
//! 1. `core` — taken by an op, and released before anything else is touched.
//! 2. `readers` — the per-subscriber handle table.
//! 3. the context — taken by `ctx.clear` / `ctx.get` / `ctx.computed`.
//!
//! A reader's compute closure runs *inside* the context lock and takes `core`,
//! which is context→core. That is the reverse of an op's core→context, so an op
//! that invalidated while still holding `core` would deadlock against a
//! concurrent reader. Every op below therefore scopes its `core` guard to a
//! block that ends before `readers` or the context is reached. The same argument
//! forbids holding `core` while looking up handles — hence the two separate
//! blocks in [`publish`](ThreadSafeTopicCell::publish), which look redundant and
//! are not.
//!
//! **Multi-root invalidation goes through `batch()`.** A publish fans out to
//! every connected subscriber whose unread suffix grew; clearing them one at a
//! time is one frontier walk each, and a concurrent reader can interleave and
//! see subscriber A advanced while B has not. `ThreadSafeContext` has no
//! `clear_slots`, but it does not need one: `clear_slot` accumulates into
//! `batched_slots` while `batch_depth > 0` and `finish_batch` hands every
//! collected root to a single `clear_frontier_locked` (pinned by
//! `thread_safe::tests::batch_gives_clear_a_single_multi_root_frontier`).

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::cell::Computed;
use crate::queue::{
    TopicDurability, TopicSnapshot, TopicSubscribeOutcome, TopicSubscriptionSnapshot,
};
use crate::thread_safe::ThreadSafeContext;
use crate::topic_core::TopicCore;

struct Inner<T, I> {
    core: Arc<Mutex<TopicCore<T, I>>>,
    readers: Mutex<HashMap<I, Computed<Vec<T>>>>,
}

/// A `Send + Sync` broadcast topic: every subscriber receives every published
/// element through an independent, non-destructive cursor.
///
/// Elements are retained until every durable cursor has passed them. Ephemeral
/// subscriptions start at the current tail, disappear on disconnect, and never
/// hold the GC frontier. Advancing one subscriber never invalidates another.
pub struct ThreadSafeTopicCell<T, I = String> {
    inner: Arc<Inner<T, I>>,
}

impl<T, I> Clone for ThreadSafeTopicCell<T, I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, I> ThreadSafeTopicCell<T, I>
where
    T: PartialEq + Clone + Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Create an empty topic at absolute offset zero.
    pub fn new(_ctx: &ThreadSafeContext) -> Self {
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
    pub fn from_snapshot(ctx: &ThreadSafeContext, snapshot: TopicSnapshot<T, I>) -> Self {
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

    /// Mint (or return) the subscriber's reader. Holds `readers` across the mint
    /// so two threads subscribing the same identity cannot orphan a graph node;
    /// safe because nothing ever acquires `readers` while holding the context.
    fn ensure_reader(&self, ctx: &ThreadSafeContext, id: I) -> Computed<Vec<T>> {
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
    /// identity without moving its cursor. Once an identity exists its stored
    /// durability wins over the caller's argument.
    pub fn subscribe(
        &self,
        ctx: &ThreadSafeContext,
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

    /// Reconnect a durable identity, preserving its cursor. Unknown identities
    /// are created as durable subscriptions at the current tail.
    pub fn reconnect(&self, ctx: &ThreadSafeContext, id: I) -> TopicSubscribeOutcome {
        self.subscribe(ctx, id, TopicDurability::Durable)
    }

    /// Disconnect a subscriber. Durable records remain offline at the same
    /// cursor; ephemeral records are removed. Returns whether state changed.
    pub fn disconnect(&self, ctx: &ThreadSafeContext, id: &I) -> bool {
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

    /// Append exactly one element, leaving every cursor unchanged. Returns its
    /// absolute offset and invalidates every connected subscriber whose unread
    /// suffix grew — in ONE frontier walk, so no reader observes a partial
    /// fan-out.
    pub fn publish(&self, ctx: &ThreadSafeContext, value: T) -> u64 {
        let (offset, connected) = {
            let mut core = self.inner.core.lock().expect("topic core");
            core.publish(value)
        };
        let roots: Vec<Computed<Vec<T>>> = {
            let readers = self.inner.readers.lock().expect("topic readers");
            connected
                .iter()
                .filter_map(|id| readers.get(id).copied())
                .collect()
        };
        if !roots.is_empty() {
            ctx.batch(|_| {
                for reader in &roots {
                    ctx.clear(reader);
                }
            });
        }
        offset
    }

    /// Reactive suffix read for one connected subscriber. Unknown and offline
    /// subscribers observe an empty stream.
    pub fn read_stream(&self, ctx: &ThreadSafeContext, id: &I) -> Vec<T> {
        let reader = self
            .inner
            .readers
            .lock()
            .expect("topic readers")
            .get(id)
            .copied();
        reader.map(|reader| ctx.get(&reader)).unwrap_or_default()
    }

    /// Reactive read of the element at a subscriber's cursor.
    pub fn read(&self, ctx: &ThreadSafeContext, id: &I) -> Option<T> {
        self.read_stream(ctx, id).into_iter().next()
    }

    /// Advance only the named connected cursor by one, returning the element it
    /// passed. At the tail, for an offline subscriber, or for an unknown id this
    /// is a no-op returning `None` that invalidates nothing.
    pub fn advance(&self, ctx: &ThreadSafeContext, id: &I) -> Option<T> {
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

    /// Remove the prefix below the minimum durable cursor. Cursors are absolute,
    /// so safe GC invalidates no reader and needs no context.
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
    pub fn reader_handle(&self, id: &I) -> Option<Computed<Vec<T>>> {
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
