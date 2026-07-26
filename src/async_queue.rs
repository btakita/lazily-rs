//! `AsyncQueueCell` — the `AsyncContext` flavor of [`QueueCell`] (`#lzqfrs`).
//!
//! Completes the reference queue pair. The shell is deliberately the same shape as
//! the thread-safe one, because the family's whole claim is that the three flavors
//! obey ONE contract: reader kinds are memoized derives, not polled counters, and
//! each flavor mints its own handles on its own graph.
//!
//! The interesting decision here is that **ordering is not async-coloured**. A
//! queue's reader kinds derive from storage the graph does not own, so there is
//! nothing to await — and routing them through `computed_async` would make every
//! `len()` return `Option` and need a settle step to observe a value that was
//! already known synchronously. That is why the Phase-3 prerequisite added
//! `AsyncContext::computed`: a synchronous compute resolves inline on read, so the
//! reader kinds here return plain values exactly like the other two flavors. What
//! stays genuinely async is anything a *caller* composes on top.
//!
//! Locking follows the thread-safe shell for the same reason: reads take the
//! context lock then storage, so an op must release storage before invalidating or
//! the two orders deadlock. Multi-root invalidation goes through
//! [`AsyncContext::clear_slots`] — the other Phase-3 prerequisite — so reader kinds
//! that transition together are cleared in one frontier walk and no subscriber
//! observes `len` decremented while `is_full` still reads stale.

use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext, AsyncSource};
use crate::queue::{QueuePopError, QueuePushError, QueueStorage, VecDequeStorage};

struct Inner<T, S> {
    storage: Arc<Mutex<S>>,
    capacity: Option<usize>,
    head: AsyncComputed<Option<T>>,
    len: AsyncComputed<usize>,
    is_empty: AsyncComputed<bool>,
    is_full: AsyncComputed<bool>,
    closed: AsyncSource<bool>,
}

/// The `AsyncContext` reactive FIFO. Reader kinds invalidate independently: a push
/// onto a non-empty queue never touches `head`, a pop always does.
pub struct AsyncQueueCell<T, S = VecDequeStorage<T>> {
    inner: Arc<Inner<T, S>>,
}

impl<T, S> Clone for AsyncQueueCell<T, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> AsyncQueueCell<T, VecDequeStorage<T>>
where
    T: PartialEq + Clone + Send + Sync + 'static,
{
    /// Unbounded queue over the reference backend.
    pub fn new(ctx: &AsyncContext) -> Self {
        Self::with_storage(ctx, VecDequeStorage::unbounded())
    }

    /// Bounded queue exposing reactive backpressure through
    /// [`is_full`](Self::is_full).
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0`.
    pub fn with_capacity(ctx: &AsyncContext, capacity: usize) -> Self {
        Self::with_storage(ctx, VecDequeStorage::with_capacity(capacity))
    }
}

impl<T, S> AsyncQueueCell<T, S>
where
    T: PartialEq + Clone + Send + Sync + 'static,
    S: QueueStorage<T> + Send + 'static,
{
    /// Build over an arbitrary [`QueueStorage`] backend.
    pub fn with_storage(ctx: &AsyncContext, storage: S) -> Self {
        let closed_val = storage.is_closed();
        let capacity = storage.capacity();
        let storage = Arc::new(Mutex::new(storage));

        let head = {
            let storage = Arc::clone(&storage);
            ctx.computed(move |_| storage.lock().expect("queue storage").peek().cloned())
        };
        let len = {
            let storage = Arc::clone(&storage);
            ctx.computed(move |_| storage.lock().expect("queue storage").len())
        };
        let is_empty = {
            let storage = Arc::clone(&storage);
            ctx.computed(move |_| storage.lock().expect("queue storage").len() == 0)
        };
        let is_full = {
            let storage = Arc::clone(&storage);
            ctx.computed(move |_| {
                let s = storage.lock().expect("queue storage");
                match s.capacity() {
                    Some(cap) => s.len() >= cap,
                    None => false,
                }
            })
        };

        Self {
            inner: Arc::new(Inner {
                storage,
                capacity,
                head,
                len,
                is_empty,
                is_full,
                closed: ctx.source(closed_val),
            }),
        }
    }

    /// Clear exactly the reader kinds whose derived value changed, in ONE frontier
    /// walk. MUST be called with the storage lock released.
    fn invalidate_readers(
        &self,
        ctx: &AsyncContext,
        len_before: usize,
        len_after: usize,
        head_changed: bool,
    ) {
        let is_empty_changed = (len_before == 0) != (len_after == 0);
        let is_full_changed = self
            .inner
            .capacity
            .map(|c| (len_before >= c) != (len_after >= c))
            .unwrap_or(false);

        // `len` always changes on a successful op, so it is always a root.
        let mut roots = [self.inner.len.id(); 4];
        let mut n = 1;
        if is_empty_changed {
            roots[n] = self.inner.is_empty.id();
            n += 1;
        }
        if is_full_changed {
            roots[n] = self.inner.is_full.id();
            n += 1;
        }
        if head_changed {
            roots[n] = self.inner.head.id();
            n += 1;
        }
        ctx.clear_slots(&roots[..n]);
    }

    /// Append to the tail. `Full` if bounded and at capacity, `Closed` if closed;
    /// on either error the queue is unchanged and nothing is invalidated.
    pub fn try_push(&self, ctx: &AsyncContext, value: T) -> Result<(), QueuePushError> {
        let (result, len_before) = {
            let mut s = self.inner.storage.lock().expect("queue storage");
            let len_before = s.len();
            (s.try_push(value), len_before)
        };
        if result.is_ok() {
            self.invalidate_readers(ctx, len_before, len_before + 1, len_before == 0);
        }
        result
    }

    /// Remove and return the head. `Empty` if open and empty, `Closed` if closed
    /// and empty — distinct. A pop on a closed non-empty queue drains.
    pub fn try_pop(&self, ctx: &AsyncContext) -> Result<T, QueuePopError> {
        let (result, len_before) = {
            let mut s = self.inner.storage.lock().expect("queue storage");
            let len_before = s.len();
            (s.try_pop(), len_before)
        };
        if result.is_ok() {
            self.invalidate_readers(ctx, len_before, len_before - 1, true);
        }
        result
    }

    /// Idempotent and terminal.
    pub fn close(&self, ctx: &AsyncContext) {
        let newly_closed = {
            let mut s = self.inner.storage.lock().expect("queue storage");
            let was = s.is_closed();
            s.close();
            !was
        };
        if newly_closed {
            ctx.set(&self.inner.closed, true);
        }
    }

    // -- Reader kinds.
    //
    // These return plain values, not `Option`. The compute is synchronous, so the
    // cell resolves inline on read and `None` is unreachable — a queue's length is
    // not a thing you wait for.

    /// Current head value — after a pop this is the *next* element, never stale.
    pub fn head(&self, ctx: &AsyncContext) -> Option<T> {
        ctx.get(&self.inner.head)
            .expect("sync compute resolves inline")
    }

    pub fn len(&self, ctx: &AsyncContext) -> usize {
        ctx.get(&self.inner.len)
            .expect("sync compute resolves inline")
    }

    pub fn is_empty(&self, ctx: &AsyncContext) -> bool {
        ctx.get(&self.inner.is_empty)
            .expect("sync compute resolves inline")
    }

    /// Reactive backpressure: flips false on a pop that leaves capacity.
    pub fn is_full(&self, ctx: &AsyncContext) -> bool {
        ctx.get(&self.inner.is_full)
            .expect("sync compute resolves inline")
    }

    pub fn closed(&self, ctx: &AsyncContext) -> bool {
        ctx.get(&self.inner.closed)
    }

    /// Declared capacity, or `None` when unbounded. Not reactive.
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity
    }
}
