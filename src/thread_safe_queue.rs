//! `ThreadSafeQueueCell` — the `Send + Sync` flavor of [`QueueCell`]
//! (`#lzqfrs`, the reference pair the topic and work-queue flavors follow).
//!
//! The shape mirrors the single-threaded shell deliberately, because the point of
//! the queue-family work is that the three flavors obey ONE contract. Reader kinds
//! are memoized derives (`Computed`), not polled version counters — a counter is
//! not a reader kind, since reading it registers a dependency on a number rather
//! than on the queue. Each flavor mints its own reader handles on its own graph;
//! nothing is shared across flavors but the laws.
//!
//! Two things differ from the sync shell, and both are consequences of being
//! `Send + Sync` rather than choices:
//!
//! **Storage is `Arc<Mutex<S>>`, and invalidation runs with that lock released.**
//! Not for throughput — for correctness. A reader's compute closure locks storage
//! while the graph holds the context lock; an op that invalidated while still
//! holding storage would take those two locks in the opposite order, and two
//! threads doing both at once deadlock. So every op scopes its storage guard to a
//! block that ends before the context is touched. The order is therefore always
//! context-then-storage, never both ways. `concurrent_push_and_read_do_not_deadlock`
//! exists to catch a regression here, because a lock-order inversion is invisible
//! in single-threaded tests and shows up as a hang, not a failure.
//!
//! **Multi-root invalidation goes through `batch()`.** The sync shell calls
//! `Context::clear_slots(&[..])` to clear several reader kinds in one frontier walk,
//! so a subscriber never observes `len` bumped while `is_full` still reads stale.
//! `ThreadSafeContext` has no `clear_slots`, and the plan assumed one had to be
//! added — it does not: `clear_slot` accumulates into `batched_slots` while
//! `batch_depth > 0`, and `finish_batch` flushes every collected root through a
//! single `clear_frontier_locked`. Pinned by
//! `thread_safe::tests::batch_gives_clear_a_single_multi_root_frontier`.

use std::sync::{Arc, Mutex};

use crate::cell::{Computed, Source};
use crate::queue::{QueuePopError, QueuePushError, QueueStorage, VecDequeStorage};
use crate::thread_safe::ThreadSafeContext;

struct Inner<T, S> {
    // `Arc<Mutex<..>>` so each reader-kind compute closure can hold the storage
    // across threads. Storage is out-of-graph: the closures take no dependency, so
    // a reader stays clean until the shell clears it on a transition.
    storage: Arc<Mutex<S>>,
    capacity: Option<usize>,
    head: Computed<Option<T>>,
    len: Computed<usize>,
    is_empty: Computed<bool>,
    is_full: Computed<bool>,
    closed: Source<bool>,
}

/// The `Send + Sync` reactive FIFO. Reader kinds invalidate independently: a push
/// onto a non-empty queue never touches `head`, a pop always does.
pub struct ThreadSafeQueueCell<T, S = VecDequeStorage<T>> {
    inner: Arc<Inner<T, S>>,
}

impl<T, S> Clone for ThreadSafeQueueCell<T, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> ThreadSafeQueueCell<T, VecDequeStorage<T>>
where
    T: PartialEq + Clone + Send + Sync + 'static,
{
    /// Unbounded queue over the reference backend.
    pub fn new(ctx: &ThreadSafeContext) -> Self {
        Self::with_storage(ctx, VecDequeStorage::unbounded())
    }

    /// Bounded queue exposing reactive backpressure through
    /// [`is_full`](Self::is_full).
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0`.
    pub fn with_capacity(ctx: &ThreadSafeContext, capacity: usize) -> Self {
        Self::with_storage(ctx, VecDequeStorage::with_capacity(capacity))
    }
}

impl<T, S> ThreadSafeQueueCell<T, S>
where
    T: PartialEq + Clone + Send + Sync + 'static,
    S: QueueStorage<T> + Send + 'static,
{
    /// Build over an arbitrary [`QueueStorage`] backend.
    pub fn with_storage(ctx: &ThreadSafeContext, storage: S) -> Self {
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
    /// walk, so no subscriber observes a partial transition (`len` bumped while
    /// `is_full` still stale).
    ///
    /// `head_changed` is a caller argument rather than derived from lengths because
    /// head depends on op direction: a pop always changes it, a push only from
    /// empty. `closed` is never touched here — only [`close`](Self::close) moves it.
    ///
    /// MUST be called with the storage lock released. See the module docs.
    fn invalidate_readers(
        &self,
        ctx: &ThreadSafeContext,
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

        ctx.batch(|_| {
            // `len` always changes on a successful op.
            ctx.clear(&self.inner.len);
            if is_empty_changed {
                ctx.clear(&self.inner.is_empty);
            }
            if is_full_changed {
                ctx.clear(&self.inner.is_full);
            }
            if head_changed {
                ctx.clear(&self.inner.head);
            }
        });
    }

    /// Append to the tail. `Full` if bounded and at capacity, `Closed` if closed;
    /// on either error the queue is unchanged and nothing is invalidated.
    pub fn try_push(&self, ctx: &ThreadSafeContext, value: T) -> Result<(), QueuePushError> {
        // Guard scoped so storage is released before the context is touched.
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
    /// and empty — the two are distinct. A pop on a closed non-empty queue drains.
    pub fn try_pop(&self, ctx: &ThreadSafeContext) -> Result<T, QueuePopError> {
        let (result, len_before) = {
            let mut s = self.inner.storage.lock().expect("queue storage");
            let len_before = s.len();
            (s.try_pop(), len_before)
        };
        if result.is_ok() {
            // A successful pop always changes the head value.
            self.invalidate_readers(ctx, len_before, len_before - 1, true);
        }
        result
    }

    /// Idempotent and terminal. The first close flips `closed`; later ones are
    /// no-ops that invalidate nothing.
    pub fn close(&self, ctx: &ThreadSafeContext) {
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

    // -- Reader kinds. Each registers a dependency on the reader, not on a counter.

    /// Current head value — after a pop this is the *next* element, never stale.
    pub fn head(&self, ctx: &ThreadSafeContext) -> Option<T> {
        ctx.get(&self.inner.head)
    }

    pub fn len(&self, ctx: &ThreadSafeContext) -> usize {
        ctx.get(&self.inner.len)
    }

    pub fn is_empty(&self, ctx: &ThreadSafeContext) -> bool {
        ctx.get(&self.inner.is_empty)
    }

    /// Reactive backpressure: flips false on a pop that leaves capacity.
    pub fn is_full(&self, ctx: &ThreadSafeContext) -> bool {
        ctx.get(&self.inner.is_full)
    }

    pub fn closed(&self, ctx: &ThreadSafeContext) -> bool {
        ctx.get(&self.inner.closed)
    }

    /// Declared capacity, or `None` when unbounded. Not reactive — a backend's
    /// capacity does not change.
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity
    }
}
