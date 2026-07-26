//! `ThreadSafeWorkQueueCell` — the `Send + Sync` flavor of [`WorkQueueCell`]
//! (`#lazilythreadsafeworkqueu`).
//!
//! This is the flavor that has an actual reason to exist beyond symmetry: a
//! competing-consumer queue whose whole point is N workers claiming exclusively
//! is the one primitive in the family you would genuinely reach for across
//! threads. Exclusivity comes from the `core` mutex — `claim` pops under it, so
//! two workers cannot be handed the same delivery.
//!
//! The lifecycle algebra lives in the flavor-neutral
//! [`WorkQueueCore`](crate::work_queue_core); this shell adds only the four
//! reader kinds, minted on *this* context's graph. Which of them a transition
//! moved is computed by `Transition::changed`, shared with the other two
//! flavors, so the invalidation rule cannot drift per flavor.
//!
//! **The clock stays a caller argument.** `now` is passed to `claim` and
//! `reap_expired` exactly as in the single-threaded flavor. A per-flavor timer
//! would make redelivery non-deterministic and unreplayable against the
//! `workqueue_*` fixtures, and the visibility-timeout seam is not
//! flavor-specific to begin with.
//!
//! **Lock discipline.** A reader's compute closure runs inside the context lock
//! and takes `core` (context→core), so an op that invalidated while still
//! holding `core` would invert the order and deadlock against a concurrent
//! reader. Every op below scopes its `core` guard to a block that ends before
//! the context is touched. Multi-root invalidation goes through `batch()`, which
//! collects the roots and hands them to a single frontier walk — so nobody
//! observes `pending_len` bumped while `is_empty` still reads stale.

use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::cell::Computed;
use crate::thread_safe::ThreadSafeContext;
use crate::work_queue::{WorkQueueDeadLetter, WorkQueueDelivery, WorkQueueItem};
use crate::work_queue_core::{ReaderChange, Transition, WorkQueueCore};

/// Independent reactive reader kinds for queue lifecycle state.
#[derive(Debug, Clone, Copy)]
pub struct ThreadSafeWorkQueueReaderHandles {
    pub pending_len: Computed<usize>,
    pub is_empty: Computed<bool>,
    pub in_flight_len: Computed<usize>,
    pub dead_letter_len: Computed<usize>,
}

struct Inner<T, I> {
    core: Arc<Mutex<WorkQueueCore<T, I>>>,
    readers: ThreadSafeWorkQueueReaderHandles,
}

/// A `Send + Sync` pull-based work queue where N consumers compete for exclusive
/// delivery.
pub struct ThreadSafeWorkQueueCell<T, I = String> {
    inner: Arc<Inner<T, I>>,
}

impl<T, I> Clone for ThreadSafeWorkQueueCell<T, I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, I> ThreadSafeWorkQueueCell<T, I>
where
    T: PartialEq + Clone + Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Create an empty local-authority work queue.
    ///
    /// # Panics
    ///
    /// Panics unless `visibility_timeout > 0` and `max_deliveries >= 1`.
    pub fn new(ctx: &ThreadSafeContext, visibility_timeout: u64, max_deliveries: u32) -> Self {
        let core = Arc::new(Mutex::new(WorkQueueCore::new(
            visibility_timeout,
            max_deliveries,
        )));

        let pending_len = {
            let core = Arc::clone(&core);
            ctx.computed(move |_| core.lock().expect("work queue core").pending_len())
        };
        let is_empty = {
            let core = Arc::clone(&core);
            ctx.computed(move |_| core.lock().expect("work queue core").is_empty())
        };
        let in_flight_len = {
            let core = Arc::clone(&core);
            ctx.computed(move |_| core.lock().expect("work queue core").in_flight_len())
        };
        let dead_letter_len = {
            let core = Arc::clone(&core);
            ctx.computed(move |_| core.lock().expect("work queue core").dead_letter_len())
        };

        Self {
            inner: Arc::new(Inner {
                core,
                readers: ThreadSafeWorkQueueReaderHandles {
                    pending_len,
                    is_empty,
                    in_flight_len,
                    dead_letter_len,
                },
            }),
        }
    }

    /// Clear exactly the reader kinds the transition moved, in ONE frontier walk.
    /// MUST be called with the core lock released — see the module docs.
    fn invalidate(&self, ctx: &ThreadSafeContext, transition: Transition) {
        let ReaderChange {
            pending_len,
            is_empty,
            in_flight_len,
            dead_letter_len,
        } = transition.changed();
        ctx.batch(|_| {
            if pending_len {
                ctx.clear(&self.inner.readers.pending_len);
            }
            if is_empty {
                ctx.clear(&self.inner.readers.is_empty);
            }
            if in_flight_len {
                ctx.clear(&self.inner.readers.in_flight_len);
            }
            if dead_letter_len {
                ctx.clear(&self.inner.readers.dead_letter_len);
            }
        });
    }

    /// Append one item to the pending FIFO and return its stable identity.
    pub fn push(&self, ctx: &ThreadSafeContext, value: T) -> u64 {
        let (item_id, transition) = {
            let mut core = self.inner.core.lock().expect("work queue core");
            core.push(value)
        };
        self.invalidate(ctx, transition);
        item_id
    }

    /// Claim the oldest pending item for `worker`, or `None` when empty. The pop
    /// happens under the core lock, so competing workers cannot both win.
    pub fn claim(
        &self,
        ctx: &ThreadSafeContext,
        worker: I,
        now: u64,
    ) -> Option<WorkQueueDelivery<T, I>> {
        let (delivery, transition) = {
            let mut core = self.inner.core.lock().expect("work queue core");
            core.claim(worker, now)?
        };
        self.invalidate(ctx, transition);
        Some(delivery)
    }

    /// Settle a matching live delivery. Wrong-worker, stale, and duplicate acks
    /// are no-ops.
    pub fn ack(&self, ctx: &ThreadSafeContext, worker: &I, delivery_id: u64) -> bool {
        let transition = {
            let mut core = self.inner.core.lock().expect("work queue core");
            match core.ack(worker, delivery_id) {
                Some(transition) => transition,
                None => return false,
            }
        };
        self.invalidate(ctx, transition);
        true
    }

    /// Reject a matching delivery, requeueing at the tail or dead-lettering at
    /// the attempt limit.
    pub fn nack(&self, ctx: &ThreadSafeContext, worker: &I, delivery_id: u64) -> bool {
        let transition = {
            let mut core = self.inner.core.lock().expect("work queue core");
            match core.nack(worker, delivery_id) {
                Some(transition) => transition,
                None => return false,
            }
        };
        self.invalidate(ctx, transition);
        true
    }

    /// Requeue/dead-letter every lease with `deadline < now`, in delivery-id
    /// order.
    pub fn reap_expired(&self, ctx: &ThreadSafeContext, now: u64) -> usize {
        let (expired_count, transition) = {
            let mut core = self.inner.core.lock().expect("work queue core");
            match core.reap_expired(now) {
                Some(reaped) => reaped,
                None => return 0,
            }
        };
        self.invalidate(ctx, transition);
        expired_count
    }

    // -- Reader kinds.

    pub fn pending_len(&self, ctx: &ThreadSafeContext) -> usize {
        ctx.get(&self.inner.readers.pending_len)
    }

    pub fn is_empty(&self, ctx: &ThreadSafeContext) -> bool {
        ctx.get(&self.inner.readers.is_empty)
    }

    pub fn in_flight_len(&self, ctx: &ThreadSafeContext) -> usize {
        ctx.get(&self.inner.readers.in_flight_len)
    }

    pub fn dead_letter_len(&self, ctx: &ThreadSafeContext) -> usize {
        ctx.get(&self.inner.readers.dead_letter_len)
    }

    pub fn reader_handles(&self) -> ThreadSafeWorkQueueReaderHandles {
        self.inner.readers
    }

    // -- Non-reactive snapshots.

    /// Pending snapshot, oldest first.
    pub fn pending(&self) -> Vec<WorkQueueItem<T>> {
        self.inner
            .core
            .lock()
            .expect("work queue core")
            .pending_items()
    }

    /// In-flight snapshot sorted by delivery id.
    pub fn in_flight(&self) -> Vec<WorkQueueDelivery<T, I>> {
        self.inner
            .core
            .lock()
            .expect("work queue core")
            .in_flight_deliveries()
    }

    /// Terminal dead-letter snapshot in failure order.
    pub fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<T>> {
        self.inner
            .core
            .lock()
            .expect("work queue core")
            .dead_letters()
    }
}
