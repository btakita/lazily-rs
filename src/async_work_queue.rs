//! `AsyncWorkQueueCell` — the `AsyncContext` flavor of [`WorkQueueCell`]
//! (`#lazilythreadsafeworkqueu`).
//!
//! Completes the work-queue triple over the same flavor-neutral
//! [`WorkQueueCore`](crate::work_queue_core), with the same
//! `Transition::changed` invalidation rule as the other two flavors.
//!
//! **Neither the lease nor the clock is async-coloured.** `claim` returns a
//! delivery, not a future: the decision is a pop from local storage the graph
//! does not own, so there is nothing to await. And `reap_expired` stays
//! caller-driven with an explicit `now` rather than the flavor owning a timer —
//! that keeps redelivery deterministic and replayable against the `workqueue_*`
//! fixtures, which is worth more than the convenience of an ambient clock.
//!
//! Reader kinds use [`AsyncContext::computed`] (sync compute, async graph) and
//! return plain values, so `pending_len()` is a number rather than an `Option`
//! needing a settle step. What stays genuinely async is whatever a *caller*
//! composes on top.
//!
//! Lock discipline follows the thread-safe shell: release `core` before touching
//! the context, or an op inverts lock order against a concurrent reader.
//! Multi-root invalidation goes through [`AsyncContext::clear_slots`].

use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext};
use crate::context::SlotId;
use crate::work_queue::{WorkQueueDeadLetter, WorkQueueDelivery, WorkQueueItem};
use crate::work_queue_core::{ReaderChange, Transition, WorkQueueCore};

/// Independent reactive reader kinds for queue lifecycle state.
#[derive(Debug, Clone, Copy)]
pub struct AsyncWorkQueueReaderHandles {
    pub pending_len: AsyncComputed<usize>,
    pub is_empty: AsyncComputed<bool>,
    pub in_flight_len: AsyncComputed<usize>,
    pub dead_letter_len: AsyncComputed<usize>,
}

struct Inner<T, I> {
    core: Arc<Mutex<WorkQueueCore<T, I>>>,
    readers: AsyncWorkQueueReaderHandles,
}

/// An `AsyncContext` pull-based work queue where N consumers compete for
/// exclusive delivery.
pub struct AsyncWorkQueueCell<T, I = String> {
    inner: Arc<Inner<T, I>>,
}

impl<T, I> Clone for AsyncWorkQueueCell<T, I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, I> AsyncWorkQueueCell<T, I>
where
    T: PartialEq + Clone + Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Create an empty local-authority work queue.
    ///
    /// # Panics
    ///
    /// Panics unless `visibility_timeout > 0` and `max_deliveries >= 1`.
    pub fn new(ctx: &AsyncContext, visibility_timeout: u64, max_deliveries: u32) -> Self {
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
                readers: AsyncWorkQueueReaderHandles {
                    pending_len,
                    is_empty,
                    in_flight_len,
                    dead_letter_len,
                },
            }),
        }
    }

    /// Clear exactly the reader kinds the transition moved, in ONE frontier walk.
    /// MUST be called with the core lock released.
    fn invalidate(&self, ctx: &AsyncContext, transition: Transition) {
        let ReaderChange {
            pending_len,
            is_empty,
            in_flight_len,
            dead_letter_len,
        } = transition.changed();
        let mut roots: Vec<SlotId> = Vec::with_capacity(4);
        if pending_len {
            roots.push(self.inner.readers.pending_len.id());
        }
        if is_empty {
            roots.push(self.inner.readers.is_empty.id());
        }
        if in_flight_len {
            roots.push(self.inner.readers.in_flight_len.id());
        }
        if dead_letter_len {
            roots.push(self.inner.readers.dead_letter_len.id());
        }
        ctx.clear_slots(&roots);
    }

    /// Append one item to the pending FIFO and return its stable identity.
    pub fn push(&self, ctx: &AsyncContext, value: T) -> u64 {
        let (item_id, transition) = {
            let mut core = self.inner.core.lock().expect("work queue core");
            core.push(value)
        };
        self.invalidate(ctx, transition);
        item_id
    }

    /// Claim the oldest pending item for `worker`, or `None` when empty.
    pub fn claim(
        &self,
        ctx: &AsyncContext,
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
    pub fn ack(&self, ctx: &AsyncContext, worker: &I, delivery_id: u64) -> bool {
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
    pub fn nack(&self, ctx: &AsyncContext, worker: &I, delivery_id: u64) -> bool {
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
    pub fn reap_expired(&self, ctx: &AsyncContext, now: u64) -> usize {
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

    // -- Reader kinds. Plain values: the compute is synchronous, so the cell
    // resolves inline on read and `None` is unreachable.

    pub fn pending_len(&self, ctx: &AsyncContext) -> usize {
        ctx.get(&self.inner.readers.pending_len)
            .expect("sync compute resolves inline")
    }

    pub fn is_empty(&self, ctx: &AsyncContext) -> bool {
        ctx.get(&self.inner.readers.is_empty)
            .expect("sync compute resolves inline")
    }

    pub fn in_flight_len(&self, ctx: &AsyncContext) -> usize {
        ctx.get(&self.inner.readers.in_flight_len)
            .expect("sync compute resolves inline")
    }

    pub fn dead_letter_len(&self, ctx: &AsyncContext) -> usize {
        ctx.get(&self.inner.readers.dead_letter_len)
            .expect("sync compute resolves inline")
    }

    pub fn reader_handles(&self) -> AsyncWorkQueueReaderHandles {
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
