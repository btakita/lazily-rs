//! Competing-consumer reactive work queue (`#lzworkqueue`).
//!
//! This is the portable local-authority lifecycle from lazily-spec. The owning
//! instance serializes `claim`; distributed/HA deployments put that decision
//! behind their leader or consensus log while preserving this API.

use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use crate::work_queue_core::{ReaderChange, Transition, WorkQueueCore};
use crate::{Computed, Context};

/// A stable queued item. `attempts` counts leases already issued for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkQueueItem<T> {
    pub item_id: u64,
    pub value: T,
    pub attempts: u32,
}

/// One exclusive, worker-owned delivery lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkQueueDelivery<T, I = String> {
    pub delivery_id: u64,
    pub item_id: u64,
    pub value: T,
    pub worker: I,
    pub attempt: u32,
    pub deadline: u64,
}

/// Why an item exhausted its delivery budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkQueueDeadLetterReason {
    Nack,
    Expired,
}

/// A terminal poison-message record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkQueueDeadLetter<T> {
    pub item_id: u64,
    pub value: T,
    pub attempts: u32,
    pub reason: WorkQueueDeadLetterReason,
}

/// Independent reactive reader kinds for queue lifecycle state.
#[derive(Debug, Clone, Copy)]
pub struct WorkQueueReaderHandles {
    pub pending_len: Computed<usize>,
    pub is_empty: Computed<bool>,
    pub in_flight_len: Computed<usize>,
    pub dead_letter_len: Computed<usize>,
}

struct WorkQueueInner<T, I> {
    core: Rc<RefCell<WorkQueueCore<T, I>>>,
    readers: WorkQueueReaderHandles,
}

/// A pull-based work queue where N consumers compete for exclusive delivery.
pub struct WorkQueueCell<T, I = String> {
    inner: Rc<WorkQueueInner<T, I>>,
}

impl<T, I> Clone for WorkQueueCell<T, I> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T, I> WorkQueueCell<T, I>
where
    T: PartialEq + Clone + 'static,
    I: Eq + Hash + Clone + 'static,
{
    /// Create an empty local-authority work queue.
    ///
    /// # Panics
    ///
    /// Panics unless `visibility_timeout > 0` and `max_deliveries >= 1`.
    pub fn new(ctx: &Context, visibility_timeout: u64, max_deliveries: u32) -> Self {
        let core = Rc::new(RefCell::new(WorkQueueCore::new(
            visibility_timeout,
            max_deliveries,
        )));

        let pending_len = {
            let core = Rc::clone(&core);
            ctx.computed(move |_| core.borrow().pending_len())
        };
        let is_empty = {
            let core = Rc::clone(&core);
            ctx.computed(move |_| core.borrow().is_empty())
        };
        let in_flight_len = {
            let core = Rc::clone(&core);
            ctx.computed(move |_| core.borrow().in_flight_len())
        };
        let dead_letter_len = {
            let core = Rc::clone(&core);
            ctx.computed(move |_| core.borrow().dead_letter_len())
        };

        Self {
            inner: Rc::new(WorkQueueInner {
                core,
                readers: WorkQueueReaderHandles {
                    pending_len,
                    is_empty,
                    in_flight_len,
                    dead_letter_len,
                },
            }),
        }
    }

    /// Clear exactly the reader kinds the transition moved, in ONE frontier walk,
    /// so no subscriber observes `pending_len` bumped while `is_empty` still
    /// reads stale. Called with the core's borrow released.
    fn invalidate(&self, ctx: &Context, transition: Transition) {
        let ReaderChange {
            pending_len,
            is_empty,
            in_flight_len,
            dead_letter_len,
        } = transition.changed();
        let mut roots = Vec::with_capacity(4);
        if pending_len {
            roots.push(self.inner.readers.pending_len.id);
        }
        if is_empty {
            roots.push(self.inner.readers.is_empty.id);
        }
        if in_flight_len {
            roots.push(self.inner.readers.in_flight_len.id);
        }
        if dead_letter_len {
            roots.push(self.inner.readers.dead_letter_len.id);
        }
        ctx.clear_slots(&roots);
    }

    /// Append one item to the pending FIFO and return its stable identity.
    pub fn push(&self, ctx: &Context, value: T) -> u64 {
        let (item_id, transition) = self.inner.core.borrow_mut().push(value);
        self.invalidate(ctx, transition);
        item_id
    }

    /// Claim the oldest pending item for `worker`, or `None` when empty.
    pub fn claim(&self, ctx: &Context, worker: I, now: u64) -> Option<WorkQueueDelivery<T, I>> {
        let (delivery, transition) = self.inner.core.borrow_mut().claim(worker, now)?;
        self.invalidate(ctx, transition);
        Some(delivery)
    }

    /// Settle a matching live delivery. Wrong-worker, stale, and duplicate acks are no-ops.
    pub fn ack(&self, ctx: &Context, worker: &I, delivery_id: u64) -> bool {
        let Some(transition) = self.inner.core.borrow_mut().ack(worker, delivery_id) else {
            return false;
        };
        self.invalidate(ctx, transition);
        true
    }

    /// Reject a matching delivery, requeueing at the tail or dead-lettering at the limit.
    pub fn nack(&self, ctx: &Context, worker: &I, delivery_id: u64) -> bool {
        let Some(transition) = self.inner.core.borrow_mut().nack(worker, delivery_id) else {
            return false;
        };
        self.invalidate(ctx, transition);
        true
    }

    /// Requeue/dead-letter every lease with `deadline < now` in delivery-id order.
    pub fn reap_expired(&self, ctx: &Context, now: u64) -> usize {
        let Some((expired_count, transition)) = self.inner.core.borrow_mut().reap_expired(now)
        else {
            return 0;
        };
        self.invalidate(ctx, transition);
        expired_count
    }

    pub fn pending_len(&self, ctx: &Context) -> usize {
        ctx.get(&self.inner.readers.pending_len)
    }

    pub fn is_empty(&self, ctx: &Context) -> bool {
        ctx.get(&self.inner.readers.is_empty)
    }

    pub fn in_flight_len(&self, ctx: &Context) -> usize {
        ctx.get(&self.inner.readers.in_flight_len)
    }

    pub fn dead_letter_len(&self, ctx: &Context) -> usize {
        ctx.get(&self.inner.readers.dead_letter_len)
    }

    pub fn reader_handles(&self) -> WorkQueueReaderHandles {
        self.inner.readers
    }

    /// Non-reactive pending snapshot, oldest first.
    pub fn pending(&self) -> Vec<WorkQueueItem<T>> {
        self.inner.core.borrow().pending_items()
    }

    /// Non-reactive in-flight snapshot sorted by delivery id.
    pub fn in_flight(&self) -> Vec<WorkQueueDelivery<T, I>> {
        self.inner.core.borrow().in_flight_deliveries()
    }

    /// Non-reactive terminal dead-letter snapshot in failure order.
    pub fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<T>> {
        self.inner.core.borrow().dead_letters()
    }
}
