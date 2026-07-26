//! `WorkQueueCore` — the graph-agnostic competing-consumer lifecycle behind
//! every `WorkQueueCell` flavor (`#lazilythreadsafeworkqueu`).
//!
//! Same split as [`TopicCore`](crate::topic_core) and
//! [`KeyedOrder`](crate::keyed_order): the lease/ack/nack/redelivery/DLQ state
//! machine touches no handle and awaits nothing, so all three flavors share it
//! verbatim, while each mints its own reader kinds on its own graph.
//!
//! The clock is **not** flavor-specific. `now` stays a caller argument on every
//! flavor, including async, so redelivery is deterministic and fixture-
//! replayable rather than owned by a per-flavor timer.
//!
//! Every mutator returns a [`Transition`], and the invalidation set is derived
//! from it by [`Transition::changed`] alone — a pure function of
//! `(counts_before, counts_after)`, never of the op. That is the property
//! `LazilyFormal.QueueReaderKinds.invalidationSet_no_derive` pins, and it is why
//! three shells can share one invalidation rule without re-deriving values.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use crate::work_queue::{
    WorkQueueDeadLetter, WorkQueueDeadLetterReason, WorkQueueDelivery, WorkQueueItem,
};

/// The three independent size observations a reader kind can project.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counts {
    pub pending: usize,
    pub in_flight: usize,
    pub dead_letters: usize,
}

/// One completed mutation, as the before/after counts a shell needs and nothing
/// else. Ops that changed nothing never produce one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Transition {
    pub before: Counts,
    pub after: Counts,
}

/// Which reader kinds this transition moved. Independent by construction: an ack
/// drains in-flight without touching pending, and a claim on a queue that stays
/// non-empty never flips `is_empty`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReaderChange {
    pub pending_len: bool,
    pub is_empty: bool,
    pub in_flight_len: bool,
    pub dead_letter_len: bool,
}

impl Transition {
    pub fn changed(&self) -> ReaderChange {
        ReaderChange {
            pending_len: self.before.pending != self.after.pending,
            is_empty: (self.before.pending == 0) != (self.after.pending == 0),
            in_flight_len: self.before.in_flight != self.after.in_flight,
            dead_letter_len: self.before.dead_letters != self.after.dead_letters,
        }
    }
}

/// Pending FIFO, live leases, and the terminal dead-letter log, plus the two
/// policy knobs that decide redelivery. No context, no handles, no interior
/// mutability.
pub(crate) struct WorkQueueCore<T, I> {
    pending: VecDeque<WorkQueueItem<T>>,
    in_flight: HashMap<u64, WorkQueueDelivery<T, I>>,
    dead_letters: Vec<WorkQueueDeadLetter<T>>,
    next_item_id: u64,
    next_delivery_id: u64,
    visibility_timeout: u64,
    max_deliveries: u32,
}

impl<T, I> WorkQueueCore<T, I>
where
    T: Clone,
    I: Eq + Hash + Clone,
{
    /// # Panics
    ///
    /// Panics unless `visibility_timeout > 0` and `max_deliveries >= 1`.
    pub fn new(visibility_timeout: u64, max_deliveries: u32) -> Self {
        assert!(
            visibility_timeout > 0,
            "visibility_timeout must be positive"
        );
        assert!(max_deliveries >= 1, "max_deliveries must be at least one");
        Self {
            pending: VecDeque::new(),
            in_flight: HashMap::new(),
            dead_letters: Vec::new(),
            next_item_id: 0,
            next_delivery_id: 0,
            visibility_timeout,
            max_deliveries,
        }
    }

    fn counts(&self) -> Counts {
        Counts {
            pending: self.pending.len(),
            in_flight: self.in_flight.len(),
            dead_letters: self.dead_letters.len(),
        }
    }

    /// Requeue at the tail while the budget lasts, else route to the DLQ.
    fn fail_delivery(
        &mut self,
        delivery: WorkQueueDelivery<T, I>,
        reason: WorkQueueDeadLetterReason,
    ) {
        if delivery.attempt < self.max_deliveries {
            self.pending.push_back(WorkQueueItem {
                item_id: delivery.item_id,
                value: delivery.value,
                attempts: delivery.attempt,
            });
        } else {
            self.dead_letters.push(WorkQueueDeadLetter {
                item_id: delivery.item_id,
                value: delivery.value,
                attempts: delivery.attempt,
                reason,
            });
        }
    }

    /// Append one item to the pending FIFO and return its stable identity.
    pub fn push(&mut self, value: T) -> (u64, Transition) {
        let before = self.counts();
        let item_id = self.next_item_id;
        self.next_item_id = self.next_item_id.checked_add(1).expect("item id exhausted");
        self.pending.push_back(WorkQueueItem {
            item_id,
            value,
            attempts: 0,
        });
        (
            item_id,
            Transition {
                before,
                after: self.counts(),
            },
        )
    }

    /// Claim the oldest pending item for `worker`, or `None` when empty. The
    /// delivery id is fresh on every attempt while the item id stays stable, so
    /// a redelivered item cannot be settled with a stale lease.
    pub fn claim(&mut self, worker: I, now: u64) -> Option<(WorkQueueDelivery<T, I>, Transition)> {
        let before = self.counts();
        self.pending.front()?;
        let next_delivery_id = self
            .next_delivery_id
            .checked_add(1)
            .expect("delivery id exhausted");
        let item = self.pending.pop_front()?;
        let delivery_id = self.next_delivery_id;
        self.next_delivery_id = next_delivery_id;
        let delivery = WorkQueueDelivery {
            delivery_id,
            item_id: item.item_id,
            value: item.value,
            worker,
            attempt: item.attempts.saturating_add(1),
            deadline: now.saturating_add(self.visibility_timeout),
        };
        self.in_flight.insert(delivery_id, delivery.clone());
        Some((
            delivery,
            Transition {
                before,
                after: self.counts(),
            },
        ))
    }

    fn owns(&self, worker: &I, delivery_id: u64) -> bool {
        self.in_flight
            .get(&delivery_id)
            .is_some_and(|delivery| &delivery.worker == worker)
    }

    /// Settle a matching live delivery. Wrong-worker, stale, and duplicate acks
    /// are no-ops that invalidate nothing.
    pub fn ack(&mut self, worker: &I, delivery_id: u64) -> Option<Transition> {
        if !self.owns(worker, delivery_id) {
            return None;
        }
        let before = self.counts();
        self.in_flight.remove(&delivery_id);
        Some(Transition {
            before,
            after: self.counts(),
        })
    }

    /// Reject a matching delivery, requeueing at the tail or dead-lettering at
    /// the attempt limit.
    pub fn nack(&mut self, worker: &I, delivery_id: u64) -> Option<Transition> {
        if !self.owns(worker, delivery_id) {
            return None;
        }
        let before = self.counts();
        let delivery = self
            .in_flight
            .remove(&delivery_id)
            .expect("delivery exists");
        self.fail_delivery(delivery, WorkQueueDeadLetterReason::Nack);
        Some(Transition {
            before,
            after: self.counts(),
        })
    }

    /// Requeue/dead-letter every lease with `deadline < now`, in delivery-id
    /// order so redelivery is deterministic. `None` when nothing expired.
    pub fn reap_expired(&mut self, now: u64) -> Option<(usize, Transition)> {
        let mut expired: Vec<u64> = self
            .in_flight
            .iter()
            .filter_map(|(id, delivery)| (delivery.deadline < now).then_some(*id))
            .collect();
        if expired.is_empty() {
            return None;
        }
        expired.sort_unstable();
        let before = self.counts();
        for delivery_id in &expired {
            let delivery = self
                .in_flight
                .remove(delivery_id)
                .expect("expired delivery exists");
            self.fail_delivery(delivery, WorkQueueDeadLetterReason::Expired);
        }
        Some((
            expired.len(),
            Transition {
                before,
                after: self.counts(),
            },
        ))
    }

    // -- Reader-kind bodies. Each flavor's compute closure calls exactly one.

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    pub fn dead_letter_len(&self) -> usize {
        self.dead_letters.len()
    }

    // -- Non-reactive snapshots.

    pub fn pending_items(&self) -> Vec<WorkQueueItem<T>> {
        self.pending.iter().cloned().collect()
    }

    pub fn in_flight_deliveries(&self) -> Vec<WorkQueueDelivery<T, I>> {
        let mut deliveries: Vec<_> = self.in_flight.values().cloned().collect();
        deliveries.sort_by_key(|delivery| delivery.delivery_id);
        deliveries
    }

    pub fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<T>> {
        self.dead_letters.clone()
    }
}
