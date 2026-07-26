//! `TopicCore` — the graph-agnostic broadcast-log algebra behind every
//! `TopicCell` flavor (`#lazilythreadsafetopiccel`).
//!
//! Same split as [`KeyedOrder`](crate::keyed_order) does for the map family, and
//! for the same reason: the cursor/retention/GC algebra touches no handle and
//! awaits nothing, so it can be shared verbatim by the single-threaded,
//! `Send + Sync`, and async shells — while **reactivity deliberately stays out**.
//! Invalidation is a graph write, so each flavor mints its own per-subscriber
//! reader on its own graph and clears it with the storage lock released.
//!
//! Every mutator therefore returns *which subscribers changed* rather than
//! performing the invalidation itself. That return value is the whole contract
//! between the core and a shell: the invalidation set is a pure function of the
//! transition (`LazilyFormal.QueueReaderKinds.invalidationSet_no_derive`), which
//! is exactly what makes the plane portable across flavors without re-deriving
//! values per flavor.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use crate::queue::{
    TopicDurability, TopicSnapshot, TopicSubscribeOutcome, TopicSubscriptionSnapshot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopicSubscription {
    pub cursor: u64,
    pub durability: TopicDurability,
    pub connected: bool,
}

/// Retained append log plus the stable-identity cursor table. No context, no
/// handles, no interior mutability — each flavor wraps this in its own
/// `RefCell`/`Mutex`.
pub(crate) struct TopicCore<T, I> {
    base_offset: u64,
    elements: VecDeque<T>,
    subscriptions: HashMap<I, TopicSubscription>,
}

impl<T, I> TopicCore<T, I>
where
    T: Clone,
    I: Eq + Hash + Clone,
{
    /// An empty topic at absolute offset zero.
    pub fn new() -> Self {
        Self {
            base_offset: 0,
            elements: VecDeque::new(),
            subscriptions: HashMap::new(),
        }
    }

    /// Rebuild from a durable snapshot without moving any cursor.
    ///
    /// # Panics
    ///
    /// Panics if any cursor lies outside `base_offset..=end_offset`, or if a
    /// disconnected ephemeral record survived into the snapshot — both are
    /// states the transition algebra cannot produce, so accepting them would
    /// let a restart resurrect a subscription the live path had removed.
    pub fn from_snapshot(snapshot: TopicSnapshot<T, I>) -> Self {
        let end_offset = snapshot.base_offset + snapshot.elements.len() as u64;
        for sub in snapshot.subscriptions.values() {
            assert!(
                (snapshot.base_offset..=end_offset).contains(&sub.cursor),
                "TopicCell cursor must be within the retained absolute offset range"
            );
            assert!(
                sub.durability != TopicDurability::Ephemeral || sub.connected,
                "disconnected ephemeral TopicCell subscriptions must be removed"
            );
        }

        Self {
            base_offset: snapshot.base_offset,
            elements: snapshot.elements.into(),
            subscriptions: snapshot
                .subscriptions
                .into_iter()
                .map(|(id, sub)| {
                    (
                        id,
                        TopicSubscription {
                            cursor: sub.cursor,
                            durability: sub.durability,
                            connected: sub.connected,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Create a cursor at the current tail, or reconnect an existing durable
    /// identity without moving its cursor. Once an identity exists its stored
    /// durability wins over the caller's argument.
    ///
    /// The `bool` is whether the shell must clear that subscriber's reader. It
    /// is false for `Created` (the shell mints a fresh handle, which is already
    /// clean) and false for `AlreadyConnected` (nothing moved).
    pub fn subscribe(
        &mut self,
        id: I,
        durability: TopicDurability,
    ) -> (TopicSubscribeOutcome, bool) {
        if let Some(sub) = self.subscriptions.get_mut(&id) {
            if sub.connected {
                return (TopicSubscribeOutcome::AlreadyConnected, false);
            }
            sub.connected = true;
            return (TopicSubscribeOutcome::Reconnected, true);
        }
        let cursor = self.end_offset();
        self.subscriptions.insert(
            id,
            TopicSubscription {
                cursor,
                durability,
                connected: true,
            },
        );
        (TopicSubscribeOutcome::Created, false)
    }

    /// Disconnect a subscriber. Durable records stay offline at the same cursor;
    /// ephemeral records are removed outright.
    ///
    /// `None` means nothing changed. `Some(remove_reader)` means the shell must
    /// clear that subscriber's reader, dropping the handle first when the record
    /// itself is gone.
    pub fn disconnect(&mut self, id: &I) -> Option<bool> {
        let sub = self.subscriptions.get_mut(id)?;
        if !sub.connected {
            return None;
        }
        if sub.durability == TopicDurability::Ephemeral {
            self.subscriptions.remove(id);
            Some(true)
        } else {
            sub.connected = false;
            Some(false)
        }
    }

    /// Append exactly one element, leaving every cursor unchanged. Returns its
    /// absolute offset and the connected subscribers whose unread suffix grew —
    /// a subscriber already past this offset is not one of them.
    pub fn publish(&mut self, value: T) -> (u64, Vec<I>) {
        let offset = self.end_offset();
        self.elements.push_back(value);
        let connected = self
            .subscriptions
            .iter()
            .filter(|(_, sub)| sub.connected && sub.cursor <= offset)
            .map(|(id, _)| id.clone())
            .collect();
        (offset, connected)
    }

    /// Advance only the named connected cursor by one, returning the element it
    /// passed. At the tail, for an offline subscriber, or for an unknown id this
    /// is a no-op returning `None` — and a `None` invalidates nothing.
    pub fn advance(&mut self, id: &I) -> Option<T> {
        let (cursor, connected) = self
            .subscriptions
            .get(id)
            .map(|sub| (sub.cursor, sub.connected))?;
        if !connected || cursor >= self.end_offset() {
            return None;
        }
        let index = cursor.saturating_sub(self.base_offset) as usize;
        let value = self.elements.get(index).cloned()?;
        self.subscriptions
            .get_mut(id)
            .expect("subscription exists")
            .cursor += 1;
        Some(value)
    }

    /// Drop the prefix below the minimum durable cursor, or the whole retained
    /// log when no durable subscription exists. Cursors are absolute, so safe GC
    /// changes no reader's value and returns no invalidation set at all.
    pub fn gc(&mut self) -> usize {
        let end_offset = self.end_offset();
        let frontier = self
            .subscriptions
            .values()
            .filter(|sub| sub.durability == TopicDurability::Durable)
            .map(|sub| sub.cursor)
            .min()
            .unwrap_or(end_offset);
        let remove = frontier.saturating_sub(self.base_offset) as usize;
        self.elements.drain(..remove);
        self.base_offset = frontier;
        remove
    }

    /// The reader-kind body: one subscriber's unread suffix. Unknown and offline
    /// subscribers observe an empty stream.
    pub fn read_suffix(&self, id: &I) -> Vec<T> {
        let Some(sub) = self.subscriptions.get(id) else {
            return Vec::new();
        };
        if !sub.connected {
            return Vec::new();
        }
        let skip = sub.cursor.saturating_sub(self.base_offset) as usize;
        self.elements.iter().skip(skip).cloned().collect()
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn end_offset(&self) -> u64 {
        self.base_offset + self.elements.len() as u64
    }

    pub fn elements(&self) -> Vec<T> {
        self.elements.iter().cloned().collect()
    }

    pub fn subscription(&self, id: &I) -> Option<TopicSubscriptionSnapshot> {
        self.subscriptions
            .get(id)
            .map(|sub| TopicSubscriptionSnapshot {
                cursor: sub.cursor,
                durability: sub.durability,
                connected: sub.connected,
            })
    }

    pub fn subscription_ids(&self) -> Vec<I> {
        self.subscriptions.keys().cloned().collect()
    }

    pub fn snapshot(&self) -> TopicSnapshot<T, I> {
        TopicSnapshot {
            base_offset: self.base_offset,
            elements: self.elements.iter().cloned().collect(),
            subscriptions: self
                .subscriptions
                .iter()
                .map(|(id, sub)| {
                    (
                        id.clone(),
                        TopicSubscriptionSnapshot {
                            cursor: sub.cursor,
                            durability: sub.durability,
                            connected: sub.connected,
                        },
                    )
                })
                .collect(),
        }
    }
}
