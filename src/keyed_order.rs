//! Graph-agnostic keyed bookkeeping core for the reactive map family.
//!
//! The three `ReactiveMap` flavors ([`ReactiveMap`](crate::ReactiveMap),
//! [`ThreadSafeReactiveMap`](crate::ThreadSafeReactiveMap),
//! [`AsyncReactiveMap`](crate::AsyncReactiveMap)) differ in exactly two places:
//! which interior-mutability wrapper guards their state, and which graph they
//! mint entry nodes and signal cells on. Everything *between* those — the
//! present set, the authoritative key order, `position`, and the atomic-move
//! algebra — is the same code three times over.
//!
//! [`KeyedOrder`] is that middle. It is the same "pure core split from the
//! reactive cell" seam the rest of the crate uses (`MembershipCore`,
//! `LeaseCore`, `CircuitBreakerCore`, …), applied to the map family:
//!
//! - **no context** — it never mints, reads, or writes a reactive node;
//! - **no closure** — it stores handles, not factories, so it needs no
//!   `Send + Sync` bound and cannot inherit one from a graph trait;
//! - **no interior mutability** — each flavor wraps it in its own `RefCell`
//!   (single-threaded) or `Mutex` (`Send + Sync`).
//!
//! What it deliberately does *not* own is **reactivity** over that bookkeeping.
//! Membership and order invalidation is `ctx.set` on a flavor's own signal
//! cells, which is context-typed and therefore stays in each shell. A core that
//! owned reactivity too would have to fake it with polled version counters —
//! which is precisely the shape lazily-zig's map has, and precisely why its
//! `keys()`/`len()` register no dependency edge. So the mutators here **report
//! what changed** ([`Mutation`], [`Move`]) and let the shell decide which
//! signals to bump.

use std::collections::HashMap;
use std::hash::Hash;

/// What an insert/remove did to the key set, so the caller knows whether to bump
/// its membership + order signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mutation {
    /// The key set changed (a key was added or removed). Bump membership *and*
    /// order — an add/remove changes the ordered key list too.
    Changed,
    /// Nothing changed (insert of a present key, remove of an absent one). Bump
    /// nothing: invalidating readers here would be a spurious wakeup.
    Unchanged,
}

/// The outcome of an atomic ordered move.
///
/// Split three ways rather than two because "the key was present" and "the order
/// actually changed" are different questions: the public `move_*` methods return
/// the former, while only the latter may bump the order signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Move {
    /// The key (or the anchor) is not in the order — nothing to move.
    Absent,
    /// The key is already at the requested position. A no-op: do **not**
    /// invalidate order readers.
    Unchanged,
    /// The key changed position. Bump the order signal — once. Membership is
    /// untouched: a reorder does not change set identity, so `len` /
    /// `contains_key` readers must stay cached.
    Reordered,
}

impl Move {
    /// Whether the move could be expressed at all — the `bool` the public
    /// `move_to` / `move_before` / `move_after` return.
    pub(crate) fn is_present(self) -> bool {
        !matches!(self, Move::Absent)
    }

    /// Whether the order actually changed, i.e. whether to bump the order signal.
    pub(crate) fn changed(self) -> bool {
        matches!(self, Move::Reordered)
    }
}

/// The present set plus its authoritative key order, with the move algebra.
///
/// `entries` and `order` are kept in lockstep: every key in `entries` appears
/// exactly once in `order` and vice versa. All mutators preserve that, including
/// on their failure paths — a partially-applied move that left a key in one and
/// not the other is the desync lazily-zig's `moveTo` can hit on allocator
/// failure.
pub(crate) struct KeyedOrder<K, H> {
    /// Per-key node handles. Each entry is its own reactive node in the owning
    /// graph; this core only stores the (`Copy`) handle.
    entries: HashMap<K, H>,
    /// Authoritative key list. Insertion-ordered until an atomic move reorders
    /// it; the snapshot returned by `keys`.
    order: Vec<K>,
}

impl<K, H> Default for KeyedOrder<K, H> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: Vec::new(),
        }
    }
}

impl<K, H> KeyedOrder<K, H>
where
    K: Eq + Hash + Clone,
    H: Copy,
{
    /// An empty core.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The handle for `key`, if present.
    pub(crate) fn get(&self, key: &K) -> Option<H> {
        self.entries.get(key).copied()
    }

    /// Whether `key` is in the present set.
    pub(crate) fn contains(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    /// Insert `handle` at `key`, appending to the order. If `key` is already
    /// present the existing handle is **kept** (first writer wins, so a key keeps
    /// a stable node identity) and the caller's `handle` is discarded.
    ///
    /// Returns the handle now bound to `key` and whether the key set grew.
    pub(crate) fn insert(&mut self, key: K, handle: H) -> (H, Mutation) {
        if let Some(existing) = self.entries.get(&key) {
            return (*existing, Mutation::Unchanged);
        }
        self.entries.insert(key.clone(), handle);
        self.order.push(key);
        (handle, Mutation::Changed)
    }

    /// Remove `key` from both planes, returning its handle if it was present.
    /// The caller is responsible for tearing the node down on its own graph.
    pub(crate) fn remove(&mut self, key: &K) -> (Option<H>, Mutation) {
        let Some(handle) = self.entries.remove(key) else {
            return (None, Mutation::Unchanged);
        };
        self.order.retain(|k| k != key);
        (Some(handle), Mutation::Changed)
    }

    /// Snapshot of the keys in their current order.
    pub(crate) fn keys(&self) -> Vec<K> {
        self.order.clone()
    }

    /// Number of present entries.
    pub(crate) fn len(&self) -> usize {
        self.order.len()
    }

    /// Current 0-based position of `key` in the order, or `None` if absent.
    pub(crate) fn position(&self, key: &K) -> Option<usize> {
        self.order.iter().position(|k| k == key)
    }

    /// Atomically move `key` to `index`.
    ///
    /// The entry keeps the **same** handle, dependents, and lineage — unlike a
    /// naive remove + re-mint, which reallocates the node and bumps membership
    /// twice. `index` is clamped to `[0, len)`.
    pub(crate) fn move_to(&mut self, key: &K, index: usize) -> Move {
        let Some(from) = self.position(key) else {
            return Move::Absent;
        };
        let to = index.min(self.order.len().saturating_sub(1));
        if from == to {
            return Move::Unchanged;
        }
        let k = self.order.remove(from);
        self.order.insert(to, k);
        Move::Reordered
    }

    /// Atomically move `key` to just before `anchor`. `Absent` if either key is
    /// missing.
    pub(crate) fn move_before(&mut self, key: &K, anchor: &K) -> Move {
        let (Some(anchor_idx), Some(from)) = (self.position(anchor), self.position(key)) else {
            return Move::Absent;
        };
        // Removing `key` first shifts `anchor` left by one when key precedes it.
        let target = if from < anchor_idx {
            anchor_idx - 1
        } else {
            anchor_idx
        };
        self.move_to(key, target)
    }

    /// Atomically move `key` to just after `anchor`. `Absent` if either key is
    /// missing.
    pub(crate) fn move_after(&mut self, key: &K, anchor: &K) -> Move {
        let (Some(anchor_idx), Some(from)) = (self.position(anchor), self.position(key)) else {
            return Move::Absent;
        };
        let target = if from <= anchor_idx {
            anchor_idx
        } else {
            anchor_idx + 1
        };
        self.move_to(key, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seeded(keys: &[&str]) -> KeyedOrder<String, u32> {
        let mut core = KeyedOrder::new();
        for (i, key) in keys.iter().enumerate() {
            core.insert((*key).to_string(), i as u32);
        }
        core
    }

    fn k(s: &str) -> String {
        s.to_string()
    }

    #[test]
    fn insert_appends_and_reports_growth() {
        let mut core = KeyedOrder::new();
        assert_eq!(core.insert(k("a"), 1), (1, Mutation::Changed));
        assert_eq!(core.insert(k("b"), 2), (2, Mutation::Changed));
        assert_eq!(core.keys(), vec![k("a"), k("b")]);
        assert_eq!(core.len(), 2);
    }

    #[test]
    fn reinsert_keeps_the_original_handle_and_reports_no_change() {
        let mut core = seeded(&["a"]);
        // First writer wins: the key keeps a stable node identity, and the caller
        // learns not to bump membership.
        assert_eq!(core.insert(k("a"), 99), (0, Mutation::Unchanged));
        assert_eq!(core.get(&k("a")), Some(0));
        assert_eq!(core.keys(), vec![k("a")]);
    }

    #[test]
    fn remove_clears_both_planes() {
        let mut core = seeded(&["a", "b", "c"]);
        let (handle, mutation) = core.remove(&k("b"));
        assert_eq!((handle, mutation), (Some(1), Mutation::Changed));
        assert_eq!(core.keys(), vec![k("a"), k("c")]);
        assert!(!core.contains(&k("b")));
        assert_eq!(core.position(&k("b")), None);
        // Absent remove is inert.
        assert_eq!(core.remove(&k("b")), (None, Mutation::Unchanged));
    }

    #[test]
    fn move_to_reorders_and_clamps() {
        let mut core = seeded(&["a", "b", "c", "d"]);
        assert_eq!(core.move_to(&k("b"), 3), Move::Reordered);
        assert_eq!(core.keys(), vec![k("a"), k("c"), k("d"), k("b")]);
        // Out-of-range index clamps to the last slot rather than panicking.
        assert_eq!(core.move_to(&k("a"), 99), Move::Reordered);
        assert_eq!(core.keys(), vec![k("c"), k("d"), k("b"), k("a")]);
    }

    #[test]
    fn move_to_same_position_is_unchanged_not_reordered() {
        let mut core = seeded(&["a", "b", "c"]);
        // The distinction the order signal depends on: a no-op move must not
        // invalidate order readers.
        assert_eq!(core.move_to(&k("b"), 1), Move::Unchanged);
        assert!(core.move_to(&k("b"), 1).is_present());
        assert!(!core.move_to(&k("b"), 1).changed());
        assert_eq!(core.keys(), vec![k("a"), k("b"), k("c")]);
    }

    #[test]
    fn move_absent_key_is_absent() {
        let mut core = seeded(&["a"]);
        assert_eq!(core.move_to(&k("zz"), 0), Move::Absent);
        assert!(!core.move_to(&k("zz"), 0).is_present());
        assert_eq!(core.move_before(&k("zz"), &k("a")), Move::Absent);
        assert_eq!(core.move_before(&k("a"), &k("zz")), Move::Absent);
        assert_eq!(core.move_after(&k("a"), &k("zz")), Move::Absent);
    }

    #[test]
    fn move_before_lands_immediately_ahead_of_the_anchor_from_either_side() {
        // Moving rightwards past the anchor.
        let mut core = seeded(&["a", "b", "c", "d"]);
        assert_eq!(core.move_before(&k("a"), &k("d")), Move::Reordered);
        assert_eq!(core.keys(), vec![k("b"), k("c"), k("a"), k("d")]);

        // Moving leftwards ahead of the anchor.
        let mut core = seeded(&["a", "b", "c", "d"]);
        assert_eq!(core.move_before(&k("d"), &k("a")), Move::Reordered);
        assert_eq!(core.keys(), vec![k("d"), k("a"), k("b"), k("c")]);
    }

    #[test]
    fn move_after_lands_immediately_behind_the_anchor_from_either_side() {
        let mut core = seeded(&["a", "b", "c", "d"]);
        assert_eq!(core.move_after(&k("a"), &k("c")), Move::Reordered);
        assert_eq!(core.keys(), vec![k("b"), k("c"), k("a"), k("d")]);

        let mut core = seeded(&["a", "b", "c", "d"]);
        assert_eq!(core.move_after(&k("d"), &k("a")), Move::Reordered);
        assert_eq!(core.keys(), vec![k("a"), k("d"), k("b"), k("c")]);
    }

    #[test]
    fn move_relative_to_an_adjacent_anchor_is_a_no_op() {
        let mut core = seeded(&["a", "b", "c"]);
        assert_eq!(core.move_before(&k("a"), &k("b")), Move::Unchanged);
        assert_eq!(core.move_after(&k("c"), &k("b")), Move::Unchanged);
        assert_eq!(core.keys(), vec![k("a"), k("b"), k("c")]);
    }

    #[test]
    fn entries_and_order_never_desync() {
        let mut core = seeded(&["a", "b", "c", "d", "e"]);
        core.move_to(&k("e"), 0);
        core.remove(&k("c"));
        core.move_before(&k("a"), &k("e"));
        core.insert(k("f"), 5);
        core.move_after(&k("b"), &k("f"));

        let keys = core.keys();
        assert_eq!(keys.len(), core.len());
        for key in &keys {
            assert!(core.contains(key), "`{key}` in order but not in entries");
            assert!(core.position(key).is_some());
        }
        // And nothing in `entries` is missing from `order`.
        for key in ["a", "b", "d", "e", "f"] {
            assert!(
                keys.contains(&k(key)),
                "`{key}` in entries but not in order"
            );
        }
        assert!(!core.contains(&k("c")));
    }
}
