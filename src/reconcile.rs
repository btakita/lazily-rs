//! Keyed reconciliation with LIS move-minimization (#lzkeyrecon).
//!
//! Given an **old** and a **new** keyed sequence, [`reconcile`] emits the
//! minimal set of [`DiffOp`]s — `{Insert, Remove, Move, Update}` — that
//! transforms old into new, **keyed by stable id, not position**. Reordering is
//! minimized with a longest-increasing-subsequence (LIS) pass over the matched
//! keys (the Vue 3 / Inferno technique): the keys already in relative order form
//! the LIS and stay put; only the rest emit `Move` ops.
//!
//! This is the algorithm that turns a structural document edit into **minimal
//! per-item ops** instead of a whole-subtree replace — the enabling step for
//! per-cell CRDT merge. [`apply_to_map`] (and [`SourceMap::reconcile`]) drive a
//! reactive [`SourceMap`] from the op set, while [`apply_to_tree`] applies the
//! same keyed child edits to a [`SourceTree`] level. In both cases an unchanged
//! ("stable") entry's value cell is **never invalidated** by a sibling reorder.
//!
//! ```
//! use lazily::{reconcile, DiffOp, Context, SourceMap};
//!
//! let old = [("a", 1), ("b", 2), ("c", 3)];
//! let new = [("c", 3), ("a", 1), ("b", 2)];
//! let ops = reconcile(&old, &new);
//! // Pure reorder: only Move ops, and fewer than a naive remove+reinsert.
//! assert!(ops.iter().all(|op| matches!(op, DiffOp::Move { .. })));
//!
//! // Apply to a live reactive collection.
//! let ctx = Context::new();
//! let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
//! for (k, v) in old { map.entry(&ctx, k, v); }
//! map.reconcile(&ctx, &new);
//! assert_eq!(map.keys(&ctx), vec!["c", "a", "b"]);
//! ```

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::Context;
use crate::cell_family::SourceMap;
use crate::source_tree::SourceTree;

/// A single reconciliation operation, keyed by stable id.
///
/// `index` / `to` are positions in the **final** (new) sequence. When applied in
/// emitted order — removes first, then inserts/moves left-to-right by index,
/// then updates — they transform the old sequence into the new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffOp<K, V> {
    /// A key present in `new` but not `old`; place its value at `index`.
    Insert { key: K, value: V, index: usize },
    /// A key present in `old` but not `new`.
    Remove { key: K },
    /// A key present in both but out of order; move it to `to`. The entry keeps
    /// its identity (this is the atomic-move, `#lzcellmove`, case).
    Move { key: K, to: usize },
    /// A key present in both whose value changed.
    Update { key: K, value: V },
}

/// Reconcile `old` → `new` by stable key, returning the minimal op set.
///
/// Move count equals `(keys in both) - |LIS|`, which is the minimum number of
/// single-element moves needed to reorder — strictly fewer than the naive
/// remove-all + insert-all whenever any key is shared.
pub fn reconcile<K, V>(old: &[(K, V)], new: &[(K, V)]) -> Vec<DiffOp<K, V>>
where
    K: Eq + Hash + Clone,
    V: Clone + PartialEq,
{
    let old_pos: HashMap<&K, usize> = old.iter().enumerate().map(|(i, (k, _))| (k, i)).collect();
    let old_val: HashMap<&K, &V> = old.iter().map(|(k, v)| (k, v)).collect();
    let new_keys: HashSet<&K> = new.iter().map(|(k, _)| k).collect();

    let mut ops = Vec::new();

    // 1. Removes — old keys absent from new, in old order.
    for (k, _) in old {
        if !new_keys.contains(k) {
            ops.push(DiffOp::Remove { key: k.clone() });
        }
    }

    // 2. Common keys in new order → their old indices form the LIS input.
    let mut common_new_idx: Vec<usize> = Vec::new();
    let mut seq: Vec<usize> = Vec::new();
    for (i, (k, _)) in new.iter().enumerate() {
        if let Some(&oi) = old_pos.get(k) {
            common_new_idx.push(i);
            seq.push(oi);
        }
    }
    // Stable (no-move) keys are the ones whose new-index is in the LIS.
    let stable: HashSet<usize> = longest_increasing_subsequence(&seq)
        .into_iter()
        .map(|j| common_new_idx[j])
        .collect();

    // 3. Inserts + Moves, left-to-right in new order (positions <i are settled).
    for (i, (k, v)) in new.iter().enumerate() {
        if old_pos.contains_key(k) {
            if !stable.contains(&i) {
                ops.push(DiffOp::Move {
                    key: k.clone(),
                    to: i,
                });
            }
        } else {
            ops.push(DiffOp::Insert {
                key: k.clone(),
                value: v.clone(),
                index: i,
            });
        }
    }

    // 4. Updates — common keys whose value changed.
    for (k, v) in new {
        if let Some(&ov) = old_val.get(k)
            && ov != v
        {
            ops.push(DiffOp::Update {
                key: k.clone(),
                value: v.clone(),
            });
        }
    }

    ops
}

/// Apply a reconcile op set to a live [`SourceMap`], driving it to the new shape
/// with minimal invalidation: stable entries are untouched (their value cells
/// keep their dependents cached), moves use [`SourceMap::move_to`] (`#lzcellmove`),
/// and only changed values are written.
pub fn apply_to_map<K, V>(ctx: &Context, map: &SourceMap<K, V>, ops: &[DiffOp<K, V>])
where
    K: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
{
    for op in ops {
        match op {
            DiffOp::Remove { key } => {
                map.remove(ctx, key);
            }
            DiffOp::Insert { key, value, index } => {
                map.entry(ctx, key.clone(), value.clone());
                map.move_to(ctx, key, *index);
            }
            DiffOp::Move { key, to } => {
                map.move_to(ctx, key, *to);
            }
            DiffOp::Update { key, value } => {
                map.set(ctx, key.clone(), value.clone());
            }
        }
    }
}

/// Apply a reconcile op set to one level of a live [`SourceTree`].
///
/// This is the tree-shaped companion to [`apply_to_map`]: inserts attach a leaf
/// child and move it into place, moves use [`SourceTree::move_child`], removes
/// detach the child subtree, and updates write only the child node's value.
pub fn apply_to_tree<K, V>(ctx: &Context, tree: &SourceTree<K, V>, ops: &[DiffOp<K, V>])
where
    K: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
{
    for op in ops {
        match op {
            DiffOp::Remove { key } => {
                tree.remove_child(ctx, key);
            }
            DiffOp::Insert { key, value, index } => {
                tree.insert_child(ctx, key.clone(), value.clone());
                tree.move_child(ctx, key, *index);
            }
            DiffOp::Move { key, to } => {
                tree.move_child(ctx, key, *to);
            }
            DiffOp::Update { key, value } => {
                if let Some(child) = tree.child(key) {
                    child.set(ctx, value.clone());
                }
            }
        }
    }
}

impl<K, V> SourceMap<K, V>
where
    K: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
{
    /// Reconcile this collection's current entries toward `new` (key + value),
    /// applying the minimal `#lzkeyrecon` op set. A stable entry keeps its value
    /// cell and dependents; siblings reorder via atomic moves.
    pub fn reconcile(&self, ctx: &Context, new: &[(K, V)]) {
        let old: Vec<(K, V)> = self
            .keys(ctx)
            .into_iter()
            .filter_map(|k| self.get(ctx, &k).map(|v| (k, v)))
            .collect();
        let ops = reconcile(&old, new);
        apply_to_map(ctx, self, &ops);
    }
}

/// Indices (into `seq`) of one longest **strictly increasing** subsequence,
/// via patience sorting (O(n log n)). Used to find the keys already in relative
/// order so they don't need `Move` ops.
fn longest_increasing_subsequence(seq: &[usize]) -> Vec<usize> {
    let n = seq.len();
    if n == 0 {
        return Vec::new();
    }
    // tails[k] = index into seq of the smallest tail of an increasing subseq of
    // length k+1; prev[i] = predecessor index for reconstruction.
    let mut tails: Vec<usize> = Vec::new();
    let mut prev: Vec<usize> = vec![usize::MAX; n];
    for i in 0..n {
        // First tail whose value is >= seq[i] (strict LIS).
        let mut lo = 0;
        let mut hi = tails.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if seq[tails[mid]] < seq[i] {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo > 0 {
            prev[i] = tails[lo - 1];
        }
        if lo == tails.len() {
            tails.push(i);
        } else {
            tails[lo] = i;
        }
    }
    let mut res = Vec::new();
    let mut k = *tails.last().unwrap();
    loop {
        res.push(k);
        if prev[k] == usize::MAX {
            break;
        }
        k = prev[k];
    }
    res.reverse();
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply_ref<K: Eq + Hash + Clone, V: Clone>(
        old: &[(K, V)],
        ops: &[DiffOp<K, V>],
    ) -> Vec<(K, V)> {
        // Reference applier over a plain Vec, mirroring `apply_to_map`.
        let mut list: Vec<(K, V)> = old.to_vec();
        for op in ops {
            match op {
                DiffOp::Remove { key } => list.retain(|(k, _)| k != key),
                DiffOp::Insert { key, value, index } => {
                    list.retain(|(k, _)| k != key);
                    let i = (*index).min(list.len());
                    list.insert(i, (key.clone(), value.clone()));
                }
                DiffOp::Move { key, to } => {
                    if let Some(from) = list.iter().position(|(k, _)| k == key) {
                        let item = list.remove(from);
                        let i = (*to).min(list.len());
                        list.insert(i, item);
                    }
                }
                DiffOp::Update { key, value } => {
                    if let Some(slot) = list.iter_mut().find(|(k, _)| k == key) {
                        slot.1 = value.clone();
                    }
                }
            }
        }
        list
    }

    #[test]
    fn lis_basic() {
        assert_eq!(longest_increasing_subsequence(&[]), Vec::<usize>::new());
        // [2,0,1] -> LIS values 0,1 at indices 1,2.
        assert_eq!(longest_increasing_subsequence(&[2, 0, 1]), vec![1, 2]);
        // already increasing -> whole thing.
        assert_eq!(
            longest_increasing_subsequence(&[0, 1, 2, 3]),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn pure_reorder_emits_only_minimal_moves() {
        let old = [("a", 1), ("b", 2), ("c", 3), ("d", 4)];
        let new = [("a", 1), ("c", 3), ("b", 2), ("d", 4)];
        let ops = reconcile(&old, &new);
        assert!(
            ops.iter().all(|op| matches!(op, DiffOp::Move { .. })),
            "pure reorder must be moves only: {ops:?}"
        );
        // a,c,d stay (LIS length 3); only b moves. 4 common - 3 LIS = 1 move.
        assert_eq!(ops.len(), 1);
        assert_eq!(apply_ref(&old, &ops), new.to_vec());
    }

    #[test]
    fn insert_remove_update_combined() {
        let old = [("a", 1), ("b", 2), ("c", 3)];
        let new = [("c", 3), ("a", 9), ("d", 4)];
        let ops = reconcile(&old, &new);
        // b removed, d inserted, a updated (1->9), c/a possibly moved.
        assert!(
            ops.iter()
                .any(|o| matches!(o, DiffOp::Remove { key } if *key == "b"))
        );
        assert!(
            ops.iter().any(
                |o| matches!(o, DiffOp::Insert { key, value, .. } if *key == "d" && *value == 4)
            )
        );
        assert!(
            ops.iter()
                .any(|o| matches!(o, DiffOp::Update { key, value } if *key == "a" && *value == 9))
        );
        assert_eq!(apply_ref(&old, &ops), new.to_vec());
    }

    #[test]
    fn full_reversal_is_minimal() {
        let old = [(1, 0), (2, 0), (3, 0), (4, 0)];
        let new = [(4, 0), (3, 0), (2, 0), (1, 0)];
        let ops = reconcile(&old, &new);
        let moves = ops
            .iter()
            .filter(|o| matches!(o, DiffOp::Move { .. }))
            .count();
        // LIS of a reversal is 1, so 4 - 1 = 3 moves (not 4).
        assert_eq!(moves, 3);
        assert_eq!(apply_ref(&old, &ops), new.to_vec());
    }

    #[test]
    fn apply_to_map_converges_and_spares_stable_value_cell() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            map.entry(&ctx, k, v);
        }
        // A reader of the STABLE key "a"'s value.
        let a_view = ctx.computed({
            let map = map.clone();
            move |ctx| map.get(ctx, &"a").unwrap_or(0) * 100
        });
        assert_eq!(ctx.get(&a_view), 100);

        // Reconcile to a reorder that leaves "a" first (stable) and only moves c,b.
        map.reconcile(&ctx, &[("a", 1), ("c", 3), ("b", 2)]);
        assert_eq!(map.keys(&ctx), vec!["a", "c", "b"]);
        // "a" was stable and its value unchanged → its value reader stays cached.
        assert!(
            ctx.is_set(&a_view),
            "stable entry's value cell must not be invalidated by a sibling reorder"
        );
        assert_eq!(ctx.get(&a_view), 100);
    }

    #[test]
    fn apply_to_map_handles_inserts_removes_updates() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            map.entry(&ctx, k, v);
        }
        let a = map.handle(&"a").unwrap();
        map.reconcile(&ctx, &[("c", 3), ("a", 9), ("d", 4)]);
        assert_eq!(map.keys(&ctx), vec!["c", "a", "d"]);
        assert_eq!(map.get(&ctx, &"a"), Some(9));
        assert_eq!(map.get(&ctx, &"d"), Some(4));
        assert!(!map.contains_key(&ctx, &"b"));
        // "a" kept its SAME value cell across the reconcile (updated in place).
        assert_eq!(map.handle(&"a").unwrap().id, a.id);
    }

    #[test]
    fn apply_to_tree_round_trips_reconcile_ops() {
        let ctx = Context::new();
        let root = SourceTree::leaf(&ctx, "root", 0);
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            root.insert_child(&ctx, k, v);
        }
        let a = root.child(&"a").unwrap();
        a.insert_child(&ctx, "a1", 100);
        let old = [("a", 1), ("b", 2), ("c", 3)];
        let new = [("c", 3), ("a", 9), ("d", 4)];
        let ops = reconcile(&old, &new);

        apply_to_tree(&ctx, &root, &ops);

        assert_eq!(root.child_ids(&ctx), vec!["c", "a", "d"]);
        assert_eq!(root.child(&"a").unwrap().get(&ctx), 9);
        assert_eq!(root.child(&"d").unwrap().get(&ctx), 4);
        assert!(root.child(&"b").is_none());
        assert_eq!(root.child(&"a").unwrap().child_ids(&ctx), vec!["a1"]);
    }

    #[test]
    fn apply_to_tree_pure_move_spares_child_value_reader() {
        let ctx = Context::new();
        let root = SourceTree::leaf(&ctx, "root", 0);
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            root.insert_child(&ctx, k, v);
        }
        let a = root.child(&"a").unwrap();
        let a_view = ctx.computed({
            let a = a.clone();
            move |ctx| a.get(ctx) * 10
        });
        assert_eq!(ctx.get(&a_view), 10);

        let ops = reconcile(
            &[("a", 1), ("b", 2), ("c", 3)],
            &[("a", 1), ("c", 3), ("b", 2)],
        );
        apply_to_tree(&ctx, &root, &ops);

        assert_eq!(root.child_ids(&ctx), vec!["a", "c", "b"]);
        assert!(
            ctx.is_set(&a_view),
            "stable child value must not be invalidated by sibling moves"
        );
        assert_eq!(ctx.get(&a_view), 10);
    }
}
