//! Free-text character sequence CRDT + re-parse (#lztextcrdt).
//!
//! The anchored-skeleton layers ([`reconcile`](crate::reconcile),
//! [`stable_id`](crate::stable_id)) buy identity for *controlled* structure. For
//! arbitrary prose with no anchors and **concurrent** edits, the merge unit drops
//! to characters: [`TextCrdt`] merges keystrokes, then you **re-parse** the merged
//! text and re-derive the structural tree. The tree is a *projection* of
//! CRDT-merged text, not the merge unit itself. Honest floor: a true rewrite *is*
//! a replace — there is no character identity to preserve through it.
//!
//! # Algorithm
//!
//! A Fugue/RGA-style tree CRDT. Each inserted character is an element with a
//! unique [`OpId`] and a **left origin** (the element it was typed after). The
//! sequence is the in-order traversal of the origin tree, with same-origin
//! siblings ordered by `OpId` descending (newest-after-origin first — the RGA
//! tiebreak). Deletes are tombstones. `order` is therefore a pure, deterministic
//! function of the element set, so [`merge`](TextCrdt::merge) (a union of
//! elements, tombstones sticky) is commutative, associative, and idempotent.
//!
//! ```
//! use lazily::TextCrdt;
//!
//! // Two replicas fork from "hi" and edit concurrently.
//! let mut a = TextCrdt::from_str(1, "hi");
//! let mut b = a.fork(2);
//! a.insert(2, '!');          // "hi!"
//! b.insert(0, 'O');          // "Ohi"
//! a.merge(&b);
//! b.merge(&a);
//! assert_eq!(a.text(), b.text()); // converged, both edits preserved
//! ```

use std::collections::{BTreeMap, HashMap};

use crate::stable_id::Block;

/// A globally-unique, totally-ordered id for one inserted character.
///
/// Ordered by `(counter, peer)`; the counter is Lamport-style (advances past
/// everything observed on merge), so a causally-later insert sorts higher and a
/// concurrent insert tiebreaks deterministically by peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OpId {
    counter: u64,
    peer: u64,
}

impl OpId {
    /// The Lamport counter component (advances past everything observed on
    /// merge). The dominant ordering key.
    pub fn counter(&self) -> u64 {
        self.counter
    }

    /// The originating peer — the final tiebreak that keeps concurrent inserts
    /// at the same counter totally ordered, and the per-peer key the distributed
    /// plane's OpId frontier groups deletions by.
    pub fn peer(&self) -> u64 {
        self.peer
    }
}

/// One text-CRDT element in a serializable, transport-ready form (#lztextsync).
///
/// The wire unit for [`TextCrdt::delta_since`] / [`TextCrdt::apply_delta`]: a full
/// snapshot is `delta_since(&TextVersionVector::new())`, and a replica is rebuilt by
/// `apply_delta`-ing that op list onto a fresh [`TextCrdt`], which preserves each
/// character's [`OpId`] identity so later deltas still merge conflict-free (unlike
/// re-parsing the text, which would mint fresh ids and duplicate on merge).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TextOp {
    /// The character's globally-unique id.
    pub id: OpId,
    /// The inserted character.
    pub ch: char,
    /// The element this was inserted after (`None` = document start).
    pub origin: Option<OpId>,
    /// `Some(delete_op)` once tombstoned, else `None`.
    pub deleted: Option<OpId>,
}

/// A version vector: the greatest [`OpId`] counter observed per originating peer —
/// the compact frontier a replica sends so a partner can compute exactly the ops it
/// lacks (#lztextsync). Serde-friendly (integer keys), unlike the raw element map
/// keyed by [`OpId`].
pub type TextVersionVector = BTreeMap<u64, u64>;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Elem {
    ch: char,
    /// The element this character was inserted *after* (None = document start).
    origin: Option<OpId>,
    /// `None` while live; `Some(delete_op)` once tombstoned. Carrying the
    /// *delete's* own [`OpId`] (not a bare flag) is what lets GC test whether the
    /// *deletion* — not merely the insertion — is causally stable (#lztombgc).
    /// Tombstones are sticky; concurrent deletes converge to the smaller `OpId`.
    deleted: Option<OpId>,
}

/// A character-granular, mergeable text buffer for concurrent free-text edits.
#[derive(Debug, Clone)]
pub struct TextCrdt {
    elems: HashMap<OpId, Elem>,
    peer: u64,
    counter: u64,
}

impl TextCrdt {
    /// An empty buffer owned by `peer`.
    pub fn new(peer: u64) -> Self {
        Self {
            elems: HashMap::new(),
            peer,
            counter: 0,
        }
    }

    /// A buffer owned by `peer` seeded with `s` (a linear chain of characters).
    pub fn from_str(peer: u64, s: &str) -> Self {
        let mut t = Self::new(peer);
        t.append_root_chain(s);
        t
    }

    /// Fork this buffer's state to a new replica `peer` (deep copy, new identity).
    /// Used to model two replicas diverging from a shared base.
    pub fn fork(&self, peer: u64) -> Self {
        Self {
            elems: self.elems.clone(),
            peer,
            counter: self.counter,
        }
    }

    fn next_id(&mut self) -> OpId {
        self.counter += 1;
        OpId {
            counter: self.counter,
            peer: self.peer,
        }
    }

    /// This replica's current Lamport position, as an [`OpId`] attributed to the
    /// local peer.
    ///
    /// The OpId analog of an HLC stamp: the counter advances on every local edit
    /// and jumps past everything observed on [`merge`](Self::merge), so it is a
    /// causally-monotone watermark of how far this replica has progressed. The
    /// distributed plane (`#lzcrdtplane`) folds each replica's `clock` into its
    /// OpId frontier; the per-peer minimum is the all-replicas-aware watermark
    /// below which a tombstone is collectable everywhere — exactly as the
    /// [`HlcStamp`](crate::HlcStamp) frontier drives [`SeqCrdt`](crate::SeqCrdt)
    /// GC. Deletes key by `OpId`, not `HlcStamp`, which is why this parallel
    /// clock exists.
    pub fn clock(&self) -> OpId {
        OpId {
            counter: self.counter,
            peer: self.peer,
        }
    }

    /// Insert `ch` at visible index `index` (0 = start, `len` = end).
    pub fn insert(&mut self, index: usize, ch: char) {
        let visible = self.ordered_ids(false);
        let origin = if index == 0 {
            None
        } else {
            visible.get(index - 1).copied()
        };
        let id = self.next_id();
        self.elems.insert(
            id,
            Elem {
                ch,
                origin,
                deleted: None,
            },
        );
    }

    /// Insert all of `s` starting at visible index `index`.
    pub fn insert_str(&mut self, index: usize, s: &str) {
        let visible = self.ordered_ids(false);
        if index > visible.len() {
            // Preserve `insert`'s historical out-of-range behavior. Valid edit
            // positions take the linear fast path below.
            for (i, ch) in s.chars().enumerate() {
                self.insert(index + i, ch);
            }
            return;
        }

        let mut origin = index.checked_sub(1).and_then(|i| visible.get(i).copied());
        for ch in s.chars() {
            let id = self.next_id();
            self.elems.insert(
                id,
                Elem {
                    ch,
                    origin,
                    deleted: None,
                },
            );
            origin = Some(id);
        }
    }

    fn append_root_chain(&mut self, s: &str) {
        let mut origin = None;
        for ch in s.chars() {
            let id = self.next_id();
            self.elems.insert(
                id,
                Elem {
                    ch,
                    origin,
                    deleted: None,
                },
            );
            origin = Some(id);
        }
    }

    /// Replace the whole visible buffer in one linear pass.
    ///
    /// The ordinary edit path is intentionally character granular, but rebuilding a
    /// document by calling `delete` / `insert` for every character recomputes the
    /// full origin order on each step. Whole-document seed and patchback paths use
    /// this method so large markdown buffers stay linear instead of quadratic.
    pub fn replace_all(&mut self, s: &str) {
        let visible_ids: Vec<OpId> = self
            .elems
            .iter()
            .filter_map(|(id, elem)| elem.deleted.is_none().then_some(*id))
            .collect();
        for id in visible_ids {
            let deleted = self.next_id();
            if let Some(elem) = self.elems.get_mut(&id)
                && elem.deleted.is_none()
            {
                elem.deleted = Some(deleted);
            }
        }
        self.append_root_chain(s);
    }

    /// Tombstone the visible character at `index`. No-op if out of range.
    pub fn delete(&mut self, index: usize) {
        self.delete_range(index, 1);
    }

    /// Tombstone up to `len` visible characters starting at `index`.
    ///
    /// The visible order is projected once for the whole edit, keeping large
    /// range deletions linear instead of rebuilding the origin tree per
    /// character.
    pub fn delete_range(&mut self, index: usize, len: usize) {
        let visible = self.ordered_ids(false);
        let end = index.saturating_add(len).min(visible.len());
        let ids = visible.get(index..end).unwrap_or_default();
        for id in ids {
            // Mint a distinct OpId for the deletion so GC can later test whether
            // the *delete* is causally stable. No-op if already tombstoned.
            let del = self.next_id();
            if let Some(e) = self.elems.get_mut(id)
                && e.deleted.is_none()
            {
                e.deleted = Some(del);
            }
        }
    }

    /// The current visible text in sequence order.
    pub fn text(&self) -> String {
        self.ordered_ids(false)
            .into_iter()
            .filter_map(|id| self.elems.get(&id).map(|e| e.ch))
            .collect()
    }

    /// Number of visible characters.
    pub fn len(&self) -> usize {
        self.elems.values().filter(|e| e.deleted.is_none()).count()
    }

    /// Number of tombstoned-but-not-yet-collected characters — the GC-pressure
    /// gauge behind the "memory bloat" critique.
    pub fn tombstone_count(&self) -> usize {
        self.elems.values().filter(|e| e.deleted.is_some()).count()
    }

    /// Whether there is any visible text.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ordered element ids via the origin tree (in-order traversal; same-origin
    /// siblings newest-first). `include_deleted` keeps tombstones in the order
    /// (needed so later origins still resolve), else they are filtered.
    fn ordered_ids(&self, include_deleted: bool) -> Vec<OpId> {
        // children[origin] = ids inserted directly after `origin`.
        let mut children: HashMap<Option<OpId>, Vec<OpId>> = HashMap::new();
        for (id, e) in &self.elems {
            children.entry(e.origin).or_default().push(*id);
        }
        for list in children.values_mut() {
            // Descending OpId: the most recent insert-after-origin comes first.
            list.sort_unstable_by(|a, b| b.cmp(a));
        }
        let mut out = Vec::with_capacity(self.elems.len());
        // Iterative pre-order DFS; stack holds ids to visit (reversed so the
        // first child is processed first).
        let mut stack: Vec<OpId> = children
            .get(&None)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .rev()
            .collect();
        while let Some(id) = stack.pop() {
            let e = &self.elems[&id];
            if include_deleted || e.deleted.is_none() {
                out.push(id);
            }
            if let Some(kids) = children.get(&Some(id)) {
                // Push reversed so the first (highest-OpId) child pops first.
                for &k in kids.iter().rev() {
                    stack.push(k);
                }
            }
        }
        out
    }

    /// Merge another replica's edits (commutative, associative, idempotent):
    /// union of elements by id, with tombstones sticky (a delete on either side
    /// wins). Advances the local counter past everything observed. Returns
    /// whether the visible text changed.
    pub fn merge(&mut self, other: &TextCrdt) -> bool {
        let before = self.text();
        for (id, oe) in &other.elems {
            self.counter = self.counter.max(id.counter);
            // Delete OpIds advance the clock too, so a local insert after a
            // merge can never collide with an observed deletion's id.
            if let Some(d) = oe.deleted {
                self.counter = self.counter.max(d.counter);
            }
            match self.elems.get_mut(id) {
                Some(e) => {
                    // Tombstone is sticky and order-independent: keep whichever
                    // delete id is smaller so concurrent deletes converge
                    // (commutative/associative) instead of depending on merge order.
                    e.deleted = match (e.deleted, oe.deleted) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                }
                None => {
                    self.elems.insert(*id, oe.clone());
                }
            }
        }
        self.text() != before
    }

    /// Garbage-collect causally-stable deletion tombstones (#lztombgc).
    ///
    /// `is_stable(delete_op_id)` is the caller-supplied "every replica has
    /// observed this deletion" policy — the distributed plane (`#lzcrdtplane`)
    /// derives it from its anti-entropy version vectors. Mechanism only, and
    /// deliberately conservative: a tombstoned element is collected only when it
    /// is **not referenced as any element's left origin**, so removing it can
    /// never orphan a surviving character. Interior tombstones are reclaimed
    /// bottom-up as their descendants are themselves collected (contiguous-run
    /// compaction with origin-rewrite is the heavier follow-up). Returns the
    /// number of elements collected.
    pub fn gc_with(&mut self, is_stable: impl Fn(OpId) -> bool) -> usize {
        let mut removed = 0;
        loop {
            let referenced: std::collections::HashSet<OpId> =
                self.elems.values().filter_map(|e| e.origin).collect();
            let collectable: Vec<OpId> = self
                .elems
                .iter()
                .filter(|(id, e)| e.deleted.is_some_and(&is_stable) && !referenced.contains(id))
                .map(|(id, _)| *id)
                .collect();
            if collectable.is_empty() {
                break;
            }
            for id in collectable {
                self.elems.remove(&id);
                removed += 1;
            }
        }
        removed
    }
}

impl TextCrdt {
    /// This replica's [`TextVersionVector`]: for each peer that authored an insert or a
    /// deletion this replica holds, the greatest counter seen from that peer. An op
    /// `(c, p)` is unknown to a partner iff `c > their_vv[p]` (0 when absent).
    pub fn version_vector(&self) -> TextVersionVector {
        let mut vv = TextVersionVector::new();
        let mut bump = |id: OpId| {
            let slot = vv.entry(id.peer()).or_insert(0);
            *slot = (*slot).max(id.counter());
        };
        for (id, elem) in &self.elems {
            bump(*id);
            if let Some(d) = elem.deleted {
                bump(d);
            }
        }
        vv
    }

    /// The ops this replica holds that `their_vv` has not observed — new inserts and
    /// newly-observed deletions of older elements. [`apply_delta`](Self::apply_delta)-ing
    /// this list into the partner converges the two replicas. A whole-state snapshot
    /// is `delta_since(&TextVersionVector::new())`.
    pub fn delta_since(&self, their_vv: &TextVersionVector) -> Vec<TextOp> {
        let seen = |id: OpId| id.counter() <= their_vv.get(&id.peer()).copied().unwrap_or(0);
        self.elems
            .iter()
            .filter_map(|(id, elem)| {
                let insert_new = !seen(*id);
                let delete_new = elem.deleted.is_some_and(|d| !seen(d));
                (insert_new || delete_new).then_some(TextOp {
                    id: *id,
                    ch: elem.ch,
                    origin: elem.origin,
                    deleted: elem.deleted,
                })
            })
            .collect()
    }

    /// Apply a delta op list (from [`delta_since`](Self::delta_since)) into this
    /// replica. Commutative, associative, and idempotent — the same convergence
    /// contract as [`merge`](Self::merge), just from the transport form: a fresh
    /// insert adds its element (preserving its [`OpId`]); an incoming tombstone is
    /// merged sticky-minimally so concurrent deletes converge. Returns whether the
    /// visible text changed.
    pub fn apply_delta(&mut self, ops: &[TextOp]) -> bool {
        let before = self.text();
        self.apply_delta_unprojected(ops);
        self.text() != before
    }

    /// Apply a delta without materializing the visible text before and after.
    ///
    /// This is the transport/recovery fast path for callers that already own a
    /// projection cache or only need to know whether stored CRDT state changed.
    /// It preserves the same commutative, associative, and idempotent merge as
    /// [`Self::apply_delta`], but returns whether the stored operation graph or
    /// local counter advanced rather than whether the visible string changed.
    /// Avoiding `text()` matters for retained histories: projection orders the
    /// complete operation graph even when an incoming snapshot is idempotent.
    pub fn apply_delta_unprojected(&mut self, ops: &[TextOp]) -> bool {
        let before_counter = self.counter;
        let mut changed = false;
        for op in ops {
            self.counter = self.counter.max(op.id.counter());
            if let Some(d) = op.deleted {
                self.counter = self.counter.max(d.counter());
            }
            match self.elems.get_mut(&op.id) {
                Some(e) => {
                    let deleted = match (e.deleted, op.deleted) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    changed |= deleted != e.deleted;
                    e.deleted = deleted;
                }
                None => {
                    self.elems.insert(
                        op.id,
                        Elem {
                            ch: op.ch,
                            origin: op.origin,
                            deleted: op.deleted,
                        },
                    );
                    changed = true;
                }
            }
        }
        changed || self.counter != before_counter
    }
}

/// Re-parse merged text into paragraph [`Block`]s (split on blank lines). This is
/// the "re-derive the tree from CRDT-merged text" step: feed the result through
/// [`assign_stable_keys`](crate::stable_id::assign_stable_keys) +
/// [`reconcile`](crate::reconcile) to project the merged text onto the keyed tree.
pub fn parse_blocks(text: &str) -> Vec<Block> {
    text.split("\n\n")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(Block::text)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_sync_converges_two_replicas() {
        // Two replicas fork a shared base, edit concurrently (insert + delete),
        // then exchange deltas keyed off each other's version vector.
        let base = TextCrdt::from_str(0, "hello\n");
        let mut a = base.fork(1);
        a.insert_str(a.len(), "world\n"); // agent appends
        let mut b = base.fork(2);
        b.delete(0); // human deletes 'h'

        let a_delta = a.delta_since(&b.version_vector());
        let b_delta = b.delta_since(&a.version_vector());
        assert!(a.apply_delta(&b_delta));
        b.apply_delta(&a_delta);

        assert_eq!(a.text(), b.text(), "replicas converge after delta exchange");
        assert_eq!(a.text(), "ello\nworld\n");
    }

    #[test]
    fn full_snapshot_delta_reconstructs_a_mergeable_replica() {
        // delta_since(empty) is a whole-state snapshot; apply_delta onto a fresh
        // replica preserves element identity, so a later concurrent edit still
        // merges conflict-free (no duplication).
        let mut canonical = TextCrdt::from_str(1, "base\n");
        let snapshot = canonical.delta_since(&TextVersionVector::new());
        let mut member = TextCrdt::new(2);
        member.apply_delta(&snapshot);
        assert_eq!(member.text(), "base\n");

        canonical.insert_str(canonical.len(), "A\n");
        member.insert_str(member.len(), "B\n");
        let to_member = canonical.delta_since(&member.version_vector());
        let to_canonical = member.delta_since(&canonical.version_vector());
        canonical.apply_delta(&to_canonical);
        member.apply_delta(&to_member);
        assert_eq!(
            canonical.text(),
            member.text(),
            "shared-identity convergence"
        );
    }

    #[test]
    fn from_str_seeds_a_large_buffer_as_one_linear_chain() {
        let text = "0123456789abcdef\n".repeat(512);
        let t = TextCrdt::from_str(7, &text);

        assert_eq!(t.text(), text);
        assert_eq!(t.elems.len(), text.chars().count());
        assert_eq!(
            t.version_vector().get(&7).copied(),
            Some(t.elems.len() as u64)
        );
    }

    #[test]
    fn whole_document_replace_deltas_converge_without_duplication() {
        let base = TextCrdt::from_str(1, "old heading\nold body\n");
        let mut canonical = base.clone();
        let mut member = TextCrdt::new(2);
        member.apply_delta(&base.delta_since(&TextVersionVector::new()));

        canonical.replace_all("new heading\nnew body\n");
        let delta = canonical.delta_since(&member.version_vector());
        assert!(member.apply_delta(&delta));

        assert_eq!(canonical.text(), "new heading\nnew body\n");
        assert_eq!(member.text(), canonical.text());
    }

    #[test]
    fn delta_apply_is_idempotent() {
        let a = TextCrdt::from_str(1, "abc\n");
        let mut b = TextCrdt::new(2);
        let delta = a.delta_since(&TextVersionVector::new());
        assert!(b.apply_delta(&delta));
        assert!(!b.apply_delta(&delta), "re-applying a delta is a no-op");
        assert_eq!(b.text(), a.text());
    }

    #[test]
    fn unprojected_delta_apply_reports_structural_change_without_rendering() {
        let a = TextCrdt::from_str(1, "retained state\n");
        let delta = a.delta_since(&TextVersionVector::new());
        let mut b = TextCrdt::new(2);

        assert!(b.apply_delta_unprojected(&delta));
        assert!(
            !b.apply_delta_unprojected(&delta),
            "an exact replay must be a structural no-op"
        );
        assert_eq!(b.text(), a.text());
    }

    #[test]
    fn local_insert_and_delete() {
        let mut t = TextCrdt::from_str(1, "helo");
        t.insert(3, 'l'); // "hello"
        assert_eq!(t.text(), "hello");
        t.insert(5, '!'); // append
        assert_eq!(t.text(), "hello!");
        t.delete(0); // drop 'h'
        assert_eq!(t.text(), "ello!");
        assert_eq!(t.len(), 5);
    }

    #[test]
    fn batch_insert_and_delete_match_character_edits() {
        let base = TextCrdt::from_str(1, "alpha omega");

        let mut batched = base.clone();
        batched.insert_str(6, "beta ");
        batched.delete_range(0, 6);

        let mut character_edits = base;
        for (i, ch) in "beta ".chars().enumerate() {
            character_edits.insert(6 + i, ch);
        }
        for _ in 0..6 {
            character_edits.delete(0);
        }

        assert_eq!(batched.text(), "beta omega");
        assert_eq!(batched.text(), character_edits.text());
        assert_eq!(
            batched.delta_since(&TextVersionVector::new()),
            character_edits.delta_since(&TextVersionVector::new())
        );
    }

    #[test]
    fn concurrent_inserts_converge_keeping_both() {
        let mut a = TextCrdt::from_str(1, "hi");
        let mut b = a.fork(2);
        a.insert(2, '!'); // "hi!"
        b.insert(0, 'O'); // "Ohi"
        let changed = a.merge(&b);
        b.merge(&a);
        assert!(changed);
        assert_eq!(a.text(), b.text(), "replicas converge");
        // Both edits survive.
        assert!(a.text().contains('!') && a.text().contains('O'));
        assert_eq!(a.text().len(), 4);
    }

    #[test]
    fn concurrent_inserts_at_same_spot_converge_deterministically() {
        let mut a = TextCrdt::from_str(1, "XY");
        let mut b = a.fork(2);
        a.insert(1, 'a'); // between X and Y on replica 1
        b.insert(1, 'b'); // between X and Y on replica 2 (concurrent)
        a.merge(&b);
        b.merge(&a);
        assert_eq!(a.text(), b.text());
        // Deterministic order, both present, anchored between X and Y.
        assert_eq!(a.text().len(), 4);
        assert!(a.text().starts_with('X') && a.text().ends_with('Y'));
    }

    #[test]
    fn concurrent_insert_and_delete_merge() {
        let mut a = TextCrdt::from_str(1, "abc");
        let mut b = a.fork(2);
        a.delete(1); // delete 'b' -> "ac"
        b.insert(3, 'd'); // append 'd' -> "abcd"
        a.merge(&b);
        b.merge(&a);
        assert_eq!(a.text(), b.text());
        assert_eq!(a.text(), "acd"); // delete + insert both applied
    }

    #[test]
    fn merge_is_idempotent_and_commutative() {
        let mut a = TextCrdt::from_str(1, "one");
        let mut b = a.fork(2);
        a.insert(3, 'X');
        b.insert(0, 'Y');
        let mut ab = a.clone();
        ab.merge(&b);
        ab.merge(&b); // idempotent
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab.text(), ba.text(), "commutative");
        let once = {
            let mut x = a.clone();
            x.merge(&b);
            x.text()
        };
        assert_eq!(ab.text(), once, "idempotent");
    }

    #[test]
    fn reparse_projects_merged_text_onto_keyed_blocks() {
        use crate::stable_id::assign_stable_keys;

        // Old doc: two paragraphs.
        let old_text = "first paragraph\n\nsecond paragraph";
        let old_blocks = parse_blocks(old_text);
        assert_eq!(old_blocks.len(), 2);

        // Concurrent edits: replica A appends a third paragraph; replica B tweaks
        // the first. Merge the *text*, then re-parse + re-key.
        let mut a = TextCrdt::from_str(1, old_text);
        let mut b = a.fork(2);
        a.insert_str(a.len(), "\n\nthird paragraph");
        b.insert_str(5, " EDITED"); // into "first"
        a.merge(&b);

        let new_blocks = parse_blocks(&a.text());
        assert_eq!(new_blocks.len(), 3, "merged text re-parses to 3 paragraphs");

        // The keyed projection: the unchanged second paragraph keeps its key
        // (identity through the merge); edited/new blocks are edits/inserts.
        let keys = assign_stable_keys(&old_blocks, &new_blocks);
        let old_keys: Vec<String> = old_blocks
            .iter()
            .map(|bl| crate::stable_id::block_key(bl).as_string())
            .collect();
        assert!(
            keys.contains(&old_keys[1]),
            "unchanged paragraph keeps identity across the text-CRDT merge"
        );
    }

    #[test]
    fn gc_collects_a_stable_deleted_leaf() {
        let mut t = TextCrdt::from_str(1, "abc");
        t.delete(2); // tombstone the trailing 'c' (a leaf: nothing follows it)
        assert_eq!(t.text(), "ab");
        assert_eq!(t.tombstone_count(), 1);
        // Nothing stable -> nothing collected.
        assert_eq!(t.gc_with(|_| false), 0);
        assert_eq!(t.tombstone_count(), 1);
        // Stable -> the leaf tombstone is reclaimed; visible text is unchanged.
        assert_eq!(t.gc_with(|_| true), 1);
        assert_eq!(t.tombstone_count(), 0);
        assert_eq!(t.text(), "ab");
    }

    #[test]
    fn gc_keeps_a_referenced_tombstone_then_collects_bottom_up() {
        // Delete the MIDDLE char: 'b' is the left-origin of 'c', so collecting it
        // would orphan 'c'. GC must keep it until 'c' is gone too.
        let mut t = TextCrdt::from_str(1, "abc");
        t.delete(1); // tombstone 'b'; 'c' still references it as origin
        assert_eq!(t.text(), "ac");
        assert_eq!(
            t.gc_with(|_| true),
            0,
            "referenced tombstone is not collected"
        );
        assert_eq!(t.tombstone_count(), 1);
        assert_eq!(
            t.text(),
            "ac",
            "live text intact while tombstone is retained"
        );

        // Now delete 'c' too. One GC pass collects 'c' (leaf), which un-references
        // 'b', so the same pass then collects 'b' bottom-up.
        t.delete(1); // visible index of 'c' is now 1
        assert_eq!(t.text(), "a");
        assert_eq!(
            t.gc_with(|_| true),
            2,
            "both tombstones collected bottom-up"
        );
        assert_eq!(t.tombstone_count(), 0);
        assert_eq!(t.text(), "a");
    }

    #[test]
    fn concurrent_deletes_of_same_char_converge() {
        // Both replicas delete the same character; the sticky tombstone must
        // converge regardless of merge order (commutative).
        let mut a = TextCrdt::from_str(1, "abc");
        let mut b = a.fork(2);
        a.delete(1); // 'b' on replica 1
        b.delete(1); // 'b' on replica 2 (concurrent, distinct delete OpIds)
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab.text(), "ac");
        assert_eq!(ba.text(), "ac");
        assert_eq!(ab.tombstone_count(), ba.tombstone_count());
        // The converged delete id is the same on both (min of the two) -> GC
        // stability is order-independent.
        ab.merge(&ba);
        ba.merge(&ab);
        assert_eq!(ab.text(), ba.text());
    }
}
