//! Lossless full-document tree CRDT — M1 syntax-agnostic core (#lzlosstree).
//!
//! Where [`TextCrdt`](crate::TextCrdt) is a *flat* lossless floor and
//! [`SeqCrdt`](crate::SeqCrdt) orders opaque keyed siblings, this is a **single
//! rooted concrete-syntax tree** whose *leaves own every rendered byte*. The
//! guiding invariant is losslessness:
//!
//! ```text
//! render(tree) == source_text
//! ```
//!
//! for valid, invalid, and unknown source alike — so the tree itself can be the
//! wire authority instead of a semantic AST layered over a separate text floor.
//! Internal [`Element`](NodeBody::Element) nodes own *structure only*; all text
//! lives in [`Leaf`](NodeBody::Leaf) nodes tagged [`Token`](LeafKind::Token) /
//! [`Trivia`](LeafKind::Trivia) / [`Raw`](LeafKind::Raw) / [`Error`](LeafKind::Error),
//! so unknown or invalid spans round-trip exactly as `Raw`/`Error` leaves rather
//! than being discarded.
//!
//! # M1 scope
//!
//! Create / tombstone / intra-parent reorder / leaf-edit / split-leaf /
//! merge-adjacent-leaves, plus op-based delta sync over a **dotted, non-contiguous
//! version frontier** ([`TreeVersionFrontier`]). Deferred to later milestones:
//! cross-parent `move_node` (single-parent + acyclicity enforcement), metadata /
//! kind mutation, subtree replace, snapshot/GC, and cross-language bindings. In
//! M1's create-only + intra-parent-reorder algebra single-parent and acyclicity
//! hold *by construction* — a freshly-minted child id cannot name a pre-existing
//! ancestor — so no runtime enforcement is needed yet.
//!
//! # Design notes (substrate reuse)
//!
//! - **Leaf text** embeds [`TextCrdt`](crate::TextCrdt) wholesale; a `LeafEdit`
//!   ships its [`TextOp`](crate::TextOp) delta, so a one-leaf edit is `O(leaf)`
//!   not `O(document)`.
//! - **Child order** is a minimal move-aware fractional-index layer reimplemented
//!   here rather than reusing [`SeqCrdt`](crate::SeqCrdt): `SeqCrdt`'s `Position`
//!   is private and cannot be *injected* from a shipped op, and M1's dotted-frontier
//!   anti-entropy is **op-based** (positions travel inside `CreateNode`/`Reorder`
//!   ops so both replicas store byte-identical keys and converge) rather than the
//!   whole-state merge `SeqCrdt` exposes. The fractional-key generator mirrors
//!   `SeqCrdt`'s proven `key_between`.
//! - **Clock** is a Lamport op-id ([`TreeOpId`]) exactly like `TextCrdt`'s `OpId`:
//!   the counter advances past every observed op, so a causally-later reorder wins
//!   LWW and concurrent ops tiebreak by peer. No HLC is needed for op-based tree
//!   deltas.
//! - **Frontier** is a dot *set* (contiguous prefix + sparse holes), never a
//!   per-peer max, so a missing non-contiguous op is representable and
//!   re-requestable — the property a version-vector shortcut cannot provide.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use crate::text_crdt::{TextCrdt, TextOp};

/// A dotted, totally-ordered operation id: a Lamport counter tiebroken by peer.
///
/// Ordered `(counter, peer)`. The counter advances past everything observed on
/// [`LosslessTreeCrdt::apply_update`], so a causally-later op sorts higher (LWW)
/// and concurrent ops at the same counter tiebreak deterministically by peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TreeOpId {
    /// Lamport counter (dominant ordering key).
    pub counter: u64,
    /// Originating peer (tiebreak + the per-peer key the dotted frontier groups by).
    pub peer: u64,
}

/// Stable identity of one tree node: the id of the op that created it.
///
/// A node keeps its id through reorder, edit, and (future) move, so ops can name
/// nodes without ambiguity. The document root is the sentinel `{counter: 0,
/// peer: 0}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TreeNodeId(pub TreeOpId);

impl TreeNodeId {
    /// The sentinel id of the document root.
    pub const ROOT: TreeNodeId = TreeNodeId(TreeOpId {
        counter: 0,
        peer: 0,
    });
}

/// The kind of a leaf's exact source text. Every rendered byte belongs to a leaf;
/// unknown/invalid spans are [`Raw`](LeafKind::Raw)/[`Error`](LeafKind::Error) so
/// nothing is ever discarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LeafKind {
    /// A syntax delimiter or marker token.
    Token,
    /// Whitespace, blank lines, indentation, comments, separators.
    Trivia,
    /// Valid text the adapter deliberately keeps opaque.
    Raw,
    /// Invalid or ambiguous text that must still round-trip exactly.
    Error,
}

/// The payload of a tree node: structure (`Element`) or exact text (`Leaf`).
#[derive(Debug, Clone)]
enum NodeBody {
    /// An internal semantic node. Owns ordered children, never text.
    Element { kind: String },
    /// A leaf owning one exact source span as an embedded text CRDT.
    Leaf { kind: LeafKind, text: TextCrdt },
}

/// A fractional-index sort key: orderable bytes tiebroken by the minting peer, so
/// concurrent inserts into the same gap get a deterministic total order. Compared
/// lexicographically by `frac`, then `peer`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct SortKey {
    frac: Vec<u8>,
    peer: u64,
}

/// One node's full record: its parent, its position within that parent (with the
/// LWW stamp of the last write that set it), its payload, and its tombstone.
#[derive(Debug, Clone)]
struct NodeRecord {
    /// `None` only for the root.
    parent: Option<TreeNodeId>,
    /// Position within `parent` (unused for the root).
    sort: SortKey,
    /// LWW stamp of the create/reorder that last set `sort`; higher wins.
    sort_stamp: TreeOpId,
    body: NodeBody,
    /// `Some(op)` once tombstoned; sticky (smaller op id wins on concurrent).
    tomb: Option<TreeOpId>,
    /// The id of the last text-affecting op on this leaf (its create, or the most
    /// recent edit/split/merge). Split/merge reseed the leaf destructively, so
    /// per-leaf text ops form a causal chain: each carries the prior `text_head`
    /// and is buffered until that op arrives, keeping out-of-order delivery
    /// convergent. Initialized to the node's own create id.
    text_head: TreeOpId,
}

/// What a `CreateNode` op materializes: an element shell or a text leaf seeded
/// from an exact string (both replicas rebuild the leaf deterministically).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NodeSeed {
    /// An internal element with the given kind.
    Element {
        /// Semantic kind label (immutable in M1).
        kind: String,
    },
    /// A leaf seeded with exact text.
    Leaf {
        /// Leaf classification.
        kind: LeafKind,
        /// Exact initial source text.
        text: String,
    },
}

/// A single tree operation. Every mutation is one of these, carrying everything a
/// remote replica needs to converge deterministically (positions and seed text
/// travel inside the op, never re-derived from local clocks).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum TreeOpKind {
    CreateNode {
        id: TreeNodeId,
        parent: TreeNodeId,
        sort: SortKey,
        seed: NodeSeed,
    },
    Tombstone {
        node: TreeNodeId,
    },
    Reorder {
        node: TreeNodeId,
        sort: SortKey,
    },
    LeafEdit {
        node: TreeNodeId,
        prev: TreeOpId,
        ops: Vec<TextOp>,
    },
    SplitLeaf {
        node: TreeNodeId,
        new: TreeNodeId,
        sort: SortKey,
        at_char: usize,
        prev: TreeOpId,
    },
    MergeLeaves {
        left: TreeNodeId,
        right: TreeNodeId,
        prev_left: TreeOpId,
        prev_right: TreeOpId,
    },
}

/// A transport-ready tree operation: its dotted id plus the change it encodes.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TreeOp {
    /// The op's dotted id (also the created node's id for `CreateNode`).
    pub id: TreeOpId,
    kind: TreeOpKind,
}

/// A batch of ops to ship — the output of [`LosslessTreeCrdt::diff`] and the input
/// to [`LosslessTreeCrdt::apply_update`].
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TreeUpdate {
    /// Ops ordered by dotted id (dependencies buffered on apply if still missing).
    pub ops: Vec<TreeOp>,
}

/// The observed dots for one peer: a contiguous prefix plus out-of-order holes.
///
/// This is the anti-skip core. A per-peer *max* would record only the highest dot
/// and silently imply every lower dot is present; delivering dot 3 while dot 2 is
/// missing would then make the partner believe it holds 2. Tracking the actual dot
/// set keeps a hole (2) representable so it is re-requested rather than skipped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct DotRange {
    /// Every dot `1..=contiguous` is present.
    contiguous: u64,
    /// Dots present above a gap in the prefix.
    sparse: BTreeSet<u64>,
}

impl DotRange {
    fn contains(&self, counter: u64) -> bool {
        counter <= self.contiguous || self.sparse.contains(&counter)
    }

    fn observe(&mut self, counter: u64) {
        if counter <= self.contiguous {
            return;
        }
        self.sparse.insert(counter);
        // Absorb any now-contiguous run starting at contiguous+1.
        while self.sparse.remove(&(self.contiguous + 1)) {
            self.contiguous += 1;
        }
    }
}

/// A dotted version frontier: per peer, exactly which op dots are held.
///
/// Unlike a version vector (a per-peer max counter), this represents
/// non-contiguous delivery, so [`LosslessTreeCrdt::diff`] never omits a missing
/// interior op.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TreeVersionFrontier {
    dots: BTreeMap<u64, DotRange>,
}

impl TreeVersionFrontier {
    /// Whether the op with dotted `id` is held.
    pub fn contains(&self, id: TreeOpId) -> bool {
        self.dots
            .get(&id.peer)
            .is_some_and(|r| r.contains(id.counter))
    }

    fn observe(&mut self, id: TreeOpId) {
        self.dots.entry(id.peer).or_default().observe(id.counter);
    }
}

/// Errors from tree mutations. Text preservation wins, so these reject a mutation
/// rather than ever risk dropping bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeError {
    /// Named node is absent.
    NotFound,
    /// Operation requires a leaf node but the target is an element.
    NotLeaf,
    /// A byte offset is out of range or not on a UTF-8 char boundary.
    NonCharBoundary,
    /// `merge_adjacent_leaves` targets that are not adjacent live siblings.
    NotAdjacent,
}

impl fmt::Display for TreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            TreeError::NotFound => "node not found",
            TreeError::NotLeaf => "node is not a leaf",
            TreeError::NonCharBoundary => "offset out of range or not on a char boundary",
            TreeError::NotAdjacent => "leaves are not adjacent live siblings",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for TreeError {}

/// The result type for tree mutations.
pub type Result<T> = std::result::Result<T, TreeError>;

/// A lossless concrete-syntax tree CRDT (M1 core).
#[derive(Debug, Clone)]
pub struct LosslessTreeCrdt {
    peer: u64,
    counter: u64,
    nodes: HashMap<TreeNodeId, NodeRecord>,
    frontier: TreeVersionFrontier,
    /// Ops this replica holds (own + adopted), for `diff` / gossip relay.
    log: Vec<TreeOp>,
    /// Ops whose causal dependency has not arrived yet.
    buffered: Vec<TreeOp>,
}

impl LosslessTreeCrdt {
    /// A fresh document owned by `peer`: just the root element.
    pub fn new(peer: u64) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            TreeNodeId::ROOT,
            NodeRecord {
                parent: None,
                sort: SortKey {
                    frac: Vec::new(),
                    peer: 0,
                },
                sort_stamp: TreeOpId {
                    counter: 0,
                    peer: 0,
                },
                body: NodeBody::Element {
                    kind: "root".to_string(),
                },
                tomb: None,
                text_head: TreeOpId {
                    counter: 0,
                    peer: 0,
                },
            },
        );
        Self {
            peer,
            counter: 0,
            nodes,
            frontier: TreeVersionFrontier::default(),
            log: Vec::new(),
            buffered: Vec::new(),
        }
    }

    /// Fork this replica's full state under a new owning `peer` (deep copy of the
    /// tree, frontier, log, and Lamport counter; new peer identity). Concurrent ops
    /// on the two forks tiebreak deterministically by peer.
    pub fn fork(&self, peer: u64) -> Self {
        Self {
            peer,
            counter: self.counter,
            nodes: self.nodes.clone(),
            frontier: self.frontier.clone(),
            log: self.log.clone(),
            buffered: self.buffered.clone(),
        }
    }

    fn next_op_id(&mut self) -> TreeOpId {
        self.counter += 1;
        TreeOpId {
            counter: self.counter,
            peer: self.peer,
        }
    }

    /// The live children of `parent`, in rendered order.
    fn live_children(&self, parent: TreeNodeId) -> Vec<TreeNodeId> {
        let mut kids: Vec<(&TreeNodeId, &SortKey)> = self
            .nodes
            .iter()
            .filter(|(_, r)| r.parent == Some(parent) && r.tomb.is_none())
            .map(|(id, r)| (id, &r.sort))
            .collect();
        kids.sort_by(|a, b| a.1.cmp(b.1));
        kids.into_iter().map(|(id, _)| *id).collect()
    }

    /// Render the whole document by concatenating live-leaf text in tree order.
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.render_into(TreeNodeId::ROOT, &mut out);
        out
    }

    fn render_into(&self, id: TreeNodeId, out: &mut String) {
        let Some(rec) = self.nodes.get(&id) else {
            return;
        };
        match &rec.body {
            NodeBody::Leaf { text, .. } => out.push_str(&text.text()),
            NodeBody::Element { .. } => {
                for child in self.live_children(id) {
                    self.render_into(child, out);
                }
            }
        }
    }

    /// The number of live nodes excluding the root — the structural-growth gauge
    /// fixtures assert against (a split grows it by one, a merge restores it).
    pub fn live_node_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|(id, r)| **id != TreeNodeId::ROOT && r.tomb.is_none())
            .count()
    }

    /// This replica's dotted version frontier (what to advertise to a partner).
    pub fn frontier(&self) -> TreeVersionFrontier {
        self.frontier.clone()
    }

    /// The kind of an element node, or `None` if `node` is absent or a leaf.
    pub fn element_kind(&self, node: TreeNodeId) -> Option<&str> {
        match self.nodes.get(&node).map(|r| &r.body) {
            Some(NodeBody::Element { kind }) => Some(kind),
            _ => None,
        }
    }

    /// The kind of a leaf node, or `None` if `node` is absent or an element.
    pub fn leaf_kind(&self, node: TreeNodeId) -> Option<LeafKind> {
        match self.nodes.get(&node).map(|r| &r.body) {
            Some(NodeBody::Leaf { kind, .. }) => Some(*kind),
            _ => None,
        }
    }

    /// The live children of `parent` in rendered order (empty if `parent` is a
    /// leaf or absent).
    pub fn children(&self, parent: TreeNodeId) -> Vec<TreeNodeId> {
        self.live_children(parent)
    }

    /// A leaf's current text, or an error if `node` is absent or an element.
    pub fn leaf_text(&self, node: TreeNodeId) -> Result<String> {
        match self.nodes.get(&node).map(|r| &r.body) {
            Some(NodeBody::Leaf { text, .. }) => Ok(text.text()),
            Some(NodeBody::Element { .. }) => Err(TreeError::NotLeaf),
            None => Err(TreeError::NotFound),
        }
    }

    /// The fractional key placing a new/moved child of `parent` immediately after
    /// `after` (or at the front when `after` is `None`). Mirrors `SeqCrdt`'s
    /// `key_between`, with the local peer as the tiebreak.
    fn key_after(&self, parent: TreeNodeId, after: Option<TreeNodeId>) -> SortKey {
        let order = self.live_children(parent);
        let (lo, hi) = match after {
            None => (None, order.first().copied()),
            Some(a) => {
                let idx = order.iter().position(|x| *x == a);
                match idx {
                    Some(i) => (Some(a), order.get(i + 1).copied()),
                    // Anchor not a live child: append at the end.
                    None => (order.last().copied(), None),
                }
            }
        };
        let lo_frac = lo.map(|id| self.nodes[&id].sort.frac.clone());
        let hi_frac = hi.map(|id| self.nodes[&id].sort.frac.clone());
        SortKey {
            frac: key_between(lo_frac.as_deref(), hi_frac.as_deref()),
            peer: self.peer,
        }
    }

    /// Create a node under `parent`, positioned after `after` (or at the front when
    /// `after` is `None`). Returns the new node's id.
    pub fn create_node(
        &mut self,
        parent: TreeNodeId,
        after: Option<TreeNodeId>,
        seed: NodeSeed,
    ) -> Result<TreeNodeId> {
        if !self.nodes.contains_key(&parent) {
            return Err(TreeError::NotFound);
        }
        let sort = self.key_after(parent, after);
        let op_id = self.next_op_id();
        let node = TreeNodeId(op_id);
        let op = TreeOp {
            id: op_id,
            kind: TreeOpKind::CreateNode {
                id: node,
                parent,
                sort,
                seed,
            },
        };
        self.commit_local(op);
        Ok(node)
    }

    /// Tombstone `node` (and, structurally, its subtree — descendants render away
    /// once their ancestor is gone). No-op-safe if already tombstoned.
    pub fn tombstone_node(&mut self, node: TreeNodeId) -> Result<()> {
        if !self.nodes.contains_key(&node) || node == TreeNodeId::ROOT {
            return Err(TreeError::NotFound);
        }
        let op_id = self.next_op_id();
        self.commit_local(TreeOp {
            id: op_id,
            kind: TreeOpKind::Tombstone { node },
        });
        Ok(())
    }

    /// Reorder `node` within its current parent to just after `after` (front when
    /// `None`). A single LWW position reassignment — identity and payload preserved.
    pub fn reorder_child(&mut self, node: TreeNodeId, after: Option<TreeNodeId>) -> Result<()> {
        let parent = self
            .nodes
            .get(&node)
            .and_then(|r| r.parent)
            .ok_or(TreeError::NotFound)?;
        let sort = self.key_after(parent, after);
        let op_id = self.next_op_id();
        self.commit_local(TreeOp {
            id: op_id,
            kind: TreeOpKind::Reorder { node, sort },
        });
        Ok(())
    }

    /// Edit a leaf's text: delete `delete_bytes` and insert `insert` at UTF-8 byte
    /// offset `at_byte` (leaf-local). Offsets must land on char boundaries.
    pub fn edit_leaf(
        &mut self,
        node: TreeNodeId,
        at_byte: usize,
        delete_bytes: usize,
        insert: &str,
    ) -> Result<()> {
        let s = self.leaf_text(node)?;
        let start = byte_to_char(&s, at_byte).ok_or(TreeError::NonCharBoundary)?;
        let end = byte_to_char(&s, at_byte + delete_bytes).ok_or(TreeError::NonCharBoundary)?;
        let delete_chars = end - start;

        // Re-own the leaf's text under this replica so concurrent edits from
        // different peers mint distinct char ids (no collision on merge).
        let editor = self.peer;
        let ops = {
            let text = self.leaf_text_mut(node)?;
            *text = text.fork(editor);
            let vv = text.version_vector();
            for _ in 0..delete_chars {
                text.delete(start);
            }
            text.insert_str(start, insert);
            text.delta_since(&vv)
        };
        let prev = self.nodes[&node].text_head;
        let op_id = self.next_op_id();
        self.commit_local(TreeOp {
            id: op_id,
            kind: TreeOpKind::LeafEdit { node, prev, ops },
        });
        Ok(())
    }

    /// Split a leaf at UTF-8 byte offset `at_byte` into two adjacent leaves of the
    /// same kind (head keeps `node`, tail becomes a fresh node returned here). Total
    /// rendered text is unchanged.
    pub fn split_leaf(&mut self, node: TreeNodeId, at_byte: usize) -> Result<TreeNodeId> {
        let s = self.leaf_text(node)?;
        let at_char = byte_to_char(&s, at_byte).ok_or(TreeError::NonCharBoundary)?;
        let sort = {
            let parent = self.nodes[&node].parent.ok_or(TreeError::NotFound)?;
            self.key_after(parent, Some(node))
        };
        let prev = self.nodes[&node].text_head;
        let op_id = self.next_op_id();
        let new = TreeNodeId(op_id);
        self.commit_local(TreeOp {
            id: op_id,
            kind: TreeOpKind::SplitLeaf {
                node,
                new,
                sort,
                at_char,
                prev,
            },
        });
        Ok(new)
    }

    /// Merge `right` into `left` when they are adjacent live leaf siblings: `left`
    /// takes both spans' text, `right` is tombstoned. Total rendered text is
    /// unchanged.
    pub fn merge_adjacent_leaves(&mut self, left: TreeNodeId, right: TreeNodeId) -> Result<()> {
        // Validate leaf-ness and adjacency before minting an op.
        self.leaf_text(left)?;
        self.leaf_text(right)?;
        let parent = self.nodes[&left].parent.ok_or(TreeError::NotFound)?;
        let order = self.live_children(parent);
        let adjacent = order
            .iter()
            .position(|x| *x == left)
            .and_then(|i| order.get(i + 1))
            .is_some_and(|nxt| *nxt == right);
        if !adjacent {
            return Err(TreeError::NotAdjacent);
        }
        let prev_left = self.nodes[&left].text_head;
        let prev_right = self.nodes[&right].text_head;
        let op_id = self.next_op_id();
        self.commit_local(TreeOp {
            id: op_id,
            kind: TreeOpKind::MergeLeaves {
                left,
                right,
                prev_left,
                prev_right,
            },
        });
        Ok(())
    }

    /// Ops this replica holds that `their` frontier lacks, ordered by dotted id so
    /// dependencies tend to precede dependents (out-of-order delivery is still
    /// handled by apply-time buffering).
    pub fn diff(&self, their: &TreeVersionFrontier) -> TreeUpdate {
        let mut ops: Vec<TreeOp> = self
            .log
            .iter()
            .filter(|op| !their.contains(op.id))
            .cloned()
            .collect();
        ops.sort_by_key(|op| (op.id.counter, op.id.peer));
        TreeUpdate { ops }
    }

    /// Apply a batch of remote ops. Idempotent (already-held ops are skipped) and
    /// order-tolerant (an op whose target/parent has not arrived is buffered and
    /// retried). Advances the Lamport counter past every observed op.
    pub fn apply_update(&mut self, update: &TreeUpdate) {
        for op in &update.ops {
            self.counter = self.counter.max(op.id.counter);
            if self.frontier.contains(op.id) {
                continue;
            }
            self.buffered.push(op.clone());
        }
        self.drain_buffered();
    }

    fn drain_buffered(&mut self) {
        loop {
            let mut progressed = false;
            let pending = std::mem::take(&mut self.buffered);
            for op in pending {
                if self.frontier.contains(op.id) {
                    continue;
                }
                if self.dependencies_ready(&op) {
                    self.apply_op(&op);
                    self.record(op);
                    progressed = true;
                } else {
                    self.buffered.push(op);
                }
            }
            if !progressed {
                break;
            }
        }
    }

    fn dependencies_ready(&self, op: &TreeOp) -> bool {
        match &op.kind {
            TreeOpKind::CreateNode { parent, .. } => self.nodes.contains_key(parent),
            TreeOpKind::Tombstone { node } | TreeOpKind::Reorder { node, .. } => {
                self.nodes.contains_key(node)
            }
            // Per-leaf text ops chain off the prior text op (split/merge reseed
            // destructively), so require both the node and its predecessor.
            TreeOpKind::LeafEdit { node, prev, .. } | TreeOpKind::SplitLeaf { node, prev, .. } => {
                self.nodes.contains_key(node) && self.frontier.contains(*prev)
            }
            TreeOpKind::MergeLeaves {
                left,
                right,
                prev_left,
                prev_right,
            } => {
                self.nodes.contains_key(left)
                    && self.nodes.contains_key(right)
                    && self.frontier.contains(*prev_left)
                    && self.frontier.contains(*prev_right)
            }
        }
    }

    /// Apply a locally-generated op (already reflected? no — apply then record).
    fn commit_local(&mut self, op: TreeOp) {
        self.apply_op(&op);
        self.record(op);
    }

    fn record(&mut self, op: TreeOp) {
        self.frontier.observe(op.id);
        self.log.push(op);
    }

    fn apply_op(&mut self, op: &TreeOp) {
        match &op.kind {
            TreeOpKind::CreateNode {
                id,
                parent,
                sort,
                seed,
            } => {
                if self.nodes.contains_key(id) {
                    return;
                }
                let body = match seed {
                    NodeSeed::Element { kind } => NodeBody::Element { kind: kind.clone() },
                    NodeSeed::Leaf { kind, text } => NodeBody::Leaf {
                        kind: *kind,
                        text: TextCrdt::from_str(id.0.peer, text),
                    },
                };
                self.nodes.insert(
                    *id,
                    NodeRecord {
                        parent: Some(*parent),
                        sort: sort.clone(),
                        sort_stamp: op.id,
                        body,
                        tomb: None,
                        text_head: op.id,
                    },
                );
            }
            TreeOpKind::Tombstone { node } => {
                if let Some(rec) = self.nodes.get_mut(node) {
                    rec.tomb = Some(match rec.tomb {
                        Some(existing) => existing.min(op.id),
                        None => op.id,
                    });
                }
            }
            TreeOpKind::Reorder { node, sort } => {
                if let Some(rec) = self.nodes.get_mut(node)
                    && op.id > rec.sort_stamp
                {
                    rec.sort = sort.clone();
                    rec.sort_stamp = op.id;
                }
            }
            TreeOpKind::LeafEdit { node, ops, .. } => {
                if let Some(rec) = self.nodes.get_mut(node)
                    && let NodeBody::Leaf { text, .. } = &mut rec.body
                {
                    text.apply_delta(ops);
                    rec.text_head = op.id;
                }
            }
            TreeOpKind::SplitLeaf {
                node,
                new,
                sort,
                at_char,
                ..
            } => self.apply_split(*node, *new, sort.clone(), *at_char, op.id),
            TreeOpKind::MergeLeaves { left, right, .. } => self.apply_merge(*left, *right, op.id),
        }
    }

    fn apply_split(
        &mut self,
        node: TreeNodeId,
        new: TreeNodeId,
        sort: SortKey,
        at_char: usize,
        op_id: TreeOpId,
    ) {
        let Some(rec) = self.nodes.get(&node) else {
            return;
        };
        let NodeBody::Leaf { kind, text } = &rec.body else {
            return;
        };
        let kind = *kind;
        let parent = rec.parent;
        let chars: Vec<char> = text.text().chars().collect();
        let clamp = at_char.min(chars.len());
        let head: String = chars[..clamp].iter().collect();
        let tail: String = chars[clamp..].iter().collect();
        // Reseed head deterministically under the original node's create peer, so
        // both replicas rebuild byte-identical leaf state.
        if let Some(rec) = self.nodes.get_mut(&node) {
            rec.body = NodeBody::Leaf {
                kind,
                text: TextCrdt::from_str(node.0.peer, &head),
            };
            rec.text_head = op_id;
        }
        self.nodes.entry(new).or_insert(NodeRecord {
            parent,
            sort,
            sort_stamp: op_id,
            body: NodeBody::Leaf {
                kind,
                text: TextCrdt::from_str(new.0.peer, &tail),
            },
            tomb: None,
            text_head: op_id,
        });
    }

    fn apply_merge(&mut self, left: TreeNodeId, right: TreeNodeId, op_id: TreeOpId) {
        let (Some(l), Some(r)) = (self.nodes.get(&left), self.nodes.get(&right)) else {
            return;
        };
        let (NodeBody::Leaf { kind, text: lt }, NodeBody::Leaf { text: rt, .. }) =
            (&l.body, &r.body)
        else {
            return;
        };
        let kind = *kind;
        let combined = format!("{}{}", lt.text(), rt.text());
        if let Some(rec) = self.nodes.get_mut(&left) {
            rec.body = NodeBody::Leaf {
                kind,
                text: TextCrdt::from_str(left.0.peer, &combined),
            };
            rec.text_head = op_id;
        }
        if let Some(rec) = self.nodes.get_mut(&right) {
            rec.tomb = Some(match rec.tomb {
                Some(existing) => existing.min(op_id),
                None => op_id,
            });
        }
    }

    fn leaf_text_mut(&mut self, node: TreeNodeId) -> Result<&mut TextCrdt> {
        match self.nodes.get_mut(&node).map(|r| &mut r.body) {
            Some(NodeBody::Leaf { text, .. }) => Ok(text),
            Some(NodeBody::Element { .. }) => Err(TreeError::NotLeaf),
            None => Err(TreeError::NotFound),
        }
    }
}

/// Byte offset → char index, or `None` if out of range / not on a char boundary.
fn byte_to_char(s: &str, byte: usize) -> Option<usize> {
    if byte > s.len() || !s.is_char_boundary(byte) {
        return None;
    }
    Some(s[..byte].chars().count())
}

/// Generate a fractional key strictly between `lo` and `hi` (each `None` = open
/// end), compared lexicographically. Mirrors `SeqCrdt::key_between`.
fn key_between(lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<u8> {
    let mut result = Vec::new();
    let mut i = 0usize;
    let cap = lo.map_or(0, |l| l.len()) + hi.map_or(0, |h| h.len()) + 2;
    while i <= cap {
        let a: u16 = lo.and_then(|l| l.get(i)).map_or(0, |&d| d as u16);
        let b: u16 = match hi {
            Some(h) => h.get(i).map_or(0, |&d| d as u16),
            None => 256,
        };
        if a + 1 < b {
            result.push(((a + b) / 2) as u8);
            return result;
        }
        result.push(a as u8);
        i += 1;
        if a < b {
            let lo_tail: Vec<u8> = lo
                .map(|l| l.get(i..).unwrap_or(&[]).to_vec())
                .unwrap_or_default();
            result.extend(key_between(Some(&lo_tail), None));
            return result;
        }
    }
    result.push(128);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn elem(kind: &str) -> NodeSeed {
        NodeSeed::Element {
            kind: kind.to_string(),
        }
    }
    fn leaf(kind: LeafKind, text: &str) -> NodeSeed {
        NodeSeed::Leaf {
            kind,
            text: text.to_string(),
        }
    }

    /// Build `root > para > [Token "# ", Raw "héllo", Trivia "\n"]` on one replica.
    fn sample() -> (LosslessTreeCrdt, TreeNodeId, [TreeNodeId; 3]) {
        let mut t = LosslessTreeCrdt::new(1);
        let para = t.create_node(TreeNodeId::ROOT, None, elem("para")).unwrap();
        let a = t
            .create_node(para, None, leaf(LeafKind::Token, "# "))
            .unwrap();
        let b = t
            .create_node(para, Some(a), leaf(LeafKind::Raw, "héllo"))
            .unwrap();
        let c = t
            .create_node(para, Some(b), leaf(LeafKind::Trivia, "\n"))
            .unwrap();
        (t, para, [a, b, c])
    }

    #[test]
    fn render_is_exact_concatenation_including_multibyte() {
        let (t, _para, _) = sample();
        assert_eq!(t.render(), "# héllo\n");
        assert_eq!(t.live_node_count(), 4); // para + 3 leaves
    }

    #[test]
    fn edit_leaf_at_byte_offset_into_multibyte_text() {
        let (mut t, _para, [_, b, _]) = sample();
        // "héllo": bytes h=0, é=1..3, l=3. Insert "X" after é (byte 3).
        t.edit_leaf(b, 3, 0, "X").unwrap();
        assert_eq!(t.render(), "# héXllo\n");
    }

    #[test]
    fn edit_leaf_rejects_non_char_boundary() {
        let (mut t, _para, [_, b, _]) = sample();
        // Byte 2 is inside 'é' (a 2-byte char) — must be rejected.
        assert_eq!(t.edit_leaf(b, 2, 0, "X"), Err(TreeError::NonCharBoundary));
    }

    #[test]
    fn split_then_merge_preserves_render() {
        let (mut t, _para, [_, b, _]) = sample();
        let before = t.render();
        let n = t.live_node_count();
        let new = t.split_leaf(b, 3).unwrap(); // split "héllo" after "hé"
        assert_eq!(t.render(), before, "split preserves render");
        assert_eq!(t.live_node_count(), n + 1);
        t.merge_adjacent_leaves(b, new).unwrap();
        assert_eq!(t.render(), before, "merge restores render");
        assert_eq!(t.live_node_count(), n);
    }

    #[test]
    fn reorder_child_changes_order_only() {
        let (mut t, para, [a, b, c]) = sample();
        t.reorder_child(c, None).unwrap(); // move trivia to front
        assert_eq!(t.render(), "\n# héllo");
        assert_eq!(t.live_children(para), vec![c, a, b]);
    }

    #[test]
    fn diff_apply_converges_two_replicas() {
        let (mut a, para, [_, _, _]) = sample();
        let mut b = a.fork(2);
        a.edit_leaf(a.live_children(para)[1], 0, 0, "!").unwrap();
        b.create_node(para, None, leaf(LeafKind::Trivia, ">> "))
            .unwrap();
        let a_to_b = a.diff(&b.frontier());
        let b_to_a = b.diff(&a.frontier());
        a.apply_update(&b_to_a);
        b.apply_update(&a_to_b);
        assert_eq!(a.render(), b.render(), "converged");
    }

    #[test]
    fn non_contiguous_delivery_leaves_a_recoverable_hole() {
        let (mut a, para, _) = sample();
        let mut b = a.fork(2);
        // Three independent creates under para on `a` (no interdependency).
        a.create_node(para, None, leaf(LeafKind::Trivia, "1"))
            .unwrap();
        a.create_node(para, None, leaf(LeafKind::Trivia, "2"))
            .unwrap();
        a.create_node(para, None, leaf(LeafKind::Trivia, "3"))
            .unwrap();
        let update = a.diff(&b.frontier());
        assert_eq!(update.ops.len(), 3);
        // Deliver only the first and third (hole in the middle).
        let holed = TreeUpdate {
            ops: vec![update.ops[0].clone(), update.ops[2].clone()],
        };
        b.apply_update(&holed);
        assert_ne!(a.render(), b.render(), "b is missing the held-back op");
        // Anti-entropy re-requests exactly the hole and converges.
        let repair = a.diff(&b.frontier());
        assert_eq!(repair.ops.len(), 1, "only the hole is resent");
        b.apply_update(&repair);
        assert_eq!(a.render(), b.render(), "converged after repair");
    }

    /// `diff` returns ops in canonical `(counter, peer)` order.
    ///
    /// This ordering is a CROSS-BINDING CONTRACT, not an implementation detail:
    /// the shared corpus addresses ops POSITIONALLY —
    /// `lossless-tree/non_contiguous_anti_entropy.json` carries
    /// `deliver.only: [0, 2]`, which indexes into whatever `diff` returns — so
    /// that fixture only means the same thing in every binding while every
    /// binding returns the same order.
    ///
    /// The corpus cannot catch a regression here. Measured in lazily-zig
    /// (`#lzzigdiffmutant`): reversing the sort, or deleting it outright, left the
    /// whole suite green including the anti-entropy fixture, because the two
    /// indices select the same SET either way and `apply_update` is order-tolerant
    /// by design. A direct test is the only thing that pins it.
    #[test]
    fn diff_returns_ops_in_canonical_counter_peer_order() {
        let mut a = LosslessTreeCrdt::new(1);
        let para = a.create_node(TreeNodeId::ROOT, None, elem("para")).unwrap();
        let base = a
            .create_node(para, None, leaf(LeafKind::Trivia, "0"))
            .unwrap();

        let mut b = a.fork(2);

        // `a` runs ahead to counter 4; `b`'s single op stays at counter 3, so it
        // arrives afterwards and lands LAST in `a`'s log while sorting EARLIER
        // than `a`'s own (4, 1).
        let one = a
            .create_node(para, Some(base), leaf(LeafKind::Trivia, "1"))
            .unwrap();
        a.create_node(para, Some(one), leaf(LeafKind::Trivia, "2"))
            .unwrap();
        b.create_node(para, Some(base), leaf(LeafKind::Trivia, "9"))
            .unwrap();

        let from_b = b.diff(&a.frontier());
        a.apply_update(&from_b);

        let all = a.diff(&TreeVersionFrontier::default());

        // Non-vacuity: the LOG order and the CANONICAL order must genuinely
        // differ, otherwise the ordering assertion below holds for a reversed or
        // unsorted `diff` too and pins nothing. Assert the difference EXPLICITLY
        // so a future refactor that makes the two coincide fails loudly here
        // instead of silently hollowing out the check.
        assert_eq!(all.ops.len(), a.log.len(), "diff returns the whole log");
        let log_ids: Vec<TreeOpId> = a.log.iter().map(|op| op.id).collect();
        let diff_ids: Vec<TreeOpId> = all.ops.iter().map(|op| op.id).collect();
        assert_ne!(
            log_ids, diff_ids,
            "log order must differ from canonical order, or this test pins nothing: {log_ids:?}"
        );

        // The returned sequence is strictly increasing by (counter, peer).
        for pair in diff_ids.windows(2) {
            let (prev, curr) = (pair[0], pair[1]);
            assert!(
                (prev.counter, prev.peer) < (curr.counter, curr.peer),
                "diff ops are not in canonical (counter, peer) order: {diff_ids:?}"
            );
        }
    }
}
