//! Ordered keyed reactive tree: [`CellTree`] (#lzordtree).
//!
//! A `CellTree<Id, V>` models a **shallow-or-deep, ordered, stably-keyed tree**
//! — the shape an agent-doc document takes (root → components → items, each with
//! a stable id). It is the substrate for keyed reconciliation (`#lzkeyrecon`)
//! and, ultimately, per-cell CRDT merge.
//!
//! Each node is `(kind/id, value_cell, ordered children)`:
//!
//! - **`id: Id`** — a *stable* identity that survives reorder and value edits.
//! - **`value: Source<V>`** — the node's own value cell. Editing node `X`'s
//!   value invalidates only readers of `X` (fine-grained), never a sibling.
//! - **ordered children** — a [`SourceMap`] of child id → child node, so child
//!   *membership* and *order* are reactive **per level**: a reader of one node's
//!   `child_ids` / `len` is invalidated only when *that* node gains, loses, or
//!   reorders a child — sibling subtrees and deeper descendants don't disturb it.
//!
//! Child order is mutated atomically via [`CellTree::move_child`], built on
//! [`SourceMap::move_to`] (`#lzcellmove`): a reorder keeps each child node's cell
//! identity, dependents, and lineage and bumps order once.
//!
//! `CellTree` is cheap to [`Clone`] (an `Rc` to shared node state), giving
//! **structural sharing**: the same subtree node can be held in several places
//! and handed to compute/effect closures without copying the graph.
//!
//! ```
//! use lazily::{CellTree, Context};
//!
//! let ctx = Context::new();
//! // A document root whose value is a section label.
//! let root: CellTree<&'static str, &'static str> = CellTree::leaf(&ctx, "root", "doc");
//! let a = root.insert_child(&ctx, "a", "alpha");
//! let _b = root.insert_child(&ctx, "b", "bravo");
//!
//! // Ordered, reactive children.
//! assert_eq!(root.child_ids(&ctx), vec!["a", "b"]);
//!
//! // Per-node value edit: only `a`'s readers recompute.
//! a.set(&ctx, "ALPHA");
//! assert_eq!(a.get(&ctx), "ALPHA");
//!
//! // Atomic ordered move (#lzcellmove) — `a` keeps its identity.
//! root.move_child(&ctx, &"a", 1);
//! assert_eq!(root.child_ids(&ctx), vec!["b", "a"]);
//! ```

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

use crate::Context;
use crate::cell::Source;
use crate::cell_family::SourceMap;
use crate::context::ComputeOps;

/// A node in an ordered, stably-keyed reactive tree (`#lzordtree`).
///
/// Holds a stable `id`, a per-node `value` cell, and an ordered keyed collection
/// of child nodes. Cheap to [`Clone`] (`Rc` to shared state) — clones share the
/// same underlying node (structural sharing), so mutating through one clone is
/// visible through the others.
pub struct CellTree<Id, V> {
    inner: Rc<CellTreeNode<Id, V>>,
}

struct CellTreeNode<Id, V> {
    id: Id,
    value: Source<V>,
    /// Reactive ordered membership of this node's direct children (the keys are
    /// child ids; values are unit). Supplies per-level `child_ids`/`len`/order
    /// reactivity and atomic move (`#lzcellmove`).
    order: SourceMap<Id, ()>,
    /// Non-reactive storage of the actual child node handles, looked up by id.
    /// Kept in lockstep with `order`'s key set.
    nodes: RefCell<HashMap<Id, CellTree<Id, V>>>,
}

impl<Id, V> Clone for CellTree<Id, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<Id, V> CellTree<Id, V>
where
    Id: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
{
    /// Create a leaf node with stable `id` and initial `value`.
    pub fn leaf(ctx: &Context, id: Id, value: V) -> Self {
        Self {
            inner: Rc::new(CellTreeNode {
                id,
                value: ctx.source(value),
                order: SourceMap::new(ctx),
                nodes: RefCell::new(HashMap::new()),
            }),
        }
    }

    /// This node's stable id.
    pub fn id(&self) -> &Id {
        &self.inner.id
    }

    /// This node's value cell handle (for wiring derived computeds directly).
    pub fn value(&self) -> Source<V> {
        self.inner.value
    }

    /// Reactively read this node's value. A reader is invalidated only when
    /// *this* node's value changes, not when a sibling or child changes.
    pub fn get<C: ComputeOps>(&self, ctx: &C) -> V
    where
        V: Clone + 'static,
    {
        self.inner.value.get(ctx)
    }

    /// Set this node's value. Invalidates only this node's value dependents
    /// (PartialEq-guarded — a no-op write does not invalidate).
    pub fn set(&self, ctx: &Context, value: V) {
        ctx.set(&self.inner.value, value);
    }

    /// Insert (and return) a new leaf child appended at the end of this node's
    /// ordered children, bumping this level's membership + order once. If `id`
    /// already exists the existing child is returned unchanged (its value and
    /// subtree are preserved).
    pub fn insert_child(&self, ctx: &Context, id: Id, value: V) -> CellTree<Id, V> {
        if let Some(existing) = self.inner.nodes.borrow().get(&id) {
            return existing.clone();
        }
        let child = CellTree::leaf(ctx, id.clone(), value);
        self.inner
            .nodes
            .borrow_mut()
            .insert(id.clone(), child.clone());
        self.inner.order.entry(ctx, id, ());
        child
    }

    /// Attach an already-built subtree as a child (structural sharing / move a
    /// subtree under this node). Appends at the end; returns the attached node.
    /// If a child with the same id exists it is left in place and returned.
    pub fn attach_child(&self, ctx: &Context, child: CellTree<Id, V>) -> CellTree<Id, V> {
        let id = child.inner.id.clone();
        if let Some(existing) = self.inner.nodes.borrow().get(&id) {
            return existing.clone();
        }
        self.inner
            .nodes
            .borrow_mut()
            .insert(id.clone(), child.clone());
        self.inner.order.entry(ctx, id, ());
        child
    }

    /// Get a child by id (non-reactive lookup of the node handle).
    pub fn child(&self, id: &Id) -> Option<CellTree<Id, V>> {
        self.inner.nodes.borrow().get(id).cloned()
    }

    /// Remove a child by id. Bumps this level's membership + order; the removed
    /// subtree stops driving this node's child readers. Returns whether present.
    pub fn remove_child(&self, ctx: &Context, id: &Id) -> bool {
        let removed = self.inner.nodes.borrow_mut().remove(id).is_some();
        if removed {
            self.inner.order.remove(ctx, id);
        }
        removed
    }

    /// Atomically move child `id` to `index` among this node's children
    /// (`#lzcellmove`): the child keeps its node identity, value cell, subtree,
    /// and dependents; only this level's order signal is bumped (once), so
    /// `child_ids` readers recompute while `len` readers stay cached.
    pub fn move_child(&self, ctx: &Context, id: &Id, index: usize) -> bool {
        self.inner.order.move_to(ctx, id, index)
    }

    /// Atomically move child `id` to just before `anchor` (`#lzcellmove`).
    pub fn move_child_before(&self, ctx: &Context, id: &Id, anchor: &Id) -> bool {
        self.inner.order.move_before(ctx, id, anchor)
    }

    /// Atomically move child `id` to just after `anchor` (`#lzcellmove`).
    pub fn move_child_after(&self, ctx: &Context, id: &Id, anchor: &Id) -> bool {
        self.inner.order.move_after(ctx, id, anchor)
    }

    /// Reactive, ordered list of child ids. Subscribes the caller to this
    /// node's child **order** (add/remove/move), not to child or descendant
    /// value changes.
    pub fn child_ids<C: ComputeOps>(&self, ctx: &C) -> Vec<Id> {
        self.inner.order.keys(ctx)
    }

    /// Reactive, ordered list of direct child nodes.
    pub fn children(&self, ctx: &Context) -> Vec<CellTree<Id, V>> {
        let nodes = self.inner.nodes.borrow();
        self.inner
            .order
            .keys(ctx)
            .into_iter()
            .filter_map(|k| nodes.get(&k).cloned())
            .collect()
    }

    /// Reactive direct-child count. Subscribes to *set membership* only — a
    /// pure reorder does not invalidate a `len` reader.
    pub fn len<C: ComputeOps>(&self, ctx: &C) -> usize {
        self.inner.order.len(ctx)
    }

    /// Reactive emptiness check (set membership).
    pub fn is_empty<C: ComputeOps>(&self, ctx: &C) -> bool {
        self.inner.order.is_empty(ctx)
    }

    /// Reactive membership test for a direct child.
    pub fn contains_child(&self, ctx: &Context, id: &Id) -> bool {
        self.inner.order.contains_key(ctx, id)
    }

    /// Resolve a node by a path of ids from this node downward (non-reactive).
    /// Returns `None` if any segment is missing.
    pub fn resolve_path(&self, path: &[Id]) -> Option<CellTree<Id, V>> {
        let mut node = self.clone();
        for seg in path {
            node = node.child(seg)?;
        }
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(ctx: &Context) -> CellTree<&'static str, &'static str> {
        let root = CellTree::leaf(ctx, "root", "doc");
        root.insert_child(ctx, "a", "alpha");
        root.insert_child(ctx, "b", "bravo");
        root.insert_child(ctx, "c", "charlie");
        root
    }

    #[test]
    fn ordered_children_and_value_access() {
        let ctx = Context::new();
        let root = doc(&ctx);
        assert_eq!(root.id(), &"root");
        assert_eq!(root.get(&ctx), "doc");
        assert_eq!(root.child_ids(&ctx), vec!["a", "b", "c"]);
        assert_eq!(root.child(&"b").unwrap().get(&ctx), "bravo");
        assert_eq!(root.len(&ctx), 3);
    }

    #[test]
    fn per_node_value_isolation() {
        let ctx = Context::new();
        let root = doc(&ctx);
        let a = root.child(&"a").unwrap();
        let b = root.child(&"b").unwrap();

        let view_a = ctx.computed({
            let a = a.clone();
            move |ctx| a.get(ctx).to_uppercase()
        });
        assert_eq!(ctx.get(&view_a), "ALPHA");

        // Editing a sibling must not invalidate a's reader.
        b.set(&ctx, "BRAVO!");
        assert!(
            ctx.is_set(&view_a),
            "sibling value edit must not invalidate"
        );
        assert_eq!(ctx.get(&view_a), "ALPHA");

        // Editing the node itself does.
        a.set(&ctx, "alfa");
        assert_eq!(ctx.get(&view_a), "ALFA");
    }

    #[test]
    fn child_membership_reactive_per_level() {
        let ctx = Context::new();
        let root = doc(&ctx);
        let a = root.child(&"a").unwrap();

        // A reader of root's child set.
        let root_kids = ctx.computed({
            let root = root.clone();
            move |ctx| root.child_ids(ctx).join(",")
        });
        // A reader of a's child set (a different subtree level).
        let a_kids = ctx.computed({
            let a = a.clone();
            move |ctx| a.len(ctx)
        });
        assert_eq!(ctx.get(&root_kids), "a,b,c");
        assert_eq!(ctx.get(&a_kids), 0);

        // Adding a child *under a* must not invalidate root's child reader.
        a.insert_child(&ctx, "a1", "alpha-one");
        assert!(
            ctx.is_set(&root_kids),
            "deeper membership change must not invalidate parent level"
        );
        assert_eq!(ctx.get(&a_kids), 1);

        // Adding a child at root invalidates root's reader.
        root.insert_child(&ctx, "d", "delta");
        assert_eq!(ctx.get(&root_kids), "a,b,c,d");
    }

    #[test]
    fn atomic_move_keeps_identity_and_spares_len_readers() {
        let ctx = Context::new();
        let root = doc(&ctx);
        let a = root.child(&"a").unwrap();
        a.insert_child(&ctx, "a1", "alpha-one"); // give `a` a subtree

        let order = ctx.computed({
            let root = root.clone();
            move |ctx| root.child_ids(ctx).join(",")
        });
        let count = ctx.computed({
            let root = root.clone();
            move |ctx| root.len(ctx)
        });
        assert_eq!(ctx.get(&order), "a,b,c");
        assert_eq!(ctx.get(&count), 3);

        // Move "a" to the end.
        assert!(root.move_child(&ctx, &"a", 2));
        assert_eq!(ctx.get(&order), "b,c,a");
        // len reader stays cached on a pure reorder.
        assert!(
            ctx.is_set(&count),
            "pure move must not invalidate len reader"
        );

        // The moved node kept its identity AND its subtree.
        let a_again = root.child(&"a").unwrap();
        assert_eq!(a_again.child_ids(&ctx), vec!["a1"]);
        assert_eq!(a_again.get(&ctx), "alpha");
    }

    #[test]
    fn structural_sharing_via_clone() {
        let ctx = Context::new();
        let root = doc(&ctx);
        let a1 = root.child(&"a").unwrap();
        let a2 = root.child(&"a").unwrap();
        // Both handles point at the same underlying node.
        a1.set(&ctx, "shared");
        assert_eq!(a2.get(&ctx), "shared");

        // Re-attaching the same node is a no-op (PartialEq via Rc identity).
        let n = root.len(&ctx);
        root.attach_child(&ctx, a1.clone());
        assert_eq!(root.len(&ctx), n);
    }

    #[test]
    fn resolve_path_walks_segments() {
        let ctx = Context::new();
        let root = CellTree::leaf(&ctx, "root", "r");
        let a = root.insert_child(&ctx, "a", "a");
        let b = a.insert_child(&ctx, "b", "b");
        b.insert_child(&ctx, "c", "c");

        assert_eq!(root.resolve_path(&["a", "b", "c"]).unwrap().get(&ctx), "c");
        assert!(root.resolve_path(&["a", "x"]).is_none());
    }
}
