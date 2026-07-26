//! Memoized semantic tree over a [`SourceTree`] (#lzsemtree).
//!
//! The syntactic document tree ([`SourceTree`]) holds *input* cells; the **semantic**
//! tree (e.g. "unresolved prompts", "drainable heads", "section summaries") is a
//! layer of **memoized `computed` nodes** derived from it. [`SemTree`] builds one
//! memoized slot per node that folds `(node value, child derived values) -> D`.
//!
//! Because each node has its own guarded [`computed`](crate::Context::computed) slot and a parent
//! reads its *children's* derived slots (not their raw cells), the derivation is
//! **incremental and glitch-free**: editing one node recomputes only that node's
//! **ancestor chain** — a sibling subtree's derived value stays cached. And the
//! memo guard means a node edit that doesn't change the folded result stops the
//! recompute from propagating to the parent at all. This is the lazy-pull win on
//! a real agent-doc-shaped query (don't materialize semantics eagerly — derive
//! them, and pay only for what actually changed).
//!
//! Incrementality covers **value edits, removals, and reorders** of children. A
//! child **insertion** adds a node the captured fold doesn't know about yet, so
//! after structural growth call [`SemTree::build`] again (a rebuild is cheap —
//! it only allocates slots; unchanged subtrees still won't recompute on later
//! edits).
//!
//! ```
//! use lazily::{SourceTree, SemTree, Context};
//!
//! let ctx = Context::new();
//! let root: SourceTree<&'static str, i32> = SourceTree::leaf(&ctx, "root", 0);
//! let a = root.insert_child(&ctx, "a", 1);
//! a.insert_child(&ctx, "a1", 10);
//! root.insert_child(&ctx, "b", 2);
//!
//! // Derived: sum of every node's value in the subtree.
//! let sums = SemTree::build(&ctx, &root, |v: &i32, kids: &[i32]| v + kids.iter().sum::<i32>());
//! assert_eq!(sums.value(&ctx), 13); // 0 + (1 + 10) + 2
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

use crate::Context;
use crate::cell::Computed;
use crate::cell_tree::SourceTree;

/// A shared fold `(node value, children derived) -> derived`.
type FoldFn<V, D> = Rc<dyn Fn(&V, &[D]) -> D>;

/// A memoized semantic derivation over a [`SourceTree`]: one `memo` slot per node,
/// each folding `(node value, child derived values) -> D`.
pub struct SemTree<Id, D> {
    root: Computed<D>,
    nodes: HashMap<Id, Computed<D>>,
}

impl<Id, D> SemTree<Id, D>
where
    Id: Eq + Hash + Clone + 'static,
    D: PartialEq + Clone + 'static,
{
    /// Build the semantic tree from `root`, folding each node with `fold`
    /// (`fold(node_value, children_derived) -> derived`). Children are folded in
    /// the tree's current order.
    pub fn build<V>(
        ctx: &Context,
        root: &SourceTree<Id, V>,
        fold: impl Fn(&V, &[D]) -> D + 'static,
    ) -> Self
    where
        V: PartialEq + Clone + 'static,
    {
        let fold: FoldFn<V, D> = Rc::new(fold);
        let mut nodes = HashMap::new();
        let root_slot = derive(ctx, root, &fold, &mut nodes);
        nodes.insert(root.id().clone(), root_slot);
        Self {
            root: root_slot,
            nodes,
        }
    }

    /// The root derived slot.
    pub fn root(&self) -> Computed<D> {
        self.root
    }

    /// Read the derived value at the root (reactive).
    pub fn value(&self, ctx: &Context) -> D {
        ctx.get(&self.root)
    }

    /// The derived slot for a node id, if it was present at build time.
    pub fn node(&self, id: &Id) -> Option<Computed<D>> {
        self.nodes.get(id).copied()
    }

    /// Read the derived value at a node id, if present (reactive).
    pub fn node_value(&self, ctx: &Context, id: &Id) -> Option<D> {
        self.nodes.get(id).map(|s| ctx.get(s))
    }
}

fn derive<Id, V, D>(
    ctx: &Context,
    node: &SourceTree<Id, V>,
    fold: &FoldFn<V, D>,
    nodes: &mut HashMap<Id, Computed<D>>,
) -> Computed<D>
where
    Id: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
    D: PartialEq + Clone + 'static,
{
    // Build child derived slots first (current structure; no tracking frame is
    // active here, so reading children does not create a spurious subscription).
    let children = node.children(ctx);
    let mut child_slots: Vec<(Id, Computed<D>)> = Vec::with_capacity(children.len());
    for c in &children {
        let s = derive(ctx, c, fold, nodes);
        nodes.insert(c.id().clone(), s);
        child_slots.push((c.id().clone(), s));
    }

    let node = node.clone();
    let fold = Rc::clone(fold);
    ctx.computed(move |ctx| {
        let v = node.get(ctx); // subscribe to this node's value cell
        // Subscribe to child order/membership and fold children in current order.
        let ids = node.child_ids(ctx);
        let mut ds = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some((_, slot)) = child_slots.iter().find(|(cid, _)| cid == id) {
                ds.push(ctx.get(slot));
            }
        }
        fold(&v, &ds)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small tree: root -> {a -> {a1, a2}, b -> {b1}}.
    fn tree(ctx: &Context) -> SourceTree<&'static str, i32> {
        let root = SourceTree::leaf(ctx, "root", 0);
        let a = root.insert_child(ctx, "a", 1);
        a.insert_child(ctx, "a1", 10);
        a.insert_child(ctx, "a2", 20);
        let b = root.insert_child(ctx, "b", 2);
        b.insert_child(ctx, "b1", 100);
        root
    }

    fn sum_tree(ctx: &Context, root: &SourceTree<&'static str, i32>) -> SemTree<&'static str, i32> {
        SemTree::build(ctx, root, |v: &i32, kids: &[i32]| {
            v + kids.iter().sum::<i32>()
        })
    }

    #[test]
    fn folds_whole_subtree() {
        let ctx = Context::new();
        let root = tree(&ctx);
        let sums = sum_tree(&ctx, &root);
        assert_eq!(sums.value(&ctx), 133); // 0 + (1+10+20) + (2+100)
        assert_eq!(sums.node_value(&ctx, &"a"), Some(31));
        assert_eq!(sums.node_value(&ctx, &"b"), Some(102));
    }

    #[test]
    fn edit_recomputes_only_ancestor_chain_not_siblings() {
        let ctx = Context::new();
        let root = tree(&ctx);
        let sums = sum_tree(&ctx, &root);
        // Prime all derived slots.
        assert_eq!(sums.value(&ctx), 133);
        let a_slot = sums.node(&"a").unwrap();
        let b_slot = sums.node(&"b").unwrap();
        assert!(ctx.is_set(&a_slot) && ctx.is_set(&b_slot));

        // Edit a node inside subtree B.
        root.child(&"b")
            .unwrap()
            .child(&"b1")
            .unwrap()
            .set(&ctx, 200);

        // Subtree A's derived value stays cached (sibling isolation, the lazy win).
        assert!(ctx.is_set(&a_slot), "sibling subtree must NOT recompute");
        // B's chain and the root update on demand.
        assert_eq!(sums.node_value(&ctx, &"b"), Some(202));
        assert_eq!(sums.value(&ctx), 233);
        assert_eq!(sums.node_value(&ctx, &"a"), Some(31));
    }

    #[test]
    fn memo_guard_stops_propagation_when_result_unchanged() {
        use std::cell::Cell as StdCell;
        use std::rc::Rc;

        let ctx = Context::new();
        // Derived: COUNT of nodes whose value is negative ("unresolved").
        let root = SourceTree::leaf(&ctx, "root", 0);
        let a = root.insert_child(&ctx, "a", -1); // unresolved
        a.insert_child(&ctx, "a1", 5);
        root.insert_child(&ctx, "b", 7);
        let count = SemTree::build(&ctx, &root, |v: &i32, kids: &[i32]| {
            (if *v < 0 { 1 } else { 0 }) + kids.iter().sum::<i32>()
        });

        // A downstream consumer of the derived root; count how often it re-runs.
        let calls = Rc::new(StdCell::new(0usize));
        let observer = ctx.computed({
            let calls = Rc::clone(&calls);
            let root_slot = count.root();
            move |ctx| {
                calls.set(calls.get() + 1);
                ctx.get(&root_slot)
            }
        });
        assert_eq!(ctx.get(&observer), 1);
        assert_eq!(calls.get(), 1);

        // Change a positive node to another positive value: the derived count is
        // unchanged, so the memo guard keeps the downstream consumer from
        // re-running its closure.
        root.child(&"b").unwrap().set(&ctx, 9);
        assert_eq!(ctx.get(&observer), 1);
        assert_eq!(
            calls.get(),
            1,
            "memo guard: unchanged derived count must not re-run the downstream consumer"
        );

        // Flipping a node to negative DOES change the count -> consumer re-runs.
        root.child(&"b").unwrap().set(&ctx, -3);
        assert_eq!(ctx.get(&observer), 2);
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn removal_and_reorder_update_derivation() {
        let ctx = Context::new();
        let root = tree(&ctx);
        let sums = sum_tree(&ctx, &root);
        assert_eq!(sums.value(&ctx), 133);

        // Remove subtree b -> its 102 drops out.
        root.remove_child(&ctx, &"b");
        assert_eq!(sums.value(&ctx), 31); // 0 + (1+10+20)

        // Reorder a's children (no value change) -> sum stays the same.
        root.child(&"a").unwrap().move_child(&ctx, &"a2", 0);
        assert_eq!(sums.node_value(&ctx, &"a"), Some(31));
    }

    #[test]
    fn agent_doc_shaped_unresolved_prompt_count() {
        // A document-shaped tree where a node value of `true` = an unresolved
        // prompt; derive the per-subtree unresolved count (a real semantic query).
        let ctx = Context::new();
        let doc = SourceTree::leaf(&ctx, "doc", false);
        let ex = doc.insert_child(&ctx, "exchange", false);
        ex.insert_child(&ctx, "q1", true); // unresolved
        ex.insert_child(&ctx, "q2", false);
        let q = doc.insert_child(&ctx, "queue", false);
        q.insert_child(&ctx, "h1", true); // unresolved

        let unresolved = SemTree::build(&ctx, &doc, |v: &bool, kids: &[usize]| {
            (*v as usize) + kids.iter().sum::<usize>()
        });
        assert_eq!(unresolved.value(&ctx), 2);
        assert_eq!(unresolved.node_value(&ctx, &"exchange"), Some(1));

        // Resolve q1 -> exchange's count drops, queue's stays cached.
        let queue_slot = unresolved.node(&"queue").unwrap();
        ex.child(&"q1").unwrap().set(&ctx, false);
        assert!(ctx.is_set(&queue_slot), "unrelated subtree stays cached");
        assert_eq!(unresolved.value(&ctx), 1);
        assert_eq!(unresolved.node_value(&ctx, &"exchange"), Some(0));
    }
}
