//! Thread-safe memoized semantic tree over a [`ThreadSafeSourceTree`].

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use crate::{Computed, ThreadSafeContext, ThreadSafeSourceTree};

type FoldFn<V, D> = Arc<dyn Fn(&V, &[D]) -> D + Send + Sync>;

/// A `Send + Sync` semantic derivation with one computed slot per source node.
pub struct ThreadSafeSemTree<Id, D> {
    root: Computed<D>,
    nodes: HashMap<Id, Computed<D>>,
}

impl<Id, D> ThreadSafeSemTree<Id, D>
where
    Id: Eq + Hash + Clone + Send + Sync + 'static,
    D: PartialEq + Clone + Send + Sync + 'static,
{
    /// Build a semantic tree whose nodes fold `(value, child values)`.
    ///
    /// As with [`crate::SemTree`], structural insertion requires rebuilding so
    /// the new node receives its own computed slot.
    pub fn build<V>(
        ctx: &ThreadSafeContext,
        root: &ThreadSafeSourceTree<Id, V>,
        fold: impl Fn(&V, &[D]) -> D + Send + Sync + 'static,
    ) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        let fold: FoldFn<V, D> = Arc::new(fold);
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

    /// Read the derived root value.
    pub fn value(&self, ctx: &ThreadSafeContext) -> D {
        ctx.get(&self.root)
    }

    /// Find the derived slot for a node that existed at build time.
    pub fn node(&self, id: &Id) -> Option<Computed<D>> {
        self.nodes.get(id).copied()
    }

    /// Read the derived value at `id`.
    pub fn node_value(&self, ctx: &ThreadSafeContext, id: &Id) -> Option<D> {
        self.nodes.get(id).map(|slot| ctx.get(slot))
    }
}

fn derive<Id, V, D>(
    ctx: &ThreadSafeContext,
    node: &ThreadSafeSourceTree<Id, V>,
    fold: &FoldFn<V, D>,
    nodes: &mut HashMap<Id, Computed<D>>,
) -> Computed<D>
where
    Id: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
    D: PartialEq + Clone + Send + Sync + 'static,
{
    let children = node.children(ctx);
    let mut child_slots = Vec::with_capacity(children.len());
    for child in &children {
        let slot = derive(ctx, child, fold, nodes);
        nodes.insert(child.id().clone(), slot);
        child_slots.push((child.id().clone(), slot));
    }

    let node = node.clone();
    let fold = Arc::clone(fold);
    ctx.computed(move |ctx| {
        let value = node.get(ctx);
        let ids = node.child_ids(ctx);
        let mut derived = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some((_, slot)) = child_slots.iter().find(|(child_id, _)| child_id == id) {
                derived.push(ctx.get(slot));
            }
        }
        fold(&value, &derived)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn sem_tree_is_send_sync() {
        assert_send_sync::<ThreadSafeSemTree<String, usize>>();
    }

    #[test]
    fn edit_recomputes_only_the_ancestor_chain() {
        let ctx = ThreadSafeContext::new();
        let root = ThreadSafeSourceTree::leaf(&ctx, "root", 0);
        let a = root.insert_child(&ctx, "a", 1);
        a.insert_child(&ctx, "a1", 10);
        let b = root.insert_child(&ctx, "b", 2);
        b.insert_child(&ctx, "b1", 100);

        let a_runs = Arc::new(AtomicUsize::new(0));
        let b_runs = Arc::new(AtomicUsize::new(0));
        let sums = ThreadSafeSemTree::build(&ctx, &root, {
            let a_runs = Arc::clone(&a_runs);
            let b_runs = Arc::clone(&b_runs);
            move |value: &i32, children: &[i32]| {
                if *value == 1 {
                    a_runs.fetch_add(1, Ordering::SeqCst);
                } else if *value == 2 {
                    b_runs.fetch_add(1, Ordering::SeqCst);
                }
                value + children.iter().sum::<i32>()
            }
        });

        assert_eq!(sums.value(&ctx), 113);
        assert_eq!(a_runs.load(Ordering::SeqCst), 1);
        assert_eq!(b_runs.load(Ordering::SeqCst), 1);

        b.child(&"b1").unwrap().set(&ctx, 200);
        assert_eq!(sums.value(&ctx), 213);
        assert_eq!(a_runs.load(Ordering::SeqCst), 1);
        assert_eq!(b_runs.load(Ordering::SeqCst), 2);
    }
}
