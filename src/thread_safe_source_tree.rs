//! Thread-safe ordered keyed reactive tree.
//!
//! [`ThreadSafeSourceTree`] is the `Send + Sync` companion to
//! [`SourceTree`](crate::SourceTree). Each node owns one reactive value cell and
//! one [`ThreadSafeSourceMap`] for ordered child membership. The child handles
//! themselves live behind a short-lived mutex; that mutex is always released
//! before the reactive context is read or written.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{Source, ThreadSafeContext, ThreadSafeSourceMap};

/// A node in an ordered, stably-keyed, thread-safe reactive tree.
pub struct ThreadSafeSourceTree<Id, V> {
    inner: Arc<ThreadSafeSourceTreeNode<Id, V>>,
}

struct ThreadSafeSourceTreeNode<Id, V> {
    id: Id,
    value: Source<V>,
    order: ThreadSafeSourceMap<Id, ()>,
    nodes: Mutex<HashMap<Id, ThreadSafeSourceTree<Id, V>>>,
}

impl<Id, V> Clone for ThreadSafeSourceTree<Id, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Id, V> ThreadSafeSourceTree<Id, V>
where
    Id: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// Create a leaf node with stable `id` and initial `value`.
    pub fn leaf(ctx: &ThreadSafeContext, id: Id, value: V) -> Self {
        Self {
            inner: Arc::new(ThreadSafeSourceTreeNode {
                id,
                value: ctx.source(value),
                order: ThreadSafeSourceMap::new(ctx),
                nodes: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn nodes(&self) -> MutexGuard<'_, HashMap<Id, ThreadSafeSourceTree<Id, V>>> {
        self.inner
            .nodes
            .lock()
            .expect("thread-safe source-tree nodes mutex poisoned")
    }

    /// This node's stable id.
    pub fn id(&self) -> &Id {
        &self.inner.id
    }

    /// This node's value cell handle.
    pub fn value(&self) -> Source<V> {
        self.inner.value
    }

    /// Reactively read this node's value.
    pub fn get(&self, ctx: &ThreadSafeContext) -> V {
        ctx.get(&self.inner.value)
    }

    /// Set this node's value.
    pub fn set(&self, ctx: &ThreadSafeContext, value: V) {
        ctx.set(&self.inner.value, value);
    }

    /// Insert a leaf child at the end, returning an existing child unchanged
    /// when `id` is already present.
    pub fn insert_child(
        &self,
        ctx: &ThreadSafeContext,
        id: Id,
        value: V,
    ) -> ThreadSafeSourceTree<Id, V> {
        if let Some(existing) = self.nodes().get(&id).cloned() {
            return existing;
        }

        let candidate = ThreadSafeSourceTree::leaf(ctx, id.clone(), value);
        let child = {
            let mut nodes = self.nodes();
            if let Some(existing) = nodes.get(&id) {
                return existing.clone();
            }
            nodes.insert(id.clone(), candidate.clone());
            candidate
        };
        self.inner.order.set(ctx, id, ());
        child
    }

    /// Attach an already-built subtree at the end, preserving its node identity.
    pub fn attach_child(
        &self,
        ctx: &ThreadSafeContext,
        child: ThreadSafeSourceTree<Id, V>,
    ) -> ThreadSafeSourceTree<Id, V> {
        let id = child.id().clone();
        {
            let mut nodes = self.nodes();
            if let Some(existing) = nodes.get(&id) {
                return existing.clone();
            }
            nodes.insert(id.clone(), child.clone());
        }
        self.inner.order.set(ctx, id, ());
        child
    }

    /// Get a child by id without creating a reactive dependency.
    pub fn child(&self, id: &Id) -> Option<ThreadSafeSourceTree<Id, V>> {
        self.nodes().get(id).cloned()
    }

    /// Remove a child by id.
    pub fn remove_child(&self, ctx: &ThreadSafeContext, id: &Id) -> bool {
        let removed = self.nodes().remove(id).is_some();
        if removed {
            self.inner.order.remove(ctx, id);
        }
        removed
    }

    /// Move child `id` to `index` while preserving its node identity.
    pub fn move_child(&self, ctx: &ThreadSafeContext, id: &Id, index: usize) -> bool {
        self.inner.order.move_to(ctx, id, index)
    }

    /// Move child `id` immediately before `anchor`.
    pub fn move_child_before(&self, ctx: &ThreadSafeContext, id: &Id, anchor: &Id) -> bool {
        self.inner.order.move_before(ctx, id, anchor)
    }

    /// Move child `id` immediately after `anchor`.
    pub fn move_child_after(&self, ctx: &ThreadSafeContext, id: &Id, anchor: &Id) -> bool {
        self.inner.order.move_after(ctx, id, anchor)
    }

    /// Reactively read the ordered direct-child ids.
    pub fn child_ids(&self, ctx: &ThreadSafeContext) -> Vec<Id> {
        self.inner.order.keys(ctx)
    }

    /// Reactively read the ordered direct-child handles.
    pub fn children(&self, ctx: &ThreadSafeContext) -> Vec<ThreadSafeSourceTree<Id, V>> {
        let ids = self.inner.order.keys(ctx);
        let nodes = self.nodes();
        ids.into_iter()
            .filter_map(|id| nodes.get(&id).cloned())
            .collect()
    }

    /// Reactively read the direct-child count.
    pub fn len(&self, ctx: &ThreadSafeContext) -> usize {
        self.inner.order.len(ctx)
    }

    /// Reactively test whether the node has no direct children.
    pub fn is_empty(&self, ctx: &ThreadSafeContext) -> bool {
        self.inner.order.is_empty(ctx)
    }

    /// Reactively test direct-child membership.
    pub fn contains_child(&self, ctx: &ThreadSafeContext, id: &Id) -> bool {
        self.inner.order.contains_key(ctx, id)
    }

    /// Resolve a node by a path of ids without creating a reactive dependency.
    pub fn resolve_path(&self, path: &[Id]) -> Option<ThreadSafeSourceTree<Id, V>> {
        let mut node = self.clone();
        for segment in path {
            node = node.child(segment)?;
        }
        Some(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn tree_is_send_sync() {
        assert_send_sync::<ThreadSafeSourceTree<String, String>>();
    }

    #[test]
    fn ordered_children_preserve_identity_and_value_isolation() {
        let ctx = ThreadSafeContext::new();
        let root = ThreadSafeSourceTree::leaf(&ctx, "root", 0);
        let a = root.insert_child(&ctx, "a", 1);
        root.insert_child(&ctx, "b", 2);
        root.insert_child(&ctx, "c", 3);

        let observed_a = {
            let a = a.clone();
            let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = Arc::clone(&runs);
            let slot = ctx.computed(move |ctx| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                a.get(ctx)
            });
            (slot, runs)
        };
        assert_eq!(ctx.get(&observed_a.0), 1);

        root.child(&"b").unwrap().set(&ctx, 20);
        root.move_child(&ctx, &"c", 0);
        assert_eq!(root.child_ids(&ctx), vec!["c", "a", "b"]);
        assert_eq!(ctx.get(&observed_a.0), 1);
        assert_eq!(observed_a.1.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(root.child(&"a").unwrap().value(), a.value());
    }
}
