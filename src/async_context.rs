use std::any::Any;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::context::{GraphNode, Read, SlotId, Write};
use crate::merge::MergePolicy;

#[cfg(not(feature = "vec_edges"))]
type EdgeVec = smallvec::SmallVec<[SlotId; 4]>;
#[cfg(feature = "vec_edges")]
type EdgeVec = Vec<SlotId>;

fn edge_insert(edges: &mut EdgeVec, id: SlotId) -> bool {
    if edges.contains(&id) {
        false
    } else {
        edges.push(id);
        true
    }
}

fn edge_remove(edges: &mut EdgeVec, id: SlotId) -> bool {
    if let Some(pos) = edges.iter().position(|x| *x == id) {
        edges.swap_remove(pos);
        true
    } else {
        false
    }
}

type AsyncAny = dyn Any + Send + Sync;
type BoxedAsyncFuture = Pin<Box<dyn Future<Output = Arc<AsyncAny>> + Send>>;
type AsyncComputeFn = dyn Fn(AsyncComputeContext) -> BoxedAsyncFuture + Send + Sync;
/// A compute that produces its value WITHOUT awaiting (`#lzqfrs`). Async slots
/// normally resolve through a spawned task, so a read before resolution yields
/// `None`. A reader kind derived from out-of-graph state has nothing to await,
/// and forcing it through the task machinery would make every queue read return
/// `Option` for no reason — so these resolve inline on read.
type AsyncSyncComputeFn = dyn Fn(&AsyncComputeContext) -> Arc<AsyncAny> + Send + Sync;
type AsyncEqualsFn = dyn Fn(&AsyncAny, &AsyncAny) -> bool + Send + Sync;
type BoxedEffectFuture = Pin<Box<dyn Future<Output = Option<BoxedCleanupFn>> + Send>>;
type BoxedCleanupFn = Box<dyn FnOnce() + Send>;
type AsyncEffectFn = dyn Fn(AsyncComputeContext) -> BoxedEffectFuture + Send + Sync;

static NEXT_ASYNC_CONTEXT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncContextId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncSlotStateView {
    None,
    Empty,
    Computing { revision: u64 },
    Resolved,
    Error,
}

#[derive(Debug)]
pub enum AsyncSlotState {
    Empty,
    Computing {
        revision: u64,
        handle: JoinHandle<()>,
    },
    Resolved,
    Error,
}

impl fmt::Display for AsyncSlotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "Empty"),
            Self::Computing { revision, .. } => {
                write!(f, "Computing(revision={revision})")
            }
            Self::Resolved => write!(f, "Resolved"),
            Self::Error => write!(f, "Error"),
        }
    }
}

#[allow(dead_code)]
pub(crate) enum TransitionOutcome {
    Accepted,
    Stale,
    Unchanged,
}

#[allow(dead_code)]
pub(crate) enum InvalidationResult {
    HadInFlight(JoinHandle<()>),
    WasResolved,
    WasError,
    AlreadyEmpty,
}

pub(crate) struct AsyncComputedNode {
    pub(crate) state: AsyncSlotState,
    pub(crate) value: Option<Arc<AsyncAny>>,
    pub(crate) error: Option<Arc<dyn Error + Send + Sync>>,
    pub(crate) revision: u64,
    pub(crate) compute: Arc<AsyncComputeFn>,
    /// Present iff built by `AsyncContext::computed` (synchronous compute).
    pub(crate) sync_compute: Option<Arc<AsyncSyncComputeFn>>,
    pub(crate) equals: Option<Arc<AsyncEqualsFn>>,
    pub(crate) dependencies: EdgeVec,
    pub(crate) dependents: EdgeVec,
    pub(crate) notifier: Option<watch::Sender<AsyncCompletion>>,
}

#[derive(Clone)]
pub(crate) enum AsyncCompletion {
    Pending,
    Resolved(Arc<dyn Any + Send + Sync>),
    Error(Arc<dyn Error + Send + Sync>),
}

impl AsyncComputedNode {
    pub(crate) fn transition_to_computing(
        &mut self,
        handle: JoinHandle<()>,
    ) -> Option<JoinHandle<()>> {
        let old = std::mem::replace(
            &mut self.state,
            AsyncSlotState::Computing {
                revision: self.revision,
                handle,
            },
        );
        match old {
            AsyncSlotState::Computing { handle, .. } => Some(handle),
            _ => None,
        }
    }

    pub(crate) fn transition_to_resolved(
        &mut self,
        revision: u64,
        value: Arc<AsyncAny>,
    ) -> TransitionOutcome {
        match &self.state {
            AsyncSlotState::Computing {
                revision: current_revision,
                ..
            } if *current_revision == revision => {}
            _ => return TransitionOutcome::Stale,
        }

        let is_new = match (&self.value, &self.equals) {
            (Some(old), Some(eq)) => !eq(old.as_ref(), value.as_ref()),
            _ => true,
        };

        self.state = AsyncSlotState::Resolved;
        self.value = Some(value);
        self.error = None;

        if is_new {
            TransitionOutcome::Accepted
        } else {
            TransitionOutcome::Unchanged
        }
    }

    pub(crate) fn transition_to_error(
        &mut self,
        revision: u64,
        error: Arc<dyn Error + Send + Sync>,
    ) -> TransitionOutcome {
        match &self.state {
            AsyncSlotState::Computing {
                revision: current_revision,
                ..
            } if *current_revision == revision => {}
            _ => return TransitionOutcome::Stale,
        }

        self.state = AsyncSlotState::Error;
        self.error = Some(error);
        self.value = None;
        TransitionOutcome::Accepted
    }

    pub(crate) fn invalidate(&mut self) -> InvalidationResult {
        self.revision += 1;
        match std::mem::replace(&mut self.state, AsyncSlotState::Empty) {
            AsyncSlotState::Computing { handle, .. } => InvalidationResult::HadInFlight(handle),
            AsyncSlotState::Resolved => InvalidationResult::WasResolved,
            AsyncSlotState::Error => InvalidationResult::WasError,
            AsyncSlotState::Empty => InvalidationResult::AlreadyEmpty,
        }
    }

    pub(crate) fn clear(&mut self) -> Option<JoinHandle<()>> {
        self.revision += 1;
        self.value = None;
        self.error = None;
        match std::mem::replace(&mut self.state, AsyncSlotState::Empty) {
            AsyncSlotState::Computing { handle, .. } => Some(handle),
            _ => None,
        }
    }
}

pub(crate) struct AsyncSourceNode {
    pub(crate) value: Arc<AsyncAny>,
    pub(crate) dependents: EdgeVec,
}

pub(crate) struct AsyncEffectNode {
    pub(crate) effect_fn: Arc<AsyncEffectFn>,
    pub(crate) cleanup: Option<BoxedCleanupFn>,
    pub(crate) dependencies: EdgeVec,
    pub(crate) dependents: EdgeVec,
    pub(crate) force_run: bool,
    pub(crate) in_flight: Option<JoinHandle<()>>,
}

#[allow(dead_code)]
pub(crate) enum AsyncNode {
    Computed(AsyncComputedNode),
    Source(AsyncSourceNode),
    Effect(AsyncEffectNode),
}

pub(crate) struct AsyncContextInner {
    pub(crate) nodes: Vec<Option<AsyncNode>>,
    /// Per-index generation counter, parallel to `nodes`. Bumped every time the
    /// node at an index is disposed and its `SlotId` recycled into `free_ids`
    /// (#lzasyncdispose2). A task spawned for an effect captures the generation
    /// at spawn time and re-checks it before writing cleanup/edges/`in_flight`
    /// back, so a run still in-flight across its `.await` can never alias a
    /// freshly-allocated node that reused the recycled id.
    generations: Vec<u64>,
    next_id: u64,
    free_ids: Vec<u64>,
    pub(crate) context_id: AsyncContextId,
    batch_depth: usize,
    batched_cells: HashSet<SlotId>,
    pending_async_effects: Vec<SlotId>,
    scheduled_async_effects: HashSet<SlotId>,
    /// Free-list of dependency trackers recycled between compute/effect runs
    /// (`#lzrsdeppool`). Every spawn used to mint a fresh
    /// `Arc<Mutex<HashSet<SlotId>>>`; pooling reuses both the `Arc` allocation
    /// and the set's table capacity, so a steady-state graph stops allocating
    /// on the spawn path entirely. Trackers only re-enter the pool when the
    /// spawn holds the sole reference (see [`Self::recycle_deps`]).
    deps_pool: Vec<Arc<Mutex<HashSet<SlotId>>>>,
}

/// Upper bound on pooled dependency trackers. Async spawns are bounded by the
/// number of concurrently in-flight nodes; capping retention keeps a burst from
/// pinning trackers (and their table capacity) for the context's lifetime.
const DEPS_POOL_CAP: usize = 32;

impl AsyncContextInner {
    /// Hand out a cleared dependency tracker, reusing a pooled one when
    /// available (`#lzrsdeppool`).
    fn take_deps(&mut self) -> Arc<Mutex<HashSet<SlotId>>> {
        self.deps_pool
            .pop()
            .unwrap_or_else(|| Arc::new(Mutex::new(HashSet::new())))
    }

    /// Return a tracker to the pool, restoring `set` (the extracted dependency
    /// set) as its storage so the table capacity survives (`#lzrsdeppool`).
    ///
    /// `Arc::get_mut` is the safety gate: it yields `Some` only when this is the
    /// last reference, so a compute future that leaked its `AsyncComputeContext`
    /// past its own completion can never observe a tracker handed to a later
    /// spawn. It also avoids locking the tracker while `AsyncContextInner` is
    /// held — `AsyncComputeContext::get_cell` locks deps *then* inner, so the
    /// reverse order would invert the lock hierarchy.
    fn recycle_deps(&mut self, mut deps: Arc<Mutex<HashSet<SlotId>>>, mut set: HashSet<SlotId>) {
        if self.deps_pool.len() >= DEPS_POOL_CAP {
            return;
        }
        let Some(tracker) = Arc::get_mut(&mut deps) else {
            return;
        };
        set.clear();
        *tracker.get_mut() = set;
        self.deps_pool.push(deps);
    }

    pub(crate) fn alloc_id(&mut self) -> SlotId {
        match self.free_ids.pop() {
            Some(id) => SlotId(id),
            None => {
                let id = SlotId(self.next_id);
                self.next_id += 1;
                id
            }
        }
    }

    fn node_index(id: SlotId) -> Option<usize> {
        usize::try_from(id.0).ok()
    }

    /// Current generation of the node slot `id` maps to. A never-allocated index
    /// reads as `0`, which matches the generation a freshly-allocated node sees.
    pub(crate) fn generation(&self, id: SlotId) -> u64 {
        Self::node_index(id)
            .and_then(|idx| self.generations.get(idx))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn get_node(&self, id: SlotId) -> Option<&AsyncNode> {
        Self::node_index(id)
            .and_then(|idx| self.nodes.get(idx))
            .and_then(|opt| opt.as_ref())
    }

    pub(crate) fn get_node_mut(&mut self, id: SlotId) -> Option<&mut AsyncNode> {
        Self::node_index(id)
            .and_then(|idx| self.nodes.get_mut(idx))
            .and_then(|opt| opt.as_mut())
    }

    pub(crate) fn insert_node(&mut self, id: SlotId, node: AsyncNode) {
        let index = Self::node_index(id).expect("SlotId does not fit usize");
        if self.nodes.len() <= index {
            self.nodes.resize_with(index + 1, || None);
        }
        if self.generations.len() <= index {
            self.generations.resize(index + 1, 0);
        }
        self.nodes[index] = Some(node);
    }
}

fn register_dependency_locked(
    inner: &mut AsyncContextInner,
    dependency_id: SlotId,
    dependent_id: SlotId,
) {
    if dependency_id == dependent_id {
        return;
    }
    if let Some(node) = inner.get_node_mut(dependent_id) {
        match node {
            AsyncNode::Computed(s) => {
                edge_insert(&mut s.dependencies, dependency_id);
            }
            AsyncNode::Effect(e) => {
                edge_insert(&mut e.dependencies, dependency_id);
            }
            AsyncNode::Source(_) => {}
        }
    }
    if let Some(node) = inner.get_node_mut(dependency_id) {
        match node {
            AsyncNode::Computed(s) => {
                edge_insert(&mut s.dependents, dependent_id);
            }
            AsyncNode::Source(c) => {
                edge_insert(&mut c.dependents, dependent_id);
            }
            AsyncNode::Effect(_) => {}
        }
    }
}

/// Test-only async hook installed via [`AsyncContext::__install_window1_hook`]
/// to deterministically exercise the `#k03k` window-1 race in `get_async` (the
/// slot transitioning `Computing -> Resolved` between the lock-free fast-path
/// check and the re-lock). Compiled out of default/release builds.
#[cfg(feature = "instrumentation")]
pub type Window1Hook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// The async handles address nodes in the same way the sync ones do, so they join
// the same sealed `GraphNode` trait rather than growing a parallel accessor set.
impl<T> crate::context::sealed::Sealed for AsyncComputed<T> {}
impl<T> GraphNode for AsyncComputed<T> {
    fn node_id(&self) -> SlotId {
        self.id
    }
}

impl<T> crate::context::sealed::Sealed for AsyncSource<T> {}
impl<T> GraphNode for AsyncSource<T> {
    fn node_id(&self) -> SlotId {
        self.id
    }
}

impl crate::context::sealed::Sealed for AsyncEffectHandle {}
impl GraphNode for AsyncEffectHandle {
    fn node_id(&self) -> SlotId {
        self.id
    }
}

/// A teardown scope over an [`AsyncContext`]: nodes created through it are
/// disposed when it drops.
///
/// Holds an owned handle rather than a borrow, for the same reason
/// [`ThreadSafeTeardownScope`](crate::ThreadSafeTeardownScope) does: the context
/// is already an `Arc` over shared state, so owning one is cheap and makes the
/// scope `Send` and `'static`-able.
pub struct AsyncTeardownScope {
    ctx: AsyncContext,
    owned: Mutex<Vec<SlotId>>,
}

impl AsyncTeardownScope {
    /// Create a guarded async computed cell owned by this scope.
    pub fn computed_async<T, F, Fut>(&self, compute: F) -> AsyncComputed<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let handle = self.ctx.computed_async(compute);
        self.owned.lock().push(handle.id);
        handle
    }

    /// Create a source cell owned by this scope.
    pub fn source<T>(&self, value: T) -> AsyncSource<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        let handle = self.ctx.source(value);
        self.owned.lock().push(handle.id);
        handle
    }

    /// Compatibility constructor for the pre-Cell-kernel API.
    #[deprecated(note = "use `AsyncTeardownScope::source`")]
    pub fn cell<T>(&self, value: T) -> AsyncSource<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        self.source(value)
    }

    /// Register an async effect owned by this scope.
    pub fn effect_async<F, Fut, C>(&self, effect: F) -> AsyncEffectHandle
    where
        F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<C>> + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        let handle = self.ctx.effect_async(effect);
        self.owned.lock().push(handle.id);
        handle
    }

    /// The context this scope belongs to.
    pub fn context(&self) -> &AsyncContext {
        &self.ctx
    }

    /// How many nodes this scope owns.
    pub fn len(&self) -> usize {
        self.owned.lock().len()
    }

    /// Whether this scope owns nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Disarm the scope: ending it afterwards disposes nothing.
    pub fn disarm(self) {
        self.owned.lock().clear();
    }
}

impl Drop for AsyncTeardownScope {
    fn drop(&mut self) {
        // Reverse creation order: dependents before what they read.
        let owned = std::mem::take(&mut *self.owned.lock());
        for id in owned.into_iter().rev() {
            self.ctx.dispose_id(id);
        }
    }
}

pub struct AsyncContext {
    inner: Arc<Mutex<AsyncContextInner>>,
    /// One-shot async seam fired inside `get_async`'s window-1 gap; `take`n on
    /// first fire so it runs exactly once. See [`Window1Hook`].
    #[cfg(feature = "instrumentation")]
    window1_hook: Mutex<Option<Window1Hook>>,
    /// Counts how many times `get_async` returned through the window-1
    /// `Resolved`-after-re-lock arm. Lets tests prove the race arm was taken
    /// rather than the fast path.
    #[cfg(feature = "instrumentation")]
    window1_resolved_hits: AtomicU64,
}

pub struct AsyncComputed<T> {
    pub(crate) id: SlotId,
    pub(crate) _marker: PhantomData<T>,
}

impl<T> AsyncComputed<T> {
    /// This cell's graph id, for [`AsyncContext::clear_slots`].
    ///
    /// `clear_slots` takes ids rather than handles because the roots of one
    /// invalidation are usually different types — a queue clears `head`
    /// (`Option<T>`), `len` (`usize`) and `is_full` (`bool`) in the same walk, and
    /// no single handle type spans them.
    pub fn id(&self) -> SlotId {
        self.id
    }
}

impl<T> Clone for AsyncComputed<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for AsyncComputed<T> {}

// Node identity, matching the single-threaded `Computed`. Without these, the
// `handle_stable` half of the collections contract — "an atomic move keeps the
// entry's node, it does not remove + re-mint" — is not even expressible against
// the async flavor.
impl<T> PartialEq for AsyncComputed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for AsyncComputed<T> {}

impl<T> fmt::Debug for AsyncComputed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncComputed")
            .field("id", &self.id)
            .finish()
    }
}

/// Compatibility name for [`AsyncComputed`].
#[deprecated(note = "use `AsyncComputed`")]
pub type AsyncSlotHandle<T> = AsyncComputed<T>;

pub struct AsyncSource<T> {
    pub(crate) id: SlotId,
    pub(crate) _marker: PhantomData<T>,
}

impl<T> Clone for AsyncSource<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for AsyncSource<T> {}

// Node identity, matching the single-threaded `Source`. See `AsyncComputed`.
impl<T> PartialEq for AsyncSource<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for AsyncSource<T> {}

impl<T> fmt::Debug for AsyncSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncSource").field("id", &self.id).finish()
    }
}

/// Compatibility name for [`AsyncSource`].
#[deprecated(note = "use `AsyncSource`")]
pub type AsyncCellHandle<T> = AsyncSource<T>;

pub struct AsyncEffectHandle {
    pub(crate) id: SlotId,
}

impl Clone for AsyncEffectHandle {
    fn clone(&self) -> Self {
        *self
    }
}
impl Copy for AsyncEffectHandle {}

/// A typed handle to an **eager** derived value within an [`AsyncContext`].
///
/// This is the async counterpart to [`crate::Computed`]. It is a guarded
/// backing slot ([`AsyncContext::computed_async`]) plus a small puller effect
/// ([`AsyncContext::effect_async`]) that awaits the slot after every
/// invalidation, so an upstream change eagerly drives the async recompute to
/// completion instead of waiting for the next read. See
/// [`AsyncContext::signal_async`].
pub struct AsyncSignalHandle<T> {
    /// Memoized backing slot that holds the derived value.
    pub(crate) slot: AsyncComputed<T>,
    /// Puller effect that keeps `slot` eagerly materialized.
    pub(crate) effect: AsyncEffectHandle,
}

impl<T> AsyncSignalHandle<T> {
    /// Read this signal's current value if it has resolved, without awaiting.
    ///
    /// Ergonomic alias for [`AsyncContext::get_signal`].
    pub fn get(&self, ctx: &AsyncContext) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        ctx.get_signal(self)
    }

    /// Await this signal's current value, driving recomputation if needed.
    ///
    /// Ergonomic alias for [`AsyncContext::get_signal_async`].
    pub async fn get_async(&self, ctx: &AsyncContext) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        ctx.get_signal_async(self).await
    }

    /// Dispose this signal's eager puller. The backing value remains readable
    /// and reverts to lazy (recomputed on next read) behavior.
    pub fn dispose(&self, ctx: &AsyncContext) {
        ctx.dispose_signal(self);
    }

    /// Check whether this signal's eager puller is still active.
    pub fn is_active(&self, ctx: &AsyncContext) -> bool {
        ctx.is_signal_active(self)
    }
}

impl<T> Clone for AsyncSignalHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for AsyncSignalHandle<T> {}

pub struct AsyncComputeContext {
    pub(crate) _context_id: AsyncContextId,
    pub(crate) _node_id: SlotId,
    /// Generation of `_node_id` captured when this context's run was spawned.
    /// Live dependency-edge writes keyed by `_node_id` are skipped once the
    /// node's current generation diverges — i.e. the node was disposed and its
    /// id potentially recycled mid-run (#lzasyncdispose2).
    pub(crate) _node_gen: u64,
    pub(crate) inner: Arc<Mutex<AsyncContextInner>>,
    pub(crate) dependencies: Arc<Mutex<HashSet<SlotId>>>,
}

impl AsyncComputeContext {
    #[deprecated(note = "use `AsyncComputeContext::get` — the unified cell read (#lzcellkernel)")]
    pub fn get_cell<T>(&self, handle: &AsyncSource<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        self.get(handle)
    }

    /// Read the current value of a cell inside a compute/effect callback
    /// (`#lzcellkernel`). Unified read superseding the deprecated `get_cell`.
    pub fn get<H: Read<Self>>(&self, handle: &H) -> <H as Read<Self>>::Output {
        handle.read(self)
    }

    fn read_source<T>(&self, handle: &AsyncSource<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        self.dependencies.lock().insert(handle.id);
        let mut inner = self.inner.lock();
        if inner.generation(self._node_id) == self._node_gen {
            register_dependency_locked(&mut inner, handle.id, self._node_id);
        }
        match inner.get_node(handle.id) {
            Some(AsyncNode::Source(cell)) => cell
                .value
                .as_ref()
                .downcast_ref::<T>()
                .expect("type mismatch in async compute get")
                .clone(),
            _ => panic!("AsyncSource does not point to a Cell node"),
        }
    }

    fn read_computed<T>(&self, handle: &AsyncComputed<T>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.dependencies.lock().insert(handle.id);
        {
            let mut inner = self.inner.lock();
            if inner.generation(self._node_id) == self._node_gen {
                register_dependency_locked(&mut inner, handle.id, self._node_id);
            }
        }
        self.owning_context().read_slot(handle)
    }

    /// Reconstruct an owning [`AsyncContext`] over the same graph.
    ///
    /// A write from inside an effect is not a dependency — it creates no edge
    /// (§9.2.3) — so it goes through the plain [`AsyncContext`] write path rather
    /// than this tracking compute context. Mirrors how [`get_async`] builds an
    /// `AsyncContext` from `self.inner` to drive a downstream slot.
    pub(crate) fn owning_context(&self) -> AsyncContext {
        AsyncContext {
            inner: self.inner.clone(),
            #[cfg(feature = "instrumentation")]
            window1_hook: Mutex::new(None),
            #[cfg(feature = "instrumentation")]
            window1_resolved_hits: AtomicU64::new(0),
        }
    }

    /// Write a cell from inside a compute/effect callback (untracked — a write
    /// is an argument, never a dependency, §9.2.3).
    #[deprecated(note = "use `AsyncComputeContext::set` — the unified cell write (#lzcellkernel)")]
    pub fn set_cell<T>(&self, handle: &AsyncSource<T>, value: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        self.set(handle, value)
    }

    /// Write a cell from inside a compute/effect callback (untracked — a write
    /// is an argument, never a dependency, §9.2.3). Unified write superseding
    /// the deprecated `set_cell`.
    pub fn set<H: Write<Self>>(&self, handle: &H, value: <H as Write<Self>>::Value) {
        handle.write(self, value)
    }

    fn write_source<T>(&self, handle: &AsyncSource<T>, value: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        self.owning_context().set(handle, value);
    }

    /// Fold `op` into a cell under policy `M` from inside a callback — the
    /// in-effect counterpart of [`AsyncContext::apply_merge`]. This is the
    /// supported shape for feeding a merge cell from an async effect (§9.1): the
    /// effect reads its upstream through the tracking context, then folds the
    /// result in synchronously here, and the merge cell acquires no edge.
    pub fn apply_merge<T, M>(&self, handle: &AsyncSource<T>, op: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        M: MergePolicy<T>,
    {
        self.owning_context().apply_merge::<T, M>(handle, op);
    }

    pub fn get_async<T>(&self, handle: &AsyncComputed<T>) -> impl Future<Output = T> + Send + use<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.dependencies.lock().insert(handle.id);
        {
            let mut inner = self.inner.lock();
            if inner.generation(self._node_id) == self._node_gen {
                register_dependency_locked(&mut inner, handle.id, self._node_id);
            }
        }
        let inner_arc = self.inner.clone();
        // Copy the handle so the returned future does not borrow the `handle`
        // parameter; this keeps the future independent of caller-local handles.
        let handle = *handle;
        async move {
            let ctx = AsyncContext {
                inner: inner_arc,
                #[cfg(feature = "instrumentation")]
                window1_hook: Mutex::new(None),
                #[cfg(feature = "instrumentation")]
                window1_resolved_hits: AtomicU64::new(0),
            };
            ctx.get_async(&handle).await
        }
    }

    /// Await an eager [`AsyncSignalHandle`] from inside a slot/effect callback,
    /// registering its backing slot as a dependency.
    ///
    /// This is the in-callback counterpart to [`AsyncContext::get_signal_async`]
    /// and is what lets async signals be chained or observed by downstream
    /// computeds/effects.
    pub fn get_signal_async<T>(
        &self,
        handle: &AsyncSignalHandle<T>,
    ) -> impl Future<Output = T> + Send + use<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.get_async(&handle.slot)
    }
}

fn spawn_async_compute(ctx: &AsyncContext, slot_id: SlotId) -> watch::Receiver<AsyncCompletion> {
    let inner_arc: Arc<Mutex<AsyncContextInner>> = ctx.inner.clone();
    let mut inner = inner_arc.lock();

    let (compute, context_id, spawn_revision) = match inner.get_node(slot_id) {
        Some(AsyncNode::Computed(slot)) => {
            if let AsyncSlotState::Computing { .. } = &slot.state {
                return slot
                    .notifier
                    .as_ref()
                    .expect("computing without notifier")
                    .subscribe();
            }
            (slot.compute.clone(), inner.context_id, slot.revision)
        }
        _ => panic!("spawn_async_compute: not a slot node"),
    };

    let (tx, rx) = watch::channel(AsyncCompletion::Pending);
    let inner_for_compute = inner_arc.clone();
    let tx_clone = tx.clone();

    let deps_arc = inner.take_deps();
    let deps_for_extract = deps_arc.clone();
    let slot_gen = inner.generation(slot_id);

    let join_handle = tokio::spawn(async move {
        let compute_ctx = AsyncComputeContext {
            _context_id: context_id,
            _node_id: slot_id,
            _node_gen: slot_gen,
            inner: inner_for_compute.clone(),
            dependencies: deps_arc,
        };
        let result = compute(compute_ctx).await;
        // Take (not clone) the tracked set: the tracker is dead once this run
        // completes, so moving the set out saves a full `HashSet` clone per
        // compute and lets the capacity be recycled with it (`#lzrsdeppool`).
        let deps = std::mem::take(&mut *deps_for_extract.lock());
        {
            let mut inner = inner_for_compute.lock();
            let current_revision = match inner.get_node(slot_id) {
                Some(AsyncNode::Computed(s)) => s.revision,
                _ => {
                    inner.recycle_deps(deps_for_extract, deps);
                    drop(inner);
                    let _ = tx_clone.send(AsyncCompletion::Error(Arc::new(std::io::Error::other(
                        "slot node removed during compute",
                    ))));
                    return;
                }
            };
            if current_revision != spawn_revision {
                inner.recycle_deps(deps_for_extract, deps);
                return;
            }
            AsyncContext::update_dependencies(&mut inner, slot_id, &deps);
            inner.recycle_deps(deps_for_extract, deps);
            if let Some(AsyncNode::Computed(slot)) = inner.get_node_mut(slot_id) {
                slot.transition_to_resolved(spawn_revision, result.clone());
            }
        }
        let _ = tx_clone.send(AsyncCompletion::Resolved(result));
    });

    if let Some(AsyncNode::Computed(slot)) = inner.get_node_mut(slot_id) {
        slot.transition_to_computing(join_handle);
        slot.notifier = Some(tx);
    }

    rx
}

impl Default for AsyncContext {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncContext {
    pub fn new() -> Self {
        let context_id = AsyncContextId(NEXT_ASYNC_CONTEXT_ID.fetch_add(1, Ordering::Relaxed));
        Self {
            inner: Arc::new(Mutex::new(AsyncContextInner {
                nodes: Vec::new(),
                generations: Vec::new(),
                next_id: 0,
                free_ids: Vec::new(),
                context_id,
                batch_depth: 0,
                batched_cells: HashSet::new(),
                pending_async_effects: Vec::new(),
                scheduled_async_effects: HashSet::new(),
                deps_pool: Vec::new(),
            })),
            #[cfg(feature = "instrumentation")]
            window1_hook: Mutex::new(None),
            #[cfg(feature = "instrumentation")]
            window1_resolved_hits: AtomicU64::new(0),
        }
    }

    /// Install a one-shot async seam fired inside `get_async`'s window-1 gap.
    /// Used by tests to deterministically resolve a slot in the window between
    /// the fast-path `get()` check and the re-lock, forcing the `#k03k`
    /// `Resolved`-after-re-lock arm. Test-only (`instrumentation` feature).
    #[cfg(feature = "instrumentation")]
    pub fn __install_window1_hook(&self, hook: Window1Hook) {
        *self.window1_hook.lock() = Some(hook);
    }

    /// Number of `get_async` returns that went through the window-1
    /// `Resolved`-after-re-lock arm. Test-only (`instrumentation` feature).
    #[cfg(feature = "instrumentation")]
    pub fn __window1_resolved_hits(&self) -> u64 {
        self.window1_resolved_hits.load(Ordering::Relaxed)
    }

    /// Create a mutable async source.
    pub fn source<T>(&self, value: T) -> AsyncSource<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        let id;
        {
            let mut inner = self.inner.lock();
            id = inner.alloc_id();
            let node = AsyncSourceNode {
                value: Arc::new(value),
                dependents: EdgeVec::new(),
            };
            inner.insert_node(id, AsyncNode::Source(node));
        }
        AsyncSource {
            id,
            _marker: PhantomData,
        }
    }

    /// Compatibility constructor for the pre-Cell-kernel API.
    #[deprecated(note = "use `AsyncContext::source`")]
    pub fn cell<T>(&self, value: T) -> AsyncSource<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        self.source(value)
    }

    #[deprecated(note = "use `AsyncContext::get` — the unified cell read (#lzcellkernel)")]
    pub fn get_cell<T>(&self, handle: &AsyncSource<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        self.get(handle)
    }

    fn read_source<T>(&self, handle: &AsyncSource<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        let inner = self.inner.lock();
        match inner.get_node(handle.id) {
            Some(AsyncNode::Source(cell)) => cell
                .value
                .as_ref()
                .downcast_ref::<T>()
                .expect("type mismatch in AsyncContext::get")
                .clone(),
            _ => panic!("AsyncSource does not point to a Cell node"),
        }
    }

    #[deprecated(note = "use `AsyncContext::set` — the unified cell write (#lzcellkernel)")]
    pub fn set_cell<T>(&self, handle: &AsyncSource<T>, value: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        self.write_source(handle, value)
    }

    fn write_source<T>(&self, handle: &AsyncSource<T>, value: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        let dependents;
        let is_batching;
        {
            let mut inner = self.inner.lock();
            is_batching = inner.batch_depth > 0;
            match inner.get_node_mut(handle.id) {
                Some(AsyncNode::Source(cell)) => {
                    let changed = !(*cell
                        .value
                        .as_ref()
                        .downcast_ref::<T>()
                        .expect("type mismatch in AsyncContext::set_cell")
                        == value);
                    if changed {
                        cell.value = Arc::new(value);
                        if is_batching {
                            inner.batched_cells.insert(handle.id);
                            return;
                        }
                        dependents = cell.dependents.clone();
                    } else {
                        return;
                    }
                }
                _ => panic!("AsyncSource does not point to a Cell node"),
            }
        }
        self.invalidate_frontier_async(&dependents);
    }

    /// Fold `op` into a cell's value under policy `M` (the merge write), the
    /// async port of [`Context::apply_merge`] (design §9.1).
    ///
    /// **Merge is synchronous even here.** Cells are the synchronous input layer
    /// (`cell` / `get_cell` / `set_cell` are sync; only computed slots and
    /// effects are async), so `apply_merge` is entirely synchronous cell ops:
    /// read the current value untracked, fold `M::merge(old, op)`, then route
    /// through [`set_cell`](Self::set_cell). §7's "merge folds synchronously
    /// inside a batch" holds verbatim; only the timing of a *feeding effect*
    /// differs, and that lives in the effect, not the fold.
    pub fn apply_merge<T, M>(&self, handle: &AsyncSource<T>, op: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        M: MergePolicy<T>,
    {
        let old = self.get(handle);
        let merged = M::merge(&old, op);
        self.set(handle, merged);
    }

    /// Whether an async computed cell currently holds a cached, resolved value.
    ///
    /// The async analog of `Context::is_set` / `ThreadSafeContext::is_set`.
    /// Invalidation drops the cell out of `Resolved`, so this is the probe the
    /// reader-class independence fixtures use to tell "recomputed" from "stayed
    /// cached" on this flavor.
    pub fn is_set<T>(&self, handle: &AsyncComputed<T>) -> bool
    where
        T: Send + Sync + 'static,
    {
        matches!(self.get_slot_state(handle.id), AsyncSlotStateView::Resolved)
    }

    pub(crate) fn get_slot_state(&self, id: SlotId) -> AsyncSlotStateView {
        let inner = self.inner.lock();
        match inner.get_node(id) {
            Some(AsyncNode::Computed(slot)) => match &slot.state {
                AsyncSlotState::Empty => AsyncSlotStateView::Empty,
                AsyncSlotState::Computing { revision, .. } => AsyncSlotStateView::Computing {
                    revision: *revision,
                },
                AsyncSlotState::Resolved => AsyncSlotStateView::Resolved,
                AsyncSlotState::Error => AsyncSlotStateView::Error,
            },
            _ => AsyncSlotStateView::None,
        }
    }

    pub(crate) fn get_slot_revision(&self, id: SlotId) -> Option<u64> {
        let inner = self.inner.lock();
        match inner.get_node(id) {
            Some(AsyncNode::Computed(slot)) => Some(slot.revision),
            _ => None,
        }
    }

    pub(crate) fn register_dependency(&self, dependency_id: SlotId, dependent_id: SlotId) {
        let mut inner = self.inner.lock();
        register_dependency_locked(&mut inner, dependency_id, dependent_id);
    }

    fn update_dependencies(
        inner: &mut AsyncContextInner,
        node_id: SlotId,
        new_deps: &HashSet<SlotId>,
    ) {
        let old_deps = match inner.get_node(node_id) {
            Some(AsyncNode::Computed(s)) => s.dependencies.iter().copied().collect::<HashSet<_>>(),
            _ => return,
        };
        for old_id in old_deps.difference(new_deps) {
            if let Some(AsyncNode::Computed(s)) = inner.get_node_mut(*old_id) {
                edge_remove(&mut s.dependents, node_id);
            }
            if let Some(AsyncNode::Source(c)) = inner.get_node_mut(*old_id) {
                edge_remove(&mut c.dependents, node_id);
            }
        }
        if let Some(AsyncNode::Computed(s)) = inner.get_node_mut(node_id) {
            s.dependencies = new_deps.iter().copied().collect();
        }
        for new_id in new_deps {
            if let Some(AsyncNode::Computed(s)) = inner.get_node_mut(*new_id) {
                edge_insert(&mut s.dependents, node_id);
            }
            if let Some(AsyncNode::Source(c)) = inner.get_node_mut(*new_id) {
                edge_insert(&mut c.dependents, node_id);
            }
        }
    }

    /// Single-locked invalidation frontier (#lzasyncfrontier). Walks the entire
    /// dependent cone of `roots` under ONE mutex acquisition — collecting slots
    /// to invalidate, their superseded notifiers / in-flight compute handles,
    /// and the effects to schedule — then releases the lock and performs the
    /// abort / notifier-drop / effect-schedule / flush work outside it. Mirrors
    /// the thread-safe variant's `ThreadSafeInvalidationPlan::from_roots_locked`
    /// → `apply_locked` split (thread_safe.rs).
    ///
    /// The recursive predecessor re-locked the mutex per dependent and per
    /// scheduled effect; a fan-out-N invalidation paid O(N) lock acquisitions.
    /// Notifier drops and in-flight aborts still happen after the lock is
    /// released, preserving the `#k03k` resolve-window semantics (a superseded
    /// compute's `watch` senders drop without a final `Resolved` send, so a
    /// waiting `get_async` re-resolves to the latest value instead of panicking).
    fn invalidate_frontier_async(&self, roots: &[SlotId]) {
        let (effects, in_flight_handles, notifiers) = {
            let mut inner = self.inner.lock();
            // SmallVec-backed scratch so the common small-cone (1-2 dependents)
            // case never heap-allocates; deep cones spill transparently.
            let mut stack: smallvec::SmallVec<[SlotId; 16]> = smallvec::SmallVec::new();
            stack.extend_from_slice(roots);
            let mut effects: EdgeVec = EdgeVec::new();
            let mut in_flight_handles: smallvec::SmallVec<[JoinHandle<()>; 4]> =
                smallvec::SmallVec::new();
            let mut notifiers: smallvec::SmallVec<[watch::Sender<AsyncCompletion>; 4]> =
                smallvec::SmallVec::new();
            // Visited set (`#lzasynccycle`). The async graph has no
            // edge-registration cycle guard: `AsyncComputeContext::get_async`
            // records the dependency edge synchronously, in its non-async
            // prelude, before the returned future is awaited. A compute can
            // therefore declare a dependency it never awaits, making `A -> B ->
            // A` constructible without either compute diverging — and this walk
            // would then push dependents forever WHILE HOLDING `inner.lock()`,
            // wedging the whole context rather than merely spinning a task.
            //
            // Draining each `dependents` set on visit (the lazily-dart / -go
            // pattern) does not fit here: rs exposes `dependent_count` on the
            // `ReactiveGraph` trait and the three-context conformance runner
            // compares it across basic/thread_safe/async. The sync walk
            // (`mark_frontier_locked`) leaves reverse edges intact and
            // terminates on saturated state instead, so draining would make
            // async's reverse-edge degree diverge from its siblings after any
            // invalidation.
            //
            // Marking on visit is monotone: an id is traversed at most once, so
            // the walk is bounded by the number of live nodes.
            let mut visited: EdgeVec = EdgeVec::new();
            while let Some(id) = stack.pop() {
                if !edge_insert(&mut visited, id) {
                    continue;
                }
                match inner.get_node_mut(id) {
                    Some(AsyncNode::Computed(slot)) => {
                        if let InvalidationResult::HadInFlight(handle) = slot.invalidate() {
                            in_flight_handles.push(handle);
                        }
                        if let Some(notifier) = slot.notifier.take() {
                            notifiers.push(notifier);
                        }
                        let dependents = slot.dependents.clone();
                        for dep_id in &dependents {
                            match inner.get_node(*dep_id) {
                                Some(AsyncNode::Effect(_)) => effects.push(*dep_id),
                                _ => stack.push(*dep_id),
                            }
                        }
                    }
                    Some(AsyncNode::Effect(_)) => {
                        effects.push(id);
                    }
                    _ => {}
                }
            }
            (effects, in_flight_handles, notifiers)
        };
        for handle in in_flight_handles {
            handle.abort();
        }
        drop(notifiers);
        let has_effects = !effects.is_empty();
        if has_effects {
            let mut inner = self.inner.lock();
            for effect_id in &effects {
                Self::schedule_async_effect(&mut inner, *effect_id);
            }
        }
        if has_effects {
            self.flush_async_effects();
        }
    }

    /// Evaluate `f` once, **detached** from any recompute: reads through the
    /// supplied [`AsyncComputeContext`] register no dependency edge, because it
    /// is bound to the `SlotId::DETACHED` sentinel, which resolves to no node.
    ///
    /// The async analog of `Context::eval_detached`. Used to produce a value
    /// under the unified view-taking closure type when there is no owning node
    /// yet — e.g. a source cell's seed value in the keyed-map family, which is
    /// evaluated once and is not a dependency edge.
    pub(crate) fn eval_detached<T>(&self, f: impl FnOnce(&AsyncComputeContext) -> T) -> T {
        let context_id = self.inner.lock().context_id;
        let ctx = AsyncComputeContext {
            _context_id: context_id,
            _node_id: SlotId::DETACHED,
            _node_gen: 0,
            inner: self.inner.clone(),
            dependencies: Arc::new(Mutex::new(HashSet::new())),
        };
        f(&ctx)
    }
    /// Create a **guarded async computed cell** (`#lzcellkernel`): an equal
    /// recompute suppresses downstream invalidation, so `T: PartialEq`. This is
    /// the primary async derived constructor; the former `memo_async`
    /// constructor is retired because `computed_async` now *is* the guarded form.
    pub fn computed_async<T, F, Fut>(&self, compute: F) -> AsyncComputed<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let equals: Arc<AsyncEqualsFn> = Arc::new(|old: &AsyncAny, new: &AsyncAny| -> bool {
            let old_val = old.downcast_ref::<T>();
            let new_val = new.downcast_ref::<T>();
            match (old_val, new_val) {
                (Some(o), Some(n)) => o == n,
                _ => false,
            }
        });
        self.slot_async_with_equals(compute, Some(equals))
    }

    fn slot_async_with_equals<T, F, Fut>(
        &self,
        compute: F,
        equals: Option<Arc<AsyncEqualsFn>>,
    ) -> AsyncComputed<T>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let compute_arc: Arc<AsyncComputeFn> = Arc::new(move |ctx| {
            let fut = compute(ctx);
            Box::pin(async move { Arc::new(fut.await) as Arc<AsyncAny> })
        });
        let id;
        {
            let mut inner = self.inner.lock();
            id = inner.alloc_id();
            let node = AsyncComputedNode {
                state: AsyncSlotState::Empty,
                value: None,
                error: None,
                revision: 0,
                compute: compute_arc,
                sync_compute: None,
                equals,
                dependencies: EdgeVec::new(),
                dependents: EdgeVec::new(),
                notifier: None,
            };
            inner.insert_node(id, AsyncNode::Computed(node));
        }
        AsyncComputed {
            id,
            _marker: PhantomData,
        }
    }

    /// Read the current value of a cell (`#lzcellkernel`). A [`Computed`]
    /// ([`AsyncComputed`]) yields `Option<T>` (`None` while pending); a
    /// [`Source`] ([`AsyncSource`]) yields `T`. Unified read superseding the
    /// deprecated `get_cell`.
    pub fn get<H: Read<Self>>(&self, handle: &H) -> <H as Read<Self>>::Output {
        handle.read(self)
    }

    /// Write a new value to a source cell ([`AsyncSource`], `#lzcellkernel`).
    /// Unified write superseding the deprecated `set_cell`.
    pub fn set<H: Write<Self>>(&self, handle: &H, value: <H as Write<Self>>::Value) {
        handle.write(self, value)
    }

    fn read_slot<T>(&self, handle: &AsyncComputed<T>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        // Fast path plus, for a synchronous compute, the closure to run. The
        // closure is deliberately NOT called while `inner` is held: it is user
        // code that reads sources, and those reads re-lock `inner`. Running it
        // under the lock would deadlock the context on its own reader.
        let (resolved, sync_compute, node_gen) = {
            let inner = self.inner.lock();
            match inner.get_node(handle.id) {
                Some(AsyncNode::Computed(slot)) => match &slot.state {
                    AsyncSlotState::Resolved => {
                        let val = slot.value.as_ref().expect("resolved without value");
                        (
                            Some(
                                val.downcast_ref::<T>()
                                    .expect("type mismatch in get")
                                    .clone(),
                            ),
                            None,
                            0,
                        )
                    }
                    _ => (None, slot.sync_compute.clone(), inner.generation(handle.id)),
                },
                _ => (None, None, 0),
            }
        };
        if let Some(value) = resolved {
            return Some(value);
        }
        let sync_compute = sync_compute?;

        // Lock released.
        let cx = AsyncComputeContext {
            _context_id: self.inner.lock().context_id,
            _node_id: handle.id,
            _node_gen: node_gen,
            inner: Arc::clone(&self.inner),
            dependencies: Arc::new(Mutex::new(HashSet::new())),
        };
        let value = sync_compute(&cx);
        let deps: EdgeVec = cx.dependencies.lock().iter().copied().collect();

        let mut inner = self.inner.lock();
        match inner.get_node_mut(handle.id) {
            Some(AsyncNode::Computed(slot)) => {
                // Re-check: a concurrent reader may have resolved it first. Either
                // value is correct (same pure compute), so keep the stored one
                // rather than racing to overwrite.
                if !matches!(slot.state, AsyncSlotState::Resolved) {
                    slot.value = Some(value);
                    slot.state = AsyncSlotState::Resolved;
                    slot.dependencies = deps;
                }
                let val = slot.value.as_ref().expect("resolved without value");
                Some(
                    val.downcast_ref::<T>()
                        .expect("type mismatch in get")
                        .clone(),
                )
            }
            _ => None,
        }
    }

    /// A derived cell whose compute is **synchronous** (`#lzqfrs`).
    ///
    /// `computed_async` resolves through a spawned task, so a read before
    /// resolution yields `None`. That is right for a compute that awaits, and
    /// wrong for one that cannot: a reader kind derived from out-of-graph state
    /// (a queue's `len`, `head`, `is_full`) has nothing to await, and routing it
    /// through the task machinery would make every such read return `Option` and
    /// need a settle step for no reason.
    ///
    /// These resolve **inline on read**, so `get` returns `Some` immediately.
    /// Invalidation is identical to any other slot — [`clear`](Self::clear) drops
    /// the memo and the next read re-derives.
    pub fn computed<T, F>(&self, compute: F) -> AsyncComputed<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        F: Fn(&AsyncComputeContext) -> T + Send + Sync + 'static,
    {
        let sync_compute: Arc<AsyncSyncComputeFn> =
            Arc::new(move |cx: &AsyncComputeContext| Arc::new(compute(cx)) as Arc<AsyncAny>);
        let equals: Arc<AsyncEqualsFn> = Arc::new(|old: &AsyncAny, new: &AsyncAny| -> bool {
            match (old.downcast_ref::<T>(), new.downcast_ref::<T>()) {
                (Some(o), Some(n)) => o == n,
                _ => false,
            }
        });
        // The async compute is never invoked for these; it exists so the node
        // shape stays uniform. Constructing it is cheap and keeps every other
        // code path (dispose, invalidate, edge walks) unchanged.
        let unused: Arc<AsyncComputeFn> =
            Arc::new(|_| Box::pin(async { Arc::new(()) as Arc<AsyncAny> }));
        let id;
        {
            let mut inner = self.inner.lock();
            id = inner.alloc_id();
            let node = AsyncComputedNode {
                state: AsyncSlotState::Empty,
                value: None,
                error: None,
                revision: 0,
                compute: unused,
                sync_compute: Some(sync_compute),
                equals: Some(equals),
                dependencies: EdgeVec::new(),
                dependents: EdgeVec::new(),
                notifier: None,
            };
            inner.insert_node(id, AsyncNode::Computed(node));
        }
        AsyncComputed {
            id,
            _marker: PhantomData,
        }
    }

    /// Drop a derived cell's memo and that of everything downstream, so the next
    /// read re-derives (`#lzqfrs`).
    pub fn clear<T>(&self, handle: &AsyncComputed<T>) {
        self.clear_slots(&[handle.id]);
    }

    /// Multi-root [`clear`](Self::clear): every root and its cone invalidated in
    /// ONE frontier walk.
    ///
    /// This matters for anything with several reader kinds that transition
    /// together. Clearing them one at a time lets a subscriber observe a partial
    /// state — a queue's `len` decremented while `is_full` still reads stale.
    /// `invalidate_frontier_async` already accepted a slice of roots; the async
    /// context simply never exposed a way to hand it more than one.
    pub fn clear_slots(&self, ids: &[SlotId]) {
        if ids.is_empty() {
            return;
        }
        self.invalidate_frontier_async(ids);
    }

    pub async fn get_async<T>(&self, handle: &AsyncComputed<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        // Outer loop re-resolves from authoritative slot state. Two concurrency
        // windows make a single straight-line pass insufficient (#k03k):
        //   1. The slot can transition `Computing -> Resolved` between the
        //      `get()` fast-path check (which drops the lock) and the re-lock
        //      below — so `Resolved` is reachable here and must be read, not
        //      treated as unreachable.
        //   2. The notifier's `watch` senders can all drop without a final
        //      `Resolved` send when an in-flight compute is superseded by a
        //      newer revision (`spawn_revision` mismatch -> early return) or the
        //      slot is invalidated. A dropped notifier therefore means "the
        //      world changed", not a fatal error: restart and re-observe.
        loop {
            // Fast path: value already published.
            if let Some(val) = self.get(handle) {
                return val;
            }

            // Test-only seam (compiled out of default/release builds): fire a
            // one-shot hook in the window-1 gap so a test can resolve the slot
            // between the fast-path check above and the re-lock below,
            // deterministically reaching the `#k03k` Resolved arm.
            #[cfg(feature = "instrumentation")]
            {
                let hook = self.window1_hook.lock().take();
                if let Some(hook) = hook {
                    hook().await;
                }
            }

            let mut recv = {
                let inner = self.inner.lock();
                match inner.get_node(handle.id) {
                    Some(AsyncNode::Computed(slot)) => match &slot.state {
                        AsyncSlotState::Computing { .. } => slot
                            .notifier
                            .as_ref()
                            .expect("computing without notifier")
                            .subscribe(),
                        AsyncSlotState::Error | AsyncSlotState::Empty => {
                            drop(inner);
                            spawn_async_compute(self, handle.id)
                        }
                        AsyncSlotState::Resolved => {
                            // Window (1): resolved since the `get()` check.
                            #[cfg(feature = "instrumentation")]
                            self.window1_resolved_hits.fetch_add(1, Ordering::Relaxed);
                            let val = slot.value.as_ref().expect("resolved without value");
                            return val
                                .downcast_ref::<T>()
                                .expect("type mismatch in get_async")
                                .clone();
                        }
                    },
                    _ => panic!("AsyncComputed does not point to a Slot node"),
                }
            };

            'await_completion: loop {
                if recv.changed().await.is_err() {
                    // Window (2): notifier dropped (compute superseded or slot
                    // invalidated). Re-resolve from current slot state instead
                    // of panicking.
                    break 'await_completion;
                }
                let completion = recv.borrow_and_update().clone();
                match completion {
                    AsyncCompletion::Resolved(val) => {
                        return val
                            .downcast_ref::<T>()
                            .expect("type mismatch in get_async completion")
                            .clone();
                    }
                    AsyncCompletion::Error(_err) => {
                        recv = spawn_async_compute(self, handle.id);
                        continue 'await_completion;
                    }
                    AsyncCompletion::Pending => continue 'await_completion,
                }
            }
        }
    }

    pub fn batch<F, R>(&self, run: F) -> R
    where
        F: FnOnce(&AsyncContext) -> R,
    {
        {
            let mut inner = self.inner.lock();
            inner.batch_depth += 1;
        }
        let result = run(self);
        {
            let mut inner = self.inner.lock();
            inner.batch_depth -= 1;
            if inner.batch_depth == 0 {
                let batched = inner.batched_cells.drain().collect::<Vec<_>>();
                drop(inner);
                // Batch ALL batched cells' dependents into a single frontier
                // walk under one lock (#lzasyncfrontier), instead of re-locking
                // per cell / per dependent.
                let mut roots: EdgeVec = EdgeVec::new();
                for cell_id in &batched {
                    let inner = self.inner.lock();
                    if let Some(AsyncNode::Source(c)) = inner.get_node(*cell_id) {
                        roots.extend_from_slice(&c.dependents);
                    }
                }
                if !roots.is_empty() {
                    self.invalidate_frontier_async(&roots);
                }
            }
        }
        result
    }

    pub fn effect_async<F, Fut, C>(&self, effect: F) -> AsyncEffectHandle
    where
        F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<C>> + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        let id;
        {
            let mut inner = self.inner.lock();
            id = inner.alloc_id();
            let effect_fn: Arc<AsyncEffectFn> = Arc::new(move |ctx| {
                let fut = effect(ctx);
                Box::pin(async move { fut.await.map(|c| Box::new(c) as BoxedCleanupFn) })
            });
            let node = AsyncEffectNode {
                effect_fn,
                cleanup: None,
                dependencies: EdgeVec::new(),
                dependents: EdgeVec::new(),
                force_run: true,
                in_flight: None,
            };
            inner.insert_node(id, AsyncNode::Effect(node));
            Self::schedule_async_effect(&mut inner, id);
        }
        let handle = AsyncEffectHandle { id };
        self.flush_async_effects();
        handle
    }

    /// How many nodes currently depend on `node` — the size of its reverse edge
    /// set (`#lzspecedgeindex`).
    pub fn dependent_count(&self, node: &impl GraphNode) -> usize {
        let inner = self.inner.lock();
        match inner.get_node(node.node_id()) {
            Some(AsyncNode::Computed(slot)) => slot.dependents.len(),
            Some(AsyncNode::Source(cell)) => cell.dependents.len(),
            // Effects are pure sinks: nothing can read one.
            Some(AsyncNode::Effect(_)) | None => 0,
        }
    }

    /// How many nodes `node` currently depends on — the size of its forward edge
    /// set (`#lzspecedgeindex`).
    pub fn dependency_count(&self, node: &impl GraphNode) -> usize {
        let inner = self.inner.lock();
        match inner.get_node(node.node_id()) {
            Some(AsyncNode::Computed(slot)) => slot.dependencies.len(),
            Some(AsyncNode::Effect(effect)) => effect.dependencies.len(),
            // Cells are pure sources.
            Some(AsyncNode::Source(_)) | None => 0,
        }
    }

    /// Check whether an async effect is still registered.
    pub fn is_async_effect_active(&self, handle: &AsyncEffectHandle) -> bool {
        let inner = self.inner.lock();
        matches!(inner.get_node(handle.id), Some(AsyncNode::Effect(_)))
    }

    /// Detach `id` from both edge directions of everything it touches.
    fn detach_edges_locked(inner: &mut AsyncContextInner, id: SlotId, node: &AsyncNode) {
        let (dependencies, dependents) = match node {
            AsyncNode::Computed(s) => (s.dependencies.clone(), s.dependents.clone()),
            AsyncNode::Source(c) => (EdgeVec::new(), c.dependents.clone()),
            AsyncNode::Effect(e) => (e.dependencies.clone(), EdgeVec::new()),
        };
        // Upstream: the sources no longer list this node as a dependent.
        for dep_id in &dependencies {
            match inner.get_node_mut(*dep_id) {
                Some(AsyncNode::Computed(s)) => {
                    edge_remove(&mut s.dependents, id);
                }
                Some(AsyncNode::Source(c)) => {
                    edge_remove(&mut c.dependents, id);
                }
                _ => {}
            }
        }
        // Downstream: the readers no longer list this node as a dependency.
        for dependent_id in &dependents {
            match inner.get_node_mut(*dependent_id) {
                Some(AsyncNode::Computed(s)) => {
                    edge_remove(&mut s.dependencies, id);
                }
                Some(AsyncNode::Effect(e)) => {
                    edge_remove(&mut e.dependencies, id);
                }
                _ => {}
            }
        }
    }

    /// Bump the generation for `id` and recycle it (`#lzasyncdispose2`).
    ///
    /// The bump must happen BEFORE the id enters the free list, so a task still
    /// in-flight for the disposed node fails its generation re-check and cannot
    /// write into whatever node a later `alloc_id` puts at this index.
    fn retire_id_locked(inner: &mut AsyncContextInner, id: SlotId) {
        let Some(idx) = AsyncContextInner::node_index(id) else {
            return;
        };
        if idx < inner.nodes.len() {
            inner.nodes[idx] = None;
        }
        if idx < inner.generations.len() {
            inner.generations[idx] += 1;
        }
        inner.free_ids.push(id.0);
    }

    /// Invalidate the cone that read a node being disposed, without scheduling
    /// anything (`#lzspecedgeindex`).
    ///
    /// The same reasoning as the sync contexts: detaching the edges is not
    /// enough, because a dependent holding a resolved value would keep serving
    /// it once its dependency edge is gone. In-flight recomputes in the cone are
    /// aborted — a computation reading a node that is being torn down has no
    /// result worth keeping. Effects are deliberately NOT scheduled: disposal is
    /// not a publish, and running one here would re-enter a compute over a node
    /// currently being disposed.
    fn invalidate_disposed_dependents(&self, roots: &[SlotId]) {
        if roots.is_empty() {
            return;
        }
        let (in_flight_handles, notifiers) = {
            let mut inner = self.inner.lock();
            let mut stack: smallvec::SmallVec<[SlotId; 16]> = smallvec::SmallVec::new();
            stack.extend_from_slice(roots);
            let mut in_flight_handles: smallvec::SmallVec<[JoinHandle<()>; 4]> =
                smallvec::SmallVec::new();
            let mut notifiers: smallvec::SmallVec<[watch::Sender<AsyncCompletion>; 4]> =
                smallvec::SmallVec::new();
            // Same cycle exposure as `invalidate_frontier_async` — a disposal
            // cone is walked with the identical unconditional push
            // (`#lzasynccycle`). See that function for why a visited set rather
            // than draining the reverse edges.
            let mut visited: EdgeVec = EdgeVec::new();
            while let Some(id) = stack.pop() {
                if !edge_insert(&mut visited, id) {
                    continue;
                }
                if let Some(AsyncNode::Computed(slot)) = inner.get_node_mut(id) {
                    if let InvalidationResult::HadInFlight(handle) = slot.invalidate() {
                        in_flight_handles.push(handle);
                    }
                    if let Some(notifier) = slot.notifier.take() {
                        notifiers.push(notifier);
                    }
                    let dependents = slot.dependents.clone();
                    for dep_id in &dependents {
                        // Effects in the cone are left unscheduled on purpose.
                        if !matches!(inner.get_node(*dep_id), Some(AsyncNode::Effect(_))) {
                            stack.push(*dep_id);
                        }
                    }
                }
            }
            (in_flight_handles, notifiers)
        };
        for handle in in_flight_handles {
            handle.abort();
        }
        drop(notifiers);
    }

    /// Tear down an async derived slot: cancel any in-flight recompute, detach
    /// both edge directions, invalidate the surviving readers, and recycle the
    /// id behind a generation bump.
    ///
    /// # In-flight computations
    ///
    /// A slot disposed mid-`await` has its computation **cancelled, not allowed
    /// to finish**: the `JoinHandle` is aborted and any result it would have
    /// produced is discarded. This follows
    /// [`Self::dispose_async_effect`]'s established precedent
    /// (`#lzasyncdispose2`) rather than inventing a second convention — the
    /// generation counter is bumped before the id is recycled, so even a task
    /// that wins the race against `abort` fails its generation re-check and
    /// cannot write into whatever node later reuses the id.
    pub fn dispose_slot<T>(&self, handle: &AsyncComputed<T>) {
        let (in_flight, notifier, stale) = {
            let mut inner = self.inner.lock();
            // Check the kind BEFORE removing: a stale handle whose id has been
            // recycled must not tear down whatever now owns it.
            if !matches!(inner.get_node(handle.id), Some(AsyncNode::Computed(_))) {
                return;
            }
            let Some(idx) = AsyncContextInner::node_index(handle.id) else {
                return;
            };
            let Some(node) = inner.nodes[idx].take() else {
                return;
            };
            Self::detach_edges_locked(&mut inner, handle.id, &node);
            let AsyncNode::Computed(mut slot) = node else {
                return;
            };
            let stale = slot.dependents.clone();
            let in_flight = slot.clear();
            let notifier = slot.notifier.take();
            Self::retire_id_locked(&mut inner, handle.id);
            (in_flight, notifier, stale)
        };
        if let Some(in_flight) = in_flight {
            in_flight.abort();
        }
        drop(notifier);
        self.invalidate_disposed_dependents(&stale);
    }

    /// Tear down an async source cell: detach its dependents, invalidate them,
    /// and recycle the id behind a generation bump.
    ///
    /// Cells are pure sources with no dependencies and no in-flight state, so
    /// only downstream edges need detaching.
    pub fn dispose_cell<T>(&self, handle: &AsyncSource<T>) {
        let stale = {
            let mut inner = self.inner.lock();
            if !matches!(inner.get_node(handle.id), Some(AsyncNode::Source(_))) {
                return;
            }
            let Some(idx) = AsyncContextInner::node_index(handle.id) else {
                return;
            };
            let Some(node) = inner.nodes[idx].take() else {
                return;
            };
            Self::detach_edges_locked(&mut inner, handle.id, &node);
            let AsyncNode::Source(cell) = node else {
                return;
            };
            let stale = cell.dependents.clone();
            Self::retire_id_locked(&mut inner, handle.id);
            stale
        };
        self.invalidate_disposed_dependents(&stale);
    }

    /// Tear down whatever node `id` names, dispatching on its own kind.
    fn dispose_id(&self, id: SlotId) {
        let kind = match self.inner.lock().get_node(id) {
            Some(AsyncNode::Computed(_)) => 0u8,
            Some(AsyncNode::Source(_)) => 1,
            Some(AsyncNode::Effect(_)) => 2,
            None => return,
        };
        let marker = std::marker::PhantomData;
        match kind {
            0 => self.dispose_slot(&AsyncComputed::<()> {
                id,
                _marker: marker,
            }),
            1 => self.dispose_cell(&AsyncSource::<()> {
                id,
                _marker: marker,
            }),
            _ => self.dispose_async_effect(&AsyncEffectHandle { id }),
        }
    }

    /// A second handle onto the same graph.
    ///
    /// Deliberately not a `Clone` impl: under `instrumentation` an
    /// `AsyncContext` also carries a one-shot test seam (`window1_hook`) that is
    /// *not* shared, so `clone()` would silently mean two different things
    /// depending on the feature set. This says exactly what it does — same
    /// graph, fresh seam.
    pub(crate) fn handle(&self) -> AsyncContext {
        AsyncContext {
            inner: Arc::clone(&self.inner),
            #[cfg(feature = "instrumentation")]
            window1_hook: Mutex::new(None),
            #[cfg(feature = "instrumentation")]
            window1_resolved_hits: AtomicU64::new(0),
        }
    }

    /// Open a teardown scope: nodes created through it are disposed when it
    /// drops.
    ///
    /// Like [`ThreadSafeContext::scope`](crate::ThreadSafeContext::scope) and
    /// unlike [`Context::scope`](crate::Context::scope), the scope holds an
    /// owned handle rather than a borrow: `AsyncContext` is already an `Arc`
    /// over shared state, so owning one costs a refcount bump and gives the
    /// scope a `'static`, `Send` life — which is what a per-request scope on a
    /// spawned task needs. The three scope types differ because the ownership
    /// models differ; please do not unify them.
    pub fn scope(&self) -> AsyncTeardownScope {
        AsyncTeardownScope {
            ctx: self.handle(),
            owned: Mutex::new(Vec::new()),
        }
    }

    pub fn dispose_async_effect(&self, handle: &AsyncEffectHandle) {
        let (cleanup, in_flight) = {
            let mut inner = self.inner.lock();
            inner.pending_async_effects.retain(|&id| id != handle.id);
            inner.scheduled_async_effects.remove(&handle.id);
            let (cleanup, in_flight) = match inner.get_node_mut(handle.id) {
                Some(AsyncNode::Effect(e)) => {
                    let deps = e.dependencies.clone();
                    let prior_cleanup = e.cleanup.take();
                    let prior_in_flight = e.in_flight.take();
                    for dep_id in &deps {
                        match inner.get_node_mut(*dep_id) {
                            Some(AsyncNode::Computed(s)) => {
                                edge_remove(&mut s.dependents, handle.id);
                            }
                            Some(AsyncNode::Source(c)) => {
                                edge_remove(&mut c.dependents, handle.id);
                            }
                            _ => {}
                        }
                    }
                    (prior_cleanup, prior_in_flight)
                }
                _ => return,
            };
            let index = usize::try_from(handle.id.0).ok();
            if let Some(idx) = index
                && idx < inner.nodes.len()
            {
                inner.nodes[idx] = None;
                // Bump the generation BEFORE recycling the id so any task still
                // in-flight for this effect fails its generation re-check and
                // cannot write into the node a future `alloc_id` reuses here
                // (#lzasyncdispose2).
                if idx < inner.generations.len() {
                    inner.generations[idx] += 1;
                }
                inner.free_ids.push(handle.id.0);
            }
            (cleanup, in_flight)
        };
        if let Some(in_flight) = in_flight {
            in_flight.abort();
        }
        if let Some(cleanup) = cleanup {
            cleanup();
        }
    }

    // -- Signal API --------------------------------------------------------

    /// Create an **eager** async derived value that drives its recomputation to
    /// completion the instant one of its dependencies is invalidated.
    ///
    /// This is the [`AsyncContext`] counterpart to [`Context::signal`]. Where
    /// [`computed_async`](Self::computed_async) is lazy (re-resolved on the next
    /// `get_async`), a signal is eager: a
    /// puller effect awaits the backing slot after every invalidation, so by the
    /// time the spawned recompute finishes the signal already holds its new
    /// value without anyone reading it.
    ///
    /// The signal is backed by a memoized slot, so a recomputation that yields
    /// an equal value (via `PartialEq`) does not invalidate downstream
    /// dependents. Recomputation is pull-based and therefore glitch-free.
    ///
    /// Because resolution is asynchronous, eager materialization completes on
    /// the runtime rather than synchronously within the invalidating
    /// `set_cell`/`batch` call. Use [`get_signal`](Self::get_signal) for a
    /// non-blocking snapshot or [`get_signal_async`](Self::get_signal_async) to
    /// await the up-to-date value.
    pub fn signal_async<T, F, Fut>(&self, compute: F) -> AsyncSignalHandle<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let slot = self.computed_async(compute);
        // Eager puller: awaits the backing slot after every invalidation. The
        // synchronous part of `get_async` registers the slot as a dependency of
        // this effect, so a later invalidation reschedules the puller.
        let effect = self.effect_async(move |ctx: AsyncComputeContext| {
            let fut = ctx.get_async(&slot);
            async move {
                let _ = fut.await;
                None::<fn()>
            }
        });
        AsyncSignalHandle { slot, effect }
    }

    /// Read a signal's current value if it has resolved, without awaiting.
    pub fn get_signal<T: Clone + Send + Sync + 'static>(
        &self,
        handle: &AsyncSignalHandle<T>,
    ) -> Option<T> {
        self.get(&handle.slot)
    }

    /// Await a signal's current value, driving recomputation if needed.
    pub async fn get_signal_async<T: Clone + Send + Sync + 'static>(
        &self,
        handle: &AsyncSignalHandle<T>,
    ) -> T {
        self.get_async(&handle.slot).await
    }

    /// Dispose a signal's eager puller.
    ///
    /// Stops eager recomputation; the backing value remains readable and
    /// reverts to lazy (recomputed on next read) behavior.
    pub fn dispose_signal<T>(&self, handle: &AsyncSignalHandle<T>) {
        self.dispose_async_effect(&handle.effect);
    }

    /// Check whether a signal's eager puller is still active.
    pub fn is_signal_active<T>(&self, handle: &AsyncSignalHandle<T>) -> bool {
        let inner = self.inner.lock();
        matches!(inner.get_node(handle.effect.id), Some(AsyncNode::Effect(_)))
    }

    fn schedule_async_effect(inner: &mut AsyncContextInner, id: SlotId) {
        if let Some(AsyncNode::Effect(e)) = inner.get_node_mut(id) {
            e.force_run = true;
        }
        if inner.scheduled_async_effects.insert(id) {
            inner.pending_async_effects.push(id);
        }
    }

    fn flush_async_effects(&self) {
        let effect_ids: Vec<SlotId>;
        {
            let mut inner = self.inner.lock();
            effect_ids = inner.pending_async_effects.drain(..).collect();
            inner.scheduled_async_effects.clear();
        }
        let ctx_inner = self.inner.clone();
        for effect_id in effect_ids {
            let should_run = {
                let inner = self.inner.lock();
                match inner.get_node(effect_id) {
                    Some(AsyncNode::Effect(e)) => e.force_run,
                    _ => false,
                }
            };
            if !should_run {
                continue;
            }
            {
                let mut inner = self.inner.lock();
                if let Some(AsyncNode::Effect(e)) = inner.get_node_mut(effect_id) {
                    e.force_run = false;
                }
            }
            let effect_fn = {
                let inner = self.inner.lock();
                match inner.get_node(effect_id) {
                    Some(AsyncNode::Effect(e)) => Some(e.effect_fn.clone()),
                    _ => None,
                }
            };
            if let Some(fn_arc) = effect_fn {
                // Abort any prior still-running task so the re-run does not race
                // it (#lzasyncrerunabort). Pre-fix the spawn below overwrote
                // `in_flight` without aborting, so a dependency that changed again
                // mid-`.await` left the old task running concurrently — double
                // execution plus a leaked (overwritten) cleanup. The cleanup of a
                // *completed* prior run still lives in `e.cleanup` and is drained
                // inside the spawned task below; aborting only cancels an
                // unfinished `.await`, never an already-stored cleanup.
                {
                    let mut inner = self.inner.lock();
                    if let Some(AsyncNode::Effect(e)) = inner.get_node_mut(effect_id)
                        && let Some(prior) = e.in_flight.take()
                    {
                        prior.abort();
                    }
                }
                let inner_for_ctx = ctx_inner.clone();
                // Capture the generation of this effect at spawn time. Every
                // write keyed by `effect_id` below re-checks it so a run still
                // in-flight after a concurrent `dispose_async_effect` (which
                // bumps the generation and recycles the id) can never alias a
                // freshly-allocated node that reused the id (#lzasyncdispose2).
                let effect_gen = {
                    let inner = self.inner.lock();
                    inner.generation(effect_id)
                };
                let join = tokio::spawn(async move {
                    {
                        let mut inner = inner_for_ctx.lock();
                        if inner.generation(effect_id) == effect_gen
                            && let Some(AsyncNode::Effect(e)) = inner.get_node_mut(effect_id)
                            && let Some(cleanup) = e.cleanup.take()
                        {
                            drop(inner);
                            cleanup();
                        }
                    }
                    let (context_id, deps_arc) = {
                        let mut inner = inner_for_ctx.lock();
                        let deps = inner.take_deps();
                        (inner.context_id, deps)
                    };
                    let deps_for_extract = deps_arc.clone();
                    let compute_ctx = AsyncComputeContext {
                        _context_id: context_id,
                        _node_id: effect_id,
                        _node_gen: effect_gen,
                        inner: inner_for_ctx.clone(),
                        dependencies: deps_arc,
                    };
                    let cleanup = fn_arc(compute_ctx).await;
                    // See the slot path: take rather than clone (`#lzrsdeppool`).
                    let deps = std::mem::take(&mut *deps_for_extract.lock());
                    {
                        let mut inner = inner_for_ctx.lock();
                        if inner.generation(effect_id) == effect_gen {
                            AsyncContext::update_effect_dependencies(&mut inner, effect_id, &deps);
                            inner.recycle_deps(deps_for_extract, deps);
                            if let Some(AsyncNode::Effect(e)) = inner.get_node_mut(effect_id) {
                                e.cleanup = cleanup;
                            }
                        } else {
                            inner.recycle_deps(deps_for_extract, deps);
                            // The effect was disposed (and its id possibly
                            // recycled) while this run was in-flight. Never write
                            // cleanup/edges into the aliased node; instead run
                            // THIS run's own cleanup so its side effects are
                            // still undone rather than leaked (#lzasyncdispose2).
                            drop(inner);
                            if let Some(cleanup) = cleanup {
                                cleanup();
                            }
                        }
                    }
                });
                let mut inner = self.inner.lock();
                if inner.generation(effect_id) == effect_gen
                    && let Some(AsyncNode::Effect(e)) = inner.get_node_mut(effect_id)
                {
                    e.in_flight = Some(join);
                }
            }
        }
    }

    fn update_effect_dependencies(
        inner: &mut AsyncContextInner,
        effect_id: SlotId,
        new_deps: &HashSet<SlotId>,
    ) {
        let old_deps = match inner.get_node(effect_id) {
            Some(AsyncNode::Effect(e)) => e.dependencies.iter().copied().collect::<HashSet<_>>(),
            _ => return,
        };
        for old_id in old_deps.difference(new_deps) {
            match inner.get_node_mut(*old_id) {
                Some(AsyncNode::Computed(s)) => {
                    edge_remove(&mut s.dependents, effect_id);
                }
                Some(AsyncNode::Source(c)) => {
                    edge_remove(&mut c.dependents, effect_id);
                }
                _ => {}
            }
        }
        if let Some(AsyncNode::Effect(e)) = inner.get_node_mut(effect_id) {
            e.dependencies = new_deps.iter().copied().collect();
        }
        for new_id in new_deps {
            match inner.get_node_mut(*new_id) {
                Some(AsyncNode::Computed(s)) => {
                    edge_insert(&mut s.dependents, effect_id);
                }
                Some(AsyncNode::Source(c)) => {
                    edge_insert(&mut c.dependents, effect_id);
                }
                _ => {}
            }
        }
    }
}

// -- Capability trait impls (#lzspecedgeindex) -------------------------------

impl crate::reactive_graph::Teardown for AsyncTeardownScope {
    fn len(&self) -> usize {
        AsyncTeardownScope::len(self)
    }
    fn disarm(self) {
        AsyncTeardownScope::disarm(self);
    }
}

impl crate::reactive_graph::ReactiveGraph for AsyncContext {
    type Computed<T> = AsyncComputed<T>;
    type Source<T> = AsyncSource<T>;
    type Effect = AsyncEffectHandle;
    type Scope<'a> = AsyncTeardownScope;

    fn dispose_slot<T: 'static>(&self, handle: &Self::Computed<T>) {
        AsyncContext::dispose_slot(self, handle);
    }
    fn dispose_cell<T: 'static>(&self, handle: &Self::Source<T>) {
        AsyncContext::dispose_cell(self, handle);
    }
    fn dispose_effect(&self, handle: &Self::Effect) {
        AsyncContext::dispose_async_effect(self, handle);
    }
    fn scope(&self) -> Self::Scope<'_> {
        AsyncContext::scope(self)
    }
    fn batch<R>(&self, run: impl FnOnce(&Self) -> R) -> R {
        AsyncContext::batch(self, run)
    }
    fn dependent_count(&self, node: &impl GraphNode) -> usize {
        AsyncContext::dependent_count(self, node)
    }
    fn dependency_count(&self, node: &impl GraphNode) -> usize {
        AsyncContext::dependency_count(self, node)
    }
}

impl crate::reactive_graph::AsyncReactiveGraph for AsyncContext {
    fn source<T>(&self, value: T) -> Self::Source<T>
    where
        T: PartialEq + Clone + Send + Sync + 'static,
    {
        AsyncContext::source(self, value)
    }
    fn get_async<T>(&self, handle: &Self::Computed<T>) -> impl Future<Output = T> + Send
    where
        T: Clone + Send + Sync + 'static,
    {
        AsyncContext::get_async(self, handle)
    }
}

impl<T: Clone + Send + Sync + 'static> Read<AsyncContext> for AsyncComputed<T> {
    type Output = Option<T>;
    fn read(&self, ctx: &AsyncContext) -> Option<T> {
        ctx.read_slot(self)
    }
}

impl<T: Clone + Send + Sync + 'static> Read<AsyncContext> for AsyncSource<T> {
    type Output = T;
    fn read(&self, ctx: &AsyncContext) -> T {
        ctx.read_source(self)
    }
}

impl<T: PartialEq + Clone + Send + Sync + 'static> Write<AsyncContext> for AsyncSource<T> {
    type Value = T;
    fn write(&self, ctx: &AsyncContext, value: T) {
        ctx.write_source(self, value)
    }
}

impl<T: Clone + Send + Sync + 'static> Read<AsyncComputeContext> for AsyncSource<T> {
    type Output = T;
    fn read(&self, ctx: &AsyncComputeContext) -> T {
        ctx.read_source(self)
    }
}

impl<T: Clone + Send + Sync + 'static> Read<AsyncComputeContext> for AsyncComputed<T> {
    type Output = Option<T>;
    fn read(&self, ctx: &AsyncComputeContext) -> Option<T> {
        ctx.read_computed(self)
    }
}

impl<T: PartialEq + Clone + Send + Sync + 'static> Write<AsyncComputeContext> for AsyncSource<T> {
    type Value = T;
    fn write(&self, ctx: &AsyncComputeContext, value: T) {
        ctx.write_source(self, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use tokio::runtime::Runtime;

    // -- #lzspecedgeindex disposal --------------------------------------

    #[test]
    fn dispose_slot_cancels_an_in_flight_recompute_and_discards_its_result() {
        // The semantic decision: a slot disposed mid-await has its computation
        // cancelled, not allowed to finish. Follows dispose_async_effect's
        // precedent (#lzasyncdispose2).
        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();
        let ctx = AsyncContext::new();
        let ran = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicU64::new(0));

        let r = Arc::clone(&ran);
        let f = Arc::clone(&finished);
        let slow = ctx.computed_async(move |_c| {
            let r = Arc::clone(&r);
            let f = Arc::clone(&f);
            Box::pin(async move {
                r.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                f.fetch_add(1, Ordering::SeqCst);
                1i64
            }) as std::pin::Pin<Box<dyn Future<Output = i64> + Send>>
        });

        // Start the computation, then dispose while it is still awaiting.
        rt.block_on(async {
            let handle = {
                let ctx = ctx.handle();
                tokio::spawn(async move { ctx.get_async(&slow).await })
            };
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            ctx.dispose_slot(&slow);
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            handle.abort();
        });

        assert_eq!(ran.load(Ordering::SeqCst), 1, "the compute did start");
        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "an in-flight compute must be cancelled by disposal, not allowed to finish"
        );
    }

    #[test]
    fn dispose_bumps_the_generation_before_recycling_the_id() {
        // The generation bump is what stops a surviving in-flight task from
        // writing into whatever node later reuses the id.
        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i64);
        let id = cell.id;
        let before = ctx.inner.lock().generation(id);

        ctx.dispose_cell(&cell);
        let after = ctx.inner.lock().generation(id);
        assert_eq!(after, before + 1, "generation must advance on disposal");
        assert!(
            ctx.inner.lock().free_ids.contains(&id.0),
            "the id must be recycled after the bump, not before"
        );
    }

    #[test]
    fn dispose_detaches_both_directions_and_is_kind_checked() {
        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();
        let ctx = AsyncContext::new();
        let src = ctx.source(4i64);
        let derived = ctx.computed_async(move |c| {
            Box::pin(async move { c.get(&src) })
                as std::pin::Pin<Box<dyn Future<Output = i64> + Send>>
        });
        rt.block_on(ctx.get_async(&derived));
        assert_eq!(ctx.dependent_count(&src), 1);
        assert_eq!(ctx.dependency_count(&derived), 1);

        ctx.dispose_slot(&derived);
        assert_eq!(ctx.dependent_count(&src), 0);
        assert_eq!(ctx.dependency_count(&derived), 0);
        // Idempotent, and kind-checked against a recycled id.
        ctx.dispose_slot(&derived);
        let stale = AsyncSource::<i64> {
            id: derived.id,
            _marker: std::marker::PhantomData,
        };
        ctx.dispose_cell(&stale);
    }

    #[test]
    fn teardown_scope_disposes_on_drop_and_disarm_cancels() {
        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();
        let ctx = AsyncContext::new();
        let topic = ctx.source(1i64);
        {
            let scope = ctx.scope();
            let a = scope.computed_async(move |c| {
                Box::pin(async move { c.get(&topic) + 1 })
                    as std::pin::Pin<Box<dyn Future<Output = i64> + Send>>
            });
            assert_eq!(scope.len(), 1);
            assert_eq!(rt.block_on(ctx.get_async(&a)), 2);
            assert_eq!(ctx.dependent_count(&topic), 1);
        }
        assert_eq!(ctx.dependent_count(&topic), 0);
    }

    fn stub_compute(_ctx: AsyncComputeContext) -> BoxedAsyncFuture {
        Box::pin(async { Arc::new(()) as Arc<AsyncAny> })
    }

    fn make_slot_node(revision: u64) -> AsyncComputedNode {
        AsyncComputedNode {
            state: AsyncSlotState::Empty,
            value: None,
            error: None,
            revision,
            compute: Arc::new(stub_compute),
            sync_compute: None,
            equals: None,
            dependencies: EdgeVec::new(),
            dependents: EdgeVec::new(),
            notifier: None,
        }
    }

    fn make_slot_node_with_memo(revision: u64, value: Option<Arc<AsyncAny>>) -> AsyncComputedNode {
        AsyncComputedNode {
            state: AsyncSlotState::Empty,
            value,
            error: None,
            revision,
            compute: Arc::new(stub_compute),
            sync_compute: None,
            equals: Some(Arc::new(|old: &AsyncAny, new: &AsyncAny| -> bool {
                let old_val = old.downcast_ref::<i32>();
                let new_val = new.downcast_ref::<i32>();
                match (old_val, new_val) {
                    (Some(o), Some(n)) => o == n,
                    _ => false,
                }
            })),
            dependencies: EdgeVec::new(),
            dependents: EdgeVec::new(),
            notifier: None,
        }
    }

    #[test]
    fn async_slot_state_starts_empty() {
        let ctx = AsyncContext::new();
        let id;
        {
            let mut inner = ctx.inner.lock();
            id = inner.alloc_id();
            inner.insert_node(id, AsyncNode::Computed(make_slot_node(0)));
        }
        let state = ctx.get_slot_state(id);
        assert!(matches!(state, AsyncSlotStateView::Empty));
    }

    #[test]
    fn empty_to_computing_transition() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async {});
        let mut node = make_slot_node(0);
        let old = node.transition_to_computing(handle);
        assert!(old.is_none());
        assert!(matches!(
            node.state,
            AsyncSlotState::Computing { revision: 0, .. }
        ));
    }

    #[test]
    fn computing_to_resolved_transition() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async {});
        let mut node = make_slot_node(0);
        node.transition_to_computing(handle);
        let result = node.transition_to_resolved(0, Arc::new(42i32));
        assert!(matches!(result, TransitionOutcome::Accepted));
        assert!(matches!(node.state, AsyncSlotState::Resolved));
        assert_eq!(
            node.value.as_ref().unwrap().downcast_ref::<i32>().unwrap(),
            &42
        );
    }

    #[test]
    fn computing_to_error_transition() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async {});
        let mut node = make_slot_node(0);
        node.transition_to_computing(handle);
        let err: Arc<dyn Error + Send + Sync> = Arc::new(std::io::Error::other("test error"));
        let result = node.transition_to_error(0, err);
        assert!(matches!(result, TransitionOutcome::Accepted));
        assert!(matches!(node.state, AsyncSlotState::Error));
        assert!(node.error.is_some());
        assert!(node.value.is_none());
    }

    #[test]
    fn stale_completion_is_rejected() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async {});
        let mut node = make_slot_node(1);
        node.transition_to_computing(handle);
        let result = node.transition_to_resolved(0, Arc::new(42i32));
        assert!(matches!(result, TransitionOutcome::Stale));
    }

    #[test]
    fn computing_to_computing_stale_returns_old_handle() {
        let rt = Runtime::new().unwrap();
        let handle1 = rt.spawn(async {});
        let handle2 = rt.spawn(async {});
        let mut node = make_slot_node(0);
        node.transition_to_computing(handle1);
        node.revision = 1;
        let old = node.transition_to_computing(handle2);
        assert!(old.is_some());
        assert!(matches!(
            node.state,
            AsyncSlotState::Computing { revision: 1, .. }
        ));
    }

    #[test]
    fn resolved_to_computing_via_invalidation() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async {});
        let mut node = make_slot_node(0);
        node.transition_to_computing(handle);
        node.transition_to_resolved(0, Arc::new(42i32));
        assert!(matches!(node.state, AsyncSlotState::Resolved));

        let result = node.invalidate();
        assert!(matches!(result, InvalidationResult::WasResolved));
        assert!(matches!(node.state, AsyncSlotState::Empty));
        assert_eq!(node.revision, 1);
    }

    #[test]
    fn error_to_computing_via_invalidation() {
        let mut node = AsyncComputedNode {
            state: AsyncSlotState::Error,
            value: None,
            error: Some(Arc::new(std::io::Error::other("test"))),
            revision: 0,
            compute: Arc::new(stub_compute),
            sync_compute: None,
            equals: None,
            dependencies: EdgeVec::new(),
            dependents: EdgeVec::new(),
            notifier: None,
        };
        let result = node.invalidate();
        assert!(matches!(result, InvalidationResult::WasError));
        assert!(matches!(node.state, AsyncSlotState::Empty));
        assert_eq!(node.revision, 1);
    }

    #[test]
    fn clear_aborts_in_flight() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async { std::future::pending::<()>().await });
        let mut node = make_slot_node(0);
        node.transition_to_computing(handle);
        let old_handle = node.clear();
        assert!(old_handle.is_some());
        old_handle.unwrap().abort();
        assert!(matches!(node.state, AsyncSlotState::Empty));
        assert!(node.value.is_none());
        assert_eq!(node.revision, 1);
    }

    #[test]
    fn memo_unchanged_transition() {
        let rt = Runtime::new().unwrap();
        let handle = rt.spawn(async {});
        let mut node = make_slot_node_with_memo(0, Some(Arc::new(42i32)));
        node.transition_to_computing(handle);
        let result = node.transition_to_resolved(0, Arc::new(42i32));
        assert!(matches!(result, TransitionOutcome::Unchanged));
    }

    #[test]
    fn async_context_cell_basic() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(10i32);
        assert_eq!(ctx.get(&cell), 10);
        ctx.set(&cell, 20);
        assert_eq!(ctx.get(&cell), 20);
    }

    #[test]
    fn async_context_cell_noop_on_equal() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(10i32);
        ctx.set(&cell, 10);
        assert_eq!(ctx.get(&cell), 10);
    }

    #[test]
    fn async_context_id_unique() {
        let ctx1 = AsyncContext::new();
        let ctx2 = AsyncContext::new();
        let id1 = ctx1.inner.lock().context_id;
        let id2 = ctx2.inner.lock().context_id;
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn computed_async_basic() {
        let ctx = AsyncContext::new();
        let slot = ctx.computed_async(|_ctx| async move { 42i32 });
        let val = ctx.get_async(&slot).await;
        assert_eq!(val, 42);
    }

    #[tokio::test]
    async fn computed_async_reads_cell() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(10i32);
        let slot = ctx.computed_async(move |ctx| {
            let val = ctx.get(&cell);
            async move { val + 1 }
        });
        let val = ctx.get_async(&slot).await;
        assert_eq!(val, 11);
    }

    #[tokio::test]
    async fn computed_async_cached() {
        let ctx = AsyncContext::new();
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let slot = ctx.computed_async(move |_| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                42i32
            }
        });
        let v1 = ctx.get_async(&slot).await;
        let v2 = ctx.get_async(&slot).await;
        assert_eq!(v1, 42);
        assert_eq!(v2, 42);
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn computed_async_invalidation() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let slot = ctx.computed_async(move |ctx| {
            let val = ctx.get(&cell);
            async move { val * 2 }
        });
        assert_eq!(ctx.get_async(&slot).await, 2);
        ctx.set(&cell, 5);
        assert_eq!(ctx.get_async(&slot).await, 10);
    }

    #[tokio::test]
    async fn memo_async_suppresses_equal() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let slot = ctx.computed_async(move |ctx| {
            let val = ctx.get(&cell);
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                val / val
            }
        });
        assert_eq!(ctx.get_async(&slot).await, 1);
        ctx.set(&cell, 2);
        assert_eq!(ctx.get_async(&slot).await, 1);
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn batch_defers_invalidation() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let slot = ctx.computed_async(move |ctx| {
            let val = ctx.get(&cell);
            async move { val * 10 }
        });
        assert_eq!(ctx.get_async(&slot).await, 10);
        ctx.batch(|ctx| {
            ctx.set(&cell, 2);
            ctx.set(&cell, 3);
        });
        assert_eq!(ctx.get_async(&slot).await, 30);
    }

    #[tokio::test]
    async fn concurrent_get_async_deduplicates() {
        let ctx = AsyncContext::new();
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let slot = ctx.computed_async(move |_| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                99i32
            }
        });
        let (v1, v2) = tokio::join!(ctx.get_async(&slot), ctx.get_async(&slot));
        assert_eq!(v1, 99);
        assert_eq!(v2, 99);
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn async_slot_reads_async_slot() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(5i32);
        let base = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            async move { v + 10 }
        });
        let derived = ctx.computed_async(move |ctx| {
            let base_handle = base;
            async move {
                let v = ctx.get_async(&base_handle).await;
                v * 2
            }
        });
        assert_eq!(ctx.get_async(&derived).await, 30);
    }

    #[tokio::test]
    async fn async_chain_invalidation() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let cell_clone = cell;
        let base = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell_clone);
            async move { v + 10 }
        });
        let derived = ctx.computed_async(move |ctx| {
            let bh = base;
            async move {
                let v = ctx.get_async(&bh).await;
                v * 2
            }
        });
        assert_eq!(ctx.get_async(&derived).await, 22);
        ctx.set(&cell_clone, 3);
        assert_eq!(ctx.get_async(&derived).await, 26);
    }

    #[tokio::test]
    async fn async_chain_three_levels() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let a = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            async move { v + 1 }
        });
        let b = ctx.computed_async(move |ctx| {
            let ah = a;
            async move { ctx.get_async(&ah).await + 1 }
        });
        let c = ctx.computed_async(move |ctx| {
            let bh = b;
            async move { ctx.get_async(&bh).await + 1 }
        });
        assert_eq!(ctx.get_async(&c).await, 4);
        ctx.set(&cell, 10);
        assert_eq!(ctx.get_async(&c).await, 13);
    }

    #[tokio::test]
    async fn async_dependency_tracks_slot_edges() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(3i32);
        let slot = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            async move { v * 2 }
        });
        let _ = ctx.get_async(&slot).await;
        {
            let inner = ctx.inner.lock();
            if let Some(AsyncNode::Computed(s)) = inner.get_node(slot.id) {
                assert!(s.dependencies.contains(&cell.id));
            }
        }
        {
            let inner = ctx.inner.lock();
            if let Some(AsyncNode::Source(c)) = inner.get_node(cell.id) {
                assert!(c.dependents.contains(&slot.id));
            }
        }
    }

    #[tokio::test]
    async fn async_dependency_updates_on_rerun() {
        let ctx = AsyncContext::new();
        let cell_a = ctx.source(1i32);
        let cell_b = ctx.source(100i32);
        let flag = ctx.source(true);
        let slot = ctx.computed_async(move |ctx| {
            let f = ctx.get(&flag);
            let v = if f {
                ctx.get(&cell_a)
            } else {
                ctx.get(&cell_b)
            };
            async move { v }
        });
        assert_eq!(ctx.get_async(&slot).await, 1);
        {
            let inner = ctx.inner.lock();
            if let Some(AsyncNode::Computed(s)) = inner.get_node(slot.id) {
                assert!(s.dependencies.contains(&cell_a.id));
                assert!(!s.dependencies.contains(&cell_b.id));
            }
        }
        ctx.set(&flag, false);
        assert_eq!(ctx.get_async(&slot).await, 100);
        {
            let inner = ctx.inner.lock();
            if let Some(AsyncNode::Computed(s)) = inner.get_node(slot.id) {
                assert!(!s.dependencies.contains(&cell_a.id));
                assert!(s.dependencies.contains(&cell_b.id));
            }
        }
    }

    // #lzrsdeppool: every spawn used to mint a fresh dependency tracker
    // (`Arc` + `HashSet`). The pool recycles the allocation *and* the set's
    // table capacity, so a steady-state graph stops allocating on the spawn
    // path. `async_dependency_updates_on_rerun` above is the correctness half:
    // a reused tracker must still re-discover edges from scratch.
    #[tokio::test]
    async fn dependency_trackers_are_pooled_and_reused() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(0i32);
        let slot = ctx.computed_async(move |cctx| {
            let v = cctx.get(&cell);
            async move { v }
        });
        assert_eq!(ctx.get_async(&slot).await, 0);

        let pooled = {
            let inner = ctx.inner.lock();
            assert_eq!(
                inner.deps_pool.len(),
                1,
                "completed run returns its tracker"
            );
            assert!(
                inner.deps_pool[0].lock().capacity() > 0,
                "table capacity is recycled with the tracker, not just the Arc"
            );
            Arc::as_ptr(&inner.deps_pool[0])
        };

        for i in 1..40i32 {
            ctx.set(&cell, i);
            assert_eq!(ctx.get_async(&slot).await, i);
            assert!(ctx.inner.lock().deps_pool.len() <= DEPS_POOL_CAP);
        }

        let inner = ctx.inner.lock();
        assert_eq!(inner.deps_pool.len(), 1, "steady state reuses one tracker");
        assert_eq!(
            Arc::as_ptr(&inner.deps_pool[0]),
            pooled,
            "the same allocation is recycled across recomputes"
        );
    }

    // #lzrsdeppool: `recycle_deps` gates on `Arc::get_mut`, so a compute that
    // stashed its tracker somewhere outliving the run keeps that tracker out of
    // the pool — a later spawn can never be handed a tracker someone else still
    // writes into.
    #[tokio::test]
    async fn leaked_dependency_tracker_is_not_recycled() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        type LeakedTrackers = Arc<Mutex<Vec<Arc<Mutex<HashSet<SlotId>>>>>>;
        let leaked: LeakedTrackers = Arc::new(Mutex::new(Vec::new()));
        let leaked_for_compute = leaked.clone();
        let slot = ctx.computed_async(move |cctx| {
            leaked_for_compute.lock().push(cctx.dependencies.clone());
            let v = cctx.get(&cell);
            async move { v }
        });

        assert_eq!(ctx.get_async(&slot).await, 1);
        assert_eq!(leaked.lock().len(), 1);
        assert!(
            ctx.inner.lock().deps_pool.is_empty(),
            "a tracker still referenced elsewhere must not re-enter the pool"
        );
    }

    // #lzasyncdispose2: disposing an effect bumps the per-index generation and
    // recycles the SlotId; the next allocation reuses the id but sees the
    // bumped generation, which is what lets an in-flight stale run detect that
    // it no longer owns the node.
    #[tokio::test]
    async fn dispose_bumps_generation_then_id_recycles_fresh() {
        let ctx = Arc::new(AsyncContext::new());
        let cell = ctx.source(1i32);
        let effect_a = ctx.effect_async(move |c| {
            let _v = c.get(&cell);
            async move { None::<fn()> }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let a_id = effect_a.id;
        let g0 = ctx.inner.lock().generation(a_id);

        // Allocate B's dependency BEFORE dispose so the recycled id goes to the
        // effect (free_ids LIFO), not to this cell.
        let cell_b = ctx.source(2i32);
        ctx.dispose_async_effect(&effect_a);
        let g1 = ctx.inner.lock().generation(a_id);
        assert_eq!(g1, g0 + 1, "dispose must bump the node generation");

        // Reuse the recycled id with a fresh effect; the bumped generation
        // sticks so any task still holding `g0` can detect the reuse.
        let effect_b = ctx.effect_async(move |c| {
            let _v = c.get(&cell_b);
            async move { None::<fn()> }
        });
        assert_eq!(
            effect_b.id, a_id,
            "free_ids LIFO should recycle A's id for B"
        );
        assert_eq!(
            ctx.inner.lock().generation(effect_b.id),
            g1,
            "recycled node keeps the bumped generation",
        );
    }

    // #lzasyncdispose2: a run still in-flight after its effect was disposed (and
    // its id recycled to a NEW effect) must not write its edges/cleanup into the
    // aliased node. `dispose`'s `abort()` is the first defense; this guards the
    // window where the run already passed its `.await` and `abort()` lost the
    // race. We exercise that second defense directly by replaying a stale-
    // generation compute context against a recycled id.
    #[tokio::test]
    async fn stale_generation_context_does_not_alias_recycled_effect() {
        let ctx = Arc::new(AsyncContext::new());
        let cell = ctx.source(1i32);
        let effect_a = ctx.effect_async(move |c| {
            let _v = c.get(&cell);
            async move { None::<fn()> }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let a_id = effect_a.id;

        // Capture a compute context exactly as A's in-flight run would hold it.
        // Each lock is its own statement so the (non-reentrant) guards do not
        // overlap.
        let ctx_id = ctx.inner.lock().context_id;
        let a_gen = ctx.inner.lock().generation(a_id);
        let stale_ctx = AsyncComputeContext {
            _context_id: ctx_id,
            _node_id: a_id,
            _node_gen: a_gen,
            inner: ctx.inner.clone(),
            dependencies: Arc::new(Mutex::new(HashSet::new())),
        };

        // Allocate B's dependency BEFORE dispose so the recycled id (free_ids
        // LIFO) goes to the effect, then dispose A and allocate B reusing the id.
        let cell_b = ctx.source(99i32);
        ctx.dispose_async_effect(&effect_a);
        let effect_b = ctx.effect_async(move |c| {
            let _v = c.get(&cell_b);
            async move { None::<fn()> }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            effect_b.id, a_id,
            "B must reuse A's recycled id for the test"
        );

        // A's stale run replays a dependency read on `cell`. Pre-fix this wrote
        // an edge `cell -> a_id` and `a_id.dependencies += cell`, aliasing B.
        let _ = stale_ctx.get(&cell);

        let inner = ctx.inner.lock();
        match inner.get_node(a_id) {
            Some(AsyncNode::Effect(e)) => {
                assert!(
                    e.dependencies.contains(&cell_b.id),
                    "B's real dependency must stay intact",
                );
                assert!(
                    !e.dependencies.contains(&cell.id),
                    "stale-generation write must not alias B with A's old dependency",
                );
            }
            _ => panic!("recycled node should be B's effect"),
        }
        if let Some(AsyncNode::Source(c)) = inner.get_node(cell.id) {
            assert!(
                !c.dependents.contains(&a_id),
                "`cell` must not gain a phantom dependent via the stale write",
            );
        }
    }

    #[tokio::test]
    async fn invalidation_aborts_in_flight() {
        let ctx = Arc::new(AsyncContext::new());
        let cell = ctx.source(1i32);
        let compute_count = Arc::new(AtomicU64::new(0));
        let count_clone = compute_count.clone();
        let slot = ctx.computed_async(move |ctx| {
            let _v = ctx.get(&cell);
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                42i32
            }
        });
        let ctx_clone = ctx.clone();
        let handle = tokio::spawn(async move { ctx_clone.get_async(&slot).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        ctx.set(&cell, 2);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let val = ctx.get_async(&slot).await;
        assert_eq!(val, 42);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn stale_revision_prevents_publish() {
        let ctx = Arc::new(AsyncContext::new());
        let cell = ctx.source(1i32);
        let slot = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            async move { v + 1 }
        });
        let ctx1 = ctx.clone();
        let h1 = tokio::spawn(async move { ctx1.get_async(&slot).await });
        let v1 = h1.await.unwrap();
        assert_eq!(v1, 2);
        let state = ctx.get_slot_state(slot.id);
        assert!(matches!(state, AsyncSlotStateView::Resolved));
        ctx.set(&cell, 10);
        let state = ctx.get_slot_state(slot.id);
        assert!(matches!(state, AsyncSlotStateView::Empty));
        let v2 = ctx.get_async(&slot).await;
        assert_eq!(v2, 11);
    }

    #[tokio::test]
    async fn dropping_one_waiter_does_not_cancel_shared_compute() {
        let ctx = AsyncContext::new();
        let compute_count = Arc::new(AtomicU64::new(0));
        let count_clone = compute_count.clone();
        let slot = ctx.computed_async(move |_| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                99i32
            }
        });
        let (v2, v3) = tokio::join!(ctx.get_async(&slot), ctx.get_async(&slot));
        assert_eq!(v2, 99);
        assert_eq!(v3, 99);
        assert_eq!(compute_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn effect_async_runs_on_creation() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(10i32);
        let result = Arc::new(Mutex::new(0i32));
        let result_clone = result.clone();
        ctx.effect_async(move |ctx| {
            let v = ctx.get(&cell);
            let r = result_clone.clone();
            async move {
                *r.lock() = v;
                None::<fn()>
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(*result.lock(), 10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn effect_async_reruns_on_cell_change() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        ctx.effect_async(move |ctx| {
            let _v = ctx.get(&cell);
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                None::<fn()>
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(count.load(Ordering::Relaxed), 1);
        ctx.set(&cell, 2);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(count.load(Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn effect_async_cleanup_runs_on_rerun() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let cleanup_count = Arc::new(AtomicU64::new(0));
        let cleanup_clone = cleanup_count.clone();
        ctx.effect_async(move |ctx| {
            let _v = ctx.get(&cell);
            let c = cleanup_clone.clone();
            async move {
                Some(move || {
                    c.fetch_add(1, Ordering::Relaxed);
                })
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(cleanup_count.load(Ordering::Relaxed), 0);
        ctx.set(&cell, 2);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(cleanup_count.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn effect_rerun_aborts_prior_inflight() {
        // Re-invalidating an effect_async dependency while the effect body is
        // still .awaiting must abort the prior run, not spawn a second concurrent
        // one. Pre-#lzasyncrerunabort the prior in_flight was overwritten without
        // abort: the effect body completed twice and the overwritten run's
        // cleanup was leaked (never invoked).
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let done_count = Arc::new(AtomicU64::new(0));
        let cleanup_count = Arc::new(AtomicU64::new(0));
        let done_clone = done_count.clone();
        let cleanup_clone = cleanup_count.clone();
        let handle = ctx.effect_async(move |ctx| {
            let _v = ctx.get(&cell);
            let d = done_clone.clone();
            let c = cleanup_clone.clone();
            async move {
                // Started; yield to the scheduler so the body is genuinely
                // in-flight at the sleep when the dependency flips.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                // Past the await = the run completed (only reached if NOT aborted).
                d.fetch_add(1, Ordering::Relaxed);
                let c = c.clone();
                Some(move || {
                    c.fetch_add(1, Ordering::Relaxed);
                })
            }
        });

        // First run is now parked in its sleep await.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        ctx.set(&cell, 2); // re-invalidate mid-flight
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        // Exactly one completed run; the aborted prior run never passed its await.
        assert_eq!(
            done_count.load(Ordering::Relaxed),
            1,
            "prior in-flight effect must be aborted on re-run, not double-executed"
        );

        // The single surviving cleanup must fire on dispose (not be leaked).
        ctx.dispose_async_effect(&handle);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            cleanup_count.load(Ordering::Relaxed),
            1,
            "the surviving run's cleanup must run exactly once on dispose"
        );
    }

    #[tokio::test]
    async fn dispose_async_effect_removes_it() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let handle = ctx.effect_async(move |ctx| {
            let _v = ctx.get(&cell);
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                None::<fn()>
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after_first = count.load(Ordering::Relaxed);
        assert!(after_first >= 1);
        ctx.dispose_async_effect(&handle);
        ctx.set(&cell, 2);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::Relaxed), after_first);
    }

    #[test]
    fn sync_get_returns_none_for_empty_slot() {
        let ctx = AsyncContext::new();
        let slot = ctx.computed_async(|_| async { 42i32 });
        assert!(ctx.get(&slot).is_none());
    }

    #[tokio::test]
    async fn sync_get_returns_some_after_resolve() {
        let ctx = AsyncContext::new();
        let slot = ctx.computed_async(|_| async { 42i32 });
        let val = ctx.get_async(&slot).await;
        assert_eq!(val, 42);
        assert_eq!(ctx.get(&slot), Some(42));
    }

    #[tokio::test]
    async fn sync_get_returns_none_after_invalidation() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(1i32);
        let slot = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            async move { v * 2 }
        });
        let _ = ctx.get_async(&slot).await;
        assert_eq!(ctx.get(&slot), Some(2));
        ctx.set(&cell, 5);
        assert!(ctx.get(&slot).is_none());
    }

    #[tokio::test]
    async fn sync_get_avoids_spawn_overhead() {
        let ctx = AsyncContext::new();
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let slot = ctx.computed_async(move |_| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                99i32
            }
        });
        let v1 = ctx.get_async(&slot).await;
        assert_eq!(v1, 99);
        assert_eq!(count.load(Ordering::Relaxed), 1);
        let v2 = ctx.get(&slot);
        assert_eq!(v2, Some(99));
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn sync_get_with_memo_returns_cached() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(3i32);
        let slot = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            async move { v.abs() }
        });
        assert_eq!(ctx.get_async(&slot).await, 3);
        assert_eq!(ctx.get(&slot), Some(3));
    }

    #[tokio::test]
    async fn get_async_uses_sync_fast_path() {
        let ctx = AsyncContext::new();
        let cell = ctx.source(10i32);
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        let slot = ctx.computed_async(move |ctx| {
            let v = ctx.get(&cell);
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                v + 1
            }
        });
        let v1 = ctx.get_async(&slot).await;
        assert_eq!(v1, 11);
        assert_eq!(count.load(Ordering::Relaxed), 1);
        let v2 = ctx.get_async(&slot).await;
        assert_eq!(v2, 11);
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn async_schedule_effect_dedupes_pending_queue() {
        let ctx = AsyncContext::new();
        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();
        let cell = ctx.source(0i32);
        let effect = ctx.effect_async(move |ctx| {
            let _ = ctx.get(&cell);
            async { None::<fn()> }
        });
        rt.block_on(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
        {
            let mut inner = ctx.inner.lock();
            AsyncContext::schedule_async_effect(&mut inner, effect.id);
            AsyncContext::schedule_async_effect(&mut inner, effect.id);
            AsyncContext::schedule_async_effect(&mut inner, effect.id);
            let count = inner
                .pending_async_effects
                .iter()
                .filter(|&&id| id == effect.id)
                .count();
            assert_eq!(
                count, 1,
                "pending_async_effects must dedupe the same effect id; got {:?}",
                inner.pending_async_effects
            );
            assert!(inner.scheduled_async_effects.contains(&effect.id));
        }
        ctx.flush_async_effects();
        {
            let inner = ctx.inner.lock();
            assert!(
                !inner.scheduled_async_effects.contains(&effect.id),
                "flush must clear scheduled_async_effects"
            );
        }
        rt.block_on(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
    }
}
