//! Async keyed reactive collection (`#reactivemap`, async flavor).
//!
//! The [`AsyncContext`] analog of [`ReactiveMap`](crate::ReactiveMap): keys `K`
//! map to per-entry async reactive nodes ([`AsyncSource<V>`] input cells /
//! [`AsyncComputed<V>`] derived slots). Like
//! [`ThreadSafeReactiveMap`](crate::ThreadSafeReactiveMap) it keeps its present-set
//! state behind an `Arc<Mutex<..>>` (the [`AsyncContext`] is itself `Send + Sync`),
//! so it can live in a cross-task owner.
//!
//! The eager/lazy behavior and present-set monotonicity are identical to the
//! single-threaded map: eager pre-mints the keyset
//! ([`materialize_all`](AsyncReactiveMap::materialize_all)); lazy mints on access
//! ([`get_or_insert_handle`](AsyncReactiveMap::get_or_insert_handle)). There is no
//! eager/lazy mode flag. The transparency law is **eventual**: an async derived
//! slot read is `None` while pending and resolves to the canonical value — so
//! `observe` returns [`Option<V>`]. Input cells are always resolved. Drive a slot
//! to resolution with [`AsyncContext::get_async`] on the handle from
//! [`get_or_insert_handle`](AsyncReactiveMap::get_or_insert_handle).
//!
//! Its two specializations are [`AsyncSourceMap`] (input cells) and [`AsyncComputedMap`]
//! (derived slots). Mirrors the async materialization case in lazily-spec and the
//! `AsyncMaterialization` proofs (eventual transparency) in lazily-formal.

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::cell_family::EntryKind;
use crate::keyed_order::{KeyedOrder, Move, Mutation};
use crate::{AsyncComputeContext, AsyncComputed, AsyncContext, AsyncSource, Read};

mod sealed {
    pub trait Sealed {}
}

/// The per-entry value producer an async map hands to
/// [`AsyncMapHandle::materialize`].
///
/// Boxed rather than a generic `impl Fn` because the map stores it across the
/// `materialize` call and it must be `Send + Sync` to run on any task the
/// context is driven from. The `&AsyncComputeContext` parameter is the entry's
/// tracking view — the thing the nullary `Fn() -> V` this replaced could not
/// carry.
pub type AsyncEntryCompute<V> = Arc<dyn Fn(&AsyncComputeContext) -> V + Send + Sync>;

/// The node kinds an async map entry can take — the [`AsyncContext`] analog of
/// [`MapHandle`](crate::MapHandle). Sealed to [`AsyncSource`] (input cells)
/// and [`AsyncComputed`] (derived slots).
pub trait AsyncMapHandle<V>: sealed::Sealed + Copy + Send + Sync + 'static {
    /// This handle's entry kind. `AsyncSource` is [`EntryKind::Source`] (always
    /// resolved); `AsyncComputed` is [`EntryKind::Computed`] (resolves
    /// asynchronously).
    const KIND: EntryKind;

    /// Allocate the node for one entry on `ctx`. `compute` is the per-key value
    /// producer; a cell sets the value directly, a derived slot wraps it in a ready
    /// future as its async recomputation.
    ///
    /// `compute` receives the entry's own [`AsyncComputeContext`] — a genuine
    /// value-threaded tracking view carrying the node id, its generation stamp,
    /// and the edge set. Reads through it register dependency edges on this
    /// entry, so a derived entry can be genuinely derived. Previously this was a
    /// nullary `Fn() -> V`, which severed the view before it could reach the map.
    fn materialize(ctx: &AsyncContext, compute: AsyncEntryCompute<V>) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static;

    /// Detach this entry's node from the graph on removal.
    ///
    /// The single-threaded flavor only clears the cached value and dependents,
    /// because its runtime exposes no node-free API. This context does, so the
    /// node is disposed outright: any in-flight compute is aborted, downstream
    /// edges are detached, dependents are invalidated, and the id is recycled.
    fn clear_dependents(self, ctx: &AsyncContext);
}

impl<V> sealed::Sealed for AsyncSource<V> {}
impl<V: Send + Sync + 'static> AsyncMapHandle<V> for AsyncSource<V> {
    const KIND: EntryKind = EntryKind::Source;

    fn materialize(ctx: &AsyncContext, compute: AsyncEntryCompute<V>) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        // An input has no derivation: materialize by setting its value directly.
        // Evaluated once, detached — a source cell's seed value is not an edge.
        ctx.source(ctx.eval_detached(|actx| compute(actx)))
    }

    fn clear_dependents(self, ctx: &AsyncContext) {
        ctx.dispose_cell(&self);
    }
}

impl<V> sealed::Sealed for AsyncComputed<V> {}
impl<V: Send + Sync + 'static> AsyncMapHandle<V> for AsyncComputed<V> {
    const KIND: EntryKind = EntryKind::Computed;

    fn materialize(ctx: &AsyncContext, compute: AsyncEntryCompute<V>) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        // A derived node whose async recompute is a ready future of the value.
        // The tracking view is *threaded into* `compute` rather than bound and
        // dropped: the entry's reads of other reactives register real edges.
        //
        // The future is still ready-by-construction, so a derived entry can
        // track but cannot yet `await`. Those are separable concerns and only
        // the first is needed for dependency edges.
        ctx.computed_async(move |actx| {
            let v = compute(&actx);
            async move { v }
        })
    }

    fn clear_dependents(self, ctx: &AsyncContext) {
        ctx.dispose_slot(&self);
    }
}

struct MapInner<K, H> {
    /// Present set + authoritative key order + the move algebra, shared verbatim
    /// with the single-threaded and thread-safe flavors. Graph-agnostic and
    /// closure-free; this flavor's only contribution is the `Mutex` around it.
    state: Mutex<KeyedOrder<K, H>>,
    /// Reactive *set-membership* signal, minted on the owning [`AsyncContext`].
    /// Bumped only when the **set** of keys changes, so `len`/`contains_key`
    /// readers are invalidated without coupling to entry values or to pure
    /// reordering. Mirrors the single-threaded map's plane.
    membership: AsyncSource<u64>,
    /// Atomic (untracked) mirror of the membership version so mutators can bump
    /// the reactive cell without registering a spurious dependency.
    version: AtomicU64,
    /// Reactive *order* signal. Bumped on add/remove and on any future
    /// move/reorder, so `keys` readers are invalidated independently of
    /// `len`/`contains_key` readers.
    order_signal: AsyncSource<u64>,
    /// Atomic mirror of the order version.
    order_version: AtomicU64,
}

/// The async keyed reactive collection (`#reactivemap`) generic over the entry
/// handle kind `H` ([`AsyncSource<V>`] input cells, [`AsyncComputed<V>`]
/// derived slots).
///
/// Cheap to [`Clone`] (an `Arc` to shared inner state) and `Send + Sync`. See the
/// module docs for the eager/lazy behavior and the eventual-transparency law.
pub struct AsyncReactiveMap<K, V, H> {
    inner: Arc<MapInner<K, H>>,
    _marker: PhantomData<V>,
}

impl<K, V, H> Clone for AsyncReactiveMap<K, V, H> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }
}

impl<K, V, H> AsyncReactiveMap<K, V, H>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
    H: AsyncMapHandle<V>,
{
    /// Create an empty map bound to `ctx`.
    ///
    /// `ctx` is load-bearing: the membership and order signals are cells minted
    /// on it, which is what makes `keys`/`len`/`contains_key` reactive here.
    pub fn new(ctx: &AsyncContext) -> Self {
        Self {
            inner: Arc::new(MapInner {
                state: Mutex::new(KeyedOrder::new()),
                membership: ctx.source(0u64),
                version: AtomicU64::new(0),
                order_signal: ctx.source(0u64),
                order_version: AtomicU64::new(0),
            }),
            _marker: PhantomData,
        }
    }

    /// Guard on the bookkeeping core.
    ///
    /// Callers must drop the guard before touching `ctx`: a `ctx.set` can drive a
    /// dependent recompute that re-enters this map and would deadlock on a
    /// still-held lock.
    fn lock(&self) -> MutexGuard<'_, KeyedOrder<K, H>> {
        self.inner.state.lock().expect("map state mutex poisoned")
    }

    /// Bump the *order* signal (invalidates `keys` readers).
    ///
    /// Must be called with the map's `Mutex` released.
    fn bump_order(&self, ctx: &AsyncContext) {
        let next = self.inner.order_version.fetch_add(1, Ordering::Relaxed) + 1;
        ctx.set(&self.inner.order_signal, next);
    }

    /// Bump set-membership (invalidates `len`/`contains_key` readers). Always
    /// paired with an order bump because add/remove change order too.
    fn bump_membership(&self, ctx: &AsyncContext) {
        let next = self.inner.version.fetch_add(1, Ordering::Relaxed) + 1;
        ctx.set(&self.inner.membership, next);
        self.bump_order(ctx);
    }

    /// Reactive snapshot of the keys in their current order.
    ///
    /// Generic over the read surface, exactly like the single-threaded map's
    /// `ComputeOps` genericity: pass an [`AsyncComputeContext`] from inside a
    /// compute and the read registers a dependency edge; pass a bare
    /// [`AsyncContext`] and it does not.
    ///
    /// [`AsyncComputeContext`]: crate::AsyncComputeContext
    pub fn keys<C>(&self, ctx: &C) -> Vec<K>
    where
        AsyncSource<u64>: Read<C, Output = u64>,
    {
        let _ = self.inner.order_signal.read(ctx);
        self.lock().keys()
    }

    /// Reactive entry count. Subscribes the caller to membership changes only.
    /// Same tracking discipline as [`keys`](Self::keys).
    pub fn len<C>(&self, ctx: &C) -> usize
    where
        AsyncSource<u64>: Read<C, Output = u64>,
    {
        let _ = self.inner.membership.read(ctx);
        self.lock().len()
    }

    /// Reactive emptiness check. Subscribes the caller to membership changes.
    pub fn is_empty<C>(&self, ctx: &C) -> bool
    where
        AsyncSource<u64>: Read<C, Output = u64>,
    {
        self.len(ctx) == 0
    }

    /// Reactive membership test for `key`. Subscribes the caller to membership
    /// changes (add/remove of any key), not to value changes.
    pub fn contains_key<C>(&self, ctx: &C, key: &K) -> bool
    where
        AsyncSource<u64>: Read<C, Output = u64>,
    {
        let _ = self.inner.membership.read(ctx);
        self.lock().contains(key)
    }

    fn mint_with(&self, ctx: &AsyncContext, key: K, compute: AsyncEntryCompute<V>) -> H {
        // Fast path under the lock; release before touching `ctx`.
        if let Some(handle) = self.lock().get(&key) {
            return handle;
        }
        let handle = H::materialize(ctx, compute);
        // First writer wins on a race, so the core keeps the existing handle and
        // reports `Unchanged`; the freshly-allocated node is orphaned.
        let (handle, mutation) = self.lock().insert(key, handle);
        if mutation == Mutation::Changed {
            // Lock released first: `ctx.set` can drive a dependent recompute.
            self.bump_membership(ctx);
        }
        handle
    }

    /// Get the entry handle for `key`, minting it via `factory(compute, &key)` on
    /// first access and caching it. For a slot map this is the
    /// [`AsyncComputed`] to drive with [`AsyncContext::get_async`].
    ///
    /// The factory's first parameter is the entry's own tracking view: reads of
    /// other reactives through it register dependency edges *on this entry*.
    /// Ignore it (`|_, key| …`) for a constant-per-key factory.
    pub fn get_or_insert_handle(
        &self,
        ctx: &AsyncContext,
        key: K,
        factory: impl Fn(&AsyncComputeContext, &K) -> V + Send + Sync + 'static,
    ) -> H {
        let k = key.clone();
        let compute: AsyncEntryCompute<V> = Arc::new(move |actx| factory(actx, &k));
        self.mint_with(ctx, key, compute)
    }

    /// Return the existing entry handle for `key`, or `None`. Non-minting.
    pub fn handle(&self, key: &K) -> Option<H> {
        self.lock().get(key)
    }

    /// Whether `key` is currently materialized (present). Non-reactive.
    pub fn is_present(&self, key: &K) -> bool {
        self.lock().contains(key)
    }

    /// The currently-materialized keys, in first-materialization order.
    pub fn present_keys(&self) -> Vec<K> {
        self.lock().keys()
    }

    /// Number of currently-materialized entries.
    pub fn present_count(&self) -> usize {
        self.lock().len()
    }

    /// Remove `key`'s entry. Bumps reactive membership and detaches the removed
    /// entry's node. Returns whether the key was present.
    pub fn remove(&self, ctx: &AsyncContext, key: &K) -> bool {
        let (removed, mutation) = self.lock().remove(key);
        let Some(handle) = removed else {
            return false;
        };
        // Lock released first: disposal invalidates dependents, which can drive a
        // recompute that re-enters this map.
        handle.clear_dependents(ctx);
        if mutation == Mutation::Changed {
            self.bump_membership(ctx);
        }
        true
    }

    /// Current 0-based position of `key` in the order, or `None` if absent.
    /// Non-reactive.
    pub fn position(&self, key: &K) -> Option<usize> {
        self.lock().position(key)
    }

    /// Atomically move `key` to `index` in the order.
    ///
    /// The entry keeps the **same** node, the same dependents, and its lineage —
    /// unlike a naive `remove` + re-mint. Only the order signal is bumped
    /// (once), so `keys` readers recompute while `len`/`contains_key` readers
    /// stay cached.
    ///
    /// Ordering is not async-coloured: it touches no entry handle and awaits
    /// nothing, so it is the same algebra the other two flavors run.
    ///
    /// `index` is clamped to `[0, len)`. Returns whether `key` was present.
    pub fn move_to(&self, ctx: &AsyncContext, key: &K, index: usize) -> bool {
        let outcome = self.lock().move_to(key, index);
        self.settle_move(ctx, outcome)
    }

    /// Atomically move `key` to just before `anchor`. Returns `false` if either
    /// key is absent.
    pub fn move_before(&self, ctx: &AsyncContext, key: &K, anchor: &K) -> bool {
        let outcome = self.lock().move_before(key, anchor);
        self.settle_move(ctx, outcome)
    }

    /// Atomically move `key` to just after `anchor`. Returns `false` if either
    /// key is absent.
    pub fn move_after(&self, ctx: &AsyncContext, key: &K, anchor: &K) -> bool {
        let outcome = self.lock().move_after(key, anchor);
        self.settle_move(ctx, outcome)
    }

    /// Bump the order signal iff the order actually changed, and report whether
    /// the move could be expressed. The lock is already released by the time
    /// this runs — see [`lock`](Self::lock).
    fn settle_move(&self, ctx: &AsyncContext, outcome: Move) -> bool {
        if outcome.changed() {
            self.bump_order(ctx);
        }
        outcome.is_present()
    }

    /// This map's entry kind.
    pub fn entry_kind(&self) -> EntryKind {
        H::KIND
    }
}

/// `AsyncSourceMap`-only surface: `set` (an input is settable).
impl<K, V> AsyncReactiveMap<K, V, AsyncSource<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// Set the value at `key`, inserting a new input cell if absent. Cell-only.
    pub fn set(&self, ctx: &AsyncContext, key: K, value: V) {
        let existing = self.lock().get(&key);
        if let Some(handle) = existing {
            ctx.set(&handle, value);
            return;
        }
        self.get_or_insert_handle(ctx, key, move |_, _| value.clone());
    }
}

/// `AsyncComputedMap`-only surface: the eager pre-mint helper.
impl<K, V> AsyncReactiveMap<K, V, AsyncComputed<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// **Eager materialization**: pre-mint a derived slot for every key in `keys`.
    ///
    /// `factory` takes the entry's own tracking view, exactly as
    /// [`get_or_insert_handle`](AsyncReactiveMap::get_or_insert_handle) does.
    pub fn materialize_all(
        &self,
        ctx: &AsyncContext,
        keys: impl IntoIterator<Item = K>,
        factory: impl Fn(&AsyncComputeContext, &K) -> V + Send + Sync + 'static,
    ) {
        let factory = Arc::new(factory);
        for key in keys {
            let f = Arc::clone(&factory);
            self.get_or_insert_handle(ctx, key, move |actx, k| f(actx, k));
        }
    }
}

/// An async **input-cell** map: every entry is an always-resolved
/// [`AsyncSource<V>`].
pub type AsyncSourceMap<K, V> = AsyncReactiveMap<K, V, AsyncSource<V>>;

/// An async **derived-slot** map: entries are [`AsyncComputed<V>`] minted lazily
/// on access or eagerly via [`materialize_all`](AsyncReactiveMap::materialize_all),
/// resolved via [`AsyncContext::get_async`].
pub type AsyncComputedMap<K, V> = AsyncReactiveMap<K, V, AsyncComputed<V>>;

impl<K, V> AsyncReactiveMap<K, V, AsyncSource<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// Non-blocking observe of an existing source entry. Generic over the read
    /// surface: an [`AsyncComputeContext`] registers a dependency edge, while a
    /// bare [`AsyncContext`] read remains untracked. Non-minting.
    pub fn observe<C>(&self, ctx: &C, key: &K) -> Option<V>
    where
        AsyncSource<V>: Read<C, Output = V>,
    {
        self.lock().get(key).map(|handle| handle.read(ctx))
    }
}

impl<K, V> AsyncReactiveMap<K, V, AsyncComputed<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// Non-blocking observe of an existing computed entry. A pending or absent
    /// entry yields `None`; a resolved entry yields `Some(value)`. Generic over
    /// the read surface with the same tracked/untracked split as source entries.
    pub fn observe<C>(&self, ctx: &C, key: &K) -> Option<V>
    where
        AsyncComputed<V>: Read<C, Output = Option<V>>,
    {
        self.lock().get(key).and_then(|handle| handle.read(ctx))
    }
}

/// Deprecated alias for [`AsyncSourceMap`].
#[deprecated(note = "renamed to AsyncSourceMap")]
pub type AsyncCellMap<K, V> = AsyncSourceMap<K, V>;

/// Deprecated alias for [`AsyncComputedMap`].
#[deprecated(note = "renamed to AsyncComputedMap")]
pub type AsyncSlotMap<K, V> = AsyncComputedMap<K, V>;

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    /// The membership plane must invalidate a dependent, not merely return the
    /// right number. Read through the *tracking* view (`AsyncComputeContext`),
    /// which is what registers the edge — a bare `AsyncContext` read is
    /// deliberately untracked.
    #[tokio::test]
    async fn membership_plane_invalidates_a_dependent_computed() {
        let ctx = AsyncContext::new();
        let fam: AsyncSourceMap<u64, bool> = AsyncSourceMap::new(&ctx);
        let f = fam.clone();
        let observed = ctx.computed_async(move |actx| {
            let n = f.len(&actx);
            async move { n }
        });
        assert_eq!(ctx.get_async(&observed).await, 0);

        fam.set(&ctx, 1, true);
        assert_eq!(
            ctx.get_async(&observed).await,
            1,
            "adding a key must invalidate a len reader on the async plane"
        );

        fam.set(&ctx, 2, true);
        assert_eq!(ctx.get_async(&observed).await, 2);
    }

    /// `keys` subscribes to the order signal and returns present-set order.
    #[tokio::test]
    async fn keys_is_reactive_and_ordered() {
        let ctx = AsyncContext::new();
        let fam: AsyncSourceMap<u64, bool> = AsyncSourceMap::new(&ctx);
        let f = fam.clone();
        let seen = ctx.computed_async(move |actx| {
            let ks = f.keys(&actx);
            async move { ks }
        });
        assert!(ctx.get_async(&seen).await.is_empty());

        for k in [3u64, 1, 2] {
            fam.set(&ctx, k, true);
        }
        assert_eq!(ctx.get_async(&seen).await, vec![3, 1, 2]);
    }

    /// A bare `AsyncContext` read is untracked by design — the same discipline
    /// the sync map expresses through `ComputeOps`. Pinning it so the generic
    /// read surface is not silently collapsed to one behaviour later.
    #[tokio::test]
    async fn bare_context_read_is_untracked_but_correct() {
        let ctx = AsyncContext::new();
        let fam: AsyncSourceMap<u64, bool> = AsyncSourceMap::new(&ctx);
        assert_eq!(fam.len(&ctx), 0);
        fam.set(&ctx, 1, true);
        assert_eq!(fam.len(&ctx), 1);
        assert!(fam.contains_key(&ctx, &1));
        assert_eq!(fam.keys(&ctx), vec![1]);
    }

    #[tokio::test]
    async fn entry_observe_tracks_only_through_compute_context() {
        let ctx = AsyncContext::new();
        let fam: AsyncSourceMap<u64, i64> = AsyncSourceMap::new(&ctx);
        fam.set(&ctx, 1, 10);
        let entry = fam.handle(&1).expect("source entry");

        assert_eq!(fam.observe(&ctx, &1), Some(10));
        assert_eq!(
            ctx.dependent_count(&entry),
            0,
            "a bare-context observe must not create an edge"
        );

        let reads = Arc::new(AtomicU64::new(0));
        let f = fam.clone();
        let observed_reads = Arc::clone(&reads);
        let observed = ctx.computed_async(move |actx| {
            observed_reads.fetch_add(1, Ordering::SeqCst);
            let value = f.observe(&actx, &1).expect("tracked source entry");
            async move { value * 2 }
        });

        assert_eq!(ctx.get_async(&observed).await, 20);
        assert_eq!(ctx.dependent_count(&entry), 1);
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        fam.set(&ctx, 1, 11);
        assert_eq!(ctx.get_async(&observed).await, 22);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn resolved_computed_entry_observe_tracks_downstream() {
        let ctx = AsyncContext::new();
        let upstream = ctx.source(5i64);
        let fam: AsyncComputedMap<u64, i64> = AsyncComputedMap::new(&ctx);
        let entry = fam.get_or_insert_handle(&ctx, 1, move |actx, _| actx.get(&upstream) * 2);
        assert_eq!(ctx.get_async(&entry).await, 10);

        let f = fam.clone();
        let downstream = ctx.computed_async(move |actx| {
            let value = f.observe(&actx, &1).expect("resolved computed entry");
            async move { value + 1 }
        });
        assert_eq!(ctx.get_async(&downstream).await, 11);
        assert_eq!(ctx.dependent_count(&entry), 1);

        ctx.set(&upstream, 6);
        assert!(
            !ctx.is_set(&downstream),
            "entry invalidation must propagate to its observe reader"
        );
        assert_eq!(ctx.get_async(&entry).await, 12);
        assert_eq!(ctx.get_async(&downstream).await, 13);
    }

    #[test]
    fn map_is_send_sync() {
        assert_send_sync::<AsyncSourceMap<u64, bool>>();
        assert_send_sync::<AsyncComputedMap<u64, usize>>();
    }

    #[tokio::test]
    async fn eager_source_map_resolves_immediately() {
        let ctx = AsyncContext::new();
        let fam: AsyncSourceMap<u64, bool> = AsyncSourceMap::new(&ctx);
        for k in [1u64, 2, 3] {
            fam.set(&ctx, k, true);
        }
        assert_eq!(fam.entry_kind(), EntryKind::Source);
        assert_eq!(fam.present_count(), 3);
        assert_eq!(fam.observe(&ctx, &2), Some(true));
        assert_eq!(fam.present_keys(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn lazy_computed_map_defers_until_read() {
        let ctx = AsyncContext::new();
        let fam: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx);
        assert_eq!(fam.present_count(), 0);
        // Materialize + drive to resolution.
        let handle = fam.get_or_insert_handle(&ctx, 4, |_, k| (*k as usize) * 10);
        assert!(fam.is_present(&4));
        assert_eq!(fam.present_count(), 1);
        assert_eq!(ctx.get_async(&handle).await, 40);
    }

    #[tokio::test]
    async fn eventual_transparency_eager_equals_lazy() {
        let ctx_e = AsyncContext::new();
        let eager: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx_e);
        eager.materialize_all(&ctx_e, [1, 2, 3], |_, k| (*k as usize) * 2);
        let ctx_l = AsyncContext::new();
        let lazy: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx_l);
        for k in [1u64, 2, 3] {
            let ve = ctx_e.get_async(&eager.handle(&k).unwrap()).await;
            let vl = ctx_l
                .get_async(&lazy.get_or_insert_handle(&ctx_l, k, |_, k| (*k as usize) * 2))
                .await;
            assert_eq!(ve, vl);
        }
    }

    #[tokio::test]
    async fn present_set_grows_monotonically() {
        let ctx = AsyncContext::new();
        let fam: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx);
        let _ = fam.get_or_insert_handle(&ctx, 5, |_, k| *k as usize);
        let _ = fam.get_or_insert_handle(&ctx, 5, |_, k| *k as usize);
        let _ = fam.get_or_insert_handle(&ctx, 9, |_, k| *k as usize);
        assert_eq!(fam.present_count(), 2);
        assert_eq!(fam.present_keys(), vec![5, 9]);
    }

    #[tokio::test]
    async fn source_map_reacts_to_set() {
        let ctx = AsyncContext::new();
        let fam: AsyncSourceMap<u64, bool> = AsyncSourceMap::new(&ctx);
        for k in [10u64, 20] {
            fam.set(&ctx, k, true);
        }
        assert_eq!(fam.observe(&ctx, &20), Some(true));
        fam.set(&ctx, 20, false);
        assert_eq!(fam.observe(&ctx, &20), Some(false));
    }
}
