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
//! [`observe`](AsyncReactiveMap::observe) returns [`Option<V>`]. Input cells are
//! always resolved. Drive a slot to resolution with [`AsyncContext::get_async`] on
//! the handle from [`get_or_insert_handle`](AsyncReactiveMap::get_or_insert_handle).
//!
//! Its two specializations are [`AsyncSourceMap`] (input cells) and [`AsyncComputedMap`]
//! (derived slots). Mirrors the async materialization case in lazily-spec and the
//! `AsyncMaterialization` proofs (eventual transparency) in lazily-formal.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::cell_family::EntryKind;
use crate::{AsyncComputed, AsyncContext, AsyncSource};

mod sealed {
    pub trait Sealed {}
}

/// The node kinds an async map entry can take — the [`AsyncContext`] analog of
/// [`MapHandle`](crate::MapHandle). Sealed to [`AsyncSource`] (input cells)
/// and [`AsyncComputed`] (derived slots).
pub trait AsyncMapHandle<V>: sealed::Sealed + Copy + Send + Sync + 'static {
    /// This handle's entry kind. `AsyncSource` is [`EntryKind::Cell`] (always
    /// resolved); `AsyncComputed` is [`EntryKind::Slot`] (resolves asynchronously).
    const KIND: EntryKind;

    /// Allocate the node for one entry on `ctx`. `compute` is the per-key value
    /// producer; a cell sets the value directly, a derived slot wraps it in a ready
    /// future as its async recomputation.
    fn materialize(ctx: &AsyncContext, compute: Arc<dyn Fn() -> V + Send + Sync>) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static;

    /// Non-blocking read: `Some(value)` for a materialized cell or a resolved slot,
    /// `None` for a slot still pending. Drive a pending slot to resolution with
    /// [`AsyncContext::get_async`].
    fn observe(self, ctx: &AsyncContext) -> Option<V>
    where
        V: Clone + Send + Sync + 'static;
}

impl<V> sealed::Sealed for AsyncSource<V> {}
impl<V: Send + Sync + 'static> AsyncMapHandle<V> for AsyncSource<V> {
    const KIND: EntryKind = EntryKind::Cell;

    fn materialize(ctx: &AsyncContext, compute: Arc<dyn Fn() -> V + Send + Sync>) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        ctx.source(compute())
    }

    fn observe(self, ctx: &AsyncContext) -> Option<V>
    where
        V: Clone + Send + Sync + 'static,
    {
        Some(ctx.get(&self))
    }
}

impl<V> sealed::Sealed for AsyncComputed<V> {}
impl<V: Send + Sync + 'static> AsyncMapHandle<V> for AsyncComputed<V> {
    const KIND: EntryKind = EntryKind::Slot;

    fn materialize(ctx: &AsyncContext, compute: Arc<dyn Fn() -> V + Send + Sync>) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        // A derived node whose async recompute is a ready future of the sync value.
        ctx.computed_async(move |_actx| {
            let v = compute();
            async move { v }
        })
    }

    fn observe(self, ctx: &AsyncContext) -> Option<V>
    where
        V: Clone + Send + Sync + 'static,
    {
        ctx.get(&self)
    }
}

/// Present-set state, guarded by the map's `Mutex`.
struct MapState<K, H> {
    materialized: HashMap<K, H>,
    order: Vec<K>,
}

struct MapInner<K, H> {
    state: Mutex<MapState<K, H>>,
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
    pub fn new(_ctx: &AsyncContext) -> Self {
        Self {
            inner: Arc::new(MapInner {
                state: Mutex::new(MapState {
                    materialized: HashMap::new(),
                    order: Vec::new(),
                }),
            }),
            _marker: PhantomData,
        }
    }

    fn mint_with(
        &self,
        ctx: &AsyncContext,
        key: K,
        compute: Arc<dyn Fn() -> V + Send + Sync>,
    ) -> H {
        // Fast path under the lock; release before touching `ctx`.
        {
            let state = self.inner.state.lock().expect("map state mutex poisoned");
            if let Some(handle) = state.materialized.get(&key) {
                return *handle;
            }
        }
        let handle = H::materialize(ctx, compute);
        let mut state = self.inner.state.lock().expect("map state mutex poisoned");
        // First writer wins on a race so the key keeps a stable handle.
        if let Some(existing) = state.materialized.get(&key) {
            return *existing;
        }
        state.materialized.insert(key.clone(), handle);
        state.order.push(key);
        handle
    }

    /// Get the entry handle for `key`, minting it via `factory(&key)` on first
    /// access and caching it. For a slot map this is the [`AsyncComputed`] to
    /// drive with [`AsyncContext::get_async`].
    pub fn get_or_insert_handle(
        &self,
        ctx: &AsyncContext,
        key: K,
        factory: impl Fn(&K) -> V + Send + Sync + 'static,
    ) -> H {
        let k = key.clone();
        let compute: Arc<dyn Fn() -> V + Send + Sync> = Arc::new(move || factory(&k));
        self.mint_with(ctx, key, compute)
    }

    /// Non-blocking observe of an existing entry: `Some(value)` for a cell or
    /// resolved slot, `None` for a pending slot or an absent key. Non-minting.
    pub fn observe(&self, ctx: &AsyncContext, key: &K) -> Option<V> {
        let handle = {
            let state = self.inner.state.lock().expect("map state mutex poisoned");
            state.materialized.get(key).copied()
        };
        handle.and_then(|h| h.observe(ctx))
    }

    /// Return the existing entry handle for `key`, or `None`. Non-minting.
    pub fn handle(&self, key: &K) -> Option<H> {
        self.inner
            .state
            .lock()
            .expect("map state mutex poisoned")
            .materialized
            .get(key)
            .copied()
    }

    /// Whether `key` is currently materialized (present). Non-reactive.
    pub fn is_present(&self, key: &K) -> bool {
        self.inner
            .state
            .lock()
            .expect("map state mutex poisoned")
            .materialized
            .contains_key(key)
    }

    /// The currently-materialized keys, in first-materialization order.
    pub fn present_keys(&self) -> Vec<K> {
        self.inner
            .state
            .lock()
            .expect("map state mutex poisoned")
            .order
            .clone()
    }

    /// Number of currently-materialized entries.
    pub fn present_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("map state mutex poisoned")
            .order
            .len()
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
        let existing = {
            let state = self.inner.state.lock().expect("map state mutex poisoned");
            state.materialized.get(&key).copied()
        };
        if let Some(handle) = existing {
            ctx.set(&handle, value);
            return;
        }
        self.get_or_insert_handle(ctx, key, move |_| value.clone());
    }
}

/// `AsyncComputedMap`-only surface: the eager pre-mint helper.
impl<K, V> AsyncReactiveMap<K, V, AsyncComputed<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// **Eager materialization**: pre-mint a derived slot for every key in `keys`.
    pub fn materialize_all(
        &self,
        ctx: &AsyncContext,
        keys: impl IntoIterator<Item = K>,
        factory: impl Fn(&K) -> V + Send + Sync + 'static,
    ) {
        let factory = Arc::new(factory);
        for key in keys {
            let f = Arc::clone(&factory);
            self.get_or_insert_handle(ctx, key, move |k| f(k));
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
        assert_eq!(fam.entry_kind(), EntryKind::Cell);
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
        let handle = fam.get_or_insert_handle(&ctx, 4, |k| (*k as usize) * 10);
        assert!(fam.is_present(&4));
        assert_eq!(fam.present_count(), 1);
        assert_eq!(ctx.get_async(&handle).await, 40);
    }

    #[tokio::test]
    async fn eventual_transparency_eager_equals_lazy() {
        let ctx_e = AsyncContext::new();
        let eager: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx_e);
        eager.materialize_all(&ctx_e, [1, 2, 3], |k| (*k as usize) * 2);
        let ctx_l = AsyncContext::new();
        let lazy: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx_l);
        for k in [1u64, 2, 3] {
            let ve = ctx_e.get_async(&eager.handle(&k).unwrap()).await;
            let vl = ctx_l
                .get_async(&lazy.get_or_insert_handle(&ctx_l, k, |k| (*k as usize) * 2))
                .await;
            assert_eq!(ve, vl);
        }
    }

    #[tokio::test]
    async fn present_set_grows_monotonically() {
        let ctx = AsyncContext::new();
        let fam: AsyncComputedMap<u64, usize> = AsyncComputedMap::new(&ctx);
        let _ = fam.get_or_insert_handle(&ctx, 5, |k| *k as usize);
        let _ = fam.get_or_insert_handle(&ctx, 5, |k| *k as usize);
        let _ = fam.get_or_insert_handle(&ctx, 9, |k| *k as usize);
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
