//! Thread-safe keyed reactive collection (`#reactivemap`, thread-safe flavor).
//!
//! The `Send + Sync` analog of [`ReactiveMap`](crate::ReactiveMap): keys `K`
//! map to per-entry reactive nodes ([`Source<V>`] input cells / [`Computed<V>`]
//! derived slots) allocated on a [`ThreadSafeContext`]. Where [`ReactiveMap`] is
//! `Rc`-based and single-threaded, this map keeps its present-set state behind an
//! `Arc<Mutex<..>>`, so it can live in a `Send` owner shared across threads (for
//! example a relay hub stored behind a global mutex, where an `Rc`-based map
//! cannot go).
//!
//! It obeys the same materialization laws as the single-threaded map:
//! - **Eager/lazy behavior:** eager pre-mints every declared node
//!   ([`materialize_all`](ThreadSafeReactiveMap::materialize_all)); lazy defers
//!   derived (slot) nodes to first read
//!   ([`get_or_insert_with`](ThreadSafeReactiveMap::get_or_insert_with)). There is
//!   no eager/lazy mode flag.
//! - **Observational transparency:** a read returns an identical value whether the
//!   entry was pre-minted or minted on access.
//! - **Present-set monotonicity:** the materialized set only grows (deferral,
//!   never de-allocation).
//!
//! Its two specializations are [`ThreadSafeSourceMap`] (input cells) and
//! [`ThreadSafeComputedMap`] (derived slots). Mirrors the `ThreadSafeComputedMap`
//! conformance case in lazily-spec and the `Materialization` proofs (plus
//! **confluence**) in lazily-formal.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::cell_family::EntryKind;
use crate::{Computed, Source, ThreadSafeContext};

mod sealed {
    pub trait Sealed {}
}

/// The node kinds a thread-safe map entry can take — the `Send + Sync` analog of
/// [`MapHandle`](crate::MapHandle). Sealed to [`Source`] (input cells) and
/// [`Computed`] (derived slots); bindings do not add new kinds.
pub trait ThreadSafeMapHandle<V>: sealed::Sealed + Copy + Send + Sync + 'static {
    /// This handle's entry kind. `Source` is [`EntryKind::Cell`]; `Computed`
    /// is [`EntryKind::Slot`].
    const KIND: EntryKind;

    /// Allocate the node for one entry on `ctx`, with `compute` producing its
    /// canonical value. An input cell sets the value directly; a derived slot wraps
    /// `compute` as its recomputation. The closure is `Send + Sync` so a slot's
    /// recompute can run on any thread the context is driven from.
    fn materialize(
        ctx: &ThreadSafeContext,
        compute: impl Fn(&ThreadSafeContext) -> V + Send + Sync + 'static,
    ) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static;

    /// Read this entry's value through its owning context (subscribes the caller as
    /// any cell/slot read does).
    fn observe(self, ctx: &ThreadSafeContext) -> V
    where
        V: Clone + Send + Sync + 'static;
}

impl<V> sealed::Sealed for Source<V> {}
impl<V: Send + Sync + 'static> ThreadSafeMapHandle<V> for Source<V> {
    const KIND: EntryKind = EntryKind::Cell;

    fn materialize(
        ctx: &ThreadSafeContext,
        compute: impl Fn(&ThreadSafeContext) -> V + Send + Sync + 'static,
    ) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        // An input has no derivation: materialize by setting its value directly.
        ctx.source(compute(ctx))
    }

    fn observe(self, ctx: &ThreadSafeContext) -> V
    where
        V: Clone + Send + Sync + 'static,
    {
        ctx.get(&self)
    }
}

impl<V> sealed::Sealed for Computed<V> {}
impl<V: Send + Sync + 'static> ThreadSafeMapHandle<V> for Computed<V> {
    const KIND: EntryKind = EntryKind::Slot;

    fn materialize(
        ctx: &ThreadSafeContext,
        compute: impl Fn(&ThreadSafeContext) -> V + Send + Sync + 'static,
    ) -> Self
    where
        V: PartialEq + Clone + Send + Sync + 'static,
    {
        // A derived node: the same node an eager pre-mint would allocate.
        ctx.computed(compute)
    }

    fn observe(self, ctx: &ThreadSafeContext) -> V
    where
        V: Clone + Send + Sync + 'static,
    {
        ctx.get(&self)
    }
}

/// Present-set state, guarded by the map's `Mutex`.
struct MapState<K, H> {
    /// Currently-allocated entries (the "present" set). Grows on materialize,
    /// never shrinks silently — deferral, not de-allocation.
    materialized: HashMap<K, H>,
    /// Insertion order of the present set (stable snapshot for `present_keys`).
    order: Vec<K>,
}

struct MapInner<K, H> {
    state: Mutex<MapState<K, H>>,
}

/// The thread-safe keyed reactive collection (`#reactivemap`) generic over the
/// entry handle kind `H` ([`Source<V>`] for input cells, [`Computed<V>`] for
/// derived slots).
///
/// Cheap to [`Clone`] (an `Arc` to shared inner state) and `Send + Sync`, so it can
/// be captured by compute/effect closures and stored in a cross-thread owner.
/// Operations run against the owning [`ThreadSafeContext`].
///
/// See the module docs for the eager/lazy behavior and the
/// [`ThreadSafeSourceMap`]/[`ThreadSafeComputedMap`] kind specializations.
pub struct ThreadSafeReactiveMap<K, V, H> {
    inner: Arc<MapInner<K, H>>,
    _marker: PhantomData<V>,
}

impl<K, V, H> Clone for ThreadSafeReactiveMap<K, V, H> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }
}

impl<K, V, H> ThreadSafeReactiveMap<K, V, H>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
    H: ThreadSafeMapHandle<V>,
{
    /// Create an empty map bound to `ctx`.
    pub fn new(_ctx: &ThreadSafeContext) -> Self {
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
        ctx: &ThreadSafeContext,
        key: K,
        compute: impl Fn(&ThreadSafeContext) -> V + Send + Sync + 'static,
    ) -> H {
        // Fast path: already allocated. Release the lock before touching `ctx` so a
        // slot recompute triggered by materialization can never re-enter this lock.
        {
            let state = self.inner.state.lock().expect("map state mutex poisoned");
            if let Some(handle) = state.materialized.get(&key) {
                return *handle; // warm: already allocated.
            }
        }
        let handle = H::materialize(ctx, compute);
        let mut state = self.inner.state.lock().expect("map state mutex poisoned");
        // Lost a materialization race for this key: first writer wins so the key keeps
        // a stable handle (cell-identity). Our freshly-allocated node is orphaned in
        // `ctx` (unreferenced, never observed) — a rare, harmless cost.
        if let Some(existing) = state.materialized.get(&key) {
            return *existing;
        }
        state.materialized.insert(key.clone(), handle);
        state.order.push(key);
        handle
    }

    /// Get the entry handle for `key`, minting it via `factory(&key)` on first
    /// access (the lazy pull) and caching it. Returns the same handle on repeat.
    pub fn get_or_insert_handle(
        &self,
        ctx: &ThreadSafeContext,
        key: K,
        factory: impl Fn(&K) -> V + Send + Sync + 'static,
    ) -> H {
        let k = key.clone();
        self.mint_with(ctx, key, move |_ctx| factory(&k))
    }

    /// Get the value at `key`, minting the entry via `factory(&key)` first if
    /// absent. For a [`ThreadSafeComputedMap`] this is the lazy materialization pull.
    pub fn get_or_insert_with(
        &self,
        ctx: &ThreadSafeContext,
        key: K,
        factory: impl Fn(&K) -> V + Send + Sync + 'static,
    ) -> V {
        self.get_or_insert_handle(ctx, key, factory).observe(ctx)
    }

    /// Observe `key`'s value if the entry is present, else `None`. Non-minting.
    pub fn observe(&self, ctx: &ThreadSafeContext, key: &K) -> Option<V> {
        let handle = {
            let state = self.inner.state.lock().expect("map state mutex poisoned");
            state.materialized.get(key).copied()
        };
        handle.map(|h| h.observe(ctx))
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

    /// Whether `key` is currently materialized (present in the allocated set).
    /// Non-reactive.
    pub fn is_present(&self, key: &K) -> bool {
        self.inner
            .state
            .lock()
            .expect("map state mutex poisoned")
            .materialized
            .contains_key(key)
    }

    /// The currently-materialized keys, in first-materialization order. The present
    /// set only grows (deferral, not de-allocation).
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

    /// This map's entry kind ([`EntryKind::Cell`] for a cell map,
    /// [`EntryKind::Slot`] for a slot map).
    pub fn entry_kind(&self) -> EntryKind {
        H::KIND
    }
}

/// `ThreadSafeSourceMap`-only surface: `set` (an input is settable).
impl<K, V> ThreadSafeReactiveMap<K, V, Source<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// Set the value at `key`, inserting a new input cell if absent. Cell-only.
    pub fn set(&self, ctx: &ThreadSafeContext, key: K, value: V) {
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

/// `ThreadSafeComputedMap`-only surface: the eager pre-mint helper.
impl<K, V> ThreadSafeReactiveMap<K, V, Computed<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: PartialEq + Clone + Send + Sync + 'static,
{
    /// **Eager materialization**: pre-mint a derived slot for every key in `keys`.
    /// Observationally identical to minting each lazily on first read.
    pub fn materialize_all(
        &self,
        ctx: &ThreadSafeContext,
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

/// A thread-safe **input-cell** map: every entry is an always-materialized
/// [`Source<V>`]. The `Send + Sync` analog of [`SourceMap`](crate::SourceMap).
pub type ThreadSafeSourceMap<K, V> = ThreadSafeReactiveMap<K, V, Source<V>>;

/// A thread-safe **derived-slot** map: entries are [`Computed<V>`] minted lazily
/// on access or eagerly via [`materialize_all`](ThreadSafeReactiveMap::materialize_all).
pub type ThreadSafeComputedMap<K, V> = ThreadSafeReactiveMap<K, V, Computed<V>>;

/// Deprecated alias for [`ThreadSafeSourceMap`].
#[deprecated(note = "renamed to ThreadSafeSourceMap")]
pub type ThreadSafeCellMap<K, V> = ThreadSafeSourceMap<K, V>;

/// Deprecated alias for [`ThreadSafeComputedMap`].
#[deprecated(note = "renamed to ThreadSafeComputedMap")]
pub type ThreadSafeSlotMap<K, V> = ThreadSafeComputedMap<K, V>;

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn map_is_send_sync() {
        // The whole point: a thread-safe map can live in a `Send + Sync` owner.
        assert_send_sync::<ThreadSafeSourceMap<u64, bool>>();
        assert_send_sync::<ThreadSafeComputedMap<u64, usize>>();
    }

    #[test]
    fn eager_source_map_materializes_all_at_build() {
        let ctx = ThreadSafeContext::new();
        let fam: ThreadSafeSourceMap<u64, bool> = ThreadSafeSourceMap::new(&ctx);
        for k in [1u64, 2, 3] {
            fam.set(&ctx, k, true);
        }
        assert_eq!(fam.entry_kind(), EntryKind::Cell);
        assert_eq!(fam.present_count(), 3);
        assert!(fam.is_present(&1) && fam.is_present(&2) && fam.is_present(&3));
        assert_eq!(fam.present_keys(), vec![1, 2, 3]);
    }

    #[test]
    fn lazy_computed_map_defers_until_read() {
        let ctx = ThreadSafeContext::new();
        // Empty map + lazy → nothing materialized until observed.
        let fam: ThreadSafeComputedMap<u64, usize> = ThreadSafeComputedMap::new(&ctx);
        assert_eq!(fam.present_count(), 0);
        assert!(!fam.is_present(&2));
        assert_eq!(fam.get_or_insert_with(&ctx, 2, |k| (*k as usize) * 10), 20);
        assert!(fam.is_present(&2));
        assert_eq!(fam.present_count(), 1);
    }

    #[test]
    fn eager_computed_map_materializes_all_up_front() {
        let ctx = ThreadSafeContext::new();
        let fam: ThreadSafeComputedMap<u64, usize> = ThreadSafeComputedMap::new(&ctx);
        fam.materialize_all(&ctx, [7, 8], |k| *k as usize);
        assert_eq!(fam.present_count(), 2);
    }

    #[test]
    fn observational_transparency_eager_equals_lazy() {
        let ctx_e = ThreadSafeContext::new();
        let eager: ThreadSafeComputedMap<u64, usize> = ThreadSafeComputedMap::new(&ctx_e);
        eager.materialize_all(&ctx_e, [1, 2, 3], |k| (*k as usize) * 2);
        let ctx_l = ThreadSafeContext::new();
        let lazy: ThreadSafeComputedMap<u64, usize> = ThreadSafeComputedMap::new(&ctx_l);
        for k in [1u64, 2, 3] {
            let ve = eager.observe(&ctx_e, &k).unwrap();
            let vl = lazy.get_or_insert_with(&ctx_l, k, |k| (*k as usize) * 2);
            assert_eq!(ve, vl);
        }
    }

    #[test]
    fn present_set_grows_monotonically() {
        let ctx = ThreadSafeContext::new();
        let fam: ThreadSafeComputedMap<u64, usize> = ThreadSafeComputedMap::new(&ctx);
        let _ = fam.get_or_insert_with(&ctx, 5, |k| *k as usize);
        let _ = fam.get_or_insert_with(&ctx, 5, |k| *k as usize); // repeat: no growth
        let _ = fam.get_or_insert_with(&ctx, 9, |k| *k as usize);
        assert_eq!(fam.present_count(), 2);
        assert_eq!(fam.present_keys(), vec![5, 9]);
    }

    #[test]
    fn derived_count_reacts_to_cell_writes() {
        // The agent-doc liveness shape: cell inputs + a derived count that recomputes
        // reactively when a cell flips — no pull-time scan.
        let ctx = ThreadSafeContext::new();
        let liveness: ThreadSafeSourceMap<u64, bool> = ThreadSafeSourceMap::new(&ctx);
        for k in [10u64, 20, 30] {
            liveness.set(&ctx, k, true);
        }
        let live_count = {
            let liveness = liveness.clone();
            ctx.computed(move |c| {
                liveness
                    .present_keys()
                    .into_iter()
                    .filter(|k| liveness.observe(c, k).unwrap_or(false))
                    .count()
            })
        };
        assert_eq!(ctx.get(&live_count), 3);
        // Flip one editor offline → derived count recomputes reactively.
        let h20 = liveness.handle(&20).unwrap();
        ctx.set(&h20, false);
        assert_eq!(ctx.get(&live_count), 2);
        ctx.set(&h20, true);
        assert_eq!(ctx.get(&live_count), 3);
    }

    #[test]
    fn shared_across_threads() {
        use std::thread;
        let ctx = Arc::new(ThreadSafeContext::new());
        let fam: ThreadSafeSourceMap<u64, bool> = ThreadSafeSourceMap::new(&ctx);
        for k in [1u64, 2, 3, 4] {
            fam.set(&ctx, k, true);
        }
        let handles: Vec<_> = (1u64..=4)
            .map(|k| {
                let fam = fam.clone();
                let ctx = Arc::clone(&ctx);
                thread::spawn(move || fam.observe(&ctx, &k).unwrap())
            })
            .collect();
        for h in handles {
            assert!(h.join().unwrap());
        }
        assert_eq!(fam.present_count(), 4);
    }
}
