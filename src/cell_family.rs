//! Keyed reactive collections: the generic [`ReactiveMap`] and its
//! [`SourceMap`] / [`ComputedMap`] specializations (`#reactivemap`).
//!
//! `Context` addresses nodes by opaque [`SlotId`](crate::context). These types
//! add a *keyed* layer on top: a hash collection whose **membership is itself
//! reactive**, with one independently-tracked reactive node per entry.
//!
//! # One primitive, two specializations
//!
//! There is a single keyed primitive, generic over the entry's **handle kind**
//! `H` (the [`MapHandle`] trait, implemented by [`Source`] for input cells
//! and [`Computed`] for derived slots):
//!
//! - **[`SourceMap<K, V>`] = `ReactiveMap<K, V, Source<V>>`** — **input-cell**
//!   entries. Adds cell-only [`set`](ReactiveMap::set) and eager value-minting
//!   ([`entry`](ReactiveMap::entry) / [`entry_with`](ReactiveMap::entry_with)).
//! - **[`ComputedMap<K, V>`] = `ReactiveMap<K, V, Computed<V>>`** — **derived-slot**
//!   entries. [`get_or_insert_with`](ReactiveMap::get_or_insert_with) mints a
//!   slot on first access (**lazy materialization**); a slot's value is derived,
//!   so `ComputedMap` has **no `set`**. Eager materialization is a pre-mint loop over
//!   the keyset ([`materialize_all`](ReactiveMap::materialize_all)); lazy is
//!   mint-on-access. There is **no eager/lazy mode flag**.
//!
//! The shared surface — `get_or_insert_with` / `remove` / `move_*` / membership /
//! order / `keys` / `len` / `contains_key` — lives on the generic `ReactiveMap`.
//! `set` and eager value-minting are the `SourceMap`-only specialization; the
//! pre-mint eager helper is the `ComputedMap`-only specialization.
//!
//! # Fine-grained vs. coarse
//!
//! Modelling a collection as a single `ctx.source(HashMap<K, V>)` is *coarse*:
//! every single-entry mutation replaces the whole map, so any reader of any
//! entry is invalidated and (over a wire) the entire map is re-sent.
//!
//! [`ReactiveMap`] is *fine-grained*. Each entry is its own reactive node, so:
//!
//! - A reader that depends on entry `a` is **not** invalidated when entry `b`
//!   changes — only that entry's dependents recompute.
//! - Membership (the set of keys) is tracked by a dedicated version cell, so
//!   [`keys`](ReactiveMap::keys) / [`len`](ReactiveMap::len) readers recompute
//!   only when keys are **added or removed**, not when an existing value changes.
//!
//! ```
//! use lazily::{SourceMap, Context};
//!
//! let ctx = Context::new();
//! let scores: SourceMap<&'static str, i32> = SourceMap::new(&ctx);
//! let alice = scores.entry(&ctx, "alice", 10);
//! let bob = scores.entry(&ctx, "bob", 20);
//!
//! // A computed over the whole collection recomputes only on membership change.
//! let n = ctx.computed({
//!     let scores = scores.clone();
//!     move |ctx| scores.len(ctx)
//! });
//! assert_eq!(ctx.get(&n), 2);
//!
//! // Mutating an existing entry does not change membership.
//! alice.set(&ctx, 11);
//! assert_eq!(ctx.get(&n), 2);
//! assert_eq!(bob.get(&ctx), 20);
//! ```

use std::cell::{Cell as StdCell, RefCell};
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::Context;
use crate::cell::Computed;
use crate::cell::Source;
use crate::context::{Compute, ComputeOps};
use crate::keyed_order::{KeyedOrder, Move, Mutation};

/// Which kind of reactive node a [`ReactiveMap`] entry is — the handle-kind axis
/// the map abstracts over.
///
/// Mirrors `EntryKind` in `lazily-formal`'s `Materialization` module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// An **input** cell ([`Source`]) — always materialized on `get`.
    Source,
    /// A **derived** slot ([`Computed`]) — materialized eagerly (pre-mint) or
    /// lazily on first read.
    Computed,
}

/// Deprecated pre-v2 spellings of the [`EntryKind`] variants, kept as
/// associated constants so existing *value* uses (`EntryKind::Cell`,
/// comparisons, `const KIND` definitions) keep compiling with a rename note.
///
/// These are constants, not variants, so they cannot be used in **pattern**
/// position (`match k { EntryKind::Cell => .. }`); migrate such matches to
/// [`EntryKind::Source`] / [`EntryKind::Computed`].
impl EntryKind {
    /// Renamed to [`EntryKind::Source`].
    #[deprecated(since = "0.49.0", note = "renamed to EntryKind::Source")]
    #[allow(non_upper_case_globals)]
    pub const Cell: EntryKind = EntryKind::Source;

    /// Renamed to [`EntryKind::Computed`].
    #[deprecated(since = "0.49.0", note = "renamed to EntryKind::Computed")]
    #[allow(non_upper_case_globals)]
    pub const Slot: EntryKind = EntryKind::Computed;
}

mod sealed {
    pub trait Sealed {}
}

/// The entry-handle axis a [`ReactiveMap`] abstracts over. Implemented by
/// [`Source`] (input cells) and [`Computed`] (derived slots) only — the
/// two node kinds of the cell model. Sealed: bindings do not add new kinds.
pub trait MapHandle<V>: sealed::Sealed + Copy + 'static {
    /// This handle's entry kind. `Source` is [`EntryKind::Source`]; `Computed`
    /// is [`EntryKind::Computed`].
    const KIND: EntryKind;

    /// Allocate the node for one entry in `ctx`, with `compute` producing its
    /// canonical value. An input cell sets the value directly; a derived slot
    /// wraps `compute` as its recomputation.
    fn materialize(ctx: &Context, compute: impl Fn(&Compute) -> V + 'static) -> Self
    where
        V: PartialEq + Clone + 'static;

    /// Read this entry's value through any reactive surface (subscribes the
    /// caller as any cell/slot read does when `ctx` is a [`Compute`]).
    fn observe<C: ComputeOps>(self, ctx: &C) -> V
    where
        V: Clone + 'static;

    /// Detach this entry's node from the graph on removal — clear its cached
    /// value and its dependents.
    fn clear_dependents(self, ctx: &Context);
}

impl<V> sealed::Sealed for Source<V> {}
impl<V: 'static> MapHandle<V> for Source<V> {
    const KIND: EntryKind = EntryKind::Source;

    fn materialize(ctx: &Context, compute: impl Fn(&Compute) -> V + 'static) -> Self
    where
        V: PartialEq + Clone + 'static,
    {
        // An input has no derivation: materialize by setting its value directly.
        // Evaluated once, detached (untracked) — an input cell's seed value is
        // not a dependency edge.
        ctx.source(ctx.eval_detached(compute))
    }

    fn observe<C: ComputeOps>(self, ctx: &C) -> V
    where
        V: Clone + 'static,
    {
        self.get(ctx)
    }

    fn clear_dependents(self, ctx: &Context) {
        Source::clear_dependents(&self, ctx);
    }
}

impl<V> sealed::Sealed for Computed<V> {}
impl<V: 'static> MapHandle<V> for Computed<V> {
    const KIND: EntryKind = EntryKind::Computed;

    fn materialize(ctx: &Context, compute: impl Fn(&Compute) -> V + 'static) -> Self
    where
        V: PartialEq + Clone + 'static,
    {
        // A derived node: the same node an eager pre-mint would allocate.
        ctx.computed(compute)
    }

    fn observe<C: ComputeOps>(self, ctx: &C) -> V
    where
        V: Clone + 'static,
    {
        self.get(ctx)
    }

    fn clear_dependents(self, ctx: &Context) {
        self.clear(ctx);
    }
}

/// A keyed reactive collection generic over the entry handle kind `H`: a hash map
/// of `K -> H` with reactive membership and independently-tracked per-entry nodes.
///
/// Cheap to [`Clone`] (an `Rc` to the shared inner state) so it can be captured
/// by compute/effect closures. All operations are taken against the owning
/// [`Context`]; like the rest of `lazily`, the graph data lives in the context.
///
/// The two specializations a binding exposes are [`SourceMap`] (input cells) and
/// [`ComputedMap`] (derived slots). See the module docs.
pub struct ReactiveMap<K, V, H> {
    inner: Rc<ReactiveMapInner<K, H>>,
    _marker: PhantomData<V>,
}

struct ReactiveMapInner<K, H> {
    /// Present set + authoritative key order + the move algebra, shared verbatim
    /// with the thread-safe and async flavors. Graph-agnostic and closure-free;
    /// this flavor's only contribution is the `RefCell` around it.
    core: RefCell<KeyedOrder<K, H>>,
    /// Reactive *set-membership* signal. Holds a monotonic version bumped only
    /// when the **set** of keys changes (add/remove). Reading it (in
    /// `len`/`contains_key`/`is_empty`) subscribes the caller to membership
    /// changes without coupling to entry values *or to pure reordering*.
    membership: Source<u64>,
    /// Plain (untracked) mirror of the membership version so mutators can bump
    /// the reactive cell without registering a spurious dependency.
    version: StdCell<u64>,
    /// Reactive *order* signal. Bumped on add/remove **and on move/reorder**.
    /// `keys` subscribes here so an atomic ordered move (`#lzcellmove`)
    /// invalidates key-order readers without disturbing `len`/`contains_key`
    /// readers that only care about set identity.
    order_signal: Source<u64>,
    /// Untracked mirror of the order version.
    order_version: StdCell<u64>,
}

impl<K, V, H> Clone for ReactiveMap<K, V, H> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            _marker: PhantomData,
        }
    }
}

impl<K, V, H> ReactiveMap<K, V, H>
where
    K: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
    H: MapHandle<V>,
{
    /// Create an empty collection bound to `ctx`.
    pub fn new(ctx: &Context) -> Self {
        Self {
            inner: Rc::new(ReactiveMapInner {
                core: RefCell::new(KeyedOrder::new()),
                membership: ctx.source(0u64),
                version: StdCell::new(0),
                order_signal: ctx.source(0u64),
                order_version: StdCell::new(0),
            }),
            _marker: PhantomData,
        }
    }

    /// Bump the *order* signal (invalidates `keys` readers). Add/remove also
    /// bump this; a pure move bumps **only** this.
    fn bump_order(&self, ctx: &Context) {
        let next = self.inner.order_version.get().wrapping_add(1);
        self.inner.order_version.set(next);
        ctx.set(&self.inner.order_signal, next);
    }

    /// Bump set-membership (invalidates `len`/`contains_key` readers). Always
    /// paired with an order bump because add/remove change order too.
    fn bump_membership(&self, ctx: &Context) {
        let next = self.inner.version.get().wrapping_add(1);
        self.inner.version.set(next);
        // A write, not a tracked read: membership readers are invalidated, but
        // no dependency is registered on whatever frame called the mutator.
        ctx.set(&self.inner.membership, next);
        // The key set changed, so the ordered key list changed too.
        self.bump_order(ctx);
    }

    /// Mint the entry node for `key` (via `H::materialize` with `compute` as its
    /// canonical value producer) on first access, caching the handle and bumping
    /// reactive membership. Re-minting an existing key returns the cached handle.
    fn mint_with(&self, ctx: &Context, key: K, compute: impl Fn(&Compute) -> V + 'static) -> H {
        if let Some(handle) = self.inner.core.borrow().get(&key) {
            return handle; // warm: already allocated.
        }
        let handle = H::materialize(ctx, compute);
        let (handle, mutation) = self.inner.core.borrow_mut().insert(key, handle);
        if mutation == Mutation::Changed {
            self.bump_membership(ctx);
        }
        handle
    }

    /// Get the value at `key`, minting the entry via `factory(compute, &key)`
    /// first if the key is absent — the mint-on-access recipe. For a
    /// [`ComputedMap`] this is the **lazy materialization** pull; for a
    /// [`SourceMap`] it seeds an input cell.
    ///
    /// The factory's first parameter is the entry's own tracking view: reads of
    /// other reactives through it register dependency edges *on this entry*, so a
    /// derived entry can be genuinely derived. Ignore it (`|_, key| …`) for a
    /// constant-per-key factory.
    ///
    /// Bumps reactive membership only on insert; an existing key returns its
    /// current value without re-running the factory.
    pub fn get_or_insert_with(
        &self,
        ctx: &Context,
        key: K,
        factory: impl Fn(&Compute, &K) -> V + 'static,
    ) -> V {
        if let Some(handle) = self.inner.core.borrow().get(&key) {
            return handle.observe(ctx);
        }
        let k = key.clone();
        let handle = self.mint_with(ctx, key, move |ctx| factory(ctx, &k));
        handle.observe(ctx)
    }

    /// Return the existing entry handle for `key`, or `None`. Non-reactive: this
    /// does not subscribe the caller to membership.
    pub fn handle(&self, key: &K) -> Option<H> {
        self.inner.core.borrow().get(key)
    }

    /// Read the value at `key` if present. Reactive on that entry only (a reader
    /// is invalidated when this entry changes, not when siblings change).
    pub fn get<C: ComputeOps>(&self, ctx: &C, key: &K) -> Option<V> {
        let handle = self.inner.core.borrow().get(key);
        handle.map(|h| h.observe(ctx))
    }

    /// Remove `key`'s entry. Bumps reactive membership and clears the removed
    /// entry's dependents. Returns whether the key was present.
    ///
    /// Note: the underlying node id is not recycled (the runtime exposes no
    /// node-free API yet); the orphaned node stops driving any dependents.
    pub fn remove(&self, ctx: &Context, key: &K) -> bool {
        let (removed, mutation) = self.inner.core.borrow_mut().remove(key);
        let Some(handle) = removed else {
            return false;
        };
        handle.clear_dependents(ctx);
        if mutation == Mutation::Changed {
            self.bump_membership(ctx);
        }
        true
    }

    /// Reactive snapshot of the keys in their current order. Subscribes the
    /// caller to **order** changes (add/remove **and move/reorder**), not to
    /// per-entry value changes.
    pub fn keys<C: ComputeOps>(&self, ctx: &C) -> Vec<K> {
        let _ = self.inner.order_signal.get(ctx);
        self.inner.core.borrow().keys()
    }

    /// The currently-materialized (present) keys, in first-materialization order.
    /// Non-reactive; the present set only grows (deferral, not de-allocation).
    pub fn present_keys(&self) -> Vec<K> {
        self.inner.core.borrow().keys()
    }

    /// Number of currently-materialized (present) entries. Non-reactive.
    pub fn present_count(&self) -> usize {
        self.inner.core.borrow().len()
    }

    /// Whether `key` is currently materialized (present in the allocated set).
    /// Non-reactive.
    pub fn is_present(&self, key: &K) -> bool {
        self.inner.core.borrow().contains(key)
    }

    /// Current 0-based position of `key` in the order, or `None` if absent.
    /// Non-reactive.
    pub fn position(&self, key: &K) -> Option<usize> {
        self.inner.core.borrow().position(key)
    }

    /// Atomically move `key` to `index` in the order (`#lzcellmove`).
    ///
    /// This is the *atomic, optimized* reorder: the entry keeps the **same**
    /// node, the same dependents, and its CRDT lineage — unlike the naive
    /// `remove` + re-mint which re-allocates the node and bumps membership twice.
    /// Only the order signal is bumped (once), so `keys` readers recompute but
    /// `len`/`contains_key` readers — which track set identity, not order —
    /// stay cached.
    ///
    /// `index` is clamped to `[0, len)`. Returns whether `key` was present.
    pub fn move_to(&self, ctx: &Context, key: &K, index: usize) -> bool {
        let outcome = self.inner.core.borrow_mut().move_to(key, index);
        self.settle_move(ctx, outcome)
    }

    /// Bump the order signal iff the order actually changed, and report whether
    /// the move could be expressed. Shared by all three `move_*` entry points.
    fn settle_move(&self, ctx: &Context, outcome: Move) -> bool {
        if outcome.changed() {
            self.bump_order(ctx);
        }
        outcome.is_present()
    }

    /// Atomically move `key` to just before `anchor` in the order
    /// (`#lzcellmove`). No-op if either key is absent or already adjacent in the
    /// requested position. Returns whether the move could be expressed.
    pub fn move_before(&self, ctx: &Context, key: &K, anchor: &K) -> bool {
        let outcome = self.inner.core.borrow_mut().move_before(key, anchor);
        self.settle_move(ctx, outcome)
    }

    /// Atomically move `key` to just after `anchor` in the order (`#lzcellmove`).
    pub fn move_after(&self, ctx: &Context, key: &K, anchor: &K) -> bool {
        let outcome = self.inner.core.borrow_mut().move_after(key, anchor);
        self.settle_move(ctx, outcome)
    }

    /// Reactive entry count. Subscribes the caller to membership changes only.
    pub fn len<C: ComputeOps>(&self, ctx: &C) -> usize {
        let _ = self.inner.membership.get(ctx);
        self.inner.core.borrow().len()
    }

    /// Reactive emptiness check. Subscribes the caller to membership changes.
    pub fn is_empty<C: ComputeOps>(&self, ctx: &C) -> bool {
        self.len(ctx) == 0
    }

    /// Reactive membership test for `key`. Subscribes the caller to membership
    /// changes (add/remove of any key), not to value changes.
    pub fn contains_key<C: ComputeOps>(&self, ctx: &C, key: &K) -> bool {
        let _ = self.inner.membership.get(ctx);
        self.inner.core.borrow().contains(key)
    }

    /// Non-reactive count. Does not subscribe the caller to anything.
    pub fn len_untracked(&self) -> usize {
        self.inner.core.borrow().len()
    }

    /// This map's entry kind ([`EntryKind::Source`] for a [`SourceMap`],
    /// [`EntryKind::Computed`] for a [`ComputedMap`]).
    pub fn entry_kind(&self) -> EntryKind {
        H::KIND
    }
}

/// A keyed **input-cell** collection: every entry is a settable [`Source<V>`].
///
/// The `SourceMap` specialization of [`ReactiveMap`] adds cell-only `set` and eager
/// value-minting (`entry` / `entry_with`) on top of the shared reactive keyed
/// surface.
pub type SourceMap<K, V> = ReactiveMap<K, V, Source<V>>;

/// A keyed **derived-slot** collection: every entry is a [`Computed<V>`] whose
/// value is derived. `get_or_insert_with` mints a slot on first access (lazy
/// materialization); [`materialize_all`](ReactiveMap::materialize_all) pre-mints
/// the keyset (eager). A slot's value is derived, so `ComputedMap` has **no `set`**.
pub type ComputedMap<K, V> = ReactiveMap<K, V, Computed<V>>;

/// Deprecated alias for [`SourceMap`]. The v2 kernel renamed the node kinds to
/// `Source`/`Computed`, so the map names now say which kind of entry they hold.
#[deprecated(note = "renamed to SourceMap")]
pub type CellMap<K, V> = SourceMap<K, V>;

/// Deprecated alias for [`ComputedMap`].
#[deprecated(note = "renamed to ComputedMap")]
pub type SlotMap<K, V> = ComputedMap<K, V>;

/// `SourceMap`-only surface: eager value-minting and `set` (an input is settable).
impl<K, V> ReactiveMap<K, V, Source<V>>
where
    K: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
{
    /// Return the value cell for `key`, minting it with `default` (computed via
    /// the closure) on first access. Subsequent calls return the cached handle.
    ///
    /// Adding a new key bumps reactive membership; re-fetching an existing key
    /// does not. Cell-only: eager value-minting has no derived-slot analog.
    pub fn entry_with(&self, ctx: &Context, key: K, default: impl FnOnce() -> V) -> Source<V> {
        if let Some(handle) = self.inner.core.borrow().get(&key) {
            return handle;
        }
        let value = default();
        self.mint_with(ctx, key, move |_ctx| value.clone())
    }

    /// Return the value cell for `key`, minting it with `default` on first
    /// access. Convenience wrapper over [`entry_with`](Self::entry_with).
    pub fn entry(&self, ctx: &Context, key: K, default: V) -> Source<V> {
        self.entry_with(ctx, key, || default)
    }

    /// Set the value at `key`, inserting a new entry (and bumping membership) if
    /// it does not exist yet. Updating an existing entry leaves membership
    /// untouched and invalidates only that entry's dependents.
    ///
    /// Cell-only: an input is settable; a derived [`ComputedMap`] slot is not.
    pub fn set(&self, ctx: &Context, key: K, value: V) {
        if let Some(handle) = self.inner.core.borrow().get(&key) {
            handle.set(ctx, value);
            return;
        }
        self.entry_with(ctx, key, || value);
    }
}

/// `ComputedMap`-only surface: the eager pre-mint helper. Lazy materialization is
/// [`get_or_insert_with`](ReactiveMap::get_or_insert_with) on the shared surface.
impl<K, V> ReactiveMap<K, V, Computed<V>>
where
    K: Eq + Hash + Clone + 'static,
    V: PartialEq + Clone + 'static,
{
    /// **Eager materialization**: pre-mint a derived slot for every key in
    /// `keys` via `factory`, up front. Observationally identical to minting each
    /// key lazily on first read — it only changes *when* the nodes are allocated.
    ///
    /// `factory` takes the entry's own tracking view, exactly as
    /// [`get_or_insert_with`](ReactiveMap::get_or_insert_with) does; eager and
    /// lazy differ in timing only, so they must not differ in what the factory
    /// can depend on.
    pub fn materialize_all(
        &self,
        ctx: &Context,
        keys: impl IntoIterator<Item = K>,
        factory: impl Fn(&Compute, &K) -> V + 'static,
    ) {
        let factory = Rc::new(factory);
        for key in keys {
            let f = Rc::clone(&factory);
            self.get_or_insert_with(ctx, key, move |ctx, k| f(ctx, k));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_caches_one_cell_per_key() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        let a1 = map.entry(&ctx, "a", 1);
        let a2 = map.entry(&ctx, "a", 999);
        // Same key -> same cell; the second default is ignored.
        assert_eq!(a1.id, a2.id);
        assert_eq!(a1.get(&ctx), 1);
        assert_eq!(map.len_untracked(), 1);
    }

    #[test]
    fn get_or_insert_with_mints_once_then_returns_existing() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        let calls = Rc::new(StdCell::new(0));
        // First access mints via the factory.
        assert_eq!(
            map.get_or_insert_with(&ctx, "a", {
                let calls = Rc::clone(&calls);
                move |_, _| {
                    calls.set(calls.get() + 1);
                    7
                }
            }),
            7
        );
        assert_eq!(map.len_untracked(), 1);
        // Second access returns the existing value; factory is NOT called again.
        assert_eq!(
            map.get_or_insert_with(&ctx, "a", {
                let calls = Rc::clone(&calls);
                move |_, _| {
                    calls.set(calls.get() + 1);
                    999
                }
            }),
            7
        );
        assert_eq!(calls.get(), 1);
        // An explicit set is observed by a subsequent get_or_insert_with.
        map.set(&ctx, "a", 42);
        assert_eq!(map.get_or_insert_with(&ctx, "a", |_, _| 0), 42);
    }

    #[test]
    fn membership_is_reactive_but_value_changes_are_not() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        let a = map.entry(&ctx, "a", 1);
        map.entry(&ctx, "b", 2);

        let count = ctx.computed({
            let map = map.clone();
            move |ctx| map.len(ctx)
        });
        assert_eq!(ctx.get(&count), 2);

        // Mutating an existing entry must NOT invalidate the membership reader.
        a.set(&ctx, 100);
        assert!(ctx.is_set(&count), "membership reader stayed cached");
        assert_eq!(ctx.get(&count), 2);

        // Adding a key DOES invalidate it.
        map.entry(&ctx, "c", 3);
        assert_eq!(ctx.get(&count), 3);

        // Removing a key invalidates it too.
        assert!(map.remove(&ctx, &"b"));
        assert_eq!(ctx.get(&count), 2);
        assert_eq!(map.keys(&ctx), vec!["a", "c"]);
    }

    #[test]
    fn per_entry_reads_are_independent() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        let a = map.entry(&ctx, "a", 1);
        let b = map.entry(&ctx, "b", 2);

        let view_a = ctx.computed({
            let map = map.clone();
            move |ctx| map.get(ctx, &"a").unwrap_or(0) * 10
        });
        assert_eq!(ctx.get(&view_a), 10);

        // Changing b must not invalidate a's reader.
        b.set(&ctx, 222);
        assert!(ctx.is_set(&view_a), "sibling change must not invalidate");
        assert_eq!(ctx.get(&view_a), 10);

        // Changing a does.
        a.set(&ctx, 5);
        assert_eq!(ctx.get(&view_a), 50);
    }

    #[test]
    fn computed_map_mints_lazily_and_caches() {
        let ctx = Context::new();
        let fam: ComputedMap<u32, u32> = ComputedMap::new(&ctx);
        // Nothing present until first access.
        assert_eq!(fam.present_count(), 0);
        assert_eq!(fam.get_or_insert_with(&ctx, 7, |_, &k| k * 2), 14);
        assert_eq!(fam.present_count(), 1);
        assert!(fam.is_present(&7));
        // Same key -> same derived slot (value preserved, factory not re-run).
        let h = fam.handle(&7).unwrap();
        assert_eq!(h.get(&ctx), 14);
        assert_eq!(fam.get_or_insert_with(&ctx, 7, |_, &k| k * 999), 14);
    }

    #[test]
    fn computed_map_materialize_all_is_eager() {
        let ctx = Context::new();
        let fam: ComputedMap<u32, u32> = ComputedMap::new(&ctx);
        fam.materialize_all(&ctx, [0u32, 1, 2, 5, 9], |_, &k| k * 3);
        assert_eq!(fam.present_count(), 5);
        for k in [0u32, 1, 2, 5, 9] {
            assert!(fam.is_present(&k));
        }
        assert_eq!(fam.get(&ctx, &5), Some(15));
        assert_eq!(fam.entry_kind(), EntryKind::Computed);
    }

    /// The pre-v2 spellings survive as deprecated aliases and denote exactly the
    /// same values, so an un-migrated caller's `EntryKind::Cell` is still the
    /// input-cell kind.
    #[test]
    #[allow(deprecated)]
    fn deprecated_entry_kind_aliases_denote_the_same_values() {
        assert_eq!(EntryKind::Cell, EntryKind::Source);
        assert_eq!(EntryKind::Slot, EntryKind::Computed);
        assert_eq!(<Source<u32> as MapHandle<u32>>::KIND, EntryKind::Cell);
        assert_eq!(<Computed<u32> as MapHandle<u32>>::KIND, EntryKind::Slot);
    }

    #[test]
    fn move_to_reorders_keys_and_keeps_cell_identity() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        let a = map.entry(&ctx, "a", 1);
        map.entry(&ctx, "b", 2);
        map.entry(&ctx, "c", 3);
        assert_eq!(map.keys(&ctx), vec!["a", "b", "c"]);

        // Move "c" to the front.
        assert!(map.move_to(&ctx, &"c", 0));
        assert_eq!(map.keys(&ctx), vec!["c", "a", "b"]);

        // The moved entry keeps the SAME value cell (identity + value intact).
        assert_eq!(map.handle(&"a").unwrap().id, a.id);
        assert_eq!(map.get(&ctx, &"a"), Some(1));
        assert_eq!(map.get(&ctx, &"c"), Some(3));

        // Absent key -> false, no reorder.
        assert!(!map.move_to(&ctx, &"z", 0));
        assert_eq!(map.keys(&ctx), vec!["c", "a", "b"]);
    }

    #[test]
    fn pure_move_invalidates_order_but_not_membership_readers() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        map.entry(&ctx, "a", 1);
        map.entry(&ctx, "b", 2);
        map.entry(&ctx, "c", 3);

        let order_reader = ctx.computed({
            let map = map.clone();
            move |ctx| map.keys(ctx).join(",")
        });
        let count = ctx.computed({
            let map = map.clone();
            move |ctx| map.len(ctx)
        });
        let has_b = ctx.computed({
            let map = map.clone();
            move |ctx| map.contains_key(ctx, &"b")
        });
        assert_eq!(ctx.get(&order_reader), "a,b,c");
        assert_eq!(ctx.get(&count), 3);
        assert!(ctx.get(&has_b));

        // A pure reorder must invalidate the order reader...
        assert!(map.move_to(&ctx, &"a", 2));
        assert_eq!(ctx.get(&order_reader), "b,c,a");
        // ...but NOT the set-identity readers (len / contains_key stay cached).
        assert!(
            ctx.is_set(&count),
            "len reader must stay cached on pure move"
        );
        assert!(
            ctx.is_set(&has_b),
            "contains_key reader must stay cached on pure move"
        );
        assert_eq!(ctx.get(&count), 3);
    }

    #[test]
    fn move_to_is_noop_when_position_unchanged() {
        let ctx = Context::new();
        let map: SourceMap<&str, i32> = SourceMap::new(&ctx);
        map.entry(&ctx, "a", 1);
        map.entry(&ctx, "b", 2);

        let order_reader = ctx.computed({
            let map = map.clone();
            move |ctx| map.keys(ctx).join(",")
        });
        assert_eq!(ctx.get(&order_reader), "a,b");

        // Moving to its current index is a no-op and must not invalidate.
        assert!(map.move_to(&ctx, &"a", 0));
        assert!(
            ctx.is_set(&order_reader),
            "no-op move must not invalidate keys readers"
        );
        // Index past the end clamps to last position.
        assert!(map.move_to(&ctx, &"a", 99));
        assert_eq!(ctx.get(&order_reader), "b,a");
    }

    #[test]
    fn move_before_and_after_place_relative_to_anchor() {
        let ctx = Context::new();
        let map: SourceMap<i32, i32> = SourceMap::new(&ctx);
        for k in 0..4 {
            map.entry(&ctx, k, k * 10);
        }
        assert_eq!(map.keys(&ctx), vec![0, 1, 2, 3]);

        // Move 3 before 1.
        assert!(map.move_before(&ctx, &3, &1));
        assert_eq!(map.keys(&ctx), vec![0, 3, 1, 2]);

        // Move 0 after 2.
        assert!(map.move_after(&ctx, &0, &2));
        assert_eq!(map.keys(&ctx), vec![3, 1, 2, 0]);

        // Unknown anchor / key -> false.
        assert!(!map.move_before(&ctx, &3, &99));
        assert!(!map.move_after(&ctx, &99, &2));
    }

    #[test]
    fn contains_key_tracks_membership() {
        let ctx = Context::new();
        let map: SourceMap<i32, i32> = SourceMap::new(&ctx);
        let has_5 = ctx.computed({
            let map = map.clone();
            move |ctx| map.contains_key(ctx, &5)
        });
        assert!(!ctx.get(&has_5));
        map.entry(&ctx, 5, 50);
        assert!(ctx.get(&has_5));
    }
}
