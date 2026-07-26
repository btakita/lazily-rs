//! The keyed-collection ordering contract replayed against **all three
//! execution models** — `SourceMap` (single-threaded), `ThreadSafeSourceMap`,
//! and `AsyncSourceMap`.
//!
//! `tests/collections_conformance.rs` replays the same two fixtures against the
//! single-threaded flavor only. That is how the gap this suite closes stayed
//! invisible: `coverage.json` marks the ordering rows green for every binding,
//! but the only executable evidence anywhere in the family is one flavor of one
//! binding. A contract verified on one of three flavors is a contract verified
//! nowhere in particular.
//!
//! The structure mirrors `tests/reactive_graph/` (and lazily-zig's
//! `Engine(comptime Model)`): one replay engine written against a [`MapModel`]
//! trait, with three implementations whose only asynchronous obligation is
//! isolated to [`MapModel::settle`], which is a no-op on the two synchronous
//! flavors.
//!
//! What is asserted per step, from the fixture:
//! - resulting `order` and `membership`;
//! - the `values` that changed;
//! - `invalidates` — the reader-class independence contract: exactly the listed
//!   value readers recompute, and membership / order readers match their flags.
//!   A pure reorder must invalidate order readers and leave membership readers
//!   cached;
//! - `handle_stable` — an atomic move keeps the entry's node identity rather
//!   than remove + re-mint.

#![cfg(all(feature = "thread-safe", feature = "async"))]

mod common;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use lazily::{
    AsyncComputed, AsyncContext, AsyncSource, AsyncSourceMap, Computed, Context, Source, SourceMap,
    ThreadSafeContext, ThreadSafeSourceMap,
};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

/// Entry value type used across all collection fixtures (JSON integers).
type V = i64;

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn spec_fixtures_present() -> bool {
    std::path::Path::new(SPEC_DIR).exists()
}

/// One execution model's keyed source map, projected onto the operations and
/// observations the collection fixtures need.
///
/// Every method is synchronous except [`settle`](MapModel::settle): only the
/// async flavor has to drive its readers to resolution, and isolating that to a
/// single method is what makes one replay engine cover all three models.
trait MapModel: Sized {
    /// This model's entry handle — compared for identity by `handle_stable`.
    type Handle: PartialEq + Debug;
    /// A reader tracking one entry's value.
    type ValueReader;
    /// A reader tracking set membership (`len`).
    type MembershipReader;
    /// A reader tracking key order (`keys`).
    type OrderReader;

    /// Human-readable flavor name, used in assertion messages.
    const FLAVOR: &'static str;

    fn new() -> Self;

    // -- mutations (the fixture op set) -----------------------------------
    fn set_value(&self, key: &str, value: V);
    fn remove(&self, key: &str);
    fn move_to(&self, key: &str, index: usize);
    fn move_before(&self, key: &str, anchor: &str);
    fn move_after(&self, key: &str, anchor: &str);

    // -- untracked observations -------------------------------------------
    fn keys(&self) -> Vec<String>;
    fn value(&self, key: &str) -> Option<V>;
    fn handle(&self, key: &str) -> Option<Self::Handle>;

    // -- readers ------------------------------------------------------------
    fn value_reader(&self, key: &str) -> Self::ValueReader;
    fn membership_reader(&self) -> Self::MembershipReader;
    fn order_reader(&self) -> Self::OrderReader;

    fn value_cached(&self, reader: &Self::ValueReader) -> bool;
    fn membership_cached(&self, reader: &Self::MembershipReader) -> bool;
    fn order_cached(&self, reader: &Self::OrderReader) -> bool;

    /// Drive every reader to a settled, cached state so the next op's
    /// invalidation is measured against a fully primed graph.
    ///
    /// The synchronous flavors settle by reading; the async flavor must await
    /// each computed cell's resolution. This is the *only* async-coloured
    /// obligation in the whole contract — ordering itself is not.
    #[allow(async_fn_in_trait)]
    async fn settle(
        &self,
        values: &HashMap<String, Self::ValueReader>,
        membership: &Self::MembershipReader,
        order: &Self::OrderReader,
    );
}

// ---------------------------------------------------------------------------
// Single-threaded
// ---------------------------------------------------------------------------

struct SyncModel {
    ctx: Context,
    map: SourceMap<String, V>,
}

impl MapModel for SyncModel {
    type Handle = Source<V>;
    type ValueReader = Computed<Option<V>>;
    type MembershipReader = Computed<usize>;
    type OrderReader = Computed<Vec<String>>;

    const FLAVOR: &'static str = "SourceMap";

    fn new() -> Self {
        let ctx = Context::new();
        let map = SourceMap::new(&ctx);
        Self { ctx, map }
    }

    fn set_value(&self, key: &str, value: V) {
        self.map.set(&self.ctx, key.to_string(), value);
    }

    fn remove(&self, key: &str) {
        self.map.remove(&self.ctx, &key.to_string());
    }

    fn move_to(&self, key: &str, index: usize) {
        self.map.move_to(&self.ctx, &key.to_string(), index);
    }

    fn move_before(&self, key: &str, anchor: &str) {
        self.map
            .move_before(&self.ctx, &key.to_string(), &anchor.to_string());
    }

    fn move_after(&self, key: &str, anchor: &str) {
        self.map
            .move_after(&self.ctx, &key.to_string(), &anchor.to_string());
    }

    fn keys(&self) -> Vec<String> {
        self.map.keys(&self.ctx)
    }

    fn value(&self, key: &str) -> Option<V> {
        self.map.get(&self.ctx, &key.to_string())
    }

    fn handle(&self, key: &str) -> Option<Self::Handle> {
        self.map.handle(&key.to_string())
    }

    fn value_reader(&self, key: &str) -> Self::ValueReader {
        let map = self.map.clone();
        let key = key.to_string();
        self.ctx.computed(move |ctx| map.get(ctx, &key))
    }

    fn membership_reader(&self) -> Self::MembershipReader {
        let map = self.map.clone();
        self.ctx.computed(move |ctx| map.len(ctx))
    }

    fn order_reader(&self) -> Self::OrderReader {
        let map = self.map.clone();
        self.ctx.computed(move |ctx| map.keys(ctx))
    }

    fn value_cached(&self, reader: &Self::ValueReader) -> bool {
        self.ctx.is_set(reader)
    }

    fn membership_cached(&self, reader: &Self::MembershipReader) -> bool {
        self.ctx.is_set(reader)
    }

    fn order_cached(&self, reader: &Self::OrderReader) -> bool {
        self.ctx.is_set(reader)
    }

    async fn settle(
        &self,
        values: &HashMap<String, Self::ValueReader>,
        membership: &Self::MembershipReader,
        order: &Self::OrderReader,
    ) {
        for reader in values.values() {
            self.ctx.get(reader);
        }
        self.ctx.get(membership);
        self.ctx.get(order);
    }
}

// ---------------------------------------------------------------------------
// Thread-safe
// ---------------------------------------------------------------------------

struct ThreadSafeModel {
    ctx: ThreadSafeContext,
    map: ThreadSafeSourceMap<String, V>,
}

impl MapModel for ThreadSafeModel {
    type Handle = Source<V>;
    type ValueReader = Computed<Option<V>>;
    type MembershipReader = Computed<usize>;
    type OrderReader = Computed<Vec<String>>;

    const FLAVOR: &'static str = "ThreadSafeSourceMap";

    fn new() -> Self {
        let ctx = ThreadSafeContext::new();
        let map = ThreadSafeSourceMap::new(&ctx);
        Self { ctx, map }
    }

    fn set_value(&self, key: &str, value: V) {
        self.map.set(&self.ctx, key.to_string(), value);
    }

    fn remove(&self, key: &str) {
        self.map.remove(&self.ctx, &key.to_string());
    }

    fn move_to(&self, key: &str, index: usize) {
        self.map.move_to(&self.ctx, &key.to_string(), index);
    }

    fn move_before(&self, key: &str, anchor: &str) {
        self.map
            .move_before(&self.ctx, &key.to_string(), &anchor.to_string());
    }

    fn move_after(&self, key: &str, anchor: &str) {
        self.map
            .move_after(&self.ctx, &key.to_string(), &anchor.to_string());
    }

    fn keys(&self) -> Vec<String> {
        self.map.keys(&self.ctx)
    }

    fn value(&self, key: &str) -> Option<V> {
        self.map.observe(&self.ctx, &key.to_string())
    }

    fn handle(&self, key: &str) -> Option<Self::Handle> {
        self.map.handle(&key.to_string())
    }

    fn value_reader(&self, key: &str) -> Self::ValueReader {
        let map = self.map.clone();
        let key = key.to_string();
        self.ctx.computed(move |ctx| map.observe(ctx, &key))
    }

    fn membership_reader(&self) -> Self::MembershipReader {
        let map = self.map.clone();
        self.ctx.computed(move |ctx| map.len(ctx))
    }

    fn order_reader(&self) -> Self::OrderReader {
        let map = self.map.clone();
        self.ctx.computed(move |ctx| map.keys(ctx))
    }

    fn value_cached(&self, reader: &Self::ValueReader) -> bool {
        self.ctx.is_set(reader)
    }

    fn membership_cached(&self, reader: &Self::MembershipReader) -> bool {
        self.ctx.is_set(reader)
    }

    fn order_cached(&self, reader: &Self::OrderReader) -> bool {
        self.ctx.is_set(reader)
    }

    async fn settle(
        &self,
        values: &HashMap<String, Self::ValueReader>,
        membership: &Self::MembershipReader,
        order: &Self::OrderReader,
    ) {
        for reader in values.values() {
            self.ctx.get(reader);
        }
        self.ctx.get(membership);
        self.ctx.get(order);
    }
}

// ---------------------------------------------------------------------------
// Async
// ---------------------------------------------------------------------------

struct AsyncModel {
    ctx: AsyncContext,
    map: AsyncSourceMap<String, V>,
}

impl MapModel for AsyncModel {
    type Handle = AsyncSource<V>;
    type ValueReader = AsyncComputed<Option<V>>;
    type MembershipReader = AsyncComputed<usize>;
    type OrderReader = AsyncComputed<Vec<String>>;

    const FLAVOR: &'static str = "AsyncSourceMap";

    fn new() -> Self {
        let ctx = AsyncContext::new();
        let map = AsyncSourceMap::new(&ctx);
        Self { ctx, map }
    }

    fn set_value(&self, key: &str, value: V) {
        self.map.set(&self.ctx, key.to_string(), value);
    }

    fn remove(&self, key: &str) {
        self.map.remove(&self.ctx, &key.to_string());
    }

    fn move_to(&self, key: &str, index: usize) {
        self.map.move_to(&self.ctx, &key.to_string(), index);
    }

    fn move_before(&self, key: &str, anchor: &str) {
        self.map
            .move_before(&self.ctx, &key.to_string(), &anchor.to_string());
    }

    fn move_after(&self, key: &str, anchor: &str) {
        self.map
            .move_after(&self.ctx, &key.to_string(), &anchor.to_string());
    }

    fn keys(&self) -> Vec<String> {
        self.map.keys(&self.ctx)
    }

    fn value(&self, key: &str) -> Option<V> {
        self.map.observe(&self.ctx, &key.to_string())
    }

    fn handle(&self, key: &str) -> Option<Self::Handle> {
        self.map.handle(&key.to_string())
    }

    /// Unlike `keys`/`len`/`contains_key`, the async map's `observe` is **not**
    /// generic over the read surface — it takes a bare `AsyncContext`, which
    /// registers no edge. A tracked per-entry read therefore has to go through
    /// the handle. (Worth closing: the asymmetry is not load-bearing.)
    fn value_reader(&self, key: &str) -> Self::ValueReader {
        let map = self.map.clone();
        let key = key.to_string();
        self.ctx.computed_async(move |actx| {
            let handle = map.handle(&key);
            let value = handle.map(|h| actx.get(&h));
            async move { value }
        })
    }

    fn membership_reader(&self) -> Self::MembershipReader {
        let map = self.map.clone();
        self.ctx.computed_async(move |actx| {
            let len = map.len(&actx);
            async move { len }
        })
    }

    fn order_reader(&self) -> Self::OrderReader {
        let map = self.map.clone();
        self.ctx.computed_async(move |actx| {
            let keys = map.keys(&actx);
            async move { keys }
        })
    }

    fn value_cached(&self, reader: &Self::ValueReader) -> bool {
        self.ctx.is_set(reader)
    }

    fn membership_cached(&self, reader: &Self::MembershipReader) -> bool {
        self.ctx.is_set(reader)
    }

    fn order_cached(&self, reader: &Self::OrderReader) -> bool {
        self.ctx.is_set(reader)
    }

    async fn settle(
        &self,
        values: &HashMap<String, Self::ValueReader>,
        membership: &Self::MembershipReader,
        order: &Self::OrderReader,
    ) {
        for reader in values.values() {
            let _ = self.ctx.get_async(reader).await;
        }
        let _ = self.ctx.get_async(membership).await;
        let _ = self.ctx.get_async(order).await;
    }
}

// ---------------------------------------------------------------------------
// The replay engine — one body, three models
// ---------------------------------------------------------------------------

fn str_of(v: &Value, field: &str) -> String {
    v.get(field)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("op missing string field `{field}`"))
        .to_string()
}

async fn run_steps_fixture<M: MapModel>(name: &str) {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} absent - run with the lazily-spec sibling");
        return;
    }
    let fixture = load_fixture(name);
    let flavor = M::FLAVOR;
    let model = M::new();

    // -- initial state --------------------------------------------------
    let initial = fixture.get("initial").expect("initial");
    let init_order = initial
        .get("order")
        .and_then(|v| v.as_array())
        .expect("initial.order");
    let init_values = initial
        .get("values")
        .and_then(|v| v.as_object())
        .expect("initial.values");
    for k in init_order {
        let key = k.as_str().expect("order key");
        let val = init_values
            .get(key)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("missing initial value for {key}"));
        model.set_value(key, val);
    }

    let steps = fixture
        .get("steps")
        .and_then(|v| v.as_array())
        .expect("steps");
    assert!(
        !steps.is_empty(),
        "{flavor}: fixture {name} has no steps - a vacuous replay would report green"
    );

    for (i, step) in steps.iter().enumerate() {
        let op = step.get("op").expect("op");
        let expected = step.get("expected").expect("expected");

        // Rebuild + prime readers from the CURRENT key set so each step's
        // invalidation is measured in isolation.
        let current_keys = model.keys();
        let value_readers: HashMap<String, M::ValueReader> = current_keys
            .iter()
            .map(|k| (k.clone(), model.value_reader(k)))
            .collect();
        let membership_reader = model.membership_reader();
        let order_reader = model.order_reader();
        model
            .settle(&value_readers, &membership_reader, &order_reader)
            .await;

        // Every reader must actually be primed, or "stayed cached" below would
        // be trivially false and "was invalidated" trivially true.
        for (key, reader) in &value_readers {
            assert!(
                model.value_cached(reader),
                "{flavor} step {i}: value reader for `{key}` failed to prime"
            );
        }
        assert!(
            model.membership_cached(&membership_reader),
            "{flavor} step {i}: membership reader failed to prime"
        );
        assert!(
            model.order_cached(&order_reader),
            "{flavor} step {i}: order reader failed to prime"
        );

        // Snapshot node identities before the op for `handle_stable`.
        let handles_before: HashMap<String, Option<M::Handle>> = current_keys
            .iter()
            .map(|k| (k.clone(), model.handle(k)))
            .collect();

        apply_op(&model, op);

        // -- invalidation (reader-class independence) --------------------
        let invalidates = expected
            .get("invalidates")
            .unwrap_or_else(|| panic!("step {i}: expected.invalidates missing from {name}"));
        let survivors: HashSet<String> = model.keys().into_iter().collect();
        let value_invalidated: HashSet<String> = invalidates
            .get("value")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().map(|v| v.as_str().unwrap().to_string()).collect())
            .unwrap_or_default();

        for key in &survivors {
            let Some(reader) = value_readers.get(key) else {
                continue; // key added by this op: no reader existed to invalidate
            };
            let cached = model.value_cached(reader);
            if value_invalidated.contains(key) {
                assert!(
                    !cached,
                    "{flavor} step {i}: value reader for `{key}` should have been invalidated"
                );
            } else {
                assert!(
                    cached,
                    "{flavor} step {i}: value reader for `{key}` should have stayed cached \
                     (unrelated change)"
                );
            }
        }

        let membership_expected = invalidates
            .get("membership")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert_eq!(
            !model.membership_cached(&membership_reader),
            membership_expected,
            "{flavor} step {i}: membership reader invalidation mismatch \
             (a pure reorder must NOT invalidate set-identity readers)"
        );

        let order_expected = invalidates
            .get("order")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert_eq!(
            !model.order_cached(&order_reader),
            order_expected,
            "{flavor} step {i}: order reader invalidation mismatch"
        );

        // -- handle stability (atomic move keeps node identity) ----------
        if let Some(stable) = expected.get("handle_stable").and_then(|v| v.as_object()) {
            for (key, want) in stable {
                if !want.as_bool().unwrap_or(false) {
                    continue;
                }
                let before = handles_before
                    .get(key)
                    .unwrap_or_else(|| panic!("no handle captured for `{key}` before op"));
                let after = model.handle(key);
                assert!(
                    after.is_some(),
                    "{flavor} step {i}: handle_stable{{{key}}} violated - handle missing after op"
                );
                assert_eq!(
                    &after, before,
                    "{flavor} step {i}: handle_stable{{{key}}} violated - node identity changed \
                     across an atomic move (remove + re-mint instead of reorder)"
                );
            }
        }

        // -- resulting state ---------------------------------------------
        if let Some(order) = expected.get("order").and_then(|v| v.as_array()) {
            let want: Vec<String> = order
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            assert_eq!(model.keys(), want, "{flavor} step {i}: order mismatch");
        }
        if let Some(membership) = expected.get("membership").and_then(|v| v.as_array()) {
            let want: HashSet<String> = membership
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            let got: HashSet<String> = model.keys().into_iter().collect();
            assert_eq!(got, want, "{flavor} step {i}: membership mismatch");
        }
        if let Some(values) = expected.get("values").and_then(|v| v.as_object()) {
            for (key, val) in values {
                let want = val
                    .as_i64()
                    .unwrap_or_else(|| panic!("non-integer value for {key}"));
                let got = model
                    .value(key)
                    .unwrap_or_else(|| panic!("{flavor} step {i}: missing key {key} after op"));
                assert_eq!(got, want, "{flavor} step {i}: value mismatch for {key}");
            }
        }
    }
}

fn apply_op<M: MapModel>(model: &M, op: &Value) {
    match op.get("type").and_then(|v| v.as_str()).expect("op.type") {
        "set_value" => {
            let key = str_of(op, "key");
            let val = op.get("value").and_then(|v| v.as_i64()).expect("op.value");
            model.set_value(&key, val);
        }
        "insert" => {
            let key = str_of(op, "key");
            let val = op.get("value").and_then(|v| v.as_i64()).expect("op.value");
            model.set_value(&key, val);
            // `at` is optional: "end" (where a fresh key lands) or a 0-based index.
            if let Some(idx) = op.get("at").and_then(|v| v.as_u64()) {
                model.move_to(&key, idx as usize);
            }
        }
        "remove" => model.remove(&str_of(op, "key")),
        "move_to" => {
            let idx = op.get("index").and_then(|v| v.as_u64()).expect("op.index");
            model.move_to(&str_of(op, "key"), idx as usize);
        }
        "move_before" => model.move_before(&str_of(op, "key"), &str_of(op, "before")),
        "move_after" => model.move_after(&str_of(op, "key"), &str_of(op, "after")),
        other => panic!("unknown collection op type: {other}"),
    }
}

// ---------------------------------------------------------------------------
// The 2 x 3 matrix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn atomic_move_sync() {
    run_steps_fixture::<SyncModel>("cellmap_atomic_move.json").await;
}

#[tokio::test]
async fn atomic_move_thread_safe() {
    run_steps_fixture::<ThreadSafeModel>("cellmap_atomic_move.json").await;
}

#[tokio::test]
async fn atomic_move_async() {
    run_steps_fixture::<AsyncModel>("cellmap_atomic_move.json").await;
}

#[tokio::test]
async fn independence_sync() {
    run_steps_fixture::<SyncModel>("cellmap_independence.json").await;
}

#[tokio::test]
async fn independence_thread_safe() {
    run_steps_fixture::<ThreadSafeModel>("cellmap_independence.json").await;
}

#[tokio::test]
async fn independence_async() {
    run_steps_fixture::<AsyncModel>("cellmap_independence.json").await;
}
