//! Async `AsyncComputedMap` materialization conformance (`#reactivemap`, async
//! flavor). Replays the canonical fixtures in
//! `lazily-spec/conformance/materialization/` through [`AsyncComputedMap`], proving
//! the async flavor obeys the same present-set materialization laws and the
//! **eventual transparency** law proved in `lazily-formal`'s
//! `AsyncMaterialization` module: a driven (resolved) async slot observes the
//! canonical value, identical whether pre-minted (eager) or minted on access
//! (lazy).
#![cfg(feature = "async")]

mod common;

use std::collections::HashSet;

use common::Expect;
use lazily::{AsyncComputedMap, AsyncContext};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/materialization";
type V = i64;

fn present() -> bool {
    std::path::Path::new(SPEC_DIR).exists()
}

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    serde_json::from_str(
        &crate::common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}")),
    )
    .unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

fn val_entries(fixture: &Value) -> Vec<(String, V)> {
    fixture
        .get("spec")
        .and_then(|s| s.get("val"))
        .and_then(|v| v.as_object())
        .expect("spec.val")
        .iter()
        .map(|(k, v)| (k.clone(), v.as_i64().expect("int val")))
        .collect()
}

fn str_array(v: &Value, path: &str) -> Vec<String> {
    v.get(path)
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("missing array {path}"))
        .iter()
        .map(|k| k.as_str().expect("string").to_string())
        .collect()
}

/// `str_array` against a guarded assertion block, so the key is recorded.
fn want_strs(want: &Value, key: &str) -> Vec<String> {
    want.as_array()
        .unwrap_or_else(|| panic!("missing array {key}"))
        .iter()
        .map(|k| k.as_str().expect("array of strings").to_string())
        .collect()
}

fn as_set(keys: &[String]) -> HashSet<String> {
    keys.iter().cloned().collect()
}

fn lookup_fn(
    entries: Vec<(String, V)>,
) -> impl Fn(&lazily::AsyncComputeContext, &String) -> V + Clone + Send + Sync + 'static {
    // The tracking view is available to the factory but this fixture's values are
    // constants per key, so it goes unused here. What matters is that the map no
    // longer *severs* it.
    move |_actx: &lazily::AsyncComputeContext, k: &String| -> V {
        entries
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("no val for {k}"))
    }
}

/// An eager `AsyncComputedMap`: pre-mint the whole keyset.
fn eager_computed_map(
    ctx: &AsyncContext,
    keys: Vec<String>,
    entries: Vec<(String, V)>,
) -> AsyncComputedMap<String, V> {
    let map: AsyncComputedMap<String, V> = AsyncComputedMap::new(ctx);
    map.materialize_all(ctx, keys, lookup_fn(entries));
    map
}

/// Eventual transparency + present-set laws replayed through the async map:
/// eager materializes all, lazy defers all, and a driven slot resolves to the
/// canonical value identically whether pre-minted or minted on access.
#[tokio::test]
async fn eventual_transparency_async() {
    if !present() {
        eprintln!("skipping: {SPEC_DIR} absent");
        return;
    }
    let fixture = load("observational_transparency.json");
    let entries = val_entries(&fixture);
    let keys: Vec<String> = entries.iter().map(|(k, _)| k.clone()).collect();
    // ONE guard per fixture per runner (`#lzassertunknownkeys`); this test
    // consumes every key of the block, including the read-sequence one, so the
    // sibling test below cannot be the only place a key is read.
    let expected = Expect::new(
        format!("{SPEC_DIR}/observational_transparency.json"),
        "expected",
        fixture.get("expected").unwrap(),
    );
    let ctx_e = AsyncContext::new();
    let eager = eager_computed_map(&ctx_e, keys.clone(), entries.clone());
    let ctx_l = AsyncContext::new();
    let lazy: AsyncComputedMap<String, V> = AsyncComputedMap::new(&ctx_l);
    let lookup = lookup_fn(entries.clone());

    // default_mode_eager. The named strategy *selects the build*; the asserted
    // fact is that a map built that way is materialized at build time, so editing
    // the fixture changes the outcome (`#lzconsumednotasserted`).
    expected.assert_key_with("default_mode", |want| {
        let mode = want.as_str().expect("default_mode");
        let default_present = match mode {
            "eager" => {
                let c = AsyncContext::new();
                eager_computed_map(&c, keys.clone(), entries.clone()).present_count()
            }
            "lazy" => {
                let c = AsyncContext::new();
                AsyncComputedMap::<String, V>::new(&c).present_count()
            }
            other => panic!("unknown default_mode {other}"),
        };
        assert_eq!(
            default_present,
            keys.len(),
            "a map built the fixture's default way ({mode}) is materialized at build"
        );
    });

    // Present-set laws (allocation axis, unchanged by async resolution).
    assert_eq!(eager.present_count(), keys.len());
    expected.assert_key_with("eager_present", |want| {
        assert_eq!(
            as_set(&eager.present_keys()),
            as_set(&want_strs(want, "eager_present"))
        )
    });
    assert_eq!(lazy.present_count(), 0);

    // The read sequence, asserted here so `lazy_present_after_reads` is consumed
    // by the same guard as the rest of the block.
    let ctx_r = AsyncContext::new();
    let reads_map: AsyncComputedMap<String, V> = AsyncComputedMap::new(&ctx_r);
    let read_lookup = lookup_fn(val_entries(&fixture));
    for k in str_array(&fixture, "reads") {
        let _ = reads_map.get_or_insert_handle(&ctx_r, k, read_lookup.clone());
    }
    expected.assert_key_with("lazy_present_after_reads", |want| {
        assert_eq!(
            as_set(&reads_map.present_keys()),
            as_set(&want_strs(want, "lazy_present_after_reads"))
        )
    });

    // Eventual transparency: drive each slot; resolved value = canonical, and the
    // eager and lazy maps agree. The awaits keep this out of a closure, so the
    // key is marked through `assert_key_with` around the borrow of the block.
    let observe = expected.assert_key_with("observe", |want| want.as_object().unwrap().clone());
    for (k, want) in &observe {
        let want = want.as_i64().unwrap();
        let ve = ctx_e.get_async(&eager.handle(k).unwrap()).await;
        let vl = ctx_l
            .get_async(&lazy.get_or_insert_handle(&ctx_l, k.clone(), lookup.clone()))
            .await;
        assert_eq!(ve, want, "eager async observe {k}");
        assert_eq!(vl, want, "lazy async observe {k}");
    }
}

/// The lazy present set after the fixture read sequence is exactly the read keys
/// (deferral, not de-allocation) — same as the sync/thread-safe maps.
#[tokio::test]
async fn deferral_not_deallocation_async() {
    if !present() {
        eprintln!("skipping: {SPEC_DIR} absent");
        return;
    }
    let fixture = load("observational_transparency.json");
    // Guarded independently: this test asserts the deferral law only, so the
    // keys it does not read are named as consumed by `eventual_transparency_async`
    // in this same binary — whose own guard fails if it ever stops reading them.
    let expected = Expect::new(
        format!("{SPEC_DIR}/observational_transparency.json"),
        "expected",
        fixture.get("expected").unwrap(),
    );
    expected.excuse_key(
        "default_mode",
        "asserted by eventual_transparency_async in this binary, under its own guard",
    );
    expected.excuse_key(
        "eager_present",
        "asserted by eventual_transparency_async in this binary, under its own guard",
    );
    expected.excuse_key(
        "observe",
        "asserted by eventual_transparency_async in this binary, under its own guard",
    );
    let entries = val_entries(&fixture);

    let ctx = AsyncContext::new();
    let lazy: AsyncComputedMap<String, V> = AsyncComputedMap::new(&ctx);
    let lookup = lookup_fn(entries);
    for k in str_array(&fixture, "reads") {
        let _ = lazy.get_or_insert_handle(&ctx, k, lookup.clone());
    }
    expected.assert_key_with("lazy_present_after_reads", |want| {
        assert_eq!(
            as_set(&lazy.present_keys()),
            as_set(&want_strs(want, "lazy_present_after_reads"))
        )
    });
}
