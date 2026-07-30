//! Thread-safe `ThreadSafeComputedMap` materialization conformance (`#reactivemap`,
//! thread-safe flavor). Replays the canonical fixtures in
//! `lazily-spec/conformance/materialization/` through [`ThreadSafeComputedMap`],
//! proving the `Send + Sync` flavor obeys the same materialization laws as the
//! single-threaded map — plus **confluence** (the order-independence proved in
//! `lazily-formal`'s `Materialization` module: `materialize_present_comm` /
//! `materialize_observe_comm`), the property that justifies mutex-serialized
//! concurrent materialization.
#![cfg(feature = "thread-safe")]

mod common;

use std::collections::HashSet;

use common::Expect;
use lazily::{ThreadSafeComputedMap, ThreadSafeContext};
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
fn exp_strs(e: &Expect, key: &str) -> Vec<String> {
    e[key]
        .as_array()
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
) -> impl Fn(&lazily::ThreadSafeContext, &String) -> V + Clone + Send + Sync + 'static {
    // The tracking view is available to the factory but this fixture's values are
    // constants per key, so it goes unused here. What matters is that the map no
    // longer *severs* it.
    move |_ctx: &lazily::ThreadSafeContext, k: &String| -> V {
        entries
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("no val for {k}"))
    }
}

/// An eager `ThreadSafeComputedMap`: pre-mint the whole keyset.
fn eager_computed_map(
    ctx: &ThreadSafeContext,
    keys: Vec<String>,
    entries: Vec<(String, V)>,
) -> ThreadSafeComputedMap<String, V> {
    let map: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(ctx);
    map.materialize_all(ctx, keys, lookup_fn(entries));
    map
}

/// The shared `spec.val` laws, replayed through the thread-safe map: default
/// eager, eager materializes all, lazy defers all, observationally-transparent
/// reads under either strategy.
fn check_val_fixture(name: &str) -> Value {
    let fixture = load(name);
    let entries = val_entries(&fixture);
    let keys: Vec<String> = entries.iter().map(|(k, _)| k.clone()).collect();
    // ONE guard per fixture per runner (`#lzassertunknownkeys`); every key of the
    // block is consumed here rather than split across sibling tests.
    let expected = Expect::new(
        format!("{SPEC_DIR}/{name}"),
        "expected",
        fixture.get("expected").expect("expected"),
    );

    assert_eq!(expected["default_mode"].as_str(), Some("eager"));

    let ctx = ThreadSafeContext::new();
    let eager = eager_computed_map(&ctx, keys.clone(), entries.clone());
    let lazy: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(&ctx);
    let lookup = lookup_fn(entries);

    assert_eq!(eager.present_count(), keys.len());
    assert_eq!(
        as_set(&eager.present_keys()),
        as_set(&exp_strs(&expected, "eager_present"))
    );
    assert_eq!(lazy.present_count(), 0);

    for (k, want) in expected["observe"].as_object().unwrap() {
        let want = want.as_i64().unwrap();
        assert_eq!(eager.observe(&ctx, k).unwrap(), want, "eager observe {k}");
        assert_eq!(
            lazy.get_or_insert_with(&ctx, k.clone(), lookup.clone()),
            want,
            "lazy observe {k}"
        );
    }

    // The read sequence, asserted here so `lazy_present_after_reads` and
    // `present_after_each_read` land under the same guard.
    let fresh_ctx = ThreadSafeContext::new();
    let fresh: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(&fresh_ctx);
    let lookup = lookup_fn(val_entries(&fixture));
    let mut sizes = Vec::new();
    for k in str_array(&fixture, "reads") {
        fresh.get_or_insert_with(&fresh_ctx, k, lookup.clone());
        sizes.push(fresh.present_count());
    }
    assert_eq!(
        as_set(&fresh.present_keys()),
        as_set(&exp_strs(&expected, "lazy_present_after_reads"))
    );
    if let Some(want_sizes) = expected.get_opt("present_after_each_read") {
        let want: Vec<usize> = want_sizes
            .as_array()
            .expect("present_after_each_read")
            .iter()
            .map(|n| n.as_u64().unwrap() as usize)
            .collect();
        assert_eq!(sizes, want, "cumulative present-set sizes");
    }

    expected.finish();
    fixture
}

#[test]
fn observational_transparency_thread_safe() {
    if !present() {
        eprintln!("skipping: {SPEC_DIR} absent");
        return;
    }
    let fixture = check_val_fixture("observational_transparency.json");
    let expected = fixture.get("expected").unwrap();
    let entries = val_entries(&fixture);

    let ctx = ThreadSafeContext::new();
    let lazy: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(&ctx);
    let lookup = lookup_fn(entries);
    for k in str_array(&fixture, "reads") {
        lazy.get_or_insert_with(&ctx, k, lookup.clone());
    }
    assert_eq!(
        as_set(&lazy.present_keys()),
        as_set(&str_array(expected, "lazy_present_after_reads"))
    );
}

#[test]
fn deferral_not_deallocation_thread_safe() {
    if !present() {
        eprintln!("skipping: {SPEC_DIR} absent");
        return;
    }
    let fixture = check_val_fixture("deferral_not_deallocation.json");
    let expected = fixture.get("expected").unwrap();
    let entries = val_entries(&fixture);

    let ctx = ThreadSafeContext::new();
    let lazy: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(&ctx);
    let lookup = lookup_fn(entries);
    let want_sizes: Vec<usize> = expected
        .get("present_after_each_read")
        .and_then(|v| v.as_array())
        .expect("present_after_each_read")
        .iter()
        .map(|n| n.as_u64().unwrap() as usize)
        .collect();
    let mut got = Vec::new();
    for k in str_array(&fixture, "reads") {
        lazy.get_or_insert_with(&ctx, k, lookup.clone());
        got.push(lazy.present_count());
    }
    assert_eq!(got, want_sizes);
    let lazy_present = as_set(&lazy.present_keys());
    assert!(lazy_present.is_subset(&as_set(&str_array(expected, "eager_present"))));
}

/// **Confluence** (`materialize_present_comm` / `materialize_observe_comm`): two
/// lazy maps over the same spec, read in *opposite* key orders, reach the same
/// present set and identical observed values — the order-independence that makes
/// the `Arc<Mutex>`-serialized map safe under any interleaving.
#[test]
fn materialization_confluent_under_reordering() {
    if !present() {
        eprintln!("skipping: {SPEC_DIR} absent");
        return;
    }
    let fixture = load("observational_transparency.json");
    let entries = val_entries(&fixture);
    let keys: Vec<String> = entries.iter().map(|(k, _)| k.clone()).collect();

    let ctx_fwd = ThreadSafeContext::new();
    let fwd: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(&ctx_fwd);
    let ctx_rev = ThreadSafeContext::new();
    let rev: ThreadSafeComputedMap<String, V> = ThreadSafeComputedMap::new(&ctx_rev);
    let lookup = lookup_fn(entries);

    let mut fwd_vals = Vec::new();
    for k in keys.iter() {
        fwd_vals.push((
            k.clone(),
            fwd.get_or_insert_with(&ctx_fwd, k.clone(), lookup.clone()),
        ));
    }
    let mut rev_vals = Vec::new();
    for k in keys.iter().rev() {
        rev_vals.push((
            k.clone(),
            rev.get_or_insert_with(&ctx_rev, k.clone(), lookup.clone()),
        ));
    }

    // Same observed value per key regardless of the order it was materialized.
    for (k, v) in &fwd_vals {
        let rv = rev_vals
            .iter()
            .find(|(rk, _)| rk == k)
            .map(|(_, v)| *v)
            .unwrap();
        assert_eq!(*v, rv, "observe {k} order-independent");
    }
    // Same present set regardless of materialization order.
    assert_eq!(as_set(&fwd.present_keys()), as_set(&rev.present_keys()));
}
