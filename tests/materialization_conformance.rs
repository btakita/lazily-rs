//! Cross-language conformance tests for `ComputedMap` materialization
//! (`#reactivemap`), driven by the canonical fixtures in
//! `lazily-spec/conformance/materialization/`. These exercise the laws proved in
//! `lazily-formal`'s `Materialization` module against the Rust `ComputedMap`
//! specialization of [`ReactiveMap`]:
//!
//! - `observational_transparency.json` — eager (pre-mint loop) and lazy
//!   (`get_or_insert_with` mint-on-access) return identical values for every key
//!   (`observe_canonical` / `eager_lazy_observationally_equivalent`); eager
//!   materializes all up front, lazy only the read keys; default is eager.
//! - `deferral_not_deallocation.json` — the present set only *grows* and is
//!   unchanged by a re-read (`materialize_present_monotone`); the lazy present
//!   set is a subset of the eager one (`lazy_present_subset_eager`).
//! - `entry_kind_orthogonal_to_mode.json` — input **cell** entries are
//!   materialized in every strategy; derived **slot** entries defer under lazy
//!   (`cell_entries_materialized_in_every_mode` / `slot_entries_deferred_under_lazy`).

mod common;

use std::collections::HashSet;

use common::Expect;
use lazily::{ComputedMap, Context, EntryKind, SourceMap};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/materialization";

type V = i64;

fn spec_fixtures_present() -> bool {
    std::path::Path::new(SPEC_DIR).exists()
}

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn str_array(v: &Value, path: &str) -> Vec<String> {
    v.get(path)
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("missing array {path}"))
        .iter()
        .map(|k| k.as_str().expect("array of strings").to_string())
        .collect()
}

/// `str_array` against the value an assertion key carries.
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

/// Parse a `spec.val` object of `key -> canonical value` into ordered keys and a
/// lookup closure input.
fn parse_val_spec(fixture: &Value) -> Vec<(String, V)> {
    let obj = fixture
        .get("spec")
        .and_then(|s| s.get("val"))
        .and_then(|v| v.as_object())
        .expect("spec.val object");
    obj.iter()
        .map(|(k, v)| (k.clone(), v.as_i64().expect("integer val")))
        .collect()
}

/// A `Fn(&String) -> V + 'static` lookup over the fixture's `spec.val` table.
fn lookup_fn(
    entries: Vec<(String, V)>,
) -> impl Fn(&lazily::Compute<'_>, &String) -> V + Clone + 'static {
    // The tracking view is available to the factory but this fixture's values are
    // constants per key, so it goes unused here. What matters is that the map no
    // longer *severs* it.
    move |_ctx: &lazily::Compute<'_>, k: &String| -> V {
        entries
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("no spec val for key {k}"))
    }
}

/// An eager `ComputedMap`: pre-mint the whole keyset.
fn eager_computed_map(
    ctx: &Context,
    keys: Vec<String>,
    entries: Vec<(String, V)>,
) -> ComputedMap<String, V> {
    let map: ComputedMap<String, V> = ComputedMap::new(ctx);
    map.materialize_all(ctx, keys, lookup_fn(entries));
    map
}

/// A lazy `ComputedMap`: empty, mint-on-access via `get_or_insert_with`.
fn lazy_computed_map(ctx: &Context) -> ComputedMap<String, V> {
    ComputedMap::new(ctx)
}

/// Assert the shared invariants both `spec.val` fixtures declare: default mode
/// eager, observationally-transparent reads, eager materializes all, and the
/// lazy present set after the read sequence equals the (deduped) read keys.
fn check_val_fixture(name: &str) -> Value {
    let fixture = load_fixture(name);
    let entries = parse_val_spec(&fixture);
    let keys: Vec<String> = entries.iter().map(|(k, _)| k.clone()).collect();
    // ONE guard per fixture per runner (`#lzassertunknownkeys`), so every key the
    // block declares is consumed here rather than split across sibling tests
    // where each half would look complete on its own.
    let expected = Expect::new(
        format!("{SPEC_DIR}/{name}"),
        "expected",
        fixture.get("expected").expect("expected"),
    );

    let ctx = Context::new();
    let eager = eager_computed_map(&ctx, keys.clone(), entries.clone());
    let lazy = lazy_computed_map(&ctx);

    // default_mode_eager. The strategy named by the fixture *selects the build*,
    // and the fact asserted is that a map built that way is fully materialized at
    // build time — so editing the fixture changes the outcome. Comparing the key
    // to the literal `"eager"` asserted nothing about the library
    // (`#lzconsumednotasserted`).
    expected.assert_key_with("default_mode", |want| {
        let mode = want.as_str().expect("default_mode");
        let default_present = match mode {
            "eager" => eager_computed_map(&ctx, keys.clone(), entries.clone()).present_count(),
            "lazy" => lazy_computed_map(&ctx).present_count(),
            other => panic!("unknown default_mode {other}"),
        };
        assert_eq!(
            default_present,
            keys.len(),
            "a map built the fixture's default way ({mode}) is materialized at build"
        );
    });

    // eager_materializes_all
    assert_eq!(eager.present_count(), keys.len());
    expected.assert_key_with("eager_present", |want| {
        assert_eq!(
            as_set(&eager.present_keys()),
            as_set(&want_strs(want, "eager_present"))
        )
    });
    // Lazy defers every derived slot: nothing present at build.
    assert_eq!(lazy.present_count(), 0);

    // observe_canonical / eager_lazy_observationally_equivalent
    let lookup = lookup_fn(entries.clone());
    expected.assert_key_with("observe", |observe| {
        for (k, want) in observe.as_object().expect("expected.observe") {
            let want = want.as_i64().expect("observe int");
            assert_eq!(eager.get(&ctx, k).unwrap(), want, "eager observe {k}");
            assert_eq!(
                lazy.get_or_insert_with(&ctx, k.clone(), lookup.clone()),
                want,
                "lazy observe {k}"
            );
        }
    });

    // The read sequence, asserted here so `lazy_present_after_reads` and
    // `present_after_each_read` are consumed by the same guard as the rest.
    let fresh_ctx = Context::new();
    let fresh = lazy_computed_map(&fresh_ctx);
    let lookup = lookup_fn(entries);
    let mut sizes = Vec::new();
    for k in str_array(&fixture, "reads") {
        fresh.get_or_insert_with(&fresh_ctx, k, lookup.clone());
        sizes.push(fresh.present_count());
    }
    expected.assert_key_with("lazy_present_after_reads", |want| {
        assert_eq!(
            as_set(&fresh.present_keys()),
            as_set(&want_strs(want, "lazy_present_after_reads"))
        )
    });
    // Peek through `raw` rather than `get_opt`: only fixtures that carry the key
    // owe an assertion for it, and a read on the ones that do not would itself be
    // a read-then-discard.
    if expected.raw().get("present_after_each_read").is_some() {
        expected.assert_key_with("present_after_each_read", |want_sizes| {
            let want: Vec<usize> = want_sizes
                .as_array()
                .expect("present_after_each_read")
                .iter()
                .map(|n| n.as_u64().expect("size") as usize)
                .collect();
            assert_eq!(sizes, want, "cumulative present-set sizes");
        });
    }

    // The guard's Drop must run before `fixture` moves out.
    expected.finish();
    fixture
}

#[test]
fn observational_transparency() {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} not present");
        return;
    }
    let fixture = check_val_fixture("observational_transparency.json");
    let expected = fixture.get("expected").unwrap();

    // Replay the lazy read sequence on a fresh map; the lazy present set is
    // exactly the read keys (lazy_defers_slots).
    let entries = parse_val_spec(&fixture);
    let ctx = Context::new();
    let lazy = lazy_computed_map(&ctx);
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
fn deferral_not_deallocation() {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} not present");
        return;
    }
    let fixture = check_val_fixture("deferral_not_deallocation.json");
    let expected = fixture.get("expected").unwrap();

    let entries = parse_val_spec(&fixture);
    let ctx = Context::new();
    let lazy = lazy_computed_map(&ctx);
    let lookup = lookup_fn(entries);

    // present_after_each_read: cumulative present-set size, monotone and
    // unchanged by a re-read (materialize_present_monotone).
    let want_sizes: Vec<usize> = expected
        .get("present_after_each_read")
        .and_then(|v| v.as_array())
        .expect("present_after_each_read")
        .iter()
        .map(|n| n.as_u64().expect("size") as usize)
        .collect();
    let mut got_sizes = Vec::new();
    for k in str_array(&fixture, "reads") {
        lazy.get_or_insert_with(&ctx, k, lookup.clone());
        got_sizes.push(lazy.present_count());
    }
    assert_eq!(got_sizes, want_sizes, "cumulative present-set sizes");

    // lazy_present_after_reads is a subset of eager_present (lazy_present_subset_eager).
    let lazy_present = as_set(&lazy.present_keys());
    assert_eq!(
        lazy_present,
        as_set(&str_array(expected, "lazy_present_after_reads"))
    );
    let eager_present = as_set(&str_array(expected, "eager_present"));
    assert!(
        lazy_present.is_subset(&eager_present),
        "lazy present set must be a subset of eager present set"
    );
}

/// Map a fixture's `entries[k].kind` token onto an [`EntryKind`].
///
/// The canonical fixture spells the two kinds `cell` / `slot`; the v2 cell
/// kernel renamed the node kinds, so the fixture will later be flipped to
/// `source` / `computed`. Both spellings are accepted so this runner stays
/// green across that flip. Any other token is a hard error — an unknown kind
/// must never be silently defaulted into one of the two buckets.
fn entry_kind_from_fixture_token(kind: &str) -> EntryKind {
    match kind {
        "cell" | "source" => EntryKind::Source,
        "slot" | "computed" => EntryKind::Computed,
        other => panic!("unknown entry kind {other}"),
    }
}

#[test]
fn entry_kind_fixture_tokens_accept_both_spellings() {
    // Pre-v2 wire spelling (what the canonical fixture ships today).
    assert_eq!(entry_kind_from_fixture_token("cell"), EntryKind::Source);
    assert_eq!(entry_kind_from_fixture_token("slot"), EntryKind::Computed);
    // v2 wire spelling (what the fixture will be flipped to).
    assert_eq!(entry_kind_from_fixture_token("source"), EntryKind::Source);
    assert_eq!(
        entry_kind_from_fixture_token("computed"),
        EntryKind::Computed
    );
}

#[test]
#[should_panic(expected = "unknown entry kind effect")]
fn entry_kind_fixture_token_rejects_unknown() {
    let _ = entry_kind_from_fixture_token("effect");
}

#[test]
fn entry_kind_orthogonal_to_mode() {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} not present");
        return;
    }
    let fixture = load_fixture("entry_kind_orthogonal_to_mode.json");
    // Guarded (`#lzassertunknownkeys`).
    let expected = Expect::new(
        format!("{SPEC_DIR}/entry_kind_orthogonal_to_mode.json"),
        "expected",
        fixture.get("expected").unwrap(),
    );
    let spec_entries = fixture
        .get("spec")
        .and_then(|s| s.get("entries"))
        .and_then(|v| v.as_object())
        .expect("spec.entries");

    // Split the map's declared entries by kind: input cells vs derived slots.
    // A single `ReactiveMap<K,V,H>` fixes one handle kind, so a mixed-kind
    // fixture is modelled by a `SourceMap` over the cell entries and a `ComputedMap`
    // over the slot entries — sharing one logical key space.
    let mut cell_keys: Vec<String> = Vec::new();
    let mut slot_keys: Vec<String> = Vec::new();
    let mut vals: Vec<(String, V)> = Vec::new();
    for (key, entry) in spec_entries {
        let kind = entry.get("kind").and_then(|v| v.as_str()).expect("kind");
        let val = entry.get("val").and_then(|v| v.as_i64()).expect("val");
        vals.push((key.clone(), val));
        match entry_kind_from_fixture_token(kind) {
            EntryKind::Source => cell_keys.push(key.clone()),
            EntryKind::Computed => slot_keys.push(key.clone()),
        }
    }
    let lookup = lookup_fn(vals.clone());
    // `SourceMap::entry` takes a plain value, not a factory, so these sites need
    // the bare lookup rather than the view-taking one.
    let value_of = |k: &String| -> V {
        vals.iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("no val for {k}"))
    };

    let ctx = Context::new();

    // Eager build: every entry present (cells + slots).
    let eager_cells: SourceMap<String, V> = SourceMap::new(&ctx);
    for k in &cell_keys {
        eager_cells.entry(&ctx, k.clone(), value_of(k));
    }
    let eager_slots: ComputedMap<String, V> = ComputedMap::new(&ctx);
    eager_slots.materialize_all(&ctx, slot_keys.clone(), lookup.clone());
    assert_eq!(eager_cells.entry_kind(), EntryKind::Source);
    assert_eq!(eager_slots.entry_kind(), EntryKind::Computed);
    let mut eager_present = as_set(&eager_cells.present_keys());
    eager_present.extend(eager_slots.present_keys());
    expected.assert_key_with("eager_present", |want| {
        assert_eq!(eager_present, as_set(&want_strs(want, "eager_present")))
    });
    // default_mode_eager across kinds: the strategy the fixture names is the one
    // whose build has every declared entry present.
    expected.assert_key_with("default_mode", |want| {
        let mode = want.as_str().expect("default_mode");
        assert_eq!(
            mode, "eager",
            "only the eager build materializes both kinds"
        );
        assert_eq!(
            eager_present.len(),
            cell_keys.len() + slot_keys.len(),
            "the fixture's default mode ({mode}) materializes every entry at build"
        );
    });

    // Lazy build: cells present at build (input cells are always materialized),
    // slots deferred until read.
    let lazy_cells: SourceMap<String, V> = SourceMap::new(&ctx);
    for k in &cell_keys {
        lazy_cells.entry(&ctx, k.clone(), value_of(k));
    }
    let lazy_slots: ComputedMap<String, V> = ComputedMap::new(&ctx);
    let present_at_build = as_set(&lazy_cells.present_keys());
    assert!(
        lazy_slots.present_keys().is_empty(),
        "slots deferred at build"
    );
    expected.assert_key_with("lazy_present_at_build", |want| {
        assert_eq!(
            present_at_build,
            as_set(&want_strs(want, "lazy_present_at_build"))
        )
    });

    // Reads (slot pulls) grow only the slot present set.
    for k in str_array(&fixture, "reads") {
        if slot_keys.contains(&k) {
            lazy_slots.get_or_insert_with(&ctx, k, lookup.clone());
        } else {
            lazy_cells.get_or_insert_with(&ctx, k, lookup.clone());
        }
    }
    let mut lazy_after = as_set(&lazy_cells.present_keys());
    lazy_after.extend(lazy_slots.present_keys());
    expected.assert_key_with("lazy_present_after_reads", |want| {
        assert_eq!(
            lazy_after,
            as_set(&want_strs(want, "lazy_present_after_reads"))
        )
    });

    // Observational transparency across kinds.
    expected.assert_key_with("observe", |observe| {
        for (k, want) in observe.as_object().unwrap() {
            let want = want.as_i64().unwrap();
            if cell_keys.contains(k) {
                assert_eq!(eager_cells.get(&ctx, k), Some(want));
                assert_eq!(lazy_cells.get(&ctx, k), Some(want));
            } else {
                assert_eq!(eager_slots.get(&ctx, k), Some(want));
                assert_eq!(
                    lazy_slots.get_or_insert_with(&ctx, k.clone(), lookup.clone()),
                    want
                );
            }
        }
    });
}
