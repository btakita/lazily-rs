//! Self-tests for the assertion-key guard (`#lzassertunknownkeys`,
//! `#lzconsumednotasserted`).
//!
//! The guard lives in `tests/common/expect.rs`, which is compiled into every
//! test binary that says `mod common;`. Its own tests live here, in one
//! top-level binary, so they run once instead of ~30 times.
//!
//! Each case is the mutation check for one direction of the guard. Rung 2: a
//! runner that consumes everything passes, a runner that misses a key fails and
//! names it, and the nested form catches the reader-kind maps (`invalidates`)
//! where the silently-skipped key is most likely to hide. Rung 3: a key that is
//! *read* and then discarded fails, an excuse that the same run also asserts
//! fails as stale, and each of the three read-then-discard shapes from
//! `#lzconsumednotasserted` is reproduced against the guard directly.

mod common;

use common::Expect;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// rung 2 — every key must be touched
// ---------------------------------------------------------------------------

#[test]
fn asserting_every_key_passes() {
    let v = json!({ "value": 1, "invalidates": { "value": true } });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
    e.sub("invalidates").assert_key("value", true);
}

#[test]
#[should_panic(expected = "never consumed")]
fn an_unconsumed_key_panics() {
    let v = json!({ "value": 1, "backend": "arrow" });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
}

#[test]
#[should_panic(expected = "\"backend\"")]
fn the_panic_names_the_offending_key() {
    let v = json!({ "value": 1, "backend": "arrow" });
    let e = Expect::new("delta_zero_copy_arrow.json", "expected", &v);
    e.assert_key("value", 1u64);
}

#[test]
#[should_panic(expected = "delta_zero_copy_arrow.json")]
fn the_panic_names_the_fixture() {
    let v = json!({ "value": 1, "backend": "arrow" });
    let e = Expect::new("delta_zero_copy_arrow.json", "expected", &v);
    e.assert_key("value", 1u64);
}

#[test]
#[should_panic(expected = "expected.invalidates")]
fn a_nested_unconsumed_reader_kind_panics() {
    let v = json!({ "invalidates": { "value": true, "len": false } });
    let e = Expect::new("f.json", "expected", &v);
    let inv = e.sub("invalidates");
    inv.assert_key("value", true);
}

// ---------------------------------------------------------------------------
// rung 3 — every read key must reach a comparison against the fixture's value
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "read but never asserted")]
fn a_bare_read_is_not_an_assertion() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    let _ = e["value"];
}

#[test]
#[should_panic(expected = "\"sibling_a_cached\"")]
fn the_read_but_not_asserted_panic_names_the_key() {
    let v = json!({ "sibling_a_cached": true });
    let e = Expect::new("f.json", "expected", &v);
    let _ = e["sibling_a_cached"];
}

/// Shape 1 of `#lzconsumednotasserted`: a named skip inside a loop that consumes
/// the block. The read marks the key, the `continue` steps past the comparison.
#[test]
#[should_panic(expected = "read but never asserted")]
fn a_named_skip_in_a_consuming_loop_panics() {
    let v = json!({ "a": 1, "downstream_consumer_reran": false });
    let e = Expect::new("f.json", "expected", &v);
    for key in ["a", "downstream_consumer_reran"] {
        let want = e[key].clone();
        if key == "downstream_consumer_reran" {
            continue;
        }
        assert_eq!(want, json!(1));
    }
}

/// Shape 2: the value is bound and never compared.
#[test]
#[should_panic(expected = "read but never asserted")]
fn a_value_bound_but_never_compared_panics() {
    let v = json!({ "x": 7 });
    let e = Expect::new("f.json", "expected", &v);
    let _want = e.get("x");
}

/// Shape 3: the fixture value gates a branch but the assertion compares against
/// a literal, so editing the fixture changes nothing.
#[test]
#[should_panic(expected = "read but never asserted")]
fn a_comparison_against_a_literal_panics() {
    let v = json!({ "downstream_consumer_reran": false });
    let e = Expect::new("f.json", "expected", &v);
    if e["downstream_consumer_reran"] == json!(false) {
        // The arm asserts a hardcoded outcome, never the fixture's own value —
        // editing the fixture would not change what is checked.
        assert_eq!(1 + 1, 2, "asserts a constant, never the fixture");
    }
}

#[test]
fn assert_key_with_marks_a_non_equality_comparison() {
    let v = json!({ "delay_ms": 100 });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key_with("delay_ms", |want| {
        let want = want.as_f64().unwrap();
        assert!((99.9f64 - want).abs() < 1.0);
    });
}

#[test]
#[should_panic(expected = "fixture expects")]
fn assert_key_reports_the_fixture_value_and_the_actual() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 2u64);
}

#[test]
#[should_panic(expected = "at step 3")]
fn assert_key_at_carries_the_call_site_into_the_message() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key_at("value", 2u64, "step 3");
}

// ---------------------------------------------------------------------------
// excuses, both directions
// ---------------------------------------------------------------------------

#[test]
fn an_excuse_satisfies_a_key_that_cannot_be_asserted_here() {
    let v = json!({ "value": 1, "gpu_backend": "metal" });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
    e.excuse_key("gpu_backend", "no GPU backend exists in this binding");
}

#[test]
fn an_excuse_covers_a_key_the_runner_reads_to_drive_the_replay() {
    let v = json!({ "mode": "eager" });
    let e = Expect::new("f.json", "expected", &v);
    let _mode = &e["mode"];
    e.excuse_key(
        "mode",
        "discriminator selecting the replay path, not a value to check",
    );
}

#[test]
#[should_panic(expected = "both excused and asserted")]
fn an_excuse_for_a_key_the_run_asserts_is_stale() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
    e.excuse_key("value", "proved by the sibling test");
}

#[test]
#[should_panic(expected = "needs a reason")]
fn an_excuse_without_a_reason_is_rejected() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    e.excuse_key("value", "");
}

#[test]
fn prose_is_exempt_from_all_three_checks() {
    let v = json!({ "value": 1, "note": "why this fixture exists" });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
    e.prose("note", "documentation, no observable behind it");
}

// ---------------------------------------------------------------------------
// shape and unwinding
// ---------------------------------------------------------------------------

#[test]
fn an_absent_key_asserts_as_null() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
    e.assert_key_with("missing", |want| assert_eq!(want, &Value::Null));
}

#[test]
fn a_non_object_block_is_inert() {
    let absent = Value::Null;
    let _ = Expect::new("f.json", "expected", &absent);
    let list = json!([1, 2, 3]);
    let _ = Expect::new("f.json", "expected", &list);
}

#[test]
#[should_panic(expected = "the real failure")]
fn a_drop_while_unwinding_does_not_mask_the_real_failure() {
    let v = json!({ "value": 1, "unread": 2 });
    let _e = Expect::new("f.json", "expected", &v);
    panic!("the real failure");
}
