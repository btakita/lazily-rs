//! Self-tests for the assertion-key consumption guard (`#lzassertunknownkeys`).
//!
//! The guard lives in `tests/common/expect.rs`, which is compiled into every
//! test binary that says `mod common;`. Its own tests live here, in one
//! top-level binary, so they run once instead of ~30 times.
//!
//! Each case is the mutation check for one direction of the guard: a runner that
//! consumes everything passes, a runner that misses a key fails and names it,
//! and the nested form catches the reader-kind maps (`invalidates`) where the
//! silently-skipped key is most likely to hide.

mod common;

use common::Expect;
use serde_json::{Value, json};

#[test]
fn consuming_every_key_passes() {
    let v = json!({ "value": 1, "invalidates": { "value": true } });
    let e = Expect::new("f.json", "expected", &v);
    let _ = e["value"];
    let _ = e["invalidates"];
}

#[test]
#[should_panic(expected = "never consumed")]
fn an_unconsumed_key_panics() {
    let v = json!({ "value": 1, "backend": "arrow" });
    let e = Expect::new("f.json", "expected", &v);
    let _ = e["value"];
}

#[test]
#[should_panic(expected = "\"backend\"")]
fn the_panic_names_the_offending_key() {
    let v = json!({ "value": 1, "backend": "arrow" });
    let e = Expect::new("delta_zero_copy_arrow.json", "expected", &v);
    let _ = e["value"];
}

#[test]
#[should_panic(expected = "delta_zero_copy_arrow.json")]
fn the_panic_names_the_fixture() {
    let v = json!({ "value": 1, "backend": "arrow" });
    let e = Expect::new("delta_zero_copy_arrow.json", "expected", &v);
    let _ = e["value"];
}

#[test]
#[should_panic(expected = "expected.invalidates")]
fn a_nested_unconsumed_reader_kind_panics() {
    let v = json!({ "invalidates": { "value": true, "len": false } });
    let e = Expect::new("f.json", "expected", &v);
    let inv = e.sub("invalidates");
    let _ = inv["value"];
}

#[test]
fn a_declared_exception_consumes() {
    let v = json!({ "value": 1, "gpu_backend": "metal" });
    let e = Expect::new("f.json", "expected", &v);
    let _ = e["value"];
    e.declared_exception("gpu_backend", "no GPU backend exists in this binding");
}

#[test]
fn an_absent_key_reads_as_null_and_still_counts() {
    let v = json!({ "value": 1 });
    let e = Expect::new("f.json", "expected", &v);
    assert_eq!(e["value"].as_u64(), Some(1));
    assert_eq!(e["missing"], Value::Null);
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
