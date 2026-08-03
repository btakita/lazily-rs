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

use common::{Expect, ProseLedger};
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

// ---------------------------------------------------------------------------
// prose keys (`#lzprosekeyconvention`)
// ---------------------------------------------------------------------------
//
// One case per failure mode the convention requires a tracker to produce, plus
// the two exemption directions. The rules, numbered as in
// lazily-spec/docs/conformance.md § Prose assertion keys:
//
//   1 a declared paragraph is ASSERTED
//   2 a declared paragraph is EXCUSED with free text
//   3 a key that is not declared is discharged
//   4 the discharged set differs from `assertions.prose`
//   5 a discharge names NO keys
//   6 a discharge names a key the fixture's run never asserted
//   7 a discharge names a key that is itself prose
//
// plus: a run that records claims and never verifies, and a block that is
// entirely prose.

#[test]
fn an_annotation_is_exempt_by_name_when_the_corpus_does_not_declare_it() {
    let v = json!({ "value": 1, "note": "why this fixture exists" });
    let e = Expect::new("f.json", "expected", &v);
    e.assert_key("value", 1u64);
    // `note`, `description` and `reason` are annotations wherever the block does
    // not list them in `prose` — the reactive-graph corpus carries ~97 of them.
}

#[test]
fn discharging_every_declared_paragraph_passes() {
    let path = "prose_ok.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key_with("backends", |want| assert_eq!(want, &json!(["shm"])));
    e.prose_key("clause", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// The obligation is routinely carried by a key asserted in a LATER block —
/// `epoch_disambiguation` by `expect.frame_epoch` — so the ledger is
/// fixture-scoped, not block-scoped.
#[test]
fn a_discharge_may_name_a_key_asserted_in_a_later_block() {
    let path = "prose_fixture_scoped.json";
    let _ledger = ProseLedger::open(path);
    let a = json!({ "prose": ["epoch_disambiguation"], "scenario_count": 1 });
    let block = Expect::new(path, "assertions", &a);
    block.assert_key("scenario_count", 1u64);
    block.prose_key("epoch_disambiguation", &["frame_epoch"]);
    block.finish();

    let sc = json!({ "frame_epoch": 9 });
    let exp = Expect::new(path, "scenarios[a].expect", &sc);
    exp.assert_key("frame_epoch", 9u64);
    exp.finish();

    common::expect::verify_prose(path);
}

/// Rule 1: comparing a paragraph — or a tally derived from one — to an English
/// string pins wording, not behaviour.
#[test]
#[should_panic(expected = "rule 1")]
fn asserting_a_declared_paragraph_panics() {
    let path = "prose_rule1.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "clause": "a decoder MUST reject", "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.assert_key("clause", "a decoder MUST reject");
    e.prose_key("clause", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 2: an unfalsifiable reason is indistinguishable from the undocumented
/// default the clause exists to remove.
#[test]
#[should_panic(expected = "rule 2")]
fn excusing_a_declared_paragraph_panics() {
    let path = "prose_rule2.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.excuse_key("clause", "prose; explains why the wire is text/hex");
    e.prose_key("clause", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 3: the corpus decides what is a paragraph, never the binding.
#[test]
#[should_panic(expected = "rule 3")]
fn discharging_a_key_the_corpus_did_not_declare_panics() {
    let path = "prose_rule3.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.prose_key("clause", &["backends"]);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("theorem", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 4: the comparison that CONSUMES `prose` itself — a forgotten paragraph
/// fails rather than vanishing.
#[test]
#[should_panic(expected = "rule 4")]
fn discharging_fewer_keys_than_the_corpus_declares_panics() {
    let path = "prose_rule4.json";
    let _ledger = ProseLedger::open(path);
    // `theorem` is declared prose and is not a key of the block — a stale entry
    // left behind by a corpus edit. Nothing else can see it: the block's own
    // unconsumed-key check walks the object's keys, and `theorem` is not one, so
    // without rule 4 the forgotten paragraph would vanish rather than fail.
    let v = json!({ "prose": ["clause", "theorem"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 5: a discharge that names nothing is the free-text excuse again,
/// spelled as an empty list.
#[test]
#[should_panic(expected = "rule 5")]
fn a_discharge_naming_no_keys_panics() {
    let path = "prose_rule5.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &[]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 6 is the whole convention: the excuse becomes falsifiable, because the
/// tracker can check the claim against what the run really asserted.
#[test]
#[should_panic(expected = "rule 6")]
fn a_discharge_naming_a_key_the_run_never_asserted_panics() {
    let path = "prose_rule6.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["frame_epoch"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 7: a paragraph cannot carry another paragraph's obligation.
#[test]
#[should_panic(expected = "rule 7")]
fn a_discharge_naming_another_paragraph_panics() {
    let path = "prose_rule7.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause", "theorem"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["theorem"]);
    e.prose_key("theorem", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// Rule 7's second half: `prose` never lists itself, so without seeding the
/// prose-name set with it, `discharged_by = ["prose"]` slips past rule 7 — and
/// the rule-4 comparison is what marks `prose` asserted, so rule 6 waves it
/// through too. A paragraph discharged by the declaration that it is a paragraph
/// proves nothing.
#[test]
#[should_panic(expected = "rule 7")]
fn a_discharge_naming_the_declaration_itself_panics() {
    let path = "prose_rule7_self.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["prose"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// A "run" is ONE TEST, not one process: the ledger is cleared at each
/// verification. Unioning asserted keys across replays of the same fixture would
/// let a discharge in one be satisfied by an assertion in another, which is the
/// accident of collocation the fixture-scoped ledger exists to bound.
#[test]
#[should_panic(expected = "rule 6")]
fn a_second_replay_does_not_inherit_the_first_replays_assertions() {
    let path = "prose_two_replays.json";
    // One ledger, two verifications — the shape a runner reaches when it replays
    // the same fixture through two codecs or two execution models.
    let _ledger = ProseLedger::open(path);

    let v = json!({ "prose": ["clause"], "backends": ["shm"], "codecs": ["json"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.assert_key("codecs", json!(["json"]));
    e.prose_key("clause", &["backends", "codecs"]);
    e.finish();
    common::expect::verify_prose(path);

    // The second replay asserts only `backends`, and must NOT be able to lean on
    // the `codecs` assertion the first one made.
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["backends", "codecs"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// A claim recorded AFTER a verification must not ride on it. Verifying clears
/// the ledger, so the guard's own teardown still reports the late claim.
#[test]
#[should_panic(expected = "never verified")]
fn a_claim_recorded_after_verification_is_still_reported() {
    let path = "prose_late_claim.json";
    let ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);

    let late = json!({ "prose": ["theorem"], "backends": ["shm"] });
    let l = Expect::new(path, "assertions_again", &late);
    l.assert_key("backends", json!(["shm"]));
    l.prose_key("theorem", &["backends"]);
    l.finish();
    drop(ledger);
}

/// An unverified claim proves exactly as much as an unconsumed key, so the
/// ledger's own teardown reports it. Reporting success by skipping the check is
/// the shape this clause removes.
#[test]
#[should_panic(expected = "never verified")]
fn a_run_that_records_claims_and_never_verifies_panics() {
    let path = "prose_unverified.json";
    let ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "backends": ["shm"] });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("backends", json!(["shm"]));
    e.prose_key("clause", &["backends"]);
    e.finish();
    drop(ledger);
}

/// Verifying an unarmed ledger would check nothing and report success.
#[test]
#[should_panic(expected = "without an open ProseLedger")]
fn verifying_without_an_armed_ledger_panics() {
    common::expect::verify_prose("prose_unarmed.json");
}

/// A block that is entirely prose has nothing that could discharge it.
#[test]
#[should_panic(expected = "carries no other key")]
fn a_block_that_is_entirely_prose_panics() {
    let path = "prose_only.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({ "prose": ["clause"], "clause": "a decoder MUST reject" });
    let other = json!({ "backends": ["shm"] });
    let sibling = Expect::new(path, "scenarios[a].expect", &other);
    sibling.assert_key("backends", json!(["shm"]));
    sibling.finish();

    let e = Expect::new(path, "assertions", &v);
    e.prose_key("clause", &["backends"]);
    e.finish();
    common::expect::verify_prose(path);
}

/// The corpus overrides the by-name exemption: a `note` listed in `prose` states
/// an obligation and must be discharged like any other paragraph.
///
/// The pin is that the BLOCK reports it, by name, the moment it drops — an
/// annotation name is a place no runner can be made to discharge anything, so a
/// `note` that keeps its exemption while declared is reported one rung late (as
/// the rule-4 set difference) and against the wrong key.
#[test]
#[should_panic(expected = "never consumed")]
fn a_declared_note_loses_the_by_name_exemption() {
    let path = "prose_declared_note.json";
    let _ledger = ProseLedger::open(path);
    let v = json!({
        "prose": ["note", "theorem"],
        "note": "`role` is the codec's ROLE, a separate sense from byte_canonical",
        "theorem": "resolve_wrong_backend",
        "role": "reference",
    });
    let e = Expect::new(path, "assertions", &v);
    e.assert_key("role", "reference");
    // `theorem` is discharged, so `prose` itself is consumed and the block's
    // report is about `note` alone.
    e.prose_key("theorem", &["role"]);
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

// ---------------------------------------------------------------------------
// Scenario identity (`#lzspecscenarioids`)
// ---------------------------------------------------------------------------
//
// The ledger's `id` -> `name` resolution used to end in a positional `#<n>`
// fallback. That fallback is the reason these tests exist rather than a comment:
// a ledger entry recorded BY POSITION silently rebinds to a different scenario
// when the corpus array is reordered, and nothing turns red — the guard compares
// "index 1 was replayed" against whatever now sits at index 1 and agrees with
// itself. lazily-spec now identifies every scenario, so the fallback is a hard
// failure here, and these pin that in both directions.

#[test]
fn scenario_id_prefers_id_over_name() {
    let sc = json!({ "id": "keep_latest", "name": "ignored" });
    assert_eq!(common::scenario_id(&sc, 7).0, "keep_latest");
}

#[test]
fn scenario_id_falls_back_to_name() {
    let sc = json!({ "name": "repair_converges" });
    assert_eq!(common::scenario_id(&sc, 7).0, "repair_converges");
}

#[test]
#[should_panic(expected = "carries neither `id` nor `name`")]
fn scenario_id_refuses_an_unidentified_scenario() {
    let sc = json!({ "policy": "Sum" });
    let _ = common::scenario_id(&sc, 1);
}

#[test]
#[should_panic(expected = "carries neither `id` nor `name`")]
fn scenario_id_refuses_a_blank_identifier() {
    // A blank id is not an identifier. Accepting it would put every blank-id
    // scenario in the corpus under the SAME ledger entry, which reads as
    // "replayed" the moment any one of them runs.
    let sc = json!({ "id": "  ", "name": "" });
    let _ = common::scenario_id(&sc, 2);
}
