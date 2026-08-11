#![cfg(all(feature = "ipc", feature = "serde"))]

//! Replay the `lazily-spec/conformance/message-passing/*.json` fixtures through
//! the [`CommandProjection`] reducer and RPC facade.
//!
//! Each fixture is a scenario: `frames` are folded in order (each frame decodes
//! into a `CommandMessage` or a `CausalReceipt`), and `expect` pins the reducer
//! image, terminal-conflict fail-closed behavior, and the RPC facade's
//! terminal-only resolution rule. This proves lazily-rs agrees with the spec and
//! (fixture-by-fixture) with the Kotlin and JS bindings.

mod common;

use common::Expect;
use lazily::{
    CausalReceipt, CommandApplyStatus, CommandMessage, CommandProjection, CommandProjectionImage,
    ReceiptMessage, ReceiptOutcome,
};
use serde_json::Value;

const FIXTURE_DIR: common::SpecDir = common::SpecDir("message-passing");

fn fixtures_present() -> bool {
    FIXTURE_DIR.is_dir()
}

fn load(name: &str) -> Value {
    let path = FIXTURE_DIR.join(name);
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// Fold one frame; returns the last apply status for message frames (receipts
/// return the last receipt's status).
fn fold_frame(projection: &mut CommandProjection, frame: &Value) -> CommandApplyStatus {
    let schema = frame["schema"].as_str().expect("frame.schema");
    let wire = &frame["wire"];
    match schema {
        "message-passing" => {
            let message: CommandMessage =
                serde_json::from_value(wire.clone()).expect("decode CommandMessage");
            projection.apply_message(&message)
        }
        "receipts" => {
            let message: ReceiptMessage =
                serde_json::from_value(wire.clone()).expect("decode ReceiptMessage");
            let ReceiptMessage::CausalReceipts(batch) = message;
            let mut last = CommandApplyStatus::Unknown;
            for receipt in &batch.receipts {
                last = projection.observe_receipt(receipt);
            }
            last
        }
        other => panic!("unknown frame schema {other}"),
    }
}

fn frames_of(obj: &Value) -> &Vec<Value> {
    obj["frames"].as_array().expect("frames array")
}

/// Guard a fixture's (or scenario's) `expect` block (`#lzassertunknownkeys`).
fn expect_of<'a>(name: &str, label: &str, block: &'a Value) -> Expect<'a> {
    Expect::new(format!("{FIXTURE_DIR}/{name}"), label.to_owned(), block)
}

/// The field names of a serialized projection image.
///
/// `serde_json::from_value` into `CommandProjectionImage` silently ignores a key
/// the struct does not declare, so the typed comparison below cannot see a field
/// the corpus adds. The KEY SET is therefore asserted separately, against the
/// image this run really produced (`#lzsubblockkeyset`).
fn image_field_names(image: &CommandProjectionImage) -> Vec<String> {
    serde_json::to_value(image)
        .expect("serialize projection image")
        .as_object()
        .expect("a projection image is a JSON object")
        .keys()
        .cloned()
        .collect()
}

/// Assert the reducer image equals the fixture's `expect.projection`.
fn assert_projection(projection: &CommandProjection, expect: &Expect) {
    let image = projection.to_image();
    expect.assert_key_with("projection", |want| {
        let want: CommandProjectionImage =
            serde_json::from_value(want.clone()).expect("decode expect.projection");
        assert_eq!(image, want, "projection image mismatch");
    });
    expect.assert_key_set("projection", image_field_names(&image));
}

#[test]
fn editor_route_submit_is_nonterminal() {
    if !fixtures_present() {
        return;
    }
    let fx = load("editor_route_submit.json");
    let mut p = CommandProjection::new();
    for frame in frames_of(&fx) {
        fold_frame(&mut p, frame);
    }
    assert_projection(
        &p,
        &expect_of("editor_route_submit.json", "expect", &fx["expect"]),
    );
    assert!(p.terminal_for("cmd-run-1").is_none());
}

#[test]
fn sync_tmux_layout_submit_shared_blob() {
    if !fixtures_present() {
        return;
    }
    let fx = load("sync_tmux_layout_submit.json");
    let mut p = CommandProjection::new();
    for frame in frames_of(&fx) {
        fold_frame(&mut p, frame);
    }
    assert_projection(
        &p,
        &expect_of("sync_tmux_layout_submit.json", "expect", &fx["expect"]),
    );
}

#[test]
fn accepted_then_applied_receipt_is_terminal_only_at_receipt() {
    if !fixtures_present() {
        return;
    }
    let fx = load("accepted_then_applied_receipt.json");
    let exp = expect_of(
        "accepted_then_applied_receipt.json",
        "expect",
        &fx["expect"],
    );
    let frames = frames_of(&fx);
    // The index is the *observable*: the first frame after which the command is
    // terminal. Compare that against the fixture rather than using the fixture
    // value only to steer a pair of `assert!`s (`#lzconsumednotasserted`).
    let mut p = CommandProjection::new();
    let mut first_terminal_at: Option<usize> = None;
    for (i, frame) in frames.iter().enumerate() {
        fold_frame(&mut p, frame);
        if p.terminal_for("cmd-run-1").is_some() && first_terminal_at.is_none() {
            first_terminal_at = Some(i);
        }
    }
    let first_terminal_at = first_terminal_at.expect("command never became terminal");
    exp.assert_key_at(
        "terminal_after_frame_index",
        first_terminal_at as u64,
        "the first frame after which cmd-run-1 is terminal",
    );
    assert_projection(&p, &exp);
}

#[test]
fn stale_generation_events_and_receipts_are_ignored() {
    if !fixtures_present() {
        return;
    }
    let fx = load("stale_generation_ignored.json");
    let exp = expect_of("stale_generation_ignored.json", "expect", &fx["expect"]);
    let frames = frames_of(&fx);
    // The set of stale frames is the observable: compare the whole set, so a
    // fixture that lists a frame the reducer accepted fails, and so does a
    // reducer that ignores a frame the fixture does not list.
    let mut p = CommandProjection::new();
    let mut stale: Vec<usize> = Vec::new();
    for (i, frame) in frames.iter().enumerate() {
        let status = fold_frame(&mut p, frame);
        if matches!(status, CommandApplyStatus::StaleGeneration { .. }) {
            stale.push(i);
        }
    }
    exp.assert_key_with("ignored_frame_indices", |want| {
        let want: Vec<usize> = want
            .as_array()
            .expect("ignored_frame_indices")
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect();
        assert_eq!(stale, want, "frames the reducer treated as StaleGeneration");
    });
    assert_projection(&p, &exp);
}

#[test]
fn terminal_conflict_fails_closed() {
    if !fixtures_present() {
        return;
    }
    let fx = load("terminal_conflict_fail_closed.json");
    let exp = expect_of(
        "terminal_conflict_fail_closed.json",
        "expect",
        &fx["expect"],
    );
    let frames = frames_of(&fx);
    let command_id = exp.assert_key_with("conflict_command_id", |want| {
        want.as_str().expect("conflict_command_id").to_owned()
    });

    let mut p = CommandProjection::new();
    let mut conflict_at: Option<usize> = None;
    for (i, frame) in frames.iter().enumerate() {
        let status = fold_frame(&mut p, frame);
        if matches!(status, CommandApplyStatus::TerminalConflict { .. }) && conflict_at.is_none() {
            conflict_at = Some(i);
        }
    }
    exp.assert_key_with("conflict_after_frame_index", |want| {
        assert_eq!(
            conflict_at,
            Some(want.as_u64().expect("conflict_after_frame_index") as usize),
            "the first frame the reducer reported TerminalConflict on"
        )
    });
    exp.assert_key_at(
        "conflict",
        p.has_conflict(&command_id),
        "conflict must be flagged",
    );

    // The applied outcome is preserved (no winner selection).
    let image = p.to_image();
    exp.assert_key_with("projection_before_conflict", |want| {
        let before: CommandProjectionImage = serde_json::from_value(want.clone()).unwrap();
        assert_eq!(image, before);
    });
    exp.assert_key_set("projection_before_conflict", image_field_names(&image));
}

#[test]
fn cancel_preempts_nonterminal_scenarios() {
    if !fixtures_present() {
        return;
    }
    let fx = load("cancel_preempts_nonterminal.json");
    // Per-scenario replay ledger (`#lzscenariocoverage`).
    for (si, _id, scenario) in common::scenarios(
        &format!("{FIXTURE_DIR}/cancel_preempts_nonterminal.json"),
        &fx,
    ) {
        let name = scenario["name"].as_str().unwrap();
        let exp = expect_of(
            "cancel_preempts_nonterminal.json",
            &format!("scenarios[{si}].expect"),
            &scenario["expect"],
        );
        let mut p = CommandProjection::new();
        // The indices whose image this run really held fixed across the fold —
        // what the key is compared against below.
        let mut held_fixed: Vec<usize> = Vec::new();
        for (i, frame) in scenario["frames"].as_array().unwrap().iter().enumerate() {
            // `ignored_frame_indices`: a late cancel against an already-terminal
            // command must leave the projection exactly as it was. The reducer
            // still *records* the input, so the observable is the image, not the
            // apply status. Previously unread on this fixture.
            let ignored = exp
                .raw()
                .get("ignored_frame_indices")
                .and_then(|v| v.as_array())
                .is_some_and(|a| a.iter().any(|v| v.as_u64() == Some(i as u64)));
            let before = ignored.then(|| p.to_image());
            fold_frame(&mut p, frame);
            if let Some(before) = before {
                assert_eq!(
                    p.to_image(),
                    before,
                    "scenario {name} frame {i}: must be ignored"
                );
                held_fixed.push(i);
            }
        }
        // Compared against the indices the fold really reached and really held
        // fixed (`#lznullformblind`). The bounds check this replaces compared
        // the fixture to its own `frames` array — green over a runner that never
        // folded a frame, and blind to a declared index the loop skipped.
        exp.assert_key_if_present("ignored_frame_indices", |want| {
            let want: Vec<usize> = want
                .as_array()
                .expect("ignored_frame_indices")
                .iter()
                .map(|v| v.as_u64().expect("frame index") as usize)
                .collect();
            assert_eq!(
                held_fixed, want,
                "scenario {name}: the frames whose projection image this run held fixed"
            );
        });
        assert_projection(&p, &exp);
        assert_eq!(
            p.terminal_for("cmd-run-1").map(|e| e.status),
            p.entry("cmd-run-1").map(|e| e.status),
            "scenario {name}: terminal command exposed via terminal_for"
        );
    }
}

#[test]
fn reconnect_command_projection_resyncs() {
    if !fixtures_present() {
        return;
    }
    let fx = load("reconnect_command_projection.json");
    let mut p = CommandProjection::new();
    for frame in frames_of(&fx) {
        fold_frame(&mut p, frame);
    }
    assert_projection(
        &p,
        &expect_of("reconnect_command_projection.json", "expect", &fx["expect"]),
    );
}

#[test]
fn rpc_call_waits_for_terminal() {
    if !fixtures_present() {
        return;
    }
    let fx = load("rpc_call_waits_for_terminal.json");
    let exp = expect_of("rpc_call_waits_for_terminal.json", "expect", &fx["expect"]);
    let frames = frames_of(&fx);
    let rpc = exp.sub("rpc");
    let command_id = rpc.assert_key_with("command_id", |want| {
        want.as_str().expect("command_id").to_owned()
    });

    // The frames on which the call is unresolved, and the one on which it
    // resolves, are both observables — compared as sets/indices rather than used
    // to steer bare `assert!`s (`#lzconsumednotasserted`).
    let mut p = CommandProjection::new();
    let mut unresolved_frames: Vec<usize> = Vec::new();
    let mut resolves_at: Option<usize> = None;
    for (i, frame) in frames.iter().enumerate() {
        fold_frame(&mut p, frame);
        if p.terminal_for(&command_id).is_some() {
            if resolves_at.is_none() {
                resolves_at = Some(i);
            }
        } else {
            unresolved_frames.push(i);
        }
    }
    rpc.assert_key_with("resolves_after_frame_index", |want| {
        assert_eq!(
            resolves_at,
            Some(want.as_u64().expect("resolves_after_frame_index") as usize),
            "the frame on which the RPC call resolves"
        )
    });
    rpc.assert_key_with("unresolved_after_frame_indices", |want| {
        let want: Vec<usize> = want
            .as_array()
            .expect("unresolved_after_frame_indices")
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect();
        assert_eq!(
            unresolved_frames, want,
            "the frames after which the RPC call is still unresolved"
        );
    });
    // `rpc.terminal_status`: WHICH terminal the call resolved to, not merely that
    // it resolved. A cancelled command also resolves an RPC call.
    let got_status = p.terminal_for(&command_id).expect("terminal").status;
    rpc.assert_key_with("terminal_status", |want| {
        assert_eq!(
            serde_json::to_value(got_status).unwrap(),
            Value::String(want.as_str().expect("terminal_status").to_owned()),
            "rpc.terminal_status"
        )
    });
    assert_projection(&p, &exp);
}

#[test]
fn receipt_outcome_maps_are_covered() {
    // Guard that the receipt->status mapping distinguishes cancelled/superseded/
    // timed_out from plain rejected via the receipt reason.
    let mut p = CommandProjection::new();
    // Minimal manual submit via a decoded fixture frame is unnecessary here; use
    // the public reducer surface directly.
    let submit_json = serde_json::json!({
        "CommandSubmit": {
            "command_id": "cmd-x",
            "causation_id": "cmd-x",
            "source": "test",
            "target": "controller",
            "namespace": "agent-doc",
            "name": "editor_route",
            "authority_generation": 1,
            "idempotency_key": "k",
            "deadline_ms": 0,
            "policy": { "dedupe": "none", "supersede": false, "cancel_on_preempt": false },
            "payload_type": "agent-doc.editor_route.v1",
            "payload_hash": "sha256:00",
            "payload": { "Inline": [1] },
            "required_features": []
        }
    });
    let message: CommandMessage = serde_json::from_value(submit_json).unwrap();
    p.apply_message(&message);
    let r = CausalReceipt::rejected("r1", "cmd-x", "controller", 1).with_reason("timed_out");
    assert_eq!(r.outcome, ReceiptOutcome::Rejected);
    p.observe_receipt(&r);
    assert_eq!(
        p.terminal_for("cmd-x").unwrap().status,
        lazily::CommandStatus::TimedOut
    );
}
