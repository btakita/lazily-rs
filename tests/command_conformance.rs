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

use std::path::{Path, PathBuf};

use common::Expect;
use lazily::{
    CausalReceipt, CommandApplyStatus, CommandMessage, CommandProjection, CommandProjectionImage,
    ReceiptMessage, ReceiptOutcome,
};
use serde_json::Value;

const FIXTURE_DIR: &str = "../lazily-spec/conformance/message-passing";

fn fixtures_present() -> bool {
    Path::new(FIXTURE_DIR).is_dir()
}

fn load(name: &str) -> Value {
    let path = PathBuf::from(FIXTURE_DIR).join(name);
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

/// Assert the reducer image equals the fixture's `expect.projection`.
fn assert_projection(projection: &CommandProjection, expect: &Expect) {
    let want: CommandProjectionImage =
        serde_json::from_value(expect["projection"].clone()).expect("decode expect.projection");
    assert_eq!(projection.to_image(), want, "projection image mismatch");
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
    let terminal_at = exp["terminal_after_frame_index"]
        .as_u64()
        .expect("terminal_after_frame_index") as usize;

    let mut p = CommandProjection::new();
    for (i, frame) in frames.iter().enumerate() {
        fold_frame(&mut p, frame);
        let is_terminal = p.terminal_for("cmd-run-1").is_some();
        if i < terminal_at {
            assert!(!is_terminal, "frame {i}: must still be non-terminal");
        } else {
            assert!(is_terminal, "frame {i}: must be terminal");
        }
    }
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
    let ignored: Vec<usize> = exp["ignored_frame_indices"]
        .as_array()
        .expect("ignored_frame_indices")
        .iter()
        .map(|v| v.as_u64().unwrap() as usize)
        .collect();

    let mut p = CommandProjection::new();
    for (i, frame) in frames.iter().enumerate() {
        let status = fold_frame(&mut p, frame);
        if ignored.contains(&i) {
            assert!(
                matches!(status, CommandApplyStatus::StaleGeneration { .. }),
                "frame {i}: expected StaleGeneration, got {status:?}"
            );
        }
    }
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
    let conflict_at = exp["conflict_after_frame_index"]
        .as_u64()
        .expect("conflict_after_frame_index") as usize;
    let command_id = exp["conflict_command_id"].as_str().unwrap();

    let mut p = CommandProjection::new();
    for (i, frame) in frames.iter().enumerate() {
        let status = fold_frame(&mut p, frame);
        if i == conflict_at {
            assert!(
                matches!(status, CommandApplyStatus::TerminalConflict { .. }),
                "frame {i}: expected TerminalConflict, got {status:?}"
            );
        }
    }
    assert_eq!(
        p.has_conflict(command_id),
        exp["conflict"].as_bool().expect("conflict"),
        "conflict must be flagged"
    );

    // The applied outcome is preserved (no winner selection).
    let before: CommandProjectionImage =
        serde_json::from_value(exp["projection_before_conflict"].clone()).unwrap();
    assert_eq!(p.to_image(), before);
}

#[test]
fn cancel_preempts_nonterminal_scenarios() {
    if !fixtures_present() {
        return;
    }
    let fx = load("cancel_preempts_nonterminal.json");
    for (si, scenario) in fx["scenarios"]
        .as_array()
        .expect("scenarios")
        .iter()
        .enumerate()
    {
        let name = scenario["name"].as_str().unwrap();
        let exp = expect_of(
            "cancel_preempts_nonterminal.json",
            &format!("scenarios[{si}].expect"),
            &scenario["expect"],
        );
        let mut p = CommandProjection::new();
        for (i, frame) in scenario["frames"].as_array().unwrap().iter().enumerate() {
            // `ignored_frame_indices`: a late cancel against an already-terminal
            // command must leave the projection exactly as it was. The reducer
            // still *records* the input, so the observable is the image, not the
            // apply status. Previously unread on this fixture.
            let ignored = exp["ignored_frame_indices"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v.as_u64() == Some(i as u64)));
            let before = ignored.then(|| p.to_image());
            fold_frame(&mut p, frame);
            if let Some(before) = before {
                assert_eq!(
                    p.to_image(),
                    before,
                    "scenario {name} frame {i}: must be ignored"
                );
            }
        }
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
    let command_id = rpc["command_id"].as_str().unwrap();
    let resolves_at = rpc["resolves_after_frame_index"].as_u64().unwrap() as usize;
    let unresolved: Vec<usize> = rpc["unresolved_after_frame_indices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap() as usize)
        .collect();

    let mut p = CommandProjection::new();
    for (i, frame) in frames.iter().enumerate() {
        fold_frame(&mut p, frame);
        let resolved = p.terminal_for(command_id).is_some();
        if unresolved.contains(&i) {
            assert!(!resolved, "frame {i}: RPC call must NOT have resolved");
        }
        if i == resolves_at {
            assert!(resolved, "frame {i}: RPC call must resolve here");
        }
    }
    // `rpc.terminal_status`: WHICH terminal the call resolved to, not merely that
    // it resolved. A cancelled command also resolves an RPC call.
    let want_status = rpc["terminal_status"].as_str().expect("terminal_status");
    let got_status = p.terminal_for(command_id).expect("terminal").status;
    assert_eq!(
        serde_json::to_value(got_status).unwrap(),
        Value::String(want_status.to_owned()),
        "rpc.terminal_status"
    );
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
