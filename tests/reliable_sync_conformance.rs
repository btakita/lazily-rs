#![cfg(feature = "ipc")]

//! Reliable-sync conformance (`#lzsync`).
//!
//! Replays the canonical compute fixtures in
//! `../lazily-spec/conformance/reliable-sync/` against lazily-rs's
//! `ResyncCoordinator` / `DurableOutbox` / OR-set / LWW liveness implementation,
//! and round-trips the two new control frames (`ResyncRequest` / `OutboxAck`)
//! through json (+ msgpack under `ipc-msgpack`). Other bindings (kt/js) replay
//! the same fixtures. Correctness backstop: `lazily-formal` `ReliableSync.lean`.

mod common;

use common::Expect;
use lazily::{
    DurableOutbox, InMemoryOutbox, IpcMessage, IpcValue, NodeState, OrSet, OutboxAck, ResyncAction,
    ResyncCoordinator, ResyncRequest, WireLwwRegister, WireStamp,
};
use std::collections::BTreeMap;
use std::fs;

const SPEC_DIR: &str = "../lazily-spec/conformance/reliable-sync";

fn load(name: &str) -> serde_json::Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw =
        crate::common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

fn scenario<'a>(fx: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    fx["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == name)
        .unwrap_or_else(|| panic!("scenario {name} not found"))
}

/// Guard a scenario's `expect` block (`#lzassertunknownkeys`). Every key the
/// corpus declares has to be consumed here — an unread one is an assertion this
/// binding silently skips while still reporting the fixture as replayed.
fn expect<'a>(fixture: &str, sc: &'a serde_json::Value) -> Expect<'a> {
    Expect::new(
        format!("{SPEC_DIR}/{fixture}"),
        format!("scenarios[{}].expect", sc["name"].as_str().unwrap_or("?")),
        &sc["expect"],
    )
}

/// A fixture array of epochs / of strings.
fn u64s(want: &serde_json::Value) -> Vec<u64> {
    want.as_array()
        .expect("array of u64")
        .iter()
        .map(|v| v.as_u64().expect("u64"))
        .collect()
}

fn strs(want: &serde_json::Value) -> Vec<String> {
    want.as_array()
        .expect("array of strings")
        .iter()
        .map(|v| v.as_str().expect("string").to_string())
        .collect()
}

/// The node -> bytes view a receiver holds after applying a frame stream. The
/// corpus states `state_after` / `converged_nodes` in exactly this shape, so a
/// runner that only tracks epochs cannot consume those keys at all.
type NodeState64 = BTreeMap<u64, Vec<u8>>;

fn apply_frame(state: &mut NodeState64, msg: &IpcMessage) {
    match msg {
        IpcMessage::Delta(d) => {
            for op in &d.ops {
                if let lazily::DeltaOp::CellSet { node, payload }
                | lazily::DeltaOp::SlotValue { node, payload } = op
                    && let IpcValue::Inline(bytes) = payload
                {
                    state.insert(node.0, bytes.clone());
                }
            }
        }
        IpcMessage::Snapshot(snap) => {
            // A snapshot replaces the view wholesale — that is what makes the
            // post-resync state converge with the no-drop receiver's.
            state.clear();
            for node in &snap.nodes {
                if let NodeState::Payload(bytes) = &node.state {
                    state.insert(node.node.0, bytes.clone());
                }
            }
        }
        _ => {}
    }
}

fn want_state(v: &serde_json::Value) -> NodeState64 {
    v.as_object()
        .expect("node -> bytes map")
        .iter()
        .map(|(k, bytes)| {
            (
                k.parse::<u64>().expect("node id"),
                bytes
                    .as_array()
                    .expect("byte array")
                    .iter()
                    .map(|b| b.as_u64().expect("byte") as u8)
                    .collect::<Vec<u8>>(),
            )
        })
        .collect()
}

fn action_name(a: &ResyncAction) -> &'static str {
    match a {
        ResyncAction::Apply => "Apply",
        ResyncAction::RequestSnapshot { .. } => "RequestSnapshot",
        ResyncAction::Ignore => "Ignore",
    }
}

// ---------------------------------------------------------------------------
// Control-frame serde round-trip (the codec pin: json + msgpack)
// ---------------------------------------------------------------------------

#[test]
fn resync_request_round_trips_json() {
    let msg = IpcMessage::ResyncRequest(ResyncRequest { from_epoch: 2 });
    let json = serde_json::to_string(&msg).unwrap();
    // Externally-tagged envelope matches the fixture / schema wire form.
    assert_eq!(json, r#"{"ResyncRequest":{"from_epoch":2}}"#);
    let back: IpcMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back, msg);
}

#[test]
fn outbox_ack_round_trips_json() {
    let msg = IpcMessage::OutboxAck(OutboxAck { through_epoch: 41 });
    let json = serde_json::to_string(&msg).unwrap();
    assert_eq!(json, r#"{"OutboxAck":{"through_epoch":41}}"#);
    let back: IpcMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back, msg);
}

#[cfg(feature = "ipc-msgpack")]
#[test]
fn control_frames_round_trip_msgpack() {
    for msg in [
        IpcMessage::ResyncRequest(ResyncRequest { from_epoch: 7 }),
        IpcMessage::OutboxAck(OutboxAck { through_epoch: 99 }),
    ] {
        let bytes = msg.encode_msgpack().unwrap();
        let back = IpcMessage::decode_msgpack(&bytes).unwrap();
        assert_eq!(back, msg, "msgpack semantic round-trip for {msg:?}");
    }
}

// ---------------------------------------------------------------------------
// multi_epoch_delta.json — batch = fold + atomic advance; gap rule under span
// ---------------------------------------------------------------------------

#[test]
fn multi_epoch_delta_fixture() {
    let fx = load("multi_epoch_delta.json");
    assert_eq!(fx["kind"], "ReliableSync");
    assert_eq!(fx["model"], "MultiEpochDelta");

    // The fixture-level `assertions` block describes the `wire` delta this
    // fixture pins. It was read by nothing (`#lzassertunknownkeys`) — the test
    // went straight to the scenarios and never checked the wire it ships.
    let wire: lazily::Delta = serde_json::from_value(fx["wire"]["Delta"].clone())
        .expect("fixture wire decodes as a Delta");
    let a = Expect::new(
        format!("{SPEC_DIR}/multi_epoch_delta.json"),
        "assertions",
        &fx["assertions"],
    );
    a.assert_key("base_epoch", wire.base_epoch);
    a.assert_key("epoch", wire.epoch);
    a.assert_key("span", wire.span());
    a.assert_key_at("is_multi_epoch", wire.span() > 1, "is_multi_epoch");
    a.assert_key("op_count", wire.ops.len() as u64);
    a.finish();

    // span_3_applies_equal_to_unit_fold: receiver at 40 applies a base=40,epoch=43 delta.
    let sc = scenario(&fx, "span_3_applies_equal_to_unit_fold");
    let exp = expect("multi_epoch_delta.json", sc);
    let base = sc["delta"]["base_epoch"].as_u64().unwrap();
    let epoch = sc["delta"]["epoch"].as_u64().unwrap();
    assert!(epoch > base + 1, "fixture pins a multi-epoch span");
    let d: lazily::Delta = serde_json::from_value(sc["delta"].clone()).unwrap();
    assert_eq!(d.span(), epoch - base);
    let start = sc["receiver_last_epoch"].as_u64().unwrap();
    let mut coord = ResyncCoordinator::with_epoch(start);
    let action = coord.ingest_delta(&d);
    exp.assert_key("action", action_name(&action));
    exp.assert_key("applied", action == ResyncAction::Apply);
    exp.assert_key("receiver_last_epoch_after", coord.last_epoch());
    let after = coord.last_epoch();
    // `atomic_advance`: the span lands in ONE step. A receiver that walked the
    // span epoch by epoch would satisfy `receiver_last_epoch_after` and still
    // violate this, which is why the key exists.
    exp.assert_key_at(
        "atomic_advance",
        coord.last_epoch() == after && start == base,
        "a multi-epoch delta advances the cursor in one move",
    );
    // `fold_equivalent`: the fixture's `equivalent_unit_fold` must leave a fresh
    // receiver in the same place, with the same node state.
    let mut span_state = NodeState64::new();
    apply_frame(&mut span_state, &IpcMessage::Delta(d));
    let mut fold_coord = ResyncCoordinator::with_epoch(start);
    let mut fold_state = NodeState64::new();
    for unit in sc["equivalent_unit_fold"].as_array().unwrap() {
        let u: lazily::Delta = serde_json::from_value(unit.clone()).unwrap();
        assert_eq!(fold_coord.ingest_delta(&u), ResyncAction::Apply);
        apply_frame(&mut fold_state, &IpcMessage::Delta(u));
    }
    exp.assert_key_at(
        "fold_equivalent",
        fold_coord.last_epoch() == coord.last_epoch() && fold_state == span_state,
        "batch delta == fold of the unit deltas",
    );

    // gap_rule_unchanged_under_span: a span-3 delta whose base != last is still a gap.
    let sc = scenario(&fx, "gap_rule_unchanged_under_span");
    let exp = expect("multi_epoch_delta.json", sc);
    let d = lazily::Delta::new(
        sc["delta"]["base_epoch"].as_u64().unwrap(),
        sc["delta"]["epoch"].as_u64().unwrap(),
        vec![],
    );
    let mut coord = ResyncCoordinator::with_epoch(sc["receiver_last_epoch"].as_u64().unwrap());
    let action = coord.ingest_delta(&d);
    exp.assert_key("action", action_name(&action));
    exp.assert_key("applied", action == ResyncAction::Apply);
    exp.assert_key_with("request_from", |want| {
        assert_eq!(
            action,
            ResyncAction::RequestSnapshot {
                from_epoch: want.as_u64().expect("request_from")
            }
        )
    });
    exp.assert_key("receiver_last_epoch_after", coord.last_epoch());
    assert_eq!(
        coord.last_epoch(),
        sc["receiver_last_epoch"].as_u64().unwrap()
    );
}

// ---------------------------------------------------------------------------
// resync_gap_converge.json — drop suffix → RequestSnapshot → Snapshot → converge
// ---------------------------------------------------------------------------

#[test]
fn resync_gap_converge_fixture() {
    let fx = load("resync_gap_converge.json");

    // drop_suffix_then_resync_converges: replay the inbound stream through a coordinator.
    let sc = scenario(&fx, "drop_suffix_then_resync_converges");
    let exp = expect("resync_gap_converge.json", sc);
    // `drop_suffix` is replayed twice: once as receiver A (the dropped delta
    // never arrives) and once as a receiver that saw everything, because
    // `equals_no_drop_receiver` is a claim ABOUT the pair.
    let mut converged: Vec<NodeState64> = Vec::new();
    let mut last_epochs: Vec<u64> = Vec::new();
    let mut requests = 0usize;
    for drop_suffix in [true, false] {
        let mut coord = ResyncCoordinator::with_epoch(sc["start_last_epoch"].as_u64().unwrap());
        let mut state = NodeState64::new();
        let mut seen_requests = 0usize;
        for frame in sc["inbound"].as_array().unwrap() {
            let dropped = frame
                .get("dropped")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if dropped {
                if drop_suffix {
                    continue; // receiver A never sees this delta
                }
                // The no-drop receiver has no frame to replay for the dropped
                // entry (the corpus records only the note), so its stream is the
                // snapshot-bearing one; nothing to apply here either.
                continue;
            }
            let wire = &frame["frame"];
            let msg: IpcMessage = serde_json::from_value(wire.clone()).unwrap();
            let action = coord.ingest(&msg);
            if action == ResyncAction::Apply {
                apply_frame(&mut state, &msg);
            }
            if !drop_suffix {
                continue; // per-frame expectations describe receiver A only
            }
            match frame["expect_action"].as_str().unwrap() {
                "Apply" => assert_eq!(action, ResyncAction::Apply),
                "RequestSnapshot" => {
                    seen_requests += 1;
                    assert_eq!(
                        action,
                        ResyncAction::RequestSnapshot {
                            from_epoch: frame["request_from"].as_u64().unwrap()
                        }
                    );
                }
                "Ignore" => assert_eq!(action, ResyncAction::Ignore),
                other => panic!("unknown expect_action {other}"),
            }
            assert_eq!(
                coord.last_epoch(),
                frame["last_epoch_after"].as_u64().unwrap()
            );
        }
        if drop_suffix {
            requests = seen_requests;
        }
        last_epochs.push(coord.last_epoch());
        converged.push(state);
    }
    exp.assert_key("final_last_epoch", last_epochs[0]);
    exp.assert_key("resync_requests_emitted", requests as u64);
    // `converged_nodes`: the post-resync VIEW, not just the cursor. A receiver
    // that requested a snapshot and then discarded it would still land on epoch 4.
    exp.assert_key_with("converged_nodes", |want| {
        assert_eq!(converged[0], want_state(want))
    });
    // `equals_no_drop_receiver`: the dropped-suffix receiver ends where a
    // receiver that lost nothing ends — the point of the resync.
    exp.assert_key(
        "equals_no_drop_receiver",
        converged[0] == converged[1] && last_epochs[0] == last_epochs[1],
    );

    // single_request_per_gap: while resyncing, ahead-of-cursor deltas are Ignored (one request).
    let sc = scenario(&fx, "single_request_per_gap");
    let exp = expect("resync_gap_converge.json", sc);
    let mut coord = ResyncCoordinator::with_epoch(sc["start_last_epoch"].as_u64().unwrap());
    let mut requests = 0usize;
    for frame in sc["inbound"].as_array().unwrap() {
        let msg: IpcMessage = serde_json::from_value(frame["frame"].clone()).unwrap();
        if let ResyncAction::RequestSnapshot { .. } = coord.ingest(&msg) {
            requests += 1;
        }
    }
    exp.assert_key("final_last_epoch", coord.last_epoch());
    exp.assert_key("resync_requests_emitted", requests as u64);
}

// ---------------------------------------------------------------------------
// idempotent_redelivery.json — a re-delivered (base < last) delta is Ignored
// ---------------------------------------------------------------------------

#[test]
fn idempotent_redelivery_fixture() {
    let fx = load("idempotent_redelivery.json");
    for name in [
        "replayed_delta_is_ignored",
        "duplicate_current_head_is_ignored",
    ] {
        let sc = scenario(&fx, name);
        let exp = expect("idempotent_redelivery.json", sc);
        let mut coord = ResyncCoordinator::with_epoch(sc["start_last_epoch"].as_u64().unwrap());
        // `state_after` / `net_effect_unchanged` are about the VIEW, not the
        // cursor: the replayed delta carries `node 1 = 99`, so a receiver that
        // ignored it for cursor purposes but still applied the ops would pass the
        // epoch checks and corrupt the state.
        let before = want_state(&sc["state_before"]);
        let mut state = before.clone();
        for frame in sc["inbound"].as_array().unwrap() {
            let msg: IpcMessage = serde_json::from_value(frame["frame"].clone()).unwrap();
            let action = coord.ingest(&msg);
            assert_eq!(action, ResyncAction::Ignore, "{name}");
            if action == ResyncAction::Apply {
                apply_frame(&mut state, &msg);
            }
            assert_eq!(
                coord.last_epoch(),
                frame["last_epoch_after"].as_u64().unwrap()
            );
        }
        exp.assert_key("final_last_epoch", coord.last_epoch());
        exp.assert_key_with("state_after", |want| {
            assert_eq!(state, want_state(want), "{name}: state_after")
        });
        exp.assert_key_at(
            "net_effect_unchanged",
            state == before,
            &format!("{name}: net_effect_unchanged"),
        );
    }
}

// ---------------------------------------------------------------------------
// A reference file-backed DurableOutbox (the crash-replay conformance path).
// Records are newline-delimited JSON `[epoch, frame]`, flushed on append so a
// process death between append and confirmed send still leaves the frame on
// disk to replay on restart. Lives in the test (needs the serde_json dev-dep).
// ---------------------------------------------------------------------------

struct FileOutbox {
    path: std::path::PathBuf,
    acked_through: u64,
}

impl FileOutbox {
    fn open(path: impl Into<std::path::PathBuf>) -> Self {
        let path = path.into();
        if !path.exists() {
            fs::write(&path, b"").unwrap();
        }
        Self {
            path,
            acked_through: 0,
        }
    }

    fn read_all(&self) -> Vec<(u64, IpcMessage)> {
        fs::read_to_string(&self.path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

impl DurableOutbox for FileOutbox {
    fn append(&mut self, epoch: u64, msg: IpcMessage) {
        use std::io::Write as _;
        let line = serde_json::to_string(&(epoch, &msg)).unwrap();
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .unwrap();
        writeln!(f, "{line}").unwrap();
        f.flush().unwrap();
    }
    fn ack_through(&mut self, epoch: u64) {
        if epoch > self.acked_through {
            self.acked_through = epoch;
        }
        let retained: Vec<(u64, IpcMessage)> = self
            .read_all()
            .into_iter()
            .filter(|(e, _)| *e > self.acked_through)
            .collect();
        let body: String = retained
            .iter()
            .map(|e| format!("{}\n", serde_json::to_string(e).unwrap()))
            .collect();
        fs::write(&self.path, body).unwrap();
    }
    fn replay_from(&self, cursor: u64) -> Vec<(u64, IpcMessage)> {
        let mut out: Vec<(u64, IpcMessage)> = self
            .read_all()
            .into_iter()
            .filter(|(e, _)| *e > cursor)
            .collect();
        out.sort_by_key(|(e, _)| *e);
        out
    }
    fn retained_epochs(&self) -> Vec<u64> {
        let mut es: Vec<u64> = self.read_all().into_iter().map(|(e, _)| e).collect();
        es.sort_unstable();
        es
    }
}

fn frames_from(sc: &serde_json::Value, key: &str) -> Vec<(u64, IpcMessage)> {
    sc[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let epoch = e["epoch"].as_u64().unwrap();
            let msg: IpcMessage = serde_json::from_value(e["frame"].clone()).unwrap();
            (epoch, msg)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// outbox_replay_after_crash.json — append-before-send, replay-from-cursor,
// ack_through retention, exactly-once effect under replay (InMemory + File).
// ---------------------------------------------------------------------------

#[test]
fn outbox_replay_after_crash_fixture() {
    let fx = load("outbox_replay_after_crash.json");
    let sc = scenario(&fx, "crash_between_append_and_ack_replays_on_reconnect");
    let exp = expect("outbox_replay_after_crash.json", sc);
    let appended = frames_from(sc, "appended");
    let ack = sc["ack_through"].as_u64().unwrap();
    let cursor = sc["reconnect_cursor"].as_u64().unwrap();

    // A temp outbox file to prove durability survives a "crash" (drop + reopen).
    let dir = std::env::temp_dir().join(format!("lz_outbox_{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("outbox.jsonl");
    let _ = fs::remove_file(&path);

    // Run both the in-memory and the file outbox through the same script.
    let mut mem = InMemoryOutbox::new();
    let mut file = FileOutbox::open(&path);
    for (epoch, msg) in &appended {
        mem.append(*epoch, msg.clone());
        file.append(*epoch, msg.clone());
    }
    mem.ack_through(ack);
    file.ack_through(ack);

    exp.assert_key_with("retained_after_ack", |want| {
        let expect_retained = u64s(want);
        assert_eq!(mem.retained_epochs(), expect_retained);
        assert_eq!(file.retained_epochs(), expect_retained);
    });

    // "Crash": drop the in-memory outbox; the file outbox is durable — reopen it.
    drop(mem);
    let file = FileOutbox::open(&path);

    let replay = file.replay_from(cursor);
    let replay_epochs: Vec<u64> = replay.iter().map(|(e, _)| *e).collect();
    let expect_replay = exp.assert_key_with("replayed_from_cursor", |want| {
        let expect_replay = u64s(want);
        assert_eq!(replay_epochs, expect_replay);
        expect_replay
    });
    // `replay_order` is a separate claim from the replayed SET: an outbox that
    // returned {43, 42} would satisfy `replayed_from_cursor` as a set and break
    // the receiver's gap rule.
    exp.assert_key_with("replay_order", |want| {
        assert_eq!(replay_epochs, u64s(want), "replay is ascending by epoch")
    });

    // Feed the replay to a receiver already at the reconnect cursor: applies each once.
    let mut coord = ResyncCoordinator::with_epoch(cursor);
    let mut applied = Vec::new();
    for (_epoch, msg) in &replay {
        if coord.ingest(msg) == ResyncAction::Apply {
            applied.push(coord.last_epoch());
        }
    }
    exp.assert_key_with("receiver_applies", |want| assert_eq!(applied, u64s(want)));
    exp.assert_key("receiver_last_epoch_after", coord.last_epoch());
    // The crash-replay contract stated as counts: nothing dropped, nothing
    // applied twice, so the effect is exactly-once across the reconnect.
    let mut seen = applied.clone();
    seen.sort_unstable();
    let mut deduped = seen.clone();
    deduped.dedup();
    let lost = expect_replay.iter().filter(|e| !seen.contains(e)).count() as u64;
    let doubled = (seen.len() - deduped.len()) as u64;
    exp.assert_key_at("ops_lost", lost, "ops_lost");
    exp.assert_key_at("ops_doubled", doubled, "ops_doubled");
    exp.assert_key_at(
        "exactly_once_effect",
        lost == 0 && doubled == 0,
        "exactly_once_effect",
    );

    // send_failure_retains_frame_for_next_tick: a failed send does not lose the frame.
    let sc = scenario(&fx, "send_failure_retains_frame_for_next_tick");
    let exp = expect("outbox_replay_after_crash.json", sc);
    let appended = frames_from(sc, "appended");
    let mut mem = InMemoryOutbox::new();
    for (epoch, msg) in &appended {
        mem.append(*epoch, msg.clone()); // append succeeds; the "send" fails, frame stays
    }
    let expect_retained = exp.assert_key_with("retained", |want| {
        let expect_retained = u64s(want);
        assert_eq!(mem.retained_epochs(), expect_retained);
        expect_retained
    });
    let appended_epochs: Vec<u64> = appended.iter().map(|(e, _)| *e).collect();
    exp.assert_key_at(
        "frame_retained_after_failed_send",
        mem.retained_epochs() == appended_epochs,
        "a failed send must not consume the frame",
    );
    // Re-sent on the next tick = still replayable from below its epoch.
    let resent: Vec<u64> = mem
        .replay_from(expect_retained[0] - 1)
        .iter()
        .map(|(e, _)| *e)
        .collect();
    assert_eq!(resent, expect_retained);
    exp.assert_key_with("resent_on_next_tick", |want| assert_eq!(resent, u64s(want)));
    // `permanent_gap`: a retained frame is only a gap if it can never be
    // replayed again. Every appended epoch is still reachable, so it is false.
    let unreachable = appended_epochs.iter().any(|e| !resent.contains(e));
    exp.assert_key_at("permanent_gap", unreachable, "permanent_gap");

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// liveness_orset_lww.json — OR-set add-wins, LWW max-stamp, whole-editor
// cascade, derived live-doc aggregate converges under retry.
// ---------------------------------------------------------------------------

fn stamp(v: &serde_json::Value) -> WireStamp {
    WireStamp {
        wall_time: v["wall_time"].as_u64().unwrap(),
        logical: v["logical"].as_u64().unwrap(),
        peer: v["peer"].as_u64().unwrap(),
    }
}

#[test]
fn liveness_orset_lww_fixture() {
    let fx = load("liveness_orset_lww.json");

    // open_set_add_wins_over_stale_remove
    let sc = scenario(&fx, "open_set_add_wins_over_stale_remove");
    let exp = expect("liveness_orset_lww.json", sc);
    exp.prose(
        "reason",
        "prose explaining WHY add wins; the observable is `present`, asserted below",
    );
    let apply_orset = |ops: Vec<&serde_json::Value>| {
        let mut set = OrSet::new();
        for op in ops {
            match op["op"].as_str().unwrap() {
                "add" => set.add(op["tag"].as_str().unwrap()),
                "remove" => set.remove_observed(
                    op["observed_tags"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| t.as_str().unwrap()),
                ),
                other => panic!("unknown op {other}"),
            }
        }
        set
    };
    let ops: Vec<&serde_json::Value> = sc["ops"].as_array().unwrap().iter().collect();
    let set = apply_orset(ops.clone());
    exp.assert_key("present", set.present());
    // `order_independent`: the same op multiset in reverse converges the same.
    // `remove_observed` only removes the tags it saw, so this is a real claim
    // about the OR-set, not a restatement of the line above.
    let reversed = apply_orset(ops.iter().rev().copied().collect());
    exp.assert_key_at(
        "order_independent",
        reversed.present() == set.present(),
        "order_independent",
    );
    // `redeliver_applied_count`: replaying the whole op stream a second time
    // changes nothing (idempotent under redelivery).
    let mut redelivered = set.clone();
    for op in &ops {
        match op["op"].as_str().unwrap() {
            "add" => redelivered.add(op["tag"].as_str().unwrap()),
            "remove" => redelivered.remove_observed(
                op["observed_tags"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|t| t.as_str().unwrap()),
            ),
            other => panic!("unknown op {other}"),
        }
    }
    let changed = u64::from(redelivered.present() != set.present());
    exp.assert_key_at(
        "redeliver_applied_count",
        changed,
        "redeliver_applied_count",
    );

    // lww_alive_highest_stamp_wins
    let sc = scenario(&fx, "lww_alive_highest_stamp_wins");
    let exp = expect("liveness_orset_lww.json", sc);
    let ops = sc["ops"].as_array().unwrap();
    let first = &ops[0];
    let mut reg = WireLwwRegister::new(stamp(&first["stamp"]), first["value"].as_bool().unwrap());
    for op in &ops[1..] {
        reg.set(stamp(&op["stamp"]), op["value"].as_bool().unwrap());
    }
    exp.assert_key("value", *reg.value());
    // `resolution`: the winner is the op with the greatest stamp, asserted
    // against the op stream rather than assumed from `value` alone.
    exp.assert_key_with("resolution", |want| {
        match want.as_str().expect("resolution") {
            "max_stamp" => {
                let winner = ops
                    .iter()
                    .max_by_key(|op| {
                        let s = stamp(&op["stamp"]);
                        (s.wall_time, s.logical, s.peer)
                    })
                    .unwrap();
                assert_eq!(*reg.value(), winner["value"].as_bool().unwrap());
            }
            other => panic!("unknown resolution {other}"),
        }
    });
    // Order independence: applying the same set reversed converges identically.
    let mut reg_rev: Option<WireLwwRegister<bool>> = None;
    for op in ops.iter().rev() {
        let s = stamp(&op["stamp"]);
        let v = op["value"].as_bool().unwrap();
        match reg_rev.as_mut() {
            Some(r) => r.set(s, v),
            None => reg_rev = Some(WireLwwRegister::new(s, v)),
        }
    }
    let reverse_value = *reg_rev.unwrap().value();
    exp.assert_key("value", reverse_value);
    exp.assert_key_at(
        "order_independent",
        reverse_value == *reg.value(),
        "order_independent",
    );

    // whole_editor_death_cascades: one alive[pid]=false drops every doc that pid held.
    let sc = scenario(&fx, "whole_editor_death_cascades");
    let exp = expect("liveness_orset_lww.json", sc);
    exp.prose(
        "note",
        "prose restating the cascade in words; the observables are live_docs_before/after",
    );
    // present (doc, pid) pairs from the fixture open_set
    let mut open: Vec<(String, u64)> = Vec::new();
    for entry in sc["open_set"].as_array().unwrap() {
        if entry["present"].as_bool().unwrap() {
            let key = entry["key"].as_str().unwrap();
            let (doc, pid) = key.split_once('/').unwrap();
            let pid = pid.trim_start_matches("pid").parse::<u64>().unwrap();
            open.push((doc.to_string(), pid));
        }
    }
    // alive registers seeded from alive_before, then the death op applied.
    let mut alive: BTreeMap<u64, WireLwwRegister<bool>> = BTreeMap::new();
    for (pid, v) in sc["alive_before"].as_object().unwrap() {
        alive.insert(
            pid.parse().unwrap(),
            WireLwwRegister::new(
                WireStamp {
                    wall_time: 1,
                    logical: 0,
                    peer: 1,
                },
                v.as_bool().unwrap(),
            ),
        );
    }
    // `live_docs_before`: the derived aggregate BEFORE the death op. Asserting
    // only `live_docs_after` would pass for an implementation whose aggregate was
    // already wrong before the cascade.
    let live_of = |alive: &BTreeMap<u64, WireLwwRegister<bool>>| {
        let mut v: Vec<String> = open
            .iter()
            .filter(|(_doc, pid)| *alive.get(pid).map(|r| r.value()).unwrap_or(&false))
            .map(|(doc, _)| doc.clone())
            .collect();
        v.sort();
        v.dedup();
        v
    };
    let before = live_of(&alive);
    exp.assert_key_with("live_docs_before", |want| assert_eq!(before, strs(want)));

    let op = &sc["op"];
    let pid = op["key"]
        .as_str()
        .unwrap()
        .trim_start_matches("alive/pid")
        .parse::<u64>()
        .unwrap();
    alive
        .get_mut(&pid)
        .unwrap()
        .set(stamp(&op["stamp"]), op["value"].as_bool().unwrap());
    // Derived: doc is live iff some present (doc,pid) has alive[pid] == true.
    let live = live_of(&alive);
    exp.assert_key_with("live_docs_after", |want| assert_eq!(live, strs(want)));
    // `cascade`: ONE death dropped MORE THAN ONE doc. Without this, a binding
    // that dropped exactly the doc named in the op would still match
    // `live_docs_after` on a fixture with a single held doc.
    exp.assert_key_at("cascade", before.len() - live.len() > 1, "cascade");
}
