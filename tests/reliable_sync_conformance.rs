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

use lazily::{
    DurableOutbox, InMemoryOutbox, IpcMessage, OrSet, OutboxAck, ResyncAction, ResyncCoordinator,
    ResyncRequest, WireLwwRegister, WireStamp,
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

    // span_3_applies_equal_to_unit_fold: receiver at 40 applies a base=40,epoch=43 delta.
    let sc = scenario(&fx, "span_3_applies_equal_to_unit_fold");
    let base = sc["delta"]["base_epoch"].as_u64().unwrap();
    let epoch = sc["delta"]["epoch"].as_u64().unwrap();
    assert!(epoch > base + 1, "fixture pins a multi-epoch span");
    let d = lazily::Delta::new(base, epoch, vec![]);
    assert_eq!(d.span(), epoch - base);
    let mut coord = ResyncCoordinator::with_epoch(sc["receiver_last_epoch"].as_u64().unwrap());
    assert_eq!(coord.ingest_delta(&d), ResyncAction::Apply);
    assert_eq!(
        coord.last_epoch(),
        sc["expect"]["receiver_last_epoch_after"].as_u64().unwrap()
    );

    // gap_rule_unchanged_under_span: a span-3 delta whose base != last is still a gap.
    let sc = scenario(&fx, "gap_rule_unchanged_under_span");
    let d = lazily::Delta::new(
        sc["delta"]["base_epoch"].as_u64().unwrap(),
        sc["delta"]["epoch"].as_u64().unwrap(),
        vec![],
    );
    let mut coord = ResyncCoordinator::with_epoch(sc["receiver_last_epoch"].as_u64().unwrap());
    assert_eq!(
        coord.ingest_delta(&d),
        ResyncAction::RequestSnapshot {
            from_epoch: sc["expect"]["request_from"].as_u64().unwrap()
        }
    );
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
    let mut coord = ResyncCoordinator::with_epoch(sc["start_last_epoch"].as_u64().unwrap());
    let mut requests = 0usize;
    for frame in sc["inbound"].as_array().unwrap() {
        if frame
            .get("dropped")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            continue; // receiver A never sees this delta
        }
        let wire = &frame["frame"];
        let msg: IpcMessage = serde_json::from_value(wire.clone()).unwrap();
        let action = coord.ingest(&msg);
        match frame["expect_action"].as_str().unwrap() {
            "Apply" => assert_eq!(action, ResyncAction::Apply),
            "RequestSnapshot" => {
                requests += 1;
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
    assert_eq!(
        coord.last_epoch(),
        sc["expect"]["final_last_epoch"].as_u64().unwrap()
    );
    assert_eq!(
        requests,
        sc["expect"]["resync_requests_emitted"].as_u64().unwrap() as usize
    );

    // single_request_per_gap: while resyncing, ahead-of-cursor deltas are Ignored (one request).
    let sc = scenario(&fx, "single_request_per_gap");
    let mut coord = ResyncCoordinator::with_epoch(sc["start_last_epoch"].as_u64().unwrap());
    let mut requests = 0usize;
    for frame in sc["inbound"].as_array().unwrap() {
        let msg: IpcMessage = serde_json::from_value(frame["frame"].clone()).unwrap();
        if let ResyncAction::RequestSnapshot { .. } = coord.ingest(&msg) {
            requests += 1;
        }
    }
    assert_eq!(
        coord.last_epoch(),
        sc["expect"]["final_last_epoch"].as_u64().unwrap()
    );
    assert_eq!(
        requests,
        sc["expect"]["resync_requests_emitted"].as_u64().unwrap() as usize
    );
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
        let mut coord = ResyncCoordinator::with_epoch(sc["start_last_epoch"].as_u64().unwrap());
        for frame in sc["inbound"].as_array().unwrap() {
            let msg: IpcMessage = serde_json::from_value(frame["frame"].clone()).unwrap();
            assert_eq!(coord.ingest(&msg), ResyncAction::Ignore, "{name}");
            assert_eq!(
                coord.last_epoch(),
                frame["last_epoch_after"].as_u64().unwrap()
            );
        }
        assert_eq!(
            coord.last_epoch(),
            sc["expect"]["final_last_epoch"].as_u64().unwrap()
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

    let expect_retained: Vec<u64> = sc["expect"]["retained_after_ack"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(mem.retained_epochs(), expect_retained);
    assert_eq!(file.retained_epochs(), expect_retained);

    // "Crash": drop the in-memory outbox; the file outbox is durable — reopen it.
    drop(mem);
    let file = FileOutbox::open(&path);

    let replay = file.replay_from(cursor);
    let replay_epochs: Vec<u64> = replay.iter().map(|(e, _)| *e).collect();
    let expect_replay: Vec<u64> = sc["expect"]["replayed_from_cursor"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(replay_epochs, expect_replay);

    // Feed the replay to a receiver already at the reconnect cursor: applies each once.
    let mut coord = ResyncCoordinator::with_epoch(cursor);
    let mut applied = Vec::new();
    for (_epoch, msg) in &replay {
        if coord.ingest(msg) == ResyncAction::Apply {
            applied.push(coord.last_epoch());
        }
    }
    let expect_applies: Vec<u64> = sc["expect"]["receiver_applies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(applied, expect_applies);
    assert_eq!(
        coord.last_epoch(),
        sc["expect"]["receiver_last_epoch_after"].as_u64().unwrap()
    );

    // send_failure_retains_frame_for_next_tick: a failed send does not lose the frame.
    let sc = scenario(&fx, "send_failure_retains_frame_for_next_tick");
    let appended = frames_from(sc, "appended");
    let mut mem = InMemoryOutbox::new();
    for (epoch, msg) in &appended {
        mem.append(*epoch, msg.clone()); // append succeeds; the "send" fails, frame stays
    }
    let expect_retained: Vec<u64> = sc["expect"]["retained"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(mem.retained_epochs(), expect_retained);
    // Re-sent on the next tick = still replayable from below its epoch.
    let resent: Vec<u64> = mem
        .replay_from(expect_retained[0] - 1)
        .iter()
        .map(|(e, _)| *e)
        .collect();
    assert_eq!(resent, expect_retained);

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
    let mut set = OrSet::new();
    for op in sc["ops"].as_array().unwrap() {
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
    assert_eq!(set.present(), sc["expect"]["present"].as_bool().unwrap());

    // lww_alive_highest_stamp_wins
    let sc = scenario(&fx, "lww_alive_highest_stamp_wins");
    let ops = sc["ops"].as_array().unwrap();
    let first = &ops[0];
    let mut reg = WireLwwRegister::new(stamp(&first["stamp"]), first["value"].as_bool().unwrap());
    for op in &ops[1..] {
        reg.set(stamp(&op["stamp"]), op["value"].as_bool().unwrap());
    }
    assert_eq!(*reg.value(), sc["expect"]["value"].as_bool().unwrap());
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
    assert_eq!(
        *reg_rev.unwrap().value(),
        sc["expect"]["value"].as_bool().unwrap()
    );

    // whole_editor_death_cascades: one alive[pid]=false drops every doc that pid held.
    let sc = scenario(&fx, "whole_editor_death_cascades");
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
    let mut live: Vec<String> = open
        .iter()
        .filter(|(_doc, pid)| *alive.get(pid).map(|r| r.value()).unwrap_or(&false))
        .map(|(doc, _)| doc.clone())
        .collect();
    live.sort();
    live.dedup();
    let expect_live: Vec<String> = sc["expect"]["live_docs_after"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(live, expect_live);
}
