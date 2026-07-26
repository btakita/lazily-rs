//! Canonical distributed CRDT-plane conformance (`#verifycrdtplaneruntimein`).
//!
//! lazily-rs had no runner for `lazily-spec/conformance/distributed/` — both
//! fixtures sat in the coverage allowlist. Its `ingest` counting is correct (the
//! count comes from `OpLog::apply_remote`, which increments on LOG INSERTION and
//! is therefore independent of whether an op became the winner), but "correct on
//! inspection" is not a gate. lazily-cpp had exactly this contract wrong —
//! returning the number of ops that CHANGED THE WINNER, so `lww_last_writer_wins`
//! reported 4 instead of 5 — and nothing in rs would have caught the same slip.
//!
//! The distinction the fixture pins: an op that is superseded still COUNTS as
//! applied, because it entered the log. Only redelivery of an already-logged op
//! applies zero. Conflating "applied" with "changed the winner" breaks the
//! idempotence rule from the other side — it under-reports first delivery rather
//! than over-reporting redelivery.

#![cfg(all(feature = "distributed", feature = "webrtc"))]

mod common;

use lazily::{
    Context, CrdtOp, CrdtPlaneRuntime, HlcStamp, IpcValue, LwwRegister, NodeId, NodeKey, PeerId,
    ReplicatedCell, WireStamp,
};
use serde_json::Value;

const SPEC: &str = "../lazily-spec/conformance/distributed";

fn load(name: &str) -> Option<Value> {
    let text = crate::common::spec_read_to_string(format!("{SPEC}/{name}")).ok()?;
    Some(serde_json::from_str(&text).expect("fixture parses"))
}

fn op_of(v: &Value) -> CrdtOp {
    let s = &v["stamp"];
    let bytes: Vec<u8> = v["state"]["Inline"]
        .as_array()
        .expect("Inline state")
        .iter()
        .map(|b| b.as_u64().expect("byte") as u8)
        .collect();
    CrdtOp {
        node: NodeId(v["node"].as_u64().expect("node")),
        key: v["key"]
            .as_str()
            .map(|k| NodeKey::new(k).expect("valid key")),
        stamp: WireStamp {
            wall_time: s["wall_time"].as_u64().expect("wall_time"),
            logical: s["logical"].as_u64().expect("logical"),
            peer: s["peer"].as_u64().expect("peer"),
        },
        state: IpcValue::Inline(bytes),
    }
}

/// Register a byte-valued LWW cell per node the scenario touches, so converged
/// state is observable rather than assumed. Without this the runner would assert
/// only counts and quietly skip the `converged` half of every scenario.
fn seeded_runtime(ctx: &Context, ops: &[CrdtOp]) -> CrdtPlaneRuntime {
    let mut rt = CrdtPlaneRuntime::new(PeerId(9));
    let mut seen: Vec<(NodeId, Option<NodeKey>)> = Vec::new();
    for op in ops {
        let id = (op.node, op.key.clone());
        if seen.contains(&id) {
            continue;
        }
        seen.push(id.clone());
        let seed = HlcStamp::from(WireStamp {
            wall_time: 0,
            logical: 0,
            peer: 9,
        });
        let cell: ReplicatedCell<LwwRegister<Vec<u8>>> = ReplicatedCell::lww(ctx, Vec::new(), seed);
        rt.register(id.0, id.1, cell);
    }
    rt
}

fn ingest_ops(rt: &mut CrdtPlaneRuntime, ctx: &Context, ops: &[CrdtOp]) -> usize {
    let sync = lazily::CrdtSync {
        frontier: Vec::new(),
        ops: ops.to_vec(),
    };
    rt.ingest(ctx, &sync, 0)
}

#[test]
fn anti_entropy_converge_conformance() {
    let Some(fixture) = load("anti_entropy_converge.json") else {
        eprintln!("SKIP: lazily-spec sibling missing");
        return;
    };
    assert_eq!(fixture["model"], "CrdtPlane");
    let scenarios = fixture["scenarios"].as_array().expect("scenarios");
    assert!(
        !scenarios.is_empty(),
        "a replay of zero scenarios is not a replay"
    );

    let mut checked_counts = 0usize;
    let mut checked_redeliver = 0usize;
    let mut checked_converged = 0usize;

    for sc in scenarios {
        let name = sc["name"].as_str().expect("name");
        let ops: Vec<CrdtOp> = sc["ops"]
            .as_array()
            .expect("ops")
            .iter()
            .map(op_of)
            .collect();
        let ctx = Context::new();
        let mut rt = seeded_runtime(&ctx, &ops);

        let applied = ingest_ops(&mut rt, &ctx, &ops);
        let want = sc["expect"]["applied_count"]
            .as_u64()
            .expect("applied_count") as usize;
        assert_eq!(
            applied, want,
            "{name}: applied_count. A superseded op still counts — it entered the \
             log. Counting only ops that changed the winner is the lazily-cpp bug."
        );
        checked_counts += 1;

        if sc
            .get("redeliver")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let again = ingest_ops(&mut rt, &ctx, &ops);
            let want_rd = sc["expect"]["redeliver_applied_count"]
                .as_u64()
                .expect("redeliver_applied_count") as usize;
            assert_eq!(again, want_rd, "{name}: redelivery must apply {want_rd}");
            checked_redeliver += 1;
        }

        // The converged view, now assertable: `CrdtPlaneRuntime::converged()` folds
        // the op log into the winning payload per (node, key), which is the shape
        // the fixture states convergence in. Before that accessor existed this half
        // was skipped, and a runner that asserts counts while silently dropping
        // `converged` is the marker-only failure this suite exists to prevent.
        if let Some(want_entries) = sc["expect"]["converged"].as_array() {
            let got = rt.converged();
            for entry in want_entries {
                let node = NodeId(entry["node"].as_u64().expect("node"));
                let want_bytes: Vec<u8> = entry["state"]["Inline"]
                    .as_array()
                    .expect("Inline")
                    .iter()
                    .map(|b| b.as_u64().expect("byte") as u8)
                    .collect();
                let found = got
                    .iter()
                    .find(|e| e.node == node)
                    .unwrap_or_else(|| panic!("{name}: no converged entry for {node:?}"));
                let IpcValue::Inline(got_bytes) = &found.state else {
                    panic!("{name}: converged state for {node:?} is not Inline");
                };
                assert_eq!(
                    got_bytes, &want_bytes,
                    "{name}: converged state for {node:?} — the winner is the \
                     greatest stamp, with peer as the final tiebreak"
                );
                checked_converged += 1;
            }
        }
    }

    // Positive proof, not an absence guard: a runner that silently skipped every
    // scenario body would otherwise pass.
    assert!(
        checked_counts >= 3 && checked_redeliver >= 1 && checked_converged >= 4,
        "too little asserted: counts={checked_counts} redeliver={checked_redeliver} \
         converged={checked_converged}"
    );
}

/// `crdt_sync_frames.json` is a WIRE fixture, not a plane-behaviour one: each frame
/// carries a `CrdtSync` payload plus assertions about its shape. It pins the codec
/// boundary — that a frame deserializes to the frontier and op counts the spec says
/// it has — which is a different contract from `anti_entropy_converge.json` and was
/// equally unreplayed here.
#[test]
fn crdt_sync_frames_conformance() {
    let Some(fixture) = load("crdt_sync_frames.json") else {
        eprintln!("SKIP: lazily-spec sibling missing");
        return;
    };
    let frames = fixture["frames"].as_array().expect("frames");
    assert!(
        !frames.is_empty(),
        "a replay of zero frames is not a replay"
    );

    let mut checked = 0usize;
    for frame in frames {
        let label = frame["label"].as_str().expect("label");
        let wire = &frame["wire"]["CrdtSync"];

        // Deserialize through the real wire type rather than reading the JSON
        // fields directly — otherwise this asserts the fixture against itself.
        let sync: lazily::CrdtSync =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("{label}: {e}"));

        let assertions = &frame["assertions"];
        if let Some(want) = assertions["frontier_len"].as_u64() {
            assert_eq!(
                sync.frontier.len() as u64,
                want,
                "{label}: frontier_len after wire round-trip"
            );
            checked += 1;
        }
        if let Some(want) = assertions["op_count"].as_u64() {
            assert_eq!(
                sync.ops.len() as u64,
                want,
                "{label}: op_count after wire round-trip"
            );
            checked += 1;
        }

        // Re-serializing must preserve those counts: a codec that drops ops on the
        // way out would still satisfy the checks above.
        let round_tripped: lazily::CrdtSync =
            serde_json::from_value(serde_json::to_value(&sync).expect("serialize"))
                .expect("re-deserialize");
        assert_eq!(
            round_tripped.ops.len(),
            sync.ops.len(),
            "{label}: ops survive round-trip"
        );
        assert_eq!(
            round_tripped.frontier.len(),
            sync.frontier.len(),
            "{label}: frontier survives round-trip"
        );
    }
    assert!(checked >= 4, "too little asserted across frames: {checked}");
}
