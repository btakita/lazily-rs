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

use lazily::{
    Context, CrdtOp, CrdtPlaneRuntime, HlcStamp, IpcValue, LwwRegister, NodeId, NodeKey, PeerId,
    ReplicatedCell, WireStamp,
};
use serde_json::Value;
use std::fs;

const SPEC: &str = "../lazily-spec/conformance/distributed";

fn load(name: &str) -> Option<Value> {
    let text = fs::read_to_string(format!("{SPEC}/{name}")).ok()?;
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

        // NOT asserted here, deliberately, and the reason is recorded rather than
        // left as a silent gap: the fixture's `converged` view is the winning op's
        // raw `Inline` bytes per (node, key). lazily-rs's plane is typed-cell
        // oriented — it merges SERIALIZED CRDT state into registered
        // `ReplicatedCell`s and reads back through `value::<C>()` — and exposes no
        // accessor for the raw winning payload. A byte-valued `LwwRegister<Vec<u8>>`
        // does not bridge it: `merge_state` deserializes, so the cells stay empty.
        //
        // Asserting the counts while quietly skipping `converged` and then calling
        // the fixture covered is the marker-only failure this suite exists to
        // prevent, so `distributed/crdt_sync_frames.json` stays in the coverage
        // allowlist and the converged half is filed as follow-up work.
    }

    // Positive proof, not an absence guard: a runner that silently skipped every
    // scenario body would otherwise pass.
    assert!(
        checked_counts >= 3 && checked_redeliver >= 1,
        "too little asserted: counts={checked_counts} redeliver={checked_redeliver}"
    );
}
