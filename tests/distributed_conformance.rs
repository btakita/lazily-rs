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

use common::Expect;
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

/// Total order on a wire stamp: greatest wall time, then logical, then peer as
/// the final tiebreak. This IS the `max_stamp` resolution the fixtures declare.
fn stamp_key(s: &WireStamp) -> (u64, u64, u64) {
    (s.wall_time, s.logical, s.peer)
}

/// The winning payload per (node, key) under `max_stamp`, computed from the op
/// stream alone. Comparing this to `CrdtPlaneRuntime::converged()` is what makes
/// the fixture's `resolution` key an assertion rather than a label.
fn max_stamp_winners(ops: &[CrdtOp]) -> Vec<(NodeId, Option<NodeKey>, IpcValue)> {
    let mut winners: Vec<(NodeId, Option<NodeKey>, WireStamp, IpcValue)> = Vec::new();
    for op in ops {
        match winners
            .iter_mut()
            .find(|(n, k, _, _)| *n == op.node && *k == op.key)
        {
            Some(entry) => {
                if stamp_key(&op.stamp) > stamp_key(&entry.2) {
                    entry.2 = op.stamp;
                    entry.3 = op.state.clone();
                }
            }
            None => winners.push((op.node, op.key.clone(), op.stamp, op.state.clone())),
        }
    }
    winners.into_iter().map(|(n, k, _, v)| (n, k, v)).collect()
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

    for (si, sc) in scenarios.iter().enumerate() {
        let name = sc["name"].as_str().expect("name");
        // Guard the scenario's `expect` block (`#lzassertunknownkeys`): `resolution`
        // and `order_independent` were both present and both unread.
        let exp = Expect::new(
            format!("{SPEC}/anti_entropy_converge.json"),
            format!("scenarios[{si}].expect"),
            &sc["expect"],
        );
        let ops: Vec<CrdtOp> = sc["ops"]
            .as_array()
            .expect("ops")
            .iter()
            .map(op_of)
            .collect();
        let ctx = Context::new();
        let mut rt = seeded_runtime(&ctx, &ops);

        let applied = ingest_ops(&mut rt, &ctx, &ops);
        exp.assert_key_at(
            "applied_count",
            applied as u64,
            &format!(
                "{name}: applied_count. A superseded op still counts — it entered \
                 the log. Counting only ops that changed the winner is the \
                 lazily-cpp bug."
            ),
        );
        checked_counts += 1;

        if sc
            .get("redeliver")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let again = ingest_ops(&mut rt, &ctx, &ops);
            exp.assert_key_at(
                "redeliver_applied_count",
                again as u64,
                &format!("{name}: redelivery"),
            );
            checked_redeliver += 1;
        }

        // The converged view, now assertable: `CrdtPlaneRuntime::converged()` folds
        // the op log into the winning payload per (node, key), which is the shape
        // the fixture states convergence in. Before that accessor existed this half
        // was skipped, and a runner that asserts counts while silently dropping
        // `converged` is the marker-only failure this suite exists to prevent.
        // `resolution`: the fixture names the conflict rule, so assert the rule
        // rather than trusting the converged payloads alone — a binding that
        // resolved by arrival order could still match `converged` on a stream
        // whose arrival order happens to agree with stamp order.
        exp.assert_key_with("resolution", |want| {
            match want.as_str().expect("resolution") {
                "max_stamp" => {
                    let got = rt.converged();
                    for (node, key, want_state) in max_stamp_winners(&ops) {
                        let found = got
                            .iter()
                            .find(|e| e.node == node && e.key == key)
                            .unwrap_or_else(|| panic!("{name}: no converged entry for {node:?}"));
                        assert_eq!(
                            found.state, want_state,
                            "{name}: resolution=max_stamp — the greatest (wall_time, \
                             logical, peer) stamp must win for {node:?}"
                        );
                    }
                }
                other => panic!("{name}: unknown resolution rule {other}"),
            }
        });

        // `order_independent` + the scenario's `reverse_order_equivalent` input:
        // replay the same ops backwards into a fresh runtime and require the same
        // converged view. Both keys were previously unread.
        // Gating the reversed replay on the flag asserted nothing when the flag
        // was `false` (`#lzconsumednotasserted`). The reversal is always run and
        // its outcome compared to the flag, so both directions are load-bearing.
        // The reversal is driven by the scenario's own `reverse_order_equivalent`
        // input; its OUTCOME is then compared to `order_independent`. Gating the
        // whole block on the expectation asserted nothing whenever the flag was
        // `false` (`#lzconsumednotasserted`).
        if exp.raw().get("order_independent").is_some() {
            assert!(
                sc["reverse_order_equivalent"].as_bool().unwrap_or(false),
                "{name}: expect.order_independent without reverse_order_equivalent"
            );
        }
        if sc["reverse_order_equivalent"].as_bool().unwrap_or(false) {
            let reversed: Vec<CrdtOp> = ops.iter().rev().cloned().collect();
            let rev_ctx = Context::new();
            let mut rev_rt = seeded_runtime(&rev_ctx, &reversed);
            ingest_ops(&mut rev_rt, &rev_ctx, &reversed);
            let mut a = rt.converged();
            let mut b = rev_rt.converged();
            a.sort_by_key(|e| (e.node.0, format!("{:?}", e.key)));
            b.sort_by_key(|e| (e.node.0, format!("{:?}", e.key)));
            let reversal_agrees = a.len() == b.len()
                && a.iter()
                    .zip(&b)
                    .all(|(x, y)| (x.node, &x.key, &x.state) == (y.node, &y.key, &y.state));
            exp.assert_key_at(
                "order_independent",
                reversal_agrees,
                &format!("{name}: reversed replay convergence"),
            );
        }

        exp.assert_key_if_present("converged", |want_entries| {
            let got = rt.converged();
            for entry in want_entries.as_array().expect("converged") {
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
        });
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
    for (fi, frame) in frames.iter().enumerate() {
        let label = frame["label"].as_str().expect("label");
        let wire = &frame["wire"]["CrdtSync"];

        // Deserialize through the real wire type rather than reading the JSON
        // fields directly — otherwise this asserts the fixture against itself.
        let sync: lazily::CrdtSync =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("{label}: {e}"));

        // Guard the frame's `assertions` block (`#lzassertunknownkeys`):
        // `has_keyed_op`, `has_keyless_op` and `frontier_omitted` were present
        // in the corpus and read by nothing.
        let assertions = Expect::new(
            format!("{SPEC}/crdt_sync_frames.json"),
            format!("frames[{fi}].assertions"),
            &frame["assertions"],
        );
        // Each key is optional per frame, so the comparison is bound to the key's
        // presence rather than to a bare read (`#lzconsumednotasserted`).
        if assertions
            .assert_key_if_present("frontier_len", |want| {
                assert_eq!(
                    sync.frontier.len() as u64,
                    want.as_u64().expect("frontier_len"),
                    "{label}: frontier_len after wire round-trip"
                )
            })
            .is_some()
        {
            checked += 1;
        }
        if assertions
            .assert_key_if_present("op_count", |want| {
                assert_eq!(
                    sync.ops.len() as u64,
                    want.as_u64().expect("op_count"),
                    "{label}: op_count after wire round-trip"
                )
            })
            .is_some()
        {
            checked += 1;
        }

        // `frontier_omitted`: the wire object carries no `frontier` at all, and
        // the decoded value must default to empty rather than to a placeholder.
        if assertions
            .assert_key_if_present("frontier_omitted", |want| {
                assert_eq!(
                    wire.get("frontier").is_none(),
                    want.as_bool().expect("frontier_omitted"),
                    "{label}: frontier_omitted describes the wire shape"
                );
                assert!(
                    sync.frontier.is_empty(),
                    "{label}: an omitted frontier must decode as empty"
                );
            })
            .is_some()
        {
            checked += 1;
        }
        if assertions
            .assert_key_if_present("has_keyed_op", |want| {
                assert_eq!(
                    sync.ops.iter().any(|o| o.key.is_some()),
                    want.as_bool().expect("has_keyed_op"),
                    "{label}: has_keyed_op"
                )
            })
            .is_some()
        {
            checked += 1;
        }
        if assertions
            .assert_key_if_present("has_keyless_op", |want| {
                assert_eq!(
                    sync.ops.iter().any(|o| o.key.is_none()),
                    want.as_bool().expect("has_keyless_op"),
                    "{label}: has_keyless_op"
                )
            })
            .is_some()
        {
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
