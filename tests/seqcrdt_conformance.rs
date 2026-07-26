//! Cross-language conformance for the move-aware sequence CRDT
//! (`lazily-spec/cell-model.md` § Move-aware sequence order), replaying the
//! canonical compute fixture `lazily-spec/conformance/collections/seqcrdt_convergence.json`.
//!
//! Each element is three independent LWW registers — value, position
//! (fractional-index byte key + peer), deleted — stamped by an HLC. A move is a
//! SINGLE LWW reassignment of position (not delete+reinsert), so concurrent
//! moves of the same element converge to the later stamp without duplication; a
//! concurrent move + value-edit both apply (independent registers). Removal is
//! an LWW tombstone. Order is the lexicographic total order on `(frac, peer)`.
//!
//! Required of every binding — see the Binding Conformance Matrix in
//! `lazily-spec/protocol.md`. Feature-gated because `SeqCrdt` lives behind the
//! `distributed` feature (the CRDT plane).

#![cfg(feature = "distributed")]

mod common;

use std::collections::HashMap;

use lazily::{PeerId, SeqCrdt};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

type V = Value;

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

/// Apply a fixture op to `replica`. `now` comes from the step (or its
/// surrounding `merge` block); the op uses `step.now` if present.
fn apply_op(replica: &mut SeqCrdt<String, V>, op: &Value, now: u64) {
    let kind = op
        .get("op")
        .and_then(|v| v.as_str())
        .expect("seqcrdt op.op");
    match kind {
        "insert_back" => {
            let id = op.get("id").and_then(|v| v.as_str()).unwrap().to_string();
            let val = value_of(op.get("value").unwrap());
            replica.insert_back(id, val, now);
        }
        "insert_front" => {
            let id = op.get("id").and_then(|v| v.as_str()).unwrap().to_string();
            let val = value_of(op.get("value").unwrap());
            replica.insert_front(id, val, now);
        }
        "set_value" => {
            let id = op.get("id").and_then(|v| v.as_str()).unwrap().to_string();
            let val = value_of(op.get("value").unwrap());
            replica.set_value(&id, val, now);
        }
        "move_after" => {
            let id = op.get("id").and_then(|v| v.as_str()).unwrap().to_string();
            let anchor = op
                .get("anchor")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string();
            replica.move_after(&id, &anchor, now);
        }
        "move_before" => {
            let id = op.get("id").and_then(|v| v.as_str()).unwrap().to_string();
            let anchor = op
                .get("anchor")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string();
            replica.move_before(&id, &anchor, now);
        }
        "remove" => {
            let id = op.get("id").and_then(|v| v.as_str()).unwrap().to_string();
            replica.remove(&id, now);
        }
        other => panic!("unknown seqcrdt op: {other}"),
    }
}

fn value_of(v: &Value) -> Value {
    v.clone()
}

fn assert_order(replica: &SeqCrdt<String, V>, expected: &[Value], msg: &str) {
    let got: Vec<String> = replica.order();
    let want: Vec<String> = expected
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            other => panic!("expected order entry is a string/number, got {other}"),
        })
        .collect();
    assert_eq!(
        got, want,
        "{msg}: order mismatch — got {got:?} expected {want:?}"
    );
}

fn run_seqcrdt_fixture(name: &str) {
    let fixture = load_fixture(name);
    let scenarios = fixture
        .get("scenarios")
        .and_then(|v| v.as_array())
        .expect("seqcrdt scenarios");

    for (i, scenario) in scenarios.iter().enumerate() {
        let mut replicas: HashMap<String, SeqCrdt<String, V>> = HashMap::new();

        // Seed: either a `replica` (empty single-peer) or a `seed` with inserts.
        if let Some(seed) = scenario.get("seed").and_then(|v| v.as_object()) {
            let peer = seed.get("peer").and_then(|v| v.as_u64()).unwrap();
            let mut a: SeqCrdt<String, V> = SeqCrdt::new(PeerId(peer));
            for ins in seed.get("inserts").and_then(|v| v.as_array()).unwrap() {
                let id = ins.get("id").and_then(|v| v.as_str()).unwrap().to_string();
                let val = value_of(ins.get("value").unwrap());
                let now = ins.get("now").and_then(|v| v.as_u64()).unwrap();
                a.insert_back(id, val, now);
            }
            replicas.insert("a".to_string(), a);
        } else if let Some(rep) = scenario.get("replica") {
            let peer = rep.get("peer").and_then(|v| v.as_u64()).unwrap();
            replicas.insert("a".to_string(), SeqCrdt::new(PeerId(peer)));
        } else {
            panic!("scenario {i}: missing seed or replica");
        }

        for step in scenario
            .get("steps")
            .and_then(|v| v.as_array())
            .expect("seqcrdt steps")
        {
            if let Some(fork_name) = step.get("fork").and_then(|v| v.as_str()) {
                let peer = step.get("peer").and_then(|v| v.as_u64()).unwrap();
                let src = replicas
                    .get("a")
                    .unwrap_or_else(|| panic!("scenario {i}: fork from missing `a`"));
                // Fork = deep copy of the source's entries (original stamps
                // preserved) under a new owning peer id; the forked replica
                // continues from the source's causal state.
                let forked = src.fork(PeerId(peer));
                replicas.insert(fork_name.to_string(), forked);
            } else if let Some(new_name) = step.get("clone").and_then(|v| v.as_str()) {
                let from = step
                    .get("from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: clone missing `from`"));
                // Clone = byte-identical deep copy (same peer, same stamps).
                let cloned = replicas
                    .get(from)
                    .unwrap_or_else(|| panic!("scenario {i}: clone from missing `{from}`"))
                    .clone();
                replicas.insert(new_name.to_string(), cloned);
            } else if let Some(merge) = step.get("merge").and_then(|v| v.as_object()) {
                let into = merge
                    .get("into")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: merge missing `into`"));
                let from = merge
                    .get("from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: merge missing `from`"));
                let now = step.get("now").and_then(|v| v.as_u64()).unwrap();
                let from_state = replicas
                    .get(from)
                    .unwrap_or_else(|| panic!("scenario {i}: merge from missing `{from}`"))
                    .clone();
                replicas
                    .get_mut(into)
                    .unwrap_or_else(|| panic!("scenario {i}: merge into missing `{into}`"))
                    .merge(&from_state, now);
            } else if let Some(on) = step.get("on").and_then(|v| v.as_str()) {
                let op = step; // op fields are inlined at the step level
                let now = step.get("now").and_then(|v| v.as_u64()).unwrap();
                apply_op(
                    replicas
                        .get_mut(on)
                        .unwrap_or_else(|| panic!("scenario {i}: `on` target `{on}` missing")),
                    op,
                    now,
                );
            } else if step.get("op").is_some() {
                let now = step.get("now").and_then(|v| v.as_u64()).unwrap();
                apply_op(
                    replicas
                        .get_mut("a")
                        .expect("scenario {i}: default target `a` missing"),
                    step,
                    now,
                );
            } else {
                panic!("scenario {i}: unrecognized step {step}");
            }
        }

        // Assertions.
        let expect = scenario.get("expect").unwrap();
        if let Some(order) = expect.get("order").and_then(|v| v.as_array()) {
            assert_order(&replicas["a"], order, &format!("scenario {i}"));
        }
        if let Some(gets) = expect.get("get").and_then(|v| v.as_object()) {
            for (id, val) in gets {
                let got = replicas["a"]
                    .get(&id.to_string())
                    .unwrap_or_else(|| panic!("scenario {i}: get({id}) missing"));
                assert_eq!(got, *val, "scenario {i}: get({id}) mismatch");
            }
        }
        if let Some(len) = expect.get("len").and_then(|v| v.as_u64()) {
            // `len` applies to the converged replica(s): the first replica in
            // the first `orders_equal` pair when present (single-replica
            // scenarios otherwise fall back to `a`).
            let target = expect
                .get("orders_equal")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .unwrap_or("a");
            assert_eq!(
                replicas[target].values().len() as u64,
                len,
                "scenario {i}: len mismatch on `{target}`"
            );
        }
        if let Some(pairs) = expect.get("orders_equal").and_then(|v| v.as_array()) {
            for pair in pairs {
                let a = pair.get(0).and_then(|v| v.as_str()).unwrap();
                let b = pair.get(1).and_then(|v| v.as_str()).unwrap();
                assert_eq!(
                    replicas[a].order(),
                    replicas[b].order(),
                    "scenario {i}: `{a}`/`{b}` orders should converge"
                );
            }
        }
        if let Some(per_replica) = expect.get("order_on").and_then(|v| v.as_object()) {
            for (name, order) in per_replica {
                assert_order(
                    &replicas[name],
                    order.as_array().unwrap(),
                    &format!("scenario {i} on `{name}`"),
                );
            }
        }
        if let Some(per_replica) = expect.get("get_on").and_then(|v| v.as_object()) {
            for (name, gets) in per_replica {
                for (id, val) in gets.as_object().unwrap() {
                    let got = replicas[name]
                        .get(&id.to_string())
                        .unwrap_or_else(|| panic!("scenario {i}: get_on({name},{id}) missing"));
                    assert_eq!(got, *val, "scenario {i}: get_on({name},{id}) mismatch");
                }
            }
        }
        if let Some(contains_all) = expect.get("contains_all").and_then(|v| v.as_array()) {
            // `contains_all` applies to the converged replica: the first in the
            // first `orders_equal` pair when present, else `a`.
            let target = expect
                .get("orders_equal")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .unwrap_or("a");
            for id in contains_all {
                let s = id.as_str().unwrap();
                assert!(
                    replicas[target].contains(&s.to_string()),
                    "scenario {i}: `{target}` should contain `{s}`"
                );
            }
        }
        if let Some(per_replica) = expect.get("not_contains_on").and_then(|v| v.as_object()) {
            for (name, ids) in per_replica {
                for id in ids.as_array().unwrap() {
                    let s = id.as_str().unwrap();
                    assert!(
                        !replicas[name].contains(&s.to_string()),
                        "scenario {i}: `{name}` should not contain `{s}`"
                    );
                }
            }
        }
    }
}

#[test]
fn conformance_seqcrdt_convergence() {
    run_seqcrdt_fixture("seqcrdt_convergence.json");
}
