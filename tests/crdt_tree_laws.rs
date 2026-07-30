#![cfg(feature = "lossless-tree")]

mod common;

use common::Expect;
use lazily::{CrdtTree, TextCrdt, TextVersionVector};
use serde_json::Value;

const FIXTURE: &str = "../lazily-spec/conformance/crdt-tree/algebra.json";

fn fixture() -> Option<Value> {
    let text = crate::common::spec_read_to_string(FIXTURE).ok()?;
    Some(serde_json::from_str(&text).expect("CrdtTree fixture JSON"))
}

#[test]
fn crdt_tree_merge_laws_and_delta_snapshot_round_trip() {
    let base = TextCrdt::from_str(1, "root\n");
    let mut a = base.fork(2);
    let mut b = base.fork(3);
    let mut c = base.fork(4);
    a.insert_str(5, "a");
    b.insert_str(5, "b");
    c.insert_str(5, "c");

    let mut ab = a.clone();
    ab.merge_from(&b);
    let mut ba = b.clone();
    ba.merge_from(&a);
    assert_eq!(ab.value(), ba.value(), "commutative");

    let mut left = a.clone();
    left.merge_from(&b);
    left.merge_from(&c);
    let mut bc = b.clone();
    bc.merge_from(&c);
    let mut right = a.clone();
    right.merge_from(&bc);
    assert_eq!(left.value(), right.value(), "associative");

    let before = left.value();
    let mut idempotent = left.clone();
    assert!(
        !idempotent.merge_from(&left),
        "idempotent merge is observably unchanged"
    );
    assert_eq!(idempotent.value(), before);

    let snapshot = left.delta_since(&TextVersionVector::new());
    let mut restored = TextCrdt::new(99);
    restored.apply_delta(&snapshot);
    assert_eq!(
        restored.value(),
        left.value(),
        "empty-frontier delta is a snapshot"
    );
    assert!(
        left.delta_since(&left.version_vector()).is_empty(),
        "frontier round-trip is empty"
    );
}

#[test]
fn crdt_tree_replays_canonical_fixture() {
    let Some(fixture) = fixture() else {
        eprintln!("skipping: lazily-spec CrdtTree fixture is not present as a sibling");
        return;
    };
    assert_eq!(fixture["kind"], "CrdtTree");
    // Per-scenario replay ledger (`#lzscenariocoverage`). This runner addresses
    // its three scenarios positionally, so each index goes through the recorder
    // rather than a bare `scenarios[n]`.
    let merge = common::scenario_at(FIXTURE, &fixture, 0);
    let peer = merge["seed"]["peer"].as_u64().unwrap();
    let seed = merge["seed"]["text"].as_str().unwrap();
    let base = TextCrdt::from_str(peer, seed);
    let replicas = merge["replicas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|definition| {
            let mut replica = base.fork(definition["peer"].as_u64().unwrap());
            replica.insert_str(replica.len(), definition["insert"].as_str().unwrap());
            (definition["name"].as_str().unwrap(), replica)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let folds = merge["merge_orders"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, order)| {
            let mut folded = base.fork(100 + index as u64);
            for name in order.as_array().unwrap() {
                folded.merge_from(replicas.get(name.as_str().unwrap()).unwrap());
            }
            folded
        })
        .collect::<Vec<_>>();
    // Guard the scenario's `expect` block (`#lzassertunknownkeys`). Both keys
    // were carried by the corpus and read by nothing: the runner asserted the
    // laws with hardcoded outcomes, so a fixture that said `texts_equal: false`
    // would have passed unchanged.
    let merge_expect = Expect::new(FIXTURE, "scenarios[0].expect", &merge["expect"]);
    let texts_equal = folds[1..].iter().all(|f| f.value() == folds[0].value());
    let vvs_equal = folds[1..]
        .iter()
        .all(|f| f.version_vector() == folds[0].version_vector());
    merge_expect.assert_key("texts_equal", texts_equal);
    merge_expect.assert_key("version_vectors_equal", vvs_equal);
    for folded in &folds[1..] {
        assert_eq!(folded.value(), folds[0].value());
        assert_eq!(folded.version_vector(), folds[0].version_vector());
    }

    let snapshot_case = common::scenario_at(FIXTURE, &fixture, 1);
    let snapshot_expect = Expect::new(FIXTURE, "scenarios[1].expect", &snapshot_case["expect"]);
    let snapshot_seed = snapshot_case["seed"]["text"].as_str().unwrap();
    let mut canonical = TextCrdt::from_str(
        snapshot_case["seed"]["peer"].as_u64().unwrap(),
        snapshot_seed,
    );
    let snapshot = canonical.delta_since(&TextVersionVector::new());
    let mut restored = TextCrdt::new(snapshot_case["restore_peer"].as_u64().unwrap());
    assert!(restored.apply_delta(&snapshot));
    snapshot_expect.assert_key("restored_text_equal", restored.value() == canonical.value());
    let mut restored_ops = restored.delta_since(&TextVersionVector::new());
    let mut snapshot_ops = snapshot;
    restored_ops.sort_by_key(|op| (op.id.counter(), op.id.peer()));
    snapshot_ops.sort_by_key(|op| (op.id.counter(), op.id.peer()));
    snapshot_expect.assert_key_at(
        "op_ids_equal",
        restored_ops == snapshot_ops,
        "snapshot preserves operation identity",
    );
    canonical.insert_str(canonical.len(), "A");
    restored.insert_str(restored.len(), "B");
    canonical.apply_delta(&restored.delta_since(&canonical.version_vector()));
    restored.apply_delta(&canonical.delta_since(&restored.version_vector()));
    assert_eq!(canonical.value(), restored.value());
    // `later_merge_duplicates`: characters over and above seed + the two
    // concurrent inserts. A CRDT that re-applied the snapshot's ops on merge
    // would show them here.
    let duplicates = canonical.len() as i64 - (snapshot_seed.chars().count() as i64 + 2);
    snapshot_expect.assert_key("later_merge_duplicates", duplicates);

    let steady_case = common::scenario_at(FIXTURE, &fixture, 2);
    let steady_expect = Expect::new(FIXTURE, "scenarios[2].expect", &steady_case["expect"]);
    let steady = &steady_case["seed"];
    let mut steady = TextCrdt::from_str(
        steady["peer"].as_u64().unwrap(),
        steady["text"].as_str().unwrap(),
    );
    let empty = steady.delta_since(&steady.version_vector());
    steady_expect.assert_key_with("delta", |want| {
        assert_eq!(
            empty.len(),
            want.as_array().expect("delta").len(),
            "own frontier emits an empty delta"
        )
    });
    steady_expect.assert_key("apply_changed", steady.apply_delta(&empty));
}
