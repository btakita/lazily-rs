//! Cross-language conformance for reactive family-granularity sync
//! (`#lzfamilysync`), driven by the canonical fixture in
//! `lazily-spec/conformance/familysync/`. Exercises the laws proved in
//! `lazily-formal`'s `FamilySync` module against the Rust `CrdtPlaneRuntime`
//! family vehicle:
//!
//! - a keyed op for an absent family entry **materializes** it on ingest
//!   (`applyOp_present` / `applyOp_absent_adopts`) — membership propagates and the
//!   value is adopted;
//! - a later last-writer-wins update converges (`applyOp_eq_merge` semilattice);
//! - re-ingest is idempotent (`applyOp_idem`);
//! - a derived aggregate (count of `true` entries) converges across replicas
//!   (`aggregate_converges`).
//!
//! Gated on `distributed` + `webrtc` (the `CrdtPlaneRuntime` feature combo);
//! replayed by `make test-crdt-plane`.
#![cfg(all(feature = "distributed", feature = "webrtc"))]

mod common;

use common::Expect;
use lazily::{Context, CrdtPlaneRuntime, PeerId};
use serde_json::Value;

const FIXTURE: &str = "../lazily-spec/conformance/familysync/materialize_on_ingest.json";

fn suffix_of(node_key: &impl ToString) -> String {
    // `NodeKey` displays as `namespace/suffix`; the conformance fixture keys by suffix.
    node_key
        .to_string()
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

#[test]
fn family_sync_materialize_on_ingest_conformance() {
    if !std::path::Path::new(FIXTURE).exists() {
        eprintln!("skipping: spec fixture {FIXTURE} not present");
        return;
    }
    let raw = crate::common::spec_read_to_string(FIXTURE).expect("read fixture");
    let fixture: Value = serde_json::from_str(&raw).expect("parse fixture");
    let namespace = fixture["namespace"].as_str().expect("namespace");
    assert_eq!(
        fixture["value_type"].as_str(),
        Some("bool"),
        "this harness replays the bool value_type"
    );

    for (si, scenario) in fixture["scenarios"]
        .as_array()
        .expect("scenarios")
        .iter()
        .enumerate()
    {
        let name = scenario["name"].as_str().unwrap_or("<unnamed>");
        // Guard the scenario's `expect` block (`#lzassertunknownkeys`).
        let expect = Expect::new(
            FIXTURE,
            format!("scenarios[{si}].expect"),
            &scenario["expect"],
        );
        let origin_peer = scenario["origin_peer"].as_u64().expect("origin_peer");
        let target_peer = scenario["target_peer"].as_u64().expect("target_peer");

        let ctx_o = Context::new();
        let mut origin = CrdtPlaneRuntime::new(PeerId(origin_peer));
        origin.register_family_lww::<bool>(&ctx_o, namespace);

        let ctx_t = Context::new();
        let mut target = CrdtPlaneRuntime::new(PeerId(target_peer));
        target.register_family_lww::<bool>(&ctx_t, namespace);
        let epoch = target.membership_epoch().expect("membership epoch");
        let epoch_before = ctx_t.get(&epoch);

        // Apply the origin's family writes in order.
        for set in scenario["origin_sets"].as_array().expect("origin_sets") {
            let key = set["key"].as_str().expect("set.key");
            let value = set["value"].as_bool().expect("set.value");
            let now = set["now"].as_u64().expect("set.now");
            origin.family_set_lww::<bool>(&ctx_o, namespace, key, value, now);
        }

        // One anti-entropy frame carries the whole op log; ingest materializes the
        // absent entries on the target.
        let frame = origin.sync_frame();
        let applied = target.ingest(&ctx_t, &frame, 1_000);
        assert!(applied > 0, "[{name}] ingest applied at least one op");

        if scenario["reingest"].as_bool().unwrap_or(false) {
            let reapplied = target.ingest(&ctx_t, &frame, 1_001);
            let expected = expect["reingest_applied"]
                .as_u64()
                .expect("reingest_applied");
            assert_eq!(
                reapplied as u64, expected,
                "[{name}] re-ingest is idempotent"
            );
        }

        // Membership propagation: the target now holds exactly the expected keys.
        let mut got_keys: Vec<String> = target
            .family_keys(namespace)
            .iter()
            .map(suffix_of)
            .collect();
        got_keys.sort();
        let mut want_keys: Vec<String> = expect["target_keys"]
            .as_array()
            .expect("target_keys")
            .iter()
            .map(|k| k.as_str().unwrap().to_string())
            .collect();
        want_keys.sort();
        assert_eq!(got_keys, want_keys, "[{name}] materialized key set");

        assert_eq!(
            target.family_keys(namespace).len() as u64,
            expect["target_present_count"]
                .as_u64()
                .expect("present_count"),
            "[{name}] present count"
        );

        // Value adoption / LWW convergence.
        for (key, want) in expect["target_values"].as_object().expect("target_values") {
            assert_eq!(
                target.family_value_lww::<bool>(namespace, key),
                Some(want.as_bool().unwrap()),
                "[{name}] value for {key}"
            );
        }

        // Derived aggregate transparency: count of `true` entries converges.
        let count_true = target
            .family_keys(namespace)
            .iter()
            .filter(|k| target.family_value_lww::<bool>(namespace, &suffix_of(*k)) == Some(true))
            .count();
        assert_eq!(
            count_true as u64,
            expect["target_count_true"].as_u64().expect("count_true"),
            "[{name}] derived count of true entries"
        );

        if expect["target_epoch_bumped"].as_bool().unwrap_or(false) {
            assert_ne!(
                epoch_before,
                ctx_t.get(&epoch),
                "[{name}] membership epoch bumped on materialize"
            );
        }
    }
}
