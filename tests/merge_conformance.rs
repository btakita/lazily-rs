//! Cross-language MergeCell merge-algebra conformance (`#relaycell`, Phase 1).
//!
//! Replays `lazily-spec/conformance/collections/mergecell_algebra.json`: for each
//! policy scenario, creates a `MergeCell` under that policy, applies each `merge`
//! op, and asserts the converged value plus whether the op fired the cascade
//! (`invalidates` — false when `⊕(old, op) == old`, so the `==` store-guard
//! suppresses the effect rerun). See `reactive-graph.md` § MergeCell and the merge
//! algebra.

mod common;

use std::cell::Cell as StdCell;
use std::rc::Rc;

use lazily::{Context, KeepLatest, Max, MergePolicy, Source, Sum};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn spec_fixtures_present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/mergecell_algebra.json")).exists()
}

/// Replay one scenario's steps against a `MergeCell<i64, M>`, asserting value and
/// invalidation (observed via a subscribed effect's rerun count) after each op.
fn replay_scenario<M>(scenario: &Value)
where
    M: MergePolicy<i64> + 'static,
{
    let ctx = Context::new();
    let initial = scenario["initial"].as_i64().expect("initial i64");
    let mc: Source<i64, M> = ctx.merge_cell(initial);

    // An active subscriber makes every state change flush a rerun, so the rerun
    // delta observes `invalidates`. subscribe() runs once immediately.
    let runs = Rc::new(StdCell::new(0u32));
    let runs2 = runs.clone();
    let _eff = ctx.effect(move |c| {
        let _ = c.get(&mc.cell());
        runs2.set(runs2.get() + 1);
    });
    assert_eq!(runs.get(), 1, "subscribe runs once on creation");

    for (i, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
        let op = step["merge"].as_i64().expect("merge i64");
        let want_value = step["expected"]["value"].as_i64().expect("value i64");
        let want_inval = step["expected"]["invalidates"]
            .as_bool()
            .expect("invalidates bool");

        let before = runs.get();
        mc.merge(&ctx, op);
        let fired = runs.get() > before;

        assert_eq!(
            mc.get(&ctx),
            want_value,
            "value mismatch at step {i} (op {op})"
        );
        assert_eq!(
            fired, want_inval,
            "invalidation mismatch at step {i} (op {op})"
        );
    }
}

#[test]
fn mergecell_algebra_fixture() {
    if !spec_fixtures_present() {
        eprintln!("skipping: lazily-spec conformance fixtures not present as sibling");
        return;
    }
    let fixture = load_fixture("mergecell_algebra.json");
    let scenarios = fixture["scenarios"].as_array().expect("scenarios array");

    let mut seen = 0;
    for scenario in scenarios {
        match scenario["policy"].as_str().expect("policy string") {
            "KeepLatest" => replay_scenario::<KeepLatest>(scenario),
            "Sum" => replay_scenario::<Sum>(scenario),
            "Max" => replay_scenario::<Max>(scenario),
            other => panic!("unknown policy in fixture: {other}"),
        }
        // Flag sanity: the fixture's declared flags must match the policy consts.
        let flags = &scenario["flags"];
        let (comm, idem) = match scenario["policy"].as_str().unwrap() {
            "KeepLatest" => (
                <KeepLatest as MergePolicy<i64>>::COMMUTATIVE,
                <KeepLatest as MergePolicy<i64>>::IDEMPOTENT,
            ),
            "Sum" => (
                <Sum as MergePolicy<i64>>::COMMUTATIVE,
                <Sum as MergePolicy<i64>>::IDEMPOTENT,
            ),
            "Max" => (
                <Max as MergePolicy<i64>>::COMMUTATIVE,
                <Max as MergePolicy<i64>>::IDEMPOTENT,
            ),
            _ => unreachable!(),
        };
        assert_eq!(
            flags["commutative"].as_bool().unwrap(),
            comm,
            "commutative flag"
        );
        assert_eq!(
            flags["idempotent"].as_bool().unwrap(),
            idem,
            "idempotent flag"
        );
        seen += 1;
    }
    assert_eq!(seen, 3, "expected 3 policy scenarios");
}
