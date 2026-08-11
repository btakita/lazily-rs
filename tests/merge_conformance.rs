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

use common::Expect;
use lazily::{Context, KeepLatest, Max, MergePolicy, Source, Sum};
use serde_json::Value;

const SPEC_DIR: common::SpecDir = common::SpecDir("collections");

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn spec_fixtures_present() -> bool {
    SPEC_DIR.join("mergecell_algebra.json").exists()
}

/// Replay one scenario's steps against a `MergeCell<i64, M>`, asserting value and
/// invalidation (observed via a subscribed effect's rerun count) after each op.
fn replay_scenario<M>(si: usize, scenario: &Value)
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
        // Guard the `expected` block (`#lzassertunknownkeys`): a key this runner
        // never reads fails the fixture instead of passing unnoticed.
        let exp = Expect::new(
            format!("{SPEC_DIR}/mergecell_algebra.json"),
            format!("scenarios[{si}].steps[{i}].expected"),
            &step["expected"],
        );
        let before = runs.get();
        mc.merge(&ctx, op);
        let fired = runs.get() > before;

        exp.assert_key_at("value", mc.get(&ctx), &format!("step {i} (op {op})"));
        exp.assert_key_at("invalidates", fired, &format!("step {i} (op {op})"));
    }
}

#[test]
fn mergecell_algebra_fixture() {
    if !spec_fixtures_present() {
        eprintln!("skipping: lazily-spec conformance fixtures not present as sibling");
        return;
    }
    let fixture = load_fixture("mergecell_algebra.json");

    // Per-scenario replay ledger (`#lzscenariocoverage`). This is the ONE fixture
    // in the corpus whose scenarios carry neither `id` nor `name` — they are
    // distinguished only by `policy` — so the ledger falls back to the positional
    // `#<n>` and the guard REPORTS that fallback. Adding the identifiers is a
    // lazily-spec change and deliberately not made here.
    let mut seen = 0;
    for (si, _id, scenario) in
        common::scenarios(&format!("{SPEC_DIR}/mergecell_algebra.json"), &fixture)
    {
        match scenario["policy"].as_str().expect("policy string") {
            "KeepLatest" => replay_scenario::<KeepLatest>(si, scenario.value()),
            "Sum" => replay_scenario::<Sum>(si, scenario.value()),
            "Max" => replay_scenario::<Max>(si, scenario.value()),
            other => panic!("unknown policy in fixture: {other}"),
        }
        // Flag sanity: the fixture's declared flags must match the policy consts.
        // The flag block is an assertion block too, so it is guarded the same way.
        let flags = Expect::new(
            format!("{SPEC_DIR}/mergecell_algebra.json"),
            format!("scenarios[{si}].flags"),
            &scenario["flags"],
        );
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
        flags.assert_key_at("commutative", comm, "commutative flag");
        flags.assert_key_at("idempotent", idem, "idempotent flag");
        seen += 1;
    }
    assert_eq!(seen, 3, "expected 3 policy scenarios");
}
