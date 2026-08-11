//! Cross-language conformance for the reactive-graph disposal/teardown plane
//! (`#lzspecedgeindex`) — see `lazily-spec/conformance/reactive-graph/*.json`.
//!
//! These fixtures shipped with no binding executing them. This runner is the
//! first executor, and it is parameterised over the *execution model* so the
//! corpus can reach every context `lazily` ships rather than only the basic one.
//! A disposal contract that structurally cannot reach two of three execution
//! models is the same defect as a suite that tests nothing, one layer down:
//! `#lzspecedgeindex` is about per-node graph memory, and the thread-safe and
//! async paths are exactly where a leak is hardest to notice and hardest to
//! reproduce.
//!
//! ## What is asserted
//!
//! Every assertion kind in the corpus: `value`, `read`, `error`
//! (`read_after_dispose`), `readable`, `observed_by`, `observed_count`,
//! `cleanup_order` (effect entries only — derived slots run no cleanup
//! callback), `scope_owned_count`, `dependents_of`, `dependencies_of`, and
//! `computes_of`. An unrecognised assertion key panics rather than being
//! skipped.
//!
//! `computes_of` (`#lzsignaleager`) is the cumulative count of compute
//! invocations per node, counted inside the compute closure itself, and it is
//! the only observable that separates an eager signal from the lazy memo it is
//! built on — every value in the three signal fixtures is identical either way.
//!
//! ## Fixture shape
//!
//! Every fixture declares a top-level `shape`, `steps` or `scenarios`, and the
//! runner dispatches on it. `scenarios` exists because a claim like
//! `observationally_equal` is a *relation between two op streams*, which a
//! single `steps` array cannot express; each scenario is replayed in its own
//! context and the resulting observations compared.
//!
//! ## Positive assertion (`#lzspecconf`)
//!
//! An absence guard is not enough — a runner that skips everything must fail.
//! For each model this asserts (a) the fixture set on disk matches `FIXTURES`
//! exactly, (b) every fixture was replayed, and (c) a non-zero number of ops and
//! assertions actually executed, per fixture and in total.

mod common;

#[path = "reactive_graph/engine.rs"]
mod engine;
#[path = "reactive_graph/model.rs"]
mod model;
#[path = "reactive_graph/models.rs"]
mod models;

use std::collections::BTreeSet;
use std::fs;

use serde_json::Value;

use common::Expect;
use engine::{Report, arr, replay};
use model::GraphModel;

const SPEC_DIR: common::SpecDir = common::SpecDir("reactive-graph");

/// The canonical fixture set. Asserted against the directory listing so a
/// fixture added or renamed upstream fails loudly instead of going unrun.
const FIXTURES: [&str; 21] = [
    "churn_returns_to_baseline.json",
    "cross_scope_teardown_hazard.json",
    "disarm_disposes_nothing.json",
    "disposal_does_not_run_surviving_effects.json",
    "dispose_detaches_edges_both_directions.json",
    "dispose_signal_reverts_to_lazy.json",
    "failed_compute_is_never_cached.json",
    "read_after_dispose_is_an_error.json",
    "signal_materializes_once_per_batch.json",
    "signal_materializes_without_a_read.json",
    "recycled_id_inherits_nothing.json",
    "scope_teardown_equals_fold_of_disposals.json",
    "scoping_bounds_teardown_not_visibility.json",
    "teardown_runs_members_in_reverse_creation_order.json",
    "transitive_invalidation_reaches_depth.json",
    // `#lzmergefeed` (design §9.1 / §8): the merge algebra + feedback-drain
    // fixtures, replayed against all three execution models.
    "exact_fold_paths_stay_exact.json",
    "feedback_drain_bound_reports_exhaustion.json",
    "merge_cell_acquires_no_dependency_edge.json",
    "merge_folds_synchronously_in_batch.json",
    "merge_per_settled_cone_not_per_write.json",
    // Additive async-scheduler fixture (design §9.1): the feed reads a formula,
    // so on the async model the effect awaits before folding, exercising the
    // per-cone coalescing the four plain-cell feeds never reach.
    "merge_feed_through_a_formula_coalesces.json",
];

/// Fixture assertions an execution model does not satisfy today, as
/// `<model>/<fixture>[<scenario>]#<step>:<key>`.
///
/// Each entry is a finding against the implementation, not a relaxation of the
/// fixture: the runner asserts this list matches the observed set exactly, so a
/// new divergence fails the build and a fixed one fails it until the entry is
/// removed.
/// Fixtures one MODEL cannot replay, with the capability gap that blocks it.
///
/// Separate from a divergence: a divergence is a finding the binding could fix,
/// this is a fixture the RUNNER cannot express against that context.
///
/// `AsyncModel`'s compute returns `BoxFuture<Output = i64>` — no error channel —
/// and the model blocks on its own runtime inside `read`. A `fail_next`-armed
/// panic therefore kills the spawned task instead of surfacing, and the awaiting
/// read never resolves: the test hangs rather than reporting anything. Both
/// alternatives were worse than a named skip. Widening the compute to a `Result`
/// would reshape every async fixture replay to chase one fixture. Returning a
/// sentinel value and reporting the error from `read` would cache the sentinel,
/// so the node would not re-run, and the fixture would report a divergence that
/// is an artifact of the runner rather than a finding against lazily-rs — which
/// implements the retry correctly
/// (`AsyncSlotState::Error | AsyncSlotState::Empty => spawn_async_compute`).
///
/// Asserted in both directions: an entry that stops applying fails the test.
const MODEL_SKIPS: &[(&str, &str, &str)] = &[(
    "AsyncContext",
    "failed_compute_is_never_cached.json",
    "compute returns BoxFuture<Output = i64> with no error channel - an armed panic kills the spawned task and the blocking read never resolves",
)];

const KNOWN_DIVERGENCES: &[&str] = &[
    // `#lzfeedbackdrain` (design §9.1 / §8): the bounded effect drain lands on
    // `Context` and `ThreadSafeContext`, so both report exhaustion for the
    // divergent scheduler-closed loop. `AsyncContext` has no drain bound — a
    // real self-writing async effect would spawn reruns unboundedly (the async
    // scheduler coalesces per revision, but a self-write advances the revision
    // every time), so the async model runs a NON-writing stand-in and never
    // exhausts. The `set_cell` kick therefore reports `drain_exhausted = false`
    // where the fixture wants `true`. Bounding divergent async feedback is
    // future work beyond the merge algebra this fixture set brought in; the gap
    // is ledgered here rather than papered over.
    //
    // The other ledger entry this file ever held was a fixture defect —
    // `scope_teardown_...` asserted `dependents_of` one step before the read
    // that registers the edge it counts — reported upstream and fixed in
    // lazily-spec f9f93d5.
    "AsyncContext/feedback_drain_bound_reports_exhaustion.json#2:drain_exhausted",
];

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    SPEC_DIR.is_dir()
}

/// Replay the whole corpus against one execution model.
fn run_corpus<M: GraphModel>() {
    let model_name = M::NAME;
    if !present() {
        eprintln!(
            "SKIP reactive_graph_conformance[{model_name}]: {SPEC_DIR} not found — clone \
             lazily-spec as a sibling to run the #lzspecedgeindex disposal/teardown fixtures"
        );
        return;
    }

    // The fixture set on disk must be exactly the one this runner knows about,
    // so an upstream addition cannot arrive unexecuted.
    let on_disk: BTreeSet<String> = fs::read_dir(SPEC_DIR.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".json"))
        .collect();
    let known: BTreeSet<String> = FIXTURES.iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(
        on_disk, known,
        "reactive-graph fixture set drifted; every fixture must be replayed by this runner"
    );

    let mut replayed = 0usize;
    let mut total_ops = 0usize;
    let mut total_checks = 0usize;
    let mut observed_divergences: BTreeSet<String> = BTreeSet::new();

    let mut skipped = 0usize;
    for name in FIXTURES {
        if let Some((_, _, reason)) = MODEL_SKIPS
            .iter()
            .find(|(m, f, _)| *m == model_name && *f == name)
        {
            // Loud, named, per-model skip. Silent skipping is the anti-pattern.
            eprintln!("SKIP reactive-graph[{model_name}] {name}: {reason}");
            skipped += 1;
            continue;
        }
        let fx = load(name);
        // Dispatch on the fixture's declared `shape`, not on its filename: a
        // filename special case goes stale silently the moment a second
        // scenarios-shaped fixture is added. An unrecognised shape is a hard
        // error rather than a fallback to `steps`.
        // ONE guard for the fixture-level `expected` tail (`#lzassertunknownkeys`),
        // shared by the scenario replays and the `observationally_equal` check
        // below, so no key of that block goes unread.
        let fixture_expected =
            Expect::new(format!("{SPEC_DIR}/{name}"), "expected", &fx["expected"]);
        let models: Vec<M>;
        let reports: Vec<Report> = match fx["shape"].as_str() {
            Some("steps") => {
                models = vec![M::create()];
                vec![replay(&models[0], name, arr(&fx["steps"]), None)]
            }
            Some("scenarios") => {
                let count = fx["scenarios"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{name}: shape is `scenarios` but no scenarios"))
                    .len();
                models = (0..count).map(|_| M::create()).collect();
                // Per-scenario replay ledger (`#lzscenariocoverage`): the helper
                // records each id as it yields, so the ledger cannot claim a
                // scenario this fold never reached.
                common::scenarios(&format!("{SPEC_DIR}/{name}"), &fx)
                    .zip(&models)
                    .map(|((_i, _id, s), m)| {
                        replay(m, name, arr(&s["steps"]), Some(&fixture_expected))
                    })
                    .collect()
            }
            Some(other) => panic!("{name}: unknown fixture shape {other}"),
            None => panic!("{name}: fixture declares no `shape`"),
        };

        // `observationally_equal`: the named scenarios must agree on every
        // observable, not merely each satisfy `expected` independently.
        if let Some(pair) = fixture_expected
            .assert_key_if_present("observationally_equal", |want| {
                want.as_array().expect("observationally_equal").clone()
            })
            .as_ref()
        {
            let names: Vec<&str> = fx["scenarios"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s["name"].as_str().unwrap())
                .collect();
            let idx: Vec<usize> = pair
                .iter()
                .map(|p| {
                    names
                        .iter()
                        .position(|n| *n == p.as_str().unwrap())
                        .unwrap_or_else(|| panic!("{name}: unknown scenario {p}"))
                })
                .collect();
            for w in idx.windows(2) {
                assert_eq!(
                    reports[w[0]].observation, reports[w[1]].observation,
                    "{model_name}/{name}: scenarios are not observationally equal"
                );
            }
            total_checks += 1;
        }

        let ops: usize = reports.iter().map(|r| r.ops).sum();
        let checks: usize = reports.iter().map(|r| r.checks).sum();
        assert!(ops > 0, "{model_name}/{name}: replayed zero ops");
        assert!(checks > 0, "{model_name}/{name}: replayed zero assertions");

        for (si, r) in reports.iter().enumerate() {
            for f in &r.failures {
                let step = if f.step == usize::MAX {
                    "expected".to_owned()
                } else {
                    f.step.to_string()
                };
                let scenario = if reports.len() > 1 {
                    format!("[{si}]")
                } else {
                    String::new()
                };
                let entry = format!("{model_name}/{name}{scenario}#{step}:{}", f.key);
                eprintln!("  DIVERGENCE {entry} — {}", f.detail);
                observed_divergences.insert(entry);
            }
        }

        eprintln!("reactive-graph[{model_name}] {name}: {ops} ops, {checks} assertions");
        total_ops += ops;
        total_checks += checks;
        replayed += 1;
    }

    eprintln!(
        "reactive-graph[{model_name}]: {replayed} fixtures, {total_ops} ops, \
         {total_checks} assertions, {} documented divergences",
        observed_divergences.len()
    );

    // Divergence ledger: the observed set must equal the documented one, so a
    // new divergence fails the build and a fixed one forces its entry to be
    // deleted. Neither direction can pass unnoticed.
    let documented: BTreeSet<String> = KNOWN_DIVERGENCES
        .iter()
        .filter(|d| d.starts_with(&format!("{model_name}/")))
        .map(|d| (*d).to_owned())
        .collect();
    assert_eq!(
        observed_divergences, documented,
        "{model_name}: divergence ledger is stale — update KNOWN_DIVERGENCES \
         (left = observed, right = documented)"
    );

    // Positive assertion: the runner must have actually executed the corpus.
    // Every fixture is either replayed or named in MODEL_SKIPS for this model —
    // asserted in both directions, so a skip that stops applying fails here
    // rather than shrinking the run silently.
    let model_skips = MODEL_SKIPS
        .iter()
        .filter(|(m, _, _)| *m == model_name)
        .count();
    assert_eq!(skipped, model_skips, "{model_name}: skip ledger is stale");
    assert_eq!(
        replayed + skipped,
        FIXTURES.len(),
        "{model_name}: did not replay every fixture"
    );
    assert!(total_ops > 0, "{model_name}: executed zero ops");
    assert!(total_checks > 0, "{model_name}: executed zero assertions");
}

#[test]
fn reactive_graph_conformance_basic() {
    run_corpus::<models::BasicModel>();
}

#[cfg(feature = "thread-safe")]
#[test]
fn reactive_graph_conformance_thread_safe() {
    run_corpus::<models::ThreadSafeModel>();
}

#[cfg(feature = "async")]
#[test]
fn reactive_graph_conformance_async() {
    run_corpus::<models::AsyncModel>();
}

/// The ledger must not name a model that no longer runs, and every model that
/// runs must be reachable from a test above. Guards against a `#[cfg]` quietly
/// removing a model's coverage while its ledger entries linger.
#[test]
fn divergence_ledger_names_only_known_models() {
    let models: BTreeSet<&str> = ["Context", "ThreadSafeContext", "AsyncContext"]
        .into_iter()
        .collect();
    for entry in KNOWN_DIVERGENCES {
        let model = entry.split('/').next().unwrap();
        assert!(
            models.contains(model),
            "KNOWN_DIVERGENCES names unknown model {model}"
        );
    }
}
