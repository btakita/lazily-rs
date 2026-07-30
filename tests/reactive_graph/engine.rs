//! The fixture interpreter, generic over the execution model.
//!
//! Replays a fixture's op stream and checks every assertion kind the corpus
//! uses. Assertion failures are *recorded* rather than panicked, so one run
//! reports the whole corpus instead of stopping at the first divergence; the
//! caller reconciles the recorded set against a documented ledger.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;

use crate::common::Expect;
use serde_json::Value;

use super::model::{
    COMPUTE_FAILED, Computes, GraphModel, Merges, Ref, ScopeModel, arm_failure, computes_seen,
    count_merge, dependencies_of, dependents_of, dispose, log_snapshot, merges_seen,
};

/// A fixture assertion the implementation does not currently satisfy.
#[derive(Debug)]
pub struct Divergence {
    pub step: usize,
    pub key: String,
    pub detail: String,
}

/// Everything a scenario leaves behind that `observationally_equal` compares.
#[derive(Default, PartialEq, Eq, Debug)]
pub struct Observation {
    cleanup_order: Vec<String>,
    readable: BTreeMap<String, bool>,
    reads: BTreeMap<String, i64>,
    after_publish_observed: Vec<String>,
    after_publish_reads: BTreeMap<String, i64>,
    degrees: BTreeMap<String, usize>,
}

pub struct Report {
    pub failures: Vec<Divergence>,
    pub ops: usize,
    pub checks: usize,
    pub observation: Observation,
}

pub fn arr(v: &Value) -> &[Value] {
    v.as_array().map(|a| a.as_slice()).unwrap_or(&[])
}

fn strs(v: &Value) -> Vec<String> {
    arr(v)
        .iter()
        .filter_map(|s| s.as_str().map(str::to_owned))
        .collect()
}

/// Replay one op stream. `tail` is the `scenarios` shape's `expected` block,
/// evaluated against the final world state when present.
/// Run `f`, converting a `fail_next`-armed compute panic into `Err(())` and
/// letting every other panic through unchanged.
///
/// The two are different contracts and must not be conflated: an armed failure
/// is the corpus asking "does this node re-run on the next read", while any
/// other panic is a genuine defect the runner must not swallow.
fn catch_armed_failure<T>(f: impl FnOnce() -> T + std::panic::UnwindSafe) -> Result<T, ()> {
    match std::panic::catch_unwind(f) {
        Ok(v) => Ok(v),
        Err(payload) => {
            let armed = payload
                .downcast_ref::<String>()
                .is_some_and(|m| m.contains(COMPUTE_FAILED));
            if !armed {
                std::panic::resume_unwind(payload);
            }
            Err(())
        }
    }
}

pub fn replay<'a, M: GraphModel>(
    model: &'a M,
    fixture: &str,
    steps: &[Value],
    tail: Option<&Expect>,
) -> Report {
    let mut nodes: HashMap<String, Ref<M::Graph>> = HashMap::new();
    // Handles are kept forever so `dispose_stale_handle` can dispose through an
    // id that has since been recycled.
    let mut stale: HashMap<String, Ref<M::Graph>> = HashMap::new();
    let mut scopes: HashMap<String, M::Scope<'a>> = HashMap::new();
    // Signals live outside `nodes` because a signal is a slot *plus* a puller
    // effect, and `dispose_signal` needs the pair. See `GraphModel::Signal`.
    let mut signals: HashMap<String, M::Signal> = HashMap::new();
    // Cumulative per-node compute counters, never reset: `computes_of` is a
    // running total from scenario start, so a fixture can assert that a step
    // did NOT compute by repeating the previous step's number.
    let mut computes: HashMap<String, Computes> = HashMap::new();
    // Cumulative per-cell merge-fold counters (`#lzmergefeed`), never reset:
    // `merges_of` is a running total from scenario start, mirroring `computes`.
    let mut merges: HashMap<String, Merges> = HashMap::new();
    let mut poisoned: BTreeSet<String> = BTreeSet::new();
    let mut failures: Vec<Divergence> = Vec::new();
    let mut ops = 0usize;
    let mut checks = 0usize;
    let mut step_idx;

    macro_rules! reads_of {
        ($op:expr) => {
            strs(&$op["reads"])
                .into_iter()
                .map(|r| {
                    *nodes
                        .get(&r)
                        .unwrap_or_else(|| panic!("{fixture}: op reads unknown node {r}"))
                })
                .collect::<Vec<Ref<M::Graph>>>()
        };
    }

    // Extract the `Source` handle for a merge target / write target by id.
    macro_rules! cell_of {
        ($id:expr) => {{
            let id: &str = $id;
            match nodes
                .get(id)
                .unwrap_or_else(|| panic!("{fixture}: unknown cell {id}"))
            {
                Ref::Cell(h) => *h,
                _ => panic!("{fixture}: {id} is not a cell"),
            }
        }};
    }

    // The corpus only ever merges under `Sum`; a fold under any other policy
    // would need a distinct model method, so reject it loudly rather than
    // silently folding under the wrong algebra.
    macro_rules! assert_sum {
        ($op:expr) => {{
            if let Some(policy) = $op["policy"].as_str() {
                assert_eq!(
                    policy, "Sum",
                    "{fixture}: unsupported merge policy {policy}"
                );
            }
        }};
    }

    // Top-level read: an `Err` here is the corpus's `read_after_dispose`.
    macro_rules! read_id {
        ($id:expr) => {{
            let id: &str = $id;
            if poisoned.contains(id) {
                Err(())
            } else {
                model.poison().store(false, Ordering::SeqCst);
                // A `fail_next`-armed compute body panics. Catch it here and
                // report a failed read WITHOUT latching `poisoned`: disposal is
                // permanent by contract, a failed compute is recoverable by
                // contract (the next read re-runs the body), and latching would
                // make the engine report the very defect
                // `failed_compute_is_never_cached.json` exists to catch.
                let caught =
                    catch_armed_failure(std::panic::AssertUnwindSafe(|| match signals.get(id) {
                        Some(sig) => model.read_signal(sig),
                        None => {
                            let node = *nodes
                                .get(id)
                                .unwrap_or_else(|| panic!("{fixture}: read of unknown node {id}"));
                            model.read(node)
                        }
                    }));
                match caught {
                    // A `fail_next` compute failure: a failed read that does NOT
                    // latch `poisoned`.
                    Err(_) => Err(()),
                    Ok(Err(())) => {
                        poisoned.insert(id.to_owned());
                        Err(())
                    }
                    Ok(Ok(v)) => {
                        if model.poison().load(Ordering::SeqCst) {
                            // A live reader that still names a disposed
                            // dependency errors on its next recompute, and stays
                            // broken until it is itself rebuilt.
                            poisoned.insert(id.to_owned());
                            Err(())
                        } else {
                            Ok(v)
                        }
                    }
                }
            }
        }};
    }

    macro_rules! degree {
        ($id:expr, $method:ident) => {{
            let id: &str = $id;
            let node = *nodes
                .get(id)
                .unwrap_or_else(|| panic!("{fixture}: degree of unknown node {id}"));
            $method(model.graph(), node)
        }};
    }

    macro_rules! check {
        ($key:expr, $got:expr, $want:expr) => {{
            checks += 1;
            let (got, want) = ($got, $want);
            if got != want {
                failures.push(Divergence {
                    step: step_idx,
                    key: $key.to_string(),
                    detail: format!("got {got:?}, want {want:?}"),
                });
            }
        }};
    }

    for (i, step) in steps.iter().enumerate() {
        step_idx = i;
        let op = &step["op"];
        let kind = op["type"]
            .as_str()
            .unwrap_or_else(|| panic!("{fixture}: step has no op type"));
        let runs_before = log_snapshot(model.run_log()).len();
        let mut op_error = false;
        let mut op_value: Option<i64> = None;
        ops += 1;
        // Measure this op's drain in isolation: `drain_exhausted` is a
        // cumulative observable on the model, so clear it before every op.
        model.clear_drain();

        match kind {
            "cell" => {
                let id = op["id"].as_str().unwrap().to_owned();
                let value = op["value"].as_i64().unwrap();
                let h = match op["scope"].as_str() {
                    Some(s) => Ref::Cell(scopes[s].source(value)),
                    None => Ref::Cell(model.source(value)),
                };
                nodes.insert(id.clone(), h);
                stale.insert(id.clone(), h);
                poisoned.remove(&id);
            }
            // `#lzmergefeed`: a merge cell is an ordinary cell node whose write
            // folds under a policy (only `Sum` in the corpus). It registers a
            // merge-fold counter so `merges_of` is observable; the node itself
            // is a `Ref::Cell`, so degree/read/dispose all work unchanged.
            "merge_cell" => {
                let id = op["id"].as_str().unwrap().to_owned();
                let value = op["value"].as_i64().unwrap();
                assert_sum!(op);
                let h = Ref::Cell(model.merge_cell(value));
                merges.entry(id.clone()).or_default();
                nodes.insert(id.clone(), h);
                stale.insert(id.clone(), h);
                poisoned.remove(&id);
            }
            // `#lzmergefeed`: an explicit `merge()` call — exact, one fold per
            // call. Not emitted by the five landed fixtures (which merge only
            // inside a batch or via a feed effect), but accepted so a future
            // single-fold fixture is not an unknown op.
            "merge" => {
                let id = op["id"].as_str().unwrap();
                assert_sum!(op);
                let value = op["value"].as_i64().unwrap();
                count_merge(merges.entry(id.to_owned()).or_default());
                model.merge(cell_of!(id), value);
            }
            "computed" => {
                let id = op["id"].as_str().unwrap().to_owned();
                let reads: Vec<Ref<M::Graph>> = reads_of!(op);
                let offset = op["offset"].as_i64().unwrap_or(0);
                let counter = computes.entry(id.clone()).or_default().clone();
                let h = match op["scope"].as_str() {
                    Some(s) => Ref::Slot(scopes[s].computed(&reads, offset, &counter)),
                    None => Ref::Slot(model.computed(&reads, offset, &counter)),
                };
                nodes.insert(id.clone(), h);
                stale.insert(id.clone(), h);
                poisoned.remove(&id);
            }
            // `#lzcellkernel` dual-accept (design §8 / Step 3): the eager
            // construction is now `formula().eager()`, so the corpus op is
            // renamed `signal` -> `drive`. Accept BOTH names for the same
            // behaviour — the model's `signal` is an eager formula under the
            // hood — so runners accept before any fixture emits the new name.
            "signal" | "drive" => {
                let id = op["id"].as_str().unwrap().to_owned();
                let reads: Vec<Ref<M::Graph>> = reads_of!(op);
                let offset = op["offset"].as_i64().unwrap_or(0);
                let counter = computes.entry(id.clone()).or_default().clone();
                signals.insert(id.clone(), model.signal(&reads, offset, &counter));
                poisoned.remove(&id);
            }
            // `#lzcellkernel` dual-accept: `dispose_signal` -> `undrive`.
            "dispose_signal" | "undrive" => {
                let id = op["id"].as_str().unwrap();
                let sig = signals
                    .get(id)
                    .unwrap_or_else(|| panic!("{fixture}: dispose_signal of unknown signal {id}"));
                // Only the puller goes. The backing slot stays in `signals` so
                // it remains readable — clause 4 is precisely that the value
                // survives and reverts to lazy.
                model.dispose_signal(sig);
            }
            "batch" => {
                let writes: Vec<_> = arr(&op["writes"])
                    .iter()
                    .map(|w| {
                        let id = w["id"].as_str().unwrap();
                        match nodes[id] {
                            Ref::Cell(h) => (h, w["value"].as_i64().unwrap()),
                            _ => panic!("{fixture}: batch write to non-cell {id}"),
                        }
                    })
                    .collect();
                // `#lzmergefeed`: explicit `merge()` calls inside a batch fold
                // synchronously (exact — one per call), while only the cascade
                // defers to batch exit. Counting here is exact for the same
                // reason: the caller decides how many ops exist.
                let merge_writes: Vec<_> = arr(&op["merges"])
                    .iter()
                    .map(|m| {
                        let id = m["id"].as_str().unwrap();
                        count_merge(merges.entry(id.to_owned()).or_default());
                        (cell_of!(id), m["value"].as_i64().unwrap())
                    })
                    .collect();
                model.batch(&writes, &merge_writes);
            }
            "effect" => {
                let id = op["id"].as_str().unwrap().to_owned();
                let reads: Vec<Ref<M::Graph>> = reads_of!(op);
                // `#lzmergefeed`: three effect flavours, chosen by the op's
                // extra field. A `merges_into` effect feeds a merge cell (reads
                // upstream, folds the sum in); a `writes_own_cone` effect closes
                // a scheduler feedback loop by writing its own dependency; a
                // plain effect is a pure sink. Scoped effects are always plain —
                // the corpus never scopes a feed or a divergent loop.
                let h = if let Some(target_id) = op["merges_into"].as_str() {
                    let target = cell_of!(target_id);
                    let counter = merges.entry(target_id.to_owned()).or_default().clone();
                    Ref::Effect(model.feed_effect(&id, &reads, target, &counter))
                } else if let Some(own_id) = op["writes_own_cone"].as_str() {
                    let own = cell_of!(own_id);
                    Ref::Effect(model.diverge_effect(&id, own))
                } else {
                    match op["scope"].as_str() {
                        Some(s) => Ref::Effect(scopes[s].effect(&id, &reads)),
                        None => Ref::Effect(model.effect(&id, &reads)),
                    }
                };
                nodes.insert(id.clone(), h);
                stale.insert(id.clone(), h);
                poisoned.remove(&id);
            }
            "read" => match read_id!(op["id"].as_str().unwrap()) {
                Ok(v) => op_value = Some(v),
                Err(()) => op_error = true,
            },
            "fail_next" => {
                // Arms the next N computes of an existing node to fail. It
                // creates nothing and touches no dependency set.
                let id = op["id"].as_str().unwrap();
                let n = op["count"].as_u64().unwrap_or(1) as usize;
                arm_failure(
                    computes
                        .get(id)
                        .unwrap_or_else(|| panic!("{fixture}: fail_next on uncounted node {id}")),
                    n,
                );
            }
            "set_cell" => {
                let id = op["id"].as_str().unwrap();
                match nodes[id] {
                    Ref::Cell(h) => model.set_cell(h, op["value"].as_i64().unwrap()),
                    _ => panic!("{fixture}: set_cell on non-cell {id}"),
                }
            }
            "dispose" => {
                // The entry stays in the map: a disposed id remains readable-as-
                // an-error, and disposing it again must be a no-op.
                dispose(model.graph(), nodes[op["id"].as_str().unwrap()]);
            }
            "fanout" => {
                let prefix = op["id_prefix"].as_str().unwrap();
                let count = op["count"].as_u64().unwrap();
                let base: Vec<Ref<M::Graph>> = reads_of!(op);
                for i in 0..count {
                    let id = format!("{prefix}_{i}");
                    // Subscribers are effects, not derived slots: the corpus
                    // asserts `observed_count` on a publish, and in a lazy
                    // binding only an eager reader observes a publish without
                    // being pulled.
                    let h = Ref::Effect(model.effect(&id, &base));
                    nodes.insert(id.clone(), h);
                    stale.insert(id, h);
                }
            }
            "dispose_fanout" => {
                let prefix = op["id_prefix"].as_str().unwrap();
                for i in 0..op["count"].as_u64().unwrap() {
                    let id = format!("{prefix}_{i}");
                    if let Some(n) = nodes.get(&id) {
                        dispose(model.graph(), *n);
                    }
                }
            }
            "churn" => {
                let source = *nodes.get(op["source"].as_str().unwrap()).unwrap();
                let prefix = op["id_prefix"].as_str().unwrap();
                let width = op["live_width"].as_u64().unwrap();
                let cycles = op["cycles"].as_u64().unwrap();
                match op["mode"].as_str().unwrap() {
                    // Hold `live_width` subscribers; each cycle disposes one and
                    // creates its replacement, so the live count is invariant.
                    "dispose_then_create" => {
                        for c in 0..cycles {
                            let id = format!("{prefix}_{}", c % width);
                            if let Some(n) = nodes.get(&id) {
                                dispose(model.graph(), *n);
                            }
                            let h = Ref::Effect(model.effect(&id, &[source]));
                            nodes.insert(id, h);
                        }
                    }
                    // One teardown scope per cycle; its subscriber is gone by
                    // the end of its own cycle.
                    "scope_per_cycle" => {
                        let name = format!("{prefix}_scoped");
                        for _ in 0..cycles {
                            let sc = model.scope();
                            sc.effect(&name, &[source]);
                            drop(sc);
                        }
                    }
                    other => panic!("{fixture}: unknown churn mode {other}"),
                }
            }
            "begin_scope" => {
                scopes.insert(op["scope"].as_str().unwrap().to_owned(), model.scope());
            }
            "end_scope" => {
                let name = op["scope"].as_str().unwrap().to_owned();
                let sc = scopes.remove(&name).unwrap();
                op_error = super::models::quiet(move || drop(sc)).is_err();
            }
            "disarm" => {
                let name = op["scope"].as_str().unwrap();
                scopes.remove(name).unwrap().disarm();
                // A disarmed scope owns nothing and is gone; re-open an empty
                // one under the same name so a later `end_scope` is a no-op.
                scopes.insert(name.to_owned(), model.scope());
            }
            "dispose_stale_handle" => {
                let of = op["handle_of"].as_str().unwrap();
                let h = *stale
                    .get(of)
                    .unwrap_or_else(|| panic!("{fixture}: no recorded handle for {of}"));
                let want = op["handle_kind"].as_str().unwrap();
                let matches = matches!(
                    (want, h),
                    ("cell", Ref::Cell(_)) | ("slot", Ref::Slot(_)) | ("effect", Ref::Effect(_))
                );
                assert!(
                    matches,
                    "{fixture}: handle_kind {want} does not match recorded handle"
                );
                dispose(model.graph(), h);
            }
            other => panic!("{fixture}: unknown op {other}"),
        }

        model.settle();
        let observed: Vec<String> = log_snapshot(model.run_log())[runs_before..].to_vec();
        // `cleanup_order` is cumulative, not per-step: the individual-disposal
        // scenario spreads three disposals over three steps and pins the whole
        // order on the last one.
        let cleaned: Vec<String> = log_snapshot(model.cleanup_log());

        let Some(expect) = step.get("expect").and_then(Value::as_object) else {
            continue;
        };

        // `computes_of` is evaluated BEFORE every other key, and deliberately.
        //
        // A step that asserts `computes_of` alongside `value`/`read`/`readable`
        // is asserting a count that a read would change: on a de-eagered signal
        // the read triggers the lazy recompute, so evaluating the read first
        // would raise the count to the number a *conforming* binding shows and
        // make a non-conforming one agree with it.
        // `dispose_signal_reverts_to_lazy` step 3 is exactly that pairing, and
        // it is the only step that separates a real `dispose_signal` from a
        // no-op. Relying on the map's key order for this would be a silent
        // dependency on serde_json's `preserve_order` feature.
        //
        // On `lazily` itself the order happens not to matter — the steps that
        // pair `computes_of` with a read are steps where the read recomputes
        // nothing either way, and the discriminating steps carry no read at
        // all. That is a property of a *conforming* binding, not of the corpus:
        // it is exactly the binding whose `dispose_signal` leaves a live puller
        // behind that a read-first ordering would let through, so the guard
        // stays.
        if let Some(want) = expect.get("computes_of") {
            for (id, v) in want.as_object().unwrap() {
                // `computes_of` on a derived node reads its compute counter; on
                // an *effect* (e.g. `merge_folds`'s `watch`, an observer of the
                // accumulator) the "computes" are its runs, already recorded in
                // the run log by name. Counting there needs no extra counter and
                // no change to how effects are built.
                let got = match computes.get(id.as_str()) {
                    Some(counter) => computes_seen(counter) as u64,
                    None => match nodes.get(id.as_str()) {
                        Some(Ref::Effect(_)) => log_snapshot(model.run_log())
                            .iter()
                            .filter(|n| n.as_str() == id.as_str())
                            .count() as u64,
                        _ => panic!("{fixture}: computes_of unknown node {id}"),
                    },
                };
                check!(format!("computes_of.{id}"), got, v.as_u64().unwrap());
            }
        }

        for (key, want) in expect {
            match key.as_str() {
                "note" | "computes_of" => {}
                // `#lzmergefeed`: cumulative fold count for a merge cell. Not
                // ordering-sensitive against reads (a read never folds), so it
                // is checked here in step order rather than pre-evaluated like
                // `computes_of`.
                "merges_of" => {
                    for (id, v) in want.as_object().unwrap() {
                        let counter = merges
                            .get(id.as_str())
                            .unwrap_or_else(|| panic!("{fixture}: merges_of unknown cell {id}"));
                        check!(
                            format!("merges_of.{id}"),
                            merges_seen(counter) as u64,
                            v.as_u64().unwrap()
                        );
                    }
                }
                // `#lzfeedbackdrain`: did the op's settle exhaust the effect
                // drain? A divergent scheduler-closed loop must report `true`
                // rather than hang; a converging op reports `false`.
                "drain_exhausted" => {
                    check!(
                        "drain_exhausted",
                        model.drain_exhausted(),
                        want.as_bool().unwrap()
                    );
                }
                "dependents_of" => {
                    for (id, v) in want.as_object().unwrap() {
                        check!(
                            format!("dependents_of.{id}"),
                            degree!(id.as_str(), dependents_of),
                            v.as_u64().unwrap() as usize
                        );
                    }
                }
                "dependencies_of" => {
                    for (id, v) in want.as_object().unwrap() {
                        check!(
                            format!("dependencies_of.{id}"),
                            degree!(id.as_str(), dependencies_of),
                            v.as_u64().unwrap() as usize
                        );
                    }
                }
                // Any non-null error code means "this op must fail"; null means
                // "must not". The runner does not model error identity — the
                // fixtures carry the code so the contract is legible, and each
                // binding's own tests pin which error it raises.
                "error" => check!("error", op_error, !want.is_null()),
                "value" => {
                    if expect.get("error").and_then(Value::as_str).is_none() {
                        // The signal fixtures assert `value` on the `signal`
                        // CREATION op, not only on `read` ops. Only `read` sets
                        // `op_value`, so without this fallback the assertion
                        // would compare `None` against the expected number —
                        // which fails loudly here, but in a runner that treated
                        // a missing value as "nothing to check" would silently
                        // assert nothing. The read is issued lazily, *after*
                        // `computes_of` has already been evaluated above, so it
                        // cannot mask a deferred materialization.
                        // `#lzmergefeed`: a feed effect's op id is the effect
                        // (unreadable), so its `value` assertion targets the
                        // merge cell it feeds — the accumulator whose baseline
                        // fold the step is pinning. Otherwise (signal creation)
                        // the op id is itself the readable node.
                        let read_target = op["merges_into"].as_str().or_else(|| op["id"].as_str());
                        let got = match op_value {
                            Some(v) => Some(v),
                            None => read_target.and_then(|id| read_id!(id).ok()),
                        };
                        check!("value", got, want.as_i64());
                    }
                }
                "read" => {
                    for (id, v) in want.as_object().unwrap() {
                        check!(
                            format!("read.{id}"),
                            read_id!(id.as_str()),
                            Ok(v.as_i64().unwrap())
                        );
                    }
                }
                "readable" => {
                    for (id, v) in want.as_object().unwrap() {
                        // A signal is readable iff its backing slot is: clause 4
                        // says disposing the puller leaves the value live, so
                        // this must NOT consult the puller's active flag.
                        let alive = if signals.contains_key(id.as_str()) {
                            read_id!(id.as_str()).is_ok()
                        } else {
                            match nodes.get(id.as_str()) {
                                None => false,
                                Some(Ref::Effect(h)) => model.is_effect_active(*h),
                                Some(_) => read_id!(id.as_str()).is_ok(),
                            }
                        };
                        check!(format!("readable.{id}"), alive, v.as_bool().unwrap());
                    }
                }
                "observed_by" => check!("observed_by", observed.clone(), strs(want)),
                "observed_count" => {
                    check!(
                        "observed_count",
                        observed.len() as u64,
                        want.as_u64().unwrap()
                    )
                }
                "cleanup_order" => {
                    // Only effects run a cleanup callback, so the expected order
                    // is projected onto its effect entries.
                    let want: Vec<String> = strs(want)
                        .into_iter()
                        .filter(|id| matches!(stale.get(id), Some(Ref::Effect(_))))
                        .collect();
                    check!("cleanup_order", cleaned.clone(), want);
                }
                "scope_owned_count" => {
                    for (name, v) in want.as_object().unwrap() {
                        check!(
                            format!("scope_owned_count.{name}"),
                            scopes[name.as_str()].owned() as u64,
                            v.as_u64().unwrap()
                        );
                    }
                }
                other => panic!("{fixture}: unknown expectation {other}"),
            }
        }
    }

    // -- `scenarios`-shaped tail --------------------------------------------
    let mut observation = Observation {
        cleanup_order: log_snapshot(model.cleanup_log()),
        ..Observation::default()
    };
    if let Some(tail) = tail {
        step_idx = usize::MAX; // the `expected` tail is not a numbered step
        // The fixture-level `expected` tail is guarded too (`#lzassertunknownkeys`);
        // its sub-blocks carry assertion names, so they are descended into.
        let fin = tail.sub("final_state");
        for (id, v) in fin["dependents_of"].as_object().into_iter().flatten() {
            let got = degree!(id.as_str(), dependents_of);
            check!(
                format!("final.dependents_of.{id}"),
                got,
                v.as_u64().unwrap() as usize
            );
            observation.degrees.insert(id.clone(), got);
        }
        for (id, v) in fin["readable"].as_object().into_iter().flatten() {
            let alive = match nodes.get(id.as_str()) {
                None => false,
                Some(Ref::Effect(h)) => model.is_effect_active(*h),
                Some(_) => read_id!(id.as_str()).is_ok(),
            };
            check!(format!("final.readable.{id}"), alive, v.as_bool().unwrap());
            observation.readable.insert(id.clone(), alive);
        }
        for (id, v) in fin["read"].as_object().into_iter().flatten() {
            let got = read_id!(id.as_str());
            check!(format!("final.read.{id}"), got, Ok(v.as_i64().unwrap()));
            observation
                .reads
                .insert(id.clone(), got.unwrap_or_default());
        }

        let publish = tail.sub("after_publish");
        if let Some(pop) = publish.get_opt("op") {
            let id = pop["id"].as_str().unwrap();
            let before = log_snapshot(model.run_log()).len();
            match nodes[id] {
                Ref::Cell(h) => model.set_cell(h, pop["value"].as_i64().unwrap()),
                _ => panic!("{fixture}: after_publish set_cell on non-cell"),
            }
            model.settle();
            observation.after_publish_observed = log_snapshot(model.run_log())[before..].to_vec();
            check!(
                "after_publish.observed_by",
                observation.after_publish_observed.clone(),
                strs(&publish["observed_by"])
            );
            for (rid, v) in publish["read"].as_object().into_iter().flatten() {
                let got = read_id!(rid.as_str());
                check!(
                    format!("after_publish.read.{rid}"),
                    got,
                    Ok(v.as_i64().unwrap())
                );
                observation
                    .after_publish_reads
                    .insert(rid.clone(), got.unwrap_or_default());
            }
            for (id, v) in publish["dependents_of"].as_object().into_iter().flatten() {
                check!(
                    format!("after_publish.dependents_of.{id}"),
                    degree!(id.as_str(), dependents_of),
                    v.as_u64().unwrap() as usize
                );
            }
        }
    }

    Report {
        failures,
        ops,
        checks,
        observation,
    }
}
