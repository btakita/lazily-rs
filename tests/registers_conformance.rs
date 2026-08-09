//! Cross-language conformance for the register CRDTs — LWW, MV, PnCounter —
//! replaying `lazily-spec/conformance/collections/registers_convergence.json`.
//!
//! The Registers coverage row was marked shipped in nine bindings and backed by
//! NO canonical fixture, so `check-coverage-claims.mjs` could not verify one of
//! those marks. `seqcrdt_convergence.json` exercises LWW registers only
//! incidentally, as the substrate for move-aware sequence order, and backs the
//! SeqCrdt row instead.
//!
//! What the fixture pins, and therefore what this replays:
//!
//! - **LWW** resolves on the total order `(wall_time, logical, peer)`. The peer
//!   component is the clause worth having a fixture for: with equal `(wall,
//!   logical)` a binding comparing only those two has no order at all, so the
//!   two replicas keep different values and never converge.
//! - **MV** keeps every value whose version vector is not dominated, and
//!   collapses them on a causally-later write. Both halves matter — a binding
//!   that only ever resolves has implemented LWW twice, and one that only ever
//!   accumulates never converges.
//! - **PnCounter** merges per-peer tallies by MAXIMUM, which is what makes
//!   re-merging a no-op; adding them instead double-counts and drifts.
//! - **CellCrdt** is the projection clause: `merge_from` reports whether the
//!   OBSERVED value changed, because that boolean decides whether the reactive
//!   cell invalidates its dependents. A binding always reporting `true` would
//!   converge correctly and cascade on every anti-entropy round — which is why
//!   the fixture carries both outcomes rather than only the interesting one.
//!
//! Feature-gated because the register types live behind the `distributed`
//! feature (the CRDT plane).

#![cfg(feature = "distributed")]

mod common;

use std::collections::HashMap;

use lazily::{CellCrdt, HlcStamp, LwwRegister, MvRegister, PeerId, PnCounter};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn u64_at(v: &Value, key: &str) -> u64 {
    v.get(key)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("expected u64 field `{key}` in {v}"))
}

fn str_at(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("expected string field `{key}` in {v}"))
        .to_string()
}

fn stamp_of(op: &Value) -> HlcStamp {
    HlcStamp {
        wall_time: u64_at(op, "wall"),
        logical: u64_at(op, "logical"),
        peer: PeerId(u64_at(op, "peer")),
    }
}

/// The three register kinds behind one replay surface.
///
/// They are genuinely different types with different write signatures — that is
/// the point of scoring them on one row — so this enum is the seam rather than
/// a trait: `CellCrdt` unifies `merge_from`/`value`, and the fixture's `set` /
/// `incr` / `decr` ops do not unify at all.
enum Replica {
    Lww(LwwRegister<String>),
    Mv(MvRegister<String>),
    Pn(PnCounter),
}

impl Replica {
    fn seed(register: &str, seed: &Value) -> Self {
        match register {
            "lww" => Replica::Lww(LwwRegister::new(
                str_at(seed, "value"),
                HlcStamp {
                    wall_time: u64_at(seed, "wall"),
                    logical: u64_at(seed, "logical"),
                    peer: PeerId(u64_at(seed, "peer")),
                },
            )),
            "mv" => Replica::Mv(MvRegister::new()),
            "pncounter" => Replica::Pn(PnCounter::new()),
            other => panic!("unknown register kind {other}"),
        }
    }

    fn fork(&self) -> Self {
        match self {
            Replica::Lww(r) => Replica::Lww(r.clone()),
            Replica::Mv(r) => Replica::Mv(r.clone()),
            Replica::Pn(r) => Replica::Pn(r.clone()),
        }
    }

    fn apply(&mut self, op: &Value) {
        let kind = str_at(op, "op");
        match (self, kind.as_str()) {
            (Replica::Lww(r), "set") => {
                r.set(str_at(op, "value"), stamp_of(op));
            }
            (Replica::Mv(r), "set") => {
                r.set(str_at(op, "value"), PeerId(u64_at(op, "peer")));
            }
            (Replica::Pn(r), "incr") => {
                r.increment(PeerId(u64_at(op, "peer")), u64_at(op, "amount"));
            }
            (Replica::Pn(r), "decr") => {
                r.decrement(PeerId(u64_at(op, "peer")), u64_at(op, "amount"));
            }
            (_, other) => panic!("op `{other}` is not defined for this register kind"),
        }
    }

    /// Merge `from` into `self`, returning the CellCrdt projection bit.
    fn merge_from(&mut self, from: &Replica) -> bool {
        match (self, from) {
            (Replica::Lww(a), Replica::Lww(b)) => a.merge_from(b),
            (Replica::Mv(a), Replica::Mv(b)) => a.merge_from(b),
            (Replica::Pn(a), Replica::Pn(b)) => a.merge_from(b),
            _ => panic!("cannot merge two different register kinds"),
        }
    }

    fn scalar(&self) -> Value {
        match self {
            Replica::Lww(r) => Value::from(r.value()),
            Replica::Pn(r) => Value::from(r.value()),
            Replica::Mv(_) => panic!("an MV register has a value SET, not a scalar"),
        }
    }

    fn values(&self) -> Vec<String> {
        match self {
            Replica::Mv(r) => {
                let mut vs = r.values();
                // Compared as a set: the corpus deliberately does not pin an
                // iteration order for concurrent values.
                vs.sort();
                vs
            }
            _ => panic!("only an MV register has a value set"),
        }
    }

    fn stamp(&self) -> HlcStamp {
        match self {
            Replica::Lww(r) => r.stamp(),
            _ => panic!("only an LWW register carries a single winning stamp"),
        }
    }
}

fn replay(scenario: &Value) -> usize {
    let register = str_at(scenario, "register");
    let seed = scenario.get("seed").expect("scenario.seed");
    let mut world: HashMap<String, Replica> = HashMap::new();
    world.insert("a".to_string(), Replica::seed(&register, seed));

    // `changed` is the LAST merge's projection bit, so it is overwritten rather
    // than accumulated: the idempotence scenarios assert precisely that the
    // SECOND delivery reported nothing.
    let mut last_merge_changed: Option<bool> = None;

    for step in scenario
        .get("steps")
        .and_then(Value::as_array)
        .expect("scenario.steps")
    {
        if let Some(name) = step.get("fork").and_then(Value::as_str) {
            let forked = world.get("a").expect("fork source `a`").fork();
            world.insert(name.to_string(), forked);
            continue;
        }
        if let Some(merge) = step.get("merge") {
            let from_name = str_at(merge, "from");
            let into_name = str_at(merge, "into");
            let from = world.get(&from_name).expect("merge source").fork();
            let into = world.get_mut(&into_name).expect("merge target");
            last_merge_changed = Some(into.merge_from(&from));
            continue;
        }
        let on = step
            .get("on")
            .and_then(Value::as_str)
            .unwrap_or("a")
            .to_string();
        world.get_mut(&on).expect("op target").apply(step);
    }

    let expect = scenario.get("expect").expect("scenario.expect");
    let mut asserted = 0usize;
    for (key, want) in expect.as_object().expect("expect is an object") {
        match key.as_str() {
            "note" => {}
            "value_on" => {
                for (name, v) in want.as_object().expect("value_on is an object") {
                    let got = world.get(name).expect("value_on names a replica").scalar();
                    assert_eq!(&got, v, "value_on.{name}");
                    asserted += 1;
                }
            }
            "values_on" => {
                for (name, v) in want.as_object().expect("values_on is an object") {
                    let mut wanted: Vec<String> = v
                        .as_array()
                        .expect("values_on entry is an array")
                        .iter()
                        .map(|e| e.as_str().expect("values_on entry item").to_string())
                        .collect();
                    wanted.sort();
                    let got = world.get(name).expect("values_on names a replica").values();
                    assert_eq!(got, wanted, "values_on.{name}");
                    asserted += 1;
                }
            }
            "stamp_on" => {
                for (name, v) in want.as_object().expect("stamp_on is an object") {
                    let got = world.get(name).expect("stamp_on names a replica").stamp();
                    assert_eq!(got.wall_time, u64_at(v, "wall"), "stamp_on.{name}.wall");
                    assert_eq!(got.logical, u64_at(v, "logical"), "stamp_on.{name}.logical");
                    assert_eq!(got.peer.0, u64_at(v, "peer"), "stamp_on.{name}.peer");
                    asserted += 1;
                }
            }
            "changed" => {
                let got = last_merge_changed.expect("`changed` asserted but no merge ran");
                assert_eq!(
                    got,
                    want.as_bool().expect("changed is a bool"),
                    "changed (the CellCrdt projection bit of the last merge)"
                );
                asserted += 1;
            }
            other => panic!(
                "unsupported expect key `{other}` — implement it rather than \
                 letting the corpus grow a claim this runner does not check"
            ),
        }
    }
    assert!(asserted > 0, "scenario asserted nothing");
    asserted
}

#[test]
fn registers_convergence_replays_every_scenario() {
    let fixture = load_fixture("registers_convergence.json");
    assert_eq!(
        fixture.get("model").and_then(Value::as_str),
        Some("Registers"),
        "fixture is not the Registers corpus"
    );
    // Iterate through `common::scenarios`, not through `fixture["scenarios"]`:
    // the manifest rung only proves this binary OPENED the file, so a runner
    // reading the array directly replays nine scenarios and books none, and the
    // fixture reads as covered because a SIBLING scenario ran. `ScenarioView`
    // books on the first read of the PAYLOAD, which is what makes the ledger a
    // statement about the run rather than about the source.
    let mut replayed = 0usize;
    let mut checks = 0usize;
    // The path is the one the fixture was READ from, not a corpus-relative
    // spelling: `record_scenario` derives the fixture id by finding the
    // `conformance/` segment, and a bare relative string resolves to None and
    // books nothing — silently, because bookkeeping never fails a suite.
    let path = format!("{SPEC_DIR}/registers_convergence.json");
    for (_index, _id, view) in common::scenarios(&path, &fixture) {
        checks += replay(view.value());
        replayed += 1;
    }
    // A positive count, not just "no failures": a fixture whose scenarios all
    // vanished upstream would otherwise pass by replaying nothing.
    assert_eq!(replayed, 9, "the corpus ships nine register scenarios");
    assert!(checks >= replayed, "every scenario must assert");
}
