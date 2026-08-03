//! Cross-language conformance tests for the full Harel state-chart spec
//! (`lazily-spec/docs/state-charts.md`). Each test loads a canonical chart
//! fixture, builds a `StateChart`, asserts `initial_active`/`initial_actions`,
//! replays the `steps`, and asserts `accepted`, `active`, `matches`, and
//! `actions` after each step — the same fixtures every binding replays.

#![cfg(feature = "statechart-json")]

mod common;

use std::collections::HashMap;

use common::Expect;
use lazily::{ChartDef, Context, StateChart};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/statechart";

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn build_chart(fixture: &Value) -> (Context, StateChart) {
    let ctx = Context::new();
    let def = ChartDef::from_json(fixture.get("chart").expect("chart"))
        .unwrap_or_else(|e| panic!("failed to parse chart: {e}"));
    let chart = StateChart::new(&ctx, def);
    (ctx, chart)
}

fn assert_active(ctx: &Context, chart: &StateChart, expected: &Value, msg: &str) {
    let mut want: Vec<String> = match expected {
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a
            .iter()
            .map(|v| v.as_str().expect("active leaf id").to_string())
            .collect(),
        _ => panic!("active must be string or array"),
    };
    want.sort();
    let mut got = chart.active_leaves(ctx);
    got.sort();
    assert_eq!(got, want, "{msg}");
}

fn assert_matches(ctx: &Context, chart: &StateChart, step: &Expect) {
    // `matches` keys are state ids — data, not assertion names — so the map is
    // compared wholesale. Optional per step, hence bound to the key's presence.
    step.assert_key_if_present("matches", |want| {
        for (id, expected) in want.as_object().expect("matches object") {
            let want = expected.as_bool().expect("matches value is bool");
            assert_eq!(chart.matches(ctx, id), want, "matches({id}) mismatch");
        }
    });
}

/// A statechart fixture has no separate `expected` block — the step object *is*
/// the assertion block, mixed with the step's inputs. Both levels are guarded
/// (`#lzassertunknownkeys`), so an assertion key this runner does not read fails
/// the fixture instead of being skipped.
fn run_fixture(name: &str) {
    let fixture = load_fixture(name);
    let (ctx, chart) = build_chart(&fixture);

    let fx = Expect::new(format!("{SPEC_DIR}/{name}"), "<fixture>", &fixture);
    fx.excuse_key("kind", "corpus routing tag, consumed by the coverage guard");
    fx.excuse_key(
        "chart",
        "the chart definition under test, built by build_chart — an input, not a \
         value to compare",
    );
    fx.excuse_key(
        "steps",
        "the event sequence replayed; each step is guarded on its own below",
    );

    // initial_active (asserted once before any step).
    fx.assert_key_with("initial_active", |want| {
        assert_active(&ctx, &chart, want, "initial_active")
    });

    // initial_actions (optional).
    fx.assert_key_if_present("initial_actions", |want| {
        let want: Vec<String> = want
            .as_array()
            .expect("initial_actions")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(chart.last_actions(), want, "initial_actions");
    });

    let steps = fx.raw()["steps"].as_array().expect("steps");
    for (i, step) in steps.iter().enumerate() {
        let step = fx.nested(format!("steps[{i}]"), step);
        // `event` and `guards` are the step's *inputs* — they drive the send, and
        // what the send produced is asserted below.
        step.excuse_key("event", "the event sent; an input, not a value to compare");
        step.excuse_key(
            "guards",
            "the guard valuation supplied to the send; an input, not a value to compare",
        );
        let event = step.raw()["event"].as_str().expect("event");
        let guards: HashMap<String, bool> = step.raw()["guards"]
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.as_bool().unwrap_or(false)))
                    .collect()
            })
            .unwrap_or_default();

        let accepted = chart.send(&ctx, event, &guards);
        step.assert_key_at("accepted", accepted, &format!("step {i} `{event}`"));

        step.assert_key_with("active", |want| {
            assert_active(&ctx, &chart, want, &format!("step {i} `{event}` active"))
        });
        assert_matches(&ctx, &chart, &step);

        step.assert_key_if_present("actions", |want| {
            let want: Vec<String> = want
                .as_array()
                .expect("actions")
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            assert_eq!(chart.last_actions(), want, "step {i} `{event}` actions");
        });
    }
}

#[test]
fn conformance_flat_cycle() {
    run_fixture("flat_cycle.json");
}

#[test]
fn conformance_hierarchical_player() {
    run_fixture("hierarchical_player.json");
}

#[test]
fn conformance_guarded_door() {
    run_fixture("guarded_door.json");
}

#[test]
fn conformance_parallel_regions() {
    run_fixture("parallel_regions.json");
}

#[test]
fn conformance_history_shallow() {
    run_fixture("history_shallow.json");
}

#[test]
fn conformance_history_deep() {
    run_fixture("history_deep.json");
}

#[test]
fn conformance_entry_exit_actions() {
    run_fixture("entry_exit_actions.json");
}
