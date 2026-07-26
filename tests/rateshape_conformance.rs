//! Cross-language conformance for the rate-shaping source operators
//! (`#lzrateshape`) — see `lazily-spec/docs/rate-shaping.md` and
//! `lazily-spec/conformance/rateshape/*.json`.
//!
//! Compute fixtures: replay each `input`/`tick` op, assert the emitted value
//! (`returns`), the projected `output`, and that the `output` reader invalidates
//! exactly on an emit (observed via `ctx.is_set` on a wrapping `computed`).

mod common;

use lazily::{
    Context, DebounceCell, Lcg, ProbabilisticSampleCell, SampleCell, SampleMode, ThrottleCell,
    ThrottleEdge,
};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/rateshape";

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/debounce.json")).exists()
}

fn steps(fx: &Value) -> &Vec<Value> {
    fx["steps"].as_array().unwrap()
}
fn op_now(step: &Value) -> u64 {
    step["op"]["now"].as_u64().unwrap()
}
fn op_val(step: &Value) -> String {
    step["op"]["value"].as_str().unwrap().to_string()
}
fn ret(step: &Value) -> Option<String> {
    step["returns"].as_str().map(|s| s.to_string())
}
fn exp_output(step: &Value) -> Option<String> {
    step["expected"]["output"].as_str().map(|s| s.to_string())
}
fn exp_inval(step: &Value) -> bool {
    step["expected"]["invalidates"]["output"].as_bool().unwrap()
}

/// Run a fixture whose emit projection is a `Source<Option<String>>`, given a
/// per-step driver returning the emitted value and the current output.
fn run<F>(ctx: &Context, fx: &Value, observed: lazily::Computed<Option<String>>, mut drive: F)
where
    F: FnMut(&Value) -> (Option<String>, Option<String>),
{
    let _ = observed.get(ctx);
    for step in steps(fx) {
        let (emitted, output) = drive(step);
        assert_eq!(emitted, ret(step), "emit for {step}");
        assert_eq!(output, exp_output(step), "output for {step}");

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(ctx);
        assert_eq!(!was_cached, exp_inval(step), "invalidation for {step}");
    }
}

#[test]
fn debounce() {
    if !present() {
        return;
    }
    let fx = load_fixture("debounce.json");
    let ctx = Context::new();
    let quiet = fx["initial"]["quiet"].as_u64().unwrap();
    let cell = DebounceCell::<String>::new(&ctx, quiet);
    let out = cell.output_cell();
    let observed = ctx.computed(move |c| out.get(c));
    run(&ctx, &fx, observed, |step| {
        let emitted = if step["op"]["type"] == "input" {
            cell.input(&ctx, op_now(step), op_val(step));
            None
        } else {
            cell.tick(&ctx, op_now(step))
        };
        (emitted, cell.output(&ctx))
    });
}

fn run_throttle(name: &str, edge: ThrottleEdge) {
    let fx = load_fixture(name);
    let ctx = Context::new();
    let window = fx["initial"]["window"].as_u64().unwrap();
    let cell = ThrottleCell::<String>::new(&ctx, edge, window);
    let out = cell.output_cell();
    let observed = ctx.computed(move |c| out.get(c));
    run(&ctx, &fx, observed, |step| {
        let emitted = if step["op"]["type"] == "input" {
            cell.input(&ctx, op_now(step), op_val(step))
        } else {
            cell.tick(&ctx, op_now(step))
        };
        (emitted, cell.output(&ctx))
    });
}

#[test]
fn throttle_leading() {
    if !present() {
        return;
    }
    run_throttle("throttle_leading.json", ThrottleEdge::Leading);
}

#[test]
fn throttle_trailing() {
    if !present() {
        return;
    }
    run_throttle("throttle_trailing.json", ThrottleEdge::Trailing);
}

#[test]
fn sample_count() {
    if !present() {
        return;
    }
    let fx = load_fixture("sample_count.json");
    let ctx = Context::new();
    let n = fx["initial"]["n"].as_u64().unwrap();
    let cell = SampleCell::<String>::new(&ctx, SampleMode::Count(n));
    let out = cell.output_cell();
    let observed = ctx.computed(move |c| out.get(c));
    run(&ctx, &fx, observed, |step| {
        let emitted = cell.input(&ctx, op_val(step));
        (emitted, cell.output(&ctx))
    });
}

#[test]
fn sample_time() {
    if !present() {
        return;
    }
    let fx = load_fixture("sample_time.json");
    let ctx = Context::new();
    let period = fx["initial"]["period"].as_u64().unwrap();
    let cell = SampleCell::<String>::new(&ctx, SampleMode::Time(period));
    let out = cell.output_cell();
    let observed = ctx.computed(move |c| out.get(c));
    run(&ctx, &fx, observed, |step| {
        let emitted = if step["op"]["type"] == "input" {
            cell.input(&ctx, op_val(step));
            None
        } else {
            cell.tick(&ctx, op_now(step))
        };
        (emitted, cell.output(&ctx))
    });
}

#[test]
fn probabilistic_sample() {
    if !present() {
        return;
    }
    let fx = load_fixture("probabilistic_sample.json");
    let ctx = Context::new();
    let rate = fx["initial"]["rate"].as_f64().unwrap();
    // Draws are injected per step via `input_with_draw`, so the owned RNG is
    // unused here; a deterministic `Lcg` satisfies the type bound.
    let cell = ProbabilisticSampleCell::<String, Lcg>::new(&ctx, rate, Lcg::new(0));
    let out = cell.output_cell();
    let observed = ctx.computed(move |c| out.get(c));
    run(&ctx, &fx, observed, |step| {
        let draw = step["op"]["draw"].as_f64().unwrap();
        let emitted = cell.input_with_draw(&ctx, op_val(step), draw);
        (emitted, cell.output(&ctx))
    });
}
