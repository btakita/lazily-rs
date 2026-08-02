//! Cross-language conformance for stream windowing (`#lzwindow`) — see
//! `lazily-spec/docs/windowing.md` and
//! `lazily-spec/conformance/windowing/*.json`. All fixtures use `Sum` (u64)
//! aggregates for determinism.

mod common;

use common::Expect;
use lazily::{Context, SessionWindow, SlidingWindow, Sum, TumblingCountWindow, TumblingTimeWindow};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/windowing";

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/tumbling_count.json")).exists()
}

fn steps(fx: &Value) -> &Vec<Value> {
    fx["steps"].as_array().unwrap()
}
fn ret(step: &Value) -> Option<u64> {
    step["returns"].as_u64()
}
/// The step's op discriminator, read and CHECKED (`#lzscenariobodyskip`).
///
/// The count-driven windows used to replay every step as a `push` without ever
/// looking at `op.type`, and the time-driven ones dispatched with a bare `else`
/// that ASSUMED the only other spelling. Both are the same fail-open: a fixture
/// naming an op this runner does not implement replays as something else, the
/// `expected` block is compared against state the named op never touched, and
/// the scenario books itself as replayed.
fn op_type<'a>(fixture: &str, op: &'a Value) -> &'a str {
    op["type"]
        .as_str()
        .unwrap_or_else(|| panic!("{fixture}: step op carries no `type`: {op}"))
}
/// Assert one step, with the `expected` block guarded (`#lzassertunknownkeys`):
/// a key this runner never reads fails the fixture instead of passing unnoticed.
fn check(
    ctx: &Context,
    observed: &lazily::Computed<Option<u64>>,
    fixture: &str,
    i: usize,
    step: &Value,
    out: Option<u64>,
) {
    let exp = Expect::new(
        format!("{SPEC_DIR}/{fixture}"),
        format!("steps[{i}].expected"),
        &step["expected"],
    );
    let inv = exp.sub("invalidates");
    exp.assert_key_with("output", |want| {
        assert_eq!(out, want.as_u64(), "output for {step}")
    });
    let was = ctx.is_set(observed);
    let _ = observed.get(ctx);
    inv.assert_key_at("output", !was, &format!("step {step}"));
}

#[test]
fn tumbling_count() {
    if !present() {
        return;
    }
    let fx = load("tumbling_count.json");
    let ctx = Context::new();
    let n = fx["config"]["n"].as_u64().unwrap();
    let w = TumblingCountWindow::<u64, Sum>::new(&ctx, n);
    let oc = w.output_cell();
    let observed = ctx.computed(move |c| oc.get(c));
    let _ = observed.get(&ctx);
    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let emitted = match op_type("tumbling_count.json", op) {
            "push" => w.push(&ctx, op["value"].as_u64().unwrap()),
            other => panic!("tumbling_count.json: unknown windowing op `{other}`"),
        };
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(
            &ctx,
            &observed,
            "tumbling_count.json",
            i,
            step,
            w.output(&ctx),
        );
    }
}

#[test]
fn tumbling_time() {
    if !present() {
        return;
    }
    let fx = load("tumbling_time.json");
    let ctx = Context::new();
    let period = fx["config"]["period"].as_u64().unwrap();
    let w = TumblingTimeWindow::<u64, Sum>::new(&ctx, period);
    let oc = w.output_cell();
    let observed = ctx.computed(move |c| oc.get(c));
    let _ = observed.get(&ctx);
    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        let emitted = match op_type("tumbling_time.json", op) {
            "push" => {
                w.push(&ctx, now, op["value"].as_u64().unwrap());
                None
            }
            "tick" => w.tick(&ctx, now),
            other => panic!("tumbling_time.json: unknown windowing op `{other}`"),
        };
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(
            &ctx,
            &observed,
            "tumbling_time.json",
            i,
            step,
            w.output(&ctx),
        );
    }
}

#[test]
fn sliding_count() {
    if !present() {
        return;
    }
    let fx = load("sliding_count.json");
    let ctx = Context::new();
    let size = fx["config"]["size"].as_u64().unwrap() as usize;
    let slide = fx["config"]["slide"].as_u64().unwrap();
    let w = SlidingWindow::<u64, Sum>::new(&ctx, size, slide);
    let oc = w.output_cell();
    let observed = ctx.computed(move |c| oc.get(c));
    let _ = observed.get(&ctx);
    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let emitted = match op_type("sliding_count.json", op) {
            "push" => w.push(&ctx, op["value"].as_u64().unwrap()),
            other => panic!("sliding_count.json: unknown windowing op `{other}`"),
        };
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(
            &ctx,
            &observed,
            "sliding_count.json",
            i,
            step,
            w.output(&ctx),
        );
    }
}

#[test]
fn session() {
    if !present() {
        return;
    }
    let fx = load("session.json");
    let ctx = Context::new();
    let gap = fx["config"]["gap"].as_u64().unwrap();
    let w = SessionWindow::<u64, Sum>::new(&ctx, gap);
    let oc = w.output_cell();
    let observed = ctx.computed(move |c| oc.get(c));
    let _ = observed.get(&ctx);
    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        let emitted = match op_type("session.json", op) {
            "push" => w.push(&ctx, now, op["value"].as_u64().unwrap()),
            "flush" => w.flush(&ctx, now),
            other => panic!("session.json: unknown windowing op `{other}`"),
        };
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(&ctx, &observed, "session.json", i, step, w.output(&ctx));
    }
}
