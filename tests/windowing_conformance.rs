//! Cross-language conformance for stream windowing (`#lzwindow`) — see
//! `lazily-spec/docs/windowing.md` and
//! `lazily-spec/conformance/windowing/*.json`. All fixtures use `Sum` (u64)
//! aggregates for determinism.

mod common;

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
fn exp_out(step: &Value) -> Option<u64> {
    step["expected"]["output"].as_u64()
}
fn inval(step: &Value) -> bool {
    step["expected"]["invalidates"]["output"].as_bool().unwrap()
}

fn check(ctx: &Context, observed: &lazily::Computed<Option<u64>>, step: &Value, out: Option<u64>) {
    assert_eq!(out, exp_out(step), "output for {step}");
    let was = ctx.is_set(observed);
    let _ = observed.get(ctx);
    assert_eq!(!was, inval(step), "inval for {step}");
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
    for step in steps(&fx) {
        let emitted = w.push(&ctx, step["op"]["value"].as_u64().unwrap());
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(&ctx, &observed, step, w.output(&ctx));
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
    for step in steps(&fx) {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        let emitted = if op["type"] == "push" {
            w.push(&ctx, now, op["value"].as_u64().unwrap());
            None
        } else {
            w.tick(&ctx, now)
        };
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(&ctx, &observed, step, w.output(&ctx));
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
    for step in steps(&fx) {
        let emitted = w.push(&ctx, step["op"]["value"].as_u64().unwrap());
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(&ctx, &observed, step, w.output(&ctx));
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
    for step in steps(&fx) {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        let emitted = if op["type"] == "push" {
            w.push(&ctx, now, op["value"].as_u64().unwrap())
        } else {
            w.flush(&ctx, now)
        };
        assert_eq!(emitted, ret(step), "emit for {step}");
        check(&ctx, &observed, step, w.output(&ctx));
    }
}
