//! Cross-language conformance for the temporal source primitives (`#lztime`) —
//! see `lazily-spec/docs/temporal-sources.md` and
//! `lazily-spec/conformance/temporal/*.json`.
//!
//! These are **compute** fixtures: lazily-rs loads the `initial` state, replays
//! each `tick(now)` op, and asserts the fire edge (`returns`), the projected
//! reader values, and — the core of the spec — that the primary reader
//! invalidates exactly on the fire edge. Invalidation is observed by wrapping the
//! reader cell in a `computed` and checking whether its cached value survives the
//! tick (`ctx.is_set`).

mod common;

use lazily::{Context, CronCell, DeadlineCell, Deadlined, IntervalCell, TimerCell};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/temporal";

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn spec_fixtures_present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/timer_single_shot.json")).exists()
}

fn steps(fx: &Value) -> &Vec<Value> {
    fx["steps"].as_array().unwrap()
}

fn now_of(step: &Value) -> u64 {
    step["op"]["now"].as_u64().unwrap()
}

fn edge_of(step: &Value) -> bool {
    step["returns"].as_bool().unwrap()
}

/// Whether the primary reader invalidates on this step (from the fixture).
fn invalidates(step: &Value, reader: &str) -> bool {
    step["expected"]["invalidates"][reader].as_bool().unwrap()
}

#[test]
fn timer_single_shot() {
    if !spec_fixtures_present() {
        return;
    }
    let fx = load_fixture("timer_single_shot.json");
    let ctx = Context::new();
    let fire_at = fx["initial"]["fire_at"].as_u64().unwrap();
    let timer = TimerCell::new(&ctx, fire_at);
    let fired = timer.fired_cell();
    let observed = ctx.computed(move |c| fired.get(c));
    let _ = observed.get(&ctx); // prime the cache

    for step in steps(&fx) {
        let edge = timer.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "fire edge for {step}");

        let exp = &step["expected"];
        assert_eq!(timer.has_fired(&ctx), exp["fired"].as_bool().unwrap());
        match exp["value"].as_str() {
            Some("()") => assert_eq!(timer.value(&ctx), Some(())),
            _ => assert_eq!(timer.value(&ctx), None),
        }
        assert_eq!(timer.next_fire(), exp["next_fire"].as_u64());

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        // The reader must have been invalidated (cache dropped) exactly when the
        // fixture says so.
        assert_eq!(
            !was_cached,
            invalidates(step, "fired"),
            "invalidation for {step}"
        );
    }
}

#[test]
fn interval_periodic() {
    if !spec_fixtures_present() {
        return;
    }
    let fx = load_fixture("interval_periodic.json");
    let ctx = Context::new();
    let period = fx["initial"]["period"].as_u64().unwrap();
    let iv = IntervalCell::new(&ctx, period);
    let count = iv.count_cell();
    let observed = ctx.computed(move |c| count.get(c));
    let _ = observed.get(&ctx);

    for step in steps(&fx) {
        let edge = iv.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "fire edge for {step}");

        let exp = &step["expected"];
        assert_eq!(iv.count(&ctx), exp["count"].as_u64().unwrap());
        assert_eq!(iv.next_fire(), exp["next_fire"].as_u64());

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was_cached, invalidates(step, "count"), "inval {step}");
    }
}

#[test]
fn cron_pattern() {
    if !spec_fixtures_present() {
        return;
    }
    let fx = load_fixture("cron_pattern.json");
    let ctx = Context::new();
    let cycle = fx["initial"]["cycle"].as_u64().unwrap();
    let offsets: Vec<u64> = fx["initial"]["offsets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    let cron = CronCell::new(&ctx, cycle, offsets);
    let count = cron.count_cell();
    let observed = ctx.computed(move |c| count.get(c));
    let _ = observed.get(&ctx);

    for step in steps(&fx) {
        let edge = cron.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "fire edge for {step}");

        let exp = &step["expected"];
        assert_eq!(cron.count(&ctx), exp["count"].as_u64().unwrap());
        assert_eq!(cron.next_fire(), exp["next_fire"].as_u64());

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was_cached, invalidates(step, "count"), "inval {step}");
    }
}

#[test]
fn deadline_expiry() {
    if !spec_fixtures_present() {
        return;
    }
    let fx = load_fixture("deadline_expiry.json");
    let ctx = Context::new();
    let value = fx["initial"]["value"].as_str().unwrap().to_string();
    let deadline = fx["initial"]["deadline"].as_u64().unwrap();
    let d = DeadlineCell::new(&ctx, value.clone(), deadline);
    let expired = d.expired_cell();
    let observed = ctx.computed(move |c| expired.get(c));
    let _ = observed.get(&ctx);

    for step in steps(&fx) {
        let edge = d.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "expiry edge for {step}");

        let exp = &step["expected"];
        let state = d.state(&ctx);
        let want_expired = exp["state"].as_str().unwrap() == "Expired";
        assert_eq!(state.is_expired(), want_expired);
        assert_eq!(state.value(), &value); // value preserved across the flip
        match state {
            Deadlined::Live(v) | Deadlined::Expired(v) => {
                assert_eq!(v, exp["value"].as_str().unwrap())
            }
        }

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was_cached, invalidates(step, "state"), "inval {step}");
    }
}
