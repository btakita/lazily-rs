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

use common::Expect;
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

/// Guard one step's `expected` block (`#lzassertunknownkeys`): a key this runner
/// never reads fails the fixture instead of passing unnoticed.
fn expected<'a>(name: &str, i: usize, step: &'a Value) -> Expect<'a> {
    Expect::new(
        format!("{SPEC_DIR}/{name}"),
        format!("steps[{i}].expected"),
        &step["expected"],
    )
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

    for (i, step) in steps(&fx).iter().enumerate() {
        let edge = timer.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "fire edge for {step}");

        let exp = expected("timer_single_shot.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_at("fired", timer.has_fired(&ctx), &format!("step {step}"));
        exp.assert_key_with("value", |want| match want.as_str() {
            Some("()") => assert_eq!(timer.value(&ctx), Some(())),
            _ => assert_eq!(timer.value(&ctx), None),
        });
        exp.assert_key_with("next_fire", |want| {
            assert_eq!(timer.next_fire(), want.as_u64())
        });

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        // The reader must have been invalidated (cache dropped) exactly when the
        // fixture says so.
        inv.assert_key_at("fired", !was_cached, &format!("step {step}"));
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

    for (i, step) in steps(&fx).iter().enumerate() {
        let edge = iv.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "fire edge for {step}");

        let exp = expected("interval_periodic.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_at("count", iv.count(&ctx), &format!("step {step}"));
        exp.assert_key_with("next_fire", |want| {
            assert_eq!(iv.next_fire(), want.as_u64())
        });

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("count", !was_cached, &format!("step {step}"));
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

    for (i, step) in steps(&fx).iter().enumerate() {
        let edge = cron.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "fire edge for {step}");

        let exp = expected("cron_pattern.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_at("count", cron.count(&ctx), &format!("step {step}"));
        exp.assert_key_with("next_fire", |want| {
            assert_eq!(cron.next_fire(), want.as_u64())
        });

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("count", !was_cached, &format!("step {step}"));
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

    for (i, step) in steps(&fx).iter().enumerate() {
        let edge = d.tick(&ctx, now_of(step));
        assert_eq!(edge, edge_of(step), "expiry edge for {step}");

        let exp = expected("deadline_expiry.json", i, step);
        let inv = exp.sub("invalidates");
        let state = d.state(&ctx);
        exp.assert_key_with("state", |want| {
            let want_expired = want.as_str().unwrap() == "Expired";
            assert_eq!(state.is_expired(), want_expired);
        });
        assert_eq!(state.value(), &value); // value preserved across the flip
        exp.assert_key_with("value", |want| match &state {
            Deadlined::Live(v) | Deadlined::Expired(v) => {
                assert_eq!(v, want.as_str().unwrap())
            }
        });

        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("state", !was_cached, &format!("step {step}"));
    }
}
