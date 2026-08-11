//! Cross-language conformance for fault-tolerance primitives (`#lzresilience`)
//! — see `lazily-spec/docs/resilience.md` and
//! `lazily-spec/conformance/resilience/*.json`.

mod common;

use common::Expect;
use lazily::{
    BreakerState, BulkheadCell, CircuitBreakerCell, Context, RetryPolicyCell, TimeoutCell,
};
use serde_json::Value;

const SPEC_DIR: common::SpecDir = common::SpecDir("resilience");

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    SPEC_DIR.join("circuit_breaker.json").exists()
}

fn steps(fx: &Value) -> &Vec<Value> {
    fx["steps"].as_array().unwrap()
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
fn circuit_breaker() {
    if !present() {
        return;
    }
    let fx = load("circuit_breaker.json");
    let ctx = Context::new();
    let cfg = &fx["config"];
    let cb = CircuitBreakerCell::new(
        &ctx,
        cfg["window"].as_u64().unwrap() as usize,
        cfg["failure_threshold"].as_u64().unwrap() as usize,
        cfg["reset_timeout"].as_u64().unwrap(),
    );
    let sc = cb.state_cell();
    let observed = ctx.computed(move |c| sc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("circuit_breaker.json", i, step);
        let inv = exp.sub("invalidates");
        let op = &step["op"];
        match op["type"].as_str().unwrap() {
            "record" => cb.record(
                &ctx,
                op["success"].as_bool().unwrap(),
                op["now"].as_u64().unwrap(),
            ),
            "allow" => {
                let got = cb.allow(&ctx, op["now"].as_u64().unwrap());
                assert_eq!(got, step["returns"].as_bool().unwrap(), "allow for {step}");
            }
            other => panic!("unknown op {other}"),
        }
        exp.assert_key_with("state", |want| {
            let want = match want.as_str().unwrap() {
                "Closed" => BreakerState::Closed,
                "Open" => BreakerState::Open,
                "HalfOpen" => BreakerState::HalfOpen,
                s => panic!("bad state {s}"),
            };
            assert_eq!(cb.state(), want, "state for {step}");
        });
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("state", !was, &format!("step {step}"));
    }
}

#[test]
fn retry() {
    if !present() {
        return;
    }
    let fx = load("retry.json");
    let ctx = Context::new();
    let cfg = &fx["config"];
    let r = RetryPolicyCell::new(
        &ctx,
        cfg["base"].as_u64().unwrap(),
        cfg["cap"].as_u64().unwrap(),
    );
    let dc = r.delay_cell();
    let observed = ctx.computed(move |c| dc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("retry.json", i, step);
        let inv = exp.sub("invalidates");
        // `op.type` was unread here while every other resilience runner
        // dispatches on it with a failing catch-all (`#lzscenariobodyskip`):
        // every step replayed as `next_delay` no matter what the fixture named,
        // so a `reset` or `record` op would have advanced the backoff instead.
        let got = match step["op"]["type"].as_str().expect("op type") {
            "next" => r.next_delay(&ctx),
            other => panic!("unknown op {other}"),
        };
        assert_eq!(got, step["returns"].as_u64().unwrap(), "delay for {step}");
        exp.assert_key_at("delay", r.delay(&ctx), &format!("step {step}"));
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("delay", !was, &format!("step {step}"));
    }
}

#[test]
fn bulkhead() {
    if !present() {
        return;
    }
    let fx = load("bulkhead.json");
    let ctx = Context::new();
    let b = BulkheadCell::new(&ctx, fx["config"]["capacity"].as_u64().unwrap());
    let uc = b.permits_in_use_cell();
    let observed = ctx.computed(move |c| uc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("bulkhead.json", i, step);
        let inv = exp.sub("invalidates");
        match step["op"]["type"].as_str().unwrap() {
            "acquire" => assert_eq!(b.acquire(&ctx), step["returns"].as_bool().unwrap()),
            "release" => b.release(&ctx),
            other => panic!("unknown op {other}"),
        }
        exp.assert_key_at("in_use", b.permits_in_use(&ctx), &format!("step {step}"));
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("in_use", !was, &format!("step {step}"));
    }
}

#[test]
fn timeout() {
    if !present() {
        return;
    }
    let fx = load("timeout.json");
    let ctx = Context::new();
    let t = TimeoutCell::new(&ctx);
    let tc = t.is_timed_out_cell();
    let observed = ctx.computed(move |c| tc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("timeout.json", i, step);
        let inv = exp.sub("invalidates");
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        let got = match op["type"].as_str().unwrap() {
            "arm" => {
                t.arm(&ctx, now, op["timeout"].as_u64().unwrap());
                false
            }
            "tick" => t.tick(&ctx, now),
            other => panic!("unknown op {other}"),
        };
        assert_eq!(got, step["returns"].as_bool().unwrap(), "edge for {step}");
        exp.assert_key_at(
            "is_timed_out",
            t.is_timed_out(&ctx),
            &format!("step {step}"),
        );
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("is_timed_out", !was, &format!("step {step}"));
    }
}
