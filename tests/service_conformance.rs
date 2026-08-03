//! Cross-language conformance for the embedded-service plane (`#lzservice`) —
//! see `lazily-spec/docs/service.md` and
//! `lazily-spec/conformance/service/*.json`.

mod common;

use std::collections::BTreeMap;

use common::Expect;
use lazily::{Context, DiscoveryCell, Health, HealthCell, ReadinessCell, ServiceRegistry};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/service";

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/health.json")).exists()
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
/// A service->endpoint projection. Its keys are service names — data, not
/// assertion names — so it is compared wholesale rather than descended into.
fn want_map(want: &Value) -> BTreeMap<String, String> {
    want.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
        .collect()
}

#[test]
fn health() {
    if !present() {
        return;
    }
    let fx = load("health.json");
    let ctx = Context::new();
    let h = HealthCell::new(&ctx);
    let hc = h.health_cell();
    let observed = ctx.computed(move |c| hc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("health.json", i, step);
        let inv = exp.sub("invalidates");
        let op = &step["op"];
        // `op.type` was unread here and in `readiness`, while `discovery` and
        // `service_registry` dispatch on it with a failing catch-all
        // (`#lzscenariobodyskip`): every step replayed as a `set` regardless of
        // what the fixture named, so a future `evict`/`clear` op would have
        // registered a component instead and the `expected` block would have
        // been compared against the wrong aggregate.
        match op["type"].as_str().unwrap() {
            "set" => h.set(
                &ctx,
                op["name"].as_str().unwrap(),
                op["up"].as_bool().unwrap(),
                op["critical"].as_bool().unwrap(),
            ),
            other => panic!("unknown op {other}"),
        }
        exp.assert_key_with("health", |want| {
            let want = match want.as_str().unwrap() {
                "Healthy" => Health::Healthy,
                "Degraded" => Health::Degraded,
                "Unhealthy" => Health::Unhealthy,
                s => panic!("bad health {s}"),
            };
            assert_eq!(h.health(), want, "health for {step}");
        });
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("health", !was, &format!("step {step}"));
    }
}

#[test]
fn readiness() {
    if !present() {
        return;
    }
    let fx = load("readiness.json");
    let ctx = Context::new();
    let r = ReadinessCell::new(&ctx);
    let rc = r.ready_cell();
    let observed = ctx.computed(move |c| rc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("readiness.json", i, step);
        let inv = exp.sub("invalidates");
        let op = &step["op"];
        match op["type"].as_str().unwrap() {
            "set" => r.set(
                &ctx,
                op["name"].as_str().unwrap(),
                op["ready"].as_bool().unwrap(),
            ),
            other => panic!("unknown op {other}"),
        }
        exp.assert_key_at("ready", r.ready(), &format!("step {step}"));
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("ready", !was, &format!("step {step}"));
    }
}

#[test]
fn discovery() {
    if !present() {
        return;
    }
    let fx = load("discovery.json");
    let ctx = Context::new();
    let d = DiscoveryCell::<u64>::new(&ctx);
    let dc = d.discovery_cell();
    let observed = ctx.computed(move |c| dc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("discovery.json", i, step);
        let inv = exp.sub("invalidates");
        let op = &step["op"];
        match op["type"].as_str().unwrap() {
            "register" => d.register(
                &ctx,
                op["service"].as_str().unwrap(),
                op["endpoint"].as_str().unwrap(),
                op["peer"].as_u64().unwrap(),
            ),
            "deregister" => d.deregister(&ctx, op["service"].as_str().unwrap()),
            "evict" => d.evict(&ctx, &op["peer"].as_u64().unwrap()),
            "resolve" => {
                let got = d.resolve(op["service"].as_str().unwrap());
                assert_eq!(got.as_deref(), step["returns"].as_str());
            }
            other => panic!("unknown op {other}"),
        }
        exp.assert_key_with("discovery", |want| {
            assert_eq!(d.discovery(&ctx), want_map(want), "map for {step}");
        });
        // The comparison above is already a whole-map equality in both
        // directions, but the tracker cannot see inside the closure; the KEY SET
        // is stated explicitly so the finish-time guard is satisfied
        // structurally rather than by inspection (`#lzsubblockkeyset`).
        exp.assert_key_set("discovery", d.discovery(&ctx).into_keys());
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("discovery", !was, &format!("step {step}"));
    }
}

#[test]
fn service_registry() {
    if !present() {
        return;
    }
    let fx = load("service_registry.json");
    let ctx = Context::new();
    let reg = ServiceRegistry::new(&ctx);
    let pc = reg.projection_cell();
    let observed = ctx.computed(move |c| pc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let exp = expected("service_registry.json", i, step);
        let inv = exp.sub("invalidates");
        let op = &step["op"];
        match op["type"].as_str().unwrap() {
            "register" => reg.register(
                &ctx,
                op["service"].as_str().unwrap(),
                op["endpoint"].as_str().unwrap(),
            ),
            "deregister" => reg.deregister(&ctx, op["service"].as_str().unwrap()),
            "replay" => reg.replay(&ctx),
            other => panic!("unknown op {other}"),
        }
        exp.assert_key_with("projection", |want| {
            assert_eq!(
                reg.projection(&ctx),
                want_map(want),
                "projection for {step}"
            );
        });
        // The comparison above is already a whole-map equality in both
        // directions, but the tracker cannot see inside the closure; the KEY SET
        // is stated explicitly so the finish-time guard is satisfied
        // structurally rather than by inspection (`#lzsubblockkeyset`).
        exp.assert_key_set("projection", reg.projection(&ctx).into_keys());
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("projection", !was, &format!("step {step}"));
    }
}
