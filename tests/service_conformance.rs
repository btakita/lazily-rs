//! Cross-language conformance for the embedded-service plane (`#lzservice`) —
//! see `lazily-spec/docs/service.md` and
//! `lazily-spec/conformance/service/*.json`.

mod common;

use std::collections::BTreeMap;

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
fn inval(step: &Value, reader: &str) -> bool {
    step["expected"]["invalidates"][reader].as_bool().unwrap()
}
fn want_map(step: &Value, key: &str) -> BTreeMap<String, String> {
    step["expected"][key]
        .as_object()
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

    for step in steps(&fx) {
        let op = &step["op"];
        h.set(
            &ctx,
            op["name"].as_str().unwrap(),
            op["up"].as_bool().unwrap(),
            op["critical"].as_bool().unwrap(),
        );
        let want = match step["expected"]["health"].as_str().unwrap() {
            "Healthy" => Health::Healthy,
            "Degraded" => Health::Degraded,
            "Unhealthy" => Health::Unhealthy,
            s => panic!("bad health {s}"),
        };
        assert_eq!(h.health(), want, "health for {step}");
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "health"), "inval for {step}");
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

    for step in steps(&fx) {
        let op = &step["op"];
        r.set(
            &ctx,
            op["name"].as_str().unwrap(),
            op["ready"].as_bool().unwrap(),
        );
        assert_eq!(r.ready(), step["expected"]["ready"].as_bool().unwrap());
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "ready"), "inval for {step}");
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

    for step in steps(&fx) {
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
        assert_eq!(
            d.discovery(&ctx),
            want_map(step, "discovery"),
            "map for {step}"
        );
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "discovery"), "inval for {step}");
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

    for step in steps(&fx) {
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
        assert_eq!(
            reg.projection(&ctx),
            want_map(step, "projection"),
            "projection for {step}"
        );
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "projection"), "inval for {step}");
    }
}
