//! Cross-language conformance for the presence + ephemeral plane
//! (`#lzpresence`) — see `lazily-spec/docs/presence.md` and
//! `lazily-spec/conformance/presence/*.json`.

mod common;

use std::collections::BTreeMap;

use lazily::{AwarenessCell, Context, EphemeralCell, PresenceCell};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/presence";

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/presence.json")).exists()
}

fn steps(fx: &Value) -> &Vec<Value> {
    fx["steps"].as_array().unwrap()
}
fn inval(step: &Value, reader: &str) -> bool {
    step["expected"]["invalidates"][reader].as_bool().unwrap()
}

fn want_map(step: &Value) -> BTreeMap<u64, String> {
    step["expected"]["present"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.parse().unwrap(), v.as_str().unwrap().to_string()))
        .collect()
}

#[test]
fn presence() {
    if !present() {
        return;
    }
    let fx = load("presence.json");
    let ctx = Context::new();
    let ttl = fx["config"]["ttl"].as_u64().unwrap();
    let cell = PresenceCell::<u64, String>::new(&ctx, ttl);
    let pc = cell.present_cell();
    let observed = ctx.computed(move |c| pc.get(c));
    let _ = observed.get(&ctx);

    for step in steps(&fx) {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        match op["type"].as_str().unwrap() {
            "heartbeat" => cell.heartbeat(
                &ctx,
                op["peer"].as_u64().unwrap(),
                op["value"].as_str().unwrap().to_string(),
                now,
            ),
            "evict" => cell.evict(&ctx, &op["peer"].as_u64().unwrap(), now),
            "tick" => cell.tick(&ctx, now),
            other => panic!("unknown op {other}"),
        }
        assert_eq!(cell.present(&ctx), want_map(step), "present after {op}");
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "present"), "inval after {op}");
    }
}

#[test]
fn awareness() {
    if !present() {
        return;
    }
    let fx = load("awareness.json");
    let ctx = Context::new();
    let ttl = fx["config"]["ttl"].as_u64().unwrap();
    let cell = AwarenessCell::<u64, String>::new(&ctx, ttl);
    let pc = cell.present_cell();
    let observed = ctx.computed(move |c| pc.get(c));
    let _ = observed.get(&ctx);

    for step in steps(&fx) {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        match op["type"].as_str().unwrap() {
            "set" => cell.set(
                &ctx,
                op["peer"].as_u64().unwrap(),
                op["value"].as_str().unwrap().to_string(),
                now,
            ),
            "tick" => cell.tick(&ctx, now),
            other => panic!("unknown op {other}"),
        }
        assert_eq!(cell.present(&ctx), want_map(step), "present after {op}");
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "present"), "inval after {op}");
    }
}

#[test]
fn ephemeral() {
    if !present() {
        return;
    }
    let fx = load("ephemeral.json");
    let ctx = Context::new();
    let cell = EphemeralCell::<String>::new(&ctx);
    let vc = cell.value_cell();
    let observed = ctx.computed(move |c| vc.get(c));
    let _ = observed.get(&ctx);

    for step in steps(&fx) {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        match op["type"].as_str().unwrap() {
            "set" => cell.set(
                &ctx,
                op["value"].as_str().unwrap().to_string(),
                now,
                op["ttl"].as_u64().unwrap(),
            ),
            "tick" => cell.tick(&ctx, now),
            other => panic!("unknown op {other}"),
        }
        let want = step["expected"]["value"].as_str().map(|s| s.to_string());
        assert_eq!(cell.value(&ctx), want, "value after {op}");
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        assert_eq!(!was, inval(step, "value"), "inval after {op}");
    }
}
