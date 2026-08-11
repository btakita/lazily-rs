//! Cross-language conformance for the presence + ephemeral plane
//! (`#lzpresence`) — see `lazily-spec/docs/presence.md` and
//! `lazily-spec/conformance/presence/*.json`.

mod common;

use std::collections::BTreeMap;

use common::Expect;
use lazily::{AwarenessCell, Context, EphemeralCell, PresenceCell};
use serde_json::Value;

const SPEC_DIR: common::SpecDir = common::SpecDir("presence");

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
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

fn present() -> bool {
    SPEC_DIR.join("presence.json").exists()
}

fn steps(fx: &Value) -> &Vec<Value> {
    fx["steps"].as_array().unwrap()
}
/// The live peer->value map. `present` is *data* — its keys are peer ids, not
/// assertion names — so it is compared wholesale rather than descended into.
fn want_map(want: &Value) -> BTreeMap<u64, String> {
    want.as_object()
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

    for (i, step) in steps(&fx).iter().enumerate() {
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
        let exp = expected("presence.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_with("present", |want| {
            assert_eq!(cell.present(&ctx), want_map(want), "present after {op}")
        });
        // The comparison above is already a whole-map equality in both
        // directions, but the tracker cannot see inside the closure; the KEY SET
        // is stated explicitly so the finish-time guard is satisfied
        // structurally rather than by inspection (`#lzsubblockkeyset`).
        exp.assert_key_set("present", cell.present(&ctx).keys().map(u64::to_string));
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("present", !was, &format!("op {op}"));
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

    for (i, step) in steps(&fx).iter().enumerate() {
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
        let exp = expected("awareness.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_with("present", |want| {
            assert_eq!(cell.present(&ctx), want_map(want), "present after {op}")
        });
        // The comparison above is already a whole-map equality in both
        // directions, but the tracker cannot see inside the closure; the KEY SET
        // is stated explicitly so the finish-time guard is satisfied
        // structurally rather than by inspection (`#lzsubblockkeyset`).
        exp.assert_key_set("present", cell.present(&ctx).keys().map(u64::to_string));
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("present", !was, &format!("op {op}"));
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

    for (i, step) in steps(&fx).iter().enumerate() {
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
        let exp = expected("ephemeral.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_with("value", |want| {
            let want = want.as_str().map(|s| s.to_string());
            assert_eq!(cell.value(&ctx), want, "value after {op}");
        });
        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("value", !was, &format!("op {op}"));
    }
}
