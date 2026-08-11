//! Cross-language conformance for membership + failure detection (`#lzmemb`) —
//! see `lazily-spec/docs/membership.md` and
//! `lazily-spec/conformance/membership/membership_lifecycle.json`.
//!
//! Replays the SWIM lifecycle: each op asserts the acted peers' `state`, the
//! `alive_set` (the reactive `PeerSet`), and that the `PeerSet` reader
//! invalidates exactly when the alive set changes (via `ctx.is_set`).

mod common;

use std::collections::BTreeSet;

use common::Expect;
use lazily::{Context, MembershipCell, MembershipConfig, PeerState};
use serde_json::Value;

const SPEC_DIR: common::SpecDir = common::SpecDir("membership");

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    SPEC_DIR.join("membership_lifecycle.json").exists()
}

fn state_name(s: PeerState) -> &'static str {
    match s {
        PeerState::Alive => "Alive",
        PeerState::Suspect => "Suspect",
        PeerState::Dead => "Dead",
        PeerState::Left => "Left",
    }
}

#[test]
fn membership_lifecycle() {
    if !present() {
        return;
    }
    let fx = load("membership_lifecycle.json");
    let cfg = &fx["config"];
    let config = MembershipConfig {
        phi_threshold: cfg["phi_threshold"].as_f64().unwrap(),
        suspect_timeout: cfg["suspect_timeout"].as_u64().unwrap(),
        max_samples: cfg["max_samples"].as_u64().unwrap() as usize,
        min_std: cfg["min_std"].as_f64().unwrap(),
    };
    let ctx = Context::new();
    let m = MembershipCell::<u64>::new(&ctx, config);
    let set = m.peer_set_cell();
    let observed = ctx.computed(move |c| set.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in fx["steps"].as_array().unwrap().iter().enumerate() {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        match op["type"].as_str().unwrap() {
            "join" => {
                m.join(&ctx, op["peer"].as_u64().unwrap(), now);
            }
            "heartbeat" => {
                m.heartbeat(&ctx, op["peer"].as_u64().unwrap(), now);
            }
            "leave" => {
                m.leave(&ctx, op["peer"].as_u64().unwrap(), now);
            }
            "tick" => {
                m.tick(&ctx, now);
            }
            other => panic!("unknown op {other}"),
        }

        // Guard the `expected` block (`#lzassertunknownkeys`): a key this runner
        // never reads fails the fixture instead of passing unnoticed.
        let exp = Expect::new(
            format!("{SPEC_DIR}/membership_lifecycle.json"),
            format!("steps[{i}].expected"),
            &step["expected"],
        );
        // Per-peer state. The keys are peer ids — data, not assertion names —
        // but the map is still an OBJECT value, so it is consumed by DESCENT
        // (`#lzsubblockkeyset`): a peer the corpus adds fails as an unconsumed
        // key instead of being compared by nothing.
        let states = exp.sub("states");
        for peer in states.raw().as_object().unwrap().keys() {
            let id: u64 = peer.parse().unwrap();
            states.assert_key_at(
                peer.as_str(),
                m.state(&id).map(state_name).unwrap_or("<absent>"),
                &format!("state of peer {id} after {op}"),
            );
        }
        states.finish();
        // Alive set.
        exp.assert_key_with("alive_set", |want| {
            let want_set: BTreeSet<u64> = want
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .collect();
            assert_eq!(m.peer_set(&ctx), want_set, "alive_set after {op}");
        });

        // PeerSet invalidation.
        let was_cached = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        exp.assert_key_at("invalidates", !was_cached, &format!("op {op}"));
    }
}
