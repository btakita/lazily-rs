//! Cross-language conformance for distributed coordination (`#lzcoord`) — see
//! `lazily-spec/docs/coordination.md` and
//! `lazily-spec/conformance/coordination/*.json`.
//!
//! Replays each primitive's op sequence, asserting the returned value, the
//! projected readers, and reader invalidation (via `ctx.is_set`).

mod common;

use common::Expect;
use lazily::{BarrierCell, Context, LeaderCell, LeaderRole, LeaseCell, LockCell, SemaphoreCell};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/coordination";

fn load(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/lease.json")).exists()
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
fn lease() {
    if !present() {
        return;
    }
    let fx = load("lease.json");
    let ctx = Context::new();
    let lease = LeaseCell::<u64>::new(&ctx);
    let hc = lease.holder_cell();
    let observed = ctx.computed(move |c| hc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        match op["type"].as_str().unwrap() {
            "acquire" => {
                let got = lease.acquire(
                    &ctx,
                    op["peer"].as_u64().unwrap(),
                    now,
                    op["ttl"].as_u64().unwrap(),
                );
                assert_eq!(got, step["returns"].as_u64(), "acquire fence");
            }
            "renew" => {
                let got = lease.renew(
                    &ctx,
                    op["peer"].as_u64().unwrap(),
                    now,
                    op["ttl"].as_u64().unwrap(),
                );
                assert_eq!(got, step["returns"].as_bool().unwrap());
            }
            "tick" => {
                let got = lease.tick(&ctx, now);
                assert_eq!(got, step["returns"].as_bool().unwrap());
            }
            other => panic!("unknown op {other}"),
        }
        let exp = expected("lease.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_with("holder", |want| {
            assert_eq!(lease.holder(now), want.as_u64())
        });
        exp.assert_key("held", lease.is_held(now));
        exp.assert_key("fence", lease.fence());

        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("holder", !was, "holder inval");
    }
}

#[test]
fn leader() {
    if !present() {
        return;
    }
    let fx = load("leader.json");
    let ctx = Context::new();
    let me = fx["config"]["me"].as_u64().unwrap();
    let leader = LeaderCell::<u64>::new(&ctx, me);
    let lc = leader.current_leader_cell();
    let observed = ctx.computed(move |c| lc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        let role = match op["type"].as_str().unwrap() {
            "campaign" => leader.campaign(&ctx, now, op["ttl"].as_u64().unwrap()),
            "contend" => leader.contend(
                &ctx,
                op["peer"].as_u64().unwrap(),
                now,
                op["ttl"].as_u64().unwrap(),
            ),
            "tick" => leader.tick(&ctx, now),
            other => panic!("unknown op {other}"),
        };
        let exp = expected("leader.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key_with("role", |want| {
            let want_role = match want.as_str().unwrap() {
                "Leader" => LeaderRole::Leader,
                "Follower" => LeaderRole::Follower,
                "Candidate" => LeaderRole::Candidate,
                r => panic!("bad role {r}"),
            };
            assert_eq!(role, want_role);
        });
        exp.assert_key_with("current_leader", |want| {
            assert_eq!(leader.current_leader(now), want.as_u64())
        });

        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("current_leader", !was, "leader inval");
    }
}

#[test]
fn lock() {
    if !present() {
        return;
    }
    let fx = load("lock.json");
    let ctx = Context::new();
    let lock = LockCell::<u64>::new(&ctx);
    let lc = lock.is_locked_cell();
    let observed = ctx.computed(move |c| lc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        let op = &step["op"];
        let now = op["now"].as_u64().unwrap();
        match op["type"].as_str().unwrap() {
            "acquire" => {
                let got = lock.acquire(
                    &ctx,
                    op["peer"].as_u64().unwrap(),
                    now,
                    op["ttl"].as_u64().unwrap(),
                );
                assert_eq!(got, step["returns"].as_u64());
            }
            "validate" => {
                let got = lock.validate(op["fence"].as_u64().unwrap());
                assert_eq!(got, step["returns"].as_bool().unwrap());
            }
            "tick" => {
                let got = lock.tick(&ctx, now);
                assert_eq!(got, step["returns"].as_bool().unwrap());
            }
            other => panic!("unknown op {other}"),
        }
        let exp = expected("lock.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key("is_locked", lock.is_locked(now));
        exp.assert_key("fence", lock.fence());

        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("is_locked", !was, "lock inval");
    }
}

#[test]
fn semaphore() {
    if !present() {
        return;
    }
    let fx = load("semaphore.json");
    let ctx = Context::new();
    let cap = fx["config"]["capacity"].as_u64().unwrap();
    let sem = SemaphoreCell::new(&ctx, cap);
    let pc = sem.permits_available_cell();
    let observed = ctx.computed(move |c| pc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        match step["op"]["type"].as_str().unwrap() {
            "acquire" => assert_eq!(sem.acquire(&ctx), step["returns"].as_bool().unwrap()),
            "release" => sem.release(&ctx),
            other => panic!("unknown op {other}"),
        }
        let exp = expected("semaphore.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key("permits_available", sem.permits_available(&ctx));

        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("permits_available", !was, "sem inval");
    }
}

#[test]
fn quorum() {
    if !present() {
        return;
    }
    let fx = load("quorum.json");
    let ctx = Context::new();
    let total = fx["config"]["total"].as_u64().unwrap();
    let q = BarrierCell::<u64>::quorum(&ctx, total);
    let oc = q.is_open_cell();
    let observed = ctx.computed(move |c| oc.get(c));
    let _ = observed.get(&ctx);

    for (i, step) in steps(&fx).iter().enumerate() {
        // `op.type` was the one discriminator in this file nobody read: every
        // step replayed as an `arrive` regardless of what the fixture named
        // (`#lzscenariobodyskip`). Every other coordination runner dispatches
        // on it with a failing catch-all; this one did not.
        let op = &step["op"];
        let got = match op["type"].as_str().expect("op type") {
            "vote" => q.arrive(&ctx, op["peer"].as_u64().unwrap()),
            other => panic!("unknown op {other}"),
        };
        assert_eq!(got, step["returns"].as_bool().unwrap());
        let exp = expected("quorum.json", i, step);
        let inv = exp.sub("invalidates");
        exp.assert_key("votes", q.count());
        exp.assert_key("is_open", q.is_open(&ctx));

        let was = ctx.is_set(&observed);
        let _ = observed.get(&ctx);
        inv.assert_key_at("is_open", !was, "quorum inval");
    }
}
