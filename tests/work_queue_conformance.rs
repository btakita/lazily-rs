//! Canonical WorkQueueCell competing-delivery lifecycle fixtures (`#lzworkqueue`).

mod common;

use std::path::Path;

use common::Expect;
use lazily::{Context, WorkQueueCell, WorkQueueDeadLetterReason};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

fn load_fixture(name: &str) -> Option<Value> {
    let path = format!("{SPEC_DIR}/{name}");
    if !Path::new(&path).is_file() {
        eprintln!("skipping: {path} is absent");
        return None;
    }
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {path}: {e}"));
    Some(serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse {path}: {e}")))
}

fn as_u64(value: &Value, label: &str) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("{label} must be u64"))
}

/// Assert the queue's whole observable state against the step's `expected`
/// block. Every record and reader map is guarded (`#lzassertunknownkeys`) — a
/// field the fixture asserts and this runner never reads fails the fixture.
fn assert_state(ctx: &Context, queue: &WorkQueueCell<String>, exp: &Expect) {
    let pending = queue.pending();
    exp.assert_key_with("pending", |expected_pending| {
        let expected_pending = expected_pending.as_array().expect("pending array");
        assert_eq!(pending.len(), expected_pending.len());
        for (j, (actual, want)) in pending.iter().zip(expected_pending).enumerate() {
            let want = exp.nested(format!("pending[{j}]"), want);
            want.assert_key("item_id", actual.item_id);
            want.assert_key("value", actual.value.as_str());
            want.assert_key("attempts", u64::from(actual.attempts));
        }
    });

    let in_flight = queue.in_flight();
    exp.assert_key_with("in_flight", |expected_in_flight| {
        let expected_in_flight = expected_in_flight.as_array().expect("in_flight array");
        assert_eq!(in_flight.len(), expected_in_flight.len());
        for (j, (actual, want)) in in_flight.iter().zip(expected_in_flight).enumerate() {
            let want = exp.nested(format!("in_flight[{j}]"), want);
            want.assert_key("delivery_id", actual.delivery_id);
            want.assert_key("item_id", actual.item_id);
            want.assert_key("value", actual.value.as_str());
            want.assert_key("worker", actual.worker.as_str());
            want.assert_key("attempt", u64::from(actual.attempt));
            want.assert_key("deadline", actual.deadline);
        }
    });

    let dead_letters = queue.dead_letters();
    exp.assert_key_with("dead_letters", |expected_dead_letters| {
        let expected_dead_letters = expected_dead_letters
            .as_array()
            .expect("dead_letters array");
        assert_eq!(dead_letters.len(), expected_dead_letters.len());
        for (j, (actual, want)) in dead_letters.iter().zip(expected_dead_letters).enumerate() {
            let want = exp.nested(format!("dead_letters[{j}]"), want);
            want.assert_key("item_id", actual.item_id);
            want.assert_key("value", actual.value.as_str());
            want.assert_key("attempts", u64::from(actual.attempts));
            let reason = match actual.reason {
                WorkQueueDeadLetterReason::Nack => "nack",
                WorkQueueDeadLetterReason::Expired => "expired",
            };
            want.assert_key("reason", reason);
        }
    });

    let reads = exp.sub("reads");
    reads.assert_key("pending_len", queue.pending_len(ctx) as u64);
    reads.assert_key("is_empty", queue.is_empty(ctx));
    reads.assert_key("in_flight_len", queue.in_flight_len(ctx) as u64);
    reads.assert_key("dead_letter_len", queue.dead_letter_len(ctx) as u64);
}

fn assert_invalidations(ctx: &Context, queue: &WorkQueueCell<String>, exp: &Expect) {
    let handles = queue.reader_handles();
    let invalidates = exp.sub("invalidates");
    invalidates.assert_key_at(
        "pending_len",
        !ctx.is_set(&handles.pending_len),
        "pending invalidation",
    );
    invalidates.assert_key_at(
        "is_empty",
        !ctx.is_set(&handles.is_empty),
        "empty invalidation",
    );
    invalidates.assert_key_at(
        "in_flight_len",
        !ctx.is_set(&handles.in_flight_len),
        "in-flight invalidation",
    );
    invalidates.assert_key_at(
        "dead_letter_len",
        !ctx.is_set(&handles.dead_letter_len),
        "dead-letter invalidation",
    );
}

/// `returns` for a `claim` is a delivery record — an assertion block in its own
/// right, so it is guarded too.
fn assert_delivery(actual: &lazily::WorkQueueDelivery<String>, expected: &Expect) {
    expected.assert_key("delivery_id", actual.delivery_id);
    expected.assert_key("item_id", actual.item_id);
    expected.assert_key("value", actual.value.as_str());
    expected.assert_key("worker", actual.worker.as_str());
    expected.assert_key("attempt", u64::from(actual.attempt));
    expected.assert_key("deadline", actual.deadline);
}

fn run_fixture(name: &str) {
    let Some(fixture) = load_fixture(name) else {
        return;
    };
    let initial = &fixture["initial"];
    let ctx = Context::new();
    let queue = WorkQueueCell::<String>::new(
        &ctx,
        as_u64(&initial["visibility_timeout"], "initial.visibility_timeout"),
        as_u64(&initial["max_deliveries"], "initial.max_deliveries") as u32,
    );
    assert!(
        initial["pending"]
            .as_array()
            .expect("initial pending")
            .is_empty()
    );

    for (i, step) in fixture["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .enumerate()
    {
        // Every reader is materialized before the mutation so fixture
        // invalidation expectations are observable through Context::is_set.
        let _ = queue.pending_len(&ctx);
        let _ = queue.is_empty(&ctx);
        let _ = queue.in_flight_len(&ctx);
        let _ = queue.dead_letter_len(&ctx);

        let op = &step["op"];
        match op["type"].as_str().expect("op type") {
            "push" => {
                let actual = queue.push(&ctx, op["value"].as_str().expect("value").to_owned());
                assert_eq!(actual, as_u64(&step["returns"], "push return"));
            }
            "claim" => {
                let actual = queue.claim(
                    &ctx,
                    op["worker"].as_str().expect("worker").to_owned(),
                    as_u64(&op["now"], "now"),
                );
                if step["returns"].is_null() {
                    assert!(actual.is_none());
                } else {
                    let want = Expect::new(
                        format!("{SPEC_DIR}/{name}"),
                        format!("steps[{i}].returns"),
                        &step["returns"],
                    );
                    assert_delivery(&actual.expect("delivery"), &want);
                }
            }
            "ack" => {
                let actual = queue.ack(
                    &ctx,
                    &op["worker"].as_str().expect("worker").to_owned(),
                    as_u64(&op["delivery_id"], "delivery_id"),
                );
                assert_eq!(actual, step["returns"].as_bool().expect("ack return"));
            }
            "nack" => {
                let actual = queue.nack(
                    &ctx,
                    &op["worker"].as_str().expect("worker").to_owned(),
                    as_u64(&op["delivery_id"], "delivery_id"),
                );
                assert_eq!(actual, step["returns"].as_bool().expect("nack return"));
            }
            "reap_expired" => {
                let actual = queue.reap_expired(&ctx, as_u64(&op["now"], "now"));
                assert_eq!(actual as u64, as_u64(&step["returns"], "reap return"));
            }
            other => panic!("unknown WorkQueueCell op {other}"),
        }

        let exp = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("steps[{i}].expected"),
            &step["expected"],
        );
        assert_invalidations(&ctx, &queue, &exp);
        assert_state(&ctx, &queue, &exp);
    }
}

#[test]
fn competing_delivery_fixture() {
    run_fixture("workqueue_competing_delivery.json");
}

#[test]
fn lease_deadletter_fixture() {
    run_fixture("workqueue_lease_deadletter.json");
}
