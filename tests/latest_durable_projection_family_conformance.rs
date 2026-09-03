//! Canonical latest-durable projection corpus replayed against every Rust graph
//! flavor (`#lzlatestdurableprojection`).

mod common;

use common::Expect;
#[cfg(feature = "async")]
use lazily::{AsyncContext, AsyncLatestDurableProjection};
use lazily::{
    Context, LatestDurableAck, LatestDurableClaim, LatestDurableEnvelope, LatestDurableFailure,
    LatestDurableProjection, LatestDurableReconnect, LatestDurableSnapshot, LatestDurableUpsert,
};
#[cfg(feature = "thread-safe")]
use lazily::{ThreadSafeContext, ThreadSafeLatestDurableProjection};
use serde_json::{Value, json};

const SPEC_DIR: common::SpecDir = common::SpecDir("egress");
const FIXTURE: &str = "latest_durable_projection.json";

trait LatestDurableModel: Sized {
    fn build(generation: u64) -> Self;
    fn upsert_desired(&self, key: String, epoch: u64, value: String) -> LatestDurableUpsert;
    fn claim(&self, key: String, generation: u64) -> LatestDurableClaim<String, String>;
    fn ack_applied(&self, key: String, generation: u64, epoch: u64) -> LatestDurableAck;
    fn fail_retryable(&self, key: String, generation: u64, epoch: u64) -> LatestDurableFailure;
    fn reconnect(&self, generation: u64) -> LatestDurableReconnect;
    fn snapshot(&self) -> LatestDurableSnapshot<String, String>;
    fn state_valid(&self) -> bool;
}

fn load() -> Option<Value> {
    let path = format!("{SPEC_DIR}/{FIXTURE}");
    let raw = common::spec_read_to_string(path).ok()?;
    Some(serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {FIXTURE}: {error}")))
}

fn envelope_value(envelope: &LatestDurableEnvelope<String, String>) -> Value {
    json!({
        "generation": envelope.generation,
        "key": envelope.key,
        "epoch": envelope.epoch,
        "value": envelope.value,
    })
}

fn snapshot_value(snapshot: &LatestDurableSnapshot<String, String>) -> Value {
    let entries: Vec<Value> = snapshot
        .keys
        .iter()
        .map(|(key, state)| {
            let desired = state
                .desired
                .as_ref()
                .map(|revision| json!({ "epoch": revision.epoch, "value": revision.value }));
            let inflight = state.inflight.as_ref().map(envelope_value);
            json!({
                "key": key,
                "desired": desired,
                "inflight": inflight,
                "durable_through": state.durable_through,
            })
        })
        .collect();
    json!({ "generation": snapshot.generation, "entries": entries })
}

fn upsert_value(outcome: LatestDurableUpsert) -> Value {
    match outcome {
        LatestDurableUpsert::Accepted => json!({ "upsert": "accepted" }),
        LatestDurableUpsert::Unchanged => json!({ "upsert": "unchanged" }),
        LatestDurableUpsert::AlreadyDurable { durable_through } => {
            json!({ "upsert": "already_durable", "durable_through": durable_through })
        }
        LatestDurableUpsert::StaleEpoch { current } => {
            json!({ "upsert": "stale_epoch", "current": current })
        }
        LatestDurableUpsert::EpochConflict => json!({ "upsert": "epoch_conflict" }),
    }
}

fn claim_value(outcome: LatestDurableClaim<String, String>) -> Value {
    match outcome {
        LatestDurableClaim::Claimed(envelope) => {
            json!({ "claim": "claimed", "envelope": envelope_value(&envelope) })
        }
        LatestDurableClaim::Empty => json!({ "claim": "empty" }),
        LatestDurableClaim::Busy => json!({ "claim": "busy" }),
        LatestDurableClaim::StaleGeneration { current } => {
            json!({ "claim": "stale_generation", "current": current })
        }
    }
}

fn ack_value(outcome: LatestDurableAck) -> Value {
    match outcome {
        LatestDurableAck::Advanced { durable_through } => {
            json!({ "ack": "advanced", "durable_through": durable_through })
        }
        LatestDurableAck::Unchanged { durable_through } => {
            json!({ "ack": "unchanged", "durable_through": durable_through })
        }
        LatestDurableAck::UnknownEpoch => json!({ "ack": "unknown_epoch" }),
        LatestDurableAck::StaleGeneration { current } => {
            json!({ "ack": "stale_generation", "current": current })
        }
    }
}

fn failure_value(outcome: LatestDurableFailure) -> Value {
    match outcome {
        LatestDurableFailure::Pending => json!({ "failure": "pending" }),
        LatestDurableFailure::Superseded => json!({ "failure": "superseded" }),
        LatestDurableFailure::UnknownEpoch => json!({ "failure": "unknown_epoch" }),
        LatestDurableFailure::StaleGeneration { current } => {
            json!({ "failure": "stale_generation", "current": current })
        }
    }
}

fn reconnect_value(outcome: LatestDurableReconnect) -> Value {
    match outcome {
        LatestDurableReconnect::Advanced {
            generation,
            requeued,
            superseded,
        } => json!({
            "reconnect": "advanced",
            "generation": generation,
            "requeued": requeued,
            "superseded": superseded,
        }),
        LatestDurableReconnect::Unchanged { generation } => {
            json!({ "reconnect": "unchanged", "generation": generation })
        }
        LatestDurableReconnect::StaleGeneration { current } => {
            json!({ "reconnect": "stale_generation", "current": current })
        }
    }
}

fn run_op<Model: LatestDurableModel>(model: &Model, op: &Value) -> Value {
    let key = || op["key"].as_str().expect("key").to_owned();
    match op["type"].as_str().expect("op type") {
        "upsert_desired" => upsert_value(model.upsert_desired(
            key(),
            op["epoch"].as_u64().expect("epoch"),
            op["value"].as_str().expect("value").to_owned(),
        )),
        "claim" => claim_value(model.claim(key(), op["generation"].as_u64().expect("generation"))),
        "ack_applied" => ack_value(model.ack_applied(
            key(),
            op["generation"].as_u64().expect("generation"),
            op["epoch"].as_u64().expect("epoch"),
        )),
        "fail_retryable" => failure_value(model.fail_retryable(
            key(),
            op["generation"].as_u64().expect("generation"),
            op["epoch"].as_u64().expect("epoch"),
        )),
        "reconnect" => {
            reconnect_value(model.reconnect(op["generation"].as_u64().expect("generation")))
        }
        other => panic!("unknown latest-durable projection op `{other}`"),
    }
}

fn assert_state(snapshot: &Value, expected: &Expect, where_: &str) {
    expected.assert_key_at("generation", snapshot["generation"].clone(), where_);
    expected.assert_key_at("entries", snapshot["entries"].clone(), where_);
}

fn replay<Model: LatestDurableModel>(fixture: &Value) -> usize {
    assert_eq!(fixture["kind"].as_str(), Some("LatestDurableProjection"));
    assert_eq!(
        fixture["model"].as_str(),
        Some("LatestDurableProjectionCore")
    );

    let path = format!("{SPEC_DIR}/{FIXTURE}");
    let mut count = 0;
    for (_, id, scenario) in common::scenarios(&path, fixture) {
        let generation = scenario["generation"].as_u64().expect("generation");
        let model = Model::build(generation);
        let mut previous = snapshot_value(&model.snapshot());

        for (index, step) in scenario["steps"]
            .as_array()
            .expect("steps")
            .iter()
            .enumerate()
        {
            assert!(model.state_valid(), "{id} step {index}: precondition");
            let op = &step["op"];
            let where_ = format!("{id} step {index} ({})", op["type"].as_str().unwrap_or("?"));
            let returned = run_op(&model, op);
            assert_eq!(returned, step["returns"], "{where_}: returned outcome");

            let valid_after_transition = model.state_valid();
            let current = snapshot_value(&model.snapshot());
            let expected = Expect::new(
                path.clone(),
                format!("scenarios[{id}].steps[{index}].expected"),
                &step["expected"],
            );
            assert_state(&current, &expected, &where_);
            expected.finish();

            assert_eq!(
                valid_after_transition,
                current == previous,
                "{where_}: snapshot invalidation must match a real transition"
            );
            previous = current;
            count += 1;
        }
    }
    count
}

struct SyncModel {
    ctx: Context,
    projection: LatestDurableProjection<String, String>,
}

impl LatestDurableModel for SyncModel {
    fn build(generation: u64) -> Self {
        let ctx = Context::new();
        let projection = LatestDurableProjection::new(&ctx, generation);
        Self { ctx, projection }
    }

    fn upsert_desired(&self, key: String, epoch: u64, value: String) -> LatestDurableUpsert {
        self.projection.upsert_desired(&self.ctx, key, epoch, value)
    }
    fn claim(&self, key: String, generation: u64) -> LatestDurableClaim<String, String> {
        self.projection.claim(&self.ctx, &key, generation)
    }
    fn ack_applied(&self, key: String, generation: u64, epoch: u64) -> LatestDurableAck {
        self.projection
            .ack_applied(&self.ctx, &key, generation, epoch)
    }
    fn fail_retryable(&self, key: String, generation: u64, epoch: u64) -> LatestDurableFailure {
        self.projection
            .fail_retryable(&self.ctx, &key, generation, epoch)
    }
    fn reconnect(&self, generation: u64) -> LatestDurableReconnect {
        self.projection.reconnect(&self.ctx, generation)
    }
    fn snapshot(&self) -> LatestDurableSnapshot<String, String> {
        self.projection.snapshot(&self.ctx)
    }
    fn state_valid(&self) -> bool {
        self.ctx.is_set(&self.projection.state_handle())
    }
}

#[cfg(feature = "thread-safe")]
struct SharedModel {
    ctx: ThreadSafeContext,
    projection: ThreadSafeLatestDurableProjection<String, String>,
}

#[cfg(feature = "thread-safe")]
impl LatestDurableModel for SharedModel {
    fn build(generation: u64) -> Self {
        let ctx = ThreadSafeContext::new();
        let projection = ThreadSafeLatestDurableProjection::new(&ctx, generation);
        Self { ctx, projection }
    }

    fn upsert_desired(&self, key: String, epoch: u64, value: String) -> LatestDurableUpsert {
        self.projection.upsert_desired(&self.ctx, key, epoch, value)
    }
    fn claim(&self, key: String, generation: u64) -> LatestDurableClaim<String, String> {
        self.projection.claim(&self.ctx, &key, generation)
    }
    fn ack_applied(&self, key: String, generation: u64, epoch: u64) -> LatestDurableAck {
        self.projection
            .ack_applied(&self.ctx, &key, generation, epoch)
    }
    fn fail_retryable(&self, key: String, generation: u64, epoch: u64) -> LatestDurableFailure {
        self.projection
            .fail_retryable(&self.ctx, &key, generation, epoch)
    }
    fn reconnect(&self, generation: u64) -> LatestDurableReconnect {
        self.projection.reconnect(&self.ctx, generation)
    }
    fn snapshot(&self) -> LatestDurableSnapshot<String, String> {
        self.projection.snapshot(&self.ctx)
    }
    fn state_valid(&self) -> bool {
        self.ctx.is_set(&self.projection.state_handle())
    }
}

#[cfg(feature = "async")]
struct AsyncModel {
    ctx: AsyncContext,
    projection: AsyncLatestDurableProjection<String, String>,
}

#[cfg(feature = "async")]
impl LatestDurableModel for AsyncModel {
    fn build(generation: u64) -> Self {
        let ctx = AsyncContext::new();
        let projection = AsyncLatestDurableProjection::new(&ctx, generation);
        Self { ctx, projection }
    }

    fn upsert_desired(&self, key: String, epoch: u64, value: String) -> LatestDurableUpsert {
        self.projection.upsert_desired(&self.ctx, key, epoch, value)
    }
    fn claim(&self, key: String, generation: u64) -> LatestDurableClaim<String, String> {
        self.projection.claim(&self.ctx, &key, generation)
    }
    fn ack_applied(&self, key: String, generation: u64, epoch: u64) -> LatestDurableAck {
        self.projection
            .ack_applied(&self.ctx, &key, generation, epoch)
    }
    fn fail_retryable(&self, key: String, generation: u64, epoch: u64) -> LatestDurableFailure {
        self.projection
            .fail_retryable(&self.ctx, &key, generation, epoch)
    }
    fn reconnect(&self, generation: u64) -> LatestDurableReconnect {
        self.projection.reconnect(&self.ctx, generation)
    }
    fn snapshot(&self) -> LatestDurableSnapshot<String, String> {
        self.projection.snapshot(&self.ctx)
    }
    fn state_valid(&self) -> bool {
        self.ctx.is_set(&self.projection.state_handle())
    }
}

fn expected_steps(fixture: &Value) -> usize {
    fixture["scenarios"]
        .as_array()
        .expect("scenarios")
        .iter()
        .map(|scenario| scenario["steps"].as_array().expect("steps").len())
        .sum()
}

#[test]
fn corpus_is_present_and_non_trivial() {
    let fixture = load().expect("canonical latest-durable projection fixture");
    assert!(expected_steps(&fixture) >= 20);
}

#[test]
fn sync_shell_replays_every_step() {
    let fixture = load().expect("canonical latest-durable projection fixture");
    assert_eq!(replay::<SyncModel>(&fixture), expected_steps(&fixture));
}

#[cfg(feature = "thread-safe")]
#[test]
fn thread_safe_shell_replays_every_step() {
    let fixture = load().expect("canonical latest-durable projection fixture");
    assert_eq!(replay::<SharedModel>(&fixture), expected_steps(&fixture));
}

#[cfg(feature = "async")]
#[test]
fn async_shell_replays_every_step() {
    let fixture = load().expect("canonical latest-durable projection fixture");
    assert_eq!(replay::<AsyncModel>(&fixture), expected_steps(&fixture));
}
