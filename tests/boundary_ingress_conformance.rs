//! Canonical boundary-ingress adapter replay across every Rust graph flavor.

mod common;

use std::path::Path;

#[cfg(feature = "async")]
use lazily::{AsyncBoundaryIngressCell, AsyncContext};
use lazily::{
    BoundaryDeliveryReceipt, BoundaryFreshness, BoundaryIngressCell, BoundaryIngressConfig,
    BoundaryIngressEvent, BoundaryIngressPayload, BoundaryIngressPhase, BoundaryIngressProjection,
    BoundaryIngressReadiness, BoundaryIngressSnapshot, BoundaryValidation, Context,
    IngressEnvelope, IngressPolicy, KeepLatest,
};
#[cfg(feature = "thread-safe")]
use lazily::{ThreadSafeBoundaryIngressCell, ThreadSafeContext};
use serde_json::Value;

const FIXTURE: &str = "../lazily-spec/conformance/ingress/boundary_ingress_adapter.json";

trait Model: Sized {
    fn build(config: BoundaryIngressConfig) -> Self;
    fn subscribe(&self, generation: u64);
    fn snapshot(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        source_keys: Vec<String>,
        members: Vec<String>,
        validation: BoundaryValidation,
    );
    fn event(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        action: &str,
        key: Option<String>,
        validation: Option<BoundaryValidation>,
    );
    fn member_join(&self, member: &str);
    fn member_leave(&self, member: &str);
    fn open_delivery(&self, receipt_id: &str);
    fn acknowledge(&self, receipt_id: &str, member: &str);
    fn tick(&self, now: u64);
    fn projection(&self) -> BoundaryIngressProjection<String>;
}

struct SyncModel {
    ctx: Context,
    adapter: BoundaryIngressCell<String, u64, KeepLatest>,
}

impl Model for SyncModel {
    fn build(config: BoundaryIngressConfig) -> Self {
        let ctx = Context::new();
        let adapter = BoundaryIngressCell::new(&ctx, config, IngressPolicy::default()).unwrap();
        Self { ctx, adapter }
    }

    fn subscribe(&self, generation: u64) {
        self.adapter.subscribe(&self.ctx, generation);
    }

    fn snapshot(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        source_keys: Vec<String>,
        members: Vec<String>,
        validation: BoundaryValidation,
    ) {
        self.adapter.apply_snapshot(
            &self.ctx,
            snapshot(
                generation,
                cursor,
                stamped_at,
                source_keys,
                members,
                validation,
            ),
        );
    }

    fn event(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        action: &str,
        key: Option<String>,
        validation: Option<BoundaryValidation>,
    ) {
        self.adapter.apply_event(
            &self.ctx,
            event(generation, cursor, stamped_at, action, key, validation),
        );
    }

    fn member_join(&self, member: &str) {
        self.adapter.member_join(&self.ctx, member);
    }

    fn member_leave(&self, member: &str) {
        self.adapter.member_leave(&self.ctx, member);
    }

    fn open_delivery(&self, receipt_id: &str) {
        self.adapter.open_delivery(&self.ctx, receipt_id);
    }

    fn acknowledge(&self, receipt_id: &str, member: &str) {
        self.adapter.acknowledge(&self.ctx, receipt_id, member);
    }

    fn tick(&self, now: u64) {
        self.adapter.tick(&self.ctx, now);
    }

    fn projection(&self) -> BoundaryIngressProjection<String> {
        self.adapter.projection(&self.ctx)
    }
}

#[cfg(feature = "thread-safe")]
struct ThreadSafeModel {
    ctx: ThreadSafeContext,
    adapter: ThreadSafeBoundaryIngressCell<String, u64, KeepLatest>,
}

#[cfg(feature = "thread-safe")]
impl Model for ThreadSafeModel {
    fn build(config: BoundaryIngressConfig) -> Self {
        let ctx = ThreadSafeContext::new();
        let adapter =
            ThreadSafeBoundaryIngressCell::new(&ctx, config, IngressPolicy::default()).unwrap();
        Self { ctx, adapter }
    }

    fn subscribe(&self, generation: u64) {
        self.adapter.subscribe(&self.ctx, generation);
    }

    fn snapshot(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        source_keys: Vec<String>,
        members: Vec<String>,
        validation: BoundaryValidation,
    ) {
        self.adapter.apply_snapshot(
            &self.ctx,
            snapshot(
                generation,
                cursor,
                stamped_at,
                source_keys,
                members,
                validation,
            ),
        );
    }

    fn event(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        action: &str,
        key: Option<String>,
        validation: Option<BoundaryValidation>,
    ) {
        self.adapter.apply_event(
            &self.ctx,
            event(generation, cursor, stamped_at, action, key, validation),
        );
    }

    fn member_join(&self, member: &str) {
        self.adapter.member_join(&self.ctx, member);
    }

    fn member_leave(&self, member: &str) {
        self.adapter.member_leave(&self.ctx, member);
    }

    fn open_delivery(&self, receipt_id: &str) {
        self.adapter.open_delivery(&self.ctx, receipt_id);
    }

    fn acknowledge(&self, receipt_id: &str, member: &str) {
        self.adapter.acknowledge(&self.ctx, receipt_id, member);
    }

    fn tick(&self, now: u64) {
        self.adapter.tick(&self.ctx, now);
    }

    fn projection(&self) -> BoundaryIngressProjection<String> {
        self.adapter.projection(&self.ctx)
    }
}

#[cfg(feature = "async")]
struct AsyncModel {
    ctx: AsyncContext,
    adapter: AsyncBoundaryIngressCell<String, u64, KeepLatest>,
}

#[cfg(feature = "async")]
impl Model for AsyncModel {
    fn build(config: BoundaryIngressConfig) -> Self {
        let ctx = AsyncContext::new();
        let adapter =
            AsyncBoundaryIngressCell::new(&ctx, config, IngressPolicy::default()).unwrap();
        Self { ctx, adapter }
    }

    fn subscribe(&self, generation: u64) {
        self.adapter.subscribe(&self.ctx, generation);
    }

    fn snapshot(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        source_keys: Vec<String>,
        members: Vec<String>,
        validation: BoundaryValidation,
    ) {
        self.adapter.apply_snapshot(
            &self.ctx,
            snapshot(
                generation,
                cursor,
                stamped_at,
                source_keys,
                members,
                validation,
            ),
        );
    }

    fn event(
        &self,
        generation: u64,
        cursor: u64,
        stamped_at: u64,
        action: &str,
        key: Option<String>,
        validation: Option<BoundaryValidation>,
    ) {
        self.adapter.apply_event(
            &self.ctx,
            event(generation, cursor, stamped_at, action, key, validation),
        );
    }

    fn member_join(&self, member: &str) {
        self.adapter.member_join(&self.ctx, member);
    }

    fn member_leave(&self, member: &str) {
        self.adapter.member_leave(&self.ctx, member);
    }

    fn open_delivery(&self, receipt_id: &str) {
        self.adapter.open_delivery(&self.ctx, receipt_id);
    }

    fn acknowledge(&self, receipt_id: &str, member: &str) {
        self.adapter.acknowledge(&self.ctx, receipt_id, member);
    }

    fn tick(&self, now: u64) {
        self.adapter.tick(&self.ctx, now);
    }

    fn projection(&self) -> BoundaryIngressProjection<String> {
        self.adapter.projection(&self.ctx)
    }
}

fn snapshot(
    generation: u64,
    cursor: u64,
    stamped_at: u64,
    source_keys: Vec<String>,
    members: Vec<String>,
    validation: BoundaryValidation,
) -> BoundaryIngressSnapshot<String, u64> {
    BoundaryIngressSnapshot {
        generation,
        cursor,
        stamped_at,
        entries: source_keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| {
                IngressEnvelope::new(key, generation, 0, stamped_at, index as u64 + 1)
            })
            .collect(),
        members,
        validation,
    }
}

fn event(
    generation: u64,
    cursor: u64,
    stamped_at: u64,
    action: &str,
    key: Option<String>,
    validation: Option<BoundaryValidation>,
) -> BoundaryIngressEvent<String, u64> {
    let payload = match action {
        "upsert" => BoundaryIngressPayload::Upsert(IngressEnvelope::new(
            key.expect("upsert key"),
            generation,
            0,
            stamped_at,
            cursor,
        )),
        "remove" => BoundaryIngressPayload::Remove(key.expect("remove key")),
        "validate" => BoundaryIngressPayload::Validate(validation.expect("validation")),
        other => panic!("unknown boundary action {other}"),
    };
    BoundaryIngressEvent {
        generation,
        cursor,
        stamped_at,
        payload,
    }
}

fn validation(value: &str) -> BoundaryValidation {
    match value {
        "valid" => BoundaryValidation::Valid,
        "invalid" => BoundaryValidation::Invalid,
        other => panic!("unknown validation {other}"),
    }
}

fn phase(value: BoundaryIngressPhase) -> &'static str {
    match value {
        BoundaryIngressPhase::Detached => "detached",
        BoundaryIngressPhase::Bootstrapping => "bootstrapping",
        BoundaryIngressPhase::Live => "live",
        BoundaryIngressPhase::ReplayRequired => "replay_required",
        BoundaryIngressPhase::Backpressured => "backpressured",
        BoundaryIngressPhase::Invalid => "invalid",
    }
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item.as_str().expect("string").to_string())
        .collect()
}

fn u64s(value: &Value) -> Vec<u64> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item.as_u64().expect("integer"))
        .collect()
}

fn assert_delivery(actual: Option<&BoundaryDeliveryReceipt>, expected: &Value, where_: &str) {
    let actual = actual.expect("active delivery");
    let object = expected.as_object().expect("delivery object");
    for (key, value) in object {
        match key.as_str() {
            "receipt_id" => assert_eq!(
                actual.receipt_id,
                value.as_str().expect("receipt id"),
                "{where_}"
            ),
            "targets" => assert_eq!(
                actual.targets.iter().cloned().collect::<Vec<_>>(),
                strings(value),
                "{where_}"
            ),
            "acked" => assert_eq!(
                actual.acknowledged.iter().cloned().collect::<Vec<_>>(),
                strings(value),
                "{where_}"
            ),
            "converged" => {
                assert_eq!(
                    actual.converged(),
                    value.as_bool().expect("bool"),
                    "{where_}"
                )
            }
            other => panic!("{where_}: unknown delivery assertion {other}"),
        }
    }
}

fn assert_expected(actual: &BoundaryIngressProjection<String>, expected: &Value, where_: &str) {
    let object = expected.as_object().expect("expected object");
    for (key, value) in object {
        match key.as_str() {
            "phase" => assert_eq!(
                phase(actual.phase),
                value.as_str().expect("phase"),
                "{where_}"
            ),
            "generation" => {
                assert_eq!(
                    actual.generation,
                    value.as_u64().expect("generation"),
                    "{where_}"
                )
            }
            "cursor" => assert_eq!(actual.cursor, value.as_u64(), "{where_}"),
            "buffered_cursors" => {
                assert_eq!(actual.buffered_cursors, u64s(value), "{where_}")
            }
            "source_keys" => assert_eq!(actual.source_keys, strings(value), "{where_}"),
            "members" => assert_eq!(actual.members, strings(value), "{where_}"),
            "validation" => assert_eq!(
                actual.validation,
                validation(value.as_str().expect("validation")),
                "{where_}"
            ),
            "replay_from" => assert_eq!(actual.replay_from, value.as_u64(), "{where_}"),
            "stale_events" => assert_eq!(
                actual.stale_events,
                value.as_u64().expect("stale events"),
                "{where_}"
            ),
            "delivery" => assert_delivery(actual.active_delivery.as_ref(), value, where_),
            "ready" => assert_eq!(
                actual.readiness() == BoundaryIngressReadiness::Ready,
                value.as_bool().expect("ready"),
                "{where_}"
            ),
            "fresh" => assert_eq!(
                actual.freshness == BoundaryFreshness::Fresh,
                value.as_bool().expect("fresh"),
                "{where_}"
            ),
            "observation_revision" | "revision" => assert_eq!(
                actual.revision,
                value.as_u64().expect("revision"),
                "{where_}"
            ),
            other => panic!("{where_}: unknown boundary assertion {other}"),
        }
    }
}

fn replay<M: Model>() -> usize {
    let raw = common::spec_read_to_string(Path::new(FIXTURE)).expect("boundary fixture");
    let fixture: Value = serde_json::from_str(&raw).expect("valid fixture");
    let base_policy = &fixture["policy"];
    let mut count = 0;
    for scenario in fixture["scenarios"].as_array().expect("scenarios") {
        let max_buffered = scenario
            .get("policy")
            .and_then(|policy| policy.get("max_buffered"))
            .unwrap_or(&base_policy["max_buffered"])
            .as_u64()
            .expect("max buffered") as usize;
        let model = M::build(BoundaryIngressConfig {
            max_buffered,
            freshness_horizon: base_policy["freshness_horizon"]
                .as_u64()
                .expect("freshness horizon"),
        });
        let id = scenario["id"].as_str().expect("scenario id");
        common::record_scenario(Path::new(FIXTURE), id, common::ScenarioIdSource::Id);
        for (index, step) in scenario["steps"]
            .as_array()
            .expect("steps")
            .iter()
            .enumerate()
        {
            let op = &step["op"];
            match op["type"].as_str().expect("op type") {
                "subscribe" => model.subscribe(op["generation"].as_u64().expect("generation")),
                "snapshot" => model.snapshot(
                    op["generation"].as_u64().expect("generation"),
                    op["cursor"].as_u64().expect("cursor"),
                    op["stamped_at"].as_u64().expect("stamp"),
                    strings(&op["source_keys"]),
                    strings(&op["members"]),
                    validation(op["validation"].as_str().expect("validation")),
                ),
                "event" => model.event(
                    op["generation"].as_u64().expect("generation"),
                    op["cursor"].as_u64().expect("cursor"),
                    op["stamped_at"].as_u64().expect("stamp"),
                    op["action"].as_str().expect("action"),
                    op.get("key").and_then(Value::as_str).map(ToOwned::to_owned),
                    op.get("validation").and_then(Value::as_str).map(validation),
                ),
                "member_join" => model.member_join(op["member"].as_str().expect("member")),
                "member_leave" => model.member_leave(op["member"].as_str().expect("member")),
                "open_receipt" => {
                    model.open_delivery(op["receipt_id"].as_str().expect("receipt id"))
                }
                "ack" => model.acknowledge(
                    op["receipt_id"].as_str().expect("receipt id"),
                    op["member"].as_str().expect("member"),
                ),
                "tick" => model.tick(op["now"].as_u64().expect("now")),
                other => panic!("{id} step {index}: unknown op {other}"),
            }
            assert_expected(
                &model.projection(),
                &step["expected"],
                &format!("{id} step {index}"),
            );
            count += 1;
        }
    }
    count
}

#[test]
fn sync_boundary_ingress_replays_canonical_corpus() {
    assert!(replay::<SyncModel>() > 0);
}

#[cfg(feature = "thread-safe")]
#[test]
fn thread_safe_boundary_ingress_replays_canonical_corpus() {
    assert!(replay::<ThreadSafeModel>() > 0);
}

#[cfg(feature = "async")]
#[test]
fn async_boundary_ingress_replays_canonical_corpus() {
    assert!(replay::<AsyncModel>() > 0);
}
