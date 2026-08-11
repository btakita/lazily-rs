//! Canonical egress corpus replayed against every Rust graph flavor
//! (`#lzegress`).
//!
//! The fixture owns transition and invalidation expectations. Each shell runs
//! the same steps and proves both positive and negative invalidation; the
//! transport Effect has separate unit coverage because the corpus describes the
//! delivery authority beneath any concrete transport.

mod common;

use common::Expect;
#[cfg(feature = "async")]
use lazily::{AsyncContext, AsyncEgressCell};
use lazily::{
    Context, EgressAck, EgressCell, EgressClaim, EgressEnvelope, EgressFailure, EgressPolicy,
    EgressReconnect, EgressRetry, EgressRetryAction,
};
#[cfg(feature = "thread-safe")]
use lazily::{ThreadSafeContext, ThreadSafeEgressCell};
use serde_json::{Value, json};

const SPEC_DIR: common::SpecDir = common::SpecDir("egress");
const FIXTURES: &[&str] = &[
    "egress_ordered_ack.json",
    "egress_inflight_window.json",
    "egress_retry_budget.json",
    "egress_generation_fence.json",
];

trait EgressModel: Sized {
    fn build(generation: u64, policy: EgressPolicy) -> Self;
    fn enqueue(&self, payload: u64) -> u64;
    fn claim(&self, generation: u64) -> EgressClaim<u64>;
    fn ack(&self, generation: u64, through: u64) -> EgressAck;
    fn fail(&self, generation: u64, sequence: u64) -> EgressFailure;
    fn retry_now(&self, generation: u64, sequence: u64) -> EgressRetryAction;
    fn reconnect(&self, generation: u64) -> EgressReconnect;

    fn generation(&self) -> u64;
    fn next_sequence(&self) -> u64;
    fn pending(&self) -> Vec<EgressEnvelope<u64>>;
    fn inflight(&self) -> Vec<EgressEnvelope<u64>>;
    fn acked_through(&self) -> Option<u64>;
    fn retry(&self) -> Option<EgressRetry>;
    fn validity(&self) -> [bool; 4];
}

fn load(name: &str) -> Option<Value> {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = common::spec_read_to_string(path).ok()?;
    Some(serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {name}: {error}")))
}

fn policy(value: &Value) -> EgressPolicy {
    EgressPolicy {
        inflight_limit: value["inflight_limit"].as_u64().expect("inflight_limit") as usize,
        retry_budget: value["retry_budget"].as_u64().expect("retry_budget") as u32,
        retry_base: value["retry_base"].as_u64().expect("retry_base"),
        retry_ceiling: value["retry_ceiling"].as_u64().expect("retry_ceiling"),
    }
}

fn envelope_value(envelope: &EgressEnvelope<u64>) -> Value {
    json!({
        "generation": envelope.generation,
        "sequence": envelope.sequence,
        "attempt": envelope.attempt,
        "payload": envelope.payload,
    })
}

fn retry_value(retry: EgressRetry) -> Value {
    json!({
        "sequence": retry.sequence,
        "attempt": retry.attempt,
        "backoff": retry.backoff,
        "exhausted": retry.exhausted,
    })
}

fn claim_value(claim: EgressClaim<u64>) -> Value {
    match claim {
        EgressClaim::Claimed(envelope) => {
            json!({ "claim": "claimed", "envelope": envelope_value(&envelope) })
        }
        EgressClaim::Empty => json!({ "claim": "empty" }),
        EgressClaim::WindowFull => json!({ "claim": "window_full" }),
        EgressClaim::StaleGeneration { current } => {
            json!({ "claim": "stale_generation", "current": current })
        }
    }
}

fn ack_value(ack: EgressAck) -> Value {
    match ack {
        EgressAck::Advanced { through } => json!({ "ack": "advanced", "through": through }),
        EgressAck::Unchanged { through } => json!({ "ack": "unchanged", "through": through }),
        EgressAck::StaleGeneration { current } => {
            json!({ "ack": "stale_generation", "current": current })
        }
    }
}

fn failure_value(failure: EgressFailure) -> Value {
    match failure {
        EgressFailure::Retrying(retry) => {
            json!({ "failure": "retrying", "retry": retry_value(retry) })
        }
        EgressFailure::Exhausted(retry) => {
            json!({ "failure": "exhausted", "retry": retry_value(retry) })
        }
        EgressFailure::UnknownSequence => json!({ "failure": "unknown_sequence" }),
        EgressFailure::StaleGeneration { current } => {
            json!({ "failure": "stale_generation", "current": current })
        }
    }
}

fn retry_action_value(action: EgressRetryAction) -> Value {
    match action {
        EgressRetryAction::Scheduled { sequence } => {
            json!({ "retry_action": "scheduled", "sequence": sequence })
        }
        EgressRetryAction::UnknownSequence => json!({ "retry_action": "unknown_sequence" }),
        EgressRetryAction::StaleGeneration { current } => {
            json!({ "retry_action": "stale_generation", "current": current })
        }
    }
}

fn reconnect_value(reconnect: EgressReconnect) -> Value {
    match reconnect {
        EgressReconnect::Advanced {
            generation,
            replayed,
        } => json!({
            "reconnect": "advanced",
            "generation": generation,
            "replayed": replayed,
        }),
        EgressReconnect::Unchanged { generation } => {
            json!({ "reconnect": "unchanged", "generation": generation })
        }
        EgressReconnect::StaleGeneration { current } => {
            json!({ "reconnect": "stale_generation", "current": current })
        }
    }
}

fn materialize<Model: EgressModel>(model: &Model) {
    let _ = model.pending();
    let _ = model.inflight();
    let _ = model.acked_through();
    let _ = model.retry();
}

fn run_op<Model: EgressModel>(model: &Model, op: &Value) -> Value {
    match op["type"].as_str().expect("op type") {
        "enqueue" => json!({
            "sequence": model.enqueue(op["payload"].as_u64().expect("payload")),
        }),
        "claim" => claim_value(model.claim(op["generation"].as_u64().expect("generation"))),
        "ack" => ack_value(model.ack(
            op["generation"].as_u64().expect("generation"),
            op["through"].as_u64().expect("through"),
        )),
        "fail" => failure_value(model.fail(
            op["generation"].as_u64().expect("generation"),
            op["sequence"].as_u64().expect("sequence"),
        )),
        "retry_now" => retry_action_value(model.retry_now(
            op["generation"].as_u64().expect("generation"),
            op["sequence"].as_u64().expect("sequence"),
        )),
        "reconnect" => {
            reconnect_value(model.reconnect(op["generation"].as_u64().expect("generation")))
        }
        other => panic!("unknown egress op `{other}`"),
    }
}

fn assert_state<Model: EgressModel>(model: &Model, expected: &Expect, where_: &str) {
    expected.assert_key_at("generation", model.generation(), where_);
    expected.assert_key_at("next_sequence", model.next_sequence(), where_);
    expected.assert_key_with("pending", |want| {
        let got: Vec<Value> = model.pending().iter().map(envelope_value).collect();
        assert_eq!(Value::Array(got), *want, "{where_}: pending");
    });
    expected.assert_key_with("inflight", |want| {
        let got: Vec<Value> = model.inflight().iter().map(envelope_value).collect();
        assert_eq!(Value::Array(got), *want, "{where_}: inflight");
    });
    expected.assert_key_at(
        "acked_through",
        serde_json::to_value(model.acked_through()).expect("watermark"),
        where_,
    );
    let retry = expected.sub("retry");
    match retry.raw().as_object() {
        None => assert_eq!(model.retry(), None, "{where_}: retry"),
        Some(_) => {
            let got = model
                .retry()
                .unwrap_or_else(|| panic!("{where_}: retry absent"));
            retry.assert_key_at("sequence", got.sequence, where_);
            retry.assert_key_at("attempt", u64::from(got.attempt), where_);
            retry.assert_key_at("backoff", got.backoff, where_);
            retry.assert_key_at("exhausted", got.exhausted, where_);
        }
    }
    retry.finish();
}

fn assert_invalidation(expected: &Expect, before: [bool; 4], after: [bool; 4], where_: &str) {
    let invalidates = expected.sub("invalidates");
    for (index, kind) in ["pending", "inflight", "acked_through", "retry"]
        .iter()
        .enumerate()
    {
        let got = before[index] && !after[index];
        invalidates.assert_key_at(
            kind,
            got,
            &format!(
                "{where_}: {kind} invalidation (was valid={}, now valid={})",
                before[index], after[index]
            ),
        );
    }
    invalidates.finish();
}

fn replay<Model: EgressModel>(fixture: &Value, name: &str) -> usize {
    assert_eq!(fixture["model"].as_str(), Some("EgressCore"));
    let model = Model::build(
        fixture["generation"].as_u64().expect("generation"),
        policy(&fixture["policy"]),
    );
    materialize(&model);

    let mut count = 0;
    for (index, step) in fixture["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .enumerate()
    {
        let op = &step["op"];
        let where_ = format!(
            "{name} step {index} ({})",
            op["type"].as_str().unwrap_or("?")
        );
        let before = model.validity();
        let got = run_op(&model, op);
        assert_eq!(got, step["returns"], "{where_}: returned outcome");
        let after = model.validity();

        let expected = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("steps[{index}].expected"),
            &step["expected"],
        );
        assert_state(&model, &expected, &where_);
        assert_invalidation(&expected, before, after, &where_);
        expected.finish();
        materialize(&model);
        count += 1;
    }
    count
}

fn replay_corpus<Model: EgressModel>() -> usize {
    FIXTURES
        .iter()
        .map(|name| {
            replay::<Model>(
                &load(name).unwrap_or_else(|| panic!("missing {name}")),
                name,
            )
        })
        .sum()
}

struct SyncModel {
    ctx: Context,
    cell: EgressCell<u64>,
}

impl EgressModel for SyncModel {
    fn build(generation: u64, policy: EgressPolicy) -> Self {
        let ctx = Context::new();
        let cell = EgressCell::new(&ctx, generation, policy).expect("policy");
        Self { ctx, cell }
    }

    fn enqueue(&self, payload: u64) -> u64 {
        self.cell.enqueue(&self.ctx, payload)
    }
    fn claim(&self, generation: u64) -> EgressClaim<u64> {
        self.cell.claim(&self.ctx, generation)
    }
    fn ack(&self, generation: u64, through: u64) -> EgressAck {
        self.cell.ack(&self.ctx, generation, through)
    }
    fn fail(&self, generation: u64, sequence: u64) -> EgressFailure {
        self.cell.fail(&self.ctx, generation, sequence)
    }
    fn retry_now(&self, generation: u64, sequence: u64) -> EgressRetryAction {
        self.cell.retry_now(&self.ctx, generation, sequence)
    }
    fn reconnect(&self, generation: u64) -> EgressReconnect {
        self.cell.reconnect(&self.ctx, generation)
    }
    fn generation(&self) -> u64 {
        self.cell.generation()
    }
    fn next_sequence(&self) -> u64 {
        self.cell.next_sequence()
    }
    fn pending(&self) -> Vec<EgressEnvelope<u64>> {
        self.cell.pending(&self.ctx)
    }
    fn inflight(&self) -> Vec<EgressEnvelope<u64>> {
        self.cell.inflight(&self.ctx)
    }
    fn acked_through(&self) -> Option<u64> {
        self.cell.acked_through(&self.ctx)
    }
    fn retry(&self) -> Option<EgressRetry> {
        self.cell.retry(&self.ctx)
    }
    fn validity(&self) -> [bool; 4] {
        [
            self.ctx.is_set(&self.cell.pending_handle()),
            self.ctx.is_set(&self.cell.inflight_handle()),
            self.ctx.is_set(&self.cell.acked_through_handle()),
            self.ctx.is_set(&self.cell.retry_handle()),
        ]
    }
}

#[cfg(feature = "thread-safe")]
struct SharedModel {
    ctx: ThreadSafeContext,
    cell: ThreadSafeEgressCell<u64>,
}

#[cfg(feature = "thread-safe")]
impl EgressModel for SharedModel {
    fn build(generation: u64, policy: EgressPolicy) -> Self {
        let ctx = ThreadSafeContext::new();
        let cell = ThreadSafeEgressCell::new(&ctx, generation, policy).expect("policy");
        Self { ctx, cell }
    }

    fn enqueue(&self, payload: u64) -> u64 {
        self.cell.enqueue(&self.ctx, payload)
    }
    fn claim(&self, generation: u64) -> EgressClaim<u64> {
        self.cell.claim(&self.ctx, generation)
    }
    fn ack(&self, generation: u64, through: u64) -> EgressAck {
        self.cell.ack(&self.ctx, generation, through)
    }
    fn fail(&self, generation: u64, sequence: u64) -> EgressFailure {
        self.cell.fail(&self.ctx, generation, sequence)
    }
    fn retry_now(&self, generation: u64, sequence: u64) -> EgressRetryAction {
        self.cell.retry_now(&self.ctx, generation, sequence)
    }
    fn reconnect(&self, generation: u64) -> EgressReconnect {
        self.cell.reconnect(&self.ctx, generation)
    }
    fn generation(&self) -> u64 {
        self.cell.generation()
    }
    fn next_sequence(&self) -> u64 {
        self.cell.next_sequence()
    }
    fn pending(&self) -> Vec<EgressEnvelope<u64>> {
        self.cell.pending(&self.ctx)
    }
    fn inflight(&self) -> Vec<EgressEnvelope<u64>> {
        self.cell.inflight(&self.ctx)
    }
    fn acked_through(&self) -> Option<u64> {
        self.cell.acked_through(&self.ctx)
    }
    fn retry(&self) -> Option<EgressRetry> {
        self.cell.retry(&self.ctx)
    }
    fn validity(&self) -> [bool; 4] {
        [
            self.ctx.is_set(&self.cell.pending_handle()),
            self.ctx.is_set(&self.cell.inflight_handle()),
            self.ctx.is_set(&self.cell.acked_through_handle()),
            self.ctx.is_set(&self.cell.retry_handle()),
        ]
    }
}

#[cfg(feature = "async")]
struct AsyncModel {
    ctx: AsyncContext,
    cell: AsyncEgressCell<u64>,
}

#[cfg(feature = "async")]
impl EgressModel for AsyncModel {
    fn build(generation: u64, policy: EgressPolicy) -> Self {
        let ctx = AsyncContext::new();
        let cell = AsyncEgressCell::new(&ctx, generation, policy).expect("policy");
        Self { ctx, cell }
    }

    fn enqueue(&self, payload: u64) -> u64 {
        self.cell.enqueue(&self.ctx, payload)
    }
    fn claim(&self, generation: u64) -> EgressClaim<u64> {
        self.cell.claim(&self.ctx, generation)
    }
    fn ack(&self, generation: u64, through: u64) -> EgressAck {
        self.cell.ack(&self.ctx, generation, through)
    }
    fn fail(&self, generation: u64, sequence: u64) -> EgressFailure {
        self.cell.fail(&self.ctx, generation, sequence)
    }
    fn retry_now(&self, generation: u64, sequence: u64) -> EgressRetryAction {
        self.cell.retry_now(&self.ctx, generation, sequence)
    }
    fn reconnect(&self, generation: u64) -> EgressReconnect {
        self.cell.reconnect(&self.ctx, generation)
    }
    fn generation(&self) -> u64 {
        self.cell.generation()
    }
    fn next_sequence(&self) -> u64 {
        self.cell.next_sequence()
    }
    fn pending(&self) -> Vec<EgressEnvelope<u64>> {
        self.cell.pending(&self.ctx)
    }
    fn inflight(&self) -> Vec<EgressEnvelope<u64>> {
        self.cell.inflight(&self.ctx)
    }
    fn acked_through(&self) -> Option<u64> {
        self.cell.acked_through(&self.ctx)
    }
    fn retry(&self) -> Option<EgressRetry> {
        self.cell.retry(&self.ctx)
    }
    fn validity(&self) -> [bool; 4] {
        [
            self.ctx.is_set(&self.cell.pending_handle()),
            self.ctx.is_set(&self.cell.inflight_handle()),
            self.ctx.is_set(&self.cell.acked_through_handle()),
            self.ctx.is_set(&self.cell.retry_handle()),
        ]
    }
}

fn expected_steps() -> usize {
    FIXTURES
        .iter()
        .map(|name| {
            load(name).unwrap_or_else(|| panic!("missing {name}"))["steps"]
                .as_array()
                .expect("steps")
                .len()
        })
        .sum()
}

#[test]
fn corpus_is_present_and_non_trivial() {
    assert!(FIXTURES.iter().all(|name| load(name).is_some()));
    assert!(
        expected_steps() >= 20,
        "egress corpus must retain its named schedule set"
    );
}

#[test]
fn sync_shell_replays_every_step() {
    assert_eq!(replay_corpus::<SyncModel>(), expected_steps());
}

#[cfg(feature = "thread-safe")]
#[test]
fn thread_safe_shell_replays_every_step() {
    assert_eq!(replay_corpus::<SharedModel>(), expected_steps());
}

#[cfg(feature = "async")]
#[test]
fn async_shell_replays_every_step() {
    assert_eq!(replay_corpus::<AsyncModel>(), expected_steps());
}

#[test]
fn invalidation_probe_discriminates() {
    let model = SyncModel::build(1, EgressPolicy::default());
    materialize(&model);
    assert_eq!(model.validity(), [true; 4]);
    model.enqueue(1);
    assert_eq!(model.validity(), [false, true, true, true]);
}
