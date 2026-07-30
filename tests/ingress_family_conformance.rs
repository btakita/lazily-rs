//! The transport-agnostic ingress contract (`#designimplementtransport`),
//! replayed against **every flavor this binding ships** — with a ledger that is
//! *enforced* rather than advisory.
//!
//! lazily-rs ships all three: `IngressCell` / `ThreadSafeIngressCell` /
//! `AsyncIngressCell`, matching the three `coverage.json` ingress rows and the
//! contract `lazily-spec/docs/transport-ingress.md` declares REQUIRED of every
//! binding × every flavor.
//!
//! The flavor axis lives in the **runner**, not the corpus: the fixtures carry a
//! `model` field naming the primitive and no execution-model field, and one
//! `IngressModel` trait replays the same JSON against each shell. Nothing in the
//! trait is async-coloured, which is the finding rather than an oversight — an
//! admission decision is a function of the fence, the watermark, the reorder
//! buffer, and the observed clock, so there is nothing to await and no `settle`
//! step anywhere below.
//!
//! Three things keep this suite from reporting green while testing nothing — each
//! one a failure mode this family of suites has actually shipped:
//!
//! * [`unshipped_flavors_are_really_absent`] greps `src/` for each flavor's type
//!   name, in **both** directions. A ledger row marked shipped whose type does
//!   not exist fails; a type that exists while its row says unshipped fails and
//!   names the runner to extend. The ledger cannot rot, because the filesystem
//!   enforces it.
//! * Every replay returns its step count, and every flavor asserts that count is
//!   non-zero and equal to the corpus total. An absence guard proves the fixtures
//!   exist on disk; only a positive count proves this binary opened them.
//! * `invalidates` is asserted in **both** directions through a cache-validity
//!   probe per reader kind. A step expecting `false` fails if the shell
//!   invalidated anyway, so over-invalidation is as visible as under-.
//!
//! Every gate below was mutation-checked; the module tail lists the seven probes
//! and what each one turned red.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use common::Expect;
#[cfg(feature = "async")]
use lazily::{AsyncContext, AsyncIngressCell};
use lazily::{
    Context, IngressAdmission, IngressAuthority, IngressCell, IngressDropReason, IngressEnvelope,
    IngressError, IngressLifecycle, IngressPolicy, IngressReadiness, IngressRetry, IngressSchedule,
    IngressTransportKind, Overflow, ReplayRequest, ScopeView,
};
use lazily::{KeepLatest, Sum};
#[cfg(feature = "thread-safe")]
use lazily::{ThreadSafeContext, ThreadSafeIngressCell};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/ingress";

/// Every fixture the ingress corpus ships. Named explicitly rather than globbed:
/// a fixture added to the corpus and not to this list is a *missing replay*, and
/// the coverage guard is what should notice, not a silently shorter run.
const FIXTURES: &[&str] = &[
    "ingress_ordered_delivery.json",
    "ingress_reorder_and_duplication.json",
    "ingress_reorder_window_overflow.json",
    "ingress_disconnect_replay.json",
    "ingress_backpressure.json",
    "ingress_generation_handoff.json",
    "ingress_freshness_and_retry.json",
];

/// One ledger row per (primitive, flavor) pair this binding claims.
struct LedgerRow {
    flavor: &'static str,
    /// The type whose presence in `src/` proves the claim.
    type_name: &'static str,
    shipped: bool,
}

/// Feature flags gate *compilation* of a flavor's replay, not whether the flavor
/// is shipped: `src/` carries all three either way, so the ledger claim is about
/// the source tree and stays true under any feature set. What a narrower feature
/// set loses is the replay, which is why `make check` runs the wider ones too.
const LEDGER: &[LedgerRow] = &[
    LedgerRow {
        flavor: "single-threaded",
        type_name: "IngressCell",
        shipped: true,
    },
    LedgerRow {
        flavor: "thread-safe",
        type_name: "ThreadSafeIngressCell",
        shipped: true,
    },
    LedgerRow {
        flavor: "async",
        type_name: "AsyncIngressCell",
        shipped: true,
    },
];

// --------------------------------------------------------------------------
// The flavor-neutral model
// --------------------------------------------------------------------------

/// What every ingress flavor must be able to do for the corpus to replay against
/// it. `M` is the merge algebra the fixture names.
///
/// The reader-kind probes (`*_is_valid`) are the whole reason this is a trait
/// rather than three copies of the runner: `invalidates` is a claim about the
/// *graph*, and only the shell can answer it.
trait IngressModel: Sized {
    fn build(policy: IngressPolicy, transport: IngressTransportKind, poll_interval: u64) -> Self;

    fn open(&self, key: &str, generation: u64);
    fn admit(&self, envelope: IngressEnvelope<String, u64>) -> IngressAdmission;
    fn suspend(&self, key: &str) -> Option<ReplayRequest>;
    fn reconnect(&self, key: &str, generation: u64) -> ReplayRequest;
    fn close(&self, key: &str);
    fn fail(&self, key: &str, error: IngressError);
    fn tick(&self, now: u64);
    fn drain(&self, key: &str) -> Option<u64>;

    /// Reactive reads; each also materializes its reader's cache, which is what
    /// makes the next step's validity probe meaningful.
    fn value(&self, key: &str) -> Option<u64>;
    fn readiness(&self, key: &str) -> IngressReadiness;
    fn authority(&self, key: &str) -> Option<IngressAuthority>;
    fn retry(&self, key: &str) -> Option<IngressRetry>;
    fn accepted_len(&self, key_hint: &[String]) -> usize;
    fn dropped_len(&self, key_hint: &[String]) -> usize;
    fn errors_len(&self, key_hint: &[String]) -> usize;
    fn schedule(&self) -> IngressSchedule;

    /// `false` when the reader is invalidated — which is what the fixture's
    /// `invalidates: true` means.
    fn value_is_valid(&self, key: &str) -> bool;
    fn readiness_is_valid(&self, key: &str) -> bool;
    fn authority_is_valid(&self, key: &str) -> bool;
    fn retry_is_valid(&self, key: &str) -> bool;
    fn accepted_is_valid(&self) -> bool;
    fn dropped_is_valid(&self) -> bool;
    fn errors_is_valid(&self) -> bool;

    fn view(&self, key: &str) -> Option<ScopeView>;
}

// --------------------------------------------------------------------------
// Fixture decoding
// --------------------------------------------------------------------------

fn fixture_path(name: &str) -> PathBuf {
    Path::new(SPEC_DIR).join(name)
}

fn load(name: &str) -> Option<Value> {
    let raw = common::spec_read_to_string(fixture_path(name)).ok()?;
    Some(serde_json::from_str(&raw).expect("fixture is valid JSON"))
}

fn corpus_present() -> bool {
    FIXTURES.iter().all(|name| fixture_path(name).exists())
}

fn overflow_of(text: &str) -> Overflow {
    match text {
        "block" => Overflow::Block,
        "drop_newest" => Overflow::DropNewest,
        "drop_oldest" => Overflow::DropOldest,
        "conflate" => Overflow::Conflate,
        "spill" => Overflow::Spill,
        other => panic!("unknown overflow `{other}`"),
    }
}

fn transport_of(text: &str) -> IngressTransportKind {
    match text {
        "event_channel" => IngressTransportKind::EventChannel,
        "rpc_triggered" => IngressTransportKind::RpcTriggered,
        "bounded_polling" => IngressTransportKind::BoundedPolling,
        other => panic!("unknown transport `{other}`"),
    }
}

fn error_of(text: &str) -> IngressError {
    match text {
        "transport_closed" => IngressError::TransportClosed,
        "decode_failed" => IngressError::DecodeFailed,
        "authority_lost" => IngressError::AuthorityLost,
        other => panic!("unknown error `{other}`"),
    }
}

fn drop_reason_of(text: &str) -> IngressDropReason {
    match text {
        "stale_generation" => IngressDropReason::StaleGeneration,
        "duplicate_sequence" => IngressDropReason::DuplicateSequence,
        "duplicate_buffered" => IngressDropReason::DuplicateBuffered,
        "reorder_window_overflow" => IngressDropReason::ReorderWindowOverflow,
        "expired" => IngressDropReason::Expired,
        "backpressure" => IngressDropReason::Backpressure,
        "scope_closed" => IngressDropReason::ScopeClosed,
        other => panic!("unknown drop reason `{other}`"),
    }
}

fn lifecycle_of(text: &str) -> IngressLifecycle {
    match text {
        "opening" => IngressLifecycle::Opening,
        "live" => IngressLifecycle::Live,
        "suspended" => IngressLifecycle::Suspended,
        "closed" => IngressLifecycle::Closed,
        other => panic!("unknown lifecycle `{other}`"),
    }
}

fn readiness_of(text: &str) -> IngressReadiness {
    match text {
        "unknown" => IngressReadiness::Unknown,
        "warming" => IngressReadiness::Warming,
        "ready" => IngressReadiness::Ready,
        "stale" => IngressReadiness::Stale,
        "suspended" => IngressReadiness::Suspended,
        "closed" => IngressReadiness::Closed,
        other => panic!("unknown readiness `{other}`"),
    }
}

fn policy_of(value: &Value) -> IngressPolicy {
    IngressPolicy {
        reorder_window: value["reorder_window"].as_u64().expect("reorder_window") as usize,
        freshness_horizon: value["freshness_horizon"]
            .as_u64()
            .expect("freshness_horizon"),
        high_water: value["high_water"].as_u64().expect("high_water"),
        overflow: overflow_of(value["overflow"].as_str().expect("overflow")),
        receipt_capacity: value["receipt_capacity"]
            .as_u64()
            .expect("receipt_capacity") as usize,
        retry_base: value["retry_base"].as_u64().expect("retry_base"),
        retry_ceiling: value["retry_ceiling"].as_u64().expect("retry_ceiling"),
    }
}

fn expected_admission(value: &Value) -> IngressAdmission {
    match value["admission"].as_str().expect("admission") {
        "accepted" => IngressAdmission::Accepted {
            delivered_through: value["delivered_through"].as_u64().expect("watermark"),
        },
        "conflated" => IngressAdmission::Conflated {
            delivered_through: value["delivered_through"].as_u64().expect("watermark"),
        },
        "buffered" => IngressAdmission::Buffered {
            gap_from: value["gap_from"].as_u64().expect("gap_from"),
        },
        "generation_handoff" => IngressAdmission::GenerationHandoff {
            from: value["from"].as_u64().expect("from"),
            to: value["to"].as_u64().expect("to"),
        },
        "dropped" => {
            IngressAdmission::Dropped(drop_reason_of(value["reason"].as_str().expect("reason")))
        }
        "blocked" => IngressAdmission::Blocked,
        other => panic!("unknown admission `{other}`"),
    }
}

/// Cache-validity snapshot of every reader kind the fixture can speak about.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ValiditySnapshot {
    scopes: BTreeMap<String, [bool; 4]>,
    receipts: [bool; 3],
}

fn snapshot_validity<Model: IngressModel>(model: &Model, keys: &[String]) -> ValiditySnapshot {
    ValiditySnapshot {
        scopes: keys
            .iter()
            .map(|key| {
                (
                    key.clone(),
                    [
                        model.value_is_valid(key),
                        model.readiness_is_valid(key),
                        model.authority_is_valid(key),
                        model.retry_is_valid(key),
                    ],
                )
            })
            .collect(),
        receipts: [
            model.accepted_is_valid(),
            model.dropped_is_valid(),
            model.errors_is_valid(),
        ],
    }
}

/// Read every reader kind, so the caches are warm and the next step's validity
/// probe measures *that step's* invalidation and nothing else.
fn materialize<Model: IngressModel>(model: &Model, keys: &[String]) {
    for key in keys {
        let _ = model.value(key);
        let _ = model.readiness(key);
        let _ = model.authority(key);
        let _ = model.retry(key);
    }
    let _ = model.accepted_len(keys);
    let _ = model.dropped_len(keys);
    let _ = model.errors_len(keys);
    let _ = model.schedule();
}

/// Replay one fixture. Returns the number of steps executed, so a caller can
/// prove this binary actually opened the corpus.
fn replay<Model: IngressModel>(fixture: &Value, label: &str) -> usize {
    let policy = policy_of(&fixture["policy"]);
    let transport = transport_of(fixture["transport"].as_str().expect("transport"));
    let poll_interval = fixture["poll_interval"].as_u64().expect("poll_interval");
    let model = Model::build(policy, transport, poll_interval);

    // Every key the fixture ever mentions, so a reader exists (and is probed)
    // from the first step — an absent reader would silently pass a `false`
    // invalidation expectation.
    let mut keys: Vec<String> = Vec::new();
    for step in fixture["steps"].as_array().expect("steps") {
        for source in [&step["op"]["key"], &step["expected"]["scopes"]] {
            match source {
                Value::String(key) => {
                    if !keys.contains(key) {
                        keys.push(key.clone());
                    }
                }
                Value::Object(map) => {
                    for key in map.keys() {
                        if !keys.contains(key) {
                            keys.push(key.clone());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    materialize(&model, &keys);
    let mut steps = 0usize;

    for (index, step) in fixture["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .enumerate()
    {
        let op = &step["op"];
        let where_ = format!(
            "{label} step {index} ({})",
            op["type"].as_str().unwrap_or("?")
        );
        let before = snapshot_validity(&model, &keys);

        match op["type"].as_str().expect("op type") {
            "admit" => {
                let envelope = IngressEnvelope::new(
                    op["key"].as_str().expect("key").to_owned(),
                    op["generation"].as_u64().expect("generation"),
                    op["sequence"].as_u64().expect("sequence"),
                    op["stamped_at"].as_u64().expect("stamped_at"),
                    op["payload"].as_u64().expect("payload"),
                );
                let admission = model.admit(envelope);
                if let Some(expected) = step.get("returns") {
                    assert_eq!(
                        admission,
                        expected_admission(expected),
                        "{where_}: admission"
                    );
                }
            }
            "open" => model.open(
                op["key"].as_str().expect("key"),
                op["generation"].as_u64().expect("generation"),
            ),
            "drain" => {
                let drained = model.drain(op["key"].as_str().expect("key"));
                if let Some(expected) = step.get("returns") {
                    assert_eq!(
                        drained,
                        expected["drained"].as_u64(),
                        "{where_}: drained value"
                    );
                }
            }
            "suspend" => {
                let request = model.suspend(op["key"].as_str().expect("key"));
                if let Some(expected) = step.get("returns") {
                    assert_eq!(
                        request,
                        expected_replay(&expected["replay"]),
                        "{where_}: replay request"
                    );
                }
            }
            "reconnect" => {
                let request = model.reconnect(
                    op["key"].as_str().expect("key"),
                    op["generation"].as_u64().expect("generation"),
                );
                if let Some(expected) = step.get("returns") {
                    assert_eq!(
                        Some(request),
                        expected_replay(&expected["replay"]),
                        "{where_}: replay request"
                    );
                }
            }
            "close" => model.close(op["key"].as_str().expect("key")),
            "fail" => model.fail(
                op["key"].as_str().expect("key"),
                error_of(op["error"].as_str().expect("error")),
            ),
            "tick" => model.tick(op["now"].as_u64().expect("now")),
            other => panic!("{where_}: unknown op `{other}`"),
        }

        let after = snapshot_validity(&model, &keys);
        // ONE guard per step (`#lzassertunknownkeys`), shared by the state and
        // invalidation assertions so the whole `expected` block is covered.
        let expected = Expect::new(
            format!("{SPEC_DIR}/{label}"),
            format!("steps[{index}].expected"),
            &step["expected"],
        );
        assert_state(&model, &expected, &keys, &where_);
        assert_invalidation(&expected, &before, &after, &where_);
        materialize(&model, &keys);
        steps += 1;
    }

    steps
}

fn expected_replay(value: &Value) -> Option<ReplayRequest> {
    if value.is_null() {
        return None;
    }
    Some(ReplayRequest {
        generation: value["generation"].as_u64().expect("generation"),
        from_sequence: value["from_sequence"].as_u64().expect("from_sequence"),
    })
}

fn assert_state<Model: IngressModel>(
    model: &Model,
    expected: &Expect,
    keys: &[String],
    where_: &str,
) {
    // `scopes` is keyed by SCOPE NAME — data, not assertion names — so it is
    // consumed wholesale; each scope's RECORD is guarded, because those keys
    // (`lifecycle`, `window`, `readiness`, ...) are assertion names.
    for (key, want) in expected["scopes"].as_object().expect("scopes") {
        let want = expected.nested(format!("scopes.{key}"), want);
        let view = model
            .view(key)
            .unwrap_or_else(|| panic!("{where_}: scope {key} absent"));
        assert_eq!(
            view.lifecycle,
            lifecycle_of(want["lifecycle"].as_str().expect("lifecycle")),
            "{where_}: {key} lifecycle"
        );
        assert_eq!(
            view.generation,
            want["generation"].as_u64().expect("generation"),
            "{where_}: {key} generation"
        );
        assert_eq!(
            view.delivered_through,
            want["delivered_through"].as_u64(),
            "{where_}: {key} watermark"
        );
        assert_eq!(
            view.buffered,
            want["buffered"].as_u64().expect("buffered") as usize,
            "{where_}: {key} buffered"
        );
        assert_eq!(
            view.consecutive_errors as u64,
            want["consecutive_errors"].as_u64().expect("errors"),
            "{where_}: {key} consecutive errors"
        );
        assert_eq!(
            model.value(key),
            want["window"].as_u64(),
            "{where_}: {key} window"
        );
        assert_eq!(
            model.readiness(key),
            readiness_of(want["readiness"].as_str().expect("readiness")),
            "{where_}: {key} readiness"
        );
        let authority = model.authority(key);
        match want["authority"].as_object() {
            None => assert_eq!(authority, None, "{where_}: {key} authority"),
            Some(want) => assert_eq!(
                authority,
                Some(IngressAuthority {
                    generation: want["generation"].as_u64().expect("generation"),
                    delivered_through: want["delivered_through"].as_u64(),
                    stamped_at: want["stamped_at"].as_u64().expect("stamped_at"),
                }),
                "{where_}: {key} authority"
            ),
        }
        let retry = model.retry(key);
        match want["retry"].as_object() {
            None => assert_eq!(retry, None, "{where_}: {key} retry"),
            Some(want) => assert_eq!(
                retry,
                Some(IngressRetry {
                    attempt: want["attempt"].as_u64().expect("attempt") as u32,
                    backoff: want["backoff"].as_u64().expect("backoff"),
                    resume_from: want["resume_from"].as_u64().expect("resume_from"),
                }),
                "{where_}: {key} retry"
            ),
        }
    }

    let receipts = expected.sub("receipts");
    assert_eq!(
        model.accepted_len(keys) as u64,
        receipts["accepted"].as_u64().expect("accepted"),
        "{where_}: accepted receipts"
    );
    assert_eq!(
        model.dropped_len(keys) as u64,
        receipts["dropped"].as_u64().expect("dropped"),
        "{where_}: dropped receipts"
    );
    assert_eq!(
        model.errors_len(keys) as u64,
        receipts["error"].as_u64().expect("error"),
        "{where_}: error receipts"
    );
}

/// Assert `invalidates` in both directions. `true` means the reader's cache went
/// from valid to invalid across the op; `false` means it stayed valid.
fn assert_invalidation(
    expected: &Expect,
    before: &ValiditySnapshot,
    after: &ValiditySnapshot,
    where_: &str,
) {
    let want = expected.sub("invalidates");
    const KINDS: [&str; 4] = ["value", "readiness", "authority", "retry"];
    for (key, want_scope) in want["scopes"].as_object().expect("invalidates.scopes") {
        let want_scope = want.nested(format!("scopes.{key}"), want_scope);
        let before_scope = before.scopes.get(key).expect("probed key");
        let after_scope = after.scopes.get(key).expect("probed key");
        for (slot, kind) in KINDS.iter().enumerate() {
            let expected = want_scope[*kind].as_bool().expect("invalidation flag");
            let invalidated = before_scope[slot] && !after_scope[slot];
            assert_eq!(
                invalidated, expected,
                "{where_}: {key}.{kind} invalidation (was valid={}, now valid={})",
                before_scope[slot], after_scope[slot]
            );
        }
    }
    const CHANNELS: [&str; 3] = ["accepted", "dropped", "error"];
    let want_receipts = want.sub("receipts");
    for (slot, channel) in CHANNELS.iter().enumerate() {
        let expected = want_receipts[*channel]
            .as_bool()
            .expect("receipt invalidation flag");
        let invalidated = before.receipts[slot] && !after.receipts[slot];
        assert_eq!(
            invalidated, expected,
            "{where_}: receipts.{channel} invalidation"
        );
    }
}

/// Replay the whole corpus against one flavor, dispatching the merge algebra the
/// fixture names. Returns the total step count.
fn replay_corpus<Sync, Keep>() -> usize
where
    Sync: IngressModel,
    Keep: IngressModel,
{
    let mut steps = 0usize;
    for name in FIXTURES {
        let fixture = load(name).unwrap_or_else(|| panic!("fixture {name} missing"));
        assert_eq!(
            fixture["model"].as_str(),
            Some("IngressCell"),
            "{name}: fixture model"
        );
        steps += match fixture["merge"].as_str().expect("merge") {
            "sum" => replay::<Sync>(&fixture, name),
            "keep_latest" => replay::<Keep>(&fixture, name),
            other => panic!("{name}: unknown merge `{other}`"),
        };
    }
    steps
}

// --------------------------------------------------------------------------
// Flavor 1 — single-threaded
// --------------------------------------------------------------------------

struct SyncModel<M> {
    ctx: Context,
    cell: IngressCell<String, u64, M>,
}

impl<M> IngressModel for SyncModel<M>
where
    M: lazily::MergePolicy<u64> + 'static,
{
    fn build(policy: IngressPolicy, transport: IngressTransportKind, poll_interval: u64) -> Self {
        let ctx = Context::new();
        let cell = IngressCell::new(&ctx, policy, transport, poll_interval).expect("policy");
        Self { ctx, cell }
    }

    fn open(&self, key: &str, generation: u64) {
        self.cell.open(&self.ctx, key.to_owned(), generation);
    }
    fn admit(&self, envelope: IngressEnvelope<String, u64>) -> IngressAdmission {
        self.cell.admit(&self.ctx, envelope)
    }
    fn suspend(&self, key: &str) -> Option<ReplayRequest> {
        self.cell.suspend(&self.ctx, &key.to_owned())
    }
    fn reconnect(&self, key: &str, generation: u64) -> ReplayRequest {
        self.cell.reconnect(&self.ctx, &key.to_owned(), generation)
    }
    fn close(&self, key: &str) {
        self.cell.close(&self.ctx, &key.to_owned());
    }
    fn fail(&self, key: &str, error: IngressError) {
        self.cell.fail(&self.ctx, &key.to_owned(), error);
    }
    fn tick(&self, now: u64) {
        self.cell.tick(&self.ctx, now);
    }
    fn drain(&self, key: &str) -> Option<u64> {
        self.cell.drain(&self.ctx, &key.to_owned())
    }

    fn value(&self, key: &str) -> Option<u64> {
        self.cell.value(&self.ctx, &key.to_owned())
    }
    fn readiness(&self, key: &str) -> IngressReadiness {
        self.cell.readiness(&self.ctx, &key.to_owned())
    }
    fn authority(&self, key: &str) -> Option<IngressAuthority> {
        self.cell.authority(&self.ctx, &key.to_owned())
    }
    fn retry(&self, key: &str) -> Option<IngressRetry> {
        self.cell.retry(&self.ctx, &key.to_owned())
    }
    fn accepted_len(&self, _keys: &[String]) -> usize {
        self.cell.accepted(&self.ctx).len()
    }
    fn dropped_len(&self, _keys: &[String]) -> usize {
        self.cell.dropped(&self.ctx).len()
    }
    fn errors_len(&self, _keys: &[String]) -> usize {
        self.cell.errors(&self.ctx).len()
    }
    fn schedule(&self) -> IngressSchedule {
        self.cell.schedule(&self.ctx)
    }

    fn value_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.value_handle(&self.ctx, &key.to_owned()))
    }
    fn readiness_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.readiness_handle(&self.ctx, &key.to_owned()))
    }
    fn authority_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.authority_handle(&self.ctx, &key.to_owned()))
    }
    fn retry_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.retry_handle(&self.ctx, &key.to_owned()))
    }
    fn accepted_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.accepted_handle())
    }
    fn dropped_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.dropped_handle())
    }
    fn errors_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.errors_handle())
    }

    fn view(&self, key: &str) -> Option<ScopeView> {
        self.cell.view(&key.to_owned())
    }
}

// --------------------------------------------------------------------------
// Flavor 2 — thread-safe
// --------------------------------------------------------------------------

#[cfg(feature = "thread-safe")]
struct ThreadSafeModel<M> {
    ctx: ThreadSafeContext,
    cell: ThreadSafeIngressCell<String, u64, M>,
}

#[cfg(feature = "thread-safe")]
impl<M> IngressModel for ThreadSafeModel<M>
where
    M: lazily::MergePolicy<u64> + Send + Sync + 'static,
{
    fn build(policy: IngressPolicy, transport: IngressTransportKind, poll_interval: u64) -> Self {
        let ctx = ThreadSafeContext::new();
        let cell =
            ThreadSafeIngressCell::new(&ctx, policy, transport, poll_interval).expect("policy");
        Self { ctx, cell }
    }

    fn open(&self, key: &str, generation: u64) {
        self.cell.open(&self.ctx, key.to_owned(), generation);
    }
    fn admit(&self, envelope: IngressEnvelope<String, u64>) -> IngressAdmission {
        self.cell.admit(&self.ctx, envelope)
    }
    fn suspend(&self, key: &str) -> Option<ReplayRequest> {
        self.cell.suspend(&self.ctx, &key.to_owned())
    }
    fn reconnect(&self, key: &str, generation: u64) -> ReplayRequest {
        self.cell.reconnect(&self.ctx, &key.to_owned(), generation)
    }
    fn close(&self, key: &str) {
        self.cell.close(&self.ctx, &key.to_owned());
    }
    fn fail(&self, key: &str, error: IngressError) {
        self.cell.fail(&self.ctx, &key.to_owned(), error);
    }
    fn tick(&self, now: u64) {
        self.cell.tick(&self.ctx, now);
    }
    fn drain(&self, key: &str) -> Option<u64> {
        self.cell.drain(&self.ctx, &key.to_owned())
    }

    fn value(&self, key: &str) -> Option<u64> {
        self.cell.value(&self.ctx, &key.to_owned())
    }
    fn readiness(&self, key: &str) -> IngressReadiness {
        self.cell.readiness(&self.ctx, &key.to_owned())
    }
    fn authority(&self, key: &str) -> Option<IngressAuthority> {
        self.cell.authority(&self.ctx, &key.to_owned())
    }
    fn retry(&self, key: &str) -> Option<IngressRetry> {
        self.cell.retry(&self.ctx, &key.to_owned())
    }
    fn accepted_len(&self, _keys: &[String]) -> usize {
        self.cell.accepted(&self.ctx).len()
    }
    fn dropped_len(&self, _keys: &[String]) -> usize {
        self.cell.dropped(&self.ctx).len()
    }
    fn errors_len(&self, _keys: &[String]) -> usize {
        self.cell.errors(&self.ctx).len()
    }
    fn schedule(&self) -> IngressSchedule {
        self.cell.schedule(&self.ctx)
    }

    fn value_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.value_handle(&self.ctx, &key.to_owned()))
    }
    fn readiness_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.readiness_handle(&self.ctx, &key.to_owned()))
    }
    fn authority_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.authority_handle(&self.ctx, &key.to_owned()))
    }
    fn retry_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.retry_handle(&self.ctx, &key.to_owned()))
    }
    fn accepted_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.accepted_handle())
    }
    fn dropped_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.dropped_handle())
    }
    fn errors_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.errors_handle())
    }

    fn view(&self, key: &str) -> Option<ScopeView> {
        self.cell.view(&key.to_owned())
    }
}

// --------------------------------------------------------------------------
// Flavor 3 — async
// --------------------------------------------------------------------------

#[cfg(feature = "async")]
struct AsyncModel<M> {
    ctx: AsyncContext,
    cell: AsyncIngressCell<String, u64, M>,
}

#[cfg(feature = "async")]
impl<M> IngressModel for AsyncModel<M>
where
    M: lazily::MergePolicy<u64> + Send + Sync + 'static,
{
    fn build(policy: IngressPolicy, transport: IngressTransportKind, poll_interval: u64) -> Self {
        let ctx = AsyncContext::new();
        let cell = AsyncIngressCell::new(&ctx, policy, transport, poll_interval).expect("policy");
        Self { ctx, cell }
    }

    fn open(&self, key: &str, generation: u64) {
        self.cell.open(&self.ctx, key.to_owned(), generation);
    }
    fn admit(&self, envelope: IngressEnvelope<String, u64>) -> IngressAdmission {
        self.cell.admit(&self.ctx, envelope)
    }
    fn suspend(&self, key: &str) -> Option<ReplayRequest> {
        self.cell.suspend(&self.ctx, &key.to_owned())
    }
    fn reconnect(&self, key: &str, generation: u64) -> ReplayRequest {
        self.cell.reconnect(&self.ctx, &key.to_owned(), generation)
    }
    fn close(&self, key: &str) {
        self.cell.close(&self.ctx, &key.to_owned());
    }
    fn fail(&self, key: &str, error: IngressError) {
        self.cell.fail(&self.ctx, &key.to_owned(), error);
    }
    fn tick(&self, now: u64) {
        self.cell.tick(&self.ctx, now);
    }
    fn drain(&self, key: &str) -> Option<u64> {
        self.cell.drain(&self.ctx, &key.to_owned())
    }

    fn value(&self, key: &str) -> Option<u64> {
        self.cell.value(&self.ctx, &key.to_owned())
    }
    fn readiness(&self, key: &str) -> IngressReadiness {
        self.cell.readiness(&self.ctx, &key.to_owned())
    }
    fn authority(&self, key: &str) -> Option<IngressAuthority> {
        self.cell.authority(&self.ctx, &key.to_owned())
    }
    fn retry(&self, key: &str) -> Option<IngressRetry> {
        self.cell.retry(&self.ctx, &key.to_owned())
    }
    fn accepted_len(&self, _keys: &[String]) -> usize {
        self.cell.accepted(&self.ctx).len()
    }
    fn dropped_len(&self, _keys: &[String]) -> usize {
        self.cell.dropped(&self.ctx).len()
    }
    fn errors_len(&self, _keys: &[String]) -> usize {
        self.cell.errors(&self.ctx).len()
    }
    fn schedule(&self) -> IngressSchedule {
        self.cell.schedule(&self.ctx)
    }

    fn value_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.value_handle(&self.ctx, &key.to_owned()))
    }
    fn readiness_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.readiness_handle(&self.ctx, &key.to_owned()))
    }
    fn authority_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.authority_handle(&self.ctx, &key.to_owned()))
    }
    fn retry_is_valid(&self, key: &str) -> bool {
        self.ctx
            .is_set(&self.cell.retry_handle(&self.ctx, &key.to_owned()))
    }
    fn accepted_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.accepted_handle())
    }
    fn dropped_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.dropped_handle())
    }
    fn errors_is_valid(&self) -> bool {
        self.ctx.is_set(&self.cell.errors_handle())
    }

    fn view(&self, key: &str) -> Option<ScopeView> {
        self.cell.view(&key.to_owned())
    }
}

// --------------------------------------------------------------------------
// The gates
// --------------------------------------------------------------------------

fn expected_step_total() -> usize {
    FIXTURES
        .iter()
        .map(|name| {
            load(name).unwrap_or_else(|| panic!("fixture {name} missing"))["steps"]
                .as_array()
                .expect("steps")
                .len()
        })
        .sum()
}

#[test]
fn corpus_is_present_and_non_trivial() {
    assert!(
        corpus_present(),
        "lazily-spec ingress corpus not found at {SPEC_DIR}"
    );
    let total = expected_step_total();
    assert!(
        total >= 30,
        "the ingress corpus replays only {total} steps; that is not the named schedule set"
    );
}

#[test]
fn single_threaded_flavor_replays_the_corpus() {
    assert!(corpus_present(), "corpus missing");
    let steps = replay_corpus::<SyncModel<Sum>, SyncModel<KeepLatest>>();
    assert_eq!(
        steps,
        expected_step_total(),
        "every corpus step must run against the single-threaded flavor"
    );
}

#[cfg(feature = "thread-safe")]
#[test]
fn thread_safe_flavor_replays_the_corpus() {
    assert!(corpus_present(), "corpus missing");
    let steps = replay_corpus::<ThreadSafeModel<Sum>, ThreadSafeModel<KeepLatest>>();
    assert_eq!(
        steps,
        expected_step_total(),
        "every corpus step must run against the thread-safe flavor"
    );
}

#[cfg(feature = "async")]
#[test]
fn async_flavor_replays_the_corpus() {
    assert!(corpus_present(), "corpus missing");
    let steps = replay_corpus::<AsyncModel<Sum>, AsyncModel<KeepLatest>>();
    assert_eq!(
        steps,
        expected_step_total(),
        "every corpus step must run against the async flavor"
    );
}

/// The ledger cannot rot: the filesystem enforces it, in both directions.
#[test]
fn unshipped_flavors_are_really_absent() {
    let mut sources = String::new();
    for entry in std::fs::read_dir("src").expect("src/ is readable") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            sources.push_str(&std::fs::read_to_string(&path).expect("source file"));
        }
    }
    for row in LEDGER {
        // `pub struct X` — the definition, not a doc-comment mention.
        let defined = sources.contains(&format!("pub struct {}<", row.type_name));
        assert_eq!(
            defined, row.shipped,
            "ledger row `{}` claims shipped={} but `pub struct {}` defined={}; \
             fix the ledger or extend the runner",
            row.flavor, row.shipped, row.type_name, defined
        );
    }
}

#[test]
fn ledger_is_not_all_skips() {
    assert_eq!(LEDGER.len(), 3, "one row per flavor this family defines");
    assert!(
        LEDGER.iter().any(|row| row.shipped),
        "a ledger of nothing-shipped is not coverage"
    );
}

/// The corpus asserts negative invalidation, so the probe itself must be able to
/// fail. This pins the probe: reading warms the cache, an op that dirties the
/// reader clears it, and one that does not leaves it warm.
#[test]
fn the_invalidation_probe_discriminates() {
    let model = SyncModel::<Sum>::build(
        IngressPolicy::default(),
        IngressTransportKind::EventChannel,
        25,
    );
    let key = "alpha";
    let _ = model.value(key);
    assert!(model.value_is_valid(key), "reading warms the cache");

    model.admit(IngressEnvelope::new(key.to_owned(), 1, 0, 0, 1));
    assert!(
        !model.value_is_valid(key),
        "a delivery must invalidate the value reader"
    );

    let _ = model.value(key);
    model.admit(IngressEnvelope::new(key.to_owned(), 1, 5, 0, 1));
    assert!(
        model.value_is_valid(key),
        "a buffered envelope must NOT invalidate the value reader"
    );
}

// Mutation-check record (`#designimplementtransport`). Each deliberate defect was
// introduced, the gate run (`--lib ingress` plus this file), and the defect
// reverted with an mtime bump — a restore that preserves mtime lets cargo reuse
// the MUTATED build and reports a false green. All seven were killed:
//
// * fence checked after dedupe → `ingress_generation_handoff` fails (reports
//   `duplicate_sequence` where the corpus expects `stale_generation`).
// * handoff keeps the superseded window → `ingress_generation_handoff` fails on
//   the window value.
// * `Buffered` marks every reader dirty → the `invalidates: false` steps in
//   `ingress_reorder_and_duplication` fail, in all three flavors.
// * `tick` marks readiness unconditionally → `ingress_freshness_and_retry` fails
//   on the in-horizon tick.
// * `Block` advances the watermark → `ingress_backpressure`'s final step reports a
//   duplicate instead of an accept.
// * thread-safe `apply` clears outside `batch()` → the frontier-walk gate in
//   `thread_safe_ingress::tests` fails (two effect runs for one admission).
// * the error-receipt channel is never cleared → the single-threaded replay fails
//   on `invalidates.receipts.error`. This one is the reason `invalidates` is
//   asserted per channel rather than by receipt COUNT: a stale cache recomputes
//   to the right count, so a count-only gate would have called it green.
