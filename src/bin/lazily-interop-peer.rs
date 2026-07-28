use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use lazily::stdlib::{
    RevisionBarrier, RevisionCheck, RevisionWaitOutcome, Timeout, TimeoutCancellation,
    TimeoutOperation, TimeoutPoll, TimeoutUnavailableReason, Timer, TimerError, TimerPoll,
};
use lazily::{
    Context, CrdtOp, CrdtPlaneRuntime, CrdtSync, IpcMessage, IpcValue, NodeId, NodeKey, PeerId,
    WireStamp,
};
use serde_json::{Value, json};

const PROTOCOL_VERSION: u64 = 1;
const STDLIB_FEATURES: [&str; 3] = [
    "stdlib_timer_v1",
    "stdlib_timeout_v1",
    "stdlib_revision_barrier_v1",
];

struct Peer {
    peer_id: Option<PeerId>,
    context: Context,
    runtime: Option<CrdtPlaneRuntime>,
    stdlib: BTreeMap<String, StdlibFeature>,
}

impl Peer {
    fn new() -> Self {
        Self {
            peer_id: None,
            context: Context::new(),
            runtime: None,
            stdlib: BTreeMap::new(),
        }
    }

    fn handle(&mut self, request: &Value) -> Value {
        let Some(command) = request.get("cmd").and_then(Value::as_str) else {
            return error("missing cmd");
        };
        match command {
            "hello" => self.hello(request),
            "local_set" => self.local_set(request),
            "deliver" => self.deliver(request),
            "snapshot" => self.snapshot(),
            "feature_reset" => self.feature_reset(request),
            "feature_step" => self.feature_step(request),
            "feature_observe" => self.feature_observe(request),
            "bye" => json!({"ok": true}),
            command if command.starts_with("link_") => json!({
                "ok": false,
                "error": "unsupported channel",
                "unsupported": true
            }),
            _ => error(format!("unknown command {command}")),
        }
    }

    fn hello(&mut self, request: &Value) -> Value {
        if request.get("protocol_version").and_then(Value::as_u64) != Some(PROTOCOL_VERSION) {
            return error(format!(
                "unsupported protocol_version {}",
                request.get("protocol_version").unwrap_or(&Value::Null)
            ));
        }
        let Some(peer) = request.get("peer").and_then(Value::as_u64) else {
            return error("hello requires peer");
        };
        let peer = PeerId(peer);
        self.peer_id = Some(peer);
        self.runtime = Some(CrdtPlaneRuntime::new(peer));
        json!({
            "ok": true,
            "binding": "lazily-rs",
            "version": env!("CARGO_PKG_VERSION"),
            "protocol_version": PROTOCOL_VERSION,
            "features": [
                "distributed_crdt",
                "stdlib_timer_v1",
                "stdlib_timeout_v1",
                "stdlib_revision_barrier_v1"
            ],
            "codecs": ["json", "msgpack"],
            "channels": [],
            "channel_variants": {},
            "platform_profile": "portable",
            "carve_outs": ["transport_links"]
        })
    }

    fn local_set(&mut self, request: &Value) -> Value {
        let Some(peer) = self.peer_id else {
            return error("hello must run first");
        };
        let Some(node) = request.get("node").and_then(Value::as_u64) else {
            return error("local_set requires node");
        };
        let key = match request.get("key") {
            Some(Value::Null) => None,
            Some(Value::String(key)) => match NodeKey::new(key) {
                Ok(key) => Some(key),
                Err(error) => return self::error(format!("invalid key: {error}")),
            },
            _ => return error("local_set requires nullable key"),
        };
        let state: IpcValue = match request.get("state").cloned().map(serde_json::from_value) {
            Some(Ok(state)) => state,
            Some(Err(error)) => return self::error(format!("invalid IpcValue: {error}")),
            None => return error("local_set requires state"),
        };
        let Some(at) = request.get("at").and_then(Value::as_u64) else {
            return error("local_set requires at");
        };
        let runtime = match self.runtime.as_mut() {
            Some(runtime) => runtime,
            None => return error("hello must run first"),
        };
        let stamp = runtime.plane_mut().tick(at);
        let wire_stamp = WireStamp::from(stamp);
        let op = match key {
            Some(key) => CrdtOp::keyed(NodeId(node), key, wire_stamp, state),
            None => CrdtOp::new(NodeId(node), wire_stamp, state),
        };
        let sync = CrdtSync::new(vec![(peer.0, wire_stamp)], vec![op]);
        if runtime.ingest(&self.context, &sync, at) != 1 {
            return error("production runtime rejected its fresh local op");
        }
        match serde_json::to_value(IpcMessage::CrdtSync(sync)) {
            Ok(frame) => json!({"ok": true, "frame": frame}),
            Err(error) => self::error(format!("encode CrdtSync: {error}")),
        }
    }

    fn deliver(&mut self, request: &Value) -> Value {
        let Some(frame) = request.get("frame").cloned() else {
            return error("deliver requires frame");
        };
        let message: IpcMessage = match serde_json::from_value(frame) {
            Ok(message) => message,
            Err(error) => return self::error(format!("decode IpcMessage: {error}")),
        };
        let IpcMessage::CrdtSync(sync) = message else {
            return error("deliver requires CrdtSync");
        };
        let Some(at) = request.get("at").and_then(Value::as_u64) else {
            return error("deliver requires at");
        };
        let Some(runtime) = self.runtime.as_mut() else {
            return error("hello must run first");
        };
        let applied = runtime.ingest(&self.context, &sync, at);
        json!({"ok": true, "applied": applied})
    }

    fn snapshot(&self) -> Value {
        let Some(runtime) = self.runtime.as_ref() else {
            return error("hello must run first");
        };
        let cells = runtime
            .converged()
            .into_iter()
            .map(|entry| {
                json!({
                    "node": entry.node.0,
                    "key": entry.key.map(|key| key.as_str().to_owned()),
                    "state": entry.state
                })
            })
            .collect::<Vec<_>>();
        json!({"ok": true, "cells": cells})
    }

    fn feature_reset(&mut self, request: &Value) -> Value {
        let Some(feature) = request.get("feature").and_then(Value::as_str) else {
            return error("feature_reset requires feature");
        };
        if !STDLIB_FEATURES.contains(&feature) {
            return json!({
                "ok": false,
                "error": format!("unsupported feature {feature}"),
                "unsupported": true
            });
        }
        let Some(adapter) = StdlibFeature::new(feature) else {
            unreachable!("validated stdlib feature has an adapter");
        };
        self.stdlib.insert(feature.to_owned(), adapter);
        json!({"ok": true, "feature": feature})
    }

    fn feature_step(&mut self, request: &Value) -> Value {
        let Some(feature) = request.get("feature").and_then(Value::as_str) else {
            return error("feature_step requires feature");
        };
        let Some(step) = request.get("step") else {
            return error("feature_step requires step");
        };
        let Some(adapter) = self.stdlib.get_mut(feature) else {
            return error(format!("feature {feature} must be reset before stepping"));
        };
        match adapter.step(step) {
            Ok(observation) => json!({
                "ok": true,
                "feature": feature,
                "observation": observation
            }),
            Err(message) => error(message),
        }
    }

    fn feature_observe(&self, request: &Value) -> Value {
        let Some(feature) = request.get("feature").and_then(Value::as_str) else {
            return error("feature_observe requires feature");
        };
        let Some(adapter) = self.stdlib.get(feature) else {
            return error(format!(
                "feature {feature} must be reset before observation"
            ));
        };
        match adapter.observation() {
            Some(observation) => json!({
                "ok": true,
                "feature": feature,
                "observation": observation
            }),
            None => error(format!("feature {feature} has no observation")),
        }
    }
}

enum StdlibFeature {
    Timer(TimerFeature),
    Timeout(TimeoutFeature),
    Barrier(BarrierFeature),
}

impl StdlibFeature {
    fn new(feature: &str) -> Option<Self> {
        match feature {
            "stdlib_timer_v1" => Some(Self::Timer(TimerFeature::new())),
            "stdlib_timeout_v1" => Some(Self::Timeout(TimeoutFeature::new())),
            "stdlib_revision_barrier_v1" => Some(Self::Barrier(BarrierFeature::new())),
            _ => None,
        }
    }

    fn step(&mut self, step: &Value) -> Result<Value, String> {
        match self {
            Self::Timer(adapter) => adapter.step(step),
            Self::Timeout(adapter) => adapter.step(step),
            Self::Barrier(adapter) => adapter.step(step),
        }
    }

    fn observation(&self) -> Option<&Value> {
        match self {
            Self::Timer(adapter) => adapter.last.as_ref(),
            Self::Timeout(adapter) => adapter.last.as_ref(),
            Self::Barrier(adapter) => adapter.last.as_ref(),
        }
    }
}

struct TimerFeature {
    base: Instant,
    timer: Option<Timer>,
    deadline: Option<u64>,
    last: Option<Value>,
}

impl TimerFeature {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            timer: None,
            deadline: None,
            last: None,
        }
    }

    fn step(&mut self, step: &Value) -> Result<Value, String> {
        let observation = match string_field(step, "op")? {
            "start" => {
                let now = u64_field(step, "now")?;
                let duration = u64_field(step, "duration")?;
                match Timer::checked_deadline_ticks(now, duration) {
                    Ok(deadline) => {
                        self.deadline = Some(deadline);
                        self.timer = Some(
                            Timer::try_after_at(
                                logical_instant(self.base, now)?,
                                Duration::from_nanos(duration),
                            )
                            .map_err(|error| error.to_string())?,
                        );
                        json!({"outcome": "pending", "deadline": deadline})
                    }
                    Err(TimerError::DeadlineOverflow) => {
                        json!({"outcome": "unavailable", "reason": "deadline_overflow"})
                    }
                    Err(TimerError::ClockRegression) => unreachable!(),
                }
            }
            "observe" => {
                let now = u64_field(step, "now")?;
                let timer = self.timer.as_mut().ok_or("timer feature is not started")?;
                match timer.try_poll_at(logical_instant(self.base, now)?) {
                    Ok(TimerPoll::Pending { .. }) => {
                        json!({"outcome": "pending", "deadline": self.deadline})
                    }
                    Ok(TimerPoll::Fired { .. }) => json!({
                        "outcome": "fired",
                        "fired_at": timer
                            .fired_at()
                            .expect("production timer records fire edge")
                            .duration_since(self.base)
                            .as_nanos() as u64
                    }),
                    Err(TimerError::ClockRegression) => json!({
                        "outcome": "unavailable",
                        "reason": "clock_regression",
                        "deadline": self.deadline
                    }),
                    Err(TimerError::DeadlineOverflow) => unreachable!(),
                }
            }
            op => return Err(format!("unsupported timer feature step {op}")),
        };
        self.last = Some(observation.clone());
        Ok(observation)
    }
}

struct TimeoutFeature {
    base: Instant,
    timeout: Option<Timeout<String>>,
    deadline: Option<u64>,
    last: Option<Value>,
}

impl TimeoutFeature {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            timeout: None,
            deadline: None,
            last: None,
        }
    }

    fn step(&mut self, step: &Value) -> Result<Value, String> {
        let observation = match string_field(step, "op")? {
            "start" => {
                let now = u64_field(step, "now")?;
                let duration = u64_field(step, "duration")?;
                let deadline = Timer::checked_deadline_ticks(now, duration)
                    .map_err(|error| error.to_string())?;
                self.deadline = Some(deadline);
                self.timeout = Some(
                    Timeout::try_after_at(
                        logical_instant(self.base, now)?,
                        Duration::from_nanos(duration),
                    )
                    .map_err(|error| error.to_string())?,
                );
                json!({"outcome": "pending", "deadline": deadline})
            }
            "poll" => {
                let now = u64_field(step, "now")?;
                let operation = string_field(step, "operation")?;
                let cancellation = string_field(step, "cancellation")?;
                if !matches!(operation, "pending" | "completed" | "unavailable") {
                    return Err(format!("unsupported operation {operation}"));
                }
                if !matches!(cancellation, "pending" | "cancelled" | "unavailable") {
                    return Err(format!("unsupported cancellation {cancellation}"));
                }
                let value = step.get("value").and_then(Value::as_str).map(str::to_owned);
                let operation_calls = Cell::new(0_u64);
                let cancellation_calls = Cell::new(0_u64);
                let timeout = self
                    .timeout
                    .as_mut()
                    .ok_or("timeout feature is not started")?;
                let poll = timeout.poll_at_with_cancellation(
                    logical_instant(self.base, now)?,
                    || {
                        operation_calls.set(operation_calls.get() + 1);
                        match operation {
                            "pending" => TimeoutOperation::Pending,
                            "completed" => TimeoutOperation::Completed(
                                value.clone().expect("completed step has value"),
                            ),
                            "unavailable" => TimeoutOperation::Unavailable,
                            _ => unreachable!("operation validated below"),
                        }
                    },
                    || {
                        cancellation_calls.set(cancellation_calls.get() + 1);
                        match cancellation {
                            "pending" => TimeoutCancellation::Pending,
                            "cancelled" => TimeoutCancellation::Cancelled,
                            "unavailable" => TimeoutCancellation::Unavailable,
                            _ => unreachable!("cancellation validated below"),
                        }
                    },
                );
                let operation_calls = operation_calls.get();
                let cancellation_calls = cancellation_calls.get();
                match poll {
                    TimeoutPoll::Pending { .. } => json!({
                        "outcome": "pending",
                        "deadline": self.deadline,
                        "operation_calls": operation_calls,
                        "cancellation_calls": cancellation_calls
                    }),
                    TimeoutPoll::Completed(value) => json!({
                        "outcome": "completed",
                        "value": value,
                        "operation_calls": operation_calls,
                        "cancellation_calls": cancellation_calls
                    }),
                    TimeoutPoll::TimedOut => json!({
                        "outcome": "timed_out",
                        "operation_calls": operation_calls,
                        "cancellation_calls": cancellation_calls
                    }),
                    TimeoutPoll::Cancelled => json!({
                        "outcome": "cancelled",
                        "operation_calls": operation_calls,
                        "cancellation_calls": cancellation_calls
                    }),
                    TimeoutPoll::Unavailable => {
                        let reason = match timeout.unavailable_reason() {
                            Some(TimeoutUnavailableReason::Operation) | None => {
                                "operation_unavailable"
                            }
                            Some(TimeoutUnavailableReason::Cancellation) => {
                                "cancellation_unavailable"
                            }
                            Some(TimeoutUnavailableReason::ClockRegression) => "clock_regression",
                        };
                        json!({
                            "outcome": "unavailable",
                            "reason": reason,
                            "operation_calls": operation_calls,
                            "cancellation_calls": cancellation_calls
                        })
                    }
                }
            }
            op => return Err(format!("unsupported timeout feature step {op}")),
        };
        self.last = Some(observation.clone());
        Ok(observation)
    }
}

struct BarrierFeature {
    barrier: Option<RevisionBarrier>,
    required_revision: u64,
    deadline: Option<u64>,
    last: Option<Value>,
}

impl BarrierFeature {
    fn new() -> Self {
        Self {
            barrier: None,
            required_revision: 0,
            deadline: None,
            last: None,
        }
    }

    fn step(&mut self, step: &Value) -> Result<Value, String> {
        let observation = match string_field(step, "op")? {
            "start" => {
                self.required_revision = u64_field(step, "required_revision")?;
                self.deadline = step.get("deadline").and_then(Value::as_u64);
                self.barrier = Some(RevisionBarrier::new(u64_field(step, "revision")?));
                barrier_value("pending", self.barrier.as_ref().unwrap(), None)
            }
            "register_recheck" => {
                let observed = u64_field(step, "observed_revision")?;
                let predicate = bool_field(step, "predicate")?;
                let barrier = self
                    .barrier
                    .as_ref()
                    .ok_or("barrier feature is not started")?
                    .clone();
                let waiter = barrier.clone();
                let after = self.required_revision.saturating_sub(1);
                let (ready_tx, ready_rx) = mpsc::channel();
                let handle = thread::spawn(move || {
                    ready_tx.send(()).expect("peer receiver");
                    waiter.wait_after(
                        after,
                        |_| {
                            if predicate {
                                RevisionCheck::Satisfied
                            } else {
                                RevisionCheck::Pending
                            }
                        },
                        None,
                        None,
                    )
                });
                ready_rx.recv().map_err(|error| error.to_string())?;
                let _ = barrier.advance(observed);
                barrier_wait_value(
                    handle
                        .join()
                        .map_err(|_| "barrier feature waiter panicked".to_owned())?,
                    &barrier,
                )
            }
            "advance" => {
                let barrier = self
                    .barrier
                    .as_ref()
                    .ok_or("barrier feature is not started")?;
                let _ = barrier.advance(u64_field(step, "revision")?);
                if barrier.is_disposed() {
                    barrier_value("disposed", barrier, None)
                } else if bool_field(step, "predicate")?
                    && barrier.revision() >= self.required_revision
                {
                    barrier_wait_value(
                        barrier.wait_after(
                            self.required_revision.saturating_sub(1),
                            |_| RevisionCheck::Satisfied,
                            None,
                            None,
                        ),
                        barrier,
                    )
                } else {
                    barrier_value("pending", barrier, None)
                }
            }
            "observe" => {
                let barrier = self
                    .barrier
                    .as_ref()
                    .ok_or("barrier feature is not started")?;
                let now = u64_field(step, "now")?;
                let reached = self.deadline.is_some_and(|deadline| now >= deadline);
                let predicate = bool_field(step, "predicate")?;
                let cancellation = string_field(step, "cancellation")?;
                let mut timer = reached.then(|| Timer::after(Duration::ZERO));
                let owned = barrier.cancellation();
                let foreign_barrier = RevisionBarrier::new(0);
                let foreign = foreign_barrier.cancellation();
                if !reached && cancellation == "cancelled" {
                    let _ = owned.cancel();
                }
                let token = match cancellation {
                    "cancelled" | "pending" => Some(&owned),
                    "unavailable" => Some(&foreign),
                    value => return Err(format!("unsupported cancellation {value}")),
                };
                let outcome = barrier.wait_after(
                    self.required_revision.saturating_sub(1),
                    |_| {
                        if predicate {
                            RevisionCheck::Satisfied
                        } else {
                            RevisionCheck::Pending
                        }
                    },
                    timer.as_mut(),
                    token,
                );
                let mut value = barrier_wait_value(outcome, barrier);
                value["cancellation_calls"] = json!(if reached
                    || (predicate && barrier.revision() >= self.required_revision)
                {
                    0
                } else {
                    1
                });
                value
            }
            "dispose" => {
                let barrier = self
                    .barrier
                    .as_ref()
                    .ok_or("barrier feature is not started")?;
                let _ = barrier.dispose();
                barrier_value("disposed", barrier, None)
            }
            "receipt" => {
                let barrier = self
                    .barrier
                    .as_ref()
                    .ok_or("barrier feature is not started")?;
                barrier.notify();
                barrier_value("pending", barrier, None)
            }
            op => return Err(format!("unsupported revision barrier feature step {op}")),
        };
        self.last = Some(observation.clone());
        Ok(observation)
    }
}

fn logical_instant(base: Instant, tick: u64) -> Result<Instant, String> {
    base.checked_add(Duration::from_nanos(tick))
        .ok_or_else(|| "logical instant exceeds the platform Instant range".to_owned())
}

fn string_field<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("feature step requires string {field}"))
}

fn u64_field(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("feature step requires u64 {field}"))
}

fn bool_field(value: &Value, field: &str) -> Result<bool, String> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("feature step requires boolean {field}"))
}

fn barrier_value(outcome: &str, barrier: &RevisionBarrier, reason: Option<&str>) -> Value {
    let mut value = json!({
        "outcome": outcome,
        "revision": barrier.revision(),
        "generation": barrier.generation()
    });
    if let Some(reason) = reason {
        value["reason"] = json!(reason);
    }
    value
}

fn barrier_wait_value(outcome: RevisionWaitOutcome, barrier: &RevisionBarrier) -> Value {
    match outcome {
        RevisionWaitOutcome::Satisfied { .. } => barrier_value("satisfied", barrier, None),
        RevisionWaitOutcome::TimedOut { .. } => barrier_value("timed_out", barrier, None),
        RevisionWaitOutcome::Cancelled { .. } => barrier_value("cancelled", barrier, None),
        RevisionWaitOutcome::Disposed { .. } => barrier_value("disposed", barrier, None),
        RevisionWaitOutcome::Unavailable { .. } => {
            barrier_value("unavailable", barrier, Some("cancellation_unavailable"))
        }
    }
}

fn error(message: impl Into<String>) -> Value {
    json!({"ok": false, "error": message.into()})
}

fn self_check() -> Result<(), String> {
    let mut peer = Peer::new();
    let hello = peer.handle(&json!({
        "cmd": "hello",
        "peer": 1,
        "protocol_version": PROTOCOL_VERSION
    }));
    if hello["ok"] != true {
        return Err(format!("hello failed: {hello}"));
    }
    let local = peer.handle(&json!({
        "cmd": "local_set",
        "node": 7,
        "key": null,
        "state": {"Inline": [65]},
        "at": 10
    }));
    if local["ok"] != true || local.pointer("/frame/CrdtSync/ops/0/key") != Some(&Value::Null) {
        return Err(format!("local_set failed canonical key check: {local}"));
    }
    let delivered = peer.handle(&json!({
        "cmd": "deliver",
        "frame": local["frame"],
        "at": 11
    }));
    if delivered["applied"] != 0 {
        return Err(format!(
            "duplicate delivery was not idempotent: {delivered}"
        ));
    }
    let snapshot = peer.handle(&json!({"cmd": "snapshot"}));
    if snapshot.pointer("/cells/0/state/Inline/0") != Some(&json!(65)) {
        return Err(format!("snapshot mismatch: {snapshot}"));
    }

    let feature_cases = [
        (
            "stdlib_timer_v1",
            vec![
                json!({"op": "start", "now": 0, "duration": 0}),
                json!({"op": "observe", "now": 0}),
            ],
            "fired",
        ),
        (
            "stdlib_timeout_v1",
            vec![
                json!({"op": "start", "now": 0, "duration": 1}),
                json!({
                    "op": "poll",
                    "now": 0,
                    "operation": "completed",
                    "value": "ok",
                    "cancellation": "pending"
                }),
            ],
            "completed",
        ),
        (
            "stdlib_revision_barrier_v1",
            vec![
                json!({
                    "op": "start",
                    "revision": 1,
                    "required_revision": 1,
                    "deadline": null
                }),
                json!({
                    "op": "observe",
                    "now": 0,
                    "predicate": true,
                    "cancellation": "pending"
                }),
            ],
            "satisfied",
        ),
    ];
    for (feature, steps, expected) in feature_cases {
        let reset = peer.handle(&json!({"cmd": "feature_reset", "feature": feature}));
        if reset["ok"] != true {
            return Err(format!("{feature} reset failed: {reset}"));
        }
        for step in steps {
            let reply =
                peer.handle(&json!({"cmd": "feature_step", "feature": feature, "step": step}));
            if reply["ok"] != true {
                return Err(format!("{feature} step failed: {reply}"));
            }
        }
        let observed = peer.handle(&json!({"cmd": "feature_observe", "feature": feature}));
        if observed.pointer("/observation/outcome") != Some(&json!(expected)) {
            return Err(format!("{feature} observation mismatch: {observed}"));
        }
    }
    Ok(())
}

fn main() {
    if std::env::args().any(|argument| argument == "--self-check") {
        match self_check() {
            Ok(()) => {
                println!("lazily-rs interop peer self-check: ok");
                return;
            }
            Err(error) => {
                eprintln!("lazily-rs interop peer self-check: {error}");
                std::process::exit(1);
            }
        }
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut peer = Peer::new();
    for line in stdin.lock().lines() {
        let response = match line {
            Ok(line) => match serde_json::from_str::<Value>(&line) {
                Ok(request) => {
                    let bye = request.get("cmd").and_then(Value::as_str) == Some("bye");
                    let response = peer.handle(&request);
                    if let Err(error) = write_response(&mut stdout, &response) {
                        eprintln!("write response: {error}");
                        std::process::exit(1);
                    }
                    if bye {
                        return;
                    }
                    continue;
                }
                Err(error) => self::error(format!("invalid JSON: {error}")),
            },
            Err(error) => {
                eprintln!("read request: {error}");
                std::process::exit(1);
            }
        };
        if let Err(error) = write_response(&mut stdout, &response) {
            eprintln!("write response: {error}");
            std::process::exit(1);
        }
    }
}

fn write_response(output: &mut impl Write, response: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *output, response)?;
    output.write_all(b"\n")?;
    output.flush()
}
