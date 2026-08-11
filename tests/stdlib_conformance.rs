mod common;

use std::cell::Cell;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use lazily::stdlib::{
    RevisionBarrier, RevisionCheck, RevisionWaitOutcome, Timeout, TimeoutCancellation,
    TimeoutOperation, TimeoutPoll, TimeoutUnavailableReason, Timer, TimerError, TimerPoll,
};
use serde_json::{Map, Value, json};

const FIXTURES: [(&str, common::SpecDir); 3] = [
    ("stdlib_timer_v1", common::SpecDir("stdlib/timer.json")),
    ("stdlib_timeout_v1", common::SpecDir("stdlib/timeout.json")),
    (
        "stdlib_revision_barrier_v1",
        common::SpecDir("stdlib/revision_barrier.json"),
    ),
];

fn load(path: &str) -> Value {
    serde_json::from_str(
        &common::spec_read_to_string(path).unwrap_or_else(|error| panic!("read {path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {path}: {error}"))
}

/// The fixture's scenarios, each RECORDED as replayed at the moment it is
/// yielded (`#lzscenariocoverage`). These three fixtures key their scenarios by
/// `id` rather than `name`, which is exactly why the ledger's id resolution is
/// `id` -> `name` -> positional and not "the name field".
fn scenarios<'a>(path: &str, fixture: &'a Value) -> common::Scenarios<'a> {
    common::scenarios(path, fixture)
}

fn steps(scenario: &Value) -> &[Value] {
    scenario["steps"].as_array().expect("scenario steps")
}

fn tick(base: Instant, logical: u64) -> Instant {
    base.checked_add(Duration::from_nanos(logical))
        .expect("small canonical logical instant must map to Instant")
}

fn fired_tick(base: Instant, timer: &Timer) -> u64 {
    timer
        .fired_at()
        .expect("fired timer records its edge")
        .duration_since(base)
        .as_nanos()
        .try_into()
        .expect("canonical tick fits u64")
}

#[test]
fn canonical_corpus_matches_production_and_an_independent_interpreter() {
    for (feature, spec) in FIXTURES {
        // Resolve once: `SpecDir` renders under whichever corpus this run reads,
        // and everything downstream wants a plain path.
        let path_owned = spec.to_string();
        let path = path_owned.as_str();
        let fixture = load(path);
        assert_eq!(fixture["feature"], feature);
        replay_production(path, &fixture);
        assert_eq!(
            independent_failures(path, &fixture, None),
            BTreeSet::new(),
            "{feature} independent interpreter diverged from the canonical corpus"
        );
    }
}

#[test]
fn every_declared_mutation_is_observed_by_the_independent_interpreter() {
    for (_, spec) in FIXTURES {
        let path_owned = spec.to_string();
        let path = path_owned.as_str();
        let fixture = load(path);
        for mutation in fixture["mutations"].as_array().expect("fixture mutations") {
            let operator = mutation["operator"].as_str().expect("mutation operator");
            let must_fail = mutation["must_fail"]
                .as_array()
                .expect("mutation must_fail")
                .iter()
                .map(|value| value.as_str().expect("scenario id").to_owned())
                .collect::<BTreeSet<_>>();
            let failed = independent_failures(path, &fixture, Some(operator));
            assert!(
                must_fail.is_subset(&failed),
                "{} mutation {operator:?} escaped scenarios {:?}",
                fixture["feature"],
                must_fail.difference(&failed).collect::<Vec<_>>()
            );
        }
    }
}

fn replay_production(path: &str, fixture: &Value) {
    match fixture["feature"].as_str().expect("feature") {
        "stdlib_timer_v1" => replay_timers(path, fixture),
        "stdlib_timeout_v1" => replay_timeouts(path, fixture),
        "stdlib_revision_barrier_v1" => replay_barriers(path, fixture),
        feature => panic!("unknown stdlib feature {feature}"),
    }
}

fn assert_step(feature: &str, scenario: &Value, index: usize, actual: Value) {
    assert_eq!(
        actual,
        scenario["steps"][index]["expect"],
        "{feature}/{} step {index}",
        scenario["id"].as_str().expect("scenario id")
    );
}

fn replay_timers(path: &str, fixture: &Value) {
    for (_index, _id, scenario) in scenarios(path, fixture) {
        let base = Instant::now();
        let mut timer = None;
        let mut logical_deadline = None;
        for (index, step) in steps(scenario.value()).iter().enumerate() {
            let actual = match step["op"].as_str().expect("timer op") {
                "start" => {
                    let now = step["now"].as_u64().expect("timer now");
                    let duration = step["duration"].as_u64().expect("timer duration");
                    match Timer::checked_deadline_ticks(now, duration) {
                        Err(TimerError::DeadlineOverflow) => {
                            json!({"outcome": "unavailable", "reason": "deadline_overflow"})
                        }
                        Err(TimerError::ClockRegression) => unreachable!(),
                        Ok(deadline) => {
                            logical_deadline = Some(deadline);
                            timer = Some(
                                Timer::try_after_at(
                                    tick(base, now),
                                    Duration::from_nanos(duration),
                                )
                                .expect("canonical timer deadline"),
                            );
                            json!({"outcome": "pending", "deadline": deadline})
                        }
                    }
                }
                "observe" => {
                    let now = step["now"].as_u64().expect("timer observation");
                    let timer = timer.as_mut().expect("timer started");
                    match timer.try_poll_at(tick(base, now)) {
                        Ok(TimerPoll::Pending { .. }) => {
                            json!({"outcome": "pending", "deadline": logical_deadline})
                        }
                        Ok(TimerPoll::Fired { .. }) => {
                            json!({"outcome": "fired", "fired_at": fired_tick(base, timer)})
                        }
                        Err(TimerError::ClockRegression) => json!({
                            "outcome": "unavailable",
                            "reason": "clock_regression",
                            "deadline": logical_deadline
                        }),
                        Err(TimerError::DeadlineOverflow) => unreachable!(),
                    }
                }
                op => panic!("unknown timer op {op}"),
            };
            assert_step("stdlib_timer_v1", scenario.value(), index, actual);
        }
    }
}

fn replay_timeouts(path: &str, fixture: &Value) {
    for (_index, _id, scenario) in scenarios(path, fixture) {
        let base = Instant::now();
        let mut timeout = None::<Timeout<String>>;
        let mut logical_deadline = None;
        for (index, step) in steps(scenario.value()).iter().enumerate() {
            let actual = match step["op"].as_str().expect("timeout op") {
                "start" => {
                    let now = step["now"].as_u64().expect("timeout now");
                    let duration = step["duration"].as_u64().expect("timeout duration");
                    match Timer::checked_deadline_ticks(now, duration) {
                        Err(TimerError::DeadlineOverflow) => {
                            json!({"outcome": "unavailable", "reason": "deadline_overflow"})
                        }
                        Err(TimerError::ClockRegression) => unreachable!(),
                        Ok(deadline) => {
                            logical_deadline = Some(deadline);
                            timeout = Some(
                                Timeout::try_after_at(
                                    tick(base, now),
                                    Duration::from_nanos(duration),
                                )
                                .expect("canonical timeout deadline"),
                            );
                            json!({"outcome": "pending", "deadline": deadline})
                        }
                    }
                }
                "poll" => {
                    let now = step["now"].as_u64().expect("timeout observation");
                    let operation_calls = Cell::new(0_u64);
                    let cancellation_calls = Cell::new(0_u64);
                    let operation = step["operation"].as_str().expect("operation");
                    let value = step.get("value").and_then(Value::as_str).map(str::to_owned);
                    let cancellation = step["cancellation"].as_str().expect("cancellation");
                    let timeout = timeout.as_mut().expect("timeout started");
                    let poll = timeout.poll_at_with_cancellation(
                        tick(base, now),
                        || {
                            operation_calls.set(operation_calls.get() + 1);
                            match operation {
                                "pending" => TimeoutOperation::Pending,
                                "completed" => TimeoutOperation::Completed(
                                    value.clone().expect("completed value"),
                                ),
                                "unavailable" => TimeoutOperation::Unavailable,
                                value => panic!("unknown operation {value}"),
                            }
                        },
                        || {
                            cancellation_calls.set(cancellation_calls.get() + 1);
                            match cancellation {
                                "pending" => TimeoutCancellation::Pending,
                                "cancelled" => TimeoutCancellation::Cancelled,
                                "unavailable" => TimeoutCancellation::Unavailable,
                                value => panic!("unknown cancellation {value}"),
                            }
                        },
                    );
                    let counts = (operation_calls.get(), cancellation_calls.get());
                    match poll {
                        TimeoutPoll::Pending { .. } => json!({
                            "outcome": "pending",
                            "deadline": logical_deadline,
                            "operation_calls": counts.0,
                            "cancellation_calls": counts.1
                        }),
                        TimeoutPoll::Completed(value) => json!({
                            "outcome": "completed",
                            "value": value,
                            "operation_calls": counts.0,
                            "cancellation_calls": counts.1
                        }),
                        TimeoutPoll::TimedOut => json!({
                            "outcome": "timed_out",
                            "operation_calls": counts.0,
                            "cancellation_calls": counts.1
                        }),
                        TimeoutPoll::Cancelled => json!({
                            "outcome": "cancelled",
                            "operation_calls": counts.0,
                            "cancellation_calls": counts.1
                        }),
                        TimeoutPoll::Unavailable => {
                            let reason = match timeout.unavailable_reason() {
                                Some(TimeoutUnavailableReason::Operation) => {
                                    "operation_unavailable"
                                }
                                Some(TimeoutUnavailableReason::Cancellation) => {
                                    "cancellation_unavailable"
                                }
                                Some(TimeoutUnavailableReason::ClockRegression) => {
                                    "clock_regression"
                                }
                                None => "operation_unavailable",
                            };
                            json!({
                                "outcome": "unavailable",
                                "reason": reason,
                                "operation_calls": counts.0,
                                "cancellation_calls": counts.1
                            })
                        }
                    }
                }
                op => panic!("unknown timeout op {op}"),
            };
            assert_step("stdlib_timeout_v1", scenario.value(), index, actual);
        }
    }
}

fn barrier_observation(outcome: &str, barrier: &RevisionBarrier, reason: Option<&str>) -> Value {
    let mut observation = json!({
        "outcome": outcome,
        "revision": barrier.revision(),
        "generation": barrier.generation()
    });
    if let Some(reason) = reason {
        observation["reason"] = json!(reason);
    }
    observation
}

fn map_wait(outcome: RevisionWaitOutcome, barrier: &RevisionBarrier) -> Value {
    match outcome {
        RevisionWaitOutcome::Satisfied { .. } => barrier_observation("satisfied", barrier, None),
        RevisionWaitOutcome::TimedOut { .. } => barrier_observation("timed_out", barrier, None),
        RevisionWaitOutcome::Cancelled { .. } => barrier_observation("cancelled", barrier, None),
        RevisionWaitOutcome::Disposed { .. } => barrier_observation("disposed", barrier, None),
        RevisionWaitOutcome::Unavailable { .. } => {
            let reason = if barrier.clock_regressed() {
                "clock_regression"
            } else {
                "cancellation_unavailable"
            };
            barrier_observation("unavailable", barrier, Some(reason))
        }
    }
}

fn replay_barriers(path: &str, fixture: &Value) {
    for (_index, _id, scenario) in scenarios(path, fixture) {
        let mut barrier = None;
        let mut required_revision = 0_u64;
        let mut deadline = None;
        for (index, step) in steps(scenario.value()).iter().enumerate() {
            let actual = match step["op"].as_str().expect("barrier op") {
                "start" => {
                    required_revision = step["required_revision"].as_u64().expect("required");
                    deadline = step["deadline"].as_u64();
                    barrier = Some(RevisionBarrier::new(
                        step["revision"].as_u64().expect("revision"),
                    ));
                    barrier_observation("pending", barrier.as_ref().unwrap(), None)
                }
                "register_recheck" => {
                    let current = barrier.as_ref().unwrap().clone();
                    if !current.observe_clock(step["now"].as_u64().expect("now")) {
                        let (outcome, reason) = if current.clock_regressed() {
                            ("unavailable", Some("clock_regression"))
                        } else {
                            ("disposed", None)
                        };
                        let actual = barrier_observation(outcome, &current, reason);
                        assert_step(
                            "stdlib_revision_barrier_v1",
                            scenario.value(),
                            index,
                            actual,
                        );
                        continue;
                    }
                    let observed = step["observed_revision"]
                        .as_u64()
                        .expect("observed revision");
                    let predicate = step["predicate"].as_bool().expect("predicate");
                    let after = required_revision.saturating_sub(1);
                    let _ = current.advance(observed);
                    if predicate && current.revision() >= required_revision {
                        map_wait(
                            current.wait_after(after, |_| RevisionCheck::Satisfied, None, None),
                            &current,
                        )
                    } else {
                        barrier_observation("pending", &current, None)
                    }
                }
                "advance" => {
                    let current = barrier.as_ref().unwrap();
                    let revision = step["revision"].as_u64().expect("revision");
                    let predicate = step["predicate"].as_bool().expect("predicate");
                    let _ = current.advance(revision);
                    if current.is_disposed() {
                        barrier_observation("disposed", current, None)
                    } else if predicate && current.revision() >= required_revision {
                        map_wait(
                            current.wait_after(
                                required_revision.saturating_sub(1),
                                |_| RevisionCheck::Satisfied,
                                None,
                                None,
                            ),
                            current,
                        )
                    } else {
                        barrier_observation("pending", current, None)
                    }
                }
                "observe" => {
                    let current = barrier.as_ref().unwrap();
                    let now = step["now"].as_u64().expect("now");
                    if !current.observe_clock(now) {
                        let (outcome, reason) = if current.clock_regressed() {
                            ("unavailable", Some("clock_regression"))
                        } else {
                            ("disposed", None)
                        };
                        let mut observation = barrier_observation(outcome, current, reason);
                        observation["cancellation_calls"] = json!(0);
                        observation
                    } else {
                        let reached = deadline.is_some_and(|value| now >= value);
                        let predicate = step["predicate"].as_bool().expect("predicate");
                        let cancellation = step["cancellation"].as_str().expect("cancellation");
                        let mut timer = reached.then(|| Timer::after(Duration::ZERO));
                        let owned = current.cancellation();
                        let foreign_barrier = RevisionBarrier::new(0);
                        let foreign = foreign_barrier.cancellation();
                        if !reached && cancellation == "cancelled" {
                            assert!(owned.cancel());
                        }
                        let token = match cancellation {
                            "cancelled" => Some(&owned),
                            "unavailable" => Some(&foreign),
                            "pending" => Some(&owned),
                            value => panic!("unknown cancellation {value}"),
                        };
                        let predicate_satisfied =
                            predicate && current.revision() >= required_revision;
                        let mut observation =
                            if reached || predicate_satisfied || cancellation != "pending" {
                                map_wait(
                                    current.wait_after(
                                        required_revision.saturating_sub(1),
                                        |_| {
                                            if predicate {
                                                RevisionCheck::Satisfied
                                            } else {
                                                RevisionCheck::Pending
                                            }
                                        },
                                        timer.as_mut(),
                                        token,
                                    ),
                                    current,
                                )
                            } else {
                                barrier_observation("pending", current, None)
                            };
                        observation["cancellation_calls"] =
                            json!(if reached || predicate_satisfied { 0 } else { 1 });
                        observation
                    }
                }
                "dispose" => {
                    let current = barrier.as_ref().unwrap();
                    let _ = current.dispose();
                    barrier_observation("disposed", current, None)
                }
                "receipt" => {
                    let current = barrier.as_ref().unwrap();
                    current.notify();
                    barrier_observation("pending", current, None)
                }
                op => panic!("unknown barrier op {op}"),
            };
            assert_step(
                "stdlib_revision_barrier_v1",
                scenario.value(),
                index,
                actual,
            );
        }
    }
}

fn independent_failures(path: &str, fixture: &Value, mutation: Option<&str>) -> BTreeSet<String> {
    let feature = fixture["feature"].as_str().expect("feature");
    let mut failures = BTreeSet::new();
    for (_index, _id, scenario) in scenarios(path, fixture) {
        let mut state = Map::new();
        for step in steps(scenario.value()) {
            let actual = independent_step(feature, &mut state, step, mutation);
            if actual != step["expect"] {
                failures.insert(scenario["id"].as_str().expect("scenario id").to_owned());
            }
        }
    }
    failures
}

fn independent_step(
    feature: &str,
    state: &mut Map<String, Value>,
    step: &Value,
    mutation: Option<&str>,
) -> Value {
    match feature {
        "stdlib_timer_v1" => independent_timer(state, step, mutation),
        "stdlib_timeout_v1" => independent_timeout(state, step, mutation),
        "stdlib_revision_barrier_v1" => independent_barrier(state, step, mutation),
        value => panic!("unknown feature {value}"),
    }
}

fn terminal(state: &Map<String, Value>, adapter_counts: bool) -> Value {
    let mut result = Map::from_iter([(
        "outcome".to_owned(),
        state.get("status").expect("status").clone(),
    )]);
    for key in ["fired_at", "value", "reason", "revision", "generation"] {
        if let Some(value) = state.get(key) {
            result.insert(key.to_owned(), value.clone());
        }
    }
    if adapter_counts {
        result.insert("operation_calls".to_owned(), json!(0));
        result.insert("cancellation_calls".to_owned(), json!(0));
    }
    Value::Object(result)
}

/// The step's op, read and CHECKED against the ops this model implements
/// (`#lzscenariobodyskip`).
///
/// The independent models used to test `step["op"] == "start"` and then let
/// everything else fall through to the second op WITHOUT checking it. An op
/// this model does not implement was therefore replayed as an observe/poll —
/// the model advanced, the fixture's `expected` block was compared against the
/// wrong transition, and nothing named the unknown op.
fn model_op<'a>(step: &'a Value, known: &[&str]) -> &'a str {
    let op = step["op"]
        .as_str()
        .unwrap_or_else(|| panic!("step carries no `op`: {step}"));
    assert!(
        known.contains(&op),
        "unknown model op `{op}` (known: {known:?}) in {step}"
    );
    op
}

fn independent_timer(
    state: &mut Map<String, Value>,
    step: &Value,
    mutation: Option<&str>,
) -> Value {
    if model_op(step, &["start", "observe"]) == "start" {
        let now = step["now"].as_u64().unwrap();
        let duration = step["duration"].as_u64().unwrap();
        let Some(deadline) = now.checked_add(duration) else {
            state.insert("status".into(), json!("unavailable"));
            state.insert("reason".into(), json!("deadline_overflow"));
            return terminal(state, false);
        };
        state.extend([
            ("status".into(), json!("pending")),
            ("deadline".into(), json!(deadline)),
            ("last_now".into(), json!(now)),
        ]);
        return json!({"outcome": "pending", "deadline": deadline});
    }
    if mutation == Some("fixture_bookkeeping") {
        return json!({"outcome": "pending", "deadline": state.get("deadline")});
    }
    if state["status"] != "pending" && mutation != Some("terminal_not_latched") {
        return terminal(state, false);
    }
    if mutation == Some("terminal_not_latched") {
        state.insert("status".into(), json!("pending"));
    }
    let now = step["now"].as_u64().unwrap();
    let last = state["last_now"].as_u64().unwrap();
    if now < last {
        return json!({
            "outcome": "unavailable",
            "reason": "clock_regression",
            "deadline": state["deadline"]
        });
    }
    state.insert("last_now".into(), json!(now));
    let deadline = state["deadline"].as_u64().unwrap();
    let reached = if mutation == Some("deadline_strict_greater") {
        now > deadline
    } else {
        now >= deadline
    };
    if reached {
        state.insert("status".into(), json!("fired"));
        state.insert("fired_at".into(), json!(now));
        terminal(state, false)
    } else {
        json!({"outcome": "pending", "deadline": deadline})
    }
}

fn independent_timeout(
    state: &mut Map<String, Value>,
    step: &Value,
    mutation: Option<&str>,
) -> Value {
    if model_op(step, &["start", "poll"]) == "start" {
        let now = step["now"].as_u64().unwrap();
        let duration = step["duration"].as_u64().unwrap();
        let Some(deadline) = now.checked_add(duration) else {
            state.insert("status".into(), json!("unavailable"));
            state.insert("reason".into(), json!("deadline_overflow"));
            return terminal(state, false);
        };
        state.extend([
            ("status".into(), json!("pending")),
            ("deadline".into(), json!(deadline)),
            ("last_now".into(), json!(now)),
        ]);
        return json!({"outcome": "pending", "deadline": deadline});
    }
    if mutation == Some("fixture_bookkeeping") {
        return json!({
            "outcome": "pending",
            "deadline": state.get("deadline"),
            "operation_calls": 0,
            "cancellation_calls": 0
        });
    }
    if state["status"] != "pending" && mutation != Some("terminal_not_latched") {
        return terminal(state, true);
    }
    if mutation == Some("terminal_not_latched") {
        state.insert("status".into(), json!("pending"));
    }
    let now = step["now"].as_u64().unwrap();
    let deadline = state["deadline"].as_u64().unwrap();
    if now < state["last_now"].as_u64().unwrap() {
        state.insert("status".into(), json!("unavailable"));
        state.insert("reason".into(), json!("clock_regression"));
        return json!({
            "outcome": "unavailable",
            "reason": "clock_regression",
            "operation_calls": 0,
            "cancellation_calls": 0
        });
    }
    state.insert("last_now".into(), json!(now));
    let reached = if mutation == Some("deadline_strict_greater") {
        now > deadline
    } else {
        now >= deadline
    };
    if reached {
        state.insert("status".into(), json!("timed_out"));
        return json!({"outcome": "timed_out", "operation_calls": 0, "cancellation_calls": 0});
    }
    // Both drive if-chains whose tail ASSUMES `pending`; validate the spelling
    // so an unknown one names itself instead of quietly meaning "pending"
    // (`#lzscenariobodyskip`).
    let operation = match step["operation"].as_str().unwrap() {
        known @ ("completed" | "pending" | "unavailable") => known,
        other => panic!("unknown operation `{other}` in {step}"),
    };
    let cancellation = match step["cancellation"].as_str().unwrap() {
        known @ ("cancelled" | "pending" | "unavailable") => known,
        other => panic!("unknown cancellation `{other}` in {step}"),
    };
    if mutation == Some("cancellation_before_completion") && cancellation == "cancelled" {
        state.insert("status".into(), json!("cancelled"));
        return json!({"outcome": "cancelled", "operation_calls": 1, "cancellation_calls": 1});
    }
    if operation == "completed" {
        state.insert("status".into(), json!("completed"));
        state.insert("value".into(), step["value"].clone());
        return json!({
            "outcome": "completed",
            "value": step["value"],
            "operation_calls": 1,
            "cancellation_calls": 1
        });
    }
    if operation == "unavailable" {
        state.insert("status".into(), json!("unavailable"));
        state.insert("reason".into(), json!("operation_unavailable"));
        return json!({
            "outcome": "unavailable",
            "reason": "operation_unavailable",
            "operation_calls": 1,
            "cancellation_calls": 1
        });
    }
    if cancellation == "cancelled" {
        state.insert("status".into(), json!("cancelled"));
        return json!({"outcome": "cancelled", "operation_calls": 1, "cancellation_calls": 1});
    }
    if cancellation == "unavailable" {
        state.insert("status".into(), json!("unavailable"));
        state.insert("reason".into(), json!("cancellation_unavailable"));
        return json!({
            "outcome": "unavailable",
            "reason": "cancellation_unavailable",
            "operation_calls": 1,
            "cancellation_calls": 1
        });
    }
    json!({
        "outcome": "pending",
        "deadline": deadline,
        "operation_calls": 1,
        "cancellation_calls": 1
    })
}

fn model_barrier_observation(state: &Map<String, Value>) -> Value {
    let mut result = json!({
        "outcome": state["status"],
        "revision": state["revision"],
        "generation": state["generation"]
    });
    if let Some(reason) = state.get("reason") {
        result["reason"] = reason.clone();
    }
    result
}

fn independent_barrier(
    state: &mut Map<String, Value>,
    step: &Value,
    mutation: Option<&str>,
) -> Value {
    let op = step["op"].as_str().unwrap();
    if op == "start" {
        state.extend([
            ("status".into(), json!("pending")),
            ("revision".into(), step["revision"].clone()),
            ("generation".into(), json!(0)),
            ("required".into(), step["required_revision"].clone()),
            ("deadline".into(), step["deadline"].clone()),
            ("last_now".into(), Value::Null),
        ]);
        return model_barrier_observation(state);
    }
    if mutation == Some("fixture_bookkeeping") {
        state.insert("status".into(), json!("pending"));
        return model_barrier_observation(state);
    }
    if state["status"] != "pending" && mutation != Some("terminal_not_latched") {
        let mut result = model_barrier_observation(state);
        if op == "observe" {
            result["cancellation_calls"] = json!(0);
        }
        return result;
    }
    if mutation == Some("terminal_not_latched") {
        state.insert("status".into(), json!("pending"));
    }
    if op == "dispose" {
        state.insert("status".into(), json!("disposed"));
        return model_barrier_observation(state);
    }
    if op == "receipt" {
        if mutation == Some("receipt_is_authority") {
            state.insert("revision".into(), state["required"].clone());
            state.insert(
                "generation".into(),
                json!(state["generation"].as_u64().unwrap() + 1),
            );
            state.insert("status".into(), json!("satisfied"));
        }
        return model_barrier_observation(state);
    }
    if op == "advance" {
        let revision = state["revision"]
            .as_u64()
            .unwrap()
            .max(step["revision"].as_u64().unwrap());
        state.insert("revision".into(), json!(revision));
        state.insert(
            "generation".into(),
            json!(state["generation"].as_u64().unwrap() + 1),
        );
        if revision >= state["required"].as_u64().unwrap() && step["predicate"] == true {
            state.insert("status".into(), json!("satisfied"));
        }
        return model_barrier_observation(state);
    }
    if op == "register_recheck" || op == "observe" {
        let now = step["now"].as_u64().unwrap();
        if state["last_now"]
            .as_u64()
            .is_some_and(|last_now| now < last_now)
            && mutation != Some("barrier_accept_clock_regression")
        {
            state.insert("status".into(), json!("unavailable"));
            state.insert("reason".into(), json!("clock_regression"));
            let mut result = model_barrier_observation(state);
            if op == "observe" {
                result["cancellation_calls"] = json!(0);
            }
            return result;
        }
        state.insert("last_now".into(), json!(now));
    }
    if op == "register_recheck" {
        state.insert(
            "generation".into(),
            json!(state["generation"].as_u64().unwrap() + 1),
        );
        if mutation != Some("barrier_skip_post_registration_recheck") {
            let revision = state["revision"]
                .as_u64()
                .unwrap()
                .max(step["observed_revision"].as_u64().unwrap());
            state.insert("revision".into(), json!(revision));
            if revision >= state["required"].as_u64().unwrap() && step["predicate"] == true {
                state.insert("status".into(), json!("satisfied"));
            }
        }
        return model_barrier_observation(state);
    }
    assert_eq!(op, "observe");
    let now = step["now"].as_u64().unwrap();
    let reached = state["deadline"].as_u64().is_some_and(|deadline| {
        if mutation == Some("deadline_strict_greater") {
            now > deadline
        } else {
            now >= deadline
        }
    });
    if reached {
        state.insert("status".into(), json!("timed_out"));
        let mut result = model_barrier_observation(state);
        result["cancellation_calls"] = json!(0);
        return result;
    }
    if state["revision"].as_u64().unwrap() >= state["required"].as_u64().unwrap()
        && step["predicate"] == true
    {
        state.insert("status".into(), json!("satisfied"));
        let mut result = model_barrier_observation(state);
        result["cancellation_calls"] = json!(0);
        return result;
    }
    // Fail-closed tail (`#lzscenariobodyskip`): the chain had no final `else`,
    // so a cancellation spelling this model does not know silently behaved like
    // `pending` and the barrier observation was asserted against it.
    match step["cancellation"].as_str().expect("cancellation") {
        "cancelled" => {
            state.insert("status".into(), json!("cancelled"));
        }
        "unavailable" => {
            state.insert("status".into(), json!("unavailable"));
            state.insert("reason".into(), json!("cancellation_unavailable"));
        }
        "pending" => {}
        other => panic!("unknown cancellation `{other}` in {step}"),
    }
    let mut result = model_barrier_observation(state);
    result["cancellation_calls"] = json!(1);
    result
}
