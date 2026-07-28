use std::cell::Cell;
use std::time::{Duration, Instant};

use lazily::stdlib::{
    Timeout, TimeoutCancellation, TimeoutOperation, TimeoutOutcome, TimeoutPoll,
    TimeoutUnavailableReason,
};

#[test]
fn completion_before_deadline_is_latched() {
    let start = Instant::now();
    let mut timeout = Timeout::at(start + Duration::from_secs(5));
    let operation_calls = Cell::new(0);

    assert_eq!(
        timeout.poll_at(
            start,
            || {
                operation_calls.set(operation_calls.get() + 1);
                TimeoutOperation::Completed("ready")
            },
            || false,
        ),
        TimeoutPoll::Completed(&"ready")
    );
    assert_eq!(timeout.outcome(), Some(&TimeoutOutcome::Completed("ready")));
    assert!(timeout.is_terminal());

    assert_eq!(
        timeout.poll_at(
            start + Duration::from_secs(10),
            || panic!("terminal timeout polled the operation again"),
            || panic!("terminal timeout polled cancellation again"),
        ),
        TimeoutPoll::Completed(&"ready")
    );
    assert_eq!(operation_calls.get(), 1);
}

#[test]
fn pending_operation_times_out_at_the_deadline() {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(3);
    let mut timeout = Timeout::<()>::at(deadline);

    assert_eq!(
        timeout.poll_at(start, || TimeoutOperation::Pending, || false),
        TimeoutPoll::Pending {
            remaining: Duration::from_secs(3)
        }
    );
    assert_eq!(
        timeout.poll_at(deadline, || TimeoutOperation::Pending, || false),
        TimeoutPoll::TimedOut
    );
    assert_eq!(timeout.outcome(), Some(&TimeoutOutcome::TimedOut));

    assert_eq!(
        timeout.poll_at(
            deadline + Duration::from_secs(1),
            || panic!("terminal timeout polled the operation again"),
            || panic!("terminal timeout polled cancellation again"),
        ),
        TimeoutPoll::TimedOut
    );
}

#[test]
fn completion_wins_a_pre_deadline_cancellation_race() {
    let start = Instant::now();
    let mut completed = Timeout::at(start + Duration::from_secs(1));

    assert_eq!(
        completed.poll_at(start, || TimeoutOperation::Completed(42), || true,),
        TimeoutPoll::Completed(&42)
    );

    let mut cancelled = Timeout::<()>::at(start + Duration::from_secs(1));
    assert_eq!(
        cancelled.poll_at(start, || TimeoutOperation::Pending, || true),
        TimeoutPoll::Cancelled
    );
    assert_eq!(cancelled.outcome(), Some(&TimeoutOutcome::Cancelled));
}

#[test]
fn zero_duration_is_immediately_timed_out() {
    let now = Instant::now();
    let mut timeout = Timeout::<()>::at(now);

    assert_eq!(
        timeout.poll_at(
            now,
            || panic!("zero-duration timeout polled the operation"),
            || panic!("zero-duration timeout polled cancellation"),
        ),
        TimeoutPoll::TimedOut
    );
}

#[test]
fn unavailable_operation_is_a_latched_terminal_outcome() {
    let start = Instant::now();
    let mut timeout = Timeout::<()>::at(start + Duration::from_secs(1));

    assert_eq!(
        timeout.poll_at(start, || TimeoutOperation::Unavailable, || false),
        TimeoutPoll::Unavailable
    );
    assert_eq!(timeout.outcome(), Some(&TimeoutOutcome::Unavailable));
    assert_eq!(
        timeout.poll_at(
            start,
            || panic!("terminal timeout polled the operation again"),
            || panic!("terminal timeout polled cancellation again"),
        ),
        TimeoutPoll::Unavailable
    );
}

#[test]
fn wait_with_exposes_deterministic_clock_and_wait_seams() {
    let start = Instant::now();
    let clock = Cell::new(start);
    let ready = Cell::new(false);
    let wait_calls = Cell::new(0);
    let mut timeout = Timeout::at(start + Duration::from_secs(5));

    let outcome = timeout.wait_with(
        || {
            if ready.get() {
                TimeoutOperation::Completed("done")
            } else {
                TimeoutOperation::Pending
            }
        },
        || false,
        || clock.get(),
        |remaining| {
            assert_eq!(remaining, Duration::from_secs(5));
            wait_calls.set(wait_calls.get() + 1);
            ready.set(true);
            clock.set(start + Duration::from_secs(1));
        },
    );

    assert_eq!(outcome, &TimeoutOutcome::Completed("done"));
    assert_eq!(wait_calls.get(), 1);
}

#[test]
fn typed_cancellation_unavailability_is_latched() {
    let start = Instant::now();
    let mut timeout = Timeout::<()>::try_after_at(start, Duration::from_secs(1)).unwrap();

    assert_eq!(
        timeout.poll_at_with_cancellation(
            start,
            || TimeoutOperation::Pending,
            || TimeoutCancellation::Unavailable,
        ),
        TimeoutPoll::Unavailable
    );
    assert_eq!(
        timeout.unavailable_reason(),
        Some(TimeoutUnavailableReason::Cancellation)
    );
}

#[test]
fn completion_observes_cancellation_once_but_wins_the_race() {
    let start = Instant::now();
    let operation_calls = Cell::new(0);
    let cancellation_calls = Cell::new(0);
    let mut timeout = Timeout::try_after_at(start, Duration::from_secs(1)).unwrap();

    assert_eq!(
        timeout.poll_at_with_cancellation(
            start,
            || {
                operation_calls.set(operation_calls.get() + 1);
                TimeoutOperation::Completed(42)
            },
            || {
                cancellation_calls.set(cancellation_calls.get() + 1);
                TimeoutCancellation::Cancelled
            },
        ),
        TimeoutPoll::Completed(&42)
    );
    assert_eq!(operation_calls.get(), 1);
    assert_eq!(cancellation_calls.get(), 1);
}
