use std::time::{Duration, Instant};

use lazily::stdlib::{Timer, TimerError, TimerPoll};

#[test]
fn timer_poll_at_is_pending_then_fires_exactly_once() {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(5);
    let mut timer = Timer::at(deadline);

    assert_eq!(timer.deadline(), deadline);
    assert_eq!(
        timer.poll_at(start),
        TimerPoll::Pending {
            remaining: Duration::from_secs(5)
        }
    );
    assert!(!timer.has_fired());

    assert_eq!(
        timer.poll_at(deadline),
        TimerPoll::Fired { newly_fired: true }
    );
    assert!(timer.has_fired());
    assert_eq!(
        timer.poll_at(deadline + Duration::from_secs(1)),
        TimerPoll::Fired { newly_fired: false }
    );
}

#[test]
fn fired_timer_is_terminal_when_polled_with_an_earlier_instant() {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(1);
    let mut timer = Timer::at(deadline);

    assert_eq!(
        timer.poll_at(deadline),
        TimerPoll::Fired { newly_fired: true }
    );
    assert_eq!(
        timer.poll_at(start),
        TimerPoll::Fired { newly_fired: false }
    );
}

#[test]
fn zero_delay_wait_fires_without_an_async_runtime() {
    let mut timer = Timer::after(Duration::ZERO);

    assert_eq!(timer.wait(), TimerPoll::Fired { newly_fired: true });
    assert_eq!(timer.wait(), TimerPoll::Fired { newly_fired: false });
}

#[test]
fn deterministic_clock_regression_is_typed_and_does_not_change_state() {
    let start = Instant::now();
    let mut timer = Timer::try_after_at(start, Duration::from_secs(5)).unwrap();

    assert_eq!(
        timer.try_poll_at(start + Duration::from_secs(3)),
        Ok(TimerPoll::Pending {
            remaining: Duration::from_secs(2)
        })
    );
    assert_eq!(
        timer.try_poll_at(start + Duration::from_secs(2)),
        Err(TimerError::ClockRegression)
    );
    assert_eq!(
        timer.try_poll_at(start + Duration::from_secs(5)),
        Ok(TimerPoll::Fired { newly_fired: true })
    );
    assert_eq!(timer.fired_at(), Some(start + Duration::from_secs(5)));
}

#[test]
fn logical_tick_deadline_overflow_is_typed() {
    assert_eq!(
        Timer::checked_deadline_ticks(u64::MAX - 1, 2),
        Err(TimerError::DeadlineOverflow)
    );
}
