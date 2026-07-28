use std::time::{Duration, Instant};

use lazily::stdlib::{Timer, TimerPoll};

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
