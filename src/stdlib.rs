//! Lazily standard-library conveniences built on top of the portable
//! primitives.
//!
//! The graph kernel stays runtime-agnostic and deterministic. This layer may
//! bind those primitives to facilities from Rust's standard library, while
//! retaining zero mandatory async-runtime dependencies.

use std::thread;
use std::time::{Duration, Instant};

use crate::{TimelineSource, TimerCore};

/// The observed state of a [`Timer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerPoll {
    /// The deadline has not arrived.
    Pending {
        /// Time remaining at the instant used for this poll.
        remaining: Duration,
    },
    /// The timer has reached its terminal state.
    Fired {
        /// `true` only for the poll that advances the underlying
        /// [`TimerCore`] across its fire edge.
        newly_fired: bool,
    },
}

/// A blocking/pollable wall-clock adapter over the logical [`TimerCore`].
///
/// `TimerCore` remains the portable primitive: it knows only monotone logical
/// ticks. `Timer` supplies the standard-library binding by mapping
/// [`Instant`]s before the deadline to logical tick `0` and instants at or
/// after it to tick `1`.
///
/// This type does not spawn a thread. Callers can poll it from an existing
/// loop, use [`wait`](Self::wait) for a blocking one-shot, or use
/// [`poll_at`](Self::poll_at) with a controlled instant in deterministic tests.
#[derive(Debug)]
pub struct Timer {
    deadline: Instant,
    core: TimerCore,
}

impl Timer {
    /// Create a timer that fires after `delay`.
    ///
    /// # Panics
    ///
    /// Panics if `delay` is too large to represent as an [`Instant`].
    pub fn after(delay: Duration) -> Self {
        let deadline = Instant::now()
            .checked_add(delay)
            .expect("timer delay exceeds the Instant range");
        Self::at(deadline)
    }

    /// Create a timer for an absolute monotone-clock `deadline`.
    pub fn at(deadline: Instant) -> Self {
        Self {
            deadline,
            core: TimerCore::new(1),
        }
    }

    /// The absolute deadline supplied at construction.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Whether the terminal fire has already been observed.
    pub fn has_fired(&self) -> bool {
        self.core.fired()
    }

    /// Poll using the current standard-library monotone clock.
    pub fn poll(&mut self) -> TimerPoll {
        self.poll_at(Instant::now())
    }

    /// Poll at an explicitly supplied monotone instant.
    ///
    /// This is the deterministic clock seam for schedulers and tests. Once
    /// fired, the timer remains terminal even if a later call supplies an
    /// earlier instant.
    pub fn poll_at(&mut self, now: Instant) -> TimerPoll {
        if self.core.fired() {
            return TimerPoll::Fired { newly_fired: false };
        }

        if now < self.deadline {
            return TimerPoll::Pending {
                remaining: self.deadline.duration_since(now),
            };
        }

        let newly_fired = self.core.tick(1);
        TimerPoll::Fired { newly_fired }
    }

    /// Block the current thread until the timer fires.
    ///
    /// The returned state is always [`TimerPoll::Fired`]. If the timer was
    /// already terminal, it returns immediately with `newly_fired: false`.
    pub fn wait(&mut self) -> TimerPoll {
        loop {
            match self.poll() {
                TimerPoll::Pending { remaining } => thread::sleep(remaining),
                fired @ TimerPoll::Fired { .. } => return fired,
            }
        }
    }
}
