//! Lazily standard-library conveniences built on top of the portable
//! primitives.
//!
//! The graph kernel stays runtime-agnostic and deterministic. This layer may
//! bind those primitives to facilities from Rust's standard library, while
//! retaining zero mandatory async-runtime dependencies.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
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

/// The result of evaluating a [`RevisionBarrier`] predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionCheck {
    /// The observed state does not satisfy the predicate yet.
    Pending,
    /// The observed state satisfies the predicate.
    Satisfied,
    /// The state required to evaluate the predicate is unavailable.
    Unavailable,
}

/// The terminal outcome of a [`RevisionBarrier::wait_after`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionWaitOutcome {
    /// A newer revision satisfied the derived predicate.
    Satisfied { revision: u64 },
    /// The deadline elapsed before the predicate was satisfied.
    TimedOut { revision: u64 },
    /// The barrier-owned cancellation input was triggered.
    Cancelled { revision: u64 },
    /// The barrier was disposed while waiting.
    Disposed { revision: u64 },
    /// The predicate state or a supplied cancellation input was unavailable.
    Unavailable { revision: u64 },
}

#[derive(Debug)]
struct RevisionBarrierState {
    revision: u64,
    generation: u64,
    disposed: bool,
}

#[derive(Debug)]
struct RevisionBarrierInner {
    state: Mutex<RevisionBarrierState>,
    changed: Condvar,
}

/// A cancellation input bound to one [`RevisionBarrier`].
///
/// Cancellation is latched. Triggering it wakes waiters without requiring a
/// polling interval or an async runtime.
#[derive(Debug, Clone)]
pub struct BarrierCancellation {
    cancelled: Arc<AtomicBool>,
    barrier: Weak<RevisionBarrierInner>,
}

impl BarrierCancellation {
    /// Trigger cancellation and wake every waiter on the owning barrier.
    ///
    /// Returns `true` only for the first call that transitions the token.
    pub fn cancel(&self) -> bool {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return false;
        }

        if let Some(barrier) = self.barrier.upgrade() {
            let mut state = barrier
                .state
                .lock()
                .expect("revision barrier mutex poisoned");
            state.generation = state.generation.wrapping_add(1);
            barrier.changed.notify_all();
        }
        true
    }

    /// Whether cancellation has been triggered.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// A blocking bridge between monotone revisions and derived reactive state.
///
/// Producers call [`advance`](Self::advance) when a revision commits and
/// [`notify`](Self::notify) when derived state (such as an external effect
/// receipt ledger) changes at the current revision. Waiters require a revision
/// newer than their captured `after_revision` plus a satisfied predicate.
///
/// Predicate callbacks run outside the barrier mutex. A generation check closes
/// the check-to-sleep lost-wakeup window when revisions, receipts, cancellation,
/// or disposal race a callback.
#[derive(Debug, Clone)]
pub struct RevisionBarrier {
    inner: Arc<RevisionBarrierInner>,
}

impl RevisionBarrier {
    /// Create a barrier at `initial_revision`.
    pub fn new(initial_revision: u64) -> Self {
        Self {
            inner: Arc::new(RevisionBarrierInner {
                state: Mutex::new(RevisionBarrierState {
                    revision: initial_revision,
                    generation: 0,
                    disposed: false,
                }),
                changed: Condvar::new(),
            }),
        }
    }

    /// Return the latest published revision.
    pub fn revision(&self) -> u64 {
        self.inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned")
            .revision
    }

    /// Publish a strictly newer revision and wake waiters.
    ///
    /// Returns `false` for stale or duplicate revisions and after disposal.
    pub fn advance(&self, revision: u64) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned");
        if state.disposed || revision <= state.revision {
            return false;
        }

        state.revision = revision;
        state.generation = state.generation.wrapping_add(1);
        self.inner.changed.notify_all();
        true
    }

    /// Wake waiters after derived state changes at the current revision.
    ///
    /// Receipt storage and transport remain application-owned; update them
    /// before calling this method.
    pub fn notify(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned");
        if state.disposed {
            return;
        }

        state.generation = state.generation.wrapping_add(1);
        self.inner.changed.notify_all();
    }

    /// Create a cancellation input owned by this barrier.
    pub fn cancellation(&self) -> BarrierCancellation {
        BarrierCancellation {
            cancelled: Arc::new(AtomicBool::new(false)),
            barrier: Arc::downgrade(&self.inner),
        }
    }

    /// Dispose the barrier and wake all waiters.
    ///
    /// Returns `true` only for the transition into the disposed state.
    pub fn dispose(&self) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned");
        if state.disposed {
            return false;
        }

        state.disposed = true;
        state.generation = state.generation.wrapping_add(1);
        self.inner.changed.notify_all();
        true
    }

    /// Whether this barrier has been disposed.
    pub fn is_disposed(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned")
            .disposed
    }

    /// Wait for a newer revision whose derived state satisfies `check`.
    ///
    /// `deadline` uses the standard-library [`Timer`] as a deadline budget.
    /// Cancellation inputs must have been created by this barrier; a token from
    /// another barrier returns [`RevisionWaitOutcome::Unavailable`].
    pub fn wait_after<F>(
        &self,
        after_revision: u64,
        mut check: F,
        mut deadline: Option<&mut Timer>,
        cancellation: Option<&BarrierCancellation>,
    ) -> RevisionWaitOutcome
    where
        F: FnMut(u64) -> RevisionCheck,
    {
        if cancellation
            .is_some_and(|token| !Weak::ptr_eq(&token.barrier, &Arc::downgrade(&self.inner)))
        {
            return RevisionWaitOutcome::Unavailable {
                revision: self.revision(),
            };
        }

        let mut state = self
            .inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned");

        loop {
            if state.disposed {
                return RevisionWaitOutcome::Disposed {
                    revision: state.revision,
                };
            }
            if cancellation.is_some_and(BarrierCancellation::is_cancelled) {
                return RevisionWaitOutcome::Cancelled {
                    revision: state.revision,
                };
            }

            if state.revision > after_revision {
                let revision = state.revision;
                let generation = state.generation;
                drop(state);
                let checked = check(revision);
                state = self
                    .inner
                    .state
                    .lock()
                    .expect("revision barrier mutex poisoned");

                if state.generation != generation {
                    continue;
                }

                match checked {
                    RevisionCheck::Satisfied => {
                        return RevisionWaitOutcome::Satisfied { revision };
                    }
                    RevisionCheck::Unavailable => {
                        return RevisionWaitOutcome::Unavailable { revision };
                    }
                    RevisionCheck::Pending => {}
                }
            }

            match deadline.as_deref_mut().map(Timer::poll) {
                Some(TimerPoll::Fired { .. }) => {
                    return RevisionWaitOutcome::TimedOut {
                        revision: state.revision,
                    };
                }
                Some(TimerPoll::Pending { remaining }) => {
                    let (next_state, _) = self
                        .inner
                        .changed
                        .wait_timeout(state, remaining)
                        .expect("revision barrier mutex poisoned");
                    state = next_state;
                }
                None => {
                    state = self
                        .inner
                        .changed
                        .wait(state)
                        .expect("revision barrier mutex poisoned");
                }
            }
        }
    }
}
