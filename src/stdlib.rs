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

/// A typed failure from a deterministic [`Timer`] clock seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerError {
    /// Adding a duration would exceed the selected clock representation.
    DeadlineOverflow,
    /// A caller-driven clock moved behind its last accepted observation.
    ClockRegression,
}

impl std::fmt::Display for TimerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeadlineOverflow => formatter.write_str("timer deadline overflow"),
            Self::ClockRegression => formatter.write_str("timer clock regressed"),
        }
    }
}

impl std::error::Error for TimerError {}

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
    last_observed: Option<Instant>,
    fired_at: Option<Instant>,
}

impl Timer {
    /// Create a timer that fires after `delay`.
    ///
    /// # Panics
    ///
    /// Panics if `delay` is too large to represent as an [`Instant`].
    pub fn after(delay: Duration) -> Self {
        Self::try_after(delay).expect("timer delay exceeds the Instant range")
    }

    /// Create a timer after `delay`, returning a typed overflow instead of
    /// panicking.
    pub fn try_after(delay: Duration) -> Result<Self, TimerError> {
        Self::try_after_at(Instant::now(), delay)
    }

    /// Create a timer from an explicitly supplied clock observation.
    ///
    /// This constructor and [`try_poll_at`](Self::try_poll_at) form the
    /// deterministic monotone-clock seam used by conformance runners.
    pub fn try_after_at(now: Instant, delay: Duration) -> Result<Self, TimerError> {
        let deadline = now.checked_add(delay).ok_or(TimerError::DeadlineOverflow)?;
        Ok(Self {
            deadline,
            core: TimerCore::new(1),
            last_observed: Some(now),
            fired_at: None,
        })
    }

    /// Checked addition for portable logical-tick fixture clocks.
    ///
    /// Wall-clock adapters still use [`Instant`], but peer and conformance
    /// adapters can validate their `u64` logical timeline before mapping it to
    /// an `Instant`.
    pub fn checked_deadline_ticks(now: u64, duration: u64) -> Result<u64, TimerError> {
        now.checked_add(duration)
            .ok_or(TimerError::DeadlineOverflow)
    }

    /// Create a timer for an absolute monotone-clock `deadline`.
    pub fn at(deadline: Instant) -> Self {
        Self {
            deadline,
            core: TimerCore::new(1),
            last_observed: None,
            fired_at: None,
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

    /// The clock instant that first observed the terminal fire edge.
    pub fn fired_at(&self) -> Option<Instant> {
        self.fired_at
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
        self.poll_at_unchecked(now)
    }

    /// Poll an explicitly supplied monotone clock and reject regressions
    /// without changing timer state.
    pub fn try_poll_at(&mut self, now: Instant) -> Result<TimerPoll, TimerError> {
        if self.core.fired() {
            return Ok(TimerPoll::Fired { newly_fired: false });
        }
        if self.last_observed.is_some_and(|last| now < last) {
            return Err(TimerError::ClockRegression);
        }
        self.last_observed = Some(now);
        Ok(self.poll_at_unchecked(now))
    }

    fn poll_at_unchecked(&mut self, now: Instant) -> TimerPoll {
        if self.core.fired() {
            return TimerPoll::Fired { newly_fired: false };
        }
        if now < self.deadline {
            return TimerPoll::Pending {
                remaining: self.deadline.duration_since(now),
            };
        }

        let newly_fired = self.core.tick(1);
        if newly_fired {
            self.fired_at = Some(now);
        }
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

/// The caller-observed state of an operation wrapped by [`Timeout`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeoutOperation<T> {
    /// The operation has not completed yet.
    Pending,
    /// The operation completed with a value.
    Completed(T),
    /// The operation can no longer be observed.
    Unavailable,
}

/// The caller-observed state of a cancellation input wrapped by [`Timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutCancellation {
    /// Cancellation is available and has not been requested.
    Pending,
    /// Cancellation was requested.
    Cancelled,
    /// The cancellation source can no longer be observed.
    Unavailable,
}

/// Why a [`Timeout`] resolved to [`TimeoutOutcome::Unavailable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutUnavailableReason {
    /// The operation adapter became unavailable.
    Operation,
    /// The cancellation adapter became unavailable.
    Cancellation,
    /// The caller-driven monotone clock regressed.
    ClockRegression,
}

/// A latched terminal outcome stored by [`Timeout`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeoutOutcome<T> {
    /// The operation completed before the deadline.
    Completed(T),
    /// The monotone deadline was reached.
    TimedOut,
    /// Cancellation was observed before the deadline.
    Cancelled,
    /// The caller-supplied operation became unavailable.
    Unavailable,
}

/// The result of polling a [`Timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutPoll<'a, T> {
    /// Neither the operation nor the deadline is terminal.
    Pending {
        /// Time remaining at the instant used for this poll.
        remaining: Duration,
    },
    /// The operation completed before the deadline.
    Completed(&'a T),
    /// The monotone deadline was reached.
    TimedOut,
    /// Cancellation was observed before the deadline.
    Cancelled,
    /// The caller-supplied operation became unavailable.
    Unavailable,
}

/// A caller-driven timeout adapter composed from [`Timer`].
///
/// `Timeout` owns no operation, executor, task, or thread. Instead,
/// [`poll`](Self::poll), [`poll_at`](Self::poll_at), and
/// [`wait_with`](Self::wait_with) accept caller-supplied adapters for an
/// operation or future and its cancellation state. This keeps runtime and wait
/// policy outside the Lazily graph kernel.
///
/// The deadline is strict: a poll at or after it resolves to
/// [`TimeoutOutcome::TimedOut`] without polling the operation. Before the
/// deadline, completion wins if completion and cancellation are both observed
/// by the same poll. Every terminal outcome is latched, so later reads do not
/// call any supplied adapter again.
#[derive(Debug)]
pub struct Timeout<T> {
    timer: Timer,
    outcome: Option<TimeoutOutcome<T>>,
    unavailable_reason: Option<TimeoutUnavailableReason>,
}

impl<T> Timeout<T> {
    /// Create a timeout whose deadline is `delay` from now.
    ///
    /// # Panics
    ///
    /// Panics if `delay` is too large to represent as an [`Instant`].
    pub fn after(delay: Duration) -> Self {
        Self::from_timer(Timer::after(delay))
    }

    /// Create a timeout from an explicitly supplied clock observation.
    pub fn try_after_at(now: Instant, delay: Duration) -> Result<Self, TimerError> {
        Timer::try_after_at(now, delay).map(Self::from_timer)
    }

    /// Create a timeout for an absolute monotone-clock `deadline`.
    pub fn at(deadline: Instant) -> Self {
        Self::from_timer(Timer::at(deadline))
    }

    /// Create a timeout from an existing standard-library [`Timer`].
    pub fn from_timer(timer: Timer) -> Self {
        Self {
            timer,
            outcome: None,
            unavailable_reason: None,
        }
    }

    /// The absolute monotone deadline.
    pub fn deadline(&self) -> Instant {
        self.timer.deadline()
    }

    /// Whether a terminal outcome has been latched.
    pub fn is_terminal(&self) -> bool {
        self.outcome.is_some()
    }

    /// Return the latched terminal outcome, if any.
    pub fn outcome(&self) -> Option<&TimeoutOutcome<T>> {
        self.outcome.as_ref()
    }

    /// Return the reason for a terminal unavailable outcome.
    pub fn unavailable_reason(&self) -> Option<TimeoutUnavailableReason> {
        self.unavailable_reason
    }

    /// Poll using the current standard-library monotone clock.
    ///
    /// `operation` may adapt a `Future::poll`, channel receive, process wait,
    /// or any other non-blocking operation probe. `cancelled` remains
    /// caller-owned and is checked only while the deadline is pending.
    pub fn poll<F, C>(&mut self, operation: F, cancelled: C) -> TimeoutPoll<'_, T>
    where
        F: FnOnce() -> TimeoutOperation<T>,
        C: FnOnce() -> bool,
    {
        self.poll_at(Instant::now(), operation, cancelled)
    }

    /// Poll at an explicitly supplied monotone instant.
    ///
    /// This is the deterministic clock seam. At or after the deadline, the
    /// operation and cancellation adapters are not called.
    pub fn poll_at<F, C>(&mut self, now: Instant, operation: F, cancelled: C) -> TimeoutPoll<'_, T>
    where
        F: FnOnce() -> TimeoutOperation<T>,
        C: FnOnce() -> bool,
    {
        self.poll_at_with_cancellation(now, operation, || {
            if cancelled() {
                TimeoutCancellation::Cancelled
            } else {
                TimeoutCancellation::Pending
            }
        })
    }

    /// Poll with a typed cancellation adapter.
    ///
    /// Before the deadline both adapters are invoked exactly once. Completion
    /// wins a simultaneous cancellation; an unavailable operation wins an
    /// unavailable or cancelled cancellation input. At or after the deadline
    /// neither adapter is invoked.
    pub fn poll_at_with_cancellation<F, C>(
        &mut self,
        now: Instant,
        operation: F,
        cancellation: C,
    ) -> TimeoutPoll<'_, T>
    where
        F: FnOnce() -> TimeoutOperation<T>,
        C: FnOnce() -> TimeoutCancellation,
    {
        if self.outcome.is_some() {
            return self
                .terminal_poll()
                .expect("terminal timeout must have an outcome");
        }

        match self.timer.try_poll_at(now) {
            Ok(TimerPoll::Fired { .. }) => {
                self.outcome = Some(TimeoutOutcome::TimedOut);
                return TimeoutPoll::TimedOut;
            }
            Err(TimerError::ClockRegression) => {
                self.outcome = Some(TimeoutOutcome::Unavailable);
                self.unavailable_reason = Some(TimeoutUnavailableReason::ClockRegression);
                return TimeoutPoll::Unavailable;
            }
            Err(TimerError::DeadlineOverflow) => {
                unreachable!("a constructed timer already has a representable deadline");
            }
            Ok(TimerPoll::Pending { .. }) => {}
        }

        let operation = operation();
        let cancellation = cancellation();
        match operation {
            TimeoutOperation::Completed(value) => {
                self.outcome = Some(TimeoutOutcome::Completed(value));
                return self
                    .terminal_poll()
                    .expect("completed timeout must have an outcome");
            }
            TimeoutOperation::Unavailable => {
                self.outcome = Some(TimeoutOutcome::Unavailable);
                self.unavailable_reason = Some(TimeoutUnavailableReason::Operation);
                return TimeoutPoll::Unavailable;
            }
            TimeoutOperation::Pending => {}
        }

        match cancellation {
            TimeoutCancellation::Cancelled => {
                self.outcome = Some(TimeoutOutcome::Cancelled);
                return TimeoutPoll::Cancelled;
            }
            TimeoutCancellation::Unavailable => {
                self.outcome = Some(TimeoutOutcome::Unavailable);
                self.unavailable_reason = Some(TimeoutUnavailableReason::Cancellation);
                return TimeoutPoll::Unavailable;
            }
            TimeoutCancellation::Pending => {}
        }

        match self.timer.try_poll_at(now) {
            Ok(TimerPoll::Pending { remaining }) => TimeoutPoll::Pending { remaining },
            Ok(TimerPoll::Fired { .. }) => {
                unreachable!("timer was pending earlier in the same deterministic poll")
            }
            Err(_) => {
                unreachable!("the same deterministic clock observation cannot regress")
            }
        }
    }

    /// Drive the timeout with caller-supplied clock and wait policies.
    ///
    /// `wait` receives the current remaining deadline budget. It may adapt a
    /// condition variable, channel timeout, thread park, reactor, or test
    /// scheduler. This method does not spawn a thread or select a runtime.
    pub fn wait_with<P, C, N, W>(
        &mut self,
        mut operation: P,
        mut cancelled: C,
        mut now: N,
        mut wait: W,
    ) -> &TimeoutOutcome<T>
    where
        P: FnMut() -> TimeoutOperation<T>,
        C: FnMut() -> bool,
        N: FnMut() -> Instant,
        W: FnMut(Duration),
    {
        while let TimeoutPoll::Pending { remaining } =
            self.poll_at(now(), &mut operation, &mut cancelled)
        {
            wait(remaining);
        }

        self.outcome
            .as_ref()
            .expect("terminal timeout must have an outcome")
    }

    fn terminal_poll(&self) -> Option<TimeoutPoll<'_, T>> {
        self.outcome.as_ref().map(|outcome| match outcome {
            TimeoutOutcome::Completed(value) => TimeoutPoll::Completed(value),
            TimeoutOutcome::TimedOut => TimeoutPoll::TimedOut,
            TimeoutOutcome::Cancelled => TimeoutPoll::Cancelled,
            TimeoutOutcome::Unavailable => TimeoutPoll::Unavailable,
        })
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
    revision_generation: u64,
    wake_generation: u64,
    disposed: bool,
    last_now: Option<u64>,
    clock_regressed: bool,
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
            state.wake_generation = state.wake_generation.wrapping_add(1);
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
                    revision_generation: 0,
                    wake_generation: 0,
                    disposed: false,
                    last_now: None,
                    clock_regressed: false,
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

    /// Return the number of accepted revision advances.
    pub fn generation(&self) -> u64 {
        self.inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned")
            .revision_generation
    }

    /// Accept one logical-clock observation for portable barrier adapters.
    ///
    /// The first observation establishes the clock. A smaller later value
    /// latches the barrier as unavailable without changing its revision or
    /// generation. `false` means either that regression was observed or that
    /// an earlier terminal state already won.
    pub fn observe_clock(&self, now: u64) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned");
        if state.disposed || state.clock_regressed {
            return false;
        }
        if state.last_now.is_some_and(|last_now| now < last_now) {
            state.clock_regressed = true;
            state.wake_generation = state.wake_generation.wrapping_add(1);
            self.inner.changed.notify_all();
            return false;
        }
        state.last_now = Some(now);
        true
    }

    /// Whether a logical-clock regression latched this barrier unavailable.
    pub fn clock_regressed(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("revision barrier mutex poisoned")
            .clock_regressed
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
        if state.disposed || state.clock_regressed || revision <= state.revision {
            return false;
        }

        state.revision = revision;
        state.revision_generation = state.revision_generation.wrapping_add(1);
        state.wake_generation = state.wake_generation.wrapping_add(1);
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
        if state.disposed || state.clock_regressed {
            return;
        }

        state.wake_generation = state.wake_generation.wrapping_add(1);
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
        if state.disposed || state.clock_regressed {
            return false;
        }

        state.disposed = true;
        state.wake_generation = state.wake_generation.wrapping_add(1);
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
            if state.clock_regressed {
                return RevisionWaitOutcome::Unavailable {
                    revision: state.revision,
                };
            }
            if state.disposed {
                return RevisionWaitOutcome::Disposed {
                    revision: state.revision,
                };
            }
            match deadline.as_deref_mut().map(Timer::poll) {
                Some(TimerPoll::Fired { .. }) => {
                    return RevisionWaitOutcome::TimedOut {
                        revision: state.revision,
                    };
                }
                Some(TimerPoll::Pending { .. }) | None => {}
            }
            if cancellation.is_some_and(BarrierCancellation::is_cancelled) {
                return RevisionWaitOutcome::Cancelled {
                    revision: state.revision,
                };
            }

            if state.revision > after_revision {
                let revision = state.revision;
                let wake_generation = state.wake_generation;
                drop(state);
                let checked = check(revision);
                state = self
                    .inner
                    .state
                    .lock()
                    .expect("revision barrier mutex poisoned");

                if state.wake_generation != wake_generation {
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
                    unreachable!("timer was pending earlier in the same barrier iteration");
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
