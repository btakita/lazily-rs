//! Transport-agnostic reactive egress core (`#lzegress`).
//!
//! The core owns delivery authority: sequence assignment, the unacknowledged
//! window, a monotone acknowledgement watermark, retry budget, and the
//! producer-generation fence. It owns no graph handles and performs no I/O.
//! `DurableOutbox`/`SpillStore` remain the persistence layer; `RelayCell`
//! remains the conflation/backpressure layer.

use std::collections::{BTreeMap, VecDeque};

/// Egress policy. `retry_budget` counts retries after the first attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressPolicy {
    pub inflight_limit: usize,
    pub retry_budget: u32,
    pub retry_base: u64,
    pub retry_ceiling: u64,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self {
            inflight_limit: 64,
            retry_budget: 3,
            retry_base: 10,
            retry_ceiling: 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressConfigError {
    ZeroInflightLimit,
    ZeroRetryBase,
    RetryCeilingBelowBase,
}

/// One send attempt. The `(generation, sequence)` pair is the transport
/// idempotency identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressEnvelope<T> {
    pub generation: u64,
    pub sequence: u64,
    pub attempt: u32,
    pub payload: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressRetry {
    pub sequence: u64,
    pub attempt: u32,
    pub backoff: u64,
    pub exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressClaim<T> {
    Claimed(EgressEnvelope<T>),
    Empty,
    WindowFull,
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressAck {
    Advanced { through: u64 },
    Unchanged { through: Option<u64> },
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressFailure {
    Retrying(EgressRetry),
    Exhausted(EgressRetry),
    UnknownSequence,
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressRetryAction {
    Scheduled { sequence: u64 },
    UnknownSequence,
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressReconnect {
    Advanced { generation: u64, replayed: usize },
    Unchanged { generation: u64 },
    StaleGeneration { current: u64 },
}

/// Reader-kind invalidations emitted by one transition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EgressChange {
    pub pending: bool,
    pub inflight: bool,
    pub acked_through: bool,
    pub retry: bool,
}

impl EgressChange {
    pub fn is_empty(self) -> bool {
        self == Self::default()
    }
}

#[derive(Debug, Clone)]
struct Record<T> {
    generation: u64,
    sequence: u64,
    attempt: u32,
    payload: T,
}

impl<T: Clone> Record<T> {
    fn envelope(&self) -> EgressEnvelope<T> {
        EgressEnvelope {
            generation: self.generation,
            sequence: self.sequence,
            attempt: self.attempt,
            payload: self.payload.clone(),
        }
    }
}

/// Pure egress delivery authority.
#[derive(Debug, Clone)]
pub struct EgressCore<T> {
    policy: EgressPolicy,
    generation: u64,
    next_sequence: u64,
    acked_through: Option<u64>,
    pending: VecDeque<Record<T>>,
    inflight: BTreeMap<u64, Record<T>>,
    failed: BTreeMap<u64, Record<T>>,
    retry: Option<EgressRetry>,
}

impl<T: Clone> EgressCore<T> {
    pub fn new(generation: u64, policy: EgressPolicy) -> Result<Self, EgressConfigError> {
        if policy.inflight_limit == 0 {
            return Err(EgressConfigError::ZeroInflightLimit);
        }
        if policy.retry_base == 0 {
            return Err(EgressConfigError::ZeroRetryBase);
        }
        if policy.retry_ceiling < policy.retry_base {
            return Err(EgressConfigError::RetryCeilingBelowBase);
        }
        Ok(Self {
            policy,
            generation,
            next_sequence: 0,
            acked_through: None,
            pending: VecDeque::new(),
            inflight: BTreeMap::new(),
            failed: BTreeMap::new(),
            retry: None,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn pending(&self) -> Vec<EgressEnvelope<T>> {
        self.pending.iter().map(Record::envelope).collect()
    }

    pub fn inflight(&self) -> Vec<EgressEnvelope<T>> {
        self.inflight.values().map(Record::envelope).collect()
    }

    pub fn acked_through(&self) -> Option<u64> {
        self.acked_through
    }

    pub fn retry(&self) -> Option<EgressRetry> {
        self.retry
    }

    /// Assign the next sequence and place the value in the pending projection.
    pub fn enqueue(&mut self, payload: T) -> (EgressChange, u64) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.pending.push_back(Record {
            generation: self.generation,
            sequence,
            attempt: 0,
            payload,
        });
        (
            EgressChange {
                pending: true,
                ..EgressChange::default()
            },
            sequence,
        )
    }

    /// Move one pending value into the unacknowledged window.
    ///
    /// The caller supplies the generation captured by its transport attachment.
    /// An attachment from a superseded incarnation cannot claim or replay.
    pub fn claim(&mut self, generation: u64) -> (EgressChange, EgressClaim<T>) {
        if generation != self.generation {
            return (
                EgressChange::default(),
                EgressClaim::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        if self.inflight.len() >= self.policy.inflight_limit {
            return (EgressChange::default(), EgressClaim::WindowFull);
        }
        let Some(mut record) = self.pending.pop_front() else {
            return (EgressChange::default(), EgressClaim::Empty);
        };
        record.generation = self.generation;
        record.attempt = record.attempt.saturating_add(1);
        let envelope = record.envelope();
        self.inflight.insert(record.sequence, record);
        let retry_changed = self
            .retry
            .is_some_and(|retry| retry.sequence == envelope.sequence);
        if retry_changed {
            self.retry = None;
        }
        (
            EgressChange {
                pending: true,
                inflight: true,
                retry: retry_changed,
                ..EgressChange::default()
            },
            EgressClaim::Claimed(envelope),
        )
    }

    /// Advance the acknowledgement watermark and prune acknowledged records.
    pub fn ack(&mut self, generation: u64, through: u64) -> (EgressChange, EgressAck) {
        if generation != self.generation {
            return (
                EgressChange::default(),
                EgressAck::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        if self.acked_through.is_some_and(|current| through <= current) {
            return (
                EgressChange::default(),
                EgressAck::Unchanged {
                    through: self.acked_through,
                },
            );
        }
        let inflight_before = self.inflight.len();
        let pending_before = self.pending.len();
        self.acked_through = Some(through);
        self.inflight.retain(|sequence, _| *sequence > through);
        self.failed.retain(|sequence, _| *sequence > through);
        self.pending.retain(|record| record.sequence > through);
        let retry_changed = self.retry.is_some_and(|retry| retry.sequence <= through);
        if retry_changed {
            self.retry = None;
        }
        (
            EgressChange {
                pending: self.pending.len() != pending_before,
                inflight: self.inflight.len() != inflight_before,
                acked_through: true,
                retry: retry_changed,
            },
            EgressAck::Advanced { through },
        )
    }

    /// Return a failed in-flight record to pending while budget remains.
    pub fn fail(&mut self, generation: u64, sequence: u64) -> (EgressChange, EgressFailure) {
        if generation != self.generation {
            return (
                EgressChange::default(),
                EgressFailure::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        let Some(record) = self.inflight.remove(&sequence) else {
            return (EgressChange::default(), EgressFailure::UnknownSequence);
        };
        let exhausted = record.attempt > self.policy.retry_budget;
        let retry = EgressRetry {
            sequence,
            attempt: record.attempt,
            backoff: self.backoff(record.attempt),
            exhausted,
        };
        self.retry = Some(retry);
        if !exhausted {
            self.failed.insert(sequence, record);
        }
        (
            EgressChange {
                pending: false,
                inflight: true,
                retry: true,
                ..EgressChange::default()
            },
            if exhausted {
                EgressFailure::Exhausted(retry)
            } else {
                EgressFailure::Retrying(retry)
            },
        )
    }

    /// Make a parked failed record eligible after its derived backoff elapses.
    pub fn retry_now(
        &mut self,
        generation: u64,
        sequence: u64,
    ) -> (EgressChange, EgressRetryAction) {
        if generation != self.generation {
            return (
                EgressChange::default(),
                EgressRetryAction::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        let Some(record) = self.failed.remove(&sequence) else {
            return (EgressChange::default(), EgressRetryAction::UnknownSequence);
        };
        self.insert_pending_ordered(record);
        let retry_changed = self.retry.is_some_and(|retry| retry.sequence == sequence);
        if retry_changed {
            self.retry = None;
        }
        (
            EgressChange {
                pending: true,
                retry: retry_changed,
                ..EgressChange::default()
            },
            EgressRetryAction::Scheduled { sequence },
        )
    }

    /// Fence the previous attachment and replay every unacknowledged in-flight
    /// record through the new generation. The ack watermark and sequence space
    /// survive; only the producer incarnation changes.
    pub fn reconnect(&mut self, generation: u64) -> (EgressChange, EgressReconnect) {
        if generation < self.generation {
            return (
                EgressChange::default(),
                EgressReconnect::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        if generation == self.generation {
            return (
                EgressChange::default(),
                EgressReconnect::Unchanged {
                    generation: self.generation,
                },
            );
        }
        self.generation = generation;
        let replayed = self.inflight.len() + self.failed.len();
        let inflight = std::mem::take(&mut self.inflight);
        for (_, mut record) in inflight {
            record.generation = generation;
            self.insert_pending_ordered(record);
        }
        let failed = std::mem::take(&mut self.failed);
        for (_, mut record) in failed {
            record.generation = generation;
            self.insert_pending_ordered(record);
        }
        for record in &mut self.pending {
            record.generation = generation;
        }
        let retry_changed = self.retry.take().is_some();
        (
            EgressChange {
                pending: replayed > 0,
                inflight: replayed > 0,
                retry: retry_changed,
                ..EgressChange::default()
            },
            EgressReconnect::Advanced {
                generation,
                replayed,
            },
        )
    }

    fn insert_pending_ordered(&mut self, record: Record<T>) {
        let position = self
            .pending
            .iter()
            .position(|existing| existing.sequence > record.sequence)
            .unwrap_or(self.pending.len());
        self.pending.insert(position, record);
    }

    fn backoff(&self, attempt: u32) -> u64 {
        let shift = attempt.saturating_sub(1).min(63);
        self.policy
            .retry_base
            .saturating_mul(1_u64 << shift)
            .min(self.policy.retry_ceiling)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> EgressPolicy {
        EgressPolicy {
            inflight_limit: 2,
            retry_budget: 1,
            retry_base: 5,
            retry_ceiling: 20,
        }
    }

    #[test]
    fn ack_is_monotone_and_reconnect_fences_old_attachment() {
        let mut core = EgressCore::new(7, policy()).unwrap();
        core.enqueue("a");
        core.enqueue("b");
        assert!(matches!(core.claim(7).1, EgressClaim::Claimed(_)));
        assert!(matches!(
            core.ack(7, 0).1,
            EgressAck::Advanced { through: 0 }
        ));
        assert!(matches!(core.ack(7, 0).1, EgressAck::Unchanged { .. }));
        assert!(matches!(
            core.reconnect(8).1,
            EgressReconnect::Advanced { generation: 8, .. }
        ));
        assert_eq!(core.claim(7).1, EgressClaim::StaleGeneration { current: 8 });
        assert_eq!(core.acked_through(), Some(0));
    }

    #[test]
    fn retry_budget_is_bounded() {
        let mut core = EgressCore::new(1, policy()).unwrap();
        core.enqueue(1);
        let EgressClaim::Claimed(first) = core.claim(1).1 else {
            panic!("first claim");
        };
        assert!(matches!(
            core.fail(1, first.sequence).1,
            EgressFailure::Retrying(_)
        ));
        assert_eq!(
            core.retry_now(1, first.sequence).1,
            EgressRetryAction::Scheduled {
                sequence: first.sequence
            }
        );
        let EgressClaim::Claimed(second) = core.claim(1).1 else {
            panic!("retry claim");
        };
        assert_eq!(second.attempt, 2);
        assert!(matches!(
            core.fail(1, second.sequence).1,
            EgressFailure::Exhausted(_)
        ));
        assert!(core.pending().is_empty());
    }
}
