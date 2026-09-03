//! Keyed latest-value durable projection authority (`#lzlatestdurableprojection`).
//!
//! This core models a durable sink whose contract is convergence to the latest
//! desired value, not delivery of every intermediate command. Each key has at
//! most one in-flight projection. Newer desired epochs supersede pending work;
//! an acknowledgement for an older in-flight epoch may advance durability but
//! can never clear that newer desire.

use std::collections::BTreeMap;

/// A desired projection revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestDurableRevision<V> {
    pub epoch: u64,
    pub value: V,
}

/// The exact projection attempt handed to a sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestDurableEnvelope<K, V> {
    pub generation: u64,
    pub key: K,
    pub epoch: u64,
    pub value: V,
}

/// Observable state for one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestDurableKeyState<K, V> {
    pub desired: Option<LatestDurableRevision<V>>,
    pub inflight: Option<LatestDurableEnvelope<K, V>>,
    pub durable_through: Option<u64>,
}

/// Complete observable state. The reactive shells expose this as a Computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestDurableSnapshot<K, V> {
    pub generation: u64,
    pub keys: BTreeMap<K, LatestDurableKeyState<K, V>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatestDurableUpsert {
    Accepted,
    Unchanged,
    AlreadyDurable { durable_through: u64 },
    StaleEpoch { current: u64 },
    EpochConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatestDurableClaim<K, V> {
    Claimed(LatestDurableEnvelope<K, V>),
    Empty,
    Busy,
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatestDurableAck {
    Advanced { durable_through: u64 },
    Unchanged { durable_through: u64 },
    UnknownEpoch,
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatestDurableFailure {
    Pending,
    Superseded,
    UnknownEpoch,
    StaleGeneration { current: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatestDurableReconnect {
    Advanced {
        generation: u64,
        requeued: usize,
        superseded: usize,
    },
    Unchanged {
        generation: u64,
    },
    StaleGeneration {
        current: u64,
    },
}

/// Whether a transition changed the observable snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatestDurableChange {
    pub state: bool,
}

#[derive(Debug, Clone)]
struct Entry<K, V> {
    desired: Option<LatestDurableRevision<V>>,
    inflight: Option<LatestDurableEnvelope<K, V>>,
    durable_through: Option<u64>,
}

impl<K, V> Default for Entry<K, V> {
    fn default() -> Self {
        Self {
            desired: None,
            inflight: None,
            durable_through: None,
        }
    }
}

/// Pure keyed latest-durable projection authority.
#[derive(Debug, Clone)]
pub struct LatestDurableProjectionCore<K, V> {
    generation: u64,
    keys: BTreeMap<K, Entry<K, V>>,
}

impl<K, V> LatestDurableProjectionCore<K, V>
where
    K: Ord + Clone,
    V: Clone + PartialEq,
{
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            keys: BTreeMap::new(),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn snapshot(&self) -> LatestDurableSnapshot<K, V> {
        LatestDurableSnapshot {
            generation: self.generation,
            keys: self
                .keys
                .iter()
                .map(|(key, entry)| {
                    (
                        key.clone(),
                        LatestDurableKeyState {
                            desired: entry.desired.clone(),
                            inflight: entry.inflight.clone(),
                            durable_through: entry.durable_through,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn state(&self, key: &K) -> Option<LatestDurableKeyState<K, V>> {
        self.keys.get(key).map(|entry| LatestDurableKeyState {
            desired: entry.desired.clone(),
            inflight: entry.inflight.clone(),
            durable_through: entry.durable_through,
        })
    }

    pub fn durable_through(&self, key: &K) -> Option<u64> {
        self.keys.get(key).and_then(|entry| entry.durable_through)
    }

    pub fn pending_keys(&self) -> Vec<K> {
        self.keys
            .iter()
            .filter(|(_, entry)| entry.inflight.is_none() && entry.desired.is_some())
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Replace the pending desire for `key` when `epoch` advances.
    pub fn upsert_desired(
        &mut self,
        key: K,
        epoch: u64,
        value: V,
    ) -> (LatestDurableChange, LatestDurableUpsert) {
        let entry = self.keys.entry(key).or_default();
        if let Some(durable) = entry.durable_through
            && epoch <= durable
        {
            return (
                LatestDurableChange::default(),
                LatestDurableUpsert::AlreadyDurable {
                    durable_through: durable,
                },
            );
        }

        let newest = entry
            .desired
            .iter()
            .map(|revision| (revision.epoch, &revision.value))
            .chain(
                entry
                    .inflight
                    .iter()
                    .map(|envelope| (envelope.epoch, &envelope.value)),
            )
            .max_by_key(|(retained_epoch, _)| *retained_epoch);
        if let Some((current_epoch, current_value)) = newest {
            if epoch < current_epoch {
                return (
                    LatestDurableChange::default(),
                    LatestDurableUpsert::StaleEpoch {
                        current: current_epoch,
                    },
                );
            }
            if epoch == current_epoch {
                let outcome = if value == *current_value {
                    LatestDurableUpsert::Unchanged
                } else {
                    LatestDurableUpsert::EpochConflict
                };
                return (LatestDurableChange::default(), outcome);
            }
        }

        entry.desired = Some(LatestDurableRevision { epoch, value });
        (
            LatestDurableChange { state: true },
            LatestDurableUpsert::Accepted,
        )
    }

    /// Move the latest pending revision into the sole in-flight slot for one key.
    pub fn claim(
        &mut self,
        key: &K,
        generation: u64,
    ) -> (LatestDurableChange, LatestDurableClaim<K, V>) {
        if generation != self.generation {
            return (
                LatestDurableChange::default(),
                LatestDurableClaim::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        let Some(entry) = self.keys.get_mut(key) else {
            return (LatestDurableChange::default(), LatestDurableClaim::Empty);
        };
        if entry.inflight.is_some() {
            return (LatestDurableChange::default(), LatestDurableClaim::Busy);
        }
        let Some(desired) = entry.desired.take() else {
            return (LatestDurableChange::default(), LatestDurableClaim::Empty);
        };
        let envelope = LatestDurableEnvelope {
            generation,
            key: key.clone(),
            epoch: desired.epoch,
            value: desired.value,
        };
        entry.inflight = Some(envelope.clone());
        (
            LatestDurableChange { state: true },
            LatestDurableClaim::Claimed(envelope),
        )
    }

    /// Record exact sink success for the sole in-flight revision of `key`.
    pub fn ack_applied(
        &mut self,
        key: &K,
        generation: u64,
        epoch: u64,
    ) -> (LatestDurableChange, LatestDurableAck) {
        if generation != self.generation {
            return (
                LatestDurableChange::default(),
                LatestDurableAck::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        let Some(entry) = self.keys.get_mut(key) else {
            return (
                LatestDurableChange::default(),
                LatestDurableAck::UnknownEpoch,
            );
        };
        if entry.inflight.as_ref().map(|envelope| envelope.epoch) != Some(epoch) {
            if let Some(durable_through) = entry.durable_through
                && epoch <= durable_through
            {
                return (
                    LatestDurableChange::default(),
                    LatestDurableAck::Unchanged { durable_through },
                );
            }
            return (
                LatestDurableChange::default(),
                LatestDurableAck::UnknownEpoch,
            );
        }

        entry.inflight = None;
        let previous = entry.durable_through;
        let durable_through = previous.map_or(epoch, |durable| durable.max(epoch));
        entry.durable_through = Some(durable_through);
        let outcome = if previous.is_some_and(|durable| durable >= epoch) {
            LatestDurableAck::Unchanged { durable_through }
        } else {
            LatestDurableAck::Advanced { durable_through }
        };
        (LatestDurableChange { state: true }, outcome)
    }

    /// Return a retryable failure to the latest-value pending state.
    pub fn fail_retryable(
        &mut self,
        key: &K,
        generation: u64,
        epoch: u64,
    ) -> (LatestDurableChange, LatestDurableFailure) {
        if generation != self.generation {
            return (
                LatestDurableChange::default(),
                LatestDurableFailure::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        let Some(entry) = self.keys.get_mut(key) else {
            return (
                LatestDurableChange::default(),
                LatestDurableFailure::UnknownEpoch,
            );
        };
        if entry.inflight.as_ref().map(|envelope| envelope.epoch) != Some(epoch) {
            return (
                LatestDurableChange::default(),
                LatestDurableFailure::UnknownEpoch,
            );
        }

        let inflight = entry.inflight.take().expect("matched in-flight revision");
        let outcome = if entry
            .desired
            .as_ref()
            .is_some_and(|desired| desired.epoch > inflight.epoch)
        {
            LatestDurableFailure::Superseded
        } else {
            entry.desired = Some(LatestDurableRevision {
                epoch: inflight.epoch,
                value: inflight.value,
            });
            LatestDurableFailure::Pending
        };
        (LatestDurableChange { state: true }, outcome)
    }

    /// Fence the previous sink incarnation and requeue its in-flight work.
    pub fn reconnect(
        &mut self,
        new_generation: u64,
    ) -> (LatestDurableChange, LatestDurableReconnect) {
        if new_generation < self.generation {
            return (
                LatestDurableChange::default(),
                LatestDurableReconnect::StaleGeneration {
                    current: self.generation,
                },
            );
        }
        if new_generation == self.generation {
            return (
                LatestDurableChange::default(),
                LatestDurableReconnect::Unchanged {
                    generation: self.generation,
                },
            );
        }

        self.generation = new_generation;
        let mut requeued = 0;
        let mut superseded = 0;
        for entry in self.keys.values_mut() {
            let Some(inflight) = entry.inflight.take() else {
                continue;
            };
            if entry
                .desired
                .as_ref()
                .is_some_and(|desired| desired.epoch > inflight.epoch)
            {
                superseded += 1;
            } else {
                entry.desired = Some(LatestDurableRevision {
                    epoch: inflight.epoch,
                    value: inflight.value,
                });
                requeued += 1;
            }
        }
        (
            LatestDurableChange { state: true },
            LatestDurableReconnect::Advanced {
                generation: new_generation,
                requeued,
                superseded,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claimed(
        core: &mut LatestDurableProjectionCore<&'static str, &'static str>,
        key: &'static str,
        generation: u64,
    ) -> LatestDurableEnvelope<&'static str, &'static str> {
        match core.claim(&key, generation).1 {
            LatestDurableClaim::Claimed(envelope) => envelope,
            other => panic!("expected claim, got {other:?}"),
        }
    }

    #[test]
    fn newer_desire_supersedes_pending_without_command_history() {
        let mut core = LatestDurableProjectionCore::new(1);
        core.upsert_desired("doc", 1, "a");
        core.upsert_desired("doc", 2, "b");
        let envelope = claimed(&mut core, "doc", 1);
        assert_eq!((envelope.epoch, envelope.value), (2, "b"));
        assert!(core.state(&"doc").unwrap().desired.is_none());
    }

    #[test]
    fn stale_success_advances_durability_but_never_clears_newer_desire() {
        let mut core = LatestDurableProjectionCore::new(4);
        core.upsert_desired("doc", 7, "old");
        claimed(&mut core, "doc", 4);
        core.upsert_desired("doc", 8, "new");

        assert_eq!(
            core.ack_applied(&"doc", 4, 7).1,
            LatestDurableAck::Advanced { durable_through: 7 }
        );
        assert_eq!(claimed(&mut core, "doc", 4).epoch, 8);
    }

    #[test]
    fn retryable_failure_leaves_only_the_latest_desire_pending() {
        let mut core = LatestDurableProjectionCore::new(1);
        core.upsert_desired("doc", 10, "old");
        claimed(&mut core, "doc", 1);
        core.upsert_desired("doc", 11, "new");

        assert_eq!(
            core.fail_retryable(&"doc", 1, 10).1,
            LatestDurableFailure::Superseded
        );
        assert_eq!(claimed(&mut core, "doc", 1).value, "new");
    }

    #[test]
    fn reconnect_fences_stale_receipts_and_requeues_inflight() {
        let mut core = LatestDurableProjectionCore::new(2);
        core.upsert_desired("doc", 1, "value");
        claimed(&mut core, "doc", 2);
        assert_eq!(
            core.reconnect(3).1,
            LatestDurableReconnect::Advanced {
                generation: 3,
                requeued: 1,
                superseded: 0,
            }
        );
        assert_eq!(
            core.ack_applied(&"doc", 2, 1).1,
            LatestDurableAck::StaleGeneration { current: 3 }
        );
        assert_eq!(claimed(&mut core, "doc", 3).epoch, 1);
    }

    #[test]
    fn duplicate_ack_is_unchanged_but_unknown_future_epoch_is_rejected() {
        let mut core = LatestDurableProjectionCore::new(1);
        core.upsert_desired("doc", 3, "value");
        claimed(&mut core, "doc", 1);
        core.ack_applied(&"doc", 1, 3);
        assert_eq!(
            core.ack_applied(&"doc", 1, 3).1,
            LatestDurableAck::Unchanged { durable_through: 3 }
        );
        assert_eq!(
            core.ack_applied(&"doc", 1, 4).1,
            LatestDurableAck::UnknownEpoch
        );
    }

    #[test]
    fn keys_have_independent_single_flight_lanes() {
        let mut core = LatestDurableProjectionCore::new(1);
        core.upsert_desired("a", 1, "a1");
        core.upsert_desired("b", 4, "b4");
        claimed(&mut core, "a", 1);
        claimed(&mut core, "b", 1);
        assert_eq!(core.claim(&"a", 1).1, LatestDurableClaim::Busy);
        assert_eq!(core.snapshot().keys.len(), 2);
    }
}
