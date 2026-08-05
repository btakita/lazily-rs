//! Boundary adapters for the existing [`IngressCell`](crate::IngressCell)
//! family (`#lzingressadapters`).
//!
//! [`IngressCore`](crate::IngressCore) remains the only value-admission owner.
//! This module owns the outer subscription facts that a non-reactive transport
//! must publish before decoded envelopes can reach that core: producer
//! generation, snapshot/event cursor, keyed membership, validation, freshness,
//! and durable delivery receipts.

use std::collections::{BTreeMap, BTreeSet};

use crate::IngressEnvelope;

/// Bounds for snapshot-plus-event bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryIngressConfig {
    /// Maximum future events retained while a snapshot or missing cursor is in flight.
    pub max_buffered: usize,
    /// Inclusive logical-clock freshness horizon.
    pub freshness_horizon: u64,
}

impl Default for BoundaryIngressConfig {
    fn default() -> Self {
        Self {
            max_buffered: 64,
            freshness_horizon: 1_000,
        }
    }
}

/// Subscription/bootstrap phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryIngressPhase {
    /// No producer is attached.
    Detached,
    /// The event subscription is hot and its covering snapshot is in flight.
    Bootstrapping,
    /// Snapshot plus every contiguous successor has been applied.
    Live,
    /// A channel cursor is missing; a cold snapshot/replay is required.
    ReplayRequired,
    /// The bounded future-event buffer filled before the gap closed.
    Backpressured,
    /// The current template/document projection is structurally invalid.
    Invalid,
}

/// Template-style structural validation projected from source facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryValidation {
    /// The current projection is structurally valid.
    Valid,
    /// The current projection is structurally invalid.
    Invalid,
}

/// Derived readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryIngressReadiness {
    /// No subscription exists.
    Detached,
    /// Waiting for a covering snapshot.
    Warming,
    /// Snapshot and event cursor are contiguous and validation is green.
    Ready,
    /// A cold replay is required.
    ReplayRequired,
    /// The future-event buffer is full.
    Backpressured,
    /// Validation rejected the current projection.
    Invalid,
}

/// Derived freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryFreshness {
    /// No stamped snapshot/event has been accepted.
    Unknown,
    /// Latest accepted stamp remains inside the inclusive horizon.
    Fresh,
    /// Latest accepted stamp crossed the horizon.
    Stale,
}

/// Cursor authority inside one producer generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryIngressAuthority {
    /// Producer/controller incarnation.
    pub generation: u64,
    /// Latest contiguous channel cursor, absent before bootstrap.
    pub cursor: Option<u64>,
}

/// The effect intent derived from Sources. The adapter never performs I/O itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryIngressEffectIntent {
    /// No subscription/replay work is currently required.
    Idle,
    /// Attach or reattach the event channel and obtain a covering snapshot.
    Subscribe { generation: u64 },
    /// Fetch a cold snapshot/replay covering `from_cursor`.
    ColdReplay { generation: u64, from_cursor: u64 },
}

/// One event-channel payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryIngressPayload<K, T> {
    /// Feed one decoded envelope to the existing IngressCell.
    Upsert(IngressEnvelope<K, T>),
    /// Close and remove one source key.
    Remove(K),
    /// Publish a new structural-validation fact.
    Validate(BoundaryValidation),
}

/// One generation-fenced event-channel frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryIngressEvent<K, T> {
    /// Producer/controller incarnation.
    pub generation: u64,
    /// Monotone channel position inside `generation`.
    pub cursor: u64,
    /// Producer logical timestamp.
    pub stamped_at: u64,
    /// Decoded payload.
    pub payload: BoundaryIngressPayload<K, T>,
}

/// Covering bootstrap snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryIngressSnapshot<K, T> {
    /// Producer/controller incarnation.
    pub generation: u64,
    /// Last event included in the snapshot.
    pub cursor: u64,
    /// Producer logical timestamp.
    pub stamped_at: u64,
    /// Current source values, represented as ordinary Ingress envelopes.
    pub entries: Vec<IngressEnvelope<K, T>>,
    /// Current subscriber/member frontier.
    pub members: Vec<String>,
    /// Structural validation of this exact snapshot.
    pub validation: BoundaryValidation,
}

/// One durable, one-shot delivery frontier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryDeliveryReceipt {
    /// Stable idempotency key.
    pub receipt_id: String,
    /// Members captured for this delivery. Never shrinks on membership churn.
    pub targets: BTreeSet<String>,
    /// Target members that projected the delivery.
    pub acknowledged: BTreeSet<String>,
}

impl BoundaryDeliveryReceipt {
    /// Empty membership is pending, never vacuously converged.
    pub fn converged(&self) -> bool {
        !self.targets.is_empty() && self.targets.is_subset(&self.acknowledged)
    }
}

/// Atomic observation published by a reactive shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryIngressProjection<K> {
    pub phase: BoundaryIngressPhase,
    pub generation: u64,
    pub cursor: Option<u64>,
    pub buffered_cursors: Vec<u64>,
    pub source_keys: Vec<K>,
    pub members: Vec<String>,
    pub validation: BoundaryValidation,
    pub freshness: BoundaryFreshness,
    pub replay_from: Option<u64>,
    pub stale_events: u64,
    pub active_delivery: Option<BoundaryDeliveryReceipt>,
    /// Advances once per observable source transaction.
    pub revision: u64,
}

impl<K> BoundaryIngressProjection<K> {
    /// Derived readiness.
    pub fn readiness(&self) -> BoundaryIngressReadiness {
        match self.phase {
            BoundaryIngressPhase::Detached => BoundaryIngressReadiness::Detached,
            BoundaryIngressPhase::Bootstrapping => BoundaryIngressReadiness::Warming,
            BoundaryIngressPhase::Live => BoundaryIngressReadiness::Ready,
            BoundaryIngressPhase::ReplayRequired => BoundaryIngressReadiness::ReplayRequired,
            BoundaryIngressPhase::Backpressured => BoundaryIngressReadiness::Backpressured,
            BoundaryIngressPhase::Invalid => BoundaryIngressReadiness::Invalid,
        }
    }

    /// Derived cursor authority.
    pub fn authority(&self) -> Option<BoundaryIngressAuthority> {
        (self.phase != BoundaryIngressPhase::Detached).then_some(BoundaryIngressAuthority {
            generation: self.generation,
            cursor: self.cursor,
        })
    }

    /// Derived delivery convergence for the active one-shot receipt.
    pub fn delivery_converged(&self) -> bool {
        self.active_delivery
            .as_ref()
            .is_some_and(BoundaryDeliveryReceipt::converged)
    }

    /// Derived subscription/replay effect intent.
    pub fn effect_intent(&self) -> BoundaryIngressEffectIntent {
        match self.phase {
            BoundaryIngressPhase::Bootstrapping => BoundaryIngressEffectIntent::Subscribe {
                generation: self.generation,
            },
            BoundaryIngressPhase::ReplayRequired | BoundaryIngressPhase::Backpressured => {
                BoundaryIngressEffectIntent::ColdReplay {
                    generation: self.generation,
                    from_cursor: self.replay_from.unwrap_or(0),
                }
            }
            BoundaryIngressPhase::Detached
            | BoundaryIngressPhase::Live
            | BoundaryIngressPhase::Invalid => BoundaryIngressEffectIntent::Idle,
        }
    }
}

/// Value-plane actions a shell applies to its existing IngressCell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryIngressAction<K, T> {
    Admit(IngressEnvelope<K, T>),
    Close(K),
}

/// One atomic core transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryIngressTransition<K, T> {
    pub actions: Vec<BoundaryIngressAction<K, T>>,
    pub changed: bool,
}

impl<K, T> BoundaryIngressTransition<K, T> {
    fn unchanged() -> Self {
        Self {
            actions: Vec::new(),
            changed: false,
        }
    }
}

/// Graph-agnostic snapshot/event bootstrap owner.
///
/// Payloads are never stored as an alternative value plane: accepted payloads
/// leave as [`BoundaryIngressAction`]s and are admitted by the wrapped
/// IngressCell.
pub struct BoundaryIngressCore<K, T> {
    config: BoundaryIngressConfig,
    phase: BoundaryIngressPhase,
    generation: u64,
    cursor: Option<u64>,
    buffered: BTreeMap<u64, BoundaryIngressEvent<K, T>>,
    source_keys: BTreeSet<K>,
    members: BTreeSet<String>,
    validation: BoundaryValidation,
    replay_from: Option<u64>,
    stale_events: u64,
    deliveries: BTreeMap<String, BoundaryDeliveryReceipt>,
    active_delivery: Option<String>,
    last_stamped_at: Option<u64>,
    now: u64,
    revision: u64,
}

impl<K, T> BoundaryIngressCore<K, T>
where
    K: Ord + Clone,
{
    /// Create a detached adapter state.
    pub fn new(config: BoundaryIngressConfig) -> Self {
        Self {
            config,
            phase: BoundaryIngressPhase::Detached,
            generation: 0,
            cursor: None,
            buffered: BTreeMap::new(),
            source_keys: BTreeSet::new(),
            members: BTreeSet::new(),
            validation: BoundaryValidation::Valid,
            replay_from: None,
            stale_events: 0,
            deliveries: BTreeMap::new(),
            active_delivery: None,
            last_stamped_at: None,
            now: 0,
            revision: 0,
        }
    }

    fn bump(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }

    /// Hot-subscribe to `generation`. A newer call fences and discards every
    /// buffered event from the previous generation.
    pub fn subscribe(&mut self, generation: u64) -> BoundaryIngressTransition<K, T> {
        if generation < self.generation {
            return BoundaryIngressTransition::unchanged();
        }
        self.generation = generation;
        self.cursor = None;
        self.buffered.clear();
        self.source_keys.clear();
        self.members.clear();
        self.validation = BoundaryValidation::Valid;
        self.replay_from = None;
        self.phase = BoundaryIngressPhase::Bootstrapping;
        self.bump();
        BoundaryIngressTransition {
            actions: Vec::new(),
            changed: true,
        }
    }

    /// Atomically install a covering snapshot and every contiguous buffered
    /// successor.
    pub fn apply_snapshot(
        &mut self,
        snapshot: BoundaryIngressSnapshot<K, T>,
    ) -> BoundaryIngressTransition<K, T> {
        if snapshot.generation < self.generation {
            self.stale_events = self.stale_events.wrapping_add(1);
            self.bump();
            return BoundaryIngressTransition {
                actions: Vec::new(),
                changed: true,
            };
        }
        if snapshot.generation > self.generation {
            self.generation = snapshot.generation;
            self.buffered.clear();
        }

        let next_keys: BTreeSet<K> = snapshot
            .entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect();
        let mut actions: Vec<_> = self
            .source_keys
            .difference(&next_keys)
            .cloned()
            .map(BoundaryIngressAction::Close)
            .collect();
        actions.extend(
            snapshot
                .entries
                .into_iter()
                .map(BoundaryIngressAction::Admit),
        );
        self.source_keys = next_keys;
        self.members = snapshot.members.into_iter().collect();
        self.validation = snapshot.validation;
        self.cursor = Some(snapshot.cursor);
        self.last_stamped_at = Some(snapshot.stamped_at);
        self.replay_from = None;
        self.phase = if self.validation == BoundaryValidation::Valid {
            BoundaryIngressPhase::Live
        } else {
            BoundaryIngressPhase::Invalid
        };
        self.buffered.retain(|cursor, _| *cursor > snapshot.cursor);
        self.drain_contiguous(&mut actions);
        self.bump();
        BoundaryIngressTransition {
            actions,
            changed: true,
        }
    }

    /// Publish one event-channel frame.
    pub fn apply_event(
        &mut self,
        event: BoundaryIngressEvent<K, T>,
    ) -> BoundaryIngressTransition<K, T> {
        if event.generation < self.generation {
            self.stale_events = self.stale_events.wrapping_add(1);
            self.bump();
            return BoundaryIngressTransition {
                actions: Vec::new(),
                changed: true,
            };
        }
        if event.generation > self.generation {
            self.generation = event.generation;
            self.cursor = None;
            self.buffered.clear();
            self.source_keys.clear();
            self.members.clear();
            self.phase = BoundaryIngressPhase::Bootstrapping;
            self.replay_from = None;
        }

        if self.cursor.is_none() {
            if self.buffered.len() >= self.config.max_buffered
                && !self.buffered.contains_key(&event.cursor)
            {
                self.phase = BoundaryIngressPhase::Backpressured;
                self.replay_from = Some(0);
                self.bump();
                return BoundaryIngressTransition {
                    actions: Vec::new(),
                    changed: true,
                };
            }
            let changed = self.buffered.insert(event.cursor, event).is_none();
            if changed {
                self.bump();
            }
            return BoundaryIngressTransition {
                actions: Vec::new(),
                changed,
            };
        }

        let cursor = self.cursor.expect("checked");
        if event.cursor <= cursor || self.buffered.contains_key(&event.cursor) {
            return BoundaryIngressTransition::unchanged();
        }
        if event.cursor == cursor + 1 {
            let mut actions = Vec::new();
            self.apply_payload(event, &mut actions);
            self.drain_contiguous(&mut actions);
            self.bump();
            return BoundaryIngressTransition {
                actions,
                changed: true,
            };
        }
        if self.buffered.len() >= self.config.max_buffered {
            self.phase = BoundaryIngressPhase::Backpressured;
            self.replay_from = Some(cursor + 1);
            self.bump();
            return BoundaryIngressTransition {
                actions: Vec::new(),
                changed: true,
            };
        }
        self.buffered.insert(event.cursor, event);
        self.phase = BoundaryIngressPhase::ReplayRequired;
        self.replay_from = Some(cursor + 1);
        self.bump();
        BoundaryIngressTransition {
            actions: Vec::new(),
            changed: true,
        }
    }

    fn drain_contiguous(&mut self, actions: &mut Vec<BoundaryIngressAction<K, T>>) {
        let Some(mut cursor) = self.cursor else {
            return;
        };
        while let Some(event) = self.buffered.remove(&(cursor + 1)) {
            self.apply_payload(event, actions);
            cursor = self.cursor.expect("payload advances cursor");
        }
        if self.buffered.is_empty() {
            self.replay_from = None;
            self.phase = if self.validation == BoundaryValidation::Valid {
                BoundaryIngressPhase::Live
            } else {
                BoundaryIngressPhase::Invalid
            };
        } else {
            self.replay_from = Some(cursor + 1);
            self.phase = BoundaryIngressPhase::ReplayRequired;
        }
    }

    fn apply_payload(
        &mut self,
        event: BoundaryIngressEvent<K, T>,
        actions: &mut Vec<BoundaryIngressAction<K, T>>,
    ) {
        self.cursor = Some(event.cursor);
        self.last_stamped_at = Some(event.stamped_at);
        match event.payload {
            BoundaryIngressPayload::Upsert(envelope) => {
                self.source_keys.insert(envelope.key.clone());
                actions.push(BoundaryIngressAction::Admit(envelope));
            }
            BoundaryIngressPayload::Remove(key) => {
                self.source_keys.remove(&key);
                actions.push(BoundaryIngressAction::Close(key));
            }
            BoundaryIngressPayload::Validate(validation) => {
                self.validation = validation;
            }
        }
    }

    /// Publish current subscriber membership.
    pub fn member_join(&mut self, member: impl Into<String>) -> bool {
        let member = member.into();
        if !self.members.insert(member.clone()) {
            return false;
        }
        if let Some(receipt_id) = self.active_delivery.as_ref()
            && let Some(receipt) = self.deliveries.get_mut(receipt_id)
            && receipt.targets.is_empty()
        {
            receipt.targets.insert(member);
        }
        self.bump();
        true
    }

    /// Remove live membership without shrinking any captured receipt frontier.
    pub fn member_leave(&mut self, member: &str) -> bool {
        if !self.members.remove(member) {
            return false;
        }
        self.bump();
        true
    }

    /// Open or return one durable delivery receipt.
    pub fn open_delivery(&mut self, receipt_id: impl Into<String>) -> bool {
        let receipt_id = receipt_id.into();
        if self.deliveries.contains_key(&receipt_id) {
            self.active_delivery = Some(receipt_id);
            return false;
        }
        self.deliveries.insert(
            receipt_id.clone(),
            BoundaryDeliveryReceipt {
                receipt_id: receipt_id.clone(),
                targets: self.members.clone(),
                acknowledged: BTreeSet::new(),
            },
        );
        self.active_delivery = Some(receipt_id);
        self.bump();
        true
    }

    /// Fold one idempotent acknowledgement fact.
    pub fn acknowledge(&mut self, receipt_id: &str, member: &str) -> bool {
        let Some(receipt) = self.deliveries.get_mut(receipt_id) else {
            return false;
        };
        if !receipt.targets.contains(member) || !receipt.acknowledged.insert(member.to_string()) {
            return false;
        }
        self.bump();
        true
    }

    /// Advance logical time. Only a freshness edge advances the revision.
    pub fn tick(&mut self, now: u64) -> bool {
        let before = self.freshness();
        self.now = now;
        if self.freshness() == before {
            return false;
        }
        self.bump();
        true
    }

    /// Derived freshness.
    pub fn freshness(&self) -> BoundaryFreshness {
        let Some(stamped_at) = self.last_stamped_at else {
            return BoundaryFreshness::Unknown;
        };
        if self.now.saturating_sub(stamped_at) <= self.config.freshness_horizon {
            BoundaryFreshness::Fresh
        } else {
            BoundaryFreshness::Stale
        }
    }

    /// Atomic read model for a reactive Source.
    pub fn projection(&self) -> BoundaryIngressProjection<K> {
        BoundaryIngressProjection {
            phase: self.phase,
            generation: self.generation,
            cursor: self.cursor,
            buffered_cursors: self.buffered.keys().copied().collect(),
            source_keys: self.source_keys.iter().cloned().collect(),
            members: self.members.iter().cloned().collect(),
            validation: self.validation,
            freshness: self.freshness(),
            replay_from: self.replay_from,
            stale_events: self.stale_events,
            active_delivery: self
                .active_delivery
                .as_ref()
                .and_then(|id| self.deliveries.get(id))
                .cloned(),
            revision: self.revision,
        }
    }
}

impl<K, T> Default for BoundaryIngressCore<K, T>
where
    K: Ord + Clone,
{
    fn default() -> Self {
        Self::new(BoundaryIngressConfig::default())
    }
}
