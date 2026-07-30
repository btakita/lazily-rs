//! `IngressCore` — the graph-agnostic admission algebra behind every
//! `IngressCell` flavor (`#designimplementtransport`).
//!
//! Same split as [`TopicCore`](crate::topic_core) does for the broadcast family
//! and [`KeyedOrder`](crate::keyed_order) does for the map family, and for the
//! same reason: deciding whether an inbound envelope is *admissible* touches no
//! handle and awaits nothing, so single-threaded, `Send + Sync`, and async shells
//! share it verbatim — while **reactivity deliberately stays out**. Invalidation
//! is a graph write, so each flavor mints its own per-scope readers on its own
//! graph and clears them with the storage lock released.
//!
//! Every mutator therefore returns [`IngressChange`] — *which* reader kinds the
//! transition dirtied — rather than performing the invalidation itself. That
//! return value is the whole contract between the core and a shell, and it is a
//! pure function of the transition, which is what makes the plane portable across
//! flavors without re-deriving values per flavor.
//!
//! # Transport-agnostic by construction
//!
//! The core never touches a transport. An envelope is a value
//! ([`IngressEnvelope`]) carrying its own provenance — `generation`, `sequence`,
//! `stamped_at` — so a WebSocket frame, an RPC response, and a polled page are
//! the *same* input once decoded. That is what makes the admission decisions
//! (stale rejection, dedupe, reorder, freshness, backpressure) independent of how
//! bytes arrived, and it is why [`IngressTransportKind`] exists only to derive a
//! *schedule*: event delivery needs no polling, and a bounded poll interval is
//! offered only where an event channel is unavailable.
//!
//! # What is a derive and what is a call
//!
//! Readiness, authority, and retry are **not** imperative refresh calls. They are
//! pure functions of scope state ([`ScopeView::readiness`],
//! [`ScopeView::authority`], [`ScopeView::retry`]) that each shell exposes as a
//! `Computed`. Freshness is time-dependent, so it enters through an explicit
//! [`IngressCore::tick`] rather than a hidden clock read — the same discipline
//! [`TimerCell::tick`](crate::TimerCell::tick) uses, and the reason staleness
//! transitions are deterministic in tests.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;

use crate::merge::MergePolicy;
use crate::relay::Overflow;

/// How envelopes reach a scope. Event delivery is the default and needs no
/// schedule; the other two exist so a binding without an event channel still has
/// a *bounded* fallback rather than an unbounded refresh loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressTransportKind {
    /// Server-initiated delivery (WebSocket, SSE, in-proc channel). Preferred.
    EventChannel,
    /// Client-initiated, but triggered by an out-of-band event rather than a
    /// timer — an RPC issued *because* something happened.
    RpcTriggered,
    /// Client-initiated on a bounded interval. The fallback of last resort.
    BoundedPolling,
}

/// When, if ever, a scope should ask the transport for more data.
///
/// `poll_interval` is `Some` only for [`IngressTransportKind::BoundedPolling`] —
/// making "we polled a transport that pushes" unrepresentable rather than merely
/// discouraged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressSchedule {
    /// The transport this schedule was derived from.
    pub kind: IngressTransportKind,
    /// Bounded poll period, or `None` when delivery is event-driven.
    pub poll_interval: Option<u64>,
}

impl IngressSchedule {
    /// Derive the schedule for `kind`. A poll interval is offered only where
    /// event delivery is unavailable, and never zero.
    pub fn for_kind(kind: IngressTransportKind, poll_interval: u64) -> Self {
        Self {
            kind,
            poll_interval: match kind {
                IngressTransportKind::BoundedPolling => Some(poll_interval.max(1)),
                IngressTransportKind::EventChannel | IngressTransportKind::RpcTriggered => None,
            },
        }
    }
}

/// A decoded source of envelopes.
///
/// The core never calls this — a shell's `pump` does — which is exactly what
/// keeps admission independent of delivery. Implementations decode; they do not
/// decide.
pub trait IngressTransport<K, T> {
    /// How this transport delivers. Drives [`IngressSchedule`] and nothing else.
    fn kind(&self) -> IngressTransportKind;

    /// Take everything decoded since the last call. Never blocks.
    fn drain(&mut self) -> Vec<IngressEnvelope<K, T>>;

    /// Ask the producer to resend from `request.from_sequence`. Returns whether
    /// the transport could carry the request — a polling transport that cannot
    /// address history answers `false`, which is what makes "this gap will never
    /// close" observable rather than silent.
    fn request_replay(&mut self, key: &K, request: ReplayRequest) -> bool;
}

/// An in-process event channel: the reference [`IngressTransport`], and the one
/// the conformance corpus replays against.
///
/// `kind` is configurable so one implementation exercises all three delivery
/// modes — including the `BoundedPolling` case that cannot serve a replay.
pub struct InProcIngress<K, T> {
    kind: IngressTransportKind,
    inbound: VecDeque<IngressEnvelope<K, T>>,
    replays: Vec<(K, ReplayRequest)>,
}

impl<K, T> InProcIngress<K, T> {
    /// An empty channel delivering as `kind`.
    pub fn new(kind: IngressTransportKind) -> Self {
        Self {
            kind,
            inbound: VecDeque::new(),
            replays: Vec::new(),
        }
    }

    /// Queue one envelope for the next [`IngressTransport::drain`].
    pub fn push(&mut self, envelope: IngressEnvelope<K, T>) {
        self.inbound.push_back(envelope);
    }

    /// Replay requests observed so far, oldest first.
    pub fn replays(&self) -> &[(K, ReplayRequest)] {
        &self.replays
    }
}

impl<K, T> IngressTransport<K, T> for InProcIngress<K, T>
where
    K: Clone,
{
    fn kind(&self) -> IngressTransportKind {
        self.kind
    }

    fn drain(&mut self) -> Vec<IngressEnvelope<K, T>> {
        self.inbound.drain(..).collect()
    }

    fn request_replay(&mut self, key: &K, request: ReplayRequest) -> bool {
        // A bounded poll has no addressable history: it can only wait for the
        // next page, so it cannot honour a replay.
        if self.kind == IngressTransportKind::BoundedPolling {
            return false;
        }
        self.replays.push((key.clone(), request));
        true
    }
}

/// One decoded inbound message, with the provenance admission needs.
///
/// `generation` fences a producer incarnation (a reconnect, a redeploy, a build
/// skew); `sequence` orders within a generation; `stamped_at` is the producer's
/// logical time, which is what freshness is measured against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressEnvelope<K, T> {
    /// Lifecycle-scoped identity this envelope belongs to.
    pub key: K,
    /// Producer incarnation. Monotone per key; a higher value fences lower ones.
    pub generation: u64,
    /// Position within `generation`, starting at 0.
    pub sequence: u64,
    /// Producer's logical timestamp, compared against the freshness horizon.
    pub stamped_at: u64,
    /// The decoded payload.
    pub payload: T,
}

impl<K, T> IngressEnvelope<K, T> {
    /// An envelope at `generation`/`sequence` stamped `stamped_at`.
    pub fn new(key: K, generation: u64, sequence: u64, stamped_at: u64, payload: T) -> Self {
        Self {
            key,
            generation,
            sequence,
            stamped_at,
            payload,
        }
    }
}

/// Why an envelope was refused. Every variant is a *decision*, not a failure —
/// dropping a superseded envelope is correct behaviour and is receipted as such.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressDropReason {
    /// `generation` is below the scope's fence: a zombie producer.
    StaleGeneration,
    /// `sequence` was already delivered in this generation.
    DuplicateSequence,
    /// `sequence` is already sitting in the reorder buffer.
    DuplicateBuffered,
    /// The reorder buffer is at `reorder_window` and this envelope does not fill
    /// the gap.
    ReorderWindowOverflow,
    /// `now - stamped_at` exceeds the freshness horizon.
    Expired,
    /// The hot window is at `high_water` under a bounding overflow policy.
    Backpressure,
    /// The scope is closed; it admits nothing until reopened.
    ScopeClosed,
}

/// A transport- or decode-level failure attributed to a scope. Distinct from a
/// drop: an error means we could not *decide*, so it drives retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressError {
    /// The transport closed or reset under us.
    TransportClosed,
    /// The frame could not be decoded into an envelope.
    DecodeFailed,
    /// The producer reported that our generation is no longer authoritative.
    AuthorityLost,
}

/// The outcome of admitting one envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressAdmission {
    /// Delivered in order, and the window holds exactly this one op.
    Accepted {
        /// Highest in-order sequence now delivered (buffered successors flush).
        delivered_through: u64,
    },
    /// Delivered in order and coalesced with at least one other op — either a
    /// prior undrained op, or a buffered successor this delivery flushed.
    Conflated {
        /// Highest in-order sequence now delivered.
        delivered_through: u64,
    },
    /// Held pending an earlier sequence. Nothing is visible yet.
    Buffered {
        /// The first sequence still missing.
        gap_from: u64,
    },
    /// A newer producer incarnation took over: sequence expectations reset and
    /// the envelope was delivered.
    GenerationHandoff {
        /// The fence we held.
        from: u64,
        /// The fence we now hold.
        to: u64,
    },
    /// Refused, with the reason receipted.
    Dropped(IngressDropReason),
    /// Refused by [`Overflow::Block`]; the producer must retry after a drain.
    Blocked,
}

impl IngressAdmission {
    /// Whether the envelope became visible to readers.
    pub fn is_delivered(&self) -> bool {
        matches!(
            self,
            IngressAdmission::Accepted { .. }
                | IngressAdmission::Conflated { .. }
                | IngressAdmission::GenerationHandoff { .. }
        )
    }
}

/// Where a scope is in its lifecycle. Scopes are keyed and independent: closing
/// one never touches another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressLifecycle {
    /// Opened, nothing delivered yet.
    Opening,
    /// Delivering.
    Live,
    /// Disconnected but retained: state and cursors survive for replay.
    Suspended,
    /// Terminal until reopened. Admits nothing.
    Closed,
}

/// The derived answer to "can a consumer trust this scope right now?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressReadiness {
    /// No such scope.
    Unknown,
    /// Open, nothing delivered yet.
    Warming,
    /// Delivered and inside the freshness horizon.
    Ready,
    /// Delivered, but the newest accepted stamp is older than the horizon.
    Stale,
    /// Disconnected; retained state may be replayed.
    Suspended,
    /// Terminal.
    Closed,
}

/// What the scope currently claims authority over — the fence plus the in-order
/// watermark a replay request must resume from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressAuthority {
    /// The generation fence currently held.
    pub generation: u64,
    /// Highest in-order sequence delivered, or `None` before first delivery.
    pub delivered_through: Option<u64>,
    /// Producer stamp of the newest delivered envelope.
    pub stamped_at: u64,
}

/// The derived retry decision for a scope that has errored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressRetry {
    /// Consecutive errors since the last delivery.
    pub attempt: u32,
    /// Exponential backoff, clamped to the policy ceiling.
    pub backoff: u64,
    /// Sequence a replay should resume from.
    pub resume_from: u64,
}

/// What a reconnect needs from the transport to close its gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayRequest {
    /// The generation being resumed.
    pub generation: u64,
    /// First sequence the consumer has not delivered.
    pub from_sequence: u64,
}

/// Bounds and taxes, all flavor-neutral.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressPolicy {
    /// How many out-of-order envelopes may be held per scope. `0` disables
    /// reordering: a gap drops immediately.
    pub reorder_window: usize,
    /// `now - stamped_at` above this marks a scope [`IngressReadiness::Stale`];
    /// an *arriving* envelope that old is dropped as
    /// [`IngressDropReason::Expired`].
    pub freshness_horizon: u64,
    /// Merged-op count at which `overflow` engages.
    pub high_water: u64,
    /// What to do at `high_water`.
    pub overflow: Overflow,
    /// Retained receipts, oldest evicted first.
    pub receipt_capacity: usize,
    /// First retry backoff; doubles per consecutive error.
    pub retry_base: u64,
    /// Backoff clamp.
    pub retry_ceiling: u64,
}

impl Default for IngressPolicy {
    fn default() -> Self {
        Self {
            reorder_window: 8,
            freshness_horizon: 1_000,
            high_water: 64,
            overflow: Overflow::Conflate,
            receipt_capacity: 256,
            retry_base: 10,
            retry_ceiling: 10_000,
        }
    }
}

/// Why a policy was refused at construction time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressConfigError {
    /// [`Overflow::Conflate`] chosen for a non-conflating merge policy.
    ConflateNotBounding,
    /// A zero receipt capacity would discard every receipt it just minted.
    ZeroReceiptCapacity,
}

/// Which receipt channel a receipt belongs to. The three are separate reader
/// kinds because they have separate consumers: a projection wants accepts, a
/// dashboard wants drops, a supervisor wants errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressReceiptChannel {
    /// Delivered.
    Accepted,
    /// Refused by a decision.
    Dropped,
    /// Could not be decided.
    Error,
}

/// The decision a receipt records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressReceiptOutcome {
    /// Delivered, with the resulting watermark.
    Accepted {
        /// Highest in-order sequence delivered after this envelope.
        delivered_through: u64,
        /// Whether the payload coalesced into a non-empty window.
        conflated: bool,
    },
    /// Refused by a decision.
    Dropped(IngressDropReason),
    /// Could not be decided.
    Error(IngressError),
}

/// One durable record of an admission decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressReceipt<K> {
    /// Monotone receipt offset, stable across eviction.
    pub offset: u64,
    /// Scope the decision was made for.
    pub key: K,
    /// Generation the decision was made under.
    pub generation: u64,
    /// Sequence the decision was made for, when there was one.
    pub sequence: Option<u64>,
    /// The decision.
    pub outcome: IngressReceiptOutcome,
}

impl<K> IngressReceipt<K> {
    /// The channel this receipt is read from.
    pub fn channel(&self) -> IngressReceiptChannel {
        match self.outcome {
            IngressReceiptOutcome::Accepted { .. } => IngressReceiptChannel::Accepted,
            IngressReceiptOutcome::Dropped(_) => IngressReceiptChannel::Dropped,
            IngressReceiptOutcome::Error(_) => IngressReceiptChannel::Error,
        }
    }
}

/// Which of a scope's reader kinds a transition dirtied.
///
/// Four kinds exist because they have four different invalidation boundaries: a
/// buffered envelope moves nothing but its own gap, a `tick` across the horizon
/// moves only readiness, and an error moves only retry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngressScopeChange {
    /// The coalesced window changed.
    pub value: bool,
    /// [`IngressReadiness`] changed.
    pub readiness: bool,
    /// [`IngressAuthority`] changed.
    pub authority: bool,
    /// [`IngressRetry`] changed.
    pub retry: bool,
}

impl IngressScopeChange {
    /// Nothing changed — the shell must not clear a slot.
    pub fn is_empty(&self) -> bool {
        !(self.value || self.readiness || self.authority || self.retry)
    }

    fn all() -> Self {
        Self {
            value: true,
            readiness: true,
            authority: true,
            retry: true,
        }
    }

    fn readiness_only() -> Self {
        Self {
            readiness: true,
            ..Self::default()
        }
    }

    fn value_only() -> Self {
        Self {
            value: true,
            ..Self::default()
        }
    }

    fn retry_only() -> Self {
        Self {
            retry: true,
            ..Self::default()
        }
    }

    /// What materializing a previously-unknown scope changes: an unknown scope
    /// reads `Unknown`/`None`, so its first appearance moves readiness and
    /// authority — and nothing else. A reader that observed a key before it
    /// opened must learn that it did.
    fn creation() -> Self {
        Self {
            readiness: true,
            authority: true,
            ..Self::default()
        }
    }

    fn union(self, other: Self) -> Self {
        Self {
            value: self.value || other.value,
            readiness: self.readiness || other.readiness,
            authority: self.authority || other.authority,
            retry: self.retry || other.retry,
        }
    }
}

/// The pure invalidation set of one transition: the whole contract between the
/// core and a flavor shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressChange<K> {
    /// Per-scope dirty reader kinds, in transition order.
    pub scopes: Vec<(K, IngressScopeChange)>,
    /// The accepted-receipt reader grew.
    pub accepted_receipts: bool,
    /// The dropped-receipt reader grew.
    pub dropped_receipts: bool,
    /// The error-receipt reader grew.
    pub error_receipts: bool,
}

impl<K> Default for IngressChange<K> {
    fn default() -> Self {
        Self {
            scopes: Vec::new(),
            accepted_receipts: false,
            dropped_receipts: false,
            error_receipts: false,
        }
    }
}

impl<K> IngressChange<K> {
    /// Whether this transition dirtied nothing at all.
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
            && !self.accepted_receipts
            && !self.dropped_receipts
            && !self.error_receipts
    }

    fn mark(&mut self, key: K, change: IngressScopeChange) {
        if !change.is_empty() {
            self.scopes.push((key, change));
        }
    }

    fn mark_channel(&mut self, channel: IngressReceiptChannel) {
        match channel {
            IngressReceiptChannel::Accepted => self.accepted_receipts = true,
            IngressReceiptChannel::Dropped => self.dropped_receipts = true,
            IngressReceiptChannel::Error => self.error_receipts = true,
        }
    }
}

/// Read-only projection of one scope, from which every derive is computed.
///
/// A shell's `Computed` closures call these and nothing else, which is why the
/// three flavors cannot disagree about readiness, authority, or retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeView {
    /// Lifecycle position.
    pub lifecycle: IngressLifecycle,
    /// Generation fence.
    pub generation: u64,
    /// In-order watermark.
    pub delivered_through: Option<u64>,
    /// Producer stamp of the newest delivered envelope.
    pub stamped_at: u64,
    /// Buffered out-of-order envelopes.
    pub buffered: usize,
    /// Merged ops in the hot window.
    pub window_depth: u64,
    /// Consecutive errors since the last delivery.
    pub consecutive_errors: u32,
    /// Logical now, as of the last [`IngressCore::tick`].
    pub observed_now: u64,
    /// Bounds in force.
    pub policy: IngressPolicy,
}

impl ScopeView {
    /// Whether the newest delivered stamp is inside the freshness horizon.
    pub fn is_fresh(&self) -> bool {
        self.observed_now.saturating_sub(self.stamped_at) <= self.policy.freshness_horizon
    }

    /// Derived readiness. A scope that has never delivered is `Warming`, not
    /// `Stale`, because there is no stamp to be old.
    pub fn readiness(&self) -> IngressReadiness {
        match self.lifecycle {
            IngressLifecycle::Closed => IngressReadiness::Closed,
            IngressLifecycle::Suspended => IngressReadiness::Suspended,
            IngressLifecycle::Opening => IngressReadiness::Warming,
            IngressLifecycle::Live => {
                if self.delivered_through.is_none() {
                    IngressReadiness::Warming
                } else if self.is_fresh() {
                    IngressReadiness::Ready
                } else {
                    IngressReadiness::Stale
                }
            }
        }
    }

    /// Derived authority. A closed scope claims none.
    pub fn authority(&self) -> Option<IngressAuthority> {
        if self.lifecycle == IngressLifecycle::Closed {
            return None;
        }
        Some(IngressAuthority {
            generation: self.generation,
            delivered_through: self.delivered_through,
            stamped_at: self.stamped_at,
        })
    }

    /// The first sequence not yet delivered in order.
    pub fn resume_from(&self) -> u64 {
        self.delivered_through.map_or(0, |seq| seq + 1)
    }

    /// Whether the scope is holding a gap open — an out-of-order buffer that a
    /// replay, not a retry, is the fix for.
    pub fn has_gap(&self) -> bool {
        self.buffered > 0
    }

    /// Derived retry. `None` while no error is outstanding — a healthy scope has
    /// no backoff, rather than a zero one.
    pub fn retry(&self) -> Option<IngressRetry> {
        if self.consecutive_errors == 0 {
            return None;
        }
        let shift = self.consecutive_errors.saturating_sub(1).min(31);
        let backoff = self
            .policy
            .retry_base
            .saturating_mul(1u64 << shift)
            .min(self.policy.retry_ceiling);
        Some(IngressRetry {
            attempt: self.consecutive_errors,
            backoff,
            resume_from: self.resume_from(),
        })
    }
}

struct Scope<T> {
    lifecycle: IngressLifecycle,
    generation: u64,
    delivered_through: Option<u64>,
    stamped_at: u64,
    pending: BTreeMap<u64, (T, u64)>,
    window: Option<T>,
    window_depth: u64,
    consecutive_errors: u32,
}

impl<T> Scope<T> {
    fn new(generation: u64) -> Self {
        Self {
            lifecycle: IngressLifecycle::Opening,
            generation,
            delivered_through: None,
            stamped_at: 0,
            pending: BTreeMap::new(),
            window: None,
            window_depth: 0,
            consecutive_errors: 0,
        }
    }

    fn view(&self, observed_now: u64, policy: IngressPolicy) -> ScopeView {
        ScopeView {
            lifecycle: self.lifecycle,
            generation: self.generation,
            delivered_through: self.delivered_through,
            stamped_at: self.stamped_at,
            buffered: self.pending.len(),
            window_depth: self.window_depth,
            consecutive_errors: self.consecutive_errors,
            observed_now,
            policy,
        }
    }

    fn next_expected(&self) -> u64 {
        self.delivered_through.map_or(0, |seq| seq + 1)
    }

    /// Everything a reader can observe *about shape rather than payload*. The
    /// buffered path diffs these to derive its invalidation set, so "a buffered
    /// envelope invalidates nothing" is a computed fact rather than a claim — and
    /// the handoff-then-buffer case (which clears the window) cannot slip through.
    fn stamp(&self) -> (IngressLifecycle, u64, Option<u64>, bool) {
        (
            self.lifecycle,
            self.generation,
            self.delivered_through,
            self.window.is_some(),
        )
    }

    fn live_or_opening(&self) -> IngressLifecycle {
        if self.delivered_through.is_some() {
            IngressLifecycle::Live
        } else {
            IngressLifecycle::Opening
        }
    }
}

/// What the admission algebra decided, before any receipt is minted. Splitting
/// the decision from its bookkeeping is what keeps the scope borrow from
/// overlapping the receipt log.
enum Decision {
    Refuse(IngressDropReason),
    Block,
    Buffered {
        gap_from: u64,
    },
    Delivered {
        delivered_through: u64,
        conflated: bool,
        handoff: Option<(u64, u64)>,
    },
}

/// Keyed lifecycle scopes, an admission algebra, and a bounded receipt log. No
/// context, no handles, no interior mutability — each flavor wraps this in its
/// own `RefCell`/`Mutex`.
pub(crate) struct IngressCore<K, T, M> {
    policy: IngressPolicy,
    scopes: HashMap<K, Scope<T>>,
    receipts: VecDeque<IngressReceipt<K>>,
    next_receipt_offset: u64,
    observed_now: u64,
    _merge: PhantomData<M>,
}

impl<K, T, M> IngressCore<K, T, M>
where
    K: Eq + Hash + Clone,
    T: Clone,
    M: MergePolicy<T>,
{
    /// Build a core over `policy`, validating the overflow choice against the
    /// merge algebra the way [`RelayCell::new`](crate::RelayCell::new) does:
    /// `Conflate` bounds nothing for a non-conflating `⊕`.
    pub fn new(policy: IngressPolicy) -> Result<Self, IngressConfigError> {
        if policy.overflow == Overflow::Conflate && !M::CONFLATES {
            return Err(IngressConfigError::ConflateNotBounding);
        }
        if policy.receipt_capacity == 0 {
            return Err(IngressConfigError::ZeroReceiptCapacity);
        }
        Ok(Self {
            policy,
            scopes: HashMap::new(),
            receipts: VecDeque::new(),
            next_receipt_offset: 0,
            observed_now: 0,
            _merge: PhantomData,
        })
    }

    /// The bounds in force.
    pub fn policy(&self) -> IngressPolicy {
        self.policy
    }

    /// Every known scope key, for a shell rebuilding its reader table.
    pub fn scope_keys(&self) -> Vec<K> {
        self.scopes.keys().cloned().collect()
    }

    /// Read-only projection of one scope, or `None` when unknown.
    pub fn view(&self, key: &K) -> Option<ScopeView> {
        self.scopes
            .get(key)
            .map(|scope| scope.view(self.observed_now, self.policy))
    }

    /// Readiness of a scope. Unknown scopes are [`IngressReadiness::Unknown`]
    /// rather than an error: a reader may legitimately observe a key before it
    /// opens.
    pub fn readiness(&self, key: &K) -> IngressReadiness {
        self.view(key)
            .map_or(IngressReadiness::Unknown, |view| view.readiness())
    }

    /// Authority claimed by a scope.
    pub fn authority(&self, key: &K) -> Option<IngressAuthority> {
        self.view(key).and_then(|view| view.authority())
    }

    /// Retry decision for a scope.
    pub fn retry(&self, key: &K) -> Option<IngressRetry> {
        self.view(key).and_then(|view| view.retry())
    }

    /// The coalesced window awaiting drain.
    pub fn peek(&self, key: &K) -> Option<T> {
        self.scopes.get(key).and_then(|scope| scope.window.clone())
    }

    /// Receipts on one channel, oldest first.
    pub fn receipts(&self, channel: IngressReceiptChannel) -> Vec<IngressReceipt<K>> {
        self.receipts
            .iter()
            .filter(|receipt| receipt.channel() == channel)
            .cloned()
            .collect()
    }

    /// Open (or reopen) a scope at `generation`.
    ///
    /// Reopening a suspended scope preserves its watermark so a replay can resume
    /// from the gap; reopening a *closed* scope resets it, because a closed
    /// scope's producer is gone and its sequence space is not resumable.
    pub fn open(&mut self, key: K, generation: u64) -> IngressChange<K> {
        let mut change = IngressChange::default();
        if !self.scopes.contains_key(&key) {
            self.scopes.insert(key.clone(), Scope::new(generation));
            change.mark(key, IngressScopeChange::creation());
            return change;
        }
        let scope = self.scopes.get_mut(&key).expect("scope present");
        let before = (scope.lifecycle, scope.generation, scope.delivered_through);
        if scope.lifecycle == IngressLifecycle::Closed {
            *scope = Scope::new(generation);
        } else {
            scope.lifecycle = scope.live_or_opening();
            if generation > scope.generation {
                scope.generation = generation;
                scope.delivered_through = None;
                scope.pending.clear();
            }
        }
        let after = (scope.lifecycle, scope.generation, scope.delivered_through);
        if before != after {
            change.mark(
                key,
                IngressScopeChange {
                    value: false,
                    readiness: before.0 != after.0,
                    authority: true,
                    retry: false,
                },
            );
        }
        change
    }

    /// Suspend a scope: retain state and cursors, stop delivering. Returns the
    /// replay request a reconnect will need, or `None` when there was nothing to
    /// suspend.
    pub fn suspend(&mut self, key: &K) -> (IngressChange<K>, Option<ReplayRequest>) {
        let mut change = IngressChange::default();
        let Some(scope) = self.scopes.get_mut(key) else {
            return (change, None);
        };
        if matches!(
            scope.lifecycle,
            IngressLifecycle::Suspended | IngressLifecycle::Closed
        ) {
            return (change, None);
        }
        scope.lifecycle = IngressLifecycle::Suspended;
        let request = ReplayRequest {
            generation: scope.generation,
            from_sequence: scope.next_expected(),
        };
        change.mark(key.clone(), IngressScopeChange::readiness_only());
        (change, Some(request))
    }

    /// Reconnect a scope at `generation`, clearing the error streak.
    ///
    /// A higher generation is a producer handoff: the sequence space restarts, so
    /// the buffered reorder window and the coalesced value are discarded rather
    /// than replayed against a fence they no longer belong to.
    pub fn reconnect(&mut self, key: &K, generation: u64) -> (IngressChange<K>, ReplayRequest) {
        let mut change = IngressChange::default();
        let created = !self.scopes.contains_key(key);
        let scope = self
            .scopes
            .entry(key.clone())
            .or_insert_with(|| Scope::new(generation));
        let handoff = generation > scope.generation;
        let had_window = scope.window.is_some();
        if handoff {
            scope.generation = generation;
            scope.delivered_through = None;
            scope.pending.clear();
            scope.window = None;
            scope.window_depth = 0;
        }
        let before_lifecycle = scope.lifecycle;
        scope.lifecycle = scope.live_or_opening();
        let had_errors = scope.consecutive_errors > 0;
        scope.consecutive_errors = 0;
        let request = ReplayRequest {
            generation: scope.generation,
            from_sequence: scope.next_expected(),
        };
        let after_lifecycle = scope.lifecycle;
        let base = IngressScopeChange {
            value: handoff && had_window,
            readiness: before_lifecycle != after_lifecycle,
            authority: handoff,
            retry: had_errors,
        };
        change.mark(
            key.clone(),
            if created {
                base.union(IngressScopeChange::creation())
            } else {
                base
            },
        );
        (change, request)
    }

    /// Close a scope. It admits nothing and claims no authority until reopened.
    pub fn close(&mut self, key: &K) -> IngressChange<K> {
        let mut change = IngressChange::default();
        let Some(scope) = self.scopes.get_mut(key) else {
            return change;
        };
        if scope.lifecycle == IngressLifecycle::Closed {
            return change;
        }
        let had_window = scope.window.is_some();
        let had_errors = scope.consecutive_errors > 0;
        scope.lifecycle = IngressLifecycle::Closed;
        scope.pending.clear();
        scope.window = None;
        scope.window_depth = 0;
        scope.consecutive_errors = 0;
        change.mark(
            key.clone(),
            IngressScopeChange {
                value: had_window,
                readiness: true,
                authority: true,
                retry: had_errors,
            },
        );
        change
    }

    /// Advance logical time. Only scopes that *crossed* the freshness horizon are
    /// dirtied — a tick inside the horizon invalidates nothing, which is what
    /// keeps a polling shell from re-rendering on every tick.
    pub fn tick(&mut self, now: u64) -> IngressChange<K> {
        let mut change = IngressChange::default();
        if now == self.observed_now {
            return change;
        }
        let policy = self.policy;
        let before = self.observed_now;
        self.observed_now = now;
        for (key, scope) in &self.scopes {
            if scope.view(before, policy).readiness() != scope.view(now, policy).readiness() {
                change.mark(key.clone(), IngressScopeChange::readiness_only());
            }
        }
        change
    }

    /// Record a transport/decode failure against a scope, deepening its backoff.
    pub fn fail(&mut self, key: &K, error: IngressError) -> IngressChange<K> {
        let mut change = IngressChange::default();
        let created = !self.scopes.contains_key(key);
        let scope = self
            .scopes
            .entry(key.clone())
            .or_insert_with(|| Scope::new(0));
        scope.consecutive_errors = scope.consecutive_errors.saturating_add(1);
        let generation = scope.generation;
        let base = IngressScopeChange::retry_only();
        change.mark(
            key.clone(),
            if created {
                base.union(IngressScopeChange::creation())
            } else {
                base
            },
        );
        let channel = self.push_receipt(IngressReceipt {
            offset: 0,
            key: key.clone(),
            generation,
            sequence: None,
            outcome: IngressReceiptOutcome::Error(error),
        });
        change.mark_channel(channel);
        change
    }

    /// Drain a scope's coalesced window, resetting its depth. Returns `None` for
    /// an empty window and dirties nothing.
    ///
    /// A drain is an *egress*, not an ack: it never moves the watermark, so a
    /// replay after a drain still resumes from the same sequence.
    pub fn drain(&mut self, key: &K) -> (IngressChange<K>, Option<T>) {
        let mut change = IngressChange::default();
        let Some(scope) = self.scopes.get_mut(key) else {
            return (change, None);
        };
        let value = scope.window.take();
        if value.is_none() {
            return (change, None);
        }
        scope.window_depth = 0;
        change.mark(key.clone(), IngressScopeChange::value_only());
        (change, value)
    }

    /// Admit one envelope, applying — in this order — scope lifecycle, the
    /// generation fence, freshness, dedupe, ordering, and backpressure.
    ///
    /// The order is the contract: a zombie generation is rejected before its
    /// stale sequence is consulted, and an expired envelope is rejected before it
    /// can occupy a reorder slot.
    pub fn admit(
        &mut self,
        envelope: IngressEnvelope<K, T>,
    ) -> (IngressChange<K>, IngressAdmission) {
        let policy = self.policy;
        let observed_now = self.observed_now;
        let IngressEnvelope {
            key,
            generation,
            sequence,
            stamped_at,
            payload,
        } = envelope;

        let created = !self.scopes.contains_key(&key);
        let before = self.scopes.get(&key).map(Scope::stamp);
        let decision = {
            let scope = self
                .scopes
                .entry(key.clone())
                .or_insert_with(|| Scope::new(generation));
            Self::decide(
                scope,
                policy,
                observed_now,
                generation,
                sequence,
                stamped_at,
                payload,
            )
        };

        // A refused envelope must not leave a scope behind: an expired or blocked
        // message for a key we do not track is not an admission plane, and
        // materializing one would report a readiness change that never happened.
        let admitted = matches!(
            decision,
            Decision::Buffered { .. } | Decision::Delivered { .. }
        );
        if created && !admitted {
            self.scopes.remove(&key);
        }

        let mut change = IngressChange::default();
        let fence = self
            .scopes
            .get(&key)
            .map_or(generation, |scope| scope.generation);

        match decision {
            Decision::Refuse(reason) => {
                let channel = self.push_receipt(IngressReceipt {
                    offset: 0,
                    key,
                    generation: fence,
                    sequence: Some(sequence),
                    outcome: IngressReceiptOutcome::Dropped(reason),
                });
                change.mark_channel(channel);
                (change, IngressAdmission::Dropped(reason))
            }
            Decision::Block => {
                let channel = self.push_receipt(IngressReceipt {
                    offset: 0,
                    key,
                    generation: fence,
                    sequence: Some(sequence),
                    outcome: IngressReceiptOutcome::Dropped(IngressDropReason::Backpressure),
                });
                change.mark_channel(channel);
                (change, IngressAdmission::Blocked)
            }
            Decision::Buffered { gap_from } => {
                // A buffered envelope mints no receipt, and for an already-current
                // scope it dirties no reader, because nothing a reader can observe
                // moved. Two cases are NOT invisible and are derived rather than
                // assumed: the scope's own first appearance (it moves off
                // `Unknown`), and a generation handoff that buffers — which resets
                // the fence, the watermark, and the window before parking the
                // envelope.
                let mut scope_change = if created {
                    IngressScopeChange::creation()
                } else {
                    IngressScopeChange::default()
                };
                if let (Some(before), Some(after)) =
                    (before, self.scopes.get(&key).map(Scope::stamp))
                {
                    scope_change = scope_change.union(IngressScopeChange {
                        value: before.3 != after.3,
                        readiness: before.0 != after.0 || before.2.is_none() != after.2.is_none(),
                        authority: before.1 != after.1 || before.2 != after.2,
                        retry: false,
                    });
                }
                change.mark(key, scope_change);
                (change, IngressAdmission::Buffered { gap_from })
            }
            Decision::Delivered {
                delivered_through,
                conflated,
                handoff,
            } => {
                change.mark(key.clone(), IngressScopeChange::all());
                let channel = self.push_receipt(IngressReceipt {
                    offset: 0,
                    key,
                    generation: fence,
                    sequence: Some(sequence),
                    outcome: IngressReceiptOutcome::Accepted {
                        delivered_through,
                        conflated,
                    },
                });
                change.mark_channel(channel);
                let admission = match handoff {
                    Some((from, to)) => IngressAdmission::GenerationHandoff { from, to },
                    None if conflated => IngressAdmission::Conflated { delivered_through },
                    None => IngressAdmission::Accepted { delivered_through },
                };
                (change, admission)
            }
        }
    }

    /// The admission algebra proper: pure over one scope, mutating only that
    /// scope, minting nothing.
    #[allow(clippy::too_many_arguments)]
    fn decide(
        scope: &mut Scope<T>,
        policy: IngressPolicy,
        observed_now: u64,
        generation: u64,
        sequence: u64,
        stamped_at: u64,
        payload: T,
    ) -> Decision {
        if scope.lifecycle == IngressLifecycle::Closed {
            return Decision::Refuse(IngressDropReason::ScopeClosed);
        }
        if generation < scope.generation {
            return Decision::Refuse(IngressDropReason::StaleGeneration);
        }
        if observed_now.saturating_sub(stamped_at) > policy.freshness_horizon {
            return Decision::Refuse(IngressDropReason::Expired);
        }

        let mut handoff = None;
        if generation > scope.generation {
            // A handoff is a baseline reset, not a continuation: the new
            // incarnation's first envelope is authoritative, so the old
            // incarnation's undrained window and buffered successors are
            // discarded rather than folded into it. Merging a superseded delta
            // into a fresh baseline is exactly the build-skew corruption the
            // generation fence exists to prevent, and it is the same rule
            // `reconnect` at a higher generation applies.
            handoff = Some((scope.generation, generation));
            scope.generation = generation;
            scope.delivered_through = None;
            scope.pending.clear();
            scope.window = None;
            scope.window_depth = 0;
        }

        let expected = scope.next_expected();
        if sequence < expected {
            return Decision::Refuse(IngressDropReason::DuplicateSequence);
        }
        if sequence > expected {
            if scope.pending.contains_key(&sequence) {
                return Decision::Refuse(IngressDropReason::DuplicateBuffered);
            }
            if scope.pending.len() >= policy.reorder_window {
                return Decision::Refuse(IngressDropReason::ReorderWindowOverflow);
            }
            scope.pending.insert(sequence, (payload, stamped_at));
            return Decision::Buffered { gap_from: expected };
        }

        // In order. Backpressure is checked here and not earlier: refusing an
        // in-order envelope leaves a gap the reorder buffer cannot close, so
        // `Block` must be observable by the producer as its own outcome.
        if scope.window_depth >= policy.high_water {
            match policy.overflow {
                Overflow::Block => return Decision::Block,
                Overflow::DropNewest => {
                    return Decision::Refuse(IngressDropReason::Backpressure);
                }
                Overflow::DropOldest => {
                    scope.window = None;
                    scope.window_depth = 0;
                }
                // `Conflate` *is* the bound; `Spill` degrades to it until a
                // durable tail is wired, exactly as `RelayCell` does.
                Overflow::Conflate | Overflow::Spill => {}
            }
        }

        let mut conflated = Self::merge_into(scope, payload, stamped_at);
        scope.delivered_through = Some(sequence);
        scope.lifecycle = IngressLifecycle::Live;
        scope.consecutive_errors = 0;
        let mut delivered_through = sequence;

        // Flush every buffered successor this delivery unblocked. One
        // invalidation covers the whole flush: readers observe the coalesced
        // window, never a partial replay.
        loop {
            let next = scope.next_expected();
            let Some((buffered, buffered_stamp)) = scope.pending.remove(&next) else {
                break;
            };
            conflated |= Self::merge_into(scope, buffered, buffered_stamp);
            scope.delivered_through = Some(next);
            delivered_through = next;
        }

        Decision::Delivered {
            delivered_through,
            conflated,
            handoff,
        }
    }

    /// Merge one payload into a scope's hot head. Returns whether it coalesced
    /// with an existing window.
    fn merge_into(scope: &mut Scope<T>, payload: T, stamped_at: u64) -> bool {
        let conflated = match scope.window.take() {
            None => {
                scope.window = Some(payload);
                false
            }
            Some(current) => {
                scope.window = Some(M::merge(&current, payload));
                true
            }
        };
        scope.window_depth += 1;
        scope.stamped_at = scope.stamped_at.max(stamped_at);
        conflated
    }

    fn push_receipt(&mut self, mut receipt: IngressReceipt<K>) -> IngressReceiptChannel {
        receipt.offset = self.next_receipt_offset;
        self.next_receipt_offset += 1;
        let channel = receipt.channel();
        self.receipts.push_back(receipt);
        while self.receipts.len() > self.policy.receipt_capacity {
            self.receipts.pop_front();
        }
        channel
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge::{KeepLatest, RawFifo, Sum};

    type Core = IngressCore<&'static str, u64, Sum>;

    fn core(policy: IngressPolicy) -> Core {
        IngressCore::new(policy).expect("policy")
    }

    fn env(
        key: &'static str,
        generation: u64,
        sequence: u64,
        stamped_at: u64,
        payload: u64,
    ) -> IngressEnvelope<&'static str, u64> {
        IngressEnvelope::new(key, generation, sequence, stamped_at, payload)
    }

    #[test]
    fn conflate_is_rejected_for_a_non_conflating_algebra() {
        let policy = IngressPolicy {
            overflow: Overflow::Conflate,
            ..IngressPolicy::default()
        };
        let built: Result<IngressCore<&str, Vec<u64>, RawFifo>, _> = IngressCore::new(policy);
        assert_eq!(built.err(), Some(IngressConfigError::ConflateNotBounding));
    }

    #[test]
    fn zero_receipt_capacity_is_rejected() {
        let policy = IngressPolicy {
            receipt_capacity: 0,
            ..IngressPolicy::default()
        };
        let built: Result<Core, _> = IngressCore::new(policy);
        assert_eq!(built.err(), Some(IngressConfigError::ZeroReceiptCapacity));
    }

    #[test]
    fn in_order_delivery_conflates_and_receipts() {
        let mut core = core(IngressPolicy::default());
        let (change, admission) = core.admit(env("a", 1, 0, 0, 5));
        assert_eq!(
            admission,
            IngressAdmission::Accepted {
                delivered_through: 0
            }
        );
        assert!(change.accepted_receipts);
        assert_eq!(change.scopes, vec![("a", IngressScopeChange::all())]);

        let (_, admission) = core.admit(env("a", 1, 1, 0, 7));
        assert_eq!(
            admission,
            IngressAdmission::Conflated {
                delivered_through: 1
            }
        );
        assert_eq!(core.peek(&"a"), Some(12));
        assert_eq!(core.receipts(IngressReceiptChannel::Accepted).len(), 2);
        assert!(core.receipts(IngressReceiptChannel::Dropped).is_empty());
    }

    #[test]
    fn reorder_buffers_then_flushes_in_one_invalidation() {
        let mut core = core(IngressPolicy::default());
        let (change, admission) = core.admit(env("a", 1, 2, 0, 4));
        assert_eq!(admission, IngressAdmission::Buffered { gap_from: 0 });
        // A buffered envelope mints no receipt and moves no value. The scope's
        // first appearance does move it off `Unknown`, and saying so is the
        // difference between a sound invalidation set and a reader stuck on
        // `Unknown` forever.
        assert!(!change.accepted_receipts && !change.dropped_receipts);
        assert_eq!(change.scopes, vec![("a", IngressScopeChange::creation())]);
        assert_eq!(core.peek(&"a"), None);

        let (change, admission) = core.admit(env("a", 1, 1, 0, 2));
        assert_eq!(admission, IngressAdmission::Buffered { gap_from: 0 });
        // Now the scope exists, so a second buffered envelope really is invisible.
        assert!(change.is_empty());

        let (_, admission) = core.admit(env("a", 1, 0, 0, 1));
        // Three ops coalesced, so the delivery reports `Conflated` even though the
        // window it started from was empty.
        assert_eq!(
            admission,
            IngressAdmission::Conflated {
                delivered_through: 2
            }
        );
        // 1 ⊕ 2 ⊕ 4 — the whole flush lands as one window.
        assert_eq!(core.peek(&"a"), Some(7));
        assert_eq!(core.view(&"a").expect("scope").buffered, 0);
        // Exactly one accepted receipt for the delivery that unblocked the flush.
        assert_eq!(core.receipts(IngressReceiptChannel::Accepted).len(), 1);
    }

    #[test]
    fn duplicates_are_dropped_after_delivery_and_while_buffered() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 1));
        let (_, admission) = core.admit(env("a", 1, 0, 0, 1));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::DuplicateSequence)
        );
        core.admit(env("a", 1, 5, 0, 1));
        let (_, admission) = core.admit(env("a", 1, 5, 0, 1));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::DuplicateBuffered)
        );
        assert_eq!(core.peek(&"a"), Some(1));
    }

    #[test]
    fn reorder_window_overflow_drops_rather_than_growing() {
        let mut core = core(IngressPolicy {
            reorder_window: 2,
            ..IngressPolicy::default()
        });
        core.admit(env("a", 1, 1, 0, 1));
        core.admit(env("a", 1, 2, 0, 1));
        let (_, admission) = core.admit(env("a", 1, 3, 0, 1));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::ReorderWindowOverflow)
        );
        assert_eq!(core.view(&"a").expect("scope").buffered, 2);
    }

    #[test]
    fn a_zero_reorder_window_drops_every_gap_immediately() {
        let mut core = core(IngressPolicy {
            reorder_window: 0,
            ..IngressPolicy::default()
        });
        let (_, admission) = core.admit(env("a", 1, 1, 0, 1));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::ReorderWindowOverflow)
        );
    }

    #[test]
    fn a_stale_generation_is_fenced_before_its_sequence_is_consulted() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 2, 0, 0, 1));
        // Sequence 0 would be a duplicate; generation 1 is stale. The fence wins,
        // which is what makes a zombie producer distinguishable from a retry.
        let (_, admission) = core.admit(env("a", 1, 0, 0, 9));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::StaleGeneration)
        );
        assert_eq!(core.peek(&"a"), Some(1));
    }

    #[test]
    fn a_newer_generation_hands_off_and_resets_the_sequence_space() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 1));
        core.admit(env("a", 1, 7, 0, 1));
        let (_, admission) = core.admit(env("a", 2, 0, 0, 4));
        assert_eq!(
            admission,
            IngressAdmission::GenerationHandoff { from: 1, to: 2 }
        );
        let view = core.view(&"a").expect("scope");
        assert_eq!(view.generation, 2);
        assert_eq!(view.delivered_through, Some(0));
        // The old generation's buffered successor is not replayed under the new
        // fence — its sequence numbers mean something else now.
        assert_eq!(view.buffered, 0);
        // Nor is its undrained window folded into the new baseline.
        assert_eq!(core.peek(&"a"), Some(4));
    }

    #[test]
    fn a_handoff_that_buffers_still_reports_the_baseline_reset() {
        // The case the formal model caught: a NEWER generation arriving out of
        // order resets the fence, the watermark, AND the window before parking the
        // envelope. Reporting that as "buffered, nothing changed" would leave every
        // reader showing the superseded generation's value forever.
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 5));
        let (change, admission) = core.admit(env("a", 2, 3, 0, 9));
        assert_eq!(admission, IngressAdmission::Buffered { gap_from: 0 });
        assert_eq!(
            change.scopes,
            vec![(
                "a",
                IngressScopeChange {
                    value: true,
                    readiness: true,
                    authority: true,
                    retry: false,
                }
            )]
        );
        assert_eq!(core.peek(&"a"), None);
        let view = core.view(&"a").expect("scope");
        assert_eq!(view.generation, 2);
        assert_eq!(view.delivered_through, None);
        assert_eq!(view.buffered, 1);
        // A buffered envelope under the SAME generation is still invisible.
        let (change, _) = core.admit(env("a", 2, 4, 0, 1));
        assert!(change.is_empty());
    }

    #[test]
    fn an_expired_envelope_never_occupies_a_reorder_slot() {
        let mut core = core(IngressPolicy {
            freshness_horizon: 10,
            reorder_window: 1,
            ..IngressPolicy::default()
        });
        core.tick(100);
        let (_, admission) = core.admit(env("a", 1, 3, 50, 1));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::Expired)
        );
        // A refused envelope leaves no scope behind: an expired message for an
        // untracked key is not an admission plane.
        assert_eq!(core.view(&"a"), None);
        // The slot is still free for a fresh out-of-order envelope.
        let (_, admission) = core.admit(env("a", 1, 3, 95, 1));
        assert_eq!(admission, IngressAdmission::Buffered { gap_from: 0 });
    }

    #[test]
    fn block_overflow_refuses_without_losing_the_window() {
        let mut core = IngressCore::<&str, u64, KeepLatest>::new(IngressPolicy {
            high_water: 1,
            overflow: Overflow::Block,
            ..IngressPolicy::default()
        })
        .expect("policy");
        core.admit(env("a", 1, 0, 0, 5));
        let (change, admission) = core.admit(env("a", 1, 1, 0, 9));
        assert_eq!(admission, IngressAdmission::Blocked);
        assert!(change.dropped_receipts);
        assert_eq!(core.peek(&"a"), Some(5));
        // The blocked envelope did not advance the watermark, so a producer retry
        // after a drain is still in order rather than a duplicate.
        assert_eq!(core.view(&"a").expect("scope").delivered_through, Some(0));
        core.drain(&"a");
        let (_, admission) = core.admit(env("a", 1, 1, 0, 9));
        assert_eq!(
            admission,
            IngressAdmission::Accepted {
                delivered_through: 1
            }
        );
    }

    #[test]
    fn drop_oldest_restarts_the_window_at_the_incoming_op() {
        let mut core = IngressCore::<&str, u64, Sum>::new(IngressPolicy {
            high_water: 2,
            overflow: Overflow::DropOldest,
            ..IngressPolicy::default()
        })
        .expect("policy");
        core.admit(env("a", 1, 0, 0, 1));
        core.admit(env("a", 1, 1, 0, 2));
        let (_, admission) = core.admit(env("a", 1, 2, 0, 30));
        assert_eq!(
            admission,
            IngressAdmission::Accepted {
                delivered_through: 2
            }
        );
        assert_eq!(core.peek(&"a"), Some(30));
    }

    #[test]
    fn drop_newest_keeps_the_window_and_receipts_the_drop() {
        let mut core = IngressCore::<&str, u64, Sum>::new(IngressPolicy {
            high_water: 1,
            overflow: Overflow::DropNewest,
            ..IngressPolicy::default()
        })
        .expect("policy");
        core.admit(env("a", 1, 0, 0, 5));
        let (change, admission) = core.admit(env("a", 1, 1, 0, 9));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::Backpressure)
        );
        assert!(change.dropped_receipts);
        assert_eq!(core.peek(&"a"), Some(5));
    }

    #[test]
    fn readiness_derives_from_lifecycle_and_freshness() {
        let mut core = core(IngressPolicy {
            freshness_horizon: 10,
            ..IngressPolicy::default()
        });
        assert_eq!(core.readiness(&"a"), IngressReadiness::Unknown);
        core.open("a", 1);
        assert_eq!(core.readiness(&"a"), IngressReadiness::Warming);
        core.admit(env("a", 1, 0, 0, 1));
        assert_eq!(core.readiness(&"a"), IngressReadiness::Ready);

        // Crossing the horizon is a readiness-only transition.
        let change = core.tick(50);
        assert_eq!(
            change.scopes,
            vec![("a", IngressScopeChange::readiness_only())]
        );
        assert_eq!(core.readiness(&"a"), IngressReadiness::Stale);
        // A further tick inside the same readiness dirties nothing.
        assert!(core.tick(60).is_empty());
    }

    #[test]
    fn suspend_retains_the_watermark_and_reconnect_replays_the_gap() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 1));
        core.admit(env("a", 1, 1, 0, 1));
        let (_, request) = core.suspend(&"a");
        assert_eq!(
            request,
            Some(ReplayRequest {
                generation: 1,
                from_sequence: 2
            })
        );
        assert_eq!(core.readiness(&"a"), IngressReadiness::Suspended);
        // The coalesced window survives a disconnect; only readiness changed.
        assert_eq!(core.peek(&"a"), Some(2));
        // Suspending twice is idempotent and dirties nothing.
        let (change, request) = core.suspend(&"a");
        assert!(change.is_empty());
        assert_eq!(request, None);

        let (_, request) = core.reconnect(&"a", 1);
        assert_eq!(
            request,
            ReplayRequest {
                generation: 1,
                from_sequence: 2
            }
        );
        assert_eq!(core.readiness(&"a"), IngressReadiness::Ready);
    }

    #[test]
    fn reconnect_at_a_higher_generation_discards_the_stale_window() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 5));
        core.suspend(&"a");
        let (change, request) = core.reconnect(&"a", 3);
        assert_eq!(
            request,
            ReplayRequest {
                generation: 3,
                from_sequence: 0
            }
        );
        assert!(change.scopes.iter().any(|(_, c)| c.value && c.authority));
        assert_eq!(core.peek(&"a"), None);
    }

    #[test]
    fn errors_deepen_backoff_and_a_delivery_clears_it() {
        let mut core = core(IngressPolicy {
            retry_base: 10,
            retry_ceiling: 25,
            ..IngressPolicy::default()
        });
        core.open("a", 1);
        assert_eq!(core.retry(&"a"), None);

        core.fail(&"a", IngressError::TransportClosed);
        assert_eq!(
            core.retry(&"a"),
            Some(IngressRetry {
                attempt: 1,
                backoff: 10,
                resume_from: 0
            })
        );
        core.fail(&"a", IngressError::TransportClosed);
        assert_eq!(core.retry(&"a").expect("retry").backoff, 20);
        // Clamped, not doubled past the ceiling.
        core.fail(&"a", IngressError::TransportClosed);
        assert_eq!(core.retry(&"a").expect("retry").backoff, 25);
        assert_eq!(core.receipts(IngressReceiptChannel::Error).len(), 3);

        core.admit(env("a", 1, 0, 0, 1));
        assert_eq!(core.retry(&"a"), None);
    }

    #[test]
    fn a_reconnect_clears_the_error_streak_without_a_delivery() {
        let mut core = core(IngressPolicy::default());
        core.open("a", 1);
        core.fail(&"a", IngressError::AuthorityLost);
        let (change, _) = core.reconnect(&"a", 1);
        assert!(change.scopes.iter().any(|(_, c)| c.retry));
        assert_eq!(core.retry(&"a"), None);
    }

    #[test]
    fn closed_scopes_admit_nothing_and_claim_no_authority() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 1));
        core.close(&"a");
        assert_eq!(core.authority(&"a"), None);
        let (_, admission) = core.admit(env("a", 1, 1, 0, 1));
        assert_eq!(
            admission,
            IngressAdmission::Dropped(IngressDropReason::ScopeClosed)
        );
        // Reopening a closed scope restarts its sequence space.
        core.open("a", 1);
        let (_, admission) = core.admit(env("a", 1, 0, 0, 4));
        assert_eq!(
            admission,
            IngressAdmission::Accepted {
                delivered_through: 0
            }
        );
    }

    #[test]
    fn scopes_are_independent() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 1));
        let (change, _) = core.admit(env("b", 1, 0, 0, 2));
        assert_eq!(change.scopes.len(), 1);
        assert_eq!(change.scopes[0].0, "b");
        core.close(&"b");
        assert_eq!(core.readiness(&"a"), IngressReadiness::Ready);
        assert_eq!(core.peek(&"a"), Some(1));
    }

    #[test]
    fn receipts_are_bounded_and_offsets_stay_monotone() {
        let mut core = core(IngressPolicy {
            receipt_capacity: 2,
            ..IngressPolicy::default()
        });
        for seq in 0..4 {
            core.admit(env("a", 1, seq, 0, 1));
        }
        let accepted = core.receipts(IngressReceiptChannel::Accepted);
        assert_eq!(accepted.len(), 2);
        assert_eq!(
            accepted.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn a_schedule_offers_a_poll_interval_only_without_event_delivery() {
        assert_eq!(
            IngressSchedule::for_kind(IngressTransportKind::EventChannel, 50).poll_interval,
            None
        );
        assert_eq!(
            IngressSchedule::for_kind(IngressTransportKind::RpcTriggered, 50).poll_interval,
            None
        );
        assert_eq!(
            IngressSchedule::for_kind(IngressTransportKind::BoundedPolling, 50).poll_interval,
            Some(50)
        );
        // A zero interval would be an unbounded refresh loop.
        assert_eq!(
            IngressSchedule::for_kind(IngressTransportKind::BoundedPolling, 0).poll_interval,
            Some(1)
        );
    }

    #[test]
    fn drain_is_a_value_only_transition_and_empty_drains_dirty_nothing() {
        let mut core = core(IngressPolicy::default());
        core.admit(env("a", 1, 0, 0, 3));
        let (change, value) = core.drain(&"a");
        assert_eq!(value, Some(3));
        assert_eq!(change.scopes, vec![("a", IngressScopeChange::value_only())]);
        let (change, value) = core.drain(&"a");
        assert_eq!(value, None);
        assert!(change.is_empty());
        // Draining does not move the watermark: a drain is an egress, not an ack.
        assert_eq!(core.view(&"a").expect("scope").delivered_through, Some(0));
    }

    #[test]
    fn out_of_order_arrival_converges_to_the_in_order_fold() {
        // The reordering tax is paid by the buffer, not by the algebra: for any
        // arrival permutation of a contiguous run, the drained window equals the
        // in-order fold.
        let permutations: [[u64; 4]; 5] = [
            [0, 1, 2, 3],
            [3, 2, 1, 0],
            [1, 0, 3, 2],
            [2, 0, 1, 3],
            [0, 3, 1, 2],
        ];
        for order in permutations {
            let mut core = core(IngressPolicy::default());
            for seq in order {
                core.admit(env("a", 1, seq, 0, 1 << seq));
            }
            assert_eq!(core.peek(&"a"), Some(1 + 2 + 4 + 8), "order {order:?}");
            assert_eq!(
                core.view(&"a").expect("scope").delivered_through,
                Some(3),
                "order {order:?}"
            );
        }
    }
}
