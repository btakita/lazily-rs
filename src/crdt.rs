//! CRDT-backed multi-write cells (#lazilycrdtrs).
//!
//! This module implements the **first multi-write merge mechanism** of the
//! lazily cell model (`lazily-spec/cell-model.md`): `merge: crdt`. The cell
//! model classifies every cell by concurrent-writer count —
//!
//! - **single-writer** (`local`/`direct`): exactly one writer, no merge — the
//!   ordinary [`Context::cell`](crate::Context::cell);
//! - **multi-write**: potentially many concurrent writers, converged by a
//!   pluggable `merge: <mechanism>` ingress on **root cells only**.
//!
//! `crdt` is the first mechanism defined (it converges without coordination);
//! [`MergeMechanism`] reserves `lww`/`ot`/`lease`/`custom` alongside it. A
//! [`ReplicatedCell`] is a multi-write **root** cell whose remote-op ingress
//! merge feeds the converged value into an ordinary reactive cell via
//! [`Context::set_cell`](crate::Context::set_cell). Everything downstream is
//! unchanged: derived slots recompute through the normal direct mechanism, and
//! the local `PartialEq` invalidation guard applies *after* merge — a merge
//! that yields an equal value invalidates nothing, exactly like a local equal
//! `set_cell`. Derived cells are never multi-write; effects stay single-writer
//! authority (see `SPEC.md` § Multi-writer coordination).
//!
//! The CRDT register types here (LWW / MV / PN) are the value shapes available
//! *within* `merge: crdt`; they are distinct from the [`MergeMechanism`] axis.
//! All three merges are commutative, associative, and idempotent, so replicas
//! converge regardless of delivery order or duplication.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;

use crate::Context;
use crate::cell::Source;
use crate::distributed::PeerId;
use crate::seq_crdt::SeqCrdt;
use crate::text_crdt::{OpId, TextCrdt};

/// The convergence mechanism a multi-write cell declares (`merge:`).
///
/// `Crdt` is the first normative mechanism — it converges without
/// coordination. The remaining variants are reserved extension points named to
/// fix the shape of the model; an implementation MUST reject an unimplemented
/// mechanism explicitly (via [`MergeMechanism::resolve`]) rather than silently
/// aliasing it to `Crdt`. Every mechanism MUST be deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum MergeMechanism {
    /// Conflict-free replicated data type (this module). Converges without
    /// coordination; covers the multi-value ([`MvRegister`]) and counter
    /// ([`PnCounter`]) register shapes, which retain concurrent writes rather
    /// than picking a single winner.
    Crdt,
    /// Last-writer-wins by [`HlcStamp`] at the cell level, backed by
    /// [`LwwRegister`]: the highest stamp wins and the losing concurrent write
    /// is dropped. The "current value" mechanism most reactive cells want.
    Lww,
    /// Operational transform (server-ordered op rebase). Reserved.
    Ot,
    /// Lease/lock-serialized single-*live*-writer. Reserved.
    Lease,
    /// Application-supplied deterministic merge function. Reserved.
    Custom,
}

/// Error returned when a [`MergeMechanism`] is named but not implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedMechanism(pub MergeMechanism);

impl std::fmt::Display for UnsupportedMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "merge mechanism {:?} is reserved but not implemented; only `crdt` is supported",
            self.0
        )
    }
}

impl std::error::Error for UnsupportedMechanism {}

impl MergeMechanism {
    /// Whether this mechanism has a working implementation in this build.
    pub fn is_implemented(self) -> bool {
        matches!(self, MergeMechanism::Crdt | MergeMechanism::Lww)
    }

    /// Fail-closed gate: returns `Ok` only for an implemented mechanism, never
    /// aliasing an unimplemented one to `crdt`.
    pub fn resolve(self) -> Result<Self, UnsupportedMechanism> {
        if self.is_implemented() {
            Ok(self)
        } else {
            Err(UnsupportedMechanism(self))
        }
    }
}

/// A hybrid logical clock stamp: wall-clock time for human-meaningful ordering,
/// a logical counter for causal tiebreak, and the originating peer.
///
/// Total order is `(wall_time, logical, peer)`, so a stamp is a deterministic
/// cross-peer tiebreaker for last-write-wins convergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HlcStamp {
    /// Wall-clock microseconds since the Unix epoch (supplied by the caller).
    pub wall_time: u64,
    /// Logical counter advancing causality within equal `wall_time`.
    pub logical: u64,
    /// Originating peer; final tiebreak so equal `(wall, logical)` is still a
    /// total order.
    pub peer: PeerId,
}

impl HlcStamp {
    fn new(wall_time: u64, logical: u64, peer: PeerId) -> Self {
        Self {
            wall_time,
            logical,
            peer,
        }
    }
}

/// Lossless conversion to the feature-free wire mirror (`#lzcrdtplane5b`): the
/// `ipc` layer carries causal-stability metadata as plain integers
/// ([`WireStamp`](crate::ipc::WireStamp)) so it never depends on the
/// `distributed` feature. The total order `(wall_time, logical, peer)` is
/// identical on both sides, so this round-trips.
#[cfg(feature = "ipc")]
impl From<HlcStamp> for crate::ipc::WireStamp {
    fn from(stamp: HlcStamp) -> Self {
        Self {
            wall_time: stamp.wall_time,
            logical: stamp.logical,
            peer: stamp.peer.0,
        }
    }
}

#[cfg(feature = "ipc")]
impl From<crate::ipc::WireStamp> for HlcStamp {
    fn from(wire: crate::ipc::WireStamp) -> Self {
        Self {
            wall_time: wire.wall_time,
            logical: wire.logical,
            peer: PeerId(wire.peer),
        }
    }
}

/// A hybrid logical clock for one peer.
///
/// The caller supplies wall-clock time (`now_micros`) on each event so the
/// clock stays deterministic and testable; it never reads the system clock
/// itself. [`send`](Hlc::send) stamps a local event; [`recv`](Hlc::recv)
/// observes a remote stamp, keeping the clock ahead of anything it has seen.
#[derive(Debug, Clone)]
pub struct Hlc {
    peer: PeerId,
    last_wall: u64,
    last_logical: u64,
}

impl Hlc {
    /// Create a clock for `peer`.
    pub fn new(peer: PeerId) -> Self {
        Self {
            peer,
            last_wall: 0,
            last_logical: 0,
        }
    }

    /// A clock for `peer` that continues from **this** clock's causal position.
    ///
    /// Both halves are load-bearing (`#lzzigforkhlcpeer`). The *position*
    /// carries because a replica forked from this one has already observed
    /// everything this one holds — restart it at zero and its next local
    /// [`send`](Hlc::send) under ordinary clock skew (a `now_micros` below
    /// `last_wall`) mints a stamp causally behind state it already carries;
    /// LWW adopts only on `>`, so that write is silently dropped. The *peer*
    /// does **not** carry, because it is the stamp's final tiebreaker: two
    /// replicas stamping under one id can mint the identical
    /// `(wall_time, logical, peer)`, and a tie means neither side adopts and
    /// they diverge permanently.
    pub fn fork(&self, peer: PeerId) -> Self {
        Self {
            peer,
            last_wall: self.last_wall,
            last_logical: self.last_logical,
        }
    }

    /// Stamp a local event at wall time `now_micros`.
    pub fn send(&mut self, now_micros: u64) -> HlcStamp {
        if now_micros > self.last_wall {
            self.last_wall = now_micros;
            self.last_logical = 0;
        } else {
            self.last_logical += 1;
        }
        HlcStamp::new(self.last_wall, self.last_logical, self.peer)
    }

    /// Observe a remote stamp at wall time `now_micros`, advancing this clock
    /// past it. Returns a fresh local stamp for the receive event.
    pub fn recv(&mut self, remote: HlcStamp, now_micros: u64) -> HlcStamp {
        let wall = self.last_wall.max(remote.wall_time).max(now_micros);
        if wall == self.last_wall && wall == remote.wall_time {
            self.last_logical = self.last_logical.max(remote.logical) + 1;
        } else if wall == self.last_wall {
            self.last_logical += 1;
        } else if wall == remote.wall_time {
            self.last_logical = remote.logical + 1;
        } else {
            self.last_logical = 0;
        }
        self.last_wall = wall;
        HlcStamp::new(self.last_wall, self.last_logical, self.peer)
    }
}

/// A CRDT cell value: a state that merges with another replica's state
/// commutatively, associatively, and idempotently.
///
/// [`merge_from`](CellCrdt::merge_from) folds another replica's state into this
/// one and reports whether the observable [`value`](CellCrdt::value) changed,
/// so a [`ReplicatedCell`] can skip a no-op `set_cell` on a redundant merge.
pub trait CellCrdt {
    /// The observable value projected into the reactive graph.
    type Value;

    /// Merge `other`'s state into `self`. Returns `true` iff the observable
    /// value changed. MUST be commutative, associative, and idempotent.
    fn merge_from(&mut self, other: &Self) -> bool;

    /// The current converged value.
    fn value(&self) -> Self::Value;
}

/// A [`CellCrdt`] that names the cell-level [`MergeMechanism`] it implements,
/// so a [`ReplicatedCell`] can report (and a `merge:` selector can choose) the
/// register kind backing a cell.
///
/// The three register shapes map to two mechanisms: [`LwwRegister`] is the
/// dedicated last-writer-wins mechanism ([`MergeMechanism::Lww`]); the
/// concurrency-retaining shapes — [`MvRegister`] and [`PnCounter`] — are the
/// general coordination-free [`MergeMechanism::Crdt`].
pub trait RegisterCrdt: CellCrdt {
    /// The `merge:` mechanism a cell backed by this register declares.
    const MECHANISM: MergeMechanism;
}

/// Last-writer-wins register: the highest [`HlcStamp`] wins. The default
/// "current value" register most reactive cells want; silently drops the losing
/// side of a concurrent write.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LwwRegister<T> {
    value: T,
    stamp: HlcStamp,
}

impl<T: Clone> LwwRegister<T> {
    /// Create a register holding `value` written at `stamp`.
    pub fn new(value: T, stamp: HlcStamp) -> Self {
        Self { value, stamp }
    }

    /// Apply a local write, overwriting iff `stamp` beats the current stamp.
    pub fn set(&mut self, value: T, stamp: HlcStamp) -> bool {
        if stamp > self.stamp {
            self.value = value;
            self.stamp = stamp;
            true
        } else {
            false
        }
    }

    /// The winning stamp currently held.
    pub fn stamp(&self) -> HlcStamp {
        self.stamp
    }
}

impl<T: Clone + PartialEq> CellCrdt for LwwRegister<T> {
    type Value = T;

    fn merge_from(&mut self, other: &Self) -> bool {
        if other.stamp > self.stamp {
            let changed = self.value != other.value;
            self.value = other.value.clone();
            self.stamp = other.stamp;
            changed
        } else {
            false
        }
    }

    fn value(&self) -> T {
        self.value.clone()
    }
}

impl<T: Clone + PartialEq> RegisterCrdt for LwwRegister<T> {
    const MECHANISM: MergeMechanism = MergeMechanism::Lww;
}

/// A version vector: per-peer event counters used to detect causal dominance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VersionVector(BTreeMap<u64, u64>);

impl VersionVector {
    fn get(&self, peer: PeerId) -> u64 {
        self.0.get(&peer.0).copied().unwrap_or(0)
    }

    /// Bump `peer` to one past the max of `self` and `floor`, so a new write is
    /// causally after everything it observed.
    fn bump(&mut self, peer: PeerId, floor: &VersionVector) {
        let next = self.get(peer).max(floor.get(peer)) + 1;
        self.0.insert(peer.0, next);
        for (&p, &c) in &floor.0 {
            let e = self.0.entry(p).or_insert(0);
            *e = (*e).max(c);
        }
    }

    /// `true` iff every component of `self` is ≥ `other` (i.e. `self`
    /// causally dominates or equals `other`).
    fn dominates(&self, other: &VersionVector) -> bool {
        other
            .0
            .iter()
            .all(|(&p, &c)| self.0.get(&p).copied().unwrap_or(0) >= c)
    }
}

/// A per-peer frontier of the highest [`HlcStamp`] observed from each peer.
///
/// This is deliberately distinct from [`VersionVector`], which is a Lamport
/// *counter* vector consumed internally by [`MvRegister`] and has no public
/// minimum. The distributed cell plane's tombstone GC needs an
/// `HlcStamp`-keyed watermark — deletes are stamped with [`HlcStamp`], not a
/// counter — and the **causal-stability frontier** is the *minimum* observed
/// stamp across every known peer: the causal point every replica has durably
/// observed, below which a tombstone is collectable everywhere. A single
/// replica's local clock is explicitly **not** a sound watermark; only this
/// cross-peer minimum is.
///
/// `observe`/`merge` are commutative, associative, and idempotent (per-peer
/// `max` of a totally-ordered stamp), so two replicas that exchange frontiers
/// in any order converge to the same map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StampFrontier(BTreeMap<PeerId, HlcStamp>);

impl StampFrontier {
    /// An empty frontier — no peer observed yet.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Record that `stamp` was observed from `peer`, keeping the higher of the
    /// stored and new stamp (per-peer `max`). Returns `true` iff the stored
    /// stamp advanced.
    pub fn observe(&mut self, peer: PeerId, stamp: HlcStamp) -> bool {
        match self.0.get(&peer) {
            Some(&cur) if cur >= stamp => false,
            _ => {
                self.0.insert(peer, stamp);
                true
            }
        }
    }

    /// The highest stamp observed from `peer`, if any.
    pub fn get(&self, peer: PeerId) -> Option<HlcStamp> {
        self.0.get(&peer).copied()
    }

    /// Merge another frontier into this one, taking the per-peer `max` stamp.
    /// Commutative, associative, idempotent. Returns `true` iff any entry
    /// advanced.
    pub fn merge(&mut self, other: &StampFrontier) -> bool {
        let mut changed = false;
        for (&peer, &stamp) in &other.0 {
            changed |= self.observe(peer, stamp);
        }
        changed
    }

    /// The causal-stability frontier across `membership`: the minimum observed
    /// stamp over every expected peer.
    ///
    /// Returns `None` until **every** peer in `membership` has been observed at
    /// least once. A member with no observed stamp may still produce an op
    /// causally earlier than anything seen so far, so nothing is yet stable —
    /// the frontier is only meaningful once the full membership is accounted
    /// for. An empty `membership` likewise yields `None`.
    pub fn frontier<I>(&self, membership: I) -> Option<HlcStamp>
    where
        I: IntoIterator<Item = PeerId>,
    {
        let mut min: Option<HlcStamp> = None;
        for peer in membership {
            let stamp = self.get(peer)?;
            min = Some(match min {
                Some(m) => m.min(stamp),
                None => stamp,
            });
        }
        min
    }

    /// `true` iff every entry in `other` is `<=` the corresponding entry in
    /// `self` — i.e. `self` causally dominates or equals `other` on every peer
    /// `other` knows about.
    pub fn dominates(&self, other: &StampFrontier) -> bool {
        other
            .0
            .iter()
            .all(|(peer, stamp)| self.0.get(peer).is_some_and(|cur| cur >= stamp))
    }

    /// Iterate the observed `(peer, stamp)` entries in [`PeerId`] order — the
    /// per-peer frontier a replica advertises over anti-entropy
    /// (`#lzcrdtplane5b`).
    pub fn iter(&self) -> impl Iterator<Item = (PeerId, HlcStamp)> + '_ {
        self.0.iter().map(|(&peer, &stamp)| (peer, stamp))
    }

    /// Number of peers with at least one observed stamp.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` iff no peer has been observed yet.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The [`OpId`]-keyed twin of [`StampFrontier`], the causal-stability watermark
/// for the *text* layer (`#lzcrdtplane4b`).
///
/// [`SeqCrdt`] keys deletes by [`HlcStamp`], so its GC is driven by the
/// [`StampFrontier`]. [`TextCrdt`], though, keys deletes by a Lamport [`OpId`]
/// (`counter + peer`) — a *distinct clock* from the `HlcStamp`. The Lamport
/// counter is causally monotone in exactly the same way (it advances past
/// everything observed on merge), so the same construction applies on the OpId
/// clock: per peer, the highest [`OpId`] observed from that peer; the
/// **causal-stability frontier** is the per-peer *minimum* over membership — the
/// OpId every replica has durably observed, below which a text tombstone is
/// collectable everywhere.
///
/// `observe`/`merge` are commutative, associative, and idempotent (per-peer
/// `max` of a totally-ordered [`OpId`]), so two replicas that exchange frontiers
/// in any order converge to the same map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OpIdFrontier(BTreeMap<u64, OpId>);

impl OpIdFrontier {
    /// An empty frontier — no peer observed yet.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Record that `op` was observed from peer [`OpId::peer`], keeping the higher
    /// of the stored and new id (per-peer `max`). Returns `true` iff the stored
    /// id advanced.
    pub fn observe(&mut self, op: OpId) -> bool {
        match self.0.get(&op.peer()) {
            Some(&cur) if cur >= op => false,
            _ => {
                self.0.insert(op.peer(), op);
                true
            }
        }
    }

    /// The highest id observed from `peer`, if any.
    pub fn get(&self, peer: u64) -> Option<OpId> {
        self.0.get(&peer).copied()
    }

    /// Merge another frontier into this one, taking the per-peer `max` id.
    /// Commutative, associative, idempotent. Returns `true` iff any entry
    /// advanced.
    pub fn merge(&mut self, other: &OpIdFrontier) -> bool {
        let mut changed = false;
        for &op in other.0.values() {
            changed |= self.observe(op);
        }
        changed
    }

    /// The causal-stability frontier across `membership`: the minimum observed
    /// id over every expected peer.
    ///
    /// Returns `None` until **every** peer in `membership` has been observed at
    /// least once — a member with no observed id may still hold a causally
    /// earlier op, so nothing is yet stable. An empty `membership` likewise
    /// yields `None`.
    pub fn frontier<I>(&self, membership: I) -> Option<OpId>
    where
        I: IntoIterator<Item = u64>,
    {
        let mut min: Option<OpId> = None;
        for peer in membership {
            let op = self.get(peer)?;
            min = Some(match min {
                Some(m) => m.min(op),
                None => op,
            });
        }
        min
    }

    /// `true` iff every entry in `other` is `<=` the corresponding entry in
    /// `self` — i.e. `self` causally dominates or equals `other` on every peer
    /// `other` knows about.
    pub fn dominates(&self, other: &OpIdFrontier) -> bool {
        other
            .0
            .iter()
            .all(|(peer, op)| self.0.get(peer).is_some_and(|cur| cur >= op))
    }

    /// Number of peers with at least one observed id.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` iff no peer has been observed yet.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Multi-value register: surfaces concurrent writes as a set rather than
/// dropping a loser. Each write is tagged with a [`VersionVector`]; a merge
/// keeps only values whose vector is not dominated by another.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MvRegister<T> {
    entries: Vec<(VersionVector, T)>,
}

impl<T: Clone + PartialEq> MvRegister<T> {
    /// An empty register (no value written yet).
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Apply a local write by `peer`. The new value causally supersedes every
    /// value currently visible to this replica, collapsing them to one.
    pub fn set(&mut self, value: T, peer: PeerId) -> bool {
        let mut vv = VersionVector::default();
        for (e, _) in &self.entries {
            for (&p, &c) in &e.0 {
                let slot = vv.0.entry(p).or_insert(0);
                *slot = (*slot).max(c);
            }
        }
        let mut next = VersionVector::default();
        next.bump(peer, &vv);
        let changed = !(self.entries.len() == 1 && self.entries[0].1 == value);
        self.entries = vec![(next, value)];
        changed
    }

    /// The current set of concurrent values, in a deterministic order.
    pub fn values(&self) -> Vec<T> {
        self.entries.iter().map(|(_, v)| v.clone()).collect()
    }

    fn normalize(&mut self) {
        // Drop any entry strictly dominated by another, then dedup.
        let mut kept: Vec<(VersionVector, T)> = Vec::new();
        for (vv, v) in self.entries.drain(..) {
            if kept.iter().any(|(k, _)| k.dominates(&vv) && k != &vv) {
                continue;
            }
            kept.retain(|(k, _)| !(vv.dominates(k) && k != &vv));
            if !kept.iter().any(|(k, kv)| k == &vv && kv == &v) {
                kept.push((vv, v));
            }
        }
        self.entries = kept;
    }
}

impl<T: Clone + PartialEq> Default for MvRegister<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + PartialEq> CellCrdt for MvRegister<T> {
    type Value = Vec<T>;

    fn merge_from(&mut self, other: &Self) -> bool {
        let before = self.values();
        self.entries.extend(other.entries.iter().cloned());
        self.normalize();
        self.values() != before
    }

    fn value(&self) -> Vec<T> {
        self.values()
    }
}

impl<T: Clone + PartialEq> RegisterCrdt for MvRegister<T> {
    const MECHANISM: MergeMechanism = MergeMechanism::Crdt;
}

/// Positive-negative counter: per-peer increment and decrement tallies merged
/// by per-peer maximum. Value is `sum(incr) - sum(decr)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PnCounter {
    incr: BTreeMap<u64, u64>,
    decr: BTreeMap<u64, u64>,
}

impl PnCounter {
    /// An empty counter (value 0).
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `amount` to `peer`'s increment tally.
    pub fn increment(&mut self, peer: PeerId, amount: u64) {
        *self.incr.entry(peer.0).or_insert(0) += amount;
    }

    /// Add `amount` to `peer`'s decrement tally.
    pub fn decrement(&mut self, peer: PeerId, amount: u64) {
        *self.decr.entry(peer.0).or_insert(0) += amount;
    }
}

fn merge_max(into: &mut BTreeMap<u64, u64>, from: &BTreeMap<u64, u64>) {
    for (&p, &c) in from {
        let e = into.entry(p).or_insert(0);
        *e = (*e).max(c);
    }
}

impl CellCrdt for PnCounter {
    type Value = i64;

    fn merge_from(&mut self, other: &Self) -> bool {
        let before = self.value();
        merge_max(&mut self.incr, &other.incr);
        merge_max(&mut self.decr, &other.decr);
        self.value() != before
    }

    fn value(&self) -> i64 {
        let p: u64 = self.incr.values().sum();
        let n: u64 = self.decr.values().sum();
        p as i64 - n as i64
    }
}

impl RegisterCrdt for PnCounter {
    const MECHANISM: MergeMechanism = MergeMechanism::Crdt;
}

/// A multi-write **root** cell backed by a CRDT (`merge: crdt`).
///
/// It owns a CRDT replica and a reactive [`Source`]. A local write or a
/// remote-op ingress merge updates the CRDT, then pushes the converged value
/// into the reactive graph via [`Context::set_cell`](crate::Context::set_cell)
/// — so downstream derived slots recompute through the ordinary direct
/// mechanism, and an equal merge invalidates nothing.
pub struct ReplicatedCell<C: CellCrdt> {
    crdt: C,
    handle: Source<C::Value>,
}

impl<C> ReplicatedCell<C>
where
    C: CellCrdt,
    C::Value: PartialEq + Clone + 'static,
{
    /// Bind a CRDT replica to a fresh reactive root cell in `ctx`, seeded with
    /// the replica's current value.
    pub fn bind(ctx: &Context, crdt: C) -> Self {
        let handle = ctx.source(crdt.value());
        Self { crdt, handle }
    }

    /// The reactive cell handle derived slots depend on.
    pub fn handle(&self) -> Source<C::Value> {
        self.handle
    }

    /// The current converged value.
    pub fn value(&self) -> C::Value {
        self.crdt.value()
    }

    /// Immutable access to the underlying CRDT replica.
    pub fn crdt(&self) -> &C {
        &self.crdt
    }

    /// Merge a remote replica's state (the ingress operation), pushing the
    /// converged value into the reactive graph. Returns `true` iff the value
    /// changed (a redundant merge is a no-op and invalidates nothing).
    pub fn merge_remote(&mut self, ctx: &Context, remote: &C) -> bool {
        if self.crdt.merge_from(remote) {
            ctx.set(&self.handle, self.crdt.value());
            true
        } else {
            false
        }
    }

    /// Apply a local mutation to the CRDT replica through `mutate`, then push
    /// the resulting value into the reactive graph. Returns `true` iff the
    /// value changed.
    pub fn update<F>(&mut self, ctx: &Context, mutate: F) -> bool
    where
        F: FnOnce(&mut C),
    {
        let before = self.crdt.value();
        mutate(&mut self.crdt);
        let after = self.crdt.value();
        if after != before {
            ctx.set(&self.handle, after);
            true
        } else {
            false
        }
    }
}

impl<C> ReplicatedCell<C>
where
    C: RegisterCrdt,
    C::Value: PartialEq + Clone + 'static,
{
    /// The `merge:` mechanism this cell declares, derived from its backing
    /// register: [`MergeMechanism::Lww`] for an [`LwwRegister`],
    /// [`MergeMechanism::Crdt`] for [`MvRegister`]/[`PnCounter`].
    pub const MERGE: MergeMechanism = C::MECHANISM;

    /// The `merge:` mechanism this cell declares (the value form of
    /// [`MERGE`](Self::MERGE)).
    pub fn mechanism(&self) -> MergeMechanism {
        C::MECHANISM
    }
}

impl<T> ReplicatedCell<LwwRegister<T>>
where
    T: Clone + PartialEq + 'static,
{
    /// Bind a last-writer-wins cell (`merge: lww`) seeded with `value` written
    /// at `stamp`.
    pub fn lww(ctx: &Context, value: T, stamp: HlcStamp) -> Self {
        Self::bind(ctx, LwwRegister::new(value, stamp))
    }
}

impl<T> ReplicatedCell<MvRegister<T>>
where
    T: Clone + PartialEq + 'static,
{
    /// Bind a multi-value cell (`merge: crdt`) that retains concurrent writes
    /// as a set until a causally-later write supersedes them. Starts empty.
    pub fn multi_value(ctx: &Context) -> Self {
        Self::bind(ctx, MvRegister::new())
    }
}

impl ReplicatedCell<PnCounter> {
    /// Bind a PN-counter cell (`merge: crdt`) — per-peer increment/decrement
    /// tallies merged by per-peer max. Starts at zero.
    pub fn counter(ctx: &Context) -> Self {
        Self::bind(ctx, PnCounter::new())
    }
}

/// The multi-writer coordination skeleton for one shared session (Phase 1 of
/// the distributed CRDT cell plane, `#lzcrdtplane`).
///
/// Today the plane owns only the pieces needed to compute a sound GC watermark:
/// the local replica identity, its hybrid logical clock ([`Hlc`]), the expected
/// peer membership, and the [`StampFrontier`]. It stamps local events
/// ([`tick`](Self::tick)), folds observed remote stamps into both the clock and
/// the frontier ([`observe_remote`](Self::observe_remote)), and exposes the
/// causal-stability frontier ([`stability_frontier`](Self::stability_frontier))
/// the tombstone GC will consume.
///
/// Anti-entropy exchange, the causal op log, and frontier-driven
/// `SeqCrdt`/`TextCrdt` GC are added in later phases (`#lzcrdtplane3`/`4`); there
/// is no transport here. With `< 2` live writers the plane is inert and the IPC
/// Snapshot/Delta mirror is unaffected.
#[derive(Debug, Clone)]
pub struct CrdtPlane {
    peer: PeerId,
    clock: Hlc,
    membership: BTreeSet<PeerId>,
    frontier: StampFrontier,
    op_frontier: OpIdFrontier,
}

impl CrdtPlane {
    /// Create a plane for the local `peer`, which is a member of its own
    /// session from the start.
    pub fn new(peer: PeerId) -> Self {
        let mut membership = BTreeSet::new();
        membership.insert(peer);
        Self {
            peer,
            clock: Hlc::new(peer),
            membership,
            frontier: StampFrontier::new(),
            op_frontier: OpIdFrontier::new(),
        }
    }

    /// The local replica identity.
    pub fn peer(&self) -> PeerId {
        self.peer
    }

    /// Declare `peer` an expected member of the session.
    ///
    /// [`stability_frontier`](Self::stability_frontier) stays `None` until every
    /// member — including any added here — has been observed, so adding a peer
    /// that has not yet produced an op correctly withholds the frontier.
    pub fn add_peer(&mut self, peer: PeerId) {
        self.membership.insert(peer);
    }

    /// The expected peer membership (including the local peer), in `PeerId`
    /// order.
    pub fn membership(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.membership.iter().copied()
    }

    /// Stamp a local event at wall time `now_micros`: advance the clock via
    /// [`Hlc::send`] and record the resulting stamp in the frontier under the
    /// local peer. Returns the fresh local stamp.
    pub fn tick(&mut self, now_micros: u64) -> HlcStamp {
        let stamp = self.clock.send(now_micros);
        self.frontier.observe(self.peer, stamp);
        stamp
    }

    /// Observe a `remote` stamp at wall time `now_micros`: add its originating
    /// peer to membership, record it in the frontier under that peer, and feed
    /// it to [`Hlc::recv`] so the local clock dominates the observed causal
    /// past. Returns the local receive stamp.
    pub fn observe_remote(&mut self, remote: HlcStamp, now_micros: u64) -> HlcStamp {
        self.membership.insert(remote.peer);
        self.frontier.observe(remote.peer, remote);
        self.clock.recv(remote, now_micros)
    }

    /// The causal-stability frontier: the minimum stamp observed across every
    /// expected member, or `None` until all members are seen.
    ///
    /// A tombstone whose stamp is `<=` this value is collectable on every
    /// replica; this is the watermark `#lzcrdtplane4` will drive periodic
    /// `SeqCrdt::gc` / `TextCrdt::gc_with` from.
    pub fn stability_frontier(&self) -> Option<HlcStamp> {
        self.frontier.frontier(self.membership.iter().copied())
    }

    /// Immutable access to the per-peer stamp frontier.
    pub fn frontier(&self) -> &StampFrontier {
        &self.frontier
    }

    /// `true` iff `stamp` is at or below the causal-stability frontier — every
    /// expected member has observed it, so a tombstone stamped `stamp` is
    /// collectable on every replica. `false` until the full membership has been
    /// observed (no member is allowed to still hold an un-seen earlier op).
    pub fn is_collectable(&self, stamp: HlcStamp) -> bool {
        self.stability_frontier()
            .is_some_and(|frontier| stamp <= frontier)
    }

    /// Drive frontier-based tombstone GC on a [`SeqCrdt`] (Phase 4 of
    /// `#lzcrdtplane`, closing `#r86a`/`#lztombgcwire` for the sequence layer):
    /// collect every tombstone whose delete stamp is `<=` the causal-stability
    /// frontier. Returns the number of entries collected, or `0` (a no-op) until
    /// the full membership has been observed — so a delete is never collected
    /// before every replica has provably seen it.
    ///
    /// [`SeqCrdt`] keys deletes by [`HlcStamp`], so the plane's frontier drives
    /// its GC directly. [`TextCrdt`] keys deletes by a Lamport [`OpId`]
    /// (a distinct clock), so it is driven by the parallel [`OpIdFrontier`] via
    /// [`gc_text`](Self::gc_text) instead.
    pub fn gc_seq<Id, V>(&self, seq: &mut SeqCrdt<Id, V>) -> usize
    where
        Id: Eq + Hash + Clone,
        V: Clone + PartialEq,
    {
        match self.stability_frontier() {
            Some(frontier) => seq.gc(frontier),
            None => 0,
        }
    }

    /// Fold a replica's [`OpId`] position into the text-layer
    /// [`OpIdFrontier`](Self::op_frontier) (`#lzcrdtplane4b`).
    ///
    /// `op` is a peer's current Lamport position — locally,
    /// [`TextCrdt::clock`](crate::TextCrdt::clock); remotely, the same value
    /// learned over the exchange — attributed to its originating peer
    /// ([`OpId::peer`]). The originating peer is added to membership, exactly as
    /// [`observe_remote`](Self::observe_remote) does for the `HlcStamp` clock, so
    /// observing a peer's text clock also expands the expected membership.
    ///
    /// This is the `OpId`-clock analog of [`tick`](Self::tick) /
    /// [`observe_remote`](Self::observe_remote): a single entry point for both
    /// the local replica's progress and an observed remote one.
    pub fn observe_op(&mut self, op: OpId) {
        self.membership.insert(PeerId(op.peer()));
        self.op_frontier.observe(op);
    }

    /// Immutable access to the per-peer [`OpId`] frontier (the text-layer twin of
    /// [`frontier`](Self::frontier)).
    pub fn op_frontier(&self) -> &OpIdFrontier {
        &self.op_frontier
    }

    /// The text-layer causal-stability frontier: the minimum [`OpId`] observed
    /// across every expected member, or `None` until all members are seen.
    ///
    /// A text tombstone whose delete [`OpId`] is `<=` this value is collectable
    /// on every replica; this is the watermark [`gc_text`](Self::gc_text) drives
    /// [`TextCrdt::gc_with`](crate::TextCrdt::gc_with) from, mirroring
    /// [`stability_frontier`](Self::stability_frontier) on the `HlcStamp` clock.
    pub fn op_stability_frontier(&self) -> Option<OpId> {
        self.op_frontier
            .frontier(self.membership.iter().map(|p| p.0))
    }

    /// `true` iff `op` is at or below the text-layer causal-stability frontier —
    /// every expected member has observed it, so a tombstone whose delete id is
    /// `op` is collectable on every replica. `false` until the full membership
    /// has been observed. The [`OpId`] analog of
    /// [`is_collectable`](Self::is_collectable).
    pub fn is_op_collectable(&self, op: OpId) -> bool {
        self.op_stability_frontier()
            .is_some_and(|frontier| op <= frontier)
    }

    /// Drive frontier-based tombstone GC on a [`TextCrdt`] (Phase 4b of
    /// `#lzcrdtplane`, completing the GC layer started in Phase 4 for the text
    /// layer): collect every tombstone whose delete [`OpId`] is `<=` the text
    /// causal-stability frontier. Returns the number of elements collected, or
    /// `0` (a no-op) until the full membership has been observed — so a delete is
    /// never collected before every replica has provably seen it.
    ///
    /// The [`OpId`]-keyed counterpart to [`gc_seq`](Self::gc_seq): `TextCrdt`
    /// keys deletes by a Lamport [`OpId`] rather than [`HlcStamp`], so its GC is
    /// driven by the [`OpIdFrontier`] instead of the [`StampFrontier`].
    pub fn gc_text(&self, text: &mut TextCrdt) -> usize {
        match self.op_stability_frontier() {
            Some(frontier) => text.gc_with(|op| op <= frontier),
            None => 0,
        }
    }
}

/// A causal op log keyed by [`HlcStamp`] (which embeds the originating peer),
/// the anti-entropy substrate of the distributed plane (Phase 3 of
/// `#lzcrdtplane`).
///
/// Each op a replica originates or applies is stored under its stamp; because
/// [`HlcStamp`] is a total order embedding the peer, every op has a unique key.
/// Two replicas reconcile pairwise:
///
/// 1. The requester advertises its [`StampFrontier`] (the highest stamp it has
///    seen per peer).
/// 2. The responder calls [`missing_since`](Self::missing_since) with that
///    frontier and ships every op the requester has not yet observed.
/// 3. The requester calls [`apply_remote`](Self::apply_remote), which folds
///    each op into its application CRDT exactly once (idempotent re-apply) in
///    causal stamp order.
///
/// The op type `Op` is application-defined (a register write, a `SeqCrdt`
/// insert/remove, a `TextCrdt` edit); the log is agnostic to it and only
/// orders and deduplicates by stamp. Delivery may be out-of-order, duplicated,
/// or batched — convergence holds because every application CRDT this drives is
/// itself commutative/associative/idempotent.
#[derive(Debug, Clone)]
pub struct OpLog<Op> {
    ops: BTreeMap<HlcStamp, Op>,
    frontier: StampFrontier,
}

impl<Op> Default for OpLog<Op> {
    fn default() -> Self {
        Self {
            ops: BTreeMap::new(),
            frontier: StampFrontier::new(),
        }
    }
}

impl<Op: Clone> OpLog<Op> {
    /// An empty op log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a locally originated op stamped `stamp`. Returns `true` iff the
    /// op was new (a duplicate stamp is ignored, keeping the log idempotent).
    pub fn record(&mut self, stamp: HlcStamp, op: Op) -> bool {
        if self.ops.contains_key(&stamp) {
            return false;
        }
        self.frontier.observe(stamp.peer, stamp);
        self.ops.insert(stamp, op);
        true
    }

    /// The per-peer frontier of stamps this log has stored — what a replica
    /// advertises to a peer to request the ops it is missing.
    pub fn frontier(&self) -> &StampFrontier {
        &self.frontier
    }

    /// Every op whose stamp the requester (described by `since`) has not yet
    /// observed: an op from peer `p` stamped `s` is missing iff `since` has no
    /// entry for `p`, or its entry is `< s`. Returned in causal stamp order.
    pub fn missing_since(&self, since: &StampFrontier) -> Vec<(HlcStamp, Op)> {
        self.ops
            .iter()
            .filter(|(stamp, _)| match since.get(stamp.peer) {
                Some(seen) => **stamp > seen,
                None => true,
            })
            .map(|(stamp, op)| (*stamp, op.clone()))
            .collect()
    }

    /// Apply remote ops, folding each not-yet-seen op into the application CRDT
    /// via `apply` exactly once, in causal stamp order. Already-stored stamps
    /// (including duplicates within `ops`) are skipped, so re-delivery is a
    /// no-op. Returns the number of ops newly applied.
    pub fn apply_remote<I, F>(&mut self, ops: I, mut apply: F) -> usize
    where
        I: IntoIterator<Item = (HlcStamp, Op)>,
        F: FnMut(&HlcStamp, &Op),
    {
        let mut incoming: Vec<(HlcStamp, Op)> = ops.into_iter().collect();
        incoming.sort_by_key(|(stamp, _)| *stamp);
        let mut applied = 0;
        for (stamp, op) in incoming {
            if self.ops.contains_key(&stamp) {
                continue;
            }
            apply(&stamp, &op);
            self.frontier.observe(stamp.peer, stamp);
            self.ops.insert(stamp, op);
            applied += 1;
        }
        applied
    }

    /// Number of ops currently stored.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// `true` iff the log holds no ops.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn peer(n: u64) -> PeerId {
        PeerId(n)
    }

    #[test]
    fn merge_mechanism_crdt_and_lww_are_implemented() {
        for m in [MergeMechanism::Crdt, MergeMechanism::Lww] {
            assert!(m.is_implemented());
            assert_eq!(m.resolve(), Ok(m));
        }
        // The remaining mechanisms stay reserved and fail closed.
        for m in [
            MergeMechanism::Ot,
            MergeMechanism::Lease,
            MergeMechanism::Custom,
        ] {
            assert!(!m.is_implemented());
            assert_eq!(m.resolve(), Err(UnsupportedMechanism(m)));
        }
    }

    #[test]
    fn hlc_send_is_monotonic_and_recv_observes_remote() {
        let mut a = Hlc::new(peer(1));
        let s1 = a.send(100);
        let s2 = a.send(100); // same wall time -> logical advances
        assert!(s2 > s1);
        let s3 = a.send(50); // earlier wall time -> still advances logically
        assert!(s3 > s2);

        let mut b = Hlc::new(peer(2));
        let remote = a.send(200);
        let got = b.recv(remote, 10);
        assert!(
            got > remote,
            "recv must move past the observed remote stamp"
        );
    }

    #[test]
    fn lww_register_keeps_highest_stamp_and_merge_is_commutative_idempotent() {
        let s_lo = HlcStamp::new(10, 0, peer(1));
        let s_hi = HlcStamp::new(20, 0, peer(2));
        let lo = LwwRegister::new("lo", s_lo);
        let hi = LwwRegister::new("hi", s_hi);

        let mut a = lo.clone();
        a.merge_from(&hi);
        let mut b = hi.clone();
        b.merge_from(&lo);
        assert_eq!(a.value(), "hi");
        assert_eq!(b.value(), "hi", "merge is commutative");

        // Idempotent: re-merging changes nothing.
        assert!(!a.merge_from(&hi));
        assert_eq!(a.value(), "hi");
    }

    #[test]
    fn mv_register_surfaces_concurrent_writes_and_collapses_on_causal_write() {
        // Two replicas write concurrently (neither saw the other).
        let mut r1: MvRegister<&str> = MvRegister::new();
        r1.set("from-1", peer(1));
        let mut r2: MvRegister<&str> = MvRegister::new();
        r2.set("from-2", peer(2));

        let mut merged = r1.clone();
        merged.merge_from(&r2);
        let mut vals = merged.values();
        vals.sort();
        assert_eq!(
            vals,
            vec!["from-1", "from-2"],
            "concurrent writes both survive"
        );

        // Commutative + idempotent.
        let mut other = r2.clone();
        other.merge_from(&r1);
        let mut ov = other.values();
        ov.sort();
        assert_eq!(ov, vals);
        assert!(!merged.merge_from(&r2));

        // A causal write (observing both) collapses to one value.
        merged.set("resolved", peer(1));
        assert_eq!(merged.values(), vec!["resolved"]);
    }

    #[test]
    fn pn_counter_merges_by_per_peer_max() {
        let mut a = PnCounter::new();
        a.increment(peer(1), 5);
        a.decrement(peer(1), 2);
        let mut b = PnCounter::new();
        b.increment(peer(2), 3);
        b.increment(peer(1), 5); // same peer-1 increment seen on both replicas

        let mut m1 = a.clone();
        m1.merge_from(&b);
        let mut m2 = b.clone();
        m2.merge_from(&a);
        // peer-1 incr = max(5,5)=5, peer-2 incr=3, peer-1 decr=2 -> 5+3-2 = 6
        assert_eq!(m1.value(), 6);
        assert_eq!(m2.value(), 6, "commutative");
        assert!(!m1.merge_from(&b), "idempotent");
    }

    #[test]
    fn replicated_cell_ingress_merge_recomputes_derived_and_suppresses_equal() {
        use std::cell::Cell as StdCell;
        use std::rc::Rc;

        let ctx = Context::new();
        let mut replica =
            ReplicatedCell::bind(&ctx, LwwRegister::new(1i32, HlcStamp::new(1, 0, peer(1))));
        assert_eq!(
            ReplicatedCell::<LwwRegister<i32>>::MERGE,
            MergeMechanism::Lww,
            "an LwwRegister-backed cell declares the lww mechanism"
        );

        let handle = replica.handle();
        let recomputes = Rc::new(StdCell::new(0usize));
        let rc = recomputes.clone();
        let doubled = ctx.computed(move |ctx| {
            rc.set(rc.get() + 1);
            ctx.get(&handle) * 2
        });
        assert_eq!(ctx.get(&doubled), 2);
        assert_eq!(recomputes.get(), 1);

        // Remote write with a higher stamp wins -> derived recomputes.
        let remote = LwwRegister::new(10i32, HlcStamp::new(5, 0, peer(2)));
        assert!(replica.merge_remote(&ctx, &remote));
        assert_eq!(ctx.get(&doubled), 20);
        assert_eq!(recomputes.get(), 2);

        // A stale remote write (lower stamp) loses -> no value change, no recompute.
        let stale = LwwRegister::new(99i32, HlcStamp::new(2, 0, peer(3)));
        assert!(!replica.merge_remote(&ctx, &stale));
        assert_eq!(ctx.get(&doubled), 20);
        assert_eq!(recomputes.get(), 2, "losing merge invalidates nothing");

        // A re-delivered winning write is idempotent: equal value, no recompute.
        assert!(!replica.merge_remote(&ctx, &remote));
        assert_eq!(recomputes.get(), 2);
    }

    #[test]
    fn replicated_cells_converge_regardless_of_merge_order() {
        // Two replicas of the same logical cell, each takes a local write,
        // then exchange state in opposite orders. They must converge.
        let ctx_a = Context::new();
        let ctx_b = Context::new();
        let mut a =
            ReplicatedCell::bind(&ctx_a, LwwRegister::new(0i32, HlcStamp::new(0, 0, peer(1))));
        let mut b =
            ReplicatedCell::bind(&ctx_b, LwwRegister::new(0i32, HlcStamp::new(0, 0, peer(2))));

        a.update(&ctx_a, |c| {
            c.set(7, HlcStamp::new(10, 0, peer(1)));
        });
        b.update(&ctx_b, |c| {
            c.set(9, HlcStamp::new(20, 0, peer(2)));
        });

        let a_state = a.crdt().clone();
        let b_state = b.crdt().clone();
        a.merge_remote(&ctx_a, &b_state);
        b.merge_remote(&ctx_b, &a_state);

        assert_eq!(a.value(), b.value());
        assert_eq!(a.value(), 9, "highest HLC stamp wins on both replicas");
        assert_eq!(ctx_a.get(&a.handle()), ctx_b.get(&b.handle()));
    }

    // --- #lzcrdtplane1: StampFrontier ---

    #[test]
    fn stamp_frontier_keeps_per_peer_max() {
        let mut f = StampFrontier::new();
        assert!(f.is_empty());

        assert!(f.observe(peer(1), HlcStamp::new(10, 0, peer(1))));
        // A strictly higher stamp from the same peer advances.
        assert!(f.observe(peer(1), HlcStamp::new(20, 0, peer(1))));
        // An older stamp is ignored (idempotent / out-of-order safe).
        assert!(!f.observe(peer(1), HlcStamp::new(15, 0, peer(1))));
        // Re-observing the current stamp is a no-op.
        assert!(!f.observe(peer(1), HlcStamp::new(20, 0, peer(1))));

        assert_eq!(f.get(peer(1)), Some(HlcStamp::new(20, 0, peer(1))));
        assert_eq!(f.get(peer(2)), None);
        assert_eq!(f.len(), 1);
    }

    #[test]
    fn stamp_frontier_merge_is_commutative_and_idempotent() {
        let a_stamp = HlcStamp::new(30, 0, peer(1));
        let b_stamp = HlcStamp::new(40, 1, peer(2));

        let mut left = StampFrontier::new();
        left.observe(peer(1), a_stamp);
        let mut right = StampFrontier::new();
        right.observe(peer(2), b_stamp);

        let mut lr = left.clone();
        lr.merge(&right);
        let mut rl = right.clone();
        rl.merge(&left);
        assert_eq!(lr, rl, "merge is commutative");

        // Idempotent: merging again changes nothing.
        assert!(!lr.merge(&right));
        assert!(!lr.merge(&left));

        assert_eq!(lr.get(peer(1)), Some(a_stamp));
        assert_eq!(lr.get(peer(2)), Some(b_stamp));
    }

    #[test]
    fn stamp_frontier_is_min_over_membership_and_none_until_all_seen() {
        let mut f = StampFrontier::new();
        let s1 = HlcStamp::new(50, 0, peer(1));
        let s2 = HlcStamp::new(40, 0, peer(2));
        let members = [peer(1), peer(2)];

        // Empty membership has no stable point.
        assert_eq!(f.frontier(std::iter::empty()), None);

        f.observe(peer(1), s1);
        // Peer 2 unseen -> no frontier yet (it could still produce an earlier op).
        assert_eq!(f.frontier(members), None);

        f.observe(peer(2), s2);
        // Now every member is seen: the frontier is the minimum stamp.
        assert_eq!(f.frontier(members), Some(s2));
    }

    #[test]
    fn stamp_frontier_dominates() {
        let mut bigger = StampFrontier::new();
        bigger.observe(peer(1), HlcStamp::new(20, 0, peer(1)));
        bigger.observe(peer(2), HlcStamp::new(30, 0, peer(2)));

        let mut smaller = StampFrontier::new();
        smaller.observe(peer(1), HlcStamp::new(10, 0, peer(1)));

        assert!(bigger.dominates(&smaller));
        assert!(!smaller.dominates(&bigger));
        assert!(bigger.dominates(&bigger), "dominance is reflexive");
    }

    // --- #lzcrdtplane1: CrdtPlane skeleton ---

    #[test]
    fn crdt_plane_tick_advances_self_frontier() {
        let mut plane = CrdtPlane::new(peer(1));
        // Single-writer session: as soon as the local peer ticks, the frontier
        // is its own stamp (membership is just itself).
        assert_eq!(plane.stability_frontier(), None);

        let s1 = plane.tick(100);
        let s2 = plane.tick(200);
        assert!(s2 > s1, "ticks produce monotonically increasing stamps");
        assert_eq!(plane.stability_frontier(), Some(s2));
        assert_eq!(plane.frontier().get(peer(1)), Some(s2));
    }

    #[test]
    fn crdt_plane_frontier_withheld_until_every_member_seen() {
        let mut plane = CrdtPlane::new(peer(1));
        plane.add_peer(peer(2));

        plane.tick(100);
        // Peer 2 is an expected member but unseen -> no stable frontier.
        assert_eq!(plane.stability_frontier(), None);

        // Observe a remote op from peer 2 older than our local stamp.
        plane.observe_remote(HlcStamp::new(50, 0, peer(2)), 110);
        assert_eq!(
            plane.stability_frontier(),
            Some(HlcStamp::new(50, 0, peer(2))),
            "frontier is the minimum across both members"
        );
        assert_eq!(
            plane.membership().collect::<Vec<_>>(),
            vec![peer(1), peer(2)]
        );
    }

    #[test]
    fn crdt_plane_observe_remote_advances_local_clock() {
        let mut plane = CrdtPlane::new(peer(1));
        // Observe a remote stamp far in the future, then take a local tick at an
        // earlier wall time: the HLC must keep the local stamp causally after
        // the observed remote one.
        plane.observe_remote(HlcStamp::new(1_000, 5, peer(2)), 100);
        let local = plane.tick(200);
        assert!(
            local > HlcStamp::new(1_000, 5, peer(2)),
            "local clock dominates the observed remote causal past"
        );
    }

    // --- #lzcrdtplane2: MergeMechanism <-> register wiring + constructors ---

    #[test]
    fn registers_declare_their_merge_mechanism() {
        assert_eq!(
            <LwwRegister<i32> as RegisterCrdt>::MECHANISM,
            MergeMechanism::Lww
        );
        assert_eq!(
            <MvRegister<i32> as RegisterCrdt>::MECHANISM,
            MergeMechanism::Crdt
        );
        assert_eq!(<PnCounter as RegisterCrdt>::MECHANISM, MergeMechanism::Crdt);
    }

    #[test]
    fn replicated_cell_constructors_pick_the_right_mechanism() {
        let ctx = Context::new();

        let lww = ReplicatedCell::lww(&ctx, 7i32, HlcStamp::new(1, 0, peer(1)));
        assert_eq!(lww.mechanism(), MergeMechanism::Lww);
        assert_eq!(lww.value(), 7);

        let mv: ReplicatedCell<MvRegister<i32>> = ReplicatedCell::multi_value(&ctx);
        assert_eq!(mv.mechanism(), MergeMechanism::Crdt);
        assert!(mv.value().is_empty());

        let pn = ReplicatedCell::counter(&ctx);
        assert_eq!(pn.mechanism(), MergeMechanism::Crdt);
        assert_eq!(pn.value(), 0);
    }

    #[test]
    fn lww_constructor_drives_the_reactive_graph() {
        let ctx = Context::new();
        let mut cell = ReplicatedCell::lww(&ctx, 1i32, HlcStamp::new(1, 0, peer(1)));
        let doubled = {
            let h = cell.handle();
            ctx.computed(move |ctx| ctx.get(&h) * 2)
        };
        assert_eq!(ctx.get(&doubled), 2);
        // A higher-stamped remote write converges and recomputes downstream.
        cell.merge_remote(&ctx, &LwwRegister::new(10i32, HlcStamp::new(5, 0, peer(2))));
        assert_eq!(ctx.get(&doubled), 20);
    }

    #[test]
    fn pn_counter_constructor_increments_through_update() {
        let ctx = Context::new();
        let mut cell = ReplicatedCell::counter(&ctx);
        cell.update(&ctx, |c| c.increment(peer(1), 3));
        cell.update(&ctx, |c| c.decrement(peer(1), 1));
        assert_eq!(cell.value(), 2);
        assert_eq!(ctx.get(&cell.handle()), 2);
    }

    // --- #lzcrdtplane2: register merge-law property tests (proptest) ---

    /// A deterministic LWW register from a `(wall, logical, peer)` tuple. The
    /// value is a function of the stamp because a stamp uniquely identifies a
    /// write — equal stamps necessarily carry equal values, so the
    /// last-writer-wins merge stays a well-defined semilattice.
    fn lww_from(parts: (u64, u64, u64)) -> LwwRegister<i32> {
        let (wall, logical, p) = parts;
        let v = (wall * 100 + logical * 10 + p) as i32;
        LwwRegister::new(v, HlcStamp::new(wall, logical, peer(p)))
    }

    fn merged_lww(a: &LwwRegister<i32>, b: &LwwRegister<i32>) -> LwwRegister<i32> {
        let mut out = a.clone();
        out.merge_from(b);
        out
    }

    /// A PN counter from per-peer (incr, decr) tallies for peers 1..=3.
    fn pn_from(tallies: [(u64, u64); 3]) -> PnCounter {
        let mut c = PnCounter::new();
        for (i, (inc, dec)) in tallies.iter().enumerate() {
            let p = peer(i as u64 + 1);
            c.increment(p, *inc);
            c.decrement(p, *dec);
        }
        c
    }

    fn merged_pn(a: &PnCounter, b: &PnCounter) -> PnCounter {
        let mut out = a.clone();
        out.merge_from(b);
        out
    }

    proptest! {
        // LWW merge is commutative, associative, and idempotent: the surviving
        // value/stamp is the maximum stamp regardless of merge order/grouping.
        #[test]
        fn lww_merge_is_a_semilattice(
            x in (0u64..4, 0u64..4, 1u64..4),
            y in (0u64..4, 0u64..4, 1u64..4),
            z in (0u64..4, 0u64..4, 1u64..4),
        ) {
            let (a, b, c) = (lww_from(x), lww_from(y), lww_from(z));

            // Commutative.
            prop_assert_eq!(merged_lww(&a, &b).value(), merged_lww(&b, &a).value());

            // Associative.
            let left = merged_lww(&merged_lww(&a, &b), &c);
            let right = merged_lww(&a, &merged_lww(&b, &c));
            prop_assert_eq!(left.value(), right.value());

            // Idempotent.
            let once = merged_lww(&a, &b);
            let twice = merged_lww(&once, &b);
            prop_assert_eq!(once.value(), twice.value());
            prop_assert!(!once.clone().merge_from(&b), "re-merge is a no-op");
        }

        // PN counter merge is commutative, associative, and idempotent.
        #[test]
        fn pn_counter_merge_is_a_semilattice(
            x in prop::array::uniform3((0u64..50, 0u64..50)),
            y in prop::array::uniform3((0u64..50, 0u64..50)),
            z in prop::array::uniform3((0u64..50, 0u64..50)),
        ) {
            let (a, b, c) = (pn_from(x), pn_from(y), pn_from(z));

            prop_assert_eq!(merged_pn(&a, &b).value(), merged_pn(&b, &a).value());

            let left = merged_pn(&merged_pn(&a, &b), &c);
            let right = merged_pn(&a, &merged_pn(&b, &c));
            prop_assert_eq!(left.value(), right.value());

            let once = merged_pn(&a, &b);
            prop_assert!(!once.clone().merge_from(&b), "re-merge is a no-op");
        }

        // StampFrontier merge is commutative, associative, and idempotent
        // (per-peer max of a totally-ordered stamp).
        #[test]
        fn stamp_frontier_merge_is_a_semilattice(
            xs in prop::collection::vec((1u64..5, 0u64..8), 0..6),
            ys in prop::collection::vec((1u64..5, 0u64..8), 0..6),
            zs in prop::collection::vec((1u64..5, 0u64..8), 0..6),
        ) {
            let build = |obs: &[(u64, u64)]| {
                let mut f = StampFrontier::new();
                for &(p, w) in obs {
                    f.observe(peer(p), HlcStamp::new(w, 0, peer(p)));
                }
                f
            };
            let (a, b, c) = (build(&xs), build(&ys), build(&zs));

            let mut ab = a.clone();
            ab.merge(&b);
            let mut ba = b.clone();
            ba.merge(&a);
            prop_assert_eq!(&ab, &ba, "commutative");

            let mut left = ab.clone();
            left.merge(&c);
            let mut bc = b.clone();
            bc.merge(&c);
            let mut right = a.clone();
            right.merge(&bc);
            prop_assert_eq!(&left, &right, "associative");

            prop_assert!(!ab.clone().merge(&b), "idempotent: re-merge changes nothing");
        }
    }

    // --- #lzcrdtplane3: OpLog + anti-entropy delta exchange ---

    /// A test op: a stamped LWW write of an i32, so a log can drive a register.
    #[derive(Debug, Clone, PartialEq)]
    struct LwwWrite(i32);

    #[test]
    fn op_log_record_is_idempotent_and_tracks_frontier() {
        let mut log: OpLog<LwwWrite> = OpLog::new();
        assert!(log.is_empty());

        let s1 = HlcStamp::new(10, 0, peer(1));
        assert!(log.record(s1, LwwWrite(1)));
        // Re-recording the same stamp is a no-op.
        assert!(!log.record(s1, LwwWrite(1)));
        assert_eq!(log.len(), 1);
        assert_eq!(log.frontier().get(peer(1)), Some(s1));
    }

    #[test]
    fn op_log_missing_since_returns_only_unseen_ops_in_order() {
        let mut log: OpLog<LwwWrite> = OpLog::new();
        let a1 = HlcStamp::new(10, 0, peer(1));
        let a2 = HlcStamp::new(20, 0, peer(1));
        let b1 = HlcStamp::new(15, 0, peer(2));
        log.record(a2, LwwWrite(2));
        log.record(a1, LwwWrite(1));
        log.record(b1, LwwWrite(3));

        // A requester that has seen peer 1 up to a1 and nothing from peer 2.
        let mut since = StampFrontier::new();
        since.observe(peer(1), a1);
        let missing: Vec<HlcStamp> = log
            .missing_since(&since)
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        // a1 already seen; a2 and b1 are missing, returned in causal order.
        assert_eq!(missing, vec![b1, a2]);

        // A requester that has seen everything gets nothing.
        let full = log.frontier().clone();
        assert!(log.missing_since(&full).is_empty());
    }

    #[test]
    fn op_log_apply_remote_is_idempotent_and_causally_ordered() {
        let mut log: OpLog<LwwWrite> = OpLog::new();
        let s1 = HlcStamp::new(10, 0, peer(2));
        let s2 = HlcStamp::new(20, 0, peer(2));

        let mut order = Vec::new();
        // Deliver out of order and duplicated.
        let applied = log.apply_remote(
            vec![(s2, LwwWrite(2)), (s1, LwwWrite(1)), (s2, LwwWrite(2))],
            |stamp, op| order.push((*stamp, op.clone())),
        );
        assert_eq!(applied, 2, "the duplicate s2 is skipped");
        assert_eq!(
            order,
            vec![(s1, LwwWrite(1)), (s2, LwwWrite(2))],
            "applied in causal order"
        );

        // Re-delivering already-applied ops applies nothing.
        let again = log.apply_remote(vec![(s1, LwwWrite(1)), (s2, LwwWrite(2))], |_, _| {
            panic!("must not re-apply a stored op")
        });
        assert_eq!(again, 0);
    }

    #[test]
    fn two_replicas_converge_through_anti_entropy_exchange() {
        // Each replica owns an Hlc, an OpLog, and an LWW register; they exchange
        // missing ops pairwise (the transport seam carries (stamp, op) tuples)
        // and must converge to the same register value.
        let mut hlc_a = Hlc::new(peer(1));
        let mut hlc_b = Hlc::new(peer(2));
        let mut log_a: OpLog<LwwWrite> = OpLog::new();
        let mut log_b: OpLog<LwwWrite> = OpLog::new();
        let mut reg_a = LwwRegister::new(0i32, HlcStamp::new(0, 0, peer(1)));
        let mut reg_b = LwwRegister::new(0i32, HlcStamp::new(0, 0, peer(2)));

        // Concurrent local writes: neither replica has seen the other.
        let sa = hlc_a.send(100);
        reg_a.set(7, sa);
        log_a.record(sa, LwwWrite(7));

        let sb = hlc_b.send(200);
        reg_b.set(9, sb);
        log_b.record(sb, LwwWrite(9));

        // A pulls B's missing ops (advertising A's frontier), applies into reg_a.
        let to_a = log_b.missing_since(log_a.frontier());
        log_a.apply_remote(to_a, |stamp, op| {
            reg_a.set(op.0, *stamp);
        });

        // B pulls A's missing ops symmetrically.
        let to_b = log_a.missing_since(log_b.frontier());
        log_b.apply_remote(to_b, |stamp, op| {
            reg_b.set(op.0, *stamp);
        });

        assert_eq!(reg_a.value(), reg_b.value(), "replicas converge");
        assert_eq!(
            reg_a.value(),
            9,
            "highest HLC stamp (peer 2 @ wall 200) wins"
        );

        // Anti-entropy is idempotent: a second exchange round changes nothing
        // and ships no ops (both frontiers now cover both peers).
        assert!(log_b.missing_since(log_a.frontier()).is_empty());
        assert!(log_a.missing_since(log_b.frontier()).is_empty());
    }

    #[test]
    fn op_log_drives_a_pn_counter_to_convergence() {
        // Two replicas each increment a shared PN counter; exchanging ops both
        // ways converges to the summed value, and re-exchange is idempotent.
        let mut log_a: OpLog<u64> = OpLog::new();
        let mut log_b: OpLog<u64> = OpLog::new();
        let mut ctr_a = PnCounter::new();
        let mut ctr_b = PnCounter::new();

        let sa = HlcStamp::new(100, 0, peer(1));
        ctr_a.increment(peer(1), 5);
        log_a.record(sa, 5);
        let sb = HlcStamp::new(110, 0, peer(2));
        ctr_b.increment(peer(2), 3);
        log_b.record(sb, 3);

        for (stamp, amt) in log_b.missing_since(log_a.frontier()) {
            log_a.apply_remote([(stamp, amt)], |s, a| ctr_a.increment(s.peer, *a));
        }
        for (stamp, amt) in log_a.missing_since(log_b.frontier()) {
            log_b.apply_remote([(stamp, amt)], |s, a| ctr_b.increment(s.peer, *a));
        }

        assert_eq!(ctr_a.value(), 8);
        assert_eq!(ctr_b.value(), 8, "both replicas reach the summed value");
    }

    // --- #lzcrdtplane4: frontier-driven tombstone GC ---

    #[test]
    fn is_collectable_tracks_the_stability_frontier() {
        let mut plane = CrdtPlane::new(peer(1));
        plane.add_peer(peer(2));
        let del = HlcStamp::new(200, 0, peer(1));

        plane.tick(250); // self frontier entry (250, 0, peer 1)
        // Peer 2 is an expected member but unseen -> no stable frontier.
        assert!(
            !plane.is_collectable(del),
            "unseen member withholds collectability"
        );

        // Peer 2 observed only BELOW the delete -> frontier = min is below it.
        plane.observe_remote(HlcStamp::new(150, 0, peer(2)), 260);
        assert!(
            !plane.is_collectable(del),
            "a member behind the delete still withholds GC"
        );

        // Peer 2 catches up past the delete -> the frontier now covers it.
        plane.observe_remote(HlcStamp::new(300, 0, peer(2)), 310);
        assert!(plane.is_collectable(del));
    }

    #[test]
    fn gc_seq_collects_a_tombstone_only_once_every_replica_has_observed_it() {
        let mut plane = CrdtPlane::new(peer(1));
        plane.add_peer(peer(2));
        let mut seq: SeqCrdt<u64, &str> = SeqCrdt::new(peer(1));

        seq.insert_back(1, "x", 100);
        seq.remove(&1, 200); // delete stamped (200, 0, peer 1)
        assert!(
            !seq.contains(&1),
            "removed element is tombstoned (still stored)"
        );

        // A's own clock is past the delete, but peer 2 is unseen -> frontier None.
        plane.tick(250);
        assert_eq!(
            plane.gc_seq(&mut seq),
            0,
            "no GC until every member is observed"
        );

        // Peer 2 seen, but only below the delete -> still not collectable.
        plane.observe_remote(HlcStamp::new(150, 0, peer(2)), 260);
        assert_eq!(
            plane.gc_seq(&mut seq),
            0,
            "a lagging member blocks collection"
        );

        // Peer 2 catches up past the delete -> the tombstone is now collectable.
        plane.observe_remote(HlcStamp::new(300, 0, peer(2)), 310);
        assert_eq!(
            plane.gc_seq(&mut seq),
            1,
            "tombstone collected once every replica has observed it"
        );
        // Idempotent: the entry is gone, a second pass collects nothing.
        assert_eq!(plane.gc_seq(&mut seq), 0);
    }

    #[test]
    fn gc_seq_is_inert_for_a_single_writer_session() {
        // A lone writer (membership = {self}) reaches a stable frontier as soon
        // as it ticks, so its own observed deletes are collectable — but with no
        // peers there is no coordination cost and GC is driven purely locally.
        let mut plane = CrdtPlane::new(peer(1));
        let mut seq: SeqCrdt<u64, &str> = SeqCrdt::new(peer(1));
        seq.insert_back(1, "x", 100);
        seq.remove(&1, 200);

        // No tick yet -> frontier is None -> nothing collected.
        assert_eq!(plane.gc_seq(&mut seq), 0);
        // After a tick past the delete, the single-member frontier covers it.
        plane.tick(250);
        assert_eq!(plane.gc_seq(&mut seq), 1);
    }

    // --- #lzcrdtplane4b: OpId-frontier-driven TextCrdt tombstone GC ---

    #[test]
    fn op_id_frontier_takes_the_per_peer_max_and_membership_min() {
        // A buffer authored by peer 1 (counter ends at 3), and one by peer 2.
        let a = TextCrdt::from_str(1, "abc");
        let mut b = TextCrdt::from_str(2, "z");

        let mut f = OpIdFrontier::new();
        assert!(f.is_empty());
        assert!(f.observe(a.clock()), "first observe advances");
        assert!(
            !f.observe(a.clock()),
            "re-observing the same id is idempotent"
        );
        f.observe(b.clock());
        assert_eq!(f.len(), 2);

        // Frontier over the full membership = the per-peer minimum id.
        assert_eq!(f.frontier([1, 2]), Some(b.clock().min(a.clock())));
        // An expected-but-unobserved member withholds the frontier.
        assert_eq!(f.frontier([1, 2, 3]), None);
        // A lower observation never lowers the stored max.
        b.delete(0); // advances peer 2's counter past `a`
        let high = b.clock();
        f.observe(high);
        assert_eq!(f.get(2), Some(high));
        f.observe(TextCrdt::from_str(2, "y").clock()); // a *lower* peer-2 id
        assert_eq!(f.get(2), Some(high), "max is sticky");
    }

    #[test]
    fn is_op_collectable_tracks_the_op_stability_frontier() {
        let mut plane = CrdtPlane::new(peer(1));
        plane.add_peer(peer(2));

        // Peer 1 tombstones a trailing leaf; the delete mints OpId (4, peer 1).
        let mut a = TextCrdt::from_str(1, "abc");
        a.delete(2); // tombstone 'c'
        let del = a.clock();

        plane.observe_op(a.clock()); // self past the delete
        // Peer 2 is an expected member but unseen -> no stable frontier.
        assert!(
            !plane.is_op_collectable(del),
            "unseen member withholds collectability"
        );

        // Peer 2 observed only BELOW the delete -> the min is below it.
        let behind = TextCrdt::from_str(2, "z"); // peer-2 counter = 1
        plane.observe_op(behind.clock());
        assert!(
            !plane.is_op_collectable(del),
            "a member behind the delete still withholds GC"
        );

        // Peer 2 catches up past the delete -> the frontier now covers it.
        let mut ahead = behind.clone();
        ahead.merge(&a); // advances peer-2's counter past the delete id
        plane.observe_op(ahead.clock());
        assert!(plane.is_op_collectable(del));
    }

    #[test]
    fn gc_text_collects_a_tombstone_only_once_every_replica_has_observed_it() {
        let mut plane = CrdtPlane::new(peer(1));
        plane.add_peer(peer(2));

        let mut a = TextCrdt::from_str(1, "abc");
        let mut behind = a.fork(2); // peer 2 forks BEFORE the delete (counter 3)
        a.delete(2); // tombstone trailing 'c' (a leaf); delete OpId = (4, peer 1)
        assert_eq!(a.text(), "ab");
        assert_eq!(a.tombstone_count(), 1);

        // A's own clock is past the delete, but peer 2 is unseen -> frontier None.
        plane.observe_op(a.clock());
        assert_eq!(
            plane.gc_text(&mut a),
            0,
            "no GC until every member is observed"
        );

        // Peer 2 seen, but only below the delete -> still not collectable.
        plane.observe_op(behind.clock());
        assert_eq!(
            plane.gc_text(&mut a),
            0,
            "a lagging member blocks collection"
        );

        // Peer 2 catches up past the delete -> the tombstone is now collectable.
        behind.merge(&a);
        plane.observe_op(behind.clock());
        assert_eq!(
            plane.gc_text(&mut a),
            1,
            "tombstone collected once every replica has observed it"
        );
        assert_eq!(a.tombstone_count(), 0);
        assert_eq!(a.text(), "ab", "visible text is unchanged by GC");
        // Idempotent: the entry is gone, a second pass collects nothing.
        assert_eq!(plane.gc_text(&mut a), 0);
    }

    #[test]
    fn gc_text_is_inert_for_a_single_writer_session() {
        // A lone writer (membership = {self}) reaches a stable frontier as soon
        // as it observes its own clock; with no peers GC is driven purely locally.
        let mut plane = CrdtPlane::new(peer(1));
        let mut a = TextCrdt::from_str(1, "abc");
        a.delete(2); // tombstone trailing 'c'

        // No observation yet -> frontier is None -> nothing collected.
        assert_eq!(plane.gc_text(&mut a), 0);
        // After observing the local clock past the delete, the single-member
        // frontier covers it.
        plane.observe_op(a.clock());
        assert_eq!(plane.gc_text(&mut a), 1);
        assert_eq!(a.text(), "ab");
    }
}
