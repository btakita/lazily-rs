//! Runtime integration of the distributed CRDT cell plane (`#lzcrdtplane5b`,
//! the FINAL phase of `#lzcrdtplane`).
//!
//! Plan: `tasks/software/plan-lazily-distributed-crdt-plane.md`.
//!
//! Phases 1–4 built the *plane primitives* — the [`CrdtPlane`] clock + stamp
//! frontier, the [`OpLog`] anti-entropy substrate, the per-cell
//! [`ReplicatedCell`]/register CRDTs, and frontier-driven Seq/Text GC. Phase 5a
//! built the *wire format* — [`CrdtOp`]/[`CrdtSync`]/[`WireStamp`] and the
//! permission-filtered [`IpcSink`]/[`IpcSource`] seam. This module is the glue
//! that makes them a live runtime:
//!
//! - **`merge:crdt` root-cell registry.** [`CrdtPlaneRuntime`] owns the session's
//!   replicated root cells, addressed by [`NodeId`] with an optional wire-stable
//!   [`NodeKey`] (producer projection, `#lzwirekey`) that survives `NodeId`
//!   churn.
//! - **Local edit → op.** [`local_update`](CrdtPlaneRuntime::local_update)
//!   mutates a typed cell, stamps the edit on the plane clock, records the
//!   converged state in the op log, and returns the [`CrdtOp`] to broadcast.
//! - **Remote op → reactive graph.** [`ingest`](CrdtPlaneRuntime::ingest) folds
//!   each not-yet-seen op into its target replica via
//!   [`ReplicatedCell::merge_remote`] — driving downstream derived slots — and
//!   advances the clock + stamp frontier so the causal-stability watermark and
//!   tombstone GC stay sound.
//! - **Anti-entropy frames.** [`sync_frame`](CrdtPlaneRuntime::sync_frame) /
//!   [`sync_reply`](CrdtPlaneRuntime::sync_reply) advertise the local frontier
//!   and ship the ops a peer is missing; delivery is bounded, idempotent, and
//!   resumable.
//!
//! With `< 2` live writers the plane is inert: nothing calls `ingest`, the
//! stability frontier withholds GC, and the single-producer Snapshot/Delta
//! mirror behaves exactly as before.
//!
//! Cell state crosses the wire as JSON ([`IpcValue::Inline`]); the module is
//! gated on `webrtc` (which pulls `ipc` + `serde_json` + a concrete transport)
//! so the runtime always has a codec and a transport seam to ride.

use std::any::Any;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cell::Source;
use crate::context::Context;
use crate::crdt::{
    CellCrdt, CrdtPlane, HlcStamp, LwwRegister, OpLog, ReplicatedCell, StampFrontier,
};
use crate::distributed::{NodeId, PeerId};
use crate::ipc::{CrdtOp, CrdtSync, DeltaSinceRequest, IpcValue, KeyIndex, NodeKey, WireStamp};

/// The base [`NodeId`] for entries a family materializes on first remote
/// observation (`#lzfamilysync`). Local family nodes are private — keyed ops
/// resolve by [`NodeKey`], never by raw `NodeId` — so this only needs to avoid
/// colliding with application-assigned node ids; the runtime skips any id already
/// in use.
const FAMILY_NODE_BASE: u64 = 1 << 48;

/// Materializes a fresh plane cell for a family entry from a remote peer's wire
/// state, the missing half of family-granularity sync (`#lzfamilysync`).
///
/// The plain [`CrdtPlaneRuntime`] drops a keyed op whose entry is not already
/// registered locally, so a family key added on one replica never appears on
/// another. A registered `FamilyFactory` closes that gap: an inbound keyed op whose
/// [`NodeKey`] first segment matches the factory's `namespace` materializes a fresh
/// entry seeded from the op's converged state, so membership and derived aggregates
/// converge across replicas.
trait FamilyFactory {
    /// The family namespace — matched against a keyed op's first [`NodeKey`]
    /// segment (e.g. namespace `live` owns keys `live/2`, `live/3`).
    fn namespace(&self) -> &str;
    /// Build a fresh plane cell seeded from a remote entry's converged `bytes`.
    /// Returns `None` if the bytes do not decode as this family's CRDT type.
    fn materialize(&self, ctx: &Context, bytes: &[u8]) -> Option<Box<dyn PlaneCell>>;
}

/// A last-writer-wins family: entries are [`LwwRegister<V>`] cells.
struct LwwFamilyFactory<V> {
    namespace: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> FamilyFactory for LwwFamilyFactory<V>
where
    V: Clone + PartialEq + Serialize + DeserializeOwned + 'static,
{
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn materialize(&self, ctx: &Context, bytes: &[u8]) -> Option<Box<dyn PlaneCell>> {
        // The op's state IS the remote register's converged value, so binding a fresh
        // cell from the decoded register seeds the new entry directly; the subsequent
        // `merge_state` in `ingest` is then an idempotent no-op.
        // INTENTIONAL leniency (`#failclosedsweep`), same contract as
        // `PlaneCell::merge_state`: `bytes` come from a peer, so an undecodable
        // state means that peer registered a different type under this family
        // namespace. Returning `None` declines to materialize the entry — the key
        // stays absent locally rather than materializing a wrong-typed cell — and
        // `ingest` continues with the rest of the frame. Pinned by
        // `an_undecodable_family_state_declines_to_materialize_the_key`.
        let remote: LwwRegister<V> = serde_json::from_slice(bytes).ok()?;
        Some(Box::new(ReplicatedCell::bind(ctx, remote)))
    }
}

/// Object-safe erasure over a `merge:crdt` root cell, so the runtime registry can
/// hold heterogeneous register/CRDT cells keyed by [`NodeId`] and still merge a
/// remote wire state into the right typed replica.
trait PlaneCell {
    /// Decode a remote replica's converged state from `bytes` and merge it into
    /// this cell, pushing the converged value into the reactive graph. Returns
    /// `true` iff the local value changed (a redundant or undecodable merge is a
    /// no-op).
    fn merge_state(&mut self, ctx: &Context, bytes: &[u8]) -> bool;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<C> PlaneCell for ReplicatedCell<C>
where
    C: CellCrdt + Serialize + DeserializeOwned + 'static,
    C::Value: PartialEq + Clone + 'static,
{
    fn merge_state(&mut self, ctx: &Context, bytes: &[u8]) -> bool {
        match serde_json::from_slice::<C>(bytes) {
            Ok(remote) => self.merge_remote(ctx, &remote),
            // INTENTIONAL leniency (`#failclosedsweep`). `bytes` is a remote
            // replica's converged state, written by another process or another
            // binding; a decode failure means that peer has a different CRDT type
            // registered under this node than we do. The plane keeps liveness: one
            // undecodable op is dropped and every other op in the same `CrdtSync`
            // still applies, because a poison frame from one peer must not stall a
            // shared session for everyone else.
            //
            // The consequence is convergence, not corruption: this replica reports
            // no change, so no wrong value is published — but the op's stamp IS
            // observed and logged by `ingest`, so the frontier advances past it and
            // a later resync will NOT re-deliver it. The two replicas stay divergent
            // for that node until a subsequent op re-states the value in a form this
            // replica can decode. Pinned by
            // `an_undecodable_remote_state_is_dropped_without_stalling_the_frame`.
            Err(_) => false,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// The live runtime that bridges the distributed CRDT plane to a reactive graph's
/// `merge:crdt` root cells (`#lzcrdtplane5b`).
///
/// One runtime per shared session per replica. It owns the [`CrdtPlane`]
/// (identity + clock + stamp frontier + membership + GC), the [`OpLog`]
/// anti-entropy substrate, and the registry of replicated root cells.
pub struct CrdtPlaneRuntime {
    plane: CrdtPlane,
    log: OpLog<CrdtOp>,
    cells: BTreeMap<NodeId, Box<dyn PlaneCell>>,
    keys: KeyIndex,
    /// Registered family factories (`#lzfamilysync`), consulted when an inbound keyed
    /// op has no locally-registered entry, so a remote-added family key materializes
    /// here instead of being dropped.
    families: Vec<Box<dyn FamilyFactory>>,
    /// Per-namespace materialized keys, in first-materialization order, so a consumer
    /// can enumerate a family's present set (deferral-not-dealloc: only grows).
    family_members: BTreeMap<String, Vec<NodeKey>>,
    /// Reactive membership signal bumped whenever a family entry materializes — a
    /// derived aggregate over the family reads it so a remote-added key forces a
    /// recompute (a brand-new entry's cell is not yet a dependency; the epoch is).
    /// Created lazily on the first [`register_family_lww`](Self::register_family_lww).
    membership_epoch: Option<Source<u64>>,
    /// Monotonic allocator for locally-private family entry node ids.
    next_family_node: u64,
}

/// One address's converged state — the winning op's payload for `(node, key)`.
///
/// Mirrors the `converged` shape in `distributed/anti_entropy_converge.json` and
/// the equivalent accessors in lazily-py and lazily-go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergedEntry {
    /// The addressed node.
    pub node: NodeId,
    /// The wire-stable key, when the producer assigned one.
    pub key: Option<NodeKey>,
    /// The winning op's state.
    pub state: IpcValue,
}

impl CrdtPlaneRuntime {
    /// Create a runtime for the local `peer`.
    pub fn new(peer: PeerId) -> Self {
        Self {
            plane: CrdtPlane::new(peer),
            log: OpLog::new(),
            cells: BTreeMap::new(),
            keys: KeyIndex::new(),
            families: Vec::new(),
            family_members: BTreeMap::new(),
            membership_epoch: None,
            next_family_node: FAMILY_NODE_BASE,
        }
    }

    /// The local replica identity.
    pub fn peer(&self) -> PeerId {
        self.plane.peer()
    }

    /// Immutable access to the underlying plane (clock, stamp frontier,
    /// membership, GC drivers).
    pub fn plane(&self) -> &CrdtPlane {
        &self.plane
    }

    /// Mutable access to the underlying plane — drive frontier-based Seq/Text
    /// tombstone GC (`CrdtPlane::gc_seq` / `gc_text`) from the same watermark the
    /// runtime advances on every applied op.
    pub fn plane_mut(&mut self) -> &mut CrdtPlane {
        &mut self.plane
    }

    /// Number of registered `merge:crdt` root cells.
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether no cells are registered.
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Register a `merge:crdt` root cell under `node`, optionally projecting a
    /// wire-stable [`NodeKey`] (`#lzwirekey`) so the cell stays addressable across
    /// `NodeId` churn. Replicas that share a session must register the same CRDT
    /// type `C` under the same `node`/`key`.
    pub fn register<C>(&mut self, node: NodeId, key: Option<NodeKey>, cell: ReplicatedCell<C>)
    where
        C: CellCrdt + Serialize + DeserializeOwned + 'static,
        C::Value: PartialEq + Clone + 'static,
    {
        if let Some(key) = key {
            self.keys.insert(key, node);
        }
        self.cells.insert(node, Box::new(cell));
    }

    /// Register a last-writer-wins **family** (`#lzfamilysync`) under `namespace`, so
    /// a keyed op an entry of this family produces on a peer materializes an entry
    /// here on [`ingest`](Self::ingest) instead of being dropped. Entries are
    /// [`LwwRegister<V>`] cells addressed by `NodeKey` `namespace/<suffix>`.
    ///
    /// Creates the reactive membership signal on first call. Replicas that share a
    /// session must register the same family namespace + value type.
    pub fn register_family_lww<V>(&mut self, ctx: &Context, namespace: impl Into<String>)
    where
        V: Clone + PartialEq + Serialize + DeserializeOwned + 'static,
    {
        let namespace = namespace.into();
        if self.membership_epoch.is_none() {
            self.membership_epoch = Some(ctx.source(0u64));
        }
        self.family_members.entry(namespace.clone()).or_default();
        self.families.push(Box::new(LwwFamilyFactory::<V> {
            namespace,
            _marker: PhantomData,
        }));
    }

    /// The reactive membership signal (`#lzfamilysync`): depend on it from a derived
    /// aggregate over a family so a remote-materialized key forces a recompute.
    /// `None` until the first family is registered.
    pub fn membership_epoch(&self) -> Option<Source<u64>> {
        self.membership_epoch
    }

    /// Allocate a locally-private node id for a family entry, skipping any id already
    /// in use so a family node can never collide with an application-registered cell.
    fn mint_family_node(&mut self) -> NodeId {
        loop {
            let candidate = NodeId(self.next_family_node);
            self.next_family_node = self.next_family_node.wrapping_add(1);
            if !self.cells.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    /// Bump the membership epoch so a derived aggregate over a family recomputes when
    /// its present set grows.
    fn bump_membership_epoch(&self, ctx: &Context) {
        if let Some(epoch) = self.membership_epoch {
            let current = ctx.get(&epoch);
            ctx.set(&epoch, current.wrapping_add(1));
        }
    }

    /// Insert or update a local LWW family entry `namespace/<key_suffix>` to `value`
    /// at `now_micros`, returning the [`CrdtOp`] to broadcast (or `None` if the value
    /// was unchanged). Materializes the entry (and bumps membership) on first insert.
    pub fn family_set_lww<V>(
        &mut self,
        ctx: &Context,
        namespace: &str,
        key_suffix: &str,
        value: V,
        now_micros: u64,
    ) -> Option<CrdtOp>
    where
        V: Clone + PartialEq + Serialize + DeserializeOwned + 'static,
    {
        let key = NodeKey::from_segments([namespace, key_suffix]).ok()?;
        let stamp = self.plane.tick(now_micros);
        let node = match self.keys.node_for_key(&key) {
            Some(node) => node,
            None => {
                // First local insert: materialize a fresh entry seeded with `value`.
                let node = self.mint_family_node();
                let cell = ReplicatedCell::lww(ctx, value.clone(), stamp);
                self.cells.insert(node, Box::new(cell));
                self.keys.insert(key.clone(), node);
                self.record_family_member(namespace, &key);
                self.bump_membership_epoch(ctx);
                let state = {
                    let cell = self
                        .cells
                        .get(&node)?
                        .as_any()
                        .downcast_ref::<ReplicatedCell<LwwRegister<V>>>()?;
                    serde_json::to_vec(cell.crdt()).ok()?
                };
                let op = CrdtOp::keyed(node, key, WireStamp::from(stamp), IpcValue::Inline(state));
                self.log.record(stamp, op.clone());
                return Some(op);
            }
        };
        // Existing entry: a normal stamped LWW update.
        let state = {
            let cell = self
                .cells
                .get_mut(&node)?
                .as_any_mut()
                .downcast_mut::<ReplicatedCell<LwwRegister<V>>>()?;
            if !cell.update(ctx, |register| {
                register.set(value, stamp);
            }) {
                return None;
            }
            serde_json::to_vec(cell.crdt()).ok()?
        };
        let op = CrdtOp::keyed(node, key, WireStamp::from(stamp), IpcValue::Inline(state));
        self.log.record(stamp, op.clone());
        Some(op)
    }

    /// The materialized keys of family `namespace`, in first-materialization order.
    pub fn family_keys(&self, namespace: &str) -> Vec<NodeKey> {
        self.family_members
            .get(namespace)
            .cloned()
            .unwrap_or_default()
    }

    /// The current converged value of family entry `namespace/<key_suffix>`.
    pub fn family_value_lww<V>(&self, namespace: &str, key_suffix: &str) -> Option<V>
    where
        V: Clone + PartialEq + 'static,
    {
        let key = NodeKey::from_segments([namespace, key_suffix]).ok()?;
        let node = self.keys.node_for_key(&key)?;
        let cell = self
            .cells
            .get(&node)?
            .as_any()
            .downcast_ref::<ReplicatedCell<LwwRegister<V>>>()?;
        Some(cell.value())
    }

    /// Record a newly-materialized key in its family's present set (dedup so a
    /// re-observed key does not duplicate).
    fn record_family_member(&mut self, namespace: &str, key: &NodeKey) {
        let members = self
            .family_members
            .entry(namespace.to_string())
            .or_default();
        if !members.contains(key) {
            members.push(key.clone());
        }
    }

    /// The reactive [`Source`] of a registered cell — depend on it from a
    /// derived slot so the graph recomputes when a remote op converges.
    pub fn handle<C>(&self, node: NodeId) -> Option<Source<C::Value>>
    where
        C: CellCrdt + 'static,
        C::Value: PartialEq + Clone + 'static,
    {
        let cell = self
            .cells
            .get(&node)?
            .as_any()
            .downcast_ref::<ReplicatedCell<C>>()?;
        Some(cell.handle())
    }

    /// The current converged value of a registered cell.
    pub fn value<C>(&self, node: NodeId) -> Option<C::Value>
    where
        C: CellCrdt + 'static,
        C::Value: PartialEq + Clone + 'static,
    {
        let cell = self
            .cells
            .get(&node)?
            .as_any()
            .downcast_ref::<ReplicatedCell<C>>()?;
        Some(cell.value())
    }

    /// Apply a local edit to the cell at `node`.
    ///
    /// The plane clock ticks first (at wall time `now_micros`) and the fresh
    /// [`HlcStamp`] is handed to `mutate` so stamp-ordered registers
    /// ([`LwwRegister`](crate::LwwRegister)) can use it; commutative registers
    /// ([`MvRegister`](crate::MvRegister)/[`PnCounter`](crate::PnCounter)) ignore
    /// it. If the edit changes the value, the converged state is recorded in the
    /// op log and returned as the [`CrdtOp`] to broadcast; an unchanged edit, an
    /// unknown `node`, or a type mismatch returns `None`.
    pub fn local_update<C, F>(
        &mut self,
        ctx: &Context,
        node: NodeId,
        now_micros: u64,
        mutate: F,
    ) -> Option<CrdtOp>
    where
        C: CellCrdt + Serialize + DeserializeOwned + 'static,
        C::Value: PartialEq + Clone + 'static,
        F: FnOnce(&mut C, HlcStamp),
    {
        let stamp = self.plane.tick(now_micros);
        let state = {
            let cell = self
                .cells
                .get_mut(&node)?
                .as_any_mut()
                .downcast_mut::<ReplicatedCell<C>>()?;
            if !cell.update(ctx, |c| mutate(c, stamp)) {
                return None;
            }
            serde_json::to_vec(cell.crdt()).ok()?
        };
        let wire = WireStamp::from(stamp);
        let op = match self.keys.key_for_node(node).cloned() {
            Some(key) => CrdtOp::keyed(node, key, wire, IpcValue::Inline(state)),
            None => CrdtOp::new(node, wire, IpcValue::Inline(state)),
        };
        self.log.record(stamp, op.clone());
        Some(op)
    }

    /// Ingest a remote anti-entropy frame: fold every not-yet-seen [`CrdtOp`] into
    /// its target replica (driving the reactive graph) exactly once, advancing the
    /// plane clock + stamp frontier so the causal-stability watermark stays sound.
    /// Returns the number of ops newly applied.
    ///
    /// Re-delivering a frame the receiver already has is a no-op (the op log
    /// dedups by stamp), so the exchange is idempotent and resumable.
    pub fn ingest(&mut self, ctx: &Context, sync: &CrdtSync, now_micros: u64) -> usize {
        for (_, wire) in &sync.frontier {
            let stamp = HlcStamp::from(*wire);
            if stamp.peer != self.plane.peer() {
                self.plane.observe_remote(stamp, now_micros);
            }
        }
        let incoming = sync
            .ops
            .iter()
            .map(|op| (HlcStamp::from(op.stamp), op.clone()));
        // Disjoint field borrows so the apply closure can touch the plane, the
        // registry, and the key index while the op log dedups.
        let Self {
            plane,
            log,
            cells,
            keys,
            families,
            family_members,
            membership_epoch,
            next_family_node,
        } = self;
        log.apply_remote(incoming, |stamp, op| {
            plane.observe_remote(*stamp, now_micros);
            let node = op
                .key
                .as_ref()
                .and_then(|key| keys.node_for_key(key))
                .unwrap_or(op.node);
            let IpcValue::Inline(bytes) = &op.state else {
                return;
            };
            if let Some(cell) = cells.get_mut(&node) {
                cell.merge_state(ctx, bytes);
                return;
            }
            // Materialize-on-ingest (`#lzfamilysync`): a keyed op whose entry is not
            // registered locally materializes it if its key belongs to a registered
            // family namespace, instead of being dropped (proved in lazily-formal
            // `FamilySync.applyOp_present` / `applyOp_absent_adopts`).
            let Some(key) = op.key.as_ref() else { return };
            let Some(namespace) = key.segments().next() else {
                return;
            };
            let Some(family) = families.iter().find(|f| f.namespace() == namespace) else {
                return;
            };
            let Some(cell) = family.materialize(ctx, bytes) else {
                return;
            };
            // Mint a locally-private node id, skipping any id already in use so a
            // family entry can never collide with an application-registered cell.
            let local_node = loop {
                let candidate = NodeId(*next_family_node);
                *next_family_node = (*next_family_node).wrapping_add(1);
                if !cells.contains_key(&candidate) {
                    break candidate;
                }
            };
            cells.insert(local_node, cell);
            keys.insert(key.clone(), local_node);
            let members = family_members.entry(namespace.to_string()).or_default();
            if !members.contains(key) {
                members.push(key.clone());
            }
            // Bump membership so a derived aggregate over the family recomputes for the
            // newly-present key (a brand-new cell is not yet a dependency; the epoch is).
            if let Some(epoch) = membership_epoch.as_ref() {
                let current = ctx.get(epoch);
                ctx.set(epoch, current.wrapping_add(1));
            }
        })
    }

    /// The converged view: the winning op's state per `(node, key)` address.
    ///
    /// The canonical `distributed/anti_entropy_converge.json` fixture expresses
    /// convergence this way — raw winning payload per address — and the other
    /// bindings (`plane.converged()` in lazily-py, `convergedFor` in lazily-go)
    /// expose the same shape. lazily-rs previously exposed only the TYPED read
    /// (`value::<C>()`), which cannot answer it: registered cells hold
    /// deserialized CRDT state, so a conformance runner had no way to compare
    /// against the fixture and simply skipped that half.
    ///
    /// The fold is the log, not a second source of truth: ops arrive from
    /// `missing_since` in ascending stamp order, so writing each into a map keyed
    /// by `(node, key)` leaves the greatest stamp per address — and `HlcStamp`
    /// already orders by `(wall_time, logical, peer)`, so the peer tiebreak the
    /// fixture exercises falls out rather than being reimplemented here.
    pub fn converged(&self) -> Vec<ConvergedEntry> {
        let mut winners: BTreeMap<(NodeId, Option<NodeKey>), ConvergedEntry> = BTreeMap::new();
        for (_, op) in self.log.missing_since(&StampFrontier::new()) {
            let addr = (op.node, op.key.clone());
            winners.insert(
                addr,
                ConvergedEntry {
                    node: op.node,
                    key: op.key.clone(),
                    state: op.state.clone(),
                },
            );
        }
        winners.into_values().collect()
    }

    /// This replica's stamp frontier in wire form — the per-peer highest observed
    /// stamp it advertises so a peer can compute what it is missing.
    pub fn wire_frontier(&self) -> Vec<(u64, WireStamp)> {
        self.plane
            .frontier()
            .iter()
            .map(|(peer, stamp)| (peer.0, WireStamp::from(stamp)))
            .collect()
    }

    /// A frame shipping the *entire* op log plus this replica's frontier. Safe to
    /// resend (the receiver dedups); use it for an initial full anti-entropy
    /// round when the peer's frontier is unknown.
    pub fn sync_frame(&self) -> CrdtSync {
        self.sync_frame_since(&StampFrontier::new())
    }

    /// A frame advertising this replica's frontier and shipping only the ops a
    /// peer described by `since` has not yet observed.
    pub fn sync_frame_since(&self, since: &StampFrontier) -> CrdtSync {
        let ops = self
            .log
            .missing_since(since)
            .into_iter()
            .map(|(_, op)| op)
            .collect();
        CrdtSync::new(self.wire_frontier(), ops)
    }

    /// Reply to a peer's anti-entropy `request`: ship exactly the ops the
    /// requester (described by `request.frontier`) is missing. The pairwise pull
    /// half of the protocol.
    pub fn sync_reply(&self, request: &CrdtSync) -> CrdtSync {
        self.sync_frame_since(&wire_frontier_to_stamp(&request.frontier))
    }

    /// Reply to a [`DeltaSinceRequest`] (`#lzspecdeltacrdt`): ship only the
    /// CRDT cell states past the requester's frontier. This is the delta-sync
    /// analog of [`sync_reply`]: the requester sends its version vector
    /// explicitly (no ops), and the receiver responds with only the ops the
    /// requester is missing — not the full converged state.
    ///
    /// An empty response (no ops past the frontier) is valid: the requester is
    /// up to date. The join is the same semilattice (`apply_delta` ≡ `merge`),
    /// so the delta is safe to resend and applies in any order.
    pub fn delta_reply(&self, request: &DeltaSinceRequest) -> CrdtSync {
        self.sync_frame_since(&wire_frontier_to_stamp(&request.their_frontier))
    }
}

/// Lift a wire frontier advertisement back into a [`StampFrontier`].
fn wire_frontier_to_stamp(frontier: &[(u64, WireStamp)]) -> StampFrontier {
    let mut stamp_frontier = StampFrontier::new();
    for (peer, wire) in frontier {
        stamp_frontier.observe(PeerId(*peer), HlcStamp::from(*wire));
    }
    stamp_frontier
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LwwRegister, MvRegister, PnCounter};

    fn lww_cell(ctx: &Context, value: i64) -> ReplicatedCell<LwwRegister<i64>> {
        // Seed at the zero stamp for the local peer; the first local_update will
        // beat it.
        let seed = HlcStamp::from(WireStamp {
            wall_time: 0,
            logical: 0,
            peer: 0,
        });
        ReplicatedCell::lww(ctx, value, seed)
    }

    #[test]
    fn local_update_emits_keyed_op_and_records_it() {
        let ctx = Context::new();
        let mut rt = CrdtPlaneRuntime::new(PeerId(1));
        let key = NodeKey::new("counter").unwrap();
        rt.register(NodeId(7), Some(key.clone()), lww_cell(&ctx, 0));

        let op = rt
            .local_update::<LwwRegister<i64>, _>(&ctx, NodeId(7), 100, |r, s| {
                r.set(42, s);
            })
            .expect("changed write yields an op");

        assert_eq!(op.node, NodeId(7));
        assert_eq!(
            op.key.as_ref(),
            Some(&key),
            "producer key projected onto op"
        );
        assert_eq!(rt.value::<LwwRegister<i64>>(NodeId(7)), Some(42));
        // The op is in the log: a full frame ships exactly it.
        assert_eq!(rt.sync_frame().ops.len(), 1);
    }

    #[test]
    fn unchanged_local_write_emits_nothing() {
        let ctx = Context::new();
        let mut rt = CrdtPlaneRuntime::new(PeerId(1));
        rt.register(NodeId(1), None, lww_cell(&ctx, 5));
        // Re-writing the current value at a fresh stamp advances the register
        // stamp but does not change the value, so the reactive graph is untouched
        // and no op is emitted.
        let op = rt.local_update::<LwwRegister<i64>, _>(&ctx, NodeId(1), 200, |r, s| {
            r.set(5, s);
        });
        assert!(op.is_none(), "a value-preserving write emits no op");
        assert_eq!(rt.value::<LwwRegister<i64>>(NodeId(1)), Some(5));
    }

    #[test]
    fn ingest_is_idempotent() {
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register(NodeId(1), None, lww_cell(&ctx_a, 0));
        let op = a
            .local_update::<LwwRegister<i64>, _>(&ctx_a, NodeId(1), 100, |r, s| {
                r.set(11, s);
            })
            .unwrap();

        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register(NodeId(1), None, lww_cell(&ctx_b, 0));

        let frame = CrdtSync::new(a.wire_frontier(), vec![op]);
        assert_eq!(b.ingest(&ctx_b, &frame, 100), 1, "first apply lands");
        assert_eq!(b.ingest(&ctx_b, &frame, 101), 0, "re-apply is a no-op");
        assert_eq!(b.value::<LwwRegister<i64>>(NodeId(1)), Some(11));
    }

    /// Pins the INTENTIONAL leniency in `PlaneCell::merge_state`: a remote state
    /// this replica cannot decode is dropped, the rest of the frame still applies,
    /// and the local value is unchanged — liveness over strictness on a decode
    /// path a peer writes.
    #[test]
    fn an_undecodable_remote_state_is_dropped_without_stalling_the_frame() {
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register(NodeId(1), None, lww_cell(&ctx_a, 0));
        a.register(NodeId(2), None, lww_cell(&ctx_a, 0));
        let good = a
            .local_update::<LwwRegister<i64>, _>(&ctx_a, NodeId(2), 100, |r, s| {
                r.set(11, s);
            })
            .unwrap();

        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register(NodeId(1), None, lww_cell(&ctx_b, 7));
        b.register(NodeId(2), None, lww_cell(&ctx_b, 0));

        // A peer that registered a different type under NodeId(1) ships state this
        // replica cannot decode as `LwwRegister<i64>`.
        let poison = CrdtOp::new(
            NodeId(1),
            WireStamp {
                wall_time: 99,
                logical: 0,
                peer: 1,
            },
            IpcValue::Inline(br#"{"not":"a register"}"#.to_vec()),
        );

        let frame = CrdtSync::new(a.wire_frontier(), vec![poison, good]);
        b.ingest(&ctx_b, &frame, 200);

        assert_eq!(
            b.value::<LwwRegister<i64>>(NodeId(1)),
            Some(7),
            "the undecodable op publishes no value — the local replica is untouched"
        );
        assert_eq!(
            b.value::<LwwRegister<i64>>(NodeId(2)),
            Some(11),
            "the well-formed op in the SAME frame still applies"
        );
    }

    /// Pins the INTENTIONAL leniency in `LwwFamilyFactory::materialize`: a family
    /// op whose state this replica cannot decode declines to materialize the key
    /// rather than materializing a wrong-typed cell, and the rest of the frame
    /// still materializes.
    #[test]
    fn an_undecodable_family_state_declines_to_materialize_the_key() {
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register_family_lww::<bool>(&ctx_a, "live");
        let good = a
            .family_set_lww::<bool>(&ctx_a, "live", "3", true, 101)
            .unwrap();

        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register_family_lww::<bool>(&ctx_b, "live");

        let poison = CrdtOp::keyed(
            NodeId(900),
            NodeKey::new("live/2").unwrap(),
            WireStamp {
                wall_time: 100,
                logical: 0,
                peer: 1,
            },
            IpcValue::Inline(b"not-json-at-all".to_vec()),
        );

        let frame = CrdtSync::new(a.wire_frontier(), vec![poison, good]);
        b.ingest(&ctx_b, &frame, 200);

        assert_eq!(
            b.family_keys("live"),
            vec![NodeKey::new("live/3").unwrap()],
            "the undecodable key stays absent; the decodable one still materializes"
        );
        assert_eq!(b.family_value_lww::<bool>("live", "3"), Some(true));
        assert_eq!(
            b.family_value_lww::<bool>("live", "2"),
            None,
            "no wrong-typed cell was materialized for the undecodable key"
        );
    }

    #[test]
    fn family_sync_materializes_remote_keys_and_converges() {
        // Replica A adds two family entries; replica B has the family registered but no
        // entries. Ingesting A's ops MATERIALIZES the entries on B (the `#lzfamilysync`
        // gap, closed — proved in lazily-formal `FamilySync.applyOp_present` /
        // `applyOp_absent_adopts`) instead of dropping them; membership + values
        // converge.
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register_family_lww::<bool>(&ctx_a, "live");

        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register_family_lww::<bool>(&ctx_b, "live");

        let op2 = a
            .family_set_lww::<bool>(&ctx_a, "live", "2", true, 100)
            .unwrap();
        let op3 = a
            .family_set_lww::<bool>(&ctx_a, "live", "3", true, 101)
            .unwrap();

        assert!(b.family_keys("live").is_empty(), "B starts with no entries");

        let frame = CrdtSync::new(a.wire_frontier(), vec![op2, op3]);
        assert_eq!(
            b.ingest(&ctx_b, &frame, 200),
            2,
            "both remote keys materialize on B"
        );

        // Membership propagated (`present_merge`); values adopted.
        assert_eq!(b.family_keys("live").len(), 2);
        assert_eq!(b.family_value_lww::<bool>("live", "2"), Some(true));
        assert_eq!(b.family_value_lww::<bool>("live", "3"), Some(true));

        // Re-ingest is a no-op (`applyOp_idem`).
        assert_eq!(b.ingest(&ctx_b, &frame, 201), 0, "re-apply changes nothing");

        // A later LWW update converges too: A flips live/2 to false.
        let op2b = a
            .family_set_lww::<bool>(&ctx_a, "live", "2", false, 300)
            .unwrap();
        let frame2 = CrdtSync::new(a.wire_frontier(), vec![op2b]);
        assert_eq!(b.ingest(&ctx_b, &frame2, 400), 1);
        assert_eq!(b.family_value_lww::<bool>("live", "2"), Some(false));
        assert_eq!(
            b.family_keys("live").len(),
            2,
            "membership only grows (deferral-not-dealloc)"
        );
    }

    #[test]
    fn family_membership_epoch_bumps_on_remote_materialize() {
        // The reactive membership signal fires when a remote key materializes, so a
        // derived aggregate over the family recomputes — the eventual-transparency
        // property proved in lazily-formal `FamilySync.aggregate_converges`.
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register_family_lww::<bool>(&ctx_a, "live");
        let op = a
            .family_set_lww::<bool>(&ctx_a, "live", "7", true, 100)
            .unwrap();

        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register_family_lww::<bool>(&ctx_b, "live");
        let epoch = b
            .membership_epoch()
            .expect("registering a family creates the membership epoch");
        let before = ctx_b.get(&epoch);

        let frame = CrdtSync::new(a.wire_frontier(), vec![op]);
        b.ingest(&ctx_b, &frame, 200);

        assert_ne!(
            before,
            ctx_b.get(&epoch),
            "materializing a remote key bumps the membership epoch"
        );
    }

    #[test]
    fn pn_counter_converges_under_concurrent_increments() {
        // Two replicas independently increment the same PN-counter cell; after a
        // mutual exchange both see the summed value (per-peer max merge).
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register(
            NodeId(3),
            None,
            ReplicatedCell::<PnCounter>::counter(&ctx_a),
        );
        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register(
            NodeId(3),
            None,
            ReplicatedCell::<PnCounter>::counter(&ctx_b),
        );

        let op_a = a
            .local_update::<PnCounter, _>(&ctx_a, NodeId(3), 100, |c, _| c.increment(PeerId(1), 3))
            .unwrap();
        let op_b = b
            .local_update::<PnCounter, _>(&ctx_b, NodeId(3), 100, |c, _| c.increment(PeerId(2), 5))
            .unwrap();

        b.ingest(&ctx_b, &CrdtSync::new(a.wire_frontier(), vec![op_a]), 101);
        a.ingest(&ctx_a, &CrdtSync::new(b.wire_frontier(), vec![op_b]), 101);

        assert_eq!(a.value::<PnCounter>(NodeId(3)), Some(8));
        assert_eq!(b.value::<PnCounter>(NodeId(3)), Some(8));
    }

    #[test]
    fn mutual_exchange_expands_membership_and_arms_the_watermark() {
        // A single replica's frontier covers only itself — not a sound
        // cross-replica GC watermark. A mutual exchange folds the other peer into
        // membership, so the causal-stability frontier then spans both.
        let ctx_a = Context::new();
        let mut a = CrdtPlaneRuntime::new(PeerId(1));
        a.register(
            NodeId(1),
            None,
            ReplicatedCell::<MvRegister<i64>>::multi_value(&ctx_a),
        );
        let ctx_b = Context::new();
        let mut b = CrdtPlaneRuntime::new(PeerId(2));
        b.register(
            NodeId(1),
            None,
            ReplicatedCell::<MvRegister<i64>>::multi_value(&ctx_b),
        );

        let op_a = a
            .local_update::<MvRegister<i64>, _>(&ctx_a, NodeId(1), 100, |r, s| {
                r.set(1, s.peer);
            })
            .unwrap();
        let op_b = b
            .local_update::<MvRegister<i64>, _>(&ctx_b, NodeId(1), 100, |r, s| {
                r.set(2, s.peer);
            })
            .unwrap();
        assert_eq!(a.plane().membership().count(), 1, "B not seen yet");

        b.ingest(&ctx_b, &CrdtSync::new(a.wire_frontier(), vec![op_a]), 101);
        a.ingest(&ctx_a, &CrdtSync::new(b.wire_frontier(), vec![op_b]), 101);

        assert_eq!(
            a.plane().membership().count(),
            2,
            "B folded into membership"
        );
        assert_eq!(b.plane().membership().count(), 2);
        assert!(
            a.plane().stability_frontier().is_some(),
            "both members observed -> watermark active, GC may run"
        );
        assert!(b.plane().stability_frontier().is_some());
        // Concurrent MV writes are both retained (neither dominates).
        let mut a_vals = a.value::<MvRegister<i64>>(NodeId(1)).unwrap();
        a_vals.sort_unstable();
        assert_eq!(a_vals, vec![1, 2]);
    }
}
