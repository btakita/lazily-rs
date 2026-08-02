//! Multi-channel reactive bridge hub (#97xn / #lzmuxbridge), behind the
//! `webrtc` feature.
//!
//! Plan: `tasks/software/plan-lazily-multichannel-bridge.md`.
//!
//! Each transport ([`WebRtcSink`]/[`WebRtcSource`] over WebRTC, WebSocket, or
//! the in-process loopback) connects one reactive graph to **one** peer set.
//! [`BridgeHub`] is the layer above that: it owns N attachments and routes a
//! source-cell write arriving on one channel out to the *other* channels, so a
//! single graph is bridged across heterogeneous transports at once.
//!
//! # Re-derive, do not relay raw frames
//!
//! The hub is the **graph-apply authority**, and it never forwards a peer's raw
//! frame to another peer. Two boundaries are enforced here, the same ones the
//! per-channel transports already model for a single peer:
//!
//! - **Inbound write enforcement.** A peer may only propose **source-cell
//!   writes** ([`DeltaOp::CellSet`]) and only on nodes it is granted
//!   [`OpKind::Write`](crate::OpKind) for ([`RemoteOp::write`]). Every other op
//!   kind from a peer (slot values, invalidations, node/edge structure) is
//!   authority-derived and is dropped — a peer cannot forge graph structure.
//! - **Outbound read filtering.** The authorized write is re-emitted to each
//!   other attachment as a fresh [`Delta`], **re-filtered to that peer's read
//!   allowlist** via [`Delta::filter_readable`] and stamped with that peer's own
//!   monotonic epoch. A peer therefore never receives a node outside its
//!   allowlist, even though the write originated on a different channel.
//!
//! Because each attachment is stored behind an object-safe, error-erased
//! handle, one hub can mix a WebRTC peer, a WebSocket peer, and an in-process
//! peer simultaneously.

use crate::distributed::{PeerId, PeerPermissions, RemoteOp};
use crate::ipc::{CrdtSync, Delta, DeltaOp, IpcMessage, IpcSink, IpcSource};

/// Error from a [`BridgeHub`] operation: an erased transport failure.
#[derive(Debug)]
pub struct HubError(pub String);

impl std::fmt::Display for HubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bridge hub transport error: {}", self.0)
    }
}

impl std::error::Error for HubError {}

/// Object-safe outbound used internally by the hub so heterogeneous transports
/// (different [`IpcSink::Error`] types) share one attachment type.
trait HubSink {
    fn send_msg(&mut self, message: &IpcMessage) -> Result<(), HubError>;
}

impl<S> HubSink for S
where
    S: IpcSink,
    S::Error: std::fmt::Display,
{
    fn send_msg(&mut self, message: &IpcMessage) -> Result<(), HubError> {
        self.send(message).map_err(|e| HubError(e.to_string()))
    }
}

/// Object-safe inbound counterpart to [`HubSink`].
trait HubSource {
    fn recv_msg(&mut self) -> Result<Option<IpcMessage>, HubError>;
}

impl<S> HubSource for S
where
    S: IpcSource,
    S::Error: std::fmt::Display,
{
    fn recv_msg(&mut self) -> Result<Option<IpcMessage>, HubError> {
        self.recv().map_err(|e| HubError(e.to_string()))
    }
}

/// One peer connected to the hub: its identity, its full permission grant, and
/// its (error-erased) transport endpoints plus its outbound epoch cursor.
struct HubAttachment {
    peer: PeerId,
    perms: PeerPermissions,
    sink: Box<dyn HubSink>,
    source: Box<dyn HubSource>,
    out_epoch: u64,
}

/// Routes authorized source-cell writes between heterogeneous peer channels.
///
/// Construct with [`BridgeHub::new`], wire peers with [`BridgeHub::attach`], and
/// drive routing with [`BridgeHub::poll`] (call it from the same loop that pumps
/// the underlying transports).
#[derive(Default)]
pub struct BridgeHub {
    attachments: Vec<HubAttachment>,
}

impl BridgeHub {
    /// An empty hub.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of attached peers.
    pub fn len(&self) -> usize {
        self.attachments.len()
    }

    /// Whether no peers are attached.
    pub fn is_empty(&self) -> bool {
        self.attachments.is_empty()
    }

    /// Attach a peer over any [`IpcSink`]/[`IpcSource`] transport pair.
    ///
    /// `perms` is the peer's full grant; the hub consults it for both inbound
    /// write checks and outbound read filtering, so it must agree with whatever
    /// filtering the sink itself performs.
    pub fn attach<Si, So>(&mut self, peer: PeerId, perms: PeerPermissions, sink: Si, source: So)
    where
        Si: IpcSink + 'static,
        Si::Error: std::fmt::Display,
        So: IpcSource + 'static,
        So::Error: std::fmt::Display,
    {
        self.attachments.push(HubAttachment {
            peer,
            perms,
            sink: Box::new(sink),
            source: Box::new(source),
            out_epoch: 0,
        });
    }

    /// Drain every attached source once and fan out the authorized writes to the
    /// other peers. Returns the number of deltas routed.
    ///
    /// The drain and the fan-out are separated so a source and a sink on
    /// different attachments are never borrowed at once; routing a write back to
    /// its originating peer is always skipped.
    pub fn poll(&mut self) -> Result<usize, HubError> {
        // Phase A: drain inbound from every source. Source-cell writes are
        // write-authorized into bare `DeltaOp`s; CRDT-plane sync frames
        // (`#lzcrdtplane5b`) are kept whole for read-filtered fan-out.
        let mut inbound: Vec<(usize, Vec<DeltaOp>)> = Vec::new();
        let mut inbound_sync: Vec<(usize, CrdtSync)> = Vec::new();
        for i in 0..self.attachments.len() {
            while let Some(message) = self.attachments[i].source.recv_msg()? {
                let att = &self.attachments[i];
                if let IpcMessage::CrdtSync(sync) = &message {
                    inbound_sync.push((i, sync.clone()));
                    continue;
                }
                let ops = authorize_inbound(att.peer, &att.perms, &message);
                if !ops.is_empty() {
                    inbound.push((i, ops));
                }
            }
        }

        // Phase B: fan each authorized write out to the other attachments,
        // re-derived (read-filtered + re-epoched) for each target peer.
        let mut routed = 0;
        for (src_i, ops) in &inbound {
            for j in 0..self.attachments.len() {
                if j == *src_i {
                    continue;
                }
                let att = &mut self.attachments[j];
                // Build from the authorized ops, then re-filter to peer j's read
                // allowlist; an op j may not read is dropped, never opaque.
                let candidate = Delta::new(att.out_epoch, att.out_epoch + 1, ops.clone());
                let filtered = candidate.filter_readable(&att.perms, att.peer);
                if filtered.ops.is_empty() {
                    continue;
                }
                att.out_epoch += 1;
                att.sink.send_msg(&IpcMessage::Delta(filtered))?;
                routed += 1;
            }
        }

        // Phase C: fan each CRDT sync frame out to the other attachments,
        // re-filtered to each target's read allowlist. Ops a target cannot read
        // are omitted; the frontier advertisement is retained so the receiver's
        // causal-stability watermark stays sound. The frame's converged CvRDT
        // ops are commutative/idempotent, so the hub forwards them rather than
        // re-deriving a single authoritative write as it does for a Delta.
        for (src_i, sync) in &inbound_sync {
            for j in 0..self.attachments.len() {
                if j == *src_i {
                    continue;
                }
                let att = &mut self.attachments[j];
                let filtered = sync.filter_readable(&att.perms, att.peer);
                if filtered.ops.is_empty() {
                    continue;
                }
                att.sink.send_msg(&IpcMessage::CrdtSync(filtered))?;
                routed += 1;
            }
        }
        Ok(routed)
    }
}

/// Keep only the source-cell writes a peer is actually allowed to make; drop
/// every other op kind (authority-derived structure a peer cannot forge).
fn authorize_inbound(peer: PeerId, perms: &PeerPermissions, message: &IpcMessage) -> Vec<DeltaOp> {
    match message {
        IpcMessage::Delta(delta) => delta
            .ops
            .iter()
            .filter(|op| match op {
                DeltaOp::CellSet { node, .. } => perms.is_allowed(peer, RemoteOp::write(*node)),
                _ => false,
            })
            .cloned()
            .collect(),
        // A peer is not the snapshot authority; it cannot push full state.
        IpcMessage::Snapshot(_) => Vec::new(),
        // CrdtSync multi-writer plane traffic is fanned out whole (read-filtered)
        // in `poll`'s Phase C (`#lzcrdtplane5b`); it never reaches this
        // write-authorization path. The arm stays for exhaustiveness.
        IpcMessage::CrdtSync(_) => Vec::new(),
        // Reliable-sync control frames are not write ops; they carry no DeltaOps.
        // `DeltaSinceRequest` (#lzspecdeltacrdt) is a read request — the reply is
        // built by `CrdtPlaneRuntime::delta_reply` and read-filtered on the way
        // out, so it grants the requester no write authority here.
        IpcMessage::ResyncRequest(_)
        | IpcMessage::OutboxAck(_)
        | IpcMessage::DeltaSinceRequest(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webrtc_transport::{DataChannel, InMemoryDataChannel, WebRtcSink, WebRtcSource};
    use crate::{IpcValue, NodeId, OpKind};

    fn perms(peer: PeerId, reads: &[u64], writes: &[u64]) -> PeerPermissions {
        let mut p = PeerPermissions::new();
        p.allow_many(peer, OpKind::Read, reads.iter().map(|&n| NodeId(n)));
        p.allow_many(peer, OpKind::Write, writes.iter().map(|&n| NodeId(n)));
        p
    }

    /// Raw-frame the message onto a channel (peer side bypasses hub filtering;
    /// inbound is not filtered, only write-authorized).
    fn send(ch: &InMemoryDataChannel, message: &IpcMessage) {
        ch.send_frame(serde_json::to_vec(message).unwrap()).unwrap();
    }

    fn recv(ch: &InMemoryDataChannel) -> Option<IpcMessage> {
        ch.try_recv_frame()
            .unwrap()
            .map(|f| serde_json::from_slice(&f).unwrap())
    }

    fn write_delta(node: u64, value: u8) -> IpcMessage {
        IpcMessage::Delta(Delta::new(
            0,
            1,
            vec![DeltaOp::cell_set(NodeId(node), vec![value])],
        ))
    }

    /// Attach a peer to the hub over an in-process channel; return the peer end.
    fn attach(hub: &mut BridgeHub, peer: PeerId, p: PeerPermissions) -> InMemoryDataChannel {
        let (peer_end, hub_end) = InMemoryDataChannel::pair();
        hub.attach(
            peer,
            p.clone(),
            WebRtcSink::new(hub_end.clone(), p, peer),
            WebRtcSource::new(hub_end),
        );
        peer_end
    }

    #[test]
    fn write_fans_out_only_to_readers() {
        // A may read+write N1; B may read N1; C may read only N2.
        let (a, b, c) = (PeerId(1), PeerId(2), PeerId(3));
        let mut hub = BridgeHub::new();
        let pa = attach(&mut hub, a, perms(a, &[1], &[1]));
        let pb = attach(&mut hub, b, perms(b, &[1], &[]));
        let pc = attach(&mut hub, c, perms(c, &[2], &[]));

        // A writes N1; the hub fans it out.
        send(&pa, &write_delta(1, 42));
        let routed = hub.poll().unwrap();

        // B (reads N1) gets it; C (cannot read N1) gets nothing; A never echoes.
        match recv(&pb).expect("B should receive the N1 write") {
            IpcMessage::Delta(d) => {
                assert_eq!(d.ops.len(), 1);
                match &d.ops[0] {
                    DeltaOp::CellSet { node, payload } => {
                        assert_eq!(*node, NodeId(1));
                        assert_eq!(*payload, IpcValue::from(vec![42u8]));
                    }
                    other => panic!("expected CellSet, got {other:?}"),
                }
            }
            other => panic!("expected delta, got {other:?}"),
        }
        assert!(
            recv(&pc).is_none(),
            "C may not read N1 — must receive nothing"
        );
        assert!(
            recv(&pa).is_none(),
            "A must not receive its own echoed write"
        );
        assert_eq!(routed, 1, "exactly one delta routed (to B)");
    }

    #[test]
    fn unauthorized_write_is_dropped() {
        // A may read+write N1 only; it tries to write N2.
        let (a, b) = (PeerId(1), PeerId(2));
        let mut hub = BridgeHub::new();
        let pa = attach(&mut hub, a, perms(a, &[1, 2], &[1]));
        let pb = attach(&mut hub, b, perms(b, &[1, 2], &[]));

        send(&pa, &write_delta(2, 7)); // N2 write — A lacks write(N2)
        let routed = hub.poll().unwrap();

        assert_eq!(routed, 0, "unauthorized write must not be routed");
        assert!(
            recv(&pb).is_none(),
            "B must not receive a write A was not allowed to make"
        );
    }

    #[test]
    fn snapshot_from_peer_is_rejected() {
        use crate::{NodeSnapshot, Snapshot};
        let (a, b) = (PeerId(1), PeerId(2));
        let mut hub = BridgeHub::new();
        let pa = attach(&mut hub, a, perms(a, &[1], &[1]));
        let pb = attach(&mut hub, b, perms(b, &[1], &[]));

        // A peer cannot assert authoritative full state.
        let snap = Snapshot::new(
            1,
            vec![NodeSnapshot::payload(NodeId(1), "t", vec![9])],
            vec![],
            vec![NodeId(1)],
        );
        send(&pa, &IpcMessage::Snapshot(snap));
        let routed = hub.poll().unwrap();

        assert_eq!(routed, 0, "a peer-pushed snapshot must be dropped");
        assert!(recv(&pb).is_none());
    }

    /// Pins the deny-by-default arm in `authorize_inbound` (`#failclosedsweep`):
    /// the op filter's catch-all is an authority boundary, not an oversight. A
    /// peer may write a source cell it is allowed to write and nothing else —
    /// structure ops (`NodeAdd` / `NodeRemove` / `EdgeAdd` / `EdgeRemove`) and
    /// authority-derived `SlotValue`/`Invalidate` publications are dropped even
    /// when the peer holds write permission on the node.
    #[test]
    fn structural_ops_from_a_peer_are_dropped_even_with_write_permission() {
        use crate::NodeState;
        let (a, b) = (PeerId(1), PeerId(2));
        let forgeries = vec![
            DeltaOp::NodeAdd {
                node: NodeId(1),
                type_tag: "t".to_string(),
                state: NodeState::Payload(vec![9]),
                key: None,
            },
            DeltaOp::NodeRemove { node: NodeId(1) },
            DeltaOp::EdgeAdd {
                dependent: NodeId(1),
                dependency: NodeId(1),
            },
            DeltaOp::EdgeRemove {
                dependent: NodeId(1),
                dependency: NodeId(1),
            },
            DeltaOp::slot_value(NodeId(1), vec![9u8]),
            DeltaOp::invalidate(NodeId(1)),
        ];
        for op in forgeries {
            let mut hub = BridgeHub::new();
            // A holds READ + WRITE on N1: only the op KIND can be what stops it.
            let pa = attach(&mut hub, a, perms(a, &[1], &[1]));
            let pb = attach(&mut hub, b, perms(b, &[1], &[]));

            send(&pa, &IpcMessage::Delta(Delta::new(0, 1, vec![op.clone()])));
            let routed = hub.poll().unwrap();

            assert_eq!(routed, 0, "{op:?} must not be routed");
            assert!(
                recv(&pb).is_none(),
                "{op:?} must not reach another peer as authoritative state"
            );
        }
    }

    #[test]
    fn crdt_sync_fans_out_to_readers_only() {
        use crate::ipc::{CrdtOp, WireStamp};

        // A sends mixed-node CRDT state; B may read N1, C may read only N2.
        let (a, b, c) = (PeerId(1), PeerId(2), PeerId(3));
        let mut hub = BridgeHub::new();
        let pa = attach(&mut hub, a, perms(a, &[1, 2, 3], &[1, 2, 3]));
        let pb = attach(&mut hub, b, perms(b, &[1], &[]));
        let pc = attach(&mut hub, c, perms(c, &[2], &[]));

        let stamp_1 = WireStamp {
            wall_time: 7,
            logical: 0,
            peer: 1,
        };
        let stamp_2 = WireStamp {
            wall_time: 8,
            logical: 0,
            peer: 1,
        };
        let stamp_3 = WireStamp {
            wall_time: 9,
            logical: 0,
            peer: 1,
        };
        let sync = CrdtSync::new(
            vec![(1, stamp_3)],
            vec![
                CrdtOp::new(NodeId(1), stamp_1, IpcValue::from(vec![42u8])),
                CrdtOp::new(NodeId(2), stamp_2, IpcValue::from(vec![43u8])),
                CrdtOp::new(NodeId(3), stamp_3, IpcValue::from(vec![44u8])),
            ],
        );
        send(&pa, &IpcMessage::CrdtSync(sync));
        let routed = hub.poll().unwrap();

        // Each target gets only the ops it can read. The frontier advert is
        // retained because it carries causal progress, not node payload.
        match recv(&pb).expect("B should receive the N1 sync frame") {
            IpcMessage::CrdtSync(s) => {
                assert_eq!(s.ops.len(), 1);
                assert_eq!(s.ops[0].node, NodeId(1));
                assert_eq!(s.ops[0].state, IpcValue::from(vec![42u8]));
                assert_eq!(s.frontier, vec![(1, stamp_3)], "frontier advert retained");
            }
            other => panic!("expected a CrdtSync frame, got {other:?}"),
        }
        match recv(&pc).expect("C should receive the N2 sync frame") {
            IpcMessage::CrdtSync(s) => {
                assert_eq!(s.ops.len(), 1);
                assert_eq!(s.ops[0].node, NodeId(2));
                assert_eq!(s.ops[0].state, IpcValue::from(vec![43u8]));
                assert_eq!(s.frontier, vec![(1, stamp_3)], "frontier advert retained");
            }
            other => panic!("expected a CrdtSync frame, got {other:?}"),
        }
        assert!(
            recv(&pa).is_none(),
            "A must not receive its own echoed frame"
        );
        assert_eq!(routed, 2, "exactly two sync frames routed (to B and C)");
    }

    /// Phase 2: one hub bridging a *WebSocket* peer to an *in-process* peer —
    /// proves the error-erased attachment really mixes transport types.
    #[cfg(feature = "websocket")]
    #[tokio::test]
    async fn write_bridges_websocket_peer_to_in_process_peer() {
        use crate::ws_backend::WsDataChannel;
        use std::time::Duration;

        let (a, b) = (PeerId(1), PeerId(2));
        let mut hub = BridgeHub::new();

        // Peer A over a real WebSocket handshake (in-process duplex, no network).
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let server =
            tokio::spawn(async move { tokio_tungstenite::accept_async(server_io).await.unwrap() });
        let (client_ws, _r) = tokio_tungstenite::client_async("ws://localhost/", client_io)
            .await
            .unwrap();
        let server_ws = server.await.unwrap();
        let a_client = WsDataChannel::from_stream(client_ws);
        let a_hub = WsDataChannel::from_stream(server_ws);
        let pa = perms(a, &[1], &[1]);
        hub.attach(
            a,
            pa.clone(),
            WebRtcSink::new(a_hub.clone(), pa, a),
            WebRtcSource::new(a_hub),
        );

        // Peer B over the in-process loopback.
        let pb_chan = attach(&mut hub, b, perms(b, &[1], &[]));

        // A writes N1 over the WebSocket.
        a_client
            .send_frame(serde_json::to_vec(&write_delta(1, 99)).unwrap())
            .unwrap();

        // Let the WS driver tasks move bytes, then poll until the write routes.
        let mut routed = 0;
        for _ in 0..500 {
            routed += hub.poll().unwrap();
            if routed > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(routed, 1, "WS->in-process write should route exactly once");

        match recv(&pb_chan).expect("in-process peer B receives the WS-originated write") {
            IpcMessage::Delta(d) => match &d.ops[0] {
                DeltaOp::CellSet { node, payload } => {
                    assert_eq!(*node, NodeId(1));
                    assert_eq!(*payload, IpcValue::from(vec![99u8]));
                }
                other => panic!("expected CellSet, got {other:?}"),
            },
            other => panic!("expected delta, got {other:?}"),
        }
    }
}
