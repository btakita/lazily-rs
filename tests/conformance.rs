#![cfg(feature = "ipc")]

//! Cross-language conformance tests for the lazily IPC wire protocol.
//!
//! Each test loads a canonical JSON fixture from `tests/conformance/` and
//! validates that lazily-rs agrees on the wire format. Other language bindings
//! (lazily-py, lazily-zig) should implement the same assertions against the
//! same fixture files so all implementations stay in sync.
//!
//! Fixture schema:
//! ```json
//! {
//!   "description": "…",
//!   "protocol_version": 1,
//!   "kind": "Snapshot" | "Delta",
//!   "assertions": { … language-agnostic field checks … },
//!   "wire": { <IpcMessage as serde_json> }
//! }
//! ```

mod common;

use lazily::{
    Delta, DeltaApplyStatus, DeltaOp, EdgeSnapshot, IpcMessage, NodeId, NodeSnapshot, NodeState,
    PeerId, PeerPermissions, SHM_BLOB_HEADER_LEN, ShmBlobArena, Snapshot,
};
use serde::Deserialize;
use std::collections::HashSet;

const FIXTURES_DIR: &str = "tests/conformance";
const SPEC_FIXTURES_DIR: &str = "../lazily-spec/conformance";

#[derive(Debug, Deserialize)]
struct Fixture {
    description: String,
    protocol_version: u64,
    kind: String,
    assertions: serde_json::Value,
    wire: serde_json::Value,
}

fn load_fixture(name: &str) -> Fixture {
    let spec_path = format!("{SPEC_FIXTURES_DIR}/{name}");
    let local_path = format!("{FIXTURES_DIR}/{name}");
    let path = if std::path::Path::new(&spec_path).exists() {
        spec_path
    } else {
        local_path
    };
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    let fixture: Fixture = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"));
    assert_eq!(
        fixture.protocol_version, 1,
        "fixture {name} uses unsupported protocol version"
    );
    fixture
}

fn parse_wire(fixture: &Fixture) -> IpcMessage {
    let wire_json = serde_json::to_string(&fixture.wire)
        .unwrap_or_else(|e| panic!("wire value should serialize: {e}"));
    serde_json::from_str(&wire_json)
        .unwrap_or_else(|e| panic!("wire value should parse as IpcMessage: {e}"))
}

fn assert_round_trip_json(message: &IpcMessage, fixture: &Fixture) {
    let wire_json = serde_json::to_string(&fixture.wire).unwrap();
    let produced = serde_json::to_string(message).unwrap();

    let expected: serde_json::Value = serde_json::from_str(&wire_json).unwrap();
    let actual: serde_json::Value = serde_json::from_str(&produced).unwrap();
    assert_eq!(
        expected, actual,
        "round-trip JSON mismatch for fixture: {}",
        fixture.description
    );
}

#[cfg(feature = "ipc-msgpack")]
fn assert_round_trip_msgpack(message: &IpcMessage) {
    let encoded = message.encode_msgpack().unwrap();
    let decoded = IpcMessage::decode_msgpack(&encoded).unwrap();
    assert_eq!(decoded, *message);
}

fn assert_u64(v: &serde_json::Value, key: &str) -> u64 {
    v.get(key)
        .and_then(|v| v.as_u64())
        .unwrap_or_else(|| panic!("assertions should contain u64 field '{key}'"))
}

fn assert_str(v: &serde_json::Value, key: &str) -> String {
    v.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| panic!("assertions should contain string field '{key}'"))
}

// ---------------------------------------------------------------------------
// Arena host fixture loader (the arena is not a wire type, so it carries
// `input` / `expected` instead of `wire`).
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ArenaFixture {
    #[allow(dead_code)]
    description: String,
    #[allow(dead_code)]
    protocol_version: u64,
    kind: String,
    input: ArenaInput,
    expected: ArenaExpected,
}

#[derive(Debug, Deserialize)]
struct ArenaInput {
    capacity: usize,
    epoch: u64,
    payload: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct ArenaDescriptor {
    offset: u64,
    len: u64,
    generation: u64,
    epoch: u64,
    checksum: u64,
}

#[derive(Debug, Deserialize)]
struct ArenaExpected {
    descriptor: ArenaDescriptor,
    header_bytes: Vec<u8>,
    #[allow(dead_code)]
    payload_region: Vec<u8>,
}

fn load_arena_fixture(name: &str) -> ArenaFixture {
    let spec_path = format!("{SPEC_FIXTURES_DIR}/{name}");
    let local_path = format!("{FIXTURES_DIR}/{name}");
    let path = if std::path::Path::new(&spec_path).exists() {
        spec_path
    } else {
        local_path
    };
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("failed to parse arena fixture {path}: {e}"))
}

// ---------------------------------------------------------------------------
// Snapshot fixtures
// ---------------------------------------------------------------------------

#[test]
fn conformance_snapshot_minimal() {
    let fixture = load_fixture("snapshot_minimal.json");
    assert_eq!(fixture.kind, "Snapshot");

    let message = parse_wire(&fixture);
    let IpcMessage::Snapshot(snapshot) = &message else {
        panic!("expected Snapshot variant");
    };

    assert_eq!(snapshot.epoch, assert_u64(&fixture.assertions, "epoch"));
    assert_eq!(
        snapshot.nodes.len(),
        assert_u64(&fixture.assertions, "node_count") as usize
    );
    assert_eq!(
        snapshot.edges.len(),
        assert_u64(&fixture.assertions, "edge_count") as usize
    );
    assert_eq!(
        snapshot.roots.len(),
        assert_u64(&fixture.assertions, "root_count") as usize
    );
    assert_eq!(
        snapshot.nodes[0].type_tag,
        assert_str(&fixture.assertions, "first_node_type_tag")
    );
    assert!(matches!(snapshot.nodes[0].state, NodeState::Payload(_)));

    assert_round_trip_json(&message, &fixture);
}

#[test]
fn conformance_snapshot_multi_node() {
    let fixture = load_fixture("snapshot_multi_node.json");
    assert_eq!(fixture.kind, "Snapshot");

    let message = parse_wire(&fixture);
    let IpcMessage::Snapshot(snapshot) = &message else {
        panic!("expected Snapshot variant");
    };

    assert_eq!(snapshot.epoch, 7);
    assert_eq!(snapshot.nodes.len(), 3);
    assert_eq!(snapshot.edges.len(), 2);
    assert_eq!(snapshot.roots.len(), 2);

    let opaque_id = assert_u64(&fixture.assertions, "opaque_node_id");
    let opaque_node = snapshot
        .nodes
        .iter()
        .find(|n| n.node == NodeId(opaque_id))
        .expect("should find opaque node");
    assert!(matches!(opaque_node.state, NodeState::Opaque));

    assert_round_trip_json(&message, &fixture);
}

#[test]
fn conformance_snapshot_shared_blob() {
    let fixture = load_fixture("snapshot_shared_blob.json");
    assert_eq!(fixture.kind, "Snapshot");

    let message = parse_wire(&fixture);
    let IpcMessage::Snapshot(snapshot) = &message else {
        panic!("expected Snapshot variant");
    };

    assert_eq!(snapshot.epoch, 9);
    assert_eq!(snapshot.nodes.len(), 1);

    let NodeState::SharedBlob(ref blob) = snapshot.nodes[0].state else {
        panic!("expected SharedBlob state");
    };
    assert_eq!(blob.offset, 0);
    assert_eq!(blob.len, 16);
    assert_eq!(blob.epoch, 9);

    assert_round_trip_json(&message, &fixture);
}

// ---------------------------------------------------------------------------
// Delta fixtures
// ---------------------------------------------------------------------------

#[test]
fn conformance_delta_sequential() {
    let fixture = load_fixture("delta_sequential.json");
    assert_eq!(fixture.kind, "Delta");

    let message = parse_wire(&fixture);
    let IpcMessage::Delta(delta) = &message else {
        panic!("expected Delta variant");
    };

    let expected_base = assert_u64(&fixture.assertions, "base_epoch");
    let expected_epoch = assert_u64(&fixture.assertions, "epoch");
    assert_eq!(delta.base_epoch, expected_base);
    assert_eq!(delta.epoch, expected_epoch);
    assert!(delta.is_next_after(expected_base));
    assert!(!delta.is_next_after(expected_base - 1));

    assert_eq!(
        delta.ops.len(),
        assert_u64(&fixture.assertions, "op_count") as usize
    );

    let mut seen_kinds: HashSet<String> = HashSet::new();
    for op in &delta.ops {
        let kind = match op {
            DeltaOp::CellSet { .. } => "CellSet",
            DeltaOp::SlotValue { .. } => "SlotValue",
            DeltaOp::Invalidate { .. } => "Invalidate",
            DeltaOp::NodeAdd { .. } => "NodeAdd",
            DeltaOp::NodeRemove { .. } => "NodeRemove",
            DeltaOp::EdgeAdd { .. } => "EdgeAdd",
            DeltaOp::EdgeRemove { .. } => "EdgeRemove",
        };
        seen_kinds.insert(kind.to_string());
    }
    assert_eq!(seen_kinds.len(), 7, "should see all 7 DeltaOp variants");

    assert_round_trip_json(&message, &fixture);
}

#[test]
fn conformance_delta_non_sequential() {
    let fixture = load_fixture("delta_non_sequential.json");
    assert_eq!(fixture.kind, "Delta");

    let message = parse_wire(&fixture);
    let IpcMessage::Delta(delta) = &message else {
        panic!("expected Delta variant");
    };

    assert_eq!(delta.base_epoch, 12);
    assert_eq!(delta.epoch, 13);
    assert!(delta.is_next_after(12));
    assert!(!delta.is_next_after(10));

    let status = delta.apply_status(10);
    assert!(matches!(
        status,
        DeltaApplyStatus::ResyncRequired {
            last_epoch: 10,
            base_epoch: 12,
            epoch: 13,
        }
    ));

    assert_round_trip_json(&message, &fixture);
}

#[test]
fn conformance_delta_shared_blob() {
    let fixture = load_fixture("delta_shared_blob.json");
    assert_eq!(fixture.kind, "Delta");

    let message = parse_wire(&fixture);
    let IpcMessage::Delta(delta) = &message else {
        panic!("expected Delta variant");
    };

    assert_eq!(delta.base_epoch, 8);
    assert_eq!(delta.epoch, 9);
    assert_eq!(delta.ops.len(), 1);

    let DeltaOp::SlotValue { payload, .. } = &delta.ops[0] else {
        panic!("expected SlotValue op");
    };
    let lazily::IpcValue::SharedBlob(blob) = payload else {
        panic!("expected SharedBlob payload");
    };
    assert_eq!(blob.offset, 40);
    assert_eq!(blob.len, 17);
    assert_eq!(blob.epoch, 9);

    assert_round_trip_json(&message, &fixture);
}

// ---------------------------------------------------------------------------
// Zero-copy transport fixture (#lzzcpy): SharedBlob descriptor with an
// optional `backend` discriminator selecting the pluggable backend.
// ---------------------------------------------------------------------------

#[test]
fn conformance_delta_zero_copy_arrow() {
    let fixture = load_fixture("delta_zero_copy_arrow.json");
    assert_eq!(fixture.kind, "Delta");

    let message = parse_wire(&fixture);
    let IpcMessage::Delta(delta) = &message else {
        panic!("expected Delta variant");
    };

    assert_eq!(delta.base_epoch, 8);
    assert_eq!(delta.epoch, 9);
    assert_eq!(delta.ops.len(), 1);

    let DeltaOp::SlotValue { payload, .. } = &delta.ops[0] else {
        panic!("expected SlotValue op");
    };
    let lazily::IpcValue::SharedBlob(blob) = payload else {
        panic!("expected SharedBlob payload");
    };
    assert_eq!(blob.offset, 40);
    assert_eq!(blob.len, 17);
    assert_eq!(blob.epoch, 9);
    // The optional `backend` discriminator selects the pluggable backend the
    // receiver resolves against (vs the default `shm`). Validates against
    // schemas/delta.json.
    assert_eq!(
        blob.backend,
        lazily::BlobBackendKind::Arrow,
        "backend discriminator should parse as Arrow"
    );
    assert_eq!(
        assert_str(&fixture.assertions, "first_op_payload_backend"),
        "arrow"
    );

    assert_round_trip_json(&message, &fixture);
}

// ---------------------------------------------------------------------------
// Permission filtering cross-language contract
// ---------------------------------------------------------------------------

#[test]
fn conformance_permission_filter_omits_unreadable_nodes() {
    let peer_a = PeerId(1);
    let peer_b = PeerId(2);
    let mut permissions = PeerPermissions::new();
    permissions.allow_many(peer_a, lazily::OpKind::Read, [NodeId(1), NodeId(2)]);

    let snapshot = Snapshot::new(
        5,
        vec![
            NodeSnapshot::payload(NodeId(1), "i32", vec![1]),
            NodeSnapshot::payload(NodeId(2), "i32", vec![2]),
            NodeSnapshot::payload(NodeId(3), "i32", vec![3]),
        ],
        vec![
            EdgeSnapshot::new(NodeId(2), NodeId(1)),
            EdgeSnapshot::new(NodeId(3), NodeId(1)),
        ],
        vec![NodeId(1), NodeId(2), NodeId(3)],
    );

    let filtered = snapshot.filter_readable(&permissions, peer_a);
    assert_eq!(filtered.nodes.len(), 2);
    assert_eq!(filtered.edges.len(), 1);
    assert_eq!(filtered.roots, vec![NodeId(1), NodeId(2)]);

    let empty = snapshot.filter_readable(&permissions, peer_b);
    assert!(empty.nodes.is_empty());
    assert!(empty.edges.is_empty());
    assert!(empty.roots.is_empty());
}

#[test]
fn conformance_permission_delta_filter_omits_without_redaction() {
    let peer_a = PeerId(1);
    let mut permissions = PeerPermissions::new();
    permissions.allow_many(
        peer_a,
        lazily::OpKind::Read,
        [NodeId(1), NodeId(2), NodeId(5)],
    );

    let delta = Delta::next(
        8,
        vec![
            DeltaOp::cell_set(NodeId(1), vec![1]),
            DeltaOp::slot_value(NodeId(2), vec![2]),
            DeltaOp::invalidate(NodeId(3)),
            DeltaOp::NodeAdd {
                node: NodeId(4),
                type_tag: "u8".into(),
                state: NodeState::Payload(vec![4]),
                key: None,
            },
            DeltaOp::NodeRemove { node: NodeId(5) },
            DeltaOp::EdgeAdd {
                dependent: NodeId(2),
                dependency: NodeId(1),
            },
            DeltaOp::EdgeRemove {
                dependent: NodeId(3),
                dependency: NodeId(1),
            },
        ],
    );

    let filtered = delta.filter_readable(&permissions, peer_a);
    assert_eq!(filtered.ops.len(), 4);

    let op_kinds: Vec<&str> = filtered
        .ops
        .iter()
        .map(|op| match op {
            DeltaOp::CellSet { .. } => "CellSet",
            DeltaOp::SlotValue { .. } => "SlotValue",
            DeltaOp::Invalidate { .. } => "Invalidate",
            DeltaOp::NodeAdd { .. } => "NodeAdd",
            DeltaOp::NodeRemove { .. } => "NodeRemove",
            DeltaOp::EdgeAdd { .. } => "EdgeAdd",
            DeltaOp::EdgeRemove { .. } => "EdgeRemove",
        })
        .collect();
    assert_eq!(
        op_kinds,
        vec!["CellSet", "SlotValue", "NodeRemove", "EdgeAdd"]
    );
}

#[test]
fn conformance_ipc_message_transport_agnostic_bytes() {
    let message = IpcMessage::Delta(Delta::next(
        15,
        vec![
            DeltaOp::cell_set(NodeId(1), b"cell".to_vec()),
            DeltaOp::slot_value(NodeId(2), b"slot".to_vec()),
        ],
    ));

    let websocket_text = serde_json::to_string(&message).unwrap();
    let webrtc_data = websocket_text.as_bytes().to_vec();
    let ffi_buffer = webrtc_data.clone();

    assert_eq!(
        serde_json::from_str::<IpcMessage>(&websocket_text).unwrap(),
        message
    );
    assert_eq!(
        serde_json::from_slice::<IpcMessage>(&webrtc_data).unwrap(),
        message
    );
    assert_eq!(
        serde_json::from_slice::<IpcMessage>(&ffi_buffer).unwrap(),
        message
    );
}

#[cfg(feature = "ipc-msgpack")]
#[test]
fn conformance_msgpack_round_trips_canonical_fixtures() {
    for name in [
        "snapshot_minimal.json",
        "snapshot_multi_node.json",
        "snapshot_shared_blob.json",
        "delta_sequential.json",
        "delta_non_sequential.json",
        "delta_shared_blob.json",
        "delta_zero_copy_arrow.json",
    ] {
        let fixture = load_fixture(name);
        let message = parse_wire(&fixture);
        assert_round_trip_msgpack(&message);
    }
}

// ---------------------------------------------------------------------------
// ShmBlobArena host fixture (not a wire type — locks the arena byte contract
// across lazily-rs / lazily-py / lazily-zig).
// ---------------------------------------------------------------------------

#[test]
fn conformance_arena_blob_descriptor_and_header() {
    let fixture = load_arena_fixture("arena_blob.json");
    assert_eq!(fixture.kind, "Arena");

    let mut arena = ShmBlobArena::with_capacity(fixture.input.capacity).unwrap();
    let desc = arena
        .write_blob(fixture.input.epoch, &fixture.input.payload)
        .unwrap();

    let expected = &fixture.expected.descriptor;
    assert_eq!(desc.offset, expected.offset);
    assert_eq!(desc.len, expected.len);
    assert_eq!(desc.generation, expected.generation);
    assert_eq!(desc.epoch, expected.epoch);
    assert_eq!(desc.checksum, expected.checksum);

    // 40-byte LZSH header byte-identical across rs / py / zig.
    let bytes = arena.bytes();
    assert_eq!(
        &bytes[..SHM_BLOB_HEADER_LEN],
        &fixture.expected.header_bytes[..]
    );
    let plen = fixture.input.payload.len();
    assert_eq!(
        &bytes[SHM_BLOB_HEADER_LEN..SHM_BLOB_HEADER_LEN + plen],
        &fixture.expected.payload_region[..]
    );

    // round-trip
    assert_eq!(arena.read_blob(desc).unwrap(), &fixture.input.payload[..]);
}

#[cfg(feature = "ipc-binary")]
mod binary_conformance {
    use lazily::{Delta, DeltaOp, EdgeSnapshot, IpcMessage, NodeId, NodeSnapshot, Snapshot};

    #[test]
    fn conformance_binary_snapshot_round_trip() {
        let snapshot = Snapshot::new(
            7,
            vec![
                NodeSnapshot::payload(NodeId(1), "i32", vec![1, 2, 3]),
                NodeSnapshot::opaque(NodeId(2), "opaque-type"),
            ],
            vec![EdgeSnapshot::new(NodeId(2), NodeId(1))],
            vec![NodeId(1), NodeId(2)],
        );
        let message = IpcMessage::Snapshot(snapshot);

        let encoded = message.encode_binary().unwrap();
        let decoded = IpcMessage::decode_binary(&encoded).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn conformance_binary_delta_round_trip() {
        let delta = Delta::next(
            3,
            vec![
                DeltaOp::cell_set(NodeId(1), vec![10, 20]),
                DeltaOp::slot_value(NodeId(2), vec![30, 40]),
                DeltaOp::invalidate(NodeId(3)),
            ],
        );
        let message = IpcMessage::Delta(delta);

        let encoded = message.encode_binary().unwrap();
        let decoded = IpcMessage::decode_binary(&encoded).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn conformance_binary_smaller_than_json() {
        let snapshot = Snapshot::new(
            42,
            vec![NodeSnapshot::payload(NodeId(1), "i32", vec![1, 2, 3, 4])],
            vec![EdgeSnapshot::new(NodeId(1), NodeId(2))],
            vec![NodeId(1)],
        );
        let message = IpcMessage::Snapshot(snapshot);

        let json_len = serde_json::to_vec(&message).unwrap().len();
        let binary_len = message.encode_binary().unwrap().len();
        assert!(
            binary_len < json_len,
            "binary ({binary_len}) should be smaller than json ({json_len})"
        );
    }
}
