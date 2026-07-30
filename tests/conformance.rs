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

use common::Expect;
use lazily::{
    Delta, DeltaApplyStatus, DeltaOp, EdgeSnapshot, IpcMessage, NodeId, NodeSnapshot, NodeState,
    PeerId, PeerPermissions, SHM_BLOB_HEADER_LEN, ShmBlobArena, Snapshot,
};
use serde::Deserialize;
use std::collections::HashSet;

const FIXTURES_DIR: &str = "tests/conformance";
const SPEC_FIXTURES_DIR: &str = "../lazily-spec/conformance";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    description: String,
    protocol_version: u64,
    kind: String,
    assertions: serde_json::Value,
    wire: serde_json::Value,
}

/// The corpus copy when the sibling spec is checked out, else the vendored one.
fn fixture_path(name: &str) -> String {
    let spec_path = format!("{SPEC_FIXTURES_DIR}/{name}");
    if std::path::Path::new(&spec_path).exists() {
        spec_path
    } else {
        format!("{FIXTURES_DIR}/{name}")
    }
}

fn load_fixture(name: &str) -> Fixture {
    let path = fixture_path(name);
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

/// Guard a fixture's `assertions` block (`#lzassertunknownkeys`). Every key the
/// fixture declares must be consumed by the test below — a key the runner does
/// not read fails the fixture rather than being invisibly skipped, which is how
/// `first_op_payload_backend` sat unasserted in lazily-kt.
fn assertions<'a>(name: &str, block: &'a serde_json::Value) -> Expect<'a> {
    Expect::new(fixture_path(name), "assertions", block)
}

fn assert_u64(v: &Expect, key: &str) -> u64 {
    v[key]
        .as_u64()
        .unwrap_or_else(|| panic!("assertions should contain u64 field '{key}'"))
}

fn assert_str(v: &Expect, key: &str) -> String {
    v[key]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| panic!("assertions should contain string field '{key}'"))
}

fn assert_bool(v: &Expect, key: &str) -> bool {
    v[key]
        .as_bool()
        .unwrap_or_else(|| panic!("assertions should contain bool field '{key}'"))
}

/// The kind discriminator the corpus uses for a node's state.
fn node_state_kind(state: &NodeState) -> &'static str {
    match state {
        NodeState::Payload(_) => "Payload",
        NodeState::Opaque => "Opaque",
        NodeState::SharedBlob(_) => "SharedBlob",
    }
}

fn delta_op_kind(op: &DeltaOp) -> &'static str {
    match op {
        DeltaOp::CellSet { .. } => "CellSet",
        DeltaOp::SlotValue { .. } => "SlotValue",
        DeltaOp::Invalidate { .. } => "Invalidate",
        DeltaOp::NodeAdd { .. } => "NodeAdd",
        DeltaOp::NodeRemove { .. } => "NodeRemove",
        DeltaOp::EdgeAdd { .. } => "EdgeAdd",
        DeltaOp::EdgeRemove { .. } => "EdgeRemove",
    }
}

fn ipc_value_kind(value: &lazily::IpcValue) -> &'static str {
    match value {
        lazily::IpcValue::Inline(_) => "Inline",
        lazily::IpcValue::SharedBlob(_) => "SharedBlob",
    }
}

// ---------------------------------------------------------------------------
// Arena host fixture loader (the arena is not a wire type, so it carries
// `input` / `expected` instead of `wire`).
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArenaFixture {
    #[allow(dead_code)]
    description: String,
    #[allow(dead_code)]
    protocol_version: u64,
    kind: String,
    assertions: serde_json::Value,
    input: ArenaInput,
    expected: ArenaExpected,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArenaInput {
    capacity: usize,
    epoch: u64,
    payload: Vec<u8>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArenaDescriptor {
    offset: u64,
    len: u64,
    generation: u64,
    epoch: u64,
    checksum: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArenaExpected {
    descriptor: ArenaDescriptor,
    header_bytes: Vec<u8>,
    #[allow(dead_code)]
    payload_region: Vec<u8>,
}

fn load_arena_fixture(name: &str) -> ArenaFixture {
    let path = fixture_path(name);
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

    let a = assertions("snapshot_minimal.json", &fixture.assertions);
    assert_eq!(snapshot.epoch, assert_u64(&a, "epoch"));
    assert_eq!(snapshot.nodes.len(), assert_u64(&a, "node_count") as usize);
    assert_eq!(snapshot.edges.len(), assert_u64(&a, "edge_count") as usize);
    assert_eq!(snapshot.roots.len(), assert_u64(&a, "root_count") as usize);
    assert_eq!(
        snapshot.nodes[0].type_tag,
        assert_str(&a, "first_node_type_tag")
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

    // Driven from the fixture rather than transcribed: a hardcoded 7/3/2/2 is
    // the same defect one level down — the fixture's own numbers go unread.
    let a = assertions("snapshot_multi_node.json", &fixture.assertions);
    assert_eq!(snapshot.epoch, assert_u64(&a, "epoch"));
    assert_eq!(snapshot.nodes.len(), assert_u64(&a, "node_count") as usize);
    assert_eq!(snapshot.edges.len(), assert_u64(&a, "edge_count") as usize);
    assert_eq!(snapshot.roots.len(), assert_u64(&a, "root_count") as usize);

    let opaque_id = assert_u64(&a, "opaque_node_id");
    let opaque_node = snapshot
        .nodes
        .iter()
        .find(|n| n.node == NodeId(opaque_id))
        .expect("should find opaque node");
    assert!(matches!(opaque_node.state, NodeState::Opaque));
    assert_eq!(
        snapshot
            .nodes
            .iter()
            .any(|n| matches!(n.state, NodeState::Opaque)),
        assert_bool(&a, "has_opaque_node")
    );

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

    let a = assertions("snapshot_shared_blob.json", &fixture.assertions);
    assert_eq!(snapshot.epoch, assert_u64(&a, "epoch"));
    assert_eq!(snapshot.nodes.len(), assert_u64(&a, "node_count") as usize);
    assert_eq!(snapshot.edges.len(), assert_u64(&a, "edge_count") as usize);
    assert_eq!(snapshot.roots.len(), assert_u64(&a, "root_count") as usize);
    assert_eq!(
        node_state_kind(&snapshot.nodes[0].state),
        assert_str(&a, "first_node_state_kind")
    );

    let NodeState::SharedBlob(ref blob) = snapshot.nodes[0].state else {
        panic!("expected SharedBlob state");
    };
    assert_eq!(blob.offset, assert_u64(&a, "blob_offset"));
    assert_eq!(blob.len, assert_u64(&a, "blob_len"));
    assert_eq!(blob.epoch, assert_u64(&a, "blob_epoch"));

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

    let a = assertions("delta_sequential.json", &fixture.assertions);
    let expected_base = assert_u64(&a, "base_epoch");
    let expected_epoch = assert_u64(&a, "epoch");
    assert_eq!(delta.base_epoch, expected_base);
    assert_eq!(delta.epoch, expected_epoch);
    assert_eq!(
        delta.is_next_after(expected_base),
        assert_bool(&a, "is_sequential")
    );
    assert!(!delta.is_next_after(expected_base - 1));

    assert_eq!(delta.ops.len(), assert_u64(&a, "op_count") as usize);

    let seen_kinds: HashSet<&str> = delta.ops.iter().map(delta_op_kind).collect();
    assert_eq!(
        seen_kinds.len() == 7,
        assert_bool(&a, "has_all_op_variants"),
        "should see all 7 DeltaOp variants"
    );

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

    let a = assertions("delta_non_sequential.json", &fixture.assertions);
    let base = assert_u64(&a, "base_epoch");
    let epoch = assert_u64(&a, "epoch");
    assert_eq!(delta.base_epoch, base);
    assert_eq!(delta.epoch, epoch);
    assert_eq!(delta.is_next_after(base), assert_bool(&a, "is_sequential"));
    assert!(!delta.is_next_after(10));

    let status = delta.apply_status(10);
    assert_eq!(
        matches!(status, DeltaApplyStatus::ResyncRequired { .. }),
        assert_bool(&a, "resync_after_epoch_10")
    );
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

    let a = assertions("delta_shared_blob.json", &fixture.assertions);
    assert_eq!(delta.base_epoch, assert_u64(&a, "base_epoch"));
    assert_eq!(delta.epoch, assert_u64(&a, "epoch"));
    assert_eq!(delta.ops.len(), assert_u64(&a, "op_count") as usize);
    assert_eq!(
        delta_op_kind(&delta.ops[0]),
        assert_str(&a, "first_op_kind")
    );

    let DeltaOp::SlotValue { payload, .. } = &delta.ops[0] else {
        panic!("expected SlotValue op");
    };
    assert_eq!(
        ipc_value_kind(payload),
        assert_str(&a, "first_op_payload_kind")
    );
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

    let a = assertions("delta_zero_copy_arrow.json", &fixture.assertions);
    assert_eq!(delta.base_epoch, assert_u64(&a, "base_epoch"));
    assert_eq!(delta.epoch, assert_u64(&a, "epoch"));
    assert_eq!(delta.ops.len(), assert_u64(&a, "op_count") as usize);
    assert_eq!(
        delta_op_kind(&delta.ops[0]),
        assert_str(&a, "first_op_kind")
    );

    let DeltaOp::SlotValue { payload, .. } = &delta.ops[0] else {
        panic!("expected SlotValue op");
    };
    assert_eq!(
        ipc_value_kind(payload),
        assert_str(&a, "first_op_payload_kind")
    );
    let lazily::IpcValue::SharedBlob(blob) = payload else {
        panic!("expected SharedBlob payload");
    };
    assert_eq!(blob.offset, 40);
    assert_eq!(blob.len, 17);
    assert_eq!(blob.epoch, 9);
    // The optional `backend` discriminator selects the pluggable backend the
    // receiver resolves against (vs the default `shm`). Validates against
    // schemas/delta.json. This is the key lazily-kt's runner never read
    // (#lzassertunknownkeys); here the guard proves it is consumed.
    assert_eq!(
        blob.backend,
        lazily::BlobBackendKind::Arrow,
        "backend discriminator should parse as Arrow"
    );
    assert_eq!(assert_str(&a, "first_op_payload_backend"), "arrow");

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

    // The `assertions` block restates the arena contract in language-agnostic
    // terms. It was read by nothing (#lzassertunknownkeys): the test consumed
    // only `input`/`expected`, so `magic`, `header_len`, `capacity`, `epoch`,
    // `payload_len` and the duplicate `descriptor` were all invisible.
    let a = assertions("arena_blob.json", &fixture.assertions);
    assert_eq!(fixture.input.capacity as u64, assert_u64(&a, "capacity"));
    assert_eq!(fixture.input.epoch, assert_u64(&a, "epoch"));
    assert_eq!(
        SHM_BLOB_HEADER_LEN as u64,
        assert_u64(&a, "header_len"),
        "header length is part of the cross-language byte contract"
    );
    assert_eq!(
        fixture.input.payload.len() as u64,
        assert_u64(&a, "payload_len")
    );
    let want_descriptor: ArenaDescriptor =
        serde_json::from_value(a["descriptor"].clone()).expect("assertions.descriptor");
    assert_eq!(*expected, want_descriptor, "assertions vs expected agree");

    // 40-byte LZSH header byte-identical across rs / py / zig.
    let bytes = arena.bytes();
    // `magic` is spelled big-endian in the corpus ("LZSH") and stored
    // little-endian in the header, so the round trip is the assertion.
    let magic_le = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    assert_eq!(
        String::from_utf8(magic_le.to_be_bytes().to_vec()).unwrap(),
        assert_str(&a, "magic")
    );
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
