#![cfg(feature = "ipc")]

use lazily::{
    CapabilityHandshake, CrdtOp, CrdtSync, Delta, DeltaApplyStatus, DeltaOp, DeltaSinceRequest,
    EdgeSnapshot, IpcMessage, KeyIndex, NODE_KEY_MAX_SEGMENTS, NodeId, NodeKey, NodeKeyError,
    NodeSnapshot, NodeState, OpKind, PeerId, PeerPermissions, RemoteOp, SHM_BLOB_HEADER_LEN,
    ShmBlobArena, ShmBlobArenaError, Snapshot, WireStamp,
};

const PEER_A: PeerId = PeerId(1);
const PEER_B: PeerId = PeerId(2);

#[test]
fn snapshot_round_trips_through_serde() {
    let snapshot = Snapshot::new(
        7,
        vec![
            NodeSnapshot::payload(NodeId(1), "i32", vec![1, 2, 3]),
            NodeSnapshot::opaque(NodeId(2), "opaque-type"),
        ],
        vec![EdgeSnapshot::new(NodeId(2), NodeId(1))],
        vec![NodeId(1), NodeId(2)],
    );

    let json = serde_json::to_string(&IpcMessage::Snapshot(snapshot.clone())).unwrap();
    let back: IpcMessage = serde_json::from_str(&json).unwrap();

    assert_eq!(back, IpcMessage::Snapshot(snapshot));
}

#[test]
fn delta_round_trips_through_serde() {
    let delta = Delta::next(
        41,
        vec![
            DeltaOp::cell_set(NodeId(1), vec![10]),
            DeltaOp::slot_value(NodeId(2), vec![20]),
            DeltaOp::invalidate(NodeId(3)),
            DeltaOp::NodeAdd {
                node: NodeId(4),
                type_tag: "u64".into(),
                state: NodeState::Payload(vec![64]),
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

    let json = serde_json::to_string(&IpcMessage::Delta(delta.clone())).unwrap();
    let back: IpcMessage = serde_json::from_str(&json).unwrap();

    assert_eq!(back, IpcMessage::Delta(delta));
}

#[test]
fn delta_status_accepts_only_sequential_epochs() {
    let next = Delta::next(10, vec![]);
    assert_eq!(next.apply_status(10), DeltaApplyStatus::Apply);
    assert!(next.is_next_after(10));

    let gap = Delta::new(12, 13, vec![]);
    assert_eq!(
        gap.apply_status(10),
        DeltaApplyStatus::ResyncRequired {
            last_epoch: 10,
            base_epoch: 12,
            epoch: 13,
        }
    );

    let non_sequential = Delta::new(10, 12, vec![]);
    assert_eq!(
        non_sequential.apply_status(10),
        DeltaApplyStatus::ResyncRequired {
            last_epoch: 10,
            base_epoch: 10,
            epoch: 12,
        }
    );
}

#[test]
fn snapshot_filter_omits_non_readable_nodes_edges_and_roots() {
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
    let mut permissions = PeerPermissions::new();
    permissions.allow_many(PEER_A, OpKind::Read, [NodeId(1), NodeId(2)]);
    permissions.allow(PEER_A, RemoteOp::write(NodeId(3)));

    let filtered = snapshot.filter_readable(&permissions, PEER_A);

    assert_eq!(
        filtered.nodes,
        vec![
            NodeSnapshot::payload(NodeId(1), "i32", vec![1]),
            NodeSnapshot::payload(NodeId(2), "i32", vec![2]),
        ]
    );
    assert_eq!(
        filtered.edges,
        vec![EdgeSnapshot::new(NodeId(2), NodeId(1))]
    );
    assert_eq!(filtered.roots, vec![NodeId(1), NodeId(2)]);

    let empty = snapshot.filter_readable(&permissions, PEER_B);
    assert!(empty.nodes.is_empty());
    assert!(empty.edges.is_empty());
    assert!(empty.roots.is_empty());
}

#[test]
fn delta_filter_omits_non_readable_ops_without_redaction() {
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
    let mut permissions = PeerPermissions::new();
    permissions.allow_many(PEER_A, OpKind::Read, [NodeId(1), NodeId(2), NodeId(5)]);

    let filtered = delta.filter_readable(&permissions, PEER_A);

    assert_eq!(
        filtered.ops,
        vec![
            DeltaOp::cell_set(NodeId(1), vec![1]),
            DeltaOp::slot_value(NodeId(2), vec![2]),
            DeltaOp::NodeRemove { node: NodeId(5) },
            DeltaOp::EdgeAdd {
                dependent: NodeId(2),
                dependency: NodeId(1),
            },
        ]
    );
}

#[test]
fn crdt_sync_round_trips_through_serde() {
    let sync = CrdtSync::new(
        vec![
            (
                1,
                WireStamp {
                    wall_time: 200,
                    logical: 0,
                    peer: 1,
                },
            ),
            (
                2,
                WireStamp {
                    wall_time: 180,
                    logical: 3,
                    peer: 2,
                },
            ),
        ],
        vec![
            CrdtOp::new(
                NodeId(1),
                WireStamp {
                    wall_time: 200,
                    logical: 0,
                    peer: 1,
                },
                vec![10, 20],
            ),
            CrdtOp::keyed(
                NodeId(2),
                NodeKey::new("scores/alice").unwrap(),
                WireStamp {
                    wall_time: 180,
                    logical: 3,
                    peer: 2,
                },
                vec![30],
            ),
        ],
    );
    let json = serde_json::to_string(&IpcMessage::CrdtSync(sync.clone())).unwrap();
    let back: IpcMessage = serde_json::from_str(&json).unwrap();

    assert_eq!(back, IpcMessage::CrdtSync(sync));
}

#[test]
fn crdt_sync_filter_omits_non_readable_ops_but_keeps_frontier() {
    let frontier = vec![
        (
            1,
            WireStamp {
                wall_time: 200,
                logical: 0,
                peer: 1,
            },
        ),
        (
            2,
            WireStamp {
                wall_time: 200,
                logical: 0,
                peer: 2,
            },
        ),
    ];
    let sync = CrdtSync::new(
        frontier.clone(),
        vec![
            CrdtOp::new(
                NodeId(1),
                WireStamp {
                    wall_time: 1,
                    logical: 0,
                    peer: 1,
                },
                vec![1],
            ),
            CrdtOp::new(
                NodeId(2),
                WireStamp {
                    wall_time: 2,
                    logical: 0,
                    peer: 1,
                },
                vec![2],
            ),
            CrdtOp::new(
                NodeId(3),
                WireStamp {
                    wall_time: 3,
                    logical: 0,
                    peer: 1,
                },
                vec![3],
            ),
        ],
    );
    let mut permissions = PeerPermissions::new();
    permissions.allow_many(PEER_A, OpKind::Read, [NodeId(1), NodeId(3)]);

    let filtered = sync.filter_readable(&permissions, PEER_A);

    // Node 2's op is dropped entirely (omission, not redaction); 1 and 3 stay.
    assert_eq!(
        filtered.ops.iter().map(|op| op.node).collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(3)],
    );
    // The stamp-frontier advertisement is metadata, retained in full so the
    // receiver can still compute a sound causal-stability watermark.
    assert_eq!(filtered.frontier, frontier);
}

// --- #lzspecfrontiersuppress ---

#[test]
fn crdt_sync_frontier_suppress_omits_empty_frontier() {
    let ops = vec![CrdtOp::new(
        NodeId(7),
        WireStamp {
            wall_time: 13,
            logical: 0,
            peer: 1,
        },
        vec![7],
    )];
    let suppressed = CrdtSync::ops_only(ops.clone());
    assert!(suppressed.is_frontier_suppressed());

    let json = serde_json::to_string(&IpcMessage::CrdtSync(suppressed.clone())).unwrap();
    // The frontier field must be absent on the wire.
    assert!(
        !json.contains("\"frontier\""),
        "frontier should be omitted, got: {json}"
    );

    // Round-trips back.
    let back: IpcMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back, IpcMessage::CrdtSync(suppressed));
}

#[test]
fn crdt_sync_with_frontier_still_serializes_frontier() {
    let sync = CrdtSync::new(
        vec![(
            1,
            WireStamp {
                wall_time: 5,
                logical: 0,
                peer: 1,
            },
        )],
        vec![CrdtOp::new(
            NodeId(1),
            WireStamp {
                wall_time: 5,
                logical: 0,
                peer: 1,
            },
            vec![1],
        )],
    );
    let json = serde_json::to_string(&IpcMessage::CrdtSync(sync.clone())).unwrap();
    assert!(json.contains("\"frontier\""), "frontier should be present");
    let back: IpcMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back, IpcMessage::CrdtSync(sync));
}

// --- #lzspecbase64 ---

#[cfg(feature = "json-base64")]
#[test]
fn json_base64_round_trips_and_shrinks_payload() {
    let payload = vec![0xACu8; 256];
    let snapshot = Snapshot::new(
        1,
        vec![NodeSnapshot::payload(NodeId(1), "bytes", payload.clone())],
        vec![EdgeSnapshot::new(NodeId(1), NodeId(1))],
        vec![NodeId(1)],
    );
    let msg = IpcMessage::Snapshot(snapshot);

    let canonical = msg.encode_json().unwrap();
    let base64_encoded = msg.encode_json_base64().unwrap();

    // base64 must be materially smaller than the JSON-u8 array form.
    assert!(
        base64_encoded.len() < canonical.len() / 2,
        "base64 {} should be < half of canonical {}",
        base64_encoded.len(),
        canonical.len()
    );

    // Round-trip.
    let back = IpcMessage::decode_json_base64(&base64_encoded).unwrap();
    assert_eq!(back, msg);
}

// --- #lzspecintern ---

#[cfg(any(feature = "ffi", feature = "webrtc"))]
#[test]
fn json_intern_round_trips_and_dedups_type_tags() {
    // Intern wins at scale: many nodes sharing few type tags. The per-tag string
    // cost is paid once in the intern table instead of N times inline.
    let nodes: Vec<NodeSnapshot> = (0..64)
        .map(|i| {
            NodeSnapshot::payload(
                NodeId(i + 1),
                if i % 2 == 0 { "alpha" } else { "beta" },
                vec![i as u8],
            )
        })
        .collect();
    let roots: Vec<NodeId> = nodes.iter().map(|n| n.node).collect();
    let snapshot = Snapshot::new(1, nodes, vec![], roots);
    let msg = IpcMessage::Snapshot(snapshot);

    let canonical = serde_json::to_vec(&msg).unwrap();
    let interned = msg.encode_json_intern().unwrap();
    let interned_str = String::from_utf8(interned.clone()).unwrap();

    assert!(
        interned_str.contains("\"intern\""),
        "intern table should be present"
    );
    assert!(
        interned.len() < canonical.len(),
        "interned {} should be < canonical {}",
        interned.len(),
        canonical.len()
    );

    let back = IpcMessage::decode_json_intern(&interned).unwrap();
    assert_eq!(back, msg);
}

#[test]
fn shm_blob_arena_round_trips_payload_by_descriptor() {
    let mut arena = ShmBlobArena::with_capacity(SHM_BLOB_HEADER_LEN + 128).unwrap();
    let blob = arena.write_blob(12, b"large context pack").unwrap();

    assert_eq!(arena.read_blob(blob).unwrap(), b"large context pack");
    assert_eq!(blob.epoch, 12);
    assert_eq!(blob.len, "large context pack".len() as u64);
}

#[test]
fn shm_blob_arena_rejects_oversized_payload() {
    let mut arena = ShmBlobArena::with_capacity(SHM_BLOB_HEADER_LEN + 4).unwrap();
    let err = arena.write_blob(1, b"12345").unwrap_err();

    assert_eq!(err, ShmBlobArenaError::BlobTooLarge { len: 5, max_len: 4 });
}

#[test]
fn shm_blob_arena_wrap_rejects_stale_descriptor() {
    let mut arena = ShmBlobArena::with_capacity((SHM_BLOB_HEADER_LEN * 2) + 8).unwrap();
    let old = arena.write_blob(1, b"old").unwrap();
    let _middle = arena.write_blob(2, b"abcd").unwrap();
    let _new = arena.write_blob(3, b"new").unwrap();

    let err = arena.read_blob(old).unwrap_err();
    assert!(matches!(
        err,
        ShmBlobArenaError::DescriptorMismatch {
            field: "generation"
        } | ShmBlobArenaError::DescriptorMismatch { field: "checksum" }
    ));
}

#[test]
fn shm_blob_arena_rejects_torn_payload() {
    let mut arena = ShmBlobArena::with_capacity(SHM_BLOB_HEADER_LEN + 32).unwrap();
    let blob = arena.write_blob(4, b"payload").unwrap();
    let payload_offset = blob.offset as usize + SHM_BLOB_HEADER_LEN;
    arena.bytes_mut()[payload_offset] ^= 0xff;

    let err = arena.read_blob(blob).unwrap_err();
    assert!(matches!(err, ShmBlobArenaError::ChecksumMismatch { .. }));
}

#[test]
fn ipc_messages_can_reference_shared_blobs() {
    let mut arena = ShmBlobArena::with_capacity(SHM_BLOB_HEADER_LEN + 128).unwrap();
    let blob = arena.write_blob(9, b"large slot value").unwrap();
    let snapshot = Snapshot::new(
        9,
        vec![NodeSnapshot::shared_blob(NodeId(7), "text/plain", blob)],
        vec![],
        vec![NodeId(7)],
    );
    let delta = Delta::next(9, vec![DeltaOp::slot_value_blob(NodeId(7), blob)]);

    let snapshot_json = serde_json::to_string(&IpcMessage::Snapshot(snapshot.clone())).unwrap();
    let delta_json = serde_json::to_string(&IpcMessage::Delta(delta.clone())).unwrap();

    assert_eq!(
        serde_json::from_str::<IpcMessage>(&snapshot_json).unwrap(),
        IpcMessage::Snapshot(snapshot)
    );
    assert_eq!(
        serde_json::from_str::<IpcMessage>(&delta_json).unwrap(),
        IpcMessage::Delta(delta)
    );
    assert_eq!(arena.read_blob(blob).unwrap(), b"large slot value");
}

#[test]
fn ipc_message_bytes_are_channel_agnostic_payloads() {
    let message = IpcMessage::Delta(Delta::next(
        15,
        vec![
            DeltaOp::cell_set(NodeId(1), b"cell".to_vec()),
            DeltaOp::slot_value(NodeId(2), b"slot".to_vec()),
        ],
    ));

    let websocket_text_frame = serde_json::to_string(&message).unwrap();
    let webrtc_data_frame = websocket_text_frame.as_bytes().to_vec();
    let ffi_owned_buffer = webrtc_data_frame.clone();

    assert_eq!(
        serde_json::from_str::<IpcMessage>(&websocket_text_frame).unwrap(),
        message
    );
    assert_eq!(
        serde_json::from_slice::<IpcMessage>(&webrtc_data_frame).unwrap(),
        message
    );
    assert_eq!(
        serde_json::from_slice::<IpcMessage>(&ffi_owned_buffer).unwrap(),
        message
    );
}

#[test]
fn node_key_validates_path_bounds() {
    assert!(NodeKey::new("scores/alice").is_ok());
    assert_eq!(NodeKey::new("").unwrap_err(), NodeKeyError::Empty);
    assert_eq!(
        NodeKey::new("a//b").unwrap_err(),
        NodeKeyError::EmptySegment
    );
    assert_eq!(
        NodeKey::new("/leading").unwrap_err(),
        NodeKeyError::EmptySegment
    );
    let too_many = vec!["s"; NODE_KEY_MAX_SEGMENTS + 1].join("/");
    assert!(matches!(
        NodeKey::new(too_many).unwrap_err(),
        NodeKeyError::TooManySegments { .. }
    ));
    let too_long = "x".repeat(2000);
    assert!(matches!(
        NodeKey::new(too_long).unwrap_err(),
        NodeKeyError::TooLong { .. }
    ));
}

#[test]
fn node_key_segments_round_trip() {
    let key = NodeKey::from_segments(["outer", "k1", "inner", "k2"]).unwrap();
    assert_eq!(key.as_str(), "outer/k1/inner/k2");
    assert_eq!(
        key.segments().collect::<Vec<_>>(),
        vec!["outer", "k1", "inner", "k2"]
    );
}

#[test]
fn keyed_node_round_trips_through_json() {
    let key = NodeKey::new("scores/alice").unwrap();
    let snapshot = Snapshot::new(
        1,
        vec![NodeSnapshot::payload(NodeId(1), "i32", vec![1]).with_key(key.clone())],
        vec![],
        vec![NodeId(1)],
    );
    let message = IpcMessage::Snapshot(snapshot);
    let json = serde_json::to_string(&message).unwrap();
    assert!(json.contains("scores/alice"));
    assert_eq!(
        serde_json::from_str::<IpcMessage>(&json).unwrap(),
        message,
        "keyed snapshot must round-trip through JSON"
    );
}

#[test]
fn unkeyed_node_omits_key_in_json() {
    // Cross-language guarantee: a `None` key is omitted from self-describing
    // wire (JSON), so pre-`key` decoders and existing conformance fixtures
    // round-trip unchanged.
    let snapshot = Snapshot::new(
        1,
        vec![NodeSnapshot::payload(NodeId(1), "i32", vec![1])],
        vec![],
        vec![NodeId(1)],
    );
    let message = IpcMessage::Snapshot(snapshot);
    let json = serde_json::to_string(&message).unwrap();
    assert!(
        !json.contains("\"key\""),
        "unkeyed node must omit the key field in JSON: {json}"
    );

    // A keyed NodeAdd in a delta omits its key when None, too.
    let delta = Delta::next(
        1,
        vec![DeltaOp::NodeAdd {
            node: NodeId(2),
            type_tag: "i32".into(),
            state: NodeState::Payload(vec![2]),
            key: None,
        }],
    );
    let delta_json = serde_json::to_string(&IpcMessage::Delta(delta)).unwrap();
    assert!(
        !delta_json.contains("\"key\""),
        "unkeyed NodeAdd must omit the key field in JSON: {delta_json}"
    );
}

#[test]
fn node_with_absent_key_decodes_to_none() {
    // Backward-compat: a node serialized before `key` existed (no `key` field)
    // still decodes, with `key` defaulting to `None`.
    let wire = r#"{"Snapshot":{"epoch":1,"nodes":[{"node":1,"type_tag":"i32","state":{"Payload":[1]}}],"edges":[],"roots":[1]}}"#;
    let IpcMessage::Snapshot(snapshot) = serde_json::from_str::<IpcMessage>(wire).unwrap() else {
        panic!("expected snapshot");
    };
    assert_eq!(snapshot.nodes[0].key, None);
}

#[test]
fn key_index_survives_nodeid_churn() {
    let key = NodeKey::new("scores/alice").unwrap();
    let mut index = KeyIndex::new();

    // Initial snapshot binds the key to NodeId(1).
    let snapshot = Snapshot::new(
        1,
        vec![NodeSnapshot::payload(NodeId(1), "i32", vec![1]).with_key(key.clone())],
        vec![],
        vec![NodeId(1)],
    );
    index.ingest_snapshot(&snapshot);
    assert_eq!(index.node_for_key(&key), Some(NodeId(1)));
    assert_eq!(index.key_for_node(NodeId(1)), Some(&key));

    // Entry is removed and re-added under a fresh NodeId(2).
    let delta = Delta::next(
        1,
        vec![
            DeltaOp::NodeRemove { node: NodeId(1) },
            DeltaOp::NodeAdd {
                node: NodeId(2),
                type_tag: "i32".into(),
                state: NodeState::Payload(vec![2]),
                key: Some(key.clone()),
            },
        ],
    );
    index.apply_delta(&delta);

    // The key-expressed subscription stays valid; the old NodeId is gone.
    assert_eq!(index.node_for_key(&key), Some(NodeId(2)));
    assert_eq!(index.key_for_node(NodeId(1)), None);
    assert_eq!(index.key_for_node(NodeId(2)), Some(&key));
    assert_eq!(index.len(), 1);
}

#[cfg(feature = "ipc-binary")]
mod binary {
    use lazily::{
        CrdtOp, CrdtSync, DecodeError, Delta, DeltaOp, EdgeSnapshot, IpcMessage, NodeId, NodeKey,
        NodeSnapshot, Snapshot, WireStamp,
    };

    #[test]
    fn ipc_message_binary_round_trip_snapshot() {
        let snapshot = Snapshot::new(
            7,
            vec![
                NodeSnapshot::payload(NodeId(1), "i32", vec![1, 2, 3]),
                NodeSnapshot::opaque(NodeId(2), "opaque-type"),
            ],
            vec![EdgeSnapshot::new(NodeId(2), NodeId(1))],
            vec![NodeId(1), NodeId(2)],
        );
        let message = IpcMessage::Snapshot(snapshot.clone());

        let encoded = message.encode_binary().unwrap();
        let decoded = IpcMessage::decode_binary(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_binary_round_trip_delta() {
        let delta = Delta::next(
            3,
            vec![
                DeltaOp::cell_set(NodeId(1), vec![10, 20]),
                DeltaOp::slot_value(NodeId(2), vec![30, 40]),
                DeltaOp::invalidate(NodeId(3)),
            ],
        );
        let message = IpcMessage::Delta(delta.clone());

        let encoded = message.encode_binary().unwrap();
        let decoded = IpcMessage::decode_binary(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_binary_round_trips_keyed_and_unkeyed_nodes() {
        // Postcard is positional/non-self-describing: the optional `key` must
        // round-trip for both the `None` (unkeyed) and `Some` (keyed) node in
        // the same message.
        let key = NodeKey::new("scores/alice").unwrap();
        let snapshot = Snapshot::new(
            7,
            vec![
                NodeSnapshot::payload(NodeId(1), "i32", vec![1]).with_key(key),
                NodeSnapshot::opaque(NodeId(2), "opaque-type"),
            ],
            vec![],
            vec![NodeId(1), NodeId(2)],
        );
        let message = IpcMessage::Snapshot(snapshot);

        let encoded = message.encode_binary().unwrap();
        let decoded = IpcMessage::decode_binary(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_binary_round_trips_crdt_sync() {
        // Postcard is positional: the CrdtSync frontier, the optional NodeKey,
        // and both keyed and unkeyed ops must survive the binary round-trip.
        let sync = CrdtSync::new(
            vec![(
                1,
                WireStamp {
                    wall_time: 200,
                    logical: 0,
                    peer: 1,
                },
            )],
            vec![
                CrdtOp::new(
                    NodeId(1),
                    WireStamp {
                        wall_time: 200,
                        logical: 0,
                        peer: 1,
                    },
                    vec![9],
                ),
                CrdtOp::keyed(
                    NodeId(2),
                    NodeKey::new("scores/alice").unwrap(),
                    WireStamp {
                        wall_time: 180,
                        logical: 1,
                        peer: 2,
                    },
                    vec![8, 7],
                ),
            ],
        );
        let message = IpcMessage::CrdtSync(sync);

        let encoded = message.encode_binary().unwrap();
        let decoded = IpcMessage::decode_binary(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_binary_rejects_invalid_bytes() {
        let result = IpcMessage::decode_binary(b"garbage");
        assert!(matches!(result, Err(DecodeError::Binary(_))));
    }

    #[test]
    fn ipc_message_binary_is_smaller_than_json() {
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

#[cfg(feature = "ipc-msgpack")]
mod msgpack {
    use lazily::{
        CrdtOp, CrdtSync, DecodeError, Delta, DeltaOp, EdgeSnapshot, EncodeError, IpcCodec,
        IpcMessage, NodeId, NodeKey, NodeSnapshot, Snapshot, WireStamp,
    };

    #[test]
    fn ipc_message_msgpack_round_trips_snapshot() {
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

        let encoded = message.encode_msgpack().unwrap();
        let decoded = IpcMessage::decode_msgpack(&encoded).unwrap();

        assert_eq!(decoded, message);
        assert_eq!(IpcCodec::MessagePack.name(), "msgpack");
        assert_eq!(IpcCodec::MessagePack.decode(&encoded).unwrap(), message);
        assert!(serde_json::from_slice::<IpcMessage>(&encoded).is_err());
    }

    #[test]
    fn ipc_message_msgpack_round_trips_delta() {
        // Pin the Delta variant (all DeltaOp kinds) across the cross-language
        // binary default codec, matching the postcard coverage.
        let delta = Delta::next(
            3,
            vec![
                DeltaOp::cell_set(NodeId(1), vec![10, 20]),
                DeltaOp::slot_value(NodeId(2), vec![30, 40]),
                DeltaOp::invalidate(NodeId(3)),
            ],
        );
        let message = IpcMessage::Delta(delta);

        let encoded = message.encode_msgpack().unwrap();
        let decoded = IpcMessage::decode_msgpack(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_msgpack_round_trips_keyed_and_unkeyed_nodes() {
        // MessagePack is self-describing (`to_vec_named`): the optional `key`
        // is omitted when absent, so both the keyed and unkeyed node must
        // survive the round-trip — the omit-when-absent evolution rule.
        let key = NodeKey::new("scores/alice").unwrap();
        let snapshot = Snapshot::new(
            7,
            vec![
                NodeSnapshot::payload(NodeId(1), "i32", vec![1]).with_key(key),
                NodeSnapshot::opaque(NodeId(2), "opaque-type"),
            ],
            vec![],
            vec![NodeId(1), NodeId(2)],
        );
        let message = IpcMessage::Snapshot(snapshot);

        let encoded = message.encode_msgpack().unwrap();
        let decoded = IpcMessage::decode_msgpack(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_msgpack_round_trips_crdt_sync() {
        // The CrdtSync anti-entropy frame is the third IpcMessage variant; the
        // spec promises it round-trips across all three codecs (JSON,
        // MessagePack, postcard). Pin the MessagePack leg: the frontier, the
        // optional NodeKey, and both keyed and unkeyed ops.
        let sync = CrdtSync::new(
            vec![(
                1,
                WireStamp {
                    wall_time: 200,
                    logical: 0,
                    peer: 1,
                },
            )],
            vec![
                CrdtOp::new(
                    NodeId(1),
                    WireStamp {
                        wall_time: 200,
                        logical: 0,
                        peer: 1,
                    },
                    vec![9],
                ),
                CrdtOp::keyed(
                    NodeId(2),
                    NodeKey::new("scores/alice").unwrap(),
                    WireStamp {
                        wall_time: 180,
                        logical: 1,
                        peer: 2,
                    },
                    vec![8, 7],
                ),
            ],
        );
        let message = IpcMessage::CrdtSync(sync);

        let encoded = message.encode_msgpack().unwrap();
        let decoded = IpcMessage::decode_msgpack(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_msgpack_is_smaller_than_json() {
        // MessagePack is the negotiated cross-language binary default; it must
        // beat the canonical JSON codec on the wire (JSON encodes blob bytes as
        // arrays of integers — see protocol.md § IpcValue).
        let snapshot = Snapshot::new(
            42,
            vec![NodeSnapshot::payload(NodeId(1), "i32", vec![1, 2, 3, 4])],
            vec![EdgeSnapshot::new(NodeId(1), NodeId(2))],
            vec![NodeId(1)],
        );
        let message = IpcMessage::Snapshot(snapshot);

        let json_len = serde_json::to_vec(&message).unwrap().len();
        let msgpack_len = message.encode_msgpack().unwrap().len();

        assert!(
            msgpack_len < json_len,
            "msgpack ({msgpack_len}) should be smaller than json ({json_len})"
        );
    }

    #[test]
    fn ipc_message_msgpack_rejects_invalid_bytes() {
        let result = IpcMessage::decode_msgpack(b"garbage");
        assert!(matches!(result, Err(DecodeError::Msgpack(_))));
    }

    #[test]
    fn encode_decode_error_implement_display() {
        let decode_err = IpcMessage::decode_msgpack(b"garbage").unwrap_err();
        let _ = std::format!("{}", decode_err);

        let encode_err =
            EncodeError::Msgpack(rmp_serde::to_vec_named(&failing_serialize()).unwrap_err());
        let _ = std::format!("{}", encode_err);
    }

    fn failing_serialize() -> impl serde::Serialize {
        struct Failing;

        impl serde::Serialize for Failing {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("expected failure"))
            }
        }

        Failing
    }
}

#[cfg(feature = "ffi")]
mod json_codec {
    use lazily::{
        DecodeError, EdgeSnapshot, EncodeError, IpcMessage, NodeId, NodeSnapshot, Snapshot,
    };

    #[test]
    fn ipc_message_json_round_trip_snapshot() {
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

        let encoded = message.encode_json().unwrap();
        let decoded = IpcMessage::decode_json(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn ipc_message_json_rejects_invalid_bytes() {
        let result = IpcMessage::decode_json(b"not json");
        assert!(matches!(result, Err(DecodeError::Json(_))));
    }

    #[test]
    fn encode_decode_error_implement_display() {
        let decode_err = IpcMessage::decode_json(b"not json").unwrap_err();
        let _ = std::format!("{}", decode_err);

        let encode_err = EncodeError::Json(serde_json::from_str::<()>("bad").unwrap_err());
        let _ = std::format!("{}", encode_err);
    }
}

// ---------------------------------------------------------------------------
// Capability negotiation (protocol.md § Capability Negotiation)
// ---------------------------------------------------------------------------

mod capability_handshake {
    use super::*;

    #[test]
    fn new_sets_protocol_defaults() {
        let hs = CapabilityHandshake::new(PEER_A, "session-1");
        assert_eq!(hs.protocol_id, "lazily-ipc");
        assert_eq!(hs.protocol_major_version, 1);
        assert_eq!(hs.codec, "json");
        assert_eq!(hs.max_frame_size, 1_048_576);
        assert!(!hs.fragmentation_supported);
        assert!(hs.ordered_reliable);
        assert_eq!(hs.peer_id, PEER_A);
        assert_eq!(hs.session_id, "session-1");
        assert!(hs.features.is_empty());
    }

    #[test]
    fn builders_configure_fields() {
        let hs = CapabilityHandshake::new(PEER_A, "s")
            .with_codec("msgpack")
            .with_max_frame_size(2_097_152)
            .with_fragmentation(true)
            .with_features(["shared-blob", "signaling-relay"]);
        assert_eq!(hs.codec, "msgpack");
        assert_eq!(hs.max_frame_size, 2_097_152);
        assert!(hs.fragmentation_supported);
        assert_eq!(hs.features, ["shared-blob", "signaling-relay"]);
        assert!(hs.has_feature("shared-blob"));
        assert!(!hs.has_feature("crdt-cell-plane"));
    }

    #[test]
    fn round_trips_through_serde_json() {
        let hs = CapabilityHandshake::new(PEER_B, "abc-123")
            .with_features(["shared-blob", "signaling-relay"]);

        let json = serde_json::to_string(&hs).unwrap();
        let back: CapabilityHandshake = serde_json::from_str(&json).unwrap();
        assert_eq!(back, hs);
    }

    #[test]
    fn serde_matches_protocol_wire_shape() {
        let hs = CapabilityHandshake::new(PeerId(1), "abc-123")
            .with_max_frame_size(1_048_576)
            .with_features(["shared-blob", "signaling-relay"]);

        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&hs).unwrap()).unwrap();
        assert_eq!(value["protocol_id"], "lazily-ipc");
        assert_eq!(value["protocol_major_version"], 1);
        assert_eq!(value["codec"], "json");
        assert_eq!(value["max_frame_size"], 1_048_576);
        assert_eq!(value["fragmentation_supported"], false);
        assert_eq!(value["ordered_reliable"], true);
        assert_eq!(value["peer_id"], 1);
        assert_eq!(value["session_id"], "abc-123");
        assert_eq!(value["features"][0], "shared-blob");
    }

    #[test]
    fn ordered_reliable_defaults_to_true_on_decode() {
        // A peer that omits `ordered_reliable` should default to true (the
        // protocol-mandated requirement).
        let json = r#"{
            "protocol_id": "lazily-ipc",
            "protocol_major_version": 1,
            "codec": "json",
            "max_frame_size": 1024,
            "peer_id": 5,
            "session_id": "s"
        }"#;
        let hs: CapabilityHandshake = serde_json::from_str(json).unwrap();
        assert!(hs.ordered_reliable);
        assert!(!hs.fragmentation_supported);
        assert!(hs.features.is_empty());
    }

    #[test]
    fn compatible_handshakes_pass() {
        let a = CapabilityHandshake::new(PEER_A, "s");
        let b = CapabilityHandshake::new(PEER_B, "s");
        assert!(a.is_compatible_with(&b));
        let negotiated = a.negotiate_with(&b).unwrap();
        assert_eq!(negotiated.max_frame_size, 1_048_576);
        assert!(!negotiated.fragmentation_supported);
    }

    #[test]
    fn wrong_protocol_id_fails_closed() {
        let mut a = CapabilityHandshake::new(PEER_A, "s");
        a.protocol_id = "other".to_owned();
        let b = CapabilityHandshake::new(PEER_B, "s");
        assert!(!a.is_compatible_with(&b));
    }

    #[test]
    fn major_version_mismatch_fails_closed() {
        let mut a = CapabilityHandshake::new(PEER_A, "s");
        a.protocol_major_version = 2;
        let b = CapabilityHandshake::new(PEER_B, "s");
        assert!(!a.is_compatible_with(&b));
        assert!(!b.is_compatible_with(&a));
    }

    #[test]
    fn codec_mismatch_fails_closed() {
        let a = CapabilityHandshake::new(PEER_A, "s").with_codec("json");
        let b = CapabilityHandshake::new(PEER_B, "s").with_codec("postcard");
        assert!(!a.is_compatible_with(&b));
    }

    #[test]
    fn unordered_reliable_fails_closed() {
        let mut a = CapabilityHandshake::new(PEER_A, "s");
        a.ordered_reliable = false;
        let b = CapabilityHandshake::new(PEER_B, "s");
        assert!(!a.is_compatible_with(&b));
        // Symmetric: either side relaxing ordering fails the session.
        assert!(!b.is_compatible_with(&a));
    }

    #[test]
    fn frame_limits_reconcile_and_features_remain_caller_driven() {
        let a = CapabilityHandshake::new(PEER_A, "s")
            .with_max_frame_size(16 * 1024 * 1024)
            .with_fragmentation(true)
            .with_features(["shared-blob"]);
        let b = CapabilityHandshake::new(PEER_B, "s")
            .with_max_frame_size(1024)
            .with_fragmentation(false)
            .with_features(["signaling-relay"]);
        assert!(a.is_compatible_with(&b));
        let negotiated = a.negotiate_with(&b).unwrap();
        assert_eq!(negotiated.max_frame_size, 1024);
        assert!(!negotiated.fragmentation_supported);
    }

    #[test]
    fn fragmentation_requires_both_peers() {
        let a = CapabilityHandshake::new(PEER_A, "s")
            .with_max_frame_size(4096)
            .with_fragmentation(true);
        let b = CapabilityHandshake::new(PEER_B, "s")
            .with_max_frame_size(8192)
            .with_fragmentation(true);
        let negotiated = a.negotiate_with(&b).unwrap();
        assert_eq!(negotiated.max_frame_size, 4096);
        assert!(negotiated.fragmentation_supported);
    }

    #[test]
    fn zero_frame_ceiling_fails_closed() {
        let a = CapabilityHandshake::new(PEER_A, "s").with_max_frame_size(0);
        let b = CapabilityHandshake::new(PEER_B, "s");
        let error = a.negotiate_with(&b).unwrap_err();
        assert_eq!(error.field(), "max_frame_size");
        assert!(!a.is_compatible_with(&b));
    }

    #[test]
    fn session_id_must_be_shared_and_non_empty() {
        let a = CapabilityHandshake::new(PEER_A, "graph-a");
        let other = CapabilityHandshake::new(PEER_B, "graph-b");
        let empty = CapabilityHandshake::new(PEER_B, "");
        assert_eq!(a.negotiate_with(&other).unwrap_err().field(), "session_id");
        assert_eq!(a.negotiate_with(&empty).unwrap_err().field(), "session_id");
    }

    // --- #lzspecdeltacrdt ---

    #[test]
    fn delta_since_request_round_trips() {
        let req = DeltaSinceRequest::new(vec![
            (
                1,
                WireStamp {
                    wall_time: 100,
                    logical: 2,
                    peer: 1,
                },
            ),
            (
                2,
                WireStamp {
                    wall_time: 90,
                    logical: 5,
                    peer: 2,
                },
            ),
        ]);
        let json = serde_json::to_string(&IpcMessage::DeltaSinceRequest(req.clone())).unwrap();
        assert!(json.contains("DeltaSinceRequest"));
        let back: IpcMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, IpcMessage::DeltaSinceRequest(req));
    }

    #[test]
    fn delta_since_request_is_control() {
        let req = DeltaSinceRequest::new(vec![]);
        let msg = IpcMessage::DeltaSinceRequest(req);
        assert!(msg.is_control());
    }
}

/// `#failclosedsweep`: the `backend` descriptor discriminator.
///
/// Forward compatibility for this field is carried by its ABSENCE — `backend` is
/// `#[serde(default, skip_serializing_if)]`, so a legacy descriptor that predates
/// the discriminator still decodes as `shm`. A present-but-unknown value means a
/// peer spilled the payload into a backend this build does not have, and reading
/// it as `shm` would route resolution into the shared-memory arena — the misroute
/// `zero-copy-transport.md`'s `resolve_wrong_backend` theorem forbids.
mod blob_backend_discriminator {
    use lazily::{BlobBackendKind, ShmBlobRef};

    fn descriptor_json(backend: &str) -> String {
        format!(
            r#"{{"offset":0,"len":4,"generation":1,"epoch":1,"checksum":7,"backend":"{backend}"}}"#
        )
    }

    #[test]
    fn an_unknown_backend_string_is_rejected_by_name() {
        let err = serde_json::from_str::<ShmBlobRef>(&descriptor_json("rdma"))
            .expect_err("an unknown backend must not decode");
        let rendered = err.to_string();
        assert!(
            rendered.contains("rdma"),
            "the decode error names the offending discriminator: {rendered}"
        );
    }

    #[test]
    fn the_three_known_backends_still_decode() {
        for (wire, expected) in [
            ("shm", BlobBackendKind::Shm),
            ("arrow", BlobBackendKind::Arrow),
            ("in_process", BlobBackendKind::InProcess),
        ] {
            let descriptor: ShmBlobRef = serde_json::from_str(&descriptor_json(wire))
                .unwrap_or_else(|e| panic!("`{wire}` must decode: {e}"));
            assert_eq!(descriptor.backend, expected);
        }
    }

    #[test]
    fn an_absent_backend_field_still_defaults_to_shm() {
        // The legacy descriptor form: no discriminator at all. This is the case
        // the leniency was there for, and it is handled by `#[serde(default)]`,
        // not by the parse.
        let descriptor: ShmBlobRef =
            serde_json::from_str(r#"{"offset":0,"len":4,"generation":1,"epoch":1,"checksum":7}"#)
                .expect("a legacy descriptor without `backend` still decodes");
        assert_eq!(descriptor.backend, BlobBackendKind::Shm);
    }

    #[test]
    fn the_parse_reports_the_offending_value() {
        let err: lazily::UnknownBlobBackend = "cuda_ipc".parse::<BlobBackendKind>().unwrap_err();
        assert_eq!(err.0, "cuda_ipc");
        assert!(err.to_string().contains("cuda_ipc"));
    }
}

/// `#failclosedsweep`: the `json-base64` codec's decode leniency.
///
/// `decode_byte_arrays` uses `.decode(text).ok()` and leaves the field untouched
/// when the base64 does not decode. That is not a silent substitution: the field
/// stays a string where the typed decode requires a byte sequence, so the message
/// fails to decode rather than arriving with an empty or truncated payload. This
/// test is what makes that distinguishable from an accident.
#[cfg(feature = "json-base64")]
mod json_base64_decode_leniency {
    use lazily::{EdgeSnapshot, IpcMessage, NodeId, NodeSnapshot, Snapshot};

    fn message() -> IpcMessage {
        IpcMessage::Snapshot(Snapshot::new(
            1,
            vec![NodeSnapshot::payload(NodeId(1), "bytes", vec![1, 2, 3, 4])],
            vec![EdgeSnapshot::new(NodeId(1), NodeId(1))],
            vec![NodeId(1)],
        ))
    }

    #[test]
    fn a_corrupt_base64_payload_fails_the_decode_rather_than_decoding_to_nothing() {
        let encoded = message().encode_json_base64().unwrap();
        let text = String::from_utf8(encoded).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let payload = value["Snapshot"]["nodes"][0]["state"]["Payload"]
            .as_str()
            .expect("the base64 codec wrote the payload as a string");
        assert!(!payload.is_empty());
        value["Snapshot"]["nodes"][0]["state"]["Payload"] =
            serde_json::Value::String("!!!! not base64 !!!!".to_string());

        let corrupt = serde_json::to_vec(&value).unwrap();
        let err = IpcMessage::decode_json_base64(&corrupt)
            .expect_err("a payload that is not base64 must not decode");
        let _ = err;
    }

    #[test]
    fn a_well_formed_base64_payload_still_decodes() {
        let encoded = message().encode_json_base64().unwrap();
        assert_eq!(
            IpcMessage::decode_json_base64(&encoded).expect("valid base64 decodes"),
            message()
        );
    }
}
