#![cfg(feature = "ipc")]

//! Schema-compliance tests: lazily-rs's own serializer output validates against
//! the canonical `lazily-spec` JSON Schemas.
//!
//! lazily-rs is the reference implementation whose serde output *defines* the
//! normative wire form, and `lazily-spec` derives its schemas from it. These
//! tests close that loop in the other direction: they prove the reference's
//! `serde_json::to_value` output (including the NodeKey / CrdtSync surface the
//! conformance fixtures do not yet cover) stays valid against the schemas — a
//! regression guard symmetric to `lazily-py`'s `test_schema_compliance.py`.
//!
//! The schemas live in the sibling `../lazily-spec/schemas`. They share wire
//! primitives via absolute `$ref` into `defs.json`; rather than pull in an HTTP
//! retriever, the relevant `$defs` are composed into each message schema at test
//! time and the external refs rewritten to local ones, yielding a single
//! self-contained schema per validation.

mod common;

use std::collections::HashSet;

use lazily::{
    BlobBackendKind, CausalReceipt, CausalReceipts, CrdtOp, CrdtSync, Delta, DeltaOp, EdgeSnapshot,
    IpcMessage, NodeId, NodeKey, NodeSnapshot, NodeState, ReceiptMessage, ShmBlobRef, Snapshot,
    WireStamp,
};
use serde_json::{Map, Value};

const SPEC_SCHEMAS_DIR: &str = "../lazily-spec/schemas";
const SCHEMA_FILES: &[&str] = &[
    "defs",
    "snapshot",
    "delta",
    "distributed",
    "receipts",
    "message-passing",
];

fn sibling_schemas_present() -> bool {
    SCHEMA_FILES
        .iter()
        .all(|n| std::path::Path::new(&format!("{SPEC_SCHEMAS_DIR}/{n}.json")).exists())
}

fn load_json(name: &str) -> Value {
    let raw = std::fs::read_to_string(format!("{SPEC_SCHEMAS_DIR}/{name}.json"))
        .unwrap_or_else(|e| panic!("failed to read schema {name}.json: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse schema {name}.json: {e}"))
}

/// Compose `defs.json`'s `$defs` into `schema` and rewrite the external
/// `https://lazily.dev/schemas/defs.json#/…` refs to local `#/…` refs, so the
/// result is a single self-contained schema with no cross-document references.
fn compose(schema: Value, defs: &Value) -> Value {
    let mut composed = schema;
    if let Some(defs_defs) = defs.get("$defs") {
        let composed_defs = composed
            .as_object_mut()
            .expect("schema is an object")
            .entry("$defs")
            .or_insert_with(|| Value::Object(Map::new()));
        if let (Some(target), Some(source)) = (composed_defs.as_object_mut(), defs_defs.as_object())
        {
            for (k, v) in source {
                target.insert(k.clone(), v.clone());
            }
        }
    }
    rewrite_refs(&mut composed);
    composed
}

fn rewrite_refs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(prefix_stripped) = map
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|s| s.strip_prefix("https://lazily.dev/schemas/defs.json#"))
            {
                map.insert(
                    "$ref".to_string(),
                    Value::String(format!("#{prefix_stripped}")),
                );
            }
            for (_, child) in map.iter_mut() {
                rewrite_refs(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_refs(item);
            }
        }
        _ => {}
    }
}

/// Load a message schema composed with `defs.json` into a self-contained schema.
fn composed_schema(name: &str, defs: &Value) -> Value {
    compose(load_json(name), defs)
}

fn assert_valid(schema: &Value, instance: &Value, schema_name: &str) {
    let validator = jsonschema::Validator::new(schema)
        .unwrap_or_else(|e| panic!("{schema_name}.json is not a valid schema: {e}"));
    let errors: Vec<_> = validator.iter_errors(instance).collect();
    assert!(
        errors.is_empty(),
        "lazily-rs wire output does not validate against {schema_name}.json:\n{}",
        errors
            .iter()
            .map(|e| format!("  - {e}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn assert_valid_message(message: &IpcMessage, schema_name: &str, defs: &Value) {
    let schema = composed_schema(schema_name, defs);
    let instance = serde_json::to_value(message).expect("message serializes");
    assert_valid(&schema, &instance, schema_name);
}

// ---------------------------------------------------------------------------
// Snapshot (incl. NodeKey + every NodeState variant)
// ---------------------------------------------------------------------------

#[test]
fn snapshot_wire_validates_schema() {
    if !sibling_schemas_present() {
        eprintln!("skipping: ../lazily-spec/schemas not present (run from the monorepo)");
        return;
    }
    let defs = load_json("defs");
    let key = NodeKey::new("scores/alice").unwrap();
    let snap = Snapshot::new(
        7,
        vec![
            NodeSnapshot::payload(NodeId(1), "i32", vec![1, 2, 3]),
            NodeSnapshot::opaque(NodeId(2), "opaque-type"),
            NodeSnapshot::shared_blob(
                NodeId(3),
                "text/plain",
                ShmBlobRef {
                    offset: 0,
                    len: 16,
                    generation: 1,
                    epoch: 7,
                    checksum: 999,
                    backend: BlobBackendKind::Shm,
                },
            ),
            NodeSnapshot::payload(NodeId(4), "i32", vec![4]).with_key(key),
        ],
        vec![
            EdgeSnapshot::new(NodeId(2), NodeId(1)),
            EdgeSnapshot::new(NodeId(3), NodeId(1)),
        ],
        vec![NodeId(1), NodeId(2)],
    );
    assert_valid_message(&IpcMessage::Snapshot(snap), "snapshot", &defs);
}

// ---------------------------------------------------------------------------
// Delta — all 7 op variants + keyed NodeAdd
// ---------------------------------------------------------------------------

#[test]
fn delta_wire_validates_schema_all_ops() {
    if !sibling_schemas_present() {
        eprintln!("skipping: ../lazily-spec/schemas not present (run from the monorepo)");
        return;
    }
    let defs = load_json("defs");
    let delta = Delta::new(
        40,
        41,
        vec![
            DeltaOp::cell_set(NodeId(1), vec![10]),
            DeltaOp::slot_value(NodeId(2), vec![20]),
            DeltaOp::invalidate(NodeId(3)),
            DeltaOp::NodeAdd {
                node: NodeId(4),
                type_tag: "u64".to_string(),
                state: NodeState::Payload(vec![64]),
                key: Some(NodeKey::new("sheet/A1").unwrap()),
            },
            DeltaOp::NodeAdd {
                node: NodeId(5),
                type_tag: "u8".to_string(),
                state: NodeState::Opaque,
                key: None,
            },
            DeltaOp::NodeRemove { node: NodeId(6) },
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
    assert_valid_message(&IpcMessage::Delta(delta), "delta", &defs);
}

// ---------------------------------------------------------------------------
// CrdtSync — the third IpcMessage variant (keyed + keyless ops)
// ---------------------------------------------------------------------------

#[test]
fn crdt_sync_wire_validates_schema() {
    if !sibling_schemas_present() {
        eprintln!("skipping: ../lazily-spec/schemas not present (run from the monorepo)");
        return;
    }
    let defs = load_json("defs");
    let stamp_a = WireStamp {
        wall_time: 200,
        logical: 0,
        peer: 1,
    };
    let stamp_b = WireStamp {
        wall_time: 180,
        logical: 3,
        peer: 2,
    };
    let sync = CrdtSync::new(
        vec![(1, stamp_a), (2, stamp_b)],
        vec![
            CrdtOp::new(NodeId(1), stamp_a, vec![10, 20]),
            CrdtOp::keyed(
                NodeId(2),
                NodeKey::new("scores/alice").unwrap(),
                stamp_b,
                vec![30],
            ),
        ],
    );
    assert_valid_message(&IpcMessage::CrdtSync(sync), "distributed", &defs);
}

// ---------------------------------------------------------------------------
// CausalReceipts — generic receipt/outcome projection
// ---------------------------------------------------------------------------

#[test]
fn causal_receipts_wire_validates_schema() {
    if !sibling_schemas_present() {
        eprintln!("skipping: ../lazily-spec/schemas not present (run from the monorepo)");
        return;
    }
    let defs = load_json("defs");
    let schema = composed_schema("receipts", &defs);
    let message = ReceiptMessage::CausalReceipts(CausalReceipts::new([
        CausalReceipt::observed("receipt-observed", "patch-123", "editor", 7),
        CausalReceipt::applied("receipt-applied", "patch-123", "editor", 7)
            .with_payload_hash("sha256:abc"),
    ]));
    let instance = serde_json::to_value(message).expect("receipt message serializes");
    assert_valid(&schema, &instance, "receipts");
}

// ---------------------------------------------------------------------------
// Every conformance fixture's `wire` is schema-valid too (the fixtures are the
// shared cross-language input set; this guards the reference against them).
// ---------------------------------------------------------------------------

#[test]
fn every_ipc_conformance_fixture_wire_is_schema_valid() {
    if !sibling_schemas_present() {
        eprintln!("skipping: ../lazily-spec/schemas not present (run from the monorepo)");
        return;
    }
    let defs = load_json("defs");
    let snapshot_schema = composed_schema("snapshot", &defs);
    let delta_schema = composed_schema("delta", &defs);

    let local_dir = "tests/conformance";
    let spec_dir = "../lazily-spec/conformance";
    let mut checked = HashSet::new();
    for dir in [spec_dir, local_dir] {
        let read = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        for entry in read.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let is_ipc = name.starts_with("snapshot") || name.starts_with("delta");
            if !name.ends_with(".json") || !is_ipc {
                continue;
            }
            if !checked.insert(name.to_string()) {
                continue;
            }
            let raw = crate::common::spec_read_to_string(&path)
                .unwrap_or_else(|e| panic!("read fixture {path:?}: {e}"));
            let fixture: Value = serde_json::from_str(&raw)
                .unwrap_or_else(|e| panic!("parse fixture {path:?}: {e}"));
            let wire = fixture.get("wire").expect("fixture has `wire`");
            let schema = if name.starts_with("snapshot") {
                &snapshot_schema
            } else {
                &delta_schema
            };
            assert_valid(schema, wire, name);
        }
    }
    assert!(
        !checked.is_empty(),
        "no IPC conformance fixtures found in {local_dir} or {spec_dir}"
    );
}

// ---------------------------------------------------------------------------
// Command / RPC message plane — every CommandMessage variant validates
// ---------------------------------------------------------------------------

#[test]
fn command_plane_wire_validates_schema() {
    if !sibling_schemas_present() {
        eprintln!("skipping: ../lazily-spec/schemas not present (run from the monorepo)");
        return;
    }
    use lazily::{
        CommandCancel, CommandEvent, CommandEventKind, CommandEvents, CommandMessage,
        CommandPolicy, CommandProjectionEntry, CommandProjectionImage, CommandStatus,
        CommandSubmit, DedupePolicy, IpcValue,
    };

    let defs = load_json("defs");
    let schema = composed_schema("message-passing", &defs);

    let submit = CommandMessage::CommandSubmit(Box::new(CommandSubmit {
        command_id: "cmd-run-1".into(),
        causation_id: "cmd-run-1".into(),
        source: "vscode-plugin".into(),
        target: "project-controller".into(),
        namespace: "agent-doc".into(),
        name: "editor_route".into(),
        authority_generation: 42,
        idempotency_key: "project-root:plan.md:run".into(),
        deadline_ms: 120_000,
        policy: CommandPolicy {
            dedupe: DedupePolicy::SameIdempotencyKey,
            supersede: false,
            cancel_on_preempt: true,
        },
        payload_type: "agent-doc.editor_route.v1".into(),
        payload_hash: "sha256:abc".into(),
        payload: IpcValue::Inline(vec![123, 125]),
        required_features: vec!["causal-receipts".into()],
    }));
    assert_valid(
        &schema,
        &serde_json::to_value(&submit).unwrap(),
        "message-passing",
    );

    let cancel = CommandMessage::CommandCancel(CommandCancel {
        command_id: "cmd-run-1".into(),
        causation_id: "cancel-1".into(),
        source: "vscode-plugin".into(),
        authority_generation: 42,
        reason: Some("operator cleared run".into()),
    });
    assert_valid(
        &schema,
        &serde_json::to_value(&cancel).unwrap(),
        "message-passing",
    );

    let events = CommandMessage::CommandEvents(CommandEvents {
        events: vec![CommandEvent {
            event_id: "ev-1".into(),
            command_id: "cmd-run-1".into(),
            kind: CommandEventKind::Accepted,
            generation: 42,
            detail: Some("queued".into()),
        }],
    });
    assert_valid(
        &schema,
        &serde_json::to_value(&events).unwrap(),
        "message-passing",
    );

    let projection = CommandMessage::CommandProjection(CommandProjectionImage {
        generation: 43,
        commands: vec![CommandProjectionEntry {
            command_id: "cmd-run-1".into(),
            status: CommandStatus::Applied,
            terminal: true,
            generation: 43,
            reason: None,
            terminal_receipt_id: Some("rcpt-1".into()),
            last_event_id: Some("ev-3".into()),
        }],
    });
    assert_valid(
        &schema,
        &serde_json::to_value(&projection).unwrap(),
        "message-passing",
    );
}
