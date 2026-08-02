#![cfg(feature = "ipc")]

//! Frame-codec round-trip conformance (`#lzmsgpackparity`).
//!
//! protocol.md § Frame codecs makes `json` (reference) and `msgpack`
//! (cross-language binary default) MUST-level for every binding and requires
//! every frame to round-trip through both for all three `IpcMessage` variants.
//! That requirement lived only in prose. The four conformance rungs — was the
//! fixture OPENED, were its keys CONSUMED, were they ASSERTED, was every
//! SCENARIO replayed — all reason about fixture *content*, and content replay
//! never exercises a codec. A binding could carve out a MUST-level codec and
//! stay green on every rung.
//!
//! This runner replays `../lazily-spec/conformance/codec/*.json` *through* the
//! codec. For each scenario it decodes `wire`, **re-encodes the decoded
//! message**, decodes again, and evaluates every `expect` key against that
//! second decode. Asserting against the fixture literal would prove nothing:
//! the literal never passed through an encoder.
//!
//! The msgpack half additionally introspects the encoded bytes. `msgpack`
//! frames MUST be a map keyed by the JSON field name (§ Frame codecs) — a
//! positional encoder round-trips a value correctly and is still
//! non-conforming, so a value-only check cannot see it.

mod common;

use common::Expect;
use lazily::{CrdtSync, Delta, DeltaOp, IpcMessage, IpcValue, NodeState, Snapshot};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/codec";
const VENDORED_DIR: &str = "tests/conformance/codec";

/// The corpus copy when the sibling spec is checked out, else the vendored one
/// (standalone CI has no sibling). Same resolution order as `conformance.rs`.
fn fixture_path(name: &str) -> String {
    let spec_path = format!("{SPEC_DIR}/{name}");
    if std::path::Path::new(&spec_path).exists() {
        spec_path
    } else {
        format!("{VENDORED_DIR}/{name}")
    }
}

fn load(name: &str) -> (String, Value) {
    let path = fixture_path(name);
    let raw = common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let fixture: Value = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));
    assert_eq!(fixture["protocol_version"], 1, "{path}: protocol version");
    assert_eq!(fixture["kind"], "FrameCodecRoundTrip", "{path}: kind");
    (path, fixture)
}

/// Guard the fixture-level `assertions` block: the codec's identity and the two
/// distinct senses of "canonical" protocol.md keeps apart (`role` vs
/// `byte_canonical`). Read by nothing until now — an unread block is exactly
/// the drift `#lzassertunknownkeys` exists to catch.
fn assert_fixture_block(path: &str, fixture: &Value, codec: &str, byte_canonical: bool) {
    assert_eq!(fixture["codec"], codec, "{path}: fixture codec field");
    let a = Expect::new(path.to_owned(), "assertions", &fixture["assertions"]);
    a.assert_key("codec", codec);
    a.assert_key("self_describing", true);
    a.assert_key("byte_canonical", byte_canonical);
    a.assert_key("required_of_binding", "MUST");
    // Expectation-side dispatch, fail-closed (`#lzscenariobodyskip`). The bare
    // `else` ASSUMED the binary role for every codec that was not `json`, so a
    // third codec — or a typo — would have been asserted against the wrong
    // half of the reference-vs-byte-canonical distinction and passed.
    a.assert_key(
        "role",
        match codec {
            "json" => "reference",
            "msgpack" => "cross_language_binary_default",
            other => panic!("{path}: unknown codec `{other}` in fixture block"),
        },
    );
    a.assert_key(
        "scenario_count",
        fixture["scenarios"].as_array().expect("scenarios").len() as u64,
    );
    a.prose(
        "note",
        "documents the reference-vs-byte-canonical distinction",
    );
    a.finish();
}

fn decode_wire(path: &str, wire: &Value) -> IpcMessage {
    serde_json::from_value(wire.clone())
        .unwrap_or_else(|e| panic!("{path}: `wire` should decode as IpcMessage: {e}"))
}

fn variant_name(message: &IpcMessage) -> &'static str {
    match message {
        IpcMessage::Snapshot(_) => "Snapshot",
        IpcMessage::Delta(_) => "Delta",
        IpcMessage::CrdtSync(_) => "CrdtSync",
        IpcMessage::ResyncRequest(_) => "ResyncRequest",
        IpcMessage::OutboxAck(_) => "OutboxAck",
        IpcMessage::DeltaSinceRequest(_) => "DeltaSinceRequest",
    }
}

fn op_variant(op: &DeltaOp) -> &'static str {
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

// ---------------------------------------------------------------------------
// Per-variant value assertions — read off the ROUND-TRIPPED message
// ---------------------------------------------------------------------------

fn assert_snapshot(exp: &Expect<'_>, snap: &Snapshot) {
    exp.assert_key("epoch", snap.epoch);
    exp.assert_key("node_count", snap.nodes.len() as u64);
    exp.assert_key("edge_count", snap.edges.len() as u64);
    exp.assert_key("root_count", snap.roots.len() as u64);
    exp.assert_key("first_node_type_tag", snap.nodes[0].type_tag.clone());
    exp.assert_key(
        "first_node_payload",
        match &snap.nodes[0].state {
            NodeState::Payload(bytes) => bytes.clone(),
            other => panic!("first node should carry Payload bytes, got {other:?}"),
        },
    );
    let opaque = snap
        .nodes
        .iter()
        .find(|n| matches!(n.state, NodeState::Opaque))
        .expect("fixture pins an Opaque node");
    exp.assert_key("opaque_node_id", opaque.node.0);
    // The externally-tagged UNIT variant is the shape most likely to decay into
    // `{"Opaque": null}` under a re-encode, so name it rather than infer it.
    exp.assert_key("opaque_node_state_tag", "Opaque");
    exp.assert_key(
        "first_edge",
        vec![snap.edges[0].dependent.0, snap.edges[0].dependency.0],
    );
    exp.assert_key(
        "roots",
        snap.roots.iter().map(|r| r.0).collect::<Vec<u64>>(),
    );
}

fn assert_delta(exp: &Expect<'_>, delta: &Delta) {
    exp.assert_key("base_epoch", delta.base_epoch);
    exp.assert_key("epoch", delta.epoch);
    exp.assert_key("op_count", delta.ops.len() as u64);
    exp.assert_key(
        "op_variants",
        delta.ops.iter().map(op_variant).collect::<Vec<_>>(),
    );
    exp.assert_key(
        "first_op_payload",
        match &delta.ops[0] {
            DeltaOp::CellSet {
                payload: IpcValue::Inline(bytes),
                ..
            } => bytes.clone(),
            other => panic!("first op should be an inline CellSet, got {other:?}"),
        },
    );
    exp.assert_key(
        "node_add_type_tag",
        delta
            .ops
            .iter()
            .find_map(|op| match op {
                DeltaOp::NodeAdd { type_tag, .. } => Some(type_tag.clone()),
                _ => None,
            })
            .expect("fixture pins a NodeAdd op"),
    );
}

fn assert_crdt_sync(exp: &Expect<'_>, sync: &CrdtSync) {
    exp.assert_key("frontier_len", sync.frontier.len() as u64);
    exp.assert_key("frontier_first_peer", sync.frontier[0].0);
    exp.assert_key(
        "frontier_first_stamp_wall_time",
        sync.frontier[0].1.wall_time,
    );
    exp.assert_key("op_count", sync.ops.len() as u64);
    exp.assert_key("first_op_node", sync.ops[0].node.0);
    // Decoded-value assertion, not an encoding one: json/msgpack both WRITE
    // `key` for a `CrdtOp` (null when unset — an anti-entropy op's addressing
    // is part of its merge identity). What must survive the round trip is that
    // the decoder reads that null back as absent.
    exp.assert_key("first_op_key_absent", sync.ops[0].key.is_none());
    exp.assert_key("second_op_node", sync.ops[1].node.0);
    exp.assert_key(
        "second_op_key",
        sync.ops[1]
            .key
            .as_ref()
            .expect("second op carries a NodeKey")
            .as_str(),
    );
    exp.assert_key("second_op_stamp_peer", sync.ops[1].stamp.peer);
}

fn assert_values(exp: &Expect<'_>, message: &IpcMessage) {
    match message {
        IpcMessage::Snapshot(snap) => assert_snapshot(exp, snap),
        IpcMessage::Delta(delta) => assert_delta(exp, delta),
        IpcMessage::CrdtSync(sync) => assert_crdt_sync(exp, sync),
        other => panic!("codec fixture pins no runner for {}", variant_name(other)),
    }
}

// ---------------------------------------------------------------------------
// json — the reference codec
// ---------------------------------------------------------------------------

#[test]
fn json_frames_round_trip() {
    let (path, fixture) = load("frame_roundtrip_json.json");
    assert_fixture_block(&path, &fixture, "json", true);

    let mut replayed = 0;
    for (_, id, scenario) in common::scenarios(&path, &fixture) {
        let source = decode_wire(&path, &scenario["wire"]);
        assert_eq!(
            variant_name(&source),
            scenario["variant"].as_str().expect("variant"),
            "{id}: fixture `variant` disagrees with the decoded frame"
        );

        // Encode the DECODED message and decode the result — the fixture
        // literal is never re-asserted, so a codec that silently drops a field
        // cannot be masked by reading the input back.
        let encoded = serde_json::to_string(&source).expect("json encode");
        let round: IpcMessage = serde_json::from_str(&encoded).expect("json decode");

        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );
        exp.assert_key("round_trip_equals_source", round == source);
        assert_values(&exp, &round);
        exp.finish();
        replayed += 1;
    }
    assert_eq!(replayed, 3, "one scenario per IpcMessage variant");
}

// ---------------------------------------------------------------------------
// msgpack — the cross-language binary default
// ---------------------------------------------------------------------------

/// Field names of a msgpack map, SORTED. A MessagePack map's key order is
/// encoder-defined (§ Frame codecs), so order is not a conformance property;
/// membership is.
fn sorted_field_names(map: &Value) -> Vec<String> {
    let mut names: Vec<String> = map
        .as_object()
        .expect("msgpack frames encode structs as named-field maps, not positional arrays")
        .keys()
        .cloned()
        .collect();
    names.sort();
    names
}

#[cfg(feature = "ipc-msgpack")]
#[test]
fn msgpack_frames_round_trip() {
    let (path, fixture) = load("frame_roundtrip_msgpack.json");
    assert_fixture_block(&path, &fixture, "msgpack", false);

    let mut replayed = 0;
    for (_, id, scenario) in common::scenarios(&path, &fixture) {
        let source = decode_wire(&path, &scenario["wire"]);
        assert_eq!(
            variant_name(&source),
            scenario["variant"].as_str().expect("variant"),
            "{id}: fixture `variant` disagrees with the decoded frame"
        );

        let bytes = source.encode_msgpack().expect("msgpack encode");
        let round = IpcMessage::decode_msgpack(&bytes).expect("msgpack decode");

        // Schema-less view of the bytes actually produced. This is the only way
        // to see the named-field rule: a positional encoder passes every value
        // assertion below and fails here.
        let generic: Value = rmp_serde::from_slice(&bytes)
            .expect("msgpack frame decodes schema-lessly (named-field map)");
        let envelope = generic
            .as_object()
            .expect("IpcMessage is externally tagged: a one-entry map");
        assert_eq!(envelope.len(), 1, "{id}: external tag is a one-entry map");
        let (tag, body) = envelope.iter().next().expect("one entry");

        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );
        exp.assert_key("round_trip_equals_source", round == source);
        exp.assert_key("encoded_envelope_key", tag.clone());
        exp.assert_key("encoded_body_field_names", sorted_field_names(body));

        match tag.as_str() {
            "Snapshot" => {
                // `NodeSnapshot.key` is optional and omitted when absent in
                // self-describing codecs — the rule that lets a pre-`key`
                // decoder read a post-`key` frame. It has to hold under
                // msgpack exactly as under json.
                exp.assert_key(
                    "first_node_encoded_field_names",
                    sorted_field_names(&body["nodes"][0]),
                );
            }
            "CrdtSync" => {
                exp.assert_key(
                    "first_op_encoded_field_names",
                    sorted_field_names(&body["ops"][0]),
                );
                exp.assert_key(
                    "second_op_encoded_field_names",
                    sorted_field_names(&body["ops"][1]),
                );
            }
            // `Delta` carries no nested field-name obligation. Naming it is the
            // point: the `_ => {}` this replaced silently skipped the nested
            // assertions for ANY tag, so an encoder emitting an unexpected
            // external tag changed which assertions ran (`#lzscenariobodyskip`).
            "Delta" => {}
            other => panic!("{id}: unknown encoded envelope tag `{other}`"),
        }

        assert_values(&exp, &round);
        exp.finish();
        replayed += 1;
    }
    assert_eq!(replayed, 3, "one scenario per IpcMessage variant");
}
