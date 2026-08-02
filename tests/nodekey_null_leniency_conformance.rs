#![cfg(all(feature = "ipc", feature = "ipc-msgpack"))]

//! `NodeKey` null-leniency on decode (`#lzkeynullstrict`).
//!
//! protocol.md § NodeKey said a self-describing codec OMITS an absent `key`,
//! and that a decoder seeing no `key` field treats it as absent. That settled
//! the omitted form and left an explicit `key: null` undefined — and three
//! bindings diverged there. The clause is now explicit: **omit-when-absent
//! binds the ENCODER, and a decoder MUST accept both forms as absent, refusing
//! neither and constructing a key from neither.**
//!
//! lazily-rs is the reference reading. `Option<NodeKey>` with serde's default
//! null handling already accepted both forms, and `skip_serializing_if` already
//! omitted the field on the way out — which is precisely why the null form is
//! not hypothetical for the other bindings: a serde peer that simply forgot
//! `skip_serializing_if` emits it, and this decoder reads it fine.
//!
//! The runner checks BOTH halves. Reading the null form as absent is only half
//! the rule: a binding that round-trips `null` straight back out has a correct
//! decoded value and a non-conforming encoder, so every scenario re-encodes the
//! decoded message and inspects the resulting frame for the field's presence —
//! under the scenario's OWN codec, because omit-when-absent is a per-encoder
//! decision (the `#lzmsgpackparity` defect was exactly a msgpack encoder writing
//! `key: null` while json omitted it).

mod common;

use common::{Expect, ScenarioIdSource};
use lazily::{Delta, DeltaOp, IpcMessage, Snapshot};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/codec";
const VENDORED_DIR: &str = "tests/conformance/codec";
const FIXTURE: &str = "nodekey_null_leniency.json";

fn fixture_path() -> String {
    let spec_path = format!("{SPEC_DIR}/{FIXTURE}");
    if std::path::Path::new(&spec_path).exists() {
        spec_path
    } else {
        format!("{VENDORED_DIR}/{FIXTURE}")
    }
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "hex string has an odd length");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digit"))
        .collect()
}

fn decode(scenario: &Value) -> IpcMessage {
    match scenario["codec"].as_str().expect("codec") {
        "json" => {
            let text = scenario["wire_json"].as_str().expect("wire_json is text");
            serde_json::from_str(text).unwrap_or_else(|e| panic!("json decode: {e}"))
        }
        "msgpack" => {
            let bytes = decode_hex(scenario["wire_msgpack_hex"].as_str().expect("hex"));
            IpcMessage::decode_msgpack(&bytes).unwrap_or_else(|e| panic!("msgpack decode: {e}"))
        }
        other => panic!("unknown codec {other}"),
    }
}

/// Re-encode under the scenario's own codec and read the result back
/// SCHEMA-LESSLY, so what is inspected is the field set the encoder actually
/// produced rather than a typed view that cannot distinguish absent from null.
fn reencoded_node(scenario: &Value, message: &IpcMessage) -> Value {
    let generic: Value = match scenario["codec"].as_str().expect("codec") {
        "json" => serde_json::to_value(message).expect("json encode"),
        "msgpack" => {
            let bytes = message.encode_msgpack().expect("msgpack encode");
            rmp_serde::from_slice(&bytes).expect("msgpack decodes schema-lessly")
        }
        other => panic!("unknown codec {other}"),
    };
    match scenario["field"].as_str().expect("field") {
        "snapshot" => generic["Snapshot"]["nodes"][0].clone(),
        "node_add" => generic["Delta"]["ops"][0]["NodeAdd"].clone(),
        other => panic!("unknown field {other}"),
    }
}

fn decoded_key(scenario: &Value, message: &IpcMessage) -> Option<String> {
    match (scenario["field"].as_str().expect("field"), message) {
        ("snapshot", IpcMessage::Snapshot(Snapshot { nodes, .. })) => nodes
            .first()
            .expect("one node")
            .key
            .as_ref()
            .map(|k| k.as_str().to_owned()),
        ("node_add", IpcMessage::Delta(Delta { ops, .. })) => match ops.first().expect("one op") {
            DeltaOp::NodeAdd { key, .. } => key.as_ref().map(|k| k.as_str().to_owned()),
            other => panic!("fixture declares a NodeAdd op, got {other:?}"),
        },
        (field, _) => panic!("scenario field {field} disagrees with the decoded variant"),
    }
}

#[test]
fn nodekey_null_leniency_is_replayed() {
    let path = fixture_path();
    let raw = common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let fixture: Value = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));

    assert_eq!(fixture["protocol_version"], 1, "{path}: protocol version");
    assert_eq!(fixture["kind"], "NodeKeyNullLeniency", "{path}: kind");

    let a = Expect::new(path.clone(), "assertions", &fixture["assertions"]);
    a.assert_key("required_of_binding", "MUST");
    a.assert_key_with("codecs", |v| {
        assert_eq!(v, &serde_json::json!(["json", "msgpack"]), "codecs");
    });
    a.assert_key_with("fields", |v| {
        assert_eq!(v, &serde_json::json!(["snapshot", "node_add"]), "fields");
    });
    a.assert_key_with("key_forms", |v| {
        assert_eq!(
            v,
            &serde_json::json!(["omitted", "null", "present"]),
            "key_forms"
        );
    });
    a.assert_key(
        "scenario_count",
        fixture["scenarios"].as_array().expect("scenarios").len() as u64,
    );
    a.prose("clause", "states the normative encoder/decoder split");
    a.prose("wire_encoding", "explains why the wire is text/hex");
    a.prose(
        "reencode_obligation",
        "explains the re-encode half of the rule",
    );
    a.prose("anti_vacuity", "explains why omitted/present are controls");
    a.prose("generator", "names the script that regenerates the fixture");
    a.finish();

    // Anti-vacuity, both directions. `decoded_key: null` is what a runner that
    // never decodes would report, and `reencoded_key_field_present: false` is
    // what a runner that never encodes would report — so the counters below pin
    // the two `present` forms per codec, which only a real decode can produce.
    let mut replayed = 0usize;
    let mut keys_decoded = 0usize;

    for scenario in fixture["scenarios"].as_array().expect("scenarios") {
        let id = scenario["id"].as_str().expect("id").to_owned();
        common::record_scenario(&path, &id, ScenarioIdSource::Id);
        replayed += 1;

        let message = decode(scenario);
        let key = decoded_key(scenario, &message);
        if key.is_some() {
            keys_decoded += 1;
        }
        let node = reencoded_node(scenario, &message);

        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );
        // The decode half: an omitted `key` and an explicit `key: null` must
        // both arrive as absent, and a real key must survive.
        exp.assert_key_with("decoded_key", |v| {
            let want = v.as_str().map(str::to_owned);
            assert_eq!(want, key, "{id}: decoded key");
        });
        // The encode half, invisible to every assertion above: a binding that
        // read `null` correctly and wrote it straight back out is still
        // non-conforming.
        exp.assert_key(
            "reencoded_key_field_present",
            node.get("key").is_some_and(|v| !v.is_null()),
        );
        exp.assert_key("node", node["node"].as_u64().expect("node id"));
        exp.assert_key("type_tag", node["type_tag"].as_str().expect("type tag"));
        exp.assert_key_with("payload", |v| {
            assert_eq!(v, &node["state"]["Payload"], "{id}: payload bytes");
        });
        exp.assert_key_with("epoch", |v| {
            let epoch = match &message {
                IpcMessage::Snapshot(s) => s.epoch,
                IpcMessage::Delta(d) => d.epoch,
                other => panic!("{id}: unexpected variant {other:?}"),
            };
            assert_eq!(v.as_u64(), Some(epoch), "{id}: epoch");
        });
        exp.finish();
    }

    assert_eq!(replayed, 12, "two fields x three key forms x two codecs");
    assert_eq!(
        keys_decoded, 4,
        "only the `present` scenarios carry a key; a runner reporting absent for \
         everything satisfies the null cases trivially"
    );
}
