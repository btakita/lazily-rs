#![cfg(all(feature = "ipc", feature = "ipc-msgpack"))]

//! `NodeId` exact-representation bound (`#lzspecdecoderbound`).
//!
//! protocol.md § NodeId / PeerId stated the 2^53 bound as a PRODUCER
//! obligation and said nothing about what a decoder does when it receives a
//! violation. That left the receiving half undefined, which is exactly where
//! the bindings diverged. The clause is now normative: **a decoder that cannot
//! represent a received identifier exactly MUST reject the frame rather than
//! round it.**
//!
//! lazily-rs is the widest case — `NodeId` is a `u64`, so every scenario in the
//! fixture is representable and this runner asserts the `exact` branch for all
//! of them, including `u64::MAX`. That makes it the reference reading of the
//! fixture: if a scenario is *rejectable* here, the fixture is wrong about the
//! wire, not about the decoder.
//!
//! The fixture carries its wire frames as raw TEXT (json) and HEX (msgpack),
//! and its expected identifier as a decimal STRING, because the fixture is
//! itself JSON: a runner on a double-backed runtime that read a bare
//! `9007199254740993` would round the *expectation* while loading the file and
//! then pass against a decoder that rounds. Rust would not, but the runner
//! honours the same contract so every binding reads the file identically.

mod common;

use common::{Expect, ScenarioIdSource};
use lazily::{IpcMessage, NodeState};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/codec";
const VENDORED_DIR: &str = "tests/conformance/codec";
const FIXTURE: &str = "nodeid_exact_range.json";

fn fixture_path() -> String {
    let spec_path = format!("{SPEC_DIR}/{FIXTURE}");
    if std::path::Path::new(&spec_path).exists() {
        spec_path
    } else {
        format!("{VENDORED_DIR}/{FIXTURE}")
    }
}

/// Decode a scenario's wire frame with the codec it names.
///
/// `Err` is a conforming outcome for an over-range identifier and a failure for
/// an `exact` one; the caller decides which, because that is the whole
/// distinction the fixture draws.
fn decode(scenario: &Value) -> Result<IpcMessage, String> {
    match scenario["codec"].as_str().expect("codec") {
        "json" => {
            let text = scenario["wire_json"].as_str().expect("wire_json is text");
            serde_json::from_str(text).map_err(|e| e.to_string())
        }
        "msgpack" => {
            let hex = scenario["wire_msgpack_hex"]
                .as_str()
                .expect("wire_msgpack_hex is text");
            let bytes = decode_hex(hex);
            IpcMessage::decode_msgpack(&bytes).map_err(|e| e.to_string())
        }
        other => panic!("unknown codec {other}"),
    }
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "hex string has an odd length");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digit"))
        .collect()
}

#[test]
fn nodeid_exact_range_is_replayed() {
    let path = fixture_path();
    let raw = common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let fixture: Value = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));

    assert_eq!(fixture["protocol_version"], 1, "{path}: protocol version");
    assert_eq!(fixture["kind"], "NodeIdExactRange", "{path}: kind");

    // Fixture-scoped prose ledger (`#lzprosekeyconvention`): the paragraphs in
    // `assertions` are discharged by per-scenario `expect` keys asserted after
    // that block drops.
    let _prose = common::ProseLedger::open(&path);

    let a = Expect::new(path.clone(), "assertions", &fixture["assertions"]);
    a.assert_key("required_of_binding", "MUST");
    a.assert_key_with("codecs", |v| {
        assert_eq!(v, &serde_json::json!(["json", "msgpack"]), "codecs");
    });
    a.assert_key(
        "scenario_count",
        fixture["scenarios"].as_array().expect("scenarios").len() as u64,
    );
    // `outcomes` is NOT prose — the corpus does not list it. It maps a
    // vocabulary to English glosses, so the assertion is the KEY SET and the
    // parent key's own assertion discharges it (`#lzprosekeyconvention`).
    a.assert_key_with("outcomes", |v| {
        let vocabulary: Vec<&str> = v
            .as_object()
            .expect("outcomes maps each outcome token to its gloss")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            vocabulary,
            ["exact", "exact_or_reject"],
            "the outcome vocabulary this runner dispatches on"
        );
    });
    // The three declared paragraphs, each naming the executable keys that carry
    // its obligation.
    a.prose_key("clause", &["node_id_decimal", "outcome"]);
    // PROXY. `wire_encoding` is a claim about how the CORPUS carries its bytes
    // and its expectation — none of the three a JSON number — which no assertion
    // a run makes can observe. The proxy is the codec vocabulary plus
    // `node_id_decimal`, which is compared as a decimal STRING against a decode
    // of `wire_json` / `wire_msgpack_hex` rather than a re-serialized
    // pre-parsed object, and so would redden against a decoder that rounds.
    a.prose_key("wire_encoding", &["codecs", "node_id_decimal"]);
    // "the two `exact` scenarios are the control" — the control is visible as
    // the `outcome` key each scenario carries, over the full `scenario_count`,
    // with `node_id_decimal` proving the boundary value really decoded.
    a.prose_key(
        "anti_vacuity",
        &["outcome", "node_id_decimal", "scenario_count"],
    );
    a.excuse_key(
        "generator",
        "the corpus-side script that regenerates the fixture; lazily-rs replays the \
         fixture and never regenerates it, so there is nothing here to compare it against",
    );
    a.finish();

    // A binding whose identity type covers the whole u64 range must decode
    // EVERY scenario, so `exact` and `exact_or_reject` collapse to the same
    // obligation here and this counter is a real coverage assertion, not a
    // "how many did we happen to reach" tally.
    let mut replayed = 0usize;
    let mut accepted = 0usize;

    for scenario in fixture["scenarios"].as_array().expect("scenarios") {
        let id = scenario["id"].as_str().expect("id").to_owned();
        common::record_scenario(&path, &id, ScenarioIdSource::Id);
        replayed += 1;

        let expected: u64 = scenario["expect"]["node_id_decimal"]
            .as_str()
            .expect("node_id_decimal is a decimal string")
            .parse()
            .expect("decimal string parses as u64");

        let message = decode(scenario).unwrap_or_else(|e| {
            panic!(
                "{id}: lazily-rs represents the full u64 range, so this frame must decode; got {e}"
            )
        });
        accepted += 1;

        let IpcMessage::Snapshot(snapshot) = message else {
            panic!("{id}: fixture declares the Snapshot variant");
        };

        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );
        // `outcome` is consumed rather than asserted equal: it is the fixture's
        // statement about what the WHOLE corpus of bindings may do, and this
        // binding satisfies both branches by decoding.
        exp.assert_key_with("outcome", |v| {
            let outcome = v.as_str().expect("outcome");
            assert!(
                matches!(outcome, "exact" | "exact_or_reject"),
                "{id}: unknown outcome {outcome}"
            );
        });
        exp.assert_key("epoch", snapshot.epoch);
        exp.assert_key("node_count", snapshot.nodes.len() as u64);

        let node = snapshot.nodes.first().expect("one node");
        // The discriminating assertion. A decoder that rounded would produce a
        // NEIGHBOURING value that still decodes cleanly, so comparing against
        // the exact expectation is the only check that sees the substitution.
        assert_eq!(
            node.node.0, expected,
            "{id}: decoder substituted a different identifier"
        );
        exp.assert_key("node_id_decimal", node.node.0.to_string());
        exp.assert_key("type_tag", node.type_tag.to_string());
        exp.assert_key_with("payload", |v| {
            let NodeState::Payload(bytes) = &node.state else {
                panic!("{id}: fixture carries a Payload node state");
            };
            assert_eq!(
                v,
                &serde_json::json!(bytes),
                "{id}: payload bytes did not survive the decode"
            );
        });
        exp.assert_key(
            "root_id_decimal",
            snapshot.roots.first().expect("one root").0.to_string(),
        );
        exp.finish();
    }

    assert_eq!(
        replayed, 6,
        "every scenario in the fixture must be replayed"
    );
    assert_eq!(
        accepted, replayed,
        "lazily-rs is u64-wide; a refusal here means the decoder narrowed"
    );

    common::expect::verify_prose(&path);
}
