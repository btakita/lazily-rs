#![cfg(all(feature = "ipc", feature = "ipc-msgpack"))]

//! Blob-backend discriminator strictness on decode (`#lzblobbackendstrict`).
//!
//! protocol.md § Shared-memory payload path now splits the `backend` field's two
//! forms and gives them opposite answers. **An OMITTED `backend` MUST decode as
//! `shm`** — that optionality is the forward-compatibility channel, and the only
//! one, because it carries every descriptor minted before the field existed. **A
//! PRESENT `backend` outside `{shm, arrow, in_process}` MUST be rejected, naming
//! the token, and MUST NOT be normalized.**
//!
//! lazily-rs is the reference reading of the strict half. `ShmBlobRef::backend`
//! is `#[serde(default, skip_serializing_if = "BlobBackendKind::is_default")]`,
//! so an absent field never reaches the parse at all — which means every string
//! that DOES reach `BlobBackendKind::from_str` names a backend this build lacks,
//! and reading it as `Shm` would route resolution into the shared-memory arena.
//! That is the misroute `resolve_wrong_backend` (docs/zero-copy-transport.md)
//! forbids. Normalizing does not merely relax the rule: it downgrades a
//! guarantee discharged STRUCTURALLY by routing to one discharged
//! probabilistically by a 64-bit checksum.
//!
//! The runner checks BOTH halves, for the same reason
//! `nodekey_null_leniency_conformance.rs` does. Rejecting the unknown token is
//! only the decoder half; a binding that emits `backend: "shm"` on the way out
//! has a conforming decoder and a non-conforming encoder, and every accept
//! scenario therefore re-encodes the decoded message and inspects the resulting
//! frame for the field's PRESENCE under the scenario's own codec.
//!
//! The wire is carried as raw text (json) and hex (msgpack) because the reject
//! frames cannot be carried as parsed objects at all: `schemas/defs.json` closes
//! `backend` to an enum, so `"backend": "rdma"` embedded as structured JSON
//! would fail the corpus's own schema gate. The enum binds a conforming
//! ENCODER; these frames are what a DECODER must survive.

mod common;

use common::{Expect, ScenarioIdSource};
use lazily::{Delta, DeltaOp, IpcMessage, IpcValue, ShmBlobRef};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/codec";
const VENDORED_DIR: &str = "tests/conformance/codec";
const FIXTURE: &str = "blob_backend_discriminator.json";

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

/// Decode a scenario's wire frame with the codec it names, from the RAW form.
///
/// `Err` is the conforming outcome for the `rdma` scenarios and a failure for
/// every other one; the caller decides which, because that split is the clause.
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
            IpcMessage::decode_msgpack(&decode_hex(hex)).map_err(|e| e.to_string())
        }
        other => panic!("unknown codec {other}"),
    }
}

/// The descriptor under test, pulled out of the fixture's declared shape
/// (`Delta` carrying one `SlotValue` op whose payload is a `SharedBlob`).
fn descriptor(id: &str, message: &IpcMessage) -> ShmBlobRef {
    let IpcMessage::Delta(Delta { ops, .. }) = message else {
        panic!("{id}: fixture declares the Delta variant, got {message:?}");
    };
    match ops.first().expect("one op") {
        DeltaOp::SlotValue {
            payload: IpcValue::SharedBlob(blob),
            ..
        } => *blob,
        other => panic!("{id}: fixture declares a SlotValue/SharedBlob op, got {other:?}"),
    }
}

fn node_id(id: &str, message: &IpcMessage) -> u64 {
    let IpcMessage::Delta(Delta { ops, .. }) = message else {
        panic!("{id}: fixture declares the Delta variant");
    };
    match ops.first().expect("one op") {
        DeltaOp::SlotValue { node, .. } => node.0,
        other => panic!("{id}: fixture declares a SlotValue op, got {other:?}"),
    }
}

/// Re-encode under the scenario's OWN codec and read the result back
/// SCHEMA-LESSLY, so what is inspected is the field set the encoder actually
/// produced rather than a typed view that cannot distinguish absent from
/// present-and-default. Omit-when-default is a per-encoder decision — the
/// `#lzmsgpackparity` defect was exactly a msgpack encoder writing a field the
/// json encoder omitted — so the msgpack half must be inspected as msgpack.
fn reencoded_blob(scenario: &Value, id: &str, message: &IpcMessage) -> Value {
    let generic: Value = match scenario["codec"].as_str().expect("codec") {
        "json" => serde_json::to_value(message).expect("json encode"),
        "msgpack" => {
            let bytes = message.encode_msgpack().expect("msgpack encode");
            rmp_serde::from_slice(&bytes).expect("msgpack decodes schema-lessly")
        }
        other => panic!("unknown codec {other}"),
    };
    let blob = &generic["Delta"]["ops"][0]["SlotValue"]["payload"]["SharedBlob"];
    assert!(
        blob.is_object(),
        "{id}: re-encoded frame does not carry a SharedBlob descriptor: {generic}"
    );
    blob.clone()
}

#[test]
fn blob_backend_discriminator_is_replayed() {
    let path = fixture_path();
    let raw = common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let fixture: Value = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));

    assert_eq!(fixture["protocol_version"], 1, "{path}: protocol version");
    assert_eq!(fixture["kind"], "BlobBackendDiscriminator", "{path}: kind");

    let a = Expect::new(path.clone(), "assertions", &fixture["assertions"]);
    a.assert_key("required_of_binding", "MUST");
    a.assert_key_with("codecs", |v| {
        assert_eq!(v, &serde_json::json!(["json", "msgpack"]), "codecs");
    });
    a.assert_key_with("backends", |v| {
        assert_eq!(
            v,
            &serde_json::json!(["shm", "arrow", "in_process"]),
            "backends"
        );
    });
    a.assert_key_with("outcomes", |v| {
        assert_eq!(v, &serde_json::json!(["accept", "reject"]), "outcomes");
    });
    a.assert_key(
        "scenario_count",
        fixture["scenarios"].as_array().expect("scenarios").len() as u64,
    );
    a.prose("clause", "states the omitted-vs-present decoder split");
    a.prose("wire_encoding", "explains why the wire is text/hex");
    a.prose("reject_obligation", "requires the error to name the token");
    a.prose("anti_vacuity", "explains the three controls");
    a.prose("theorem", "names the routing theorem the clause discharges");
    a.prose("generator", "names the script that regenerates the fixture");
    a.finish();

    // Anti-vacuity counters, one per way this runner could report green having
    // proved less than the clause. `accepted`/`rejected` split the corpus, so a
    // decoder that refused everything (or accepted everything) fails here even
    // if every surviving per-scenario assertion happened to hold. `arrow_seen`
    // pins the scenarios that force the field to actually be READ — a decoder
    // that ignores `backend` and hardcodes `Shm` satisfies four of the six
    // accept assertions — and `emitted_backend` pins the ENCODER half, which is
    // false for shm and true for arrow, so round-tripping whatever arrived
    // cannot satisfy both.
    let mut replayed = 0usize;
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    let mut arrow_seen = 0usize;
    let mut emitted_backend = 0usize;

    for scenario in fixture["scenarios"].as_array().expect("scenarios") {
        let id = scenario["id"].as_str().expect("id").to_owned();
        common::record_scenario(&path, &id, ScenarioIdSource::Id);
        replayed += 1;

        let outcome = scenario["outcome"].as_str().expect("outcome");
        let backend_form = scenario["backend_form"].as_str().expect("backend_form");
        assert_eq!(
            scenario["variant"].as_str().expect("variant"),
            "Delta",
            "{id}: this runner reads the Delta variant"
        );

        let result = decode(scenario);
        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );

        match outcome {
            "accept" => {
                let message = result.unwrap_or_else(|e| {
                    panic!("{id}: a known backend (or an absent field) must decode; got {e}")
                });
                accepted += 1;

                let blob = descriptor(&id, &message);
                if blob.backend == lazily::BlobBackendKind::Arrow {
                    arrow_seen += 1;
                }

                // The decode half. `backend_form: omitted` must arrive as `shm`
                // via `serde(default)`; `arrow` must arrive as `arrow`, which is
                // what stops the leniency being implemented by ignoring the
                // field.
                exp.assert_key("decoded_backend", blob.backend.as_str());
                assert_eq!(
                    blob.backend.as_str(),
                    match backend_form {
                        "omitted" => "shm",
                        other => other,
                    },
                    "{id}: decoded backend disagrees with the scenario's wire form"
                );

                // The encode half, invisible to every assertion above: a
                // conforming encoder OMITS `backend` when it is `shm`, so a
                // pre-field descriptor round-trips byte-identically.
                let reencoded = reencoded_blob(scenario, &id, &message);
                let present = reencoded.get("backend").is_some();
                if present {
                    emitted_backend += 1;
                }
                exp.assert_key("reencoded_backend_field_present", present);

                exp.assert_key("node", node_id(&id, &message));
                exp.assert_key("offset", blob.offset);
                exp.assert_key("len", blob.len);
                exp.assert_key("generation", blob.generation);
                exp.assert_key("checksum", blob.checksum);
                exp.assert_key_with("epoch", |v| {
                    // The descriptor and the frame carry the same epoch in this
                    // fixture; assert against the descriptor, which is the field
                    // the clause is about.
                    assert_eq!(v.as_u64(), Some(blob.epoch), "{id}: descriptor epoch");
                });
            }
            "reject" => {
                let error = match result {
                    Ok(message) => panic!(
                        "{id}: a present `backend` outside the enum MUST be rejected, not \
                         normalized; decoded {:?}",
                        descriptor(&id, &message).backend
                    ),
                    Err(error) => error,
                };
                rejected += 1;

                exp.assert_key("rejected", true);
                // The assertion that separates "the frame was refused" from
                // "the frame was refused for the stated reason". A decoder that
                // rejected because it mis-parsed `checksum` passes a bare
                // is-error check while implementing none of the clause.
                exp.assert_key_with("error_names_token", |v| {
                    let token = v.as_str().expect("error_names_token is a string");
                    assert_eq!(
                        token,
                        scenario["backend_form"].as_str().expect("backend_form"),
                        "{id}: the fixture's token and its wire form must agree"
                    );
                    assert!(
                        error.contains(token),
                        "{id}: rejection must NAME the offending token `{token}`; got {error}"
                    );
                });
            }
            other => panic!("{id}: unknown outcome {other}"),
        }
        exp.finish();
    }

    assert_eq!(
        replayed, 8,
        "four backend forms x two codecs; every scenario in the fixture must be replayed"
    );
    assert_eq!(
        accepted, 6,
        "omitted + explicit shm + arrow, per codec, must all decode"
    );
    assert_eq!(
        rejected, 2,
        "the `rdma` scenarios are the clause; a runner that accepted them proves nothing"
    );
    assert_eq!(
        arrow_seen, 2,
        "a decoder that hardcodes `Shm` and ignores the field would report zero here"
    );
    assert_eq!(
        emitted_backend, 2,
        "the encoder must OMIT `backend` for shm and EMIT it for arrow — only the two \
         arrow scenarios may carry the field"
    );
}
