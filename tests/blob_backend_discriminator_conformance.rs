#![cfg(all(feature = "ipc", feature = "ipc-msgpack"))]

//! Blob-backend discriminator strictness on decode (`#lzblobbackendstrict`).
//!
//! protocol.md § Shared-memory payload path splits the `backend` field's forms
//! and gives them opposite answers. **An OMITTED or NULL `backend` MUST decode
//! as `shm`** — that optionality is the forward-compatibility channel, and the
//! only one, because it carries every descriptor minted before the field
//! existed. **A PRESENT `backend` outside `{shm, arrow, in_process}` MUST be
//! rejected, naming the token, and MUST NOT be normalized**, and a `backend`
//! that is present but not a string at all MUST be rejected too — through the
//! codec's own decode-error family, so one `catch` around a decode handles both
//! refusals.
//!
//! lazily-rs is the reference reading of the strict half. `ShmBlobRef::backend`
//! is `#[serde(default, deserialize_with = …, skip_serializing_if =
//! "BlobBackendKind::is_default")]`, so neither absence form reaches the token
//! parse at all — which means every string that DOES reach
//! `BlobBackendKind::from_str` names a backend this build lacks, and reading it
//! as `Shm` would route resolution into the shared-memory arena. That is the
//! misroute `resolve_wrong_backend` (docs/zero-copy-transport.md) forbids.
//! Normalizing does not merely relax the rule: it downgrades a guarantee
//! discharged STRUCTURALLY by routing to one discharged probabilistically by a
//! 64-bit checksum.
//!
//! Fixture v2 added four shapes v1 declared or implied without carrying, and
//! this binding was not uniformly green on them. `in_process` and the
//! non-string refusal already held — `BlobBackendKind` has carried three
//! variants since the field landed in v0.25.0, and a non-string has always come
//! back as an ordinary `serde_json`/`rmp_serde` decode error. The **null form
//! did not**: `#[serde(default)]` supplies the default when the KEY is missing,
//! and a present null still reaches the enum's own `Deserialize`, where it was a
//! type error in both codecs. That is now a field-level
//! `deserialize_backend_null_as_absent`.
//!
//! The runner checks BOTH halves, for the same reason
//! `nodekey_null_leniency_conformance.rs` does. Rejecting the unknown token is
//! only the decoder half; a binding that emits `backend: "shm"` on the way out
//! has a conforming decoder and a non-conforming encoder, and every accept
//! scenario therefore re-encodes the decoded message and inspects the resulting
//! frame for the field's PRESENCE under the scenario's own codec.
//!
//! Two more assertions carry the v2 hardening, and neither is reachable from a
//! scenario count. `backends_decoded` is a SET DIFFERENCE against
//! `assertions.backends`: a binding that knew only `{shm, arrow}` passed all
//! eight v1 scenarios while implementing a smaller enum than the clause
//! declares, and only "every declared backend appeared as some accept
//! scenario's `decoded_backend`" sees that. And `frame_epoch` (9, the Delta's)
//! and `blob_epoch` (5, the descriptor's) are now separate keys asserted against
//! separate sources; v1 carried 9 in both, so a runner reading either satisfied
//! the one `expect.epoch` and the assertion could not tell them apart.
//!
//! The wire is carried as raw text (json) and hex (msgpack) because the reject
//! and null frames cannot be carried as parsed objects at all: `schemas/defs.json`
//! closes `backend` to an enum, so `"backend": "rdma"` embedded as structured
//! JSON would fail the corpus's own schema gate. The enum binds a conforming
//! ENCODER; these frames are what a DECODER must survive.

mod common;

use common::{Expect, ScenarioIdSource};
use lazily::{DecodeError, Delta, DeltaOp, IpcMessage, IpcValue, ShmBlobRef};
use serde_json::Value;
use std::collections::BTreeSet;

const SPEC_DIR: common::SpecDir = common::SpecDir("codec");
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

/// A refusal, carried as the codec's OWN decode-error type rather than as a
/// string.
///
/// `expect.rejection_is_decode_error` is not "an error happened" — it is "the
/// error arrived through the family a caller already guards a decode with". A
/// `String` erases exactly the fact under test, so the type survives to the
/// assertion site.
enum Refusal {
    Json(serde_json::Error),
    Msgpack(DecodeError),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "{e}"),
            Self::Msgpack(e) => write!(f, "{e}"),
        }
    }
}

impl Refusal {
    /// Whether this refusal is a *data* refusal from the typed layer — the
    /// codec's documented decode-error family — rather than a malformed-frame
    /// or I/O failure that would have refused any frame at all.
    ///
    /// For json that is `Category::Data` directly. `lazily::DecodeError` has no
    /// equivalent classifier, so the caller supplies the evidence: the same
    /// bytes parsed schema-lessly under the same codec, which proves the
    /// container was well-formed and the refusal therefore came from the typed
    /// decode.
    fn is_codec_decode_error(&self, container_parsed: bool) -> bool {
        container_parsed
            && match self {
                Self::Json(e) => e.classify() == serde_json::error::Category::Data,
                // The crate's own documented family for this codec:
                // `IpcMessage::decode_msgpack` hands back `DecodeError`, and the
                // refusal must be its msgpack arm rather than, say, a JSON one
                // reached through some fallback path. What the container check
                // adds is that the bytes were a valid msgpack frame, so this is
                // the schema refusing them rather than the reader failing.
                Self::Msgpack(e) => matches!(e, DecodeError::Msgpack(_)),
            }
    }
}

/// Decode a scenario's wire frame with the codec it names, from the RAW form.
///
/// The decode runs inside `catch_unwind` because the obligation includes *how*
/// a refusal arrives: a panic refuses the frame too, and refuses it past every
/// handler a caller wrapped the decode in. Unwinding here turns that into a
/// named test failure rather than an aborted binary.
fn decode(scenario: &Value, id: &str, exp: &Expect) -> Result<IpcMessage, Refusal> {
    let codec = scenario["codec"].as_str().expect("codec");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match codec {
        "json" => {
            let text = scenario["wire_json"].as_str().expect("wire_json is text");
            exp.assert_key("wire_input_fnv1a64", common::fnv1a64_hex(text.as_bytes()));
            serde_json::from_str(text).map_err(Refusal::Json)
        }
        "msgpack" => {
            let hex = scenario["wire_msgpack_hex"]
                .as_str()
                .expect("wire_msgpack_hex is text");
            let bytes = decode_hex(hex);
            exp.assert_key("wire_input_fnv1a64", common::fnv1a64_hex(&bytes));
            IpcMessage::decode_msgpack(&bytes).map_err(Refusal::Msgpack)
        }
        other => panic!("unknown codec {other}"),
    }));
    match outcome {
        Ok(result) => result,
        Err(_) => panic!(
            "{id}: the decode PANICKED. A refusal must arrive as the codec's decode error, \
             not as an unwind — a panic fails past every handler a caller wrapped the decode in"
        ),
    }
}

/// The wire frame parsed SCHEMA-LESSLY under its own codec: no `ShmBlobRef`, no
/// enum, just the shape the bytes actually carry.
///
/// Two jobs. It proves the container is well-formed, so a refusal of the typed
/// decode is the SCHEMA refusing the frame rather than the reader failing to
/// read it. And it exposes the `backend` slot's real wire shape, which is what
/// lets `backend_form` be checked against the bytes instead of being taken on
/// the fixture's word.
fn wire_shape(scenario: &Value) -> Option<Value> {
    match scenario["codec"].as_str().expect("codec") {
        "json" => serde_json::from_str(scenario["wire_json"].as_str().expect("wire_json")).ok(),
        "msgpack" => {
            let bytes = decode_hex(scenario["wire_msgpack_hex"].as_str().expect("hex"));
            rmp_serde::from_slice(&bytes).ok()
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

/// The FRAME's epoch, which orders deltas — a different fact from the
/// descriptor's, which names the arena incarnation the blob was written into.
/// v1 carried 9 in both and one `expect.epoch` could not tell the two readings
/// apart.
fn frame_epoch(id: &str, message: &IpcMessage) -> u64 {
    let IpcMessage::Delta(Delta { epoch, .. }) = message else {
        panic!("{id}: fixture declares the Delta variant");
    };
    *epoch
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

fn string_set(v: &Value, what: &str) -> BTreeSet<String> {
    v.as_array()
        .unwrap_or_else(|| panic!("{what} is an array"))
        .iter()
        .map(|x| {
            x.as_str()
                .unwrap_or_else(|| panic!("{what} holds strings"))
                .to_owned()
        })
        .collect()
}

#[test]
fn blob_backend_discriminator_is_replayed() {
    let path = fixture_path();
    let raw = common::spec_read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let fixture: Value = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"));

    assert_eq!(fixture["protocol_version"], 1, "{path}: protocol version");
    assert_eq!(fixture["kind"], "BlobBackendDiscriminator", "{path}: kind");

    // Fixture-scoped, because the paragraphs in `assertions` are discharged by
    // per-scenario `expect` keys asserted long after that block drops
    // (`#lzprosekeyconvention`). The guard fails the run if `verify_prose`
    // below is never reached.
    let _prose = common::ProseLedger::open(&path);

    // Anti-vacuity counters, one per way this runner could report green having
    // proved less than the clause. `accepted`/`rejected` split the corpus, so a
    // decoder that refused everything (or accepted everything) fails here even
    // if every surviving per-scenario assertion happened to hold. `arrow_seen`
    // and `in_process_seen` pin the scenarios that force the field to actually
    // be READ; `emitted_backend` pins the ENCODER half, false for shm and true
    // for the other two, so round-tripping whatever arrived cannot satisfy both;
    // `null_read_as_shm` pins the leniency separately from the omitted form,
    // which `serde(default)` already satisfied; `epochs_distinct` pins that the
    // two epoch keys really are two facts.
    let mut replayed = 0usize;
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    let mut arrow_seen = 0usize;
    let mut in_process_seen = 0usize;
    let mut emitted_backend = 0usize;
    let mut null_read_as_shm = 0usize;
    let mut epochs_distinct = 0usize;
    let mut backends_decoded: BTreeSet<String> = BTreeSet::new();
    let mut forms_seen: BTreeSet<String> = BTreeSet::new();
    let mut rejection_kinds_seen: BTreeSet<String> = BTreeSet::new();
    let mut codecs_replayed: BTreeSet<String> = BTreeSet::new();
    let mut outcomes_replayed: BTreeSet<String> = BTreeSet::new();

    for scenario in fixture["scenarios"].as_array().expect("scenarios") {
        let id = scenario["id"].as_str().expect("id").to_owned();
        common::record_scenario(&path, &id, ScenarioIdSource::Id);
        replayed += 1;

        let outcome = scenario["outcome"].as_str().expect("outcome");
        let backend_form = scenario["backend_form"].as_str().expect("backend_form");
        forms_seen.insert(backend_form.to_owned());
        codecs_replayed.insert(scenario["codec"].as_str().expect("codec").to_owned());
        outcomes_replayed.insert(outcome.to_owned());
        assert_eq!(
            scenario["variant"].as_str().expect("variant"),
            "Delta",
            "{id}: this runner reads the Delta variant"
        );

        // v1's `expect.epoch` was REMOVED rather than redefined, so a runner
        // still reading it fails loudly instead of silently reading whichever
        // epoch it happened to reach. The `Expect` guard already fails on an
        // unconsumed key; this says why.
        assert!(
            scenario["expect"].get("epoch").is_none(),
            "{id}: `expect.epoch` is gone in fixture v2 — the frame's epoch and the \
             descriptor's are `frame_epoch` and `blob_epoch`, and they are different numbers"
        );

        // The bytes' own view of the `backend` slot, so `backend_form` is
        // checked against the wire rather than believed.
        let shape = wire_shape(scenario);
        let slot = shape
            .as_ref()
            .map(|v| v["Delta"]["ops"][0]["SlotValue"]["payload"]["SharedBlob"].clone());
        if let Some(blob_shape) = &slot {
            let entry = blob_shape.get("backend");
            match backend_form {
                "omitted" => assert!(
                    entry.is_none(),
                    "{id}: `backend_form: omitted` but the wire carries a `backend` entry"
                ),
                "null" => assert_eq!(
                    entry,
                    Some(&Value::Null),
                    "{id}: `backend_form: null` must be an explicit nil on the wire"
                ),
                "non_string" => assert!(
                    entry.is_some_and(|v| !v.is_string() && !v.is_null()),
                    "{id}: `backend_form: non_string` must carry a present non-string; got \
                     {entry:?}"
                ),
                token => assert_eq!(
                    entry.and_then(Value::as_str),
                    Some(token),
                    "{id}: the wire's `backend` token must be the scenario's own form"
                ),
            }
        }

        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );
        let result = decode(scenario, &id, &exp);

        match outcome {
            "accept" => {
                let message = result.unwrap_or_else(|e| {
                    panic!("{id}: an absent, null, or known backend must decode; got {e}")
                });
                accepted += 1;

                let blob = descriptor(&id, &message);
                match blob.backend {
                    lazily::BlobBackendKind::Arrow => arrow_seen += 1,
                    lazily::BlobBackendKind::InProcess => in_process_seen += 1,
                    lazily::BlobBackendKind::Shm => {}
                }
                backends_decoded.insert(blob.backend.as_str().to_owned());
                if backend_form == "null" && blob.backend == lazily::BlobBackendKind::Shm {
                    null_read_as_shm += 1;
                }

                // The decode half. `omitted` and `null` are the two spellings of
                // absence and must both arrive as `shm`; `arrow` and
                // `in_process` must arrive as themselves, which is what stops
                // the leniency being implemented by ignoring the field and stops
                // the vocabulary being smaller than the enum.
                exp.assert_key("decoded_backend", blob.backend.as_str());
                assert_eq!(
                    blob.backend.as_str(),
                    match backend_form {
                        "omitted" | "null" => "shm",
                        other => other,
                    },
                    "{id}: decoded backend disagrees with the scenario's wire form"
                );

                // The encode half, invisible to every assertion above: a
                // conforming encoder OMITS `backend` when it is `shm`, so a
                // pre-field descriptor round-trips byte-identically and a null
                // does not survive the trip.
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

                // Two epochs, two sources. Reading the frame's where the
                // descriptor's is expected is a real defect that v1's single
                // `expect.epoch` could not see, so each key is compared against
                // the field it names.
                let frame = frame_epoch(&id, &message);
                let want_frame = exp.assert_key_with("frame_epoch", |v| {
                    let want = v.as_u64().expect("frame_epoch is a number");
                    assert_eq!(frame, want, "{id}: the Delta frame's epoch");
                    want
                });
                let want_blob = exp.assert_key_with("blob_epoch", |v| {
                    let want = v.as_u64().expect("blob_epoch is a number");
                    assert_eq!(blob.epoch, want, "{id}: the ShmBlobRef descriptor's epoch");
                    want
                });
                assert_ne!(
                    want_frame, want_blob,
                    "{id}: the fixture must carry DIFFERENT epochs, or the two assertions \
                     above cannot tell a runner reading the frame from one reading the \
                     descriptor — that is the v1 hole"
                );
                epochs_distinct += 1;
            }
            "reject" => {
                let error = match result {
                    Ok(message) => panic!(
                        "{id}: a present `backend` that is not a known token MUST be rejected, \
                         not normalized; decoded {:?}",
                        descriptor(&id, &message).backend
                    ),
                    Err(error) => error,
                };
                rejected += 1;

                exp.assert_key("rejected", true);

                // Which refusal this is, read off the RAW WIRE's `backend` slot
                // rather than off the scenario's own `backend_form` label
                // (`#lznullformblind`). Comparing the fixture's `rejection_kind`
                // against a `match` on the fixture's own label is the closure
                // tautology: both operands come out of the same scenario, so no
                // input can fail it. The slot's wire TYPE is the real
                // discriminator — a present string is a token this build does
                // not know, a present non-string has no token to name at all —
                // and it is a read of the bytes, which the scenario cannot lie
                // about.
                let observed_kind = match slot
                    .as_ref()
                    .unwrap_or_else(|| {
                        panic!("{id}: a reject scenario's frame must still parse schema-lessly")
                    })
                    .get("backend")
                {
                    Some(Value::String(_)) => "unknown_token",
                    Some(Value::Null) | None => panic!(
                        "{id}: an absent or nil `backend` is the ACCEPT path; a reject \
                         scenario must carry a present value"
                    ),
                    Some(_) => "non_string",
                };
                let kind = exp.assert_key_with("rejection_kind", |v| {
                    let kind = v.as_str().expect("rejection_kind is a string").to_owned();
                    assert_eq!(
                        kind, observed_kind,
                        "{id}: the declared rejection kind and the type the wire's `backend` \
                         slot really carries must agree"
                    );
                    kind
                });
                rejection_kinds_seen.insert(kind.clone());

                // "The frame was refused" is not the obligation; "the frame was
                // refused through the family a caller already catches" is. A
                // refusal raised outside it fails PAST the handler, so the peer
                // never sees the error even though the frame is gone. The
                // container check is what makes this non-tautological: the bytes
                // parse schema-lessly, so it is the SCHEMA refusing them, not
                // the reader failing to read them.
                exp.assert_key(
                    "rejection_is_decode_error",
                    error.is_codec_decode_error(slot.is_some()),
                );

                // The assertion that separates "the frame was refused" from
                // "the frame was refused for the stated reason". A decoder that
                // rejected because it mis-parsed `checksum` passes a bare
                // is-error check while implementing none of the clause. Only
                // `unknown_token` carries it — a type error has no token to
                // name, and requiring the field name would pin a message format
                // no codec's native type error carries.
                let named = exp.assert_key_if_present("error_names_token", |v| {
                    let token = v.as_str().expect("error_names_token is a string");
                    assert_eq!(
                        token, backend_form,
                        "{id}: the fixture's token and its wire form must agree"
                    );
                    assert!(
                        error.to_string().contains(token),
                        "{id}: rejection must NAME the offending token `{token}`; got {error}"
                    );
                });
                assert_eq!(
                    named.is_some(),
                    kind == "unknown_token",
                    "{id}: `error_names_token` belongs to the unknown-token refusal and only \
                     to it"
                );
            }
            other => panic!("{id}: unknown outcome {other}"),
        }
        exp.finish();
    }

    // The fixture-level block is evaluated AFTER the replay, so every vocabulary
    // is compared against what the run PRODUCED rather than against a literal or
    // against the fixture's own structure (`#lznullformblind`). `backends`,
    // `backend_forms` and `rejection_kinds` were already checked as set
    // differences below the loop; folding those comparisons INTO the assertion
    // keys is what makes the keys themselves load-bearing, so a discharge naming
    // them names something falsifiable.
    //
    // `backends` in particular is the v2 assertion a scenario count cannot reach
    // (`assertions.backend_form_vocabulary`). v1 declared three backends and
    // carried scenarios for two, so a binding knowing only `{shm, arrow}`
    // rejected `in_process` — conformingly, by the letter of the clause — and
    // passed all eight scenarios while implementing a smaller enum than the
    // clause declares. Reading the discriminator and knowing the vocabulary are
    // different facts, and this is a SET DIFFERENCE, not a count.
    let a = Expect::new(path.clone(), "assertions", &fixture["assertions"]);
    a.assert_key("required_of_binding", "MUST");
    a.assert_key_with("codecs", |v| {
        assert_eq!(
            string_set(v, "codecs"),
            codecs_replayed,
            "codecs: declared vs the codecs this run really decoded through"
        );
    });
    a.assert_key_with("backends", |v| {
        assert_eq!(
            string_set(v, "backends"),
            backends_decoded,
            "every backend in `assertions.backends` must appear as the `decoded_backend` \
             of some accept scenario, and no other backend may"
        );
    });
    a.assert_key_with("backend_forms", |v| {
        assert_eq!(
            string_set(v, "backend_forms"),
            forms_seen,
            "every declared wire form must be exercised, and no undeclared one may appear"
        );
    });
    a.assert_key_with("rejection_kinds", |v| {
        assert_eq!(
            string_set(v, "rejection_kinds"),
            rejection_kinds_seen,
            "both declared rejection kinds must actually be reached"
        );
    });
    a.assert_key_with("outcomes", |v| {
        assert_eq!(
            string_set(v, "outcomes"),
            outcomes_replayed,
            "outcomes: declared vs the branches this run really dispatched into"
        );
    });
    // Against the scenarios this run REACHED, not `fixture["scenarios"].len()` —
    // the fixture compared to itself is green over a runner that decodes
    // nothing, the exact vacuity `anti_vacuity` exists to name.
    a.assert_key("scenario_count", replayed as u64);
    // The nine paragraphs the corpus declares in `assertions.prose`
    // (`#lzprosekeyconvention`). Each names the executable keys this run really
    // asserts and that genuinely carry the obligation — the free-text reasons
    // that used to sit here ("explains why the wire is text/hex") named nothing
    // and were checkable by nothing.
    a.prose_key(
        "clause",
        &["decoded_backend", "rejected", "rejection_kind", "backends"],
    );
    a.prose_key("wire_encoding", &["wire_input_fnv1a64"]);
    a.prose_key(
        "reject_obligation",
        &["error_names_token", "rejection_kind"],
    );
    a.prose_key(
        "backend_form_vocabulary",
        &["backends", "backend_forms", "decoded_backend"],
    );
    a.prose_key(
        "null_form",
        &["decoded_backend", "reencoded_backend_field_present"],
    );
    a.prose_key(
        "non_string_form",
        &["rejected", "rejection_kind", "rejection_is_decode_error"],
    );
    a.prose_key("epoch_disambiguation", &["frame_epoch", "blob_epoch"]);
    // The four controls, in order: (1) and (2) are `decoded_backend`, (3) is
    // `reencoded_backend_field_present`, (4) is the `backends` set difference.
    // `scenario_count` now counts the scenarios the run reached, so it is a
    // control rather than a restatement of the file's own length.
    a.prose_key(
        "anti_vacuity",
        &[
            "decoded_backend",
            "reencoded_backend_field_present",
            "backends",
            "scenario_count",
        ],
    );
    // PROXY. `theorem` names a Lean theorem in lazily-formal; a run can only
    // prove its consequence. `resolve_wrong_backend`'s operative consequence
    // here is that a kind is never normalized: an unknown one is `rejected`, and
    // a known one arrives as itself in `decoded_backend`.
    a.prose_key("theorem", &["decoded_backend", "rejected"]);
    a.finish();

    assert_eq!(
        replayed, 14,
        "seven backend forms x two codecs; every scenario in the fixture must be replayed"
    );
    assert_eq!(
        accepted, 10,
        "omitted + explicit shm + arrow + in_process + null, per codec, must all decode"
    );
    assert_eq!(
        rejected, 4,
        "the `rdma` and non-string scenarios are the clause; a runner that accepted them \
         proves nothing"
    );
    assert_eq!(
        arrow_seen, 2,
        "a decoder that hardcodes `Shm` and ignores the field would report zero here"
    );
    assert_eq!(
        in_process_seen, 2,
        "the third declared backend; a binding with a two-value enum would have rejected \
         these and reported zero"
    );
    assert_eq!(
        emitted_backend, 4,
        "the encoder must OMIT `backend` for shm — including the null form, which must not \
         survive the round trip — and EMIT it for arrow and in_process"
    );
    assert_eq!(
        null_read_as_shm, 2,
        "an explicit null is the ABSENT form, not a present-unknown one; `serde(default)` \
         alone does not deliver this, so it is counted apart from `omitted`"
    );
    assert_eq!(
        epochs_distinct, accepted,
        "every accept scenario must carry a frame epoch and a descriptor epoch that DIFFER"
    );

    // Every discharge above is checked here, once the whole replay has recorded
    // which keys it asserted (`#lzprosekeyconvention`).
    common::expect::verify_prose(&path);
}
