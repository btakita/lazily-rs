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
//!
//! It also carries a RAW-WIRE control (`#lznullformblind`). Every key in this
//! fixture's `expect` blocks is byte-identical across the `omitted` and `null`
//! families — `decoded_key` is null for both by design, because reading an
//! explicit null as absent IS the leniency — so post-decode the four `null`
//! scenarios are the four `omitted` ones wearing a different id, and this
//! decoder's `#[serde(default)]` collapses them on contact. Four scenarios
//! proving nothing, invisible to the manifest rung, the scenario-replay rung and
//! both assertion-key rungs at once, since an unreplayed distinction contributes
//! no unconsumed and no unasserted key. `wire_key_form` classifies the `key`
//! slot out of the `wire_json` TEXT and the `wire_msgpack_hex` BYTES before any
//! decode runs, `raw_key_form` witnesses the same slot a second time WITHOUT any
//! decoder — a text scan and a msgpack type-tag read — because the schema-less
//! classification runs on the very crates the decode under test runs on and
//! cannot see a defect it shares with them, and the fixture-level vocabularies
//! are asserted against what the run really replayed rather than against
//! literals or the fixture's own lists.

mod common;

use common::{Expect, ScenarioIdSource};
use lazily::{Delta, DeltaOp, IpcMessage, Snapshot};
use serde_json::Value;
use std::collections::BTreeSet;

const SPEC_DIR: common::SpecDir = common::SpecDir("codec");
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

fn decode(scenario: &Value, exp: &Expect) -> IpcMessage {
    match scenario["codec"].as_str().expect("codec") {
        "json" => {
            let text = scenario["wire_json"].as_str().expect("wire_json is text");
            exp.assert_key("wire_input_fnv1a64", common::fnv1a64_hex(text.as_bytes()));
            serde_json::from_str(text).unwrap_or_else(|e| panic!("json decode: {e}"))
        }
        "msgpack" => {
            let bytes = decode_hex(scenario["wire_msgpack_hex"].as_str().expect("hex"));
            exp.assert_key("wire_input_fnv1a64", common::fnv1a64_hex(&bytes));
            IpcMessage::decode_msgpack(&bytes).unwrap_or_else(|e| panic!("msgpack decode: {e}"))
        }
        other => panic!("unknown codec {other}"),
    }
}

/// The map carrying the `key` slot inside a SCHEMA-LESS frame tree, dispatching
/// on which optional-key site the scenario exercises.
///
/// Shared by the RAW-WIRE control and the RE-ENCODED inspection, so both read
/// the same slot out of the same shape. Fails closed on an unknown field
/// (`#lzscenariobodyskip`): a fixture naming a site this runner does not
/// navigate must redden rather than silently classify nothing.
fn key_site(scenario: &Value, frame: &Value, whence: &str) -> Value {
    let site = match scenario["field"].as_str().expect("field") {
        "snapshot" => &frame["Snapshot"]["nodes"][0],
        "node_add" => &frame["Delta"]["ops"][0]["NodeAdd"],
        other => panic!("unknown field {other}"),
    };
    assert!(
        site.is_object(),
        "{whence}: the `key` site is not a map: {site}"
    );
    site.clone()
}

/// Classify the `key` slot out of the RAW wire — the `wire_json` TEXT and the
/// `wire_msgpack_hex` BYTES — BEFORE any decode runs.
///
/// This control is the whole reason `wire_encoding` is dischargeable here, and
/// lazily-rs was one of the four bindings still blind without it. Every key in
/// this fixture's `expect` blocks is byte-identical for the `omitted` and `null`
/// families — `decoded_key` is null for both BY DESIGN, because reading an
/// explicit null as absent is the leniency under test — so the four `null`
/// scenarios are the four `omitted` ones wearing a different id as far as any
/// post-decode assertion can tell, and `#[serde(default)]` collapses them on
/// contact. Four scenarios proving nothing, invisible to the manifest rung, the
/// scenario-replay rung and both assertion-key rungs at once, because an
/// unreplayed distinction contributes no unconsumed and no unasserted key.
///
/// Only a read of the raw slot sees the difference: in json an absent map entry
/// versus a literal `null`, in msgpack an absent entry versus the one-byte nil
/// `0xc0`. `rmp_serde` into `serde_json::Value` preserves both — `deserialize_any`
/// visits nil as `Value::Null` and never invents the entry — so the three-way
/// split survives into the runner in each codec. The sibling blob-backend runner
/// already applies exactly this control to `backend`; the references are
/// lazily-go's `nodeKeyWireForm` and lazily-cpp's `wire_key_form`.
///
/// Fails closed: every branch is explicit and there is no defaulting arm, so an
/// unknown codec or an unclassifiable slot panics instead of being read as one
/// of the three forms.
fn wire_key_form(scenario: &Value, id: &str) -> &'static str {
    let frame: Value = match scenario["codec"].as_str().expect("codec") {
        "json" => {
            let text = scenario["wire_json"].as_str().expect("wire_json is text");
            serde_json::from_str(text)
                .unwrap_or_else(|e| panic!("{id}: wire_json is not JSON: {e}"))
        }
        "msgpack" => {
            let bytes = decode_hex(scenario["wire_msgpack_hex"].as_str().expect("hex"));
            rmp_serde::from_slice(&bytes)
                .unwrap_or_else(|e| panic!("{id}: wire_msgpack_hex is not msgpack: {e}"))
        }
        other => panic!("{id}: unknown codec {other}"),
    };
    let site = key_site(scenario, &frame, "the scenario's own wire");
    let map = site.as_object().expect("key site is a map");
    match map.get("key") {
        // No entry at all — the form a conforming encoder emits.
        None => "omitted",
        // The entry is there and holds json `null` / msgpack nil (`0xc0`).
        Some(Value::Null) => "null",
        // A real key. Anything else present is `present` by construction, and
        // the equality against the scenario's declared `key_form` below is what
        // refuses a form this vocabulary does not name.
        Some(_) => "present",
    }
}

/// MessagePack's `nil` tag — the one byte that spells an explicit `key: null`
/// on a msgpack wire, as against the field simply not being written.
const MSGPACK_NIL: u8 = 0xc0;

/// MessagePack's `fixstr` header for a three-byte string, followed by `key`:
/// the on-wire spelling of the field NAME. Four bytes, matched literally.
const MSGPACK_KEY_FIELD: [u8; 4] = [0xa3, b'k', b'e', b'y'];

/// SECOND WITNESS for the `key` slot, taken WITHOUT going through any decoder.
///
/// [`wire_key_form`] classifies through `serde_json` / `rmp_serde` — the same
/// two crates the typed decode under test runs on. That makes it a control with
/// a blind spot of its own: a defect in either crate's schema-less path corrupts
/// the control and the thing being controlled together, and the control cannot
/// see it. A control is only as trustworthy as the code path it avoids, so this
/// witness avoids the whole of it.
///
/// json is scanned as TEXT: find the field name, step over the colon, and look
/// at whether the value literally begins `null`. msgpack is scanned as BYTES:
/// find the `fixstr` header for the field name and read the one byte that
/// follows it — `MSGPACK_NIL` is the explicit nil, any other type tag is a real
/// value, and the field name never appearing at all is the omitted form.
///
/// Both halves require the field name to occur AT MOST ONCE, so a frame in which
/// the scan could be ambiguous fails closed instead of classifying the wrong
/// slot. The caller holds this witness and the schema-less one to each other.
fn raw_key_form(scenario: &Value, id: &str) -> &'static str {
    match scenario["codec"].as_str().expect("codec") {
        "json" => {
            let text = scenario["wire_json"].as_str().expect("wire_json is text");
            let hits = text.match_indices("\"key\"").count();
            assert!(
                hits <= 1,
                "{id}: `\"key\"` occurs {hits} times in the raw json; this witness \
                 classifies one slot and fails closed rather than guessing which"
            );
            match text.find("\"key\"") {
                None => "omitted",
                Some(at) => {
                    let after = text[at + "\"key\"".len()..].trim_start();
                    let value = after
                        .strip_prefix(':')
                        .unwrap_or_else(|| {
                            panic!("{id}: raw json `\"key\"` is not a member name: {after}")
                        })
                        .trim_start();
                    if value.starts_with("null") {
                        "null"
                    } else {
                        "present"
                    }
                }
            }
        }
        "msgpack" => {
            let bytes = decode_hex(scenario["wire_msgpack_hex"].as_str().expect("hex"));
            let hits: Vec<usize> = bytes
                .windows(MSGPACK_KEY_FIELD.len())
                .enumerate()
                .filter(|(_, window)| *window == MSGPACK_KEY_FIELD)
                .map(|(at, _)| at)
                .collect();
            assert!(
                hits.len() <= 1,
                "{id}: the `key` field name occurs {} times in the raw msgpack; this \
                 witness classifies one slot and fails closed rather than guessing which",
                hits.len()
            );
            match hits.first() {
                None => "omitted",
                Some(&at) => {
                    let tag = *bytes.get(at + MSGPACK_KEY_FIELD.len()).unwrap_or_else(|| {
                        panic!("{id}: the raw msgpack ends immediately after the `key` field name")
                    });
                    if tag == MSGPACK_NIL {
                        "null"
                    } else {
                        "present"
                    }
                }
            }
        }
        other => panic!("{id}: unknown codec {other}"),
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
    key_site(scenario, &generic, "the re-encoded frame")
}

/// The external tag the decoded frame really carries, for holding the
/// scenario's `variant` label to the decode.
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

/// The declared vocabulary of an `assertions` list, as a SET.
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

    // Fixture-scoped prose ledger (`#lzprosekeyconvention`).
    let _prose = common::ProseLedger::open(&path);

    // Anti-vacuity, both directions. `decoded_key: null` is what a runner that
    // never decodes would report, and `reencoded_key_field_present: false` is
    // what a runner that never encodes would report — so the counters below pin
    // the two `present` forms per codec, which only a real decode can produce.
    //
    // The three vocabularies are collected here and asserted AFTER the loop,
    // against what the replay really dispatched on (`#lznullformblind`).
    // Compared against a hand-written literal — or against the fixture's own
    // list — they were green over a runner that decodes nothing, which is the
    // exact vacuity `anti_vacuity` exists to name, so naming them in a discharge
    // discharged nothing. `forms_replayed` in particular is read off the RAW
    // WIRE rather than off the fixture's `key_form` labels.
    let mut replayed = 0usize;
    let mut keys_decoded = 0usize;
    let mut codecs_replayed: BTreeSet<String> = BTreeSet::new();
    let mut fields_replayed: BTreeSet<String> = BTreeSet::new();
    let mut forms_replayed: BTreeSet<String> = BTreeSet::new();

    for scenario in fixture["scenarios"].as_array().expect("scenarios") {
        let id = scenario["id"].as_str().expect("id").to_owned();
        common::record_scenario(&path, &id, ScenarioIdSource::Id);
        replayed += 1;

        codecs_replayed.insert(scenario["codec"].as_str().expect("codec").to_owned());
        fields_replayed.insert(scenario["field"].as_str().expect("field").to_owned());

        // THE RAW-WIRE CONTROL. Read the `key` slot out of the scenario's own
        // bytes before the decoder touches them, and hold the scenario's
        // declared `key_form` to it. Not a selector: a scenario tagged `null`
        // whose frame omits the entry, a corpus that lost the distinction in
        // carriage, or a runner that re-serialized a pre-parsed object before
        // looking, all redden HERE and nowhere else — every downstream `expect`
        // key is identical across the two families.
        let on_wire = wire_key_form(scenario, &id);
        // The second witness, taken without any decoder at all. The two must
        // agree: the schema-less classification runs on the same crates as the
        // decode under test, so on its own it cannot see a defect shared with
        // what it is controlling.
        let raw = raw_key_form(scenario, &id);
        assert_eq!(
            on_wire, raw,
            "{id}: the schema-less witness and the decoder-free byte/text witness \
             disagree about the `key` slot"
        );
        let declared = scenario["key_form"].as_str().expect("key_form");
        assert!(
            matches!(declared, "omitted" | "null" | "present"),
            "{id}: scenario declares the unknown key form `{declared}`; this runner \
             classifies exactly `omitted` / `null` / `present` and fails closed rather \
             than defaulting an unrecognised one into the lenient branch"
        );
        assert_eq!(
            declared, on_wire,
            "{id}: the scenario's label and its own bytes disagree about the `key` slot"
        );
        forms_replayed.insert(on_wire.to_owned());

        let exp = Expect::new(
            path.clone(),
            format!("scenarios[{id}].expect"),
            &scenario["expect"],
        );
        let message = decode(scenario, &exp);
        // The scenario's `variant` label, held against the variant the decode
        // really produced (`#lznullformblind`). It was read by NOTHING: this
        // runner has no tracker over the scenario's own top-level keys, so an
        // unread label there is invisible to every rung — the unconsumed-key
        // guard only covers blocks a runner already bound. A label that names
        // one variant over a frame that decodes as another is a fixture defect
        // no `expect` key can see, since `field` would still navigate.
        assert_eq!(
            scenario["variant"].as_str().expect("variant"),
            variant_name(&message),
            "{id}: the scenario's `variant` label and the decoded frame disagree"
        );
        let key = decoded_key(scenario, &message);
        if key.is_some() {
            keys_decoded += 1;
        }
        let node = reencoded_node(scenario, &message);

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

    // The fixture-level block is evaluated AFTER the replay, so every vocabulary
    // is compared against what the run PRODUCED (`#lznullformblind`) — and
    // BEFORE the runner-side coverage gates below, which is the other half of
    // the ordering. A correct assertion placed behind an earlier `assert_eq!`
    // that fires first is unreachable: with the gates first, a divergence
    // between the fixture's `scenario_count` and the scenarios really replayed
    // could only ever surface as the gate's message, and the assertion key —
    // the thing the corpus can read — would never be reached at all.
    let a = Expect::new(path.clone(), "assertions", &fixture["assertions"]);
    a.assert_key("required_of_binding", "MUST");
    a.assert_key_with("codecs", |v| {
        assert_eq!(
            string_set(v, "codecs"),
            codecs_replayed,
            "codecs: declared vs the codecs this run really decoded through"
        );
    });
    a.assert_key_with("fields", |v| {
        assert_eq!(
            string_set(v, "fields"),
            fields_replayed,
            "fields: declared vs the optional-key sites this run really navigated"
        );
    });
    // Both directions, against the RAW WIRE rather than a list of literals:
    // every declared form was carried by a scenario whose own bytes this runner
    // classified before decoding, and no scenario carried a form the block does
    // not declare. A literal here is green over a runner that never opens a
    // frame, and it cannot see `null` collapsing into `omitted`.
    a.assert_key_with("key_forms", |v| {
        assert_eq!(
            string_set(v, "key_forms"),
            forms_replayed,
            "key_forms: declared vs the forms read off the scenarios' own bytes"
        );
    });
    // Against the scenarios this run REACHED, not against
    // `fixture["scenarios"].len()` — the fixture compared to itself is green
    // over a runner that decodes nothing.
    a.assert_key("scenario_count", replayed as u64);
    // The four declared paragraphs (`#lzprosekeyconvention`). The clause is the
    // encoder/decoder split, so it takes both halves; the re-encode obligation
    // is exactly the encoder half, which no decode assertion can reach.
    // `key_forms` now carries the clause's premise as well — it is what proves
    // the two forms were DISTINCT going in, and `decoded_key` is what proves
    // they arrive the same.
    a.prose_key(
        "clause",
        &["key_forms", "decoded_key", "reencoded_key_field_present"],
    );
    a.prose_key("wire_encoding", &["wire_input_fnv1a64"]);
    a.prose_key("reencode_obligation", &["reencoded_key_field_present"]);
    // The controls are the `omitted` and `present` forms of `key_forms`, counted
    // off the raw wire rather than off the fixture's labels, seen through
    // `decoded_key` across the full `scenario_count`.
    a.prose_key(
        "anti_vacuity",
        &["decoded_key", "key_forms", "scenario_count"],
    );
    a.finish();

    // Runner-side coverage gates LAST, so they can only ever add a failure the
    // assertion keys above did not already name.
    assert_eq!(replayed, 12, "two fields x three key forms x two codecs");
    assert_eq!(
        keys_decoded, 4,
        "only the `present` scenarios carry a key; a runner reporting absent for \
         everything satisfies the null cases trivially"
    );

    common::expect::verify_prose(&path);
}
