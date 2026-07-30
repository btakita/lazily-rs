#![cfg(feature = "signaling-client")]

mod common;

use common::Expect;
use lazily::{ClientMessage, ServerMessage};
use serde::Deserialize;
use serde_json::Value;

const FRAMES_PATH: &str = "../lazily-spec/conformance/signaling/frames.json";
const SESSION_PATH: &str = "../lazily-spec/conformance/signaling/anti_spoof_session.json";

#[derive(Deserialize)]
struct FrameCase {
    label: String,
    direction: String,
    wire: Value,
    /// Language-agnostic claims about the DECODED frame. Read by nothing until
    /// `#lzassertunknownkeys`: the runner round-tripped the wire and never
    /// checked a single one, so `server_stamped_from` / `roster_excludes_self`
    /// were carried by the corpus and asserted by no binding here.
    #[serde(default)]
    assertions: Value,
}

#[derive(Deserialize)]
struct FramesFixture {
    frames: Vec<FrameCase>,
    rejects: Vec<FrameCase>,
}

#[derive(Deserialize)]
struct SessionInput {
    recv: Value,
}

#[derive(Deserialize)]
struct SessionReject {
    label: String,
    input: SessionInput,
}

#[derive(Deserialize)]
struct SessionFixture {
    rejects: Vec<SessionReject>,
    /// Session-level claims. See `anti_spoof_fixture_rejects_client_supplied_from`
    /// for why this Rust runner cannot consume them.
    #[serde(default)]
    assertions: Value,
}

fn decode(direction: &str, wire: &Value) -> Result<Value, String> {
    let bytes = serde_json::to_vec(wire).expect("fixture wire serializes");
    match direction {
        "client" => serde_json::from_slice::<ClientMessage>(&bytes)
            .and_then(serde_json::to_value)
            .map_err(|error| error.to_string()),
        "server" => ServerMessage::from_json_slice(&bytes)
            .and_then(serde_json::to_value)
            .map_err(|error| error.to_string()),
        other => Err(format!("unknown fixture direction {other:?}")),
    }
}

#[test]
fn signaling_frames_replay_positive_and_negative_cases() {
    let raw = common::spec_read_to_string(FRAMES_PATH).expect("read signaling frames fixture");
    let fixture: FramesFixture =
        serde_json::from_str(&raw).expect("parse signaling frames fixture");

    for case in fixture.frames {
        let actual = decode(&case.direction, &case.wire)
            .unwrap_or_else(|error| panic!("{} should decode: {error}", case.label));
        assert_eq!(
            actual, case.wire,
            "{} should round-trip exactly",
            case.label
        );
        assert_frame_assertions(&case, &actual);
    }

    for case in fixture.rejects {
        assert!(
            decode(&case.direction, &case.wire).is_err(),
            "{} should be rejected",
            case.label
        );
    }
}

/// Assert every key of a frame's `assertions` block against the DECODED frame.
///
/// Reading them off `case.wire` would assert the fixture against itself; these
/// read `actual`, the value the codec produced.
fn assert_frame_assertions(case: &FrameCase, actual: &Value) {
    let exp = Expect::new(
        FRAMES_PATH,
        format!("frames[{}].assertions", case.label),
        &case.assertions,
    );
    // Each key is optional per frame, so every comparison is bound to the key's
    // *presence*: a bare read on a frame that does not carry the key marked it
    // consumed while comparing nothing (`#lzconsumednotasserted`).
    exp.assert_key_if_present("peer", |want| {
        assert_eq!(
            actual["peer"].as_u64(),
            want.as_u64(),
            "{}: peer",
            case.label
        );
    });
    exp.assert_key_if_present("to", |want| {
        assert_eq!(actual["to"].as_u64(), want.as_u64(), "{}: to", case.label);
    });
    exp.assert_key_if_present("from", |want| {
        assert_eq!(
            actual["from"].as_u64(),
            want.as_u64(),
            "{}: from",
            case.label
        );
    });
    exp.assert_key_if_present("code", |want| {
        assert_eq!(
            actual["code"].as_str(),
            want.as_str(),
            "{}: code",
            case.label
        );
    });
    exp.assert_key_if_present("has_capabilities", |want| {
        assert_eq!(
            actual.get("capabilities").is_some_and(|c| !c.is_null()),
            want.as_bool().expect("has_capabilities"),
            "{}: has_capabilities",
            case.label
        );
    });
    exp.assert_key_if_present("capabilities", |want| {
        assert_eq!(
            actual["capabilities"].as_array(),
            want.as_array(),
            "{}: capabilities",
            case.label
        );
    });
    exp.assert_key_if_present("peers", |want| {
        assert_eq!(
            actual["peers"].as_array(),
            want.as_array(),
            "{}: peers",
            case.label
        );
    });
    exp.assert_key_if_present("roster_excludes_self", |want| {
        // A welcome roster that lists the joining peer is the spoof this key
        // pins; `rejects` covers the wire-level form, this covers the decoded one.
        let self_peer = actual["peer"].as_u64();
        let excluded = actual["peers"]
            .as_array()
            .expect("welcome carries a roster")
            .iter()
            .all(|p| p.as_u64() != self_peer);
        assert_eq!(
            excluded,
            want.as_bool().expect("roster_excludes_self"),
            "{}: roster_excludes_self",
            case.label
        );
    });
    exp.assert_key_if_present("server_stamped_from", |want| {
        // A forwarded frame carries `from` (stamped by the server) and never a
        // client-supplied `to` — mixing both is the anti-spoof reject.
        let stamped = actual.get("from").is_some_and(|v| !v.is_null())
            && !actual.get("to").is_some_and(|v| !v.is_null());
        assert_eq!(
            stamped,
            want.as_bool().expect("server_stamped_from"),
            "{}: server_stamped_from",
            case.label
        );
    });
}

#[test]
fn anti_spoof_fixture_rejects_client_supplied_from() {
    let raw = common::spec_read_to_string(SESSION_PATH).expect("read anti-spoof fixture");
    let fixture: SessionFixture =
        serde_json::from_str(&raw).expect("parse signaling anti-spoof fixture");

    // `assertions` here are claims about a SERVER SESSION replayed over `steps`
    // (roster construction, server-side `from` stamping, roster ordering). The
    // signalling *server* is not part of the Rust crate — `lazily` ships the
    // client codec only, and the session model lives in `signaling/` (TypeScript),
    // whose `signaling/test/protocol.test.ts` replays these same steps. This Rust
    // runner exercises the codec's reject behaviour, so the capability the keys
    // describe genuinely does not exist here (`#lzassertunknownkeys`).
    let exp = Expect::new(SESSION_PATH, "assertions", &fixture.assertions);
    for key in [
        "forwarded_from_is_server_registered",
        "roster_excludes_self",
        "roster_sorted_ascending",
    ] {
        exp.excuse_key(
            key,
            "server-session claim; the Rust crate ships the signalling CLIENT codec only \
             (the session model + its replay live in signaling/, TypeScript)",
        );
    }

    for case in fixture.rejects {
        assert!(
            decode("client", &case.input.recv).is_err(),
            "{} should be rejected",
            case.label
        );
    }
}
