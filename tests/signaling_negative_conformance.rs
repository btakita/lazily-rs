#![cfg(feature = "signaling-client")]

mod common;

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
    }

    for case in fixture.rejects {
        assert!(
            decode(&case.direction, &case.wire).is_err(),
            "{} should be rejected",
            case.label
        );
    }
}

#[test]
fn anti_spoof_fixture_rejects_client_supplied_from() {
    let raw = common::spec_read_to_string(SESSION_PATH).expect("read anti-spoof fixture");
    let fixture: SessionFixture =
        serde_json::from_str(&raw).expect("parse signaling anti-spoof fixture");

    for case in fixture.rejects {
        assert!(
            decode("client", &case.input.recv).is_err(),
            "{} should be rejected",
            case.label
        );
    }
}
