#![cfg(feature = "signaling-client")]

mod common;

use common::Expect;
use lazily::{ClientMessage, ServerMessage};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

const FRAMES_PATH: &str = "../lazily-spec/conformance/signaling/frames.json";
const SESSION_PATH: &str = "../lazily-spec/conformance/signaling/anti_spoof_session.json";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameCase {
    label: String,
    direction: String,
    wire: Value,
    /// The frame's message variant. Carried by every positive frame and read by
    /// NOTHING until `#lznullformblind` — serde drops an undeclared field
    /// silently, so an unread label on a plain struct is invisible to every
    /// rung: the assertion-key guard only covers blocks a runner bound, and this
    /// one was never in a block at all. It is held to the DECODED frame below,
    /// not to the input wire, so a label naming one variant over a frame the
    /// codec reads as another reddens. `Option` because the `rejects` cases
    /// share this struct and carry no variant.
    #[serde(default)]
    variant: Option<String>,
    /// Language-agnostic claims about the DECODED frame. Read by nothing until
    /// `#lzassertunknownkeys`: the runner round-tripped the wire and never
    /// checked a single one, so `server_stamped_from` / `roster_excludes_self`
    /// were carried by the corpus and asserted by no binding here.
    #[serde(default)]
    assertions: Value,
    #[serde(default)]
    #[allow(dead_code)]
    #[serde(rename = "reason")]
    frame_reason: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FramesFixture {
    #[allow(dead_code)]
    #[serde(rename = "description")]
    frames_description: String,
    protocol_version: u64,
    kind: String,
    frames: Vec<FrameCase>,
    rejects: Vec<FrameCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInput {
    /// Which connection sent the frame. The server's whole anti-spoof job is to
    /// stamp `from` off THIS, never off anything the client wrote.
    #[serde(default)]
    conn: Option<String>,
    recv: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionReject {
    label: String,
    input: SessionInput,
    #[allow(dead_code)]
    #[serde(rename = "reason")]
    session_reason: String,
}

/// One frame the session emits, and the connection it goes to.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionEmit {
    #[allow(dead_code)]
    #[serde(rename = "to")]
    emit_to: String,
    frame: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionStep {
    input: SessionInput,
    #[serde(default)]
    expect: Vec<SessionEmit>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionFixture {
    #[allow(dead_code)]
    #[serde(rename = "description")]
    session_description: String,
    protocol_version: u64,
    kind: String,
    mode: String,
    #[serde(default)]
    steps: Vec<SessionStep>,
    rejects: Vec<SessionReject>,
    /// Session-level claims, asserted over the transcript's own emitted frames
    /// decoded through the shipped codec (`#lznullformblind`).
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
        // Fail-closed (`#lzscenariobodyskip`). This used to return `Err`, which
        // the negative loop below asserts with `is_err()` — so a `rejects` case
        // carrying a misspelled `direction` was "rejected" because the RUNNER
        // did not recognise the direction, never because the codec rejected the
        // frame. The fixture's whole claim went unexercised and the case passed.
        other => panic!("unknown fixture direction {other:?}"),
    }
}

#[test]
fn signaling_frames_replay_positive_and_negative_cases() {
    let raw = common::spec_read_to_string(FRAMES_PATH).expect("read signaling frames fixture");
    let fixture: FramesFixture =
        serde_json::from_str(&raw).expect("parse signaling frames fixture");
    assert_eq!(fixture.protocol_version, 1);
    assert_eq!(fixture.kind, "SignalingFrames");

    let mut variants_replayed = 0usize;
    for case in fixture.frames {
        let actual = decode(&case.direction, &case.wire)
            .unwrap_or_else(|error| panic!("{} should decode: {error}", case.label));
        assert_eq!(
            actual, case.wire,
            "{} should round-trip exactly",
            case.label
        );
        // The label held to the DECODE (`#lznullformblind`). Every positive
        // frame declares its variant and nothing checked it; the discriminator
        // the codec really produced is the round-tripped frame's `type`.
        let variant = case
            .variant
            .as_deref()
            .unwrap_or_else(|| panic!("{}: a positive frame must declare a variant", case.label));
        assert_eq!(
            actual["type"].as_str(),
            Some(variant),
            "{}: the frame's `variant` label and the decoded discriminator disagree",
            case.label
        );
        variants_replayed += 1;
        assert_frame_assertions(&case, &actual);
    }
    assert_eq!(
        variants_replayed, 17,
        "every positive frame must reach the variant check"
    );

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
    assert_eq!(fixture.protocol_version, 1);
    assert_eq!(fixture.kind, "SignalingSession");
    assert_eq!(fixture.mode, "open");

    // These three were EXCUSED as "server-session claims the Rust crate cannot
    // reach", on the grounds that `lazily` ships the signalling client codec only
    // and the session model lives in `signaling/` (TypeScript). A corpus
    // perturbation pass showed what that cost (`#lznullformblind`): flipping any
    // of the three to `false` in a scratch copy of the corpus left this suite
    // GREEN, so the anti-spoof invariant the fixture exists for was one corpus
    // edit away from being switched off with nothing noticing. lazily-go found
    // the identical three keys from the other direction.
    //
    // Running the SERVER is indeed out of reach here. Asserting the claims is
    // not: the transcript's own emitted frames are ordinary signalling frames,
    // and this crate ships the decoder for them. Each claim below is checked
    // over those frames decoded through `ServerMessage` — the shipped codec, not
    // a re-read of the fixture — so it is grounded in the library rather than
    // fabricated. What stays out of scope is whether a server WOULD emit this
    // transcript; `signaling/test/protocol.test.ts` covers that.
    //
    // `conn -> peer` is the server-side registry: the peer id bound to a
    // connection when it joined. `forwarded_from_is_server_registered` is
    // exactly the claim that a forwarded frame's `from` equals the registry
    // entry for the connection that SENT it, never anything the sender wrote.
    let mut registry: BTreeMap<String, u64> = BTreeMap::new();
    let mut rosters_checked = 0usize;
    let mut forwarded_checked = 0usize;
    let mut roster_excludes_self = true;
    let mut roster_sorted = true;
    let mut from_is_registered = true;

    for (i, step) in fixture.steps.iter().enumerate() {
        let conn = step
            .input
            .conn
            .clone()
            .unwrap_or_else(|| panic!("step {i}: a session step names the sending connection"));
        // The inbound frame goes through the CLIENT codec, which is what binds
        // the registry to a decode rather than to the fixture's text.
        let recv = decode("client", &step.input.recv)
            .unwrap_or_else(|e| panic!("step {i}: session input should decode: {e}"));
        if recv["type"] == "join" {
            registry.insert(
                conn.clone(),
                recv["peer"].as_u64().expect("a join names its peer"),
            );
        }

        for emit in &step.expect {
            let frame = decode("server", &emit.frame)
                .unwrap_or_else(|e| panic!("step {i}: emitted frame should decode: {e}"));
            match frame["type"].as_str() {
                Some("welcome") => {
                    let self_peer = frame["peer"].as_u64().expect("welcome names its peer");
                    let peers: Vec<u64> = frame["peers"]
                        .as_array()
                        .expect("welcome carries a roster")
                        .iter()
                        .map(|p| p.as_u64().expect("roster holds peer ids"))
                        .collect();
                    roster_excludes_self &= !peers.contains(&self_peer);
                    roster_sorted &= peers.windows(2).all(|w| w[0] < w[1]);
                    rosters_checked += 1;
                }
                // A forwarded frame is one carrying `from`. The server stamps it;
                // the sender never supplies it (the `rejects` half below is the
                // frame that tries).
                _ if frame.get("from").is_some_and(|v| !v.is_null()) => {
                    let stamped = frame["from"].as_u64().expect("`from` is a peer id");
                    from_is_registered &= registry.get(&conn) == Some(&stamped);
                    // ...and it is the REGISTRY's value, not an echo of whatever
                    // the input happened to carry.
                    from_is_registered &= step.input.recv.get("from").is_none();
                    forwarded_checked += 1;
                }
                _ => {}
            }
        }
    }

    // "Exercised at least once" (`#lzvacuousrun`). A transcript-wide invariant
    // only fires where a frame of the right shape appears, so a match arm that
    // stopped recognising `welcome` — or a forwarded variant — would silence the
    // rule while every assertion below still read `true`.
    assert!(
        rosters_checked >= 2,
        "the roster rules were never asked: {rosters_checked} welcome frames reached the check"
    );
    assert!(
        forwarded_checked >= 3,
        "the anti-spoof rule this fixture exists for was never asked: \
         {forwarded_checked} forwarded frames reached the check"
    );

    let exp = Expect::new(SESSION_PATH, "assertions", &fixture.assertions);
    exp.assert_key("roster_excludes_self", roster_excludes_self);
    exp.assert_key("roster_sorted_ascending", roster_sorted);
    exp.assert_key("forwarded_from_is_server_registered", from_is_registered);

    for case in fixture.rejects {
        assert!(
            decode("client", &case.input.recv).is_err(),
            "{} should be rejected",
            case.label
        );
    }
}
