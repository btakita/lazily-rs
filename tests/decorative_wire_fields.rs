#![cfg(feature = "ipc")]

use lazily::{
    CapabilityHandshake, CausalReceipt, CommandAdmissionDecision, CommandAdmissionRejection,
    CommandAdmitter, CommandProjection, CommandSubmit, PeerId,
};
use serde_json::Value;

const FIXTURE: &str = include_str!("fixtures/decorative_wire_fields.json");

fn merge_json(base: &mut Value, overrides: &Value) {
    match (base, overrides) {
        (Value::Object(base), Value::Object(overrides)) => {
            for (key, value) in overrides {
                match base.get_mut(key) {
                    Some(base_value) => merge_json(base_value, value),
                    None => {
                        base.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base, override_value) => *base = override_value.clone(),
    }
}

fn decoded_submit(base: &Value, overrides: Option<&Value>) -> CommandSubmit {
    let mut wire = base.clone();
    if let Some(overrides) = overrides {
        merge_json(&mut wire, overrides);
    }
    serde_json::from_value(wire).expect("fixture CommandSubmit must decode")
}

fn assert_submit_decision(scenario_id: &str, expected: &Value, actual: CommandAdmissionDecision) {
    let kind = expected["decision"]
        .as_str()
        .expect("submit expectation needs a decision");
    match (kind, actual) {
        ("record", CommandAdmissionDecision::Record) => {}
        (
            "deduplicate",
            CommandAdmissionDecision::Deduplicate {
                existing_command_id,
            },
        ) => assert_eq!(
            existing_command_id, expected["existing_command_id"],
            "{scenario_id}"
        ),
        (
            "supersede",
            CommandAdmissionDecision::Supersede {
                existing_command_id,
                cancel_existing,
            },
        ) => {
            assert_eq!(
                existing_command_id, expected["existing_command_id"],
                "{scenario_id}"
            );
            assert_eq!(
                cancel_existing,
                expected["cancel_existing"]
                    .as_bool()
                    .expect("supersede expectation needs cancel_existing"),
                "{scenario_id}"
            );
        }
        (
            "deadline_elapsed",
            CommandAdmissionDecision::Reject(CommandAdmissionRejection::DeadlineElapsed {
                deadline_ms,
                now_ms,
            }),
        ) => {
            assert_eq!(deadline_ms, expected["deadline_ms"], "{scenario_id}");
            assert_eq!(now_ms, expected["now_ms"], "{scenario_id}");
        }
        (
            "invalid_payload_type",
            CommandAdmissionDecision::Reject(CommandAdmissionRejection::InvalidPayloadType {
                expected_prefix,
                ..
            }),
        ) => assert_eq!(
            expected_prefix, expected["expected_prefix"],
            "{scenario_id}"
        ),
        (
            "missing_required_features",
            CommandAdmissionDecision::Reject(CommandAdmissionRejection::MissingRequiredFeatures {
                missing,
            }),
        ) => {
            let expected_missing = expected["missing"]
                .as_array()
                .expect("missing feature expectation must be an array")
                .iter()
                .map(|value| value.as_str().expect("feature must be a string").to_owned())
                .collect::<Vec<_>>();
            assert_eq!(missing, expected_missing, "{scenario_id}");
        }
        (_, actual) => {
            panic!("{scenario_id}: expected admission decision {kind:?}, got {actual:?}")
        }
    }
}

#[test]
fn decoded_wire_fields_drive_command_admission() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture must be valid JSON");
    let base_submit = &fixture["base_submit"];
    let scenarios = fixture["command_scenarios"]
        .as_array()
        .expect("command_scenarios must be an array");
    assert!(
        !scenarios.is_empty(),
        "fixture must contain command scenarios"
    );

    for scenario in scenarios {
        let scenario_id = scenario["id"].as_str().expect("scenario needs an id");
        let features = scenario["features"]
            .as_array()
            .expect("scenario features must be an array")
            .iter()
            .map(|feature| feature.as_str().expect("feature must be a string"));
        let mut admitter = CommandAdmitter::new(features);
        admitter.advance_to(
            scenario["now_ms"]
                .as_u64()
                .expect("scenario now_ms must be an unsigned integer"),
        );
        let mut projection = CommandProjection::new();

        for step in scenario["steps"]
            .as_array()
            .expect("scenario steps must be an array")
        {
            let expected = &step["expect"];
            match step["op"].as_str().expect("step needs an op") {
                "submit" => {
                    let submit = decoded_submit(base_submit, step.get("overrides"));
                    let actual = admitter.admit(&mut projection, &submit);
                    assert_submit_decision(scenario_id, expected, actual);
                }
                "receipt" => {
                    let receipt: CausalReceipt = serde_json::from_value(step["wire"].clone())
                        .expect("fixture CausalReceipt must decode");
                    let error = admitter
                        .observe_receipt(&mut projection, &receipt)
                        .expect_err("observer mismatch must fail closed");
                    match error {
                        CommandAdmissionRejection::UnexpectedObserver {
                            expected: actual_expected,
                            actual,
                            ..
                        } => {
                            assert_eq!(actual_expected, expected["expected"], "{scenario_id}");
                            assert_eq!(actual, expected["actual"], "{scenario_id}");
                        }
                        other => {
                            panic!("{scenario_id}: expected UnexpectedObserver, got {other:?}")
                        }
                    }
                }
                other => panic!("{scenario_id}: unknown fixture op {other:?}"),
            }
        }
    }
}

#[test]
fn decoded_session_id_drives_handshake_compatibility() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture must be valid JSON");
    let scenarios = fixture["handshake_scenarios"]
        .as_array()
        .expect("handshake_scenarios must be an array");
    assert!(
        !scenarios.is_empty(),
        "fixture must contain handshake scenarios"
    );

    for scenario in scenarios {
        let local = CapabilityHandshake::new(
            PeerId(1),
            scenario["local_session_id"]
                .as_str()
                .expect("local_session_id must be a string"),
        );
        let remote = CapabilityHandshake::new(
            PeerId(2),
            scenario["remote_session_id"]
                .as_str()
                .expect("remote_session_id must be a string"),
        );
        let error = local
            .negotiate_with(&remote)
            .expect_err("session mismatch must fail closed");
        assert_eq!(
            error.field(),
            scenario["expect"]
                .as_str()
                .expect("handshake expectation must be a string"),
            "{}",
            scenario["id"]
        );
    }
}
