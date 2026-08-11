#![cfg(feature = "ipc")]

mod common;

use common::Expect;
use lazily::{Delta, DurableOutbox, InMemoryOutbox, IpcMessage};
use serde_json::Value;

fn fixture() -> Option<Value> {
    let text = crate::common::spec_read_to_string(FIXTURE.path()).ok()?;
    Some(serde_json::from_str(&text).expect("outbox-store fixture JSON"))
}

const FIXTURE: common::SpecDir = common::SpecDir("reliable-sync/outbox_store_protocol.json");

/// Guard a scenario's `expect` block (`#lzassertunknownkeys`).
fn expect<'a>(sc: &'a Value) -> Expect<'a> {
    Expect::new(
        FIXTURE,
        format!("scenarios[{}].expect", sc["name"].as_str().unwrap_or("?")),
        &sc["expect"],
    )
}

/// A fixture array of epochs.
fn u64s(want: &Value) -> Vec<u64> {
    want.as_array()
        .expect("array of epochs")
        .iter()
        .map(Value::as_u64)
        .collect::<Option<Vec<_>>>()
        .expect("array of epochs")
}

fn frame(epoch: u64) -> IpcMessage {
    IpcMessage::Delta(Delta::new(epoch.saturating_sub(1), epoch, vec![]))
}

#[test]
fn generic_outbox_retains_orders_prunes_and_keeps_cursor_monotone() {
    let mut outbox = InMemoryOutbox::default();
    outbox.append(3, frame(3));
    outbox.append(1, frame(1));
    outbox.append(2, frame(2));
    assert_eq!(outbox.retained_epochs(), vec![1, 2, 3]);
    assert_eq!(
        outbox
            .replay_from(1)
            .into_iter()
            .map(|(e, _)| e)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    outbox.ack_through(2);
    outbox.ack_through(1);
    assert_eq!(outbox.acked_through(), 2);
    assert_eq!(outbox.retained_epochs(), vec![3]);
}

#[test]
fn generic_outbox_replays_canonical_store_fixture() {
    let Some(fixture) = fixture() else {
        eprintln!("skipping: lazily-spec outbox-store fixture is not present as a sibling");
        return;
    };
    assert_eq!(fixture["model"], "OutboxStore");
    for (index, scenario) in fixture["scenarios"].as_array().unwrap().iter().enumerate() {
        if scenario["save_cursor"].is_array() {
            // The shared in-memory adapter is owned by value; the serialized
            // multi-handle case is replayed against SQLite below.
            continue;
        }
        // Recorded AFTER the `continue` (`#lzscenariocoverage`): a scenario this
        // loop steps past must not record itself as replayed, or the skip becomes
        // invisible again — which is the whole defect.
        let (id, source) = common::scenario_id(scenario, index);
        common::record_scenario(FIXTURE.path(), &id, source);
        let mut outbox = InMemoryOutbox::default();
        for epoch in scenario["put_epochs"].as_array().unwrap() {
            let epoch = epoch.as_u64().unwrap();
            outbox.append(epoch, frame(epoch));
        }
        let expected = expect(scenario);
        if let Some(cursor) = scenario["scan_after"].as_u64() {
            let epochs = outbox
                .replay_from(cursor)
                .into_iter()
                .map(|(epoch, _)| epoch)
                .collect::<Vec<_>>();
            expected.assert_key_with("epochs", |want| assert_eq!(epochs, u64s(want)));
        }
        if let Some(acks) = scenario["ack_through"].as_array() {
            for ack in acks {
                outbox.ack_through(ack.as_u64().unwrap());
            }
        }
        // `cursor` and `loaded_cursor` are two spellings of the same fact and
        // `replay` / `replay_from_zero` likewise; whichever the scenario carries
        // is asserted, and a scenario carrying neither owes nothing.
        for key in ["cursor", "loaded_cursor"] {
            expected.assert_key_if_present(key, |want| {
                assert_eq!(outbox.acked_through(), want.as_u64().expect("cursor u64"))
            });
        }
        expected.assert_key_if_present("retained", |want| {
            assert_eq!(outbox.retained_epochs(), u64s(want))
        });
        let replayed = outbox
            .replay_from(0)
            .into_iter()
            .map(|(epoch, _)| epoch)
            .collect::<Vec<_>>();
        for key in ["replay_from_zero", "replay"] {
            expected.assert_key_if_present(key, |want| assert_eq!(replayed, u64s(want)));
        }
    }
}

#[cfg(feature = "durable-sqlite")]
#[test]
fn sqlite_outbox_recovers_cursor_and_unacked_suffix_after_reopen() {
    use lazily::SqliteOutbox;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("outbox.db");
    {
        let mut outbox = SqliteOutbox::open(&path, "doc").unwrap();
        outbox.append(1, frame(1));
        outbox.append(2, frame(2));
        outbox.append(3, frame(3));
        outbox.ack_through(1);
    }
    let outbox = SqliteOutbox::open(&path, "doc").unwrap();
    assert_eq!(outbox.acked_through(), 1);
    assert_eq!(outbox.retained_epochs(), vec![2, 3]);
    assert_eq!(
        outbox
            .replay_from(0)
            .into_iter()
            .map(|(e, _)| e)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[cfg(feature = "durable-sqlite")]
#[test]
fn stale_sqlite_handle_cannot_regress_serialized_cursor() {
    use lazily::SqliteOutbox;

    let fixture = fixture().expect("lazily-spec outbox-store fixture");
    let scenario = common::scenario_by_id(
        &FIXTURE.to_string(),
        &fixture,
        "stale_handle_cannot_regress_cursor",
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("stale-cursor.db");
    let mut stale = SqliteOutbox::open(&path, "doc").unwrap();
    let mut current = SqliteOutbox::open(&path, "doc").unwrap();
    for save in scenario["save_cursor"].as_array().unwrap() {
        let epoch = save["epoch"].as_u64().unwrap();
        match save["handle"].as_str().unwrap() {
            "current" => current.ack_through(epoch),
            "stale" => stale.ack_through(epoch),
            handle => panic!("unknown fixture handle {handle}"),
        }
    }
    let exp = expect(scenario.value());
    let expected = exp.assert_key_with("loaded_cursor", |want| {
        let want = want.as_u64().expect("loaded_cursor u64");
        assert_eq!(stale.acked_through(), want);
        want
    });
    drop(stale);
    drop(current);

    let reopened = SqliteOutbox::open(&path, "doc").unwrap();
    assert_eq!(reopened.acked_through(), expected);
}
