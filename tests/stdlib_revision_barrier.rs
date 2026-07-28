use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use lazily::stdlib::{RevisionBarrier, RevisionCheck, RevisionWaitOutcome, Timer};

#[test]
fn already_satisfied_revision_returns_immediately() {
    let barrier = RevisionBarrier::new(4);

    assert_eq!(
        barrier.wait_after(3, |_| RevisionCheck::Satisfied, None, None),
        RevisionWaitOutcome::Satisfied { revision: 4 }
    );
}

#[test]
fn revision_racing_pending_check_is_rechecked_without_a_lost_wakeup() {
    let barrier = RevisionBarrier::new(1);
    let waiter = barrier.clone();
    let ready = Arc::new(AtomicBool::new(false));
    let waiter_ready = Arc::clone(&ready);
    let (checked_tx, checked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let mut first_check = true;
        waiter.wait_after(
            0,
            move |_| {
                if first_check {
                    first_check = false;
                    checked_tx.send(()).expect("test receiver dropped");
                    release_rx.recv().expect("test sender dropped");
                    RevisionCheck::Pending
                } else if waiter_ready.load(Ordering::Acquire) {
                    RevisionCheck::Satisfied
                } else {
                    RevisionCheck::Pending
                }
            },
            None,
            None,
        )
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter did not enter predicate");
    ready.store(true, Ordering::Release);
    assert!(barrier.advance(2));
    release_tx.send(()).expect("waiter dropped release channel");

    assert_eq!(
        handle.join().expect("waiter panicked"),
        RevisionWaitOutcome::Satisfied { revision: 2 }
    );
}

#[test]
fn cancellation_wakes_waiter_without_a_deadline() {
    let barrier = RevisionBarrier::new(1);
    let waiter = barrier.clone();
    let cancellation = barrier.cancellation();
    let waiter_cancellation = cancellation.clone();
    let (started_tx, started_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        started_tx.send(()).expect("test receiver dropped");
        waiter.wait_after(
            1,
            |_| RevisionCheck::Pending,
            None,
            Some(&waiter_cancellation),
        )
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter did not start");
    assert!(cancellation.cancel());
    assert!(!cancellation.cancel());
    assert!(cancellation.is_cancelled());

    assert_eq!(
        handle.join().expect("waiter panicked"),
        RevisionWaitOutcome::Cancelled { revision: 1 }
    );
}

#[test]
fn timer_deadline_returns_a_typed_timeout() {
    let barrier = RevisionBarrier::new(7);
    let mut deadline = Timer::after(Duration::ZERO);

    assert_eq!(
        barrier.wait_after(7, |_| RevisionCheck::Pending, Some(&mut deadline), None,),
        RevisionWaitOutcome::TimedOut { revision: 7 }
    );
}

#[test]
fn disposal_wakes_waiters_and_latches() {
    let barrier = RevisionBarrier::new(3);
    let waiter = barrier.clone();
    let (started_tx, started_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        started_tx.send(()).expect("test receiver dropped");
        waiter.wait_after(3, |_| RevisionCheck::Pending, None, None)
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter did not start");
    assert!(barrier.dispose());
    assert!(!barrier.dispose());
    assert!(barrier.is_disposed());
    assert!(!barrier.advance(4));

    assert_eq!(
        handle.join().expect("waiter panicked"),
        RevisionWaitOutcome::Disposed { revision: 3 }
    );
}

#[test]
fn foreign_cancellation_input_is_unavailable() {
    let barrier = RevisionBarrier::new(2);
    let other = RevisionBarrier::new(9);
    let foreign = other.cancellation();

    assert_eq!(
        barrier.wait_after(1, |_| RevisionCheck::Satisfied, None, Some(&foreign),),
        RevisionWaitOutcome::Unavailable { revision: 2 }
    );
}

#[test]
fn keyed_effect_receipts_remain_external_to_the_barrier() {
    let barrier = RevisionBarrier::new(5);
    let waiter = barrier.clone();
    let receipts = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let waiter_receipts = Arc::clone(&receipts);
    let (checked_tx, checked_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let mut announced = false;
        waiter.wait_after(
            4,
            move |revision| {
                let satisfied = waiter_receipts
                    .lock()
                    .expect("receipt ledger mutex poisoned")
                    .get("save")
                    .is_some_and(|receipt| *receipt >= revision);
                if satisfied {
                    RevisionCheck::Satisfied
                } else {
                    if !announced {
                        announced = true;
                        checked_tx.send(()).expect("test receiver dropped");
                    }
                    RevisionCheck::Pending
                }
            },
            None,
            None,
        )
    });

    checked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiter did not check receipt ledger");
    receipts
        .lock()
        .expect("receipt ledger mutex poisoned")
        .insert("save".to_owned(), 5);
    barrier.notify();

    assert_eq!(
        handle.join().expect("waiter panicked"),
        RevisionWaitOutcome::Satisfied { revision: 5 }
    );
}
