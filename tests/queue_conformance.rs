//! Cross-language conformance tests for the reactive queue (`QueueCell`), the
//! layer required of every binding — see the Binding Conformance Matrix in
//! `lazily-spec/protocol.md` and `lazily-spec/cell-model.md` § "Reactive
//! queues".
//!
//! These are **compute** fixtures: lazily-rs loads the `initial` state, replays
//! each `step`'s `op`, and asserts the `expected` observable effects (resulting
//! `elements` / `head` / `len` / `is_empty` / `is_full` / `closed`, and — the
//! core of the spec — exactly which reader classes (`head` / `len` / `is_empty`
//! / `is_full` / `closed`) invalidate). The five fixtures cover SPSC total FIFO,
//! the popped-head observation, MPSC multi-writer inside `batch()`, bounded
//! reactive backpressure, and the closure lifecycle.

mod common;

use common::Expect;
use lazily::{Context, QueueCell, QueuePopError, QueuePushError, QueueStorage};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

type V = String;

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn spec_fixtures_present() -> bool {
    std::path::Path::new(&format!("{SPEC_DIR}/queuecell_spsc_push_pop.json")).exists()
}

fn build_initial(ctx: &Context, initial: &Value) -> QueueCell<V> {
    let cap = initial.get("capacity").and_then(|v| v.as_u64());
    let q = match cap {
        Some(c) => QueueCell::with_capacity(ctx, c as usize),
        None => QueueCell::new(ctx),
    };
    if let Some(elems) = initial.get("elements").and_then(|v| v.as_array()) {
        for e in elems {
            q.try_push(ctx, e.as_str().unwrap().to_string()).unwrap();
        }
    }
    // `closed` in initial is rare but supported: honor it.
    if initial
        .get("closed")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        q.close(ctx);
    }
    q
}

/// A reader-kind slot whose invalidation we can observe via `ctx.is_set`.
type Reader = lazily::Computed<()>;

fn make_readers(ctx: &Context, q: &QueueCell<V>) -> Readers {
    // Each reader subscribes to exactly one reader-kind cell. We wrap the
    // reactive read in a `computed` returning `()` so `ctx.is_set` reports
    // whether the cached value survived the last op.
    let head = {
        let q = q.clone();
        ctx.computed(move |ctx| {
            q.head(ctx);
        })
    };
    let len = {
        let q = q.clone();
        ctx.computed(move |ctx| {
            q.len(ctx);
        })
    };
    let is_empty = {
        let q = q.clone();
        ctx.computed(move |ctx| {
            q.is_empty(ctx);
        })
    };
    let is_full = {
        let q = q.clone();
        ctx.computed(move |ctx| {
            q.is_full(ctx);
        })
    };
    let closed = {
        let q = q.clone();
        ctx.computed(move |ctx| {
            q.is_closed(ctx);
        })
    };
    Readers {
        head,
        len,
        is_empty,
        is_full,
        closed,
    }
}

struct Readers {
    head: Reader,
    len: Reader,
    is_empty: Reader,
    is_full: Reader,
    closed: Reader,
}

/// Materialize every reader's cache so the next op's invalidation is observable
/// via `ctx.is_set` (a cached reader that stays cached was not invalidated).
fn materialize_all(ctx: &Context, readers: &Readers) {
    ctx.get(&readers.head);
    ctx.get(&readers.len);
    ctx.get(&readers.is_empty);
    ctx.get(&readers.is_full);
    ctx.get(&readers.closed);
}

/// Assert the per-reader-kind invalidation matrix for one step. Call this
/// immediately after the op (with readers still holding their pre-op cached
/// values), then it re-materializes for the next step.
///
/// A reader kind explicitly present in `invalidates` is asserted
/// (`true` ⇒ must invalidate, `false` ⇒ must stay cached). A reader kind
/// **absent** from `invalidates` is not asserted — fixtures that focus on one
/// reader kind (e.g. `popped_head_observation`) only declare the kind under
/// test, so absence means "don't check", not "must be false".
fn assert_invalidation(ctx: &Context, readers: &Readers, invalidates: &Expect) {
    // DESCENT (`#lzsubblockkeyset`). The matrix is an OBJECT value, and the
    // shape below — a fixed list of `check` calls the runner knows about — is
    // exactly the defect: a reader kind the corpus adds would be read by nobody
    // and compared by nothing, while every guard above reported clean. Routing
    // each kind through the child tracker makes the omission an unconsumed key.
    let check = |name: &str, reader: &Reader| {
        // Only assert reader kinds the fixture explicitly declares.
        invalidates.assert_key_if_present(name, |node| {
            let expected_inv = node.as_bool().unwrap_or(false);
            let cached = ctx.is_set(reader);
            if expected_inv {
                assert!(
                    !cached,
                    "reader `{name}` should have been invalidated but stayed cached"
                );
            } else {
                assert!(
                    cached,
                    "reader `{name}` should have stayed cached but was invalidated"
                );
            }
        });
    };

    check("head", &readers.head);
    check("len", &readers.len);
    check("is_empty", &readers.is_empty);
    check("is_full", &readers.is_full);
    check("closed", &readers.closed);

    // Re-materialize all readers so the next step starts from a known-cached
    // state regardless of which were invalidated.
    materialize_all(ctx, readers);
}

/// Assert the observable queue state after a step.
fn assert_state(ctx: &Context, q: &QueueCell<V>, expected: &Expect) {
    // Each key is optional per fixture, so every comparison is bound to the key's
    // *presence*: `expected["k"].as_bool()` marked the key consumed even when the
    // fixture did not carry it and nothing was compared
    // (`#lzconsumednotasserted`).
    expected.assert_key_if_present("elements", |want| {
        let want: Vec<String> = want
            .as_array()
            .expect("elements")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(q.elements(), want, "elements mismatch");
    });
    expected.assert_key_if_present("head", |head| {
        let want: Option<String> = if head.is_null() {
            None
        } else {
            Some(head.as_str().unwrap().to_string())
        };
        assert_eq!(q.head(ctx), want, "head mismatch");
    });
    expected.assert_key_if_present("len", |want| {
        assert_eq!(
            q.len(ctx),
            want.as_u64().expect("len") as usize,
            "len mismatch"
        );
    });
    expected.assert_key_if_present("is_empty", |want| {
        assert_eq!(q.is_empty(ctx), want.as_bool().expect("is_empty"));
    });
    expected.assert_key_if_present("is_full", |want| {
        assert_eq!(q.is_full(ctx), want.as_bool().expect("is_full"));
    });
    expected.assert_key_if_present("closed", |want| {
        assert_eq!(q.is_closed(ctx), want.as_bool().expect("closed"));
    });
}

/// Expected `returns` value for an op (an element string, or an error label).
fn returns_value(step: &Value) -> Option<Value> {
    step.get("returns")
        .and_then(|r| if r.is_null() { None } else { Some(r.clone()) })
}

/// Run a single fixture file: replay every step and assert state + invalidation.
fn run_fixture(ctx: &Context, name: &str, fixture: &Value) {
    let q = build_initial(ctx, fixture.get("initial").expect("initial"));
    let readers = make_readers(ctx, &q);
    materialize_all(ctx, &readers);

    for (i, step) in fixture
        .get("steps")
        .and_then(|v| v.as_array())
        .expect("steps")
        .iter()
        .enumerate()
    {
        let op = step.get("op").expect("op");
        let op_type = op.get("type").and_then(|v| v.as_str()).expect("op.type");
        // Guard the step's `expected` block (`#lzassertunknownkeys`): a key this
        // runner never reads fails the fixture instead of passing unnoticed.
        let expected = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("steps[{i}].expected"),
            step.get("expected").unwrap_or(&Value::Null),
        );

        let got_returns = match op_type {
            "push" => {
                let val = op.get("value").unwrap().as_str().unwrap().to_string();
                let r = q.try_push(ctx, val);
                assert!(r.is_ok(), "step {i}: push should succeed, got {r:?}");
                Some(Value::Null)
            }
            "try_push" => {
                let val = op.get("value").unwrap().as_str().unwrap().to_string();
                match q.try_push(ctx, val) {
                    Ok(()) => Some(Value::Null),
                    Err(QueuePushError::Full) => Some(Value::String("Full".into())),
                    Err(QueuePushError::Closed) => Some(Value::String("Closed".into())),
                }
            }
            "pop" => match q.try_pop(ctx) {
                Ok(v) => Some(Value::String(v)),
                Err(QueuePopError::Empty) => Some(Value::String("Empty".into())),
                Err(QueuePopError::Closed) => Some(Value::String("Closed".into())),
            },
            "try_pop" => match q.try_pop(ctx) {
                Ok(v) => Some(Value::String(v)),
                Err(QueuePopError::Empty) => Some(Value::String("Empty".into())),
                Err(QueuePopError::Closed) => Some(Value::String("Closed".into())),
            },
            "close" => {
                q.close(ctx);
                Some(Value::Null)
            }
            "batch" => {
                let ops = op.get("ops").and_then(|v| v.as_array()).expect("batch.ops");
                ctx.batch(|ctx| {
                    for inner in ops {
                        let ty = inner
                            .get("type")
                            .and_then(|v| v.as_str())
                            .expect("batch op.type");
                        assert_eq!(ty, "push", "batch currently only wraps pushes");
                        let val = inner.get("value").unwrap().as_str().unwrap().to_string();
                        q.try_push(ctx, val).unwrap();
                    }
                });
                Some(Value::Null)
            }
            other => panic!("unknown queue op type: {other}"),
        };

        // Assert the observable state.
        assert_state(ctx, &q, &expected);

        // Assert the `returns` value (element or error label).
        if let Some(want) = returns_value(step) {
            let got = got_returns.unwrap_or(Value::Null);
            assert_eq!(got, want, "step {i}: returns mismatch");
        }

        // Assert the per-reader-kind invalidation matrix.
        if let Some(invalidates) = expected.sub_if_present("invalidates") {
            assert_invalidation(ctx, &readers, &invalidates);
        }
    }
}

macro_rules! queue_conformance {
    ($($name:ident => $file:literal),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                if !spec_fixtures_present() {
                    eprintln!(
                        "lazily-spec conformance fixtures not found at {SPEC_DIR}; skipping."
                    );
                    return;
                }
                let fixture = load_fixture($file);
                let ctx = Context::new();
                run_fixture(&ctx, $file, &fixture);
            }
        )+
    };
}

queue_conformance! {
    spsc_push_pop => "queuecell_spsc_push_pop.json",
    popped_head_observation => "queuecell_popped_head_observation.json",
    mpsc_multi_writer => "queuecell_mpsc_multi_writer.json",
    bounded_backpressure => "queuecell_bounded_backpressure.json",
    closure_lifecycle => "queuecell_closure_lifecycle.json",
}

// ---------------------------------------------------------------------------
// Direct (non-fixture) tests of the backpressure effect wiring — the spec's
// signature property: a consumer's pop that transitions full → not-full wakes a
// producer-side effect that was backed off on is_full.
// ---------------------------------------------------------------------------

#[test]
fn backpressure_pop_wakes_push_side_effect() {
    let ctx = Context::new();
    let q = QueueCell::<i32>::with_capacity(&ctx, 1);

    // A push-side effect that observes is_full and records each (is_full, len)
    // sample. When full it "backs off" (records Full); when not full it resumes
    // (records Ready).
    use std::cell::RefCell;
    let log = std::rc::Rc::new(RefCell::new(Vec::<(bool, usize)>::new()));
    let log_eff = log.clone();
    let q_eff = q.clone();
    ctx.effect(move |ctx| {
        let full = q_eff.is_full(ctx);
        let len = q_eff.len(ctx);
        log_eff.borrow_mut().push((full, len));
    });
    // After effect setup, the initial sample is (false, 0).
    assert_eq!(*log.borrow(), vec![(false, 0)]);

    // Fill the queue → is_full flips → effect reruns and records (true, 1).
    q.try_push(&ctx, 1).unwrap();
    assert_eq!(*log.borrow(), vec![(false, 0), (true, 1)]);

    // A consumer pop transitions full → not-full. The effect's is_full
    // subscription is invalidated (true → false) and the effect reruns without
    // polling — the reactive backpressure signal.
    q.try_pop(&ctx).unwrap();
    assert_eq!(*log.borrow(), vec![(false, 0), (true, 1), (false, 0)]);
}

#[test]
fn pluggable_storage_via_trait() {
    // A minimal custom backend proving the QueueStorage adapter seam works.
    use std::collections::VecDeque;

    struct BoundedRing<T> {
        buf: VecDeque<T>,
        cap: usize,
        closed: bool,
    }

    impl<T> QueueStorage<T> for BoundedRing<T> {
        fn try_push(&mut self, value: T) -> Result<(), QueuePushError> {
            if self.closed {
                return Err(QueuePushError::Closed);
            }
            if self.buf.len() >= self.cap {
                return Err(QueuePushError::Full);
            }
            self.buf.push_back(value);
            Ok(())
        }
        fn try_pop(&mut self) -> Result<T, QueuePopError> {
            self.buf.pop_front().ok_or(QueuePopError::Empty)
        }
        fn peek(&self) -> Option<&T> {
            self.buf.front()
        }
        fn len(&self) -> usize {
            self.buf.len()
        }
        fn capacity(&self) -> Option<usize> {
            Some(self.cap)
        }
        fn is_closed(&self) -> bool {
            self.closed
        }
        fn close(&mut self) {
            self.closed = true;
        }
    }

    let ctx = Context::new();
    let storage = BoundedRing {
        buf: VecDeque::new(),
        cap: 2,
        closed: false,
    };
    let q = QueueCell::<i32, BoundedRing<i32>>::with_storage(&ctx, storage);

    q.try_push(&ctx, 1).unwrap();
    q.try_push(&ctx, 2).unwrap();
    assert!(q.is_full(&ctx));
    assert_eq!(q.try_push(&ctx, 3), Err(QueuePushError::Full));
    assert_eq!(q.try_pop(&ctx).unwrap(), 1);
    assert!(!q.is_full(&ctx));
    assert_eq!(q.len(&ctx), 1);
    assert_eq!(q.head(&ctx), Some(2));
}

#[cfg(feature = "serde")]
#[test]
fn vecdeque_storage_serde_roundtrip() {
    // VecDequeStorage serializes as a JSON array (element order = FIFO order)
    // per lazily-spec/cell-model.md § "Wire and snapshot shape".
    let mut storage = lazily::VecDequeStorage::<i32>::with_capacity(4);
    storage.try_push(1).unwrap();
    storage.try_push(2).unwrap();
    storage.try_push(3).unwrap();
    let json = serde_json::to_string(&storage).unwrap();
    assert_eq!(json, "[1,2,3]");
}
