//! The queue-family reader-kind contract, replayed against **every flavor this
//! binding ships** — with a ledger that is *enforced* rather than advisory.
//!
//! lazily-rs now ships all nine: `QueueCell` / `TopicCell` / `WorkQueueCell` ×
//! single-threaded / thread-safe / async, matching the nine `coverage.json` rows
//! and the Core surface `cell-model.md` § "Core surface vs. binding extensions
//! (queue family)" declares REQUIRED of every binding × every flavor.
//!
//! The flavor axis lives in the **runner**, not the corpus: the fixtures carry a
//! `model` field naming the primitive and no execution-model field, and one
//! `TopicModel` / `WorkQueueModel` trait replays the same JSON against each
//! shell. Nothing in either trait is async-coloured, which is the finding rather
//! than an oversight — a queue's length, a subscription cursor, and a lease
//! decision all derive from storage the graph does not own, so there is nothing
//! to await and no `settle` step anywhere below.
//!
//! Three things keep this suite from reporting green while testing nothing —
//! each one a failure mode this family has actually shipped:
//!
//! * `unshipped_flavors_are_really_absent` greps `src/` for each flavor's type
//!   name, in **both** directions. A ledger row marked shipped whose type does
//!   not exist fails; a type that exists while its row says unshipped fails and
//!   names the runner to extend. The ledger cannot rot, because the filesystem
//!   enforces it. `shipped` must mean "the corpus runs against it" — flipping a
//!   flag without adding a replay is the exact false green this file exists to
//!   prevent.
//! * Every replay returns its step count and every flavor asserts that count is
//!   non-zero and plausible. An absence guard proves the fixtures exist on disk;
//!   only a positive count proves this binary opened them.
//! * `ledger_is_not_all_skips` fails if every flavor is unshipped, and pins the
//!   row count at nine, so a quietly-deleted row is a red test rather than
//!   silently narrowed coverage.
//!
//! Every gate below was mutation-checked: a deliberate defect (drop one
//! invalidation, unbatch a multi-root clear, reverse the redelivery order) turns
//! the owning flavor red. One probe did **not** discriminate on the first pass —
//! reversing `reap_expired`'s sort left the whole corpus green, because no
//! fixture step expires more than one lease at a time. That gap is closed by
//! `multi_expiry_requeues_in_delivery_order` rather than left as a green
//! assertion of nothing.
//!
//! Mirrors the shape of `collections_family_conformance.rs`, which closed the
//! same gap for `ReactiveMap`.

mod common;

use std::fs;

use common::Expect;
use std::path::Path;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

/// The canonical `QueueCell` corpus.
const QUEUE_FIXTURES: &[&str] = &[
    "queuecell_spsc_push_pop.json",
    "queuecell_popped_head_observation.json",
    "queuecell_mpsc_multi_writer.json",
    "queuecell_bounded_backpressure.json",
    "queuecell_closure_lifecycle.json",
];

/// The canonical `TopicCell` corpus. Until this file grew a replay for it these
/// four fixtures were never opened by lazily-rs at all — `topic_conformance.rs`
/// hand-transcribes the same scenarios in Rust, which is the "green against a
/// fixture nobody reads" failure mode the family has hit before.
const TOPIC_FIXTURES: &[&str] = &[
    "topiccell_broadcast_cursor_isolation.json",
    "topiccell_durable_replay_gc.json",
    "topiccell_ephemeral_lifecycle.json",
    "topiccell_offline_tail_bounds.json",
];

/// The canonical `WorkQueueCell` corpus.
const WORK_QUEUE_FIXTURES: &[&str] = &[
    "workqueue_competing_delivery.json",
    "workqueue_lease_deadletter.json",
];

/// One primitive × one execution flavor, and the type name that would prove it
/// exists.
struct Flavor {
    /// Human name, used in failure messages.
    name: &'static str,
    /// The type a binding defines when it ships this flavor. Grepped for, not
    /// referenced, because referencing a type that does not exist would not
    /// compile — and a ledger that cannot be written until the work is done is
    /// no ledger at all.
    marker_type: &'static str,
    /// Whether this binding ships it today. `false` entries are the ledger.
    shipped: bool,
}

/// Nine rows: three primitives × three flavors, matching the nine
/// `coverage.json` rows the queue family was split into.
const LEDGER: &[Flavor] = &[
    Flavor {
        name: "QueueCell/single-threaded",
        marker_type: "pub struct QueueCell",
        shipped: true,
    },
    Flavor {
        name: "QueueCell/thread-safe",
        marker_type: "ThreadSafeQueueCell",
        shipped: true,
    },
    Flavor {
        name: "QueueCell/async",
        marker_type: "AsyncQueueCell",
        shipped: true,
    },
    Flavor {
        name: "TopicCell/single-threaded",
        marker_type: "pub struct TopicCell",
        shipped: true,
    },
    Flavor {
        name: "TopicCell/thread-safe",
        marker_type: "ThreadSafeTopicCell",
        shipped: true,
    },
    Flavor {
        name: "TopicCell/async",
        marker_type: "AsyncTopicCell",
        shipped: true,
    },
    Flavor {
        name: "WorkQueueCell/single-threaded",
        marker_type: "pub struct WorkQueueCell",
        shipped: true,
    },
    Flavor {
        name: "WorkQueueCell/thread-safe",
        marker_type: "ThreadSafeWorkQueueCell",
        shipped: true,
    },
    Flavor {
        name: "WorkQueueCell/async",
        marker_type: "AsyncWorkQueueCell",
        shipped: true,
    },
];

/// Every fixture the family owns, across all three primitives.
fn all_fixtures() -> Vec<&'static str> {
    QUEUE_FIXTURES
        .iter()
        .chain(TOPIC_FIXTURES)
        .chain(WORK_QUEUE_FIXTURES)
        .copied()
        .collect()
}

fn spec_fixtures_present() -> bool {
    Path::new(&format!("{SPEC_DIR}/{}", QUEUE_FIXTURES[0])).exists()
}

/// Concatenated `src/` contents. Grepping the source is what makes the ledger
/// self-enforcing: it observes what the crate actually defines rather than what a
/// comment claims.
fn crate_sources() -> String {
    let mut out = String::new();
    let mut stack = vec![Path::new("src").to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs")
                && let Ok(text) = crate::common::spec_read_to_string(&path)
            {
                out.push_str(&text);
            }
        }
    }
    out
}

/// **The ledger is enforced, not advisory.**
///
/// Every flavor recorded as unshipped must genuinely be absent from `src/`. When
/// Phase 3 lands `ThreadSafeQueueCell` or `AsyncQueueCell`, this fails and says
/// what to do — so a newly-shipped flavor cannot sit silently unreplayed while
/// the suite reports green.
#[test]
fn unshipped_flavors_are_really_absent() {
    let sources = crate_sources();
    assert!(
        !sources.is_empty(),
        "read no crate sources from src/; the ledger check would be vacuous"
    );

    for flavor in LEDGER {
        let defined = sources.contains(flavor.marker_type);
        if flavor.shipped {
            assert!(
                defined,
                "flavor `{}` is recorded as shipped but `{}` is not defined in src/ — \
                 the ledger claims coverage this crate does not have",
                flavor.name, flavor.marker_type
            );
        } else {
            assert!(
                !defined,
                "flavor `{}` now EXISTS in src/ (`{}`) but the queue-family ledger \
                 still records it as unshipped, so the canonical corpus is not being \
                 replayed against it.\n\n\
                 Fix: flip `shipped: true` for `{}` in tests/queue_family_conformance.rs \
                 and extend the replay to drive it, exactly as \
                 collections_family_conformance.rs drives all three map flavors. \
                 Do NOT flip the flag alone — that would restore the false green this \
                 test exists to prevent.",
                flavor.name, flavor.marker_type, flavor.name
            );
        }
    }
}

/// The ledger can never be all-skips.
///
/// zig's reactive-graph runner established this rule for the family: a runner
/// that skips everything must fail, because "skipped" and "passed" are
/// indistinguishable in a summary line.
#[test]
fn ledger_is_not_all_skips() {
    let shipped = LEDGER.iter().filter(|f| f.shipped).count();
    assert!(
        shipped > 0,
        "every queue flavor is recorded as unshipped, so this suite would assert \
         nothing while still reporting success"
    );
    assert_eq!(
        LEDGER.len(),
        9,
        "the ledger must cover three primitives × three execution flavors, matching \
         the nine coverage.json rows; a missing entry is an unscored gap, not an \
         absent one"
    );
}

/// **Positive proof the shipped flavor read the corpus.**
///
/// An absence guard proves the fixtures exist on disk. It cannot prove this
/// binary opened them, which is how a vacuous replay reports green. Count the
/// fixtures and the steps, and require both to be non-zero.
#[test]
fn shipped_flavor_replays_the_corpus() {
    if !spec_fixtures_present() {
        eprintln!("lazily-spec conformance fixtures not found at {SPEC_DIR}; skipping.");
        return;
    }

    let mut fixtures_read = 0usize;
    let mut steps_seen = 0usize;
    let mut matrices_seen = 0usize;
    let declared = all_fixtures();

    for name in &declared {
        let text = crate::common::spec_read_to_string(format!("{SPEC_DIR}/{name}"))
            .unwrap_or_else(|e| panic!("read {name}: {e}"));
        let fixture: serde_json::Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {name}: {e}"));
        fixtures_read += 1;

        let steps = fixture
            .get("steps")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("{name}: no steps array"));
        assert!(
            !steps.is_empty(),
            "{name}: fixture has no steps — a vacuous replay would report green"
        );
        steps_seen += steps.len();

        // The invalidation matrix nests under `expected`, NOT on the step.
        // lazily-rs's *map* runner read it off the step, so it was always absent
        // and the assertion never ran once. Assert the nesting here so that
        // regression cannot recur silently in the queue corpus.
        for (i, step) in steps.iter().enumerate() {
            let expected = step
                .get("expected")
                .unwrap_or_else(|| panic!("{name} step {i}: no expected block"));
            assert!(
                step.get("invalidates").is_none(),
                "{name} step {i}: `invalidates` appears at STEP level. The runners read \
                 expected.invalidates; a step-level copy would be silently ignored."
            );
            if expected.get("invalidates").is_some() {
                matrices_seen += 1;
            }
        }
    }

    assert_eq!(
        fixtures_read,
        declared.len(),
        "did not read every declared queue-family fixture"
    );
    assert_eq!(
        declared.len(),
        11,
        "the queue family owns eleven canonical fixtures (5 queue + 4 topic + \
         2 work-queue); a shrinking list is coverage silently dropped"
    );
    assert!(
        steps_seen > 0,
        "read the corpus but saw zero steps across all fixtures"
    );
    assert!(
        matrices_seen > 0,
        "no fixture carried an expected.invalidates matrix — the reader-kind \
         independence contract would be unasserted"
    );
}

// -- Thread-safe flavor: the canonical corpus, actually replayed ---------------
//
// The ledger flag above went `true` in the same change that added the replay
// below, and that pairing is the whole point. Flipping the flag alone would
// silence the absence guard while testing nothing — the precise failure this file
// was built to catch. `shipped` must mean "the corpus runs against it".
//
// This replays the same `queuecell_*.json` fixtures `queue_conformance.rs` runs
// against the single-threaded shell, but through `ThreadSafeQueueCell`, asserting
// the reader-kind values AND the `invalidates` matrix per step. `invalidates`
// lives under `expected`; a runner reading it at step level would find nothing.
#[cfg(feature = "thread-safe")]
mod thread_safe_flavor {
    use super::{QUEUE_FIXTURES, SPEC_DIR, spec_fixtures_present};
    use lazily::{ThreadSafeContext, ThreadSafeQueueCell};
    use serde_json::Value;

    type V = String;

    struct Readers {
        head: lazily::Computed<Option<V>>,
        len: lazily::Computed<usize>,
        is_empty: lazily::Computed<bool>,
        is_full: lazily::Computed<bool>,
        closed: lazily::Computed<bool>,
    }

    // Derived readers OVER the queue's readers, so "was it invalidated" is a real
    // question about a graph node rather than about a cached number.
    fn make_readers(ctx: &ThreadSafeContext, q: &ThreadSafeQueueCell<V>) -> Readers {
        let (a, b, c, d, e) = (q.clone(), q.clone(), q.clone(), q.clone(), q.clone());
        Readers {
            head: ctx.computed(move |cx| a.head(cx)),
            len: ctx.computed(move |cx| b.len(cx)),
            is_empty: ctx.computed(move |cx| c.is_empty(cx)),
            is_full: ctx.computed(move |cx| d.is_full(cx)),
            closed: ctx.computed(move |cx| e.closed(cx)),
        }
    }

    fn materialize(ctx: &ThreadSafeContext, r: &Readers) {
        let _ = ctx.get(&r.head);
        let _ = ctx.get(&r.len);
        let _ = ctx.get(&r.is_empty);
        let _ = ctx.get(&r.is_full);
        let _ = ctx.get(&r.closed);
    }

    fn replay(name: &str) -> usize {
        let text = crate::common::spec_read_to_string(format!("{SPEC_DIR}/{name}"))
            .unwrap_or_else(|e| panic!("canonical fixture {name} unreadable: {e}"));
        let fixture: Value = serde_json::from_str(&text).expect("fixture parses");
        let initial = &fixture["initial"];

        let ctx = ThreadSafeContext::new();
        let q = match initial["capacity"].as_u64() {
            Some(cap) => ThreadSafeQueueCell::<V>::with_capacity(&ctx, cap as usize),
            None => ThreadSafeQueueCell::<V>::new(&ctx),
        };
        assert!(
            initial["elements"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true),
            "this runner does not seed initial.elements; a fixture needing it must \
             extend the runner rather than be skipped"
        );

        let r = make_readers(&ctx, &q);
        let steps = fixture["steps"].as_array().expect("steps array");
        assert!(!steps.is_empty(), "a replay of zero steps is not a replay");

        for (i, step) in steps.iter().enumerate() {
            materialize(&ctx, &r);

            let op = &step["op"];
            let ty = op["type"].as_str().expect("op type");
            let got_returns: Option<String> = match ty {
                "push" | "try_push" => {
                    let v = op["value"].as_str().expect("push value").to_string();
                    match q.try_push(&ctx, v) {
                        Ok(()) => Some("Ok".into()),
                        Err(e) => Some(format!("{e:?}")),
                    }
                }
                "pop" | "try_pop" => match q.try_pop(&ctx) {
                    Ok(v) => Some(v),
                    Err(e) => Some(format!("{e:?}")),
                },
                "close" => {
                    q.close(&ctx);
                    None
                }
                "batch" => {
                    let inner = op["ops"].as_array().expect("batch ops");
                    ctx.batch(|_| {
                        for sub in inner {
                            assert_eq!(
                                sub["type"].as_str(),
                                Some("push"),
                                "batch currently only wraps pushes"
                            );
                            let v = sub["value"].as_str().expect("value").to_string();
                            q.try_push(&ctx, v).expect("batched push");
                        }
                    });
                    None
                }
                other => panic!("{name} step {i}: unhandled op `{other}`"),
            };

            // Guard the step's `expected` block (`#lzassertunknownkeys`): a key
            // this flavor's replay never reads fails the fixture instead of
            // passing unnoticed.
            let expected = crate::Expect::new(
                format!("{SPEC_DIR}/{name}"),
                format!("steps[{i}].expected"),
                &step["expected"],
            );

            // `invalidates` BEFORE any read — reading revalidates. DESCENT
            // (`#lzsubblockkeyset`): a reader kind the corpus adds must fail as
            // an unconsumed key, not vanish past a fixed list of five.
            if let Some(inv) = expected.sub_if_present("invalidates") {
                for (key, node_valid) in [
                    ("head", ctx.is_set(&r.head)),
                    ("len", ctx.is_set(&r.len)),
                    ("is_empty", ctx.is_set(&r.is_empty)),
                    ("is_full", ctx.is_set(&r.is_full)),
                    ("closed", ctx.is_set(&r.closed)),
                ] {
                    inv.assert_key_if_present(key, |want| {
                        assert_eq!(
                            !node_valid,
                            want.as_bool().expect("invalidates flag"),
                            "{name} step {i}: invalidates.{key} — thread-safe flavor \
                             disagrees with the canonical fixture"
                        );
                    });
                }
            }

            if let Some(want) = step.get("returns").and_then(|v| v.as_str()) {
                let got = got_returns.as_deref().unwrap_or("");
                assert!(
                    got == want || got.starts_with(want),
                    "{name} step {i}: returns `{got}`, fixture says `{want}`"
                );
            }

            expected.assert_key_if_present("len", |want| {
                assert_eq!(
                    q.len(&ctx) as u64,
                    want.as_u64().expect("len"),
                    "{name} step {i}: len"
                );
            });
            expected.assert_key_if_present("is_empty", |want| {
                assert_eq!(
                    q.is_empty(&ctx),
                    want.as_bool().expect("is_empty"),
                    "{name} step {i}: is_empty"
                );
            });
            expected.assert_key_if_present("is_full", |want| {
                assert_eq!(
                    q.is_full(&ctx),
                    want.as_bool().expect("is_full"),
                    "{name} step {i}: is_full"
                );
            });
            expected.assert_key_if_present("closed", |want| {
                assert_eq!(
                    q.closed(&ctx),
                    want.as_bool().expect("closed"),
                    "{name} step {i}: closed"
                );
            });
            expected.assert_key_if_present("head", |want| match want {
                Value::String(want) => assert_eq!(
                    q.head(&ctx).as_deref(),
                    Some(want.as_str()),
                    "{name} step {i}: head"
                ),
                Value::Null => assert_eq!(q.head(&ctx), None, "{name} step {i}: head"),
                other => panic!("{name} step {i}: head must be a string or null, got {other}"),
            });
            // `elements`: the whole buffered FIFO sequence. The single-threaded
            // replay always asserted it; this flavor never read the key, so the
            // total-order claim went unchecked on exactly the flavor where it is
            // hardest to get right (`#lzassertunknownkeys`).
            expected.assert_key_if_present("elements", |want| {
                let want: Vec<String> = want
                    .as_array()
                    .expect("elements")
                    .iter()
                    .map(|v| v.as_str().expect("element").to_string())
                    .collect();
                assert_eq!(q.elements(), want, "{name} step {i}: elements");
            });
        }
        steps.len()
    }

    #[test]
    fn thread_safe_flavor_replays_the_canonical_corpus() {
        if !spec_fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let mut total = 0;
        for f in QUEUE_FIXTURES {
            total += replay(f);
        }
        // Positive proof, not an absence guard: a replay that loaded nothing would
        // otherwise print the same success.
        assert!(
            total >= 25,
            "thread-safe flavor replayed only {total} steps across \
             {} fixtures — too few to be the real corpus",
            QUEUE_FIXTURES.len()
        );
    }

    // Atomic invalidation needs an observer that runs DURING the op, not a reader
    // inspected after it. The step replay above cannot see this: each reader ends up
    // cleared either way, so batching only changes how many frontier walks happened
    // in between — invisible to anything that looks afterwards.
    //
    // An effect subscribed to two reader kinds that transition together can see it.
    // A pop that takes a full bounded queue off capacity changes `len` AND
    // `is_full`; batched, the effect reruns once, and a subscriber never observes
    // `len` decremented while `is_full` still reads true. Unbatched it can rerun
    // twice, which IS the glitch.
    #[test]
    fn one_op_invalidates_reader_kinds_atomically() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ctx = ThreadSafeContext::new();
        let q = ThreadSafeQueueCell::<V>::with_capacity(&ctx, 2);
        q.try_push(&ctx, "a".into()).expect("push a");
        q.try_push(&ctx, "b".into()).expect("push b");
        assert!(
            q.is_full(&ctx),
            "queue must be at capacity before the probe"
        );

        let runs = Arc::new(AtomicUsize::new(0));
        {
            let (q, runs) = (q.clone(), Arc::clone(&runs));
            ctx.effect(move |cx| {
                runs.fetch_add(1, Ordering::SeqCst);
                // Observe BOTH kinds that this pop transitions.
                let _ = q.len(cx);
                let _ = q.is_full(cx);
            });
        }
        let baseline = runs.load(Ordering::SeqCst);
        assert!(baseline >= 1, "effect must run once on creation");

        // This pop changes len (2 -> 1) and is_full (true -> false) together.
        q.try_pop(&ctx).expect("pop");

        assert_eq!(
            runs.load(Ordering::SeqCst) - baseline,
            1,
            "one op must rerun a two-kind subscriber exactly ONCE; more means the \
             reader kinds were invalidated in separate frontier walks and a \
             subscriber can observe len decremented while is_full is still stale"
        );
        assert!(!q.is_full(&ctx), "pop must take the queue off capacity");
        assert_eq!(q.len(&ctx), 1);
    }

    // A lock-order inversion between the storage mutex and the context lock is
    // invisible single-threaded and manifests as a HANG, not a failure — so it
    // needs a concurrent probe. Ops take storage then release it before touching
    // the context; readers take the context then storage. If an op ever
    // invalidated while still holding storage, this deadlocks.
    #[test]
    fn concurrent_push_and_read_do_not_deadlock() {
        use std::sync::Arc;
        use std::thread;

        let ctx = Arc::new(ThreadSafeContext::new());
        let q = ThreadSafeQueueCell::<V>::new(&ctx);

        let writers: Vec<_> = (0..4)
            .map(|w| {
                let (ctx, q) = (Arc::clone(&ctx), q.clone());
                thread::spawn(move || {
                    for i in 0..50 {
                        q.try_push(&ctx, format!("w{w}-{i}"))
                            .expect("unbounded push");
                    }
                })
            })
            .collect();
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (ctx, q) = (Arc::clone(&ctx), q.clone());
                thread::spawn(move || {
                    for _ in 0..50 {
                        let _ = q.len(&ctx);
                        let _ = q.head(&ctx);
                        let _ = q.is_empty(&ctx);
                    }
                })
            })
            .collect();

        for t in writers.into_iter().chain(readers) {
            t.join().expect("no thread panicked or deadlocked");
        }
        assert_eq!(q.len(&ctx), 200, "every push must be visible after joining");
    }
}

// -- Async flavor: the same corpus, through AsyncQueueCell ---------------------
//
// Same rule as the thread-safe flavor above: the ledger flag went `true` in the
// change that added this replay, never before it.
//
// Note what is NOT here — a settle step. `AsyncQueueCell`'s reader kinds are built
// on `AsyncContext::computed` (synchronous compute), so they resolve inline on
// read. Ordering is not async-coloured: a queue's length is not something you wait
// for. If these reads ever start returning `None`, that is the regression, not a
// reason to add an await.
#[cfg(feature = "async")]
mod async_flavor {
    use super::{QUEUE_FIXTURES, SPEC_DIR, spec_fixtures_present};
    use lazily::{AsyncContext, AsyncQueueCell};
    use serde_json::Value;

    type V = String;

    // The queue's OWN reader nodes, not derives over them. Asserting `is_set` here
    // asks whether the library cleared the node it is responsible for — the same
    // check `work_queue_conformance.rs` makes against the single-threaded flavor.
    fn materialize(ctx: &AsyncContext, r: &lazily::AsyncQueueReaderHandles<V>) {
        let _ = ctx.get(&r.head);
        let _ = ctx.get(&r.len);
        let _ = ctx.get(&r.is_empty);
        let _ = ctx.get(&r.is_full);
    }

    fn replay(name: &str) -> usize {
        let text = crate::common::spec_read_to_string(format!("{SPEC_DIR}/{name}"))
            .unwrap_or_else(|e| panic!("canonical fixture {name} unreadable: {e}"));
        let fixture: Value = serde_json::from_str(&text).expect("fixture parses");
        let initial = &fixture["initial"];

        let ctx = AsyncContext::new();
        let q = match initial["capacity"].as_u64() {
            Some(cap) => AsyncQueueCell::<V>::with_capacity(&ctx, cap as usize),
            None => AsyncQueueCell::<V>::new(&ctx),
        };
        assert!(
            initial["elements"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true),
            "this runner does not seed initial.elements"
        );

        let r = q.reader_handles();
        let steps = fixture["steps"].as_array().expect("steps array");
        assert!(!steps.is_empty(), "a replay of zero steps is not a replay");

        for (i, step) in steps.iter().enumerate() {
            materialize(&ctx, &r);
            let op = &step["op"];
            let ty = op["type"].as_str().expect("op type");
            let got_returns: Option<String> = match ty {
                "push" | "try_push" => {
                    let v = op["value"].as_str().expect("push value").to_string();
                    match q.try_push(&ctx, v) {
                        Ok(()) => Some("Ok".into()),
                        Err(e) => Some(format!("{e:?}")),
                    }
                }
                "pop" | "try_pop" => match q.try_pop(&ctx) {
                    Ok(v) => Some(v),
                    Err(e) => Some(format!("{e:?}")),
                },
                "close" => {
                    q.close(&ctx);
                    None
                }
                "batch" => {
                    let inner = op["ops"].as_array().expect("batch ops");
                    for sub in inner {
                        assert_eq!(sub["type"].as_str(), Some("push"));
                        let v = sub["value"].as_str().expect("value").to_string();
                        q.try_push(&ctx, v).expect("batched push");
                    }
                    None
                }
                other => panic!("{name} step {i}: unhandled op `{other}`"),
            };

            // Guard the step's `expected` block (`#lzassertunknownkeys`): a key
            // this flavor's replay never reads fails the fixture instead of
            // passing unnoticed.
            let expected = crate::Expect::new(
                format!("{SPEC_DIR}/{name}"),
                format!("steps[{i}].expected"),
                &step["expected"],
            );

            // `invalidates` BEFORE any read — reading revalidates. `closed` is a
            // source rather than a derive, so it is asserted by value below.
            // DESCENT (`#lzsubblockkeyset`), as above.
            if let Some(inv) = expected.sub_if_present("invalidates") {
                for (key, node_valid) in [
                    ("head", ctx.is_set(&r.head)),
                    ("len", ctx.is_set(&r.len)),
                    ("is_empty", ctx.is_set(&r.is_empty)),
                    ("is_full", ctx.is_set(&r.is_full)),
                ] {
                    inv.assert_key_if_present(key, |want| {
                        assert_eq!(
                            !node_valid,
                            want.as_bool().expect("invalidates flag"),
                            "{name} step {i}: invalidates.{key} — async flavor \
                             disagrees with the canonical fixture"
                        );
                    });
                }
                // `closed` is a source rather than a derive in the async
                // flavor, so it is asserted by VALUE below rather than by
                // cache validity; the key is excused here so the omission is
                // written down instead of silently skipped.
                if inv.raw().get("closed").is_some() {
                    inv.excuse_key(
                        "closed",
                        "async flavor models `closed` as a source, not a derived reader; \
                         asserted by value below",
                    );
                }
            }

            if let Some(want) = step.get("returns").and_then(|v| v.as_str()) {
                let got = got_returns.as_deref().unwrap_or("");
                assert!(
                    got == want || got.starts_with(want),
                    "{name} step {i}: returns `{got}`, fixture says `{want}`"
                );
            }
            expected.assert_key_if_present("len", |want| {
                assert_eq!(
                    q.len(&ctx) as u64,
                    want.as_u64().expect("len"),
                    "{name} step {i}: len"
                );
            });
            expected.assert_key_if_present("is_empty", |want| {
                assert_eq!(
                    q.is_empty(&ctx),
                    want.as_bool().expect("is_empty"),
                    "{name} step {i}: is_empty"
                );
            });
            expected.assert_key_if_present("is_full", |want| {
                assert_eq!(
                    q.is_full(&ctx),
                    want.as_bool().expect("is_full"),
                    "{name} step {i}: is_full"
                );
            });
            expected.assert_key_if_present("closed", |want| {
                assert_eq!(
                    q.closed(&ctx),
                    want.as_bool().expect("closed"),
                    "{name} step {i}: closed"
                );
            });
            expected.assert_key_if_present("head", |want| match want {
                Value::String(want) => assert_eq!(
                    q.head(&ctx).as_deref(),
                    Some(want.as_str()),
                    "{name} step {i}: head"
                ),
                Value::Null => assert_eq!(q.head(&ctx), None, "{name} step {i}: head"),
                other => panic!("{name} step {i}: head must be a string or null, got {other}"),
            });
            // `elements`: the whole buffered FIFO sequence. The single-threaded
            // replay always asserted it; this flavor never read the key, so the
            // total-order claim went unchecked on exactly the flavor where it is
            // hardest to get right (`#lzassertunknownkeys`).
            expected.assert_key_if_present("elements", |want| {
                let want: Vec<String> = want
                    .as_array()
                    .expect("elements")
                    .iter()
                    .map(|v| v.as_str().expect("element").to_string())
                    .collect();
                assert_eq!(q.elements(), want, "{name} step {i}: elements");
            });
        }
        steps.len()
    }

    #[test]
    fn async_flavor_replays_the_canonical_corpus() {
        if !spec_fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let mut total = 0;
        for f in QUEUE_FIXTURES {
            total += replay(f);
        }
        assert!(
            total >= 25,
            "async flavor replayed only {total} steps — too few to be the real corpus"
        );
    }

    // The claim that ordering is not async-coloured, made falsifiable: every reader
    // kind yields a value on a bare read, with nothing driven and nothing awaited.
    #[test]
    fn reader_kinds_resolve_without_being_driven() {
        let ctx = AsyncContext::new();
        let q = AsyncQueueCell::<V>::with_capacity(&ctx, 2);
        q.try_push(&ctx, "a".into()).expect("push");

        assert_eq!(q.len(&ctx), 1);
        assert_eq!(q.head(&ctx).as_deref(), Some("a"));
        assert!(!q.is_empty(&ctx));
        assert!(!q.is_full(&ctx));
        assert!(!q.closed(&ctx));

        q.try_push(&ctx, "b".into()).expect("push");
        assert!(q.is_full(&ctx), "second push must reach capacity");
        q.try_pop(&ctx).expect("pop");
        assert!(!q.is_full(&ctx), "pop must take it off capacity");
        assert_eq!(q.head(&ctx).as_deref(), Some("b"), "head follows the pop");
    }
}

// -- TopicCell: the canonical corpus, replayed against all three flavors -------
//
// Before this module lazily-rs never opened `topiccell_*.json` at all.
// `topic_conformance.rs` hand-transcribes the same scenarios as Rust asserts,
// which reads as coverage and is not: a fixture nobody loads cannot detect drift
// from the other eight bindings, and the transcription can quietly disagree with
// the JSON it was copied from.
//
// The flavor axis lives in the runner, not the corpus: one `TopicModel` trait,
// one replay, three implementations. The fixtures carry no execution-model field
// and should not — that is the same shape zig's `Engine(comptime Model)` uses for
// the reactive-graph corpus.
mod topic_flavors {
    use super::{SPEC_DIR, TOPIC_FIXTURES, spec_fixtures_present};
    use lazily::{TopicDurability, TopicSnapshot, TopicSubscriptionSnapshot};
    use serde_json::Value;
    use std::collections::{BTreeSet, HashMap};

    /// The Core surface every flavor must present. Nothing here is async-coloured:
    /// a subscription cursor is monotone and per-subscriber, and the unread suffix
    /// derives from a retained log the graph does not own, so there is nothing to
    /// await and no `settle` method on this trait.
    pub trait TopicModel: Sized {
        /// A restart is a fresh graph over preserved durable state, so this both
        /// constructs and re-mints readers.
        fn from_snapshot(snapshot: TopicSnapshot<String>) -> Self;
        fn subscribe(&self, id: &str, durability: TopicDurability);
        fn reconnect(&self, id: &str);
        fn disconnect(&self, id: &str) -> bool;
        fn publish(&self, value: String) -> u64;
        fn advance(&self, id: &str) -> Option<String>;
        fn gc(&self) -> usize;
        /// Reactive read; also materializes the reader.
        fn read_stream(&self, id: &str) -> Vec<String>;
        /// `false` when the reader is invalidated **or absent** — an absent reader
        /// has no valid cached value, which is what the fixture means by `true`.
        fn is_reader_valid(&self, id: &str) -> bool;
        fn base_offset(&self) -> u64;
        fn elements(&self) -> Vec<String>;
        fn subscription(&self, id: &str) -> Option<TopicSubscriptionSnapshot>;
        fn snapshot(&self) -> TopicSnapshot<String>;
    }

    fn durability_of(value: &Value) -> TopicDurability {
        match value.as_str().expect("durability string") {
            "durable" => TopicDurability::Durable,
            "ephemeral" => TopicDurability::Ephemeral,
            other => panic!("unknown durability `{other}`"),
        }
    }

    fn snapshot_from(initial: &Value) -> TopicSnapshot<String> {
        let mut subscriptions = HashMap::new();
        if let Some(map) = initial["subscriptions"].as_object() {
            for (id, sub) in map {
                subscriptions.insert(
                    id.clone(),
                    TopicSubscriptionSnapshot {
                        cursor: sub["cursor"].as_u64().expect("cursor"),
                        durability: durability_of(&sub["durability"]),
                        connected: sub["connected"].as_bool().expect("connected"),
                    },
                );
            }
        }
        TopicSnapshot {
            base_offset: initial["base_offset"].as_u64().unwrap_or(0),
            elements: initial["elements"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().expect("element string").to_owned())
                        .collect()
                })
                .unwrap_or_default(),
            subscriptions,
        }
    }

    fn strings(value: &Value) -> Vec<String> {
        value
            .as_array()
            .expect("array")
            .iter()
            .map(|v| v.as_str().expect("string").to_owned())
            .collect()
    }

    /// Returns the number of steps replayed, so the caller can prove it ran.
    pub fn replay<M: TopicModel>(name: &str, flavor: &str) -> usize {
        let text = crate::common::spec_read_to_string(format!("{SPEC_DIR}/{name}"))
            .unwrap_or_else(|e| panic!("canonical fixture {name} unreadable: {e}"));
        let fixture: Value = serde_json::from_str(&text).expect("fixture parses");

        let initial = snapshot_from(&fixture["initial"]);
        let mut known: BTreeSet<String> = initial.subscriptions.keys().cloned().collect();
        let mut topic = M::from_snapshot(initial);

        let steps = fixture["steps"].as_array().expect("steps array");
        assert!(!steps.is_empty(), "a replay of zero steps is not a replay");

        for (i, step) in steps.iter().enumerate() {
            // Every known reader is materialized BEFORE the op, so a post-op
            // `is_reader_valid` of false means this op cleared it — not that it
            // was never read.
            for id in &known {
                let _ = topic.read_stream(id);
            }

            let op = &step["op"];
            let ty = op["type"].as_str().expect("op type");
            if let Some(subscriber) = op.get("subscriber").and_then(|v| v.as_str()) {
                known.insert(subscriber.to_owned());
            }

            let returns: Option<Value> = match ty {
                "publish" => {
                    let v = op["value"].as_str().expect("publish value").to_owned();
                    Some(Value::from(topic.publish(v)))
                }
                "subscribe" => {
                    topic.subscribe(
                        op["subscriber"].as_str().expect("subscriber"),
                        durability_of(&op["durability"]),
                    );
                    None
                }
                "reconnect" => {
                    topic.reconnect(op["subscriber"].as_str().expect("subscriber"));
                    None
                }
                "disconnect" => {
                    topic.disconnect(op["subscriber"].as_str().expect("subscriber"));
                    None
                }
                "advance" => topic
                    .advance(op["subscriber"].as_str().expect("subscriber"))
                    .map(Value::from),
                "gc" => Some(Value::from(topic.gc() as u64)),
                "restart" => {
                    // A restart rebuilds the graph from durable state. Its fixture
                    // expectation is `invalidates: false` everywhere, which a fresh
                    // set of unread nodes could never satisfy — so materializing
                    // them is part of the op, exactly as construction materializes
                    // before step 0. What actually discriminates a restart that
                    // LOST a cursor is `reads` and `subscriptions` below, both of
                    // which are asserted on this same step.
                    topic = M::from_snapshot(topic.snapshot());
                    for id in &known {
                        let _ = topic.read_stream(id);
                    }
                    None
                }
                other => panic!("{flavor} {name} step {i}: unhandled op `{other}`"),
            };

            // Guard the step's `expected` block (`#lzassertunknownkeys`): a key
            // this flavor's replay never reads fails the fixture instead of
            // passing unnoticed.
            let expected = crate::Expect::new(
                format!("{SPEC_DIR}/{name}"),
                format!("steps[{i}].expected"),
                &step["expected"],
            );

            // `invalidates` BEFORE any read — a read revalidates the node.
            // DESCENT (`#lzsubblockkeyset`): the subscriber ids are the child's
            // keys, so each comparison is made inside the tracker.
            if let Some(inv) = expected.sub_if_present("invalidates") {
                for id in inv.raw().as_object().expect("invalidates object").keys() {
                    inv.assert_key_with(id.as_str(), |want| {
                        assert_eq!(
                            !topic.is_reader_valid(id),
                            want.as_bool().expect("invalidates flag"),
                            "{flavor} {name} step {i}: invalidates.{id} disagrees with \
                             the canonical fixture"
                        );
                    });
                }
            }
            assert!(
                step.get("invalidates").is_none(),
                "{name} step {i}: `invalidates` at STEP level would be silently \
                 ignored; the runner reads expected.invalidates"
            );

            match (step.get("returns"), returns) {
                (Some(Value::Null) | None, _) => {}
                (Some(want), Some(got)) => assert_eq!(
                    &got, want,
                    "{flavor} {name} step {i}: returns disagrees with the fixture"
                ),
                (Some(want), None) => {
                    panic!(
                        "{flavor} {name} step {i}: fixture expects `{want}`, op returned nothing"
                    )
                }
            }

            expected.assert_key_if_present("base_offset", |want| {
                assert_eq!(
                    topic.base_offset(),
                    want.as_u64().expect("base_offset"),
                    "{flavor} {name} step {i}: base_offset"
                );
            });
            expected.assert_key_if_present("elements", |want| {
                assert_eq!(
                    topic.elements(),
                    strings(want),
                    "{flavor} {name} step {i}: retained elements"
                );
            });
            // DESCENT twice over (`#lzsubblockkeyset`): `subscriptions` is keyed
            // by subscriber id, and each subscriber's value is itself a RECORD
            // whose keys are assertion names, so a field added to either level
            // fails as an unconsumed key instead of being compared by nothing.
            if let Some(subs) = expected.sub_if_present("subscriptions") {
                let ids: Vec<String> = subs
                    .raw()
                    .as_object()
                    .expect("subscriptions object")
                    .keys()
                    .cloned()
                    .collect();
                for id in &ids {
                    let got = topic.subscription(id).unwrap_or_else(|| {
                        panic!("{flavor} {name} step {i}: no subscription {id}")
                    });
                    let want = subs.sub(id);
                    let at = format!("{flavor} {name} step {i}: {id}");
                    want.assert_key_at("cursor", got.cursor, &at);
                    want.assert_key_at("connected", got.connected, &at);
                    want.assert_key_with("durability", |w| {
                        assert_eq!(got.durability, durability_of(w), "{at}.durability");
                    });
                    want.finish();
                }
                // A subscription the fixture dropped must really be gone —
                // otherwise an ephemeral disconnect that forgot to remove the
                // record would still pass every positive assertion above.
                for id in &known {
                    if !ids.contains(id) {
                        assert!(
                            topic.subscription(id).is_none(),
                            "{flavor} {name} step {i}: subscription {id} survived a \
                             step whose fixture no longer lists it"
                        );
                    }
                }
            }
            // DESCENT (`#lzsubblockkeyset`): the subscriber ids are the child's
            // keys, so each stream comparison happens inside the tracker.
            if let Some(reads) = expected.sub_if_present("reads") {
                let ids: Vec<String> = reads
                    .raw()
                    .as_object()
                    .expect("reads object")
                    .keys()
                    .cloned()
                    .collect();
                for id in &ids {
                    reads.assert_key_with(id.as_str(), |want| {
                        assert_eq!(
                            topic.read_stream(id),
                            strings(want),
                            "{flavor} {name} step {i}: {id} read stream"
                        );
                    });
                }
            }
        }
        steps.len()
    }

    /// The corpus, once per flavor. Returns the total replayed steps so each
    /// flavor's test can assert it is non-zero and plausible.
    pub fn replay_corpus<M: TopicModel>(flavor: &str) -> usize {
        TOPIC_FIXTURES
            .iter()
            .map(|name| replay::<M>(name, flavor))
            .sum()
    }

    /// Every flavor must clear this bar, so "the async one ran two steps" cannot
    /// hide behind a green summary line.
    pub const MIN_STEPS: usize = 29;

    pub fn fixtures_present() -> bool {
        spec_fixtures_present()
    }
}

/// Single-threaded `TopicCell` — the reference the other two flavors mirror.
mod topic_sync {
    use super::topic_flavors::{MIN_STEPS, TopicModel, fixtures_present, replay_corpus};
    use lazily::{Context, TopicCell, TopicDurability, TopicSnapshot, TopicSubscriptionSnapshot};

    struct Model {
        ctx: Context,
        cell: TopicCell<String>,
    }

    impl TopicModel for Model {
        fn from_snapshot(snapshot: TopicSnapshot<String>) -> Self {
            let ctx = Context::new();
            let cell = TopicCell::<String>::from_snapshot(&ctx, snapshot);
            Self { ctx, cell }
        }
        fn subscribe(&self, id: &str, durability: TopicDurability) {
            self.cell.subscribe(&self.ctx, id.to_owned(), durability);
        }
        fn reconnect(&self, id: &str) {
            self.cell.reconnect(&self.ctx, id.to_owned());
        }
        fn disconnect(&self, id: &str) -> bool {
            self.cell.disconnect(&self.ctx, &id.to_owned())
        }
        fn publish(&self, value: String) -> u64 {
            self.cell.publish(&self.ctx, value)
        }
        fn advance(&self, id: &str) -> Option<String> {
            self.cell.advance(&self.ctx, &id.to_owned())
        }
        fn gc(&self) -> usize {
            self.cell.gc()
        }
        fn read_stream(&self, id: &str) -> Vec<String> {
            self.cell.read_stream(&self.ctx, &id.to_owned())
        }
        fn is_reader_valid(&self, id: &str) -> bool {
            self.cell
                .reader_handle(&id.to_owned())
                .is_some_and(|handle| self.ctx.is_set(&handle))
        }
        fn base_offset(&self) -> u64 {
            self.cell.base_offset()
        }
        fn elements(&self) -> Vec<String> {
            self.cell.elements()
        }
        fn subscription(&self, id: &str) -> Option<TopicSubscriptionSnapshot> {
            self.cell.subscription(&id.to_owned())
        }
        fn snapshot(&self) -> TopicSnapshot<String> {
            self.cell.snapshot()
        }
    }

    #[test]
    fn single_threaded_topic_replays_the_canonical_corpus() {
        if !fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let total = replay_corpus::<Model>("single-threaded");
        assert!(
            total >= MIN_STEPS,
            "single-threaded topic replayed only {total} steps — too few to be the \
             real corpus"
        );
    }
}

/// `ThreadSafeTopicCell` — same corpus, same trait, `ThreadSafeContext` graph.
#[cfg(feature = "thread-safe")]
mod topic_thread_safe {
    use super::topic_flavors::{MIN_STEPS, TopicModel, fixtures_present, replay_corpus};
    use lazily::{
        ThreadSafeContext, ThreadSafeTopicCell, TopicDurability, TopicSnapshot,
        TopicSubscriptionSnapshot,
    };

    struct Model {
        ctx: ThreadSafeContext,
        cell: ThreadSafeTopicCell<String>,
    }

    impl TopicModel for Model {
        fn from_snapshot(snapshot: TopicSnapshot<String>) -> Self {
            let ctx = ThreadSafeContext::new();
            let cell = ThreadSafeTopicCell::<String>::from_snapshot(&ctx, snapshot);
            Self { ctx, cell }
        }
        fn subscribe(&self, id: &str, durability: TopicDurability) {
            self.cell.subscribe(&self.ctx, id.to_owned(), durability);
        }
        fn reconnect(&self, id: &str) {
            self.cell.reconnect(&self.ctx, id.to_owned());
        }
        fn disconnect(&self, id: &str) -> bool {
            self.cell.disconnect(&self.ctx, &id.to_owned())
        }
        fn publish(&self, value: String) -> u64 {
            self.cell.publish(&self.ctx, value)
        }
        fn advance(&self, id: &str) -> Option<String> {
            self.cell.advance(&self.ctx, &id.to_owned())
        }
        fn gc(&self) -> usize {
            self.cell.gc()
        }
        fn read_stream(&self, id: &str) -> Vec<String> {
            self.cell.read_stream(&self.ctx, &id.to_owned())
        }
        fn is_reader_valid(&self, id: &str) -> bool {
            self.cell
                .reader_handle(&id.to_owned())
                .is_some_and(|handle| self.ctx.is_set(&handle))
        }
        fn base_offset(&self) -> u64 {
            self.cell.base_offset()
        }
        fn elements(&self) -> Vec<String> {
            self.cell.elements()
        }
        fn subscription(&self, id: &str) -> Option<TopicSubscriptionSnapshot> {
            self.cell.subscription(&id.to_owned())
        }
        fn snapshot(&self) -> TopicSnapshot<String> {
            self.cell.snapshot()
        }
    }

    #[test]
    fn thread_safe_topic_replays_the_canonical_corpus() {
        if !fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let total = replay_corpus::<Model>("thread-safe");
        assert!(
            total >= MIN_STEPS,
            "thread-safe topic replayed only {total} steps — too few to be the real \
             corpus"
        );
    }

    // A publish fans out to N subscribers. Clearing them one at a time is N
    // frontier walks, and a subscriber watching two cursors can rerun twice for one
    // publish — the glitch. The step replay above cannot see this: both readers end
    // up cleared either way, so only an observer that runs DURING the op
    // discriminates.
    #[test]
    fn one_publish_invalidates_every_subscriber_atomically() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ctx = ThreadSafeContext::new();
        let topic = ThreadSafeTopicCell::<String>::new(&ctx);
        topic.subscribe(&ctx, "alpha".into(), TopicDurability::Durable);
        topic.subscribe(&ctx, "beta".into(), TopicDurability::Durable);

        let runs = Arc::new(AtomicUsize::new(0));
        {
            let (topic, runs) = (topic.clone(), Arc::clone(&runs));
            ctx.effect(move |cx| {
                runs.fetch_add(1, Ordering::SeqCst);
                let _ = topic.read_stream(cx, &"alpha".to_owned());
                let _ = topic.read_stream(cx, &"beta".to_owned());
            });
        }
        let baseline = runs.load(Ordering::SeqCst);
        assert!(baseline >= 1, "effect must run once on creation");

        topic.publish(&ctx, "x".into());

        assert_eq!(
            runs.load(Ordering::SeqCst) - baseline,
            1,
            "one publish must rerun a two-subscriber observer exactly ONCE; more \
             means the per-subscriber readers were cleared in separate frontier \
             walks and a subscriber can observe a half-delivered broadcast"
        );
        assert_eq!(topic.read_stream(&ctx, &"alpha".to_owned()), ["x"]);
        assert_eq!(topic.read_stream(&ctx, &"beta".to_owned()), ["x"]);
    }

    // A lock-order inversion between the core mutex and the context lock is
    // invisible single-threaded and manifests as a HANG. Publishers take core then
    // release it; readers take the context then core. If publish ever invalidated
    // while still holding core, this deadlocks.
    #[test]
    fn concurrent_publish_and_read_do_not_deadlock() {
        use std::sync::Arc;
        use std::thread;

        let ctx = Arc::new(ThreadSafeContext::new());
        let topic = ThreadSafeTopicCell::<String>::new(&ctx);
        for id in ["a", "b", "c", "d"] {
            topic.subscribe(&ctx, id.to_owned(), TopicDurability::Durable);
        }

        let writers: Vec<_> = (0..4)
            .map(|w| {
                let (ctx, topic) = (Arc::clone(&ctx), topic.clone());
                thread::spawn(move || {
                    for i in 0..50 {
                        topic.publish(&ctx, format!("w{w}-{i}"));
                    }
                })
            })
            .collect();
        let readers: Vec<_> = ["a", "b", "c", "d"]
            .map(|id| {
                let (ctx, topic) = (Arc::clone(&ctx), topic.clone());
                let id = id.to_owned();
                thread::spawn(move || {
                    for _ in 0..50 {
                        let _ = topic.read_stream(&ctx, &id);
                    }
                })
            })
            .into_iter()
            .collect();

        for t in writers.into_iter().chain(readers) {
            t.join().expect("no thread panicked or deadlocked");
        }
        assert_eq!(
            topic.read_stream(&ctx, &"a".to_owned()).len(),
            200,
            "every publish must be visible to every subscriber after joining"
        );
    }
}

/// `AsyncTopicCell` — same corpus, `AsyncContext` graph. Note the absence of a
/// settle step: cursors are not async-coloured.
#[cfg(feature = "async")]
mod topic_async {
    use super::topic_flavors::{MIN_STEPS, TopicModel, fixtures_present, replay_corpus};
    use lazily::{
        AsyncContext, AsyncTopicCell, TopicDurability, TopicSnapshot, TopicSubscriptionSnapshot,
    };

    struct Model {
        ctx: AsyncContext,
        cell: AsyncTopicCell<String>,
    }

    impl TopicModel for Model {
        fn from_snapshot(snapshot: TopicSnapshot<String>) -> Self {
            let ctx = AsyncContext::new();
            let cell = AsyncTopicCell::<String>::from_snapshot(&ctx, snapshot);
            Self { ctx, cell }
        }
        fn subscribe(&self, id: &str, durability: TopicDurability) {
            self.cell.subscribe(&self.ctx, id.to_owned(), durability);
        }
        fn reconnect(&self, id: &str) {
            self.cell.reconnect(&self.ctx, id.to_owned());
        }
        fn disconnect(&self, id: &str) -> bool {
            self.cell.disconnect(&self.ctx, &id.to_owned())
        }
        fn publish(&self, value: String) -> u64 {
            self.cell.publish(&self.ctx, value)
        }
        fn advance(&self, id: &str) -> Option<String> {
            self.cell.advance(&self.ctx, &id.to_owned())
        }
        fn gc(&self) -> usize {
            self.cell.gc()
        }
        fn read_stream(&self, id: &str) -> Vec<String> {
            self.cell.read_stream(&self.ctx, &id.to_owned())
        }
        fn is_reader_valid(&self, id: &str) -> bool {
            self.cell
                .reader_handle(&id.to_owned())
                .is_some_and(|handle| self.ctx.is_set(&handle))
        }
        fn base_offset(&self) -> u64 {
            self.cell.base_offset()
        }
        fn elements(&self) -> Vec<String> {
            self.cell.elements()
        }
        fn subscription(&self, id: &str) -> Option<TopicSubscriptionSnapshot> {
            self.cell.subscription(&id.to_owned())
        }
        fn snapshot(&self) -> TopicSnapshot<String> {
            self.cell.snapshot()
        }
    }

    #[test]
    fn async_topic_replays_the_canonical_corpus() {
        if !fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let total = replay_corpus::<Model>("async");
        assert!(
            total >= MIN_STEPS,
            "async topic replayed only {total} steps — too few to be the real corpus"
        );
    }

    // The claim that a cursor is not async-coloured, made falsifiable: every read
    // yields a value with nothing driven and nothing awaited.
    #[test]
    fn cursor_reads_resolve_without_being_driven() {
        let ctx = AsyncContext::new();
        let topic = AsyncTopicCell::<String>::new(&ctx);
        topic.subscribe(&ctx, "alpha".into(), TopicDurability::Durable);
        topic.publish(&ctx, "a".into());

        assert_eq!(topic.read_stream(&ctx, &"alpha".to_owned()), ["a"]);
        assert_eq!(topic.read(&ctx, &"alpha".to_owned()).as_deref(), Some("a"));
        assert_eq!(
            topic.advance(&ctx, &"alpha".to_owned()).as_deref(),
            Some("a")
        );
        assert!(topic.read_stream(&ctx, &"alpha".to_owned()).is_empty());
    }
}

// -- WorkQueueCell: the canonical corpus, replayed against all three flavors ---
//
// `work_queue_conformance.rs` drives the single-threaded flavor only. This module
// drives all three through one `WorkQueueModel` trait, so the thread-safe and
// async shells cannot diverge from the lifecycle the fixtures pin.
//
// Nothing here awaits, and `now` is a caller argument on every flavor: lease
// expiry is time-driven but the clock seam is not flavor-specific, and a flavor
// that owned a timer could not replay these fixtures deterministically at all.
mod work_queue_flavors {
    use super::{SPEC_DIR, WORK_QUEUE_FIXTURES, spec_fixtures_present};
    use lazily::{
        WorkQueueDeadLetter, WorkQueueDeadLetterReason, WorkQueueDelivery, WorkQueueItem,
    };
    use serde_json::Value;

    /// Reader-kind validity, in a fixed order so the replay can name them.
    pub const READER_KINDS: [&str; 4] = [
        "pending_len",
        "is_empty",
        "in_flight_len",
        "dead_letter_len",
    ];

    pub trait WorkQueueModel: Sized {
        fn new(visibility_timeout: u64, max_deliveries: u32) -> Self;
        fn push(&self, value: String) -> u64;
        fn claim(&self, worker: &str, now: u64) -> Option<WorkQueueDelivery<String>>;
        fn ack(&self, worker: &str, delivery_id: u64) -> bool;
        fn nack(&self, worker: &str, delivery_id: u64) -> bool;
        fn reap_expired(&self, now: u64) -> usize;
        /// Read every reader kind, so a post-op invalidation is attributable to
        /// the op rather than to never having been read.
        fn materialize(&self);
        /// `[pending_len, is_empty, in_flight_len, dead_letter_len]`, `true` when
        /// the node still holds a valid memo.
        fn reader_validity(&self) -> [bool; 4];
        /// `[pending_len, is_empty(as 0/1), in_flight_len, dead_letter_len]` read
        /// reactively — the values the fixture's `reads` block pins.
        fn reads(&self) -> (u64, bool, u64, u64);
        fn pending(&self) -> Vec<WorkQueueItem<String>>;
        fn in_flight(&self) -> Vec<WorkQueueDelivery<String>>;
        fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<String>>;
    }

    fn as_u64(value: &Value, label: &str) -> u64 {
        value
            .as_u64()
            .unwrap_or_else(|| panic!("{label} must be u64"))
    }

    fn assert_delivery(actual: &WorkQueueDelivery<String>, expected: &Value, label: &str) {
        assert_eq!(
            actual.delivery_id,
            as_u64(&expected["delivery_id"], "delivery_id"),
            "{label}: delivery_id"
        );
        assert_eq!(
            actual.item_id,
            as_u64(&expected["item_id"], "item_id"),
            "{label}: item_id"
        );
        assert_eq!(
            actual.value,
            expected["value"].as_str().expect("value"),
            "{label}: value"
        );
        assert_eq!(
            actual.worker,
            expected["worker"].as_str().expect("worker"),
            "{label}: worker"
        );
        assert_eq!(
            u64::from(actual.attempt),
            as_u64(&expected["attempt"], "attempt"),
            "{label}: attempt"
        );
        assert_eq!(
            actual.deadline,
            as_u64(&expected["deadline"], "deadline"),
            "{label}: deadline"
        );
    }

    pub fn replay<M: WorkQueueModel>(name: &str, flavor: &str) -> usize {
        let text = crate::common::spec_read_to_string(format!("{SPEC_DIR}/{name}"))
            .unwrap_or_else(|e| panic!("canonical fixture {name} unreadable: {e}"));
        let fixture: Value = serde_json::from_str(&text).expect("fixture parses");
        let config = &fixture["config"];
        let queue = M::new(
            as_u64(&config["visibility_timeout"], "visibility_timeout"),
            as_u64(&config["max_deliveries"], "max_deliveries") as u32,
        );
        assert!(
            fixture["initial"]["pending"]
                .as_array()
                .expect("initial pending")
                .is_empty(),
            "this runner does not seed initial.pending; a fixture needing it must \
             extend the runner rather than be skipped"
        );

        let steps = fixture["steps"].as_array().expect("steps array");
        assert!(!steps.is_empty(), "a replay of zero steps is not a replay");

        for (i, step) in steps.iter().enumerate() {
            queue.materialize();

            let op = &step["op"];
            match op["type"].as_str().expect("op type") {
                "push" => {
                    let got = queue.push(op["value"].as_str().expect("value").to_owned());
                    assert_eq!(got, as_u64(&step["returns"], "push return"));
                }
                "claim" => {
                    let got = queue.claim(
                        op["worker"].as_str().expect("worker"),
                        as_u64(&op["now"], "now"),
                    );
                    if step["returns"].is_null() {
                        assert!(got.is_none(), "{flavor} {name} step {i}: expected no claim");
                    } else {
                        assert_delivery(
                            &got.expect("delivery"),
                            &step["returns"],
                            &format!("{flavor} {name} step {i}"),
                        );
                    }
                }
                "ack" => {
                    let got = queue.ack(
                        op["worker"].as_str().expect("worker"),
                        as_u64(&op["delivery_id"], "delivery_id"),
                    );
                    assert_eq!(got, step["returns"].as_bool().expect("ack return"));
                }
                "nack" => {
                    let got = queue.nack(
                        op["worker"].as_str().expect("worker"),
                        as_u64(&op["delivery_id"], "delivery_id"),
                    );
                    assert_eq!(got, step["returns"].as_bool().expect("nack return"));
                }
                "reap_expired" => {
                    let got = queue.reap_expired(as_u64(&op["now"], "now"));
                    assert_eq!(got as u64, as_u64(&step["returns"], "reap return"));
                }
                other => panic!("{flavor} {name} step {i}: unknown op `{other}`"),
            }

            // Guard the step's `expected` block (`#lzassertunknownkeys`): a key
            // this flavor's replay never reads fails the fixture instead of
            // passing unnoticed.
            let expected = crate::Expect::new(
                format!("{SPEC_DIR}/{name}"),
                format!("steps[{i}].expected"),
                &step["expected"],
            );
            assert!(
                step.get("invalidates").is_none(),
                "{name} step {i}: `invalidates` at STEP level would be silently \
                 ignored; the runner reads expected.invalidates"
            );

            // Invalidation BEFORE the value reads below, which revalidate.
            let validity = queue.reader_validity();
            // DESCENT (`#lzsubblockkeyset`): `READER_KINDS` is the runner's own
            // list, so a kind the corpus adds outside it was compared by
            // nothing; the child tracker now reports it as unconsumed.
            let invalidates = expected.sub("invalidates");
            for (kind, valid) in READER_KINDS.iter().zip(validity) {
                invalidates.assert_key_with(kind, |want| {
                    assert_eq!(
                        !valid,
                        want.as_bool()
                            .unwrap_or_else(|| panic!("{name} step {i}: no invalidates.{kind}")),
                        "{flavor} {name} step {i}: invalidates.{kind} disagrees with the \
                         canonical fixture"
                    );
                });
            }
            invalidates.finish();

            let pending = queue.pending();
            expected.assert_key_with("pending", |want_pending| {
                let want_pending = want_pending.as_array().expect("pending array");
                assert_eq!(
                    pending.len(),
                    want_pending.len(),
                    "{flavor} {name} step {i}: pending length"
                );
                for (actual, want) in pending.iter().zip(want_pending) {
                    assert_eq!(actual.item_id, as_u64(&want["item_id"], "item_id"));
                    assert_eq!(actual.value, want["value"].as_str().expect("value"));
                    assert_eq!(
                        u64::from(actual.attempts),
                        as_u64(&want["attempts"], "attempts")
                    );
                }
            });

            let in_flight = queue.in_flight();
            expected.assert_key_with("in_flight", |want_in_flight| {
                let want_in_flight = want_in_flight.as_array().expect("in_flight array");
                assert_eq!(
                    in_flight.len(),
                    want_in_flight.len(),
                    "{flavor} {name} step {i}: in_flight length"
                );
                for (actual, want) in in_flight.iter().zip(want_in_flight) {
                    assert_delivery(actual, want, &format!("{flavor} {name} step {i} in_flight"));
                }
            });

            let dead_letters = queue.dead_letters();
            expected.assert_key_with("dead_letters", |want_dead| {
                let want_dead = want_dead.as_array().expect("dead_letters array");
                assert_eq!(
                    dead_letters.len(),
                    want_dead.len(),
                    "{flavor} {name} step {i}: dead_letters length"
                );
                for (actual, want) in dead_letters.iter().zip(want_dead) {
                    assert_eq!(actual.item_id, as_u64(&want["item_id"], "item_id"));
                    assert_eq!(actual.value, want["value"].as_str().expect("value"));
                    assert_eq!(
                        u64::from(actual.attempts),
                        as_u64(&want["attempts"], "attempts")
                    );
                    let reason = match actual.reason {
                        WorkQueueDeadLetterReason::Nack => "nack",
                        WorkQueueDeadLetterReason::Expired => "expired",
                    };
                    assert_eq!(reason, want["reason"].as_str().expect("reason"));
                }
            });

            // DESCENT (`#lzsubblockkeyset`): a reader kind the corpus adds to
            // the `reads` record must fail as an unconsumed key rather than be
            // read by nobody.
            let (pending_len, is_empty, in_flight_len, dead_letter_len) = queue.reads();
            let reads = expected.sub("reads");
            let at = format!("{flavor} {name} step {i}: reads");
            reads.assert_key_at("pending_len", pending_len, &at);
            reads.assert_key_at("is_empty", is_empty, &at);
            reads.assert_key_at("in_flight_len", in_flight_len, &at);
            reads.assert_key_at("dead_letter_len", dead_letter_len, &at);
            reads.finish();
        }
        steps.len()
    }

    pub fn replay_corpus<M: WorkQueueModel>(flavor: &str) -> usize {
        WORK_QUEUE_FIXTURES
            .iter()
            .map(|name| replay::<M>(name, flavor))
            .sum()
    }

    pub const MIN_STEPS: usize = 14;

    pub fn fixtures_present() -> bool {
        spec_fixtures_present()
    }

    /// **Found by mutation check, not by review.** Reversing `reap_expired`'s
    /// delivery-id sort left the whole corpus green: no fixture step ever expires
    /// more than one lease at a time, so the ordering clause the spec states —
    /// "in delivery-id order", which is what makes redelivery deterministic — was
    /// asserted nowhere. A probe that passes with and without the defect is not a
    /// gate, so the corpus gap gets a direct drive here rather than a new fixture
    /// (which would turn the other eight bindings red for a rule they already
    /// obey).
    ///
    /// Flavor-generic on purpose: the ordering lives in the shared core, so all
    /// three shells must show it.
    pub fn multi_expiry_requeues_in_delivery_order<M: WorkQueueModel>(flavor: &str) {
        let q = M::new(5, 3);
        q.push("a".into());
        q.push("b".into());
        let first = q.claim("w0", 0).expect("first claim");
        let second = q.claim("w1", 0).expect("second claim");
        assert!(
            first.delivery_id < second.delivery_id,
            "{flavor}: delivery ids must be monotone"
        );

        assert_eq!(
            q.reap_expired(6),
            2,
            "{flavor}: both leases are past their deadline"
        );
        let pending: Vec<_> = q.pending().into_iter().map(|item| item.value).collect();
        assert_eq!(
            pending,
            vec!["a".to_owned(), "b".to_owned()],
            "{flavor}: a multi-lease expiry must requeue in delivery-id order, so \
             redelivery is deterministic rather than HashMap-iteration order"
        );
        assert_eq!(q.in_flight().len(), 0, "{flavor}: no lease survives expiry");
    }
}

/// Single-threaded `WorkQueueCell` — the reference the other two flavors mirror.
mod work_queue_sync {
    use super::work_queue_flavors::{MIN_STEPS, WorkQueueModel, fixtures_present, replay_corpus};
    use lazily::{Context, WorkQueueCell, WorkQueueDeadLetter, WorkQueueDelivery, WorkQueueItem};

    struct Model {
        ctx: Context,
        cell: WorkQueueCell<String>,
    }

    impl WorkQueueModel for Model {
        fn new(visibility_timeout: u64, max_deliveries: u32) -> Self {
            let ctx = Context::new();
            let cell = WorkQueueCell::<String>::new(&ctx, visibility_timeout, max_deliveries);
            Self { ctx, cell }
        }
        fn push(&self, value: String) -> u64 {
            self.cell.push(&self.ctx, value)
        }
        fn claim(&self, worker: &str, now: u64) -> Option<WorkQueueDelivery<String>> {
            self.cell.claim(&self.ctx, worker.to_owned(), now)
        }
        fn ack(&self, worker: &str, delivery_id: u64) -> bool {
            self.cell.ack(&self.ctx, &worker.to_owned(), delivery_id)
        }
        fn nack(&self, worker: &str, delivery_id: u64) -> bool {
            self.cell.nack(&self.ctx, &worker.to_owned(), delivery_id)
        }
        fn reap_expired(&self, now: u64) -> usize {
            self.cell.reap_expired(&self.ctx, now)
        }
        fn materialize(&self) {
            let _ = self.reads();
        }
        fn reader_validity(&self) -> [bool; 4] {
            let h = self.cell.reader_handles();
            [
                self.ctx.is_set(&h.pending_len),
                self.ctx.is_set(&h.is_empty),
                self.ctx.is_set(&h.in_flight_len),
                self.ctx.is_set(&h.dead_letter_len),
            ]
        }
        fn reads(&self) -> (u64, bool, u64, u64) {
            (
                self.cell.pending_len(&self.ctx) as u64,
                self.cell.is_empty(&self.ctx),
                self.cell.in_flight_len(&self.ctx) as u64,
                self.cell.dead_letter_len(&self.ctx) as u64,
            )
        }
        fn pending(&self) -> Vec<WorkQueueItem<String>> {
            self.cell.pending()
        }
        fn in_flight(&self) -> Vec<WorkQueueDelivery<String>> {
            self.cell.in_flight()
        }
        fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<String>> {
            self.cell.dead_letters()
        }
    }

    #[test]
    fn multi_expiry_requeues_in_delivery_order() {
        super::work_queue_flavors::multi_expiry_requeues_in_delivery_order::<Model>(
            "single-threaded",
        );
    }

    #[test]
    fn single_threaded_work_queue_replays_the_canonical_corpus() {
        if !fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let total = replay_corpus::<Model>("single-threaded");
        assert!(
            total >= MIN_STEPS,
            "single-threaded work queue replayed only {total} steps"
        );
    }
}

/// `ThreadSafeWorkQueueCell` — the flavor with a real use case: N workers on N
/// threads competing for exclusive delivery.
#[cfg(feature = "thread-safe")]
mod work_queue_thread_safe {
    use super::work_queue_flavors::{MIN_STEPS, WorkQueueModel, fixtures_present, replay_corpus};
    use lazily::{
        ThreadSafeContext, ThreadSafeWorkQueueCell, WorkQueueDeadLetter, WorkQueueDelivery,
        WorkQueueItem,
    };

    struct Model {
        ctx: ThreadSafeContext,
        cell: ThreadSafeWorkQueueCell<String>,
    }

    impl WorkQueueModel for Model {
        fn new(visibility_timeout: u64, max_deliveries: u32) -> Self {
            let ctx = ThreadSafeContext::new();
            let cell =
                ThreadSafeWorkQueueCell::<String>::new(&ctx, visibility_timeout, max_deliveries);
            Self { ctx, cell }
        }
        fn push(&self, value: String) -> u64 {
            self.cell.push(&self.ctx, value)
        }
        fn claim(&self, worker: &str, now: u64) -> Option<WorkQueueDelivery<String>> {
            self.cell.claim(&self.ctx, worker.to_owned(), now)
        }
        fn ack(&self, worker: &str, delivery_id: u64) -> bool {
            self.cell.ack(&self.ctx, &worker.to_owned(), delivery_id)
        }
        fn nack(&self, worker: &str, delivery_id: u64) -> bool {
            self.cell.nack(&self.ctx, &worker.to_owned(), delivery_id)
        }
        fn reap_expired(&self, now: u64) -> usize {
            self.cell.reap_expired(&self.ctx, now)
        }
        fn materialize(&self) {
            let _ = self.reads();
        }
        fn reader_validity(&self) -> [bool; 4] {
            let h = self.cell.reader_handles();
            [
                self.ctx.is_set(&h.pending_len),
                self.ctx.is_set(&h.is_empty),
                self.ctx.is_set(&h.in_flight_len),
                self.ctx.is_set(&h.dead_letter_len),
            ]
        }
        fn reads(&self) -> (u64, bool, u64, u64) {
            (
                self.cell.pending_len(&self.ctx) as u64,
                self.cell.is_empty(&self.ctx),
                self.cell.in_flight_len(&self.ctx) as u64,
                self.cell.dead_letter_len(&self.ctx) as u64,
            )
        }
        fn pending(&self) -> Vec<WorkQueueItem<String>> {
            self.cell.pending()
        }
        fn in_flight(&self) -> Vec<WorkQueueDelivery<String>> {
            self.cell.in_flight()
        }
        fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<String>> {
            self.cell.dead_letters()
        }
    }

    #[test]
    fn multi_expiry_requeues_in_delivery_order() {
        super::work_queue_flavors::multi_expiry_requeues_in_delivery_order::<Model>("thread-safe");
    }

    #[test]
    fn thread_safe_work_queue_replays_the_canonical_corpus() {
        if !fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let total = replay_corpus::<Model>("thread-safe");
        assert!(
            total >= MIN_STEPS,
            "thread-safe work queue replayed only {total} steps"
        );
    }

    // A push moves pending_len AND (from empty) is_empty. Clearing them in two
    // frontier walks lets an observer of both rerun twice for one push and, in
    // between, read pending_len == 1 while is_empty still says true.
    #[test]
    fn one_push_invalidates_reader_kinds_atomically() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ctx = ThreadSafeContext::new();
        let q = ThreadSafeWorkQueueCell::<String>::new(&ctx, 10, 2);

        let runs = Arc::new(AtomicUsize::new(0));
        {
            let (q, runs) = (q.clone(), Arc::clone(&runs));
            ctx.effect(move |cx| {
                runs.fetch_add(1, Ordering::SeqCst);
                let _ = q.pending_len(cx);
                let _ = q.is_empty(cx);
            });
        }
        let baseline = runs.load(Ordering::SeqCst);
        assert!(baseline >= 1, "effect must run once on creation");

        q.push(&ctx, "job".into());

        assert_eq!(
            runs.load(Ordering::SeqCst) - baseline,
            1,
            "one push must rerun a two-kind subscriber exactly ONCE; more means \
             pending_len and is_empty were cleared in separate frontier walks"
        );
        assert_eq!(q.pending_len(&ctx), 1);
        assert!(!q.is_empty(&ctx));
    }

    // The reason this flavor exists: competing consumers on real threads. Each
    // item must be delivered to exactly one worker, and the run must not deadlock
    // against readers taking the context lock then the core lock.
    #[test]
    fn competing_workers_never_share_a_delivery() {
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        use std::thread;

        let ctx = Arc::new(ThreadSafeContext::new());
        let q = ThreadSafeWorkQueueCell::<String>::new(&ctx, 1_000, 3);
        for i in 0..200 {
            q.push(&ctx, format!("job-{i}"));
        }

        let claimed: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let workers: Vec<_> = (0..8)
            .map(|w| {
                let (ctx, q, claimed) = (Arc::clone(&ctx), q.clone(), Arc::clone(&claimed));
                thread::spawn(move || {
                    while let Some(delivery) = q.claim(&ctx, format!("w{w}"), 0) {
                        claimed.lock().expect("claimed").push(delivery.item_id);
                        let _ = q.pending_len(&ctx);
                        assert!(q.ack(&ctx, &format!("w{w}"), delivery.delivery_id));
                    }
                })
            })
            .collect();
        for t in workers {
            t.join().expect("no worker panicked or deadlocked");
        }

        let claimed = claimed.lock().expect("claimed");
        assert_eq!(claimed.len(), 200, "every item must be delivered once");
        let unique: HashSet<_> = claimed.iter().copied().collect();
        assert_eq!(
            unique.len(),
            200,
            "an item delivered to two workers means `claim` is not exclusive"
        );
        assert_eq!(q.pending_len(&ctx), 0);
        assert_eq!(q.in_flight_len(&ctx), 0);
    }
}

/// `AsyncWorkQueueCell` — same corpus, `AsyncContext` graph, caller-driven clock.
#[cfg(feature = "async")]
mod work_queue_async {
    use super::work_queue_flavors::{MIN_STEPS, WorkQueueModel, fixtures_present, replay_corpus};
    use lazily::{
        AsyncContext, AsyncWorkQueueCell, WorkQueueDeadLetter, WorkQueueDelivery, WorkQueueItem,
    };

    struct Model {
        ctx: AsyncContext,
        cell: AsyncWorkQueueCell<String>,
    }

    impl WorkQueueModel for Model {
        fn new(visibility_timeout: u64, max_deliveries: u32) -> Self {
            let ctx = AsyncContext::new();
            let cell = AsyncWorkQueueCell::<String>::new(&ctx, visibility_timeout, max_deliveries);
            Self { ctx, cell }
        }
        fn push(&self, value: String) -> u64 {
            self.cell.push(&self.ctx, value)
        }
        fn claim(&self, worker: &str, now: u64) -> Option<WorkQueueDelivery<String>> {
            self.cell.claim(&self.ctx, worker.to_owned(), now)
        }
        fn ack(&self, worker: &str, delivery_id: u64) -> bool {
            self.cell.ack(&self.ctx, &worker.to_owned(), delivery_id)
        }
        fn nack(&self, worker: &str, delivery_id: u64) -> bool {
            self.cell.nack(&self.ctx, &worker.to_owned(), delivery_id)
        }
        fn reap_expired(&self, now: u64) -> usize {
            self.cell.reap_expired(&self.ctx, now)
        }
        fn materialize(&self) {
            let _ = self.reads();
        }
        fn reader_validity(&self) -> [bool; 4] {
            let h = self.cell.reader_handles();
            [
                self.ctx.is_set(&h.pending_len),
                self.ctx.is_set(&h.is_empty),
                self.ctx.is_set(&h.in_flight_len),
                self.ctx.is_set(&h.dead_letter_len),
            ]
        }
        fn reads(&self) -> (u64, bool, u64, u64) {
            (
                self.cell.pending_len(&self.ctx) as u64,
                self.cell.is_empty(&self.ctx),
                self.cell.in_flight_len(&self.ctx) as u64,
                self.cell.dead_letter_len(&self.ctx) as u64,
            )
        }
        fn pending(&self) -> Vec<WorkQueueItem<String>> {
            self.cell.pending()
        }
        fn in_flight(&self) -> Vec<WorkQueueDelivery<String>> {
            self.cell.in_flight()
        }
        fn dead_letters(&self) -> Vec<WorkQueueDeadLetter<String>> {
            self.cell.dead_letters()
        }
    }

    #[test]
    fn multi_expiry_requeues_in_delivery_order() {
        super::work_queue_flavors::multi_expiry_requeues_in_delivery_order::<Model>("async");
    }

    #[test]
    fn async_work_queue_replays_the_canonical_corpus() {
        if !fixtures_present() {
            eprintln!("SKIP: lazily-spec sibling missing");
            return;
        }
        let total = replay_corpus::<Model>("async");
        assert!(
            total >= MIN_STEPS,
            "async work queue replayed only {total} steps"
        );
    }

    // Neither the lease nor the clock is async-coloured: a claim returns a
    // delivery, not a future, and `reap_expired` takes the caller's `now`.
    #[test]
    fn lease_lifecycle_resolves_without_being_driven() {
        let ctx = AsyncContext::new();
        let q = AsyncWorkQueueCell::<String>::new(&ctx, 5, 1);
        q.push(&ctx, "job".into());
        assert_eq!(q.pending_len(&ctx), 1);
        assert!(!q.is_empty(&ctx));

        let delivery = q.claim(&ctx, "w0".into(), 0).expect("claim");
        assert_eq!(q.in_flight_len(&ctx), 1);
        assert_eq!(q.pending_len(&ctx), 0);

        // Deadline is 0 + 5; nothing expires before it.
        assert_eq!(q.reap_expired(&ctx, 5), 0);
        assert_eq!(q.reap_expired(&ctx, 6), 1);
        assert_eq!(q.in_flight_len(&ctx), 0);
        assert_eq!(
            q.dead_letter_len(&ctx),
            1,
            "max_deliveries is 1, so the first expiry is terminal"
        );
        assert!(!q.ack(&ctx, &"w0".to_owned(), delivery.delivery_id));
    }
}
