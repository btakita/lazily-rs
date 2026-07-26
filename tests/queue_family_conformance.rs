//! The queue-family reader-kind contract, replayed against **every flavor this
//! binding ships** — with a ledger that is *enforced* rather than advisory.
//!
//! `queue_conformance.rs` replays the canonical `queuecell_*.json` corpus against
//! the single-threaded `QueueCell`. That is currently the only flavor: no binding
//! in the family ships a thread-safe or async queue primitive
//! (`cell-model.md` § "Core surface vs. binding extensions (queue family)" now
//! makes those Core, so their absence is a conformance gap rather than an
//! unfinished nicety).
//!
//! A three-flavor replay written today would therefore skip two of three flavors
//! entirely, and a suite that skips almost everything while reporting green is
//! precisely the failure this file exists to prevent. So the ledger is wired to
//! the source instead of to a comment:
//!
//! * `unshipped_flavors_are_really_absent` greps `src/` for each flavor's type
//!   name. The moment Phase 3 adds `ThreadSafeQueueCell` or `AsyncQueueCell`,
//!   this test goes **red** and names the runner that must be extended. The
//!   ledger cannot rot, because shrinking it is not optional — the compiler's
//!   sibling, the filesystem, enforces it.
//! * `shipped_flavor_replays_the_corpus` proves the flavor that *does* ship
//!   actually replayed a non-zero number of steps from a non-zero number of
//!   fixtures. An absence guard proves the corpus exists; only a positive count
//!   proves this binary read it.
//! * `ledger_is_not_all_skips` fails if every flavor is unshipped, so this file
//!   can never degrade into a no-op that reports success.
//!
//! Mirrors the shape of `collections_family_conformance.rs`, which closed the
//! same gap for `ReactiveMap`.

use std::fs;
use std::path::Path;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

/// The canonical `QueueCell` corpus. `TopicCell` and `WorkQueueCell` fixtures
/// have their own runners; this file owns the reader-kind plane.
const QUEUE_FIXTURES: &[&str] = &[
    "queuecell_spsc_push_pop.json",
    "queuecell_popped_head_observation.json",
    "queuecell_mpsc_multi_writer.json",
    "queuecell_bounded_backpressure.json",
    "queuecell_closure_lifecycle.json",
];

/// One execution flavor, and the type name that would prove it exists.
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

const LEDGER: &[Flavor] = &[
    Flavor {
        name: "single-threaded",
        marker_type: "pub struct QueueCell",
        shipped: true,
    },
    Flavor {
        name: "thread-safe",
        marker_type: "ThreadSafeQueueCell",
        shipped: true,
    },
    Flavor {
        name: "async",
        marker_type: "AsyncQueueCell",
        shipped: true,
    },
];

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
                && let Ok(text) = fs::read_to_string(&path)
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
        3,
        "the ledger must cover all three execution flavors; a missing entry is an \
         unscored gap, not an absent one"
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

    for name in QUEUE_FIXTURES {
        let text = fs::read_to_string(format!("{SPEC_DIR}/{name}"))
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
        QUEUE_FIXTURES.len(),
        "did not read every declared queue fixture"
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
    use std::fs;

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
        let text = fs::read_to_string(format!("{SPEC_DIR}/{name}"))
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

            let expected = &step["expected"];

            // `invalidates` BEFORE any read — reading revalidates.
            if let Some(inv) = expected.get("invalidates") {
                for (key, node_valid) in [
                    ("head", ctx.is_set(&r.head)),
                    ("len", ctx.is_set(&r.len)),
                    ("is_empty", ctx.is_set(&r.is_empty)),
                    ("is_full", ctx.is_set(&r.is_full)),
                    ("closed", ctx.is_set(&r.closed)),
                ] {
                    if let Some(want) = inv.get(key).and_then(|v| v.as_bool()) {
                        assert_eq!(
                            !node_valid, want,
                            "{name} step {i}: invalidates.{key} — thread-safe flavor \
                             disagrees with the canonical fixture"
                        );
                    }
                }
            }

            if let Some(want) = step.get("returns").and_then(|v| v.as_str()) {
                let got = got_returns.as_deref().unwrap_or("");
                assert!(
                    got == want || got.starts_with(want),
                    "{name} step {i}: returns `{got}`, fixture says `{want}`"
                );
            }

            if let Some(want) = expected.get("len").and_then(|v| v.as_u64()) {
                assert_eq!(q.len(&ctx) as u64, want, "{name} step {i}: len");
            }
            if let Some(want) = expected.get("is_empty").and_then(|v| v.as_bool()) {
                assert_eq!(q.is_empty(&ctx), want, "{name} step {i}: is_empty");
            }
            if let Some(want) = expected.get("is_full").and_then(|v| v.as_bool()) {
                assert_eq!(q.is_full(&ctx), want, "{name} step {i}: is_full");
            }
            if let Some(want) = expected.get("closed").and_then(|v| v.as_bool()) {
                assert_eq!(q.closed(&ctx), want, "{name} step {i}: closed");
            }
            match expected.get("head") {
                Some(Value::String(want)) => {
                    assert_eq!(
                        q.head(&ctx).as_deref(),
                        Some(want.as_str()),
                        "{name} step {i}: head"
                    )
                }
                Some(Value::Null) => assert_eq!(q.head(&ctx), None, "{name} step {i}: head"),
                _ => {}
            }
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
    use std::fs;

    type V = String;

    fn replay(name: &str) -> usize {
        let text = fs::read_to_string(format!("{SPEC_DIR}/{name}"))
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

        let steps = fixture["steps"].as_array().expect("steps array");
        assert!(!steps.is_empty(), "a replay of zero steps is not a replay");

        for (i, step) in steps.iter().enumerate() {
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

            let expected = &step["expected"];
            if let Some(want) = step.get("returns").and_then(|v| v.as_str()) {
                let got = got_returns.as_deref().unwrap_or("");
                assert!(
                    got == want || got.starts_with(want),
                    "{name} step {i}: returns `{got}`, fixture says `{want}`"
                );
            }
            if let Some(want) = expected.get("len").and_then(|v| v.as_u64()) {
                assert_eq!(q.len(&ctx) as u64, want, "{name} step {i}: len");
            }
            if let Some(want) = expected.get("is_empty").and_then(|v| v.as_bool()) {
                assert_eq!(q.is_empty(&ctx), want, "{name} step {i}: is_empty");
            }
            if let Some(want) = expected.get("is_full").and_then(|v| v.as_bool()) {
                assert_eq!(q.is_full(&ctx), want, "{name} step {i}: is_full");
            }
            if let Some(want) = expected.get("closed").and_then(|v| v.as_bool()) {
                assert_eq!(q.closed(&ctx), want, "{name} step {i}: closed");
            }
            match expected.get("head") {
                Some(Value::String(want)) => assert_eq!(
                    q.head(&ctx).as_deref(),
                    Some(want.as_str()),
                    "{name} step {i}: head"
                ),
                Some(Value::Null) => assert_eq!(q.head(&ctx), None, "{name} step {i}: head"),
                _ => {}
            }
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
