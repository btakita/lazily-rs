//! Pilot for `#lazilystatetable`: the retained-document-transition decision,
//! expressed as a typed state table over a finite product state.
//!
//! Plan: `tasks/software/plan-lazily-state-tables.md` § "Pilot: retained
//! document transition". This is Phase 1's deliverable — *write the pilot
//! product-state enum and exhaustive table before changing runtime behavior* —
//! so nothing here reaches into Agent Doc. What it proves is that the shape
//! carries the decision faithfully, and that the coverage contract is
//! mechanically checkable rather than a review convention.
//!
//! The real consumer today is `agent_doc_state_backbone::retained_write::
//! settlement_verdict`: a total pure function over `Option`-shaped facts, wired
//! as `Source`s into a `Computed<SettlementVerdict>`. It is already a state
//! table in everything but name. Two things it does not have, and this pilot
//! does:
//!
//! 1. **A finite input.** Its input is three `Option`s of hash- and
//!    payload-bearing structs, so it has no row set and cannot be enumerated.
//!    Here the unbounded facts are classified at the projection boundary and
//!    only the classification enters the table.
//! 2. **Impossible combinations made unrepresentable.** Presence is carried by
//!    `Option`, so "no retained intent, but a divergent visible projection" is
//!    a constructible input that only the function body rules out. Here the
//!    nesting rules it out in the type.

use std::cell::Cell as StdCell;
use std::rc::Rc;

use lazily::{
    Context, FiniteState, StateTable, finite_state, projected_state_table, state_table,
    table_coverage,
};

// ---------------------------------------------------------------------------
// The product state
// ---------------------------------------------------------------------------

/// How the visible document content relates to the retained target.
///
/// This is the classification of an unbounded payload — content hash, lineage,
/// document body — into a finite axis. The payload never enters the table.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Visible {
    /// Exactly the retained target of an unfinished owning cycle.
    ExactUnfinishedOwningCycle,
    /// Newer than the target, and proves a terminal post-commit reposition
    /// materialized.
    NewerTerminalReposition,
    /// Malformed, or conflicts with the retained lineage.
    Divergent,
    /// Well-formed, on-lineage, and neither the target nor a newer terminal
    /// reposition — delivery has not landed yet.
    Unrelated,
}
finite_state!(Visible {
    Visible::ExactUnfinishedOwningCycle,
    Visible::NewerTerminalReposition,
    Visible::Divergent,
    Visible::Unrelated,
});

/// Whether the projection facts a classification depends on are available.
///
/// Deliberately *not* `Option<Visible>`: an absent projection is a named
/// semantic state, and the axis reads as one when it is spelled as one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Observation {
    Unavailable,
    Classified(Visible),
}

impl FiniteState for Observation {
    fn all() -> Vec<Self> {
        let mut out = vec![Observation::Unavailable];
        out.extend(Visible::all().into_iter().map(Observation::Classified));
        out
    }
}

/// The applied effect frontier for this transition — the fed-back fact that a
/// publication succeeded. It is an *observation*, never a gate the table waits
/// on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Frontier {
    Behind,
    EqualOrNewer,
}
finite_state!(Frontier {
    Frontier::Behind,
    Frontier::EqualOrNewer
});

/// The facts that exist only while a transition is retained.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct RetainedFacts {
    observation: Observation,
    frontier: Frontier,
}

/// The pilot product state.
///
/// The plan's coverage contract asks that impossible combinations be
/// "represented explicitly as `Invalid` or rejected while constructing
/// `InputState`". Nesting is the stronger form of that rejection: with no
/// retained transition there is no target to classify and no frontier to
/// compare against, so those axes do not exist rather than existing and being
/// ignored. The naive product of the four axes is 2x2x4x2 = 32; only 11 are
/// constructible, and the other 21 are compile errors rather than rows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RetainedInput {
    NoRetainedTransition,
    Retained(RetainedFacts),
}

impl FiniteState for RetainedInput {
    fn all() -> Vec<Self> {
        let mut out = vec![RetainedInput::NoRetainedTransition];
        for observation in Observation::all() {
            for frontier in Frontier::all() {
                out.push(RetainedInput::Retained(RetainedFacts {
                    observation,
                    frontier,
                }));
            }
        }
        out
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RetainedDecision {
    Idle,
    Wait,
    ResumePinnedContinuation,
    SettleMaterializedTransition,
    RejectConflict,
    Settled,
}

const EVERY_DECISION: &[RetainedDecision] = &[
    RetainedDecision::Idle,
    RetainedDecision::Wait,
    RetainedDecision::ResumePinnedContinuation,
    RetainedDecision::SettleMaterializedTransition,
    RetainedDecision::RejectConflict,
    RetainedDecision::Settled,
];

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

struct RetainedTransition;

impl StateTable for RetainedTransition {
    type Input = RetainedInput;
    type Decision = RetainedDecision;

    fn decide(input: &RetainedInput) -> RetainedDecision {
        let RetainedInput::Retained(facts) = input else {
            return RetainedDecision::Idle;
        };
        // Ordered first on purpose: an equal-or-newer applied frontier means a
        // publication for this transition already succeeded, so every remaining
        // row would re-publish it. This is the row that makes the effect
        // idempotent without an ACK protocol.
        if facts.frontier == Frontier::EqualOrNewer {
            return RetainedDecision::Settled;
        }
        match facts.observation {
            Observation::Unavailable => RetainedDecision::Wait,
            Observation::Classified(Visible::Divergent) => RetainedDecision::RejectConflict,
            Observation::Classified(Visible::ExactUnfinishedOwningCycle) => {
                RetainedDecision::ResumePinnedContinuation
            }
            Observation::Classified(Visible::NewerTerminalReposition) => {
                RetainedDecision::SettleMaterializedTransition
            }
            Observation::Classified(Visible::Unrelated) => RetainedDecision::Wait,
        }
    }
}

// ---------------------------------------------------------------------------
// Coverage contract (plan § "Coverage contract" 1-4)
// ---------------------------------------------------------------------------

#[test]
fn every_constructible_row_is_enumerated_and_every_decision_is_reached() {
    let coverage = table_coverage::<RetainedTransition>();

    // 1 (no transition) + 5 observations x 2 frontiers.
    coverage.assert_at_least(11);
    assert_eq!(coverage.len(), 11);
    coverage.assert_decisions_exactly(EVERY_DECISION);
}

#[test]
fn impossible_combinations_are_unrepresentable_rather_than_ignored() {
    let naive_product = 2 * 2 * 4 * 2;
    let coverage = table_coverage::<RetainedTransition>();
    assert!(
        coverage.len() < naive_product,
        "nesting should remove impossible rows, not preserve them"
    );

    // The one row without a transition decides Idle, and nothing else does.
    coverage.assert_every_row("consistent about Idle", |input, decision| {
        matches!(input, RetainedInput::NoRetainedTransition)
            == (*decision == RetainedDecision::Idle)
    });
}

#[test]
fn a_published_frontier_dominates_every_observation() {
    let coverage = table_coverage::<RetainedTransition>();
    coverage.assert_no_row(
        "deciding work while the applied frontier is already equal-or-newer",
        |input, decision| match input {
            RetainedInput::Retained(facts) => {
                facts.frontier == Frontier::EqualOrNewer && *decision != RetainedDecision::Settled
            }
            RetainedInput::NoRetainedTransition => false,
        },
    );
}

// ---------------------------------------------------------------------------
// Wiring (plan § "Coverage contract" 5-7)
// ---------------------------------------------------------------------------

/// Raw, unbounded facts as a consumer actually holds them. None of this enters
/// the table.
#[derive(Clone, PartialEq, Debug)]
struct Facts {
    retained_target: Option<String>,
    visible_hash: Option<String>,
    visible_is_terminal_reposition: bool,
    visible_on_lineage: bool,
    applied_frontier: Option<String>,
}

impl Facts {
    fn idle() -> Self {
        Self {
            retained_target: None,
            visible_hash: None,
            visible_is_terminal_reposition: false,
            visible_on_lineage: true,
            applied_frontier: None,
        }
    }
}

/// The projection boundary: classify unbounded payloads into the finite product
/// state. Everything hash-shaped stops here.
fn project(facts: &Facts) -> RetainedInput {
    let Some(target) = facts.retained_target.as_deref() else {
        return RetainedInput::NoRetainedTransition;
    };
    let frontier = match facts.applied_frontier.as_deref() {
        Some(applied) if applied == target => Frontier::EqualOrNewer,
        _ => Frontier::Behind,
    };
    let observation = match facts.visible_hash.as_deref() {
        None => Observation::Unavailable,
        Some(_) if !facts.visible_on_lineage => Observation::Classified(Visible::Divergent),
        Some(visible) if visible == target => {
            Observation::Classified(Visible::ExactUnfinishedOwningCycle)
        }
        Some(_) if facts.visible_is_terminal_reposition => {
            Observation::Classified(Visible::NewerTerminalReposition)
        }
        Some(_) => Observation::Classified(Visible::Unrelated),
    };
    RetainedInput::Retained(RetainedFacts {
        observation,
        frontier,
    })
}

#[test]
fn the_wired_table_decides_what_the_row_set_says_it_decides() {
    let ctx = Context::new();
    let facts = ctx.source(Facts::idle());
    let decision =
        projected_state_table::<RetainedTransition, _>(&ctx, move |c| project(&facts.get(c)));

    assert_eq!(decision.get(&ctx), RetainedDecision::Idle);

    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            ..Facts::idle()
        },
    );
    assert_eq!(decision.get(&ctx), RetainedDecision::Wait);

    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("target".into()),
            ..Facts::idle()
        },
    );
    assert_eq!(
        decision.get(&ctx),
        RetainedDecision::ResumePinnedContinuation
    );

    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("newer".into()),
            visible_is_terminal_reposition: true,
            ..Facts::idle()
        },
    );
    assert_eq!(
        decision.get(&ctx),
        RetainedDecision::SettleMaterializedTransition
    );

    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("elsewhere".into()),
            visible_on_lineage: false,
            ..Facts::idle()
        },
    );
    assert_eq!(decision.get(&ctx), RetainedDecision::RejectConflict);

    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("target".into()),
            applied_frontier: Some("target".into()),
            ..Facts::idle()
        },
    );
    assert_eq!(decision.get(&ctx), RetainedDecision::Settled);
}

/// Plan § "Coverage contract" 5 — both arrival orders for independently
/// propagated facts.
#[test]
fn independently_propagated_facts_converge_regardless_of_arrival_order() {
    fn run(visible_first: bool) -> (RetainedDecision, Vec<RetainedDecision>) {
        let ctx = Context::new();
        let target = ctx.source(Option::<String>::None);
        let visible = ctx.source(Option::<String>::None);
        let frontier = ctx.source(Option::<String>::None);
        let decision = projected_state_table::<RetainedTransition, _>(&ctx, move |c| {
            project(&Facts {
                retained_target: target.get(c),
                visible_hash: visible.get(c),
                applied_frontier: frontier.get(c),
                ..Facts::idle()
            })
        });

        let seen = Rc::new(StdCell::new(Vec::new()));
        let sink = Rc::clone(&seen);
        let _effect = decision.subscribe(&ctx, move |_, d| {
            let mut v = sink.take();
            v.push(*d);
            sink.set(v);
        });

        target.set(&ctx, Some("target".into()));
        if visible_first {
            visible.set(&ctx, Some("target".into()));
            frontier.set(&ctx, Some("target".into()));
        } else {
            frontier.set(&ctx, Some("target".into()));
            visible.set(&ctx, Some("target".into()));
        }
        (decision.get(&ctx), seen.take())
    }

    let (visible_first, visible_first_seen) = run(true);
    let (frontier_first, frontier_first_seen) = run(false);

    assert_eq!(visible_first, RetainedDecision::Settled);
    assert_eq!(frontier_first, RetainedDecision::Settled);

    // The paths differ — the point of testing both orders — but neither passes
    // through a decision that would publish twice after the frontier landed.
    assert_ne!(visible_first_seen, frontier_first_seen);
    for seen in [&visible_first_seen, &frontier_first_seen] {
        let settled_at = seen.iter().position(|d| *d == RetainedDecision::Settled);
        let settled_at = settled_at.expect("both orders must reach Settled");
        assert!(
            seen[settled_at..]
                .iter()
                .all(|d| *d == RetainedDecision::Settled),
            "no decision may follow Settled: {seen:?}"
        );
    }
}

/// Plan § "Coverage contract" 6 — restart/replay from durable facts, without
/// consulting current-cycle mutable state. The table holds nothing, so a fresh
/// context fed the same ordered facts lands on the same decision.
#[test]
fn replaying_ordered_facts_into_a_fresh_context_reproduces_the_decision() {
    fn replay(events: &[Facts]) -> RetainedDecision {
        let ctx = Context::new();
        let facts = ctx.source(Facts::idle());
        let decision =
            projected_state_table::<RetainedTransition, _>(&ctx, move |c| project(&facts.get(c)));
        for event in events {
            facts.set(&ctx, event.clone());
        }
        decision.get(&ctx)
    }

    let events = vec![
        Facts {
            retained_target: Some("target".into()),
            ..Facts::idle()
        },
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("drifted".into()),
            ..Facts::idle()
        },
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("target".into()),
            ..Facts::idle()
        },
    ];

    assert_eq!(
        replay(&events),
        RetainedDecision::ResumePinnedContinuation,
        "the first run establishes the expected decision"
    );
    assert_eq!(
        replay(&events),
        RetainedDecision::ResumePinnedContinuation,
        "a fresh graph replaying the same events must agree"
    );
    // Truncated replay is a different prefix, so a different decision — the
    // check would be vacuous if every input produced the same answer.
    assert_eq!(replay(&events[..2]), RetainedDecision::Wait);
}

/// Plan § "Coverage contract" 7 — an effect fires per *distinct* decision, and
/// a successful publication feeds back as an observation rather than gating the
/// table.
#[test]
fn the_effect_sink_sees_distinct_decisions_and_a_publication_settles_the_table() {
    let ctx = Context::new();
    let target = ctx.source(Option::<String>::None);
    let visible = ctx.source(Option::<String>::None);
    let published = ctx.source(Option::<String>::None);
    let decision = projected_state_table::<RetainedTransition, _>(&ctx, move |c| {
        project(&Facts {
            retained_target: target.get(c),
            visible_hash: visible.get(c),
            applied_frontier: published.get(c),
            ..Facts::idle()
        })
    });

    let attempts = Rc::new(StdCell::new(Vec::new()));
    let sink = Rc::clone(&attempts);
    let _effect = decision.subscribe(&ctx, move |_, d| {
        let mut v = sink.take();
        v.push(*d);
        sink.set(v);
    });

    target.set(&ctx, Some("target".into()));
    // The product state MOVES here — Unavailable becomes Classified(Unrelated) —
    // but both rows decide Wait, so the decision cell's guard must keep the sink
    // asleep. Source-level equality cannot suppress this one; only the guard on
    // the decision can.
    visible.set(&ctx, Some("elsewhere".into()));
    visible.set(&ctx, Some("target".into()));
    // A re-observation of the same visible content is not a new decision.
    visible.set(&ctx, Some("target".into()));

    assert_eq!(
        attempts.take(),
        vec![
            RetainedDecision::Idle,
            RetainedDecision::Wait,
            RetainedDecision::ResumePinnedContinuation,
        ]
    );

    // The publication succeeded. Nothing acknowledges anything: the frontier is
    // just another observed fact, and the table settles on the next propagation.
    published.set(&ctx, Some("target".into()));
    assert_eq!(decision.get(&ctx), RetainedDecision::Settled);
    assert_eq!(attempts.take(), vec![RetainedDecision::Settled]);

    // Re-publishing the same frontier decides nothing new — the idempotence the
    // frontier row buys.
    published.set(&ctx, Some("target".into()));
    assert_eq!(attempts.take(), Vec::new());
}

/// The projection is a guarded cell of its own, so payload churn that leaves
/// the product state alone never reaches `decide`.
#[test]
fn payload_churn_that_does_not_move_the_product_state_never_reaches_the_table() {
    thread_local! {
        static DECIDE_CALLS: StdCell<usize> = const { StdCell::new(0) };
    }

    struct Counting;
    impl StateTable for Counting {
        type Input = RetainedInput;
        type Decision = RetainedDecision;
        fn decide(input: &RetainedInput) -> RetainedDecision {
            DECIDE_CALLS.with(|c| c.set(c.get() + 1));
            RetainedTransition::decide(input)
        }
    }

    let ctx = Context::new();
    let facts = ctx.source(Facts {
        retained_target: Some("target".into()),
        visible_hash: Some("drifted-1".into()),
        ..Facts::idle()
    });
    let decision = projected_state_table::<Counting, _>(&ctx, move |c| project(&facts.get(c)));

    assert_eq!(decision.get(&ctx), RetainedDecision::Wait);
    let baseline = DECIDE_CALLS.with(|c| c.get());
    assert!(baseline >= 1, "the first read must evaluate the table");

    // A different hash — a real upstream write — that still classifies as
    // Unrelated. The projection recomputes; the table must not.
    for n in 2..8 {
        facts.set(
            &ctx,
            Facts {
                retained_target: Some("target".into()),
                visible_hash: Some(format!("drifted-{n}")),
                ..Facts::idle()
            },
        );
        assert_eq!(decision.get(&ctx), RetainedDecision::Wait);
    }
    assert_eq!(
        DECIDE_CALLS.with(|c| c.get()),
        baseline,
        "six upstream writes that leave the product state unchanged re-ran `decide`"
    );

    // A write that does move the product state still gets through, so the
    // assertion above is a guard rather than a dead table.
    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("target".into()),
            ..Facts::idle()
        },
    );
    assert_eq!(
        decision.get(&ctx),
        RetainedDecision::ResumePinnedContinuation
    );
    assert!(DECIDE_CALLS.with(|c| c.get()) > baseline);
}

#[test]
fn state_table_accepts_an_input_cell_the_caller_already_holds() {
    let ctx = Context::new();
    let facts = ctx.source(Facts::idle());
    let input = ctx.computed(move |c| project(&facts.get(c)));
    let decision = state_table::<RetainedTransition>(&ctx, input);

    assert_eq!(decision.get(&ctx), RetainedDecision::Idle);
    facts.set(
        &ctx,
        Facts {
            retained_target: Some("target".into()),
            visible_hash: Some("target".into()),
            ..Facts::idle()
        },
    );
    assert!(matches!(input.get(&ctx), RetainedInput::Retained(_)));
    assert_eq!(
        decision.get(&ctx),
        RetainedDecision::ResumePinnedContinuation
    );
}

/// Plan § Phase 4 — parity is semantic, not shape-identical. The thread-safe
/// flavor must decide the same row set.
#[cfg(feature = "thread-safe")]
#[test]
fn the_thread_safe_flavor_decides_the_same_rows() {
    use lazily::{ThreadSafeContext, thread_safe_projected_state_table};

    let ctx = ThreadSafeContext::new();
    let facts = ctx.source(Facts::idle());
    let decision = thread_safe_projected_state_table::<RetainedTransition, _>(
        &ctx,
        move |c: &ThreadSafeContext| project(&c.get(&facts)),
    );

    let mut observed = Vec::new();
    for input in RetainedInput::all() {
        let raw = match input {
            RetainedInput::NoRetainedTransition => Facts::idle(),
            RetainedInput::Retained(f) => Facts {
                retained_target: Some("target".into()),
                visible_hash: match f.observation {
                    Observation::Unavailable => None,
                    Observation::Classified(Visible::ExactUnfinishedOwningCycle) => {
                        Some("target".into())
                    }
                    Observation::Classified(_) => Some("other".into()),
                },
                visible_is_terminal_reposition: matches!(
                    f.observation,
                    Observation::Classified(Visible::NewerTerminalReposition)
                ),
                visible_on_lineage: !matches!(
                    f.observation,
                    Observation::Classified(Visible::Divergent)
                ),
                applied_frontier: match f.frontier {
                    Frontier::Behind => None,
                    Frontier::EqualOrNewer => Some("target".into()),
                },
            },
        };
        ctx.set(&facts, raw);
        let decided = ctx.get(&decision);
        assert_eq!(
            decided,
            RetainedTransition::decide(&input),
            "thread-safe flavor diverged on {input:?}"
        );
        observed.push(decided);
    }

    // Parity against `decide` is only meaningful if the sweep actually moved
    // through the row set; a flavor that answered one decision for everything
    // would otherwise agree with a table that did the same.
    assert_eq!(observed.len(), 11);
    for want in EVERY_DECISION {
        assert!(
            observed.contains(want),
            "the thread-safe sweep never observed {want:?}"
        );
    }
}
