//! Typed **state tables** (`#lazilystatetable`) — a total pure decision
//! function over a finite product state, wired as a [`Computed`].
//!
//! See `tasks/software/plan-lazily-state-tables.md`.
//!
//! ## What a state table is, and what it is not
//!
//! A [`StateMachine`](crate::StateMachine) **owns** an accepted sequence of
//! state changes: it holds current state and folds events into it. A
//! `StateTable` **derives** a decision from the current product of independently
//! observed facts. It holds nothing.
//!
//! ```text
//! Computed<Input>  ── StateTable::decide ──>  Computed<Decision>
//!  (finite product                              │
//!   of enum axes)                               └── Effect sink observes distinct decisions
//! ```
//!
//! The table itself is a free function on a type — `fn decide(&Input) ->
//! Decision`. It owns no snapshot, cache, sidecar, scheduler, or hidden mutable
//! state, so replaying the same ordered facts into a fresh [`Context`] always
//! reproduces the same decision. Effects stay outside the table: a successful
//! publication is fed back as *another observed fact*, never as an imperative
//! request/ACK gate.
//!
//! ## Why the input must be a finite product
//!
//! Exhaustive `match` proves the function is *total*; it does not prove the
//! table is *reviewed*. Reviewing needs the row set, and a row set only exists
//! when every input axis is finite. So large or unbounded payloads — hashes,
//! document text, revisions, continuation bodies — are classified into a
//! semantic enum at the boundary, before entering the table. [`FiniteState`]
//! names that obligation, and [`table_coverage`] turns it into the Cartesian
//! product test.
//!
//! ## Example
//!
//! ```
//! use lazily::{Context, FiniteState, StateTable, finite_state, state_table, table_coverage};
//!
//! #[derive(Clone, Copy, PartialEq, Eq, Debug)]
//! enum Link { Down, Up }
//! #[derive(Clone, Copy, PartialEq, Eq, Debug)]
//! enum Work { Idle, Pending }
//! finite_state!(Link { Link::Down, Link::Up });
//! finite_state!(Work { Work::Idle, Work::Pending });
//!
//! #[derive(Clone, Copy, PartialEq, Eq, Debug)]
//! enum Decision { Park, Wait, Send }
//!
//! struct Pump;
//! impl StateTable for Pump {
//!     type Input = (Link, Work);
//!     type Decision = Decision;
//!     fn decide(&(link, work): &(Link, Work)) -> Decision {
//!         match (link, work) {
//!             (Link::Down, _) => Decision::Park,
//!             (Link::Up, Work::Idle) => Decision::Wait,
//!             (Link::Up, Work::Pending) => Decision::Send,
//!         }
//!     }
//! }
//!
//! // Every row is enumerated and every declared decision is reachable.
//! let coverage = table_coverage::<Pump>();
//! assert_eq!(coverage.len(), 4);
//! coverage.assert_reaches_all(&[Decision::Park, Decision::Wait, Decision::Send]);
//!
//! // The same table, wired into the graph.
//! let ctx = Context::new();
//! let link = ctx.source(Link::Down);
//! let work = ctx.source(Work::Pending);
//! let decision = state_table::<Pump>(&ctx, ctx.computed(move |c| (link.get(c), work.get(c))));
//! assert_eq!(decision.get(&ctx), Decision::Park);
//! link.set(&ctx, Link::Up);
//! assert_eq!(decision.get(&ctx), Decision::Send);
//! ```

use crate::Context;
use crate::cell::Computed;
use crate::context::Compute;
#[cfg(feature = "thread-safe")]
use crate::thread_safe::ThreadSafeContext;

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/// A total pure transition table: one decision per product state.
///
/// Implement this on a zero-sized marker type. `decide` must be a pure function
/// of `Input` alone — no clock, no I/O, no captured mutable state — because
/// [`state_table`] may call it at any point the graph invalidates, and because
/// restart/replay correctness depends on it.
///
/// Where the language can prove exhaustiveness (a Rust `match` over enums), a
/// missing row is a compile error. Where it cannot — a catch-all arm, an
/// unconstructible-by-convention combination — [`table_coverage`] is the
/// generated test that keeps the gap visible.
pub trait StateTable {
    /// The product state. A finite enum, or a struct/tuple of finite enums.
    type Input;
    /// The exhaustive decision alternative.
    type Decision;

    /// Decide. Total and pure.
    fn decide(input: &Self::Input) -> Self::Decision;
}

/// Wire a table over an existing input cell.
///
/// Both cells are guarded (`PartialEq`), so an upstream write that leaves the
/// product state unchanged does not re-run `decide`, and an input change that
/// lands on the same decision does not wake the effect sink.
pub fn state_table<T>(ctx: &Context, input: Computed<T::Input>) -> Computed<T::Decision>
where
    T: StateTable,
    T::Input: Clone + PartialEq + 'static,
    T::Decision: PartialEq + 'static,
{
    ctx.computed(move |c: &Compute| T::decide(&input.get(c)))
}

/// Project raw facts into the product state, then table it.
///
/// This is the shape real consumers want: `project` reads several sources and
/// classifies their payloads into the finite [`StateTable::Input`], and the
/// returned cell is the decision. Two guarded cells are created rather than
/// one, which is the point — a payload change that does not move the product
/// state stops at the projection and never reaches `decide`.
pub fn projected_state_table<T, F>(ctx: &Context, project: F) -> Computed<T::Decision>
where
    T: StateTable,
    T::Input: Clone + PartialEq + 'static,
    T::Decision: PartialEq + 'static,
    F: Fn(&Compute) -> T::Input + 'static,
{
    let input = ctx.computed(project);
    state_table::<T>(ctx, input)
}

/// Thread-safe [`state_table`]: same semantics over a [`ThreadSafeContext`].
#[cfg(feature = "thread-safe")]
pub fn thread_safe_state_table<T>(
    ctx: &ThreadSafeContext,
    input: Computed<T::Input>,
) -> Computed<T::Decision>
where
    T: StateTable,
    T::Input: Clone + PartialEq + Send + Sync + 'static,
    T::Decision: PartialEq + Send + Sync + 'static,
{
    ctx.computed(move |c: &ThreadSafeContext| T::decide(&c.get(&input)))
}

/// Thread-safe [`projected_state_table`].
#[cfg(feature = "thread-safe")]
pub fn thread_safe_projected_state_table<T, F>(
    ctx: &ThreadSafeContext,
    project: F,
) -> Computed<T::Decision>
where
    T: StateTable,
    T::Input: Clone + PartialEq + Send + Sync + 'static,
    T::Decision: PartialEq + Send + Sync + 'static,
    F: Fn(&ThreadSafeContext) -> T::Input + Send + Sync + 'static,
{
    let input = ctx.computed(project);
    thread_safe_state_table::<T>(ctx, input)
}

// ---------------------------------------------------------------------------
// Finite input axes
// ---------------------------------------------------------------------------

/// A type whose inhabitants can be enumerated, so a table over it has a row set.
///
/// Implement it on each semantic axis; the tuple impls below then give the
/// product for free. Implement it *by hand* on a product type when some
/// combinations are impossible — enumerate the Cartesian product and filter it
/// through the smart constructor, so unrepresentable rows never enter coverage
/// and cannot be quietly decided.
pub trait FiniteState: Sized {
    /// Every inhabitant, in a stable order. Must be non-empty.
    fn all() -> Vec<Self>;
}

impl FiniteState for bool {
    fn all() -> Vec<Self> {
        vec![false, true]
    }
}

impl FiniteState for () {
    fn all() -> Vec<Self> {
        vec![()]
    }
}

impl<T: FiniteState> FiniteState for Option<T> {
    fn all() -> Vec<Self> {
        let mut out = vec![None];
        out.extend(T::all().into_iter().map(Some));
        out
    }
}

macro_rules! finite_tuple {
    ($($name:ident),+) => {
        impl<$($name: FiniteState + Clone),+> FiniteState for ($($name,)+) {
            fn all() -> Vec<Self> {
                let mut out = Vec::new();
                finite_tuple!(@loop out, (), $($name),+);
                out
            }
        }
    };
    (@loop $out:ident, ($($bound:ident),*), $head:ident) => {
        for v in $head::all() {
            $out.push(($($bound.clone(),)* v,));
        }
    };
    (@loop $out:ident, ($($bound:ident),*), $head:ident, $($rest:ident),+) => {
        #[allow(non_snake_case)]
        for $head in $head::all() {
            finite_tuple!(@loop $out, ($($bound,)* $head), $($rest),+);
        }
    };
}

finite_tuple!(A);
finite_tuple!(A, B);
finite_tuple!(A, B, C);
finite_tuple!(A, B, C, D);
finite_tuple!(A, B, C, D, E);
finite_tuple!(A, B, C, D, E, F);

/// Implement [`FiniteState`] for a type by listing its inhabitants.
///
/// ```
/// use lazily::{FiniteState, finite_state};
///
/// #[derive(Clone, Copy, PartialEq, Debug)]
/// enum Link { Down, Up }
/// finite_state!(Link { Link::Down, Link::Up });
///
/// assert_eq!(Link::all().len(), 2);
/// ```
///
/// This is a declarative macro, not a derive: the list is written out, so
/// adding a variant without adding it here is visible in review rather than
/// silently absorbed. When some combinations of a product are impossible,
/// implement [`FiniteState`] by hand and filter through the smart constructor.
#[macro_export]
macro_rules! finite_state {
    ($ty:ty { $($variant:expr),+ $(,)? }) => {
        impl $crate::FiniteState for $ty {
            fn all() -> ::std::vec::Vec<Self> {
                ::std::vec![$($variant),+]
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

/// The full row set of a table: every input state paired with its decision.
///
/// Produced by [`table_coverage`]. This is the reviewable artifact — the thing
/// a reader can scan to see the transition relation without reconstructing it
/// from nested conditionals.
#[derive(Clone, Debug)]
pub struct TableCoverage<I, D> {
    rows: Vec<(I, D)>,
}

impl<I, D> TableCoverage<I, D> {
    /// Every `(input, decision)` row.
    pub fn rows(&self) -> &[(I, D)] {
        &self.rows
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the row set is empty. An empty row set means the coverage test
    /// examined nothing; every assertion below fails on it rather than passing
    /// vacuously.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The decision of every row, in row order.
    pub fn decisions(&self) -> impl Iterator<Item = &D> {
        self.rows.iter().map(|(_, d)| d)
    }

    /// Rows satisfying `pred`.
    pub fn rows_where(&self, pred: impl Fn(&I, &D) -> bool) -> Vec<&(I, D)> {
        self.rows.iter().filter(|(i, d)| pred(i, d)).collect()
    }

    /// Fail unless the row set is non-empty and at least `expected` rows wide.
    ///
    /// A guard against the vacuous pass: an axis that lost its variants, or a
    /// hand-written [`FiniteState`] whose filter rejected everything, otherwise
    /// reports a clean sweep over nothing.
    ///
    /// # Panics
    /// If the row set is empty or narrower than `expected`.
    pub fn assert_at_least(&self, expected: usize) {
        assert!(
            expected > 0,
            "state table coverage: expected must be positive; a floor of 0 cannot fail"
        );
        assert!(
            self.rows.len() >= expected,
            "state table coverage: examined {} row(s), expected at least {expected}",
            self.rows.len()
        );
    }
}

impl<I: std::fmt::Debug, D: std::fmt::Debug> TableCoverage<I, D> {
    /// Fail if any row satisfies `pred`. Use it to assert that no row decides
    /// an `Invalid`/fall-through variant.
    ///
    /// # Panics
    /// If the row set is empty, or any row matches.
    pub fn assert_no_row(&self, what: &str, pred: impl Fn(&I, &D) -> bool) {
        self.assert_at_least(1);
        let hits = self.rows_where(pred);
        assert!(
            hits.is_empty(),
            "state table coverage: {} row(s) unexpectedly {what}: {:?}",
            hits.len(),
            hits
        );
    }

    /// Fail unless every row satisfies `pred`.
    ///
    /// # Panics
    /// If the row set is empty, or any row fails.
    pub fn assert_every_row(&self, what: &str, pred: impl Fn(&I, &D) -> bool) {
        self.assert_at_least(1);
        let misses = self.rows_where(|i, d| !pred(i, d));
        assert!(
            misses.is_empty(),
            "state table coverage: {} row(s) are not {what}: {:?}",
            misses.len(),
            misses
        );
    }
}

impl<I: std::fmt::Debug, D: std::fmt::Debug + PartialEq> TableCoverage<I, D> {
    /// Decisions in `expected` that no row produces — dead alternatives.
    pub fn unreached<'a>(&self, expected: &'a [D]) -> Vec<&'a D> {
        expected
            .iter()
            .filter(|want| !self.rows.iter().any(|(_, d)| d == *want))
            .collect()
    }

    /// Fail unless every decision in `expected` is produced by at least one row.
    ///
    /// A decision alternative no row reaches is either dead code or a row the
    /// table forgot; either way it is a defect, and an exhaustive `match`
    /// cannot see it.
    ///
    /// # Panics
    /// If `expected` is empty, the row set is empty, or any expected decision
    /// is unreached.
    pub fn assert_reaches_all(&self, expected: &[D]) {
        assert!(
            !expected.is_empty(),
            "state table coverage: assert_reaches_all needs at least one expected decision"
        );
        self.assert_at_least(1);
        let missing = self.unreached(expected);
        assert!(
            missing.is_empty(),
            "state table coverage: {} decision(s) unreachable from any row: {missing:?}",
            missing.len()
        );
    }

    /// Fail unless the row set produces exactly the decisions in `expected` —
    /// none missing, none extra.
    ///
    /// # Panics
    /// If any expected decision is unreached, or any row decides something not
    /// in `expected`.
    pub fn assert_decisions_exactly(&self, expected: &[D]) {
        self.assert_reaches_all(expected);
        let extra = self.rows_where(|_, d| !expected.contains(d));
        assert!(
            extra.is_empty(),
            "state table coverage: {} row(s) decide outside the expected set: {:?}",
            extra.len(),
            extra
        );
    }
}

/// Enumerate every input state and record its decision.
///
/// This is the Cartesian product test from the coverage contract. It calls
/// `decide` directly — no [`Context`], no graph — because the table is a pure
/// function; wiring is tested separately.
pub fn table_coverage<T>() -> TableCoverage<T::Input, T::Decision>
where
    T: StateTable,
    T::Input: FiniteState,
{
    let rows = T::Input::all()
        .into_iter()
        .map(|input| {
            let decision = T::decide(&input);
            (input, decision)
        })
        .collect();
    TableCoverage { rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Link {
        Down,
        Up,
    }
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Work {
        Idle,
        Pending,
    }
    finite_state!(Link { Link::Down, Link::Up });
    finite_state!(Work { Work::Idle, Work::Pending });

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Pumped {
        Park,
        Wait,
        Send,
    }

    struct Pump;
    impl StateTable for Pump {
        type Input = (Link, Work);
        type Decision = Pumped;
        fn decide(&(link, work): &(Link, Work)) -> Pumped {
            match (link, work) {
                (Link::Down, _) => Pumped::Park,
                (Link::Up, Work::Idle) => Pumped::Wait,
                (Link::Up, Work::Pending) => Pumped::Send,
            }
        }
    }

    #[test]
    fn tuple_product_enumerates_every_row_in_stable_order() {
        assert_eq!(
            <(Link, Work)>::all(),
            vec![
                (Link::Down, Work::Idle),
                (Link::Down, Work::Pending),
                (Link::Up, Work::Idle),
                (Link::Up, Work::Pending),
            ]
        );
    }

    #[test]
    fn option_axis_adds_the_absent_inhabitant_first() {
        assert_eq!(
            <Option<Link>>::all(),
            vec![None, Some(Link::Down), Some(Link::Up)]
        );
    }

    #[test]
    fn wider_products_stay_row_major() {
        let rows = <(bool, bool, bool)>::all();
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0], (false, false, false));
        assert_eq!(rows[1], (false, false, true));
        assert_eq!(rows[7], (true, true, true));
        assert_eq!(<(bool, bool, bool, bool, bool, bool)>::all().len(), 64);
    }

    #[test]
    fn coverage_enumerates_the_product_and_reaches_every_decision() {
        let coverage = table_coverage::<Pump>();
        coverage.assert_at_least(4);
        assert_eq!(coverage.len(), 4);
        coverage.assert_decisions_exactly(&[Pumped::Park, Pumped::Wait, Pumped::Send]);
    }

    #[test]
    fn unreached_names_the_dead_alternative() {
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        enum Never {
            Reached,
        }
        struct Dead;
        impl StateTable for Dead {
            type Input = (Link, Work);
            type Decision = Option<Never>;
            fn decide(_: &(Link, Work)) -> Option<Never> {
                None
            }
        }
        let coverage = table_coverage::<Dead>();
        assert_eq!(coverage.unreached(&[Some(Never::Reached)]).len(), 1);
        assert!(coverage.unreached(&[None]).is_empty());
    }

    #[test]
    #[should_panic(expected = "expected at least 5")]
    fn assert_at_least_fails_when_the_row_set_shrank() {
        table_coverage::<Pump>().assert_at_least(5);
    }

    #[test]
    #[should_panic(expected = "a floor of 0 cannot fail")]
    fn a_zero_floor_is_rejected_rather_than_passing_vacuously() {
        table_coverage::<Pump>().assert_at_least(0);
    }

    #[test]
    #[should_panic(expected = "examined 0 row(s)")]
    fn assertions_fail_on_an_empty_row_set_instead_of_sweeping_nothing() {
        struct Empty;
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        struct NoRows;
        impl FiniteState for NoRows {
            fn all() -> Vec<Self> {
                Vec::new()
            }
        }
        impl StateTable for Empty {
            type Input = NoRows;
            type Decision = Pumped;
            fn decide(_: &NoRows) -> Pumped {
                Pumped::Park
            }
        }
        table_coverage::<Empty>().assert_reaches_all(&[Pumped::Park]);
    }

    #[test]
    #[should_panic(expected = "unreachable from any row")]
    fn assert_reaches_all_fails_on_a_dead_decision() {
        table_coverage::<Pump>().assert_reaches_all(&[Pumped::Park, Pumped::Wait, Pumped::Send]);
        struct Stuck;
        impl StateTable for Stuck {
            type Input = (Link, Work);
            type Decision = Pumped;
            fn decide(_: &(Link, Work)) -> Pumped {
                Pumped::Park
            }
        }
        table_coverage::<Stuck>().assert_reaches_all(&[Pumped::Park, Pumped::Send]);
    }

    #[test]
    fn a_hand_written_finite_state_filters_impossible_rows_out_of_coverage() {
        // A pump cannot be Down and Pending: the queue drains on disconnect.
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        struct Reachable(Link, Work);
        impl Reachable {
            fn new(link: Link, work: Work) -> Option<Self> {
                match (link, work) {
                    (Link::Down, Work::Pending) => None,
                    _ => Some(Reachable(link, work)),
                }
            }
        }
        impl FiniteState for Reachable {
            fn all() -> Vec<Self> {
                <(Link, Work)>::all()
                    .into_iter()
                    .filter_map(|(l, w)| Reachable::new(l, w))
                    .collect()
            }
        }

        struct Constrained;
        impl StateTable for Constrained {
            type Input = Reachable;
            type Decision = Pumped;
            fn decide(&Reachable(link, work): &Reachable) -> Pumped {
                Pump::decide(&(link, work))
            }
        }

        let coverage = table_coverage::<Constrained>();
        assert_eq!(coverage.len(), 3, "the impossible row must not be decided");
        coverage.assert_decisions_exactly(&[Pumped::Park, Pumped::Wait, Pumped::Send]);
        coverage.assert_no_row("the unreachable Down/Pending row", |i, _| {
            *i == Reachable(Link::Down, Work::Pending)
        });
    }

    #[test]
    fn wiring_derives_the_decision_and_tracks_both_axes() {
        let ctx = Context::new();
        let link = ctx.source(Link::Down);
        let work = ctx.source(Work::Idle);
        let decision = projected_state_table::<Pump, _>(&ctx, move |c| (link.get(c), work.get(c)));

        assert_eq!(decision.get(&ctx), Pumped::Park);
        work.set(&ctx, Work::Pending);
        assert_eq!(decision.get(&ctx), Pumped::Park);
        link.set(&ctx, Link::Up);
        assert_eq!(decision.get(&ctx), Pumped::Send);
        work.set(&ctx, Work::Idle);
        assert_eq!(decision.get(&ctx), Pumped::Wait);
    }
}
