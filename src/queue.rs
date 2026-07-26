//! Reactive queue: [`QueueCell`] + pluggable [`QueueStorage`] backend (#lzqueue).
//!
//! A `QueueCell<T>` is a FIFO collection composed of reactive cells — **not a
//! new cell kind** — that adds queue semantics (push to tail, pop from head) to
//! the reactive graph. It is specified as a **single-producer, single-consumer
//! (SPSC)** primitive; **MPSC** (multi-producer) is a *usage rule* on the same
//! primitive — multiple producers push inside a [`Context::batch`] boundary, and
//! the batch serializes the pushes into a deterministic order. There is no
//! separate `MPSCQueueCell` type (`lazily-spec/cell-model.md` § "QueueCell —
//! SPSC primitive with MPSC usage rule").
//!
//! ## Shell vs storage
//!
//! The reactive shell owns the reader-kinds (`head` / `len` / `is_empty` /
//! `is_full` as demand-driven Slots, `closed` as a Cell) and the invalidation
//! logic; it is storage-agnostic. The storage backend owns the actual FIFO data
//! structure and is pluggable via [`QueueStorage`], whose **minimal required
//! contract** is `try_push` / `try_pop` / `len` / `is_closed` / `close`
//! (`peek`/`capacity` are optional capabilities). The default [`VecDequeStorage`] is an
//! unbounded ring buffer; a bounded variant exposes reactive backpressure via
//! `is_full`. A distributed backend (`RaftQueueStorage`, future work per the
//! distributed-queue PRD) or an external-broker adapter (`KafkaStorage`, etc.)
//! plugs into the same reactive shell.
//!
//! ## Reader-kind invalidation
//!
//! Invalidation is scoped to **reader kind**, not to individual positions. A
//! push invalidates `len` / `is_empty` readers (and `head` when transitioning
//! from empty, and `is_full` when transitioning to capacity); a pop invalidates
//! `head` / `len` / `is_empty` readers (and `is_full` when transitioning off
//! capacity). The head reader observes the *current* head value — after a pop,
//! the head reader sees the next element (or `None`), not a stale value.
//!
//! Reader-kinds are **demand-driven derived Slots**, not eagerly-`Set` cells
//! (`relaycell-backpressure-analysis.md` §5 — a derived reader *is* a Slot). A
//! successful push/pop does not derive any reader value; it only invalidates the
//! Slots whose value **provably changed**, computed from the op plus the pre-op
//! `len()` (exact for any FIFO backend — no `peek` needed to decide):
//!
//! - `len` — every successful push/pop;
//! - `is_empty` — push-to-empty or pop-to-empty;
//! - `is_full` — push-to-capacity or pop-off-capacity (bounded backend only);
//! - `head` — every pop, or push-to-empty.
//!
//! Each reader's value is derived lazily from storage on first `Get` after
//! invalidation and memoized until the next relevant transition. This gives
//! reader-kind independence (a push to a non-empty queue never touches the `head`
//! Slot) without deriving anything eagerly: an **unsubscribed** `QueueCell`
//! collapses toward raw-storage cost per op (the reactive shell is charged only
//! along a path an effect actually observes — the merge cost law, §4.0), while a
//! subscribed reader stays glitch-free because the transitioning Slots are
//! invalidated together in one frontier walk ([`Context::clear_slots`]).
//!
//! ## Closure, bounded backpressure, ordering
//!
//! - **Closure** is an observable contract: pop on closed+non-empty drains;
//!   pop on closed+empty returns [`QueuePopError::Closed`] (distinct from
//!   [`QueuePopError::Empty`]); push on closed is an error; close is idempotent
//!   and terminal.
//! - **Bounded backpressure**: when the backend is bounded, `is_full` is a
//!   reactive read. A consumer's pop that transitions full → not-full
//!   invalidates `is_full` readers (true → false), enabling push-side effects to
//!   react to capacity recovery without polling.
//! - **Ordering**: SPSC gives total FIFO (pop order exactly matches push order).
//!   MPSC gives per-producer FIFO; inter-producer interleaving is deterministic
//!   within a `batch()` but the cross-batch order is batch-sequential.
//!
//! ```
//! use lazily::{Context, QueueCell};
//!
//! let ctx = Context::new();
//! let q: QueueCell<&'static str> = QueueCell::new(&ctx);
//!
//! // SPSC: total FIFO.
//! q.try_push(&ctx, "a").unwrap();
//! q.try_push(&ctx, "b").unwrap();
//! assert_eq!(q.head(&ctx), Some("a"));
//! assert_eq!(q.len(&ctx), 2);
//!
//! assert_eq!(q.try_pop(&ctx).unwrap(), "a");
//! assert_eq!(q.try_pop(&ctx).unwrap(), "b");
//! assert!(q.is_empty(&ctx));
//!
//! // MPSC: multiple producers push inside one batch → one invalidation pass.
//! ctx.batch(|ctx| {
//!     q.try_push(ctx, "p1-a").unwrap();
//!     q.try_push(ctx, "p2-a").unwrap();
//!     q.try_push(ctx, "p1-b").unwrap();
//! });
//! assert_eq!(q.len(&ctx), 3);
//! ```
//!
//! See `lazily-spec/cell-model.md` § "Reactive queues" for the full spec, and
//! `lazily-spec/docs/distributed-queue-prd.md` for the future consensus-backed
//! `RaftQueueStorage` backend.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::rc::Rc;

use crate::Context;
use crate::cell::Computed;
use crate::cell::Source;
use crate::context::ComputeOps;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failure modes for [`QueueStorage::try_push`] / [`QueueCell::try_push`].
///
/// `Full` and `Closed` are the two observable rejection reasons distinguished by
/// the shell's contract (`lazily-spec/cell-model.md` § "Storage backend
/// contract"). Neither changes queue state, so neither invalidates any reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueuePushError {
    /// The backend is bounded and at capacity. The overflow policy (block /
    /// drop-oldest / drop-newest / reject) is a backend property; the reference
    /// [`VecDequeStorage`] rejects. Distinct from `Closed`.
    Full,
    /// The queue is closed; push is rejected regardless of capacity. Terminal —
    /// once closed, a queue cannot be reopened.
    Closed,
}

/// Failure modes for [`QueueStorage::try_pop`] / [`QueueCell::try_pop`].
///
/// `Empty` and `Closed` are distinct observable signals: `Empty` means "try
/// again later," `Closed` means "the producer is done and the queue is drained."
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueuePopError {
    /// The queue is open but contains no elements.
    Empty,
    /// The queue is closed and empty — the producer is done and all buffered
    /// elements have been consumed. Pop on a closed *non-empty* queue still
    /// drains (returns the next element); only closed+empty yields `Closed`.
    Closed,
}

// ---------------------------------------------------------------------------
// QueueStorage trait
// ---------------------------------------------------------------------------

/// Pluggable FIFO storage backend for a [`QueueCell`].
///
/// The shell / storage split (`lazily-spec/cell-model.md` § "Reactive shell vs
/// storage backend") keeps the reactive shell storage-agnostic: the shell owns
/// the reader-kind version cells and invalidation logic, the backend owns the
/// actual FIFO data structure. The default backend is [`VecDequeStorage`]
/// (unbounded `VecDeque`); future backends include `RaftQueueStorage` (embedded
/// consensus, per the distributed-queue PRD) and `KafkaStorage` /
/// `RedisStreamStorage` / `SqsStorage` (external-broker adapters).
///
/// # Conformance
///
/// **Minimal required contract** — a conforming backend MUST implement exactly:
/// [`try_push`](QueueStorage::try_push), [`try_pop`](QueueStorage::try_pop),
/// [`len`](QueueStorage::len), [`is_closed`](QueueStorage::is_closed), and
/// [`close`](QueueStorage::close). [`peek`](QueueStorage::peek) and
/// [`capacity`](QueueStorage::capacity) are **optional capabilities** with
/// default impls returning `None`; a raw channel that satisfies only the five
/// required methods is fully conforming (it simply has no `head`/`is_full`
/// reader). A conforming backend MUST also:
///
/// 1. **FIFO order** — `try_pop` returns elements in `try_push` order.
/// 2. **Cardinality compatibility** — its native producer/consumer shape is a
///    superset of the shell's required shape (SPSC shell = any backend; MPSC
///    usage requires a multi-writer backend).
/// 3. **Bounded contract (optional)** — a bounded backend overrides
///    [`capacity`](QueueStorage::capacity) to return `Some(n)` and `try_push`
///    returns [`Full`](QueuePushError::Full) at capacity. The overflow policy is
///    a backend property. An unbounded backend uses the default `None`.
/// 4. **Position identity** — invalidation is phrased over reader kind, not
///    storage indices. A ring-buffer backend whose slot index wraps MUST NOT
///    cause spurious invalidations; the shell layers its own demand-driven
///    reader-kind Slots above the storage.
//
// `is_empty` is deliberately NOT on this trait: emptiness is a shell-level
// reader kind, not a storage property (the shell derives `is_empty` from
// `len()`). See `lazily-spec/cell-model.md` § "Storage backend contract".
#[allow(clippy::len_without_is_empty)]
pub trait QueueStorage<T> {
    /// Append `value` to the tail. Returns [`QueuePushError::Full`] if bounded
    /// and at capacity, or [`QueuePushError::Closed`] if the queue is closed.
    /// On error the queue state is unchanged.
    fn try_push(&mut self, value: T) -> Result<(), QueuePushError>;

    /// Remove and return the head element. Returns [`QueuePopError::Empty`] if
    /// open and empty, or [`QueuePopError::Closed`] if closed and empty. Pop on
    /// a closed *non-empty* queue drains (returns the next element).
    fn try_pop(&mut self) -> Result<T, QueuePopError>;

    /// **Optional capability.** Peek the current head element without removing
    /// it, or `None` when empty. The shell reads this to materialize its `head`
    /// reader-kind Slot.
    ///
    /// This is **not** part of the required contract. The default returns `None`,
    /// so a backend that cannot peek (a raw channel, a consuming stream) is fully
    /// conforming — it simply has no meaningful `head` reader, exactly as an
    /// unbounded backend (`capacity() == None`) has no meaningful `is_full`. A
    /// backend that *can* cheaply inspect its head overrides this to expose a
    /// reactive `head`.
    ///
    /// *Footnote — `LookaheadShim`.* A caller that needs a `head` over a
    /// non-peekable backend MAY opt into a shell-level lookahead shim that
    /// prefetches (early-pops) one element into a one-slot buffer and reports it
    /// as the head. This is **SPSC-local only**: early-popping is incorrect for
    /// competing-consumer or consensus backends, where the popped element must
    /// not be committed to one consumer before assignment. The shim is not part
    /// of the core and is out of scope for the minimal contract.
    fn peek(&self) -> Option<&T> {
        None
    }

    /// Current number of buffered elements. **Required.**
    fn len(&self) -> usize;

    /// **Optional capability.** Bounded capacity, or `None` for an unbounded
    /// backend (the default). When `Some(n)`, the shell exposes `is_full` as a
    /// reactive backpressure read; when `None`, `is_full` is trivially `false`.
    fn capacity(&self) -> Option<usize> {
        None
    }

    /// Whether the queue has been closed. Close is terminal — once true, it
    /// stays true.
    fn is_closed(&self) -> bool;

    /// Close the queue. Idempotent — closing an already-closed queue is a
    /// no-op. After close, [`try_push`](QueueStorage::try_push) returns
    /// [`Closed`](QueuePushError::Closed); [`try_pop`](QueueStorage::try_pop)
    /// continues to drain buffered elements and returns
    /// [`Closed`](QueuePopError::Closed) only once empty.
    fn close(&mut self);
}

// ---------------------------------------------------------------------------
// VecDequeStorage — the reference unbounded/bounded backend
// ---------------------------------------------------------------------------

/// The reference [`QueueStorage`] backend: a `VecDeque`-backed FIFO, optionally
/// bounded.
///
/// The unbounded form (the default) is what [`QueueCell::new`] uses; the bounded
/// form ([`VecDequeStorage::with_capacity`] / [`QueueCell::with_capacity`])
/// exposes reactive backpressure via the shell's `is_full` reader. The overflow
/// policy is **reject** — `try_push` at capacity returns
/// [`QueuePushError::Full`] (elements are never silently dropped); other
/// backends may choose block / drop-oldest / drop-newest.
///
/// Serializes as a JSON array (element order = FIFO order) for conformance
/// fixture purposes, matching `lazily-spec/cell-model.md` § "Wire and snapshot
/// shape".
#[derive(Debug, Clone, Default)]
pub struct VecDequeStorage<T> {
    elements: VecDeque<T>,
    capacity: Option<usize>,
    closed: bool,
}

impl<T> VecDequeStorage<T> {
    /// Create an unbounded storage (no capacity limit).
    pub fn unbounded() -> Self {
        Self {
            elements: VecDeque::new(),
            capacity: None,
            closed: false,
        }
    }

    /// Create a bounded storage that rejects pushes once it holds `capacity`
    /// elements.
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0` (a zero-capacity queue can never accept an
    /// element and has no useful semantics).
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "VecDequeStorage capacity must be > 0");
        Self {
            elements: VecDeque::with_capacity(capacity),
            capacity: Some(capacity),
            closed: false,
        }
    }

    /// Snapshot the buffered elements in FIFO order (clone of each element).
    /// Non-reactive — for snapshot/serde and conformance-fixture verification.
    pub fn elements(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.elements.iter().cloned().collect()
    }
}

impl<T> QueueStorage<T> for VecDequeStorage<T> {
    fn try_push(&mut self, value: T) -> Result<(), QueuePushError> {
        if self.closed {
            return Err(QueuePushError::Closed);
        }
        if let Some(cap) = self.capacity
            && self.elements.len() >= cap
        {
            return Err(QueuePushError::Full);
        }
        self.elements.push_back(value);
        Ok(())
    }

    fn try_pop(&mut self) -> Result<T, QueuePopError> {
        match self.elements.pop_front() {
            Some(v) => Ok(v),
            None => {
                if self.closed {
                    Err(QueuePopError::Closed)
                } else {
                    Err(QueuePopError::Empty)
                }
            }
        }
    }

    fn peek(&self) -> Option<&T> {
        self.elements.front()
    }

    fn len(&self) -> usize {
        self.elements.len()
    }

    fn capacity(&self) -> Option<usize> {
        self.capacity
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn close(&mut self) {
        self.closed = true;
    }
}

#[cfg(feature = "serde")]
impl<T> serde::Serialize for VecDequeStorage<T>
where
    T: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(None)?;
        for e in &self.elements {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

// ---------------------------------------------------------------------------
// QueueCell — the reactive shell
// ---------------------------------------------------------------------------

struct QueueCellInner<T, S> {
    // `Rc` so each reader-kind Slot's compute closure can capture the storage
    // independently of this struct (the struct also holds the Slot handles, so
    // a closure cannot borrow the struct itself without a cycle).
    storage: Rc<std::cell::RefCell<S>>,
    // Cached at construction: capacity is a contractually fixed backend property
    // (`QueueStorage` §Conformance). Caching it keeps the per-op invalidation
    // path off the storage borrow when deciding whether `is_full` transitioned.
    capacity: Option<usize>,
    // Reader-kind derived Slots (demand-driven). Each derives its value lazily
    // from `storage` on first `Get` after invalidation and memoizes it; the
    // shell invalidates only the Slots whose value provably changed on a given
    // op (see the module docs). `closed` stays a `Cell` because it changes only
    // via `close()`, never derived from a mutation transition.
    head: Computed<Option<T>>,
    len: Computed<usize>,
    is_empty: Computed<bool>,
    is_full: Computed<bool>,
    closed: Source<bool>,
}

/// A reactive FIFO queue — SPSC primitive with an MPSC usage rule
/// (`#lzqueue`).
///
/// Cheap to [`Clone`] (an `Rc` to shared shell state), so the same queue can be
/// handed to producer and consumer closures. The reactive shell wraps a
/// pluggable [`QueueStorage`] backend (default [`VecDequeStorage`]); the shell
/// owns the reader-kind version cells (`head` / `len` / `is_empty` / `is_full`
/// / `closed`) and invalidates by reader kind — a push to a non-empty queue does
/// NOT invalidate the `head` reader, a pop does. See the module docs for the
/// full reader-kind independence contract.
pub struct QueueCell<T, S = VecDequeStorage<T>> {
    inner: Rc<QueueCellInner<T, S>>,
}

impl<T, S> Clone for QueueCell<T, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T> QueueCell<T, VecDequeStorage<T>>
where
    T: PartialEq + Clone + 'static,
{
    /// Create an unbounded queue (the default reference backend).
    pub fn new(ctx: &Context) -> Self {
        Self::with_storage(ctx, VecDequeStorage::unbounded())
    }

    /// Create a bounded queue with `capacity`. Exposes reactive backpressure via
    /// [`is_full`](Self::is_full): a pop that transitions full → not-full
    /// invalidates `is_full` readers.
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0`.
    pub fn with_capacity(ctx: &Context, capacity: usize) -> Self {
        Self::with_storage(ctx, VecDequeStorage::with_capacity(capacity))
    }
}

impl<T, S> QueueCell<T, S>
where
    T: PartialEq + Clone + 'static,
    S: QueueStorage<T> + 'static,
{
    /// Build a queue over an arbitrary [`QueueStorage`] backend. The shell wires
    /// its reader-kind Slots to derive lazily from the backend; no value is
    /// materialized until a reader is first observed.
    pub fn with_storage(ctx: &Context, storage: S) -> Self {
        let closed_val = storage.is_closed();
        let capacity = storage.capacity();
        let storage = Rc::new(std::cell::RefCell::new(storage));

        // Each reader-kind Slot memoizes a value derived from storage. The Slot
        // has no reactive dependencies (storage is out-of-graph), so once
        // computed it stays clean until the shell explicitly invalidates it on a
        // transition (see `invalidate_readers`).
        let head = {
            let storage = Rc::clone(&storage);
            ctx.computed(move |_ctx| storage.borrow().peek().cloned())
        };
        let len = {
            let storage = Rc::clone(&storage);
            ctx.computed(move |_ctx| storage.borrow().len())
        };
        let is_empty = {
            let storage = Rc::clone(&storage);
            ctx.computed(move |_ctx| storage.borrow().len() == 0)
        };
        let is_full = {
            let storage = Rc::clone(&storage);
            ctx.computed(move |_ctx| {
                let s = storage.borrow();
                match s.capacity() {
                    Some(cap) => s.len() >= cap,
                    None => false,
                }
            })
        };

        Self {
            inner: Rc::new(QueueCellInner {
                storage,
                capacity,
                head,
                len,
                is_empty,
                is_full,
                closed: ctx.source(closed_val),
            }),
        }
    }

    /// Invalidate exactly the reader-kind Slots whose derived value changed on a
    /// successful op that took the queue from `len_before` to `len_after`, in one
    /// atomic frontier walk so a subscribed observer never sees a partial state
    /// (e.g. `len` bumped but `is_full` not yet flipped). `head_changed` is
    /// passed by the caller because head depends on op *direction*, not just
    /// `len` (a pop always changes head; a push changes it only from empty).
    ///
    /// No reader value is derived here — invalidation only clears the changed
    /// Slots' caches (each re-derives lazily on its next `Get`). A reader-kind
    /// with no subscribers and no cache hits the `clear_slots` no-op fast path,
    /// so an unobserved op skips all derivation, effect scheduling, and flush.
    /// `closed` is never touched here: it changes only via [`close`](Self::close).
    fn invalidate_readers(
        &self,
        ctx: &Context,
        len_before: usize,
        len_after: usize,
        head_changed: bool,
    ) {
        let is_empty_changed = (len_before == 0) != (len_after == 0);
        let is_full_changed = self
            .inner
            .capacity
            .map(|c| (len_before >= c) != (len_after >= c))
            .unwrap_or(false);

        // Fixed-capacity buffer (no heap alloc on the hot path). `len` always
        // changes on a successful op, so it is always the first root.
        let mut roots = [self.inner.len.id; 4];
        let mut n = 1;
        if is_empty_changed {
            roots[n] = self.inner.is_empty.id;
            n += 1;
        }
        if is_full_changed {
            roots[n] = self.inner.is_full.id;
            n += 1;
        }
        if head_changed {
            roots[n] = self.inner.head.id;
            n += 1;
        }
        ctx.clear_slots(&roots[..n]);
    }

    /// Append `value` to the tail of the queue.
    ///
    /// Returns [`QueuePushError::Full`] if bounded and at capacity (reject
    /// policy — the default `VecDequeStorage` never silently drops), or
    /// [`QueuePushError::Closed`] if the queue is closed. On error the queue
    /// state is unchanged and no reader is invalidated.
    ///
    /// Invalidates `head` (only when transitioning from empty), `len`, and
    /// `is_empty` readers as appropriate; `is_full` when transitioning onto
    /// capacity. Does not touch `closed`.
    pub fn try_push(&self, ctx: &Context, value: T) -> Result<(), QueuePushError> {
        let (result, len_before) = {
            let mut s = self.inner.storage.borrow_mut();
            let len_before = s.len();
            (s.try_push(value), len_before)
        };
        if result.is_ok() {
            self.invalidate_readers(ctx, len_before, len_before + 1, len_before == 0);
        }
        result
    }

    /// Remove and return the head element.
    ///
    /// Returns [`QueuePopError::Empty`] if open and empty, or
    /// [`QueuePopError::Closed`] if closed and empty. Pop on a closed
    /// *non-empty* queue drains (returns the next element).
    ///
    /// Invalidates `head` (always — the head value changes), `len`, and
    /// `is_empty` (when transitioning to empty) readers as appropriate;
    /// `is_full` when transitioning off capacity.
    pub fn try_pop(&self, ctx: &Context) -> Result<T, QueuePopError> {
        let (result, len_before) = {
            let mut s = self.inner.storage.borrow_mut();
            let len_before = s.len();
            (s.try_pop(), len_before)
        };
        if result.is_ok() {
            // A successful pop always advances head and always decrements len.
            self.invalidate_readers(ctx, len_before, len_before - 1, true);
        }
        result
    }

    /// Close the queue. Idempotent — closing an already-closed queue is a
    /// no-op (no invalidation). Terminal — once closed, a queue cannot be
    /// reopened. After close, [`try_push`](Self::try_push) returns `Closed`;
    /// [`try_pop`](Self::try_pop) continues to drain and returns `Closed` only
    /// once empty.
    ///
    /// Invalidates the `closed` reader only on the false → true transition.
    pub fn close(&self, ctx: &Context) {
        let was_closed = self.inner.storage.borrow().is_closed();
        if was_closed {
            return;
        }
        self.inner.storage.borrow_mut().close();
        ctx.set(&self.inner.closed, true);
    }

    // -- Reactive reader-kind reads ----------------------------------------

    /// Reactive read of the current head value. `None` when the queue is empty.
    /// A reader is invalidated when the head value *changes* — every pop, and a
    /// push only when transitioning from empty.
    pub fn head<C: ComputeOps>(&self, ctx: &C) -> Option<T> {
        self.inner.head.get(ctx)
    }

    /// Handle to the `head` reader-kind Slot, for wiring derived computeds
    /// directly. Subscribe-to-head semantics: invalidated on head-value change.
    pub fn head_handle(&self) -> Computed<Option<T>> {
        self.inner.head
    }

    /// Reactive read of the number of buffered elements. Invalidated whenever
    /// the count changes (every successful push/pop).
    pub fn len<C: ComputeOps>(&self, ctx: &C) -> usize {
        self.inner.len.get(ctx)
    }

    /// Reactive emptiness check. Invalidated only on the empty ↔ non-empty
    /// transition.
    pub fn is_empty<C: ComputeOps>(&self, ctx: &C) -> bool {
        self.inner.is_empty.get(ctx)
    }

    /// Reactive fullness check (only meaningful when the backend is bounded).
    /// Invalidated on the full ↔ not-full transition — this is the backpressure
    /// signal: a producer observes `is_full` and backs off; a consumer's pop
    /// that transitions full → not-full invalidates the producer's `is_full`
    /// subscription and the producer resumes. For an unbounded backend this is
    /// always `false` and never invalidates.
    pub fn is_full<C: ComputeOps>(&self, ctx: &C) -> bool {
        self.inner.is_full.get(ctx)
    }

    /// Reactive read of the closed flag. Invalidated only on the open → closed
    /// transition.
    pub fn is_closed<C: ComputeOps>(&self, ctx: &C) -> bool {
        self.inner.closed.get(ctx)
    }

    /// Handles to the reader-kind nodes, for advanced wiring (e.g. effects that
    /// subscribe to multiple reader kinds).
    pub fn reader_handles(&self) -> QueueReaderHandles<T> {
        QueueReaderHandles {
            head: self.inner.head,
            len: self.inner.len,
            is_empty: self.inner.is_empty,
            is_full: self.inner.is_full,
            closed: self.inner.closed,
        }
    }

    // -- Non-reactive storage access ---------------------------------------

    /// The backend's capacity, or `None` if unbounded. Cached at construction
    /// (capacity is a fixed backend property), so this does not touch storage.
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity
    }
}

impl<T> QueueCell<T, VecDequeStorage<T>>
where
    T: PartialEq + Clone + 'static,
{
    /// Snapshot the buffered elements in FIFO order. Non-reactive — for
    /// debugging, snapshot/serde, and conformance-fixture verification. There
    /// is no reactive random-access `queue[N]` reader; per-position reactivity
    /// is the domain of `SourceMap`, not `QueueCell`.
    pub fn elements(&self) -> Vec<T> {
        self.inner.storage.borrow().elements()
    }
}

/// Handles to all five reader-kinds of a [`QueueCell`], for effects that need to
/// subscribe to several reader kinds at once. The four derived reader-kinds are
/// demand-driven [`Computed`]s; `closed` is a [`Source`] because it is a
/// direct input (set by [`close`](QueueCell::close)), not a derived value.
#[derive(Debug, Clone, Copy)]
pub struct QueueReaderHandles<T> {
    /// The head value (`None` when empty).
    pub head: Computed<Option<T>>,
    /// The element count.
    pub len: Computed<usize>,
    /// Whether the queue is empty.
    pub is_empty: Computed<bool>,
    /// Whether the queue is at capacity (bounded backpressure signal).
    pub is_full: Computed<bool>,
    /// Whether the queue has been closed.
    pub closed: Source<bool>,
}

// ---------------------------------------------------------------------------
// TopicCell — broadcast log with independent reactive subscriber cursors
// ---------------------------------------------------------------------------

/// Whether a [`TopicCell`] subscription survives disconnects and participates
/// in the retention frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicDurability {
    /// Cursor state persists while disconnected and holds back safe GC.
    Durable,
    /// Cursor state exists only for the connected session and never holds GC.
    Ephemeral,
}

/// Public, serializable-in-spirit state for one topic subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSubscriptionSnapshot {
    /// Absolute offset of the next element to read.
    pub cursor: u64,
    /// Durable subscriptions survive disconnect; ephemeral ones are removed.
    pub durability: TopicDurability,
    /// Offline durable subscriptions retain data but are not scheduled.
    pub connected: bool,
}

/// Durable state required to recreate a [`TopicCell`] without moving cursors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSnapshot<T, I: Eq + Hash = String> {
    /// Absolute offset of `elements[0]`.
    pub base_offset: u64,
    /// Retained append log, oldest first.
    pub elements: Vec<T>,
    /// Stable subscriber identity to persisted subscription state.
    pub subscriptions: HashMap<I, TopicSubscriptionSnapshot>,
}

/// Result of subscribing a stable identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicSubscribeOutcome {
    /// A new cursor was created at the current tail.
    Created,
    /// An offline durable cursor was reconnected without moving it.
    Reconnected,
    /// The identity was already connected; no state changed.
    AlreadyConnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TopicSubscription {
    cursor: u64,
    durability: TopicDurability,
    connected: bool,
}

struct TopicState<T, I> {
    base_offset: u64,
    elements: VecDeque<T>,
    subscriptions: HashMap<I, TopicSubscription>,
}

struct TopicCellInner<T, I> {
    state: Rc<std::cell::RefCell<TopicState<T, I>>>,
    // A distinct demand-driven reader per stable subscriber is the essential
    // invalidation boundary: publish fans out to connected readers; advance,
    // disconnect, and reconnect touch only the named reader.
    readers: std::cell::RefCell<HashMap<I, Computed<Vec<T>>>>,
}

/// A broadcast topic: every subscriber receives every published element using
/// an independent, non-destructive cursor (`#lztopiccell`).
///
/// Elements are retained until every durable cursor has passed them. Ephemeral
/// subscriptions start at the current tail, disappear on disconnect, and never
/// hold the GC frontier. Each subscription owns a demand-driven reactive read
/// stream, so advancing one subscriber never invalidates another subscriber.
pub struct TopicCell<T, I = String> {
    inner: Rc<TopicCellInner<T, I>>,
}

impl<T, I> Clone for TopicCell<T, I> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T, I> TopicCell<T, I>
where
    T: PartialEq + Clone + 'static,
    I: Eq + Hash + Clone + 'static,
{
    /// Create an empty topic at absolute offset zero.
    pub fn new(_ctx: &Context) -> Self {
        Self {
            inner: Rc::new(TopicCellInner {
                state: Rc::new(std::cell::RefCell::new(TopicState {
                    base_offset: 0,
                    elements: VecDeque::new(),
                    subscriptions: HashMap::new(),
                })),
                readers: std::cell::RefCell::new(HashMap::new()),
            }),
        }
    }

    /// Restore an atomic topic snapshot. Durable cursors are preserved exactly;
    /// connected ephemeral records are accepted for live-state fixture replay.
    ///
    /// # Panics
    ///
    /// Panics if any cursor lies outside `base_offset..=end_offset`.
    pub fn from_snapshot(ctx: &Context, snapshot: TopicSnapshot<T, I>) -> Self {
        let end_offset = snapshot.base_offset + snapshot.elements.len() as u64;
        for sub in snapshot.subscriptions.values() {
            assert!(
                (snapshot.base_offset..=end_offset).contains(&sub.cursor),
                "TopicCell cursor must be within the retained absolute offset range"
            );
            assert!(
                sub.durability != TopicDurability::Ephemeral || sub.connected,
                "disconnected ephemeral TopicCell subscriptions must be removed"
            );
        }

        let state = Rc::new(std::cell::RefCell::new(TopicState {
            base_offset: snapshot.base_offset,
            elements: snapshot.elements.into(),
            subscriptions: snapshot
                .subscriptions
                .into_iter()
                .map(|(id, sub)| {
                    (
                        id,
                        TopicSubscription {
                            cursor: sub.cursor,
                            durability: sub.durability,
                            connected: sub.connected,
                        },
                    )
                })
                .collect(),
        }));
        let topic = Self {
            inner: Rc::new(TopicCellInner {
                state,
                readers: std::cell::RefCell::new(HashMap::new()),
            }),
        };
        let ids: Vec<I> = topic
            .inner
            .state
            .borrow()
            .subscriptions
            .keys()
            .cloned()
            .collect();
        for id in ids {
            topic.ensure_reader(ctx, id);
        }
        topic
    }

    fn ensure_reader(&self, ctx: &Context, id: I) -> Computed<Vec<T>> {
        if let Some(handle) = self.inner.readers.borrow().get(&id) {
            return *handle;
        }
        let state = Rc::clone(&self.inner.state);
        let reader_id = id.clone();
        let handle = ctx.computed(move |_ctx| {
            let state = state.borrow();
            let Some(sub) = state.subscriptions.get(&reader_id) else {
                return Vec::new();
            };
            if !sub.connected {
                return Vec::new();
            }
            let skip = sub.cursor.saturating_sub(state.base_offset) as usize;
            state.elements.iter().skip(skip).cloned().collect()
        });
        self.inner.readers.borrow_mut().insert(id, handle);
        handle
    }

    /// Create a cursor at the current tail, or reconnect an existing durable
    /// identity without moving its cursor. Once an identity exists its stored
    /// durability wins over the caller's argument.
    pub fn subscribe(
        &self,
        ctx: &Context,
        id: I,
        durability: TopicDurability,
    ) -> TopicSubscribeOutcome {
        let existing = {
            let mut state = self.inner.state.borrow_mut();
            if let Some(sub) = state.subscriptions.get_mut(&id) {
                if sub.connected {
                    Some((TopicSubscribeOutcome::AlreadyConnected, false))
                } else {
                    sub.connected = true;
                    Some((TopicSubscribeOutcome::Reconnected, true))
                }
            } else {
                let cursor = state.base_offset + state.elements.len() as u64;
                state.subscriptions.insert(
                    id.clone(),
                    TopicSubscription {
                        cursor,
                        durability,
                        connected: true,
                    },
                );
                None
            }
        };

        let reader = self.ensure_reader(ctx, id);
        match existing {
            Some((outcome, true)) => {
                ctx.clear_slots(&[reader.id]);
                outcome
            }
            Some((outcome, false)) => outcome,
            None => TopicSubscribeOutcome::Created,
        }
    }

    /// Reconnect a durable identity, preserving its cursor. Unknown identities
    /// are created as durable subscriptions at the current tail.
    pub fn reconnect(&self, ctx: &Context, id: I) -> TopicSubscribeOutcome {
        self.subscribe(ctx, id, TopicDurability::Durable)
    }

    /// Disconnect a subscriber. Durable records remain offline at the same
    /// cursor; ephemeral records and their retention-neutral session state are
    /// removed. Returns whether state changed.
    pub fn disconnect(&self, ctx: &Context, id: &I) -> bool {
        let (changed, remove_reader) = {
            let mut state = self.inner.state.borrow_mut();
            let Some(sub) = state.subscriptions.get_mut(id) else {
                return false;
            };
            if !sub.connected {
                return false;
            }
            if sub.durability == TopicDurability::Ephemeral {
                state.subscriptions.remove(id);
                (true, true)
            } else {
                sub.connected = false;
                (true, false)
            }
        };
        let reader = if remove_reader {
            self.inner.readers.borrow_mut().remove(id)
        } else {
            self.inner.readers.borrow().get(id).copied()
        };
        if let Some(reader) = reader {
            ctx.clear_slots(&[reader.id]);
        }
        changed
    }

    /// Append exactly one element, leaving every cursor unchanged. Returns its
    /// absolute offset and invalidates every connected subscriber independently.
    pub fn publish(&self, ctx: &Context, value: T) -> u64 {
        let (offset, connected): (u64, Vec<I>) = {
            let mut state = self.inner.state.borrow_mut();
            let offset = state.base_offset + state.elements.len() as u64;
            state.elements.push_back(value);
            let connected = state
                .subscriptions
                .iter()
                .filter(|(_, sub)| sub.connected && sub.cursor <= offset)
                .map(|(id, _)| id.clone())
                .collect();
            (offset, connected)
        };
        let readers = self.inner.readers.borrow();
        let roots: Vec<_> = connected
            .iter()
            .filter_map(|id| readers.get(id).map(|reader| reader.id))
            .collect();
        ctx.clear_slots(&roots);
        offset
    }

    /// Reactive suffix read for one connected subscriber. Unknown and offline
    /// subscribers observe an empty stream.
    pub fn read_stream(&self, ctx: &Context, id: &I) -> Vec<T> {
        self.inner
            .readers
            .borrow()
            .get(id)
            .copied()
            .map(|reader| ctx.get(&reader))
            .unwrap_or_default()
    }

    /// Reactive read of the element at a subscriber's cursor.
    pub fn read(&self, ctx: &Context, id: &I) -> Option<T> {
        self.read_stream(ctx, id).into_iter().next()
    }

    /// Advance only the named connected cursor by one, returning the element it
    /// passed. At the tail, for an offline subscriber, or for an unknown id this
    /// is a no-op returning `None`.
    pub fn advance(&self, ctx: &Context, id: &I) -> Option<T> {
        let value = {
            let mut state = self.inner.state.borrow_mut();
            let (cursor, connected) = state
                .subscriptions
                .get(id)
                .map(|sub| (sub.cursor, sub.connected))?;
            let end_offset = state.base_offset + state.elements.len() as u64;
            if !connected || cursor >= end_offset {
                return None;
            }
            let index = cursor.saturating_sub(state.base_offset) as usize;
            let value = state.elements.get(index).cloned()?;
            state
                .subscriptions
                .get_mut(id)
                .expect("subscription exists")
                .cursor += 1;
            value
        };
        if let Some(reader) = self.inner.readers.borrow().get(id) {
            ctx.clear_slots(&[reader.id]);
        }
        Some(value)
    }

    /// Remove the prefix below the minimum durable cursor, or all retained
    /// elements when no durable subscription exists. Subscriber cursors remain
    /// absolute, so safe GC invalidates no reader. Returns the removed count.
    pub fn gc(&self) -> usize {
        let mut state = self.inner.state.borrow_mut();
        let end_offset = state.base_offset + state.elements.len() as u64;
        let frontier = state
            .subscriptions
            .values()
            .filter(|sub| sub.durability == TopicDurability::Durable)
            .map(|sub| sub.cursor)
            .min()
            .unwrap_or(end_offset);
        let remove = frontier.saturating_sub(state.base_offset) as usize;
        state.elements.drain(..remove);
        state.base_offset = frontier;
        remove
    }

    /// Absolute offset of the first retained element.
    pub fn base_offset(&self) -> u64 {
        self.inner.state.borrow().base_offset
    }

    /// Absolute offset immediately after the retained append log.
    pub fn end_offset(&self) -> u64 {
        let state = self.inner.state.borrow();
        state.base_offset + state.elements.len() as u64
    }

    /// Non-reactive retained-log snapshot, oldest first.
    pub fn elements(&self) -> Vec<T> {
        self.inner.state.borrow().elements.iter().cloned().collect()
    }

    /// Snapshot one subscription record.
    pub fn subscription(&self, id: &I) -> Option<TopicSubscriptionSnapshot> {
        self.inner
            .state
            .borrow()
            .subscriptions
            .get(id)
            .map(|sub| TopicSubscriptionSnapshot {
                cursor: sub.cursor,
                durability: sub.durability,
                connected: sub.connected,
            })
    }

    /// Handle to the named subscriber's reactive unread suffix.
    pub fn reader_handle(&self, id: &I) -> Option<Computed<Vec<T>>> {
        self.inner.readers.borrow().get(id).copied()
    }

    /// Atomic durable/live-state snapshot suitable for restart and conformance.
    pub fn snapshot(&self) -> TopicSnapshot<T, I> {
        let state = self.inner.state.borrow();
        TopicSnapshot {
            base_offset: state.base_offset,
            elements: state.elements.iter().cloned().collect(),
            subscriptions: state
                .subscriptions
                .iter()
                .map(|(id, sub)| {
                    (
                        id.clone(),
                        TopicSubscriptionSnapshot {
                            cursor: sub.cursor,
                            durability: sub.durability,
                            connected: sub.connected,
                        },
                    )
                })
                .collect(),
        }
    }
}

// `WorkQueueCell` remains separate future work: N consumers compete for
// exclusive handoff, which requires an authority to serialize assignment.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spsc_fifo_basic() {
        let ctx = Context::new();
        let q: QueueCell<i32> = QueueCell::new(&ctx);
        assert!(q.is_empty(&ctx));
        assert_eq!(q.head(&ctx), None);

        q.try_push(&ctx, 1).unwrap();
        q.try_push(&ctx, 2).unwrap();
        q.try_push(&ctx, 3).unwrap();

        assert_eq!(q.len(&ctx), 3);
        assert_eq!(q.head(&ctx), Some(1));
        assert_eq!(q.elements(), vec![1, 2, 3]);

        assert_eq!(q.try_pop(&ctx).unwrap(), 1);
        assert_eq!(q.try_pop(&ctx).unwrap(), 2);
        assert_eq!(q.try_pop(&ctx).unwrap(), 3);
        assert_eq!(q.try_pop(&ctx), Err(QueuePopError::Empty));
    }

    #[test]
    fn bounded_rejects_at_capacity() {
        let ctx = Context::new();
        let q = QueueCell::<i32>::with_capacity(&ctx, 2);
        assert_eq!(q.capacity(), Some(2));
        assert!(!q.is_full(&ctx));

        q.try_push(&ctx, 1).unwrap();
        q.try_push(&ctx, 2).unwrap();
        assert!(q.is_full(&ctx));
        assert_eq!(q.try_push(&ctx, 3), Err(QueuePushError::Full));

        // pop frees a slot → is_full flips → reactive backpressure signal
        assert_eq!(q.try_pop(&ctx).unwrap(), 1);
        assert!(!q.is_full(&ctx));
        q.try_push(&ctx, 3).unwrap();
        assert!(q.is_full(&ctx));
    }

    #[test]
    fn closure_lifecycle() {
        let ctx = Context::new();
        let q: QueueCell<&str> = QueueCell::new(&ctx);
        q.try_push(&ctx, "a").unwrap();
        q.try_push(&ctx, "b").unwrap();

        q.close(&ctx);
        assert!(q.is_closed(&ctx));

        // push on closed is an error
        assert_eq!(q.try_push(&ctx, "c"), Err(QueuePushError::Closed));

        // pop on closed+non-empty drains
        assert_eq!(q.try_pop(&ctx).unwrap(), "a");
        assert_eq!(q.try_pop(&ctx).unwrap(), "b");

        // pop on closed+empty returns Closed (distinct from Empty)
        assert_eq!(q.try_pop(&ctx), Err(QueuePopError::Closed));

        // idempotent close — no-op, no invalidation
        q.close(&ctx);
        assert!(q.is_closed(&ctx));
    }

    #[test]
    fn reader_kind_independence_head_not_invalidated_on_push_to_nonempty() {
        let ctx = Context::new();
        let q: QueueCell<i32> = QueueCell::new(&ctx);

        let head_reader = ctx.computed({
            let q = q.clone();
            move |ctx| q.head(ctx)
        });
        assert_eq!(ctx.get(&head_reader), None);

        q.try_push(&ctx, 1).unwrap();
        // push to empty changes head → invalidated
        assert_eq!(ctx.get(&head_reader), Some(1));

        q.try_push(&ctx, 2).unwrap();
        q.try_push(&ctx, 3).unwrap();
        // head reader still cached (head unchanged) — reader-kind independence
        assert!(
            ctx.is_set(&head_reader),
            "push to non-empty must not invalidate head reader"
        );

        q.try_pop(&ctx).unwrap();
        // pop changes head → invalidated
        assert_eq!(ctx.get(&head_reader), Some(2));
    }

    #[test]
    fn mpsc_via_batch_is_one_invalidation_pass() {
        let ctx = Context::new();
        let q: QueueCell<i32> = QueueCell::new(&ctx);

        let len_reader = ctx.computed({
            let q = q.clone();
            move |ctx| q.len(ctx)
        });
        assert_eq!(ctx.get(&len_reader), 0);

        ctx.batch(|ctx| {
            q.try_push(ctx, 10).unwrap();
            q.try_push(ctx, 20).unwrap();
            q.try_push(ctx, 30).unwrap();
        });
        // After the batch the len reader is invalidated exactly once and sees 3.
        assert!(
            !ctx.is_set(&len_reader),
            "batch should have invalidated the len reader once"
        );
        assert_eq!(ctx.get(&len_reader), 3);
        assert_eq!(q.elements(), vec![10, 20, 30]);
    }

    #[test]
    fn clone_shares_state() {
        let ctx = Context::new();
        let q: QueueCell<i32> = QueueCell::new(&ctx);
        let producer = q.clone();
        producer.try_push(&ctx, 42).unwrap();
        assert_eq!(q.try_pop(&ctx).unwrap(), 42);
    }

    /// A raw-channel-style backend that implements ONLY the minimal required
    /// contract — `try_push` / `try_pop` / `len` / `is_closed` / `close` — and
    /// deliberately does not override `peek` or `capacity`. This is the shape of
    /// a Go channel / consuming mpsc: no lookahead, no bound advertised. It
    /// proves the minimal contract (0c): such a backend is fully conforming and
    /// simply has no meaningful `head`/`is_full` reader.
    #[derive(Default)]
    struct MinimalFifoStorage<T> {
        elements: VecDeque<T>,
        closed: bool,
    }

    impl<T> QueueStorage<T> for MinimalFifoStorage<T> {
        fn try_push(&mut self, value: T) -> Result<(), QueuePushError> {
            if self.closed {
                return Err(QueuePushError::Closed);
            }
            self.elements.push_back(value);
            Ok(())
        }

        fn try_pop(&mut self) -> Result<T, QueuePopError> {
            match self.elements.pop_front() {
                Some(v) => Ok(v),
                None if self.closed => Err(QueuePopError::Closed),
                None => Err(QueuePopError::Empty),
            }
        }

        fn len(&self) -> usize {
            self.elements.len()
        }

        fn is_closed(&self) -> bool {
            self.closed
        }

        fn close(&mut self) {
            self.closed = true;
        }
        // NB: no `peek`, no `capacity` — the trait defaults apply.
    }

    #[test]
    fn raw_channel_backend_conforms_to_minimal_contract() {
        let ctx = Context::new();
        let q: QueueCell<i32, MinimalFifoStorage<i32>> =
            QueueCell::with_storage(&ctx, MinimalFifoStorage::default());

        // FIFO + len + is_empty derive from the required methods alone.
        assert!(q.is_empty(&ctx));
        q.try_push(&ctx, 1).unwrap();
        q.try_push(&ctx, 2).unwrap();
        assert_eq!(q.len(&ctx), 2);
        assert!(!q.is_empty(&ctx));

        // No peek capability → no meaningful head reader (trivially None), just
        // as an unbounded backend has no meaningful is_full (trivially false).
        assert_eq!(q.head(&ctx), None);
        assert!(!q.is_full(&ctx));
        assert_eq!(q.capacity(), None);

        // FIFO drain order is preserved by try_pop alone.
        assert_eq!(q.try_pop(&ctx).unwrap(), 1);
        assert_eq!(q.try_pop(&ctx).unwrap(), 2);
        assert!(q.is_empty(&ctx));

        // Closure lifecycle: Closed distinct from Empty, push-after-close errors.
        q.close(&ctx);
        assert!(q.is_closed(&ctx));
        assert_eq!(q.try_push(&ctx, 3), Err(QueuePushError::Closed));
        assert_eq!(q.try_pop(&ctx), Err(QueuePopError::Closed));
    }

    /// A subscribed reader over the minimal backend still reacts correctly: the
    /// demand-driven `len` Slot invalidates on each op even without `peek`.
    #[test]
    fn raw_channel_backend_reader_kinds_stay_reactive() {
        let ctx = Context::new();
        let q: QueueCell<i32, MinimalFifoStorage<i32>> =
            QueueCell::with_storage(&ctx, MinimalFifoStorage::default());

        let len_reader = ctx.computed({
            let q = q.clone();
            move |ctx| q.len(ctx)
        });
        assert_eq!(ctx.get(&len_reader), 0);

        q.try_push(&ctx, 10).unwrap();
        assert!(!ctx.is_set(&len_reader), "push must invalidate len reader");
        assert_eq!(ctx.get(&len_reader), 1);

        q.try_pop(&ctx).unwrap();
        assert!(!ctx.is_set(&len_reader), "pop must invalidate len reader");
        assert_eq!(ctx.get(&len_reader), 0);
    }
}
