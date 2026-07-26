# lazily-rs Specification

Rust library for lazy evaluation with context-aware dependency tracking and cache invalidation. Counterpart to lazily-zig and lazily-py.

## Core Concepts

### Context

Container for all slots and cells. Owns all allocations via interior mutability
using a single `RefCell<ContextInner>`.

```rust
struct ContextInner {
    nodes: Vec<Option<Node>>,
    next_id: u64,
    free_ids: Vec<u64>,
    pending_effects: VecDeque<SlotId>,
    scheduled_effects: HashSet<SlotId>,
    flushing_effects: bool,
    batch_depth: usize,
    batched_cells: HashSet<SlotId>,
    batched_cell_clears: HashSet<SlotId>,
    batched_slots: HashSet<SlotId>,
}

pub struct Context {
    inner: RefCell<ContextInner>,
}
```

**API:**

| Method | Purpose |
|--------|---------|
| `Context::new()` | Create a new context |
| `ctx.computed(\|ctx\| T)` | Create a derived lazily-computed value |
| `ctx.slot(\|ctx\| T)` | Create a lazily-computed slot; synonym of `ctx.computed()` |
| `ctx.memo(\|ctx\| T)` | Create a lazily-computed slot with a `PartialEq` memoization guard |
| `slot.get(&ctx)` | Get value (computes if unset) |
| `ctx.get(&slot)` | Context method alias for `slot.get(&ctx)` |
| `ctx.get_rc(&slot)` | Get slot value as `Rc<T>`, avoiding deep clone |
| `ctx.source(value)` | Create a mutable cell |
| `source.get(&ctx)` | Get cell value |
| `ctx.get(&cell)` | Context method alias for `source.get(&ctx)` |
| `ctx.get_rc(&source)` | Get source value as `Rc<T>`, avoiding deep clone |
| `ctx.set(&cell, value)` | Update cell (marks dependents dirty if changed) |
| `source.set(&ctx, value)` | Handle method alias for `ctx.set(&cell, value)` |
| `ctx.batch(\|ctx\| { ... })` | Defer changed-cell dirty marking and explicit clears until the outermost batch exits |
| `ctx.effect(\|ctx\| { ... })` | Run an effect immediately and rerun it after tracked dependencies invalidate |
| `ctx.is_set(&slot)` | Check if slot has a cached, fresh value |
| `slot.clear(&ctx)` | Clear cached value and cascade to dependents |
| `cell.clear_dependents(&ctx)` | Clear downstream slots without changing cell value |
| `effect.dispose(&ctx)` | Dispose an effect, unsubscribe dependencies, and run cleanup |
| `effect.is_active(&ctx)` | Check whether an effect is still registered |

`Context` stores nodes in a slot-id-indexed `Vec<Option<Node>>` rather than a
hash map. `SlotId` values are allocated sequentially; effect disposal returns the
ID to a free list for reuse, preventing unbounded `Vec` growth from transient
effects while keeping lookups contiguous and hash-free.

Dependency and dependent edges use `SmallVec<[SlotId; 4]>` rather than `HashSet<SlotId>`.
For the typical 1-3 dependency fan-out, SmallVec stores edges inline without heap
allocation, avoids hash computation overhead, and eliminates the temporary
`Vec<SlotId>` allocations that were previously required in every refresh/clear/dirty
hot path (replaced with `SmallVec::clone()` or `std::mem::take`). Sets that require
true dedup semantics (scheduled effects, batch queues, tracking frames) remain as
`HashSet<SlotId>`.

The single-threaded `Context` consolidates all mutable state behind one
`RefCell<ContextInner>` instead of ten separate `RefCell` fields. This reduces
borrow-check overhead from 4-6 flag modifications per `get()` to 1 and eliminates
the risk of re-entrant borrows across fields. Slot and effect compute closures are
stored as `Rc<dyn Fn>` so they can be cloned cheaply without `unsafe` pointer copies.

### ThreadSafeContext

Mutex-backed counterpart to `Context` for sharing one reactive graph across OS
threads. It mirrors the core local-context API and requires thread-safe values
and callbacks.

```rust
pub struct ThreadSafeContext {
    inner: Arc<ThreadSafeInner>,
}
```

**API:**

| Method | Purpose |
|--------|---------|
| `ThreadSafeContext::new()` | Create a new thread-safe context |
| `ctx.computed(\|ctx\| T)` | Create a `Send + Sync` derived lazily-computed value |
| `ctx.slot(\|ctx\| T)` | Create a `Send + Sync` lazily-computed slot |
| `ctx.memo(\|ctx\| T)` | Create a `Send + Sync` lazily-computed slot with a `PartialEq` memoization guard |
| `slot.get(&ctx)` | Get value from any thread (computes if unset) |
| `ctx.get(&slot)` | Context method alias for `slot.get(&ctx)` |
| `ctx.get_arc(&slot)` | Get slot value as `Arc<T>`, avoiding deep clone (the `Send + Sync` analog of `ctx.get_rc`) |
| `ctx.source(value)` | Create a mutable `Send + Sync` cell |
| `source.get(&ctx)` | Get cell value from any thread |
| `ctx.get(&cell)` | Context method alias for `source.get(&ctx)` |
| `ctx.set(&cell, value)` | Update cell and invalidate dependents across threads |
| `ctx.batch(\|ctx\| { ... })` | Defer invalidation until the outermost shared batch exits |
| `ctx.effect(\|ctx\| { ... })` | Run a `Send + Sync` effect immediately and rerun it after tracked dependencies invalidate |
| `ctx.clear(&slot)` | Clear cached value and cascade to dependents |
| `ctx.clear_cell_dependents(&cell)` | Clear downstream slots without changing cell value |
| `ctx.dispose_effect(&effect)` | Dispose an effect, unsubscribe dependencies, and run cleanup |
| `ctx.is_effect_active(&effect)` | Check whether an effect is still registered |

### Slot

Lazily-computed cached value with dependency tracking. A Slot is **fresh**, **dirty**, or **unset**; dirty slots may retain a previous cached value for memo validation.
`ctx.memo()` creates a Slot whose values implement `PartialEq` so dirty caches can be compared against recomputed values.

```rust
type ComputeFn = dyn Fn(&Context) -> Rc<dyn Any>;
type EqualsFn = dyn Fn(&dyn Any, &dyn Any) -> bool;

struct SlotNode {
    value: Option<Rc<dyn Any>>,
    type_id: TypeId,
    compute: Rc<ComputeFn>,
    equals: Option<Box<EqualsFn>>,
    dependencies: SmallVec<[SlotId; 4]>,
    dependents: SmallVec<[SlotId; 4]>,
    dirty: bool,
    force_recompute: bool,
}
```

**Semantics:**

- **Activation:** First `ctx.get()` calls the compute function, caches the result
- **Computed alias:** `ctx.computed()` creates the same Slot as `ctx.slot()` for derived-value ergonomics
- **Invalidation:** Marks the cached value dirty and marks downstream slots dirty without discarding their cached values
- **Clearing:** Explicit `slot.clear(&ctx)` removes the cached value and clears all dependent slots recursively
- **Memo guard:** Dirty `ctx.memo()` slots compare recomputed values with the previous cache via `PartialEq`; equal values make downstream dirty slots fresh without recomputing them
- **Dependencies:** If Slot B accesses Slot A during computation, B depends on A. If A clears, B clears automatically; if A's value changes after dirty validation, B is forced stale
- **Immutable by default:** Once set, a Slot's value doesn't change — only clear + recompute
- **Dynamic:** Dependencies re-discovered on each recomputation (no stale subscriptions)

### Cell

Mutable value container. Changing a Cell's value marks dependent Slots dirty.

```rust
struct CellNode {
    value: Rc<dyn Any>,
    type_id: TypeId,
    dependents: SmallVec<[SlotId; 4]>,
}
```

**Semantics:**

- `ctx.set()` and `source.set(&ctx, value)` compare old and new via `PartialEq`
- If unchanged, no invalidation occurs (no-op)
- If changed, dependent Slots are marked dirty while cached values are preserved for memo validation

### Effect

Side-effect callback that automatically tracks dependencies. Effects run
immediately on creation, then rerun after any Cell or Slot read during the last
run is invalidated.

```rust
type EffectFn = dyn Fn(&Context) -> Option<Box<dyn FnOnce()>>;

struct EffectNode {
    run: Rc<EffectFn>,
    dependencies: SmallVec<[SlotId; 4]>,
    cleanup: Option<Box<dyn FnOnce()>>,
    force_run: bool,
}
```

**Semantics:**

- **Immediate activation:** `ctx.effect()` runs the callback once during creation
- **Auto-tracking:** Any Slot or Cell accessed during the callback becomes a dependency
- **Scheduling:** Dependency invalidation schedules the effect, then the context flushes scheduled effects after the invalidation pass
- **Coalescing:** An effect scheduled through multiple dependency paths in the same invalidation pass runs once
- **Memo guard:** Effects scheduled by dirty slot dependencies first validate those slots and skip cleanup/rerun when values are unchanged
- **Cleanup:** Returning a cleanup closure runs it before the next rerun and on disposal
- **Disposal:** `effect.dispose(&ctx)` unsubscribes from dependencies, removes pending scheduled work, and prevents future reruns

### Signal

Eager derived value. A Signal sits one step beyond a Slot on the
`Slot -> Cell -> Signal` progression: where a Slot is **lazy** (invalidation
only marks it dirty; the value is recomputed on the next read), a Signal is
**eager** — it recomputes the instant any dependency is invalidated. The value
is always materialized, so observers never see an intermediate unset value: a
dependency change drives the value directly from `v1` to `v2`.

A Signal is composed from existing primitives: a memoized Slot (`ctx.memo`)
plus a small puller Effect that re-materializes the slot after every
invalidation. This composition is intentional — it inherits the Slot's
glitch-free, pull-based recomputation and the memo guard, while the Effect
supplies eagerness.

```rust
let n = ctx.source(1);
let doubled = ctx.signal(|ctx| n.get(ctx) * 2); // materialized now: 2
n.set(&ctx, 5);                                  // doubled is already 10
assert_eq!(doubled.get(&ctx), 10);
```

**Semantics:**

- **Eager activation:** `ctx.signal()` computes the value once at creation; the value is set from the start
- **Eager recomputation:** Dependency invalidation recomputes the value during the invalidation flush, before the invalidating `set`/`set`/`batch` call returns — no read is required to drive it
- **No unset state:** The backing slot is invalidated via dirty-marking (not hard-cleared), so the value transitions `v1 -> v2` and is never observed as unset
- **Memo guard:** Backed by `ctx.memo`, a recomputation that yields an equal value (via `PartialEq`) does not invalidate downstream dependents
- **Glitch-free:** Recomputation is pull-based; a Signal that reads other Signals/Slots always observes values consistent with the current inputs (e.g. a diamond `D = f(A, g(A))` never surfaces a mixed new-`A`/old-`g(A)` intermediate)
- **Batch coalescing:** Writes inside `ctx.batch()` settle to a single consistent recomputation at batch exit
- **Type bounds:** `signal<T>` requires `T: PartialEq + 'static` (for the memo guard); `get_signal` additionally requires `T: Clone`
- **Disposal:** `signal.dispose(&ctx)` removes the eager puller; the value remains readable and reverts to lazy (recomputed on next read) behavior

#### Signal across context types

The eager-Signal primitive is exposed on all three context types with the same
`memo-slot + puller-effect` composition, so shared-graph and async consumers get
the same always-set, glitch-free `v1 -> v2` derived values that the
single-threaded `Context` provides (#lzsignalparity).

- **`ThreadSafeContext::signal`** — shared-graph counterpart. Returns a
  `ThreadSafeSignalHandle<T>` with `.get`/`.dispose`/`.is_active(&ctx)` helpers
  and matching `ctx.get_signal`/`dispose_signal`/`is_signal_active`. Recomputation
  is eager (driven during the invalidation flush before the `set`/`batch`
  call returns), glitch-free, memo-guarded, and batch-coalesced — identical to
  the single-threaded semantics above. Type bounds add `Send + Sync`:
  `signal<T>` requires `T: PartialEq + Send + Sync + 'static`; `get_signal`
  additionally requires `T: Clone`. The handle is `Copy + Send + Sync` and may be
  read from any thread sharing the context.
- **`AsyncContext::signal_async`** — async counterpart. Returns an
  `AsyncSignalHandle<T>` backed by `memo_async` plus an `effect_async` puller
  that awaits the slot after every invalidation. Reads: `ctx.get_signal` (or
  `handle.get`) returns `Option<T>` as a non-blocking snapshot;
  `ctx.get_signal_async` (or `handle.get_async`) awaits the up-to-date value.
  Inside a slot/effect callback, `AsyncComputeContext::get_signal_async` reads a
  signal and registers its backing slot as a dependency, enabling chained async
  signals and downstream observers. Type bounds:
  `T: PartialEq + Clone + Send + Sync + 'static`.
  - **Eagerness is runtime-driven:** because resolution is asynchronous, the
    puller drives the recompute to completion on the runtime shortly after the
    invalidating write rather than synchronously within it.
  - **Propagation is not suppressed on equal recompute:** the async memo guard
    keeps the *value* correct on an equal recompute, but — unlike the
    single-threaded/thread-safe graph — does not suppress downstream
    propagation (async invalidation force-reruns effect dependents on every
    upstream change). No inconsistent (glitch) value is ever observed. This
    matches the documented `async memo does not suppress downstream propagation`
    behavior of `memo_async`.

### Batch

Write-coalescing boundary for multiple cell updates or explicit slot clears.

```rust
ctx.batch(|ctx| {
    ctx.set(&a, 1);
    ctx.set(&b, 2);
});
```

**Semantics:**

- **Outermost boundary:** Nested batches flush only when the outermost batch exits
- **Changed cells:** `ctx.set()` still updates the cell value immediately, but dependent dirty marking is queued until batch exit
- **Explicit clears:** `slot.clear(&ctx)` and `cell.clear_dependents(&ctx)` are queued until batch exit
- **Coalescing:** Repeated updates to the same cell or clears of the same slot queue one invalidation root
- **Thread-safe local batching:** same-thread `ThreadSafeContext` batch writes buffer changed cells and clears in a thread-local batch frame, then merge that frame into the graph-owned batch queue at batch exit; cross-thread writes during another active batch still fall back to the graph-owned queue
- **Effect flushing:** Effects scheduled by batched invalidation rerun after the batch invalidation pass and coalesce duplicate schedules
- **Reads during a batch:** Direct `ctx.get()` reads see the latest cell value immediately; dependent slot reads keep their pre-batch cached value until dirty marking flushes at batch exit

### State Machine

A finite state machine built on top of `Source` and the reactive graph.
`StateMachine<S, E>` wraps a `Source<S>` as the current state and a pure
transition function `Fn(&S, &E) -> Option<S>`.

```rust
use lazily::{Context, StateMachine};

let ctx = Context::new();
let m = StateMachine::new(&ctx, Door::Closed, |s, e| match (s, e) {
    (Door::Closed, DoorEvent::Button) => Some(Door::Opening),
    (Door::Opening, DoorEvent::Open) => Some(Door::Open),
    _ => None,
});

m.send(&ctx, DoorEvent::Button);
assert_eq!(m.state(&ctx), Door::Opening);
```

**API:**

| Method | Description |
|--------|-------------|
| `StateMachine::new(ctx, initial, transition_fn)` | Create with initial state + pure transition function |
| `send(ctx, event) -> bool` | Evaluate transition; `true` if accepted, `false` if rejected (`None`) |
| `state(ctx) -> S` | Read the current state |
| `state_handle() -> Source<S>` | Underlying cell for reactive dependencies |
| `on_transition(ctx, \|old, new\| ...) -> EffectHandle` | Observer that fires on each state change with `(old, new)` |
| `state_is(ctx, target) -> SignalHandle<bool>` | Eager signal: `true` when in `target` state |

**Semantics:**

- **PartialEq guard:** A transition to an equal state is accepted (`true`) but does not invalidate dependents (the `Source` equality guard suppresses no-op updates). To force re-entry, call `cell.clear_dependents(ctx)` before `send`.
- **Reactive integration:** Any `ctx.computed`, `ctx.memo`, `ctx.signal`, or `ctx.effect` that reads `state_handle()` automatically recomputes/reruns on transition.
- **On-enter / on-exit:** Use `ctx.effect` with cleanup — the effect body is on-enter, the returned cleanup closure is on-exit (runs before the next rerun). Alternatively, use `on_transition` for a single `(old, new)` observer.
- **Batch atomicity:** `ctx.batch()` coalesces multiple `send()` calls — effects fire once after the batch settles.
- **Single-threaded:** `StateMachine` is backed by `Context` (single-threaded `RefCell`).

#### `ThreadSafeStateMachine` — cross-thread

`ThreadSafeStateMachine<S, E>` is the lock-backed counterpart to
`StateMachine`, mirroring the same API over [`ThreadSafeContext`](#threadsafcontext).
The transition function and state must be `Send + Sync + 'static`, so the
machine and its owning context can be shared across OS threads. The machine is
`Clone` — cloning yields another handle to the same state cell and transition
function.

```rust
use lazily::{ThreadSafeContext, ThreadSafeStateMachine};
use std::sync::Arc;

let ctx = Arc::new(ThreadSafeContext::new());
let m = ThreadSafeStateMachine::new(&ctx, Door::Closed, |s, e| match (s, e) {
    (Door::Closed, DoorEvent::Button) => Some(Door::Opening),
    _ => None,
});

m.send(&ctx, DoorEvent::Button);
assert_eq!(m.state(&ctx), Door::Opening);
```

`on_transition` returns an `EffectHandle` (dispose via
`ctx.dispose_effect`); `state_is` returns a `ThreadSafeSignalHandle<bool>`.
Observers fire synchronously within the invalidating `send`/`batch` call,
preserving the glitch-free pull-based ordering of `ThreadSafeContext`.

#### `AsyncStateMachine` — Tokio (`async` feature)

`AsyncStateMachine<S, E>` is the async counterpart, backed by
[`AsyncContext`](#asynccontext). The state lives in an `AsyncSource<S>`;
because cells are the synchronous input layer of `AsyncContext`, `send` and
`state` are synchronous. Reactive observers use the async effect/signal APIs:
`on_transition` returns an `AsyncEffectHandle` and `state_is` returns an
`AsyncSignalHandle<bool>`. Because resolution is asynchronous, eager
recomputation settles on the runtime rather than synchronously within `send`.

```rust
use lazily::{AsyncContext, AsyncStateMachine};

let ctx = AsyncContext::new();
let m = AsyncStateMachine::new(&ctx, Door::Closed, |s, e| match (s, e) {
    (Door::Closed, DoorEvent::Button) => Some(Door::Opening),
    _ => None,
});

m.send(&ctx, DoorEvent::Button);
assert_eq!(m.state(&ctx), Door::Opening);
```

### Regression property harness

The default Rust test suite includes `tests/property_graph.rs`, a `proptest`
harness that drives a fixed reactive graph with generated programs of cell
sets, equal-value sets, `cell.clear_dependents`, `slot.clear`, memo-guarded
dependencies, batch boundaries, reads, and effect disposal/recreation.

Each generated program is checked against a pure model:

- Cell and slot reads must match the model after every operation.
- Equal-value cell sets must not schedule effect cleanup/rerun work.
- Same-parity cell changes through a `memo` slot must preserve downstream
effect run counts when the observable output is unchanged.
- Explicit slot and cell-dependent clears must hard-clear cached slots when no
effect is active, and must rerun active effects to re-prime their dependencies.
- Operations inside `ctx.batch` must not clear cached dependent slots or run
effect cleanups/reruns until the outermost batch exits; dependent reads during
the batch must continue to see the pre-batch cached value.

### SlotId

Unique identifier for reactive nodes. Lightweight `Copy` type wrapping a `u64`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(u64);
```

Both `Computed<T>` and `Source<T>` wrap a `SlotId` with `PhantomData<T>` for type safety.

## Keyed cell collections (`CellFamily` / `SourceMap`)

`SourceMap<K, V>` and `CellFamily<K, V>` add a *keyed* layer over the flat `SlotId`
address space: a hash collection whose **membership is itself reactive**, with one
independently-tracked value cell per entry.

- **`SourceMap<K, V>`** — a `K → Source<V>` map with two independent reactivity
  surfaces:
  - **Per-entry value reactivity.** Each entry is its own cell, so a reader that
    depends on entry `a` is invalidated only when `a` changes — never when a sibling
    entry `b` changes. This is the *fine-grained* model; it is the opposite of a coarse
    `Cell<HashMap<K, V>>`, where any single-entry write replaces the whole map and
    invalidates every reader.
  - **Reactive membership and order (two independent signals).** Two version cells are
    tracked separately: a **set-membership** signal (bumped when a key is added or
    removed) and an **order** signal (bumped on add/remove **and** on move/reorder).
    `len()`, `is_empty()`, and `contains_key()` subscribe to set-membership only;
    `keys()` subscribes to the order signal. So adding/removing invalidates both; a
    **pure reorder invalidates only `keys()` readers**, leaving `len()`/`contains_key()`
    readers cached. Mutators bump via an untracked write so they never register a
    spurious dependency on the caller's frame.
- **`CellFamily<K, V>`** — a parameterized factory (à la Recoil/Jotai `atomFamily`)
  layered on `SourceMap`: it lazily mints and caches one cell per distinct key on first
  `get(key)`, via a `Fn(&K) -> V` factory. Repeated `get`s of the same key return the
  same cell.

### Wire-stable keyed addressing (`#lzwirekey`)

A `SourceMap`/`CellFamily` entry is addressable internally by its key, but on the
wire (`PROTOCOL.md`) a peer historically could only refer to it by the opaque,
volatile `NodeId` it happened to receive — which a producer may re-mint under a
*new* value after a resync or a remove-then-readd. The wire protocol therefore
carries an optional, wire-stable **`NodeKey`**: a `/`-joined path (`scores/alice`,
`outer/k1/inner/k2`) attached to the `NodeSnapshot` and `NodeAdd` ops that
introduce a node.

- **Additive and backward compatible.** `NodeKey` never changes `NodeId`
  semantics; it is an optional field. Self-describing codecs (JSON, MessagePack)
  **omit** it when absent, so pre-`key` encoders/decoders and existing
  conformance fixtures round-trip unchanged; positional Postcard always carries
  the optional discriminant for binary schema stability.
- **Path = nesting.** A multi-segment path addresses nested collections (an
  entry of a `SourceMap` inside a `SourceMap` entry) with no extra machinery. Length
  (≤ 1024 bytes) and segment count (≤ 32) are bounded and rejected on the wire.
- **Consumer key index.** A subscriber maintains a bijective `NodeKey ↔ NodeId`
  index (`KeyIndex`): ingesting a `Snapshot` or applying a keyed `NodeAdd` /
  `NodeRemove` keeps a key resolvable across NodeId churn, so a key-expressed
  subscription (`node_for_key("scores/alice")`) stays valid when the entry is
  removed and re-added under a fresh NodeId.
- **Producer wiring is staged.** The wire types, codecs, and consumer index land
  with `#lzwirekey`; threading each entry's `NodeKey` through the runtime→IPC
  projection waits on the graph→snapshot producer (today `NodeSnapshot`s are
  constructed only at the transport seam, not minted from a live `SourceMap`).
  Multi-producer key-uniqueness is owned by the distributed plane's last-writer
  rule (`#lzcrdtplane`), not this protocol.

### Atomic ordered move (`#lzcellmove`)

`SourceMap` exposes `move_to(key, index)`, `move_before(key, anchor)`, and
`move_after(key, anchor)`: the **atomic, optimized** reorder. A move MUST keep the
entry's **same value cell** (handle identity), its dependents, and its CRDT lineage —
unlike the naive `remove` + `entry`, which re-mints the cell and bumps membership twice.
A move MUST bump only the order signal (once), and MUST be a no-op (no invalidation) when
the requested position equals the current position. `index` is clamped to `[0, len)`.

### Ordered keyed tree (`CellTree`, `#lzordtree`)

`CellTree<Id, V>` is a **shallow-or-deep, ordered, stably-keyed tree** composed from the
primitives above — the document shape (root → components → items, each with a stable id)
that keyed reconciliation (`#lzkeyrecon`) and per-cell CRDT merge build on. Each node is
`(id, value_cell, ordered children)`:

- **Stable `id`** survives reorder and value edits.
- **`value` cell** gives per-node value reactivity: editing node `X` MUST invalidate only
  readers of `X`, never a sibling or descendant.
- **Ordered children** are a `SourceMap`-backed reactive collection, so `child_ids()`/`len()`
  and order are reactive **per level**: a reader of one node's children MUST NOT be
  invalidated by a membership/order change in a *sibling* subtree or a deeper descendant.
  `move_child*` inherit the atomic-move guarantee (child node keeps identity + subtree).

`CellTree` is cheap to `Clone` (`Rc` to shared node state), giving **structural sharing**:
the same subtree node may be held in several places. Conformance: per-node value
reactivity, per-level membership/order reactivity, and atomic child move MUST hold; a
`CellTree` is a *composition* of cells (value cells + a keyed ordered collection), not a
new cell kind.

Conformance: a keyed collection MUST keep value reactivity, set-membership reactivity, and
order reactivity independent (an entry-value write MUST NOT invalidate membership or order
readers; a pure reorder MUST NOT invalidate membership readers), and MUST return a stable
cell handle for the lifetime of a key. Entry removal clears the removed entry's dependents;
the underlying `SlotId` is not required to be recycled (the runtime exposes no node-free
API today). Each entry remains an ordinary cell, so the single-writer / multi-write
classification and per-cell merge rules in the cell model apply to entries unchanged — a
keyed collection (and the tree composed from it) is a *composition* of cells, not a new
cell kind.

### Keyed reconciliation (`#lzkeyrecon`)

`reconcile(old, new)` diffs two keyed sequences **by stable key, not position**, and
returns the minimal `{Insert, Remove, Move, Update}` op set that transforms `old` into
`new`:

- Keys in `old` only → `Remove`; keys in `new` only → `Insert{index}`; keys in both with a
  changed value → `Update`.
- **Move-minimization** is mandatory: of the keys present in both, those already in relative
  order (the longest-increasing-subsequence over their old indices) MUST stay put; only the
  remainder emit `Move{to}`. Move count therefore equals `(keys in both) − |LIS|`, which is
  the minimum number of single-element moves — strictly fewer than a remove-all + insert-all
  whenever any key is shared. `index`/`to` are positions in the final sequence; applied in
  emitted order (removes, then inserts/moves left-to-right, then updates) they reproduce
  `new`.

`apply_to_map` (and the `SourceMap::reconcile` convenience) drive a reactive `SourceMap` from
the op set. Conformance for the reactive application: a **stable** entry (in the LIS, value
unchanged) MUST NOT have its value cell invalidated by a sibling reorder — moves go through
the atomic-move path, so reactivity is proportional to what actually changed. This minimal
per-item op set, applied per-cell, is the enabling step for per-cell CRDT merge of document
trees (replacing whole-subtree replacement).

### Manufactured identity for text (`#lzstableid`)

Plain markdown has no node ids, so reconciliation needs identity *manufactured* from text.
`block_key` / `align` / `assign_stable_keys` provide it in three layers of decreasing
certainty:

- **Anchored ids** — an in-band marker/id on a block (agent-doc emits these). Exact, and
  MUST survive an arbitrary rewrite of the block body (matched by id, classified `Same`).
- **Content-derived keys** — a hash of the block's *normalized* text (whitespace collapsed),
  so an unchanged block keeps its key across reflow/rewrap/reorder; an edit changes it.
- **Alignment** — a block with no exact key match is matched to the most-similar unmatched
  old block above a threshold (word-LCS ratio `2·|LCS|/(|a|+|b|)`, nearest index breaking
  ties) and classified `Edited`; otherwise `Inserted`. Unmatched old blocks are `Removed`.
  This distinguishes an *edit* from a real *insert*; a true rewrite legitimately reads as
  insert+remove — no identity remains to preserve.

`assign_stable_keys` is the bridge to `#lzkeyrecon`: a `Same`/`Edited` block reuses its
matched old block's key so identity flows through an edit (the reconciler emits `Update`,
not remove+insert); an `Inserted` block gets its own key. This is why agent-doc leans on
in-band anchors — the cheapest way to buy stable identity over fundamentally unstable text.

### Memoized semantic tree (`SemTree`, `#lzsemtree`)

The syntactic `CellTree` holds *input* cells; the **semantic** tree (unresolved prompts,
drainable heads, section summaries) is a layer of **memoized `computed` nodes** derived from
it. `SemTree::build` creates one `memo` slot per node that folds
`(node value, child derived values) -> D`.

Because each node has its own memo slot and a parent reads its *children's* derived slots
(not their raw cells), the derivation MUST be **incremental and glitch-free**:

- Editing one node recomputes only that node's **ancestor chain** — a **sibling subtree's**
  derived value MUST stay cached (it is not in the dirty cone). This is the lazy-pull win:
  cost is proportional to the diff, not the document.
- The **memo guard** means a node edit that does **not** change the folded result MUST NOT
  cause a downstream consumer of an ancestor to re-run (the value version doesn't bump).

Incrementality covers value edits, removals, and reorders of children; a child *insertion*
adds a node the captured fold doesn't know about yet, so structural growth calls `build`
again (cheap — only slot allocation; unchanged subtrees still won't recompute later). The
rule: don't materialize semantics eagerly — derive them as memoized computeds and pay only
for what actually changed.

### Free-text CRDT + re-parse (`TextCrdt`, `#lztextcrdt`)

The anchored layers above buy identity for *controlled* structure. For arbitrary prose with
no anchors under **concurrent** edits, the merge unit drops to **characters**. `TextCrdt`
merges keystrokes; the structural tree is then a **projection** of the merged text
(re-parse), not the merge unit itself.

- **Algorithm** — a Fugue/RGA-style tree CRDT. Each inserted character is an element with a
  unique `OpId` and a **left origin** (the element it was typed after). The sequence is the
  in-order traversal of the origin tree, same-origin siblings ordered by `OpId` descending.
  Deletes are tombstones carrying the **delete's own `OpId`** (not a bare flag) so GC can
  test deletion stability; `order` is therefore a pure function of the element set, so
  `merge` (union of elements, tombstones sticky — concurrent deletes converge to the smaller
  delete `OpId`) MUST be commutative, associative, and idempotent, and concurrent inserts at
  the same point MUST converge deterministically with both preserved.
- **Tombstone GC** (`#lztombgc`) — `gc_with(is_stable)` reclaims tombstones the caller proves
  causally stable ("every replica has observed the deletion"; the policy is supplied by the
  distributed plane `#lzcrdtplane`, never derived from a single replica's clock). It is
  deliberately conservative: an element is collected only when it is **not referenced as any
  element's left origin**, so removal never orphans a survivor; interior tombstones are
  reclaimed **bottom-up** as their descendants are collected. Contiguous-run compaction with
  origin-rewrite is the heavier follow-up.
- **Re-parse** — `parse_blocks` splits merged text into blocks; feed them through
  `assign_stable_keys` (`#lzstableid`) + `reconcile` (`#lzkeyrecon`) to project the merged
  text onto the keyed tree. An unchanged block keeps its key (identity) across the
  text-CRDT merge.
- **Honest floor** — a true rewrite *is* a replace: there is no character identity to
  preserve through it. The anchored layer keeps per-node lineage; the free-text layer's
  guarantee is "merge the text, re-derive the tree," which is the correct floor for prose
  the user can edit arbitrarily.

Together, the seven primitives (`#lzcellmove` atomic move → `#lzordtree` ordered keyed tree
→ `#lzkeyrecon` keyed reconciliation → `#lzseqcrdt` sequence order → `#lzstableid`
manufactured identity → `#lzsemtree` memoized semantics → `#lztextcrdt` free-text CRDT) are
the substrate for a reactive, per-cell-mergeable agent-doc document.

## Dependency Tracking

Uses a thread-local tracking stack (mirroring lazily-zig's `TrackingFrame` approach).

1. When a Slot computes, it pushes a frame onto the tracking stack
2. When an Effect runs, it also pushes a frame onto the tracking stack
3. Any nested slot/cell access sees the parent frame
4. The child registers the parent as a dependent
5. When a dependency clears, slot dependents hard-clear recursively; when a Cell changes, slot dependents are marked dirty and effect dependents are scheduled

## Threading and Concurrency Contract

### Current `Context`

`Context` is intentionally local to one OS thread. It owns `RefCell` graph state,
cached values as `Box<dyn Any>`, compute callbacks as `Box<dyn Fn(&Context)>`,
effect callbacks as `Box<dyn Fn(&Context)>`, and cleanups as `Box<dyn FnOnce()>`.
Those storage choices avoid synchronization overhead for the common single-threaded
path and make `Context` neither `Send` nor `Sync`.

Current guarantees:

- Independent `Context` instances may be used on different OS threads
- A single `Context` must not be moved into, shared with, or accessed from another thread
- `Computed<T>` and `Source<T>` are lightweight ids and are `Send + Sync` when `T` is `Send + Sync`, but they are only meaningful with their owning context
- `EffectHandle` is a lightweight id; effect execution and cleanup remain tied to the owning context thread
- Dependency tracking is thread-local; a compute/effect callback cannot split work onto another thread and expect nested reads there to attach to the original tracking frame

### `ThreadSafeContext`

Thread-safe support is explicit rather than a silent change to `Context`.
`ThreadSafeContext` mirrors the existing `Context` methods while preserving the
single-threaded fast path.

API bounds:

| Method family | Additional bounds |
|---------------|-------------------|
| `cell`, `get`, `set` | `T: PartialEq + Clone + Send + Sync + 'static` |
| `slot`, `computed` | `T: Clone + Send + Sync + 'static`; compute closure `Fn(&ThreadSafeContext) -> T + Send + Sync + 'static` |
| `memo` | `T: PartialEq + Clone + Send + Sync + 'static`; compute closure `Send + Sync + 'static` |
| `effect` | effect callback `Fn(&ThreadSafeContext) -> R + Send + Sync + 'static`; cleanup `FnOnce() + Send + 'static` |
| handles | remain id-only and copyable; usable from any thread only with the owning `ThreadSafeContext` |

Locking model:

- Uses one context-level `Mutex` synchronization primitive for graph state before introducing finer-grained graph locks
- `ThreadSafeState` stores nodes in a slot-id-indexed `Vec<Option<ThreadSafeNode>>` matching the single-threaded `Context`, eliminating hash-map lookup overhead on every node access. Slot IDs are reused via a free list on effect disposal.
- Fresh cached slot reads use a per-slot read-mostly cached-value sidecar; dependency-edge changes, invalidation frontier application, batch queues, effect queues, and disposal remain graph mutex mutations
- Read-mostly cached slot access is versioned optimistically: the getter loads a per-slot atomic cache revision before cloning the retained cached `Arc`, then validates the revision and dirty/force flags again after the clone. Any concurrent invalidation, clear, or value publish changes the revision and forces the getter onto the graph-validated refresh path.
- Each thread-safe slot also owns a per-slot recompute/value-publish sidecar for the cached-value visibility flags, in-flight bit, waiter `Condvar`, and revision used to reject stale callback results. Graph-state dirty/revision fields remain mirrored under the context mutex for dependency-frontier traversal and tests.
- Each thread-safe slot sidecar mirrors a per-slot dependency summary: the
  current dependency ids plus a slot-dependency count. A cell-only dirty refresh
  may claim the SlotId-partitioned recompute sidecar, snapshot old dependencies,
  and skip the graph-locked `get_refresh` dependency scan. The owner still takes
  the final `publish` graph mutation to diff dynamic dependencies, publish the
  value, notify dependents, and reject stale in-flight revisions.
- Cells and slots mirror their dependent frontiers into per-node sidecars keyed
  by `SlotId`. Changed-cell invalidation may use these sidecars without taking
  the context graph mutex only when no callback is actively discovering
  dependencies, the context is not inside a batch, and the discovered frontier
  contains slots only. The per-slot cache revision acts as the dirty epoch for
  sidecar publication, and instrumentation records each epoch advance so
  parallel writers can publish version state without the graph mutex while
  cached reads still reject mid-read invalidation races.
  Effect scheduling, batching, dynamic dependency discovery, disposal, and any
  frontier that reaches an effect fall back to the graph mutex.
- Do not hold the graph lock while running user compute callbacks, effect callbacks, or cleanup closures
- Re-acquire the lock only to publish computed values, dependency edges, invalidation state, and pending effect work
- Slot refresh must avoid helper-level lock churn: a fresh cached get should clone the value through the per-slot fast path without taking a `get_refresh` graph lock or recursively validating unchanged dependencies, dependency refresh should not take a separate node-kind probe lock before recursively validating a dependency, cell dependencies should not be probed as refreshable slots, clean dirty flags should be folded into the refresh decision lock, and recompute must diff old/new dependency sets at publish so unchanged edges stay subscribed while only stale edges are removed
- Recompute dependency tracking must skip graph-lock edge registration for dependencies already present in the slot's previous dependency set, while still eagerly registering newly discovered dependencies during the callback so concurrent invalidation can mark the in-flight result stale
- Effect rerun dependency tracking must also preserve unchanged edges: dependencies already present on the effect remain subscribed through the rerun, newly discovered dependencies are registered during tracking, and stale dependencies are removed in one post-callback graph mutation before the next cleanup is stored
- Re-entrant user code must be able to call back into the same context without deadlocking
- Concurrent first access and dirty same-slot contention share one in-flight computation for the current slot revision; waiters check the per-slot recompute sidecar before the `get_refresh`/`publish` graph-lock path, park on that slot's notification primitive, then return the published cache or retry if an invalidation makes the in-flight result stale
- Recompute waiters observe the per-slot in-flight/revision state while holding the sidecar mutex, then park on the same sidecar `Condvar`. Finishers publish value and dirty-state sidecar updates before clearing the in-flight bit and notifying, so a stale in-flight completion cannot be missed.
- Recompute notifications are scoped to the slot that finished. A completion for one in-flight slot must not wake waiters parked behind another in-flight slot.
- Per-slot recompute wakeups use a waiter-counted handoff instead of `notify_all`: the finisher calls `notify_one` when waiters exist, and each awakened waiter notifies the next parked waiter after observing the completed sidecar state. This drains all waiters without a completion-wide wakeup stampede.
- Optimistic cached reads fall back whenever the context is dirty, forced to recompute, or racing with a sidecar revision change. They do not publish dependency edges, flush effects, observe batch-local unflushed invalidations as fresh, or replace graph-locked refresh for ambiguous callback/dependency states.
- If an upstream invalidation happens while a slot callback is running, the in-flight stale result is not published as fresh; the getter retries until it can return a value that matches the latest dependency state
- Batch exit, effect scheduling, disposal, and explicit clears must each have a single atomic graph mutation boundary and one coalesced effect flush per outermost invalidation pass
- The outermost thread-safe batch exit must collect dependents for all changed cells and apply one coalesced frontier invalidation, so a shared dependent reached through many changed cells is marked dirty and advances revision once per batch flush
- Thread-safe invalidation uses an explicit `InvalidationPlan` computed from a
frontier work queue under the graph mutex instead of recursive dependent
walks. Changed-cell and slot-value-change roots snapshot dependent frontiers,
coalesce duplicate slot ids in one invalidation pass, preserve direct
changed-value `force_recompute` upgrades when a slot is reached through both
  direct and downstream paths, snapshot hard-clear frontiers for explicit
  slot/cell clears, then apply dirty, clear, revision, and effect-scheduling
  mutations at the same graph mutation boundary. The plan shape is partitionable
for future bounded worker traversal, but this prototype keeps snapshot and application under the context mutex until benchmark and model-checking
evidence proves a parallel apply path safe.
- Thread-safe stress coverage must run the same contention script under both
`LowConcurrency` and `HighConcurrency`, mixing batched cell writes, explicit
slot and cell-dependent clears, effect cleanup/rerun, effect disposal racing
with writers, and concurrent cached reads. The harness lives in
`tests/thread_safe_stress.rs` and is part of `make check`.

Lock strategy evaluation:

- Keep one context-level graph synchronization primitive until benchmark instrumentation shows a finer-grained design improves the relevant workload without trading off other contention cases
- `ThreadSafeContext` uses read-mostly per-slot cached-value sidecars for fresh
  cached reads, a per-slot recompute/value-publish sidecar for in-flight
  same-slot waiters, a per-slot dependency summary for cell-only dirty refresh
  routing, and per-node dependent frontier sidecars for slot-only changed-cell
  invalidation; same-thread batches use thread-local batch frames to coalesce
  changed-cell queueing before the graph-owned batch flush; dependency graph
  mutations still require the context mutex
- The read-mostly cached-value sidecar is an optimistic validation path, not a
  lock-free graph replacement. Its atomic cache revision rejects mid-read
  invalidation and mid-read publish races, then falls back to the existing
  graph refresh path.
- `ThreadSafeContext` uses per-slot sidecar recompute `Condvar`s for in-flight waiters. Those Condvars guard only per-slot in-flight/revision/cache-visibility state, use waiter-counted `notify_one` handoff wakeups to avoid broad `notify_all` contention, and must not mutate dependency graph state independently of the context mutex
- The read-mostly prototype is benchmark-gated by the 1/2/4/8/16-worker `same_slot_write_read`, `independent_slots`, `read_mostly_waiters`, and `batched_write_bursts` matrix after the `#lazybatch1` and `#lazybatch2` invalidation/read-churn fixes
- The dependent-frontier sidecar prototype is benchmark-gated by
`thread_safe_contention / independent_slots` and
`set_cell_invalidation / independent_slot_contention` at 8 and 16 workers. It
should reduce `set_cell_invalidation` graph-lock acquisitions for independent
slot-only roots without changing effect, batch, or dynamic-dependency
semantics.
- A sidecar frontier invalidation that reaches a slot with recompute in flight
falls back to the graph-locked invalidation path, so stale publishes cannot
clear newer dirty markers.
- High-parallel graph propagation profiles gate the lazy-invalidation path with
`thread_safe_graph_propagation` at 8 and 16 workers. The matrix compares
fan-out eager validation, fan-out lazy dirty epoch publication, fan-in lazy
  dirty epoch publication, and fan-in batched flush behavior using throughput,
  p50/p95 latency, lock attribution, effect queue pushes, dependency-edge
  counters, sidecar dirty marks, sidecar fallbacks, and dirty epoch advances.
- The local batch-frame prototype is benchmark-gated by `set_cell_invalidation /
  batched_write_bursts` and `thread_safe_contention / batched_write_bursts` at 8
  and 16 workers. It should reduce per-write graph queueing during same-thread
  batches while preserving one coalesced dirty/effect flush at the outermost
  batch exit.
- Effect-heavy contention profiles gate any queue or batch synchronization
  change. `thread_safe_effect_contention` isolates effect queue coalescing,
  cleanup execution, and nested batch flush behavior at 8 and 16 workers with
  deterministic lock-site budgets before a sharded graph-lock design can be
  considered.
- Synchronization strategy comparison is release-gated by one fixed evidence
  table. The current `std::sync` mutex/Condvar path is the baseline; narrower
  Condvar wakeups are adopted only for per-slot recompute waiters; parking_lot
  style parking and targeted CAS remain candidates. A candidate must report
  throughput plus p50/p95 latency for the required 8/16-worker contention and
  effect-heavy cases, stay within lock-site budgets, and carry Loom/Shuttle
  proof for stale completion, effect scheduling/disposal, batch flush, and
  re-entrant callbacks before release.
- Benchmark watch items from generated README deltas must be confirmed with a
  controlled A/B rerun before tuning. The rerun should use the same benchmark
  filter on the same host/toolchain when possible, isolate baseline and current
  code in clean worktrees or Criterion baselines, and record whether the signal
  reproduces. If confidence intervals overlap or Criterion reports no
  statistically significant change, document the watch item and avoid
  speculative synchronization changes.
- Edge storage A/B benchmarking procedure: the `vec_edges` feature flag
  switches `EdgeVec` from `SmallVec<[SlotId; 4]>` (default) to `Vec<SlotId>`.
  To compare:
  1. `cargo bench --bench context -- dependency_fan_out,set_cell_invalidation/high_fan_out --save-baseline smallvec`
  2. `cargo bench --bench context --features vec_edges -- dependency_fan_out,set_cell_invalidation/high_fan_out --baseline smallvec`
  3. Compare Criterion output — if confidence intervals overlap, keep SmallVec;
     if Vec is faster at the tested fan-out widths, reconsider the default.
  The comparison must run on the same host/toolchain with no other workload.
- **Cached-read strategy is runtime-selectable** (#vd5v / #rdstrat1). Both read paths are compiled in and chosen at context construction via `ThreadSafeContext::with_read_strategy(ReadStrategy)`, defaulting to `LowConcurrency`; the slot sidecar `ThreadSafeSlotFastPath.value` is a `CachedReadStorage` enum:
  - `LowConcurrency` → `parking_lot::RwLock<Option<Arc<dyn Any + Send + Sync>>>` read — optimal uncontended / low core counts (the default).
  - `HighConcurrency` → `arc_swap::ArcSwapOption<Arc<dyn Any + Send + Sync>>` wait-free load — no read lock; optimal at 8+ cores. (`arc-swap`'s `RefCnt` is `Sized`-only, so it stores `Arc<Arc<dyn Any>>`; the extra outer `Arc` is allocated only on the cold publish path, never on the read.)

  Both reconstruct `&T` via the inline `type_id` without vtable indirection, and both carry the **same** atomic `cache_revision` + `dirty`/`force_recompute` validation envelope (loaded before, re-checked after the clone), so a `get` starting after a completed cross-thread invalidation cannot return the pre-invalidation value regardless of strategy. The runtime selection costs one per-read enum branch (the price of compiling both paths). **0.9.0 shipped arc-swap as the default; 0.10.0 (#rdstrat1) flips the default to `LowConcurrency`** with explicit opt-in to `HighConcurrency`. Verified by the full default suite (both strategies) and the `thread_safe_loom` model (the validation algorithm is identical across variants). The inline small-`Copy` seqlock fast path that subsumes this tradeoff for small values is #rdstrat2 (**implemented**; opt-in via `slot_copy`/`computed_copy`/`memo_copy` — see *Inline small-`Copy` seqlock* below).

  **Contention tradeoff (rigorous isolated-worktree A/B, #xtwf).** This is a deliberate low-contention-for-high-contention trade, not a free win. Comparing `463ca71` (arc-swap) against its parent `06fd3c2` (the `parking_lot::RwLock` read) on a shared Criterion target dir, same host/toolchain:

  | benchmark | arc-swap vs RwLock |
  | --- | --- |
  | `cached_reads/thread_safe_context` (1 thread) | **+3.1%** (slower) |
  | `read_mostly_waiters/4` | **+11.9%** (slower) |
  | `read_mostly_waiters/8` | **−6.5%** (faster) |
  | `read_mostly_waiters/16` | **−28.6%** (faster) |

  arc-swap's wait-free read wins at high core counts where `RwLock` read-lock cache-line traffic dominates, but its debt-tracking load plus the `Arc<Arc<…>>` double-indirection costs a few percent when uncontended. lazily-rs `ThreadSafeContext` targets highly-parallel reactive graphs, so the 8/16-worker win is the operative case and the ≤~3% uncontended/low-contention regression is accepted (see the revised benchmark gate below). The contended numbers carry wide confidence intervals; treat the crossover (~4→8 workers) as approximate.
- Any future sharding or CAS path must include a Loom or Shuttle safety model covering concurrent first get, stale in-flight completion, invalidation during compute, effect scheduling/disposal, and re-entrant callbacks before it can replace the single-graph-lock design
- The current sidecar `Mutex`/`Condvar` waiter path, optimistic cached-read fallback, and explicit invalidation-plan safety envelope are covered by `cargo test --features loom --test thread_safe_loom`, which models concurrent first get, scoped slot notification, waiter-counted handoff wakeup draining, stale in-flight completion and retry, read-mostly waiter handoff, mid-read optimistic validation fallback, invalidation during compute, fast-frontier fallback while dependency discovery is active, dynamic dependency switch/disposal cleanup, effect scheduling/disposal races, re-entrant callback graph access, duplicate diamond paths marking each frontier slot once, effect enqueue coalescing, nested batch invalidation flushing only at the outermost boundary, and the inline small-`Copy` seqlock (#rdstrat2) refusing torn reads and stale post-invalidation reads under concurrent single-writer publish
- A lock-strategy change must preserve the rule that user compute/effect/cleanup callbacks never run while holding graph-state locks

Sharded/versioned storage evaluation:

- After the frontier invalidation and read-churn prototypes, the
  `thread_safe_contention` benchmark and instrumentation rows are the gate for
  storage changes. The read-mostly cached-value sidecar is limited to fresh
  cached slot reads; write-side graph mutation remains serialized so the same
  benchmark matrix can show whether read-side wins trade off same-slot writes,
  independent slots, or batched aggregate writes.
- Do not replace the single graph lock with sharded storage in the current
  implementation. Sharding may help independent roots and slots, but it does
  not remove serialization for same-root writes, shared aggregate slots, effect
  queues, batch-depth accounting, disposal, or dynamic dependency edge changes.
  A shard design must first define shard ownership for dependency edges that
  cross shards, a deterministic merge/apply order for dirty/revision/effect
  mutations, and a Loom or Shuttle model for deadlock-free multi-shard
  invalidation and disposal.
- Do not treat versioned optimistic reads as an invalidation optimization.
  Versioned reads target fresh cached `get` latency, while the isolated
  `set_cell_invalidation` profiles attribute invalidation pressure to
  write-side graph mutation. The current prototype keeps independently retained
  sidecar `Arc` snapshots plus atomic dirty/revision validation so a `get`
  starting after a cross-thread invalidation cannot return the pre-invalidation
  cached value.
- The next storage experiment, if pursued, should be a benchmark-gated
  prototype rather than a replacement: shard independent `ThreadSafeState`
  mutation by stable node id, keep effect queue and batch flush as one
  deterministic merge boundary, and require the isolated `thread_safe_effect_contention`
  profiles, `set_cell_invalidation` matrix, and Loom/Shuttle coverage to improve
  before adopting it.

### Typed cache fast-path

`Context` and `ThreadSafeContext` store cached values as type-erased trait
objects (`Rc<dyn Any>` and `Arc<dyn Any + Send + Sync>`).  Every cached read
calls `downcast_ref::<T>()`, which invokes a virtual `type_id()` through the
trait object's vtable, compares the returned `TypeId`, and then performs a
pointer cast.  The vtable call adds indirect branching overhead to the hot
cached-read path.

The typed cache fast-path stores a `TypeId` inline in each node at creation
time.  Cached reads compare the stored `TypeId` directly (a single inline
`u64` equality check) and, on match, use an unchecked pointer cast to recover
the typed reference.  This eliminates the vtable indirection on every cached
slot and cell read for both `Context` and `ThreadSafeContext`.

Storage layout:

- `SlotNode.type_id: TypeId` — set once at `slot()` / `computed()` / `memo()`
  creation from `TypeId::of::<T>()`
- `CellNode.type_id: TypeId` — set once at `cell()` creation
- `ThreadSafeSlotFastPath.type_id: TypeId` — set once at slot creation
- `ThreadSafeCellFastPath.type_id: TypeId` — set once at cell creation

Read-path fast-path:

1. Load the node's `type_id` field (inline `u64` load, no vtable)
2. Compare with `TypeId::of::<T>()`
3. On match, cast the stored value pointer to `&T` without going through
   `dyn Any::downcast_ref`
4. On mismatch, panic with the same "type mismatch" message as before

`get_rc()` follows the same pattern for both computed and source handles but clones the
reference-counted pointer instead of cloning the inner value.
`ThreadSafeContext::get_arc()` is their `Send + Sync` analog. It reads the
authoritative `Arc` on the node rather than the cached-read sidecar (which
stores `T` by value and so has no box to share), which means it is a wash for
small `Copy` values and a win in proportion to the cost of cloning `T`.

The compute-function signature (`dyn Fn(&Context) -> Rc<dyn Any>` for
`Context`, `dyn Fn(&ThreadSafeContext) -> Box<ThreadSafeAny>` for
`ThreadSafeContext`) remains unchanged; the typed cache is a storage-side
optimization that does not affect the closure API.

Benchmark gate:

- `cached_reads/context` must show a measurable improvement over the
  `downcast_ref` baseline.  The current baseline is approximately 8 ns per
  cached read; the typed cache target is to reduce this by the vtable call
  overhead (typically 0.5–1.5 ns on x86-64).
- `cached_reads/thread_safe_context` (single-thread, uncontended) is a
  **bounded-regression** gate, not a no-regression gate, as of the #vd5v
  lock-free read. The `arc-swap` read sidecar may regress the uncontended
  cached read by up to ~3% in exchange for the high-contention win
  (`read_mostly_waiters` −6%/−29% at 8/16 workers; see *Lock strategy
  evaluation* → contention tradeoff). A regression beyond ~3% uncontended, or
  any regression in the 8/16-worker read-contention matrix, fails the gate and
  must be investigated. The earlier inline-`TypeId` vtable-elimination win for
  `cached_reads/context` (single-threaded `Context`) is unaffected and must
  still hold.
- The improvement must reproduce under controlled Criterion A/B comparison
  (same host, same toolchain, non-overlapping confidence intervals).

Future implications:

- Fully lock-free cached reads require typed storage so that a reader can
  reconstruct a typed reference from an atomically published pointer without
  vtable indirection.  The inline `TypeId` is a prerequisite: it proves the
  type at compile time and makes the unchecked cast sound. **Implemented**
  (#vd5v): `ThreadSafeSlotFastPath.value` is now an `arc_swap::ArcSwapOption`,
  so `read_fresh` loads the published snapshot wait-free and recovers `&T` via
  the inline `type_id` — see *Lock strategy evaluation* above.
- A future `ErasedValue` storage type could replace `Rc<dyn Any>` /
  `Arc<dyn Any>` entirely, storing the value inline for small types and
  avoiding heap allocation on compute.  The current inline-`TypeId` step
  preserves the `Rc<dyn Any>` / `Arc<dyn Any>` layout while unlocking the
  fast-path read optimization. **Partially implemented for `ThreadSafeContext`
  cached reads** (#rdstrat2): the slot cached-read sidecar
  (`CachedReadStorage::Inline`) stores small `Copy` values inline behind a
  wait-free seqlock — see *Inline small-`Copy` seqlock* below. The node still
  retains its `Arc<dyn Any>` value (the inline buffer is a read-acceleration
  duplicate); a full `ErasedValue` that removes the `Arc` for non-`Copy` types
  remains future work.

### Inline small-`Copy` seqlock (#rdstrat2)

For small `Copy` values, the `ThreadSafeContext` slot cached-read sidecar
(`ThreadSafeSlotFastPath.value`) selects a third `CachedReadStorage` variant,
`Inline`, instead of the strategy-selected `Locked`/`LockFree` path. The value's
bytes are stored inline in `[AtomicU8; INLINE_CAP]` (`INLINE_CAP = 24`,
alignment bound 16) behind a **single-writer / multi-reader seqlock**, with no
heap `Arc`, no refcount traffic on either read or publish. The inline path is
optimal under **both** `ReadStrategy` modes; the runtime mode only governs the
large / non-`Copy` fallback.

- **Soundness conditions.** Inline is chosen only when `T: Copy` *and*
  `size_of::<T>() <= INLINE_CAP` *and* `align_of::<T>() <= 16`. `Copy` removes
  any `Drop` / ownership hazard from a discarded torn read; the size/alignment
  bound keeps the byte copy in-bounds. The bytes are read/written with
  **relaxed atomic** per-byte operations (not a plain `memcpy`), so a reader
  racing the single writer is well-defined under the Rust memory model — unlike
  a classic non-atomic seqlock, which has a formally-UB benign data race. The
  inline `type_id` proves `T` so the validated byte snapshot can be
  reconstructed into `T` without a vtable.
- **Single-writer invariant.** Every `write` (value publish in
  `recompute_slot_now`, clear in `apply_locked`) runs while holding the graph
  state write lock, so writes are serialized; only reads are lock-free. The
  `seq` counter is even when stable, odd while a write is in progress; a reader
  observing an odd or changed `seq` discards its snapshot and retries. The
  closing `Release` store of the even `seq` and the reader's bracketing Acquire
  loads (with the canonical Acquire fence) make an accepted snapshot a
  consistent image of exactly one publish.
- **Same validation envelope.** The inline path carries the identical atomic
  `cache_revision` + `dirty` / `force_recompute` envelope as the other two
  strategies, so a read racing a publish/invalidation is rejected identically.
- **Opt-in constructors.** Inline selection is **not** automatic on the generic
  `slot` / `computed` / `memo` constructors: stable Rust cannot branch on
  `T: Copy` inside a generic fn that lacks the bound (method resolution is
  pre-monomorphization, so a `Copy`-gated impl is never *applicable* where the
  bound is unprovable; automatic detection would require nightly
  `specialization`). The inline path is therefore opt-in through the
  `Copy`-bounded `slot_copy` / `computed_copy` / `memo_copy` constructors,
  which fall back transparently to the strategy path when the value exceeds the
  inline size/alignment bound.
- **Loom gate.** The seqlock orderings (single-writer publish vs. concurrent
  lock-free readers, plus the `cache_revision`/`dirty` envelope) are modeled by
  `cargo test --features loom --test thread_safe_loom`
  (`inline_seqlock_reader_never_observes_torn_value`,
  `inline_seqlock_envelope_rejects_torn_and_stale_under_concurrent_publish`,
  `inline_seqlock_read_after_completed_invalidation_is_rejected`). The two
  single-property models run unbounded (exhaustive); the combined envelope model
  uses a preemption-bounded `Builder` (bound 4, 60 s duration cap) because the
  full 6-atomic envelope across two threads plus the driving body makes the
  unbounded permutation space non-terminating — the bound is validated by
  confirming the model still flags an injected torn-read regression, satisfying
  the lock-strategy Loom gate before landing.

Tokio integration is scoped in two stages:

1. Synchronous thread-safe sharing first: `ThreadSafeContext` should work inside
   `tokio::spawn` and `tokio::task::spawn_blocking` when all captured values and
   callbacks satisfy the `Send + Sync` bounds above. This is exposed behind the
   optional `tokio` feature with async tests and the `tokio_sync` example; it
   must not introduce async compute/effect semantics.
2. True async computations/effects are separate future work. They need explicit
   semantics for in-flight future deduplication, cancellation, dependency tracking
   across `.await`, stale future completion, cleanup ordering, and `Send` versus
   `LocalSet` futures.

### AsyncContext

True async support is a new explicit async context surface, not an overload of
`Context` or `ThreadSafeContext`. The `AsyncContext` API lives behind a separate
`async` feature flag so downstream users do not accidentally accept the larger
semantic surface. The `async` feature depends on `tokio` for runtime primitives
(`spawn`, `JoinHandle`, notification).

#### AsyncContext type definitions

```rust
pub struct AsyncContext {
    inner: Arc<AsyncContextInner>,
}

pub struct AsyncComputed<T> {
    id: SlotId,
    _marker: PhantomData<T>,
}

pub struct AsyncSource<T> {
    id: SlotId,
    _marker: PhantomData<T>,
}

pub struct AsyncEffectHandle {
    id: SlotId,
}

pub struct AsyncComputeContext<'a> {
    context_id: AsyncContextId,
    node_id: SlotId,
    inner: &'a AsyncContextInner,
}
```

#### AsyncContext API surface

| Method | Signature | Purpose |
|--------|-----------|---------|
| `new` | `fn new() -> Self` | Create a new async context |
| `source` | `fn source<T>(&self, value: T) -> AsyncSource<T>` | Create a mutable source (`T: PartialEq + Clone + Send + Sync + 'static`) |
| `get` (source) | `fn get<T>(&self, handle: &AsyncSource<T>) -> T` | Get source value synchronously through the unified `Read` API |
| `set` | `fn set<T>(&self, handle: &AsyncSource<T>, value: T)` | Update a source and invalidate dependents |
| `computed_async` | `fn computed_async<T, F, Fut>(&self, compute: F) -> AsyncComputed<T>` | Create an async computed slot |
| `get` (computed) | `fn get<T>(&self, handle: &AsyncComputed<T>) -> Option<T>` | Synchronous cached read; returns `Some(T)` if resolved, `None` otherwise. Avoids async overhead on warm paths |
| `get_async` | `async fn get_async<T>(&self, handle: &AsyncComputed<T>) -> T` | Await slot value; uses `get()` fast-path for resolved slots, otherwise spawns async compute |
| `memo_async` | `fn memo_async<T, F, Fut>(&self, compute: F) -> AsyncComputed<T>` | Like `computed_async` with `PartialEq` memo guard |
| `effect_async` | `fn effect_async<F, Fut, C, CleanupFut>(&self, effect: F) -> AsyncEffectHandle` | Create an async effect |
| `dispose_async_effect` | `fn dispose_async_effect(&self, handle: &AsyncEffectHandle)` | Dispose async effect and await cleanup |
| `batch` | `fn batch<F, R>(&self, run: F) -> R` | Synchronous batch boundary; schedules async reruns at batch exit |

API bounds:

| Method family | Additional bounds |
|---------------|-------------------|
| `get` | `T: Clone + Send + Sync + 'static` |
| `source`, source `get`/`set` | `T: PartialEq + Clone + Send + Sync + 'static` |
| `computed_async`, `memo_async` | `T: PartialEq + Clone + Send + Sync + 'static`; compute `Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static`; future `Future<Output = T> + Send + 'static` |
| `effect_async` | effect `Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static`; future `Future<Output = Option<C>> + Send + 'static`; cleanup `FnOnce() -> CleanupFut + Send + 'static`; cleanup future `Future<Output = ()> + Send + 'static` |
| handles | remain id-only and copyable; usable from any task only with the owning `AsyncContext` |

#### AsyncSlotNode state machine

Each async slot tracks its state through a finite state machine:

```rust
enum AsyncSlotState {
    Empty,
    Computing { revision: u64, handle: JoinHandle<()> },
    Resolved,
    Error,
}
```

States:

- **Empty:** no cached value, no in-flight computation. Entered on creation and
  after hard clear.
- **Computing:** a `JoinHandle` tracks the in-flight future for the current
  revision. Concurrent `get_async` callers attach waiters to the same
  in-flight result instead of spawning duplicate futures.
- **Resolved:** the cached value is fresh. The value remains until dependency
  invalidation transitions back to Computing.
- **Error:** the last computation failed. Callers receive the error or retry on
  the next `get_async`.

State transitions:

- **Empty → Computing:** first `get_async` call or dependency invalidation when
  no cached value exists.
- **Computing → Resolved:** future completes with `Ok`, and the recorded
  revision still matches the current slot revision. The value is cached.
- **Computing → Error:** future completes with `Err`, and the recorded revision
  still matches.
- **Computing → Computing (stale):** dependency invalidation advances the slot
  revision during an in-flight computation. The completing future finds its
  revision no longer matches and discards the result. A new future is spawned
  for the updated revision.
- **Resolved → Computing:** dependency invalidation marks the cached value stale
  and spawns a new computation.
- **Error → Computing:** `get_async` retry after an error.

Revision tracking ensures stale completions are discarded: an async computation
records the slot revision at start; at publish time the graph accepts the value
only if the revision is still current.

#### AsyncContext cancellation contract

1. **Waiter cancellation is safe:** dropping one `get_async` future does not
   cancel the shared in-flight computation while other waiters still need it.
   Each waiter holds a shared handle (e.g., oneshot receiver or `Shared<...>`);
   dropping the receiver does not abort the `JoinHandle`.

2. **Stale completion handling:** when dependency invalidation advances the slot
   revision during an in-flight computation, the completing future finds its
   recorded revision no longer matches and discards the result. Waiting callers
   are retried against the new revision or attached to the newly spawned future.

3. **Explicit cancellation:** `slot.clear()`, dependency invalidation, or
   context disposal may mark the in-flight revision as canceled. If the runtime
   provides an abort handle, the task is aborted. User futures must be
   cancellation-safe because aborting drops them at an `.await` boundary.

4. **Context disposal:** dropping the `AsyncContext` cancels all in-flight
   computations via their `JoinHandle::abort()` handles and awaits completion
   of all active cleanup futures before returning.

5. **Effect cleanup futures** must complete before the next effect body starts.
   Disposal removes pending reruns before awaiting cleanup.

#### `get_async` re-resolve contract (#k03k)

`get_async` must treat the slot state as authoritative and **re-resolve** rather
than assert, because the slot can change between its lock acquisitions and a
notifier can close under it. It runs an outer loop that, each pass, re-reads the
slot via the `get()` fast path and then re-locks to attach to / spawn a
computation. Two concurrency windows are load-bearing:

1. **Resolved-since-`get()`:** the slot can transition `Computing → Resolved`
   between the `get()` fast-path check (which releases the lock) and the
   re-lock. Observing `Resolved` at the re-lock is therefore expected and the
   cached value is read directly — it is **not** an unreachable state.
2. **Notifier dropped:** the per-computation `watch` senders can all drop
   without a final `Resolved` send when an in-flight compute is superseded by a
   newer revision (the stale `Computing → Computing` transition early-returns) or
   the slot is invalidated. A `recv.changed()` error means "the world changed",
   not a fatal error: the awaiter restarts the outer loop and re-resolves from
   current slot state (returning the now-published value, attaching to the new
   in-flight compute, or respawning).

Neither window is a data inconsistency — the published value is always correct;
the contract is that `get_async` never panics on these benign races. Covered by
`async_context_concurrent_set_and_get_async_never_panics_k03k`, which fails
deterministically against the prior assert-based implementation.

**Deterministic window coverage.** Unlike `ThreadSafeContext`, the async resolve
loop **cannot** be modeled with Loom: `AsyncContext` runs on tokio's async
executor and `tokio::sync::watch`, while Loom only shims synchronous `loom::sync`
primitives and has no async runtime. Each window is instead pinned by a targeted
deterministic test in `tests/async_resolve_loop.rs` (`async` + `instrumentation`
features):

- **Window 1** is forced via a one-shot `instrumentation`-gated seam
  (`AsyncContext::__install_window1_hook`) that resolves the slot inside the
  synchronous gap between the fast-path `get()` and the re-lock — the gap has no
  `.await`, so cooperative scheduling alone cannot reach it. The test asserts the
  reader returned through the `Resolved`-after-re-lock arm via
  `__window1_resolved_hits`. The seam compiles out of default/release builds.
- **Window 2** is forced by gating an in-flight compute and superseding it with a
  newer revision so the notifier drops without a final send, asserting the waiter
  re-resolves to the latest value rather than panicking (mirrors the broader
  `async_stress.rs::get_async_waiter_cancellation_and_stale_completion_keep_latest`).

Exhaustive interleaving exploration of the async path (beyond these two known
windows) would require a Shuttle model, which in turn requires making
`async_context.rs` generic over its concurrency primitives — a larger
architectural change tracked separately, not a Loom drop-in.

Async race stress coverage must exercise `get_async` waiter cancellation, stale
in-flight completion after dependency invalidation, dynamic dependency
replacement across awaited slot reads, and async effect cleanup-before-rerun
ordering. The harness lives in `tests/async_stress.rs` under the `async` feature
so `make test-async` and `make check` run it with the rest of AsyncContext
coverage.

#### AsyncContext dependency tracking

Async compute and effect callbacks do not use thread-local tracking stacks.
Instead, each callback receives an `AsyncComputeContext`:

```rust
impl<'a> AsyncComputeContext<'a> {
    pub async fn get_async<T>(&self, handle: &AsyncComputed<T>) -> T;
    pub fn get<T>(&self, handle: &AsyncSource<T>) -> T;
}
```

- `get_async` on the compute context records the accessed slot as a dependency
before awaiting its value.
- `get` on the compute context records the accessed cell as a dependency
synchronously.
- Async reads register the graph edge immediately, so source invalidation while
the future is suspended can cancel or supersede the in-flight computation before
it publishes stale data.
- Dependencies are collected into a `HashSet<SlotId>` attached to the async
node. On rerun, stale dependencies are removed and new dependencies are
registered.
- This design survives executor thread migration and suspension/resume across
  `.await` points because the dependency set is carried by the
  `AsyncComputeContext`, not a thread-local.

#### AsyncContext async effects

```rust
fn effect_async<F, Fut, C, CleanupFut>(&self, effect: F) -> AsyncEffectHandle
where
    F: Fn(AsyncComputeContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<C>> + Send + 'static,
    C: FnOnce() -> CleanupFut + Send + 'static,
    CleanupFut: Future<Output = ()> + Send + 'static;
```

- **Serialized reruns:** async effect reruns are serialized per effect. A rerun
  does not start until the previous cleanup future completes.
- **Cleanup ordering:** the cleanup future from the previous run completes before
  the next effect body starts. Disposal awaits the current cleanup before
  removing the effect node.
- **Auto-tracking:** the effect body receives an `AsyncComputeContext` and
  tracks dependencies through `get_async` and `get` calls.
- **Dependency invalidation** schedules an async rerun after the current
  invalidation pass. The rerun is spawned on the runtime executor, not inline.
- **Effect disposal:** removes pending scheduled reruns, awaits the current
  cleanup future, and unsubscribes dependency edges.

#### AsyncContext batch support

- `ctx.batch()` is a synchronous boundary. Cell updates queue invalidation
  roots.
- At batch exit, queued roots trigger invalidation propagation. Async slots
  and effects are scheduled for rerun but do not execute inside the batch
  callback.
- Async reruns execute after the batch returns, on the runtime executor.
- Batching semantics remain synchronous at the graph mutation boundary:
  invalidations schedule async reruns only after the outermost batch exits.

#### AsyncContext feature flag

```toml
[features]
async = ["dep:tokio"]
```

The `async` feature depends on Tokio for `spawn`, `JoinHandle`, and runtime
primitives. It is separate from the `tokio` feature (which covers synchronous
`ThreadSafeContext` sharing inside Tokio tasks). The `async` feature implies
`tokio`.

Integration tests live in `tests/async_integration.rs` and are gated behind
`#![cfg(feature = "async")]`. Run with `make test-async` (or
`cargo test --locked --features async`). The `make check` target includes
`test-async` alongside `test`, `test-tokio`, and `test-loom`.

#### AsyncContext implementation notes

- Async graph locks must never be held while polling user futures or cleanup
  futures. Acquire the lock only to read/write graph state, then release before
  polling.
- Nested async slot reads register dependencies on the awaiting parent before
  awaiting the child result.
- The sync `tokio` feature is not enough to enable this API; true async support
  uses the separate `async` feature flag.
- The `Send` async context requires `Send + Sync + 'static` values, callbacks,
  futures, and cleanup futures. A future `LocalAsyncContext` may support `!Send`
  futures on `tokio::task::LocalSet`, but handles must not be interchangeable
  with the `Send` async context.
- In-flight future deduplication: each async slot has one published cache and
  at most one in-flight computation for the current slot revision. Concurrent
  `get_async` callers await the same in-flight result instead of spawning
  duplicate futures.
- Synchronous cached-read fast-path: `get()` returns the cached value
  synchronously when the slot is `Resolved`, avoiding async overhead. `get_async()`
  calls `get()` first; only unresolved or dirty slots enter the async spawn path.

## Invalidation Semantics

- `ctx.set()` → if value changed (PartialEq) → mark all dependent slots dirty
- `slot.clear(&ctx)` → remove cached value → cascade clear to all dependents
- `cell.clear_dependents(&ctx)` → clear all dependent slots without changing cell value
- `ctx.batch()` → queue changed cells and explicit slot/cell clears, then flush queued roots when the outermost batch exits
- Slot invalidation → preserve cached value as dirty → validate/recompute on next `ctx.get()` access
- Thread-safe slot invalidation walks an explicit coalesced frontier, so diamond
  paths mark each reachable slot at most once per invalidation pass unless a
  later direct changed-value path upgrades that slot to forced recompute
- If a dirty `ctx.memo()` slot recomputes to an equal value, downstream dirty slots become fresh without recomputing
- Slot clearing → remove cached value → hard-clear dependents recursively
- Effects rerun after the invalidation pass if any tracked dependency invalidated
- Effects scheduled only by dirty slot dependencies skip rerun if those slots validate unchanged
- Effect cleanup runs before rerun and on disposal

## Design Goals

- **Lazy evaluation:** Values computed only when first accessed or when dirty caches are validated
- **Ergonomic derived values:** `ctx.computed()` is the preferred spelling for ordinary derived slots
- **Fine-grained reactivity:** Only affected dependents recompute
- **Memoized invalidation:** Equal intermediate `ctx.memo()` recomputation suppresses downstream recomputation/effect reruns
- **Effects:** Side effects are scheduled from the same dependency graph as slots
- **Batching:** Multiple writes can share one invalidation/effect flush boundary
- **Zero mandatory runtime dependencies:** The default library surface uses only the Rust standard library and `smallvec`; Tokio is optional and Criterion is dev-only for benchmarks
- **Single-threaded fast path:** `Context` uses a single `RefCell<ContextInner>` with no mutex overhead and no `unsafe` code
- **Contiguous local storage:** Both `Context` and `ThreadSafeContext` index nodes directly by `SlotId` in a `Vec<Option<Node>>` to avoid hash-map lookup and churn; slot IDs are reused via a free list to prevent unbounded growth from transient effects
- **Explicit thread-safe path:** `ThreadSafeContext` uses a context-level lock and `Send + Sync` bounds for shared reactive graphs
- **Performance tracking:** Criterion benchmarks cover both `Context` and `ThreadSafeContext` for cached reads, cold first access, dependency fan-out, memo equality suppression, effect flushing, and batch storms; `ThreadSafeContext` also tracks a 1/2/4/8/16-worker contention matrix that separates hot shared-slot writes, independent per-worker slots, read-mostly waiters, and batched write bursts.
- **Benchmark instrumentation:** The optional `instrumentation` feature exposes lightweight counters for recompute starts, duplicate speculative thread-safe computes, dependency edge churn, effect queue depth, reactive node allocations, aggregate `ThreadSafeContext` lock wait/hold timing, and per-operation thread-safe lock attribution.

## Performance Benchmarks

The benchmark suite is a development-only surface under `benches/context.rs`.
It must compile with `cargo bench --no-run` and should remain focused on public
API behavior rather than private graph internals.

Required benchmark scenarios:

- Cached reads after the slot has already been computed
- Cold first `get` including graph construction and initial dependency capture
- Dependency fan-out invalidation followed by dependent reads
- Memo equality suppression where an equal intermediate value prevents downstream recomputation
- Effect flushing after dependency mutation
- Batch storms that coalesce many writes into one invalidation/effect flush boundary
- `ThreadSafeContext` `set` invalidation isolation, split into:
  - high fan-out changed-cell invalidation without dependent reads
  - same-root/same-slot write contention without dependent reads
  - independent per-worker roots and slots without dependent reads
  - batched write bursts over per-worker cell groups without dependent reads
- `ThreadSafeContext` contention at 1, 2, 4, 8, and 16 workers, split into:
  - same-root/same-slot write plus read contention
  - independent per-worker roots and computed slots
  - read-mostly waiters with one writer and many readers
  - batched write bursts over per-worker cell groups
- `ThreadSafeContext` effect-heavy contention at 8 and 16 workers, split into:
  - effect queue coalescing across batched worker writes
  - effect cleanup execution during concurrent cell updates
  - nested batch flushes that schedule computed-effect dependencies
- `ThreadSafeContext` synchronization model checking with the optional `loom` feature

The optional `instrumentation` feature adds `instrumentation_snapshot()` and
`reset_instrumentation()` to both context types and exports
`InstrumentationSnapshot`. The snapshot records:

- Reactive node allocation events as a stable allocation proxy
- Slot recompute callback starts
- Duplicate speculative `ThreadSafeContext` recomputes that lose publication races; this should remain zero when in-flight deduplication is effective
- Dependency edges added and removed
- Effect queue pushes and maximum pending queue depth
- `ThreadSafeContext` lock/coordination acquisitions plus total wait and hold nanoseconds

`ThreadSafeContext::lock_profile_snapshot()` returns per-operation lock and
coordination counters for the thread-safe path. The buckets are intentionally
high-level: unattributed/other work, `get` refresh, dependency edge add/remove,
`set` invalidation, recompute publication, and in-flight recompute waiting.
For the per-slot sidecar recompute `Condvar`s, the in-flight wait bucket records
the parked wait and reacquire boundary. The bucket acquisition counts must sum
to the aggregate `lock_acquisitions` counter so profile consumers can attribute
contention without losing the stable summary fields.

The instrumentation profile bench lives in `benches/profile.rs` and is gated
behind `required-features = ["instrumentation"]`; compile it with
`cargo bench --features instrumentation --no-run`.

The benchmark report harness lives at
`scripts/update-benchmark-results.py`. It runs
`cargo bench --features instrumentation`, reads Criterion estimate files from
`target/criterion`, captures `examples/instrumentation_profile.rs` counter
snapshots in `target/lazily-instrumentation-profile.csv`, and rewrites the
generated README section between
`<!-- benchmark-results:start -->` and `<!-- benchmark-results:end -->`.
The generated section must include the current Cargo package version, the
refresh command, the Criterion baseline comparison workflow, one timing row for
each required benchmark scenario above, p50/p95 Criterion sample latency rows
for the required 8/16-worker same-slot, independent-slot, read-mostly,
batched-write, and effect-heavy cases, and instrumentation rows covering
recomputes, duplicate speculative recomputes, dependency edge churn, effect
queue depth, node allocations, lock wait/hold time, and per-operation
`ThreadSafeContext` lock attribution for every 1/2/4/8/16-worker contention
matrix profile and every `set` invalidation isolation profile. The generated
section must also publish regression budgets for the slow contention profiles and
their lock-site acquisition totals. `--check` verifies that the README section is
already current without rewriting it and fails when required p50/p95 latency
rows are missing or any instrumentation profile exceeds its lock-acquisition
budget; `--no-run` reuses existing Criterion estimate files for a report-only
refresh after a manual baseline comparison run while refreshing the
instrumentation CSV unless it is also running in check mode.

## Serialization (`lazily-serde` feature gate)

The `serde` feature is declared in `Cargo.toml` but currently has no
implementation. Its purpose is to produce a serializable snapshot of context
state so the planned `lazily-ipc` (snapshot + incremental update protocol) and
`lazily-distributed` (CRDT/Raft remote graphs) layers have a stable on-the-wire
representation to build on. This section fixes the design of that feature gate
before any code lands.

### The type-erasure problem

`Context` and `ThreadSafeContext` store cached values as type-erased trait
objects (`Rc<dyn Any>` and `Arc<dyn Any + Send + Sync>`; see *Typed cache
fast-path*). `serde::Serialize` is **not object-safe** — it has a generic
`serialize<S: Serializer>` method — so a `dyn Any` cannot be serialized
directly, and the concrete type `T` is gone by the time a snapshot walks the
node `Vec`. Any serialization design must recover the ability to call a
monomorphized `Serialize`/`Deserialize` for each node's erased `T`. Two
approaches were considered.

### Approach A — trait bounds (erased-serde style)

Replace `dyn Any` cache storage with a serialize-aware trait object — a sealed
`dyn ReactiveValue: Any` whose vtable also carries an `erased_serde`-style
`erased_serialize(&self, &mut dyn Serializer)`. Every signal-creation API
(`slot`, `computed`, `memo`, `cell`) gains a `T: Serialize + DeserializeOwned`
bound under `#[cfg(feature = "serde")]`.

- **Pros:** serialization is *total* — every cached node is serializable with
  no per-node opt-in; one storage type, one source of truth.
- **Cons:**
  - The `T: Serialize` bound is **viral**: it propagates through the whole
    public API and leaks onto `Context` itself even for signals that are never
    serialized, forcing callers to satisfy it for purely local reactive state.
  - Requires either the `erased-serde` crate or a hand-rolled vtable
    equivalent — acceptable under a feature gate but still added surface.
  - **Breaks the typed cache fast-path.** That optimization recovers `&T` via
    an unchecked pointer cast keyed on an inline `TypeId`; swapping the trait
    object out from under it for a serialize-aware type would have to preserve
    the same unchecked-cast guarantee.
  - `Deserialize` is the hard half: reconstruction needs the concrete type at
    the call site, so a type-tag → constructor registry is required anyway —
    Approach A does not avoid the registry, it only adds the viral bound on top
    of it.

### Approach B — type-erased closures captured at creation (recommended)

Keep `dyn Any` storage untouched. At signal creation under the `serde` feature,
capture a monomorphized serde **vtable** — a small `&'static` struct of
function pointers — alongside the `TypeId` the typed cache fast-path already
records:

```rust
#[cfg(feature = "serde")]
pub struct SerdeVTable {
    pub type_tag: &'static str,                                  // stable cross-process key
    pub serialize:   fn(&dyn Any, &mut dyn erased_serde::Serializer)
                       -> Result<(), erased_serde::Error>,
    pub deserialize: fn(&mut dyn erased_serde::Deserializer)
                       -> Result<Rc<dyn Any>, erased_serde::Error>,
}
```

Each `SlotNode`/`CellNode` (and the `ThreadSafe*FastPath` mirrors) gains a
`#[cfg(feature = "serde")] serde_vtable: Option<&'static SerdeVTable>` field,
set once at `slot()` / `computed()` / `memo()` / `cell()` creation. The thunks
are monomorphized per `T` at the construction call site, so the concrete type
is captured exactly where it is still known; invoking `serialize` downcasts the
`&dyn Any` back to `&T` and calls the real `T: Serialize`.

- **Pros:**
  - **Composes with the typed cache fast-path** — both capture per-`T`
    metadata at creation; the read hot path is untouched and pays zero
    overhead (thunks run only at snapshot time).
  - **Opt-in per node.** A signal whose `T: !Serialize` stores `None` and is
    emitted as an `Opaque` placeholder, matching how `lazily-ipc` will snapshot
    only an explicitly shared subgraph rather than the whole context. No viral
    bound on `Context`.
  - The `deserialize` thunk doubles as the type-tag → constructor registry
    Approach A needs anyway; populating it at slot construction keeps tags and
    constructors in one place.
- **Cons:**
  - Per-node storage grows by one pointer (`Option<&'static SerdeVTable>`, 8
    bytes) — eliminated entirely when the `serde` feature is off via
    `#[cfg(feature = "serde")]` on the field.
  - Without specialization on stable Rust, "serialize if `T: Serialize`, else
    `None`" needs a sealed `MaybeSerialize<T>` autoref/marker helper or
    explicit `*_serde` constructor variants rather than a blanket impl.

### Decision

**Adopt Approach B.** The `lazily-ipc`/`lazily-distributed` roadmap serializes
an explicit allowlisted `RemoteOp` set (#39c5), not arbitrary nodes, so the
*totality* Approach A buys is unneeded — and it costs a viral bound plus a
rework of the typed cache fast-path to buy it. Approach B keeps the default
build byte-for-byte identical (field compiled out), preserves the zero-overhead
read path, and reuses the `TypeId`-at-creation pattern already proven by the
typed cache.

### Feature-gate shape

- `serde = ["dep:serde"]` (declared) gains `dep:erased-serde` under the same
  gate; both stay optional, preserving the **zero mandatory runtime
  dependency** goal.
- A sealed `MaybeSerialize<T>` helper resolves the vtable to `Some` when
  `T: Serialize + DeserializeOwned` and `None` otherwise, so existing
  constructor signatures are unchanged and non-serializable signals keep
  compiling.
- `Context::snapshot()` / `ThreadSafeContext::snapshot()` walk live nodes,
  invoke each present vtable, and emit `ContextSnapshot { nodes: Vec<NodeSnapshot
  { slot_id, type_tag, payload | Opaque }> }`. Restoration reads `type_tag`,
  looks up the `deserialize` thunk, and rebuilds typed cache entries.
- This snapshot type is the input to #ipc2 (snapshot + incremental update
  protocol) and the value substrate for #ipc3 (CRDT vs Raft).

## IPC snapshot + incremental update protocol (`lazily-ipc`)

`lazily-ipc` transmits a reactive graph's state to a remote observer and keeps
it in sync as the graph mutates. It builds directly on the `ContextSnapshot`
from the `lazily-serde` design and reuses the existing batch-flush and
cache-revision machinery as its consistency boundary rather than inventing a
new one.

### Two message kinds

- **`Snapshot`** — full graph state. Sent on connect and on resync.
- **`Delta`** — incremental change set. Sent **once per outermost batch-flush
  invalidation pass** — the single atomic graph-mutation boundary the
  thread-safe path already guarantees (see *Threading and Concurrency
  Contract*: "Batch exit ... a single atomic graph mutation boundary and one
  coalesced effect flush per outermost invalidation pass"). Coalescing is
  therefore free: one delta per flush, never one per `set`.

### Epoch / versioning

A context-level monotonic `ipc_epoch: u64` advances **once per outermost batch
flush**, not per write. It is independent of the per-slot `cache_revision`
atomics (which remain the read-path dirty epoch) — `ipc_epoch` is the wire
sequence number.

- `Snapshot` carries `epoch`.
- Each `Delta` carries `{ base_epoch, epoch }` with `epoch == base_epoch + 1`.
  Deltas are strictly sequential, so a receiver detects any gap, reorder, or
  sender restart by checking `base_epoch == last_epoch`.

### Payloads

```
Snapshot { epoch: u64, nodes: Vec<NodeSnapshot>,           // NodeSnapshot from lazily-serde
           edges: Vec<(SlotId /*dependent*/, SlotId /*dependency*/)>,
           roots: Vec<SlotId> }                            // cells + source slots

Delta { base_epoch: u64, epoch: u64, ops: Vec<DeltaOp> }

DeltaOp =
  | CellSet    { slot_id, payload }           // changed-value cell write (PartialEq-guarded)
  | SlotValue  { slot_id, payload }           // a recompute published a new value
  | Invalidate { slot_id }                    // dirtied, not yet recomputed (lazy)
  | NodeAdd    { slot_id, type_tag, payload | Opaque }
  | NodeRemove { slot_id }                    // freed slot id (free-list reuse → Remove then Add)
  | EdgeAdd    { dependent, dependency }
  | EdgeRemove { dependent, dependency }
```

### Consistency invariants (inherited, not re-derived)

- **PartialEq cell guard:** an equal `set` invalidates nothing, so it emits
  no `CellSet` and no downstream ops — the wire is silent exactly when the graph
  is (SPEC *Invalidation Semantics*).
- **memo equality suppression:** a dirty `memo()` that recomputes to an equal
  value emits no `SlotValue` and no downstream `Invalidate`, mirroring the
  local "downstream dirty slots become fresh without recomputing" rule.
- **Coalesced frontier:** a dependent reached through many changed cells in one
  batch appears at most once per delta — the same once-per-pass guarantee the
  thread-safe frontier already enforces.

### Lazy reconciliation

Because lazily-rs is lazy, a flush can invalidate a slot without producing a new
value. Two receiver modes:

- **Value-mirror (default for IPC):** at flush the *sender* resolves each
  invalidated allowlisted slot via `ctx.get()` so the delta carries concrete
  `SlotValue`s. The receiver stays a pure data mirror holding no compute
  closures. Trades local laziness for a value-complete wire image.
- **Mirror-lazy:** the sender emits bare `Invalidate` and the receiver keeps a
  stale marker, recomputing only on its own read. This requires the compute
  closures to be replicated too and is therefore **deferred to
  `lazily-distributed`** (#ipc3), not `lazily-ipc`.

### Resync / gap handling

The receiver tracks `last_epoch`. On a `Delta` whose `base_epoch != last_epoch`
(gap, reorder, or sender restart) it discards the delta and requests a
`Snapshot`; the sender replies with a fresh `Snapshot { epoch }` and resumes
deltas from there. Messages are length-prefixed and tagged `Snapshot` / `Delta`
via `serde`/`erased-serde`; the protocol is transport-agnostic (unix socket,
pipe, WebSocket — the last feeds the #yxjw signaling server).

### Permission boundary (forward link to #39c5)

Only nodes on the per-peer allowlist (#39c5 `RemoteOp`) are serialized into a
snapshot or delta; non-allowlisted nodes are **omitted entirely** — not even as
`Opaque` — so a peer cannot infer their existence. The allowlist is applied at
snapshot/delta *construction*, before serialization, so the filter is the same
on the full and incremental paths.

#### Implemented (#39c5)

The permission policy layer ships behind the `distributed` feature in
`src/distributed.rs`:

- `NodeId` / `PeerId` — wire-stable identifiers (decoupled from the internal
  `SlotId`), `serde`-derived under the `serde` feature.
- `OpKind` (`Read` / `Write` / `TriggerEffect`) and `RemoteOp { kind, node }` —
  the gated, serializable unit a peer requests; the three kinds are gated
  **independently** (a read grant never implies write or effect-trigger).
- `PeerPermissions` — **default-deny** per-peer allowlist with `allow`,
  `allow_many`, `revoke` (prunes empty peer entries), `revoke_peer`,
  `is_allowed`, and a fail-closed `check` → `Result<(), PermissionDenied>`.
- `filter_readable(peer, nodes)` enforces the **omission** invariant above:
  non-readable nodes are dropped from the result entirely, preserving input
  order, so it can be applied at snapshot/delta construction before
  serialization.

`PeerPermissions` is local server-side state and is intentionally **not**
serializable; only the wire-facing `RemoteOp` family is. Higher layers
(`lazily-ipc` snapshot/delta construction, the `lazily-distributed` CRDT cell
plane, and the single-writer effect authority) gate every remote request
through `check` and build observable subgraphs through `filter_readable`.

### Feature gate

A new `ipc = ["serde"]` feature adds the pure-protocol `Snapshot`/`Delta` types
plus a transport-agnostic `IpcSink` / `IpcSource` trait pair. No transport
dependency enters the core crate. The `Delta`/`ipc_epoch` model is a
single-writer linear log; whether multi-writer needs CRDT merge or Raft
consensus on top of that log is exactly the #ipc3 question.

Implemented surface:

- `Snapshot { epoch, nodes, edges, roots }`, `NodeSnapshot`, `NodeState`, and
  `EdgeSnapshot` define the full graph image.
- `Delta { base_epoch, epoch, ops }` and `DeltaOp` define the one-flush
  incremental image. `Delta::next(base_epoch, ops)` enforces
  `epoch == base_epoch + 1`; `Delta::apply_status(last_epoch)` returns
  `Apply` or `ResyncRequired`.
- `Snapshot::filter_readable` and `Delta::filter_readable` apply
  `PeerPermissions` before serialization. Non-readable nodes and operations are
  omitted entirely; edges are retained only when both endpoints are readable.
- `IpcMessage`, `IpcSink`, and `IpcSource` keep Unix sockets, pipes,
  WebSockets, and shared-memory ring buffers outside the core crate.
- `ShmBlobArena`, `ShmBlobRef`, and `IpcValue::SharedBlob` provide the shared
  memory payload path. The arena writes a fixed header before each payload with
  generation, epoch, length, and checksum metadata; readers validate that
  header before accepting a descriptor. `IpcMessage` control frames can carry a
  `ShmBlobRef` instead of embedding large bytes inline.
- **Cross-process zero-copy transport** (`#lzzcpy`): the `BlobBackend` trait
  (`src/transport.rs`) is the pluggable-backend adapter seam. A producer calls
  `spill_message(&mut msg, &mut backend, threshold)` to replace large
  `Inline`/`Payload` sites with a `SharedBlob` descriptor; a receiver resolves
  via a `BlobRouter` that routes by the descriptor's `backend` discriminator.
  `InProcessBackend` wraps `ShmBlobArena` (in-process / FFI host); `ArrowBackend`
  holds Arrow IPC stream bytes; `ShmBackend` (POSIX `shm_open` + `mmap`, behind
  the `shm` feature) is the cross-process backend. The `ShmBlobRef` gained an
  optional `backend` field (`BlobBackendKind::Shm` | `Arrow` | `InProcess`,
  default `Shm`) so legacy descriptors validate unchanged. The formal laws
  (spill-then-resolve identity, backend isolation, ABA generation safety,
  checksum integrity) are proven for any backend in
  `lazily-formal/LazilyFormal/ZeroCopyTransport.lean`.

Formal companion: `lazily-spec/formal/lean` models the shared IPC
Snapshot/Delta state machine in Lean 4 and proves the epoch sequencing,
fail-closed resync, PartialEq/memo suppression, batch coalescing, and eager
Signal `slot_value` invariants. It is intentionally a spec-layer oracle; Rust
implementation behavior remains covered by the crate tests and conformance
fixtures.

Shared-memory IPC is therefore a supported transport direction, not a separate
reactive-graph mode: the shared memory segment carries large blob payloads, and
the ordinary control transport carries framed `IpcMessage`s with blob
descriptors. Each process keeps its own local `Context` / `ThreadSafeContext`
and reconciles via snapshots and deltas. A live `Context` is not shared across
process address spaces.

## Cross-language channel compatibility (FFI / IPC / WebSocket / WebRTC data)

Yes: lazily-rs has a viable FFI strategy, but the FFI layer should be an
adapter around the same transport-agnostic state plane used by IPC and
distributed peers. It should not expose the closure-based Rust `Context`,
`ThreadSafeContext`, `Computed<T>`, `Source<T>`, or `&T` cached values
directly across an ABI boundary.

### Compatibility model

The cross-language lazily family has one canonical message plane:

- `IpcMessage::Snapshot` and `IpcMessage::Delta` are the graph-state payloads.
- `NodeId`, `PeerId`, `RemoteOp`, `Snapshot`, `Delta`, and `DeltaOp` are the
  wire-facing contract; internal `SlotId` values and typed handles remain local
  implementation details.
- `IpcPayload` is opaque serialized value bytes. The producing language owns
  type-aware encoding through stable `type_tag`s; the channel only moves bytes.
- `ShmBlobRef` is a descriptor carried by a control frame. Shared memory stores
  large payload bytes, but reconciliation still happens through ordinary
  `IpcMessage`s.

Every supported channel carries that same message plane:

| Channel | Compatibility strategy |
|---------|------------------------|
| FFI | C ABI exposes opaque context/session handles plus owned byte buffers for `IpcMessage` encode/decode, snapshot export, delta apply, and memory release. No Rust references, trait objects, closures, or typed handles cross the boundary. |
| IPC | Unix sockets, pipes, local TCP, or process channels carry length-prefixed serialized `IpcMessage`s. Shared-memory IPC is an optimization for large `IpcValue::SharedBlob` payload bytes, not a separate graph-sharing mode. |
| WebSocket | One WebSocket frame carries one serialized `IpcMessage` or a negotiated fragment. The #yxjw signaling server may relay the frame as opaque payload and must not parse CRDT/IPC state. |
| WebRTC data | Reliable ordered data channels carry the same serialized `IpcMessage`s after #yxjw peer discovery. Unordered or unreliable channels are only acceptable for optional lossy telemetry; `Delta`s need ordered reliable delivery or receiver-side gap detection and snapshot resync. |

### FFI boundary shape

The Rust FFI surface is deliberately narrow:

```rust
#[repr(C)]
pub struct LazilyFfiBytes {
    pub ptr: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub enum LazilyFfiStatus {
    Ok = 0,
    Empty = 1,
    NullPointer = 2,
    InvalidMessage = 3,
    EncodeFailed = 4,
    Panic = 5,
}

#[repr(C)]
pub enum LazilyFfiMessageKind {
    Unknown = 0,
    Snapshot = 1,
    Delta = 2,
}
```

The `ffi` feature exports `extern "C"` functions for creating/freeing an opaque
`LazilyFfiChannel`, validating/classifying encoded `IpcMessage` frames,
enqueueing accepted frames, receiving Rust-owned `LazilyFfiBytes` frames, and
freeing buffers allocated by Rust. All allocation ownership is explicit: the
caller owns input bytes, Rust owns output buffers until the paired free function
is called. Errors return `LazilyFfiStatus`; panics must be caught before crossing the C ABI.

The implemented channel is a local ABI adapter. It decodes each accepted frame
as `IpcMessage`, stores the decoded message, then re-encodes with the requested
receive/clone codec. That keeps FFI byte transport aligned with IPC, WebSocket,
and WebRTC data transport while leaving snapshot export, delta application to a
live graph, and richer typed convenience APIs as higher-level work on top of the
same message plane.

The FFI layer may expose convenience helpers for local cells, but those helpers
still encode/decode through the same `type_tag` + payload registry used by
`lazily-serde`. A foreign runtime can therefore choose either direct local FFI
calls or framed IPC/WebSocket/WebRTC transport without changing the graph-state
protocol.

### Shared library build

The crate produces both an `rlib` (for Rust consumers) and a `cdylib` (for
FFI consumers) via `crate-type = ["lib", "cdylib"]` in `Cargo.toml`.

- `make build-ffi` builds the shared library with the `ffi` feature enabled.
- `make ffi-headers` generates a C header file (`target/lazily.h`) via
  `cbindgen` using `cbindgen.toml`.
- Future options include `safer-ffi` (safer FFI wrappers with auto-generated
  headers) and `diplomat` (multi-language binding generation). The current
  `cbindgen` approach is sufficient for C ABI consumers.

### Frame codecs (`ffi` / `webrtc` / `ipc-msgpack` / `ipc-binary` features)

`IpcMessage` frames have one semantic schema and several negotiated codecs:

- `IpcMessage::encode_json()` / `IpcMessage::decode_json()` — JSON
  encode/decode (gated behind `ffi` or `webrtc`, which pull in `serde_json`).
- `IpcMessage::encode_msgpack()` / `IpcMessage::decode_msgpack()` — named
  MessagePack encode/decode via `rmp-serde` (`ipc-msgpack`). This is the
  preferred cross-language binary codec because field names survive the frame.
- `IpcMessage::encode_binary()` / `IpcMessage::decode_binary()` — postcard
  encode/decode (`ipc-binary`). This is compact but not self-describing.
- `IpcCodec` — negotiated codec token (`json`, `msgpack`, `postcard`) used by
  transports such as WebRTC.
- FFI binary functions: `lazily_ffi_channel_send_binary`,
  `lazily_ffi_channel_recv_binary`, `lazily_ffi_ipc_message_validate_binary`,
  `lazily_ffi_ipc_message_kind_binary`, `lazily_ffi_ipc_message_clone_binary`
  — mirror the JSON FFI functions but use the postcard codec.
- FFI MessagePack functions: `lazily_ffi_channel_send_msgpack`,
  `lazily_ffi_channel_recv_msgpack`, `lazily_ffi_ipc_message_validate_msgpack`,
  `lazily_ffi_ipc_message_kind_msgpack`, `lazily_ffi_ipc_message_clone_msgpack`
  — mirror the JSON FFI functions but use named MessagePack.
- `EncodeError` / `DecodeError` — codec-agnostic error types with `Json`,
  `Msgpack`, and `Binary` variants gated by their respective features.

JSON remains the canonical/debug/default form. MessagePack is the cross-language
production binary form. Postcard remains for same-Rust or explicitly
postcard-aware peers.

### WebRTC data channel transport (`webrtc-data` feature)

WebRTC data channels carry `IpcMessage` frames peer-to-peer after the
`signaling-client` (#yxjw) completes SDP/ICE negotiation. This is the
internet-scale transport layer — no server relay needed for graph state
after the initial signaling handshake.

#### Feature gate

```toml
webrtc-data = ["ipc", "dep:str0m", "dep:tokio"]
```

Uses `str0m` for the WebRTC stack (pure Rust, no C dependencies) and
`tokio` for async runtime integration. Separate from `signaling-client`
so consumers can use signaling without incurring the full WebRTC
dependency.

#### Transport interface

```rust
#[cfg(feature = "webrtc-data")]
pub struct WebRtcDataChannel {
    // str0m session wrapping a single data channel
}

#[cfg(feature = "webrtc-data")]
impl IpcSink for WebRtcDataChannel {
    type Error = WebRtcDataError;
    fn send(&mut self, message: &IpcMessage) -> Result<(), Self::Error>;
}

#[cfg(feature = "webrtc-data")]
impl IpcSource for WebRtcDataChannel {
    type Error = WebRtcDataError;
    fn recv(&mut self) -> Result<Option<IpcMessage>, Self::Error>;
}
```

#### Channel contract

- **Ordered + reliable**: data channels must be created with
  `ordered: true, maxRetransmits: None` so `Delta` delivery matches the
  single-writer epoch contract. Unordered/unreliable channels are only
  acceptable for optional lossy telemetry, never for graph state.
- **Framing**: each `IpcMessage` is length-prefixed (4-byte LE length +
  payload). `json`, `msgpack`, or `postcard` codec negotiated during capability
  handshake.
- **Back-pressure**: `send` blocks or yields when the SCTP congestion
  window is full; the caller must not flood faster than the channel drains.
- **Reconnect**: on channel close or SCTP failure, the transport signals
  `Err` so the caller can re-signaling and re-establish a fresh channel.
  The `Delta` resync mechanism handles any gap.

#### Lifecycle

1. `SignalingClient` exchanges SDP offer/answer with peer via #yxjw
2. ICE candidates trickle through the signaling channel
3. On ICE completion, `WebRtcDataChannel::from_sdp(local_sdp, remote_sdp)`
   creates the str0m session and opens the data channel
4. Capability handshake on the data channel (protocol id, codec, features)
5. `IpcMessage` frames flow bidirectionally
6. On disconnect, re-signaling via `SignalingClient` and repeat

#### Integration test surface

- `tests/webrtc_data.rs` — gated behind `#[cfg(feature = "webrtc-data")]`
- Loopback test: create two str0m sessions back-to-back, send
  `IpcMessage::Snapshot` and `IpcMessage::Delta`, verify round-trip
- Codec negotiation: JSON, MessagePack, and postcard on the same channel
- Ordered delivery: send 100 deltas, verify epoch order on recv
- `make test-webrtc-data` target in Makefile

#### str0m backends: loopback vs networked (`webrtc-str0m` feature)

The `webrtc-str0m` feature ships two concrete `DataChannel` backends over the
same sans-IO str0m pump loop, differing only in transport and clock:

- **`Str0mLoopback`** (`src/str0m_backend.rs`) — two `Rtc` instances in one
  thread, connected by an **in-memory packet route** advanced on a **synthetic
  clock**. No sockets, no threads, no wall-clock dependency, so the full
  ICE/DTLS/SCTP handshake is **deterministically testable in-process**. This is
  the unit/CI substrate for the `WebRtcSink`/`WebRtcSource` bridge.
- **`Str0mNet`** (`src/str0m_net.rs`) — one `Rtc` driven over a **real UDP
  socket** by a **background driver thread**, with the SDP offer/answer and
  trickled ICE candidates exchanged by the caller (typically over
  `SignalingClient`, #yxjw). This is the real "beyond signaling" peer-to-peer
  path that can reach a peer on another host.

`Str0mNet` lifecycle:

1. `Str0mNet::offer(bind)` → binds the UDP socket, opens the `lazily-ipc` channel,
   returns the SDP offer string; `Str0mNet::answer(bind, offer)` returns the SDP
   answer string. The offerer applies the peer's answer with `accept_answer`.
2. Each peer exposes its host candidate via `local_candidate()`; the caller
   trickles it to the remote, which feeds it to `add_remote_candidate()`.
3. The driver thread pumps `poll_output` → UDP `send_to`, and UDP `recv_from` →
   `handle_input`, advancing real timers, until the SCTP data channel opens
   (`wait_open`). Inbound frames queue for `try_recv_frame`; outbound frames
   requested before open are buffered and flushed on open.
4. On `ChannelClose`, socket failure, or a dead `Rtc`, the channel reports closed
   so the sync sink/source surface `Err` and the caller re-signals.

Because `Str0mNet` needs live two-peer connectivity it cannot use the synthetic
clock. `tests/str0m_net.rs` exercises a real two-socket round trip over
`127.0.0.1` (real UDP/DTLS/SCTP/timers); a cross-host round trip through the live
signaling Worker is operator-gated.

##### `Str0mNet` outbound backpressure contract (#lzstr0mframe)

The `Str0mNet` driver's outbound frame queue is **bounded** at
`MAX_PENDING_FRAMES` (1024). When a caller's `send_frame` rate exceeds the SCTP
drain rate, the driver applies backpressure on two layers:

1. **`Channel::write` backpressure (`Ok(false)`)** — str0m returns `Ok(false)`
   when the SCTP send buffer is full. The driver's flush loop **re-queues the
   frame and yields** (rather than popping it), so the next `poll_output` /
   `recv_from` cycle can drain the SCTP window and accept the frame on the
   following iteration. Pre-#lzstr0mframe this branch was a bare
   `if ch.write(...).is_err() { break; }` that ignored the `bool`, silently
   dropping every frame that hit the `Ok(false)` path — violating the
   ordered/reliable DataChannel invariant `WebRtcSink`/`WebRtcSource` rely on.
2. **Queue cap (`Str0mNetError::Backpressure`)** — once the driver's
   `out_pending` VecDeque reaches `MAX_PENDING_FRAMES`, `send_frame` itself
   returns `Err(Str0mNetError::Backpressure)` so the caller applies flow control
   (sleep / await / shed load) instead of growing memory without bound. The
   counter is decremented when `Channel::write` accepts the frame (`Ok(true)`)
   and reset to zero on driver exit (the queue will never drain after close).

The regression test `burst_of_frames_arrives_in_order_under_backpressure`
exercises this contract by bursting 100 × 8 KiB frames through a single
`Str0mNetChannel` and asserting all 100 arrive in order at the remote peer,
retrying on `Backpressure` as needed.

##### `Str0mNet` driver I/O error handling (#lzstr0mpolldrive)

The driver's UDP I/O surfaces failures instead of silently dropping packets:

- **`socket.send_to`** — pre-fix this was `let _ = socket.send_to(...)`, which
  discarded every error: `ENOBUFS` (send-buffer pressure), `ECONNREFUSED` (ICMP
  port-unreachable, peer down), `ENETUNREACH`/`EHOSTUNREACH` (route flap),
  `EBADF` (socket closed). The corresponding ICE/DTLS/SCTP packet was silently
  lost and the handshake stalled without diagnostics. Post-fix,
  `WouldBlock`/`Interrupted` retryable errors `continue` the drain loop (str0m
  re-emits the `Transmit` on a later `poll_output`); any other error breaks the
  driver (`'outer`), surfacing `Closed` so the caller re-signals.
- **Read-timeout cap as command-poll interval** — `recv_from` waits at most
  `COMMAND_POLL_INTERVAL` (15 ms) so control commands (`Send` / `AcceptAnswer`
  / `AddRemoteCandidate` / `Shutdown`) read from `cmd_rx` at the top of each
  outer iteration stay bounded-latency. This is *not* a str0m timing parameter:
  str0m is fed an accurate time advance via `Input::Timeout(now)` whenever the
  socket times out without data, and an "early" `Input::Timeout` (every 15 ms
  during idle) is harmless — str0m just re-emits its pending deadline if it
  isn't time yet.

#### Signaling glue: `Str0mNet` over `SignalingClient` (#lzwebrtcwire)

`Str0mNet` exchanges its SDP offer/answer and trickled ICE candidates *out of
band*; `SignalingClient` (#yxjw, below) is the out-of-band channel. The
`webrtc_signaling` module (enabled when **both** `signaling-client` and
`webrtc-str0m` are on) is the wire between them — two `async` driver functions
that own the full handshake:

- `offer_to_peer(client, peer, bind, timeout)` — binds the socket via
  `Str0mNet::offer`, sends the SDP `offer` and the local `local_candidate()` to
  `peer` over the signaling client, then pumps incoming `ServerMessage`s
  (`answer` → `accept_answer`, `ice` → `add_remote_candidate`) until the data
  channel opens, returning the connected `Str0mNet`.
- `answer_next_offer(client, bind, timeout)` — waits for the next `offer` frame,
  produces the SDP answer via `Str0mNet::answer`, returns the answer + local
  candidate over signaling, applies any ICE candidate that raced ahead of the
  offer, then pumps until open. Returns the offering `PeerId` and the connected
  `Str0mNet`.

Both pump loops re-check `Str0mNet::is_open()` on a short poll tick as well as on
each signaling frame, because the channel opens on the backend's driver thread,
off the signaling path. The caller is responsible for learning the target peer is
present (from the `welcome` roster or a `peer-joined` frame) before offering; an
offer to an absent peer is dropped by the relay and surfaces only as a timeout.

`tests/webrtc_signaling.rs` drives this end to end over a **loopback signaling
relay**: an in-process `tokio-tungstenite` server implementing the #yxjw roster +
`from`-stamped routing on `127.0.0.1`, two real `SignalingClient` WebSocket
connections, and the real `Str0mNet` UDP/DTLS/SCTP transport — proving a
permission-filtered `Snapshot` crosses a data channel negotiated entirely through
`SignalingClient`. The only remaining slice is the live two-host / NAT run through
the deployed #yxjw Worker, which is operator-gated (#h6qb).

### Capability negotiation

Each non-local session starts with a small compatibility handshake before graph
state flows:

- protocol id: `lazily-ipc`
- protocol major version
- codec (`json` today; binary codecs can be transport crates as long as they
  encode the same `IpcMessage` schema)
- maximum frame size and fragmentation support
- ordered/reliable delivery guarantee
- `PeerId` and session/graph id
- supported features such as `shared-blob`, `crdt-cell-plane`, and
  `signaling-relay`

If peers disagree on protocol major version, codec, ordering guarantees, or
required feature flags, they fail closed before applying any `Snapshot` or
`Delta`.

### Cross-language family rules

- The shared semantics are lazy slots, mutable cells, dynamic dependency
  tracking, `PartialEq`/equality-guarded invalidation, memo equality
  suppression, batching, and permission-filtered snapshots/deltas.
- Compute closures are language-local. Cross-language sync shares the cell
  state plane by default; derived slots converge remotely only when peers use a
  shared compiled graph or an explicit compute-descriptor system.
- JavaScript/TypeScript peers must keep `PeerId` values at or below
  `Number.MAX_SAFE_INTEGER`, matching the #s0fc signaling protocol.
- Permission filtering happens before serialization on every channel. A
  WebSocket relay, WebRTC data channel, or FFI caller must not receive nodes or
  operations that `PeerPermissions` would omit.
- Channel code must preserve back-pressure and resync behavior: if frame
  delivery gaps, reorders, truncates, or exceeds negotiated size, the receiver
  requests a fresh `Snapshot` instead of applying a partial delta.

This keeps FFI viable without making it a special semantic path. FFI, IPC,
WebSocket, and WebRTC data differ only in framing, ownership, and reliability;
the lazily family stays compatible because all channels carry the same
permission-filtered `IpcMessage` state plane.

## Multi-writer coordination: CRDT vs Raft (`lazily-distributed`)

`lazily-ipc` (above) is a **single-writer** linear log: one authority mutates
the graph and streams `Delta`s stamped by a monotonic `ipc_epoch`.
`lazily-distributed` asks the harder question — when **multiple peers** may
write the same shared reactive graph, what coordination model orders those
writes: a CRDT (conflict-free replicated data types, eventual consistency) or
Raft (leader-ordered consensus, strong consistency)?

### The reactive structure collapses most of the question

The key observation is that **not all graph state is writable**. lazily-rs has
exactly two node kinds with respect to authorship:

- **Cells / source slots** — externally writable. These are the *only* state a
  peer can directly set.
- **Derived slots** (`computed` / `memo`) — pure deterministic functions of
  their dependencies. They are never written; they *recompute*. Their values,
  and the dynamic dependency topology discovered during recompute, are a
  **deterministic view** of the cell state — *provided every peer runs
  identical compute closures* (the closure-replication prerequisite already
  flagged as `lazily-ipc`'s mirror-lazy mode).

So coordination is only needed on the small **cell plane**. The entire derived
graph — typically the large majority of nodes, plus all edges and the effect
schedule — converges automatically once the cells converge and recompute runs.
This is the same property the local engine already relies on: derived state is a
function, not a source of truth.

### The two models on the cell plane

| Aspect | CRDT (cell-plane registers) | Raft (leader-ordered log) |
|--------|------------------------------|---------------------------|
| Consistency | Eventual; peers converge after delivery | Strong; one total order, every peer identical |
| Availability | Local-first — peers read/write while partitioned | Minority partition cannot write; needs quorum |
| Write latency | Local (no round-trip) | Quorum round-trip to leader per write |
| Offline peers | Native (merge on reconnect) | Not supported (writes need quorum) |
| P2P / WAN fit | Direct (no leader); fits #yxjw signaling | Awkward — leader election over WAN, quorum cost |
| Conflict model | Per-cell merge (LWW / MV register) | None — serialized, last in order wins by fiat |
| Extends #ipc2 | Per-peer `Delta`s + causal stamps, merged | One global Raft-replicated `Delta` log |
| Cost | Every writable cell must *be* a CRDT | Election, log replication, quorum machinery |

### Recommendation — CRDT cell plane (HLC-stamped registers), not Raft

Adopt a **CRDT layer on the cell plane only**, with derived slots recomputing
deterministically on each peer. Concretely:

- Each replicated cell is a **register CRDT keyed by a hybrid logical clock
  (HLC)** — wall-clock for human-meaningful ordering, logical counter for
  causal tiebreak. Two flavors, chosen per cell via a trait:
  - **LWW-register** (last-write-wins) — default; "current value" semantics that
    most reactive cells want. Silently drops the losing concurrent write.
  - **MV-register** (multi-value) — surfaces concurrent writes as a set for the
    compute layer (or app) to resolve, when dropping a write is unacceptable.
  - Additive cells can opt into a **PN-counter** instead of a register.
- The local **PartialEq invalidation guard** still applies — *after* merge: a
  merge that yields an equal value invalidates nothing, exactly as a local equal
  `set` does. **memo equality suppression** likewise holds post-merge, so
  convergent peers do the same downstream work.
- `lazily-ipc`'s `Delta` generalizes from one monotonic `ipc_epoch` to
  **per-peer causal stamps**: each peer keeps its own sequence; cross-peer order
  comes from the HLC/dot metadata carried on each `CellSet`. Delivery can be
  out-of-order; merge is commutative/associative/idempotent so gaps self-heal
  without the snapshot-resync that the single-writer log needed.

Raft is the wrong default because the `lazily-distributed` roadmap is
explicitly **availability-first** — P2P signaling (#yxjw) and offline peers —
and Raft trades exactly that away for a global total order that the reactive
model does not need: derived state is already deterministic, and the writable
surface is small.

### The narrow exception — irreversible effects need an authority

CRDT convergence is correct for *state*. It is **not** sufficient for **effects
that perform irreversible external actions** (send an email, charge a card,
fire a webhook): convergence may run the same effect on every peer, or run it
twice as merges arrive. Pure state can converge; an external side effect cannot
be merged.

For that narrow class, gate the effect behind a **single-writer effect
authority** — a designated peer (or a small Raft group owning *only the
effect-intent log*, not the whole graph) decides when an irreversible effect
fires, at-most-once. This is a hybrid: **CRDT for the state plane, a
single-writer/Raft authority for the irreversible-effect plane**, with the
#39c5 `RemoteOp` allowlist already gating which remote writes and effects a peer
may trigger at all. The large reactive core stays leaderless and local-first;
only the small irreversible-effect tail pays for consensus.

### Open prototype gates (deferred to implementation)

- HLC skew bounds and the LWW-vs-MV default per cell category.
- Whether closure replication (required for peers to recompute derived slots) is
  shipped as serialized compute descriptors or restricted to a shared compiled
  graph — this gates how much of the derived plane can live remotely at all.
- Delta-state vs op-based CRDT encoding on the wire, reusing `lazily-serde`.

### Move-aware sequence CRDT (`SeqCrdt`, `#lzseqcrdt`)

The register CRDTs above merge a cell's *value*; a document tree also needs
**mergeable sibling order**. `SeqCrdt<Id, V>` is the order layer: a coordinator-
free ordered sequence of keyed elements, the concurrency substrate beneath keyed
reconciliation (`#lzkeyrecon`).

- **Fractional-index positions.** Each element holds an orderable byte key
  (tiebroken by the minting peer). Inserting between two neighbours generates a
  key strictly between theirs, so concurrent inserts into the same gap on
  different replicas both survive and converge to one deterministic order.
- **Move-awareness (the requirement).** A move MUST be a *single* LWW
  reassignment of the element's position (highest HLC stamp wins) — **not** a
  delete + reinsert. So a reorder keeps the element's identity and value, and two
  concurrent moves of the same element converge to the later one **without
  duplicating it** (the failure mode of naive RGA delete+reinsert moves).
- **Independent registers.** Value, position, and tombstone are separate LWW
  registers, so a concurrent *move* and *value edit* of one element do not
  conflict; both apply. Removal is an LWW tombstone, so it converges and a
  concurrent resurrection is decided by stamp order.
- **Merge** is per-element LWW of value/position/tombstone plus adoption of
  unknown elements — commutative, associative, idempotent — and advances the
  local HLC past observed stamps so later local writes still win. This keeps the
  IPC `Snapshot`/`Delta` single-producer mirror as-is; the sequence CRDT lives
  only at the multi-writer boundary (pairs with `#lzcrdtplane`).
- **Tombstone GC** (`#lztombgc`) — `gc_with(is_stable)` / `gc(watermark)` drop
  tombstoned entries the caller proves causally stable (observed by every
  replica; the version-vector frontier is `#lzcrdtplane`'s, not a single
  replica's clock). Because `order`/`contains` already skip tombstones, dropping
  a stable one is observationally inert and convergent: an un-collected replica
  re-merges it as a tombstone (still skipped), and a genuine resurrection carries
  a newer stamp and wins by LWW regardless.

### Distributed wire transport (`CrdtSync`, `#lzcrdtplane5`)

The plane rides the existing `lazily-ipc` transport. A third `IpcMessage` variant,
`IpcMessage::CrdtSync(CrdtSync)`, carries multi-writer plane traffic beside the
single-producer `Snapshot`/`Delta` mirror (which is untouched):

- **`WireStamp { wall_time, logical, peer }`** — a plain-integer wire mirror of `HlcStamp`
  (same `(wall, logical, peer)` total order), so the wire format is codec-stable and usable
  whether or not a peer compiles the `distributed` feature. The `distributed` + `ipc`
  integration owns the lossless `HlcStamp ↔ WireStamp` conversion.
- **`CrdtOp { node, key: Option<NodeKey>, stamp: WireStamp, state: IpcValue }`** — one
  state-based (CvRDT) op: the converged register/sequence/text `state` for a node, tagged
  with the producing stamp and an optional wire-stable `NodeKey` (`#lzwirekey`) that
  survives `NodeId` churn.
- **`CrdtSync { frontier: Vec<(peer, WireStamp)>, ops: Vec<CrdtOp> }`** — an anti-entropy
  frame: the sender's per-peer **stamp-frontier** advertisement plus an op batch. This
  resolves the "delta-state vs op-based encoding" prototype gate above in favour of
  **state-based** sync (the registers and Seq/Text CRDTs all merge by state).

The frame round-trips through every codec (JSON / MessagePack / postcard) and is classified
by the FFI message kind (`LazilyFfiMessageKind::CrdtSync`). `CrdtSync::filter_readable(peer)`
drops ops for non-readable nodes entirely (omission, not redaction — like `Delta`), keeping
the frontier advertisement (metadata, not node content) so the receiver still computes a
sound watermark. Convergence (`merge` is a commutative/associative/idempotent semilattice)
and the watermark/GC safety contract (a collectable tombstone is observed by every replica)
are formally proven in `../lazily-spec/formal/lean/LazilyFormal/CRDT.lean` and documented in
[`protocol.md`](../lazily-spec/protocol.md) §Distributed.

The wire format, codec round-trips, permission filtering, and point-to-point
`IpcSink`/`IpcSource` delivery land in `#lzcrdtplane5a`.

**Runtime integration (`#lzcrdtplane5b`, FINAL — completes Phase 5).** `CrdtPlaneRuntime`
(behind `distributed` + `webrtc`) is the live glue between the plane primitives and a
reactive graph's `merge: crdt` root cells:

- **Registry.** It owns the session's `ReplicatedCell` root cells, addressed by `NodeId`
  with an optional wire-stable `NodeKey` (producer projection, `#lzwirekey`) so a cell stays
  addressable across `NodeId` churn.
- **Local edit → op.** `local_update` ticks the plane `Hlc`, mutates the typed cell, records
  the converged state in the `OpLog`, and returns the `CrdtOp` to broadcast (JSON-encoded
  `IpcValue` state).
- **Remote op → reactive graph.** `ingest` folds each not-yet-seen `CrdtOp` into its target
  replica via `ReplicatedCell::merge_remote` — driving downstream derived slots — while
  `CrdtPlane::observe_remote` advances the clock + stamp frontier so the causal-stability
  watermark and Seq/Text tombstone GC stay sound. Re-delivery is idempotent (the `OpLog`
  dedups by stamp).
- **Anti-entropy frames.** `sync_frame` / `sync_frame_since` / `sync_reply` advertise the
  local stamp frontier and ship only the ops a peer is missing.
- **`BridgeHub` fan-out.** `BridgeHub::poll` now fans `CrdtSync` frames out to the other
  peers, re-filtered to each target's read allowlist (ops a peer cannot read are omitted; the
  frontier advertisement is retained), beside the existing single-writer `Delta` routing.

An end-to-end two-replica convergence test drives a full edit → `CrdtSync` → ingest cycle
over the real `webrtc` `IpcSink`/`IpcSource` transport (`tests/crdt_plane.rs`).

## Internet-scale peer discovery: signaling server (`#yxjw`)

The CRDT cell plane (above) is leaderless and local-first, but peers still have
to *find* each other and open transport before any `Delta` can flow. On a LAN
that is mDNS or a known address; across the internet it needs a rendezvous
point. `#yxjw` is that rendezvous: a small **Cloudflare Worker signaling
server** that brokers peer discovery and relays the WebRTC SDP/ICE handshake so
peers can establish **direct P2P data channels**, falling back to server relay
of opaque payloads when a direct channel cannot be formed. It is strictly a
*discovery + relay* layer — it never parses or merges CRDT state, so it stays
trivially horizontally scalable and never becomes the consistency authority the
CRDT design deliberately avoids.

### Why a Cloudflare Worker + Durable Objects

- **One Durable Object per session id.** A WebSocket upgrade to
  `GET /session/:id` routes to a `SignalingRoom` DO keyed by
  `idFromName(sessionId)`. Cloudflare guarantees a single global instance per
  id, so each session gets a lock-free single-threaded coordination point for
  its roster with no external store. Scale is achieved by **sharding sessions
  across DO instances**, not by growing one server — which matches the
  availability-first, P2P posture of the CRDT recommendation (it "fits #yxjw
  signaling").
- **Edge-local.** Workers run close to peers worldwide, minimizing handshake
  latency; the DO migrates to wherever its session is most active.

### Roles and protocol

The server tracks a per-session **roster** of connected peers and forwards three
classes of frame. `PeerId` is the same `u64` as Rust `PeerId` (serialized by
`serde` as a bare JSON number; ids must stay ≤ `Number.MAX_SAFE_INTEGER`).

- Client → server: `join { peer, capabilities? }`, `offer { to, sdp }`,
  `answer { to, sdp }`, `ice { to, candidate }`, `relay { to, payload }`,
  `leave`.
- Server → client: `welcome { peer, peers }` (roster on join),
  `peer-joined`/`peer-left`, forwarded `offer`/`answer`/`ice`/`relay` stamped
  with the real `from`, and `error { code, message }`.

**Anti-spoofing:** the `from` on every forwarded frame is the *sender
connection's* registered peer id, never a client-supplied field, so a peer
cannot impersonate another.

### Permission boundary (reuses #39c5)

Admission and relay are gated by `SignalingPermissions`, the discovery-layer
mirror of `lazily::distributed::PeerPermissions`:

- `open` mode — any peer may join and signal any other joined peer (trusted /
  LAN / common discovery case).
- `allowlist` mode — **default-deny**: a peer may join only when explicitly
  granted, and may send directed frames only to explicitly allowed targets,
  exactly as #39c5 gates `RemoteOp`. This is the discovery-layer half of the
  same boundary; the Rust data plane still re-checks every `RemoteOp` locally.

### Reconnect / resync

The roster lives in the DO; it is authoritative. A peer that drops simply
re-`join`s and receives a fresh `welcome` roster — no snapshot epoch is needed
at this layer because signaling carries no CRDT state (the data plane's
HLC/dot-stamped `Delta`s self-heal independently per the CRDT design).

### Implemented (#yxjw)

Ships as a standalone TypeScript Worker under `signaling/` (its own Node
toolchain; not part of the Rust crate build):

- `src/protocol.ts` — wire types + untrusted-frame validation/codec.
- `src/permissions.ts` — `SignalingPermissions` (`open` / default-deny
  `allowlist`).
- `src/room-core.ts` — transport-agnostic `RoomCore`: roster, routing,
  anti-spoofing, permission gating.
- `src/room.ts` — `SignalingRoom` Durable Object (thin WebSocket adapter).
- `src/index.ts` — Worker entry: `/health` + `/session/:id` routing.
- `test/` — 24 vitest tests (protocol/permissions/room-core units plus an
  end-to-end Worker + DO + WebSocket test in the workerd runtime).

### Open gates (deferred)

- TURN/relay fallback policy when both peers are behind symmetric NAT (today the
  server can relay payloads, but a dedicated TURN allocation is out of scope).
- Authenticated admission tokens feeding the `allowlist` grants from an external
  identity source rather than static configuration.
- Capacity/back-pressure limits per session DO.

### Consumable clients (#s0fc)

So a project can depend on the signaling endpoint for distributed peer discovery
(the plan for agent-doc), the endpoint ships two clients that speak one shared
wire protocol — this section is the normative source of truth both conform to.

**Wire protocol (normative).** All frames are JSON with a `type` tag. `PeerId`
is a `u64` serialized as a bare JSON number (Rust `PeerId(u64)` ⇄ TS `number`;
keep ids ≤ 2^53).

- Client → server: `join {peer, capabilities?}`, `offer {to, sdp}`,
  `answer {to, sdp}`, `ice {to, candidate}`, `relay {to, payload}`, `leave`.
- Server → client: `welcome {peer, peers}`, `peer-joined {peer}`,
  `peer-left {peer}`, `offer/answer/ice/relay` (each stamped `from`),
  `error {code, message}`.

**Rust client** (`signaling-client` feature, `src/signaling_client.rs`):
`lazily::SignalingClient::connect(base_url, session, peer)` opens a
`tokio-tungstenite` WebSocket to `{base_url}/session/{session}`, joins, and
exposes `offer`/`answer`/`ice`/`relay`/`leave` plus `recv()` for
`ServerMessage`s. `ClientMessage`/`ServerMessage` are `serde`-tagged
(`rename_all = "kebab-case"`) and reuse the #39c5 `PeerId`. Conformance tests
assert the exact JSON shapes above. The feature pulls `tokio-tungstenite`
(rustls) only when enabled; the default build is unaffected.

**TypeScript client** (`@lazily/signaling` package, `signaling/src/client.ts`):
`SignalingClient.connect(baseUrl, session, peer)` (or `attach(socket, peer)` for
a pre-opened socket) works against any `WebSocket`-like transport (browser,
Node ≥ 22, or injected), with `onMessage` + the same send helpers. The package
exports `./client` and `./protocol`. Unit tests plus an end-to-end test drive
the real Worker + Durable Object in `workerd`.

Both clients are covered in CI (`cargo test --features signaling-client`; the
Worker job's `npm run check`). The Rust conformance tests and the TS protocol
share the byte-for-byte frame shapes defined above, so the two implementations
stay wire-compatible.

## Differences from lazily-zig

| Aspect | lazily-zig | lazily-rs |
|--------|-----------|-----------|
| Context | Explicit allocator | Owned allocations |
| Slot creation | `comptime` function pointers | Closures (`Box<dyn Fn>`) |
| Storage modes | `.direct` / `.indirect` | Unified via generics |
| FFI | Built-in `StringView` | Via `#[no_mangle]` + `extern "C"` |
| Thread safety | Mutex by default; `-Dthread_safe=false` removes locking | `Context` is single-threaded (`RefCell`); `ThreadSafeContext` uses a context-level lock |

## Differences from lazily-py

| Aspect | lazily-py | lazily-rs |
|--------|----------|-----------|
| Context | Plain `dict` | Typed `Context` struct |
| Slot keys | Object identity | `SlotId` (u64) |
| Cell equality | `!=` operator | `PartialEq` trait |
| Context resolvers | `resolve_ctx` functions | Direct context passing |
| Dependencies | Zero mandatory runtime crates by default; optional Tokio support and dev-only Criterion benchmarks | Zero (pure Rust) |
