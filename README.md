# lazily

Lazy reactive primitives for Rust — Context, Slots, Cells with automatic dependency tracking and cache invalidation.

[![crates.io](https://img.shields.io/crates/v/lazily.svg)](https://crates.io/crates/lazily)

## Overview

`lazily` provides five core primitives for reactive computation:

- **Context** — owns all reactive state and manages the dependency graph
- **Slot** — a lazily-computed cached value that automatically tracks dependencies
- **Cell** — a mutable value that invalidates dependent Slots when changed
- **Signal** — an eager derived value that recomputes the instant a dependency invalidates, with no intermediate unset value
- **Effect** — a side-effect callback that automatically reruns after tracked dependencies invalidate

Values are **lazy by default**: dependents are marked dirty on invalidation but only validated or recomputed when accessed. When you need eager push-style semantics — recompute immediately, observe `v1 -> v2` with no unset window — reach for **`Signal`**, which layers a puller effect over a memoized slot. The `Slot -> Cell -> Signal` progression lets you choose lazy or eager per derived value within one graph.
`ctx.computed()` cells are **guarded** (`T: PartialEq`): if recomputation produces the same value, downstream dirty caches and effects are left alone. There is no unguarded mode.
Multiple updates can be grouped with `ctx.batch(...)` so invalidation and effect reruns happen once after the outermost batch exits.

## Feature Set

Coverage by feature family across every binding, generated from
`coverage.json` in lazily-spec. Legend: ✅ shipped · `~` partial · `—` absent · `⊘` not applicable. The
canonical matrix with per-cell notes and platform carve-outs lives in
[`lazily-spec` § Cross-Language Coverage](https://github.com/lazily-hub/lazily-spec/blob/main/docs/coverage.md).

<!-- coverage-table:start -->
#### Summary — family × language

| Family | Rust | Python | Kotlin | JS | Dart | Zig | Go | C++ | C# | GDScript |
| --------- | :----: | :------: | :------: | :--: | :----: | :---: | :--: | :---: | :--: | :--------: |
| Reactive graph | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ~ |
| Materialization | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Family sync | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Statecharts | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Keyed collections | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ | — |
| Reactive queue | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Broadcast topic | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Work queue | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| CRDT data types | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ | — |
| Lossless tree | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ | — |
| Egress | ✅ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | — |
| Ingress | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Wire codec | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ | ✅ | — |
| Transport & FFI | ✅ | ✅ | ✅ | ~ | ~ | ✅ | ✅ | ~ | ✅ | — |
| Message passing | ✅ | ✅ | ✅ | ✅ | ✅ | ~ | ✅ | ✅ | ✅ | — |
| Reliable sync | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | — |
| Distributed plane | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Causal receipts | ~ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Security boundary | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Membership | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Coordination | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Presence | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Temporal | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Rate shaping | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Windowing | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Resilience | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Portable stdlib | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Service plane | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| Instrumentation | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | — |

**Roll-up rule:** a family cell is `✅` only when *every required* row in that family is `✅`; `~` when the family is mixed (some shipped or partial); `—` when no required row is shipped or partial; `⊘` only when every required row in the family is not applicable. Rows the spec marks **MAY** (`optional`) are excluded from the roll-up — declining an optional feature is not a gap.

A family cell summarises 73 feature rows. For row-level marks, per-cell notes, and platform carve-outs see [the canonical coverage matrix in `lazily-spec`](https://github.com/lazily-hub/lazily-spec/blob/main/docs/coverage.md).
<!-- coverage-table:end -->

CRDT convergence and the wire protocol are pinned by the shared conformance fixtures
and JSON Schemas in [`lazily-spec`](https://github.com/lazily-hub/lazily-spec) and the Lean models in
[`lazily-formal`](https://github.com/lazily-hub/lazily-formal).

## Development

**Minimum supported Rust version (MSRV): 1.88** — declared via `rust-version` in `Cargo.toml`. The crate uses `let_chains` (stabilized in 1.88) pervasively.

Run the local CI-equivalent suite with:

```bash
make check
```

The Makefile also exposes focused targets such as `make test-tokio`,
`make test-loom`, `make benchmark-evidence`, `make benchmark-check`, and
`make benchmark-update`.

`make check` measures the benchmark budgets rather than assuming them: it runs
`benchmark-evidence` (a reduced-sample Criterion pass over the budgeted groups,
plus the deterministic instrumentation profile) and then `benchmark-check`, which
fails if that evidence is absent or if it measured a different source tree than
the one checked out. There is no "budgets skipped" path that still exits 0.

## Usage

```rust
use lazily::Context;

let ctx = Context::new();

// Create a mutable cell
let counter = ctx.source(0i32);

// Create a derived value (automatically tracks dependencies)
let doubled = ctx.computed(|ctx| {
    let val = counter.get(ctx);
    val * 2
});

assert_eq!(doubled.get(&ctx), 0);

// Mutate the cell — dependents are marked dirty (not recomputed yet)
counter.set(&ctx, 5);

// Slot recomputes lazily on next access
assert_eq!(doubled.get(&ctx), 10);

// Effects run immediately and then after tracked dependencies change
let effect = ctx.effect(move |ctx| {
    println!("counter = {}", counter.get(ctx));
});

counter.set(&ctx, 6); // schedules and runs the effect once
effect.dispose(&ctx); // unsubscribes and prevents future reruns

// Batch writes coalesce invalidation and effect reruns.
ctx.batch(|ctx| {
    counter.set(ctx, 7);
    counter.set(ctx, 8);
});
```

### Lossless CRDT documents and durable replay

`CrdtTree` is the shared document contract for identity-preserving merge,
version-vector deltas, and materialized values. A snapshot is deliberately the
same operation as `delta_since` an empty frontier, so full hydration and
incremental synchronization cannot drift into separate semantics. `TextCrdt`
implements the contract, and downstream document CRDTs can implement it without
depending on a storage backend.

Reliable senders use `Outbox<S>` for one append/ack/prune/replay protocol and an
`OutboxStore` for five ordered-byte persistence operations. `InMemoryStore`
exercises the same path in tests; the `durable-sqlite` feature adds
`SqliteStore`/`SqliteOutbox`, partitioned by document hash, so acknowledged
epochs remain pruned across process restarts.

### Decorator-style typed factories

`#[lazily::source]` and `#[lazily::computed]` provide the same factory style as
`lazily-py`: the factory takes only a typed context and Lazily memoizes the
source/computed handle on that context. `ctx.get(factory)` reads a memoized cell,
and `ctx.set(source_factory, value)` mutates a memoized source cell.
(`#[lazily::cell]` / `#[lazily::slot]` remain as deprecated v1 aliases.)

This example is covered by `tests/decorator_factories.rs`.

```rust
use lazily::TypedContext;

lazily::define_schema!(CounterSchema);
type CounterContext = TypedContext<CounterSchema>;

#[lazily::source]
fn counter(_ctx: &CounterContext) -> i32 {
    0
}

#[lazily::computed]
fn doubled(ctx: &CounterContext) -> i32 {
    ctx.get(counter) * 2
}

let ctx = CounterContext::new();

assert_eq!(ctx.get(doubled), 0);

ctx.set(counter, 5);
assert_eq!(ctx.get(doubled), 10);
```

`define_schema!` intentionally creates a concrete, uninhabited marker type for
stable Rust 2024. It is "opaque" in the everyday sense that user code should not
construct or inspect values of it; Lazily uses only the type identity to prevent
mixing handles from different context families. Rust nightly's
`#[define_opaque]` for `type Alias = impl Trait` is a separate unstable compiler
feature for hidden concrete return types, and is not needed for Lazily context
schemas.

### Actor recipe (mailbox + RPC)

An **actor** — private state that talks only through messages — falls out of two
primitives: a `QueueCell` mailbox the actor drains, and correlation-by-id for
request/response RPC. No thread, no polling loop, no async runtime.

- **Mailbox**: `QueueCell<Request>`. A push flips the `is_empty` reader
  empty → non-empty, which reruns the actor's drain effect; the single-threaded
  scheduler flushes effects synchronously, so the message is handled by the time
  `send` returns.
- **RPC (request → response)**: each request carries a correlation `id`; the
  actor answers on a shared **outbox** `QueueCell<Reply>` and the caller pops the
  reply whose id matches. Correlating by id (rather than embedding a reply queue
  in each message) keeps every payload `PartialEq + Clone` — `QueueCell<T>`'s
  bound on its element.
- **Fire-and-forget**: a request with no reply is pure message passing — the
  actor mutates its private state and returns nothing.
- The actor's own state lives in a plain `Cell`, deliberately outside the
  reactive graph, so the drain effect subscribes to the mailbox alone.

```rust
use std::cell::Cell as StdCell;
use std::rc::Rc;
use lazily::{Context, QueueCell};

let ctx = Context::new();
let mailbox: QueueCell<(u64, i64)> = QueueCell::new(&ctx); // (id, delta); id 0 == "report"
let outbox: QueueCell<(u64, i64)> = QueueCell::new(&ctx);  // (id, total)
let total = Rc::new(StdCell::new(0i64));                    // private actor state

// Drain effect: wakes on every push, drains to empty, answers reports on the outbox.
let _drain = {
    let (mailbox, outbox, total) = (mailbox.clone(), outbox.clone(), Rc::clone(&total));
    ctx.effect(move |ctx| {
        while !mailbox.is_empty(ctx) {
            let Ok((id, delta)) = mailbox.try_pop(ctx) else { break };
            if id == 0 {
                total.set(total.get() + delta);            // fire-and-forget
            } else {
                let _ = outbox.try_push(ctx, (id, total.get())); // RPC reply
            }
        }
    })
};

mailbox.try_push(&ctx, (0, 5)).unwrap(); // handled synchronously on push
mailbox.try_push(&ctx, (0, 3)).unwrap();
mailbox.try_push(&ctx, (7, 0)).unwrap(); // request a report, id 7
assert_eq!(outbox.try_pop(&ctx).unwrap(), (7, 8));
```

The full runnable version — a typed `CounterActor` with a `send`/`get` API and
id-matched reply dispatch — is in
[`examples/actor_rpc.rs`](examples/actor_rpc.rs) (`cargo run --example
actor_rpc`). For a distributed actor whose messages cross a process boundary
with causal-receipt delivery guarantees, project this same shape onto the
command/RPC plane (`CommandRpcClient` / `CommandTransport` in `src/command.rs`,
feature `ipc`).

### Competing-consumer work queue

`WorkQueueCell` is the pull-based local-authority work queue: `claim` hands the
oldest pending item to exactly one worker under a fresh delivery ID; only that
worker can `ack` or `nack` it. Unacked leases redeliver after their strict
visibility deadline, and items reaching `max_deliveries` move to the DLQ.

```rust
use lazily::{Context, WorkQueueCell};

let ctx = Context::new();
let work = WorkQueueCell::<String>::new(&ctx, 30, 3);
work.push(&ctx, "render-report".into());
let delivery = work.claim(&ctx, "worker-a".into(), 100).unwrap();
assert!(work.ack(&ctx, &"worker-a".into(), delivery.delivery_id));
```

The instance is the serialization point. A distributed/HA backend must put
`claim` behind its leader or consensus log; the local shell does not pretend to
provide cross-process consensus.

## Why Lazy?

| | Lazy (Slots) | Eager (Signals) |
|---|---|---|
| **When does recomputation happen?** | On access (`get`) | Immediately on change |
| **Wasted work** | Zero — only compute what's read | Can compute values nobody uses |
| **Glitch-free** | By construction | Requires topological sorting |
| **Ordering** | Irrelevant — pull-based | Critical — push-based DAG walk |
| **Use case** | Request handling, data pipelines | UI rendering, real-time updates |

In a web server handling requests, you might have 50 computed values available but any given request only uses 5. With eager reactivity, all 50 recompute on every change. With lazy, only the 5 actually accessed compute.

`lazily` defaults to lazy but does not force the choice on you: derive with `ctx.computed()` for pull-based laziness, or `ctx.signal()` for the eager column above (UI rendering, real-time mirrors, always-materialized values). Both share the same context, dependency graph, glitch-freedom, and equality guard — pick per value.

## Core Concepts

### Context

`Context` owns all Slots and Cells. It manages the dependency graph and provides the API for creating, reading, and mutating reactive values. Think of it as the "world" for your reactive computations — in web frameworks, this maps to a request context, application scope, or component tree.

The current `Context` is intentionally single-threaded. It uses `RefCell` and
non-`Send` callback storage to keep the fast path allocation-only and mutex-free.
Create independent contexts per OS thread for local graphs, or use
`ThreadSafeContext` when one reactive graph must be shared across threads.

### Slot

A `Computed<T>` wraps a compute function `Fn(&Context) -> T`. The result is cached after first access. Dependencies are discovered automatically via a thread-local tracking stack — any Slot or Cell accessed during computation becomes a dependency. `ctx.computed()` is the derived-value constructor and is **guarded** (`T: PartialEq`): equal recomputations suppress downstream work. `ctx.slot()` is the bound-free storage-sense primitive (no guard, holds non-`PartialEq` values).

When a dependency is invalidated, the Slot marks its cached value dirty. It does **not** validate or recompute until `ctx.get()` is called again.
For `ctx.computed()` cells, if recomputation returns a value equal to the previous cache, downstream dirty Slots become fresh without recomputing, and scheduled effects that only depended on unchanged Slots skip cleanup/rerun.

**Dependencies are dynamic.** Every time a Slot recomputes, it re-discovers its dependencies from scratch. If your compute function has conditional branches that access different Cells depending on state, the dependency graph updates automatically. No stale subscriptions, no manual cleanup.

### Cell

A `Source<T>` holds a mutable value. `source.set(&ctx, value)` and `ctx.set()` compare old and new values via `PartialEq` — if unchanged, no invalidation occurs. If changed, all dependent Slots are recursively marked dirty.

### Signal

A `SignalHandle<T>` is an **eager** derived value — a *derived construct, not a core primitive* (`Signal ≡ Computed.eager`: a guarded computed cell plus a puller Effect). Where a Slot only marks itself dirty on invalidation and recomputes on the next read, a Signal recomputes *the instant a dependency is invalidated*, before the invalidating `set`/`set`/`batch` call returns. The value is always materialized, so observers never see an intermediate unset value — a dependency change drives the value directly from `v1` to `v2`.

```rust
let n = ctx.source(1);
let doubled = ctx.signal(|ctx| n.get(ctx) * 2); // materialized now: 2
n.set(&ctx, 5);                                  // doubled is already 10 — eager
assert_eq!(doubled.get(&ctx), 10);
```

A Signal is **composed from existing primitives**, not a parallel engine: a guarded computed cell (`ctx.computed`) supplies glitch-free, pull-based, equality-guarded recomputation, and a small puller `Effect` re-materializes that slot after every invalidation to supply the eagerness. Consequently a Signal inherits the equality guard (an equal recompute suppresses downstream work) and diamond glitch-freedom (`D = f(A, g(A))` never surfaces a mixed new-`A`/old-`g(A)` intermediate), and batched writes settle to one consistent recomputation at batch exit.

`ctx.signal()` requires `T: PartialEq + 'static` (the equality guard); `get_signal` additionally requires `T: Clone`. `signal.dispose(&ctx)` removes the eager puller — the value stays readable but reverts to lazy (recompute-on-read) behavior. The same primitive is available on `ThreadSafeContext` (`signal`, returning a `Send + Sync` `ThreadSafeSignalHandle<T>`) and `AsyncContext` (`signal_async`, with a non-blocking `get_signal` snapshot and an awaiting `get_signal_async`); see `SPEC.md` for the per-context type bounds and the async eagerness caveat.

### Batch Updates

`ctx.batch(|ctx| { ... })` groups multiple cell updates and explicit slot/cell clears into one invalidation pass. Nested batches flush only when the outermost batch exits. Direct `ctx.get()` reads inside the callback see the latest cell value immediately; changed-cell dependents are marked dirty after the batch, so Slot reads during the callback return their pre-batch cached value until the batch completes.

### Effect

An `Effect` represents a side-effect callback registered with `ctx.effect()`. Effects run immediately, track any Slots or Cells read during that run, and rerun after those dependencies invalidate. Scheduled effect reruns are flushed after the invalidation pass, so diamond dependency paths coalesce to one rerun. Effects scheduled only by dirty Slot dependencies first validate those Slots and skip cleanup/rerun when values are unchanged.

Effects can return a cleanup closure. Cleanup runs before the next rerun and when the handle is disposed:

```rust
let effect = ctx.effect(move |ctx| {
    let value = counter.get(ctx);
    move || println!("cleanup for {value}")
});

effect.dispose(&ctx);
```

### Durable effect sinks (`#lzdurablesink`)

Durable storage is an **effect sink**, not a transition authority. While a Lazily
runtime is live, transitions are decided from live state; durable storage
receives a projection or an ordered fact as an effect, and a sink MUST NOT reload
storage to arbitrate the transition it is currently persisting. Authority flows
one way:

```text
cold durable state ──hydrate once──▶ live Lazily state
                                      │   (computed / fact stream)
                                      ▼
                            Effect / AsyncEffect
                                      ▼
                          write-only durable sink
                                      │  ack / failure
                                      └────────▶ live Lazily state
```

- **Projection** (latest recoverable state): a `Computed` read by an `Effect` /
  `AsyncEffect` does an idempotent upsert of the settled epoch. Lazily's existing
  effect-batch coalescing means a batch `A → B → C` persists only `C` — correct
  for a current-state projection.
- **History** (every accepted fact, ordered): use the existing `TopicCell` /
  `Outbox` drain — append, replay-from-cursor, `ack_through` — not ordinary
  effects. Do not modify effects to retain intermediate values; that would
  duplicate the ordered-stream primitives.
- **Acknowledgement**: success advances a monotone `durable_through(epoch)`; a
  sink failure is represented in live state as `pending` / `retrying` /
  `backpressured` and MUST NOT trigger a storage reload at the decision seam.
- **Markers**: values on the [`Ephemeral`](src/presence.rs) plane MUST NOT enter a
  durable sink — the `Ephemeral`/`Durable` markers statically reject the mismatch
  (compile-fail doctest in `src/presence.rs`). Cold loading and migration belong
  to a separate startup hydrator, not the runtime effect.

Lazily ships no storage backend for this — the sink is an application-owned
write-only trait (the `OutboxStore` boundary is the existing example). The
authority rule, the projection-vs-history shape table, the two reference examples
(coalesced projection; lossless ordered fact sink), and the caller-chosen
`eventual_projection` / `durable_before_applied` / `ephemeral` visibility
policies live in [`lazily-spec` § Durable Effect Sinks](https://github.com/lazily-hub/lazily-spec/blob/main/docs/durable-sinks.md);
the formal backstop is `lazily-formal/LazilyFormal/DurableSink.lean`.

## API

| Method | Purpose |
|--------|---------|
| `Context::new()` | Create a new context |
| `lazily::define_schema!(Name)` | Define an uninhabited schema marker for `TypedContext<Name>` |
| `ctx.computed(\|ctx\| T)` | Create a derived lazily-computed value |
| `ctx.slot(\|ctx\| T)` | Create a lazily-computed slot; synonym of `ctx.computed()` |
| `ctx.memo(\|ctx\| T)` | Create a lazily-computed slot with a `PartialEq` memoization guard |
| `ctx.memoized_slot::<Key, T, _>(\|ctx\| T)` | Return a context-local factory slot handle, creating it on first use |
| `slot.get(&ctx)` | Get value (computes if unset) |
| `ctx.get(&slot)` | Context method alias for `slot.get(&ctx)` |
| `ctx.source(value)` | Create a mutable cell |
| `ctx.memoized_cell::<Key, T, _>(\|ctx\| T)` | Return a context-local factory cell handle, creating it on first use |
| `source.get(&ctx)` | Get cell value |
| `ctx.get(&cell)` | Context method alias for `source.get(&ctx)` |
| `ctx.set(&cell, value)` | Update cell (marks dependents dirty if changed) |
| `source.set(&ctx, value)` | Handle method alias for `ctx.set(&cell, value)` |
| `#[lazily::computed] fn name(ctx: &TypedContext<_>) -> T` | Decorator-style typed computed factory over `TypedContext` |
| `#[lazily::source] fn name(ctx: &TypedContext<_>) -> T` | Decorator-style typed source factory over `TypedContext` |
| `ctx.signal(\|ctx\| T)` | Create an eager derived value (recomputes on invalidation, no unset window); `T: PartialEq + 'static` |
| `signal.get(&ctx)` | Get the signal's value (`T: Clone`); also `ctx.get_signal(&signal)` |
| `signal.dispose(&ctx)` | Remove the eager puller; value reverts to lazy recompute-on-read |
| `signal.is_active(&ctx)` | Check whether the eager puller is still registered |
| `ctx.batch(\|ctx\| { ... })` | Defer changed-cell dirty marking and explicit clears until the outermost batch exits |
| `ctx.effect(\|ctx\| { ... })` | Run an effect immediately and rerun it after tracked dependencies invalidate |
| `ctx.is_set(&slot)` | Check if slot has a cached, fresh value |
| `slot.clear(&ctx)` | Clear cached value and cascade to dependents |
| `cell.clear_dependents(&ctx)` | Clear downstream slots without changing cell value |
| `effect.dispose(&ctx)` | Dispose an effect and unsubscribe dependencies |
| `effect.is_active(&ctx)` | Check whether an effect is still registered |

### Lazily standard library

`lazily::stdlib` contains optional-convenience semantics built from the
portable primitives without moving runtime policy into the graph kernel. Its
`Timer` adapts the logical `TimerCore` to Rust's monotone `Instant`: use
`Timer::after` / `Timer::at`, non-blocking `poll` / deterministic `poll_at`, or
the zero-async-runtime blocking `wait`.

`Timeout<T>` composes that timer with caller-owned operation and cancellation
probes. It latches typed completed, timed-out, cancelled, or unavailable
outcomes without owning a future, executor, or hidden thread. `poll_at` is the
deterministic clock seam; `wait_with` additionally accepts the caller's clock
and wait policy so channels, condition variables, reactors, or test schedulers
can supply wakeups. The deadline is strict, while completion wins a
completion/cancellation race observed before it.

`RevisionBarrier` is the corresponding blocking coordination bridge. Producers
publish monotone revisions with `advance`; waiters require both a revision newer
than their captured `after_revision` and a derived predicate. Predicate checks
run outside the barrier lock and are retried if a revision or receipt
notification races them, closing the check-to-sleep lost-wakeup window. Optional
`Timer` and barrier-owned cancellation inputs produce typed satisfied,
timed-out, cancelled, disposed, or unavailable outcomes. Keyed effect-receipt
storage and transport remain application-owned; update that ledger and call
`notify`.

This layer deliberately does not add generic mutexes, semaphores, latches,
channels, or task groups: Rust already supplies local synchronization and Lazily
already supplies distributed `LockCell`, `SemaphoreCell`, `BarrierCell`, and
`QuorumCell`. New standard-library primitives should be admitted only when they
add a reactive revision or effect-lifecycle guarantee.

### ThreadSafeContext

Enable the `thread-safe` feature (v0.18.0+, was default before):

```bash
cargo test --features thread-safe
```

`ThreadSafeContext` is the mutex-backed counterpart for sharing one reactive
graph across OS threads. It mirrors the core `Context` methods while requiring
`Send + Sync + 'static` values and compute/effect callbacks. The graph lock is
released before user compute callbacks, effect callbacks, or cleanup closures
run, so callbacks can re-enter the same context without deadlocking. If a slot
is invalidated while its callback is running, the stale result is discarded and
the getter retries before returning a fresh value.

Cell values use a read-scaling sidecar (v0.23.0+): `ctx.source()` reads take a
shared `RwLock` read (concurrent readers don't serialize), and `ctx.source_copy()`
opts small `Copy` values into a wait-free inline seqlock — no heap allocation,
no refcount traffic on read. Both mirror the slot fast-path design.

The graph state lock is an `RwLock` (v0.24.0+, #lzstateinvalidation):
`read_state()` acquires a shared read lock, `lock_state()` an exclusive write
lock. All invalidation routes through the state-locked path — one lock for the
entire BFS pass, with atomics-only dirty marking (no per-node Mutex
acquisitions). This mirrors lazily-cpp's single-recursive-mutex model: fewer,
coarser locks with a fast inner loop beat many fine-grained locks for reactive
fan-out workloads.

## Design

- **Lazy by default, eager on demand:** Slots mark dirty on invalidation and validate/recompute on access; `ctx.signal()` opts a value into eager recomputation (a guarded computed cell + puller-effect composition) with no intermediate unset state
- **Derived constructor:** `ctx.computed()` names guarded derived values while preserving `ctx.slot()` as the bound-free storage-sense primitive
- **PartialEq guard:** `Source::set()` only invalidates when value actually changes
- **Guarded computed:** every `ctx.computed()` cell (`T: PartialEq`) compares recomputed values and suppresses downstream recomputation/effect reruns when values are equal — there is no unguarded mode
- **Dynamic dependencies:** Edges re-discovered on each recomputation (no stale subscriptions)
- **Batching:** Multiple writes share one invalidation/effect flush boundary
- **Effect scheduling:** Effects rerun after dependency invalidation and coalesce duplicate schedules
- Slot-id-indexed contiguous node storage for the single-threaded fast path
- Interior mutability via `RefCell` (single-threaded)
- Thread-local tracking stack for automatic dependency discovery
- Zero mandatory runtime dependencies in the default library surface
- Optional `instrumentation` feature for benchmark counters, lock timing, and thread-safe lock attribution

## Threading Roadmap

`lazily-rs` guarantees local, single-threaded `Context` graphs plus an explicit
`ThreadSafeContext` for shared graphs. `Computed<T>` and `Source<T>` are
`Send + Sync` when `T` is `Send + Sync`, and `Effect` is also `Send + Sync`,
but handles must be used with their owning context.

Enable the optional `loom` feature to run the thread-safe synchronization model:

```bash
cargo test --features loom --test thread_safe_loom
```

Enable the optional `tokio` feature for sync-on-Tokio integration tests and the
`tokio_sync` example (requires `thread-safe` since v0.18.0 — the integration
exercises `ThreadSafeContext` through `tokio::spawn`):

```bash
cargo test --features "tokio thread-safe"
cargo run --example tokio_sync --features "tokio thread-safe"
```

The feature proves `ThreadSafeContext` can be shared through `tokio::spawn` and
`tokio::task::spawn_blocking`. It does not add async computations or effects;
those need the separate `AsyncContext` design captured in `SPEC.md`, including
in-flight future deduplication, stale completion handling, cleanup ordering, and
separate `Send` versus `LocalSet` surfaces.

`ThreadSafeContext` intentionally keeps one state `RwLock` (v0.24.0+,
#lzstateinvalidation) while fresh cached slot reads use a per-slot read-mostly
cached-value sidecar. Dependency edges, dirty/revision state, cached-value
publication, batching, and effect queues all mutate under the state lock.
In-flight recompute waiters use per-slot generation `Condvar` sidecars so they
can park while the compute owner runs user code, and a completion only wakes
waiters for that finished slot. Changed-cell and slot-value invalidation build
an explicit frontier plan, then apply dirty flags, revisions, and effect
scheduling in one state-lock mutation boundary with atomics-only dirty marking.
The `thread_safe_graph_propagation` benchmarks compare fan-out eager validation,
fan-out/fan-in lazy dirty epoch publication, and fan-in batched flush behavior
with lock attribution. Sharded-lock or CAS variants should wait for lock
wait/hold benchmark evidence and a Loom or Shuttle safety model for stale
in-flight completion, invalidation during compute, dynamic dependency
cleanup/disposal, effect scheduling/disposal, and re-entrant callbacks. A
lock-free versioned optimistic read path is deferred until cached values can be
retained independently of graph-protected erased-value storage.

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for full benchmark results, regression budgets, lock attribution, instrumentation profiles, and a cross-language comparison with lazily-cpp and lazily-zig.

For large-graph evidence, see the [Scale (≥1M cells)](BENCHMARKS.md#scale-1m-cells--lzscalebench) section (a criterion-tracked `scale` group): a spreadsheet-shaped graph of ~2M nodes builds in ~0.13 s and fully recomputes from cold in ~0.10 s, while a single-cell edit + bounded viewport read recomputes only the viewport (~11.5 µs / 1,000 cells, ~5,000× cheaper than a full recalc).

**Google Sheets scale (10,000,000 cells/workbook — the documented limit).** Run at the full Sheets cap, lazily builds the whole workbook in **~0.7 s**, recomputes it cold in **~0.5 s**, and still does a viewport edit in **~11 µs** (scale-independent). (Microsoft Excel's 1,048,576 × 16,384 = 17,179,869,184-cell grid is *capacity*, not populated cells — lazily's sparse arena only pays for populated cells, so the limit is populated-cells vs RAM, not the grid.)

> **A "cell count" here counts two cells per row** — the benchmark models a column of formulas `=A_i + A_{i-1}`, so each row is **one input cell `A_i` plus one formula cell**. `N` rows ⇒ `N` inputs + `N` formulas = `2N` cells, matching how a real sheet mixes value cells and formula cells. (Each formula *depends on* two inputs, but is itself a single cell.) So "10M cells" = 5,000,000 inputs + 5,000,000 formulas.

```bash
cargo bench --features scale-bench --bench scale                     # default 1M (2M nodes)
LAZILY_SCALE_N=5000000 cargo bench --features scale-bench --bench scale   # Google Sheets 10M cells
```

## Multi-Language

lazily is implemented across three languages with shared semantics:

| | [lazily-rs](https://crates.io/crates/lazily) | [lazily-zig](https://github.com/lazily-hub/lazily-zig) | [lazily-py](https://github.com/lazily-hub/lazily-py) |
|---|---|---|---|
| Context | Owned `Context` struct | Explicit allocator | Plain `dict` |
| Slot creation | `Box<dyn Fn>` closures | `comptime` function pointers | Lambdas |
| Cell equality | `PartialEq` trait | `std.meta.eql` | `!=` operator |
| Thread safety | Single-threaded `Context`; explicit `ThreadSafeContext` | Mutex by default | GIL |
| Storage | Unified generics | `.direct` / `.indirect` | Object identity |

## Cross-Channel Compatibility

The cross-language family should use one graph-state protocol across channels:
`IpcMessage::Snapshot` and `IpcMessage::Delta`. Rust FFI is viable as a narrow
C ABI adapter with opaque handles and owned byte buffers, not by sharing live
Rust contexts, closures, typed handles, or references across the ABI.

IPC, WebSocket frames, WebRTC data channels, and FFI byte buffers can then carry
the same permission-filtered snapshots and deltas. Transport code owns framing,
memory ownership, reliability, and back-pressure; lazily semantics stay in the
shared message schema.

Enable the `ffi` feature for the C ABI adapter. It exposes an opaque
`LazilyFfiChannel`, JSON `IpcMessage` validation/classification helpers, and
Rust-owned `LazilyFfiBytes` buffers with an explicit free function. The adapter
re-encodes every accepted frame as canonical `IpcMessage` JSON, so FFI callers
share the same state plane as other channels.

## Cross-Process Zero-Copy Transport (`#lzzcpy`)

A `Snapshot` / `Delta` / `CrdtSync` message may carry large payloads (an Arrow
record-batch, an image, a serialized sub-document). Copying those bytes through
the wire codec on every hop is the dominant cost of a distributed deployment.
The zero-copy transport instead **spills** a large payload to a **blob backend**
and ships a small `ShmBlobRef` descriptor; the receiver **resolves** the
descriptor against the same backend and reads the bytes in place — no copy, no
checksum recompute.

Spec: [`lazily-spec/docs/zero-copy-transport.md`](https://github.com/lazily-hub/lazily-spec/blob/main/docs/zero-copy-transport.md).
Formal: [`lazily-formal/LazilyFormal/ZeroCopyTransport.lean`](https://github.com/lazily-hub/lazily-formal/blob/main/LazilyFormal/ZeroCopyTransport.lean)
— proves spill-then-resolve identity, backend isolation, ABA/generation safety,
and checksum integrity for **any** backend satisfying the contract.

```rust
use lazily::{
    BlobBackend, BlobRouter, InProcessBackend, ArrowBackend,
    spill_message, Delta, DeltaOp, IpcMessage, NodeId,
};

let mut inproc = InProcessBackend::new()?;   // wraps ShmBlobArena (in-process)
let mut arrow = ArrowBackend::new()?;         // holds Arrow IPC stream bytes

// Producer: spill large Inline payloads above a threshold.
let big = vec![0x5Au8; 500];
let mut msg = IpcMessage::Delta(Delta::next(1, vec![DeltaOp::slot_value(NodeId(7), big.clone())]));
let spilled = spill_message(&mut msg, &mut inproc, 64);
assert_eq!(spilled, 500); // payload replaced with a SharedBlob descriptor

// Receiver: resolve by routing the descriptor's `backend` discriminator.
let mut router = BlobRouter::new();
router.register(&inproc).register(&arrow);
// ...after decoding the wire message...
//   let bytes = router.resolve(&payload);  // zero-copy view into the backend
```

Three backends ship:

| Backend | Holds the bytes | Cross-process? | Feature |
|---|---|---|---|
| `InProcessBackend` | wraps `ShmBlobArena` (single address space) | no | `ipc` |
| `ArrowBackend` | Arrow IPC stream bytes (zero-copy columnar) | no | `ipc` |
| `ShmBackend` | POSIX `shm_open` + `mmap` region | yes (same host) | `shm` |

The `ShmBlobRef` descriptor gained an optional `backend` discriminator
(`BlobBackendKind::Shm` \| `Arrow` \| `InProcess`), defaulting to `Shm` so
legacy descriptors validate unchanged. New backends (RDMA/verbs, CUDA IPC) plug
in by implementing the `BlobBackend` trait and adding a discriminator value — no
transport or codec change.

## The lazily family

lazily is one reactive model implemented across many languages — the same cell
kernel, the same keyed collections and CRDTs, and the same wire protocol — so
peers written in different languages talk to each other without a translation
layer.

- [`lazily-spec`](https://github.com/lazily-hub/lazily-spec) — the
  language-agnostic wire protocol, the cross-language feature matrix, and the
  conformance corpus every binding replays.
- [`lazily-formal`](https://github.com/lazily-hub/lazily-formal) — the Lean 4
  formal model every binding inherits its proofs from.

| Repo | Language |
|---|---|
| **`lazily-rs`** | Rust — the reference implementation (you are here) |
| [`lazily-py`](https://github.com/lazily-hub/lazily-py) | Python |
| [`lazily-go`](https://github.com/lazily-hub/lazily-go) | Go |
| [`lazily-kt`](https://github.com/lazily-hub/lazily-kt) | Kotlin / JVM |
| [`lazily-js`](https://github.com/lazily-hub/lazily-js) | JavaScript / TypeScript |
| [`lazily-cs`](https://github.com/lazily-hub/lazily-cs) | C# / .NET |
| [`lazily-cpp`](https://github.com/lazily-hub/lazily-cpp) | C++ |
| [`lazily-zig`](https://github.com/lazily-hub/lazily-zig) | Zig |
| [`lazily-dart`](https://github.com/lazily-hub/lazily-dart) | Dart / Flutter |
| [`lazily-react`](https://github.com/lazily-hub/lazily-react) | React / Preact bindings layered over `lazily-js` (not a separate language binding) |

Per-binding parity, with per-cell notes and platform carve-outs, lives in
[`lazily-spec` § Cross-Language Coverage](https://github.com/lazily-hub/lazily-spec/blob/main/docs/coverage.md)
— it is generated from `coverage.json`, so it does not rot the way a hand-copied
table would.

## Related

- [lazily-spec](https://github.com/lazily-hub/lazily-spec) — language-agnostic wire protocol + conformance fixtures shared by every binding
- [lazily-formal](https://github.com/lazily-hub/lazily-formal) — Lean 4 formal model (flat FSM kernel, full Harel state chart, reactive graph kernel, keyed collections, ordered tree, LIS reconciliation, async slot state) with universal proofs every binding inherits
- [lazily-zig](https://github.com/lazily-hub/lazily-zig) — Zig implementation with FFI support
- [lazily-py](https://github.com/lazily-hub/lazily-py) — Python implementation with context-as-dict
- [Blog post: Lazily — Reactive Primitives Done Right](https://briantakita.me/posts/lazily-reactive-signals)

## License

MIT
