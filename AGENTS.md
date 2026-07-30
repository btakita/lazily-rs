# lazily-rs

Lazy reactive primitives library for Rust.

## Commit & Push

Commit and push completed work at the end of every turn that changed code,
tests, docs, or fixtures — do not leave finished work uncommitted. Run `make
check` first and ensure it is green; stage only the files that belong to the
change (never secrets or private customer names — see the workspace
`runbooks/private-name-hygiene.md`); write a concise commit message in the
repo's existing style; push to the current branch on `origin`. This standing
rule overrides the harness default of "commit only when explicitly asked" for
this repo.

## Architecture

- `src/reactive_graph.rs` — capability traits over the three execution models (`#lzspecedgeindex`): bound-free `ReactiveGraph` (disposal, scopes, batch, degree introspection, associated `Computed`/`Source`/`Effect`/`Scope` types), `Teardown` (scope contract), `SyncReactiveGraph`/`AsyncReactiveGraph` (construction + reads, split by read discipline), and the blanket-implemented `ThreadSafeReactiveGraph` marker. Read discipline and thread-safety are orthogonal axes, hence four traits
- `src/context.rs` — `Context` struct, dependency graph, value-threaded tracking via the fortified `Compute` view (no thread-local tracking stack; `#lzcellkernel`)
- `src/cell.rs` — **the Cell kernel** (`#lzcellkernel`): `Cell` is a *conceptual* word (a value-bearing reactive node), not a type — there is **no `Cell<T, K>` genus struct**. The two kinds of cell are two concrete `Copy` handle structs (each a lightweight `SlotId` into Context): `Source<T, M = KeepLatest>` (source cell — written from outside, folds under policy `M`) and `Computed<T>` (computed cell — computed from upstream). The former `Source<M>` / `Formula` kind markers dissolve — `M` is now `Source`'s own policy param. Reads (`get`, `subscribe`, `dispose`) exist on both handles; writes (`set`, `merge`) exist **only** on `impl<T, M: MergePolicy<T>> Source<T, M>`, so `computed.set(…)` is a *"no method"* compile error (write protection without a trait, design §3). Computed lifecycle (`eager`/`lazy`/`is_eager`/`clear`/`get_rc`) is on `impl<T> Computed<T>`. Replaces the former `SlotHandle`/`CellHandle`/`SignalHandle`/`MergeCellHandle` structs (all deleted) and the vestigial `Reactive<T>`/`Source<T>` traits (deleted). Note `src/slot.rs` and `src/signal.rs` are gone — `Slot` survives only as the *storage* sense (`SlotId`, and the arena node struct `ComputedNode`/variant `Node::Computed` in `context.rs`; the source node is `SourceNode`/`Node::Source`)
- **Unified shared reads (`#lzrsgetarc`).** `Context::get_rc` and tracked `Compute::get_rc` accept either `Source` or `Computed` through `ReadRc`, return the same current value as `get` without requiring `T: Clone`, refresh computed values identically, and register the same dependency edge when tracked. This is the Rust spelling of the `Reactive.readShared_eq_readCell`, `Reactive.trackedSharedRead_eq_trackedRead`, and `Reactive.trackedSharedRead_registers_edge` formal pins.
- `src/keyed_order.rs` — `KeyedOrder<K, H>`, the graph-agnostic bookkeeping core shared by all three map flavors: present set + authoritative key order, `position`, and the atomic-move algebra (`move_to`/`move_before`/`move_after`, LIS-preserving). Closure-free, context-free, and with no interior mutability — each flavor wraps it in its own `RefCell`/`Mutex` and owns its own reactivity. Mutators report `Mutation::{Changed,Unchanged}` / `Move::{Absent,Unchanged,Reordered}` so the shell decides which signal cells to bump; reactivity deliberately stays out of the core, because a core that faked it with polled counters is exactly lazily-zig's non-reactive map
- `src/cell_family.rs` — `ReactiveMap<K, V, H>` unified keyed reactive collection (`#reactivemap`) over the `MapHandle` trait (`Source` input cells / `Computed` derived cells) + `EntryKind`. `SourceMap<K,V> = ReactiveMap<K,V,Source<V>>` (adds cell-only `set` + eager value-minting `entry`/`entry_with`); `ComputedMap<K,V> = ReactiveMap<K,V,Computed<V>>` (lazy `get_or_insert_with` mint-on-access + eager `materialize_all` pre-mint; no `set`). `MapHandle` is **kept** (not collapsed): its two impls are for the distinct handle structs `Source<V, KeepLatest>` and `Computed<V>`, carrying `const KIND: EntryKind` per kind. No eager/lazy mode flag — eager = pre-mint loop, lazy = mint-on-access
- `src/thread_safe_reactive_family.rs` — `ThreadSafeReactiveMap<K, V, H>` (`#reactivemap`, feature `thread-safe`): the `Send + Sync` flavor over `ThreadSafeContext` (`Arc<Mutex<KeyedOrder>>` present-set state; reactive `keys`/`len`/`contains_key` plus `position`/`move_*`/`remove`) so a keyed map can live in a cross-thread owner; `ThreadSafeMapHandle` trait, `ThreadSafeSourceMap`/`ThreadSafeComputedMap` aliases. Materialization confluence proved in lazily-formal
- `src/async_reactive_family.rs` — `AsyncReactiveMap<K, V, H>` (`#reactivemap`, feature `async`): the `AsyncContext` flavor, with the same `KeyedOrder` core, membership/order plane, and `position`/`move_*`/`remove` surface — ordering is not async-coloured; derived slots resolve asynchronously so `observe` returns `Option<V>` (eventual transparency); `AsyncMapHandle` trait, `AsyncSourceMap`/`AsyncComputedMap` aliases
- **Eager construction** (`#lzcellkernel`, formerly `src/signal.rs`) — an eager `Computed`: `ctx.computed(f).eager()` (or the `ctx.signal(f)` convenience, which builds a guarded computed cell via `computed` and makes it eager) attaches a puller `Effect` that keeps it materialized. `.eager()` is idempotent and returns the *same* handle; `.lazy()` reverses it. Eagerness is graph state — an `eager` bit on `ComputedNode` plus the `eager_by: HashMap<SlotId, SlotId>` side table in `context.rs` (cleared on dispose/lazy) — not a distinct type, so the former `Signal`/`SignalHandle` are retired and the `#lzsignaleager` per-write-puller bug is structurally unwritable. `ThreadSafeContext`/`AsyncContext` keep their own signal handles for now
- `src/async_context.rs` — `AsyncContext` async reactive graph (feature-gated behind `async`)
- `src/thread_safe.rs` — `ThreadSafeContext` mutex-backed shared graph (feature-gated behind `thread-safe` since v0.18.0)
- `src/merge.rs` — RelayCell Phase 1 (`#relaycell`): the `MergePolicy` merge algebra (associative `⊕`, `const COMMUTATIVE`/`IDEMPOTENT` flags) and concrete policies (`KeepLatest`/`Sum`/`Max`/`SetUnion`/`RawFifo`, plus `CrdtJoin<C>` wiring existing `CellCrdt` units behind `distributed`). Under the Cell kernel (`#lzcellkernel`) a "merge cell" is just a `Source<T, M>` with `M ≠ KeepLatest`; the identity `Source ≡ Source<T, KeepLatest>` is a default type parameter, not a spec assertion. The former `MergeCellHandle<T,M>` struct and the `Reactive<T>`/`Source<T>: Reactive<T>` read/write traits are **deleted**. Constructors `Context::source::<M>(v)` (was `merge_cell`) / `Context::apply_merge` (the merge write by id, routes through `set_source`).
- `src/relay.rs` — RelayCell Phase 2 (`#relaycell`): the in-proc `RelayCell<T,M>` conflating relay — hot head (`Option<T>` coalesced under `MergePolicy`), reactive `BackpressurePolicy` (cells: `dimension`/`high_water`/`low_water`/`overflow`), `Overflow` (Block/DropNewest/DropOldest/Conflate/Spill), `IngressOutcome`, demand-driven `depth`/`is_full`/`is_empty` Slots, `ingress`/`drain`. Construction validates overflow vs `M::CONFLATES` (Conflate rejected for RawFifo). Converged egress independent of drain schedule (the `relay_converges` invariant).
- `src/spill.rs` — RelayCell Phase 3 (`#relaycell`): `SpillStore<T,M>` paged durable tail (generalizes `DurableOutbox`) — immutable cold `SpillPage`s (coalesced window summaries), bounded `manifest`, egress cursor, ack-before-reclaim; `SpillMode` (CompactOnWrite/AppendCompact); `reconstruct` (spill_lossless) + `replay_unacked` (idempotent crash-replay).
- `src/relay_roles.rs` — RelayCell Phase 5 (`#relaycell`): `Outbox<T,M>`/`Inbox<T,M>` role facades over `RelayCell` — Outbox backpressures the local producer via `is_full` (default Conflate); Inbox meters the remote via a credit budget (`ready`/`credits`/`receive`/`consume`). A link is Outbox→Transport→Inbox.
- `src/service.rs` — embedded-service plane (`#lzservice`): `HealthCell`/`Core` (composed liveness probe → Healthy/Degraded/Unhealthy, worst component dominates), `ReadinessCell`/`Core` (ready iff all conditions), `DiscoveryCell`/`Core` (service→endpoint keyed by owner peer, membership `evict` removes), `ServiceRegistry`/`Core` (durable log + replayable projection). Pure cores split from reactive cells; composes membership/lease/resilience.
- `src/resilience.rs` — fault-tolerance primitives (`#lzresilience`): `CircuitBreakerCell`/`Core` (Closed/Open/HalfOpen over a sliding failure window, gates `CommandTransport`), `RetryPolicyCell`/`Core` (exponential backoff `min(cap, base·2^attempt)`), `BulkheadCell`/`Core` (bounded isolation pool, `permits_in_use`), `TimeoutCell`/`Core` (deadline fast-fail). Pure cores split from reactive cells projecting state/delay/in_use/is_timed_out.
- `src/windowing.rs` — stream windowing (`#lzwindow`): `TumblingCountCore`/`Window` + `TumblingTimeCore`/`Window` (fixed non-overlapping), `SlidingCore`/`Window` (overlapping fold-recompute), `SessionCore`/`Window` (gap-based sessionization). Window aggregation reuses `MergePolicy` (`Sum`/`Max`/`SetUnion`) — the aggregate is the associative fold of window elements. Pure cores split from reactive cells projecting the last emitted aggregate.
- `src/presence.rs` — presence + ephemeral plane (`#lzpresence`): `Ephemeral`/`Durable` plane markers (durable sink statically rejects ephemeral — compile-fail doctest), `EphemeralCell`/`Core` (single value + auto-expiry), `PresenceCell` (per-peer heartbeat, membership `evict` + TTL) and `AwarenessCell` (last-writer-per-peer cursors/selections) over the shared `EphemeralMapCore`. Reactive cells project the live view/map; invalidate only on a live-view change.
- `src/coordination.rs` — distributed coordination (`#lzcoord`): `LeaseCore`/`LeaseCell` (single-writer authority + monotone fencing token), `LeaderCell`/`LeaderRole` (Leader/Follower/Candidate over a lease), `LockCell` (mutex + fencing `validate`), `SemaphoreCore`/`SemaphoreCell` (bounded permits), `BarrierCore`/`BarrierCell` (wait-for-N, `quorum()` = strict majority). Pure cores split from reactive cells projecting holder/role/is_locked/permits/is_open.
- `src/membership.rs` — membership + failure detection (`#lzmemb`): `PhiAccrual` (Akka-style bit-portable phi over a heartbeat inter-arrival window) + `MembershipCore<P>` SWIM state machine (`PeerState` Alive/Suspect/Dead/Left, `join`/`heartbeat`/`leave`/`tick`, `PeerChangeEvent` diff stream) split from the reactive `MembershipCell<P>` projecting the alive `PeerSet` onto a `Cell<BTreeSet<P>>` (invalidates only on set change). Generic peer id; distributed plane plugs in `PeerId`. Below the CRDT plane.
- `src/rateshape.rs` — rate-shaping source operators (`#lzrateshape`): the lifted `RatePolicy`/`WindowPolicy`/`ExpiryPolicy` (moved out of `relay_policy`; re-exported at crate top-level so relay semantics/API are unchanged) plus source-level operators over any cell handle — `DebounceCell`/`Core` (emit latest after quiet period), `ThrottleCell`/`Core` + `ThrottleEdge` (Leading/Trailing, one emit per window), `SampleCell`/`Core` + `SampleMode` (Count every-nth / Time every-boundary), `ProbabilisticSampleCell`/`Core` + `SampleRng`/`Lcg` (tail sampling `draw < rate`, the plan's only new algorithm). Pure cores split from cells projecting emit onto a `Computed<Option<T>>`; emit-only invalidation.
- `src/relay_policy.rs` — RelayCell Phase 6 (`#relaycell`): `PriorityStorage<T>` (max-priority, FIFO within priority) + `KeyedRelay<K,T,M>` (sharded relays by key). `RatePolicy`/`WindowPolicy`/`ExpiryPolicy` moved to `rateshape` (`#lzrateshape`). Logical-clock time for determinism.
- `src/relay_transport.rs` — RelayCell Phase 4 (`#relaycell`): the `Transport<T>` delivery seam (`deliver`/`poll`/`has_pending`) + `InProcTransport` (direct) and `FramedTransport` (MTU-style framing = CrossThread/Ipc/Ws). The merge algebra, not the transport, guarantees convergence (transport_independent).
- `src/queue.rs` — `QueueCell` (SPSC reactive FIFO + MPSC-via-`batch()` usage rule) + `QueueStorage` adapter trait + `VecDequeStorage` default backend (`#lzqueue`). Reader-kind invalidation (head/len/is_empty/is_full/closed); bounded reactive backpressure via `is_full`; closure lifecycle (drain / Closed-distinct-from-Empty / idempotent+terminal).
- `src/work_queue.rs` — `WorkQueueCell` competing-consumer local authority
  (`#lzworkqueue`): FIFO exclusive claims with stable item/fresh delivery ids,
  worker-owned ack/nack, strict visibility-timeout redelivery, bounded attempts,
  DLQ, and independent pending/in-flight/dead-letter reader kinds.
- `src/topic_core.rs` / `src/work_queue_core.rs` — the graph-agnostic cores every
  queue-family flavor shares (`#lazilythreadsafetopiccel`,
  `#lazilythreadsafeworkqueu`), the same split `keyed_order.rs` makes for the map
  family and for the same reason: the cursor/retention/GC algebra and the
  lease/ack/nack/redelivery/DLQ state machine touch no handle and await nothing.
  **Reactivity is deliberately excluded** — invalidation is a graph write, so
  every mutator returns *which readers changed* (a `Vec` of subscriber ids;
  `Transition::changed() -> ReaderChange`) and each flavor clears exactly that set
  on its own graph. `QueueStorage` already played this role for `QueueCell`
- `src/thread_safe_queue.rs` / `src/thread_safe_topic.rs` /
  `src/thread_safe_work_queue.rs` — the `Send + Sync` flavors (feature
  `thread-safe`). `Arc<Mutex<core>>` storage with invalidation run **outside** the
  lock (a reader's compute takes the context lock then the core lock, so an op
  that invalidated while holding the core inverts the order and deadlocks), and
  multi-root invalidation via `batch()` — `ThreadSafeContext` needs no
  `clear_slots` because `finish_batch` hands every collected root to one
  `clear_frontier_locked`
- `src/async_queue.rs` / `src/async_topic.rs` / `src/async_work_queue.rs` — the
  `AsyncContext` flavors (feature `async`). Reader kinds use
  `AsyncContext::computed` (synchronous compute, async graph), so `len()`,
  `read_stream()` and `pending_len()` return plain values rather than an `Option`
  needing a settle step: **nothing in the queue family is async-coloured**, and
  the work queue's clock stays a caller argument so lease expiry is deterministic
  and fixture-replayable. Multi-root invalidation via `AsyncContext::clear_slots`
- `src/ingress_core.rs` — the graph-agnostic admission algebra behind every
  ingress flavor (`#designimplementtransport`), the same split `topic_core.rs`
  makes for the broadcast family. Keyed lifecycle scopes (Opening/Live/Suspended/
  Closed), a normative admission order (lifecycle → generation fence → freshness →
  handoff → dedupe → ordering → backpressure → merge), a bounded reorder buffer, a
  coalescing hot window under `MergePolicy`, and a bounded three-channel receipt
  log with eviction-stable offsets. **Reactivity is deliberately excluded** — every
  mutator returns `IngressChange`, the set of dirtied reader kinds, and each shell
  clears exactly that set on its own graph. Also carries the `IngressTransport`
  seam and `InProcIngress`: the core never touches a transport, so a WebSocket
  frame, an RPC response, and a polled page are the same input once decoded.
  Freshness enters through an explicit `tick(now)`, so staleness transitions are
  deterministic and fixture-replayable
- `src/ingress.rs` / `src/thread_safe_ingress.rs` / `src/async_ingress.rs` — the
  three flavor shells (`#designimplementtransport`). Four reader kinds per scope
  (`value` / `readiness` / `authority` / `retry`) plus three receipt readers and a
  derived `IngressSchedule`; readiness, authority, and retry are **derives, not
  refresh calls**. The thread-safe shell runs invalidation outside the core lock
  and fans out through `batch()`; the async shell uses `AsyncContext::clear_slots`
  and `AsyncContext::computed` (sync compute, async graph) because **admission is
  not async-coloured** — an admission decision is a function of the fence, the
  watermark, the reorder buffer, and the observed clock, so there is nothing to
  await. Spec: `lazily-spec/docs/transport-ingress.md`; formal:
  `lazily-formal/LazilyFormal/Ingress.lean`
- `src/time.rs` — temporal source primitives (`#lztime`): logical-clock-driven `TimelineSource` cores (`TimerCore`/`IntervalCore`/`CronCore`/`DeadlineCore`) split from thin reactive cells (`TimerCell` single-shot / `IntervalCell` periodic / `CronCell` pattern-periodic / `DeadlineCell<T>` value+deadline → `Deadlined`), plus `ManualClock`. Edge-only reactive invalidation; `BytesPayload` cores (`DeadlineCell` is `PyObjectPayload`). Foundation for leases/expiry/windows/presence.
- `src/stdlib.rs` — Lazily standard-library conveniences layered over portable primitives rather than added to the graph kernel. `stdlib::Timer` binds logical `TimerCore` to Rust's monotone `Instant`; `Timeout<T>` adds caller-driven operation/cancellation polling, strict monotone deadlines, typed latched outcomes, and deterministic clock/wait seams without owning a future, executor, or thread; `RevisionBarrier` combines monotone revisions with derived predicates, barrier-owned cancellation, `Timer` deadlines, disposal, and application-owned keyed effect receipts while closing check-to-sleep lost wakeups.
- `src/transport.rs` — cross-process zero-copy transport (`#lzzcpy`): `BlobBackend` adapter trait + `InProcessBackend` (wraps `ShmBlobArena`) + `ArrowBackend` (Arrow IPC stream bytes) + `ShmBackend` (POSIX `shm_open`+`mmap`, `shm` feature, Linux) + `spill_message`/`resolve_value` policy + `BlobRouter` multi-backend resolver
- `src/crdt_tree.rs` — `CrdtTree` lossless document contract (`#lzcrdttree`): merge, frontier, delta, empty-frontier snapshot, and materialized value; implemented by `TextCrdt`
- `src/outbox.rs` — storage-independent durable outbox (`#lzdurableoutbox`): `OutboxStore` ordered-byte boundary, shared `Outbox<S>` append/ack/prune/replay protocol, in-memory backend, and `durable-sqlite` adapter
- `tests/ingress_family_conformance.rs` — the ingress contract
  (`#designimplementtransport`) replayed against **all three flavors** through one
  `IngressModel` trait, over `lazily-spec/conformance/ingress/*.json`: ordered
  delivery, reorder + both duplicate classes, reorder-window overflow,
  disconnect/replay, `Block` backpressure, build-skew generation handoff (including
  the handoff that *buffers*), freshness horizon and retry backoff. `invalidates`
  is asserted per reader kind in **both** directions via a cache-validity probe, so
  over-invalidation is as visible as under-. Carries a three-row ledger enforced by
  grepping `src/` in both directions, plus a mutation-check record of seven probes
- `tests/temporal_conformance.rs` — temporal sources (`#lztime`) compute fixtures (lazily-spec/conformance/temporal/`*.json`); timer single-shot idempotent fire, interval boundary counting under clock jumps, cron pattern matching, deadline expiry preserving value, edge-only reader invalidation
- `tests/common/mod.rs` — the runtime conformance manifest recorder
  (`#lazilyupgradeconformance`). Rust integration tests are separate crates, so
  the seam is a shared `mod common;` rather than a Go-style package-wide helper;
  `common::spec_read_to_string` wraps every fixture read in `tests/` and appends
  the corpus-relative id to `$LAZILY_CONFORMANCE_MANIFEST` (absolute, truncated
  once by `make check`, appended by every test binary). Unset ⇒ no-op, so a bare
  `cargo test` is unaffected. `scripts/check-conformance-coverage.sh` then fails
  on any canonical fixture the suite did not OPEN — and on a missing manifest,
  which is missing evidence rather than evidence of absence. The static grep it
  replaced could not tell a replayed fixture from a hand-transcribed one
- `tests/common/expect.rs` — the assertion-key consumption guard
  (`#lzassertunknownkeys`), one level below the manifest: having OPENED a
  fixture, did the runner actually CONSUME the keys it asserts? `Expect` wraps
  one `expected` / `expect` / `assertions` block, records every key the runner
  reads through it, and panics on drop naming any key left untouched — so a
  corpus key no binding implements fails loudly instead of being invisibly
  skipped (the defect found in lazily-kt, where `delta_zero_copy_arrow.json`'s
  `backend` discriminator was never read). Observational, not declarative: only a
  key really read counts. `Expect::sub` descends into nested assertion blocks
  (`invalidates`, `final_state`); `get` consumes data maps keyed by ids or peer
  names wholesale. `Expect::prose` marks documentation keys (`note`, `reason`);
  `Expect::declared_exception` marks a key whose capability this binding does not
  have, with a required reason at the call site. Self-tested in
  `tests/expect_guard.rs`
- `tests/integration.rs` — 13 integration tests
- `tests/spec_compliance.rs` — 68 spec compliance tests
- `tests/conformance.rs` — cross-language IPC fixture round-trip tests (lazily-spec/conformance)
- `tests/collections_family_conformance.rs` — the ordering/independence contract replayed against **all three execution models** (`SourceMap`, `ThreadSafeSourceMap`, `AsyncSourceMap`) via a `MapModel` trait whose only async-coloured method is `settle`. `collections_conformance.rs` covers the single-threaded flavor only, which is how the thread-safe/async ordering gap stayed invisible while `coverage.json` read green
- `tests/collections_conformance.rs` — keyed cell collections compute fixtures (lazily-spec/conformance/collections); value/membership/order independence, atomic move, LIS reconciliation, memoized semantic tree, manufactured text identity, character CRDT convergence
- `tests/materialization_conformance.rs` — `ComputedMap` materialization (`#reactivemap`) compute fixtures (lazily-spec/conformance/materialization/`*.json`); observational transparency eager (pre-mint) vs lazy (`get_or_insert_with`), deferral-not-deallocation present-set monotonicity, entry-kind orthogonal to strategy (input cells always materialized / derived slots deferred under lazy)
- `tests/materialization_threadsafe_conformance.rs` — same materialization fixtures replayed through `ThreadSafeComputedMap` (feature-gated `thread-safe`); proves the `Send + Sync` flavor obeys the shared laws plus materialization confluence (order-independent present set + observed values)
- `tests/materialization_async_conformance.rs` — same materialization fixtures replayed through `AsyncComputedMap` (feature-gated `async`, tokio); present-set laws + eventual transparency (a driven async slot resolves to the canonical value, eager ≡ lazy)
- `tests/relay_examples.rs` — RelayCell Phase 7 (`#relaycell`) example systems as integration tests: §7.2 telemetry pipeline (Sum relay → SpillStore(AppendCompact) → rate-paced batch egress, lossless); §7.4 doc-sync (per-cell KeepLatest KeyedRelay plane converges per cell); §7.1 broadcast (per-subscriber Outbox<KeepLatest> conflation)
- `tests/relay_roles.rs` — RelayCell Phase 5 (`#relaycell`) spike: Outbox state-conflation + Block producer-backpressure; Inbox credit metering; Outbox→Inbox link convergence
- `tests/relay_policy.rs` — RelayCell Phase 6 (`#relaycell`) spike: RatePolicy token bucket; WindowPolicy flush-on-fill/tick + converged-sum preserved; ExpiryPolicy TTL drop; PriorityStorage ordering; KeyedRelay sharding (regression guard for the `#lzrateshape` policy lift — passes unmodified)
- `tests/service_conformance.rs` — embedded-service (`#lzservice`) fixtures (lazily-spec/conformance/service/`*.json`): health aggregation, readiness gating, discovery register/evict, durable registry replay; reader invalidation
- `tests/resilience_conformance.rs` — fault-tolerance (`#lzresilience`) fixtures (lazily-spec/conformance/resilience/`*.json`): circuit-breaker trip/probe/close, retry exponential saturation, bulkhead bounds, timeout deadline edge; reader invalidation
- `tests/windowing_conformance.rs` — stream windowing (`#lzwindow`) fixtures (lazily-spec/conformance/windowing/`*.json`): tumbling count/time, sliding, session windows with Sum aggregate; emit-only invalidation
- `tests/presence_conformance.rs` — presence/ephemeral (`#lzpresence`) fixtures (lazily-spec/conformance/presence/`*.json`): presence heartbeat/evict/TTL, awareness last-writer, ephemeral value expiry; live-view invalidation
- `tests/coordination_conformance.rs` — coordination (`#lzcoord`) fixtures (lazily-spec/conformance/coordination/`*.json`): lease grant/renew/expire + fence monotonicity, leader handover, lock fencing validate, semaphore bounds, quorum majority gate; reader invalidation
- `tests/reactive_graph_conformance.rs` — reactive-graph disposal/teardown (`#lzspecedgeindex`) fixtures (lazily-spec/conformance/reactive-graph/`*.json`), replayed against **all three execution models** (`Context`, `ThreadSafeContext`, `AsyncContext`) via the `GraphModel` trait in `tests/reactive_graph/`: edge detach in both directions, read-after-dispose, churn returns to baseline, recycled-id cleanliness, scope teardown vs the fold of individual disposals, `disarm()`, cross-scope teardown hazard. First executor of that corpus in the family; carries a per-model `KNOWN_DIVERGENCES` ledger asserted in both directions
- `tests/membership_conformance.rs` — membership (`#lzmemb`) lifecycle fixture (lazily-spec/conformance/membership/`*.json`): SWIM join→Alive/heartbeat/leave, phi gap→Suspect→Dead timeout, PeerSet invalidation only on set change
- `tests/rateshape_conformance.rs` — rate-shaping operators (`#lzrateshape`) compute fixtures (lazily-spec/conformance/rateshape/`*.json`): debounce quiet-period emit, throttle leading/trailing, count/time sampling, probabilistic draw<rate; emit-only reader invalidation
- `tests/relay_transport.rs` — RelayCell Phase 4 (`#relaycell`) spike: converged egress independent of transport framing (operational `transport_independent`) across InProc vs Framed at several MTUs, for Sum/Max/KeepLatest; framed transport preserves the op stream
- `tests/relay_spill.rs` — RelayCell Phase 3 (`#relaycell`) spike: `spill_lossless` (reconstruct cold pages + hot = flat fold, both modes); `spill_replay_idempotent` (Max/SetUnion crash-replay converges); CompactOnWrite page bounding; ack-before-reclaim; RelayCell Spill-overflow → SpillStore end-to-end
- `tests/relay_core.rs` — RelayCell Phase 2 (`#relaycell`) spike: converged-egress independent of drain schedule (operational `relay_converges`) across Sum/Max/KeepLatest; Block/DropNewest/DropOldest/Conflate overflow behaviour; reactive `depth`/`is_full`/`is_empty`; construction rejects Conflate for RawFifo
- `tests/merge_conformance.rs` — RelayCell Phase 1 (`#relaycell`) cross-language fixture replay (lazily-spec/conformance/collections/`mergecell_algebra.json`); KeepLatest/Sum/Max per-op converged value + invalidation (idempotent/identity no-op), fixture flags vs policy `const`s
- `tests/merge_laws.rs` — RelayCell Phase 1 (`#relaycell`) property-based law-tests: every `MergePolicy` is associative; commutativity/idempotency asserted per `const` flag (and flag-honesty counterexamples); `Cell ≡ MergeCell<KeepLatest>`, converged-state determinism regardless of op order, idempotent-`⊕` free dedup via the `PartialEq` store-guard, `Reactive`/`Source` supertype uniformity
- `tests/queue_family_conformance.rs` — the queue family's per-flavor gate: all
  eleven canonical fixtures (`queuecell_*` / `topiccell_*` / `workqueue_*`)
  replayed against **all three flavors** through one `TopicModel` /
  `WorkQueueModel` trait, plus a nine-row ledger (3 primitives × 3 flavors)
  enforced by grepping `src/` in both directions so a shipped flavor cannot sit
  unreplayed. Every gate was mutation-checked. Note the topic fixtures were
  previously opened by nothing — `topic_conformance.rs` hand-transcribes the same
  scenarios as Rust asserts, which is coverage that cannot detect drift
- `tests/queue_conformance.rs` — reactive queue (`QueueCell`) compute fixtures (lazily-spec/conformance/collections/`queuecell_*.json`); SPSC total FIFO, popped-head reader-kind independence, MPSC multi-writer inside `batch()`, bounded reactive backpressure (`is_full`), closure lifecycle
- `tests/work_queue_conformance.rs` — canonical `workqueue_*.json` replay:
  exclusive competing delivery, ownership rejection, at-least-once lease
  redelivery, and poison routing to the DLQ
- `tests/seqcrdt_conformance.rs` — move-aware sequence CRDT compute fixture (lazily-spec/conformance/collections/seqcrdt_convergence.json); concurrent-insert/move/value-edit convergence, tombstone commutativity (feature-gated, needs `distributed`)
- `tests/schema_compliance.rs` — lazily-rs serde output validates against lazily-spec JSON Schemas (#lzspecschema)
- `tests/command_conformance.rs` — command/RPC message plane (`command-plane-v1`) fixture replay (lazily-spec/conformance/message-passing); projection reducer + RPC facade terminal-only rule (feature-gated `ipc`)
- `tests/signal.rs` — 19 eager-Signal integration tests (single-threaded + thread-safe)
- `tests/tokio_sync.rs` — 2 Tokio feature-gated sync integration tests (requires `tokio` + `thread-safe`)
- `tests/async_integration.rs` — AsyncContext feature-gated integration tests (incl. eager `signal_async`)

## Key Design Decisions

- **Lazy by default, eager when asked:** Slots mark dirty on invalidation and recompute on access; `ctx.signal()` opts into eager recomputation (guarded computed cell + puller-effect) with no intermediate unset value (`v1 -> v2`)
- **PartialEq guard:** `Cell.set()` only invalidates when value actually changes
- **Guarded computed (`#lzcellkernel`):** every `ctx.computed()` cell is guarded — `T: PartialEq`, and a recompute that yields an equal value keeps downstream caches (no version bump). There is **no unguarded mode**; the former `memo` constructor is retired because `computed` now *is* the guarded form (`slot()` remains the bound-free storage-sense primitive)
- **Dynamic dependencies:** Edges re-discovered on each recomputation (no stale subscriptions)
- **RefCell interior mutability:** Single-threaded by design
- **Explicit thread safety:** `ThreadSafeContext` preserves `Context`'s fast path while adding `Send + Sync` shared graph support

## Commands

```bash
make check           # Run fmt, clippy, build, every Rust feature test, both Lean formal models (lazily-spec + lazily-formal), and benchmark result checks
make test-thread-safe  # ThreadSafeContext + ThreadSafeStateMachine (feature-gated since v0.18.0)
make test-tokio      # Tokio sync integration tests (requires tokio + thread-safe)
make test-async      # AsyncContext integration tests
make test-loom       # Run thread-safe Loom model tests
make test-lean-formal    # Build ../lazily-spec/formal/lean with lake
make test-lazily-formal  # Build ../lazily-formal with lake (full Harel chart + reactive graph + collections/tree/reconciliation/async proofs)
make test-seqcrdt-conformance  # Replay ../lazily-spec/conformance/collections/seqcrdt_convergence.json (needs --features distributed)
make test-queue-conformance   # Replay ../lazily-spec/conformance/collections/queuecell_*.json (needs --features serde)
make benchmark-evidence # Quick gating measurement (~1 min): reduced-sample Criterion over the budgeted groups + instrumentation profile + source fingerprint
make benchmark-evidence-full # Full-fidelity measurement (tens of minutes); backs BENCHMARKS.md wall-clock numbers
make benchmark-check # Enforce instrumentation budgets against that evidence
make benchmark-update # Run python3 scripts/update-benchmark-results.py to regenerate BENCHMARKS.md
make instrumentation-profile # Run examples/instrumentation_profile.rs with --features instrumentation
```

### Benchmark budgets are never "skipped"

`make check` runs `benchmark-evidence` then `benchmark-check`, so the budgets are
measured against the tree in front of you every time. `benchmark-check` on its
own has no skip path and three outcomes:

| exit | meaning |
| --- | --- |
| 0 | budgets measured against THIS source tree and green |
| 2 | no evidence — nothing was measured, so nothing can be green |
| 3 | evidence is stale — it measured a different source tree |

Staleness is a content hash (`target/lazily-benchmark-evidence.json`) over
`src/`, `benches/`, `macros/src/`, `examples/instrumentation_profile.rs`, and the
manifests — not an mtime, because `git checkout`, `touch`, and restoring a backup
all move mtimes without any relationship to whether the code changed. Regenerate
with `make benchmark-evidence`; deleting the evidence is not a way to turn a red
budget green.

## Benchmark Skill

Use `/lazily-benchmark` to check, update, or run A/B regression workflows for `BENCHMARKS.md`. See [runbooks/lazily-benchmark.md](../../runbooks/lazily-benchmark.md) for the full workflow.

## Related Projects

- `lazily-zig` — Zig counterpart with FFI, thread-safe mutex
- `lazily-py` — Python counterpart with context-as-dict model


## Library Context Policy

This library follows the agent-loop library-context policy. Contributors
authoring `AGENTS.md`, `SKILL.md`, or runbooks in this repo must read:

[Library Context Policy](../instruction-files/LIBRARY_CONTEXT_POLICY.md)

before making changes.

<!-- tsift:code-navigation v=0.1.77 -->
## Code Navigation

Keep this block self-contained for Codex/OpenCode prompt reuse. If this repository also ships current `.claude/skills/tsift/SKILL.md` or `runbooks/code-navigation.md`, use those deeper runbooks for command detail instead of expanding this block.

Run `tsift status` at session start from the owning repo root. If the task or file lives under a git submodule (for example `src/tsift/...`), switch to that submodule root first so the harness loads the narrower local instructions and repo state instead of the superproject root. If status prints a `run:` recommendation for stale or missing tsift state, run `tsift status --fix` before relying on tsift results; when the harness cannot perform write commands, ask the user to run the printed command instead. Codex projects can install a prompt-time auto-reindex hook with `tsift init --codex`; OpenCode projects can install per-project tsift command shortcuts with `tsift init --opencode`.

Use the commands listed in its `use:` output:
- `tsift --envelope source-read <file> --budget normal` — AST-symbol projection with span metadata and source-window expansion commands (prefer over cat/head for source code files)
- `tsift --envelope symbol-read <symbol> --budget normal` — token-budgeted symbol body, AST span metadata, child refs, and graph/source expansion commands
- `tsift --envelope search <query> --budget normal` — AST-aware hybrid search preview (prefer over grep/rg)
- `tsift --envelope explain <symbol> --budget normal` — callers, callees, community preview
- `tsift graph <symbol> --callers` / `--callees` — call graph navigation
- `tsift summarize <symbol>` — cached summary (only when listed in `use:`)
- `tsift workflow search` — ordered exact/search/explain/summarize/digest recipe that preserves result handles across expansions

When a search envelope includes `report.scale_guard`, run one of its `narrow_commands` before dispatching parallel agents. The guard means the original result set or corpus is broad enough that fan-out should start from a narrower cited handle, path, or exact query.

Prefer bounded digest commands over raw transcript, diff, and verbose-log reads:
- `tsift --envelope session-review <path> --next-context --budget normal` or `tsift --envelope context-pack <path> --budget normal` instead of replaying long session docs, JSONL transcripts, or agent-doc runtime logs with `cat`, `tail`, or `sed`.
- `tsift diff-digest [path]` (`--cached`, `--revision <rev>`) instead of `git diff`, `git show`, or patch-style `git log`.
- `tsift --envelope digest-runner --kind test --path . --shell-command '<test command>'` / `tsift --envelope digest-runner --kind log --path . --shell-command '<build command>'` for noisy test/build/install output, or let the rewrite/hooks create those artifact-backed envelopes for `cargo test`, `pytest`, and verbose cargo commands.
- If RTK is installed, digest-runner delegates supported generic command families through `rtk rewrite` and records the chosen compact filter in `report.filter` while preserving tsift artifact handles.
- Codex, OpenCode, and other harnesses without Claude-style `PreToolUse` hooks should run `tsift rewrite --run '<command>'` before broad `rg`/recursive grep, raw transcript/session/log reads, `git diff`/`git show`/single-patch `git log`, `cargo test`/`pytest`, and cargo build/check/clippy/install commands so the same search, session-digest, diff-digest, and digest-runner rewrites apply manually. OpenCode can install this path as `/tsift-rewrite-run` with `tsift init --opencode`.

For local verification, run `make check` before committing. After local changes, check the latest GitHub Actions CI run with `gh run list --workflow CI --limit 1` and fix any failing tests before calling the work complete.

Only read full source files when tsift results are insufficient.
<!-- /tsift:code-navigation -->
