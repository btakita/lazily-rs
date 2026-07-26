.RECIPEPREFIX := >

CARGO ?= cargo
PYTHON ?= python3
LAKE ?= lake
LEAN_SPEC_DIR ?= ../lazily-spec/formal/lean
LEAN_FORMAL_DIR ?= ../lazily-formal

# Runtime conformance manifest (#lazilyupgradeconformance). The path is ABSOLUTE
# and EXPORTED to every recipe: `check` runs a dozen separate `cargo test`
# invocations over different feature sets, each spawning several test binaries,
# and a relative path would scatter partial manifests instead of accumulating one
# union. Each recorded read APPENDS (tests/common/mod.rs); the file is truncated
# exactly once, by `conformance-manifest-reset`, which `check` runs FIRST. That
# ordering is a serial-make assumption — do not run `make check -j`.
CONFORMANCE_MANIFEST ?= $(CURDIR)/build/conformance-fixtures-loaded.txt
export LAZILY_CONFORMANCE_MANIFEST = $(CONFORMANCE_MANIFEST)

.PHONY: \
	conformance-coverage \
	conformance-manifest-reset \
	check \
	fmt \
	clippy \
	build \
	build-ffi \
	ffi-headers \
	test \
	test-thread-safe \
	test-tokio \
	test-async \
	test-async-resolve \
	test-loom \
	test-distributed \
	test-crdt-plane \
	test-interop-peer \
	test-distributed-conformance \
	test-ffi \
	test-ffi-binary \
	test-ipc \
	test-ipc-binary \
	test-ipc-conformance \
	test-reliable-sync-conformance \
	test-collections-conformance \
	test-collections-family-conformance \
	test-queue-family-conformance \
	test-queue-conformance \
	test-queue-demand-driven \
	test-seqcrdt-conformance \
	test-lossless-tree \
	test-schema-compliance \
	test-statechart-conformance \
	test-shm \
	test-lean-formal \
	test-lazily-formal \
	test-signaling-client \
	test-webrtc \
	test-websocket \
	benchmark-check \
	benchmark-check-strict \
	benchmark-update \
	instrumentation-profile

	check: conformance-manifest-reset fmt clippy build test test-thread-safe test-tokio test-async test-async-resolve test-loom test-distributed test-crdt-plane test-interop-peer test-distributed-conformance test-ffi test-ffi-binary test-ipc test-ipc-binary test-ipc-conformance test-reliable-sync-conformance test-shm test-collections-conformance test-collections-family-conformance test-queue-family-conformance test-queue-conformance test-queue-demand-driven test-seqcrdt-conformance test-lossless-tree test-schema-compliance test-statechart-conformance test-lean-formal test-lazily-formal test-signaling-client test-webrtc test-webrtc-signaling test-websocket benchmark-check conformance-coverage

fmt:
>$(CARGO) fmt --all --check

clippy:
>$(CARGO) clippy --locked --all-targets --all-features -- -D warnings

build:
>$(CARGO) build --locked --all-targets --all-features

build-ffi:
>$(CARGO) build --locked --features ffi

ffi-headers: build-ffi
>cbindgen --config cbindgen.toml --crate lazily -o target/lazily.h

test:
>$(CARGO) test --locked

# ThreadSafeContext + ThreadSafeStateMachine (feature-gated behind `thread-safe`
# since v0.18.0; lazily-spec requires this layer conditionally — see
# protocol.md § "Concurrency layers are required").
test-thread-safe:
>$(CARGO) test --locked --features thread-safe

# tokio_sync.rs + benches/tokio_sync.rs require BOTH tokio and thread-safe.
test-tokio:
>$(CARGO) test --locked --features "tokio thread-safe"

test-async:
>$(CARGO) test --locked --features async

# Deterministic #k03k resolve-loop window coverage needs the instrumentation
# seam (window 1) alongside the async feature; test-async alone compiles it out.
test-async-resolve:
>$(CARGO) test --locked --features "async instrumentation" --test async_resolve_loop

# Phase-0 demand-driven reader-kind + store-without-cascade acceptance
# (relaycell-backpressure-analysis.md §5/§4.0): asserts the merge cost law via
# instrumentation counters — unobserved ops derive nothing, bursts coalesce.
test-queue-demand-driven:
>$(CARGO) test --locked --features instrumentation --test queue_demand_driven

test-loom:
>$(CARGO) test --locked --features loom --test thread_safe_loom

test-distributed:
>$(CARGO) test --locked --features "distributed serde"

# Distributed CRDT plane runtime integration (#lzcrdtplane5b): the
# CrdtPlaneRuntime glue + the end-to-end two-replica-over-transport convergence
# test need BOTH the plane primitives (`distributed`) and the wire + in-memory
# DataChannel transport (`webrtc`), a combo no other target exercises.
test-crdt-plane:
>$(CARGO) test --locked --features "distributed webrtc"

test-interop-peer:
>$(CARGO) run --locked --quiet --features "distributed webrtc" --bin lazily-interop-peer -- --self-check

# Canonical distributed conformance (#verifycrdtplaneruntimein): the ingest
# op-count contract — a SUPERSEDED op still counts, because it entered the log;
# only redelivery of an already-logged op applies zero. lazily-cpp counted ops
# that changed the winner instead and reported 4 of 5. rs is correct (its log
# sorts and counts on insertion) but had no runner at all, so the same slip
# would have gone unnoticed. Named explicitly rather than left riding on
# test-crdt-plane's broader feature run, so it is visible in `check`.
test-distributed-conformance:
>$(CARGO) test --locked --features "distributed webrtc" --test distributed_conformance

test-ffi:
>$(CARGO) test --locked --features ffi --test ffi

test-ffi-binary:
>$(CARGO) test --locked --features "ffi ipc-binary" --test ffi

test-ipc:
>$(CARGO) test --locked --features ffi --test ipc

test-ipc-binary:
>$(CARGO) test --locked --features ipc-binary --test ipc

test-ipc-conformance:
>$(CARGO) test --locked --features ipc --test conformance

# Reliable sync (#lzsync): ResyncCoordinator / DurableOutbox / OR-set-LWW
# liveness + the ResyncRequest/OutboxAck control-frame codec round-trip. Replays
# ../lazily-spec/conformance/reliable-sync/ (msgpack pin needs ipc-msgpack).
test-reliable-sync-conformance:
>$(CARGO) test --locked --features ipc,ipc-msgpack --test reliable_sync_conformance
>$(CARGO) test --locked --features ipc,ipc-msgpack --lib reliable_sync::

# Cross-process zero-copy transport (#lzzcpy): BlobBackend trait +
# InProcessBackend / ArrowBackend + POSIX ShmBackend (shm feature). The lib
# unit tests cover spill/resolve/router + the shm fork() cross-process smoke.
test-shm:
>$(CARGO) test --locked --features ipc,shm --lib transport::

# Keyed cell collections conformance (#lzcellfamily / #lzkeyrecon): lazily-rs
# replays the canonical compute fixtures in lazily-spec/conformance/collections/
# — value / set-membership / order reactivity independence, atomic ordered move
# (handle_stable), and LIS move-minimized reconciliation. Required of every
# binding (see the Binding Conformance Matrix). Collections are unconditional, so
# this target needs no feature flags.
test-collections-conformance:
>$(CARGO) test --locked --test collections_conformance

# The SAME ordering contract replayed against ALL THREE flavors. It needs BOTH
# `thread-safe` and `async` (plus tokio), so no partial-feature target compiles
# it: under the default features, `--features thread-safe`, or `--features async`
# alone, `cargo test --test collections_family_conformance` reports
# "running 0 tests" and exits 0. A gate that silently runs nothing is the exact
# failure this suite exists to prevent, so it gets its own all-features target
# rather than riding on someone else's feature set.
test-collections-family-conformance:
>$(CARGO) test --locked --all-features --test collections_family_conformance

# The queue-family flavor ledger (#lzqffx). Enforced, not advisory: it greps src/
# for each unshipped flavor's type name, so the moment a ThreadSafeQueueCell or
# AsyncQueueCell appears this goes red and names the runner to extend. Wired into
# `check` on purpose — the collections gate spent its whole life compiling to
# "running 0 tests" because no target ran it.
# Needs `thread-safe`: the thread-safe flavor replays are behind that cfg, so the
# featureless spelling compiles those modules out and reports a fraction of the
# suite — a gate quietly running half of itself. BOTH flavors are cfg-gated, so
# both features are required. Counts, verified rather than inferred from an exit
# code that is 0 in every case: 6 with neither feature, 16 with thread-safe
# alone, 13 with async alone, 23 with both. If this number drops, a flavor
# stopped being replayed.
test-queue-family-conformance:
>$(CARGO) test --locked --features "thread-safe async" --test queue_family_conformance

# Reactive queue conformance (#lzqueue): lazily-rs replays the canonical compute
# fixtures in lazily-spec/conformance/collections/ `queuecell_*.json` — SPSC
# total FIFO, popped-head observation (reader-kind independence), MPSC
# multi-writer inside batch(), bounded reactive backpressure (is_full), and the
# closure lifecycle. Required of every binding (see the Binding Conformance
# Matrix). The `serde` feature is enabled for the VecDequeStorage wire-shape test.
test-queue-conformance:
>$(CARGO) test --locked --features serde --test queue_conformance

# Move-aware sequence CRDT conformance (#lzseqcrdt): lazily-rs replays the
# canonical compute fixture in lazily-spec/conformance/collections/
# `seqcrdt_convergence.json` — concurrent-insert convergence, single-LWW move
# (no duplication), concurrent move + value-edit independence, tombstone
# convergence + commutative merge. SeqCrdt is feature-gated behind `distributed`
# (the CRDT plane), so this target needs that feature.
test-seqcrdt-conformance:
>$(CARGO) test --locked --features distributed --test seqcrdt_conformance

# Lossless full-document tree CRDT (#lzlosstree): M1 syntax-agnostic core. Replays
# the shared compute fixtures in lazily-spec/conformance/lossless-tree/ (exact
# round-trip, one-leaf edit delta, split/merge, concurrent insert, concurrent
# reorder + edit, non-contiguous anti-entropy, token/trivia preservation, invalid
# source round-trip, structural-conflict text preservation) plus randomized
# convergence property tests, plus schema compliance of the `TreeUpdate` /
# frontier serde output against lazily-spec's lossless-tree schemas (needs
# `serde`). Feature-gated behind `lossless-tree` (which implies `distributed`).
#
# `crdt_tree_laws` is named here because it was named NOWHERE: it is
# `#![cfg(feature = "lossless-tree")]`, no other target enables that feature, and
# this target enumerates its test binaries explicitly — so the whole file
# compiled to nothing under `make check` and its replay of
# `crdt-tree/algebra.json` never happened. The static coverage grep could not see
# that (the filename was right there in the source); the runtime manifest named
# it on the first run (#lazilyupgradeconformance).
test-lossless-tree:
>$(CARGO) test --locked --features "lossless-tree serde" --test lossless_tree_conformance --test lossless_tree_proptest --test lossless_tree_schema --test crdt_tree_laws

# JSON Schema compliance: lazily-rs's own serde output (Snapshot/Delta/CrdtSync,
# incl. NodeKey) validates against the sibling lazily-spec/schemas, and every IPC
# conformance fixture's `wire` is schema-valid. Closes the binding<->schema loop.
test-schema-compliance:
>$(CARGO) test --locked --features ipc --test schema_compliance

test-statechart-conformance:
>$(CARGO) test --locked --features statechart-json --test statechart_conformance

test-lean-formal:
>test -d "$(LEAN_SPEC_DIR)" || { echo "missing $(LEAN_SPEC_DIR); clone lazily-spec as a sibling or set LEAN_SPEC_DIR"; exit 1; }
>cd "$(LEAN_SPEC_DIR)" && $(LAKE) build

# Build the full Harel state-chart formal model + the new universal proofs
# (parallel_region_confluence, single_region_refines_flat_machine) in
# lazily-formal — the neutral formal-artifact home every binding depends on.
test-lazily-formal:
>test -d "$(LEAN_FORMAL_DIR)" || { echo "missing $(LEAN_FORMAL_DIR); clone lazily-formal as a sibling or set LEAN_FORMAL_DIR"; exit 1; }
>cd "$(LEAN_FORMAL_DIR)" && $(LAKE) build

test-signaling-client:
>$(CARGO) test --locked --features signaling-client

# WebRTC DataChannel transport (#webrtc2/#webrtc3) + concrete str0m backends:
# the deterministic in-memory/synthetic-clock loopback (#webrtcbackend) plus the
# networked Str0mNet backend (#lzwebrtcnet), whose test does a real two-socket
# round trip over 127.0.0.1 (real UDP/DTLS/SCTP/timers).
test-webrtc:
>$(CARGO) test --locked --features webrtc-str0m

# Full WebRTC handshake driven THROUGH SignalingClient over a loopback signaling
# relay (#lzwebrtcwire): real WebSocket offer/answer/ICE on 127.0.0.1 plus the
# real Str0mNet UDP/DTLS/SCTP transport. Needs both feature trees.
test-webrtc-signaling:
>$(CARGO) test --locked --features "signaling-client webrtc-str0m" --test webrtc_signaling

# WebSocket DataChannel backend (#akp3): in-process loopback over a real WS
# handshake, no real network.
test-websocket:
>$(CARGO) test --locked --features websocket

# Reuses whatever `target/criterion` already holds. When that directory is ABSENT
# (fresh clone, pruned target) it prints a loud "no budget was enforced" banner and
# passes, because hard-failing there made `make check` unreachable for a reason
# that has nothing to do with the code — and a gate that is always red stops being
# read. The skip is narrow on purpose: a directory that EXISTS but yields no
# estimates is a broken bench run, not a fresh checkout, and still fails, so
# deleting it is not a way to turn a red budget green.
benchmark-check:
>$(PYTHON) scripts/update-benchmark-results.py --check

# The enforcing spelling: refresh the deterministic profile and hard-fail missing
# Criterion evidence or budget regressions. Machine-specific wall-clock estimates
# remain report data, not CI thresholds; the scheduled regressions workflow runs
# the benches first, then invokes this target on a pinned runner image.
benchmark-check-strict:
>$(PYTHON) scripts/update-benchmark-results.py --check --require-evidence --refresh-profile --budgets-only

benchmark-update:
>$(PYTHON) scripts/update-benchmark-results.py

instrumentation-profile:
>$(CARGO) run --example instrumentation_profile --features "instrumentation thread-safe" --quiet

# Truncate the manifest before the suite. Run FIRST by `check`; every recorded
# read appends, so without this the file would union across runs and a fixture
# that stopped being replayed would stay "covered" forever.
conformance-manifest-reset:
>@mkdir -p $(dir $(CONFORMANCE_MANIFEST))
>@: > $(CONFORMANCE_MANIFEST)

# Conformance-coverage guard (#portconformancecoverage). RUNTIME
# (#lazilyupgradeconformance): fails when a canonical fixture was not OPENED by
# the suite, and fails when the manifest is absent, because that is missing
# evidence rather than evidence of absence. See the script header.
conformance-coverage:
>./scripts/check-conformance-coverage.sh
