#!/usr/bin/env python3
"""Refresh the generated benchmark results section in BENCHMARKS.md."""

from __future__ import annotations

import argparse
import csv
import datetime as _datetime
import hashlib
import json
import math
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback.
    tomllib = None


START_MARKER = "<!-- benchmark-results:start -->"
END_MARKER = "<!-- benchmark-results:end -->"
INSERT_BEFORE = "\n## Multi-Language\n"
BENCHMARKS_INSERT_BEFORE = "\n## Multi-Language\n"
DEFAULT_PROFILE_OUTPUT = Path("target/lazily-instrumentation-profile.csv")
DEFAULT_EVIDENCE_MANIFEST = Path("target/lazily-benchmark-evidence.json")
EVIDENCE_MANIFEST_VERSION = 1

# #vnmr: what the benchmark evidence is a measurement OF.
#
# The staleness signal is a content hash of these inputs, not an mtime. mtime is
# not a property of the code: `git clone`/`git checkout` stamp "now" on files
# that did not change, restoring a backup moves a timestamp BACKWARDS while the
# bytes differ, and `touch` moves it forward while the bytes do not. Any of those
# makes an mtime comparison answer a question nobody asked. A content hash
# answers the only question the gate cares about — "were these numbers produced
# by the code that is checked out right now?" — and it answers it identically on
# a fresh clone, a dirty worktree, and a CI runner.
#
# It is also preferred over a recorded git commit, because a dirty worktree is
# the normal state while developing: a commit id would call every uncommitted
# edit "current" (the exact rot this gate exists to catch) or, if it compared
# against a dirty flag, call every uncommitted tree "stale" regardless of whether
# the edit could move a counter.
#
# The set is deliberately the measured surface plus its build inputs. `tests/`
# and `docs/` are excluded: they cannot change a benchmark number, and including
# them would invalidate evidence on every test edit, which is how a freshness
# gate trains people to regenerate reflexively instead of reading it.
EVIDENCE_SOURCE_GLOBS: tuple[str, ...] = (
    "src/**/*.rs",
    "benches/**/*.rs",
    "macros/src/**/*.rs",
    "examples/instrumentation_profile.rs",
    "Cargo.toml",
    "Cargo.lock",
    "macros/Cargo.toml",
)

# Exit codes. `make check` only reads zero/non-zero, but the three non-zero modes
# are distinct so a log or a wrapper can tell "never measured" from "measured the
# wrong tree" from "measured, and red".
EXIT_BUDGET_REGRESSION = 1
EXIT_EVIDENCE_ABSENT = 2
EXIT_EVIDENCE_STALE = 3

GROUP_ORDER = {
    "cached_reads": 0,
    "cold_first_get": 1,
    "dependency_fan_out": 2,
    "set_cell_invalidation": 3,
    "memo_equality_suppression": 4,
    "effect_flushing": 5,
    "batch_storms": 6,
    "thread_safe_contention": 7,
    "thread_safe_effect_contention": 8,
    "thread_safe_graph_propagation": 9,
    "profile_instrumentation": 10,
    "async_cached_resolve": 11,
    "async_cold_resolve": 12,
    "async_invalidation_throughput": 13,
    "async_cancellation_throughput": 14,
    "async_concurrent_contention": 15,
    "async_effect_throughput": 16,
    "async_batch_throughput": 17,
    "tokio_sync_cached_read": 18,
    "tokio_sync_cold_first_get": 19,
    "tokio_sync_invalidation": 20,
    "tokio_sync_concurrent_contention": 21,
    "tokio_sync_batch": 22,
    "tokio_sync_effect": 23,
    # #lzscalebench: >=1M-node scale group (feature-gated `scale-bench`).
    "scale": 24,
}

# #lzscalecompare: criterion groups that must NOT appear in the auto-generated
# results table. `scale_compare` is the cross-library head-to-head (lazily vs
# leptos_reactive) documented manually in BENCHMARKS.md prose; its estimates land
# in `target/criterion` when the comparison bench runs, but they are not a tracked
# lazily benchmark, so the generator skips them (keeps `benchmark-check` green).
EXCLUDED_GROUPS = {"scale_compare"}
SET_CELL_INVALIDATION_CASE_ORDER = {
    "high_fan_out": 0,
    "same_slot_contention": 1,
    "independent_slot_contention": 2,
    "batched_write_bursts": 3,
}
THREAD_SAFE_CONTENTION_CASE_ORDER = {
    "same_slot_write_read": 0,
    "independent_slots": 1,
    "read_mostly_waiters": 2,
    "batched_write_bursts": 3,
}
THREAD_SAFE_EFFECT_CONTENTION_CASE_ORDER = {
    "queue_coalescing": 0,
    "cleanup_execution": 1,
    "batch_flush": 2,
}
THREAD_SAFE_GRAPH_PROPAGATION_CASE_ORDER = {
    "fan_out_eager_validation": 0,
    "fan_out_lazy_dirty_epochs": 1,
    "fan_in_lazy_dirty_epochs": 2,
    "fan_in_batched_flush": 3,
}
ASYNC_CONCURRENT_CONTENTION_CASE_ORDER = {
    "async_context": 0,
    "thread_safe_context_baseline": 1,
}
TOKIO_SYNC_CONCURRENT_CONTENTION_CASE_ORDER = {
    "same_slot_write_read": 0,
    "independent_slots": 1,
}
REQUIRED_LATENCY_CASES: tuple[tuple[str, str], ...] = (
    ("thread_safe_contention", "same_slot_write_read / 8"),
    ("thread_safe_contention", "same_slot_write_read / 16"),
    ("thread_safe_contention", "independent_slots / 8"),
    ("thread_safe_contention", "independent_slots / 16"),
    ("thread_safe_contention", "read_mostly_waiters / 8"),
    ("thread_safe_contention", "read_mostly_waiters / 16"),
    ("thread_safe_contention", "batched_write_bursts / 8"),
    ("thread_safe_contention", "batched_write_bursts / 16"),
    ("thread_safe_effect_contention", "queue_coalescing / 8"),
    ("thread_safe_effect_contention", "queue_coalescing / 16"),
    ("thread_safe_effect_contention", "cleanup_execution / 8"),
    ("thread_safe_effect_contention", "cleanup_execution / 16"),
    ("thread_safe_effect_contention", "batch_flush / 8"),
    ("thread_safe_effect_contention", "batch_flush / 16"),
    ("thread_safe_graph_propagation", "fan_out_eager_validation / 8"),
    ("thread_safe_graph_propagation", "fan_out_eager_validation / 16"),
    ("thread_safe_graph_propagation", "fan_out_lazy_dirty_epochs / 8"),
    ("thread_safe_graph_propagation", "fan_out_lazy_dirty_epochs / 16"),
    ("thread_safe_graph_propagation", "fan_in_lazy_dirty_epochs / 8"),
    ("thread_safe_graph_propagation", "fan_in_lazy_dirty_epochs / 16"),
    ("thread_safe_graph_propagation", "fan_in_batched_flush / 8"),
    ("thread_safe_graph_propagation", "fan_in_batched_flush / 16"),
)


@dataclass(frozen=True)
class BenchmarkResult:
    group: str
    case: str
    mean_ns: float
    lower_ns: float
    upper_ns: float


@dataclass(frozen=True)
class LatencyResult:
    group: str
    case: str
    p50_ns: float
    p95_ns: float
    samples: int


@dataclass(frozen=True)
class InstrumentationProfile:
    profile: str
    node_allocations: int
    slot_recomputes: int
    duplicate_speculative_recomputes: int
    dependency_edges_added: int
    dependency_edges_removed: int
    effect_queue_pushes: int
    max_effect_queue_depth: int
    lock_acquisitions: int
    lock_wait_nanos: int
    lock_hold_nanos: int
    sidecar_invalidation_frontiers: int
    sidecar_dirty_marks: int
    sidecar_invalidation_fallbacks: int
    dirty_epoch_advances: int
    lock_attribution: tuple["LockAttribution", ...]


@dataclass(frozen=True)
class LockAttribution:
    site: str
    lock_acquisitions: int
    lock_wait_nanos: int
    lock_hold_nanos: int


# Every budget ceiling below is DERIVED from a recorded measurement sweep, never
# hand-typed (#lzbenchbudgetheadroom). The old table typed each ceiling by hand,
# which got the gate exactly backwards: it was loose where the counter is
# perfectly deterministic (`dependency_edge <= 1600` for a counter that is always
# 64 — a 25x regression passed) and tight where the counter is pure scheduling
# noise (`set_cell_invalidation <= 16` for a counter observed from 1 to 256 — it
# reddened on a busy machine). A gate that reddens on noise trains everyone to
# ignore it, which is how a real regression gets waived as a flake.
#
# `--measure-budget-spread N` reruns the instrumentation profile N times and
# prints the block below, so refreshing these numbers is a measurement rather
# than a guess. The recorded spreads come from 750 runs spanning four
# environments: idle (200), moderate load (200, 8 CPU burners), heavy load (200,
# 24 burners on 32 cores) and 2-core-pinned (150, the shape of a GitHub runner).
CLASS_DETERMINISTIC = "deterministic"
CLASS_SCHEDULING_SENSITIVE = "scheduling_sensitive"
CLASS_SCHEDULING_DOMINATED = "scheduling_dominated"


@dataclass(frozen=True)
class ObservedSpread:
    """A counter's measured range — the evidence a ceiling is derived from."""

    samples: int
    minimum: int
    maximum: int

    @property
    def range(self) -> int:
        return self.maximum - self.minimum

    @property
    def classification(self) -> str:
        """Classify a counter by how much of its magnitude is scheduling noise.

        Zero spread over a sweep that spans idle, loaded and 2-core-pinned runs
        means the counter measures work items, not interleaving: it is
        structural and portable, and every one of the 22 counters in this class
        held the identical constant on 2 cores and on 32. When the spread grows
        past half the observed maximum, the counter is measuring the scheduler
        rather than the code, and no ceiling over it can distinguish a
        regression from a busy machine.
        """
        if self.range == 0:
            return CLASS_DETERMINISTIC
        if self.range * 2 <= self.maximum:
            return CLASS_SCHEDULING_SENSITIVE
        return CLASS_SCHEDULING_DOMINATED

    @property
    def ceiling(self) -> int | None:
        """The enforced ceiling, or None when the counter cannot support one.

        Deterministic counters are enforced EXACTLY: any deviation is signal.
        Scheduling-sensitive counters get headroom proportional to their own
        measured variance — one full observed range above the observed maximum.
        Scheduling-dominated counters are recorded and reported, but NOT
        enforced; pretending to gate them is what produced the flakes.
        """
        classification = self.classification
        if classification == CLASS_DETERMINISTIC:
            return self.maximum
        if classification == CLASS_SCHEDULING_SENSITIVE:
            return self.maximum + self.range
        return None


@dataclass(frozen=True)
class SiteSpread:
    site: str
    spread: ObservedSpread


@dataclass(frozen=True)
class InstrumentationBudget:
    profile: str
    total: ObservedSpread
    site_spreads: tuple[SiteSpread, ...] = ()


REGRESSION_BUDGETS: tuple[InstrumentationBudget, ...] = (
    InstrumentationBudget(
        "thread_safe_set_cell_invalidation_independent_slot_contention_16",
        total=ObservedSpread(750, 654, 893),
        site_spreads=(
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 255, 255)),
            SiteSpread("dependency_edge", ObservedSpread(750, 16, 16)),
            SiteSpread("get_refresh", ObservedSpread(750, 32, 32)),
            SiteSpread("publish", ObservedSpread(750, 16, 16)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_set_cell_invalidation_batched_write_bursts_16",
        total=ObservedSpread(750, 712, 1477),
        site_spreads=(
            SiteSpread("other", ObservedSpread(750, 644, 1154)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 1, 256)),
            SiteSpread("dependency_edge", ObservedSpread(750, 64, 64)),
            SiteSpread("get_refresh", ObservedSpread(750, 2, 2)),
            SiteSpread("publish", ObservedSpread(750, 1, 1)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_contention_same_slot_write_read_16",
        total=ObservedSpread(750, 876, 1420),
        site_spreads=(
            SiteSpread("get_refresh", ObservedSpread(750, 2, 125)),
            SiteSpread("publish", ObservedSpread(750, 186, 257)),
            SiteSpread("in_flight_wait", ObservedSpread(750, 0, 367)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 256, 256)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_contention_independent_slots_16",
        total=ObservedSpread(750, 924, 1148),
        site_spreads=(
            SiteSpread("other", ObservedSpread(750, 350, 574)),
            SiteSpread("get_refresh", ObservedSpread(750, 32, 32)),
            SiteSpread("publish", ObservedSpread(750, 271, 271)),
            SiteSpread("dependency_edge", ObservedSpread(750, 16, 16)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 255, 255)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_contention_read_mostly_waiters_16",
        total=ObservedSpread(750, 72, 144),
        site_spreads=(
            SiteSpread("get_refresh", ObservedSpread(750, 2, 32)),
            SiteSpread("publish", ObservedSpread(750, 17, 21)),
            SiteSpread("in_flight_wait", ObservedSpread(750, 0, 54)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_contention_batched_write_bursts_16",
        total=ObservedSpread(750, 713, 1915),
        site_spreads=(
            SiteSpread("other", ObservedSpread(750, 644, 1154)),
            SiteSpread("get_refresh", ObservedSpread(750, 2, 38)),
            SiteSpread("dependency_edge", ObservedSpread(750, 64, 64)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 1, 256)),
            SiteSpread("publish", ObservedSpread(750, 2, 256)),
            SiteSpread("in_flight_wait", ObservedSpread(750, 0, 250)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_effect_contention_queue_coalescing_16",
        total=ObservedSpread(750, 720, 2025),
        site_spreads=(
            SiteSpread("other", ObservedSpread(750, 655, 1705)),
            SiteSpread("dependency_edge", ObservedSpread(750, 64, 64)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 1, 256)),
            SiteSpread("get_refresh", ObservedSpread(750, 0, 0)),
            SiteSpread("publish", ObservedSpread(750, 0, 0)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_effect_contention_cleanup_execution_16",
        total=ObservedSpread(750, 619, 1859),
        site_spreads=(
            SiteSpread("other", ObservedSpread(750, 332, 1572)),
            SiteSpread("dependency_edge", ObservedSpread(750, 32, 32)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 255, 255)),
            SiteSpread("get_refresh", ObservedSpread(750, 0, 0)),
            SiteSpread("publish", ObservedSpread(750, 0, 0)),
        ),
    ),
    InstrumentationBudget(
        "thread_safe_effect_contention_batch_flush_16",
        total=ObservedSpread(750, 1239, 2649),
        site_spreads=(
            SiteSpread("other", ObservedSpread(750, 1169, 2199)),
            SiteSpread("get_refresh", ObservedSpread(750, 2, 2)),
            SiteSpread("dependency_edge", ObservedSpread(750, 65, 65)),
            SiteSpread("set_cell_invalidation", ObservedSpread(750, 1, 256)),
            SiteSpread("publish", ObservedSpread(750, 2, 177)),
        ),
    ),
)

SYNC_STRATEGY_ADOPTION_GATE: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "current_std_mutex_condvar",
        "baseline",
        "thread_safe_contention and thread_safe_effect_contention at 8/16 workers",
        "p50/p95 latency for same-slot, read-mostly, batch, and effect-heavy cases",
        "must stay within current lock-site budgets and Loom safety coverage",
    ),
    (
        "narrower_condvar_wakeups",
        "adopted for per-slot recompute waiters",
        "same-slot write/read and read-mostly waiter throughput at 8/16 workers",
        "p50/p95 latency for waiter wakeup handoff and stale-completion retry",
        "must not regress effect queue, cleanup, or batch flush budgets",
    ),
    (
        "parking_lot_style_parking",
        "candidate only",
        "same contention matrix measured against current_std_mutex_condvar",
        "p50/p95 latency for parking/unparking under 8/16 workers",
        "requires no worse lock-site budgets plus a deadlock/starvation model",
    ),
    (
        "targeted_cas",
        "candidate only",
        "fresh cached reads and independent-slot throughput at 8/16 workers",
        "p50/p95 latency for revision validation fallback and publish races",
        "requires unchanged effect/batch/disposal budgets plus Loom/Shuttle proof",
    ),
)

WATCH_ITEM_AB_CHECKS: tuple[tuple[str, str, str, str, str], ...] = (
    (
        "cached ThreadSafeContext read latency",
        "a8b6fc3 vs c917401",
        "cargo bench --features instrumentation,thread-safe --bench context -- cached_reads/thread_safe_context",
        "73.48 ns baseline vs 73.20 ns current on warm-cache repeat",
        "no tuning; the archived 56.5 ns row did not reproduce under controlled A/B",
    ),
    (
        "effect cleanup contention at 16 workers",
        "a8b6fc3 vs c917401",
        "cargo bench --features instrumentation,thread-safe --bench context -- thread_safe_effect_contention/cleanup_execution/16",
        "2.31 ms baseline vs 2.43 ms current on warm-cache repeat with overlapping CIs",
        "keep watching; Criterion reported no statistically significant change",
    ),
    (
        "invalidation-frontier fast-path Arc cache (#lzfrontierarc)",
        "15d4206 vs this change (controlled --save-baseline before_opt A/B, same session)",
        "cargo bench --features instrumentation,thread-safe --bench context -- --baseline before_opt",
        "fan_out_lazy_dirty_epochs/16 -46.8% (p=0.00), fan_in_lazy_dirty_epochs/16 -22.6% (p=0.00), independent_slot_contention/16 -17.3% (p=0.00), independent_slots/16 -5.3% (p=0.37 n.s.)",
        "adopted; the cached Arc reuses the BFS-time fast path in the marking pass, halving uninstrumented slot_fast_paths RwLock read acquisitions whose reader-count atomics dominate under 16-way contention. Deterministic state-mutex acquisition counts (the budget metric) are unchanged because slot_fast_paths is a separate uninstrumented lock; the evidence is the controlled wall-clock A/B. Microbench cases (cached_reads) correctly show no change as they do not touch the invalidation frontier.",
    ),
    (
        "Context slot clean-cache-hit fast path (#lzslotfastpath)",
        "8c64f33 vs this change (controlled --save-baseline before_slot A/B, same session)",
        "cargo bench --features instrumentation,thread-safe --bench context -- --baseline before_slot 'cached_reads|typed_cache_reads'",
        "typed_cache_reads/context_slot -58.9% (p=0.00), cached_reads/context -51.6% (p=0.00), typed_cache_reads/context_cell -2.1% (p=0.76 n.s.)",
        "adopted; refresh_slot now early-returns when the slot holds a value and is neither dirty nor force-recompute, skipping the cycle-guard borrowMut + guard-drop borrowMut + dependencies Vec clone + per-dep is_slot_node borrows + clear_slot_dirty_flags borrowMut on the cache-hit path. Correctness rests on mark_slot_dirty always being called with force_recompute=true from invalidate_dependent_from_changed_value, so any upstream change sets dirty=true and bypasses the fast path. context_slot 11.8 -> 4.7 ns, now within ~1.5 ns of context_cell (3.0 ns); the previous downcast 'tax' framing was wrong (the cell also downcasts) - the real cost was refresh_slot's redundant work on clean reads.",
    ),
)


def run(command: list[str]) -> None:
    print("$ " + " ".join(command), flush=True)
    subprocess.run(command, check=True)


def read_package_metadata(cargo_toml: Path) -> tuple[str, str]:
    if tomllib is not None:
        package = tomllib.loads(cargo_toml.read_text(encoding="utf-8"))["package"]
        return str(package["name"]), str(package["version"])

    in_package = False
    values: dict[str, str] = {}
    for raw_line in cargo_toml.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line == "[package]":
            in_package = True
            continue
        if line.startswith("[") and in_package:
            break
        if in_package and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"')
    return values["name"], values["version"]


def rustc_version() -> str:
    result = subprocess.run(
        ["rustc", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def rustc_host() -> str:
    result = subprocess.run(
        ["rustc", "-vV"],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("host: "):
            return line.split(":", 1)[1].strip()
    return "unknown"


def read_estimate(path: Path) -> tuple[float, float, float]:
    data = json.loads(path.read_text(encoding="utf-8"))
    mean = data["mean"]
    interval = mean["confidence_interval"]
    return (
        float(mean["point_estimate"]),
        float(interval["lower_bound"]),
        float(interval["upper_bound"]),
    )


def read_sample_latencies(path: Path) -> tuple[float, float, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    iters = data["iters"]
    times = data["times"]
    latencies = sorted(
        float(time_ns) / float(iter_count)
        for iter_count, time_ns in zip(iters, times)
        if float(iter_count) > 0
    )
    if not latencies:
        raise ValueError(f"{path}: no non-empty Criterion samples")
    return (
        percentile(latencies, 0.50),
        percentile(latencies, 0.95),
        len(latencies),
    )


def percentile(sorted_values: list[float], quantile: float) -> float:
    index = math.ceil(quantile * len(sorted_values)) - 1
    index = min(max(index, 0), len(sorted_values) - 1)
    return sorted_values[index]


def discover_results(criterion_dir: Path) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    for estimates in criterion_dir.glob("**/new/estimates.json"):
        rel_parts = estimates.relative_to(criterion_dir).parts
        case_parts = rel_parts[:-2]
        if not case_parts:
            continue

        group = case_parts[0]
        case = " / ".join(case_parts[1:]) if len(case_parts) > 1 else group
        if group == "thread_safe_contention" and case.isdigit():
            continue
        # #lzscalecompare: the `scale_compare` group is the cross-library
        # head-to-head (lazily vs leptos_reactive) documented manually in
        # BENCHMARKS.md's "Cross-library comparison" prose, NOT a tracked lazily
        # benchmark. Exclude it from the auto-generated results table so running
        # `cargo bench --features scale-compare` never makes `benchmark-check`
        # stale (its criterion estimates would otherwise leak into the table).
        if group in EXCLUDED_GROUPS:
            continue
        mean_ns, lower_ns, upper_ns = read_estimate(estimates)
        results.append(
            BenchmarkResult(
                group=group,
                case=case,
                mean_ns=mean_ns,
                lower_ns=lower_ns,
                upper_ns=upper_ns,
            )
        )

    return sorted(
        results,
        key=lambda item: (
            GROUP_ORDER.get(item.group, len(GROUP_ORDER)),
            item.group,
            benchmark_case_key(item),
        ),
    )


def discover_latency_results(criterion_dir: Path) -> list[LatencyResult]:
    required = set(REQUIRED_LATENCY_CASES)
    results: list[LatencyResult] = []

    for sample in criterion_dir.glob("**/new/sample.json"):
        rel_parts = sample.relative_to(criterion_dir).parts
        case_parts = rel_parts[:-2]
        if not case_parts:
            continue

        group = case_parts[0]
        case = " / ".join(case_parts[1:]) if len(case_parts) > 1 else group
        if (group, case) not in required:
            continue

        p50_ns, p95_ns, samples = read_sample_latencies(sample)
        results.append(
            LatencyResult(
                group=group,
                case=case,
                p50_ns=p50_ns,
                p95_ns=p95_ns,
                samples=samples,
            )
        )

    return sorted(
        results,
        key=lambda item: (
            GROUP_ORDER.get(item.group, len(GROUP_ORDER)),
            item.group,
            benchmark_case_key(item),
        ),
    )


def run_instrumentation_profile(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "cargo",
        "run",
        "--example",
        "instrumentation_profile",
        "--features",
        "instrumentation,thread-safe",
        "--quiet",
    ]
    print("$ " + " ".join(command), flush=True)
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    output.write_text(result.stdout, encoding="utf-8")


def read_instrumentation_profiles(path: Path) -> list[InstrumentationProfile]:
    rows: list[InstrumentationProfile] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                InstrumentationProfile(
                    profile=row["profile"],
                    node_allocations=int(row["node_allocations"]),
                    slot_recomputes=int(row["slot_recomputes"]),
                    duplicate_speculative_recomputes=int(
                        row["duplicate_speculative_recomputes"]
                    ),
                    dependency_edges_added=int(row["dependency_edges_added"]),
                    dependency_edges_removed=int(row["dependency_edges_removed"]),
                    effect_queue_pushes=int(row["effect_queue_pushes"]),
                    max_effect_queue_depth=int(row["max_effect_queue_depth"]),
                    lock_acquisitions=int(row["lock_acquisitions"]),
                    lock_wait_nanos=int(row["lock_wait_nanos"]),
                    lock_hold_nanos=int(row["lock_hold_nanos"]),
                    sidecar_invalidation_frontiers=int(
                        row["sidecar_invalidation_frontiers"]
                    ),
                    sidecar_dirty_marks=int(row["sidecar_dirty_marks"]),
                    sidecar_invalidation_fallbacks=int(
                        row["sidecar_invalidation_fallbacks"]
                    ),
                    dirty_epoch_advances=int(row["dirty_epoch_advances"]),
                    lock_attribution=parse_lock_attribution(
                        row.get("lock_attribution", "")
                    ),
                )
            )
    return rows


def parse_lock_attribution(value: str) -> tuple[LockAttribution, ...]:
    if not value:
        return ()

    sites: list[LockAttribution] = []
    for item in value.split("|"):
        site, counters = item.split("=", 1)
        acquisitions, wait_nanos, hold_nanos = counters.split(":", 2)
        sites.append(
            LockAttribution(
                site=site,
                lock_acquisitions=int(acquisitions),
                lock_wait_nanos=int(wait_nanos),
                lock_hold_nanos=int(hold_nanos),
            )
        )
    return tuple(sites)


def lock_attribution_by_site(profile: InstrumentationProfile) -> dict[str, int]:
    return {
        attribution.site: attribution.lock_acquisitions
        for attribution in profile.lock_attribution
    }


def measure_budget_spread(samples: int, profile_output: Path) -> int:
    """Rerun the instrumentation profile N times and print the measured spreads.

    This is how the recorded numbers in REGRESSION_BUDGETS are produced. The
    point is that no ceiling in this file is a guess: the sweep prints both a
    per-counter table and a paste-ready REGRESSION_BUDGETS block, and reports
    every counter whose observation fell OUTSIDE its recorded spread, which is
    the signal that the recording needs refreshing.

    Run it under the conditions the gate actually runs in. A sweep taken only on
    an idle machine understates the spread, which is exactly how a 0.4 percent
    headroom budget got written in the first place; the recorded numbers span
    idle, loaded, and 2-core-pinned (`taskset -c 0,1`) runs.
    """
    observed: dict[tuple[str, str], list[int]] = {}
    for index in range(samples):
        run_instrumentation_profile(profile_output)
        for profile in read_instrumentation_profiles(profile_output):
            observed.setdefault((profile.profile, "lock_acquisitions"), []).append(
                profile.lock_acquisitions
            )
            for attribution in profile.lock_attribution:
                observed.setdefault(
                    (profile.profile, attribution.site), []
                ).append(attribution.lock_acquisitions)
        print(f"  sampled {index + 1}/{samples}", flush=True)

    drift: list[str] = []
    print()
    print(
        f"{'profile / counter':78} {'recorded':>14} {'measured':>14} "
        f"{'classification':>22} {'ceiling':>12}"
    )
    for budget in REGRESSION_BUDGETS:
        for counter, spread in budget_counters(budget):
            values = observed.get((budget.profile, counter))
            if not values:
                drift.append(f"{budget.profile} / {counter}: counter not emitted")
                continue
            measured = ObservedSpread(len(values), min(values), max(values))
            ceiling = measured.ceiling
            print(
                f"{budget.profile + ' / ' + counter:78} "
                f"{str(spread.minimum) + '-' + str(spread.maximum):>14} "
                f"{str(measured.minimum) + '-' + str(measured.maximum):>14} "
                f"{measured.classification:>22} "
                f"{'not enforced' if ceiling is None else ceiling:>12}"
            )
            if measured.minimum < spread.minimum or measured.maximum > spread.maximum:
                drift.append(
                    f"{budget.profile} / {counter}: measured "
                    f"{measured.minimum}-{measured.maximum} falls outside recorded "
                    f"{spread.minimum}-{spread.maximum}"
                )

    print()
    print("Paste-ready block (merge with the recorded spreads — widen, do not narrow):")
    print("REGRESSION_BUDGETS: tuple[InstrumentationBudget, ...] = (")
    for budget in REGRESSION_BUDGETS:
        total = observed.get((budget.profile, "lock_acquisitions"), [])
        print("    InstrumentationBudget(")
        print(f'        "{budget.profile}",')
        if total:
            print(
                f"        total=ObservedSpread({len(total)}, {min(total)}, {max(total)}),"
            )
        print("        site_spreads=(")
        for site in budget.site_spreads:
            values = observed.get((budget.profile, site.site), [])
            if values:
                print(
                    f'            SiteSpread("{site.site}", '
                    f"ObservedSpread({len(values)}, {min(values)}, {max(values)})),"
                )
        print("        ),")
        print("    ),")
    print(")")

    if drift:
        print()
        print("recorded spread drift:", file=sys.stderr)
        for item in drift:
            print(f"- {item}", file=sys.stderr)
        print(
            "Fix: widen the recorded ObservedSpread values so the ceilings are "
            "derived from what this machine actually observes.",
            file=sys.stderr,
        )
        return EXIT_BUDGET_REGRESSION
    print()
    print(f"every counter stayed inside its recorded spread over {samples} run(s)")
    return 0


def budget_counters(
    budget: InstrumentationBudget,
) -> tuple[tuple[str, ObservedSpread], ...]:
    """Every gated counter of a profile: the total, then each lock site."""
    return (("lock_acquisitions", budget.total),) + tuple(
        (site.site, site.spread) for site in budget.site_spreads
    )


def enforced_counter_count() -> int:
    return sum(
        1
        for budget in REGRESSION_BUDGETS
        for _, spread in budget_counters(budget)
        if spread.ceiling is not None
    )


def observation_only_counter_count() -> int:
    return sum(
        1
        for budget in REGRESSION_BUDGETS
        for _, spread in budget_counters(budget)
        if spread.ceiling is None
    )


def regression_budget_failures(
    profiles: list[InstrumentationProfile],
) -> list[str]:
    by_profile = {profile.profile: profile for profile in profiles}
    failures: list[str] = []

    for budget in REGRESSION_BUDGETS:
        profile = by_profile.get(budget.profile)
        if profile is None:
            failures.append(f"{budget.profile}: missing instrumentation profile")
            continue

        by_site = lock_attribution_by_site(profile)
        for counter, spread in budget_counters(budget):
            ceiling = spread.ceiling
            if ceiling is None:
                # Scheduling-dominated: recorded and reported, never enforced.
                continue
            actual = (
                profile.lock_acquisitions
                if counter == "lock_acquisitions"
                else by_site.get(counter, 0)
            )
            if actual > ceiling:
                failures.append(
                    "{profile}: {counter} {actual} > {ceiling} "
                    "({classification}; recorded {lo}-{hi} over {samples} runs)".format(
                        profile=budget.profile,
                        counter=counter,
                        actual=actual,
                        ceiling=ceiling,
                        classification=spread.classification,
                        lo=spread.minimum,
                        hi=spread.maximum,
                        samples=spread.samples,
                    )
                )

    return failures


def required_latency_failures(latencies: list[LatencyResult]) -> list[str]:
    present = {(latency.group, latency.case) for latency in latencies}
    return [
        f"{group} / {case}: missing required p50/p95 latency row"
        for group, case in REQUIRED_LATENCY_CASES
        if (group, case) not in present
    ]


def natural_case_key(value: str) -> list[tuple[int, object]]:
    parts: list[tuple[int, object]] = []
    current = ""
    for char in value:
        if char.isdigit():
            current += char
        else:
            if current:
                parts.append((0, int(current)))
                current = ""
            parts.append((1, char))
    if current:
        parts.append((0, int(current)))
    return parts


def benchmark_case_key(
    result: BenchmarkResult | LatencyResult,
) -> tuple[int, list[tuple[int, object]]]:
    if result.group == "set_cell_invalidation":
        case_name, _, worker = result.case.partition(" / ")
        return (
            SET_CELL_INVALIDATION_CASE_ORDER.get(
                case_name, len(SET_CELL_INVALIDATION_CASE_ORDER)
            ),
            natural_case_key(worker or result.case),
        )

    if result.group == "thread_safe_contention":
        case_name, _, worker = result.case.partition(" / ")
        return (
            THREAD_SAFE_CONTENTION_CASE_ORDER.get(
                case_name, len(THREAD_SAFE_CONTENTION_CASE_ORDER)
            ),
            natural_case_key(worker or result.case),
        )

    if result.group == "thread_safe_effect_contention":
        case_name, _, worker = result.case.partition(" / ")
        return (
            THREAD_SAFE_EFFECT_CONTENTION_CASE_ORDER.get(
                case_name, len(THREAD_SAFE_EFFECT_CONTENTION_CASE_ORDER)
            ),
            natural_case_key(worker or result.case),
        )

    if result.group == "thread_safe_graph_propagation":
        case_name, _, worker = result.case.partition(" / ")
        return (
            THREAD_SAFE_GRAPH_PROPAGATION_CASE_ORDER.get(
                case_name, len(THREAD_SAFE_GRAPH_PROPAGATION_CASE_ORDER)
            ),
            natural_case_key(worker or result.case),
        )

    if result.group == "async_concurrent_contention":
        case_name, _, worker = result.case.partition(" / ")
        return (
            ASYNC_CONCURRENT_CONTENTION_CASE_ORDER.get(
                case_name, len(ASYNC_CONCURRENT_CONTENTION_CASE_ORDER)
            ),
            natural_case_key(worker or result.case),
        )

    if result.group == "tokio_sync_concurrent_contention":
        case_name, _, worker = result.case.partition(" / ")
        return (
            TOKIO_SYNC_CONCURRENT_CONTENTION_CASE_ORDER.get(
                case_name, len(TOKIO_SYNC_CONCURRENT_CONTENTION_CASE_ORDER)
            ),
            natural_case_key(worker or result.case),
        )

    return (0, natural_case_key(result.case))


def format_duration(ns: float) -> str:
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.3f} s"
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.3f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.3f} us"
    return f"{ns:.3f} ns"


def build_section(
    package: str,
    version: str,
    results: list[BenchmarkResult],
    latencies: list[LatencyResult],
    profiles: list[InstrumentationProfile],
) -> str:
    lines = [
        START_MARKER,
        f"Generated for package `{package}` version `{version}`.",
        "",
        f"Environment: `{rustc_version()}` on `{rustc_host()}`.",
        "",
        "Refresh command:",
        "",
        "```bash",
        "python3 scripts/update-benchmark-results.py",
        "```",
        "",
        "Regression workflow:",
        "",
        "```bash",
        "cargo bench --features instrumentation,thread-safe -- --save-baseline before",
        "# apply the performance patch",
        "cargo bench --features instrumentation,thread-safe -- --baseline before",
        "python3 scripts/update-benchmark-results.py --no-run",
        "```",
        "",
        "Regression budgets enforced by `python3 scripts/update-benchmark-results.py --check`:",
        "",
        "Every ceiling is DERIVED from the recorded spread, never hand-typed; refresh",
        "the spreads with `python3 scripts/update-benchmark-results.py --measure-budget-spread N`.",
        "A counter with zero spread across idle, loaded and 2-core-pinned runs measures",
        "work items rather than interleaving, so it is enforced EXACTLY. A counter whose",
        "spread is under half its maximum gets headroom of one full observed range above",
        "the observed maximum. A counter whose spread exceeds half its maximum is measuring",
        "the scheduler, not the code: it is recorded as an observation and NOT enforced,",
        "because a gate that reddens on noise trains everyone to ignore it.",
        "",
        "| Profile | Counter | Observed range | Samples | Classification | Enforced ceiling |",
        "|---|---|---:|---:|---|---:|",
    ]

    for budget in REGRESSION_BUDGETS:
        for counter, spread in budget_counters(budget):
            ceiling = spread.ceiling
            lines.append(
                "| {profile} | {counter} | {lo}-{hi} | {samples} | {classification} | {ceiling} |".format(
                    profile=budget.profile,
                    counter=counter,
                    lo=spread.minimum,
                    hi=spread.maximum,
                    samples=spread.samples,
                    classification=spread.classification,
                    ceiling="not enforced" if ceiling is None else ceiling,
                )
            )

    lines.extend(
        [
            "",
            "Budgets use lock acquisition counts instead of elapsed wait/hold time. Those "
            "counts are only deterministic for the {enforced_deterministic} counters "
            "classified as such above; {observation_only} of {total} gated counters are "
            "scheduling-dominated and carry no regression signal at all.".format(
                enforced_deterministic=sum(
                    1
                    for budget in REGRESSION_BUDGETS
                    for _, spread in budget_counters(budget)
                    if spread.classification == CLASS_DETERMINISTIC
                ),
                observation_only=observation_only_counter_count(),
                total=enforced_counter_count() + observation_only_counter_count(),
            ),
            "",
            "Synchronization strategy adoption gate:",
            "",
            "| Strategy | Status | Required throughput evidence | Required p50/p95 latency evidence | Lock-site and safety gate |",
            "|---|---|---|---|---|",
        ]
    )

    for strategy, status, throughput, latency, gate in SYNC_STRATEGY_ADOPTION_GATE:
        lines.append(
            "| {strategy} | {status} | {throughput} | {latency} | {gate} |".format(
                strategy=strategy,
                status=status,
                throughput=throughput,
                latency=latency,
                gate=gate,
            )
        )

    lines.extend(
        [
            "",
            "Candidates do not replace the current strategy before the same run reports throughput, p50/p95 latency, and lock-site budgets for the required 8/16-worker cases.",
            "",
            "Required latency evidence uses Criterion sample per-iteration timing.",
            "",
            "Watch-item A/B follow-up:",
            "",
            "| Watch item | Baseline/current refs | Focused command | Controlled rerun result | Decision |",
            "|---|---|---|---|---|",
        ]
    )

    for item, refs, command, result, decision in WATCH_ITEM_AB_CHECKS:
        lines.append(
            "| {item} | {refs} | `{command}` | {result} | {decision} |".format(
                item=item,
                refs=refs,
                command=command,
                result=result,
                decision=decision,
            )
        )

    lines.extend(
        [
            "",
            "| Group | Case | p50 | p95 | Samples |",
            "|---|---|---:|---:|---:|",
        ]
    )

    for latency in latencies:
        lines.append(
            "| {group} | {case} | {p50} | {p95} | {samples} |".format(
                group=latency.group,
                case=latency.case,
                p50=format_duration(latency.p50_ns),
                p95=format_duration(latency.p95_ns),
                samples=latency.samples,
            )
        )

    lines.extend(
        [
            "",
        ]
    )

    lines.extend(
        [
            "Criterion estimates are local mean wall-clock time per iteration.",
            "",
            "| Group | Case | Mean | 95% CI |",
            "|---|---|---:|---:|",
        ]
    )

    for result in results:
        lines.append(
            "| {group} | {case} | {mean} | {lower} - {upper} |".format(
                group=result.group,
                case=result.case,
                mean=format_duration(result.mean_ns),
                lower=format_duration(result.lower_ns),
                upper=format_duration(result.upper_ns),
            )
        )

    lines.extend(
        [
            "",
            "Instrumentation snapshots are single local profile runs captured by",
            "`examples/instrumentation_profile.rs`.",
            "",
            "| Profile | Alloc | Recomputes | Duplicate recomputes | Edges + | Edges - | Effect pushes | Max queue | Lock acquisitions | Lock wait | Lock hold | Sidecar frontiers | Sidecar dirty marks | Sidecar fallbacks | Dirty epochs |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for profile in profiles:
        lines.append(
            "| {profile} | {alloc} | {recomputes} | {duplicates} | {edges_added} | "
            "{edges_removed} | {effect_pushes} | {max_queue} | {locks} | "
            "{lock_wait} | {lock_hold} | {sidecar_frontiers} | {sidecar_dirty} | "
            "{sidecar_fallbacks} | {dirty_epochs} |".format(
                profile=profile.profile,
                alloc=profile.node_allocations,
                recomputes=profile.slot_recomputes,
                duplicates=profile.duplicate_speculative_recomputes,
                edges_added=profile.dependency_edges_added,
                edges_removed=profile.dependency_edges_removed,
                effect_pushes=profile.effect_queue_pushes,
                max_queue=profile.max_effect_queue_depth,
                locks=profile.lock_acquisitions,
                lock_wait=format_duration(profile.lock_wait_nanos),
                lock_hold=format_duration(profile.lock_hold_nanos),
                sidecar_frontiers=profile.sidecar_invalidation_frontiers,
                sidecar_dirty=profile.sidecar_dirty_marks,
                sidecar_fallbacks=profile.sidecar_invalidation_fallbacks,
                dirty_epochs=profile.dirty_epoch_advances,
            )
        )

    attribution_rows = [
        (profile, attribution)
        for profile in profiles
        if profile.profile.startswith("thread_safe_contention_")
        or profile.profile.startswith("thread_safe_set_cell_invalidation_")
        or profile.profile.startswith("thread_safe_effect_contention_")
        or profile.profile.startswith("thread_safe_graph_propagation_")
        for attribution in profile.lock_attribution
        if attribution.lock_acquisitions > 0
    ]
    if attribution_rows:
        lines.extend(
            [
                "",
                "ThreadSafe lock attribution for contention profiles:",
                "",
                "| Profile | Site | Lock acquisitions | Lock wait | Lock hold |",
                "|---|---|---:|---:|---:|",
            ]
        )
        for profile, attribution in attribution_rows:
            lines.append(
                "| {profile} | {site} | {locks} | {lock_wait} | {lock_hold} |".format(
                    profile=profile.profile,
                    site=attribution.site,
                    locks=attribution.lock_acquisitions,
                    lock_wait=format_duration(attribution.lock_wait_nanos),
                    lock_hold=format_duration(attribution.lock_hold_nanos),
                )
            )

    lines.extend(["", END_MARKER])
    return "\n".join(lines)


def replace_section(readme: str, section: str) -> str:
    if START_MARKER in readme and END_MARKER in readme:
        start = readme.index(START_MARKER)
        end = readme.index(END_MARKER, start) + len(END_MARKER)
        return readme[:start] + section + readme[end:]

    new_section = "\n## Benchmark Results\n\n" + section + "\n"
    if INSERT_BEFORE in readme:
        return readme.replace(INSERT_BEFORE, new_section + INSERT_BEFORE, 1)
    return readme.rstrip() + "\n" + new_section + "\n"


def replace_benchmarks_section(content: str, section: str) -> str:
    if START_MARKER in content and END_MARKER in content:
        start = content.index(START_MARKER)
        end = content.index(END_MARKER, start) + len(END_MARKER)
        return content[:start] + section + content[end:]

    new_section = "\n## Benchmark Results\n\n" + section + "\n"
    if BENCHMARKS_INSERT_BEFORE in content:
        return content.replace(
            BENCHMARKS_INSERT_BEFORE,
            new_section + BENCHMARKS_INSERT_BEFORE,
            1,
        )
    return content.rstrip() + "\n" + new_section + "\n"


# #vnmr: evidence provenance.
#
# There used to be a skip here: no `target/criterion` meant "fresh clone", which
# printed a loud banner and exited 0. The banner was honest and the exit code was
# not, and only one of those is what a caller reads. A green `make check` said
# "budgets verified" to every reader who did not scroll. Absent evidence is now a
# failure, and so is evidence that measured a different tree — because "measure,
# then edit, then trust the old numbers" is the same lie with extra steps.
def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_file_hashes(root: Path = Path(".")) -> dict[str, str]:
    """Content hash of every input a benchmark number can depend on."""
    hashes: dict[str, str] = {}
    for pattern in EVIDENCE_SOURCE_GLOBS:
        for path in sorted(root.glob(pattern)):
            if not path.is_file():
                continue
            hashes[path.as_posix()] = hash_file(path)
    return hashes


def source_fingerprint(hashes: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for name in sorted(hashes):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashes[name].encode("utf-8"))
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def write_evidence_manifest(path: Path, mode: str, command: list[str]) -> None:
    hashes = source_file_hashes()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": EVIDENCE_MANIFEST_VERSION,
                "mode": mode,
                "command": command,
                "recorded_at": _datetime.datetime.now(
                    _datetime.timezone.utc
                ).isoformat(timespec="seconds"),
                "source_fingerprint": source_fingerprint(hashes),
                "source_files": hashes,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"recorded benchmark evidence provenance in {path} (mode={mode})")


def read_evidence_manifest(path: Path) -> dict | None:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict):
        return None
    if manifest.get("version") != EVIDENCE_MANIFEST_VERSION:
        return None
    if "source_fingerprint" not in manifest:
        return None
    return manifest


def fail_evidence_absent(missing: list[str]) -> int:
    print("", file=sys.stderr)
    print("=" * 72, file=sys.stderr)
    print(
        "BENCHMARK BUDGETS WERE NOT ENFORCED: this checkout has no benchmark "
        "evidence.",
        file=sys.stderr,
    )
    for item in missing:
        print(f"  missing: {item}", file=sys.stderr)
    print(
        "Absent evidence is a FAILURE, not a skip. Exiting 0 here would report "
        '"budgets verified" for budgets nothing measured.',
        file=sys.stderr,
    )
    print(
        "Fix: run `make benchmark-evidence` (quick gating measurement, ~1 min), "
        "or `make benchmark-evidence-full` for the full-fidelity run that also "
        "backs BENCHMARKS.md.",
        file=sys.stderr,
    )
    print("=" * 72, file=sys.stderr)
    return EXIT_EVIDENCE_ABSENT


def fail_evidence_stale(manifest: dict, current: dict[str, str]) -> int:
    recorded: dict[str, str] = manifest.get("source_files", {}) or {}
    added = sorted(set(current) - set(recorded))
    removed = sorted(set(recorded) - set(current))
    modified = sorted(
        name for name in set(current) & set(recorded) if current[name] != recorded[name]
    )
    changed = (
        [f"M {name}" for name in modified]
        + [f"A {name}" for name in added]
        + [f"D {name}" for name in removed]
    )

    print("", file=sys.stderr)
    print("=" * 72, file=sys.stderr)
    print(
        "BENCHMARK EVIDENCE IS STALE: it measured a different tree than the one "
        "checked out.",
        file=sys.stderr,
    )
    print(
        f"  measured : {manifest.get('source_fingerprint')} "
        f"(recorded {manifest.get('recorded_at', 'unknown')}, "
        f"mode={manifest.get('mode', 'unknown')})",
        file=sys.stderr,
    )
    print(f"  checkout : {source_fingerprint(current)}", file=sys.stderr)
    print(
        f"  {len(changed)} of {len(current)} measured source files changed since "
        "the measurement:",
        file=sys.stderr,
    )
    for line in changed[:20]:
        print(f"    {line}", file=sys.stderr)
    if len(changed) > 20:
        print(f"    ... and {len(changed) - 20} more", file=sys.stderr)
    print(
        "Passing these budgets would be a statement about code that is no longer "
        "here.",
        file=sys.stderr,
    )
    print(
        "Fix: re-measure with `make benchmark-evidence` (or "
        "`make benchmark-evidence-full`).",
        file=sys.stderr,
    )
    print("=" * 72, file=sys.stderr)
    return EXIT_EVIDENCE_STALE


def check_evidence(
    criterion_dir: Path,
    profile_output: Path,
    manifest_path: Path,
    results: list[BenchmarkResult],
) -> int:
    """0 when the evidence is present AND measures the current tree."""
    missing: list[str] = []
    if not results:
        missing.append(
            f"{criterion_dir} (Criterion estimates; the directory is "
            + ("present but yielded none" if criterion_dir.exists() else "absent")
            + ")"
        )
    if not profile_output.exists():
        missing.append(f"{profile_output} (instrumentation counters)")
    manifest = read_evidence_manifest(manifest_path)
    if manifest is None:
        missing.append(
            f"{manifest_path} (evidence provenance; absent, unreadable, or written "
            "by an older manifest version)"
        )
    if missing:
        return fail_evidence_absent(missing)

    assert manifest is not None
    current = source_file_hashes()
    if source_fingerprint(current) != manifest["source_fingerprint"]:
        return fail_evidence_stale(manifest, current)
    return 0


def run_gating_benches(quick: bool) -> list[str]:
    """The measurement the gate is a gate on.

    Quick mode narrows to the benchmark groups the budgets and the required
    latency rows actually read, and runs them under Criterion's `--quick`
    sampling. It is a reduced-precision measurement, not a stub: the same code
    paths execute and the same counters are produced. The full run stays the
    source for BENCHMARKS.md, where the wall-clock precision matters.
    """
    if quick:
        command = [
            "cargo",
            "bench",
            "--locked",
            "--features",
            "instrumentation,thread-safe",
            "--bench",
            "context",
            "--",
            "--quick",
            "thread_safe_",
        ]
    else:
        # `scale-bench` enables the gated >=1M-node scale group.
        command = [
            "cargo",
            "bench",
            "--features",
            "instrumentation,async,tokio,thread-safe,scale-bench",
        ]
    run(command)
    return command


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if README.md is stale")
    parser.add_argument(
        "--no-run",
        action="store_true",
        help="reuse existing target/criterion results instead of running benches",
    )
    parser.add_argument("--readme", default=Path("README.md"), type=Path)
    parser.add_argument(
        "--benchmarks-file",
        default=Path("BENCHMARKS.md"),
        type=Path,
        help="path to BENCHMARKS.md for generated benchmark results",
    )
    parser.add_argument("--cargo-toml", default=Path("Cargo.toml"), type=Path)
    parser.add_argument(
        "--criterion-dir",
        default=Path("target/criterion"),
        type=Path,
    )
    parser.add_argument(
        "--record-evidence",
        action="store_true",
        help=(
            "run the gating measurement (benches + instrumentation profile) and "
            "record its source fingerprint; does not touch BENCHMARKS.md"
        ),
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help=(
            "with --record-evidence, use Criterion's reduced-sample mode over the "
            "benchmark groups the budgets read (a real measurement, lower precision)"
        ),
    )
    parser.add_argument(
        "--evidence-manifest",
        default=DEFAULT_EVIDENCE_MANIFEST,
        type=Path,
        help="JSON path recording which source tree the evidence measured",
    )
    parser.add_argument(
        "--refresh-profile",
        action="store_true",
        help="regenerate the instrumentation profile before checking",
    )
    parser.add_argument(
        "--budgets-only",
        action="store_true",
        help=(
            "enforce evidence and regression budgets without comparing "
            "machine-dependent BENCHMARKS.md timings"
        ),
    )
    parser.add_argument(
        "--measure-budget-spread",
        type=int,
        metavar="N",
        help=(
            "rerun the instrumentation profile N times and print the measured "
            "per-counter spread, the derived ceilings, and any drift from the "
            "recorded spreads (how REGRESSION_BUDGETS is refreshed)"
        ),
    )
    parser.add_argument(
        "--profile-output",
        default=DEFAULT_PROFILE_OUTPUT,
        type=Path,
        help="CSV path for instrumentation profile snapshots",
    )
    args = parser.parse_args()

    if args.budgets_only and not args.check:
        parser.error("--budgets-only requires --check")
    if args.quick and not args.record_evidence:
        parser.error("--quick requires --record-evidence")
    if args.record_evidence and args.check:
        parser.error("--record-evidence and --check are separate steps")
    if args.measure_budget_spread is not None:
        if args.measure_budget_spread < 1:
            parser.error("--measure-budget-spread requires a positive sample count")
        return measure_budget_spread(args.measure_budget_spread, args.profile_output)

    if args.record_evidence:
        # Generate, then record what was generated FROM. The manifest is written
        # last on purpose: a crashed bench run leaves no provenance, so the next
        # `--check` reports absent evidence rather than blessing a partial run.
        command = [] if args.no_run else run_gating_benches(args.quick)
        run_instrumentation_profile(args.profile_output)
        write_evidence_manifest(
            args.evidence_manifest,
            "quick" if args.quick else "full",
            command,
        )
        return 0

    if args.refresh_profile:
        run_instrumentation_profile(args.profile_output)
    elif args.check:
        pass
    elif not args.no_run:
        command = run_gating_benches(quick=False)
        run_instrumentation_profile(args.profile_output)
        write_evidence_manifest(args.evidence_manifest, "full", command)
    else:
        run_instrumentation_profile(args.profile_output)

    results = discover_results(args.criterion_dir)
    if args.check:
        evidence_status = check_evidence(
            args.criterion_dir,
            args.profile_output,
            args.evidence_manifest,
            results,
        )
        if evidence_status != 0:
            return evidence_status
    elif not results:
        print(
            f"no Criterion estimates found under {args.criterion_dir}; run without --no-run",
            file=sys.stderr,
        )
        return EXIT_EVIDENCE_ABSENT
    latencies = discover_latency_results(args.criterion_dir)
    latency_failures = required_latency_failures(latencies)
    if latency_failures:
        print("required latency evidence failure(s):", file=sys.stderr)
        for failure in latency_failures:
            print(f"- {failure}", file=sys.stderr)
        print(
            "Fix: re-measure with `make benchmark-evidence`.",
            file=sys.stderr,
        )
        return EXIT_BUDGET_REGRESSION
    if not args.profile_output.exists():
        print(
            f"no instrumentation profile found at {args.profile_output}; run without --check",
            file=sys.stderr,
        )
        return EXIT_EVIDENCE_ABSENT
    profiles = read_instrumentation_profiles(args.profile_output)
    if not profiles:
        print(
            f"no instrumentation profile rows found in {args.profile_output}",
            file=sys.stderr,
        )
        return EXIT_EVIDENCE_ABSENT
    budget_failures = regression_budget_failures(profiles)
    if budget_failures:
        print("instrumentation regression budget failure(s):", file=sys.stderr)
        for failure in budget_failures:
            print(f"- {failure}", file=sys.stderr)
        return EXIT_BUDGET_REGRESSION

    if args.budgets_only:
        manifest = read_evidence_manifest(args.evidence_manifest) or {}
        print(
            "benchmark regression budgets ENFORCED and green against evidence "
            f"recorded {manifest.get('recorded_at', 'unknown')} "
            f"(mode={manifest.get('mode', 'unknown')}, "
            f"{manifest['source_fingerprint'][:19] if manifest.get('source_fingerprint') else 'unknown'})"
        )
        # Say out loud how much was NOT gated. A gate that reports "green"
        # without naming its own blind spots is how a scheduling-dominated
        # counter gets read as covered (#lzbenchbudgetheadroom).
        print(
            f"{enforced_counter_count()} counter(s) enforced; "
            f"{observation_only_counter_count()} scheduling-dominated counter(s) "
            "recorded as observations only and NOT enforced"
        )
        return 0

    package, version = read_package_metadata(args.cargo_toml)
    section = build_section(package, version, results, latencies, profiles)
    current = args.benchmarks_file.read_text(encoding="utf-8")
    updated = replace_benchmarks_section(current, section)

    if args.check:
        if current != updated:
            print(
                "BENCHMARKS.md benchmark results are stale; run "
                "`make benchmark-update`",
                file=sys.stderr,
            )
            return EXIT_BUDGET_REGRESSION
        return 0

    args.benchmarks_file.write_text(updated, encoding="utf-8")
    print(f"updated {args.benchmarks_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
