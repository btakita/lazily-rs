//! Runtime conformance manifest (#lazilyupgradeconformance).
//!
//! The static coverage guard greps test sources for fixture filenames. That
//! catches a fixture nobody mentions, but not one mentioned in a comment and
//! hand-transcribed — the drift found in lazily-cpp's queue tests, and in this
//! repo's own `topic_conformance.rs`, where four `topiccell_*.json` fixtures
//! were named in the module docs while nothing ever opened them. Only observing
//! the read proves the corpus was replayed.
//!
//! # Why this seam
//!
//! Go had one package, so one helper served every file. Rust integration tests
//! are separate crates — each `tests/*.rs` compiles to its own binary — so
//! there is no free shared helper and no `TestMain`.
//!
//! The seam chosen is `tests/common/mod.rs` plus `mod common;` in each test file
//! that opens a fixture. Reasons:
//!
//! * Cargo only auto-discovers `tests/*.rs` at the top level, so this file is
//!   *not* compiled as its own (empty) test binary; it is compiled into each
//!   crate that asks for it.
//! * It is the conventional Rust spelling, so it needs no explanation at the
//!   ~30 call sites — one `mod common;` line and one identifier substitution per
//!   read.
//! * The recorder stays out of the shipped `lazily` library crate. Nothing here
//!   is compiled into what users install; adding a test-manifest sink to the
//!   public crate just to share it would put build-gate machinery in the
//!   product.
//!
//! `#[path = "..."] mod` was the alternative. It buys nothing here — the path is
//! already the default one — and it would obscure the fact that this is an
//! ordinary shared test module.
//!
//! # Contract
//!
//! * Reads outside the conformance corpus pass straight through unrecorded, so
//!   routing every read in `tests/` through [`spec_read_to_string`] is harmless.
//! * The manifest is APPENDED, never truncated. `make check` runs a dozen
//!   separate `cargo test` invocations over different feature sets, each
//!   producing several test binaries; every one must contribute its share to one
//!   union. The Makefile truncates once, before the suite.
//! * The manifest path comes from `LAZILY_CONFORMANCE_MANIFEST` and must be
//!   ABSOLUTE — test binaries can run from a different working directory. Unset
//!   means the recorder is a no-op, so a bare `cargo test` is unaffected.
//! * Rust has no `TestMain`, so this appends on each newly seen read rather than
//!   flushing at exit: no process-exit machinery, and it matches the append
//!   contract exactly.
//! * A write failure never fails a suite. A manifest we cannot write surfaces
//!   downstream as missing evidence, which is the correct outcome.
//!
//! # Sibling guard
//!
//! Opening a fixture is one level above *consuming* its assertions. See
//! [`expect`] (`#lzassertunknownkeys`) for the guard that fails a runner which
//! replays a fixture while silently ignoring a key the fixture asserts.
//!
//! # Per-scenario accounting (`#lzscenariocoverage`)
//!
//! A fixture with several named scenarios can be PARTIALLY replayed and nothing
//! above notices. The manifest asks only whether the FILE was opened — one
//! scenario is enough — and the key guards only bind blocks a runner actually
//! reaches, so an unreplayed scenario contributes no unconsumed key and no
//! unasserted key. Skipping a whole scenario is invisible to a guard that only
//! inspects the scenarios you ran.
//!
//! `reliable-sync/liveness_orset_lww.json` carries four scenarios; this binding
//! replayed three, and the suite was green.
//!
//! So this module carries a second runtime ledger, on exactly the manifest's
//! terms: [`record_scenario`] appends `fixture<TAB>id<TAB>source` to
//! `$LAZILY_CONFORMANCE_SCENARIOS` at the point of replay, and
//! `scripts/check-conformance-coverage.sh` compares it against the scenarios
//! present in each opened fixture on disk, in both directions. Prefer the
//! iteration helpers ([`scenarios`], [`scenario_by_name`], [`scenario_at`]) so a
//! new runner cannot forget to record.
//!
//! Ids resolve `id` -> `name` -> positional `#<n>` (0-based), identically in
//! every binding, because the corpus is not uniform: three `stdlib` fixtures key
//! by `id`, 28 by `name`, and `collections/mergecell_algebra.json` carries no
//! identifier at all. The positional fallback is *reported* by the guard rather
//! than silently accepted — its visibility is what makes the corpus gap fixable
//! upstream later.

#![allow(dead_code)]

pub mod expect;

// Re-exported for `use common::Expect;`. Not every test binary that compiles
// this module opens a fixture, so the re-export is unused in some of them.
#[allow(unused_imports)]
pub use expect::Expect;

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

/// Path segment that marks a read as belonging to the canonical corpus. Ids are
/// recorded relative to the directory that follows it, e.g.
/// `collections/queuecell_spsc_push_pop.json`.
const CONFORMANCE_MARKER: &str = "lazily-spec/conformance/";

fn seen() -> &'static Mutex<HashSet<String>> {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SEEN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// `std::fs::read_to_string` plus a record of any conformance fixture it opens.
pub fn spec_read_to_string<P: AsRef<Path>>(path: P) -> io::Result<String> {
    let path = path.as_ref();
    let result = std::fs::read_to_string(path);
    if result.is_ok() {
        record_conformance_read(path);
    }
    result
}

/// Resolve `path` to absolute and, when it lives in the canonical corpus, append
/// its corpus-relative id to the manifest the first time this process sees it.
pub fn record_conformance_read(path: &Path) {
    let Some(id) = conformance_id(path) else {
        return;
    };
    {
        let Ok(mut seen) = seen().lock() else {
            return;
        };
        if !seen.insert(id.clone()) {
            return;
        }
    }
    let Ok(out) = std::env::var("LAZILY_CONFORMANCE_MANIFEST") else {
        return;
    };
    if out.is_empty() {
        return;
    }
    // Never fail a suite over bookkeeping — an unwritable manifest shows up
    // downstream as missing evidence, which is what the guard wants.
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&out) {
        let _ = f.write_all(format!("{id}\n").as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Per-scenario replay ledger (#lzscenariocoverage)
// ---------------------------------------------------------------------------

/// Which field a scenario's id came from. Carried into the ledger so the guard
/// can REPORT a positional fallback instead of silently accepting it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ScenarioIdSource {
    /// The scenario carries an explicit `id` (the three `stdlib` fixtures).
    Id,
    /// The scenario carries a `name` (28 of the 31 scenario-bearing fixtures).
    Name,
    /// Neither — the id is the 0-based position, spelled `#<n>`.
    Index,
}

impl ScenarioIdSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::Index => "index",
        }
    }
}

/// Resolve a scenario's id: `id`, else `name`, else the positional `#<n>`.
///
/// The order is fixed and identical in every binding — a binding that preferred
/// `name` over `id` would build a ledger that cannot be compared with anyone
/// else's, and the whole point of the corpus is that the nine agree.
pub fn scenario_id(scenario: &serde_json::Value, index: usize) -> (String, ScenarioIdSource) {
    if let Some(id) = scenario.get("id").and_then(|v| v.as_str()) {
        return (id.to_owned(), ScenarioIdSource::Id);
    }
    if let Some(name) = scenario.get("name").and_then(|v| v.as_str()) {
        return (name.to_owned(), ScenarioIdSource::Name);
    }
    (format!("#{index}"), ScenarioIdSource::Index)
}

fn scenarios_seen() -> &'static Mutex<HashSet<String>> {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SEEN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Record that the runner REPLAYED scenario `id` of the fixture at `path`.
///
/// Call it at the top of the loop body, *after* any `continue`, so a scenario
/// the runner steps past does not record itself. Reads outside the canonical
/// corpus are ignored, exactly as [`record_conformance_read`] ignores them.
pub fn record_scenario(path: impl AsRef<Path>, id: &str, source: ScenarioIdSource) {
    let Some(fixture) = conformance_id(path.as_ref()) else {
        return;
    };
    let line = format!("{fixture}\t{id}\t{}", source.as_str());
    {
        let Ok(mut seen) = scenarios_seen().lock() else {
            return;
        };
        if !seen.insert(line.clone()) {
            return;
        }
    }
    let Ok(out) = std::env::var("LAZILY_CONFORMANCE_SCENARIOS") else {
        return;
    };
    if out.is_empty() {
        return;
    }
    // Same contract as the fixture manifest: bookkeeping never fails a suite.
    // An unwritable ledger surfaces downstream as missing evidence.
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&out) {
        let _ = f.write_all(format!("{line}\n").as_bytes());
    }
}

/// The scenarios of `fixture`, recording each id at the moment it is YIELDED.
///
/// This is the seam the contract prefers: recording is automatic, so a runner
/// added later cannot forget it, and a runner that `break`s early records only
/// what it reached.
pub struct Scenarios<'a> {
    path: String,
    items: std::iter::Enumerate<std::slice::Iter<'a, serde_json::Value>>,
}

impl<'a> Iterator for Scenarios<'a> {
    /// `(index, id, scenario)`.
    type Item = (usize, String, &'a serde_json::Value);

    fn next(&mut self) -> Option<Self::Item> {
        let (index, scenario) = self.items.next()?;
        let (id, source) = scenario_id(scenario, index);
        record_scenario(&self.path, &id, source);
        Some((index, id, scenario))
    }
}

/// Iterate `fixture["scenarios"]`, recording each scenario as replayed.
///
/// Panics when the fixture carries no `scenarios` array — a runner that reaches
/// for the array is claiming there is one, and "zero scenarios" is not a replay.
pub fn scenarios<'a>(path: &str, fixture: &'a serde_json::Value) -> Scenarios<'a> {
    let items = fixture["scenarios"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: fixture carries no `scenarios` array"));
    assert!(
        !items.is_empty(),
        "{path}: a replay of zero scenarios is not a replay"
    );
    Scenarios {
        path: path.to_owned(),
        items: items.iter().enumerate(),
    }
}

/// Look a scenario up by `name` (or `id`) and record it as replayed.
///
/// For the many runners that address scenarios individually rather than in a
/// loop — the record still happens exactly once, at the point of replay.
pub fn scenario_by_name<'a>(
    path: &str,
    fixture: &'a serde_json::Value,
    name: &str,
) -> &'a serde_json::Value {
    let items = fixture["scenarios"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: fixture carries no `scenarios` array"));
    for (index, scenario) in items.iter().enumerate() {
        let (id, source) = scenario_id(scenario, index);
        if id == name {
            record_scenario(path, &id, source);
            return scenario;
        }
    }
    panic!("{path}: scenario `{name}` not found");
}

/// Address a scenario by position and record it as replayed. For fixtures whose
/// scenarios carry no identifier at all, and for runners that legitimately index.
pub fn scenario_at<'a>(
    path: &str,
    fixture: &'a serde_json::Value,
    index: usize,
) -> &'a serde_json::Value {
    let items = fixture["scenarios"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: fixture carries no `scenarios` array"));
    let scenario = items
        .get(index)
        .unwrap_or_else(|| panic!("{path}: no scenario at index {index}"));
    let (id, source) = scenario_id(scenario, index);
    record_scenario(path, &id, source);
    scenario
}

fn conformance_id(path: &Path) -> Option<String> {
    // `canonicalize` needs the file to exist and gives the cleanest string;
    // `absolute` is the lexical fallback (it keeps `..`, which is harmless here
    // because the marker still matches inside `.../lazily-rs/../lazily-spec/...`).
    let candidates = [
        std::fs::canonicalize(path).ok(),
        std::path::absolute(path).ok(),
    ];
    for candidate in candidates.into_iter().flatten() {
        let text = candidate.to_string_lossy().replace('\\', "/");
        if let Some(idx) = text.find(CONFORMANCE_MARKER) {
            return Some(text[idx + CONFORMANCE_MARKER.len()..].to_owned());
        }
    }
    None
}
