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
//! iteration helpers ([`scenarios`], [`scenario_by_id`], [`scenario_at`]) so a
//! new runner cannot forget to record.
//!
//! Ids resolve `id`, else `name`, identically in every binding. There is no
//! third option (`#lzspecscenarioids`): the positional `#<n>` fallback existed
//! so this rung was not blocked on a shared-corpus edit, and it was load-bearing
//! for exactly one fixture — `collections/mergecell_algebra.json`, whose three
//! scenarios were distinguishable only by `policy`. They now carry ids, and
//! lazily-spec's `scenario-identity-check` keeps every scenario identified, so an
//! unidentified one is a hard failure here rather than a note. A ledger entry
//! recorded BY POSITION silently rebinds to a different scenario when the corpus
//! array is reordered, which is precisely the kind of quietly-wrong evidence this
//! ladder exists to prevent.

#![allow(dead_code)]

pub mod expect;

// Re-exported for `use common::Expect;`. Not every test binary that compiles
// this module opens a fixture, so the re-export is unused in some of them.
#[allow(unused_imports)]
pub use expect::{Expect, ProseLedger};

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// Path segment that marks a read as belonging to the canonical corpus. Ids are
/// recorded relative to the directory that follows it, e.g.
/// `collections/queuecell_spsc_push_pop.json`.
const CONFORMANCE_MARKER: &str = "lazily-spec/conformance/";

/// The corpus location when nothing overrides it.
const DEFAULT_CONFORMANCE_ROOT: &str = "../lazily-spec/conformance";

/// Corpus root for this run, or `None` when the default applies.
///
/// `LAZILY_SPEC_CONFORMANCE_DIR` names the corpus directly; `LAZILY_SPEC_DIR`
/// names the sibling checkout. Resolved once — the corpus a run reads must not
/// change halfway through it.
pub fn conformance_root_override() -> Option<&'static str> {
    static ROOT: OnceLock<Option<String>> = OnceLock::new();
    ROOT.get_or_init(|| {
        std::env::var("LAZILY_SPEC_CONFORMANCE_DIR")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                std::env::var("LAZILY_SPEC_DIR")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .map(|v| format!("{v}/conformance"))
            })
    })
    .as_deref()
}

/// Rewrite a corpus path onto the overridden root (`#lzoverrideallrunnersaudit`).
///
/// Every conformance test in this repo declares its own
/// `const SPEC_DIR: &str = "../lazily-spec/conformance/<area>"` — 47 sites across
/// 41 files — and formats fixture paths from it. Nothing read an environment
/// variable, so `LAZILY_SPEC_CONFORMANCE_DIR` moved the coverage guard and NOT ONE
/// replay: truncating a fixture in a scratch corpus and pointing the suite at it
/// reddened 0 of 25 areas, while the same bytes read directly redden all 25.
///
/// The redirect lives HERE, at the single seam every corpus read already passes
/// through, rather than being threaded into 47 constants. That is deliberate and
/// it is the stronger property: a rewrite of the constants fixes the sites someone
/// remembered, while this cannot miss one and covers every future runner that uses
/// the seam — which `spec_read_to_string`'s own doc comment already requires.
///
/// It matches on the marker rather than on the literal default, so an absolute or
/// canonicalised path redirects the same way a relative one does.
fn redirect(path: &Path) -> Option<PathBuf> {
    let root = conformance_root_override()?;
    let text = path.to_string_lossy().replace('\\', "/");
    let idx = text.find(CONFORMANCE_MARKER)?;
    Some(Path::new(root).join(&text[idx + CONFORMANCE_MARKER.len()..]))
}

/// `path` with any corpus override applied — for presence probes, which check a
/// path without reading it and would otherwise test the DEFAULT corpus while the
/// suite replays the overridden one.
pub fn spec_path<P: AsRef<Path>>(path: P) -> PathBuf {
    let path = path.as_ref();
    redirect(path).unwrap_or_else(|| path.to_path_buf())
}

/// The corpus directory itself, honouring the override.
pub fn spec_root() -> PathBuf {
    PathBuf::from(conformance_root_override().unwrap_or(DEFAULT_CONFORMANCE_ROOT))
}

fn seen() -> &'static Mutex<HashSet<String>> {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SEEN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// `std::fs::read_to_string` plus a record of any conformance fixture it opens
/// and an inventory of every `assertions` block that fixture carries.
pub fn spec_read_to_string<P: AsRef<Path>>(path: P) -> io::Result<String> {
    let requested = path.as_ref();
    let redirected = redirect(requested);
    let path = redirected.as_deref().unwrap_or(requested);
    let result = std::fs::read_to_string(path);
    if let Ok(text) = &result {
        record_conformance_read(path);
        if let Some(id) = conformance_id(path) {
            record_declared_blocks(&id, text);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Rung 0: the assertion-block BIND ledger (`#lznullformblind`)
// ---------------------------------------------------------------------------
//
// Every rung above this one is scoped to blocks a runner already BOUND. The
// unconsumed-key guard fires on a key nothing read; the unasserted-key guard
// fires on a key read and discarded; the prose ledger fires on a discharge that
// names nothing. None of them can fire for a block no runner ever handed to the
// tracker, because there is no tracker — its keys are not unread, nothing reads
// them, and the fixture reports exactly nothing. lazily-dart found two such
// blocks carrying eight silent keys, one of them a load-bearing anti-spoof
// invariant.
//
// So the loader inventories every `assertions` block at READ time and `Expect`
// books one as BOUND at construction. The two sides are matched by the block's
// CONTENT, never by its `where` label: runners spell those labels inconsistently
// (`assertions`, `frames[warn].assertions`, `scenarios[3].expect`) and a
// label-keyed ledger would silently miss the mismatches instead of reporting
// them. `scripts/check-conformance-coverage.sh` fails on any inventoried block
// with no bind.

/// Ledger of declared-versus-bound `assertions` blocks. Same contract as the
/// manifest: ABSOLUTE path, appended by every test binary, truncated once.
const BLOCK_LEDGER_ENV: &str = "LAZILY_CONFORMANCE_BLOCKS";

/// FNV-1a over exact bytes, rendered as sixteen lowercase hexadecimal digits.
pub fn fnv1a64_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// FNV-1a over a block's canonical JSON. A content key, so a block is booked by
/// what it SAYS rather than by what a runner chose to call it.
fn block_digest(value: &serde_json::Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_default();
    fnv1a64_hex(text.as_bytes())
}

fn append_block_record(line: &str) {
    let Ok(out) = std::env::var(BLOCK_LEDGER_ENV) else {
        return;
    };
    if out.is_empty() {
        return;
    }
    // Bookkeeping never fails a suite; an unwritable ledger surfaces downstream
    // as missing evidence, which is the outcome the guard wants.
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&out) {
        let _ = f.write_all(format!("{line}\n").as_bytes());
    }
}

/// Inventory every `assertions` block in a freshly read fixture: the top-level
/// one plus any carried per-frame or per-scenario.
fn record_declared_blocks(id: &str, text: &str) {
    let Ok(doc) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    let declare = |where_: String, block: &serde_json::Value| {
        append_block_record(&format!(
            "declared\t{id}\t{}\t{where_}",
            block_digest(block)
        ));
    };
    if let Some(block) = doc.get("assertions").filter(|v| v.is_object()) {
        declare("assertions".to_owned(), block);
    }
    for container in ["frames", "scenarios", "rejects"] {
        let Some(items) = doc.get(container).and_then(|v| v.as_array()) else {
            continue;
        };
        for (i, item) in items.iter().enumerate() {
            if let Some(block) = item.get("assertions").filter(|v| v.is_object()) {
                declare(format!("{container}[{i}].assertions"), block);
            }
        }
    }
}

/// Book an `assertions` block as BOUND. Called by `Expect::new`, so every block
/// a runner hands to the tracker is booked whatever it calls it.
pub fn record_block_bind(value: &serde_json::Value) {
    if !value.is_object() {
        return;
    }
    append_block_record(&format!("bound\t{}", block_digest(value)));
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
/// reads the same identity the runner recorded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ScenarioIdSource {
    /// The scenario carries an explicit `id`.
    Id,
    /// The scenario carries a `name`.
    Name,
}

impl ScenarioIdSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
        }
    }
}

/// Resolve a scenario's id: `id`, else `name`. There is no third option.
///
/// The order is fixed and identical in every binding — a binding that preferred
/// `name` over `id` would build a ledger that cannot be compared with anyone
/// else's, and the whole point of the corpus is that the nine agree.
///
/// As of `#recommendedconformanceco` the canonical spelling is settled: `id`,
/// with `name` demoted to an optional human label, and every scenario in the
/// corpus carries one. The `name` leg is therefore no longer load-bearing for
/// the canonical corpus — lazily-spec's `scenario-identity-check` requires a
/// unique snake_case `id` on every scenario — and survives only for
/// out-of-corpus fixtures a runner may read. Keying identity on `name` was the
/// same defect as keying it on position: 35 scenarios named themselves with a
/// prose sentence, so a copy-edit silently rebound the entry.
///
/// The positional `#<n>` fallback is GONE (`#lzspecscenarioids`). It existed so
/// this rung was not blocked on a shared-corpus edit, and it was load-bearing for
/// exactly one fixture; the corpus now identifies every scenario, and
/// lazily-spec's `scenario-identity-check` keeps it that way. Keeping the
/// fallback would leave the ledger able to record a scenario BY POSITION, where
/// inserting one ahead of it silently rebinds that entry — and every excuse
/// naming it — to a different scenario, with nothing turning red. A fallback that
/// nothing needs is a hole waiting to become load-bearing again, so an
/// unidentified scenario is now a hard failure.
pub fn scenario_id(scenario: &serde_json::Value, index: usize) -> (String, ScenarioIdSource) {
    if let Some(id) = scenario
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        return (id.to_owned(), ScenarioIdSource::Id);
    }
    if let Some(name) = scenario
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        return (name.to_owned(), ScenarioIdSource::Name);
    }
    panic!(
        "scenario at index {index} carries neither `id` nor `name`. The replay ledger \
         would have to record it by POSITION, where inserting a scenario ahead of it \
         silently rebinds that entry to a different scenario. Give it a stable id \
         upstream in lazily-spec (#lzspecscenarioids)."
    )
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

/// Keys that IDENTIFY or narrate a scenario rather than drive one
/// (`#lzscenariobodyskip`). Reading only these is *looking at the label*, not
/// replaying: a dispatch chain that reads `name`, matches no arm and falls
/// through has replayed nothing, and a by-name lookup walks past every scenario
/// ahead of its match. Booking on those reads is what let a skipped body book
/// itself.
///
/// No scenario in the corpus carries only these, so every one is reachable
/// through some payload key.
pub const SCENARIO_LABEL_KEYS: [&str; 9] = [
    "comment",
    "description",
    "id",
    "label",
    "name",
    "note",
    "notes",
    "reason",
    "why",
];

/// One scenario, booked as replayed on the first read of its PAYLOAD.
///
/// Yielding is not replaying (`#lzscenariobodyskip`). The ledger used to book at
/// the moment the iterator handed a scenario over, which cannot tell a loop body
/// that ran from one that `continue`d — the iterator sees the same thing either
/// way — so a skipped scenario booked itself and this rung stayed silent about
/// the very defect it exists for. lazily-py proved that against the contract's
/// own probe; this is the port of its fix.
///
/// The `steps`/`ops`/`frames`/`expect` of a scenario are read only by a runner
/// about to replay it, so those reads book. `id`/`name`/`description` are what a
/// skip reads on its way past, so those stay silent.
pub struct ScenarioView<'a> {
    path: String,
    id: String,
    source: ScenarioIdSource,
    scenario: &'a serde_json::Value,
}

impl<'a> ScenarioView<'a> {
    fn book(&self) {
        record_scenario(&self.path, &self.id, self.source);
    }

    fn touch(&self, key: &str) {
        if !SCENARIO_LABEL_KEYS.contains(&key) {
            self.book();
        }
    }

    /// The resolved id. A label read: never books.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Look a key up, booking unless it is a label key.
    pub fn get(&self, key: &str) -> Option<&'a serde_json::Value> {
        self.touch(key);
        self.scenario.get(key)
    }

    /// The whole scenario, BOOKED — for handing the payload to a replay helper.
    /// Reaching for the entire object is the strongest statement a runner can
    /// make that it is about to replay this scenario, so it books unconditionally.
    pub fn value(&self) -> &'a serde_json::Value {
        self.book();
        self.scenario
    }

    /// The whole scenario WITHOUT booking, for inspecting a label before deciding
    /// whether to replay. Use it only where a read really is a look, never as a
    /// way to reach the payload — that is the skip this rung exists to catch.
    pub fn peek(&self) -> &'a serde_json::Value {
        self.scenario
    }
}

impl std::ops::Index<&str> for ScenarioView<'_> {
    type Output = serde_json::Value;

    fn index(&self, key: &str) -> &Self::Output {
        self.touch(key);
        &self.scenario[key]
    }
}

/// The scenarios of `fixture`, each booked when its payload is first read.
///
/// This is the seam the contract prefers: booking is automatic, so a runner added
/// later cannot forget it, a `break` leaves the rest unbooked, and — unlike the
/// yield-time booking this replaced — a `continue` past the payload leaves that
/// one unbooked too.
pub struct Scenarios<'a> {
    path: String,
    items: std::iter::Enumerate<std::slice::Iter<'a, serde_json::Value>>,
}

impl<'a> Iterator for Scenarios<'a> {
    /// `(index, id, scenario)`.
    type Item = (usize, String, ScenarioView<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (index, scenario) = self.items.next()?;
        let (id, source) = scenario_id(scenario, index);
        Some((
            index,
            id.clone(),
            ScenarioView {
                path: self.path.clone(),
                id,
                source,
                scenario,
            },
        ))
    }
}

/// Iterate `fixture["scenarios"]`.
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

/// Look a scenario up by `name` (or `id`).
///
/// The LOOKUP does not book (`#lzscenariobodyskip`): matching on the id walks past
/// every scenario ahead of it, and a scenario selected and then not replayed is
/// not replayed. The returned view books when its payload is read.
pub fn scenario_by_id<'a>(
    path: &str,
    fixture: &'a serde_json::Value,
    wanted: &str,
) -> ScenarioView<'a> {
    let items = fixture["scenarios"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: fixture carries no `scenarios` array"));
    for (index, scenario) in items.iter().enumerate() {
        let (id, source) = scenario_id(scenario, index);
        if id == wanted {
            return ScenarioView {
                path: path.to_owned(),
                id,
                source,
                scenario,
            };
        }
    }
    panic!("{path}: scenario `{wanted}` not found");
}

/// Address a scenario by position. For fixtures whose scenarios carry no
/// identifier at all, and for runners that legitimately index. Books on payload
/// read, exactly as the other two seams do.
pub fn scenario_at<'a>(
    path: &str,
    fixture: &'a serde_json::Value,
    index: usize,
) -> ScenarioView<'a> {
    let items = fixture["scenarios"]
        .as_array()
        .unwrap_or_else(|| panic!("{path}: fixture carries no `scenarios` array"));
    let scenario = items
        .get(index)
        .unwrap_or_else(|| panic!("{path}: no scenario at index {index}"));
    let (id, source) = scenario_id(scenario, index);
    ScenarioView {
        path: path.to_owned(),
        id,
        source,
        scenario,
    }
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
        // Under an override the scratch corpus need not be named `lazily-spec`, so
        // the marker cannot appear. Strip the resolved root instead — otherwise the
        // manifest records nothing and the coverage guard reports missing evidence
        // for a run that in fact replayed everything (`#lzoverrideallrunnersaudit`).
        if let Some(root) = conformance_root_override() {
            let root = std::fs::canonicalize(root)
                .ok()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_else(|| root.replace('\\', "/"));
            if let Some(rest) = text.strip_prefix(&format!("{root}/")) {
                return Some(rest.to_owned());
            }
        }
    }
    None
}
