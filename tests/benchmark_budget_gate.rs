//! The benchmark budget gate has no skip path (#vnmr).
//!
//! `benchmark-check` used to print a loud "NO BENCHMARK BUDGET WAS ENFORCED BY
//! THIS RUN" banner and then exit 0 whenever `target/criterion` was absent. The
//! banner was honest; the exit code was not, and the exit code is the part every
//! caller reads. A green `make check` on a pruned target dir said "budgets
//! verified" about budgets nothing had measured.
//!
//! These tests assert the CONTRACT BY EXIT CODE, not by printed text, because
//! printed text is exactly what the old bug got right while still passing:
//!
//! | exit | meaning |
//! | ---- | ------- |
//! |    0 | budgets measured against this source tree and green |
//! |    2 | evidence absent — nothing measured, so nothing can be green |
//! |    3 | evidence stale — it measured a different source tree |
//!
//! Each case drives the real `scripts/update-benchmark-results.py` with the real
//! `--check --budgets-only` arguments `make benchmark-check` uses, differing only
//! in which evidence paths it is pointed at.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const EXIT_BUDGET_REGRESSION: i32 = 1;
const EXIT_EVIDENCE_ABSENT: i32 = 2;
const EXIT_EVIDENCE_STALE: i32 = 3;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lazily-benchmark-gate-{name}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("scratch dir should be creatable");
    dir
}

/// Run the gate exactly as `make benchmark-check` does, against caller-chosen
/// evidence paths.
fn gate_exit_code(criterion_dir: &Path, profile: &Path, manifest: &Path) -> i32 {
    let output = Command::new("python3")
        .current_dir(repo_root())
        .args([
            "scripts/update-benchmark-results.py",
            "--check",
            "--budgets-only",
        ])
        .arg("--criterion-dir")
        .arg(criterion_dir)
        .arg("--profile-output")
        .arg(profile)
        .arg("--evidence-manifest")
        .arg(manifest)
        .output()
        .expect("python3 should be available to run the benchmark budget gate");

    output
        .status
        .code()
        .expect("the benchmark budget gate should exit normally, not by signal")
}

/// A minimal but structurally real Criterion estimate, so `discover_results`
/// finds evidence and the gate proceeds to the freshness question.
fn write_fake_criterion_evidence(criterion_dir: &Path) {
    let case_dir = criterion_dir.join("cached_reads").join("warm").join("new");
    fs::create_dir_all(&case_dir).expect("criterion case dir should be creatable");
    fs::write(
        case_dir.join("estimates.json"),
        r#"{"mean":{"point_estimate":42.0,"confidence_interval":{"lower_bound":41.0,"upper_bound":43.0}}}"#,
    )
    .expect("estimates.json should be writable");
}

/// The fingerprint the gate computes for the tree this test is compiled from,
/// obtained from the gate's own implementation rather than reimplemented here —
/// a second implementation would drift and the test would stop testing the gate.
fn current_source_fingerprint() -> String {
    let output = Command::new("python3")
        .current_dir(repo_root())
        // Importing the gate as a module would otherwise leave a
        // `scripts/__pycache__/` behind in the working tree.
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg("-c")
        .arg(
            "import importlib.util, sys\n\
             spec = importlib.util.spec_from_file_location('gate', 'scripts/update-benchmark-results.py')\n\
             gate = importlib.util.module_from_spec(spec)\n\
             sys.modules['gate'] = gate\n\
             spec.loader.exec_module(gate)\n\
             print(gate.source_fingerprint(gate.source_file_hashes()))\n",
        )
        .output()
        .expect("python3 should be available to compute the source fingerprint");
    assert!(
        output.status.success(),
        "fingerprint helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("fingerprint should be utf-8")
        .trim()
        .to_string()
}

fn write_manifest(path: &Path, fingerprint: &str, source_files: &str) {
    fs::write(
        path,
        format!(
            r#"{{"version":1,"mode":"quick","command":[],"recorded_at":"2026-01-01T00:00:00+00:00","source_fingerprint":"{fingerprint}","source_files":{source_files}}}"#
        ),
    )
    .expect("manifest should be writable");
}

/// SPEC: absent benchmark evidence is a failure, not a skip. Deleting or never
/// producing the evidence must never be a way to reach a green budget check.
#[test]
fn absent_evidence_fails_the_gate() {
    let dir = scratch("absent");

    let code = gate_exit_code(
        &dir.join("criterion-that-does-not-exist"),
        &dir.join("profile-that-does-not-exist.csv"),
        &dir.join("manifest-that-does-not-exist.json"),
    );

    assert_eq!(
        code, EXIT_EVIDENCE_ABSENT,
        "a checkout with no benchmark evidence must FAIL the budget gate; \
         exiting 0 here reports \"budgets verified\" for budgets nothing measured"
    );
}

/// SPEC: an existing-but-empty evidence directory is a broken measurement, not a
/// fresh checkout, and fails the same way. Otherwise `mkdir target/criterion`
/// would be the escape hatch that deleting it used to be.
#[test]
fn empty_evidence_directory_fails_the_gate() {
    let dir = scratch("empty");
    let criterion_dir = dir.join("criterion");
    fs::create_dir_all(&criterion_dir).expect("criterion dir should be creatable");

    let code = gate_exit_code(
        &criterion_dir,
        &dir.join("profile.csv"),
        &dir.join("manifest.json"),
    );

    assert_eq!(
        code, EXIT_EVIDENCE_ABSENT,
        "an evidence directory that yields no estimates must FAIL the budget gate"
    );
}

/// SPEC: evidence that measured a different source tree fails with a DISTINCT
/// outcome. "Measure, edit the code, keep trusting the old numbers" is the rot
/// the freshness signal exists to catch, and it is not the same failure as never
/// having measured at all.
#[test]
fn stale_evidence_fails_the_gate_distinctly() {
    let dir = scratch("stale");
    let criterion_dir = dir.join("criterion");
    write_fake_criterion_evidence(&criterion_dir);
    let profile = dir.join("profile.csv");
    fs::write(&profile, "profile\n").expect("profile should be writable");
    let manifest = dir.join("manifest.json");
    write_manifest(
        &manifest,
        // A fingerprint no checkout can produce: this evidence measured
        // something else.
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        r#"{"src/context.rs":"0000000000000000000000000000000000000000000000000000000000000000"}"#,
    );

    let code = gate_exit_code(&criterion_dir, &profile, &manifest);

    assert_eq!(
        code, EXIT_EVIDENCE_STALE,
        "evidence recorded against a different source tree must FAIL the budget \
         gate, with an outcome distinct from absent evidence"
    );
    assert_ne!(
        EXIT_EVIDENCE_STALE, EXIT_EVIDENCE_ABSENT,
        "the two failure modes must stay distinguishable"
    );
}

/// SPEC: the freshness signal is a content hash of the measured sources, so
/// evidence recorded against THIS tree passes the freshness check. Asserted as
/// "not one of the evidence failures" rather than "exit 0", because this fixture
/// deliberately carries no real instrumentation counters — it isolates the
/// freshness question from the budget question.
#[test]
fn evidence_matching_this_tree_passes_the_freshness_check() {
    let dir = scratch("fresh");
    let criterion_dir = dir.join("criterion");
    write_fake_criterion_evidence(&criterion_dir);
    let profile = dir.join("profile.csv");
    fs::write(&profile, "profile\n").expect("profile should be writable");
    let manifest = dir.join("manifest.json");
    write_manifest(&manifest, &current_source_fingerprint(), "{}");

    let code = gate_exit_code(&criterion_dir, &profile, &manifest);

    assert_ne!(
        code, EXIT_EVIDENCE_ABSENT,
        "present evidence must not be reported as absent"
    );
    assert_ne!(
        code, EXIT_EVIDENCE_STALE,
        "evidence whose fingerprint matches this checkout must not be reported as stale"
    );
}

/// SPEC: the freshness signal is a CONTENT hash, not an mtime. `git checkout`,
/// `git clone`, and `touch` all move mtimes without changing bytes, and
/// restoring a backup moves an mtime backwards while the bytes differ — an mtime
/// comparison would answer a question nobody asked.
#[test]
fn freshness_signal_is_a_content_hash_not_an_mtime() {
    let script = fs::read_to_string(repo_root().join("scripts/update-benchmark-results.py"))
        .expect("the gate script should be readable");

    assert!(
        script.contains("def source_file_hashes"),
        "the gate should fingerprint source CONTENT"
    );
    assert!(
        script.contains("hashlib.sha256"),
        "the fingerprint should be a content hash"
    );
    assert!(
        !script.contains("st_mtime") && !script.contains(".stat().st_mtime"),
        "the gate must not fall back to mtime comparison"
    );
}

// ---------------------------------------------------------------------------
// Ceilings are derived from a recorded spread (#lzbenchbudgetheadroom).
//
// The gate used to carry hand-typed ceilings, and it had them exactly backwards:
// loose where the counter is perfectly deterministic (`dependency_edge <= 1600`
// for a counter that is always 64 — a 25x regression sailed through) and tight
// where the counter is pure scheduling noise (`set_cell_invalidation <= 16` for
// a counter measured from 1 to 256 — it reddened on a busy machine at roughly
// 0.4 percent headroom). A gate that reddens on noise trains everyone to ignore
// it, which is how a real regression gets waived as a flake.
//
// These tests drive the real gate with synthesized instrumentation profiles, so
// they assert what the SCRIPT does with a counter value, not what a table in
// this file says it should. The classification of each counter comes from the
// gate's own module rather than being restated here: a second copy would drift,
// and a drifted copy is a test that passes while proving nothing.
// ---------------------------------------------------------------------------

/// One gated counter as the gate itself classifies it.
struct GateCounter {
    profile: String,
    counter: String,
    samples: u64,
    minimum: i64,
    maximum: i64,
    classification: String,
    /// `None` for a scheduling-dominated counter, which is deliberately not enforced.
    ceiling: Option<i64>,
}

/// Ask the gate for its own budget table and required latency cases.
///
/// Read out of the script rather than restated in Rust for the same reason
/// `current_source_fingerprint` shells out: a transcription cannot detect drift
/// from the thing it transcribed.
fn gate_metadata() -> (Vec<GateCounter>, Vec<(String, String)>) {
    let output = Command::new("python3")
        .current_dir(repo_root())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg("-c")
        .arg(
            "import importlib.util, sys, json\n\
             spec = importlib.util.spec_from_file_location('gate', 'scripts/update-benchmark-results.py')\n\
             gate = importlib.util.module_from_spec(spec)\n\
             sys.modules['gate'] = gate\n\
             spec.loader.exec_module(gate)\n\
             counters = [\n\
             \x20   [b.profile, name, s.samples, s.minimum, s.maximum, s.classification,\n\
             \x20    -1 if s.ceiling is None else s.ceiling]\n\
             \x20   for b in gate.REGRESSION_BUDGETS for name, s in gate.budget_counters(b)\n\
             ]\n\
             print(json.dumps({'counters': counters,\n\
             \x20                'latency': [list(c) for c in gate.REQUIRED_LATENCY_CASES]}))\n",
        )
        .output()
        .expect("python3 should be available to read the gate's budget table");
    assert!(
        output.status.success(),
        "budget table helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).expect("budget table should be utf-8");
    parse_gate_metadata(&text)
}

/// A deliberately small hand parser for the fixed JSON shape emitted above, so
/// this test binary needs no serde feature to run.
fn parse_gate_metadata(text: &str) -> (Vec<GateCounter>, Vec<(String, String)>) {
    let counters_body = between(text, "\"counters\": [[", "]], \"latency\"");
    let mut counters = Vec::new();
    for row in counters_body.split("], [") {
        let fields = split_json_row(row);
        assert_eq!(fields.len(), 7, "unexpected counter row: {row}");
        let ceiling: i64 = fields[6].parse().expect("ceiling should be an integer");
        counters.push(GateCounter {
            profile: fields[0].clone(),
            counter: fields[1].clone(),
            samples: fields[2].parse().expect("samples should be an integer"),
            minimum: fields[3].parse().expect("minimum should be an integer"),
            maximum: fields[4].parse().expect("maximum should be an integer"),
            classification: fields[5].clone(),
            ceiling: (ceiling >= 0).then_some(ceiling),
        });
    }

    let latency_body = between(text, "\"latency\": [[", "]]}");
    let latency = latency_body
        .split("], [")
        .map(|row| {
            let fields = split_json_row(row);
            assert_eq!(fields.len(), 2, "unexpected latency row: {row}");
            (fields[0].clone(), fields[1].clone())
        })
        .collect();

    (counters, latency)
}

fn between<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
    let from = text.find(start).expect("start marker should be present") + start.len();
    let to = text[from..]
        .find(end)
        .expect("end marker should be present")
        + from;
    &text[from..to]
}

fn split_json_row(row: &str) -> Vec<String> {
    row.split(", ")
        .map(|field| field.trim().trim_matches('"').to_string())
        .collect()
}

/// Write a profile CSV in which every gated counter sits at `value_for`.
///
/// The CSV shape mirrors `examples/instrumentation_profile.rs` output, which is
/// what the gate parses in production.
fn write_profile_csv(
    path: &Path,
    counters: &[GateCounter],
    value_for: impl Fn(&GateCounter) -> i64,
) {
    let mut profiles: Vec<&str> = Vec::new();
    for counter in counters {
        if !profiles.contains(&counter.profile.as_str()) {
            profiles.push(&counter.profile);
        }
    }

    let mut csv = String::from(
        "profile,node_allocations,slot_recomputes,duplicate_speculative_recomputes,\
         dependency_edges_added,dependency_edges_removed,effect_queue_pushes,\
         max_effect_queue_depth,lock_acquisitions,lock_wait_nanos,lock_hold_nanos,\
         sidecar_invalidation_frontiers,sidecar_dirty_marks,sidecar_invalidation_fallbacks,\
         dirty_epoch_advances,lock_attribution\n",
    );
    for profile in profiles {
        let rows: Vec<&GateCounter> = counters.iter().filter(|c| c.profile == profile).collect();
        let total = rows
            .iter()
            .find(|c| c.counter == "lock_acquisitions")
            .map(|c| value_for(c))
            .expect("every budgeted profile carries a total counter");
        let attribution = rows
            .iter()
            .filter(|c| c.counter != "lock_acquisitions")
            .map(|c| format!("{}={}:0:0", c.counter, value_for(c)))
            .collect::<Vec<_>>()
            .join("|");
        csv.push_str(&format!(
            "{profile},0,0,0,0,0,0,0,{total},0,0,0,0,0,0,{attribution}\n"
        ));
    }
    fs::write(path, csv).expect("profile csv should be writable");
}

/// Criterion sample files for every required latency case, so a budget test
/// reaches the budget check instead of failing earlier on missing latency rows.
fn write_required_latency_evidence(criterion_dir: &Path, cases: &[(String, String)]) {
    for (group, case) in cases {
        let mut dir = criterion_dir.join(group);
        for part in case.split(" / ") {
            dir = dir.join(part);
        }
        let dir = dir.join("new");
        fs::create_dir_all(&dir).expect("criterion latency dir should be creatable");
        fs::write(
            dir.join("sample.json"),
            r#"{"iters":[1.0,1.0,1.0],"times":[1000.0,1100.0,1200.0]}"#,
        )
        .expect("sample.json should be writable");
    }
}

/// Evidence that passes every check EXCEPT the budgets, so each test below
/// isolates one counter value as the only thing that can change the exit code.
fn budget_only_fixture(
    name: &str,
    counters: &[GateCounter],
    latency: &[(String, String)],
) -> (PathBuf, PathBuf, PathBuf) {
    let dir = scratch(name);
    let criterion_dir = dir.join("criterion");
    write_fake_criterion_evidence(&criterion_dir);
    write_required_latency_evidence(&criterion_dir, latency);
    let profile = dir.join("profile.csv");
    let manifest = dir.join("manifest.json");
    write_manifest(&manifest, &current_source_fingerprint(), "{}");
    let _ = counters;
    (criterion_dir, profile, manifest)
}

/// SPEC: a profile sitting at every recorded maximum is green. This is the
/// baseline the mutation tests below perturb — without it, a test that expects a
/// FAILURE proves nothing, because a gate that fails on everything would pass it.
#[test]
fn evidence_at_every_recorded_maximum_passes_the_budgets() {
    let (counters, latency) = gate_metadata();
    let (criterion_dir, profile, manifest) = budget_only_fixture("baseline", &counters, &latency);
    write_profile_csv(&profile, &counters, |counter| counter.maximum);

    assert_eq!(
        gate_exit_code(&criterion_dir, &profile, &manifest),
        0,
        "a run at every counter's recorded maximum must be green; the recorded \
         spread is what the machine actually produces, so a ceiling under it \
         would redden on noise"
    );
}

/// SPEC: a deterministic counter is enforced EXACTLY. Its spread is zero across
/// idle, loaded and 2-core-pinned runs, so one extra lock acquisition is signal,
/// not scheduling — and these are the counters that carry the regression signal
/// the whole gate exists for.
#[test]
fn deterministic_counters_are_enforced_exactly() {
    let (counters, latency) = gate_metadata();
    let deterministic: Vec<&GateCounter> = counters
        .iter()
        .filter(|c| c.classification == "deterministic")
        .collect();
    assert!(
        !deterministic.is_empty(),
        "the budget table must classify some counters as deterministic, or the \
         gate has no regression signal left at all"
    );

    for target in &deterministic {
        assert_eq!(
            target.ceiling,
            Some(target.maximum),
            "{}/{}: a deterministic counter must be enforced at its recorded \
             value with no slack",
            target.profile,
            target.counter
        );
    }

    // Drive the real gate for one of them: a single extra acquisition must fail.
    let target = deterministic
        .iter()
        .find(|c| c.maximum > 0)
        .expect("at least one deterministic counter should be non-zero");
    let (criterion_dir, profile, manifest) =
        budget_only_fixture("deterministic", &counters, &latency);
    write_profile_csv(&profile, &counters, |counter| {
        if counter.profile == target.profile && counter.counter == target.counter {
            counter.maximum + 1
        } else {
            counter.maximum
        }
    });

    assert_eq!(
        gate_exit_code(&criterion_dir, &profile, &manifest),
        EXIT_BUDGET_REGRESSION,
        "one extra acquisition on the deterministic counter {}/{} must FAIL the \
         gate; a counter with zero spread over 750 runs has no noise to absorb",
        target.profile,
        target.counter
    );
}

/// SPEC: a scheduling-sensitive counter gets headroom proportional to its OWN
/// measured variance — one full observed range above the observed maximum — and
/// that headroom is a real boundary, not an open door.
#[test]
fn scheduling_sensitive_headroom_is_proportional_to_the_observed_spread() {
    let (counters, latency) = gate_metadata();
    let target_index = counters
        .iter()
        .position(|c| c.classification == "scheduling_sensitive")
        .expect("the budget table must classify some counters as scheduling-sensitive");
    let (target_profile, target_counter, ceiling) = {
        let target = &counters[target_index];
        (
            target.profile.clone(),
            target.counter.clone(),
            target
                .ceiling
                .expect("a scheduling-sensitive counter is enforced"),
        )
    };

    for counter in counters
        .iter()
        .filter(|c| c.classification == "scheduling_sensitive")
    {
        assert_eq!(
            counter.ceiling,
            Some(counter.maximum + (counter.maximum - counter.minimum)),
            "{}/{}: headroom must be one full observed range above the observed \
             maximum, derived from the recording rather than chosen",
            counter.profile,
            counter.counter
        );
    }

    let (criterion_dir, profile, manifest) =
        budget_only_fixture("sensitive-at", &counters, &latency);
    write_profile_csv(&profile, &counters, |counter| {
        if counter.profile == target_profile && counter.counter == target_counter {
            ceiling
        } else {
            counter.maximum
        }
    });
    assert_eq!(
        gate_exit_code(&criterion_dir, &profile, &manifest),
        0,
        "a value exactly at the derived ceiling must pass"
    );

    let (criterion_dir, profile, manifest) =
        budget_only_fixture("sensitive-over", &counters, &latency);
    write_profile_csv(&profile, &counters, |counter| {
        if counter.profile == target_profile && counter.counter == target_counter {
            ceiling + 1
        } else {
            counter.maximum
        }
    });
    assert_eq!(
        gate_exit_code(&criterion_dir, &profile, &manifest),
        EXIT_BUDGET_REGRESSION,
        "one past the derived ceiling must FAIL; headroom proportional to the \
         observed spread is still a boundary"
    );
}

/// SPEC: a scheduling-dominated counter is NOT enforced, and this test exists to
/// keep that deliberate blind spot visible.
///
/// These counters were measured from 1 to 256 — the spread exceeds half the
/// magnitude, so no ceiling over them can tell a regression from a busy machine.
/// Gating them anyway is what produced the flakes. Their value is still recorded
/// and printed, so the blind spot is documented rather than silently green.
#[test]
fn scheduling_dominated_counters_are_recorded_but_not_enforced() {
    let (counters, latency) = gate_metadata();
    let dominated: Vec<&GateCounter> = counters
        .iter()
        .filter(|c| c.classification == "scheduling_dominated")
        .collect();
    assert!(
        !dominated.is_empty(),
        "the measured sweep found scheduling-dominated counters; the table must \
         still name them, because an unnamed blind spot reads as coverage"
    );
    for counter in &dominated {
        assert!(
            counter.ceiling.is_none(),
            "{}/{}: a counter whose spread exceeds half its maximum cannot carry \
             a ceiling that distinguishes a regression from scheduling",
            counter.profile,
            counter.counter
        );
    }

    let (criterion_dir, profile, manifest) = budget_only_fixture("dominated", &counters, &latency);
    write_profile_csv(&profile, &counters, |counter| {
        if counter.classification == "scheduling_dominated" {
            counter.maximum * 100
        } else {
            counter.maximum
        }
    });

    assert_eq!(
        gate_exit_code(&criterion_dir, &profile, &manifest),
        0,
        "scheduling-dominated counters are deliberately not enforced; if this \
         starts failing, the gate began enforcing a counter that carries no \
         regression signal, which is the flake this work removed"
    );
}

/// SPEC: the gate says out loud how many counters it does NOT enforce. A gate
/// that prints "budgets ENFORCED and green" without naming its blind spots reads
/// as full coverage to everyone who does not open the source.
#[test]
fn the_gate_reports_the_counters_it_does_not_enforce() {
    let (counters, latency) = gate_metadata();
    let (criterion_dir, profile, manifest) = budget_only_fixture("reporting", &counters, &latency);
    write_profile_csv(&profile, &counters, |counter| counter.maximum);

    let output = Command::new("python3")
        .current_dir(repo_root())
        .args([
            "scripts/update-benchmark-results.py",
            "--check",
            "--budgets-only",
        ])
        .arg("--criterion-dir")
        .arg(&criterion_dir)
        .arg("--profile-output")
        .arg(&profile)
        .arg("--evidence-manifest")
        .arg(&manifest)
        .output()
        .expect("python3 should be available to run the benchmark budget gate");
    let stdout = String::from_utf8_lossy(&output.stdout);

    let enforced = counters.iter().filter(|c| c.ceiling.is_some()).count();
    let observed_only = counters.iter().filter(|c| c.ceiling.is_none()).count();
    assert!(
        stdout.contains(&format!("{enforced} counter(s) enforced")),
        "the gate must report how many counters it enforces; got: {stdout}"
    );
    assert!(
        stdout.contains(&format!("{observed_only} scheduling-dominated counter(s)")),
        "the gate must report how many counters it does NOT enforce; got: {stdout}"
    );
}

/// SPEC: no ceiling is hand-typed. Every one is derived from an `ObservedSpread`
/// carrying a real sample count, and the sweep that produces those spreads ships
/// with the gate — so refreshing them is a measurement, not a guess.
#[test]
fn every_ceiling_is_derived_from_a_recorded_measurement() {
    let (counters, _) = gate_metadata();
    for counter in &counters {
        assert!(
            counter.samples > 0,
            "{}/{}: a spread with no samples is a guess wearing a dataclass",
            counter.profile,
            counter.counter
        );
        assert!(
            counter.minimum <= counter.maximum,
            "{}/{}: recorded spread is inverted",
            counter.profile,
            counter.counter
        );
    }

    let script = fs::read_to_string(repo_root().join("scripts/update-benchmark-results.py"))
        .expect("the gate script should be readable");
    assert!(
        !script.contains("max_lock_acquisitions="),
        "budget ceilings must be derived from recorded spreads, not typed by hand"
    );
    assert!(
        script.contains("--measure-budget-spread"),
        "the sweep that records the spreads must ship with the gate, or the \
         numbers cannot be refreshed by measurement"
    );
}
