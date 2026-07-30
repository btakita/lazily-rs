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
