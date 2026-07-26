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

#![allow(dead_code)]

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
