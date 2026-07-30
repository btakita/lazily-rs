//! Assertion-key consumption guard (`#lzassertunknownkeys`).
//!
//! # The failure this closes
//!
//! `tests/common/mod.rs` proves a fixture was *opened*. That is one level above
//! the defect this module exists for: having opened and replayed a fixture, did
//! the runner actually **consume** the keys the fixture asserts?
//!
//! Every conformance runner in this repo reads its assertion block by name —
//! `step["expected"]["invalidates"]["value"]`, `sc["expect"]["final_last_epoch"]`
//! — and `serde_json::Value`'s `Index` returns `Value::Null` for a key that is
//! not there. So a key the runner does not know about is not an error; it is
//! invisible. The fixture round-trips, the suite goes green, and the one thing
//! the fixture exists to pin is never checked.
//!
//! This was found concretely in lazily-kt: `delta_zero_copy_arrow.json` carries a
//! `backend` discriminator that `assertAssertions` never read, so the fixture
//! would have been "replayed" while never testing the backend. Adding the missing
//! arm fixed that instance and not the property. Worse, a corpus assertion key
//! that *no* binding implements would be skipped in all nine at once, with
//! nothing anywhere reporting it.
//!
//! # The seam
//!
//! [`Expect`] wraps one assertion block and records every key the runner asks
//! for. On drop it compares the recorded set against the keys actually present in
//! the fixture and panics naming any key the runner never touched, with the
//! fixture path and the block's position in it.
//!
//! It is deliberately *observational*: only a key the runner really read counts.
//! A declared list of "keys this runner handles" would go stale the same way the
//! static coverage grep did — it proves a spelling, not a read.
//!
//! ```ignore
//! let exp = Expect::new(&path, format!("steps[{i}].expected"), &step["expected"]);
//! assert_eq!(cell.value(&ctx), exp["value"].as_str());
//! assert_eq!(invalidated, exp.sub("invalidates")["value"].as_bool().unwrap());
//! // `exp` drops here; an unconsumed key panics.
//! ```
//!
//! # Depth policy
//!
//! `get`/`Index` marks a key consumed and hands back the raw subtree — the
//! runner is trusted with what it asked for. [`Expect::sub`] descends one level
//! and guards the nested object too. Descend with `sub` wherever the nested keys
//! are themselves *assertion names* (`invalidates`, `final_state`,
//! `after_publish`, per-record expectations); use `get` where the nested object
//! is *data* whose keys are ids, peer names, or map entries the runner compares
//! wholesale. Guarding data keys would report a fixture's payload as an
//! unconsumed assertion, which is noise, not a finding.
//!
//! # Exceptions
//!
//! [`Expect::declared_exception`] marks a key consumed without reading it. It is
//! for a key whose capability this binding genuinely does not have — never for a
//! key that is merely unimplemented in the runner. The reason is required and
//! belongs at the call site, where review can see it.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::ops::Index;

use serde_json::Value;

/// A fixture assertion block plus the set of keys the runner consumed from it.
///
/// Panics on drop if the block carries a key the runner never asked for. Drops
/// during an in-flight panic are silent, so the original failure is the one that
/// gets reported.
pub struct Expect<'a> {
    fixture: String,
    label: String,
    value: &'a Value,
    seen: RefCell<BTreeSet<String>>,
    armed: Cell<bool>,
}

impl<'a> Expect<'a> {
    /// Guard `value` as the assertion block at `label` of fixture `fixture`.
    ///
    /// A `value` that is not a JSON object (absent block, array, scalar) carries
    /// no keys and therefore no obligation — the guard is inert rather than an
    /// error, because "this fixture has no assertion block" is a shape question
    /// the runner already answers.
    pub fn new(fixture: impl Into<String>, label: impl Into<String>, value: &'a Value) -> Self {
        Self {
            fixture: fixture.into(),
            label: label.into(),
            value,
            seen: RefCell::new(BTreeSet::new()),
            armed: Cell::new(true),
        }
    }

    /// Record `key` as consumed and return its value (`Value::Null` when absent,
    /// matching `serde_json`'s own indexing so call sites need no reshaping).
    pub fn get(&self, key: &str) -> &'a Value {
        static NULL: Value = Value::Null;
        self.seen.borrow_mut().insert(key.to_owned());
        self.value.get(key).unwrap_or(&NULL)
    }

    /// Record `key` as consumed and return `Some` only when it is actually
    /// present. Use where "absent" and "present but null" mean different things
    /// — `get` collapses them, because `serde_json`'s own indexing does.
    pub fn get_opt(&self, key: &str) -> Option<&'a Value> {
        self.seen.borrow_mut().insert(key.to_owned());
        self.value.get(key)
    }

    /// Record `key` as consumed and guard the nested object under it as well.
    pub fn sub(&self, key: &str) -> Expect<'a> {
        let child = self.get(key);
        Expect::new(
            self.fixture.clone(),
            format!("{}.{}", self.label, key),
            child,
        )
    }

    /// Guard an object nested inside `self`'s subtree that `self` already
    /// consumed — array elements, for instance, which have no key of their own.
    pub fn nested(&self, label: impl Into<String>, value: &'a Value) -> Expect<'a> {
        Expect::new(
            self.fixture.clone(),
            format!("{}.{}", self.label, label.into()),
            value,
        )
    }

    /// Mark `key` consumed because it is *documentation*, not an assertion —
    /// `note`, `reason`, `description`: prose aimed at a human reading the
    /// fixture, with no observable behind it.
    ///
    /// Kept distinct from [`Expect::declared_exception`] so review can tell "this
    /// is not an assertion" from "this is an assertion this binding cannot make".
    pub fn prose(&self, key: &str, why: &str) {
        self.declared_exception(key, why);
    }

    /// Mark `key` consumed without reading it, for a capability this binding
    /// genuinely does not have. `reason` documents which — it is not a knob for
    /// silencing a key the runner simply has not got round to.
    pub fn declared_exception(&self, key: &str, reason: &str) {
        assert!(
            !reason.is_empty(),
            "{}: declared_exception for `{}.{}` needs a reason",
            self.fixture,
            self.label,
            key
        );
        self.seen.borrow_mut().insert(key.to_owned());
    }

    /// The block itself, for panic messages and wholesale decoding.
    pub fn raw(&self) -> &'a Value {
        self.value
    }

    /// Keys present in the block that the runner never consumed.
    pub fn unconsumed(&self) -> Vec<String> {
        let Some(obj) = self.value.as_object() else {
            return Vec::new();
        };
        let seen = self.seen.borrow();
        obj.keys()
            .filter(|k| !seen.contains(k.as_str()))
            .cloned()
            .collect()
    }

    /// Check now rather than at end of scope. `Drop` runs the same check, so
    /// this is for readability at a site where the block is finished early.
    pub fn finish(self) {
        drop(self);
    }

    fn check(&self) {
        let missed = self.unconsumed();
        if missed.is_empty() {
            return;
        }
        panic!(
            "{}: assertion key(s) {:?} in `{}` were never consumed by the runner \
             (#lzassertunknownkeys). The fixture asserts something this binding \
             never checks — implement the assertion, or, only if the capability \
             genuinely does not exist here, record it with \
             Expect::declared_exception and say why.",
            self.fixture, missed, self.label
        );
    }
}

impl Index<&str> for Expect<'_> {
    type Output = Value;

    fn index(&self, key: &str) -> &Value {
        self.get(key)
    }
}

impl Drop for Expect<'_> {
    fn drop(&mut self) {
        // A drop while unwinding must not replace the real failure with this
        // one; the unconsumed-key report is only meaningful on a passing run.
        if !self.armed.get() || std::thread::panicking() {
            return;
        }
        self.check();
    }
}
