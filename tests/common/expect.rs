//! Assertion-key consumption *and assertion* guard (`#lzassertunknownkeys`,
//! `#lzconsumednotasserted`).
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
//! # Reading is not asserting (`#lzconsumednotasserted`)
//!
//! Recording a *read* proves consumption. It does not prove assertion. A runner
//! can read a key and discard it, and the guard above stays green. Three shapes
//! do this:
//!
//! 1. a named skip inside a loop that consumes the block — the read marks the key,
//!    then `continue` steps past the comparison;
//! 2. a value bound and never compared — `let want = exp["x"];` and nothing after;
//! 3. a comparison against a *literal* rather than the fixture value, so editing
//!    the fixture changes nothing.
//!
//! So [`Expect`] tracks a second set. A key becomes **asserted** only by passing
//! through [`Expect::assert_key`] (fixture value vs actual, compared here) or
//! [`Expect::assert_key_with`] (fixture value handed to the caller's own check,
//! for tolerances, containment, and shapes `Value` equality cannot express). A
//! literal-comparison arm never reaches either, so it never marks the key.
//!
//! The drop check therefore has three failure modes:
//!
//! | mode | meaning |
//! |---|---|
//! | never read | the fixture asserts something the runner does not know about |
//! | read but not asserted | the runner looked at the key and discarded it |
//! | stale excuse | a key is both excused *and* asserted, so the excuse hides nothing |
//!
//! # The ladder
//!
//! `#lzcoverageaudit` proves the fixture was opened. `#lzassertunknownkeys`
//! proves every key was read. This module's second set proves every read key
//! reached a comparison against the fixture's own value.
//!
//! # The seam
//!
//! ```ignore
//! let exp = Expect::new(&path, format!("steps[{i}].expected"), &step["expected"]);
//! exp.assert_key("value", cell.value(&ctx));
//! exp.sub("invalidates").assert_key("value", invalidated);
//! // `exp` drops here; an unconsumed *or* unasserted key panics.
//! ```
//!
//! It is deliberately *observational*: only a key the runner really compared
//! counts. A declared list of "keys this runner handles" would go stale the same
//! way the static coverage grep did — it proves a spelling, not a comparison.
//!
//! # Depth policy
//!
//! [`Expect::sub`] descends one level and guards the nested object too; the
//! parent key is satisfied *structurally*, because the child's own drop check
//! now owns every key beneath it. [`Expect::get`] marks a key read and hands back
//! the raw subtree — it is the escape hatch for a value that drives a code path
//! rather than one that is compared, and on its own it is now a **failure**
//! unless the same key is also asserted or excused.
//!
//! Descend with `sub` wherever the nested keys are themselves *assertion names*
//! (`invalidates`, `final_state`, `after_publish`, per-record expectations); use
//! `assert_key`/`assert_key_with` where the nested object is *data* whose keys
//! are ids, peer names, or map entries the runner compares wholesale. Guarding
//! data keys would report a fixture's payload as an unconsumed assertion, which
//! is noise, not a finding.
//!
//! # Exceptions
//!
//! [`Expect::excuse_key`] marks a key satisfied without asserting it. It is for a
//! key that genuinely cannot be compared at this call site — the binding proves
//! the fact elsewhere, or the field is a discriminator selecting a code path
//! rather than a value to check. The reason is required and belongs at the call
//! site, where review can see it.
//!
//! It runs in **both directions**, exactly as the `KNOWN_UNCOVERED` allowlists
//! do: excusing a key the same run also asserts is a failure, because the excuse
//! has gone stale and is now hiding nothing.
//!
//! [`Expect::prose`] is for `note` / `reason` / `description` — documentation
//! aimed at a human reading the fixture, with no observable behind it. Prose is
//! exempt from all three checks and is kept distinct from an excuse so review can
//! tell "this is not an assertion" from "this is an assertion I cannot make here".

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Index;

use serde_json::Value;

/// A fixture assertion block plus the sets of keys the runner read, asserted,
/// excused, descended into, and marked as prose.
///
/// Panics on drop if the block carries a key the runner never asked for, a key
/// the runner read and then discarded, or an excuse the same run made redundant.
/// Drops during an in-flight panic are silent, so the original failure is the one
/// that gets reported.
pub struct Expect<'a> {
    fixture: String,
    label: String,
    value: &'a Value,
    read: RefCell<BTreeSet<String>>,
    asserted: RefCell<BTreeSet<String>>,
    excused: RefCell<BTreeMap<String, String>>,
    prose: RefCell<BTreeSet<String>>,
    descended: RefCell<BTreeSet<String>>,
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
            read: RefCell::new(BTreeSet::new()),
            asserted: RefCell::new(BTreeSet::new()),
            excused: RefCell::new(BTreeMap::new()),
            prose: RefCell::new(BTreeSet::new()),
            descended: RefCell::new(BTreeSet::new()),
            armed: Cell::new(true),
        }
    }

    /// Record `key` as read and return its value (`Value::Null` when absent,
    /// matching `serde_json`'s own indexing so call sites need no reshaping).
    ///
    /// A read alone no longer satisfies the key — see the module docs. Use it for
    /// a value that *drives* the run (a discriminator, an input to replay) and
    /// pair it with [`Expect::assert_key_with`] or [`Expect::excuse_key`].
    pub fn get(&self, key: &str) -> &'a Value {
        static NULL: Value = Value::Null;
        self.read.borrow_mut().insert(key.to_owned());
        self.value.get(key).unwrap_or(&NULL)
    }

    /// Record `key` as read and return `Some` only when it is actually present.
    /// Use where "absent" and "present but null" mean different things — `get`
    /// collapses them, because `serde_json`'s own indexing does.
    pub fn get_opt(&self, key: &str) -> Option<&'a Value> {
        self.read.borrow_mut().insert(key.to_owned());
        self.value.get(key)
    }

    /// The one assertion entry point: compare `actual` against the fixture's own
    /// value for `key`, and mark the key asserted.
    ///
    /// The comparison happens *here*, against the value read out of the block, so
    /// editing the fixture changes the outcome. That is the whole point — an arm
    /// that compares against a hardcoded constant never reaches this path and so
    /// never marks the key.
    pub fn assert_key(&self, key: &str, actual: impl Into<Value>) {
        self.assert_key_at(key, actual, "");
    }

    /// [`Expect::assert_key`] plus a `where` note naming the call site — the
    /// scenario name, the flavor, the step index — carried into the panic
    /// message when the same key is asserted from several places.
    pub fn assert_key_at(&self, key: &str, actual: impl Into<Value>, where_: &str) {
        let want = self.mark_asserted(key);
        let got: Value = actual.into();
        if &got != want {
            let at = if where_.is_empty() {
                String::new()
            } else {
                format!(" at {where_}")
            };
            panic!(
                "{}: `{}.{}`{} — fixture expects {}, runner produced {}",
                self.fixture, self.label, key, at, want, got
            );
        }
    }

    /// Mark `key` asserted and hand the fixture's value to the caller's own
    /// check, returning whatever that check returns.
    ///
    /// For a comparison `Value` equality cannot express: a tolerance, a set
    /// containment, a regex, a decode-then-compare. The requirement is that the
    /// fixture's value reaches the comparison, not that the comparison is `==`.
    /// The closure must actually assert — handing the value to `|_| ()` marks the
    /// key while proving nothing, which is the defect this guard exists for.
    pub fn assert_key_with<R>(&self, key: &str, check: impl FnOnce(&'a Value) -> R) -> R {
        let want = self.mark_asserted(key);
        check(want)
    }

    /// [`Expect::assert_key_with`], but only when the block actually carries
    /// `key`; returns `Some` when it did.
    ///
    /// An absent key carries no obligation — the guard reports only keys the
    /// fixture declares — so this neither reads nor marks it. It exists because
    /// the `if let Some(x) = exp["k"].as_array()` shape is otherwise
    /// indistinguishable from a read-then-discard: the read happens whether or
    /// not the comparison follows.
    pub fn assert_key_if_present<R>(
        &self,
        key: &str,
        check: impl FnOnce(&'a Value) -> R,
    ) -> Option<R> {
        self.value.get(key)?;
        Some(self.assert_key_with(key, check))
    }

    /// Record `key` as read and guard the nested object under it as well.
    ///
    /// The parent key is satisfied structurally: the returned child owns the drop
    /// check for every key beneath it, so the obligation moves down rather than
    /// disappearing.
    pub fn sub(&self, key: &str) -> Expect<'a> {
        let child = self.get(key);
        self.descended.borrow_mut().insert(key.to_owned());
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

    /// Mark `key` satisfied because it is *documentation*, not an assertion —
    /// `note`, `reason`, `description`: prose aimed at a human reading the
    /// fixture, with no observable behind it.
    ///
    /// Exempt from all three checks, and kept distinct from
    /// [`Expect::excuse_key`] so review can tell "this is not an assertion" from
    /// "this is an assertion this binding cannot make here".
    pub fn prose(&self, key: &str, why: &str) {
        assert!(
            !why.is_empty(),
            "{}: prose for `{}.{}` needs a reason",
            self.fixture,
            self.label,
            key
        );
        self.prose.borrow_mut().insert(key.to_owned());
    }

    /// Mark `key` satisfied *without* asserting it, for a comparison that
    /// genuinely cannot be made at this call site.
    ///
    /// `reason` must name where the fact is proven instead, or why it is
    /// unprovable here — it is not a knob for silencing a key the runner simply
    /// has not got round to. Prefer implementing the assertion.
    ///
    /// Runs in both directions: excusing a key that the same run also asserts is
    /// a failure, because the excuse has gone stale and is hiding nothing.
    pub fn excuse_key(&self, key: &str, reason: &str) {
        assert!(
            !reason.is_empty(),
            "{}: excuse_key for `{}.{}` needs a reason",
            self.fixture,
            self.label,
            key
        );
        self.excused
            .borrow_mut()
            .insert(key.to_owned(), reason.to_owned());
    }

    /// Former spelling of [`Expect::excuse_key`], kept so the `#lzassertunknownkeys`
    /// call sites read the same as the other bindings' trackers.
    pub fn declared_exception(&self, key: &str, reason: &str) {
        self.excuse_key(key, reason);
    }

    /// The block itself, for panic messages and wholesale decoding.
    pub fn raw(&self) -> &'a Value {
        self.value
    }

    /// Keys present in the block that the runner never touched at all.
    pub fn unconsumed(&self) -> Vec<String> {
        let Some(obj) = self.value.as_object() else {
            return Vec::new();
        };
        obj.keys().filter(|k| !self.touched(k)).cloned().collect()
    }

    /// Keys the runner read and then discarded — never asserted, never excused.
    pub fn read_but_not_asserted(&self) -> Vec<String> {
        let asserted = self.asserted.borrow();
        let excused = self.excused.borrow();
        let prose = self.prose.borrow();
        let descended = self.descended.borrow();
        self.read
            .borrow()
            .iter()
            .filter(|k| {
                !asserted.contains(k.as_str())
                    && !excused.contains_key(k.as_str())
                    && !prose.contains(k.as_str())
                    && !descended.contains(k.as_str())
            })
            .cloned()
            .collect()
    }

    /// Keys carrying an excuse that the same run also asserted.
    pub fn stale_excuses(&self) -> Vec<String> {
        let asserted = self.asserted.borrow();
        self.excused
            .borrow()
            .keys()
            .filter(|k| asserted.contains(k.as_str()))
            .cloned()
            .collect()
    }

    /// Check now rather than at end of scope. `Drop` runs the same check, so
    /// this is for readability at a site where the block is finished early.
    pub fn finish(self) {
        drop(self);
    }

    fn touched(&self, key: &str) -> bool {
        self.read.borrow().contains(key)
            || self.asserted.borrow().contains(key)
            || self.excused.borrow().contains_key(key)
            || self.prose.borrow().contains(key)
            || self.descended.borrow().contains(key)
    }

    fn mark_asserted(&self, key: &str) -> &'a Value {
        let want = self.get(key);
        self.asserted.borrow_mut().insert(key.to_owned());
        want
    }

    fn check(&self) {
        let stale = self.stale_excuses();
        if !stale.is_empty() {
            panic!(
                "{}: key(s) {:?} in `{}` are both excused and asserted \
                 (#lzconsumednotasserted). The excuse is stale — the runner does \
                 make the comparison, so the excuse is hiding nothing. Delete it.",
                self.fixture, stale, self.label
            );
        }
        let discarded = self.read_but_not_asserted();
        if !discarded.is_empty() {
            panic!(
                "{}: assertion key(s) {:?} in `{}` were read but never asserted \
                 (#lzconsumednotasserted). The runner looked at the fixture's \
                 value and discarded it — a named skip, a binding with no \
                 comparison, or a comparison against a literal instead of the \
                 fixture value. Route it through Expect::assert_key / \
                 Expect::assert_key_with, or, if there is genuinely nothing to \
                 compare here, record it with Expect::excuse_key and say why.",
                self.fixture, discarded, self.label
            );
        }
        let missed = self.unconsumed();
        if !missed.is_empty() {
            panic!(
                "{}: assertion key(s) {:?} in `{}` were never consumed by the runner \
                 (#lzassertunknownkeys). The fixture asserts something this binding \
                 never checks — implement the assertion, or, only if the capability \
                 genuinely does not exist here, record it with \
                 Expect::excuse_key and say why.",
                self.fixture, missed, self.label
            );
        }
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
