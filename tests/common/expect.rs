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
//! # Prose keys (`#lzprosekeyconvention`)
//!
//! An `assertions` block mixes two kinds of key. Most carry a value a runner can
//! compare against observed behaviour. A few carry an English paragraph that
//! states an obligation and nothing comparable — `clause`, `anti_vacuity`,
//! `null_form`, `theorem`, `note`. The corpus, not the binding, says which:
//! `assertions.prose` is an array of sibling key names, and because it is itself
//! a key of the block the guards above see it — a runner that ignores it fails
//! with an unconsumed key, which is what makes the convention self-enforcing.
//!
//! A prose key is **discharged**, never asserted and never excused. To discharge
//! it a runner names the executable assertion keys that carry its obligation:
//!
//! ```ignore
//! exp.prose_key("epoch_disambiguation", &["frame_epoch", "blob_epoch"]);
//! ```
//!
//! and the ledger checks the naming. That is the whole convention: the excuse
//! becomes falsifiable. "`epoch_disambiguation` is discharged by `frame_epoch`
//! and `blob_epoch`" is a claim about the run; "`epoch_disambiguation` is prose"
//! is not. The former `Expect::prose` — a third exempt state that required a
//! reason and then discarded it — is **deleted** rather than kept alongside; two
//! paths to satisfy one key is the ambiguity this closes.
//!
//! The ledger fails the run when:
//!
//! | # | failure |
//! |---|---|
//! | 1 | a key listed in `assertions.prose` is **asserted** |
//! | 2 | a key listed in `assertions.prose` is **excused** with free text |
//! | 3 | a key **not** listed in `assertions.prose` is discharged |
//! | 4 | the set of discharged keys differs from `assertions.prose` |
//! | 5 | a discharge names **no** keys |
//! | 6 | a discharge names a key the same fixture's run did not assert |
//! | 7 | a discharge names a key that is itself prose, **or names `prose`** |
//!
//! Rule 7's second half is not redundant: `prose` never lists itself, so the
//! prose-name set is SEEDED with `prose` — otherwise `discharged_by = ["prose"]`
//! slips past rule 7, and the rule-4 comparison is what marks `prose` asserted,
//! so rule 6 waves it through too. A paragraph discharged by the declaration
//! that it is a paragraph proves nothing.
//!
//! Rule 6 is why the NAME MATCHING is **fixture-wide**:
//! `epoch_disambiguation` is stated in `assertions` and discharged by
//! `expect.frame_epoch` / `expect.blob_epoch`, asserted long after the
//! `assertions` block has dropped. The DECLARATION is block-local, and so are
//! rules 3 and 4 — each block owns its own `prose` array. Verification happens
//! when the replay is finished — `expect::verify_prose(fixture)`, armed by a
//! [`ProseLedger`] guard whose own `Drop` fails a run that never verified. An
//! unverified claim is as bad as an unconsumed key. A "run" is ONE TEST, so the
//! ledger is cleared at each verification rather than unioning asserted keys
//! across tests.
//!
//! A discharge may name a key that carries the obligation only by PROXY, and two
//! in the corpus must: `wire_encoding` is a claim about how the corpus carries
//! its bytes, which no assertion a run makes can observe, and `theorem` names a
//! Lean theorem in another repository. Naming the closest executable keys is
//! conforming; naming a key with nothing to do with the obligation is not, and
//! no tracker can tell the two apart. That judgement stays with review, which is
//! why the discharge is written at the call site where review sees it — and why
//! each proxy says at that call site that it IS one.
//!
//! `note`, `description` and `reason` stay exempt **by name** in a block that
//! does not declare them prose — they are per-step and per-scenario annotations,
//! and the reactive-graph corpus carries ~97 of them. The declaration is
//! evaluated on the RAW block FIRST, before any name-based exemption: a tracker
//! that subtracts its reserved names first makes `frame_roundtrip_json`'s
//! declared `note` invisible — exempt from the unread guard, exempt from the
//! unasserted guard, never discharged — so the fixture skips the whole
//! convention while the binding still reports conforming. Two of the nine hit
//! that independently.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Index;

use serde_json::Value;

/// Key names that are annotations wherever they are not declared prose.
const ANNOTATION_KEYS: [&str; 3] = ["note", "description", "reason"];

/// The corpus's own declaration of which sibling keys are paragraphs.
const PROSE_DECLARATION_KEY: &str = "prose";

/// One `prose_key` claim: the paragraph, and the executable keys the runner says
/// carry its obligation.
#[derive(Clone, Debug)]
struct Claim {
    key: String,
    discharged_by: Vec<String>,
}

/// What one guarded block contributed to its fixture's prose ledger.
#[derive(Clone, Debug, Default)]
struct BlockRecord {
    label: String,
    declared: BTreeSet<String>,
    asserted: BTreeSet<String>,
    excused: BTreeSet<String>,
    non_prose_keys: usize,
    claims: Vec<Claim>,
}

/// Fixture-scoped state: every key any block of this fixture asserted, plus the
/// blocks that declared or discharged prose.
#[derive(Clone, Debug, Default)]
struct FixtureLedger {
    open: bool,
    verified: bool,
    asserted: BTreeSet<String>,
    blocks: Vec<BlockRecord>,
}

thread_local! {
    /// Keyed by fixture path. Thread-local because Rust runs `#[test]` functions
    /// on separate threads and each runner replays its own fixture — a process
    /// -global map would let one test's assertions discharge another's claim.
    static PROSE_LEDGER: RefCell<BTreeMap<String, FixtureLedger>> =
        const { RefCell::new(BTreeMap::new()) };
}

fn with_ledger<R>(fixture: &str, f: impl FnOnce(&mut FixtureLedger) -> R) -> R {
    PROSE_LEDGER.with(|l| f(l.borrow_mut().entry(fixture.to_owned()).or_default()))
}

/// Arms `expect::verify_prose` for one fixture's replay.
///
/// Open it as the FIRST binding in the test body, before any [`Expect`]. Rust
/// drops in reverse declaration order, so a ledger opened first is torn down
/// last — after every block's own consumption check and after the explicit
/// `verify_prose` call at the end of the body. Opening it later inverts that and
/// the teardown net fires before the verification it is meant to police.
///
/// [`Expect::prose_key`] refuses to record a claim without one, and this guard's
/// own `Drop` fails a run that recorded claims and never verified them. Both
/// halves are needed: a tracker that reports success by skipping the check is
/// the shape `#lzprosekeyconvention` exists to remove.
pub struct ProseLedger {
    fixture: String,
}

impl ProseLedger {
    /// Open the ledger for `fixture`, resetting anything a previous replay of the
    /// same path on this thread left behind.
    pub fn open(fixture: impl Into<String>) -> Self {
        let fixture = fixture.into();
        with_ledger(&fixture, |l| {
            *l = FixtureLedger {
                open: true,
                ..FixtureLedger::default()
            };
        });
        Self { fixture }
    }
}

impl Drop for ProseLedger {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let (verified, claims) = with_ledger(&self.fixture, |l| {
            l.open = false;
            (
                l.verified,
                l.blocks.iter().map(|b| b.claims.len()).sum::<usize>(),
            )
        });
        if !verified && claims > 0 {
            panic!(
                "{}: {claims} prose discharge claim(s) were recorded and never verified \
                 (#lzprosekeyconvention). Call `expect::verify_prose(fixture)` at the end \
                 of the replay — an unverified claim proves exactly as much as an \
                 unconsumed key.",
                self.fixture
            );
        }
    }
}

/// Verify every prose discharge recorded for `fixture`, then mark the ledger
/// satisfied.
///
/// Runs rules 1-4, 6 and 7 of `#lzprosekeyconvention` (rule 5 — a discharge
/// naming nothing — fails eagerly at the `prose_key` call site). Panics when the
/// fixture has no open [`ProseLedger`], because "nothing to check" and "the
/// runner forgot to arm the check" are the two things this must tell apart.
pub fn verify_prose(fixture: &str) {
    let ledger = PROSE_LEDGER.with(|l| l.borrow().get(fixture).cloned());
    let Some(ledger) = ledger.filter(|l| l.open) else {
        panic!(
            "{fixture}: verify_prose called without an open ProseLedger \
             (#lzprosekeyconvention). Hold `let _prose = ProseLedger::open(&path);` for the \
             whole replay — verifying an unarmed ledger checks nothing and reports success."
        );
    };

    // Rule 7 needs every paragraph named anywhere in the fixture, not just the
    // block being checked — that is the fixture-wide half, and the only one.
    //
    // SEEDED WITH `prose` ITSELF. `prose` never self-lists, so without the seed
    // rule 7 misses `discharged_by = ["prose"]` — and the rule-4 comparison
    // below is what marks `prose` asserted, so rule 6 would wave it straight
    // through. A paragraph discharged by the declaration that it is a paragraph
    // proves exactly nothing.
    let all_prose: BTreeSet<String> = std::iter::once(PROSE_DECLARATION_KEY.to_owned())
        .chain(
            ledger
                .blocks
                .iter()
                .flat_map(|b| b.declared.iter().cloned()),
        )
        .collect();

    for block in &ledger.blocks {
        if block.declared.is_empty() && block.claims.is_empty() {
            continue;
        }
        let at = format!("`{}` of {fixture}", block.label);

        // 1. a declared paragraph was asserted.
        let asserted: Vec<&String> = block
            .declared
            .iter()
            .filter(|k| block.asserted.contains(*k))
            .collect();
        assert!(
            asserted.is_empty(),
            "{at}: prose key(s) {asserted:?} were ASSERTED (#lzprosekeyconvention rule 1). \
             Comparing a paragraph — or a tally derived from one — to an English string \
             pins wording, not behaviour: a copy-edit reddens the run and a library \
             regression does not. Discharge it with `prose_key` instead."
        );

        // 2. a declared paragraph was excused with free text.
        let excused: Vec<&String> = block
            .declared
            .iter()
            .filter(|k| block.excused.contains(*k))
            .collect();
        assert!(
            excused.is_empty(),
            "{at}: prose key(s) {excused:?} were EXCUSED (#lzprosekeyconvention rule 2). \
             An unfalsifiable reason is indistinguishable from the undocumented default \
             this clause removes. Name the executable keys with `prose_key` instead."
        );

        let discharged: BTreeSet<String> = block.claims.iter().map(|c| c.key.clone()).collect();

        // 3. a key that is not declared prose was discharged.
        let undeclared: Vec<&String> = discharged.difference(&block.declared).collect();
        assert!(
            undeclared.is_empty(),
            "{at}: key(s) {undeclared:?} were discharged but are NOT listed in \
             `{PROSE_DECLARATION_KEY}` (#lzprosekeyconvention rule 3). The corpus decides \
             what is a paragraph; a binding that decides for itself is how four treatments \
             of one rule went unnoticed."
        );

        // 4. the discharged set is the declared set — the comparison that
        //    consumes `prose` itself, and what makes a forgotten key fail
        //    rather than vanish.
        assert!(
            discharged == block.declared,
            "{at}: discharged prose keys {discharged:?} differ from `{PROSE_DECLARATION_KEY}` \
             {:?} (#lzprosekeyconvention rule 4).",
            block.declared
        );

        // A block that is entirely prose has nothing that could discharge it.
        assert!(
            block.declared.is_empty() || block.non_prose_keys > 0,
            "{at}: the block declares `{PROSE_DECLARATION_KEY}` and carries no other key \
             (#lzprosekeyconvention). A block that is entirely prose has nothing that \
             could discharge it."
        );

        for claim in &block.claims {
            for named in &claim.discharged_by {
                // 7 before 6: a paragraph can never be asserted (rule 1), so a
                // discharge naming one would otherwise always report as rule 6
                // and the real defect — naming a paragraph — would never be
                // said out loud.
                assert!(
                    !all_prose.contains(named),
                    "{at}: `{}` claims to be discharged by `{named}`, which is itself a \
                     prose key (#lzprosekeyconvention rule 7). A paragraph cannot carry \
                     another paragraph's obligation.",
                    claim.key
                );
                // 6. the named key was never asserted by this fixture's run.
                assert!(
                    ledger.asserted.contains(named),
                    "{at}: `{}` claims to be discharged by `{named}`, which this fixture's \
                     run never ASSERTED (#lzprosekeyconvention rule 6). The keys this run \
                     asserted are {:?}. Rule 6 is the whole convention — the discharge is \
                     a claim about the run, so the ledger can check it.",
                    claim.key,
                    ledger.asserted
                );
            }
        }
    }

    // A "run" is ONE TEST, not one process. Where a fixture is replayed by
    // several tests the ledger is CLEARED at each verification: unioning
    // asserted keys across tests would let a discharge in one be satisfied by an
    // assertion in another, which is the accident-of-collocation the
    // fixture-scoped ledger exists to bound in the first place.
    with_ledger(fixture, |l| {
        l.verified = true;
        l.asserted.clear();
        l.blocks.clear();
    });
}

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
    /// Keys satisfied by [`Expect::prose_key`] — this block's paragraphs.
    discharged: RefCell<BTreeSet<String>>,
    /// The block's own `assertions.prose` value, read once at construction.
    declared: BTreeSet<String>,
    claims: RefCell<Vec<Claim>>,
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
        let declared: BTreeSet<String> = value
            .get(PROSE_DECLARATION_KEY)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .map(|v| {
                        v.as_str()
                            .expect("`prose` declares sibling key NAMES")
                            .to_owned()
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            fixture: fixture.into(),
            label: label.into(),
            value,
            read: RefCell::new(BTreeSet::new()),
            asserted: RefCell::new(BTreeSet::new()),
            excused: RefCell::new(BTreeMap::new()),
            discharged: RefCell::new(BTreeSet::new()),
            declared,
            claims: RefCell::new(Vec::new()),
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

    /// Discharge the prose key `key` by naming the executable assertion keys
    /// that carry its obligation (`#lzprosekeyconvention`).
    ///
    /// `key` MUST be listed in the block's own `prose` array, `discharged_by`
    /// MUST be non-empty, and every key it names MUST be asserted somewhere in
    /// the same fixture's run and MUST NOT itself be prose. The first two are
    /// checked here; the rest are fixture-scoped and checked by
    /// [`verify_prose`], because a paragraph in `assertions` is routinely
    /// carried by a per-scenario `expect` key asserted long afterwards.
    ///
    /// Naming the keys is what makes the exemption falsifiable — it replaces
    /// the free-text reason the deleted `prose()` required and then discarded.
    pub fn prose_key(&self, key: &str, discharged_by: &[&str]) {
        // Rule 5, eagerly: a discharge naming nothing is the free-text excuse
        // again, spelled as an empty list.
        assert!(
            !discharged_by.is_empty(),
            "{}: `{}.{}` was discharged naming NO keys (#lzprosekeyconvention rule 5). \
             A discharge that names nothing is the unfalsifiable excuse this clause \
             removes — name the executable assertion keys that carry the obligation.",
            self.fixture,
            self.label,
            key
        );
        // Recording a claim un-verifies the ledger, so a claim made AFTER a
        // verification is still caught by the guard's teardown rather than
        // riding on the previous test's green.
        let open = with_ledger(&self.fixture, |l| {
            if l.open {
                l.verified = false;
            }
            l.open
        });
        assert!(
            open,
            "{}: `{}.{}` was discharged without an open ProseLedger \
             (#lzprosekeyconvention). Hold `let _prose = ProseLedger::open(&path);` for the \
             whole replay and call `expect::verify_prose(&path)` when it finishes — an \
             unverified discharge claim checks nothing.",
            self.fixture, self.label, key
        );
        // The declaration itself is consumed by the rule-4 comparison in
        // `verify_prose`, so record it as asserted here rather than leaving the
        // block's own drop check to report `prose` as unconsumed.
        self.read.borrow_mut().insert(PROSE_DECLARATION_KEY.into());
        self.asserted
            .borrow_mut()
            .insert(PROSE_DECLARATION_KEY.into());
        self.read.borrow_mut().insert(key.to_owned());
        self.discharged.borrow_mut().insert(key.to_owned());
        self.claims.borrow_mut().push(Claim {
            key: key.to_owned(),
            discharged_by: discharged_by.iter().map(|s| (*s).to_owned()).collect(),
        });
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
        let discharged = self.discharged.borrow();
        let descended = self.descended.borrow();
        self.read
            .borrow()
            .iter()
            .filter(|k| {
                !asserted.contains(k.as_str())
                    && !excused.contains_key(k.as_str())
                    && !discharged.contains(k.as_str())
                    && !descended.contains(k.as_str())
                    && !self.annotation_exempt(k)
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
            || self.discharged.borrow().contains(key)
            || self.descended.borrow().contains(key)
            || self.annotation_exempt(key)
    }

    /// `note` / `description` / `reason` are annotations, exempt by name — the
    /// reactive-graph corpus carries ~97 per-step ones and no runner should have
    /// to hand-wave each. The exemption is **overridden** by the corpus: a block
    /// that lists the name in its own `prose` array has said it states an
    /// obligation, and it must then be discharged like any other paragraph.
    fn annotation_exempt(&self, key: &str) -> bool {
        ANNOTATION_KEYS.contains(&key) && !self.declared.contains(key)
    }

    /// Hand this block's contribution to the fixture-scoped prose ledger.
    ///
    /// Every block contributes its asserted key names — a paragraph in
    /// `assertions` is routinely discharged by a per-scenario `expect` key, so
    /// rule 6 cannot be answered from one block. Only blocks that declared or
    /// discharged prose contribute a record.
    fn record_prose(&self) {
        let asserted = self.asserted.borrow().clone();
        let declared = self.declared.clone();
        let claims = self.claims.borrow().clone();
        let excused: BTreeSet<String> = self.excused.borrow().keys().cloned().collect();
        let non_prose_keys = self
            .value
            .as_object()
            .map(|o| {
                o.keys()
                    .filter(|k| k.as_str() != PROSE_DECLARATION_KEY && !declared.contains(*k))
                    .count()
            })
            .unwrap_or(0);
        let label = self.label.clone();
        with_ledger(&self.fixture, |l| {
            l.asserted.extend(asserted.iter().cloned());
            if !declared.is_empty() || !claims.is_empty() {
                l.blocks.push(BlockRecord {
                    label,
                    declared,
                    asserted,
                    excused,
                    non_prose_keys,
                    claims,
                });
            }
        });
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
        // The ledger is fixture-scoped and outlives this block, so it is fed
        // BEFORE the block's own drop check can panic.
        self.record_prose();
        self.check();
    }
}
