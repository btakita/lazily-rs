#!/usr/bin/env bash
# Conformance-coverage guard (#portconformancecoverage).
#
# Fails the build when the canonical corpus in ../lazily-spec/conformance/ grows a
# fixture that no test in this repo replays. That is the drift this guard exists
# for: a fixture lands upstream, every binding stays green, and nobody learns that
# one of them is not replaying it.
#
# This binding uses the RUNTIME manifest (#lazilyupgradeconformance), not the
# static grep it started with. The suite records every file it actually opens from
# the conformance corpus (see tests/common/mod.rs), so a fixture named in a comment
# and hand-transcribed — the drift found in lazily-cpp's queue tests, and in this
# repo's own topic_conformance.rs — is caught here. A source grep cannot see that
# case at all: `present in a grep` is not proof of replay; only observing the read
# is.
#
# A missing manifest is missing EVIDENCE and fails. It does not mean "no fixtures
# were read"; it means the suite ran without the recorder attached, and passing in
# that state is the vacuous green this guard exists to prevent.
set -euo pipefail

SPEC_DIR="${LAZILY_SPEC_CONFORMANCE_DIR:-../lazily-spec/conformance}"
if [ ! -d "$SPEC_DIR" ]; then
  echo "SKIP: canonical corpus not found at $SPEC_DIR (clone the lazily-spec sibling)" >&2
  exit 0
fi

# Fixtures deliberately not covered by this binding yet. Each entry is a claim that
# someone looked; shrinking this list is the work. Adding to it silently is how the
# guard rots, so keep a reason with any new entry.
KNOWN_UNCOVERED=(
  # No runner at all — these were already excused under the static guard.
  "agent-doc/delta_agent_doc_state.json"
  "agent-doc/snapshot_agent_doc_state.json"
  "receipts/causal_receipts.json"
  "reliable-sync/coalesce_bounds_outbox.json"
  "reliable-sync/liveness_lease_eviction.json"
)

# Scenarios of an OPENED fixture that this binding deliberately does not replay
# (#lzscenariocoverage). One rung below KNOWN_UNCOVERED and deliberately kept
# beside it: a fixture with four scenarios of which the suite replays three is
# green under the fixture manifest alone, so this is the second place to read
# what this binding does not prove.
#
# Format: "fixture|scenario id|reason". The reason is REQUIRED — an excuse with
# no reason is an unexplained gap wearing a green badge. Prefer implementing the
# scenario; excuse only what this binding genuinely cannot express.
#
# Runs in both directions (see the python phase below): an excuse for a scenario
# the run DID replay, or for an id the fixture does not carry, is a failure.
KNOWN_UNREPLAYED_SCENARIOS=(
)

MANIFEST="${LAZILY_CONFORMANCE_MANIFEST:-build/conformance-fixtures-loaded.txt}"
SCENARIO_LEDGER="${LAZILY_CONFORMANCE_SCENARIOS:-build/conformance-scenarios-replayed.txt}"

if [ ! -s "$MANIFEST" ]; then
  echo "FAIL: no conformance manifest at $MANIFEST." >&2
  echo "      Run the suite with LAZILY_CONFORMANCE_MANIFEST set to an ABSOLUTE" >&2
  echo "      path so the recorder attaches (\`make check\` does this). An absent" >&2
  echo "      manifest is missing evidence, not evidence of absence." >&2
  exit 1
fi
OPENED="$(sort -u "$MANIFEST")"

missing=0
total=0
covered=0
while IFS= read -r fixture; do
  total=$((total + 1))
  # Here-string, NOT a pipe. With `set -o pipefail`, `printf ... | grep -q` reports
  # FAILURE when grep matches: grep -q exits immediately on the first hit, printf
  # takes SIGPIPE writing the rest, and pipefail surfaces printf's death as the
  # pipeline's status. The check then inverts — every covered fixture is reported
  # missing. That is exactly how it behaved before this line changed.
  if grep -qxF "$fixture" <<< "$OPENED"; then
    covered=$((covered + 1))
    continue
  fi
  excused=0
  for known in "${KNOWN_UNCOVERED[@]:-}"; do
    if [ "$known" = "$fixture" ]; then excused=1; break; fi
  done
  if [ "$excused" -eq 0 ]; then
    echo "ERROR: canonical fixture '$fixture' was NOT opened by the suite." >&2
    echo "       A runner may still name it in source while no longer reading it —" >&2
    echo "       that is the drift this manifest exists to catch. Replay it, or add" >&2
    echo "       it to KNOWN_UNCOVERED with a reason." >&2
    missing=$((missing + 1))
  fi
done < <(cd "$SPEC_DIR" && find . -name '*.json' | sed 's|^\./||' | sort)

# The evidence channel guards itself. Every recorded id must resolve against the
# corpus root; otherwise the manifest was truncated or interleaved in transit,
# and coverage computed from it cannot be trusted.
while IFS= read -r id; do
  [ -n "$id" ] || continue
  if [ ! -f "$SPEC_DIR/$id" ]; then
    echo "ERROR: manifest records '$id', which names no file in $SPEC_DIR." >&2
    echo "       The recorder is dropping or interleaving writes; coverage computed" >&2
    echo "       from this manifest cannot be trusted." >&2
    missing=$((missing + 1))
  fi
done <<< "$OPENED"

# A stale allowlist is its own drift, in two directions (#lzcovallowlistrot).
#
#   1. An entry naming a fixture that no longer EXISTS means the corpus moved and
#      nobody updated the excuse.
#   2. An entry naming a fixture the suite DOES open means the excuse outlived the
#      gap it described. Nothing else catches this: the covered-check `continue`s
#      before it ever consults the allowlist, so a stale excuse costs nothing and
#      is invisible. Left alone it understates coverage — the ledger reports a gap
#      the binding closed — and it also re-arms the original drift, because if that
#      fixture later stops being replayed the excuse silently absorbs the failure.
#
# The presence test below is byte-identical to the covered-check above
# (`grep -qxF <needle> <<< "$OPENED"`) on purpose: two spellings of "is this
# fixture in the opened set" can drift apart, and then the guard contradicts
# itself about the same string.
for known in "${KNOWN_UNCOVERED[@]:-}"; do
  if [ ! -f "$SPEC_DIR/$known" ]; then
    echo "ERROR: KNOWN_UNCOVERED lists '$known', which is not in the canonical corpus." >&2
    missing=$((missing + 1))
    continue
  fi
  if grep -qxF "$known" <<< "$OPENED"; then
    echo "ERROR: KNOWN_UNCOVERED lists '$known', but the suite DID open it." >&2
    echo "       The excuse is stale — the gap it described is closed. Delete the" >&2
    echo "       entry from KNOWN_UNCOVERED. Keeping it understates coverage and" >&2
    echo "       silently absorbs the failure if replay ever stops." >&2
    missing=$((missing + 1))
  fi
done

if [ "$missing" -gt 0 ]; then
  echo "conformance coverage FAILED: $missing problem(s)" >&2
  exit 1
fi

echo "conformance coverage OK: $covered/$total canonical fixtures OPENED by the suite" \
     "(${#KNOWN_UNCOVERED[@]} listed as known-uncovered; runtime manifest — these bytes were really read)"

# ---------------------------------------------------------------------------
# Per-scenario replay accounting (#lzscenariocoverage)
# ---------------------------------------------------------------------------
#
# The phase above proves the FILE was opened; one scenario out of four is enough
# to satisfy it. This phase compares the runtime scenario ledger
# (tests/common/mod.rs `record_scenario`) against the scenarios each opened
# fixture actually carries on disk. JSON parsing is why this leg is python — the
# ids live inside the fixtures, and `grep` cannot resolve `id` -> `name` ->
# positional without reading the structure.
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 is required to read scenario ids out of the corpus." >&2
  exit 1
fi

SCENARIO_EXCUSES="$(printf '%s\n' "${KNOWN_UNREPLAYED_SCENARIOS[@]:-}")" \
python3 - "$SPEC_DIR" "$MANIFEST" "$SCENARIO_LEDGER" <<'PY'
import json
import os
import sys

spec_dir, manifest_path, ledger_path = sys.argv[1:4]

if not os.path.isfile(ledger_path) or os.path.getsize(ledger_path) == 0:
    sys.stderr.write(
        "FAIL: no scenario ledger at %s.\n"
        "      Run the suite with LAZILY_CONFORMANCE_SCENARIOS set to an ABSOLUTE\n"
        "      path so the recorder attaches (`make check` does this). An absent\n"
        "      ledger is missing evidence, not evidence of absence.\n" % ledger_path
    )
    sys.exit(1)

opened = set()
with open(manifest_path) as handle:
    for line in handle:
        line = line.strip()
        if line:
            opened.add(line)

# fixture -> {scenario id: source} as RECORDED at the point of replay.
ledger = {}
for line in open(ledger_path):
    line = line.rstrip("\n")
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 3:
        sys.stderr.write("ERROR: malformed scenario ledger line %r\n" % line)
        sys.exit(1)
    fixture, scenario_id, source = parts
    ledger.setdefault(fixture, {})[scenario_id] = source


def scenario_ids(path):
    """`id` -> `name` -> positional `#<n>`, the order every binding shares."""
    try:
        with open(path) as handle:
            doc = json.load(handle)
    except (ValueError, OSError):
        return None
    if not isinstance(doc, dict):
        return None
    scenarios = doc.get("scenarios")
    if not isinstance(scenarios, list):
        return None
    out = []
    for index, scenario in enumerate(scenarios):
        if isinstance(scenario, dict) and isinstance(scenario.get("id"), str):
            out.append((scenario["id"], "id"))
        elif isinstance(scenario, dict) and isinstance(scenario.get("name"), str):
            out.append((scenario["name"], "name"))
        else:
            out.append(("#%d" % index, "index"))
    return out


problems = 0
replayed = 0
total = 0
positional = []

excuses = []
for raw in os.environ.get("SCENARIO_EXCUSES", "").splitlines():
    raw = raw.strip()
    if not raw:
        continue
    parts = raw.split("|")
    if len(parts) != 3:
        sys.stderr.write(
            "ERROR: KNOWN_UNREPLAYED_SCENARIOS entry %r is not "
            "'fixture|scenario id|reason'.\n" % raw
        )
        problems += 1
        continue
    fixture, scenario_id, reason = (part.strip() for part in parts)
    if not reason:
        sys.stderr.write(
            "ERROR: KNOWN_UNREPLAYED_SCENARIOS entry for '%s' scenario '%s' has no "
            "reason.\n"
            "       An excuse with no reason is an unexplained gap wearing a green "
            "badge.\n" % (fixture, scenario_id)
        )
        problems += 1
        continue
    excuses.append((fixture, scenario_id, reason))

excused_ids = {}
for fixture, scenario_id, _reason in excuses:
    excused_ids.setdefault(fixture, set()).add(scenario_id)

# Direction 1: every scenario of an opened fixture must appear in the ledger.
for fixture in sorted(opened):
    ids = scenario_ids(os.path.join(spec_dir, fixture))
    if ids is None:
        continue
    seen = ledger.get(fixture, {})
    for scenario_id, source in ids:
        total += 1
        if scenario_id in seen:
            replayed += 1
            if seen[scenario_id] == "index":
                positional.append("%s :: %s" % (fixture, scenario_id))
            continue
        if scenario_id in excused_ids.get(fixture, ()):  # excused, see below
            continue
        sys.stderr.write(
            "ERROR: '%s' scenario '%s' was OPENED but never REPLAYED "
            "(#lzscenariocoverage).\n"
            "       The fixture is counted as covered because a SIBLING scenario "
            "ran.\n"
            "       Replay it, or add '%s|%s|<reason>' to "
            "KNOWN_UNREPLAYED_SCENARIOS.\n" % (fixture, scenario_id, fixture, scenario_id)
        )
        problems += 1
        if source == "index":
            positional.append("%s :: %s (not replayed)" % (fixture, scenario_id))

# The evidence channel guards itself, exactly as the fixture manifest does: an
# id the corpus does not carry means the recorder and the corpus disagree, and
# coverage computed from the ledger cannot be trusted.
for fixture, seen in sorted(ledger.items()):
    ids = scenario_ids(os.path.join(spec_dir, fixture))
    if ids is None:
        sys.stderr.write(
            "ERROR: scenario ledger records '%s', which is not a scenario-bearing "
            "fixture in %s.\n" % (fixture, spec_dir)
        )
        problems += 1
        continue
    known = {scenario_id for scenario_id, _ in ids}
    for scenario_id in sorted(seen):
        if scenario_id not in known:
            sys.stderr.write(
                "ERROR: scenario ledger records '%s :: %s', which the fixture does "
                "not carry.\n"
                "       The recorder is resolving ids differently from the corpus; "
                "coverage\n"
                "       computed from this ledger cannot be trusted.\n"
                % (fixture, scenario_id)
            )
            problems += 1

# Direction 2: a stale excuse is its own drift, in the same two shapes the
# KNOWN_UNCOVERED allowlist guards (#lzcovallowlistrot).
for fixture, scenario_id, _reason in excuses:
    ids = scenario_ids(os.path.join(spec_dir, fixture))
    if ids is None:
        sys.stderr.write(
            "ERROR: KNOWN_UNREPLAYED_SCENARIOS names '%s', which is not a "
            "scenario-bearing fixture in %s.\n" % (fixture, spec_dir)
        )
        problems += 1
        continue
    if scenario_id not in {sid for sid, _ in ids}:
        sys.stderr.write(
            "ERROR: KNOWN_UNREPLAYED_SCENARIOS excuses '%s :: %s', which the fixture "
            "does not carry.\n"
            "       The excuse is stale — the corpus renamed or dropped that "
            "scenario. Delete it.\n" % (fixture, scenario_id)
        )
        problems += 1
        continue
    if scenario_id in ledger.get(fixture, {}):
        sys.stderr.write(
            "ERROR: KNOWN_UNREPLAYED_SCENARIOS excuses '%s :: %s', but the suite DID "
            "replay it.\n"
            "       The excuse is stale — the gap it described is closed. Delete the "
            "entry.\n"
            "       Keeping it understates coverage and silently absorbs the failure "
            "if replay ever stops.\n" % (fixture, scenario_id)
        )
        problems += 1

if positional:
    # Reported, never silently accepted. The fallback exists so this guard is not
    # blocked on a corpus edit; its visibility is what makes the gap fixable
    # upstream. Adding the missing identifiers is a lazily-spec change.
    sys.stderr.write(
        "NOTE: %d scenario id(s) fell back to the POSITIONAL index — the fixture "
        "carries\n"
        "      neither `id` nor `name`, so the ledger cannot survive a reorder "
        "upstream:\n" % len(positional)
    )
    for entry in sorted(set(positional)):
        sys.stderr.write("      %s\n" % entry)

if problems:
    sys.stderr.write("scenario coverage FAILED: %d problem(s)\n" % problems)
    sys.exit(1)

print(
    "scenario coverage OK: %d/%d scenarios of OPENED fixtures REPLAYED by the suite "
    "(%d excused; runtime ledger — these scenarios really ran)"
    % (replayed, total, len(excuses))
)
PY
