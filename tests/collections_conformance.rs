//! Cross-language conformance tests for the keyed cell collections layer
//! (`lazily-spec/cell-model.md#keyed-cell-collections`), which is **required of
//! every binding** — see the Binding Conformance Matrix in
//! `lazily-spec/protocol.md`.
//!
//! Unlike the IPC fixtures these are **compute** fixtures: lazily-rs loads the
//! `initial` state, replays each `step`'s `op`, and asserts the `expected`
//! observable effects (resulting `order`, `values`, `membership`, which reader
//! classes — `value` / `membership` / `order` — invalidate, and that an atomic
//! move keeps the entry's cell `handle_stable`). The reconciliation fixture is
//! declarative: diff `prior` → `target` and assert the emitted minimal op set
//! plus that stable entries are not invalidated by a sibling reorder.

mod common;

use std::cell::Cell as StdCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use common::Expect;
use lazily::{
    Block, Context, DiffOp, Match, SemTree, Source, SourceMap, SourceTree, TextCrdt,
    TextVersionVector, align, apply_to_map, assign_stable_keys, block_key, reconcile,
};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/collections";

/// Cell value type used across all collection fixtures (JSON integers).
type V = i64;

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn spec_fixtures_present() -> bool {
    std::path::Path::new(SPEC_DIR).exists()
}

fn build_initial(ctx: &Context, initial: &Value) -> SourceMap<String, V> {
    let map: SourceMap<String, V> = SourceMap::new(ctx);
    let order = initial
        .get("order")
        .and_then(|v| v.as_array())
        .expect("initial.order");
    let values = initial
        .get("values")
        .and_then(|v| v.as_object())
        .expect("initial.values");
    for k in order {
        let key = k.as_str().expect("order key").to_string();
        let val = values
            .get(&key)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("missing initial value for {key}"));
        map.entry(ctx, key, val);
    }
    map
}

fn value_reader(
    ctx: &Context,
    map: &SourceMap<String, V>,
    key: &str,
) -> lazily::Computed<Option<V>> {
    let map = map.clone();
    let key = key.to_string();
    ctx.computed(move |ctx| map.get(ctx, &key))
}

/// Apply a fixture `op` to the live collection. Returns the removed key, if any,
/// so the caller can exclude it from the survivor value-reader checks.
fn apply_op(ctx: &Context, map: &SourceMap<String, V>, op: &Value) -> Option<String> {
    let ty = op.get("type").and_then(|v| v.as_str()).expect("op.type");
    match ty {
        "set_value" => {
            let key = op.get("key").and_then(|v| v.as_str()).unwrap().to_string();
            let val = op.get("value").and_then(|v| v.as_i64()).unwrap();
            map.set(ctx, key, val);
            None
        }
        "insert" => {
            let key = op.get("key").and_then(|v| v.as_str()).unwrap().to_string();
            let val = op.get("value").and_then(|v| v.as_i64()).unwrap();
            map.entry(ctx, key.clone(), val);
            // `at` is optional: "end" (the default after `entry`) or a 0-based index.
            if let Some(idx) = op.get("at").and_then(|v| v.as_u64()) {
                map.move_to(ctx, &key, idx as usize);
            }
            None
        }
        "remove" => {
            let key = op.get("key").and_then(|v| v.as_str()).unwrap().to_string();
            map.remove(ctx, &key);
            Some(key)
        }
        "move_to" => {
            let key = op.get("key").and_then(|v| v.as_str()).unwrap().to_string();
            let idx = op.get("index").and_then(|v| v.as_u64()).unwrap() as usize;
            map.move_to(ctx, &key, idx);
            None
        }
        "move_before" => {
            let key = op.get("key").and_then(|v| v.as_str()).unwrap().to_string();
            let before = op
                .get("before")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string();
            map.move_before(ctx, &key, &before);
            None
        }
        "move_after" => {
            let key = op.get("key").and_then(|v| v.as_str()).unwrap().to_string();
            let after = op
                .get("after")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string();
            map.move_after(ctx, &key, &after);
            None
        }
        other => panic!("unknown collection op type: {other}"),
    }
}

fn assert_state(ctx: &Context, map: &SourceMap<String, V>, expected: &Expect) {
    if let Some(order) = expected["order"].as_array() {
        let want: Vec<String> = order
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(map.keys(ctx), want, "order mismatch");
    }
    if let Some(membership) = expected["membership"].as_array() {
        let want: HashSet<String> = membership
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let got: HashSet<String> = map.keys(ctx).into_iter().collect();
        assert_eq!(got, want, "membership mismatch");
    }
    if let Some(values) = expected["values"].as_object() {
        for (key, val) in values {
            let want = val
                .as_i64()
                .unwrap_or_else(|| panic!("non-integer value for {key}"));
            let got = map
                .get(ctx, key)
                .unwrap_or_else(|| panic!("missing key {key} after op"));
            assert_eq!(got, want, "value mismatch for {key}");
        }
    }
}

/// Verify the `invalidates` reader-class independence contract: among survivors,
/// exactly the listed value readers invalidate; the membership reader matches
/// `membership`; the order reader matches `order`.
fn assert_invalidation(
    ctx: &Context,
    value_readers: &HashMap<String, lazily::Computed<Option<V>>>,
    membership_reader: &lazily::Computed<usize>,
    order_reader: &lazily::Computed<Vec<String>>,
    invalidates: &Value,
    survivors: &HashSet<String>,
) {
    let value_inv: HashSet<String> = invalidates
        .get("value")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().map(|v| v.as_str().unwrap().to_string()).collect())
        .unwrap_or_default();

    for key in survivors {
        let Some(reader) = value_readers.get(key) else {
            continue;
        };
        let cached = ctx.is_set(reader);
        if value_inv.contains(key) {
            assert!(
                !cached,
                "value reader for `{key}` should have been invalidated"
            );
        } else {
            assert!(
                cached,
                "value reader for `{key}` should have stayed cached (unrelated change)"
            );
        }
    }

    let mem_inv = invalidates
        .get("membership")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mem_cached = ctx.is_set(membership_reader);
    if mem_inv {
        assert!(
            !mem_cached,
            "membership reader should have been invalidated"
        );
    } else {
        assert!(
            mem_cached,
            "membership reader should have stayed cached (set identity unchanged)"
        );
    }

    let ord_inv = invalidates
        .get("order")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let ord_cached = ctx.is_set(order_reader);
    if ord_inv {
        assert!(!ord_cached, "order reader should have been invalidated");
    } else {
        assert!(
            ord_cached,
            "order reader should have stayed cached (order unchanged)"
        );
    }
}

/// Verify the `handle_stable` contract: a key marked stable keeps the SAME cell
/// handle (node identity) across the op — the atomic-move guarantee rather than
/// a remove + re-mint.
fn assert_handle_stable(
    map: &SourceMap<String, V>,
    expected: &Expect,
    handle_before: &HashMap<String, Option<Source<V>>>,
) {
    let Some(hs) = expected["handle_stable"].as_object() else {
        return;
    };
    for (key, want) in hs {
        if !want.as_bool().unwrap_or(false) {
            continue;
        }
        let before = handle_before
            .get(key)
            .unwrap_or_else(|| panic!("no handle captured for `{key}` before op"));
        let after = map.handle(key);
        assert_eq!(
            after, *before,
            "handle_stable{{{key}}} violated: cell handle (node identity) changed across op"
        );
        // A stable handle is observable too: it still reads a value (the cell lives on).
        assert!(
            after.is_some(),
            "handle_stable{{{key}}} violated: handle missing after op"
        );
    }
}

/// Replay a step-based collection fixture (`initial` + `steps`).
fn run_steps_fixture(name: &str) {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} absent - run with the lazily-spec sibling");
        return;
    }
    let fixture = load_fixture(name);
    let ctx = Context::new();
    let map = build_initial(&ctx, fixture.get("initial").expect("initial"));

    let steps = fixture
        .get("steps")
        .and_then(|v| v.as_array())
        .expect("steps");

    for (i, step) in steps.iter().enumerate() {
        let op = step.get("op").expect("op");
        // Guard the step's `expected` block (`#lzassertunknownkeys`): a key this
        // runner never reads fails the fixture instead of passing unnoticed.
        let expected = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("steps[{i}].expected"),
            step.get("expected").expect("expected"),
        );

        // Build + prime value/membership/order readers from the CURRENT key set
        // so each step's invalidation is measured in isolation.
        let current_keys = map.keys(&ctx);
        let mut value_readers: HashMap<String, lazily::Computed<Option<V>>> = HashMap::new();
        for key in &current_keys {
            value_readers.insert(key.clone(), value_reader(&ctx, &map, key));
        }
        let membership_reader = {
            let map = map.clone();
            ctx.computed(move |ctx| map.len(ctx))
        };
        let order_reader = {
            let map = map.clone();
            ctx.computed(move |ctx| map.keys(ctx))
        };
        for reader in value_readers.values() {
            ctx.get(reader);
        }
        ctx.get(&membership_reader);
        ctx.get(&order_reader);

        // Snapshot handles (node identities) before the op for handle_stable.
        let handle_before: HashMap<String, Option<Source<V>>> = current_keys
            .iter()
            .map(|k| (k.clone(), map.handle(k)))
            .collect();

        apply_op(&ctx, &map, op);

        let survivors: HashSet<String> = map.keys(&ctx).into_iter().collect();

        // `invalidates` lives under `expected`, not on the step. Reading it off
        // the step silently skipped the whole reader-class independence contract.
        let invalidates = &expected["invalidates"];
        assert!(
            invalidates.is_object(),
            "step {i}: expected.invalidates missing from fixture {name}"
        );
        assert_invalidation(
            &ctx,
            &value_readers,
            &membership_reader,
            &order_reader,
            invalidates,
            &survivors,
        );
        assert_handle_stable(&map, &expected, &handle_before);
        assert_state(&ctx, &map, &expected);
    }
}

/// Replay the declarative LIS reconciliation fixture (`reconcile` block).
fn run_reconcile_fixture(name: &str) {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} absent - run with the lazily-spec sibling");
        return;
    }
    let fixture = load_fixture(name);
    let reconcile_block = fixture.get("reconcile").expect("reconcile");
    let prior = keyed_pairs(reconcile_block, "prior");
    let target = keyed_pairs(reconcile_block, "target");
    let expected = Expect::new(
        format!("{SPEC_DIR}/{name}"),
        "expected",
        fixture.get("expected").expect("expected"),
    );
    let result_order: Vec<String> = expected["result_order"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();

    // 1. The emitted op set is minimal and matches the fixture op-for-op.
    let ops = reconcile(&prior, &target);
    let expected_ops = expected["ops"].as_array().unwrap();
    assert_eq!(
        ops.len(),
        expected_ops.len(),
        "op count must be minimal (LIS); emitted {ops:?}"
    );
    for expected_op in expected_ops {
        let ty = expected_op.get("type").and_then(|v| v.as_str()).unwrap();
        let key = expected_op
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        match ty {
            "remove" => assert!(
                ops.iter()
                    .any(|o| matches!(o, DiffOp::Remove { key: k } if k == &key)),
                "expected Remove{{{key}}} in emitted ops {ops:?}"
            ),
            "insert" => assert!(
                ops.iter()
                    .any(|o| matches!(o, DiffOp::Insert { key: k, .. } if k == &key)),
                "expected Insert{{{key}}} in emitted ops {ops:?}"
            ),
            "update" => assert!(
                ops.iter()
                    .any(|o| matches!(o, DiffOp::Update { key: k, .. } if k == &key)),
                "expected Update{{{key}}} in emitted ops {ops:?}"
            ),
            "move" => {
                // Resolve the fixture's (possibly relative) anchor to the key's
                // absolute final index, which is what DiffOp::Move carries.
                let want_to = if let Some(after) = expected_op.get("after").and_then(|v| v.as_str())
                {
                    result_order.iter().position(|k| k == after).unwrap() + 1
                } else if let Some(before) = expected_op.get("before").and_then(|v| v.as_str()) {
                    result_order.iter().position(|k| k == before).unwrap()
                } else {
                    expected_op.get("to").and_then(|v| v.as_u64()).unwrap() as usize
                };
                assert!(
                    ops.iter().any(|o| matches!(
                        o,
                        DiffOp::Move { key: k, to } if k == &key && *to == want_to
                    )),
                    "expected Move{{{key}, to={want_to}}} in emitted ops {ops:?}"
                );
            }
            other => panic!("unknown expected op type: {other}"),
        }
    }

    // 2. Driving a live SourceMap from `prior` with the op set converges to
    //    result_order.
    let ctx = Context::new();
    let map: SourceMap<String, V> = SourceMap::new(&ctx);
    for (k, v) in &prior {
        map.entry(&ctx, k.clone(), *v);
    }
    apply_to_map(&ctx, &map, &ops);
    assert_eq!(
        map.keys(&ctx),
        result_order,
        "reconcile op set did not converge to result_order"
    );

    // 3. Stable entries' value cells are NOT invalidated by the sibling reorder.
    let stable: Vec<String> = expected["stable_keys_not_invalidated"]
        .as_array()
        .map(|a| a.iter().map(|v| v.as_str().unwrap().to_string()).collect())
        .unwrap_or_default();
    if !stable.is_empty() {
        let ctx = Context::new();
        let map: SourceMap<String, V> = SourceMap::new(&ctx);
        for (k, v) in &prior {
            map.entry(&ctx, k.clone(), *v);
        }
        let readers: HashMap<String, lazily::Computed<Option<V>>> = stable
            .iter()
            .map(|k| (k.clone(), value_reader(&ctx, &map, k)))
            .collect();
        for reader in readers.values() {
            ctx.get(reader);
        }
        map.reconcile(&ctx, &target);
        for key in &stable {
            assert!(
                ctx.is_set(&readers[key]),
                "stable entry `{key}` value cell must not be invalidated by sibling reorder"
            );
        }
    }
}

fn keyed_pairs(v: &Value, field: &str) -> Vec<(String, V)> {
    let block = v.get(field).unwrap_or_else(|| panic!("reconcile.{field}"));
    let order = block.get("order").and_then(|v| v.as_array()).unwrap();
    let values = block.get("values").and_then(|v| v.as_object()).unwrap();
    order
        .iter()
        .map(|k| {
            let key = k.as_str().unwrap().to_string();
            let val = values
                .get(&key)
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| panic!("missing {field} value for {key}"));
            (key, val)
        })
        .collect()
}

#[test]
fn conformance_cellmap_independence() {
    run_steps_fixture("cellmap_independence.json");
}

#[test]
fn conformance_cellmap_atomic_move() {
    run_steps_fixture("cellmap_atomic_move.json");
}

#[test]
fn conformance_keyed_reconciliation_lis() {
    run_reconcile_fixture("keyed_reconciliation_lis.json");
}

// === SemTree (memoized semantic tree) ======================================
// Replay `semtree_incremental.json`: one memo slot per node folds
// (node value, child derived values). Editing one node recomputes only its
// ANCESTOR CHAIN (sibling subtrees stay cached); a node edit that doesn't
// change the folded result MUST NOT re-run a downstream consumer (memo guard).

fn build_sem_tree(ctx: &Context, node: &Value) -> SourceTree<String, i64> {
    let id = node.get("id").and_then(|v| v.as_str()).unwrap().to_string();
    let value = node.get("value").and_then(|v| v.as_i64()).unwrap();
    let root = SourceTree::leaf(ctx, id, value);
    if let Some(children) = node.get("children") {
        let order = children.get("order").and_then(|v| v.as_array()).unwrap();
        let values = children.get("values").and_then(|v| v.as_object()).unwrap();
        for kid in order {
            let kid_id = kid.as_str().unwrap();
            let kid_node = values.get(kid_id).unwrap();
            let subtree = build_sem_tree(ctx, kid_node);
            root.attach_child(ctx, subtree);
        }
    }
    root
}

/// Recursively search for a node by id (SourceTree::child only sees one level).
fn find_in_tree<V: PartialEq + Clone + 'static>(
    ctx: &Context,
    node: &SourceTree<String, V>,
    id: &str,
) -> Option<SourceTree<String, V>> {
    if node.id() == id {
        return Some(node.clone());
    }
    for child in node.children(ctx) {
        if let Some(found) = find_in_tree(ctx, &child, id) {
            return Some(found);
        }
    }
    None
}

fn run_semtree_fixture(name: &str) {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} absent - run with the lazily-spec sibling");
        return;
    }
    let fixture = load_fixture(name);
    let scenarios = fixture
        .get("scenarios")
        .and_then(|v| v.as_array())
        .expect("semtree scenarios");

    for (i, scenario) in scenarios.iter().enumerate() {
        let fold = scenario
            .get("fold")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("scenario {i}: missing fold"));
        let tree_json = scenario.get("tree").unwrap();
        let ctx = Context::new();
        let root = build_sem_tree(&ctx, tree_json);

        match fold {
            "sum" => {
                let sums = SemTree::build(&ctx, &root, |v: &i64, kids: &[i64]| {
                    v + kids.iter().sum::<i64>()
                });
                let expect_initial = scenario.get("expect_initial").unwrap();
                assert_eq!(
                    sums.value(&ctx),
                    expect_field_i64(expect_initial, "root"),
                    "scenario {i} initial root"
                );
                if let Some(a) = expect_initial.get("a").and_then(|v| v.as_i64()) {
                    assert_eq!(
                        sums.node_value(&ctx, &"a".to_string()),
                        Some(a),
                        "scenario {i} initial a"
                    );
                }

                // Prime sibling slot cache before edit so we can verify isolation.
                let a_slot = sums.node(&"a".to_string());
                let b_slot = sums.node(&"b".to_string());

                if let Some(edit) = scenario.get("edit") {
                    let id = edit.get("id").and_then(|v| v.as_str()).unwrap().to_string();
                    let value = edit.get("value").and_then(|v| v.as_i64()).unwrap();
                    let node = find_in_tree(&ctx, &root, &id)
                        .unwrap_or_else(|| panic!("scenario {i}: edit target {id} not in tree"));
                    node.set(&ctx, value);
                } else if let Some(rm) = scenario.get("remove_child") {
                    let parent_id = rm
                        .get("parent")
                        .and_then(|v| v.as_str())
                        .unwrap()
                        .to_string();
                    let child_id = rm
                        .get("child")
                        .and_then(|v| v.as_str())
                        .unwrap()
                        .to_string();
                    let parent = find_in_tree(&ctx, &root, &parent_id).unwrap_or_else(|| {
                        panic!("scenario {i}: remove parent {parent_id} missing")
                    });
                    assert!(
                        parent.remove_child(&ctx, &child_id),
                        "scenario {i}: remove_child {child_id} not found under {parent_id}"
                    );
                }

                let expect_after = scenario.get("expect_after").unwrap();
                assert_eq!(
                    sums.value(&ctx),
                    expect_field_i64(expect_after, "root"),
                    "scenario {i}: root after edit"
                );

                if let Some(sibling_cached) = expect_after
                    .get("sibling_a_cached")
                    .and_then(|v| v.as_bool())
                {
                    let a_slot =
                        a_slot.expect("scenario checks sibling_a_cached but no `a` node slot");
                    assert_eq!(
                        ctx.is_set(&a_slot),
                        sibling_cached,
                        "scenario {i}: sibling_a_cached contract ({}cached expected)",
                        if sibling_cached { "" } else { "un" }
                    );
                }
                if let Some(b) = expect_after.get("b").and_then(|v| v.as_i64()) {
                    let b_slot = b_slot.expect("expect_after.b present but no `b` slot");
                    assert_eq!(ctx.get(&b_slot), b, "scenario {i}: b after edit");
                }
                if let Some(a) = expect_after.get("a").and_then(|v| v.as_i64()) {
                    let a_slot = a_slot.expect("expect_after.a present but no `a` slot");
                    assert_eq!(ctx.get(&a_slot), a, "scenario {i}: a unchanged after edit");
                }
            }
            "count_positive" => {
                let count = SemTree::build(&ctx, &root, |v: &i64, kids: &[usize]| {
                    (if *v > 0 { 1usize } else { 0 }) + kids.iter().sum::<usize>()
                });
                let expect_initial = scenario.get("expect_initial").unwrap();
                assert_eq!(
                    count.value(&ctx),
                    expect_field_i64(expect_initial, "root") as usize,
                    "scenario {i}: initial positive count"
                );

                // Downstream consumer of the derived root; count how often it re-runs.
                let calls = Rc::new(StdCell::new(0usize));
                let root_slot = count.root();
                let observer = {
                    let calls = Rc::clone(&calls);
                    ctx.computed(move |ctx| {
                        calls.set(calls.get() + 1);
                        ctx.get(&root_slot)
                    })
                };
                assert_eq!(ctx.get(&observer), count.value(&ctx));
                let calls_before = calls.get();

                if let Some(edit) = scenario.get("edit") {
                    let id = edit.get("id").and_then(|v| v.as_str()).unwrap().to_string();
                    let value = edit.get("value").and_then(|v| v.as_i64()).unwrap();
                    let node = find_in_tree(&ctx, &root, &id)
                        .unwrap_or_else(|| panic!("scenario {i}: edit target {id} not in tree"));
                    node.set(&ctx, value);
                }

                let expect_after = scenario.get("expect_after").unwrap();
                assert_eq!(
                    count.value(&ctx),
                    expect_field_i64(expect_after, "root") as usize,
                    "scenario {i}: positive count after edit"
                );
                let _ = ctx.get(&observer); // pull observer
                if let Some(reran) = expect_after
                    .get("downstream_consumer_reran")
                    .and_then(|v| v.as_bool())
                {
                    let did_rerun = calls.get() > calls_before;
                    assert_eq!(
                        did_rerun, reran,
                        "scenario {i}: downstream_consumer_reran contract ({reran} expected)"
                    );
                }
            }
            other => panic!("scenario {i}: unknown fold {other}"),
        }
    }
}

fn expect_field_i64(v: &Value, field: &str) -> i64 {
    v.get(field)
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| panic!("missing {field}: {v}"))
}

#[test]
fn conformance_semtree_incremental() {
    run_semtree_fixture("semtree_incremental.json");
}

// === StableId (manufactured text identity) =================================
// Replay `stableid_alignment.json`: anchor/content/similarity layers.

fn build_blocks(v: &Value, field: &str) -> Vec<Block> {
    let arr = v
        .get(field)
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("missing {field}"));
    arr.iter().map(block_from_json).collect()
}

fn block_from_json(v: &Value) -> Block {
    let text = v.get("text").and_then(|v| v.as_str()).unwrap().to_string();
    if let Some(anchor) = v.get("anchor").and_then(|v| v.as_str()) {
        Block::anchored(anchor.to_string(), text)
    } else {
        Block::text(text)
    }
}

fn run_stableid_fixture(name: &str) {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} absent - run with the lazily-spec sibling");
        return;
    }
    let fixture = load_fixture(name);
    let scenarios = fixture
        .get("scenarios")
        .and_then(|v| v.as_array())
        .expect("stableid scenarios");

    for (i, scenario) in scenarios.iter().enumerate() {
        // Guard the scenario's `expect` block (`#lzassertunknownkeys`).
        let expect = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("scenarios[{i}].expect"),
            scenario.get("expect").unwrap_or(&Value::Null),
        );
        // --- key-equality scenarios (blocks, no old/new) ---
        if let Some(blocks_json) = scenario.get("blocks") {
            let blocks: Vec<Block> = blocks_json
                .as_array()
                .unwrap()
                .iter()
                .map(block_from_json)
                .collect();
            if let Some(pairs) = expect["key_equal"].as_array() {
                for pair in pairs {
                    let a = pair.get(0).and_then(|v| v.as_u64()).unwrap() as usize;
                    let b = pair.get(1).and_then(|v| v.as_u64()).unwrap() as usize;
                    assert_eq!(
                        block_key(&blocks[a]),
                        block_key(&blocks[b]),
                        "scenario {i}: blocks {a}/{b} keys should be equal"
                    );
                }
            }
            if let Some(pairs) = expect["key_not_equal"].as_array() {
                for pair in pairs {
                    let a = pair.get(0).and_then(|v| v.as_u64()).unwrap() as usize;
                    let b = pair.get(1).and_then(|v| v.as_u64()).unwrap() as usize;
                    assert_ne!(
                        block_key(&blocks[a]),
                        block_key(&blocks[b]),
                        "scenario {i}: blocks {a}/{b} keys should differ"
                    );
                }
            }
            continue;
        }

        // --- alignment scenarios (old + new) ---
        let old = build_blocks(scenario, "old");
        let new = build_blocks(scenario, "new");

        if let Some(matches) = expect["matches"].as_array() {
            let al = align(&old, &new);
            assert_eq!(
                al.new_matches.len(),
                matches.len(),
                "scenario {i}: matches length mismatch"
            );
            for (j, m) in matches.iter().enumerate() {
                let s = m.as_str().unwrap();
                let got = &al.new_matches[j];
                if let Some(rest) = s.strip_prefix("Same:") {
                    let idx: usize = rest.parse().unwrap();
                    assert!(
                        matches!(got, Match::Same { old } if *old == idx),
                        "scenario {i}.{j}: expected Same:{idx}, got {got:?}"
                    );
                } else if s == "Inserted" {
                    assert!(
                        matches!(got, Match::Inserted),
                        "scenario {i}.{j}: expected Inserted, got {got:?}"
                    );
                } else if let Some(rest) = s.strip_prefix("Edited:") {
                    let idx: usize = rest.parse().unwrap();
                    assert!(
                        matches!(got, Match::Edited { old, .. } if *old == idx),
                        "scenario {i}.{j}: expected Edited:{idx}, got {got:?}"
                    );
                    if let Some(min) = expect["similarity_min"].as_f64()
                        && let Match::Edited { similarity, .. } = got
                    {
                        assert!(
                            *similarity as f64 >= min,
                            "scenario {i}.{j}: similarity {similarity} < min {min}"
                        );
                    }
                } else {
                    panic!("scenario {i}.{j}: unknown match spec {s}");
                }
            }
            if let Some(removed) = expect["removed"].as_array() {
                let want: Vec<usize> = removed
                    .iter()
                    .map(|v| v.as_u64().unwrap() as usize)
                    .collect();
                assert_eq!(al.removed, want, "scenario {i}: removed mismatch");
            }
        }

        if let Some(pairs) = expect["new_key_equals_old_key"].as_array() {
            let old_keys: Vec<String> = old.iter().map(|b| block_key(b).as_string()).collect();
            let new_keys = assign_stable_keys(&old, &new);
            for pair in pairs {
                let new_idx = pair.get(0).and_then(|v| v.as_u64()).unwrap() as usize;
                let old_idx = pair.get(1).and_then(|v| v.as_u64()).unwrap() as usize;
                assert_eq!(
                    new_keys[new_idx], old_keys[old_idx],
                    "scenario {i}: new[{new_idx}] key should equal old[{old_idx}]"
                );
            }
        }
    }
}

#[test]
fn conformance_stableid_alignment() {
    run_stableid_fixture("stableid_alignment.json");
}

// === TextCrdt (Fugue/RGA character CRDT) ===================================
// Replay `textcrdt_convergence.json`: commutative/idempotent merge, concurrent
// same-point inserts, sticky tombstones, GC.

fn run_textcrdt_fixture(name: &str) {
    if !spec_fixtures_present() {
        eprintln!("skipping: {SPEC_DIR} absent - run with the lazily-spec sibling");
        return;
    }
    let fixture = load_fixture(name);
    let scenarios = fixture
        .get("scenarios")
        .and_then(|v| v.as_array())
        .expect("textcrdt scenarios");

    for (i, scenario) in scenarios.iter().enumerate() {
        let mut replicas: HashMap<String, TextCrdt> = HashMap::new();

        // Seed: either a peer-only replica (empty) or a seeded text replica.
        if let Some(seed_text) = scenario.get("seed").and_then(|v| v.as_str()) {
            // seed is a raw string -> single-peer replica named "a".
            let peer = scenario
                .get("replica")
                .and_then(|v| v.get("peer"))
                .and_then(|v| v.as_u64())
                .unwrap_or(1);
            replicas.insert("a".to_string(), TextCrdt::from_str(peer, seed_text));
        } else if let Some(seed_obj) = scenario.get("seed").and_then(|v| v.as_object()) {
            let peer = seed_obj.get("peer").and_then(|v| v.as_u64()).unwrap();
            let text = seed_obj.get("text").and_then(|v| v.as_str()).unwrap();
            replicas.insert("a".to_string(), TextCrdt::from_str(peer, text));
        } else if let Some(rep) = scenario.get("replica") {
            let peer = rep.get("peer").and_then(|v| v.as_u64()).unwrap();
            replicas.insert("a".to_string(), TextCrdt::new(peer));
        } else {
            panic!("scenario {i}: missing seed or replica");
        }

        for step in scenario
            .get("steps")
            .and_then(|v| v.as_array())
            .expect("textcrdt steps")
        {
            if let Some(fork_name) = step.get("fork").and_then(|v| v.as_str()) {
                let peer = step.get("peer").and_then(|v| v.as_u64()).unwrap();
                let src = replicas
                    .get("a")
                    .unwrap_or_else(|| panic!("scenario {i}: fork from missing `a`"));
                replicas.insert(fork_name.to_string(), src.fork(peer));
            } else if let Some(new_name) = step.get("clone").and_then(|v| v.as_str()) {
                // `{ "clone": "ab", "from": "a" }` -> clone `a` as `ab`.
                let from = step
                    .get("from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: clone missing `from`"));
                let cloned = replicas
                    .get(from)
                    .unwrap_or_else(|| panic!("scenario {i}: clone from missing `{from}`"))
                    .clone();
                replicas.insert(new_name.to_string(), cloned);
            } else if let Some(merge) = step.get("merge").and_then(|v| v.as_object()) {
                let into = merge
                    .get("into")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: merge missing `into`"));
                let from = merge
                    .get("from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: merge missing `from`"));
                // Clone-out to satisfy the borrow checker (mutable + shared borrow).
                let from_state = replicas
                    .get(from)
                    .unwrap_or_else(|| panic!("scenario {i}: merge from missing `{from}`"))
                    .clone();
                replicas
                    .get_mut(into)
                    .unwrap_or_else(|| panic!("scenario {i}: merge into missing `{into}`"))
                    .merge(&from_state);
            } else if let Some(name) = step.get("new").and_then(|v| v.as_str()) {
                // `{ "new": "b", "peer": 2 }` -> fresh empty replica.
                let peer = step.get("peer").and_then(|v| v.as_u64()).unwrap();
                replicas.insert(name.to_string(), TextCrdt::new(peer));
            } else if let Some(d) = step.get("delta").and_then(|v| v.as_object()) {
                // `{ "delta": { "into": "b", "from": "a" } }` -> b.apply_delta(a.delta_since(b.vv)).
                let into = d
                    .get("into")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: delta missing `into`"));
                let from = d
                    .get("from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: delta missing `from`"));
                let their_vv = replicas
                    .get(into)
                    .unwrap_or_else(|| panic!("scenario {i}: delta into missing `{into}`"))
                    .version_vector();
                let ops = replicas
                    .get(from)
                    .unwrap_or_else(|| panic!("scenario {i}: delta from missing `{from}`"))
                    .delta_since(&their_vv);
                let changed = replicas
                    .get_mut(into)
                    .unwrap_or_else(|| panic!("scenario {i}: delta into missing `{into}`"))
                    .apply_delta(&ops);
                if let Some(expect) = step.get("expect_changed").and_then(|v| v.as_bool()) {
                    assert_eq!(
                        changed, expect,
                        "scenario {i}: delta {from}->{into} expect_changed={expect} got={changed}"
                    );
                }
            } else if let Some(s) = step.get("snapshot").and_then(|v| v.as_object()) {
                // `{ "snapshot": { "from": "a", "into": "b", "peer": 2 } }` -> a
                // whole-state snapshot is delta_since({}); apply_delta onto a fresh
                // replica preserves OpId identity (#lztextsync).
                let from = s
                    .get("from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: snapshot missing `from`"));
                let into = s
                    .get("into")
                    .and_then(|v| v.as_str())
                    .unwrap_or_else(|| panic!("scenario {i}: snapshot missing `into`"));
                let peer = s
                    .get("peer")
                    .and_then(|v| v.as_u64())
                    .unwrap_or_else(|| panic!("scenario {i}: snapshot missing `peer`"));
                let empty = TextVersionVector::new();
                let ops = replicas
                    .get(from)
                    .unwrap_or_else(|| panic!("scenario {i}: snapshot from missing `{from}`"))
                    .delta_since(&empty);
                let mut replica = TextCrdt::new(peer);
                let changed = replica.apply_delta(&ops);
                replicas.insert(into.to_string(), replica);
                if let Some(expect) = step.get("expect_changed").and_then(|v| v.as_bool()) {
                    assert_eq!(
                        changed, expect,
                        "scenario {i}: snapshot {from}->{into} expect_changed={expect} got={changed}"
                    );
                }
            } else if let Some(pair) = step.get("exchange").and_then(|v| v.as_array()) {
                // `{ "exchange": ["x", "y"] }` -> bidirectional delta sync: each
                // replica apply_delta's the partner's delta_since(its own vv).
                assert!(
                    pair.len() == 2,
                    "scenario {i}: exchange must name two replicas"
                );
                let x = pair[0].as_str().unwrap();
                let y = pair[1].as_str().unwrap();
                let to_x = {
                    let xv = replicas
                        .get(x)
                        .unwrap_or_else(|| panic!("scenario {i}: exchange missing `{x}`"))
                        .version_vector();
                    replicas
                        .get(y)
                        .unwrap_or_else(|| panic!("scenario {i}: exchange missing `{y}`"))
                        .delta_since(&xv)
                };
                let to_y = {
                    let yv = replicas.get(y).unwrap().version_vector();
                    replicas.get(x).unwrap().delta_since(&yv)
                };
                replicas.get_mut(x).unwrap().apply_delta(&to_x);
                replicas.get_mut(y).unwrap().apply_delta(&to_y);
            } else if let Some(on) = step.get("on").and_then(|v| v.as_str()) {
                apply_textcrdt_op(
                    replicas
                        .get_mut(on)
                        .unwrap_or_else(|| panic!("scenario {i}: `on` target `{on}` missing")),
                    step,
                );
            } else if step.get("op").is_some() {
                // No `on`: apply to default replica "a".
                apply_textcrdt_op(
                    replicas
                        .get_mut("a")
                        .expect("scenario {i}: default target `a` missing"),
                    step,
                );
            } else {
                panic!("scenario {i}: unrecognized step {step}");
            }
        }

        // Assertions. The `expect` block is guarded (`#lzassertunknownkeys`).
        let expect = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("scenarios[{i}].expect"),
            scenario.get("expect").unwrap_or(&Value::Null),
        );
        if let Some(text) = expect["text"].as_str() {
            assert_eq!(replicas["a"].text(), text, "scenario {i}: text mismatch");
        }
        if let Some(len) = expect["len"].as_u64() {
            // `len` applies to the converged replica named `a`, or to all
            // replicas referenced by `texts_equal`/`orders_equal`.
            let target = expect["text"]
                .as_str()
                .map(|_| "a".to_string())
                .or_else(|| {
                    let group = match expect["texts_equal"].as_array() {
                        Some(g) => Some(g),
                        None => expect["orders_equal"].as_array(),
                    };
                    group
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_array())
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .unwrap_or_else(|| "a".to_string());
            assert_eq!(
                replicas[&target].len() as u64,
                len,
                "scenario {i}: len mismatch on `{target}`"
            );
        }
        if let Some(pairs) = expect["texts_equal"].as_array() {
            for pair in pairs {
                let a = pair.get(0).and_then(|v| v.as_str()).unwrap();
                let b = pair.get(1).and_then(|v| v.as_str()).unwrap();
                assert_eq!(
                    replicas[a].text(),
                    replicas[b].text(),
                    "scenario {i}: `{a}`/`{b}` texts should converge"
                );
            }
        }
        if let Some(text_on) = expect["text_on"].as_object() {
            for (name, val) in text_on {
                let want = val.as_str().unwrap_or_else(|| {
                    panic!("scenario {i}: text_on `{name}` value must be a string")
                });
                let got = replicas
                    .get(name)
                    .unwrap_or_else(|| panic!("scenario {i}: text_on names missing `{name}`"))
                    .text();
                assert_eq!(got, want, "scenario {i}: text_on `{name}` mismatch");
            }
        }
        if let Some(vv_on) = expect["version_vector_on"].as_object() {
            for (name, val) in vv_on {
                let want = val.as_object().unwrap_or_else(|| {
                    panic!("scenario {i}: version_vector_on `{name}` must be an object")
                });
                let got = serde_json::to_value(
                    replicas
                        .get(name)
                        .unwrap_or_else(|| {
                            panic!("scenario {i}: version_vector_on names missing `{name}`")
                        })
                        .version_vector(),
                )
                .unwrap_or_else(|e| panic!("scenario {i}: vv serialize `{name}`: {e}"));
                let got_obj = got.as_object().unwrap();
                // BTreeMap<u64,u64> serializes to {"<peer>": <counter>}; compare as JSON.
                assert_eq!(
                    got_obj, want,
                    "scenario {i}: version_vector_on `{name}` mismatch"
                );
            }
        }
        if let Some(prefix) = expect["a_starts_with"].as_str() {
            assert!(
                replicas["a"].text().starts_with(prefix),
                "scenario {i}: `a` should start with `{prefix}`"
            );
        }
        if let Some(suffix) = expect["a_ends_with"].as_str() {
            assert!(
                replicas["a"].text().ends_with(suffix),
                "scenario {i}: `a` should end with `{suffix}`"
            );
        }
        if let Some(tc) = expect["tombstone_count"].as_u64() {
            assert_eq!(
                replicas["a"].tombstone_count() as u64,
                tc,
                "scenario {i}: tombstone_count mismatch"
            );
        }
    }
}

fn apply_textcrdt_op(t: &mut TextCrdt, op: &Value) {
    let kind = op
        .get("op")
        .and_then(|v| v.as_str())
        .expect("textcrdt op.op");
    match kind {
        "insert" => {
            let index = op.get("index").and_then(|v| v.as_u64()).unwrap() as usize;
            let ch = op
                .get("ch")
                .and_then(|v| v.as_str())
                .unwrap()
                .chars()
                .next()
                .unwrap();
            t.insert(index, ch);
        }
        "insert_str" => {
            let index = op.get("index").and_then(|v| v.as_u64()).unwrap() as usize;
            let s = op.get("str").and_then(|v| v.as_str()).unwrap();
            t.insert_str(index, s);
        }
        "delete" => {
            let index = op.get("index").and_then(|v| v.as_u64()).unwrap() as usize;
            t.delete(index);
        }
        "gc" => {
            let stable = op.get("stable").and_then(|v| v.as_bool()).unwrap();
            let collected = t.gc_with(|_| stable);
            if let Some(expect) = op.get("expect_collected").and_then(|v| v.as_u64()) {
                assert_eq!(collected as u64, expect, "gc expect_collected mismatch");
            }
        }
        other => panic!("unknown textcrdt op: {other}"),
    }
}

#[test]
fn conformance_textcrdt_convergence() {
    run_textcrdt_fixture("textcrdt_convergence.json");
}

#[test]
fn conformance_textcrdt_delta_sync() {
    run_textcrdt_fixture("textcrdt_delta_sync.json");
}
