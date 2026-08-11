//! Cross-language conformance for the lossless full-document tree CRDT
//! (`lazily-spec/lossless-tree-crdt.md`), replaying the shared compute fixtures in
//! `lazily-spec/conformance/lossless-tree/`.
//!
//! Each fixture builds an initial tree on replica `a`, runs a schedule of ops /
//! forks / anti-entropy syncs across named replicas, and asserts exact rendered
//! text, live-node counts, and convergence across delivery orders — the same
//! `{scenarios: [{seed, steps, expect}]}` shape as the other collections fixtures.
//! The lossless invariant `render(tree) == source_text` is what every assertion
//! ultimately checks. Feature-gated because `LosslessTreeCrdt` lives behind the
//! `lossless-tree` feature.

#![cfg(feature = "lossless-tree")]

mod common;

use std::collections::HashMap;

use common::Expect;
use lazily::{LeafKind, LosslessTreeCrdt, NodeSeed, TreeNodeId, TreeUpdate};
use serde_json::Value;

const SPEC_DIR: common::SpecDir = common::SpecDir("lossless-tree");

fn load_fixture(name: &str) -> Value {
    let path = format!("{SPEC_DIR}/{name}");
    let raw = crate::common::spec_read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse fixture {path}: {e}"))
}

fn leaf_kind(s: &str) -> LeafKind {
    match s {
        "token" => LeafKind::Token,
        "trivia" => LeafKind::Trivia,
        "raw" => LeafKind::Raw,
        "error" => LeafKind::Error,
        other => panic!("unknown leaf kind: {other}"),
    }
}

fn node_seed(spec: &Value) -> NodeSeed {
    if let Some(kind) = spec.get("element").and_then(|v| v.as_str()) {
        NodeSeed::Element {
            kind: kind.to_string(),
        }
    } else if let Some(leaf) = spec.get("leaf").and_then(|v| v.as_object()) {
        NodeSeed::Leaf {
            kind: leaf_kind(leaf["kind"].as_str().expect("leaf.kind")),
            text: leaf["text"].as_str().expect("leaf.text").to_string(),
        }
    } else {
        panic!("node spec has neither `element` nor `leaf`: {spec}");
    }
}

/// A named world of replicas plus the shared label→id map. Nodes created on any
/// replica are addressed by stable string labels across the whole scenario.
struct World {
    replicas: HashMap<String, LosslessTreeCrdt>,
    ids: HashMap<String, TreeNodeId>,
}

impl World {
    fn id(&self, label: &str) -> TreeNodeId {
        *self
            .ids
            .get(label)
            .unwrap_or_else(|| panic!("unknown node label `{label}`"))
    }

    fn after_of(&self, op: &Value) -> Option<TreeNodeId> {
        match op.get("after") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(self.id(s)),
            other => panic!("bad `after`: {other:?}"),
        }
    }

    /// Recursively create `spec`'s children under `parent` on replica `a`.
    fn build_children(&mut self, spec: &Value, parent: TreeNodeId) {
        let Some(children) = spec.get("children").and_then(|v| v.as_array()) else {
            return;
        };
        let mut prev: Option<TreeNodeId> = None;
        for child in children {
            let label = child["label"].as_str().expect("node.label").to_string();
            let seed = node_seed(child);
            let id = self
                .replicas
                .get_mut("a")
                .unwrap()
                .create_node(parent, prev, seed)
                .expect("seed create");
            self.ids.insert(label, id);
            self.build_children(child, id);
            prev = Some(id);
        }
    }
}

fn apply_step(world: &mut World, step: &Value) {
    if let Some(name) = step.get("fork").and_then(|v| v.as_str()) {
        let peer = step["peer"].as_u64().expect("fork.peer");
        let forked = world.replicas["a"].fork(peer);
        world.replicas.insert(name.to_string(), forked);
    } else if let Some(name) = step.get("clone").and_then(|v| v.as_str()) {
        let from = step["from"].as_str().expect("clone.from");
        let cloned = world.replicas[from].clone();
        world.replicas.insert(name.to_string(), cloned);
    } else if let Some(sync) = step.get("sync").and_then(|v| v.as_object()) {
        let from = sync["from"].as_str().expect("sync.from");
        let to = sync["to"].as_str().expect("sync.to");
        let update = world.replicas[from].diff(&world.replicas[to].frontier());
        world.replicas.get_mut(to).unwrap().apply_update(&update);
    } else if let Some(deliver) = step.get("deliver").and_then(|v| v.as_object()) {
        let from = deliver["from"].as_str().expect("deliver.from");
        let to = deliver["to"].as_str().expect("deliver.to");
        // The canonical diff: EXACTLY the batch `sync` would send, which `diff`
        // already returns sorted by dotted `(counter, peer)`. Both selectors are
        // 0-based indexes INTO this list, so the two step shapes disagree only
        // about which entries and in what sequence — never about what the list is.
        let full = world.replicas[from].diff(&world.replicas[to].frontier());

        let indexes = |key: &str| -> Vec<usize> {
            deliver[key]
                .as_array()
                .unwrap_or_else(|| panic!("deliver.{key} must be an array of indexes"))
                .iter()
                .map(|v| {
                    v.as_u64().unwrap_or_else(|| {
                        panic!("deliver.{key} entries must be non-negative integers, got {v}")
                    }) as usize
                })
                .collect()
        };

        // Exactly one of `only` / `order`. Defaulting to either when both or
        // neither are present would silently replay a batch the fixture never
        // described — and the whole point of `order` is that the batch's
        // SEQUENCE is the thing under test.
        let picked: Vec<usize> = match (deliver.contains_key("only"), deliver.contains_key("order"))
        {
            // `only`: that SUBSET, delivered in canonical order.
            (true, false) => {
                let mut idx = indexes("only");
                idx.sort_unstable();
                idx
            }
            // `order`: exactly those entries, in the LISTED sequence, so an op
            // can arrive before the op it depends on. Deliberately not sorted,
            // deliberately not deduplicated, and need not cover the whole diff.
            (false, true) => indexes("order"),
            (true, true) => panic!(
                "deliver step carries BOTH `only` and `order`; exactly one is required: {step}"
            ),
            (false, false) => panic!(
                "deliver step carries NEITHER `only` nor `order`; exactly one is required: {step}"
            ),
        };

        // An index past the end of the canonical diff means the fixture and this
        // binding disagree about what `from` owes `to`. Clamping or skipping
        // would deliver a different batch under the fixture's name, so fail.
        let ops = picked
            .iter()
            .map(|&i| {
                full.ops.get(i).cloned().unwrap_or_else(|| {
                    panic!(
                        "deliver index {i} is out of range: the canonical diff from `{from}` \
                         to `{to}` holds {} op(s)",
                        full.ops.len()
                    )
                })
            })
            .collect();

        // ONE `apply_update` call. Splitting the batch across calls would let the
        // runner's own loop supply the ordering the library's dependency buffer
        // is supposed to supply, which is exactly the behaviour under test.
        world
            .replicas
            .get_mut(to)
            .unwrap()
            .apply_update(&TreeUpdate { ops });
    } else if let Some(on) = step.get("on").and_then(|v| v.as_str()) {
        apply_op(world, on, step);
    } else {
        panic!("unrecognized step: {step}");
    }
}

fn apply_op(world: &mut World, on: &str, op: &Value) {
    let kind = op["op"].as_str().expect("op.op");
    match kind {
        "create" => {
            let parent = world.id(op["parent"].as_str().expect("create.parent"));
            let after = world.after_of(op);
            let seed = node_seed(op);
            let label = op["label"].as_str().expect("create.label").to_string();
            let id = world
                .replicas
                .get_mut(on)
                .unwrap()
                .create_node(parent, after, seed)
                .expect("create");
            world.ids.insert(label, id);
        }
        "edit_leaf" => {
            let node = world.id(op["node"].as_str().expect("edit_leaf.node"));
            let at = op["at_byte"].as_u64().expect("at_byte") as usize;
            let del = op["delete_bytes"].as_u64().unwrap_or(0) as usize;
            let insert = op["insert"].as_str().unwrap_or("");
            world
                .replicas
                .get_mut(on)
                .unwrap()
                .edit_leaf(node, at, del, insert)
                .expect("edit_leaf");
        }
        "split" => {
            let node = world.id(op["node"].as_str().expect("split.node"));
            let at = op["at_byte"].as_u64().expect("split.at_byte") as usize;
            let label = op["new_label"]
                .as_str()
                .expect("split.new_label")
                .to_string();
            let new = world
                .replicas
                .get_mut(on)
                .unwrap()
                .split_leaf(node, at)
                .expect("split");
            world.ids.insert(label, new);
        }
        "merge_leaves" => {
            let left = world.id(op["left"].as_str().expect("merge.left"));
            let right = world.id(op["right"].as_str().expect("merge.right"));
            world
                .replicas
                .get_mut(on)
                .unwrap()
                .merge_adjacent_leaves(left, right)
                .expect("merge_leaves");
        }
        "reorder" => {
            let node = world.id(op["node"].as_str().expect("reorder.node"));
            let after = world.after_of(op);
            world
                .replicas
                .get_mut(on)
                .unwrap()
                .reorder_child(node, after)
                .expect("reorder");
        }
        "tombstone" => {
            let node = world.id(op["node"].as_str().expect("tombstone.node"));
            world
                .replicas
                .get_mut(on)
                .unwrap()
                .tombstone_node(node)
                .expect("tombstone");
        }
        other => panic!("unknown op: {other}"),
    }
}

fn assert_expect(world: &World, expect: &Expect, scenario: &str) {
    // Each key is optional per scenario, so the comparison is bound to the key's
    // *presence* rather than reached through a bare read that a missing key would
    // silently skip (`#lzconsumednotasserted`).
    expect.assert_key_if_present("render", |want| {
        assert_eq!(
            world.replicas["a"].render(),
            want.as_str().expect("render"),
            "{scenario}: render on `a`"
        );
    });
    // DESCENT (`#lzsubblockkeyset`): the replica names are the child's keys, so
    // a replica the corpus adds fails as an unconsumed key.
    if let Some(render_on) = expect.sub_if_present("render_on") {
        for name in render_on.raw().as_object().expect("render_on").keys() {
            render_on.assert_key_at(
                name.as_str(),
                world.replicas[name].render(),
                &format!("{scenario}.render_on"),
            );
        }
    }
    expect.assert_key_if_present("live_nodes", |want| {
        assert_eq!(
            world.replicas["a"].live_node_count() as u64,
            want.as_u64().expect("live_nodes"),
            "{scenario}: live_nodes on `a`"
        );
    });
    expect.assert_key_if_present("converged", |want| {
        let names: Vec<&str> = want
            .as_array()
            .expect("converged")
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let first = world.replicas[names[0]].render();
        for name in &names[1..] {
            assert_eq!(
                world.replicas[*name].render(),
                first,
                "{scenario}: `{}`/`{name}` should converge",
                names[0]
            );
        }
    });
}

fn run_fixture(name: &str) {
    let fixture = load_fixture(name);
    // Per-scenario replay ledger (`#lzscenariocoverage`).
    for (i, _id, scenario) in common::scenarios(&format!("{SPEC_DIR}/{name}"), &fixture) {
        let label = scenario
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| format!("{name}[{s}]"))
            .unwrap_or_else(|| format!("{name}[{i}]"));

        let seed = scenario["seed"].as_object().expect("scenario.seed");
        let peer = seed["peer"].as_u64().expect("seed.peer");
        let mut world = World {
            replicas: HashMap::new(),
            ids: HashMap::new(),
        };
        world
            .replicas
            .insert("a".to_string(), LosslessTreeCrdt::new(peer));
        let tree = seed["tree"].clone();
        world.build_children(&tree, TreeNodeId::ROOT);

        if let Some(steps) = scenario.get("steps").and_then(|v| v.as_array()) {
            for step in steps {
                apply_step(&mut world, step);
            }
        }
        // Guard the scenario's `expect` block (`#lzassertunknownkeys`).
        let expect = Expect::new(
            format!("{SPEC_DIR}/{name}"),
            format!("scenarios[{i}].expect"),
            &scenario["expect"],
        );
        assert_expect(&world, &expect, &label);
    }
}

#[test]
fn conformance_exact_roundtrip() {
    run_fixture("exact_roundtrip.json");
}

#[test]
fn conformance_one_leaf_edit_delta() {
    run_fixture("one_leaf_edit_delta.json");
}

#[test]
fn conformance_split_merge() {
    run_fixture("split_merge.json");
}

#[test]
fn conformance_concurrent_insert_same_parent() {
    run_fixture("concurrent_insert_same_parent.json");
}

#[test]
fn conformance_concurrent_reorder_and_leaf_edit() {
    run_fixture("concurrent_reorder_and_leaf_edit.json");
}

#[test]
fn conformance_non_contiguous_anti_entropy() {
    run_fixture("non_contiguous_anti_entropy.json");
}

#[test]
fn conformance_token_trivia_preservation() {
    run_fixture("token_trivia_preservation.json");
}

#[test]
fn conformance_invalid_source_roundtrip() {
    run_fixture("invalid_source_roundtrip.json");
}

#[test]
fn conformance_concurrent_conflict_preserves_text() {
    run_fixture("concurrent_conflict_preserves_text.json");
}

/// `apply_update` advances the Lamport counter past every observed op —
/// unconditionally, and BEFORE the idempotence skip — so a write minted AFTER a
/// sync outranks the stamps that sync delivered. The only lossless-tree scenario
/// that mutates a replica after a sync INTO it, which is why no earlier fixture
/// could see this. Both replicas still converge when the counter is not
/// advanced, so `render_on` is the load-bearing assertion, not `converged`.
#[test]
fn conformance_apply_update_advances_counter() {
    run_fixture("apply_update_advances_counter.json");
}

/// `apply_update` BUFFERS an op whose dependency has not arrived and retries it
/// as later ops in the same batch land. The batch is delivered in strictly
/// reversed order as ONE call (`deliver.order`), so every op precedes the op it
/// depends on; without the buffer the `LeafEdit` is dropped while its dot is
/// recorded, and the trailing full sync cannot repair it because both frontiers
/// already hold every op.
#[test]
fn conformance_out_of_order_delivery_buffers() {
    run_fixture("out_of_order_delivery_buffers.json");
}
