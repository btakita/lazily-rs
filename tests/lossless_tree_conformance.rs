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

use lazily::{LeafKind, LosslessTreeCrdt, NodeSeed, TreeNodeId, TreeUpdate};
use serde_json::Value;

const SPEC_DIR: &str = "../lazily-spec/conformance/lossless-tree";

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
        let full = world.replicas[from].diff(&world.replicas[to].frontier());
        let only: Vec<usize> = deliver["only"]
            .as_array()
            .expect("deliver.only")
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect();
        let ops = only.iter().map(|&i| full.ops[i].clone()).collect();
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

fn assert_expect(world: &World, expect: &Value, scenario: &str) {
    if let Some(text) = expect.get("render").and_then(|v| v.as_str()) {
        assert_eq!(
            world.replicas["a"].render(),
            text,
            "{scenario}: render on `a`"
        );
    }
    if let Some(per) = expect.get("render_on").and_then(|v| v.as_object()) {
        for (name, text) in per {
            assert_eq!(
                world.replicas[name].render(),
                text.as_str().unwrap(),
                "{scenario}: render on `{name}`"
            );
        }
    }
    if let Some(n) = expect.get("live_nodes").and_then(|v| v.as_u64()) {
        assert_eq!(
            world.replicas["a"].live_node_count() as u64,
            n,
            "{scenario}: live_nodes on `a`"
        );
    }
    if let Some(names) = expect.get("converged").and_then(|v| v.as_array()) {
        let names: Vec<&str> = names.iter().map(|v| v.as_str().unwrap()).collect();
        let first = world.replicas[names[0]].render();
        for name in &names[1..] {
            assert_eq!(
                world.replicas[*name].render(),
                first,
                "{scenario}: `{}`/`{name}` should converge",
                names[0]
            );
        }
    }
}

fn run_fixture(name: &str) {
    let fixture = load_fixture(name);
    let scenarios = fixture["scenarios"].as_array().expect("scenarios");
    for (i, scenario) in scenarios.iter().enumerate() {
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
        assert_expect(&world, &scenario["expect"], &label);
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
