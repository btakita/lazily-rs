//! Schema compliance for the lossless full-document tree CRDT wire types
//! (#lzlosstree): lazily-rs's own serde output for [`TreeUpdate`] /
//! [`TreeVersionFrontier`] validates against the canonical `lazily-spec`
//! schemas (`lossless-tree.json` vocabulary + `lossless-tree-delta.json`).
//!
//! lazily-rs is the reference whose serde output *defines* the normative wire
//! form; the schemas are derived from it and the Kotlin/JS ports validate their
//! own emitted frames against the same schemas, so this test is the anchor of
//! that cross-language loop. Mirrors `tests/schema_compliance.rs` for the IPC
//! plane. Needs both the `lossless-tree` core and `serde`.
//!
//! The schema root is resolved through `common::schemas_root`, so
//! `LAZILY_SPEC_SCHEMAS_DIR` can point a probe at a scratch copy rather than at
//! the shared sibling checkout every binding reads (`#lzspecschemasoverride`).

#![cfg(all(feature = "lossless-tree", feature = "serde"))]

mod common;

use lazily::{LeafKind, LosslessTreeCrdt, NodeSeed, TreeNodeId, TreeUpdate};
use serde_json::Value;

const SCHEMA_FILES: &[&str] = &["lossless-tree", "lossless-tree-delta"];

fn sibling_schemas_present() -> bool {
    common::schemas_present(SCHEMA_FILES)
}

fn load_json(name: &str) -> Value {
    let raw = common::read_schema(name);
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse schema {name}.json: {e}"))
}

/// Compose the `lossless-tree.json` vocabulary `$defs` into `schema` and rewrite
/// external `https://lazily.dev/schemas/lossless-tree.json#/…` refs to local
/// `#/…` refs, yielding a single self-contained schema (same trick as
/// `schema_compliance.rs`).
fn compose(mut schema: Value, defs: &Value) -> Value {
    if let (Some(obj), Some(src)) = (schema.as_object_mut(), defs.get("$defs")) {
        let target = obj
            .entry("$defs")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let (Some(t), Some(s)) = (target.as_object_mut(), src.as_object()) {
            for (k, v) in s {
                t.insert(k.clone(), v.clone());
            }
        }
    }
    rewrite_refs(&mut schema);
    schema
}

fn rewrite_refs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(stripped) = map
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|s| s.strip_prefix("https://lazily.dev/schemas/lossless-tree.json#"))
            {
                map.insert("$ref".to_string(), Value::String(format!("#{stripped}")));
            }
            for (_, child) in map.iter_mut() {
                rewrite_refs(child);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(rewrite_refs),
        _ => {}
    }
}

fn assert_valid(schema: &Value, instance: &Value, schema_name: &str) {
    let validator = jsonschema::Validator::new(schema)
        .unwrap_or_else(|e| panic!("{schema_name}.json is not a valid schema: {e}"));
    let errors: Vec<_> = validator.iter_errors(instance).collect();
    assert!(
        errors.is_empty(),
        "lazily-rs wire output does not validate against {schema_name}.json:\n{}\ninstance:\n{}",
        errors
            .iter()
            .map(|e| format!("  - {e}"))
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::to_string_pretty(instance).unwrap(),
    );
}

/// Build a replica whose op log exercises every M1 op variant, so the emitted
/// `TreeUpdate` carries one of each (CreateNode / LeafEdit / SplitLeaf /
/// MergeLeaves / Reorder / Tombstone).
fn all_ops_update() -> (TreeUpdate, Value) {
    let mut t = LosslessTreeCrdt::new(1);
    let para = t
        .create_node(
            TreeNodeId::ROOT,
            None,
            NodeSeed::Element {
                kind: "para".into(),
            },
        )
        .unwrap();
    let a = t
        .create_node(
            para,
            None,
            NodeSeed::Leaf {
                kind: LeafKind::Raw,
                text: "hello world".into(),
            },
        )
        .unwrap();
    let b = t
        .create_node(
            para,
            Some(a),
            NodeSeed::Leaf {
                kind: LeafKind::Token,
                text: "!".into(),
            },
        )
        .unwrap();
    // LeafEdit
    t.edit_leaf(a, 5, 0, "X").unwrap();
    // SplitLeaf + MergeLeaves (offsets on char boundaries of "helloX world")
    let tail = t.split_leaf(a, 6).unwrap();
    t.merge_adjacent_leaves(a, tail).unwrap();
    // Reorder (move b before a)
    t.reorder_child(b, None).unwrap();
    // Tombstone
    t.tombstone_node(b).unwrap();

    let update = t.diff(&lazily::TreeVersionFrontier::default());
    let json = serde_json::to_value(&update).expect("TreeUpdate serializes");
    (update, json)
}

#[test]
fn tree_update_validates_delta_schema() {
    if !sibling_schemas_present() {
        eprintln!(
            "skipping: {} not present (run from the monorepo)",
            common::schemas_root()
        );
        return;
    }
    let defs = load_json("lossless-tree");
    let schema = compose(load_json("lossless-tree-delta"), &defs);
    let (_u, json) = all_ops_update();
    assert_valid(&schema, &json, "lossless-tree-delta");
}

#[test]
fn frontier_validates_vocabulary_schema() {
    if !sibling_schemas_present() {
        eprintln!(
            "skipping: {} not present (run from the monorepo)",
            common::schemas_root()
        );
        return;
    }
    // Validate a real dotted frontier (with a non-contiguous hole) against the
    // TreeVersionFrontier $def in the vocabulary schema.
    let mut a = LosslessTreeCrdt::new(1);
    let root_child = a
        .create_node(
            TreeNodeId::ROOT,
            None,
            NodeSeed::Element { kind: "x".into() },
        )
        .unwrap();
    a.create_node(
        root_child,
        None,
        NodeSeed::Leaf {
            kind: LeafKind::Raw,
            text: "y".into(),
        },
    )
    .unwrap();
    a.create_node(
        root_child,
        None,
        NodeSeed::Leaf {
            kind: LeafKind::Raw,
            text: "z".into(),
        },
    )
    .unwrap();
    let frontier = serde_json::to_value(a.frontier()).expect("frontier serializes");

    let mut vocab = load_json("lossless-tree");
    // Point the schema root at the TreeVersionFrontier $def so we validate the
    // frontier instance directly (refs are already local `#/$defs/…`).
    vocab.as_object_mut().unwrap().insert(
        "$ref".to_string(),
        Value::String("#/$defs/TreeVersionFrontier".to_string()),
    );
    assert_valid(&vocab, &frontier, "lossless-tree#TreeVersionFrontier");
}

#[test]
fn delta_schema_rejects_base64_frac_and_lowercase_leaf_kind() {
    if !sibling_schemas_present() {
        return;
    }
    let defs = load_json("lossless-tree");
    let schema = compose(load_json("lossless-tree-delta"), &defs);
    let validator = jsonschema::Validator::new(&schema).expect("schema builds");

    // `frac` must be a u8 array, never a base64 string.
    let base64_frac: Value = serde_json::from_str(
        r#"{"ops":[{"id":{"counter":1,"peer":1},"kind":{"CreateNode":{"id":{"counter":1,"peer":1},"parent":{"counter":0,"peer":0},"sort":{"frac":"gA==","peer":1},"seed":{"Element":{"kind":"para"}}}}}]}"#,
    )
    .unwrap();
    assert!(
        validator.iter_errors(&base64_frac).count() > 0,
        "schema must reject a base64 `frac` (normative is a u8 array)"
    );

    // Leaf kind is PascalCase on the wire; lowercase `raw` (the fixture-DSL form)
    // must be rejected as a wire value.
    let lower_kind: Value = serde_json::from_str(
        r#"{"ops":[{"id":{"counter":2,"peer":1},"kind":{"CreateNode":{"id":{"counter":2,"peer":1},"parent":{"counter":1,"peer":1},"sort":{"frac":[128],"peer":1},"seed":{"Leaf":{"kind":"raw","text":"x"}}}}}]}"#,
    )
    .unwrap();
    assert!(
        validator.iter_errors(&lower_kind).count() > 0,
        "schema must reject lowercase leaf kind on the wire (normative is PascalCase)"
    );
}
