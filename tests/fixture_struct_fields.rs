//! Rung 3 for typed conformance fixtures (`#lzstructfieldreads`).
//!
//! Serde consumes a JSON key when it fills a struct field, but Rust does not
//! reject a field that is never read afterwards. Parse the test sources, find
//! every field on a `Deserialize` struct, and require either a selector read or
//! a reasoned, two-direction excuse.

use proc_macro2::{TokenStream, TokenTree};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{ExprField, Fields, ItemStruct, Member, Path, Token};

#[derive(Default)]
struct SourceScan {
    fields: Vec<(String, String)>,
    reads: BTreeSet<String>,
}

fn collect_macro_field_reads(tokens: TokenStream, reads: &mut BTreeSet<String>) {
    let mut after_dot = false;
    for token in tokens {
        match token {
            TokenTree::Punct(punctuation) if punctuation.as_char() == '.' => after_dot = true,
            TokenTree::Ident(identifier) if after_dot => {
                reads.insert(identifier.to_string());
                after_dot = false;
            }
            TokenTree::Group(group) => {
                collect_macro_field_reads(group.stream(), reads);
                after_dot = false;
            }
            _ => after_dot = false,
        }
    }
}

fn derives_deserialize(item: &ItemStruct) -> bool {
    item.attrs.iter().any(|attr| {
        attr.path().is_ident("derive")
            && attr
                .parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)
                .is_ok_and(|paths| {
                    paths.iter().any(|path| {
                        path.segments
                            .last()
                            .is_some_and(|segment| segment.ident == "Deserialize")
                    })
                })
    })
}

impl<'ast> Visit<'ast> for SourceScan {
    fn visit_item_struct(&mut self, item: &'ast ItemStruct) {
        if derives_deserialize(item)
            && let Fields::Named(fields) = &item.fields
        {
            for field in &fields.named {
                if let Some(name) = &field.ident {
                    self.fields.push((item.ident.to_string(), name.to_string()));
                }
            }
        }
        visit::visit_item_struct(self, item);
    }

    fn visit_expr_field(&mut self, expression: &'ast ExprField) {
        if let Member::Named(name) = &expression.member {
            self.reads.insert(name.to_string());
        }
        visit::visit_expr_field(self, expression);
    }

    fn visit_macro(&mut self, expression: &'ast syn::Macro) {
        collect_macro_field_reads(expression.tokens.clone(), &mut self.reads);
        visit::visit_macro(self, expression);
    }
}

#[test]
fn conformance_struct_fields_are_read() {
    let excuses = BTreeMap::from([
        (
            "conformance.rs:ArenaFixture.arena_description",
            "corpus prose: free-form fixture documentation",
        ),
        (
            "signaling_negative_conformance.rs:FrameCase.frame_reason",
            "negative-case prose: explains why the codec must reject the wire",
        ),
        (
            "signaling_negative_conformance.rs:FramesFixture.frames_description",
            "corpus prose: free-form fixture documentation",
        ),
        (
            "signaling_negative_conformance.rs:SessionReject.session_reason",
            "negative-case prose: explains why the session must reject the input",
        ),
        (
            "signaling_negative_conformance.rs:SessionEmit.emit_to",
            "server-routing transcript metadata; lazily-rs ships the signaling codec but not the server session that produces the route",
        ),
        (
            "signaling_negative_conformance.rs:SessionFixture.session_description",
            "corpus prose: free-form fixture documentation",
        ),
    ]);

    let mut declared = BTreeSet::new();
    let mut problems = Vec::new();
    let tests_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    for entry in fs::read_dir(&tests_dir).expect("read tests directory") {
        let entry = entry.expect("read tests directory entry");
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("UTF-8 test filename");
        let source = fs::read_to_string(&path).expect("read Rust test source");
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
        let mut scan = SourceScan::default();
        scan.visit_file(&syntax);
        for (structure, field) in scan.fields {
            let key = format!("{file_name}:{structure}.{field}");
            declared.insert(key.clone());
            match (scan.reads.contains(&field), excuses.get(key.as_str())) {
                (true, Some(reason)) => problems.push(format!(
                    "{key} is both read and excused ({reason:?}); delete the stale excuse"
                )),
                (true, None) => {}
                (false, Some(reason)) if !reason.trim().is_empty() => {}
                (false, Some(_)) => problems.push(format!("{key} has an empty excuse")),
                (false, None) => problems.push(format!(
                    "{key} is decoded from a fixture and never read; implement the check or add a reasoned excuse"
                )),
            }
        }
    }

    assert!(!declared.is_empty(), "no Deserialize fixture fields found");
    for key in excuses.keys() {
        if !declared.contains(*key) {
            problems.push(format!(
                "{key} is excused but no such fixture field is declared"
            ));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
