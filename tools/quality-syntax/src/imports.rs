//! Lexically scoped, hoisted import aliases. Globs are emitted as unresolved
//! facts by the visitor; this resolver never guesses what a glob exports.
use std::collections::BTreeMap;
use syn::{Item, UseTree};

#[derive(Clone, Default)]
pub(super) struct Imports(BTreeMap<String, String>);

impl Imports {
    pub(super) fn with_items(&self, items: &[Item]) -> Self {
        let mut result = self.clone();
        for item in items {
            if let Item::Use(item) = item {
                result.add(&item.tree, "");
            }
        }
        result
    }

    pub(super) fn with_block(&self, block: &syn::Block) -> Self {
        let mut result = self.clone();
        for statement in &block.stmts {
            if let syn::Stmt::Item(Item::Use(item)) = statement {
                result.add(&item.tree, "");
            }
        }
        result
    }

    pub(super) fn path(&self, path: &syn::Path) -> String {
        let raw = path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        self.resolve(&raw)
    }

    pub(super) fn targets(&self, tree: &UseTree) -> Vec<String> {
        let mut names = Self::default();
        names.add(tree, "");
        names.0.values().map(|value| self.resolve(value)).collect()
    }

    fn resolve(&self, raw: &str) -> String {
        let mut value = raw.to_owned();
        // Import chains cannot legitimately exceed the number of aliases.
        // Cycles retain the final spelling and are diagnosed by the compiler.
        for _ in 0..=self.0.len() {
            let (head, rest) = value.split_once("::").unwrap_or((&value, ""));
            let Some(prefix) = self.0.get(head) else {
                break;
            };
            let next = if rest.is_empty() {
                prefix.clone()
            } else {
                format!("{prefix}::{rest}")
            };
            if next == value {
                break;
            }
            value = next;
        }
        value
    }

    fn add(&mut self, tree: &UseTree, prefix: &str) {
        match tree {
            UseTree::Path(path) => self.add(&path.tree, &format!("{prefix}{}::", path.ident)),
            UseTree::Name(name) if name.ident == "self" => {
                let full = prefix.trim_end_matches("::");
                if let Some(last) = full.rsplit("::").next() {
                    self.0.insert(last.to_owned(), full.to_owned());
                }
            }
            UseTree::Name(name) => {
                self.0
                    .insert(name.ident.to_string(), format!("{prefix}{}", name.ident));
            }
            UseTree::Rename(rename) => {
                let full = if rename.ident == "self" {
                    prefix.trim_end_matches("::").to_owned()
                } else {
                    format!("{prefix}{}", rename.ident)
                };
                self.0.insert(rename.rename.to_string(), full);
            }
            UseTree::Group(group) => {
                for child in &group.items {
                    self.add(child, prefix);
                }
            }
            UseTree::Glob(_) => {}
        }
    }
}
