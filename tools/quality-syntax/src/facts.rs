use proc_macro2::Span;
use quote::ToTokens;
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Location {
    pub(super) line: usize,
    pub(super) column: usize,
    pub(super) end_line: usize,
    pub(super) end_column: usize,
}

impl From<Span> for Location {
    fn from(span: Span) -> Self {
        Self {
            line: span.start().line,
            column: span.start().column,
            end_line: span.end().line,
            end_column: span.end().column,
        }
    }
}

#[derive(Serialize)]
pub(super) struct Item {
    pub(super) qualified: String,
    pub(super) kind: &'static str,
    pub(super) signature: String,
    pub(super) test_only: bool,
    pub(super) explicit_test_cfg: bool,
    pub(super) location: Location,
}

#[derive(Serialize)]
pub(super) struct Fact {
    pub(super) qualified: String,
    pub(super) kind: &'static str,
    pub(super) value: String,
    pub(super) test_only: bool,
    pub(super) location: Location,
}

#[derive(Default, Serialize)]
pub(super) struct Source {
    pub(super) path: String,
    pub(super) tokens: String,
    pub(super) test_only_file: bool,
    pub(super) items: Vec<Item>,
    pub(super) facts: Vec<Fact>,
}

pub(super) fn tokens(value: &impl ToTokens) -> String {
    value.to_token_stream().to_string()
}

/// Only a direct attribute on this AST node proves this exact cfg boundary.
pub(super) fn explicit_test_cfg(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Path>()
                .is_ok_and(|path| path.is_ident("test"))
    })
}

/// Classify only a positive `cfg(test)` requirement as test-only. Unknown and
/// mixed cfg expressions remain production-visible facts, never skipped.
pub(super) fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr
                .path()
                .segments
                .last()
                .is_some_and(|s| s.ident == "test"))
            || (attr.path().is_ident("cfg")
                && attr
                    .parse_args::<syn::Meta>()
                    .is_ok_and(|meta| requires_test(&meta)))
    })
}

fn requires_test(meta: &syn::Meta) -> bool {
    use syn::{Meta, Token, punctuated::Punctuated};
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) => {
            let Ok(children) =
                list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            else {
                return false;
            };
            if list.path.is_ident("all") {
                children.iter().any(requires_test)
            } else if list.path.is_ident("any") {
                !children.is_empty() && children.iter().all(requires_test)
            } else {
                false
            }
        }
        Meta::NameValue(_) => false,
    }
}
