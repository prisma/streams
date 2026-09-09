//! Inspect conditional attributes with Rust's meta grammar, including nesting.
use super::{Scan, tokens};
use syn::{Meta, Token, parse::Parser, punctuated::Punctuated, spanned::Spanned};

pub(super) fn record(scan: &mut Scan, kind: &'static str, meta: &Meta) {
    scan.fact(kind, tokens(meta), meta.span());
    let Meta::List(list) = meta else { return };
    if !list.path.is_ident("cfg_attr") {
        return;
    }
    match Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone()) {
        Ok(parts) if parts.len() >= 2 => {
            // The first meta is a predicate; subsequent entries are attributes.
            // Inspect every branch regardless of the host/test configuration.
            for nested in parts.iter().skip(1) {
                record(scan, kind, nested);
            }
        }
        _ => scan.fact("unparsed-attribute", tokens(meta), meta.span()),
    }
}
