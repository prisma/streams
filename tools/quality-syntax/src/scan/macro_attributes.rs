//! Macro bodies are DSLs, but Rust attribute token groups still have an exact
//! grammar. Inspect those groups without treating strings/comments as code.
use super::Scan;
use proc_macro2::{Delimiter, TokenStream, TokenTree};

pub(super) fn visit(scan: &mut Scan, stream: TokenStream) {
    let mut attribute = false;
    for token in stream {
        match token {
            TokenTree::Punct(mark) if mark.as_char() == '#' => attribute = true,
            TokenTree::Punct(mark) if attribute && mark.as_char() == '!' => {}
            TokenTree::Group(group) => {
                if attribute && group.delimiter() == Delimiter::Bracket {
                    record(scan, &group);
                }
                attribute = false;
                visit(scan, group.stream());
            }
            _ => attribute = false,
        }
    }
}

fn record(scan: &mut Scan, group: &proc_macro2::Group) {
    match syn::parse2::<syn::Meta>(group.stream()) {
        Ok(meta) => scan.fact("macro-attribute", super::tokens(&meta), group.span()),
        Err(_) => scan.fact(
            "unparsed-macro-attribute",
            group.stream().to_string(),
            group.span(),
        ),
    }
}
