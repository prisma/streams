use super::source;

fn values(code: &str, kind: &str) -> Vec<String> {
    source("src/example.rs", code)
        .unwrap()
        .facts
        .into_iter()
        .filter(|f| f.kind == kind)
        .map(|f| f.value)
        .collect()
}

#[test]
fn resolves_braced_renamed_and_chained_imports_even_when_hoisted() {
    let code = "fn work() { root::spawn(async {}); } use tokio as rt; use rt::{self as root};";
    assert!(values(code, "path").contains(&"tokio::spawn".to_owned()));
    assert!(
        values(
            "use crate::{http as wire}; fn bad(_: wire::AppState) {}",
            "path"
        )
        .contains(&"crate::http::AppState".to_owned())
    );
}

#[test]
fn local_aliases_do_not_leak_between_functions() {
    let facts = source(
        "src/example.rs",
        "fn a(){ use std::env as e; e::var(\"x\"); } fn b(){ e::var(\"x\"); }",
    )
    .unwrap();
    assert!(
        facts
            .facts
            .iter()
            .any(|f| f.qualified == "crate::a" && f.value == "std::env::var")
    );
    assert!(
        facts
            .facts
            .iter()
            .any(|f| f.qualified == "crate::b" && f.value == "e::var")
    );
}

#[test]
fn strings_and_comments_cannot_fabricate_a_boundary_edge() {
    assert!(
        !values(
            "fn ok(){ let _=\"crate::http::AppState\"; /* tokio::spawn */ }",
            "path"
        )
        .iter()
        .any(|v| v.contains("http") || v.contains("spawn"))
    );
}

#[test]
fn fields_and_qualified_impls_have_distinct_owners() {
    let facts = source("src/a.rs", "struct A { hidden: u8, pub leaked: u8 } impl A {fn same(){}} struct B; impl B {fn same(){}}").unwrap();
    assert!(facts.items.iter().any(|i| i.qualified == "crate::A::same"));
    assert!(facts.items.iter().any(|i| i.qualified == "crate::B::same"));
    assert!(
        facts
            .facts
            .iter()
            .any(|f| f.qualified == "crate::A::hidden" && f.value.is_empty())
    );
    assert!(
        facts
            .facts
            .iter()
            .any(|f| f.qualified == "crate::A::leaked" && f.value == "pub")
    );
}

#[test]
fn mixed_cfg_is_not_silently_exempted_as_test_only() {
    let facts = source("src/a.rs", "#[cfg(test)] fn a(){} #[cfg(any(test, unix))] fn b(){} #[cfg(all(test, unix))] fn c(){} #[cfg(not(test))] fn d(){}").unwrap();
    let tests: Vec<_> = facts
        .items
        .into_iter()
        .filter(|i| i.test_only)
        .map(|i| i.qualified)
        .collect();
    assert_eq!(tests, ["crate::a", "crate::c"]);
}

#[test]
fn globs_macros_and_parse_failures_are_reported() {
    assert_eq!(
        values(
            "use super::*; fn x(){ custom!(tokio::spawn(async {})); }",
            "unresolved-glob"
        )
        .len(),
        1
    );
    assert_eq!(
        values("fn x(){ custom!(std::env::var(\"x\")); }", "macro"),
        ["custom"]
    );
    assert!(source("src/bad.rs", "fn (").is_err());
}

#[test]
fn macro_attributes_cannot_hide_blanket_suppressions_in_dsl_tokens() {
    let facts = super::source(
        "src/a.rs",
        r##"
        make_tests! { #[allow(clippy::all)] fn concealed() {} }
        fn message() { println!("#[allow(clippy::all)]"); }
    "##,
    )
    .unwrap();
    let attrs: Vec<_> = facts
        .facts
        .iter()
        .filter(|f| f.kind == "macro-attribute")
        .collect();
    assert_eq!(attrs.len(), 1);
    assert!(attrs[0].value.contains("clippy :: all"));
}
