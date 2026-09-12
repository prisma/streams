//! The tenant identity grammar's unit tests.
#![cfg(test)]
use super::*;

fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).unwrap()
}

fn sref(p: &str, n: &str) -> TenantStreamRef {
    TenantStreamRef::new(pid(p), CanonicalStreamName::new(n).unwrap())
}

#[test]
fn id_bounds_are_enforced() {
    assert_eq!(ProjectId::new(""), Err(IdentityError::Empty));
    assert!(ProjectId::new(&"x".repeat(128)).is_ok());
    assert_eq!(
        ProjectId::new(&"x".repeat(129)),
        Err(IdentityError::TooLong { max: 128, got: 129 })
    );
    assert_eq!(
        ProjectId::new("a b"),
        Err(IdentityError::ForbiddenByte { at: 1 })
    );
    assert_eq!(
        ProjectId::new("a\tb"),
        Err(IdentityError::ForbiddenByte { at: 1 })
    );
    // Allowlist, not denylist: zero-width space, bidi override,
    // dot, slash, and '+' are all outside [A-Za-z0-9_-].
    for bad in ["a\u{200b}b", "a\u{202e}b", "a.b", "a/b", "a+b"] {
        assert_eq!(
            ProjectId::new(bad),
            Err(IdentityError::ForbiddenByte { at: 1 }),
            "{bad:?} must be rejected"
        );
    }
    assert!(ProjectId::new("proj_ABC-123").is_ok());
    assert!(WorkspaceId::new("ws_789").is_ok());
    assert!(validate_cell_id("fra-cell-07").is_ok());
}

#[test]
fn encoding_is_length_prefixed_and_exact() {
    let mut out = Vec::new();
    append_component(&mut out, b"ab");
    assert_eq!(out, vec![0, 0, 0, 2, b'a', b'b']);
    let enc = encode_hash_input(HashDomain::RouteV1, &[b"p", b"n"]);
    let mut want = Vec::new();
    append_component(&mut want, b"route-v1");
    append_component(&mut want, b"p");
    append_component(&mut want, b"n");
    assert_eq!(enc, want);
}

#[test]
fn encoding_cannot_alias_component_boundaries() {
    // ("ab","c") vs ("a","bc") — the delimiter-concatenation bug
    // the contract forbids.
    let x = encode_hash_input(HashDomain::RouteV1, &[b"ab", b"c"]);
    let y = encode_hash_input(HashDomain::RouteV1, &[b"a", b"bc"]);
    assert_ne!(x, y);
}

#[test]
fn domains_are_separated() {
    let r = encode_hash_input(HashDomain::RouteV1, &[b"p", b"n"]);
    let s = encode_hash_input(HashDomain::StorageV1, &[b"p", b"n"]);
    assert_ne!(r, s);
}

#[test]
fn same_name_in_two_projects_yields_different_identity_inputs() {
    let a = sref("proj_a", "orders");
    let b = sref("proj_b", "orders");
    assert_ne!(route_hash_input(&a), route_hash_input(&b));
    assert_ne!(storage_hash_input(&a, "e1"), storage_hash_input(&b, "e1"));
    assert_ne!(
        segment_identity_input(&a, "e1", 0),
        segment_identity_input(&b, "e1", 0)
    );
    // Same project, same name: identical (the reference identity).
    let a2 = sref("proj_a", "orders");
    assert_eq!(route_hash_input(&a), route_hash_input(&a2));
}

#[test]
fn segment_id_is_a_fixed_width_component() {
    // Adjacent segment ids must not alias epoch bytes.
    let r = sref("p", "n");
    assert_ne!(
        segment_identity_input(&r, "e", 1),
        segment_identity_input(&r, "e", 256)
    );
}

#[test]
fn child_route_is_its_own_domain() {
    let r = sref("proj_a", "orders");
    let child = route_child_hash_input(&r, 3, b"salt1");
    assert_ne!(child, route_hash_input(&r));
    assert_ne!(child, route_child_hash_input(&r, 4, b"salt1"));
    assert_ne!(child, route_child_hash_input(&r, 3, b"salt2"));
    let other = sref("proj_b", "orders");
    assert_ne!(child, route_child_hash_input(&other, 3, b"salt1"));
}

#[test]
fn scope_parsing_matches_the_contract() {
    let (set, unknown) =
        ScopeSet::parse("streams.records.read streams.records.append streams.metadata.read");
    assert_eq!(unknown, 0);
    assert!(set.has(Scope::RecordsRead));
    assert!(set.has(Scope::RecordsAppend));
    assert!(set.has(Scope::MetadataRead));
    assert!(!set.has(Scope::LifecycleManage));
    assert_eq!(set.iter().count(), 3);
    // Unknown scopes are counted, never granted.
    let (set2, unknown2) = ScopeSet::parse("streams.future.thing streams.create");
    assert_eq!(unknown2, 1);
    assert!(set2.has(Scope::Create));
    assert_eq!(set2.iter().count(), 1);
    // Every scope round-trips through its canonical string.
    for s in Scope::ALL {
        assert_eq!(Scope::parse(s.as_str()), Some(s));
    }
}

#[test]
fn prefix_matching_is_component_aware() {
    // The contract's example, verbatim (§6.2).
    let g = CanonicalPrefix::normalize("customers/acme").unwrap();
    assert!(g.matches("customers/acme"));
    assert!(g.matches("customers/acme/orders"));
    assert!(!g.matches("customers/acme-other"));
    // Escape attempts.
    assert!(!g.matches("customers"));
    assert!(!g.matches("customers/ac"));
    assert!(!g.matches("customers/acmeX"));
    assert!(!g.matches("xcustomers/acme"));
}

#[test]
fn prefix_normalization_rejects_non_canonical_forms() {
    assert_eq!(CanonicalPrefix::normalize(""), Err(PrefixError::Empty));
    assert_eq!(
        CanonicalPrefix::normalize("/a"),
        Err(PrefixError::EmptyComponent)
    );
    assert_eq!(
        CanonicalPrefix::normalize("a/"),
        Err(PrefixError::EmptyComponent)
    );
    assert_eq!(
        CanonicalPrefix::normalize("a//b"),
        Err(PrefixError::EmptyComponent)
    );
    assert_eq!(
        CanonicalPrefix::normalize("a b"),
        Err(PrefixError::ForbiddenChar { at: 1 })
    );
    assert!(CanonicalPrefix::normalize(&"x".repeat(257)).is_err());
    // Shared component rules with stream names (review round):
    assert_eq!(
        CanonicalPrefix::normalize("."),
        Err(PrefixError::DotComponent)
    );
    assert_eq!(
        CanonicalPrefix::normalize(".."),
        Err(PrefixError::DotComponent)
    );
    assert_eq!(
        CanonicalPrefix::normalize("a/../b"),
        Err(PrefixError::DotComponent)
    );
    assert_eq!(
        CanonicalPrefix::normalize("__ds"),
        Err(PrefixError::ReservedRoot)
    );
    assert_eq!(
        CanonicalPrefix::normalize("__ds/x"),
        Err(PrefixError::ReservedRoot)
    );
    // Only the ROOT is reserved, and addressability finals are
    // deliberately legal in prefixes:
    assert!(CanonicalPrefix::normalize("a/__ds").is_ok());
    assert!(CanonicalPrefix::normalize("a/records").is_ok());
}

#[test]
fn prefix_set_drops_redundant_grants() {
    let set = normalize_prefix_set(&["customers/acme/orders", "customers/acme", "other"]).unwrap();
    let strs: Vec<&str> = set.iter().map(|p| p.as_str()).collect();
    assert_eq!(strs, vec!["other", "customers/acme"]);
    // Duplicates collapse too.
    let set2 = normalize_prefix_set(&["a", "a"]).unwrap();
    assert_eq!(set2.len(), 1);
    // Non-covering near-misses are kept: 'a-b' is not under 'a'.
    let set3 = normalize_prefix_set(&["a", "a-b"]).unwrap();
    assert_eq!(set3.len(), 2);
    assert!(prefix_set_matches(&set3, "a-b/x"));
    assert!(!prefix_set_matches(&set3, "a-c"));
}

#[test]
fn canonical_stream_name_is_checked_construction() {
    assert!(CanonicalStreamName::new("orders").is_ok());
    assert!(CanonicalStreamName::new("customers/acme/orders").is_ok());
    assert_eq!(CanonicalStreamName::new(""), Err(NameError::Empty));
    assert_eq!(
        CanonicalStreamName::new("a//b"),
        Err(NameError::EmptyComponent)
    );
    assert_eq!(
        CanonicalStreamName::new("/a"),
        Err(NameError::EmptyComponent)
    );
    assert_eq!(
        CanonicalStreamName::new("a/"),
        Err(NameError::EmptyComponent)
    );
    assert_eq!(
        CanonicalStreamName::new("a/./b"),
        Err(NameError::DotComponent)
    );
    assert_eq!(
        CanonicalStreamName::new("a/../b"),
        Err(NameError::DotComponent)
    );
    assert_eq!(
        CanonicalStreamName::new("__ds/x"),
        Err(NameError::ReservedRoot)
    );
    assert_eq!(
        CanonicalStreamName::new("__ds"),
        Err(NameError::ReservedRoot)
    );
    // __ds below the root is a legal component (only the ROOT is reserved).
    assert!(CanonicalStreamName::new("a/__ds").is_ok());
}

/// PR 4.1: the canonical layer OWNS error precedence. Structural
/// component problems win over the reserved root (the product
/// surface's historical wire order); a well-formed reserved name
/// still reports the root.
#[test]
fn name_error_precedence() {
    assert_eq!(
        CanonicalStreamName::new("__ds/..").unwrap_err(),
        NameError::DotComponent
    );
    assert_eq!(
        CanonicalStreamName::new("__ds//x").unwrap_err(),
        NameError::EmptyComponent
    );
    assert_eq!(
        CanonicalStreamName::new("__ds/x").unwrap_err(),
        NameError::ReservedRoot
    );
    assert_eq!(
        CanonicalStreamName::new("__ds").unwrap_err(),
        NameError::ReservedRoot
    );
    assert_eq!(
        CanonicalStreamName::new("a\u{7}b"),
        Err(NameError::ControlChar { at: 1 })
    );
    assert!(CanonicalStreamName::new(&"x".repeat(513)).is_err());
    assert!(CanonicalStreamName::new(&"x".repeat(512)).is_ok());
}

#[test]
fn prefix_count_limit_is_enforced() {
    let raws: Vec<String> = (0..65).map(|i| format!("p{i}")).collect();
    let refs: Vec<&str> = raws.iter().map(String::as_str).collect();
    assert_eq!(
        normalize_prefix_set(&refs),
        Err(PrefixError::TooMany { max: 64, got: 65 })
    );
}
