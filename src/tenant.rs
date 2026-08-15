//! Core tenant identity types for shared-cell multitenancy.
//!
//! This module is the code form of docs/MULTITENANCY.md §1–2 and §6.2
//! (the FROZEN contract). The stable data identity is
//! `project_id + stream_name + stream_epoch`; the mutable commercial
//! identity (`workspace_id`, `ownership_version`) exists only in
//! authorization, policy, audit, and billing. Nothing in this module
//! accepts a workspace id as an input to a storage identity — that is
//! a contract invariant, not an oversight: a project transfer between
//! workspaces must not change any registry path, route hash, storage
//! hash, segment identity, or cursor.
//!
//! Layout-4 hash inputs are built here (length-prefixed, domain-
//! separated) so no call site can concatenate identities with
//! delimiters and accidentally alias `("ab","c")` with `("a","bc")`.

// Consumed incrementally from Stage 2 (auth.rs principal middleware)
// through Stage 4 (surface conversion); until those land, items here
// are exercised only by their unit tests.
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;

/// Identifier length bounds from the contract (§2): 1–128 bytes for
/// workspace, project, and cell ids. Stream names keep their existing
/// canonical rules (`product::canonical_name`, 1–512 bytes).
pub const ID_MAX_BYTES: usize = 128;

/// Prefix-grant normalization limits (§6.2). These bound credential
/// documents and token size, not stream names.
pub const PREFIX_MAX_BYTES: usize = 256;
pub const PREFIX_MAX_COUNT: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    Empty,
    TooLong {
        max: usize,
        got: usize,
    },
    /// Whitespace, control bytes, and DEL are rejected in ids: ids are
    /// hex-encoded in registry paths so charset is not a path-safety
    /// issue, but an id with invisible bytes is an operator trap.
    ForbiddenByte {
        at: usize,
    },
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdentityError::Empty => write!(f, "identifier is empty"),
            IdentityError::TooLong { max, got } => {
                write!(f, "identifier is {got} bytes; maximum is {max}")
            }
            IdentityError::ForbiddenByte { at } => {
                write!(
                    f,
                    "identifier contains a control/whitespace byte at offset {at}"
                )
            }
        }
    }
}

fn validate_id(raw: &str, max: usize) -> Result<(), IdentityError> {
    if raw.is_empty() {
        return Err(IdentityError::Empty);
    }
    if raw.len() > max {
        return Err(IdentityError::TooLong {
            max,
            got: raw.len(),
        });
    }
    if let Some(at) = raw.bytes().position(|b| b <= 0x20 || b == 0x7f) {
        return Err(IdentityError::ForbiddenByte { at });
    }
    Ok(())
}

/// Mutable commercial/authorization identity (§1.1). Never an input to
/// a storage identity; deliberately NOT a field of [`TenantStreamRef`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkspaceId(Arc<str>);

impl WorkspaceId {
    pub fn new(raw: &str) -> Result<Self, IdentityError> {
        validate_id(raw, ID_MAX_BYTES)?;
        Ok(Self(Arc::from(raw)))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable tenant identity (§1.1): appears in registry paths, route
/// hashes, storage hashes, segment identities, cursors, consumer
/// state, usage rows, and internal RPC targets.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ProjectId(Arc<str>);

impl ProjectId {
    pub fn new(raw: &str) -> Result<Self, IdentityError> {
        validate_id(raw, ID_MAX_BYTES)?;
        Ok(Self(Arc::from(raw)))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Validate a cell id (§2). Cells stay `Arc<str>` in principals; this
/// is the shared bound check for config and token claims.
pub fn validate_cell_id(raw: &str) -> Result<(), IdentityError> {
    validate_id(raw, ID_MAX_BYTES)
}

/// The project-qualified stream reference every handler and registry
/// call operates on after Stage 4. `name` is ALREADY canonical
/// (`product::canonical_name`); this type does not re-derive it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantStreamRef {
    pub project_id: ProjectId,
    pub name: Arc<str>,
}

impl TenantStreamRef {
    /// `name` must be the output of `product::canonical_name`. The
    /// debug assertion catches call sites that skip canonicalization;
    /// the contract's validation lives there, not here.
    pub fn from_canonical(project_id: ProjectId, name: &str) -> Self {
        debug_assert!(
            !name.is_empty(),
            "TenantStreamRef requires a canonical name"
        );
        Self {
            project_id,
            name: Arc::from(name),
        }
    }
}

impl fmt::Display for TenantStreamRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display is for logs/errors only; '§' cannot appear in either
        // component (ids reject non-graphic-ASCII bytes ≤0x20/0x7f and
        // canonical names have no control chars, but '/' appears in
        // names, so the separator must not be '/').
        write!(f, "{}\u{00a7}{}", self.project_id, self.name)
    }
}

// ---------------------------------------------------------------------------
// Canonical binary encoding (§2.1)
// ---------------------------------------------------------------------------

/// Domain-separation tags for every layout-4 hash. Adding a variant is
/// a contract change; renaming one is forbidden (it would silently
/// re-key existing data).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashDomain {
    RouteV1,
    StorageV1,
    SegmentV1,
    CatalogCursorV1,
    WatchCapabilityV1,
}

impl HashDomain {
    pub const fn tag(self) -> &'static [u8] {
        match self {
            HashDomain::RouteV1 => b"route-v1",
            HashDomain::StorageV1 => b"storage-v1",
            HashDomain::SegmentV1 => b"segment-v1",
            HashDomain::CatalogCursorV1 => b"catalog-cursor-v1",
            HashDomain::WatchCapabilityV1 => b"watch-capability-v1",
        }
    }
}

/// Length-prefixed component append (§2.1, verbatim from the
/// contract). Never concatenate identities with delimiters.
pub fn append_component(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}

/// Build the canonical, unambiguous hash input for `domain` over
/// `components`. The domain tag is itself a length-prefixed component,
/// so no tag can collide with a component sequence of another domain.
pub fn encode_hash_input(domain: HashDomain, components: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        4 + domain.tag().len() + components.iter().map(|c| 4 + c.len()).sum::<usize>(),
    );
    append_component(&mut out, domain.tag());
    for c in components {
        append_component(&mut out, c);
    }
    out
}

/// route hash input: route-v1 + project_id + stream_name
pub fn route_hash_input(sref: &TenantStreamRef) -> Vec<u8> {
    encode_hash_input(
        HashDomain::RouteV1,
        &[sref.project_id.as_bytes(), sref.name.as_bytes()],
    )
}

/// storage hash input: storage-v1 + project_id + stream_name + stream_epoch
pub fn storage_hash_input(sref: &TenantStreamRef, stream_epoch: &str) -> Vec<u8> {
    encode_hash_input(
        HashDomain::StorageV1,
        &[
            sref.project_id.as_bytes(),
            sref.name.as_bytes(),
            stream_epoch.as_bytes(),
        ],
    )
}

/// segment identity input:
/// segment-v1 + project_id + stream_name + stream_epoch + segment_id
pub fn segment_identity_input(
    sref: &TenantStreamRef,
    stream_epoch: &str,
    segment_id: u32,
) -> Vec<u8> {
    encode_hash_input(
        HashDomain::SegmentV1,
        &[
            sref.project_id.as_bytes(),
            sref.name.as_bytes(),
            stream_epoch.as_bytes(),
            &segment_id.to_be_bytes(),
        ],
    )
}

// ---------------------------------------------------------------------------
// Scopes (§6)
// ---------------------------------------------------------------------------

/// The thirteen explicit data-plane scopes (§6). Broad scopes like
/// STREAM_MANAGE are deliberately absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Scope {
    MetadataRead = 1 << 0,
    RecordsRead = 1 << 1,
    RecordsAppend = 1 << 2,
    Create = 1 << 3,
    LifecycleManage = 1 << 4,
    ConsumersPull = 1 << 5,
    ConsumersSettle = 1 << 6,
    ConsumersConfigure = 1 << 7,
    ForksCreate = 1 << 8,
    DlqConfigure = 1 << 9,
    WatchesManage = 1 << 10,
    CatalogRead = 1 << 11,
    UsageRead = 1 << 12,
}

impl Scope {
    pub const ALL: [Scope; 13] = [
        Scope::MetadataRead,
        Scope::RecordsRead,
        Scope::RecordsAppend,
        Scope::Create,
        Scope::LifecycleManage,
        Scope::ConsumersPull,
        Scope::ConsumersSettle,
        Scope::ConsumersConfigure,
        Scope::ForksCreate,
        Scope::DlqConfigure,
        Scope::WatchesManage,
        Scope::CatalogRead,
        Scope::UsageRead,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Scope::MetadataRead => "streams.metadata.read",
            Scope::RecordsRead => "streams.records.read",
            Scope::RecordsAppend => "streams.records.append",
            Scope::Create => "streams.create",
            Scope::LifecycleManage => "streams.lifecycle.manage",
            Scope::ConsumersPull => "streams.consumers.pull",
            Scope::ConsumersSettle => "streams.consumers.settle",
            Scope::ConsumersConfigure => "streams.consumers.configure",
            Scope::ForksCreate => "streams.forks.create",
            Scope::DlqConfigure => "streams.dlq.configure",
            Scope::WatchesManage => "streams.watches.manage",
            Scope::CatalogRead => "streams.catalog.read",
            Scope::UsageRead => "streams.usage.read",
        }
    }

    pub fn parse(s: &str) -> Option<Scope> {
        Scope::ALL.iter().copied().find(|sc| sc.as_str() == s)
    }
}

/// Compact scope set parsed from the OAuth `scope` claim.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScopeSet(u16);

impl ScopeSet {
    pub const EMPTY: ScopeSet = ScopeSet(0);

    /// Parse a space-separated OAuth scope string. Unknown scopes are
    /// counted but NEVER granted: the issuer already intersected the
    /// request with the credential grant (§5), so an unknown value is
    /// either a newer scope this binary predates (must not authorize
    /// anything here) or garbage (must not authorize anything, and the
    /// count is an audit signal).
    pub fn parse(scope_claim: &str) -> (ScopeSet, usize) {
        let mut set = ScopeSet::EMPTY;
        let mut unknown = 0usize;
        for word in scope_claim.split_ascii_whitespace() {
            match Scope::parse(word) {
                Some(s) => set.0 |= s as u16,
                None => unknown += 1,
            }
        }
        (set, unknown)
    }

    pub fn has(self, scope: Scope) -> bool {
        self.0 & scope as u16 != 0
    }

    pub fn with(mut self, scope: Scope) -> Self {
        self.0 |= scope as u16;
        self
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn iter(self) -> impl Iterator<Item = Scope> {
        Scope::ALL.into_iter().filter(move |s| self.has(*s))
    }
}

impl fmt::Display for ScopeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for s in self.iter() {
            if !first {
                f.write_str(" ")?;
            }
            f.write_str(s.as_str())?;
            first = false;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Prefix grants (§6.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixError {
    Empty,
    TooLong {
        max: usize,
        got: usize,
    },
    /// leading/trailing/double slash — the grant must be a canonical
    /// component path, matching the stream-name component rules.
    EmptyComponent,
    ForbiddenChar {
        at: usize,
    },
    TooMany {
        max: usize,
        got: usize,
    },
}

impl fmt::Display for PrefixError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrefixError::Empty => write!(f, "prefix is empty"),
            PrefixError::TooLong { max, got } => {
                write!(f, "prefix is {got} bytes; maximum is {max}")
            }
            PrefixError::EmptyComponent => {
                write!(
                    f,
                    "prefix has an empty path component (leading/trailing/double slash)"
                )
            }
            PrefixError::ForbiddenChar { at } => {
                write!(
                    f,
                    "prefix contains a control/whitespace character at offset {at}"
                )
            }
            PrefixError::TooMany { max, got } => {
                write!(f, "{got} prefixes; maximum is {max}")
            }
        }
    }
}

/// A normalized prefix grant. Matching is component-aware (§6.2):
/// `customers/acme` matches `customers/acme` and
/// `customers/acme/orders` but NOT `customers/acme-other`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CanonicalPrefix(Arc<str>);

impl CanonicalPrefix {
    pub fn normalize(raw: &str) -> Result<Self, PrefixError> {
        if raw.is_empty() {
            return Err(PrefixError::Empty);
        }
        if raw.len() > PREFIX_MAX_BYTES {
            return Err(PrefixError::TooLong {
                max: PREFIX_MAX_BYTES,
                got: raw.len(),
            });
        }
        if let Some(at) = raw.bytes().position(|b| b <= 0x20 || b == 0x7f) {
            return Err(PrefixError::ForbiddenChar { at });
        }
        if raw.split('/').any(str::is_empty) {
            return Err(PrefixError::EmptyComponent);
        }
        Ok(Self(Arc::from(raw)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Component-aware match against a canonical stream name.
    pub fn matches(&self, stream_name: &str) -> bool {
        let p = self.0.as_ref();
        match stream_name.strip_prefix(p) {
            Some("") => true,
            Some(rest) => rest.starts_with('/'),
            None => false,
        }
    }

    /// True when `other` is this prefix or lies under it — used to
    /// drop redundant grants at normalization time.
    fn covers(&self, other: &CanonicalPrefix) -> bool {
        self.matches(other.as_str())
    }
}

/// Normalize a credential's full prefix-grant set (§6.2): validate
/// each prefix, enforce the count limit, sort, dedupe, and remove
/// grants made redundant by a shorter covering grant. An EMPTY result
/// is only produced by an empty input; callers must define empty-set
/// semantics at the policy layer (the contract's token carries the
/// set verbatim — this module does not invent "match everything").
pub fn normalize_prefix_set(raw: &[&str]) -> Result<Vec<CanonicalPrefix>, PrefixError> {
    if raw.len() > PREFIX_MAX_COUNT {
        return Err(PrefixError::TooMany {
            max: PREFIX_MAX_COUNT,
            got: raw.len(),
        });
    }
    let mut all = raw
        .iter()
        .map(|r| CanonicalPrefix::normalize(r))
        .collect::<Result<Vec<_>, _>>()?;
    // Shortest first, then lexicographic: a covering grant always
    // precedes anything it covers, so one forward pass suffices.
    all.sort_by(|a, b| {
        a.as_str()
            .len()
            .cmp(&b.as_str().len())
            .then_with(|| a.as_str().cmp(b.as_str()))
    });
    let mut kept: Vec<CanonicalPrefix> = Vec::with_capacity(all.len());
    for p in all {
        if !kept.iter().any(|k| k.covers(&p)) {
            kept.push(p);
        }
    }
    Ok(kept)
}

/// True when any grant in the (normalized, non-empty) set matches.
pub fn prefix_set_matches(grants: &[CanonicalPrefix], stream_name: &str) -> bool {
    grants.iter().any(|g| g.matches(stream_name))
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(s: &str) -> ProjectId {
        ProjectId::new(s).unwrap()
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
        let a = TenantStreamRef::from_canonical(pid("proj_a"), "orders");
        let b = TenantStreamRef::from_canonical(pid("proj_b"), "orders");
        assert_ne!(route_hash_input(&a), route_hash_input(&b));
        assert_ne!(storage_hash_input(&a, "e1"), storage_hash_input(&b, "e1"));
        assert_ne!(
            segment_identity_input(&a, "e1", 0),
            segment_identity_input(&b, "e1", 0)
        );
        // Same project, same name: identical (the reference identity).
        let a2 = TenantStreamRef::from_canonical(pid("proj_a"), "orders");
        assert_eq!(route_hash_input(&a), route_hash_input(&a2));
    }

    #[test]
    fn segment_id_is_a_fixed_width_component() {
        // Adjacent segment ids must not alias epoch bytes.
        let r = TenantStreamRef::from_canonical(pid("p"), "n");
        assert_ne!(
            segment_identity_input(&r, "e", 1),
            segment_identity_input(&r, "e", 256)
        );
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
    }

    #[test]
    fn prefix_set_drops_redundant_grants() {
        let set =
            normalize_prefix_set(&["customers/acme/orders", "customers/acme", "other"]).unwrap();
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
    fn prefix_count_limit_is_enforced() {
        let raws: Vec<String> = (0..65).map(|i| format!("p{i}")).collect();
        let refs: Vec<&str> = raws.iter().map(String::as_str).collect();
        assert_eq!(
            normalize_prefix_set(&refs),
            Err(PrefixError::TooMany { max: 64, got: 65 })
        );
    }
}
