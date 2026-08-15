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

// Narrow dead-code allowances only (review round): each unconsumed
// item carries its own `#[allow(dead_code)]` with the stage that
// consumes it, and the allowance is REMOVED in that stage's commits.
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
    /// Byte outside the contract-r1 allowlist [A-Za-z0-9_-]. Strict on
    /// purpose: ids are hex-encoded in registry paths so charset is
    /// not a path-safety issue, but Unicode confusables, bidi
    /// controls, and zero-width characters in a tenant id are a
    /// security trap, and the Control Plane only mints ASCII ids.
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
                    "identifier contains a byte outside [A-Za-z0-9_-] at offset {at}"
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
    // Contract r1 frozen grammar: 1*128( ALPHA / DIGIT / "_" / "-" ).
    // A strict allowlist, not a denylist: Unicode whitespace, bidi
    // controls, and zero-width characters are all multi-byte UTF-8 and
    // fall outside it byte-by-byte. Supersedable only by a SHARED
    // Control Plane parser, and only in the tightening direction.
    if let Some(at) = raw
        .bytes()
        .position(|b| !(b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))
    {
        return Err(IdentityError::ForbiddenByte { at });
    }
    Ok(())
}

/// Mutable commercial/authorization identity (§1.1). Never an input to
/// a storage identity; deliberately NOT a field of [`TenantStreamRef`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkspaceId(Arc<str>);

#[allow(dead_code)] // consumed from MT Stage 2b (auth.rs principal)
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

#[allow(dead_code)] // fully consumed at MT Stage 2b/3 (principal + registry)
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

impl serde::Serialize for ProjectId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for ProjectId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        ProjectId::new(&raw).map_err(serde::de::Error::custom)
    }
}

/// Same validated-construction rule as `ProjectId`: no unchecked bytes
/// enter through a deserializer side door (Stage 5 feed files).
impl<'de> serde::Deserialize<'de> for WorkspaceId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        WorkspaceId::new(&raw).map_err(serde::de::Error::custom)
    }
}

impl ProjectId {
    /// Registry identity of `canonical_name` INSIDE this project —
    /// the per-request addressing primitive (MULTITENANCY Stage 5d):
    /// the verified principal's project selects the tenant-qualified
    /// storage identity; the deployment tenant no longer addresses
    /// customer data on principal-carrying paths.
    pub fn stream_ref(&self, canonical_name: &str) -> TenantStreamRef {
        TenantStreamRef::new(
            self.clone(),
            CanonicalStreamName::new(canonical_name)
                .expect("caller passed a canonical stream name"),
        )
    }
}

/// The reserved project id that owns system streams (_usage,
/// _ops_events, _ops_metrics, _audit_events — MULTITENANCY §10.4).
/// Registry paths for it live under `system/v1/cells/<cell-id>/`,
/// OUTSIDE every customer project root; auth refuses it in customer
/// token claims, and startup refuses it as the deployment project.
pub const SYSTEM_PROJECT: &str = "system";

#[allow(dead_code)] // consumed at MT Stage 4/7 (system-stream relocation)
impl ProjectId {
    pub fn system() -> Self {
        ProjectId(Arc::from(SYSTEM_PROJECT))
    }
    pub fn is_system(&self) -> bool {
        self.as_str() == SYSTEM_PROJECT
    }
}

/// Validate a cell id (§2). Cells stay `Arc<str>` in principals; this
/// is the shared bound check for config and token claims.
#[allow(dead_code)] // consumed from MT Stage 2b (auth.rs principal)
pub fn validate_cell_id(raw: &str) -> Result<(), IdentityError> {
    validate_id(raw, ID_MAX_BYTES)
}

/// Stream-name length bound (matches `product::canonical_name`).
pub const NAME_MAX_BYTES: usize = 512;
/// The reserved system namespace root. Owned here (identity layer);
/// `product.rs` re-exports it for the HTTP surface.
pub const RESERVED_ROOT: &str = "__ds";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameError {
    Empty,
    TooLong { max: usize, got: usize },
    ControlChar { at: usize },
    EmptyComponent,
    DotComponent,
    ReservedRoot,
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameError::Empty => write!(f, "stream name must be 1-{NAME_MAX_BYTES} UTF-8 bytes"),
            NameError::TooLong { max, got } => {
                write!(f, "stream name is {got} bytes; maximum is {max}")
            }
            NameError::ControlChar { at } => {
                write!(f, "control characters are not allowed (offset {at})")
            }
            NameError::EmptyComponent => write!(f, "empty path segments are not allowed"),
            NameError::DotComponent => write!(f, "'.' and '..' segments are not allowed"),
            NameError::ReservedRoot => write!(f, "the {RESERVED_ROOT} namespace is reserved"),
        }
    }
}

/// The shared per-component rule for stream names AND prefix grants:
/// non-empty, not `.` or `..`. Character-level rules (control chars)
/// are enforced on the whole string by the callers because their
/// offsets differ.
fn valid_component(c: &str) -> Result<(), NameError> {
    if c.is_empty() {
        return Err(NameError::EmptyComponent);
    }
    if c == "." || c == ".." {
        return Err(NameError::DotComponent);
    }
    Ok(())
}

/// A structurally canonical stream name — the only name type identity
/// derivation accepts. Construction is checked (no unchecked public
/// path): 1–512 bytes, no control characters, slash-separated
/// components that are non-empty and not `.`/`..`, and not rooted in
/// the reserved `__ds` namespace.
///
/// `product::canonical_name` layers the product-surface
/// ADDRESSABILITY rules on top (reserved final segments, no
/// subresource-shaped names); those are HTTP-creation concerns, not
/// identity-safety concerns, which is why they live there. A unit test
/// pins the two validators together: everything `canonical_name`
/// accepts, this constructor accepts.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CanonicalStreamName(Arc<str>);

#[allow(dead_code)] // fully consumed at MT Stage 3/4 (registry + handlers)
impl CanonicalStreamName {
    pub fn new(raw: &str) -> Result<Self, NameError> {
        if raw.is_empty() {
            return Err(NameError::Empty);
        }
        if raw.len() > NAME_MAX_BYTES {
            return Err(NameError::TooLong {
                max: NAME_MAX_BYTES,
                got: raw.len(),
            });
        }
        if let Some(at) = raw
            .char_indices()
            .find(|(_, c)| c.is_control())
            .map(|(i, _)| i)
        {
            return Err(NameError::ControlChar { at });
        }
        let mut first = true;
        for comp in raw.split('/') {
            valid_component(comp)?;
            if first && comp == RESERVED_ROOT {
                return Err(NameError::ReservedRoot);
            }
            first = false;
        }
        Ok(Self(Arc::from(raw)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for CanonicalStreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The project-qualified stream reference every handler and registry
/// call operates on after Stage 4. Fields are PRIVATE and construction
/// is checked end to end: this type is a security boundary — an
/// unvalidated name here would flow into registry paths and every
/// layout-4 hash.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantStreamRef {
    project_id: ProjectId,
    name: CanonicalStreamName,
}

#[allow(dead_code)] // fully consumed at MT Stage 3/4 (registry + handlers)
impl TenantStreamRef {
    pub fn new(project_id: ProjectId, name: CanonicalStreamName) -> Self {
        Self { project_id, name }
    }

    pub fn project_id(&self) -> &ProjectId {
        &self.project_id
    }

    pub fn name(&self) -> &CanonicalStreamName {
        &self.name
    }
}

impl fmt::Display for TenantStreamRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display is for logs/errors only; '§' cannot appear in either
        // component (ids are an ASCII allowlist and canonical names
        // have no control chars, but '/' appears in names, so the
        // separator must not be '/').
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
#[allow(dead_code)] // CatalogCursorV1/WatchCapabilityV1 consumed at MT Stage 3/4
pub enum HashDomain {
    RouteV1,
    /// Contract r1: scaler-minted placement of a split-child segment.
    RouteChildV1,
    StorageV1,
    SegmentV1,
    CatalogCursorV1,
    WatchCapabilityV1,
}

impl HashDomain {
    pub const fn tag(self) -> &'static [u8] {
        match self {
            HashDomain::RouteV1 => b"route-v1",
            HashDomain::RouteChildV1 => b"route-child-v1",
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
        &[sref.project_id().as_bytes(), sref.name().as_bytes()],
    )
}

/// storage hash input: storage-v1 + project_id + stream_name + stream_epoch
pub fn storage_hash_input(sref: &TenantStreamRef, stream_epoch: &str) -> Vec<u8> {
    encode_hash_input(
        HashDomain::StorageV1,
        &[
            sref.project_id().as_bytes(),
            sref.name().as_bytes(),
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
            sref.project_id().as_bytes(),
            sref.name().as_bytes(),
            stream_epoch.as_bytes(),
            &segment_id.to_be_bytes(),
        ],
    )
}

/// split-child route hash input (contract r1):
/// route-child-v1 + project_id + stream_name + child_segment_id + salt
pub fn route_child_hash_input(
    sref: &TenantStreamRef,
    child_segment_id: u32,
    salt: &[u8],
) -> Vec<u8> {
    encode_hash_input(
        HashDomain::RouteChildV1,
        &[
            sref.project_id().as_bytes(),
            sref.name().as_bytes(),
            &child_segment_id.to_be_bytes(),
            salt,
        ],
    )
}

// ---------------------------------------------------------------------------
// Scopes (§6)
// ---------------------------------------------------------------------------

/// The thirteen explicit data-plane scopes (§6). Broad scopes like
/// STREAM_MANAGE are deliberately absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
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

#[allow(dead_code)] // consumed from MT Stage 2b (auth.rs authorization)
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
pub struct ScopeSet(u64);

#[allow(dead_code)] // consumed from MT Stage 2b (auth.rs authorization)
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
                Some(s) => set.0 |= s as u64,
                None => unknown += 1,
            }
        }
        (set, unknown)
    }

    pub fn has(self, scope: Scope) -> bool {
        self.0 & scope as u64 != 0
    }

    pub fn with(mut self, scope: Scope) -> Self {
        self.0 |= scope as u64;
        self
    }

    /// Effective-authority intersection (auth.rs: token ∩ credential).
    pub fn intersect(self, other: ScopeSet) -> ScopeSet {
        ScopeSet(self.0 & other.0)
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
    DotComponent,
    ReservedRoot,
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
            PrefixError::DotComponent => {
                write!(
                    f,
                    "'.' and '..' components are not allowed in a prefix grant"
                )
            }
            PrefixError::ReservedRoot => {
                write!(
                    f,
                    "prefix grants may not target the reserved {RESERVED_ROOT} namespace"
                )
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

#[allow(dead_code)] // consumed from MT Stage 2b (auth.rs prefix checks)
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
        if let Some(at) = raw
            .char_indices()
            .find(|(_, c)| c.is_control() || *c == ' ')
            .map(|(i, _)| i)
        {
            return Err(PrefixError::ForbiddenChar { at });
        }
        // A prefix grant follows the stream-name COMPONENT rules
        // (shared `valid_component`): a grant of `.`/`..`/`a//b` can
        // never match a canonical name and is an issuer bug, and a
        // grant rooted in the reserved system namespace must not
        // exist. Deliberately NOT the full stream-name validator:
        // final-segment addressability rules (records/consumers/
        // watches, subresource grammar) do not apply to a prefix — a
        // grant `a/records` legitimately narrows to streams UNDER
        // `a/records/…` even though no stream may be NAMED exactly
        // that.
        let mut first = true;
        for comp in raw.split('/') {
            valid_component(comp).map_err(|e| match e {
                NameError::EmptyComponent => PrefixError::EmptyComponent,
                _ => PrefixError::DotComponent,
            })?;
            if first && comp == RESERVED_ROOT {
                return Err(PrefixError::ReservedRoot);
            }
            first = false;
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
#[allow(dead_code)] // consumed from MT Stage 2b (credential-grant cache)
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
#[allow(dead_code)] // consumed from MT Stage 2b (auth.rs prefix checks)
pub fn prefix_set_matches(grants: &[CanonicalPrefix], stream_name: &str) -> bool {
    grants.iter().any(|g| g.matches(stream_name))
}

/// A credential's stream grant (contract r1, §6.2): an explicit type,
/// never an overloaded empty set. In the access token, an ABSENT
/// `stream_prefixes` claim means `All`; an EMPTY ARRAY is invalid and
/// the token is rejected — "no streams" is expressed by not issuing
/// the credential, and an empty array is far more likely an issuer
/// bug than an intent.
#[derive(Clone, Debug)]
pub enum StreamGrant {
    All,
    Prefixes(Arc<[CanonicalPrefix]>),
}

impl StreamGrant {
    pub fn permits(&self, stream_name: &str) -> bool {
        match self {
            StreamGrant::All => true,
            StreamGrant::Prefixes(p) => prefix_set_matches(p, stream_name),
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
}
