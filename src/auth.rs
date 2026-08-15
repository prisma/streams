//! Data-plane authentication (docs/MULTITENANCY.md §5–§8, Stage 2b).
//!
//! Local verification ONLY (§7.1): every check reads process-local
//! arc-swap snapshots (JWKS, project policy, credential grants) that
//! background refreshers publish; no request performs a synchronous
//! Control Plane lookup, and anything unverifiable — bad signature,
//! version mismatch, unknown project/credential, snapshot older than
//! the staleness window — fails CLOSED. The single deliberate
//! exception in SPIRIT is wrong-cell: a valid token for a project
//! placed elsewhere is not an authentication failure (§8.1) and gets
//! its own error so the HTTP layer can answer 421/`wrong_cell`
//! instead of tricking clients into refreshing a valid credential.
//!
//! Verification is a pure function of (token, now, snapshots) — the
//! clock is passed in, so the whole §19 authorization matrix runs
//! deterministically in unit tests against local fixture keys.
//!
//! ACTIVATION: this module is INERT for real traffic until the atomic
//! layout-4 switch (review round constraint). `AuthMode::Enforce`
//! refuses to boot while `registry::LAYOUT_VERSION < 4`; the default
//! mode is `Off`.

#![allow(dead_code)] // wired into the request path at the layout-4 switch

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};

use crate::project_policy::{CredentialStatus, GrantSnapshot, PolicySnapshot, ProjectStatus};
use crate::tenant::{
    CanonicalPrefix, ProjectId, Scope, ScopeSet, StreamGrant, WorkspaceId, validate_cell_id,
};

/// §5 recommended constraints.
pub const MAX_TOKEN_BYTES: usize = 8 * 1024;
pub const CLOCK_SKEW_SECS: i64 = 30;
pub const MAX_TOKEN_LIFETIME_SECS: i64 = 24 * 3600;
/// §7.1: policy/credential data unavailable beyond this window fails
/// closed. Generous relative to the 30–60s refresh cadence so a brief
/// Control Plane blip does not take the data plane down with it.
pub const POLICY_STALENESS_MAX_SECS: i64 = 300;

/// Separate trust boundaries (§14): three audiences, three verify
/// entry points. A customer token can never satisfy an internal or
/// operator check and vice versa.
pub const AUD_CUSTOMER: &str = "prisma-streams-data";
pub const AUD_INTERNAL: &str = "prisma-streams-internal";
pub const AUD_OPERATOR: &str = "prisma-streams-operator";

/// Explicit algorithm allowlist (§5). RS256 is what Prisma Auth mints
/// today; EdDSA is pre-approved for the planned key migration. Anything
/// else — including every HMAC alg, which would turn the PUBLIC jwks
/// into a signing oracle — is refused before signature verification.
const ALLOWED_ALGS: [Algorithm; 2] = [Algorithm::RS256, Algorithm::EdDSA];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthMode {
    Off,
    Shadow,
    Enforce,
}

impl AuthMode {
    pub fn from_env(raw: Option<&str>) -> anyhow::Result<Self> {
        match raw.unwrap_or("off") {
            "off" => Ok(AuthMode::Off),
            "shadow" => Ok(AuthMode::Shadow),
            "enforce" => {
                // Review-round constraint: principals must not gate real
                // traffic until storage identity is project-qualified,
                // or a verified project_id would authorize against
                // name-global data.
                anyhow::ensure!(
                    crate::registry::LAYOUT_VERSION >= 4,
                    "STREAMS_AUTH_MODE=enforce requires the layout-4 \
                     tenant-qualified registry (current layout {})",
                    crate::registry::LAYOUT_VERSION
                );
                Ok(AuthMode::Enforce)
            }
            other => anyhow::bail!("STREAMS_AUTH_MODE must be off|shadow|enforce, got {other:?}"),
        }
    }
}

/// Every distinct fail-closed reason, for metrics and (in shadow mode)
/// field diagnosis. `WrongCell` is special-cased by the HTTP layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthError {
    TokenTooLarge,
    Malformed(&'static str),
    KidMissing,
    KidUnknown,
    AlgNotAllowed,
    BadSignature,
    WrongIssuer,
    WrongAudience,
    /// §8.1: valid token, wrong cell — NOT an authentication failure.
    /// Also covers a project absent from this cell's policy snapshot:
    /// this cell does not serve it (or the feed lags), never a 401.
    WrongCell,
    Expired,
    NotYetValid,
    LifetimeTooLong,
    ClaimInvalid(&'static str),
    /// Contract r1: an empty stream_prefixes ARRAY is an issuer bug.
    EmptyPrefixArray,
    ProjectNotActive(ProjectStatus),
    OwnershipVersionMismatch,
    WorkspaceMismatch,
    CredentialUnknown,
    CredentialNotActive(CredentialStatus),
    CredentialExpired,
    CredentialProjectMismatch,
    GrantVersionMismatch,
    PolicyStale,
    GrantsStale,
    /// Post-verification authorization denials (scope / prefix).
    MissingScope(Scope),
    PrefixDenied,
}

impl AuthError {
    /// Stable metric key.
    pub fn kind(&self) -> &'static str {
        match self {
            AuthError::TokenTooLarge => "token_too_large",
            AuthError::Malformed(_) => "malformed",
            AuthError::KidMissing => "kid_missing",
            AuthError::KidUnknown => "kid_unknown",
            AuthError::AlgNotAllowed => "alg_not_allowed",
            AuthError::BadSignature => "bad_signature",
            AuthError::WrongIssuer => "wrong_issuer",
            AuthError::WrongAudience => "wrong_audience",
            AuthError::WrongCell => "wrong_cell",
            AuthError::Expired => "expired",
            AuthError::NotYetValid => "not_yet_valid",
            AuthError::LifetimeTooLong => "lifetime_too_long",
            AuthError::ClaimInvalid(_) => "claim_invalid",
            AuthError::EmptyPrefixArray => "empty_prefix_array",
            AuthError::ProjectNotActive(_) => "project_not_active",
            AuthError::OwnershipVersionMismatch => "ownership_version_mismatch",
            AuthError::WorkspaceMismatch => "workspace_mismatch",
            AuthError::CredentialUnknown => "credential_unknown",
            AuthError::CredentialNotActive(_) => "credential_not_active",
            AuthError::CredentialExpired => "credential_expired",
            AuthError::CredentialProjectMismatch => "credential_project_mismatch",
            AuthError::GrantVersionMismatch => "grant_version_mismatch",
            AuthError::PolicyStale => "policy_stale",
            AuthError::GrantsStale => "grants_stale",
            AuthError::MissingScope(_) => "missing_scope",
            AuthError::PrefixDenied => "prefix_denied",
        }
    }
}

/// §7: the verified per-request principal handlers receive. Field
/// note vs the contract sketch: `stream_prefixes` became the r1
/// `StreamGrant` (absent claim = All; empty array = rejected), and it
/// is the EFFECTIVE grant — the intersection of the token's grant and
/// the cached credential grant at the same grant_version, so neither
/// a stale cache nor a widened token can grant beyond the other.
#[derive(Clone, Debug)]
pub struct RequestPrincipal {
    pub workspace_id: WorkspaceId,
    pub project_id: ProjectId,
    /// Review item 5: the quotas from the EXACT policy snapshot this
    /// request verified against — quota admission must never re-read
    /// a later snapshot (a project vanishing between the two reads
    /// used to yield unlimited quotas).
    pub quotas: crate::project_policy::ProjectQuotas,
    pub project_policy_version: u64,
    pub cell_id: Arc<str>,
    pub credential_id: Arc<str>,
    pub subject: Arc<str>,
    pub ownership_version: u64,
    pub grant_version: u64,
    pub scopes: ScopeSet,
    pub grant: StreamGrant,
    pub token_id: Arc<str>,
    pub issued_at: i64,
    pub expires_at: i64,
}

impl RequestPrincipal {
    /// §6.1 route/method matrix entry point.
    pub fn require(&self, scope: Scope) -> Result<(), AuthError> {
        if self.scopes.has(scope) {
            Ok(())
        } else {
            Err(AuthError::MissingScope(scope))
        }
    }

    /// §6.2 component-aware prefix authorization.
    pub fn require_stream(&self, canonical_name: &str) -> Result<(), AuthError> {
        if self.grant.permits(canonical_name) {
            Ok(())
        } else {
            Err(AuthError::PrefixDenied)
        }
    }
}

/// §14.1 fleet workload principal (minimal Stage 2b shape; Stage 4
/// binds it to InternalStreamTarget verification).
#[derive(Clone, Debug)]
pub struct InternalPrincipal {
    pub subject: Arc<str>,
    pub cell_id: Arc<str>,
    pub operations: Vec<String>,
    pub expires_at: i64,
}

/// One published JWKS generation: kid -> verification key.
/// `feed_version` orders generations for the monotonic-publication
/// rule (review item 2) — key sets change on rotation, and an
/// out-of-order feed must not resurrect a retired signing key.
pub struct JwksSnapshot {
    pub keys: HashMap<String, DecodingKey>,
    pub fetched_at_unix: i64,
    pub feed_version: u64,
}

impl JwksSnapshot {
    pub fn empty() -> Self {
        Self {
            keys: HashMap::new(),
            fetched_at_unix: 0,
            feed_version: 0,
        }
    }
}

/// The raw claim set (§5). Deserialized only after the signature
/// verifies; every field is then range/format-checked into typed form.
#[derive(serde::Deserialize)]
struct RawClaims {
    iss: String,
    aud: String,
    sub: String,
    credential_id: String,
    project_id: String,
    workspace_id: String,
    cell_id: String,
    ownership_version: u64,
    grant_version: u64,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    stream_prefixes: Option<Vec<String>>,
    jti: String,
    iat: i64,
    #[serde(default)]
    nbf: Option<i64>,
    exp: i64,
}

#[derive(serde::Deserialize)]
struct RawInternalClaims {
    iss: String,
    aud: String,
    sub: String,
    cell_id: String,
    #[serde(default)]
    operations: Vec<String>,
    #[serde(default)]
    nbf: Option<i64>,
    exp: i64,
}

#[derive(Default)]
pub struct ShadowCounters {
    pub ok: AtomicU64,
    pub missing: AtomicU64,
    pub failed: AtomicU64,
    pub wrong_cell: AtomicU64,
}

/// §7.1. Request-path reads are lock-free snapshot loads; refreshers
/// publish via the `publish_*` methods (Stage 5 wires the real feed;
/// tests publish fixtures directly).
pub struct AuthService {
    pub mode: AuthMode,
    issuer: String,
    cell_id: Arc<str>,
    jwks: ArcSwap<JwksSnapshot>,
    projects: ArcSwap<PolicySnapshot>,
    credentials: ArcSwap<GrantSnapshot>,
    /// Unknown-kid sightings since the last JWKS refresh — the
    /// refresher's rate-limited signal to fetch out of cadence (§7.1).
    pub unknown_kid_seen: AtomicU64,
    pub shadow: ShadowCounters,
}

impl AuthService {
    pub fn new(mode: AuthMode, issuer: String, cell_id: &str) -> anyhow::Result<Self> {
        validate_cell_id(cell_id)
            .map_err(|e| anyhow::anyhow!("invalid cell id {cell_id:?}: {e}"))?;
        Ok(Self {
            mode,
            issuer,
            cell_id: Arc::from(cell_id),
            jwks: ArcSwap::from_pointee(JwksSnapshot::empty()),
            projects: ArcSwap::from_pointee(PolicySnapshot::empty()),
            credentials: ArcSwap::from_pointee(GrantSnapshot::empty()),
            unknown_kid_seen: AtomicU64::new(0),
            shadow: ShadowCounters::default(),
        })
    }

    /// Monotonic publication (review item 2, authorization P0): a
    /// stale or out-of-order feed must never restore an earlier
    /// authorization state — an earlier workspace owner, a revoked
    /// credential, a removed scope, a retired signing key. Each
    /// publish REFUSES a snapshot that would move any version
    /// backward; the refused snapshot is dropped, the current one
    /// keeps aging toward the §7.1 staleness refusal, and the
    /// refresher logs the refusal. Entries ABSENT from the new
    /// snapshot are removals (fail-closed); versions are only
    /// comparable while an entry is present on both sides, so the
    /// FEED must not resurrect removed entries at lower versions —
    /// recorded as the feed contract.
    pub fn publish_jwks(&self, snapshot: JwksSnapshot) -> Result<(), &'static str> {
        let cur = self.jwks.load();
        if snapshot.feed_version < cur.feed_version {
            return Err("jwks feed_version regressed");
        }
        self.jwks.store(Arc::new(snapshot));
        self.unknown_kid_seen.store(0, Ordering::Relaxed);
        Ok(())
    }

    pub fn publish_policies(&self, snapshot: PolicySnapshot) -> Result<(), &'static str> {
        let cur = self.projects.load();
        if snapshot.feed_version < cur.feed_version {
            return Err("policy feed_version regressed");
        }
        for (pid, np) in &snapshot.projects {
            if let Some(op) = cur.projects.get(pid) {
                if np.ownership_version < op.ownership_version {
                    return Err("ownership_version regressed");
                }
                if np.project_policy_version < op.project_policy_version {
                    return Err("project_policy_version regressed");
                }
            }
        }
        self.projects.store(Arc::new(snapshot));
        Ok(())
    }

    pub fn publish_grants(&self, snapshot: GrantSnapshot) -> Result<(), &'static str> {
        let cur = self.credentials.load();
        if snapshot.feed_version < cur.feed_version {
            return Err("grant feed_version regressed");
        }
        for (id, nc) in &snapshot.credentials {
            if let Some(oc) = cur.credentials.get(id) {
                if nc.grant_version < oc.grant_version {
                    return Err("grant_version regressed");
                }
                let was_dead = matches!(
                    oc.status,
                    CredentialStatus::Revoked | CredentialStatus::Disabled
                );
                if was_dead
                    && nc.status == CredentialStatus::Active
                    && nc.grant_version <= oc.grant_version
                {
                    // Un-revocation is an explicit act, never a replay:
                    // it must arrive under a STRICTLY newer version.
                    return Err("revoked credential reactivated without a newer grant_version");
                }
            }
        }
        self.credentials.store(Arc::new(snapshot));
        Ok(())
    }

    /// Signature + structural verification shared by all audiences.
    /// Returns the still-untrusted-but-authentic claim JSON.
    fn verify_signature<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
    ) -> Result<T, AuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::TokenTooLarge);
        }
        let header = decode_header(token).map_err(|_| AuthError::Malformed("header"))?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::AlgNotAllowed);
        }
        let kid = header.kid.ok_or(AuthError::KidMissing)?;
        let jwks = self.jwks.load();
        let key = match jwks.keys.get(&kid) {
            Some(k) => k,
            None => {
                self.unknown_kid_seen.fetch_add(1, Ordering::Relaxed);
                return Err(AuthError::KidUnknown);
            }
        };
        // jsonwebtoken verifies the signature with EXACTLY the header
        // alg (already allowlisted). Time and audience checks are done
        // by us against the injected clock, so tests are deterministic
        // and error kinds are precise.
        let mut v = Validation::new(header.alg);
        v.validate_exp = false;
        v.validate_nbf = false;
        v.validate_aud = false;
        v.required_spec_claims.clear();
        let data = decode::<T>(token, key, &v).map_err(|_| AuthError::BadSignature)?;
        Ok(data.claims)
    }

    fn check_times(&self, iat: i64, nbf: Option<i64>, exp: i64, now: i64) -> Result<(), AuthError> {
        if exp + CLOCK_SKEW_SECS <= now {
            return Err(AuthError::Expired);
        }
        if let Some(nbf) = nbf
            && nbf - CLOCK_SKEW_SECS > now
        {
            return Err(AuthError::NotYetValid);
        }
        if iat - CLOCK_SKEW_SECS > now {
            return Err(AuthError::ClaimInvalid("iat in the future"));
        }
        if exp - iat > MAX_TOKEN_LIFETIME_SECS + CLOCK_SKEW_SECS {
            return Err(AuthError::LifetimeTooLong);
        }
        Ok(())
    }

    /// §5 + §7.1: the customer-token pipeline. Pure in (token, now,
    /// published snapshots) — no ambient clock, no I/O.
    pub fn verify_customer(&self, token: &str, now: i64) -> Result<RequestPrincipal, AuthError> {
        let c: RawClaims = self.verify_signature(token)?;
        if c.iss != self.issuer {
            return Err(AuthError::WrongIssuer);
        }
        if c.aud != AUD_CUSTOMER {
            return Err(AuthError::WrongAudience);
        }
        self.check_times(c.iat, c.nbf, c.exp, now)?;

        let project_id =
            ProjectId::new(&c.project_id).map_err(|_| AuthError::ClaimInvalid("project_id"))?;
        // §10.4: the reserved system project is internal-only; a
        // customer token claiming it is hostile or a grave issuer bug.
        if project_id.is_system() {
            return Err(AuthError::ClaimInvalid("project_id is reserved"));
        }
        let workspace_id = WorkspaceId::new(&c.workspace_id)
            .map_err(|_| AuthError::ClaimInvalid("workspace_id"))?;
        validate_cell_id(&c.cell_id).map_err(|_| AuthError::ClaimInvalid("cell_id"))?;
        if c.credential_id.is_empty() || c.jti.is_empty() || c.sub.is_empty() {
            return Err(AuthError::ClaimInvalid("empty identity claim"));
        }

        // Token-carried grant (contract r1): absent = All, empty
        // array = issuer bug, entries must normalize.
        let token_grant = match &c.stream_prefixes {
            None => StreamGrant::All,
            Some(v) if v.is_empty() => return Err(AuthError::EmptyPrefixArray),
            Some(v) => {
                let mut out = Vec::with_capacity(v.len());
                for p in v {
                    out.push(
                        CanonicalPrefix::normalize(p)
                            .map_err(|_| AuthError::ClaimInvalid("stream_prefixes"))?,
                    );
                }
                StreamGrant::Prefixes(out.into())
            }
        };
        let (token_scopes, _unknown) = ScopeSet::parse(&c.scope);

        // §7.1 fail-closed policy checks, all from local snapshots.
        let policies = self.projects.load();
        if now - policies.fetched_at_unix > POLICY_STALENESS_MAX_SECS {
            return Err(AuthError::PolicyStale);
        }
        // §8.1: placement is not an authorization problem. This cell's
        // policy snapshot lists EXACTLY the projects placed here, so a
        // project absent from it is not served by this cell (or the
        // feed lags a fresh transfer) — either way it is WrongCell, not
        // a 401 that would make the client refresh a perfectly valid
        // credential. Then, for a project we DO serve, confirm the
        // token's own cell claim agrees.
        let policy = policies
            .projects
            .get(&project_id)
            .ok_or(AuthError::WrongCell)?;
        if policy.cell_id.as_ref() != self.cell_id.as_ref() || c.cell_id != *self.cell_id {
            return Err(AuthError::WrongCell);
        }
        // Ownership BEFORE status: a token minted under a previous owner
        // must not learn the CURRENT owner's project state (a stale
        // post-transfer token seeing "suspended" is an information
        // leak about someone else's project).
        if c.ownership_version != policy.ownership_version {
            return Err(AuthError::OwnershipVersionMismatch);
        }
        if policy.status != ProjectStatus::Active {
            return Err(AuthError::ProjectNotActive(policy.status));
        }
        if workspace_id != policy.workspace_id {
            return Err(AuthError::WorkspaceMismatch);
        }

        let grants = self.credentials.load();
        if now - grants.fetched_at_unix > POLICY_STALENESS_MAX_SECS {
            return Err(AuthError::GrantsStale);
        }
        let cred = grants
            .credentials
            .get(c.credential_id.as_str())
            .ok_or(AuthError::CredentialUnknown)?;
        if cred.project_id != project_id {
            return Err(AuthError::CredentialProjectMismatch);
        }
        if cred.status != CredentialStatus::Active {
            return Err(AuthError::CredentialNotActive(cred.status));
        }
        if let Some(cexp) = cred.expires_at
            && cexp <= now
        {
            return Err(AuthError::CredentialExpired);
        }
        // §4.3: EXACT grant-version equality — never `<=`, which would
        // let an old token keep a permission a newer grant removed.
        if c.grant_version != cred.grant_version {
            return Err(AuthError::GrantVersionMismatch);
        }

        // Effective authority = token ∩ credential at the SAME grant
        // version. The issuer already intersected; this keeps a buggy
        // issuer (or a poisoned cache) from widening either side.
        let scopes = token_scopes.intersect(cred.scopes);
        let grant = intersect_grants(&token_grant, &cred.grant);

        Ok(RequestPrincipal {
            workspace_id,
            quotas: policy.quotas.clone(),
            project_policy_version: policy.project_policy_version,
            project_id,
            cell_id: self.cell_id.clone(),
            credential_id: Arc::from(c.credential_id.as_str()),
            subject: Arc::from(c.sub.as_str()),
            ownership_version: c.ownership_version,
            grant_version: c.grant_version,
            scopes,
            grant,
            token_id: Arc::from(c.jti.as_str()),
            issued_at: c.iat,
            expires_at: c.exp,
        })
    }

    /// §14.1: fleet workload token (separate audience; no project
    /// authority — target binding is Stage 4's delegated capability).
    pub fn verify_internal(&self, token: &str, now: i64) -> Result<InternalPrincipal, AuthError> {
        let c: RawInternalClaims = self.verify_signature(token)?;
        if c.iss != self.issuer {
            return Err(AuthError::WrongIssuer);
        }
        if c.aud != AUD_INTERNAL {
            return Err(AuthError::WrongAudience);
        }
        // Workload tokens are short-lived; iat is not part of the §14
        // shape, so lifetime is bounded by exp alone.
        self.check_times(now, c.nbf, c.exp, now)?;
        if c.cell_id != *self.cell_id {
            return Err(AuthError::WrongCell);
        }
        if c.sub.is_empty() {
            return Err(AuthError::ClaimInvalid("sub"));
        }
        Ok(InternalPrincipal {
            subject: Arc::from(c.sub.as_str()),
            cell_id: self.cell_id.clone(),
            operations: c.operations,
            expires_at: c.exp,
        })
    }

    /// Shadow-mode observation hook: verify and count, never reject.
    /// Wired into the request path (behind `mode == Shadow`) at the
    /// layout-4 switch; exposed here so field diagnostics land in
    /// /v1/debug/load before enforcement ever turns on.
    pub fn shadow_observe(&self, bearer: Option<&str>, now: i64) {
        let Some(token) = bearer else {
            self.shadow.missing.fetch_add(1, Ordering::Relaxed);
            return;
        };
        match self.verify_customer(token, now) {
            Ok(_) => {
                self.shadow.ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(AuthError::WrongCell) => {
                self.shadow.wrong_cell.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.shadow.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The CURRENT policy quotas for a project (§17.2: enforcement
    /// reads the policy, never token claims). None = project not in
    /// the snapshot; callers treat that as no-quota because the
    /// request already passed verification against the same snapshot.
    pub fn quotas_for(
        &self,
        project: &crate::tenant::ProjectId,
    ) -> Option<crate::project_policy::ProjectQuotas> {
        self.projects
            .load()
            .projects
            .get(project)
            .map(|p| p.quotas.clone())
    }

    /// Snapshot freshness for the operator surface: whether each feed
    /// has EVER been published, how old it is against the fail-closed
    /// window, and what it contains.
    pub fn feed_json(&self, now: i64) -> serde_json::Value {
        let jwks = self.jwks.load();
        let policies = self.projects.load();
        let grants = self.credentials.load();
        let age = |fetched: i64| {
            if fetched == 0 {
                None
            } else {
                Some(now - fetched)
            }
        };
        serde_json::json!({
            "stalenessMaxSecs": POLICY_STALENESS_MAX_SECS,
            "jwks": { "keys": jwks.keys.len(), "ageSecs": age(jwks.fetched_at_unix) },
            "policies": {
                "projects": policies.projects.len(),
                "feedVersion": policies.feed_version,
                "ageSecs": age(policies.fetched_at_unix),
                "stale": now - policies.fetched_at_unix > POLICY_STALENESS_MAX_SECS,
            },
            "grants": {
                "credentials": grants.credentials.len(),
                "feedVersion": grants.feed_version,
                "ageSecs": age(grants.fetched_at_unix),
                "stale": now - grants.fetched_at_unix > POLICY_STALENESS_MAX_SECS,
            },
        })
    }

    pub fn shadow_json(&self) -> serde_json::Value {
        serde_json::json!({
            "mode": match self.mode {
                AuthMode::Off => "off",
                AuthMode::Shadow => "shadow",
                AuthMode::Enforce => "enforce",
            },
            "ok": self.shadow.ok.load(Ordering::Relaxed),
            "missing": self.shadow.missing.load(Ordering::Relaxed),
            "failed": self.shadow.failed.load(Ordering::Relaxed),
            "wrong_cell": self.shadow.wrong_cell.load(Ordering::Relaxed),
            "unknown_kid_seen": self.unknown_kid_seen.load(Ordering::Relaxed),
        })
    }
}

/// Grant intersection: a stream is permitted by the result iff BOTH
/// inputs permit it. For component-prefix sets that is exactly: keep
/// each prefix that the other side covers (the deeper of any covering
/// pair survives).
pub fn intersect_grants(a: &StreamGrant, b: &StreamGrant) -> StreamGrant {
    match (a, b) {
        (StreamGrant::All, g) | (g, StreamGrant::All) => g.clone(),
        (StreamGrant::Prefixes(pa), StreamGrant::Prefixes(pb)) => {
            let mut out: Vec<CanonicalPrefix> = Vec::new();
            for p in pa.iter() {
                if pb.iter().any(|q| q.matches(p.as_str())) && !out.contains(p) {
                    out.push(p.clone());
                }
            }
            for q in pb.iter() {
                if pa.iter().any(|p| p.matches(q.as_str())) && !out.contains(q) {
                    out.push(q.clone());
                }
            }
            StreamGrant::Prefixes(out.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_policy::{CredentialGrant, ProjectPolicy, ProjectQuotas};
    use jsonwebtoken::{EncodingKey, Header, encode};

    // Test-only keypair, checked in as a fixture (never deployed).
    const PRIV: &str = include_str!("dst/fixtures/mt-test-rsa.pem");
    const PUB: &str = include_str!("dst/fixtures/mt-test-rsa.pub.pem");
    const KID: &str = "test-1";
    const ISS: &str = "https://auth.prisma.io";
    const CELL: &str = "fra-cell-07";
    const NOW: i64 = 1_786_600_600;

    #[derive(serde::Serialize)]
    struct C {
        iss: String,
        aud: String,
        sub: String,
        credential_id: String,
        project_id: String,
        workspace_id: String,
        cell_id: String,
        ownership_version: u64,
        grant_version: u64,
        scope: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_prefixes: Option<Vec<String>>,
        jti: String,
        iat: i64,
        exp: i64,
    }

    fn claims() -> C {
        C {
            iss: ISS.into(),
            aud: AUD_CUSTOMER.into(),
            sub: "svc-1".into(),
            credential_id: "strcred_123".into(),
            project_id: "proj_456".into(),
            workspace_id: "ws_789".into(),
            cell_id: CELL.into(),
            ownership_version: 12,
            grant_version: 7,
            scope: "streams.records.read streams.records.append streams.metadata.read".into(),
            stream_prefixes: Some(vec!["customers/acme".into()]),
            jti: "tok_1".into(),
            iat: NOW - 60,
            exp: NOW + 600,
        }
    }

    fn sign(c: &C) -> String {
        sign_with(c, KID, jsonwebtoken::Algorithm::RS256)
    }

    fn sign_with(c: &C, kid: &str, alg: jsonwebtoken::Algorithm) -> String {
        let mut h = Header::new(alg);
        h.kid = Some(kid.to_string());
        encode(&h, c, &EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap()).unwrap()
    }

    fn service() -> AuthService {
        let svc = AuthService::new(AuthMode::Shadow, ISS.into(), CELL).unwrap();
        let mut keys = HashMap::new();
        keys.insert(
            KID.to_string(),
            DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
        );
        svc.publish_jwks(JwksSnapshot {
            keys,
            fetched_at_unix: NOW,
            feed_version: 1,
        })
        .unwrap();
        let pid = ProjectId::new("proj_456").unwrap();
        let mut projects = HashMap::new();
        projects.insert(
            pid.clone(),
            ProjectPolicy {
                project_id: pid.clone(),
                workspace_id: WorkspaceId::new("ws_789").unwrap(),
                cell_id: Arc::from(CELL),
                project_policy_version: 40,
                ownership_version: 12,
                status: ProjectStatus::Active,
                quotas: ProjectQuotas::default(),
            },
        );
        svc.publish_policies(PolicySnapshot {
            projects,
            fetched_at_unix: NOW,
            feed_version: 40,
        })
        .unwrap();
        let mut credentials = HashMap::new();
        credentials.insert(
            Arc::from("strcred_123"),
            CredentialGrant {
                credential_id: Arc::from("strcred_123"),
                project_id: pid,
                grant_version: 7,
                status: CredentialStatus::Active,
                scopes: {
                    let (s, _) = ScopeSet::parse(
                        "streams.records.read streams.records.append \
                         streams.metadata.read streams.create",
                    );
                    s
                },
                grant: StreamGrant::Prefixes(
                    vec![CanonicalPrefix::normalize("customers/acme").unwrap()].into(),
                ),
                expires_at: None,
            },
        );
        svc.publish_grants(GrantSnapshot {
            credentials,
            fetched_at_unix: NOW,
            feed_version: 7,
        })
        .unwrap();
        svc
    }

    #[test]
    fn happy_path_yields_a_full_principal() {
        let svc = service();
        let p = svc.verify_customer(&sign(&claims()), NOW).unwrap();
        assert_eq!(p.project_id.as_str(), "proj_456");
        assert_eq!(p.workspace_id.as_str(), "ws_789");
        assert_eq!(p.ownership_version, 12);
        assert_eq!(p.grant_version, 7);
        // Effective scopes = token ∩ credential: streams.create is in
        // the credential but NOT the token — not granted.
        assert!(p.require(Scope::RecordsRead).is_ok());
        assert!(p.require(Scope::RecordsAppend).is_ok());
        assert!(matches!(
            p.require(Scope::Create),
            Err(AuthError::MissingScope(Scope::Create))
        ));
        // §6.2 prefix matrix, component-aware.
        assert!(p.require_stream("customers/acme").is_ok());
        assert!(p.require_stream("customers/acme/orders").is_ok());
        assert_eq!(
            p.require_stream("customers/acme-other"),
            Err(AuthError::PrefixDenied)
        );
    }

    #[test]
    fn negative_matrix_tokens() {
        let svc = service();
        // Missing / oversized / garbage.
        assert_eq!(
            svc.verify_customer(&"x".repeat(MAX_TOKEN_BYTES + 1), NOW)
                .unwrap_err(),
            AuthError::TokenTooLarge
        );
        assert_eq!(
            svc.verify_customer("not-a-jwt", NOW).unwrap_err(),
            AuthError::Malformed("header")
        );
        // Expired / not-yet-valid / future-iat / lifetime cap.
        let mut c = claims();
        c.exp = NOW - 120;
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::Expired
        );
        let mut c = claims();
        c.iat = NOW + 600;
        c.exp = NOW + 1200;
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::ClaimInvalid("iat in the future")
        );
        let mut c = claims();
        c.iat = NOW - 60;
        c.exp = c.iat + MAX_TOKEN_LIFETIME_SECS + 3600;
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::LifetimeTooLong
        );
        // Wrong issuer / audience / cell.
        let mut c = claims();
        c.iss = "https://evil.example".into();
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::WrongIssuer
        );
        let mut c = claims();
        c.aud = AUD_INTERNAL.into();
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::WrongAudience
        );
        let mut c = claims();
        c.cell_id = "sin-cell-01".into();
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::WrongCell
        );
        // kid discipline + tampering.
        let c = claims();
        let mut h = Header::new(jsonwebtoken::Algorithm::RS256);
        h.kid = None;
        let no_kid = encode(&h, &c, &EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap()).unwrap();
        assert_eq!(
            svc.verify_customer(&no_kid, NOW).unwrap_err(),
            AuthError::KidMissing
        );
        assert_eq!(
            svc.verify_customer(
                &sign_with(&c, "unknown-kid", jsonwebtoken::Algorithm::RS256),
                NOW
            )
            .unwrap_err(),
            AuthError::KidUnknown
        );
        assert_eq!(svc.unknown_kid_seen.load(Ordering::Relaxed), 1);
        let tampered = {
            let t = sign(&claims());
            let mut parts: Vec<String> = t.split('.').map(String::from).collect();
            parts[1] = {
                use base64::Engine as _;
                let eng = base64::engine::general_purpose::URL_SAFE_NO_PAD;
                let mut body: serde_json::Value =
                    serde_json::from_slice(&eng.decode(&parts[1]).unwrap()).unwrap();
                body["project_id"] = "proj_evil".into();
                eng.encode(serde_json::to_vec(&body).unwrap())
            };
            parts.join(".")
        };
        assert_eq!(
            svc.verify_customer(&tampered, NOW).unwrap_err(),
            AuthError::BadSignature
        );
        // Empty prefix ARRAY is an issuer bug (contract r1).
        let mut c = claims();
        c.stream_prefixes = Some(vec![]);
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::EmptyPrefixArray
        );
        // Absent prefixes = All (bounded by the credential grant).
        let mut c = claims();
        c.stream_prefixes = None;
        let p = svc.verify_customer(&sign(&c), NOW).unwrap();
        assert!(p.require_stream("customers/acme/orders").is_ok());
        assert_eq!(p.require_stream("other"), Err(AuthError::PrefixDenied));
    }

    #[test]
    fn negative_matrix_policy_and_credential() {
        let svc = service();
        // Old ownership_version token refused (§4.2).
        let mut c = claims();
        c.ownership_version = 11;
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::OwnershipVersionMismatch
        );
        // Old grant_version token refused (§4.3) — exact, never <=.
        let mut c = claims();
        c.grant_version = 6;
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::GrantVersionMismatch
        );
        // A project absent from this cell's snapshot is WrongCell (§8.1),
        // never a 401 — the credential is valid, this cell just does not
        // serve that project.
        let mut c = claims();
        c.project_id = "proj_nope".into();
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::WrongCell
        );
        let mut c = claims();
        c.credential_id = "strcred_nope".into();
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::CredentialUnknown
        );
        // Suspended project fails closed.
        {
            let mut snap = svc.projects.load().as_ref().clone();
            snap.projects
                .get_mut(&ProjectId::new("proj_456").unwrap())
                .unwrap()
                .status = ProjectStatus::Suspended;
            svc.publish_policies(snap).unwrap();
            assert_eq!(
                svc.verify_customer(&sign(&claims()), NOW).unwrap_err(),
                AuthError::ProjectNotActive(ProjectStatus::Suspended)
            );
        }
        // Stale snapshots fail closed (§7.1).
        let svc = service();
        let mut snap = svc.projects.load().as_ref().clone();
        snap.fetched_at_unix = NOW - POLICY_STALENESS_MAX_SECS - 1;
        svc.publish_policies(snap).unwrap();
        assert_eq!(
            svc.verify_customer(&sign(&claims()), NOW).unwrap_err(),
            AuthError::PolicyStale
        );
    }

    #[test]
    fn internal_audience_is_a_separate_boundary() {
        let svc = service();
        // A CUSTOMER token never verifies as internal, and vice versa.
        assert_eq!(
            svc.verify_internal(&sign(&claims()), NOW).unwrap_err(),
            AuthError::WrongAudience
        );
        #[derive(serde::Serialize)]
        struct IC {
            iss: String,
            aud: String,
            sub: String,
            cell_id: String,
            operations: Vec<String>,
            exp: i64,
        }
        let ic = IC {
            iss: ISS.into(),
            aud: AUD_INTERNAL.into(),
            sub: "slot-3".into(),
            cell_id: CELL.into(),
            operations: vec!["segment-read".into()],
            exp: NOW + 300,
        };
        let mut h = Header::new(jsonwebtoken::Algorithm::RS256);
        h.kid = Some(KID.into());
        let t = encode(
            &h,
            &ic,
            &EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
        )
        .unwrap();
        let p = svc.verify_internal(&t, NOW).unwrap();
        assert_eq!(p.operations, vec!["segment-read"]);
        assert!(matches!(
            svc.verify_customer(&t, NOW),
            Err(AuthError::Malformed(_)) | Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn grant_intersection_is_sound() {
        let px = |s: &str| CanonicalPrefix::normalize(s).unwrap();
        let a = StreamGrant::Prefixes(vec![px("customers/acme"), px("internal")].into());
        let b = StreamGrant::Prefixes(vec![px("customers")].into());
        let i = intersect_grants(&a, &b);
        assert!(i.permits("customers/acme/orders"));
        assert!(!i.permits("internal/x"), "b never permitted internal");
        assert!(
            !i.permits("customers/zeta"),
            "a never permitted customers/zeta"
        );
        let i2 = intersect_grants(&StreamGrant::All, &b);
        assert!(i2.permits("customers/x") && !i2.permits("other"));
        let disjoint = intersect_grants(
            &StreamGrant::Prefixes(vec![px("a")].into()),
            &StreamGrant::Prefixes(vec![px("b")].into()),
        );
        assert!(!disjoint.permits("a/x") && !disjoint.permits("b/x"));
    }

    #[test]
    fn enforce_mode_refuses_pre_layout_4() {
        assert!(AuthMode::from_env(Some("off")).is_ok());
        assert!(AuthMode::from_env(Some("shadow")).is_ok());
        let enforce = AuthMode::from_env(Some("enforce"));
        if crate::registry::LAYOUT_VERSION < 4 {
            assert!(enforce.is_err(), "enforce must refuse pre-layout-4 boot");
        } else {
            assert!(enforce.is_ok());
        }
    }

    /// Review item 2 red tests: a STALE snapshot must never restore an
    /// earlier authorization state.
    #[test]
    fn stale_snapshot_cannot_unrevoke_a_credential() {
        let svc = service();
        // The stale snapshot: the credential still Active at grant v7.
        let stale = {
            let mut credentials = HashMap::new();
            credentials.insert(
                Arc::from("strcred_123"),
                CredentialGrant {
                    credential_id: Arc::from("strcred_123"),
                    project_id: ProjectId::new("proj_456").unwrap(),
                    grant_version: 7,
                    status: CredentialStatus::Active,
                    scopes: ScopeSet::parse("streams.records.read").0,
                    grant: StreamGrant::All,
                    expires_at: None,
                },
            );
            GrantSnapshot {
                credentials,
                fetched_at_unix: NOW,
                feed_version: 8,
            }
        };
        // REVOKE at grant v8, feed v9.
        let mut credentials = HashMap::new();
        credentials.insert(
            Arc::from("strcred_123"),
            CredentialGrant {
                credential_id: Arc::from("strcred_123"),
                project_id: ProjectId::new("proj_456").unwrap(),
                grant_version: 8,
                status: CredentialStatus::Revoked,
                scopes: ScopeSet::parse("streams.records.read").0,
                grant: StreamGrant::All,
                expires_at: None,
            },
        );
        svc.publish_grants(GrantSnapshot {
            credentials,
            fetched_at_unix: NOW,
            feed_version: 9,
        })
        .unwrap();
        // The stale replay is REFUSED on feed_version alone...
        assert!(svc.publish_grants(stale.clone()).is_err());
        // ...and even a same-feed-version forgery that re-activates at
        // the same grant_version is refused by the un-revocation rule.
        let mut forged = stale.clone();
        forged.feed_version = 9;
        forged.credentials.insert(
            Arc::from("strcred_123"),
            CredentialGrant {
                credential_id: Arc::from("strcred_123"),
                project_id: ProjectId::new("proj_456").unwrap(),
                grant_version: 8,
                status: CredentialStatus::Active,
                scopes: ScopeSet::parse("streams.records.read").0,
                grant: StreamGrant::All,
                expires_at: None,
            },
        );
        assert!(svc.publish_grants(forged).is_err());
        // The credential stays revoked.
        let c = claims();
        let mut c = c;
        c.grant_version = 8;
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::CredentialNotActive(CredentialStatus::Revoked)
        );
    }

    #[test]
    fn stale_snapshot_cannot_restore_a_previous_owner() {
        let svc = service();
        // Transfer: ownership_version 12 -> 13, new workspace.
        let pid = ProjectId::new("proj_456").unwrap();
        let mut projects = HashMap::new();
        projects.insert(
            pid.clone(),
            ProjectPolicy {
                project_id: pid.clone(),
                workspace_id: WorkspaceId::new("ws_NEW").unwrap(),
                cell_id: Arc::from(CELL),
                project_policy_version: 41,
                ownership_version: 13,
                status: ProjectStatus::Active,
                quotas: ProjectQuotas::default(),
            },
        );
        svc.publish_policies(PolicySnapshot {
            projects: projects.clone(),
            fetched_at_unix: NOW,
            feed_version: 41,
        })
        .unwrap();
        // A stale snapshot carrying the PREVIOUS owner (ownership 12,
        // ws_789) at a newer feed_version — the per-project rule
        // refuses it even when the feed counter says "newer".
        let mut old_projects = HashMap::new();
        old_projects.insert(
            pid.clone(),
            ProjectPolicy {
                project_id: pid.clone(),
                workspace_id: WorkspaceId::new("ws_789").unwrap(),
                cell_id: Arc::from(CELL),
                project_policy_version: 40,
                ownership_version: 12,
                status: ProjectStatus::Active,
                quotas: ProjectQuotas::default(),
            },
        );
        assert!(
            svc.publish_policies(PolicySnapshot {
                projects: old_projects,
                fetched_at_unix: NOW,
                feed_version: 42,
            })
            .is_err()
        );
        // Tokens minted under the OLD ownership stay dead.
        let c = claims(); // ownership_version 12
        assert_eq!(
            svc.verify_customer(&sign(&c), NOW).unwrap_err(),
            AuthError::OwnershipVersionMismatch
        );
    }

    #[test]
    fn stale_snapshot_cannot_restore_removed_scopes() {
        let svc = service();
        // Narrow the credential: records.read only, grant v8.
        let mut credentials = HashMap::new();
        credentials.insert(
            Arc::from("strcred_123"),
            CredentialGrant {
                credential_id: Arc::from("strcred_123"),
                project_id: ProjectId::new("proj_456").unwrap(),
                grant_version: 8,
                status: CredentialStatus::Active,
                scopes: ScopeSet::parse("streams.records.read").0,
                grant: StreamGrant::All,
                expires_at: None,
            },
        );
        svc.publish_grants(GrantSnapshot {
            credentials,
            fetched_at_unix: NOW,
            feed_version: 8,
        })
        .unwrap();
        // The stale wide-scope snapshot (grant v7) is refused.
        let mut wide = HashMap::new();
        wide.insert(
            Arc::from("strcred_123"),
            CredentialGrant {
                credential_id: Arc::from("strcred_123"),
                project_id: ProjectId::new("proj_456").unwrap(),
                grant_version: 7,
                status: CredentialStatus::Active,
                scopes: ScopeSet::parse(
                    "streams.records.read streams.records.append streams.create",
                )
                .0,
                grant: StreamGrant::All,
                expires_at: None,
            },
        );
        assert!(
            svc.publish_grants(GrantSnapshot {
                credentials: wide,
                fetched_at_unix: NOW,
                feed_version: 9,
            })
            .is_err()
        );
        // A v8 token authorizes with ONLY the narrowed scope.
        let mut c = claims();
        c.grant_version = 8;
        c.scope = "streams.records.read streams.records.append".into();
        let p = svc.verify_customer(&sign(&c), NOW).unwrap();
        assert!(p.scopes.has(Scope::RecordsRead));
        assert!(
            !p.scopes.has(Scope::RecordsAppend),
            "removed scope stays gone"
        );
    }

    #[test]
    fn stale_jwks_cannot_resurrect_a_retired_key() {
        let svc = service(); // publishes feed_version 1 keys
        let fresh = JwksSnapshot {
            keys: HashMap::new(), // rotation: the old kid is GONE
            fetched_at_unix: NOW,
            feed_version: 2,
        };
        svc.publish_jwks(fresh).unwrap();
        // The stale set (feed 1, containing the retired key) is refused.
        let mut keys = HashMap::new();
        keys.insert(
            KID.to_string(),
            DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
        );
        assert!(
            svc.publish_jwks(JwksSnapshot {
                keys,
                fetched_at_unix: NOW,
                feed_version: 1,
            })
            .is_err()
        );
        // Tokens under the retired kid stay dead.
        assert_eq!(
            svc.verify_customer(&sign(&claims()), NOW).unwrap_err(),
            AuthError::KidUnknown
        );
    }

    #[test]
    fn shadow_observe_counts_without_rejecting() {
        let svc = service();
        svc.shadow_observe(None, NOW);
        svc.shadow_observe(Some(&sign(&claims())), NOW);
        svc.shadow_observe(Some("garbage"), NOW);
        let mut c = claims();
        c.cell_id = "sin-cell-01".into();
        svc.shadow_observe(Some(&sign(&c)), NOW);
        let j = svc.shadow_json();
        assert_eq!(j["missing"], 1);
        assert_eq!(j["ok"], 1);
        assert_eq!(j["failed"], 1);
        assert_eq!(j["wrong_cell"], 1);
    }
}
