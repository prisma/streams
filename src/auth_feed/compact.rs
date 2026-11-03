//! The policy and grant feeds parsed straight into their snapshots (tuning
//! item 3c, step 1): from the file's bytes, with no whole-file `String` and
//! no list of parsed documents beside the map, and with one shared copy of
//! each distinct cell id, workspace id and (in the grants) project id. The
//! strict rules are `parse_policies`' and `parse_grants`': an item that
//! breaks one refuses the whole feed.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use serde::de::{Deserialize, Deserializer, Error as _, SeqAccess, Visitor};

use super::{GrantDoc, PolicyDoc};
use crate::project_policy::{CredentialGrant, GrantSnapshot, PolicySnapshot, ProjectPolicy};
use crate::tenant::{ProjectId, ScopeSet, StreamGrant};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFeed {
    feed_version: u64,
    projects: Policies,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantFeed {
    feed_version: u64,
    credentials: Grants,
}

/// The `projects` list, as the map it becomes.
struct Policies(HashMap<ProjectId, ProjectPolicy>);

/// The `credentials` list, as the map it becomes.
// mt-lint: allow(name-keyed-map): credential id -> grant (the feed snapshot itself)
struct Grants(HashMap<Arc<str>, CredentialGrant>);

/// The policy snapshot `json` holds, unstamped.
pub(super) fn policies(json: &[u8]) -> anyhow::Result<PolicySnapshot> {
    let feed: PolicyFeed = serde_json::from_slice(json)?;
    Ok(PolicySnapshot {
        projects: feed.projects.0,
        fetched_at_unix: 0,
        feed_version: feed.feed_version,
    })
}

/// The grant snapshot `json` holds, unstamped.
pub(super) fn grants(json: &[u8]) -> anyhow::Result<GrantSnapshot> {
    let feed: GrantFeed = serde_json::from_slice(json)?;
    Ok(GrantSnapshot {
        credentials: feed.credentials.0,
        fetched_at_unix: 0,
        feed_version: feed.feed_version,
    })
}

/// One shared copy of each distinct value.
struct Interned<T>(HashSet<T>);

impl<T: Clone + Eq + std::hash::Hash> Interned<T> {
    fn new() -> Self {
        Self(HashSet::new())
    }

    fn share(&mut self, value: T) -> T {
        if let Some(shared) = self.0.get(&value) {
            return shared.clone();
        }
        self.0.insert(value.clone());
        value
    }
}

impl<'de> Deserialize<'de> for Policies {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(PoliciesVisitor)
    }
}

struct PoliciesVisitor;

impl<'de> Visitor<'de> for PoliciesVisitor {
    type Value = Policies;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a list of project policies")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Policies, A::Error> {
        let mut projects = HashMap::with_capacity(items.size_hint().unwrap_or(0));
        let (mut cells, mut workspaces) = (Interned::new(), Interned::new());
        while let Some(doc) = items.next_element::<PolicyDoc>()? {
            crate::tenant::validate_cell_id(&doc.cell_id).map_err(|e| {
                A::Error::custom(format!("project {:?}: bad cell_id: {e}", doc.project_id))
            })?;
            let policy = ProjectPolicy {
                project_id: doc.project_id.clone(),
                workspace_id: workspaces.share(doc.workspace_id),
                cell_id: cells.share(Arc::<str>::from(doc.cell_id)),
                project_policy_version: doc.project_policy_version,
                ownership_version: doc.ownership_version,
                status: doc.status,
                quotas: doc.quotas,
            };
            if projects.insert(doc.project_id.clone(), policy).is_some() {
                let duplicate = format!("duplicate project {:?}", doc.project_id);
                return Err(A::Error::custom(duplicate));
            }
        }
        Ok(Policies(projects))
    }
}

impl<'de> Deserialize<'de> for Grants {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(GrantsVisitor)
    }
}

struct GrantsVisitor;

impl<'de> Visitor<'de> for GrantsVisitor {
    type Value = Grants;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a list of credential grants")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Grants, A::Error> {
        let mut credentials = HashMap::with_capacity(items.size_hint().unwrap_or(0));
        let mut projects = Interned::new();
        while let Some(doc) = items.next_element::<GrantDoc>()? {
            let mut grant = grant(doc).map_err(|e| A::Error::custom(format!("{e:#}")))?;
            grant.project_id = projects.share(grant.project_id);
            let id = grant.credential_id.clone();
            if credentials.insert(id.clone(), grant).is_some() {
                return Err(A::Error::custom(format!("duplicate credential {id:?}")));
            }
        }
        Ok(Grants(credentials))
    }
}

/// One credential's grant, refused as `parse_grants` refuses it: an empty
/// id, an unknown scope, an empty or malformed `stream_prefixes`.
fn grant(g: GrantDoc) -> anyhow::Result<CredentialGrant> {
    anyhow::ensure!(!g.credential_id.is_empty(), "credential with empty id");
    let (scopes, unknown) = ScopeSet::parse(&g.scopes);
    anyhow::ensure!(
        unknown == 0,
        "credential {:?}: {unknown} unknown scope(s) in {:?} — a typo \
         here silently narrows authority, refuse it at load",
        g.credential_id,
        g.scopes
    );
    let grant = match g.stream_prefixes {
        None => StreamGrant::All,
        Some(v) if v.is_empty() => anyhow::bail!(
            "credential {:?}: stream_prefixes is an EMPTY array (grants \
             nothing); omit the field for all-streams or list prefixes",
            g.credential_id
        ),
        Some(v) => {
            // Review item 6: the same SET normalizer as tokens —
            // per-prefix grammar, the 64-prefix cap, and redundancy
            // pruning.
            let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
            let set = crate::tenant::normalize_prefix_set(&refs).map_err(|e| {
                anyhow::anyhow!("credential {:?}: stream_prefixes: {e:?}", g.credential_id)
            })?;
            StreamGrant::Prefixes(set.into())
        }
    };
    Ok(CredentialGrant {
        credential_id: Arc::from(g.credential_id.as_str()),
        project_id: g.project_id,
        grant_version: g.grant_version,
        status: g.status,
        scopes,
        grant,
        expires_at: g.expires_at,
    })
}
