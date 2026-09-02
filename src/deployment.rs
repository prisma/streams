//! Deployment identity (WP-02 / PR 6-D): WHO this deployment is — the
//! deployment tenant the raw surface addresses, the billing account,
//! the telemetry cell and region — proven by validation and owned
//! here, extracted from `http::AppState`. The registry owner
//! (`registry::Registry`) already existed; this is the identity the
//! raw adapters and the catalog sweep address it through.

use std::sync::Arc;

use crate::tenant::{CanonicalStreamName, CellId, ProjectId, TenantStreamRef};

#[derive(Clone, Debug)]
pub struct DeploymentIdentity {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    tenant: ProjectId,
    account_id: String,
    cell_id: CellId,
    region: String,
}

impl DeploymentIdentity {
    pub fn new(tenant: ProjectId, account_id: String, cell_id: CellId, region: String) -> Self {
        Self {
            inner: Arc::new(Inner {
                tenant,
                account_id,
                cell_id,
                region,
            }),
        }
    }

    /// Project-qualify a canonical stream name under the DEPLOYMENT
    /// tenant — the raw-surface adapter's identity source (§14.3: the
    /// raw surface is internal-only and always addresses the deployment
    /// tenant). The name says the scope: ONLY the raw adapters
    /// (`get_segments`, `stream_entry_inner`, `read`) and test fixtures
    /// may call this; everywhere else identity comes from the verified
    /// principal or an existing TenantStreamRef. Enforced by
    /// mt_lint::multitenancy_identity_lint (SR-6). `canonical_name` MUST
    /// already be canonical (`canonical_name` ran at the route boundary);
    /// the checked construction keeps unvalidated bytes out of registry
    /// paths and identity hashes.
    // mt-lint: allow(name-param-shared-core): the raw adapters' ONE identity source — a canonical name becomes deployment-tenant identity here and nowhere else (SR-6)
    pub fn raw_adapter_sref(&self, canonical_name: &str) -> TenantStreamRef {
        TenantStreamRef::new(
            self.inner.tenant.clone(),
            CanonicalStreamName::new(canonical_name)
                .expect("caller passed a canonical stream name"),
        )
    }

    /// The deployment tenant. ADOPTING it is a reviewed act: mt-lint's
    /// `state-tenant-read` applies to every call of this accessor exactly
    /// as it applied to the field it replaced — each caller carries a
    /// reviewed marker naming its posture.
    pub fn deployment_tenant(&self) -> &ProjectId {
        // mt-lint: allow(state-tenant-read): the identity owner's own accessor — every caller is linted at its call site
        &self.inner.tenant
    }

    pub fn account_id(&self) -> &str {
        &self.inner.account_id
    }

    pub fn cell_id(&self) -> &CellId {
        &self.inner.cell_id
    }

    pub fn region(&self) -> &str {
        &self.inner.region
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> DeploymentIdentity {
        DeploymentIdentity::new(
            ProjectId::new("proj-x").unwrap(),
            "acct-x".into(),
            CellId::new("cell-x").unwrap(),
            "eu".into(),
        )
    }

    /// The raw adapter's identity source qualifies a canonical name
    /// under the deployment tenant and nothing else.
    #[test]
    fn raw_adapter_sref_qualifies_under_the_deployment_tenant() {
        let d = identity();
        let sref = d.raw_adapter_sref("a/b");
        assert_eq!(sref.project_id(), d.deployment_tenant());
        assert_eq!(sref.name().as_str(), "a/b");
        assert_eq!(d.account_id(), "acct-x");
        assert_eq!(d.cell_id().as_str(), "cell-x");
        assert_eq!(d.region(), "eu");
    }

    /// A non-canonical name is a caller bug, never a laundered identity.
    #[test]
    #[should_panic(expected = "caller passed a canonical stream name")]
    fn raw_adapter_sref_refuses_a_non_canonical_name() {
        identity().raw_adapter_sref("__ds/..");
    }
}
