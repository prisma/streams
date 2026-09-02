//! The deployment bearer (WP-02 / PR 6-C): the raw Durable Streams
//! surface's static account credential and the conformance default
//! key — extracted from `http::AppState`. Deliberately separate from
//! the fleet-internal credential: the two are separate trust
//! boundaries (round-19 security finding), so an internal token can
//! never perform a product operation.

use crate::auth::AuthMode;

#[derive(Clone, Debug)]
pub struct DeploymentBearer {
    token: Option<String>,
    default_key: Option<String>,
}

impl DeploymentBearer {
    pub fn new(token: Option<String>, default_key: Option<String>) -> Self {
        Self { token, default_key }
    }

    /// Does a presented bearer authorize the raw surface? SR-5 (Søren
    /// review): allow-if-unset is a LOCAL DEVELOPMENT convenience and
    /// exists only in Off mode. Shadow and enforce are multi-tenant
    /// postures — an unconfigured bearer there must close the surface,
    /// not open it.
    pub fn authorizes(&self, presented: Option<&str>, mode: AuthMode) -> bool {
        match &self.token {
            None => mode == AuthMode::Off,
            Some(t) => presented
                .map(|v| crate::crypto::secret_eq(v, t))
                .unwrap_or(false),
        }
    }

    /// The conformance suite's default stream key, when configured.
    pub fn default_key(&self) -> Option<&str> {
        self.default_key.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_bearer_opens_only_the_off_mode() {
        let b = DeploymentBearer::new(None, None);
        assert!(b.authorizes(None, AuthMode::Off));
        assert!(b.authorizes(Some("anything"), AuthMode::Off));
        assert!(!b.authorizes(None, AuthMode::Shadow));
        assert!(!b.authorizes(Some("anything"), AuthMode::Enforce));
        assert_eq!(b.default_key(), None);
    }

    #[test]
    fn set_bearer_compares_in_every_mode() {
        let b = DeploymentBearer::new(Some("s3cret".into()), Some("k".into()));
        for mode in [AuthMode::Off, AuthMode::Shadow, AuthMode::Enforce] {
            assert!(b.authorizes(Some("s3cret"), mode));
            assert!(!b.authorizes(Some("s3creT"), mode));
            assert!(!b.authorizes(None, mode));
        }
        assert_eq!(b.default_key(), Some("k"));
    }
}
