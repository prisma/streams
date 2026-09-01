//! Explicit environment source (WP-01 PR 3.1, hermetic since PR 3.2).
//!
//! Configuration parsing reads from an [`Environment`] value, never from
//! the ambient process environment directly. Production uses
//! [`ProcessEnvironment`] exactly once at the composition root; tests use
//! [`MapEnvironment`]. No test mutates process-wide environment state:
//! the one smoke test that exercises the real environment runs its
//! subject in a subprocess whose environment is established before it
//! starts (`config::tests::process_environment_smoke_test`).

#[cfg(test)]
use std::collections::BTreeMap;

/// A read-only view over named environment values.
pub trait Environment: Send + Sync {
    fn get(&self, key: &str) -> Option<String>;
}

/// The real process environment. Used once, in the binary composition
/// root.
pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// A map-backed environment for tests and deterministic rigs.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct MapEnvironment {
    // mt-lint: allow(name-keyed-map): keyed by environment-variable name
    values: BTreeMap<String, String>,
}

#[cfg(test)]
impl MapEnvironment {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from<K: Into<String>, V: Into<String>>(
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        Self {
            values: entries
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

#[cfg(test)]
impl Environment for MapEnvironment {
    fn get(&self, key: &str) -> Option<String> {
        self.values.get(key).cloned()
    }
}
