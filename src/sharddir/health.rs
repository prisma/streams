//! Retained readiness policy for one directory.
use std::sync::{Arc, Mutex};
/// Per-directory readiness. Process-wide open counters below remain
/// metrics; another runtime's success can never heal this directory.
#[derive(Clone, Default)]
pub(crate) struct ShardHealth(pub(super) Arc<Mutex<OpenHealth>>);

#[derive(Default)]
pub(super) struct OpenHealth {
    ever_opened: bool,
    // Only the three distinct strikes needed by policy are retained.
    // mt-lint: allow(name-keyed-map): physical shard prefixes for open-health strikes, never stream names.
    pub(super) failed: std::collections::BTreeSet<String>,
    last_error: Option<String>,
    /// Required worker loss is a cell failure, retained through eviction.
    engine_failure: Option<String>,
}

impl ShardHealth {
    #[expect(
        clippy::unwrap_used,
        reason = "ShardHealth::succeeded; a poisoned health record may hold a half-recorded failure; recovering it could report readiness a failure already withdrew"
    )]
    pub(super) fn succeeded(&self) {
        self.0.lock().unwrap().ever_opened = true;
    }
    #[expect(
        clippy::unwrap_used,
        reason = "ShardHealth::failed; a poisoned health record may hold a half-recorded failure; recovering it could report readiness a failure already withdrew"
    )]
    pub(super) fn failed(&self, prefix: &str, error: String) {
        let mut h = self.0.lock().unwrap();
        if h.failed.len() < 3 {
            h.failed.insert(prefix.to_string());
        }
        h.last_error = Some(error);
    }
    #[expect(
        clippy::unwrap_used,
        reason = "ShardHealth::engine_failed; a poisoned health record may hold a half-recorded failure; recovering it could report readiness a failure already withdrew"
    )]
    pub(super) fn engine_failed(&self, prefix: &str, role: &str) {
        self.0
            .lock()
            .unwrap()
            .engine_failure
            .get_or_insert_with(|| format!("required engine task terminated: {prefix}/{role}"));
    }
    #[expect(
        clippy::unwrap_used,
        reason = "ShardHealth::unready_reason; a poisoned health record may hold a half-recorded failure; recovering it could report readiness a failure already withdrew"
    )]
    pub(crate) fn unready_reason(&self) -> Option<String> {
        let h = self.0.lock().unwrap();
        if let Some(failure) = &h.engine_failure {
            return Some(failure.clone());
        }
        if h.ever_opened || h.failed.len() < 3 {
            return None;
        }
        Some(format!(
            "no shard has ever opened ({} distinct shards failed); last error: {}",
            h.failed.len(),
            h.last_error.as_deref().unwrap_or("unknown")
        ))
    }
}
