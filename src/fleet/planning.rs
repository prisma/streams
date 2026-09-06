//! Pure fleet-view preparation. Required authority reads must all succeed
//! before the controller publishes the resulting ring and override snapshot.
use std::collections::HashMap;

// mt-lint: allow(name-keyed-map): fleet instance -> heartbeat age
pub(super) fn active_members(
    count: u64,
    instance: &str,
    ages: &HashMap<String, i64>,
) -> Vec<String> {
    let ordinal: Vec<String> = (1..=count.max(1))
        .map(|index| format!("streams-{index}"))
        .collect();
    let active: Vec<String> = ordinal
        .iter()
        .filter(|name| {
            name.as_str() == instance || ages.get(*name).is_some_and(|age| *age < 30_000)
        })
        .cloned()
        .collect();
    if active.is_empty() { ordinal } else { active }
}

// mt-lint: allow(name-keyed-map): fleet instance -> validated peer base URL
pub(super) fn trusted_urls(
    map: HashMap<String, String>,
    policy: &crate::config::FleetConfig,
) -> HashMap<String, String> {
    map.into_iter()
        .filter(|(instance, url)| {
            let valid = super::valid_peer_url(url, policy);
            if !valid {
                tracing::warn!(%instance,"rejecting malformed peer URL from urls.json");
            }
            valid
        })
        .collect()
}
