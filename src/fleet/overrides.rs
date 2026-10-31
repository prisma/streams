//! `fleet/overrides.json`: the rebalancer's shard moves.

/// fleet/overrides.json: rebalancer shard moves, CAS-updated by the
/// initiating (laggard) instance, read by everyone each fleet tick.
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub(crate) struct Overrides {
    #[serde(default)]
    // mt-lint: allow(name-keyed-map): shard prefix -> owner override
    pub entries: std::collections::HashMap<String, OverrideEntry>,
    /// The prefixes whose override a count move placed (`count_balance`):
    /// return-home leaves them, and a later count move may move them
    /// again. A document written before the field has none.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    // mt-lint: allow(name-keyed-map): shard prefixes a count move placed
    pub counted: std::collections::BTreeSet<String>,
    /// Pending-event outbox (§12.4) — same contract as Desired's.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_events: Vec<crate::ops::OpsEvent>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub(crate) struct OverrideEntry {
    pub to: String,
    pub ms: i64,
}

#[cfg(test)]
mod tests {
    use super::Overrides;

    #[test]
    fn a_document_without_counted_prefixes_reads_and_writes_as_before() {
        let text = r#"{"entries":{"00":{"to":"streams-2","ms":5}}}"#;
        let old: Overrides = serde_json::from_str(text).expect("an old document");
        assert!(old.counted.is_empty());
        assert_eq!(serde_json::to_string(&old).expect("serialize"), text);
        let mut counted = old;
        counted.counted.insert("00".to_owned());
        assert_eq!(
            serde_json::to_string(&counted).expect("serialize"),
            r#"{"entries":{"00":{"to":"streams-2","ms":5}},"counted":["00"]}"#
        );
    }
}
