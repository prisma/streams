//! Protocol request-body bounds and executable conformance-pin assertions.
//! Runtime admission uses the bounds below. Release version assertions are
//! compiled and run by the Rust test gate, alongside the actual suite manifest.

/// Protocol ceiling on a request body (wire pin). A deployment may
/// LOWER its effective limit (`MAX_REQUEST_BODY_BYTES`, proven by
/// `config::validation`) but never raise it above this.
pub(crate) const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Floor under the effective body ceiling — below this the product
/// surface stops being usable.
pub(crate) const MIN_BODY_BYTES: usize = 64 * 1024;

#[cfg(test)]
mod tests {
    const DURABLE_STREAMS_PROTOCOL_COMMIT: &str =
        "npm:@durable-streams/server-conformance-tests@0.3.6";
    const DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION: &str = "0.3.6";
    const DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION: &str = "0.2.12";

    #[test]
    fn release_pins_match_the_executable_conformance_manifest() {
        let manifest: serde_json::Value =
            serde_json::from_str(include_str!("../conformance/package.json")).unwrap();
        let expected: serde_json::Value =
            serde_json::from_str(include_str!("../conformance/expected.json")).unwrap();
        let package = "@durable-streams/server-conformance-tests";
        assert_eq!(
            manifest["dependencies"][package].as_str(),
            Some(DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION)
        );
        let suite = format!("{package}@{DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION}");
        assert_eq!(expected["suite"].as_str(), Some(suite.as_str()));
        assert_eq!(DURABLE_STREAMS_PROTOCOL_COMMIT, format!("npm:{suite}"));
        assert!(
            [
                DURABLE_STREAMS_PROTOCOL_COMMIT,
                DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION,
                DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION,
            ]
            .iter()
            .all(|pin| !pin.is_empty() && !pin.contains("UNPINNED"))
        );
    }
}
