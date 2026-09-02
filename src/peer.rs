//! Peer client (WP-02 / PR 6-C): how this instance addresses and
//! authenticates to its fleet peers, and how it recognizes a peer's
//! static credential — extracted from `http::AppState`. The trusted
//! peer table is written by the fleet loop (published URLs beat
//! heartbeat URLs), read by every relay; the outbound bearer is workload
//! identity when a token source is configured, else the static bridge
//! token; SR3-1 makes the two EXCLUSIVE at runtime.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// An expiry-aware workload-token source: `force_refresh` re-reads the
/// rotated credential (peer 401) instead of serving the cache.
pub type FleetTokenSource = Arc<dyn Fn(bool) -> Option<String> + Send + Sync>;

#[derive(Clone)]
pub struct PeerClient {
    inner: Arc<Inner>,
}

struct Inner {
    /// Fresh peers' published base URLs, updated by the fleet loop.
    /// Empty in standalone mode or when SELF_URL isn't deployed.
    // mt-lint: allow(name-keyed-map): instance name -> base URL
    peer_urls: RwLock<HashMap<String, String>>,
    /// The static bridge token (legacy posture) — absent in workload
    /// mode whatever the environment carried.
    static_token: Option<String>,
    token_source: Option<FleetTokenSource>,
    fleet_store: Option<Arc<dyn object_store::ObjectStore>>,
}

impl PeerClient {
    pub fn new(
        static_token: Option<String>,
        token_source: Option<FleetTokenSource>,
        fleet_store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                peer_urls: RwLock::new(HashMap::new()),
                static_token,
                token_source,
                fleet_store,
            }),
        }
    }

    /// The trusted base URL of a peer, if the fleet published one.
    pub fn url_for(&self, instance: &str) -> Option<String> {
        self.inner.peer_urls.read().unwrap().get(instance).cloned()
    }

    pub fn has_peer(&self, instance: &str) -> bool {
        self.inner.peer_urls.read().unwrap().contains_key(instance)
    }

    /// Replace the trusted peer table (the fleet loop, every tick).
    pub fn set_peers(&self, peers: HashMap<String, String>) {
        *self.inner.peer_urls.write().unwrap() = peers;
    }

    /// One peer, as a rig wires two instances together.
    #[cfg(test)]
    pub fn set_peer(&self, instance: &str, url: &str) {
        self.inner
            .peer_urls
            .write()
            .unwrap()
            .insert(instance.to_string(), url.to_string());
    }

    pub fn has_workload_source(&self) -> bool {
        self.inner.token_source.is_some()
    }

    /// The bearer this instance presents to peers: workload identity
    /// when a source is configured, else the static bridge token.
    pub fn outbound_bearer(&self, force_refresh: bool) -> Option<String> {
        if let Some(src) = &self.inner.token_source {
            return src(force_refresh);
        }
        self.inner.static_token.clone()
    }

    /// Does a presented bearer match the static bridge token? SR3-1:
    /// exclusive modes at runtime — with a workload source configured
    /// the static credential is DEAD even if a legacy token leaked into
    /// the environment (startup refuses that coexistence under the
    /// release posture; this is the defense-in-depth layer beneath it).
    pub fn inbound_static_ok(&self, presented: Option<&str>) -> bool {
        match (&self.inner.token_source, &self.inner.static_token) {
            (Some(_), _) => false,
            (None, Some(t)) => presented
                .map(|v| crate::crypto::secret_eq(v, t))
                .unwrap_or(false),
            (None, None) => false,
        }
    }

    /// Send one fleet-internal request built by `mk` (which receives the
    /// bearer to attach, if any). On a 401 with a workload source
    /// configured, the token is force-refreshed and the request retried
    /// ONCE — the rotated-credential path (§14.1). Any other outcome
    /// returns as-is.
    pub async fn send(
        &self,
        mk: impl Fn(Option<&str>) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let t = self.outbound_bearer(false);
        let resp = mk(t.as_deref()).send().await?;
        // (numeric: the transport-type lexer must not see a status type here)
        if resp.status().as_u16() == 401 && self.has_workload_source() {
            let t2 = self.outbound_bearer(true);
            return mk(t2.as_deref()).send().await;
        }
        Ok(resp)
    }

    /// The fleet object store (heartbeats, desired count), when fleet
    /// mode is on.
    pub fn fleet_store(&self) -> Option<&Arc<dyn object_store::ObjectStore>> {
        self.inner.fleet_store.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// The peer table: published URLs, replaced wholesale by the fleet
    /// loop, read by relays; unknown instances are unroutable.
    #[test]
    fn peer_table_is_replaced_wholesale_and_read_by_name() {
        let p = PeerClient::new(None, None, None);
        assert!(!p.has_peer("b"));
        p.set_peer("b", "http://b:1");
        assert_eq!(p.url_for("b").as_deref(), Some("http://b:1"));
        p.set_peers(HashMap::from([("c".to_string(), "http://c:1".to_string())]));
        assert!(!p.has_peer("b"), "replaced, not merged");
        assert_eq!(p.url_for("c").as_deref(), Some("http://c:1"));
    }

    /// SR3-1: with a workload source the static credential is dead in
    /// both directions; without one, the bridge token is compared in
    /// constant time; with neither, nothing authorizes.
    #[test]
    fn credential_modes_are_exclusive() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let src: FleetTokenSource = Arc::new(move |force| {
            c.fetch_add(1, Ordering::Relaxed);
            Some(if force {
                "fresh".into()
            } else {
                "cached".into()
            })
        });
        let workload = PeerClient::new(Some("leaked-static".into()), Some(src), None);
        assert!(workload.has_workload_source());
        assert_eq!(workload.outbound_bearer(false).as_deref(), Some("cached"));
        assert_eq!(workload.outbound_bearer(true).as_deref(), Some("fresh"));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(
            !workload.inbound_static_ok(Some("leaked-static")),
            "the leaked static token is dead in workload mode"
        );

        let bridge = PeerClient::new(Some("bridge-token".into()), None, None);
        assert_eq!(
            bridge.outbound_bearer(true).as_deref(),
            Some("bridge-token")
        );
        assert!(bridge.inbound_static_ok(Some("bridge-token")));
        assert!(!bridge.inbound_static_ok(Some("bridge-tokeN")));
        assert!(!bridge.inbound_static_ok(None));

        let none = PeerClient::new(None, None, None);
        assert_eq!(none.outbound_bearer(false), None);
        assert!(!none.inbound_static_ok(Some("anything")));
        assert!(none.fleet_store().is_none());
    }
}
