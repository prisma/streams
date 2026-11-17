//! Peer client (WP-02 / PR 6-C): how this instance addresses and
//! authenticates to its fleet peers, and how it recognizes a peer's
//! static credential — extracted from `http::AppState`. The trusted
//! peer table is written by the fleet loop (published URLs beat
//! heartbeat URLs), read by every relay; the outbound bearer is workload
//! identity when a token source is configured, else the static bridge
//! token; SR3-1 makes the two EXCLUSIVE at runtime.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

/// How long a forward to a stream's owner waits to connect (edge change
/// #235): past it nothing was sent, and the client gets the non-owner's
/// `409` to replay.
pub(crate) const FORWARD_CONNECT: Duration = Duration::from_secs(1);

/// An expiry-aware workload-token source: `force_refresh` re-reads the
/// rotated credential (peer 401) instead of serving the cache.
pub(crate) type FleetTokenSource = Arc<dyn Fn(bool) -> Option<String> + Send + Sync>;

/// The fleet-internal header naming the overrides version an instance holds (F6 step 8): on a
/// forward and its `409`, and on a peer read page or span page and its `409` (the peer read hop).
pub(crate) const OVERRIDES_VERSION: &str = "streams-overrides-version";

/// This instance's overrides version as its fleet requests carry it, and the bounded refresh a
/// peer's answer naming another version asks for (F6 step 8 on the peer read hop, addendum
/// 2026-11-18 A1-A3). The fleet installs it; without it a request carries no version.
pub(crate) trait OverridesView: Send + Sync {
    /// The version this instance holds.
    fn tag(&self) -> String;
    /// Refresh this instance's mirror for a peer's version `seen`, within its bounds.
    fn refresh(&self, seen: String) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
}

#[derive(Clone)]
pub(crate) struct PeerClient {
    inner: Arc<Inner>,
}

struct Inner {
    /// Fresh peers' published base URLs, updated by the fleet loop.
    /// Empty in standalone mode or when SELF_URL isn't deployed.
    // mt-lint: allow(name-keyed-map): instance name -> base URL
    peer_urls: RwLock<HashMap<String, String>>,
    credentials: PeerCredentials,
    /// The transport of forwards to a stream's owner, built at the first
    /// forward; `None` inside when it could not be built.
    forward: OnceLock<Option<reqwest::Client>>,
    /// The fleet's overrides view, installed once at the fleet's start.
    overrides: OnceLock<Arc<dyn OverridesView>>,
}

/// Workload identity takes precedence at construction. The static bridge
/// secret is never retained alongside a workload source.
enum PeerCredentials {
    Workload(FleetTokenSource),
    Static(String),
    Absent,
}

impl PeerClient {
    pub(crate) fn new(
        static_token: Option<String>,
        token_source: Option<FleetTokenSource>,
    ) -> Self {
        let credentials = match (token_source, static_token) {
            (Some(source), _) => PeerCredentials::Workload(source),
            (None, Some(token)) => PeerCredentials::Static(token),
            (None, None) => PeerCredentials::Absent,
        };
        Self {
            inner: Arc::new(Inner {
                peer_urls: RwLock::new(HashMap::new()),
                credentials,
                forward: OnceLock::new(),
                overrides: OnceLock::new(),
            }),
        }
    }

    /// The trusted base URL of a peer, if the fleet published one.
    #[expect(
        clippy::unwrap_used,
        reason = "PeerClient trusted table; a poisoned replacement may be partial; recovery could route through an invalid peer snapshot"
    )]
    pub(crate) fn url_for(&self, instance: &str) -> Option<String> {
        self.inner.peer_urls.read().unwrap().get(instance).cloned()
    }

    #[expect(
        clippy::unwrap_used,
        reason = "PeerClient trusted table; a poisoned replacement may be partial; recovery could route through an invalid peer snapshot"
    )]
    pub(crate) fn has_peer(&self, instance: &str) -> bool {
        self.inner.peer_urls.read().unwrap().contains_key(instance)
    }

    /// Whether the fleet published any peer's URL: never on a fleet-off
    /// cell. A poisoned table answers yes, so the reader that needs a URL
    /// meets the poison where it reads one.
    pub(crate) fn has_peers(&self) -> bool {
        self.inner
            .peer_urls
            .read()
            .map_or(true, |peers| !peers.is_empty())
    }

    /// The transport of forwards to a stream's owner (`http::forward`): the
    /// peer pool's settings (HTTP/1.1, idle connections closed within 4 s,
    /// nodelay) with a [`FORWARD_CONNECT`] connect bound, one per instance.
    /// `None` when it cannot be built; a forward then answers its `409`.
    pub(crate) fn forward_client(&self) -> Option<&reqwest::Client> {
        self.inner
            .forward
            .get_or_init(|| {
                reqwest::Client::builder()
                    .http1_only()
                    .pool_idle_timeout(Duration::from_secs(4))
                    .tcp_nodelay(true)
                    .connect_timeout(FORWARD_CONNECT)
                    .build()
                    .ok()
            })
            .as_ref()
    }

    /// Replace the trusted peer table (the fleet loop, every tick).
    #[expect(
        clippy::unwrap_used,
        reason = "PeerClient trusted table; a poisoned replacement may be partial; recovery could route through an invalid peer snapshot"
    )]
    pub(crate) fn set_peers(&self, peers: HashMap<String, String>) {
        *self.inner.peer_urls.write().unwrap() = peers;
    }

    /// One peer, as a rig wires two instances together.
    #[cfg(test)]
    pub(crate) fn set_peer(&self, instance: &str, url: &str) {
        self.inner
            .peer_urls
            .write()
            .unwrap()
            .insert(instance.to_string(), url.to_string());
    }

    pub(crate) fn has_workload_source(&self) -> bool {
        matches!(self.inner.credentials, PeerCredentials::Workload(_))
    }

    /// The bearer this instance presents to peers: workload identity
    /// when a source is configured, else the static bridge token.
    pub(crate) fn outbound_bearer(&self, force_refresh: bool) -> Option<String> {
        match &self.inner.credentials {
            PeerCredentials::Workload(source) => source(force_refresh),
            PeerCredentials::Static(token) => Some(token.clone()),
            PeerCredentials::Absent => None,
        }
    }

    /// Does a presented bearer match the static bridge token? SR3-1:
    /// exclusive modes at runtime — with a workload source configured
    /// the static credential is DEAD even if a legacy token leaked into
    /// the environment (startup refuses that coexistence under the
    /// release posture; this is the defense-in-depth layer beneath it).
    pub(crate) fn inbound_static_ok(&self, presented: Option<&str>) -> bool {
        match &self.inner.credentials {
            PeerCredentials::Static(token) => {
                presented.is_some_and(|value| crate::crypto::secret_eq(value, token))
            }
            PeerCredentials::Workload(_) | PeerCredentials::Absent => false,
        }
    }

    /// Send one fleet-internal request built by `mk` (which receives the
    /// bearer to attach, if any). On a 401 with a workload source
    /// configured, the token is force-refreshed and the request retried
    /// ONCE — the rotated-credential path (§14.1). Any other outcome
    /// returns as-is.
    pub(crate) async fn send(
        &self,
        mk: impl Fn(Option<&str>) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let t = self.outbound_bearer(false);
        let mut resp = self.tagged(mk(t.as_deref())).send().await?;
        // (numeric: the transport-type lexer must not see a status type here)
        if resp.status().as_u16() == 401 && self.has_workload_source() {
            let t2 = self.outbound_bearer(true);
            resp = self.tagged(mk(t2.as_deref())).send().await?;
        }
        self.learn(&resp).await;
        Ok(resp)
    }

    /// Install the fleet's overrides view; the first one stays.
    pub(crate) fn watch_overrides(&self, view: Arc<dyn OverridesView>) {
        if self.inner.overrides.set(view).is_err() {
            tracing::debug!("the peer client already watches the fleet's overrides");
        }
    }

    /// `request` carrying this instance's overrides version, once the fleet installed its view.
    fn tagged(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.inner.overrides.get() {
            Some(view) => request.header(OVERRIDES_VERSION, view.tag()),
            None => request,
        }
    }

    /// A peer's `409` naming the owner to replay to under its overrides version refreshes this
    /// instance's mirror before the asker takes its next step (the peer read hop, A2): a second
    /// hop then carries the refreshed version, and an asker the `409` names resolves the read
    /// here.
    async fn learn(&self, answer: &reqwest::Response) {
        let Some(view) = self.inner.overrides.get() else {
            return;
        };
        let headers = answer.headers();
        if answer.status().as_u16() != 409 || !headers.contains_key("streams-replay-to") {
            return;
        }
        if let Some(seen) = headers.get(OVERRIDES_VERSION).and_then(|v| v.to_str().ok()) {
            view.refresh(seen.to_string()).await;
        }
    }
}

/// Percent-encode a stream name for use as a URL PATH, preserving the
/// hierarchy separator. Product names are hierarchical UTF-8 and may
/// legally contain '?', '#', '%' — interpolating one raw into a relay
/// URL turned the rest of the name into a query, fragment, or invalid
/// escape and addressed the wrong stream (round-19 fleet-contract
/// finding). Every internal relay must route its name through this.
// mt-lint: allow(name-param-shared-core): pure URL path encoding; no tenant, lookup, routing, or identity derivation
pub(crate) fn encode_stream_name_path(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    for seg in name.split('/') {
        if !out.is_empty() {
            out.push('/');
        }
        for b in seg.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
    }
    out
}

/// Shared client for fleet-internal peer calls (segment fan-out). One
/// pool, HTTP/1.1, idle timeout under the platform's ~5 s VM-suspend
/// socket kill (same rule as the store client and the pilot LB).
#[expect(
    clippy::expect_used,
    reason = "process peer HTTP pool; unusable transport initialization must fail before a request is sent; a fallback client would change the pinned transport policy"
)]
pub(crate) fn client() -> &'static reqwest::Client {
    static C: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .http1_only()
            .pool_idle_timeout(std::time::Duration::from_secs(4))
            .tcp_nodelay(true)
            .build()
            .expect("peer client")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// An overrides view that holds `tag` and records each refresh, which adopts the version seen.
    #[derive(Default)]
    struct Recorded {
        tag: std::sync::Mutex<String>,
        refreshed: std::sync::Mutex<Vec<String>>,
    }

    impl OverridesView for Recorded {
        fn tag(&self) -> String {
            self.tag.lock().unwrap().clone()
        }

        fn refresh(&self, seen: String) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
            self.refreshed.lock().unwrap().push(seen.clone());
            *self.tag.lock().unwrap() = seen;
            Box::pin(async {})
        }
    }

    /// **A peer's `409` refreshes the asker's view before its next hop, and that hop carries the
    /// refreshed version** (F6's peer read hop, addendum 2026-11-18 A2, its first case). The
    /// first request carries `v1`; the peer answers `409` naming another owner under `v2`; the
    /// view is refreshed for `v2` before `send` returns, and the second request carries `v2`.
    /// An answer that is not such a `409` refreshes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn a_409_refreshes_the_view_before_the_next_hop_which_carries_it() {
        let carried = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = carried.clone();
        let answer = move |headers: axum::http::HeaderMap| {
            let tag = headers
                .get(OVERRIDES_VERSION)
                .map(|v| v.to_str().unwrap().to_string());
            let mut seen = seen.lock().unwrap();
            seen.push(tag);
            let refusal = [("streams-replay-to", "p3"), (OVERRIDES_VERSION, "v2")];
            let status = [axum::http::StatusCode::CONFLICT, axum::http::StatusCode::OK];
            std::future::ready((status[seen.len().min(2) - 1], refusal))
        };
        let app = axum::Router::new().route("/r", axum::routing::get(answer));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/r", listener.local_addr().unwrap());
        let view = Arc::new(Recorded::default());
        *view.tag.lock().unwrap() = "v1".into();
        let peer = PeerClient::new(None, None);
        peer.watch_overrides(view.clone());
        let hops = async {
            let first = peer.send(|_| client().get(&url)).await.unwrap();
            let refreshed = view.refreshed.lock().unwrap().clone();
            let second = peer.send(|_| client().get(&url)).await.unwrap();
            (first.status().as_u16(), refreshed, second.status().as_u16())
        };
        let serve = axum::serve(listener, app).into_future();
        let ended = futures_util::future::select(Box::pin(hops), Box::pin(serve)).await;
        let futures_util::future::Either::Left((hops, _)) = ended else {
            panic!("the server ended first");
        };
        assert_eq!(hops, (409, vec!["v2".to_string()], 200));
        let carried = carried.lock().unwrap().clone();
        assert_eq!(carried, [Some("v1".to_string()), Some("v2".to_string())]);
        assert_eq!(
            view.refreshed.lock().unwrap().len(),
            1,
            "a 200 refreshes nothing"
        );
    }

    /// The peer table: published URLs, replaced wholesale by the fleet
    /// loop, read by relays; unknown instances are unroutable.
    #[test]
    fn peer_table_is_replaced_wholesale_and_read_by_name() {
        let p = PeerClient::new(None, None);
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
        let workload = PeerClient::new(Some("leaked-static".into()), Some(src));
        assert!(workload.has_workload_source());
        assert_eq!(workload.outbound_bearer(false).as_deref(), Some("cached"));
        assert_eq!(workload.outbound_bearer(true).as_deref(), Some("fresh"));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(
            !workload.inbound_static_ok(Some("leaked-static")),
            "the leaked static token is dead in workload mode"
        );

        let bridge = PeerClient::new(Some("bridge-token".into()), None);
        assert_eq!(
            bridge.outbound_bearer(true).as_deref(),
            Some("bridge-token")
        );
        assert!(bridge.inbound_static_ok(Some("bridge-token")));
        assert!(!bridge.inbound_static_ok(Some("bridge-tokeN")));
        assert!(!bridge.inbound_static_ok(None));

        let none = PeerClient::new(None, None);
        assert_eq!(none.outbound_bearer(false), None);
        assert!(!none.inbound_static_ok(Some("anything")));
    }

    #[test]
    fn unavailable_workload_identity_never_falls_back_to_a_static_secret() {
        let peer = PeerClient::new(Some("discarded-static".into()), Some(Arc::new(|_| None)));
        assert!(peer.has_workload_source());
        assert!(peer.outbound_bearer(false).is_none());
        assert!(peer.outbound_bearer(true).is_none());
        assert!(!peer.inbound_static_ok(Some("discarded-static")));
        assert!(!peer.inbound_static_ok(None));
        assert!(matches!(
            peer.inner.credentials,
            PeerCredentials::Workload(_)
        ));
    }

    #[test]
    fn partial_peer_table_poison_refuses_reads_and_replacement() {
        let peers = PeerClient::new(None, None);
        peers.set_peer("before", "http://before:1");
        let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut table = peers.inner.peer_urls.write().unwrap();
            table.clear();
            table.insert("partial".into(), "http://partial:1".into());
            panic!("interrupted trusted-table publication");
        }));
        assert!(interrupted.is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                peers.url_for("partial")
            }))
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                peers.has_peer("partial")
            }))
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                peers.set_peers(HashMap::new());
            }))
            .is_err()
        );
    }

    #[test]
    fn process_peer_transport_reuses_one_initialized_pool() {
        assert!(std::ptr::eq(client(), client()));
    }
}
