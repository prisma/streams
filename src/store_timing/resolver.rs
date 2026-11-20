//! The store client's DNS cache (Addendum 17 of the forward-wedge ruling, 2026-11-20, its second
//! commit). The client opens a connection per request when its pool is cold, and `object_store`'s
//! default resolver looks the endpoint up on the blocking pool for every one of them, with no
//! cache, so a burst of 2,500 new connections after an idle spell makes 2,500 lookups. This one
//! keeps each host's addresses for `DNS_TTL`, resolves a miss once however many connections wait
//! for it, and forgets a host whose connect failed, so the next connection looks it up again.
//!
//! Each answer rotates the host's addresses by one, so successive connections start at
//! different addresses, as the default resolver's shuffle spread them.

use std::collections::HashMap;
use std::net::IpAddr;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use object_store::client::{DnsError, DnsFuture, DnsResolver};
use tokio::time::Instant;

/// How long a host's addresses are kept.
pub(crate) const DNS_TTL: Duration = Duration::from_secs(10);

/// How a host's addresses are found.
pub(crate) trait Lookup: std::fmt::Debug + Send + Sync + UnwindSafe + RefUnwindSafe {
    fn lookup(&self, host: String) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>>;
}

/// The system's resolver, on tokio's blocking pool.
#[derive(Debug)]
struct SystemLookup;

impl Lookup for SystemLookup {
    fn lookup(&self, host: String) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
        Box::pin(async move {
            let found = tokio::net::lookup_host((host.as_str(), 0)).await?;
            Ok(found.map(|addr| addr.ip()).collect())
        })
    }
}

/// One lookup in flight, which every connection waiting for the host shares.
type Pending = Shared<BoxFuture<'static, Result<Arc<[IpAddr]>, Arc<str>>>>;

/// What is known of one host.
#[derive(Debug)]
enum Entry {
    Known {
        addrs: Arc<[IpAddr]>,
        until: Instant,
    },
    Looking(Pending),
}

type Hosts = Arc<Mutex<HashMap<String, Entry>>>;

/// The cache, shared by one HTTP client's connections.
#[derive(Debug)]
pub(crate) struct CachingResolver {
    hosts: Hosts,
    lookup: Arc<dyn Lookup>,
    turn: AtomicUsize,
}

impl Default for CachingResolver {
    fn default() -> Self {
        Self::with_lookup(Arc::new(SystemLookup))
    }
}

impl CachingResolver {
    pub(crate) fn with_lookup(lookup: Arc<dyn Lookup>) -> Self {
        Self {
            hosts: Arc::default(),
            lookup,
            turn: AtomicUsize::new(0),
        }
    }

    /// Forgets `host`'s addresses after a connect to it failed. A lookup in flight is kept: it
    /// began after the addresses that failed were found.
    pub(crate) fn forget(&self, host: &str) {
        let mut hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(hosts.get(host), Some(Entry::Known { .. })) {
            hosts.remove(host);
        }
    }

    /// `host`'s addresses while they are fresh, or the lookup every waiter shares.
    fn known_or_pending(&self, host: &str) -> Result<Arc<[IpAddr]>, Pending> {
        let mut hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        match hosts.get(host) {
            Some(Entry::Known { addrs, until }) if Instant::now() < *until => Ok(addrs.clone()),
            Some(Entry::Looking(pending)) => Err(pending.clone()),
            _ => {
                let pending = self.look_up(host);
                hosts.insert(host.to_string(), Entry::Looking(pending.clone()));
                Err(pending)
            }
        }
    }

    /// The lookup of `host`: on an answer with addresses it keeps them for `DNS_TTL`; on any
    /// other answer it forgets the host, so the next connection looks it up again.
    fn look_up(&self, host: &str) -> Pending {
        let (hosts, name) = (self.hosts.clone(), host.to_string());
        let found = self.lookup.lookup(name.clone());
        async move {
            let answer = match found.await {
                Ok(addrs) if !addrs.is_empty() => Ok(Arc::<[IpAddr]>::from(addrs)),
                Ok(_) => Err(Arc::<str>::from(format!("no addresses for {name}"))),
                Err(error) => Err(Arc::<str>::from(error.to_string())),
            };
            let mut hosts = hosts.lock().unwrap_or_else(PoisonError::into_inner);
            match &answer {
                Ok(addrs) => {
                    let until = Instant::now() + DNS_TTL;
                    let addrs = addrs.clone();
                    hosts.insert(name, Entry::Known { addrs, until });
                }
                Err(_) => {
                    hosts.remove(&name);
                }
            }
            answer
        }
        .boxed()
        .shared()
    }
}

/// `addrs` started at the `turn`-th, wrapping.
fn rotated(addrs: &[IpAddr], turn: usize) -> Vec<IpAddr> {
    let start = turn % addrs.len().max(1);
    addrs[start..]
        .iter()
        .chain(&addrs[..start])
        .copied()
        .collect()
}

impl DnsResolver for CachingResolver {
    fn resolve(&self, host: &str) -> DnsFuture {
        let turn = self.turn.fetch_add(1, Ordering::Relaxed);
        let found = self.known_or_pending(host);
        Box::pin(async move {
            let addrs = match found {
                Ok(addrs) => addrs,
                Err(pending) => pending.await.map_err(|error| DnsError::from(&*error))?,
            };
            Ok(rotated(&addrs, turn))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{CachingResolver, DNS_TTL, Lookup, rotated};
    use futures_util::future::BoxFuture;
    use object_store::client::DnsResolver;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Answers `addrs` after 50 ms (an error while `fail` is set), counting its lookups.
    #[derive(Debug)]
    struct Counted {
        addrs: Vec<IpAddr>,
        lookups: AtomicUsize,
        fail: std::sync::atomic::AtomicBool,
    }

    impl Lookup for Counted {
        fn lookup(&self, _host: String) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            let answer = if self.fail.load(Ordering::Relaxed) {
                Err(std::io::Error::other("refused"))
            } else {
                Ok(self.addrs.clone())
            };
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                answer
            })
        }
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    fn resolver_of(addrs: Vec<IpAddr>) -> (Arc<Counted>, CachingResolver) {
        let counted = Arc::new(Counted {
            addrs,
            lookups: AtomicUsize::new(0),
            fail: std::sync::atomic::AtomicBool::new(false),
        });
        let resolver = CachingResolver::with_lookup(counted.clone());
        (counted, resolver)
    }

    fn resolver() -> (Arc<Counted>, CachingResolver) {
        resolver_of(vec![ip(1), ip(2)])
    }

    #[tokio::test(start_paused = true, crate = "crate::test_rt")]
    async fn a_burst_after_an_idle_spell_is_one_lookup_kept_for_the_ttl() {
        let (counted, resolver) = resolver();
        let burst = (0..2_500).map(|_| resolver.resolve("store.example"));
        let answers = futures_util::future::try_join_all(burst).await.unwrap();
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            1,
            "one lookup for the burst"
        );
        assert_eq!(
            (answers[0].clone(), answers[1].clone()),
            (vec![ip(1), ip(2)], vec![ip(2), ip(1)])
        );
        tokio::time::advance(DNS_TTL - Duration::from_millis(1)).await;
        resolver.resolve("store.example").await.unwrap();
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            1,
            "fresh within the TTL"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        resolver.resolve("store.example").await.unwrap();
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            2,
            "looked up again after it"
        );
        resolver.resolve("other.example").await.unwrap();
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            3,
            "each host its own"
        );
    }

    #[tokio::test(start_paused = true, crate = "crate::test_rt")]
    async fn a_failed_connect_or_lookup_forgets_the_host() {
        let (counted, resolver) = resolver();
        resolver.resolve("store.example").await.unwrap();
        resolver.forget("store.example");
        resolver.resolve("store.example").await.unwrap();
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            2,
            "a failed connect forgets"
        );
        resolver.forget("store.example");
        counted.fail.store(true, Ordering::Relaxed);
        let error = resolver.resolve("store.example").await.unwrap_err();
        assert_eq!(error.to_string(), "refused");
        counted.fail.store(false, Ordering::Relaxed);
        resolver.resolve("store.example").await.unwrap();
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            4,
            "a failure is not kept"
        );
    }

    #[test]
    fn rotation_starts_each_answer_one_address_on() {
        let addrs = [ip(1), ip(2), ip(3)];
        assert_eq!(rotated(&addrs, 4), vec![ip(2), ip(3), ip(1)]);
        assert_eq!(rotated(&[], 7), Vec::<IpAddr>::new());
    }

    /// One GET of `/o` on `store.test` at `port` through `http`: its status, or its error's kind.
    async fn get(
        http: &object_store::client::HttpClient,
        port: u16,
    ) -> Result<u16, object_store::client::HttpErrorKind> {
        let request = axum::http::Request::builder()
            .uri(format!("http://store.test:{port}/o"))
            .body(object_store::client::HttpRequestBody::empty())
            .unwrap();
        match http.execute(request).await {
            Ok(answer) => Ok(answer.status().as_u16()),
            Err(error) => Err(error.kind()),
        }
    }

    #[tokio::test(crate = "crate::test_rt")]
    async fn the_store_client_shares_one_lookup_and_looks_up_again_after_a_failed_connect() {
        use object_store::client::{HttpConnector, HttpErrorKind};
        let (counted, dns) = resolver_of(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let dns = Arc::new(dns);
        let options = object_store::ClientOptions::new()
            .with_allow_http(true)
            .with_dns_resolver(dns.clone());
        let http = crate::store_timing::SniffConnector(dns)
            .connect(&options)
            .unwrap();
        // Each answer closes its connection, so every request of the burst connects anew.
        let answer = || std::future::ready(([("connection", "close")], ""));
        let app = axum::Router::new().route("/o", axum::routing::get(answer));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let burst = futures_util::future::join_all((0..50).map(|_| get(&http, port)));
        let serve = axum::serve(listener, app).into_future();
        let ended = futures_util::future::select(Box::pin(burst), Box::pin(serve)).await;
        let futures_util::future::Either::Left((answers, serving)) = ended else {
            panic!("the server ended first");
        };
        // Its listener closes with it: the next connect is refused.
        drop(serving);
        assert_eq!(answers, vec![Ok(200); 50]);
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            1,
            "one lookup for the burst"
        );
        // The server is gone: the cached address refuses, and the host is forgotten.
        assert_eq!(get(&http, port).await, Err(HttpErrorKind::Connect));
        assert_eq!(counted.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(get(&http, port).await, Err(HttpErrorKind::Connect));
        assert_eq!(
            counted.lookups.load(Ordering::Relaxed),
            2,
            "looked up after the failure"
        );
    }
}
