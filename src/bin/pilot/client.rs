//! HTTP pool lifetime for the pilot workload. Each access renews an expired
//! pool before returning a client. Idle owners need no refresh task, and
//! outstanding requests retain their own clones of the previous pool.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const POOL_LIFETIME: Duration = Duration::from_secs(60);

struct Pool {
    client: reqwest::Client,
    renewed: Instant,
}

#[derive(Clone)]
pub(super) struct RotatingClient(Arc<Mutex<Pool>>);

impl RotatingClient {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(Pool {
            client: client(),
            renewed: Instant::now(),
        })))
    }

    #[expect(
        clippy::unwrap_used,
        reason = "pilot pool publication; a poisoned lock may cover partial renewal state; continuing with that state would invalidate the workload's connection lifetime"
    )]
    pub(super) fn get(&self) -> reqwest::Client {
        let mut pool = self.0.lock().unwrap();
        if pool.renewed.elapsed() >= POOL_LIFETIME {
            pool.client = client();
            pool.renewed = Instant::now();
        }
        pool.client.clone()
    }
}

#[expect(
    clippy::unwrap_used,
    reason = "pilot HTTP construction; the fixed TLS and connection-pool configuration must initialize before measurements run; silently substituting another client would change the workload"
)]
pub(super) fn client() -> reqwest::Client {
    reqwest::Client::builder()
        // http1_only is load-bearing: the platform edge negotiates h2 via
        // ALPN, and h2 multiplexes everything over ONE TCP connection per
        // host (bounded by the server's max-concurrent-streams and pinned
        // to a single LB replica) — measured throughput FELL as workers
        // doubled. HTTP/1.1 with a big pool gets one connection per
        // in-flight request and spreads across replicas.
        .http1_only()
        .pool_max_idle_per_host(8192)
        // <5 s: Compute suspends idle VMs after ~5 s and silently kills
        // flows; a pooled socket idle past that is a corpse the next
        // request eats. Same rule as the server's store client (RUNBOOK
        // §3.1). The 60 s client rotation handles replica pinning; this
        // handles dead sockets.
        .pool_idle_timeout(Duration::from_secs(4))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::{POOL_LIFETIME, RotatingClient};
    use std::time::Instant;

    #[tokio::test]
    async fn renewal_uses_a_new_connection_and_keeps_in_flight_clients_usable() {
        use axum::{Router, extract::ConnectInfo, routing::get};
        use std::net::SocketAddr;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/",
            get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.to_string() }),
        );
        #[expect(
            clippy::disallowed_methods,
            reason = "pilot pool regression; the loopback server is aborted and joined after connection assertions; concurrent serving is required to exercise real keep-alive reuse"
        )]
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let owner = RotatingClient::new();
        let original = owner.get();
        let url = format!("http://{address}/");
        let first = original
            .get(&url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        owner.0.lock().unwrap().renewed = Instant::now();
        let reused = owner
            .get()
            .get(&url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            first, reused,
            "an unexpired pool should reuse its connection"
        );
        owner.0.lock().unwrap().renewed = Instant::now().checked_sub(POOL_LIFETIME).unwrap();
        let renewed = owner
            .get()
            .get(&url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_ne!(
            first, renewed,
            "an expired owner must establish a fresh connection"
        );
        let retained = original
            .get(&url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            first, retained,
            "already captured clients remain usable across renewal"
        );
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn a_partial_pool_update_is_not_reused_after_poisoning() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let owner = RotatingClient::new();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let mut pool = owner.0.lock().unwrap();
                pool.renewed = Instant::now().checked_sub(POOL_LIFETIME).unwrap();
                panic!("partial renewal");
            }))
            .is_err()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| owner.get())).is_err());
    }

    #[tokio::test]
    async fn dropping_the_final_owner_releases_the_pool_without_a_background_task() {
        let owner = RotatingClient::new();
        let weak = std::sync::Arc::downgrade(&owner.0);
        let other = owner.clone();
        drop(owner);
        assert!(weak.upgrade().is_some());
        drop(other);
        assert!(weak.upgrade().is_none());
    }
}
