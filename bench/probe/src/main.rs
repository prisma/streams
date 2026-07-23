//! Tigris latency observatory (one instance per Prisma region).
//!
//! Every 10 s (solo mode): PUT + hot GET at 1 KB and 256 KB, plus a cold
//! GET of a fixed anchor object. Every 60 s: a fresh-connection GET
//! (coldconn mode — includes DNS/TCP/TLS). Once an hour: a 60 s burst of
//! 16 concurrent 256 KB PUTs (burst mode) so time-of-day effects can be
//! separated from load-correlated contention. Every sample is one row in
//! Prisma Postgres; the same binary serves the daily report page.
//!
//! Env: PROBE_REGION, DATABASE_URL (direct postgres://), SLATE_S3_ENDPOINT/
//! BUCKET/ACCESS_KEY_ID/SECRET_ACCESS_KEY, OTHER_REGIONS
//! ("region=url,region=url"), PORT.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::signer::Signer;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::mpsc;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

#[derive(Debug, Clone)]
struct Sample {
    ts: chrono::DateTime<chrono::Utc>,
    op: &'static str,   // put | get_hot | get_cold
    mode: &'static str, // solo | burst | coldconn
    size: i32,
    ms: f64,
    ok: bool,
    err: Option<String>,
    /// Tigris-internal time (Server-Timing: total;dur=...), when visible.
    server_ms: Option<f64>,
    /// X-Tigris-Served-From / X-Tigris-Regions, when visible.
    served_from: Option<String>,
    regions: Option<String>,
    /// Bucket variant under test: "global" (Prisma Bucket) or "pinned"
    /// (region-restricted bucket) — the 2026-07-22 comparison dimension.
    variant: &'static str,
}

fn s3_concrete() -> anyhow::Result<object_store::aws::AmazonS3> {
    Ok(object_store::aws::AmazonS3Builder::new()
        .with_endpoint(env("SLATE_S3_ENDPOINT").context("SLATE_S3_ENDPOINT")?)
        .with_bucket_name(env("SLATE_S3_BUCKET").context("SLATE_S3_BUCKET")?)
        .with_region(env("SLATE_S3_REGION").unwrap_or_else(|| "auto".into()))
        .with_access_key_id(env("SLATE_S3_ACCESS_KEY_ID").context("key id")?)
        .with_secret_access_key(env("SLATE_S3_SECRET_ACCESS_KEY").context("secret")?)
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_idle_timeout(Duration::from_secs(4)),
        )
        .build()?)
}

fn store_client() -> anyhow::Result<Arc<dyn ObjectStore>> {
    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(env("SLATE_S3_ENDPOINT").context("SLATE_S3_ENDPOINT")?)
        .with_bucket_name(env("SLATE_S3_BUCKET").context("SLATE_S3_BUCKET")?)
        .with_region(env("SLATE_S3_REGION").unwrap_or_else(|| "auto".into()))
        .with_access_key_id(env("SLATE_S3_ACCESS_KEY_ID").context("key id")?)
        .with_secret_access_key(env("SLATE_S3_SECRET_ACCESS_KEY").context("secret")?)
        // Production discipline: Compute kills flows idle ≳5 s.
        .with_client_options(
            object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_idle_timeout(Duration::from_secs(4)),
        )
        .build()?;
    Ok(Arc::new(s3))
}

async fn timed_put(store: &Arc<dyn ObjectStore>, path: &str, body: Bytes) -> (f64, bool, Option<String>) {
    let t0 = Instant::now();
    match store.put(&ObjPath::from(path), body.into()).await {
        Ok(_) => (t0.elapsed().as_secs_f64() * 1000.0, true, None),
        Err(e) => (t0.elapsed().as_secs_f64() * 1000.0, false, Some(e.to_string())),
    }
}

async fn timed_get(store: &Arc<dyn ObjectStore>, path: &str) -> (f64, bool, Option<String>) {
    let t0 = Instant::now();
    match store.get(&ObjPath::from(path)).await {
        Ok(r) => match r.bytes().await {
            Ok(_) => (t0.elapsed().as_secs_f64() * 1000.0, true, None),
            Err(e) => (t0.elapsed().as_secs_f64() * 1000.0, false, Some(e.to_string())),
        },
        Err(e) => (t0.elapsed().as_secs_f64() * 1000.0, false, Some(e.to_string())),
    }
}

fn body_of(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn parse_server_ms(headers: &reqwest::header::HeaderMap) -> Option<f64> {
    let v = headers.get("server-timing")?.to_str().ok()?;
    // "total;dur=247,cache;..." — take the first dur after "total;".
    let idx = v.find("total;dur=")?;
    v[idx + 10..]
        .split(|c: char| c == ',' || c == ';')
        .next()?
        .trim()
        .parse()
        .ok()
}

fn hdr(headers: &reqwest::header::HeaderMap, k: &str) -> Option<String> {
    headers.get(k).and_then(|v| v.to_str().ok()).map(String::from)
}

async fn signed_op(
    s3: &object_store::aws::AmazonS3,
    http: &reqwest::Client,
    method: reqwest::Method,
    path: &str,
    body: Option<Bytes>,
) -> (f64, bool, Option<String>, Option<f64>, Option<String>, Option<String>) {
    let sign_method = if method == reqwest::Method::PUT { http::Method::PUT } else { http::Method::GET };
    let url = match s3.signed_url(sign_method, &ObjPath::from(path), Duration::from_secs(300)).await {
        Ok(u) => u,
        Err(e) => return (0.0, false, Some(format!("sign: {e}")), None, None, None),
    };
    let t0 = Instant::now();
    let mut req = http.request(method, url);
    if let Some(b) = body {
        req = req.body(b);
    }
    match req.send().await {
        Ok(r) => {
            let server_ms = parse_server_ms(r.headers());
            let served = hdr(r.headers(), "x-tigris-served-from");
            let regions = hdr(r.headers(), "x-tigris-regions");
            let status = r.status();
            let body_ok = r.bytes().await.is_ok();
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            if status.is_success() && body_ok {
                (ms, true, None, server_ms, served, regions)
            } else {
                (ms, false, Some(format!("status {status}")), server_ms, served, regions)
            }
        }
        Err(e) => (t0.elapsed().as_secs_f64() * 1000.0, false, Some(e.to_string()), None, None, None),
    }
}

async fn probe_loop(store: Arc<dyn ObjectStore>, tx: mpsc::Sender<Sample>) {
    const KB: usize = 1024;
    const SIZES: [usize; 2] = [KB, 256 * KB];
    for s in SIZES {
        let p = format!("probe/anchor-{s}");
        let _ = store.put(&ObjPath::from(p), body_of(s).into()).await;
    }
    let s3 = match s3_concrete() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("signer client failed: {e}");
            return;
        }
    };
    let http = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(4))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    let mut n: u64 = 0;
    loop {
        tick.tick().await;
        n += 1;
        // Untimed warmup: the 4 s pool-idle rule guarantees a dead pool at
        // every 10 s tick, so without this the FIRST timed op absorbs
        // DNS+TCP+TLS (v4 data: FRA PUT "164 ms" was ~140 ms handshake +
        // 24 ms Tigris). Solo ops measure the warm path; coldconn mode
        // remains the explicit cold measurement.
        let _ = signed_op(&s3, &http, reqwest::Method::GET, "probe/anchor-1024", None).await;
        let now = chrono::Utc::now();
        for s in SIZES {
            let hot = format!("probe/current-{s}");
            let (ms, ok, err, server_ms, served_from, regions) =
                signed_op(&s3, &http, reqwest::Method::PUT, &hot, Some(body_of(s))).await;
            let _ = tx.send(Sample { ts: now, op: "put", mode: "solo", size: s as i32, ms, ok, err, server_ms, served_from, regions, variant: variant() }).await;
            let (ms, ok, err, server_ms, served_from, regions) =
                signed_op(&s3, &http, reqwest::Method::GET, &hot, None).await;
            let _ = tx.send(Sample { ts: now, op: "get_hot", mode: "solo", size: s as i32, ms, ok, err, server_ms, served_from, regions, variant: variant() }).await;
            let (ms, ok, err, server_ms, served_from, regions) =
                signed_op(&s3, &http, reqwest::Method::GET, &format!("probe/anchor-{s}"), None).await;
            let _ = tx.send(Sample { ts: now, op: "get_cold", mode: "solo", size: s as i32, ms, ok, err, server_ms, served_from, regions, variant: variant() }).await;
        }
        if n % 6 == 0 {
            if let Ok(fresh) = store_client() {
                let (ms, ok, err) = timed_get(&fresh, "probe/anchor-1024").await;
                let _ = tx
                    .send(Sample { ts: chrono::Utc::now(), op: "get_cold", mode: "coldconn", size: KB as i32, ms, ok, err, server_ms: None, served_from: None, regions: None, variant: variant() })
                    .await;
            }
        }
    }
}

/// Hourly 60 s burst: 16 workers looping 256 KB PUTs. Separates
/// load-correlated tails from time-of-day tails.
async fn burst_loop(store: Arc<dyn ObjectStore>, tx: mpsc::Sender<Sample>) {
    loop {
        // Sleep to the top of the next hour.
        let now = chrono::Utc::now();
        let secs_into = (now.timestamp() % 3600) as u64;
        tokio::time::sleep(Duration::from_secs(3600 - secs_into)).await;
        let until = Instant::now() + Duration::from_secs(60);
        let mut workers = Vec::new();
        for w in 0..16u32 {
            let store = store.clone();
            let tx = tx.clone();
            workers.push(tokio::spawn(async move {
                let mut i = 0u32;
                while Instant::now() < until {
                    i += 1;
                    let p = format!("probe/burst-{w}-{}", i % 4);
                    let (ms, ok, err) = timed_put(&store, &p, body_of(256 * 1024)).await;
                    let _ = tx
                        .send(Sample { ts: chrono::Utc::now(), op: "put", mode: "burst", size: 256 * 1024, ms, ok, err, server_ms: None, served_from: None, regions: None, variant: variant() })
                        .await;
                }
            }));
        }
        for w in workers {
            let _ = w.await;
        }
    }
}

// ---------------------------------------------------------------- PG ----

async fn pg_connect(url: &str) -> anyhow::Result<tokio_postgres::Client> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let (client, conn) = tokio_postgres::connect(url, tls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("pg connection ended: {e}");
        }
    });
    Ok(client)
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS probe (
  ts timestamptz NOT NULL,
  op text NOT NULL,
  mode text NOT NULL,
  size_bytes int NOT NULL,
  ms double precision NOT NULL,
  ok boolean NOT NULL,
  err text
);
CREATE INDEX IF NOT EXISTS probe_ts ON probe (ts);
ALTER TABLE probe ADD COLUMN IF NOT EXISTS server_ms double precision;
ALTER TABLE probe ADD COLUMN IF NOT EXISTS served_from text;
ALTER TABLE probe ADD COLUMN IF NOT EXISTS regions text;
ALTER TABLE probe ADD COLUMN IF NOT EXISTS variant text NOT NULL DEFAULT 'global';
";

async fn writer_loop(url: String, mut rx: mpsc::Receiver<Sample>) {
    let mut client: Option<tokio_postgres::Client> = None;
    let mut buf: Vec<Sample> = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_secs(3));
    loop {
        tokio::select! {
            s = rx.recv() => {
                match s { Some(s) => buf.push(s), None => return }
            }
            _ = tick.tick() => {
                if buf.is_empty() { continue; }
                if client.is_none() {
                    match pg_connect(&url).await {
                        Ok(c) => {
                            if let Err(e) = c.batch_execute(SCHEMA).await {
                                eprintln!("schema: {e}");
                            }
                            client = Some(c);
                        }
                        Err(e) => { eprintln!("pg connect: {e}"); continue; }
                    }
                }
                let c = client.as_ref().unwrap();
                // One multi-row INSERT per flush.
                let mut q = String::from("INSERT INTO probe (ts, op, mode, size_bytes, ms, ok, err, server_ms, served_from, regions, variant) VALUES ");
                let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
                let rows: Vec<Sample> = buf.drain(..).collect();
                for (i, s) in rows.iter().enumerate() {
                    if i > 0 { q.push(','); }
                    let b = i * 11;
                    q.push_str(&format!("(${},${},${},${},${},${},${},${},${},${},${})", b+1,b+2,b+3,b+4,b+5,b+6,b+7,b+8,b+9,b+10,b+11));
                    params.push(&s.ts); params.push(&s.op); params.push(&s.mode);
                    params.push(&s.size); params.push(&s.ms); params.push(&s.ok); params.push(&s.err);
                    params.push(&s.server_ms); params.push(&s.served_from); params.push(&s.regions);
                    params.push(&s.variant);
                }
                if let Err(e) = c.execute(q.as_str(), &params).await {
                    eprintln!("insert failed ({} rows): {e}", rows.len());
                    client = None; // reconnect next flush; rows dropped (lossy by design)
                }
            }
        }
    }
}

// -------------------------------------------------------------- page ----

struct App {
    region: String,
    others: Vec<(String, String)>,
    db_url: String,
}

async fn data(State(app): State<Arc<App>>, Query(q): Query<std::collections::HashMap<String, String>>) -> Response {
    let day = q
        .get("day")
        .cloned()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());
    let client = match pg_connect(&app.db_url).await {
        Ok(c) => c,
        Err(e) => return (axum::http::StatusCode::SERVICE_UNAVAILABLE, format!("pg: {e}")).into_response(),
    };
    let sql = "
      SELECT date_trunc('hour', ts) AS h, op, mode, size_bytes, variant,
             count(*) AS n,
             count(*) FILTER (WHERE NOT ok) AS errs,
             percentile_cont(0.5)  WITHIN GROUP (ORDER BY ms) AS p50,
             percentile_cont(0.9)  WITHIN GROUP (ORDER BY ms) AS p90,
             percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99,
             max(ms) AS mx,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY server_ms) AS sp50,
             percentile_cont(0.99) WITHIN GROUP (ORDER BY server_ms) AS sp99
      FROM probe
      WHERE ts >= $1::text::date AND ts < $1::text::date + interval '1 day' AND ok
      GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5";
    let rows = match client.query(sql, &[&day]).await {
        Ok(r) => r,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")).into_response(),
    };
    let mut out = Vec::new();
    for r in rows {
        let h: chrono::DateTime<chrono::Utc> = r.get(0);
        out.push(serde_json::json!({
            "hour": h.format("%H").to_string(),
            "op": r.get::<_, String>(1),
            "mode": r.get::<_, String>(2),
            "size": r.get::<_, i32>(3),
            "variant": r.get::<_, String>(4),
            "n": r.get::<_, i64>(5),
            "errs": r.get::<_, i64>(6),
            "p50": r.get::<_, f64>(7),
            "p90": r.get::<_, f64>(8),
            "p99": r.get::<_, f64>(9),
            "max": r.get::<_, f64>(10),
            "sp50": r.get::<_, Option<f64>>(11),
            "sp99": r.get::<_, Option<f64>>(12),
        }));
    }
    axum::Json(serde_json::json!({
        "region": app.region,
        "day": day,
        "others": app.others.iter().map(|(n, u)| serde_json::json!({"name": n, "url": u})).collect::<Vec<_>>(),
        "rows": out,
    }))
    .into_response()
}

const PAGE: &str = include_str!("page.html");

// ---------------------------------------------------- v7: /diag ----------
// Network-level decomposition for the Tigris routing question. microVMs
// have no ICMP, so everything is TCP/TLS/HTTP: DNS answer set, raw TCP
// connect and rustls handshake per resolved IP (3 rounds each), full
// header echo (fly-request-id names the edge PoP; x-amz-request-id lets
// Tigris find the request), egress IP (what their edge sees as source),
// plus 24 h served_from / Server-Timing summaries straight from PG.

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

fn tls_probe_sync(host: &str) -> serde_json::Value {
    use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
    let t0 = Instant::now();
    let addrs: Vec<SocketAddr> = match (host, 443u16).to_socket_addrs() {
        Ok(a) => a.collect(),
        Err(e) => return serde_json::json!({"host": host, "dns_err": e.to_string()}),
    };
    let dns_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let mut ips: Vec<std::net::IpAddr> = addrs.iter().map(|a| a.ip()).collect();
    ips.dedup();
    ips.truncate(4);
    let cfg = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            })
            .with_no_client_auth(),
    );
    let mut per_ip = Vec::new();
    for ip in &ips {
        let mut tcp_ms = Vec::new();
        let mut tls_ms = Vec::new();
        let mut err: Option<String> = None;
        for _ in 0..3 {
            let t1 = Instant::now();
            match TcpStream::connect_timeout(&SocketAddr::new(*ip, 443), Duration::from_secs(5)) {
                Ok(mut s) => {
                    tcp_ms.push(round1(t1.elapsed().as_secs_f64() * 1000.0));
                    let _ = s.set_nodelay(true);
                    let sn = match rustls::pki_types::ServerName::try_from(host.to_string()) {
                        Ok(sn) => sn,
                        Err(e) => {
                            err = Some(e.to_string());
                            continue;
                        }
                    };
                    let t2 = Instant::now();
                    match rustls::ClientConnection::new(cfg.clone(), sn) {
                        Ok(mut conn) => {
                            let mut ok = true;
                            while conn.is_handshaking() {
                                if let Err(e) = conn.complete_io(&mut s) {
                                    err = Some(format!("tls: {e}"));
                                    ok = false;
                                    break;
                                }
                            }
                            if ok {
                                tls_ms.push(round1(t2.elapsed().as_secs_f64() * 1000.0));
                            }
                        }
                        Err(e) => err = Some(e.to_string()),
                    }
                }
                Err(e) => err = Some(format!("tcp: {e}")),
            }
        }
        per_ip.push(serde_json::json!({
            "ip": ip.to_string(), "tcp_ms": tcp_ms, "tls_ms": tls_ms, "err": err,
        }));
    }
    serde_json::json!({"host": host, "dns_ms": round1(dns_ms), "ips": per_ip})
}

// ---- v8: DNS identity (GeoDNS debugging with the Tigris team) ----------
// Hand-rolled DNS TXT query (UDP, no new deps): o-o.myaddr.l.google.com
// echoes the SOURCE IP Google's nameserver sees. Sent directly to
// ns1.google.com it reveals this VM's public egress; sent to the local
// resolv.conf nameserver it reveals the RECURSIVE RESOLVER's public IP —
// the address GeoDNS databases actually geolocate.

fn dns_txt_query(server: std::net::SocketAddr, name: &str) -> Result<Vec<String>, String> {
    use std::net::UdpSocket;
    let mut q = Vec::with_capacity(64);
    q.extend_from_slice(&[0x13, 0x37, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in name.trim_end_matches('.').split('.') {
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.push(0);
    q.extend_from_slice(&[0, 16, 0, 1]); // QTYPE TXT, QCLASS IN
    let sock = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    sock.send_to(&q, server).map_err(|e| e.to_string())?;
    let mut buf = [0u8; 1024];
    let (n, _) = sock.recv_from(&mut buf).map_err(|e| e.to_string())?;
    let b = &buf[..n];
    if n < 12 || b[0] != 0x13 || b[1] != 0x37 {
        return Err("bad dns response".into());
    }
    let ancount = u16::from_be_bytes([b[6], b[7]]) as usize;
    // skip question section
    let mut i = 12;
    while i < n && b[i] != 0 {
        i += 1 + b[i] as usize;
    }
    i += 5; // null + qtype + qclass
    let mut out = Vec::new();
    for _ in 0..ancount {
        if i + 12 > n {
            break;
        }
        // NAME: either pointer (0xc0..) or labels
        if b[i] & 0xc0 == 0xc0 {
            i += 2;
        } else {
            while i < n && b[i] != 0 {
                i += 1 + b[i] as usize;
            }
            i += 1;
        }
        if i + 10 > n {
            break;
        }
        let rtype = u16::from_be_bytes([b[i], b[i + 1]]);
        let rdlen = u16::from_be_bytes([b[i + 8], b[i + 9]]) as usize;
        i += 10;
        if rtype == 16 && i + rdlen <= n {
            let mut j = i;
            while j < i + rdlen {
                let l = b[j] as usize;
                j += 1;
                if j + l <= n {
                    out.push(String::from_utf8_lossy(&b[j..j + l]).to_string());
                }
                j += l;
            }
        }
        i += rdlen;
    }
    Ok(out)
}

fn dns_identity() -> serde_json::Value {
    let resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    let local_ns: Option<std::net::IpAddr> = resolv
        .lines()
        .find_map(|l| l.trim().strip_prefix("nameserver ").map(str::trim).and_then(|s| s.parse().ok()));
    // Resolve ns1.google.com via the system resolver.
    let google_ns: Option<std::net::SocketAddr> = {
        use std::net::ToSocketAddrs;
        ("ns1.google.com", 53u16)
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.find(|s| s.is_ipv4()))
    };
    let direct = google_ns
        .map(|s| dns_txt_query(s, "o-o.myaddr.l.google.com"))
        .unwrap_or_else(|| Err("ns1.google.com unresolvable".into()));
    let via_resolver = local_ns
        .map(|ip| dns_txt_query(std::net::SocketAddr::new(ip, 53), "o-o.myaddr.l.google.com"))
        .unwrap_or_else(|| Err("no nameserver in resolv.conf".into()));
    serde_json::json!({
        "resolv_conf": resolv,
        "ns1_google": google_ns.map(|s| s.ip().to_string()),
        "myaddr_direct_at_ns1_google": match direct { Ok(v) => serde_json::json!(v), Err(e) => serde_json::json!({"err": e}) },
        "myaddr_via_local_resolver": match via_resolver { Ok(v) => serde_json::json!(v), Err(e) => serde_json::json!({"err": e}) },
    })
}

const ECHO_HEADERS: [&str; 8] = [
    "server",
    "via",
    "fly-request-id",
    "x-amz-request-id",
    "x-tigris-served-from",
    "x-tigris-regions",
    "server-timing",
    "date",
];

async fn diag(State(app): State<Arc<App>>) -> Response {
    let endpoint_host = env("SLATE_S3_ENDPOINT")
        .unwrap_or_default()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let bucket = env("SLATE_S3_BUCKET").unwrap_or_default();
    let bucket_host = format!("{bucket}.{endpoint_host}");

    // What Tigris's edge sees as our source address.
    let egress_ip = async {
        let c = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        let t = c
            .get("https://checkip.amazonaws.com")
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        Some(t.trim().to_string())
    }
    .await;

    // DNS/TCP/TLS per resolved IP, both hosts.
    let eh = endpoint_host.clone();
    let bh = bucket_host.clone();
    let tcp_tls = tokio::task::spawn_blocking(move || {
        serde_json::json!([tls_probe_sync(&eh), tls_probe_sync(&bh)])
    })
    .await
    .unwrap_or_else(|e| serde_json::json!({"join_err": e.to_string()}));

    // Three authenticated GETs of the 1 KB anchor on ONE fresh client:
    // round 1 pays the full connection setup, rounds 2-3 ride the warm
    // conn. Header echo on every round (request ids differ per round).
    let mut http_rounds = Vec::new();
    if let Ok(s3) = s3_concrete() {
        if let Ok(url) = s3
            .signed_url(
                http::Method::GET,
                &ObjPath::from("probe/anchor-1024"),
                Duration::from_secs(300),
            )
            .await
        {
            if let Ok(client) = reqwest::Client::builder()
                .tcp_nodelay(true)
                .timeout(Duration::from_secs(15))
                .build()
            {
                for round in 0..3u8 {
                    let t0 = Instant::now();
                    match client.get(url.clone()).send().await {
                        Ok(r) => {
                            let status = r.status().as_u16();
                            let mut headers = serde_json::Map::new();
                            for k in ECHO_HEADERS {
                                if let Some(v) = hdr(r.headers(), k) {
                                    headers.insert(k.into(), serde_json::json!(v));
                                }
                            }
                            let _ = r.bytes().await;
                            http_rounds.push(serde_json::json!({
                                "round": round,
                                "conn": if round == 0 { "cold" } else { "warm" },
                                "total_ms": round1(t0.elapsed().as_secs_f64() * 1000.0),
                                "status": status,
                                "headers": headers,
                            }));
                        }
                        Err(e) => http_rounds.push(serde_json::json!({
                            "round": round, "err": e.to_string(),
                        })),
                    }
                }
            }
        }
    }

    // v8: DNS identity for the GeoDNS investigation (blocking: UDP + fs).
    let dns = tokio::task::spawn_blocking(dns_identity)
        .await
        .unwrap_or_else(|e| serde_json::json!({"err": e.to_string()}));

    // v8: in-region PUT sweep — Tigris-internal write time measured from
    // THIS VM (presigned PUTs on a warm client), per size. This is the
    // ground truth for "is <region> write latency still elevated".
    let mut put_sweep = serde_json::Map::new();
    if let Ok(s3) = s3_concrete() {
        if let Ok(client) = reqwest::Client::builder()
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(20))
            .build()
        {
            // warm the connection
            let _ = signed_op(&s3, &client, reqwest::Method::GET, "probe/anchor-1024", None).await;
            for (label, size) in [("put_1k", 1024usize), ("put_256k", 262144)] {
                let mut internal: Vec<f64> = Vec::new();
                let mut wall: Vec<f64> = Vec::new();
                for i in 0..10 {
                    let p = format!("probe/diag-sweep-{}", i % 4);
                    let (ms, ok, _e, server, _sf, _rg) = signed_op(
                        &s3,
                        &client,
                        reqwest::Method::PUT,
                        &p,
                        Some(body_of(size)),
                    )
                    .await;
                    if ok {
                        wall.push(ms);
                        if let Some(s) = server {
                            internal.push(s);
                        }
                    }
                }
                internal.sort_by(|a, b| a.partial_cmp(b).unwrap());
                wall.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let med = |v: &Vec<f64>| v.get(v.len() / 2).copied().unwrap_or(-1.0);
                put_sweep.insert(
                    label.into(),
                    serde_json::json!({
                        "n": internal.len(),
                        "internal_p50_ms": med(&internal),
                        "internal_max_ms": internal.last().copied().unwrap_or(-1.0),
                        "wall_p50_ms": med(&wall),
                        "internal_all": internal,
                    }),
                );
            }
        }
    }

    // 24 h summaries from PG: where GETs were served from, and Tigris's
    // own per-op internal time, per variant.
    let mut served_from = serde_json::json!(null);
    let mut server_ms = serde_json::json!(null);
    let mut pg_err = serde_json::json!(null);
    match pg_connect(&app.db_url).await {
        Err(e) => pg_err = serde_json::json!(e.to_string()),
        Ok(pg) => {
        if let Ok(rows) = pg
            .query(
                "SELECT op, served_from, count(*) FROM probe \
                 WHERE ts > now() - interval '24 hours' AND served_from IS NOT NULL \
                 GROUP BY 1, 2 ORDER BY 1, 3 DESC",
                &[],
            )
            .await
        {
            let v: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "op": r.get::<_, String>(0),
                        "served_from": r.get::<_, String>(1),
                        "n": r.get::<_, i64>(2),
                    })
                })
                .collect();
            served_from = serde_json::json!(v);
        }
        if let Ok(rows) = pg
            .query(
                "SELECT op, size_bytes, variant, count(*), \
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY server_ms), \
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY server_ms) \
                 FROM probe \
                 WHERE ts > now() - interval '24 hours' AND server_ms IS NOT NULL \
                       AND mode = 'solo' AND ok \
                 GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
                &[],
            )
            .await
        {
            let v: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "op": r.get::<_, String>(0),
                        "size": r.get::<_, i32>(1),
                        "variant": r.get::<_, String>(2),
                        "n": r.get::<_, i64>(3),
                        "sp50_ms": round1(r.get::<_, f64>(4)),
                        "sp99_ms": round1(r.get::<_, f64>(5)),
                    })
                })
                .collect();
            server_ms = serde_json::json!(v);
        }
        }
    }

    axum::Json(serde_json::json!({
        "region": app.region,
        "ts": chrono::Utc::now().to_rfc3339(),
        "endpoint": endpoint_host,
        "bucket_host": bucket_host,
        "egress_ip": egress_ip,
        "tcp_tls": tcp_tls,
        "http_rounds": http_rounds,
        "served_from_24h": served_from,
        "server_ms_24h": server_ms,
        "pg_err": pg_err,
        "dns": dns,
        "put_sweep": put_sweep,
    }))
    .into_response()
}

async fn page() -> Response {
    ([("content-type", "text/html; charset=utf-8"), ("cache-control", "no-store")], PAGE).into_response()
}

fn variant() -> &'static str {
    // Leaked once at startup; constant for the process lifetime.
    static V: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();
    V.get_or_init(|| {
        Box::leak(
            env("PROBE_VARIANT")
                .unwrap_or_else(|| "global".into())
                .into_boxed_str(),
        )
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Two rustls crypto providers end up in the dep tree (ring here,
    // aws-lc via reqwest's stack); 0.23 panics on ambiguity unless one is
    // installed process-wide.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let region = env("PROBE_REGION").unwrap_or_else(|| "unknown".into());
    let db_url = env("DATABASE_URL").context("DATABASE_URL")?;
    let others: Vec<(String, String)> = env("OTHER_REGIONS")
        .unwrap_or_default()
        .split(',')
        .filter_map(|kv| kv.split_once('=').map(|(a, b)| (a.to_string(), b.to_string())))
        .collect();

    let store = store_client()?;
    let (tx, rx) = mpsc::channel::<Sample>(65_536);
    tokio::spawn(writer_loop(db_url.clone(), rx));
    tokio::spawn(probe_loop(store.clone(), tx.clone()));
    tokio::spawn(burst_loop(store, tx));

    let app = Arc::new(App { region, others, db_url });
    let router = axum::Router::new()
        .route("/", get(page))
        .route("/data", get(data))
        .route("/diag", get(diag))
        .with_state(app);
    let port = env("PORT").unwrap_or_else(|| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    eprintln!("tigris-probe serving :{port}");
    axum::serve(listener, router).await?;
    Ok(())
}
