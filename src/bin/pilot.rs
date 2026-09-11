// Pilot harness, run 2: high-concurrency Rust replacements for the Bun
// load balancer and workload generator (which capped run 1 at ~256
// in-flight requests each — see EXPERIMENT-PILOT.md).
//
//   MODE=lb   rendezvous-hash reverse proxy + live dashboard
//   MODE=gen  closed-loop generator: concurrency doubles every RAMP_SECS
//
// The generator is closed-loop (workers issue the next request only after
// the previous completes), so offered load self-paces to what the fleet
// can absorb and congestion collapse is impossible by construction.

#[path = "pilot/client.rs"]
mod http_client;
use http_client::RotatingClient;
#[cfg(test)]
use http_client::client;

#[path = "pilot/proxy.rs"]
mod routing;

#[path = "pilot/benchmark.rs"]
mod benchmark;

#[path = "pilot/generator.rs"]
mod workload_generator;

#[path = "pilot/lb.rs"]
mod lb;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[expect(
    clippy::disallowed_methods,
    reason = "pilot process configuration; this independent workload binary owns its explicit environment inputs; routing them through server runtime state would couple separate executables"
)]
fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}
fn now_ms() -> u64 {
    epoch_millis(SystemTime::now())
}

fn epoch_millis(now: SystemTime) -> u64 {
    u64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

// Matches the JS FNV-1a in the run-1 Bun LB so stream→server pinning is
// unchanged across harnesses.
fn fnv1a(s: &str) -> u32 {
    let mut h: u32 = 2166136261;
    for b in s.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    h
}
/// Log an upstream transport error at most once a second: a dead
/// instance under load produces tens of thousands of identical errors.
fn tracing_warn_once(e: &reqwest::Error) {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = now_ms();
    let prev = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(prev) > 1000
        && LAST
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        eprintln!("lb: upstream transport error: {e}");
    }
}

/// How long an upstream stays locally ejected after an unmarked
/// (platform-edge) response. Long enough to cover a redeploy gap,
/// short enough that a revived instance rejoins quickly.
fn eject_ms() -> u64 {
    env("LB_EJECT_MS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000)
}

fn pick(stream: &str, upstreams: &[String]) -> usize {
    let mut best = 0usize;
    let mut best_score = 0u32;
    for (i, u) in upstreams.iter().enumerate() {
        let score = fnv1a(&format!("{stream} {u}"));
        if i == 0 || score > best_score {
            best_score = score;
            best = i;
        }
    }
    best
}

// ---- fleet view (COMPUTE-SPEC §2/§4): read desired.json + heartbeats ----

#[derive(Clone, Default)]
struct FleetView {
    desired: usize,
    /// Per-upstream-index: (rps, ack_p50_ms, live, cpu_pct) from heartbeats.
    heartbeats: Vec<(f64, f64, bool, f64)>,
    /// Ring active set (instance names): first `desired` ordinal instances
    /// minus any heartbeat-dark >30 s (mirrors the servers' R2 view), with
    /// an unfiltered fallback so a fully-asleep fleet still gets woken.
    active: Vec<String>,
    /// Shard bit-prefixes from the data namespace's topology.json.
    topology: Vec<String>,
    /// Rebalancer shard moves (fleet/overrides.json): prefix -> instance.
    /// Routing consults these before the rendezvous pick, mirroring the
    /// servers' effective_owner — without this every request for an
    /// override-owned shard takes a 409 Streams-Replay-To double hop
    /// (FLEET-CAMPAIGN.md: streams-3's entire steady-state load).
    overrides: std::collections::HashMap<String, String>,
}

#[expect(
    clippy::expect_used,
    reason = "pilot object-store startup; missing credentials or invalid store configuration must stop the selected workload; defaults could publish measurements to an unintended store"
)]
fn fleet_store(prefix: &str) -> Arc<dyn object_store::ObjectStore> {
    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(env("S3_ENDPOINT").expect("S3_ENDPOINT"))
        .with_bucket_name(env("S3_BUCKET").expect("S3_BUCKET"))
        .with_region(env("S3_REGION").unwrap_or_else(|| "auto".into()))
        .with_access_key_id(env("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID"))
        .with_secret_access_key(env("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY"))
        // Local fleets use plain-http object stores (s3lite); without
        // this every heartbeat/desired read fails silently and the LB
        // routes the whole fleet to instance 1 (docker staircase find).
        .with_allow_http(true)
        .build()
        .expect("s3 store");
    Arc::new(object_store::prefix::PrefixStore::new(s3, prefix))
}

/// The COLLECTION NAME a request targets, decoded, mirroring the
/// server's ProductRoute grammar. Names are hierarchical UTF-8
/// ("customers/acme/orders"), so hashing only the first path segment
/// routed every stream under a shared prefix to one instance and made
/// the 409-replay path carry traffic that should never have missed
/// (round-19 fleet-contract finding). Sub-resources and the known final
/// action verbs terminate the name; a ':' anywhere else is part of it.
fn collection_name(path: &str) -> Option<String> {
    const SUBRESOURCES: [&str; 5] = ["records", "consumers", "producers", "watches", "forks"];
    let rest = path
        .strip_prefix("/v1/streams/")
        .or_else(|| path.strip_prefix("/v1/stream/"))?;
    let rest = rest.split('?').next().unwrap_or(rest);
    // Trim the trailing action verb, if any: ".../name:seal".
    let (head, _verb) = match rest.rsplit_once(':') {
        Some((h, v)) if !v.contains('/') => (h, Some(v)),
        _ => (rest, None),
    };
    // Trim a trailing sub-resource path ("<name>/records", "<name>/
    // consumers/<c>"): everything from the FIRST segment that is a known
    // sub-resource keyword onward belongs to the route, not the name.
    let segs: Vec<&str> = head.split('/').collect();
    let end = segs
        .iter()
        .position(|s| SUBRESOURCES.contains(s))
        .unwrap_or(segs.len());
    let name = segs[..end].join("/");
    let name = percent_decode(&name);
    if name.is_empty() { None } else { Some(name) }
}

/// One-shot percent-decoding of a path (the SDK encodes each name
/// segment). '+' is NOT a space — same rule as the server.
fn percent_decode(v: &str) -> String {
    let b = v.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = |c: u8| match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                b'A'..=b'F' => Some(c - b'A' + 10),
                _ => None,
            };
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| v.to_string())
}

/// Mirrors crypto::stream_hash — shard choice keys off the stream name.
fn name_hash(name: &str) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let d = Sha256::digest(name.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&d[..16]);
    out
}

/// Mirrors registry::hash_bits + shard_for_hash (longest-prefix match).
fn shard_for(topology: &[String], hash: &[u8; 16]) -> String {
    let mut bits = String::with_capacity(24);
    for byte in hash.iter().take(3) {
        bits.push_str(&format!("{byte:08b}"));
    }
    topology
        .iter()
        .filter(|p| bits.starts_with(p.as_str()))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = env("MODE").unwrap_or_else(|| std::env::args().nth(1).unwrap_or_default());
    match mode.as_str() {
        "lb" => lb::run().await,
        "gen" => workload_generator::run().await?,
        "bench" => benchmark::run(env).await?,
        m => {
            eprintln!("unknown MODE '{m}' (want lb|gen)");
            std::process::exit(1);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- LB ----

#[derive(Default)]
struct UpStat {
    reqs: AtomicU64,
    errs: AtomicU64,
    window: AtomicU64,
    ewma_us: AtomicU64,
    last_us: AtomicU64,
    cold_starts: AtomicU64,
    last_seen_ms: AtomicU64,
    /// 409 Streams-Replay-To bounces THIS upstream issued — its ownership
    /// view disagreed with the router's pick. Nonzero steady-state means
    /// the router's override/topology view is stale.
    replays: AtomicU64,
    /// Unmarked (platform-edge) responses seen from this upstream: it is
    /// dead or unpublished, not answering as a Streams server.
    unmarked: AtomicU64,
    /// Local ejection deadline (ms since epoch). Set the instant an
    /// unmarked response arrives — the router must stop sending NEW
    /// requests here immediately rather than waiting out the ~30 s
    /// heartbeat-dark window (round-19 must-fix 4).
    eject_until_ms: AtomicU64,
}

struct Lb {
    /// Live per-ordinal upstream URLs. Seeded from UPSTREAMS; the fleet
    /// poller overwrites entries from fleet/urls.json so a replaced
    /// instance (new preview URL after redeploy) is reachable without
    /// redeploying this router — env-frozen URLs turn every instance
    /// replacement into a full redeploy cascade.
    upstreams: std::sync::RwLock<Vec<String>>,
    stats: Vec<UpStat>,
    history: Mutex<VecDeque<serde_json::Value>>,
    gen_stats: Mutex<serde_json::Value>,
    fleet: Mutex<FleetView>,
    http: RotatingClient,
}

// --------------------------------------------------------- dashboard ----

const DASH: &str = r##"<!doctype html><meta charset="utf-8"><title>Streams pilot</title>
<style>body{font:14px system-ui;background:#0b0e14;color:#d8dee9;margin:24px}h1{font-size:18px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}
.card{background:#141926;border-radius:8px;padding:12px}.big{font-size:26px;font-weight:600}
.dim{color:#7b8496;font-size:12px}canvas{width:100%;height:260px;background:#141926;border-radius:8px}
.slp{opacity:0.45}</style>
<h1>Prisma Streams pilot (run 3): fleet-coordinated autoscaling</h1>
<div class="dim" id="ramp">loading</div>
<div class="grid" id="cards"></div>
<canvas id="chart" width="1200" height="260"></canvas>
<div class="dim">req/s per server from fleet heartbeats (stacked), 15-minute window. The fleet publishes its own desired count (fleet/desired.json); the LB routes to only that many servers — the rest go stale and scale to zero.</div>
<script>
const colors=["#5e81ac","#a3be8c","#ebcb8b","#bf616a"];
async function tick(){
  const d=await (await fetch("/stats")).json();
  const g=d.gen||{};
  document.getElementById("ramp").textContent=
    "fleet desired: "+(d.desired??"?")+" of "+d.upstreams+" | gen concurrency: "+(g.concurrency??"n/a")+
    " | achieved: "+(g.achievedPerSec??"n/a")+"/s | win p50 "+(g.winP50Ms??0).toFixed(0)+
    "ms p99 "+(g.winP99Ms??0).toFixed(0)+"ms | errs "+(g.errs??0)+
    " | updated "+new Date().toLocaleTimeString();
  document.getElementById("cards").innerHTML=(d.heartbeats||[]).map(function(s,i){
    const active=i<(d.desired||1);
    const state=s.live?(active?"ACTIVE":"live, leaving ring"):"SLEEPING";
    return '<div class="card'+(s.live?"":" slp")+'"><div class="dim">server '+(i+1)+' — '+state+'</div><div class="big">'+
    (s.live?s.rps.toFixed(0):"0")+' req/s</div>'+
    '<div class="dim">'+(s.live?('ack p50 '+(s.ackMs||0).toFixed(0)+'ms'):'heartbeat stale (scaled to zero)')+'</div></div>';
  }).join("");
  const c=document.getElementById("chart").getContext("2d");
  c.clearRect(0,0,1200,260);
  const h=d.history; if(!h.length) return;
  let max=4;
  h.forEach(function(p){max=Math.max(max,(p.hb||[]).reduce(function(a,b){return a+b},0))});
  const w=1200/Math.max(900,h.length);
  h.forEach(function(p,x){
    let y=260;
    (p.hb||[]).forEach(function(v,i){const hh=v/max*250;c.fillStyle=colors[i];c.fillRect(x*w,y-hh,Math.max(1,w-0.5),hh);y-=hh;});
  });
}
setInterval(tick,1000);tick();
</script>"##;
