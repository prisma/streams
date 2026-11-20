#![cfg(test)]
//! The store requests one instance holds in flight around a split, an open and a pile, and the
//! count bound on them (Addenda 9, 10, 13, 16 and 17 of the forward-wedge ruling, 2026-11-20).
//! Scouting legs saw 561 to 2,400 concurrent store connections after a split's publish, 2,600 to
//! 4,300 about 4 to 5 s after every open (once with 188 sheds), and 1,000 to 3,000 with no event at
//! all, while the instance's own requests in flight stayed in the tens to low hundreds. Each
//! fan-out is bounded (64 cold loads an engine, 64 tail GETs an open, 16 run reads a split); the
//! instance's total was not, until the count bound in `StoreResources`.
//!
//! The gauge sits beneath the production store wrapper (`TimingStore`) on `split_pause`'s parent,
//! cold, on a store 20 ms away. It measures, by kind and path and beside the instance's own
//! requests in flight: windows around a split (to the park, the pause, the publish to every
//! writer's first ack) and a move-in open (to every writer's first ack) under many writers; a
//! half-second timeline for 10 s after each open and 10 s with no event; and the call sites of the
//! requests in each. Its one asserted test is the count bound's: a pile of 2,500 distinct expired
//! streams touched at once after a move-in stays within the bound, with the commit path never
//! waiting. A measurement reports a split's pause with a pile in flight.

use super::fixture_http::{HttpRig, engine_shutdown};
use super::fixture_requests::hreq;
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::store_timing::{COMMIT_PERMITS, GENERAL_PERMITS, StoreResources, TimingStore, classify};
use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What a request is and the objects it touches: its kind (`GET`, `HEAD`, `PUT`, `LIST`, `COPY`,
/// `DELETE`) and its path with every component that names one object or shard replaced by `*`.
type Key = (&'static str, String);

/// Reads the instance's own requests in flight now (`AdmissionController::active`, the count the
/// heartbeat reports).
type RequestsNow = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Requests begun while more than this many were in flight have their call stack sampled.
const SAMPLE_ABOVE: usize = 32;

/// Below `SAMPLE_ABOVE`, one request in this many is sampled.
const SAMPLE_EVERY: u64 = 8;

/// The most call stacks sampled in one measurement.
const MAX_SAMPLES: usize = 8_000;

/// The timeline's step, and how long it follows an event.
const STEP: Duration = Duration::from_millis(500);
const FOLLOW: Duration = Duration::from_secs(10);

/// One sampled request: when it began, its key, and its call stack, unresolved.
type Sample = (Instant, Key, std::backtrace::Backtrace);

/// The requests in flight now, and the highest total seen since the window opened, with the
/// requests in flight at that moment, the instance's own requests then, and each key's highest.
#[derive(Debug, Default)]
struct Counts {
    now: HashMap<Key, usize>,
    total: usize,
    peak: usize,
    at_peak: Vec<(Key, usize)>,
    requests_at_peak: i64,
    key_peaks: HashMap<Key, usize>,
    /// The general class's requests in flight now (every class but the WAL's, the manifest's and
    /// the fleet's, as the count bound pools them), and its highest since the window opened.
    general: usize,
    general_peak: usize,
}

/// One window's peak: the highest total, what was in flight then, the instance's own requests
/// then, and each key's highest.
#[derive(Debug)]
struct Peak {
    total: usize,
    at_peak: Vec<(Key, usize)>,
    requests_at_peak: i64,
    key_peaks: Vec<(Key, usize)>,
    /// The general class's highest.
    general: usize,
}

impl Peak {
    /// The peak and its largest contributors, at most `top` of each.
    fn shown(&self, top: usize) -> String {
        format!(
            "peak {} (general class {}) with {} requests in flight [at the peak: {}] [each key's \
             highest: {}]",
            self.total,
            self.general,
            self.requests_at_peak,
            rows(&self.at_peak, top),
            rows(&self.key_peaks, top)
        )
    }
}

/// `rows`, at most `top`, as `KIND path n`.
fn rows(rows: &[(Key, usize)], top: usize) -> String {
    rows.iter()
        .take(top)
        .map(|((kind, path), n)| format!("{kind} {path} {n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Counts the requests in flight through a `GaugeStore`, by window and by quarter second, and
/// samples their call stacks.
#[derive(Default)]
struct Gauge {
    counts: Mutex<Counts>,
    samples: Mutex<Vec<Sample>>,
    requests: Mutex<Option<RequestsNow>>,
    /// Each quarter second's peak since the measurement began: the store requests in flight and
    /// the instance's own requests then.
    quarters: Mutex<HashMap<u64, (usize, i64)>>,
    began: Mutex<Option<Instant>>,
    begun: AtomicU64,
}

impl std::fmt::Debug for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Gauge")
    }
}

impl Gauge {
    fn begin(self: &Arc<Self>, kind: &'static str, path: &str) -> InFlight {
        let key = (kind, shape(path));
        // A listing's class is its objects' (`TimingStore`'s `listed_class`).
        let listed = if kind == "LIST" { "/" } else { "" };
        let general = !matches!(classify(&format!("{path}{listed}")), 0 | 1 | 3);
        let total = self.count(&key, general);
        self.quarter(total);
        let begun = self.begun.fetch_add(1, Ordering::Relaxed);
        if total > SAMPLE_ABOVE || begun.is_multiple_of(SAMPLE_EVERY) {
            self.sample(&key);
        }
        InFlight {
            gauge: self.clone(),
            key: Some(key),
            general,
        }
    }

    /// Counts `key` in, of the general class or not: the total in flight now.
    fn count(&self, key: &Key, general: bool) -> usize {
        let mut counts = self.counts.lock().unwrap();
        if general {
            counts.general += 1;
            counts.general_peak = counts.general_peak.max(counts.general);
        }
        let now = {
            let n = counts.now.entry(key.clone()).or_insert(0);
            *n += 1;
            *n
        };
        counts.total += 1;
        let peak = counts.key_peaks.entry(key.clone()).or_insert(0);
        *peak = (*peak).max(now);
        if counts.total > counts.peak {
            counts.peak = counts.total;
            counts.at_peak = sorted(&counts.now);
            counts.requests_at_peak = self.requests_now();
        }
        counts.total
    }

    /// Raises this quarter second's peak to `total` (once the measurement has begun).
    fn quarter(&self, total: usize) {
        let Some(began) = *self.began.lock().unwrap() else {
            return;
        };
        let at = u64::try_from(began.elapsed().as_millis() / 250).unwrap();
        let mut quarters = self.quarters.lock().unwrap();
        let peak = quarters.entry(at).or_insert((0, 0));
        if total > peak.0 {
            *peak = (total, self.requests_now());
        }
    }

    fn requests_now(&self) -> i64 {
        self.requests
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |now| now())
    }

    /// Keeps the call stack of a request begun now, up to `MAX_SAMPLES`, once the measurement
    /// has begun.
    fn sample(&self, key: &Key) {
        let began = self.began.lock().unwrap().is_some();
        if !began || self.samples.lock().unwrap().len() >= MAX_SAMPLES {
            return;
        }
        let stack = std::backtrace::Backtrace::force_capture();
        let sample = (Instant::now(), key.clone(), stack);
        self.samples.lock().unwrap().push(sample);
    }

    fn end(&self, key: &Key, general: bool) {
        let mut counts = self.counts.lock().unwrap();
        if general {
            counts.general -= 1;
        }
        if let Some(n) = counts.now.get_mut(key) {
            *n -= 1;
        }
        counts.total -= 1;
    }

    /// Starts the measurement: windows, quarters and samples count from now.
    fn start(&self) -> Instant {
        let now = Instant::now();
        *self.began.lock().unwrap() = Some(now);
        drop(self.window());
        now
    }

    /// The window's peak; the next window opens at what is in flight now.
    fn window(&self) -> Peak {
        let mut counts = self.counts.lock().unwrap();
        let peak = Peak {
            total: counts.peak,
            at_peak: std::mem::take(&mut counts.at_peak),
            requests_at_peak: counts.requests_at_peak,
            key_peaks: sorted(&counts.key_peaks),
            general: counts.general_peak,
        };
        counts.peak = counts.total;
        counts.general_peak = counts.general;
        counts.at_peak = sorted(&counts.now);
        counts.requests_at_peak = self.requests_now();
        counts.key_peaks = counts.now.clone();
        peak
    }

    /// The highest quarter in `[from, from + span)`: the store requests in flight and the
    /// instance's own requests then.
    fn peak_in(&self, from: Instant, span: Duration) -> (usize, i64) {
        let Some(began) = *self.began.lock().unwrap() else {
            return (0, 0);
        };
        let quarter = |at: Instant| u64::try_from(at.duration_since(began).as_millis() / 250);
        let (first, last) = (quarter(from).unwrap(), quarter(from + span).unwrap());
        let quarters = self.quarters.lock().unwrap();
        (first..last)
            .filter_map(|q| quarters.get(&q).copied())
            .max_by_key(|(total, _)| *total)
            .unwrap_or((0, 0))
    }
}

/// One sampled request, resolved: when it began, its key and its call site.
type Site = (Instant, Key, String);

/// `samples`, each with its call site resolved (`call_site`).
fn resolved(samples: Vec<Sample>) -> Vec<Site> {
    samples
        .into_iter()
        .map(|(at, key, stack)| (at, key, call_site(&stack.to_string())))
        .collect()
}

/// The call sites of the sampled requests begun in `[from, from + span)`, most frequent first, at
/// most `top`, each with how many began there and the kinds and paths they touched.
fn sites_in(sites: &[Site], from: Instant, span: Duration, top: usize) -> String {
    let mut counted: HashMap<&str, (usize, HashMap<Key, usize>)> = HashMap::new();
    let within = sites
        .iter()
        .filter(|(at, _, _)| *at >= from && *at < from + span);
    for (_, key, site) in within {
        let entry = counted.entry(site.as_str()).or_default();
        entry.0 += 1;
        *entry.1.entry(key.clone()).or_insert(0) += 1;
    }
    let mut ordered: Vec<_> = counted.into_iter().collect();
    ordered.sort_by(|a, b| b.1.0.cmp(&a.1.0).then_with(|| a.0.cmp(b.0)));
    ordered
        .into_iter()
        .take(top)
        .map(|(site, (n, keys))| format!("{n} at {site} ({})", rows(&sorted(&keys), 3)))
        .collect::<Vec<_>>()
        .join("; ")
}

/// The half-second timeline from `from` for `FOLLOW`: each step's highest store requests in
/// flight, the instance's own requests then, and its two most frequent sampled call sites.
fn timeline(gauge: &Gauge, sites: &[Site], from: Instant) -> String {
    let steps = u32::try_from(FOLLOW.as_millis() / STEP.as_millis()).unwrap();
    (0..steps)
        .map(|k| {
            let at = from + STEP * k;
            let (peak, requests) = gauge.peak_in(at, STEP);
            let offset = (STEP * k).as_secs_f64();
            format!(
                "\n    +{offset:4.1}s {peak:5} in flight, {requests:4} requests: {}",
                sites_in(sites, at, STEP, 2)
            )
        })
        .collect()
}

/// The innermost four frames of `stack` (a resolved backtrace) in this crate or SlateDB, each
/// without its hash and closure suffixes, outside the gauge and the fault store.
fn call_site(stack: &str) -> String {
    let frames: Vec<String> = stack
        .lines()
        .filter_map(|line| {
            line.trim()
                .split_once(": ")
                .map(|(_, name)| name.to_string())
        })
        .filter(|name| name.starts_with("streams_slate::") || name.starts_with("slatedb::"))
        .filter(|name| !name.contains("split_store_inflight") && !name.contains("fault_store"))
        .map(|name| {
            let name = name
                .split("::{{closure}}")
                .next()
                .unwrap_or(&name)
                .to_string();
            match name.rsplit_once("::h") {
                Some((head, hash)) if hash.chars().all(|c| c.is_ascii_hexdigit()) => head.into(),
                _ => name,
            }
        })
        .collect();
    let mut kept: Vec<String> = Vec::new();
    for frame in frames {
        if kept.last() != Some(&frame) {
            kept.push(frame);
        }
        if kept.len() == 4 {
            break;
        }
    }
    kept.join(" < ")
}

/// `counts`' nonzero rows, largest first.
fn sorted(counts: &HashMap<Key, usize>) -> Vec<(Key, usize)> {
    let mut rows: Vec<(Key, usize)> = counts
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(key, n)| (key.clone(), *n))
        .collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows
}

/// `path` with each component that names one object or shard (digits, hex, a `~` home, an
/// extension's file) shown as `*`, its first five components kept.
fn shape(path: &str) -> String {
    path.split('/')
        .take(5)
        .map(|part| {
            let named = part.contains('~')
                || part.contains('.')
                || part.len() > 12
                || part.chars().all(|c| c.is_ascii_hexdigit());
            if named { "*" } else { part }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// One request in flight, until it is answered or dropped.
struct InFlight {
    gauge: Arc<Gauge>,
    key: Option<Key>,
    general: bool,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.gauge.end(&key, self.general);
        }
    }
}

/// A store that counts the requests in flight through it.
#[derive(Debug)]
struct GaugeStore {
    inner: Arc<dyn ObjectStore>,
    gauge: Arc<Gauge>,
}

impl std::fmt::Display for GaugeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GaugeStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GaugeStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let _flight = self.gauge.begin("PUT", location.as_ref());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let _flight = self.gauge.begin("PUT", location.as_ref());
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        let kind = if options.head { "HEAD" } else { "GET" };
        let _flight = self.gauge.begin(kind, location.as_ref());
        self.inner.get_opts(location, options).await
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        let path = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        until_first_item(self.inner.list(prefix), self.gauge.clone(), "LIST", path)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        let path = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let _flight = self.gauge.begin("LIST", &path);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
        let _flight = self.gauge.begin("COPY", from.as_ref());
        self.inner.copy_opts(from, to, opts).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
        let deletes = self.inner.delete_stream(locations);
        until_first_item(deletes, self.gauge.clone(), "DELETE", String::new())
    }
}

/// `inner`, counted in flight as `kind` at `path` from its first poll until its first item or its
/// end. A stream sends nothing before it is polled, and `TimingStore` polls it only once it holds
/// its count permit (`count_gate::Gated`), so a stream created while its pool is full is queued, not
/// at the store. The gauge's store is in memory: a listing's or a delete stream's store work is
/// done by its first item, after which nothing of it is in flight at the store. From the first poll
/// to the first item is the span the count bound's permit covers (a call's own wait, never the rest
/// of its stream).
fn until_first_item<T: Send + 'static>(
    inner: futures_util::stream::BoxStream<'static, T>,
    gauge: Arc<Gauge>,
    kind: &'static str,
    path: String,
) -> futures_util::stream::BoxStream<'static, T> {
    let state = (inner, Some((gauge, path)));
    futures_util::stream::unfold(state, move |(mut inner, first)| async move {
        let flight = first.map(|(gauge, path)| gauge.begin(kind, &path));
        let item = inner.next().await;
        drop(flight);
        Some((item?, (inner, None)))
    })
    .boxed()
}

/// Writers each appending to a stream of their own until stopped: each one's acks, and every
/// answer other than `204`.
struct Writers {
    acks: Vec<AtomicU64>,
    refused: Mutex<Vec<u16>>,
    stop: AtomicBool,
}

impl Writers {
    fn new(writers: usize) -> Self {
        Self {
            acks: (0..writers).map(|_| AtomicU64::new(0)).collect(),
            refused: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
        }
    }

    /// Every writer appending to `load-1`, `load-2`, ... at `addr` until `stop`.
    async fn run(&self, addr: std::net::SocketAddr) {
        let all = self.acks.iter().enumerate().map(|(n, acked)| {
            writer(
                addr,
                format!("load-{}", n + 1),
                acked,
                &self.refused,
                &self.stop,
            )
        });
        drop(futures_util::future::join_all(all).await);
    }

    /// Waits, at most 60 s, until every writer has more acks than `then` (each writer's count at
    /// some earlier moment).
    async fn each_acked_since(&self, then: &[u64]) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self
            .acks
            .iter()
            .zip(then)
            .any(|(now, then)| now.load(Ordering::Relaxed) <= *then)
        {
            assert!(Instant::now() < deadline, "every writer acks");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn counts(&self) -> Vec<u64> {
        self.acks
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect()
    }
}

/// Appends to `stream` at `addr` until `stop`, counting each `204` in `acks`; any other answer
/// is kept in `refused`.
async fn writer(
    addr: std::net::SocketAddr,
    stream: String,
    acks: &AtomicU64,
    refused: &Mutex<Vec<u16>>,
    stop: &AtomicBool,
) {
    let (path, json) = (
        format!("/v1/stream/{stream}"),
        [("content-type", "application/json")],
    );
    for k in 1u32.. {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let body = format!(r#"[{{"n":{k}}}]"#);
        let (status, _, _) = hreq(addr, "POST", &path, &json, body.as_bytes()).await;
        if status == 204 {
            acks.fetch_add(1, Ordering::Relaxed);
        } else {
            refused.lock().unwrap().push(status);
        }
    }
}

/// One measurement: what it ran, each window's peak and call sites, and each timeline.
struct Measured {
    what: String,
    refused: Vec<u16>,
    windows: Vec<(&'static str, Peak, String)>,
    timelines: Vec<(&'static str, String)>,
}

/// `split_pause`'s parent of `streams` streams, cold, on a store 20 ms away behind a gauge that
/// reads the instance's requests in flight, beneath the store wrapper production puts over every
/// store (`TimingStore`, its count bound in `io`).
async fn gauged(streams: usize) -> (HttpRig, Arc<Gauge>, Arc<StoreResources>) {
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (20, 20),
        ..FaultPlan::CLEAN
    };
    let fault = FaultStore::uniform(super::fixture_storage::mem(), 0x5419, plan);
    let gauge = Arc::new(Gauge::default());
    let gauged = GaugeStore {
        inner: fault,
        gauge: gauge.clone(),
    };
    let io = Arc::new(StoreResources::new(&crate::config::StorageConfig::default()));
    let store = Arc::new(TimingStore::new(gauged, io.clone()));
    let rig = super::split_pause::loaded(store, streams).await;
    let state = rig.state.clone();
    *gauge.requests.lock().unwrap() = Some(Arc::new(move || state.admission.active()));
    (rig, gauge, io)
}

/// Closes the rig and resolves the gauge's samples.
async fn finished(rig: &HttpRig, gauge: &Gauge) -> Vec<Site> {
    engine_shutdown(&rig.state).await;
    drop(gauge.requests.lock().unwrap().take());
    resolved(std::mem::take(&mut *gauge.samples.lock().unwrap()))
}

/// The parent split while `writers` writers append through it: the windows to the park, the
/// pause (the park to the publish) and the publish to every writer's first ack after it, and the
/// timeline of the 10 s after the publish (the children's opens).
async fn split_measured(streams: usize, writers: usize) -> Measured {
    let (rig, gauge, _) = gauged(streams).await;
    let parent = rig.state.shards.open("").expect("the leaf is open");
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    parent
        .gc_state()
        .gate
        .pause_at_park(crate::topology::gate::PausePoint { reached, go });
    let w = Writers::new(writers);
    let begun = gauge.start();
    let clock = rig.state.runtime.clock.clone();
    let measure = async {
        let control = async {
            reached_rx.await.expect("the split parks");
            let to_park = (gauge.window(), Instant::now());
            assert!(go_tx.send(()).is_ok(), "the split waits for go");
            to_park
        };
        let split = crate::topology::split::split(
            &rig.state.shards,
            &parent,
            "op-gauge",
            "inst-a",
            &*clock,
        );
        let (published, (to_park, parked)) = futures_util::future::join(split, control).await;
        let (pause, opened) = (gauge.window(), Instant::now());
        let published = published
            .map_err(|refused| refused.to_string())
            .expect("the split publishes");
        w.each_acked_since(&w.counts()).await;
        let (acks, acked) = (gauge.window(), Instant::now());
        tokio::time::sleep_until((opened + FOLLOW).into()).await;
        w.stop.store(true, Ordering::Relaxed);
        let phases = crate::topology::split::shown_phases(&published);
        let times = [begun, parked, opened, acked];
        (
            published.pause.as_millis(),
            phases,
            [to_park, pause, acks],
            times,
        )
    };
    let ((pause_ms, phases, peaks, times), ()) =
        futures_util::future::join(measure, w.run(rig.addr)).await;
    let sites = finished(&rig, &gauge).await;
    let names = ["to the park", "the pause", "the publish to the first acks"];
    let windows = peaks
        .into_iter()
        .zip(names)
        .enumerate()
        .map(|(n, (peak, name))| {
            let span = times[n + 1].duration_since(times[n]);
            (name, peak, sites_in(&sites, times[n], span, 6))
        })
        .collect();
    Measured {
        what: format!(
            "split, {streams} streams, {writers} writers: pause {pause_ms} ms [{phases}]"
        ),
        refused: std::mem::take(&mut *w.refused.lock().unwrap()),
        windows,
        timelines: vec![("10 s after the publish", timeline(&gauge, &sites, times[2]))],
    }
}

/// The parent's first open on an instance that holds no engine (a move-in): every engine closed,
/// then `writers` writers append; the window to every writer's first ack, the timeline of the 10
/// s after the open's start, and then 10 s with no event, its window and timeline.
async fn open_measured(streams: usize, writers: usize) -> Measured {
    let (rig, gauge, _) = gauged(streams).await;
    engine_shutdown(&rig.state).await;
    let w = Writers::new(writers);
    let begun = gauge.start();
    let measure = async {
        w.each_acked_since(&vec![0; writers]).await;
        let (open, acked) = (gauge.window(), Instant::now());
        tokio::time::sleep_until((begun + FOLLOW).into()).await;
        drop(gauge.window());
        let quiet_from = Instant::now();
        tokio::time::sleep(FOLLOW).await;
        let quiet = gauge.window();
        w.stop.store(true, Ordering::Relaxed);
        (open, acked, quiet, quiet_from)
    };
    let ((open, acked, quiet, quiet_from), ()) =
        futures_util::future::join(measure, w.run(rig.addr)).await;
    let sites = finished(&rig, &gauge).await;
    let open_sites = sites_in(&sites, begun, acked.duration_since(begun), 6);
    let quiet_sites = sites_in(&sites, quiet_from, FOLLOW, 6);
    Measured {
        what: format!(
            "open, {streams} streams, {writers} writers: first acks {} ms after the open began",
            acked.duration_since(begun).as_millis()
        ),
        refused: std::mem::take(&mut *w.refused.lock().unwrap()),
        windows: vec![
            ("the open to the first acks", open, open_sites),
            ("10 s with no event", quiet, quiet_sites),
        ],
        timelines: vec![
            ("10 s after the open", timeline(&gauge, &sites, begun)),
            ("10 s with no event", timeline(&gauge, &sites, quiet_from)),
        ],
    }
}

/// **The store requests in flight around a split and an open** (a diagnosis for Addenda 9, 10
/// and 13 of the forward-wedge ruling, run under the machine's lock; ignored in the suite).
/// `split_pause`'s parent, cold, on a store 20 ms away, at 2,500 and 10,000 streams, under 64 and
/// then 256 writers each appending to a stream of its own: split, and separately opened as a
/// move-in on an instance that holds no engine. For each it reports the peak of store requests in
/// flight by kind and path beside the instance's own requests in flight, and the call sites, in
/// each window (to the park, the pause, the publish to every writer's first ack; the open to every
/// writer's first ack; 10 s with no event), and a half-second timeline for the 10 s after each
/// open and the 10 s with no event. Nothing is asserted of the peaks; each split publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a diagnosis, run under the machine's lock"]
async fn store_requests_in_flight_around_a_split_and_an_open() {
    for (streams, writers) in [(2_500, 64), (2_500, 256), (10_000, 64), (10_000, 256)] {
        report(&split_measured(streams, writers).await);
        report(&open_measured(streams, writers).await);
    }
}

/// One append of one record to `stream` at `addr`: its status and how long it took to answer.
async fn touch(addr: std::net::SocketAddr, stream: String) -> (u16, Duration) {
    let path = format!("/v1/stream/{stream}");
    let json = [("content-type", "application/json")];
    let sent = Instant::now();
    let (status, _, _) = hreq(addr, "POST", &path, &json, br#"[{"pile":1}]"#).await;
    (status, sent.elapsed())
}

/// The answers' statuses, counted, and the commit delay of the `204`s: p50, p99 and the most.
fn delays(answers: &[(u16, Duration)]) -> String {
    let mut statuses: std::collections::BTreeMap<u16, usize> = std::collections::BTreeMap::new();
    for (status, _) in answers {
        *statuses.entry(*status).or_insert(0) += 1;
    }
    let mut acked: Vec<Duration> = answers
        .iter()
        .filter(|(status, _)| *status == 204)
        .map(|(_, took)| *took)
        .collect();
    acked.sort();
    let at = |q: usize| acked.get((acked.len() * q / 100).min(acked.len().saturating_sub(1)));
    let ms = |d: Option<&Duration>| d.map_or(0, Duration::as_millis);
    format!(
        "statuses {statuses:?}, commit delay p50 {} ms p99 {} ms max {} ms",
        ms(at(50)),
        ms(at(99)),
        ms(acked.last())
    )
}

/// A pile: `touches` appends to as many distinct streams of the parent at `addr`, sent at once.
async fn pile(addr: std::net::SocketAddr, touches: usize) -> Vec<(u16, Duration)> {
    // The parent holds `load-0` to `load-{streams - 1}`.
    let all = (0..touches).map(|n| touch(addr, format!("load-{n}")));
    futures_util::future::join_all(all).await
}

/// The answers other than `204`.
fn refused(answers: &[(u16, Duration)]) -> Vec<u16> {
    answers
        .iter()
        .map(|(status, _)| *status)
        .filter(|status| *status != 204)
        .collect()
}

/// A pile after a move-in open (Addendum 16): `split_pause`'s parent of `streams` streams, every
/// engine closed and every descriptor past the cache's TTL, then `touches` appends to as many
/// distinct streams sent at once (the first opens the leaf). The window over the pile, its call
/// sites, the timeline from its start, each append's commit delay, and the count bound's pools.
async fn pile_measured(streams: usize, touches: usize) -> (Measured, [(usize, u64); 2]) {
    let (rig, gauge, io) = gauged(streams).await;
    engine_shutdown(&rig.state).await;
    // Past the descriptor cache's 5 s TTL: every stream's next read revalidates at the store.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let begun = gauge.start();
    let answers = pile(rig.addr, touches).await;
    let answered = begun.elapsed();
    tokio::time::sleep_until((begun + FOLLOW).into()).await;
    let peak = gauge.window();
    let sites = finished(&rig, &gauge).await;
    let measured = Measured {
        what: format!(
            "pile, {streams} streams, {touches} distinct expired streams touched at once after a \
             move-in: all answered in {} ms, {}, pools (peak, waits) general {:?} commit {:?}",
            answered.as_millis(),
            delays(&answers),
            io.count_seen()[0],
            io.count_seen()[1],
        ),
        refused: refused(&answers),
        windows: vec![("the pile", peak, sites_in(&sites, begun, FOLLOW, 6))],
        timelines: vec![("10 s from the pile", timeline(&gauge, &sites, begun))],
    };
    (measured, io.count_seen())
}

/// **A pile after a move-in stays within the store's count bound** (Addendum 17 of the
/// forward-wedge ruling). `split_pause`'s parent of 2,500 streams on a store 20 ms away, every
/// engine closed and every descriptor expired, then 2,500 appends to its 2,500 streams at once:
/// unbounded, its descriptor revalidations put 1,550 store requests in flight. Through the
/// production store wrapper the store sees at most `GENERAL_PERMITS + COMMIT_PERMITS` at once and
/// the general class at most `GENERAL_PERMITS`; the pile fills the general pool (its requests
/// wait), no request of the WAL's, the manifest's or the fleet's class waits, and every append is
/// acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_pile_after_a_move_in_stays_within_the_store_count_bound() {
    let (measured, [general, commit]) = pile_measured(2_500, 2_500).await;
    report(&measured);
    let peak = &measured.windows[0].1;
    assert_eq!(
        (
            measured.refused.len(),
            peak.total <= GENERAL_PERMITS + COMMIT_PERMITS
        ),
        (0, true),
        "every touch is acknowledged and the store sees at most {} at once: {}",
        GENERAL_PERMITS + COMMIT_PERMITS,
        peak.shown(8)
    );
    assert!(peak.general <= GENERAL_PERMITS, "{}", peak.shown(8));
    assert!(
        general.0 <= GENERAL_PERMITS && commit.0 <= COMMIT_PERMITS,
        "the pools' own peaks: {general:?} {commit:?}"
    );
    assert!(
        general.1 > 0,
        "the pile fills the general pool: {general:?}"
    );
    assert_eq!(commit.1, 0, "the commit path never waits: {commit:?}");
}

/// **The gauge counts a listing as the count bound does** (Addendum 17's red on PR #167's first
/// run: the gauge saw 257 general requests beside a general pool that peaked at 256). A listing's
/// permit covers its call to its first item, and on the gauge's in-memory store that is all of
/// its store work. A listing open past its first item beside 256 parked descriptor GETs is 256
/// general requests in flight, as the general pool counts, not 257.
#[tokio::test(crate = "crate::test_rt")]
async fn the_gauge_counts_an_open_listing_only_to_its_first_item() {
    use object_store::ObjectStoreExt;
    let mem = super::fixture_storage::mem();
    for n in 0..2 {
        let key = ObjPath::from(format!("registry/v5/listed/{n}.json"));
        mem.put(&key, b"{}".to_vec().into()).await.unwrap();
    }
    let fault = FaultStore::uniform(mem, 0x5419, FaultPlan::CLEAN);
    let parked = fault.hold_class(
        crate::dst::StoreOp::Get,
        crate::dst::ObjClass::Other,
        GENERAL_PERMITS as u64,
    );
    let gauge = Arc::new(Gauge::default());
    let io = Arc::new(StoreResources::new(&crate::config::StorageConfig::default()));
    let gauged = GaugeStore {
        inner: fault.clone(),
        gauge: gauge.clone(),
    };
    let store = TimingStore::new(gauged, io.clone());
    let mut listing = store.list(Some(&ObjPath::from("registry/v5/listed")));
    assert!(listing.next().await.is_some(), "the listing's first item");
    let keys: Vec<ObjPath> = (0..GENERAL_PERMITS)
        .map(|n| ObjPath::from(format!("registry/v5/g/{n}")))
        .collect();
    let gets = keys.iter().map(|key| store.get(key));
    let parked_all = async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while parked.load(Ordering::Relaxed) < GENERAL_PERMITS as u64 {
            assert!(Instant::now() < deadline, "every GET parks");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let peak = gauge.window();
        fault.release_hold();
        peak
    };
    let (_, peak) =
        futures_util::future::join(futures_util::future::join_all(gets), parked_all).await;
    assert_eq!(
        (peak.general, io.count_seen()[0].0),
        (GENERAL_PERMITS, GENERAL_PERMITS),
        "{}",
        peak.shown(4)
    );
    assert!(listing.next().await.is_some(), "the listing's second item");
}

/// **The gauge counts a listing or a delete stream from its first poll** (PR #169's red: the gauge
/// saw 257 general requests beside a general pool that peaked at 256, one of them a compacted
/// listing). `TimingStore` takes a stream's permit on its first poll and polls the stream only
/// then, so a listing and a delete stream created while the general pool is full are queued for
/// their permits, not in flight at the store; once the pool frees, each is one in flight to its
/// first item.
#[tokio::test(crate = "crate::test_rt")]
async fn the_gauge_counts_a_stream_queued_for_its_permit_only_once_it_holds_it() {
    use object_store::ObjectStoreExt;
    let mem = super::fixture_storage::mem();
    let key = ObjPath::from("shards/root/compacted/0.sst");
    mem.put(&key, b"x".to_vec().into()).await.unwrap();
    let gauge = Arc::new(Gauge::default());
    let io = Arc::new(StoreResources::new(&crate::config::StorageConfig::default()));
    let gauged = GaugeStore {
        inner: mem,
        gauge: gauge.clone(),
    };
    let store = TimingStore::new(gauged, io.clone());
    let held = io.hold_general().await;
    let mut listing = store.list(Some(&ObjPath::from("shards/root/compacted")));
    let mut deletes = store.delete_stream(futures_util::stream::iter([Ok(key)]).boxed());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(
        listing.poll_next_unpin(&mut cx).is_pending()
            && deletes.poll_next_unpin(&mut cx).is_pending(),
        "both wait for a general permit"
    );
    let queued = gauge.window();
    assert_eq!(
        queued.general,
        0,
        "a stream queued for its permit is not at the store: {}",
        queued.shown(4)
    );
    drop(held);
    assert!(listing.next().await.is_some(), "the listing's item");
    assert!(deletes.next().await.is_some(), "the delete's item");
    let ran = gauge.window();
    assert_eq!(
        ran.general,
        1,
        "each in flight alone to its first item: {}",
        ran.shown(4)
    );
}

/// `split_pause`'s parent of `streams` streams, every descriptor past the cache's TTL, split
/// while a pile of `touches` appends is in flight (once its general pool is full), or with
/// nothing in flight when `touches` is 0: the split's pause and phases, and the pools.
async fn split_under_pile(streams: usize, touches: usize) -> String {
    let (rig, _gauge, io) = gauged(streams).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let parent = rig.state.shards.open("").expect("the leaf is open");
    let clock = rig.state.runtime.clock.clone();
    let split = async {
        let deadline = Instant::now() + Duration::from_secs(30);
        while touches > 0 && io.count_seen()[0].1 == 0 {
            assert!(Instant::now() < deadline, "the pile fills the general pool");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        crate::topology::split::split(&rig.state.shards, &parent, "op-pile", "inst-a", &*clock)
            .await
            .map_err(|refused| refused.to_string())
            .expect("the split publishes")
    };
    let (published, answers) = futures_util::future::join(split, pile(rig.addr, touches)).await;
    engine_shutdown(&rig.state).await;
    format!(
        "split, {streams} streams, a pile of {touches} in flight: pause {} ms [{}], refused {:?}, \
         pools (peak, waits) general {:?} commit {:?}",
        published.pause.as_millis(),
        crate::topology::split::shown_phases(&published),
        refused(&answers),
        io.count_seen()[0],
        io.count_seen()[1],
    )
}

/// **A split's pause with a pile in flight** (a measurement for Addendum 17, run under the
/// machine's lock; ignored in the suite). A split's topology requests share the general pool with
/// a pile's descriptor revalidations: `split_pause`'s parent of 2,500 streams split with nothing
/// in flight, and again with a pile of 2,500 appends in flight once the pile has filled the pool.
/// Nothing is asserted of the pauses; each split publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a measurement, run under the machine's lock"]
async fn split_pause_with_a_pile_in_flight() {
    for touches in [0, 2_500, 0, 2_500] {
        eprintln!("STORE_INFLIGHT {}", split_under_pile(2_500, touches).await);
    }
}

/// Prints one measurement's lines.
fn report(m: &Measured) {
    eprintln!("STORE_INFLIGHT {}, refused {:?}", m.what, m.refused);
    for (name, peak, sites) in &m.windows {
        eprintln!("STORE_INFLIGHT   {name}: {}", peak.shown(8));
        eprintln!("STORE_INFLIGHT   {name}, call sites: {sites}");
    }
    for (name, line) in &m.timelines {
        eprintln!("STORE_INFLIGHT   timeline, {name}:{line}");
    }
}
