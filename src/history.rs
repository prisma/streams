//! History tier (§3.6): per-stream WAL-less SlateDBs under shared-bucket
//! prefixes, block-transformer encrypted with the stream key, block-zstd
//! compressed — plus the absorber that drains shard logs into them.
//!
//! History keyspace:
//!   'r' '!' <offset u64 BE>                       record (plaintext in blocks)
//!   'k' '!' <rk_len u16 BE> <rk> <offset u64 BE>  routing-key index (copy)
//!
//! History record value: [ver u8=1][ts i64 LE][key_version u32 LE]
//!                       [rk_len u16 LE][rk][payload]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::{CompressionCodec, Settings, WriteOptions};
use slatedb::{BlockTransformer, Db, DbReader, WriteBatch};
use tokio::sync::mpsc;

use crate::crypto::{StreamKey, decode_frame, decrypt_frame, derive_subkey, hex};
use crate::shard::{AbsorbSignal, ShardEngine, now_ms, read_frames_range};

// ---- block transformer: AES-256-GCM with a random nonce per block ----

pub struct AesBlockTransformer {
    cipher: Aes256Gcm,
}

impl AesBlockTransformer {
    pub fn new(key: &StreamKey) -> AesBlockTransformer {
        AesBlockTransformer {
            cipher: Aes256Gcm::new((&key.0).into()),
        }
    }
}

#[async_trait::async_trait]
impl BlockTransformer for AesBlockTransformer {
    async fn encode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        let mut nonce = [0u8; 12];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut nonce);
        let ct = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &data,
                    aad: b"",
                },
            )
            .map_err(|_| block_err("block encrypt failed"))?;
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(Bytes::from(out))
    }

    async fn decode(&self, data: Bytes) -> Result<Bytes, slatedb::Error> {
        if data.len() < 12 {
            return Err(block_err("block too short"));
        }
        let (nonce, ct) = data.split_at(12);
        let pt = self
            .cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad: b"" })
            .map_err(|_| block_err("block decrypt failed (wrong stream key?)"))?;
        Ok(Bytes::from(pt))
    }
}

fn block_err(msg: &str) -> slatedb::Error {
    slatedb::Error::data(msg.to_string())
}

// ---- history record codec ----

pub fn hist_record_key(offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(b"r!");
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

pub fn hist_key_index_key(rk: &str, offset: u64) -> Vec<u8> {
    let rkb = rk.as_bytes();
    let mut k = Vec::with_capacity(12 + rkb.len());
    k.extend_from_slice(b"k!");
    k.extend_from_slice(&(rkb.len() as u16).to_be_bytes());
    k.extend_from_slice(rkb);
    k.extend_from_slice(&offset.to_be_bytes());
    k
}

pub fn encode_hist_record(ts: i64, key_version: u32, rk: &str, payload: &[u8]) -> Vec<u8> {
    let rkb = rk.as_bytes();
    let mut v = Vec::with_capacity(15 + rkb.len() + payload.len());
    v.push(1);
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&key_version.to_le_bytes());
    v.extend_from_slice(&(rkb.len() as u16).to_le_bytes());
    v.extend_from_slice(rkb);
    v.extend_from_slice(payload);
    v
}

pub struct HistRecord {
    pub ts: i64,
    pub key_version: u32,
    pub routing_key: String,
    pub payload: Bytes,
}

pub fn decode_hist_record(v: &Bytes) -> Option<HistRecord> {
    if v.len() < 15 || v[0] != 1 {
        return None;
    }
    let ts = i64::from_le_bytes(v[1..9].try_into().ok()?);
    let key_version = u32::from_le_bytes(v[9..13].try_into().ok()?);
    let rk_len = u16::from_le_bytes(v[13..15].try_into().ok()?) as usize;
    let routing_key = String::from_utf8(v.get(15..15 + rk_len)?.to_vec()).ok()?;
    let payload = v.slice(15 + rk_len..);
    Some(HistRecord {
        ts,
        key_version,
        routing_key,
        payload,
    })
}

// ---- settings (D23 maintenance profile + F2 pattern) ----

/// Shared block cache for ALL history DBs (absorber writes + reads):
/// SlateDB's per-DB default is 512 MB, and the absorber opens a DB per
/// absorbed stream — unbounded aggregate cache on a 1 GB box.
fn history_cache() -> Arc<slatedb::db_cache::foyer::FoyerCache> {
    static CACHE: std::sync::OnceLock<Arc<slatedb::db_cache::foyer::FoyerCache>> =
        std::sync::OnceLock::new();
    CACHE
        .get_or_init(|| {
            let bytes = std::env::var("HISTORY_CACHE_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(32 * 1024 * 1024);
            Arc::new(slatedb::db_cache::foyer::FoyerCache::new_with_opts(
                slatedb::db_cache::foyer::FoyerCacheOptions {
                    max_capacity: bytes,
                    ..Default::default()
                },
            ))
        })
        .clone()
}

fn history_settings() -> Settings {
    // Bench-only escape hatch: HISTORY_COMPACTOR=off disables the embedded
    // compactor (and lifts the L0 caps so flushes never block on it). Used
    // with the s3lite --discard-substr mode, where history SST bodies are
    // dropped and must never be re-read. Production keeps the compactor.
    let compactor_off = std::env::var("HISTORY_COMPACTOR")
        .map(|v| v == "off")
        .unwrap_or(false);
    Settings {
        wal_enabled: false,
        flush_interval: Some(Duration::from_millis(100)),
        manifest_poll_interval: Duration::from_secs(300),
        compression_codec: Some(CompressionCodec::Zstd),
        // Upstream default is 512 MB — on a 1 GB instance the absorber sink
        // buffers toward the kernel kill line long before backpressure
        // fires (same finding as the shard tier, 2026-07-14; reproduced as
        // an OOM loop at 10 MB/s absorb on Compute, 2026-07-23). Bound it.
        max_unflushed_bytes: 32 * 1024 * 1024,
        // 4 MB (was 16): each history SST build runs zstd + AES over the
        // whole SST inside SlateDB's flush task ON OUR RUNTIME — 16 MB
        // builds blocked the event loop in 100s-of-ms bursts (run 12
        // timer evidence). Smaller SSTs = shorter bursts; the embedded
        // compactor consolidates them.
        l0_sst_size_bytes: 4 * 1024 * 1024,
        l0_max_ssts: if compactor_off { 1_000_000 } else { 64 },
        l0_max_ssts_per_key: if compactor_off { 1_000_000 } else { 64 },
        compactor_options: if compactor_off {
            None
        } else {
            // Embedded compactor kept for phase 1 so L0s consolidate while
            // the absorber has the DB open; the detached model arrives with
            // the compactor service.
            Settings::default().compactor_options
        },
        ..Default::default()
    }
}

pub fn history_db_path(hash: &[u8; 16]) -> String {
    format!("streams/{}", hex(hash))
}

// ---- key cache (transient; fed by keyed requests) ----

pub struct KeyEntry {
    pub key: StreamKey,
    pub epoch: [u8; 16],
    pub at: Instant,
}

#[derive(Default)]
pub struct KeyCache {
    map: Mutex<HashMap<[u8; 16], KeyEntry>>,
}

impl KeyCache {
    pub fn put(&self, hash: [u8; 16], key: StreamKey, epoch: [u8; 16]) {
        self.map.lock().unwrap().insert(
            hash,
            KeyEntry {
                key,
                epoch,
                at: Instant::now(),
            },
        );
    }

    pub fn get(&self, hash: &[u8; 16]) -> Option<(StreamKey, [u8; 16])> {
        let map = self.map.lock().unwrap();
        let e = map.get(hash)?;
        if e.at.elapsed() > Duration::from_secs(900) {
            return None;
        }
        Some((e.key.clone(), e.epoch))
    }
}

// ---- absorber ----

pub struct AbsorberConfig {
    pub threshold_bytes: u64,
    pub threshold_age: Duration,
    pub tick: Duration,
    pub batch_puts: usize,
    /// Upper bound on plaintext bytes buffered per absorb pass. absorb_one
    /// holds the whole pass in memory; without a cap, a pass that starts
    /// behind a high-throughput stream buffers the entire lag (GBs on a
    /// 1 GB instance). The boundary advances per pass, so a capped pass
    /// just means more passes.
    pub pass_bytes: u64,
}

impl Default for AbsorberConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 4 * 1024 * 1024,
            threshold_age: Duration::from_secs(300),
            tick: Duration::from_secs(5),
            batch_puts: 4_096,
            pass_bytes: 256 * 1024 * 1024,
        }
    }
}

struct PendingAbsorb {
    bytes: u64,
    since: Instant,
    /// Consecutive non-fence absorb failures; drives exponential backoff so
    /// a persistent error retries at tick·2^n instead of every tick.
    failures: u32,
    /// Earliest next attempt (backoff); zero-delay until the first failure.
    retry_after: Option<Instant>,
}

/// Fence-class absorb errors mean this engine lost the shard to a new owner:
/// retrying can never succeed and — worse — keeps evicting the rightful
/// owner's history db in a ping-pong ("the absorption war", 2026-07-20).
/// The correct move is to DROP the claim; the owner accumulates its own
/// signals from its own appends.
fn absorb_error_is_fence(msg: &str) -> bool {
    msg.contains("detected newer DB client")
        || msg.contains("Fenced")
        || msg.contains("Closed error")
}

pub struct Absorber {
    data_store: Arc<dyn ObjectStore>,
    shard: Arc<ShardEngine>,
    keys: Arc<KeyCache>,
    cfg: AbsorberConfig,
    /// History DB handles kept open across passes. The original F2 design
    /// opened and closed per pass ("maintenance-free"), but each open is
    /// 1-2 s of manifest round-trips — at a 32 MB pass that caps absorb
    /// throughput near ~5-8k rec/s, below a loaded stream's ingest, and
    /// the backlog compounds into the OOM spiral (sinmax run 11 marathon).
    /// Small LRU (4) + idle eviction keeps V4's idle-per-DB-overhead
    /// concern bounded; entries are dropped on fence-class errors and on
    /// absorber exit.
    open_dbs: tokio::sync::Mutex<HashMap<[u8; 16], (Arc<Db>, Instant)>>,
}

const HISTORY_DB_LRU: usize = 4;
const HISTORY_DB_IDLE: Duration = Duration::from_secs(120);

impl Absorber {
    pub fn start(
        data_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
        mut rx: mpsc::Receiver<AbsorbSignal>,
    ) -> tokio::task::JoinHandle<()> {
        let absorber = Absorber {
            data_store,
            shard,
            keys,
            cfg,
            open_dbs: tokio::sync::Mutex::new(HashMap::new()),
        };
        tokio::spawn(async move {
            let mut pending: HashMap<[u8; 16], PendingAbsorb> = HashMap::new();
            let mut tick = tokio::time::interval(absorber.cfg.tick);
            loop {
                // Lifecycle: this task holds the engine Arc, so the signal
                // channel can never close on its own — without this check a
                // fenced shard's absorber survives as a zombie, retrying
                // forever against a dead db (the absorption war's fuel).
                if absorber.shard.is_closed() {
                    let dropped: u64 = pending.values().map(|p| p.bytes).sum();
                    tracing::info!(
                        shard = %absorber.shard.prefix,
                        pending_bytes = dropped,
                        "absorber exiting: shard fenced/closed"
                    );
                    let handles: Vec<[u8; 16]> =
                        absorber.open_dbs.lock().await.keys().copied().collect();
                    for h in handles {
                        absorber.close_db(&h).await;
                    }
                    return;
                }
                tokio::select! {
                    sig = rx.recv() => {
                        let Some(sig) = sig else { return };
                        let e = pending.entry(sig.hash).or_insert(PendingAbsorb {
                            bytes: 0,
                            since: Instant::now(),
                            failures: 0,
                            retry_after: None,
                        });
                        e.bytes += sig.appended_bytes;
                    }
                    _ = tick.tick() => {
                        let now = Instant::now();
                        // Publish absorption lag (scale-out signal): age of
                        // the oldest unabsorbed bytes per stream.
                        for (h, p) in pending.iter() {
                            crate::usage::set_absorb_lag(h, p.since.elapsed().as_secs());
                        }
                        let due: Vec<[u8; 16]> = pending
                            .iter()
                            .filter(|(_, p)| {
                                (p.bytes >= absorber.cfg.threshold_bytes
                                    || p.since.elapsed() >= absorber.cfg.threshold_age)
                                    && p.retry_after.map(|t| now >= t).unwrap_or(true)
                            })
                            .map(|(h, _)| *h)
                            .collect();
                        for hash in due {
                            if absorber.shard.is_closed() {
                                break; // exit path above runs on next loop
                            }
                            match absorber.absorb_one(&hash).await {
                                Ok(absorbed) => {
                                    if absorbed {
                                        pending.remove(&hash);
                                        crate::usage::clear_absorb_lag(&hash);
                                    } else if let Some(p) = pending.get_mut(&hash) {
                                        // key missing: keep pending; retried
                                        // when the next keyed request arrives.
                                        p.failures = 0;
                                        p.retry_after = None;
                                    }
                                }
                                Err(e) => {
                                    let msg = e.to_string();
                                    if absorb_error_is_fence(&msg) {
                                        tracing::warn!(
                                            "dropping absorb claim for {} (fence-class): {msg}",
                                            hex(&hash)
                                        );
                                        pending.remove(&hash);
                                        absorber.close_db(&hash).await;
                                    } else if let Some(p) = pending.get_mut(&hash) {
                                        p.failures = p.failures.saturating_add(1);
                                        let shift = p.failures.min(6);
                                        p.retry_after =
                                            Some(now + absorber.cfg.tick * 2u32.pow(shift));
                                        // Log at failure 1, 2, 4, 8, ... only.
                                        if p.failures.is_power_of_two() {
                                            tracing::warn!(
                                                failures = p.failures,
                                                "absorb failed for {}: {msg}",
                                                hex(&hash)
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Close a cached history handle (fence-class failure or eviction).
    async fn close_db(&self, hash: &[u8; 16]) {
        let entry = self.open_dbs.lock().await.remove(hash);
        if let Some((db, _)) = entry {
            let _ = db.close().await;
        }
    }

    /// LRU + idle eviction: bound resident history DBs to HISTORY_DB_LRU,
    /// and drop any handle unused for HISTORY_DB_IDLE.
    async fn evict_idle_dbs(&self) {
        let mut victims: Vec<(Arc<Db>, [u8; 16])> = Vec::new();
        {
            let mut cache = self.open_dbs.lock().await;
            let now = Instant::now();
            let idle: Vec<[u8; 16]> = cache
                .iter()
                .filter(|(_, (_, last))| now.duration_since(*last) >= HISTORY_DB_IDLE)
                .map(|(h, _)| *h)
                .collect();
            for h in idle {
                if let Some((db, _)) = cache.remove(&h) {
                    victims.push((db, h));
                }
            }
            while cache.len() > HISTORY_DB_LRU {
                let oldest = cache
                    .iter()
                    .min_by_key(|(_, (_, last))| *last)
                    .map(|(h, _)| *h);
                match oldest {
                    Some(h) => {
                        if let Some((db, _)) = cache.remove(&h) {
                            victims.push((db, h));
                        }
                    }
                    None => break,
                }
            }
        }
        for (db, _) in victims {
            let _ = db.close().await;
        }
    }

    /// Returns Ok(false) if the stream key isn't available.
    async fn absorb_one(&self, hash: &[u8; 16]) -> anyhow::Result<bool> {
        let Some((key, epoch)) = self.keys.get(hash) else {
            return Ok(false);
        };
        let handle = self.shard.stream_handle(*hash).await?;
        let (from, upto) = {
            let st = handle.state.lock().unwrap();
            (st.durable.absorbed, st.durable.next)
        };
        if from >= upto {
            return Ok(true);
        }

        // Read + decrypt the un-absorbed durable range from the shard log.
        // Reads are issued as disjoint offset windows with bounded
        // concurrency (offsets below the durable frontier are dense, so
        // windows partition the log exactly); results are processed in
        // order so the absorbed boundary stays a contiguous prefix. The
        // old serial 8 MB chunk loop capped absorption ~10k rec/s.
        let mut subkeys: HashMap<(String, u32), [u8; 32]> = HashMap::new();
        let mut items: Vec<(u64, HistRecord)> = Vec::new();
        let mut pass_bytes = 0u64;
        // `items` holds the DECRYPTED pass. With v3 (compressed) frames the
        // raw byte budget alone is a memory landmine: 32 MB of frames can
        // decompress to ~1 GB of plaintext, which OOM-killed 1 GB instances
        // before the first history write (sinmax run 9, 2026-07-23). Bound
        // the pass by plaintext bytes with the same budget.
        let mut pt_bytes = 0u64;
        const WINDOW: u64 = 32_768;
        let mut window_reads = {
            use futures_util::StreamExt;
            let shard = self.shard.clone();
            let handle = handle.clone();
            futures_util::stream::iter((from..upto).step_by(WINDOW as usize).map(move |s| {
                let shard = shard.clone();
                let handle = handle.clone();
                let e = (s + WINDOW).min(upto);
                async move {
                    read_frames_range(&shard, &handle, s, e, 64 * 1024 * 1024)
                        .await
                        .map(|r| (e, r))
                }
            }))
            .buffered(4)
        };
        use futures_util::StreamExt;
        while let Some(res) = window_reads.next().await {
            let (win_end, chunk) = res?;
            if chunk.frames.is_empty() {
                break;
            }
            pass_bytes += chunk.frames.iter().map(|f| f.len() as u64).sum::<u64>();
            let last_offset = chunk.last_offset;
            // Decode+decrypt+decompress is CPU-bound (v3 frames expand up
            // to ~30x). Run 12 measured the cost of doing it on the async
            // runtime: tokio timer p99 848 ms vs 3.6 ms on a raw thread —
            // the whole ack path starved behind these loops. The blocking
            // pool gets OS preemption instead of cooperative starvation.
            let key_b = key.clone();
            let epoch_b = epoch;
            let hash_b = *hash;
            let frames = chunk.frames;
            let subkeys_in = std::mem::take(&mut subkeys);
            type SubkeyMap = HashMap<(String, u32), [u8; 32]>;
            let joined = tokio::task::spawn_blocking(
                move || -> Result<(Vec<(u64, HistRecord)>, SubkeyMap), String> {
                    let mut subkeys = subkeys_in;
                    let mut out = Vec::with_capacity(frames.len());
                    for raw in &frames {
                        let frame = decode_frame(raw)
                            .ok_or_else(|| "undecodable frame during absorb".to_string())?;
                        let sk = *subkeys
                            .entry((frame.header.routing_key.clone(), frame.header.key_version))
                            .or_insert_with(|| {
                                derive_subkey(
                                    &key_b,
                                    &epoch_b,
                                    &frame.header.routing_key,
                                    frame.header.key_version,
                                )
                            });
                        let pt = decrypt_frame(&sk, &hash_b, &frame, raw)
                            .map_err(|e| format!("absorb decrypt: {e}"))?;
                        out.push((
                            frame.header.offset,
                            HistRecord {
                                ts: frame.header.ts_ms,
                                key_version: frame.header.key_version,
                                routing_key: frame.header.routing_key,
                                payload: Bytes::from(pt),
                            },
                        ));
                    }
                    Ok((out, subkeys))
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("absorb decode join: {e}"))?;
            let (decoded, subkeys_back) = joined.map_err(|e| anyhow::anyhow!(e))?;
            subkeys = subkeys_back;
            for (offset, rec) in decoded {
                pt_bytes += rec.payload.len() as u64;
                items.push((offset, rec));
            }
            // A byte-truncated window breaks offset contiguity past its
            // last frame: stop here; the boundary advances to what we have.
            let complete = last_offset.map(|l| l + 1 >= win_end).unwrap_or(false);
            if !complete || pass_bytes >= self.cfg.pass_bytes || pt_bytes >= self.cfg.pass_bytes {
                break;
            }
        }
        drop(window_reads);
        if items.is_empty() {
            return Ok(true);
        }
        let absorbed_upto = items.last().map(|(o, _)| o + 1).unwrap_or(upto);

        // Bulk write through a cached handle (open once, reuse across
        // passes), explicit flush per pass so the boundary only advances
        // over durable data. See open_dbs field note for why not
        // open/close per pass.
        let db = {
            let mut cache = self.open_dbs.lock().await;
            if let Some((db, last_used)) = cache.get_mut(hash) {
                *last_used = Instant::now();
                db.clone()
            } else {
                let path = history_db_path(hash);
                let store = self.data_store.clone();
                let k = key.clone();
                let db = Arc::new(
                    crate::on_slatedb_rt(async move {
                        Db::builder(path.as_str(), store)
                            .with_settings(history_settings())
                            .with_db_cache(history_cache())
                            .with_block_transformer(Arc::new(AesBlockTransformer::new(&k)))
                            .build()
                            .await
                    })
                    .await?,
                );
                cache.insert(*hash, (db.clone(), Instant::now()));
                db
            }
        };
        let mut i = 0;
        while i < items.len() {
            let mut wb = WriteBatch::new();
            let end = (i + self.cfg.batch_puts / 2).min(items.len());
            for (offset, rec) in &items[i..end] {
                let value =
                    encode_hist_record(rec.ts, rec.key_version, &rec.routing_key, &rec.payload);
                wb.put(hist_record_key(*offset), value.clone());
                wb.put(hist_key_index_key(&rec.routing_key, *offset), value);
            }
            db.write_with_options(
                wb,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await?;
            i = end;
        }
        db.flush().await?; // wal off => memtable -> L0 (durable)
        self.evict_idle_dbs().await;

        // Advance the readers' boundary + trim (deferred) in the shard log.
        self.shard.submit_absorbed(*hash, absorbed_upto).await;
        tracing::info!(
            "absorbed {} records into {} (upto {})",
            items.len(),
            history_db_path(hash),
            absorbed_upto
        );
        Ok(true)
    }
}

// ---- history reads ----

pub struct HistoryReadResult {
    pub records: Vec<(u64, HistRecord)>,
    pub last_offset: Option<u64>,
    /// True when the requested range was fully scanned (not byte-truncated):
    /// the caller may treat everything below `upto` as consumed.
    pub completed: bool,
}

/// Read [from, upto) from a stream's history DB (plaintext records).
/// `key_filter` uses the k! index (contiguous per routing key).
pub async fn read_history(
    data_store: &Arc<dyn ObjectStore>,
    hash: &[u8; 16],
    key: &StreamKey,
    from: u64,
    upto: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> anyhow::Result<HistoryReadResult> {
    let reader = {
        let path = history_db_path(hash);
        let store = data_store.clone();
        let k = key.clone();
        crate::on_slatedb_rt(async move {
            DbReader::builder(path.as_str(), store)
                .with_block_transformer(Arc::new(AesBlockTransformer::new(&k)))
                .build()
                .await
        })
        .await?
    };
    let mut out = HistoryReadResult {
        records: Vec::new(),
        last_offset: None,
        completed: true,
    };
    let mut total = 0usize;
    match key_filter {
        None => {
            let mut iter = reader
                .scan(hist_record_key(from)..hist_record_key(upto))
                .await?;
            while let Some(kv) = iter.next().await? {
                let off = u64::from_be_bytes(kv.key[2..10].try_into().expect("hist key"));
                if let Some(rec) = decode_hist_record(&kv.value) {
                    total += rec.payload.len();
                    out.records.push((off, rec));
                    out.last_offset = Some(off);
                    if total >= max_bytes {
                        out.completed = false;
                        break;
                    }
                }
            }
        }
        Some(rk) => {
            let range = hist_key_index_key(rk, from)..hist_key_index_key(rk, upto);
            let mut iter = reader.scan(range).await?;
            while let Some(kv) = iter.next().await? {
                let klen = kv.key.len();
                let off = u64::from_be_bytes(kv.key[klen - 8..].try_into().expect("k! key"));
                if let Some(rec) = decode_hist_record(&kv.value) {
                    total += rec.payload.len();
                    out.records.push((off, rec));
                    out.last_offset = Some(off);
                    if total >= max_bytes {
                        out.completed = false;
                        break;
                    }
                }
            }
        }
    }
    reader.close().await.ok();
    Ok(out)
}

pub fn absorber_channel() -> (mpsc::Sender<AbsorbSignal>, mpsc::Receiver<AbsorbSignal>) {
    mpsc::channel(65_536)
}

pub fn ts_now() -> i64 {
    now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{PutOptions, PutPayload, PutResult, path::Path as OPath};

    #[derive(Debug)]
    struct SlowPuts(Arc<dyn ObjectStore>);
    impl std::fmt::Display for SlowPuts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SlowPuts")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for SlowPuts {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            // Stall only WAL flushes: setup (manifest writes) stays fast,
            // and the flusher wedges exactly like a slow-store day.
            if location.as_ref().contains("wal") {
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<OPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<OPath>> {
            self.0.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    use crate::shard::{ShardConfig, ShardEngine};
    use slatedb::Db;

    #[test]
    fn fence_class_errors_are_recognized() {
        assert!(absorb_error_is_fence(
            "error: detected newer DB client at manifest 7"
        ));
        assert!(absorb_error_is_fence("Fenced"));
        assert!(absorb_error_is_fence("io wrapper: Closed error: db closed"));
        assert!(!absorb_error_is_fence("timeout waiting for PUT"));
        assert!(!absorb_error_is_fence("connection reset by peer"));
    }

    /// The absorption-war regression test: an absorber whose shard engine
    /// is fenced by a second opener must EXIT (it holds the engine Arc, so
    /// nothing else can end it), not retry forever against the dead db.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn absorber_exits_when_shard_engine_is_fenced() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db1 = Db::builder("t/shard", store.clone()).build().await.unwrap();
        let (absorb_tx, absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "t".into(),
            Arc::new(db1),
            ShardConfig::default(),
            absorb_tx,
            None,
        );
        let handle = Absorber::start(
            store.clone(),
            engine.clone(),
            Arc::new(KeyCache::default()),
            AbsorberConfig {
                tick: Duration::from_millis(50),
                ..Default::default()
            },
            absorb_rx,
        );
        // Give it pending work so exit isn't the empty-queue accident.
        engine.stream_handle([7u8; 16]).await.ok();

        // Second opener on the same path fences the first (SlateDB CAS).
        let _db2 = Db::builder("t/shard", store.clone()).build().await.unwrap();

        // The fenced engine flips closed, and the absorber task exits.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !engine.is_closed() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(engine.is_closed(), "engine never observed the fence");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("absorber did not exit after its engine was fenced")
            .unwrap();
    }

    /// Wedge detector: a stale in-progress db.write reads as blocked;
    /// idle (0) and fresh writes do not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_blocked_ms_tracks_stale_writes() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = Db::builder("w/shard", store.clone()).build().await.unwrap();
        let (absorb_tx, _absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "w".into(),
            Arc::new(db),
            ShardConfig::default(),
            absorb_tx,
            None,
        );
        assert_eq!(engine.commit_blocked_ms(), 0, "idle engine must read 0");
        engine.set_commit_write_started_ms(crate::shard::now_ms() - 5_000);
        assert!(
            engine.commit_blocked_ms() >= 4_500,
            "stale write must read blocked"
        );
        engine.set_commit_write_started_ms(crate::shard::now_ms());
        assert!(
            engine.commit_blocked_ms() < 2_000,
            "fresh write must not trip the shed"
        );
        engine.set_commit_write_started_ms(0);
        assert_eq!(engine.commit_blocked_ms(), 0);
    }

    /// End-to-end wedge detection under REAL SlateDB byte backpressure: a
    /// store whose PUTs stall blocks the commit db.write once the unflushed
    /// cap fills, and commit_blocked_ms() must cross the shed threshold.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_blocked_detects_real_flush_stall() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowPuts(mem));
        let db = Db::builder("s/shard", slow)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(25)),
                max_unflushed_bytes: 8 * 1024,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let (absorb_tx, _absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "s".into(),
            Arc::new(db),
            ShardConfig::default(),
            absorb_tx,
            None,
        );

        // Continuous feed: a LATER db.write must find the unflushed cap
        // full (the first group is admitted regardless) and block there.
        let feeder = engine.clone();
        let feed = tokio::spawn(async move {
            for i in 0..4096u64 {
                let (tx, _rx) = tokio::sync::oneshot::channel();
                let req = crate::shard::AppendReq {
                    usage: Default::default(),
                    hash: [9u8; 16],
                    enqueued_at: Instant::now(),
                    entries: vec![bytes::Bytes::from(vec![b'x'; 1024])],
                    routing_key: String::new(),
                    key_version: 1,
                    subkey: [0u8; 32],
                    ts_hint_ms: Some(i as i64),
                    seq: None,
                    bytes: 1024,
                    close: false,
                    producer: None,
                    deferred_error: None,
                    touch: None,
                    resp: tx,
                };
                let _ = feeder.try_enqueue(req);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let mut blocked = 0;
        while Instant::now() < deadline {
            blocked = engine.wedge_ms();
            if blocked > 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        feed.abort();
        assert!(
            blocked > 5_000,
            "wedge_ms never crossed the shed threshold (last {blocked})"
        );
    }

    /// The stale-durability wedge mode: db.write keeps succeeding (default
    /// unflushed cap is huge) while WAL flushes stall — committed groups
    /// age in in_flight and oldest_inflight_ms must cross the threshold.
    /// This is the mode the 2026-07-22 cloud gate proved commit_blocked_ms
    /// alone cannot see.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wedge_detects_stale_durability() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let slow: Arc<dyn ObjectStore> = Arc::new(SlowPuts(mem));
        // Default (large) unflushed cap: writes are admitted, durability stalls.
        let db = Db::builder("d/shard", slow)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(25)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let (absorb_tx, _absorb_rx) = absorber_channel();
        let engine = ShardEngine::start(
            "d".into(),
            Arc::new(db),
            ShardConfig::default(),
            absorb_tx,
            None,
        );
        for i in 0..8u64 {
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let req = crate::shard::AppendReq {
                usage: Default::default(),
                hash: [7u8; 16],
                enqueued_at: Instant::now(),
                entries: vec![bytes::Bytes::from(vec![b'y'; 512])],
                routing_key: String::new(),
                key_version: 1,
                subkey: [0u8; 32],
                ts_hint_ms: Some(i as i64),
                seq: None,
                bytes: 512,
                close: false,
                producer: None,
                deferred_error: None,
                touch: None,
                resp: tx,
            };
            let _ = engine.try_enqueue(req);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut wedge = 0;
        while Instant::now() < deadline {
            wedge = engine.wedge_ms();
            if wedge > 5_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            wedge > 5_000,
            "oldest_inflight_ms never crossed the shed threshold (last {wedge})"
        );
        assert!(
            engine.oldest_inflight_ms() > 5_000,
            "the stale-durability component specifically must be the signal"
        );
    }
}
