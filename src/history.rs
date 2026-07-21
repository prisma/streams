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
        l0_sst_size_bytes: 16 * 1024 * 1024,
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
}

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
            for raw in &chunk.frames {
                let frame = decode_frame(raw)
                    .ok_or_else(|| anyhow::anyhow!("undecodable frame during absorb"))?;
                let sk = *subkeys
                    .entry((frame.header.routing_key.clone(), frame.header.key_version))
                    .or_insert_with(|| {
                        derive_subkey(
                            &key,
                            &epoch,
                            &frame.header.routing_key,
                            frame.header.key_version,
                        )
                    });
                let pt = decrypt_frame(&sk, hash, &frame, raw)
                    .map_err(|e| anyhow::anyhow!("absorb decrypt: {e}"))?;
                items.push((
                    frame.header.offset,
                    HistRecord {
                        ts: frame.header.ts_ms,
                        key_version: frame.header.key_version,
                        routing_key: frame.header.routing_key,
                        payload: Bytes::from(pt),
                    },
                ));
            }
            // A byte-truncated window breaks offset contiguity past its
            // last frame: stop here; the boundary advances to what we have.
            let complete = chunk.last_offset.map(|l| l + 1 >= win_end).unwrap_or(false);
            if !complete || pass_bytes >= self.cfg.pass_bytes {
                break;
            }
        }
        drop(window_reads);
        if items.is_empty() {
            return Ok(true);
        }
        let absorbed_upto = items.last().map(|(o, _)| o + 1).unwrap_or(upto);

        // Open the history DB maintenance-free, bulk write, explicit flush
        // (F2), close.
        let db = Db::builder(history_db_path(hash).as_str(), self.data_store.clone())
            .with_settings(history_settings())
            .with_db_cache(history_cache())
            .with_block_transformer(Arc::new(AesBlockTransformer::new(&key)))
            .build()
            .await?;
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
        db.close().await?;

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
    let reader = DbReader::builder(history_db_path(hash).as_str(), data_store.clone())
        .with_block_transformer(Arc::new(AesBlockTransformer::new(key)))
        .build()
        .await?;
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
}
