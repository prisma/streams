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

use std::collections::{HashMap, VecDeque};
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
use crate::registry::StorageHash;
use crate::shard::{AbsorbSignal, ShardEngine, read_frames_range};

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

pub fn history_db_path(hash: &StorageHash) -> String {
    format!("streams/{}", hex(hash))
}

// ---- key cache (transient; fed by keyed requests) ----

pub struct KeyEntry {
    pub key: StreamKey,
    pub epoch: [u8; 16],
    pub at: Instant,
    generation: u64,
}

pub struct KeyCache {
    inner: Mutex<KeyCacheInner>,
    ttl: Duration,
    capacity: usize,
}

struct KeyCacheInner {
    map: HashMap<StorageHash, KeyEntry>,
    order: VecDeque<(StorageHash, u64)>,
    next_generation: u64,
}

const DEFAULT_KEY_CACHE_CAPACITY: usize = 50_000;

impl Drop for KeyEntry {
    fn drop(&mut self) {
        // Best-effort cache hygiene: an evicted key must not remain in the
        // allocator's reusable memory. Request-local clones have their own
        // lifetime and are intentionally unaffected.
        self.key.0.fill(0);
        self.epoch.fill(0);
    }
}

impl Default for KeyCache {
    fn default() -> Self {
        Self::with_limits(DEFAULT_KEY_CACHE_CAPACITY, Duration::from_secs(900))
    }
}

impl KeyCache {
    fn with_limits(capacity: usize, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(KeyCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                next_generation: 0,
            }),
            ttl,
            capacity: capacity.max(1),
        }
    }

    pub fn put(&self, hash: StorageHash, key: StreamKey, epoch: [u8; 16]) {
        let mut inner = self.inner.lock().unwrap();
        inner.next_generation = inner.next_generation.wrapping_add(1);
        let generation = inner.next_generation;
        inner.map.insert(
            hash,
            KeyEntry {
                key,
                epoch,
                at: Instant::now(),
                generation,
            },
        );
        inner.order.push_back((hash, generation));
        while inner.map.len() > self.capacity {
            let Some((candidate, queued_generation)) = inner.order.pop_front() else {
                break;
            };
            if inner
                .map
                .get(&candidate)
                .is_some_and(|entry| entry.generation == queued_generation)
            {
                inner.map.remove(&candidate);
            }
        }
        if inner.order.len() > self.capacity.saturating_mul(4) {
            let live: HashMap<StorageHash, u64> = inner
                .map
                .iter()
                .map(|(hash, entry)| (*hash, entry.generation))
                .collect();
            inner
                .order
                .retain(|(hash, generation)| live.get(hash) == Some(generation));
        }
    }

    pub fn get(&self, hash: &StorageHash) -> Option<(StreamKey, [u8; 16])> {
        let mut inner = self.inner.lock().unwrap();
        let expired = inner.map.get(hash)?.at.elapsed() > self.ttl;
        if expired {
            inner.map.remove(hash);
            return None;
        }
        let e = inner.map.get(hash)?;
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
    /// Maximum encrypted history SST size accepted by the writer-verified
    /// integrity ledger and the continuous cell scrubber.
    pub integrity_max_object_bytes: u64,
}

impl Default for AbsorberConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 4 * 1024 * 1024,
            threshold_age: Duration::from_secs(300),
            tick: Duration::from_secs(5),
            batch_puts: 4_096,
            pass_bytes: 256 * 1024 * 1024,
            integrity_max_object_bytes: 256 * 1024 * 1024,
        }
    }
}

struct PendingAbsorb {
    bytes: u64,
    since: Instant,
}

pub struct Absorber {
    data_store: Arc<dyn ObjectStore>,
    integrity_store: Arc<dyn ObjectStore>,
    shard: Arc<ShardEngine>,
    keys: Arc<KeyCache>,
    cfg: AbsorberConfig,
}

impl Absorber {
    pub fn start(
        data_store: Arc<dyn ObjectStore>,
        integrity_store: Arc<dyn ObjectStore>,
        shard: Arc<ShardEngine>,
        keys: Arc<KeyCache>,
        cfg: AbsorberConfig,
        mut rx: mpsc::Receiver<AbsorbSignal>,
    ) {
        let absorber = Absorber {
            data_store,
            integrity_store,
            shard,
            keys,
            cfg,
        };
        tokio::spawn(async move {
            let mut pending: HashMap<StorageHash, PendingAbsorb> = HashMap::new();
            let mut tick = tokio::time::interval(absorber.cfg.tick);
            loop {
                tokio::select! {
                    sig = rx.recv() => {
                        let Some(sig) = sig else { return };
                        let e = pending.entry(sig.hash).or_insert(PendingAbsorb {
                            bytes: 0,
                            since: Instant::now(),
                        });
                        e.bytes += sig.appended_bytes;
                    }
                    _ = tick.tick() => {
                        let due: Vec<StorageHash> = pending
                            .iter()
                            .filter(|(_, p)| {
                                p.bytes >= absorber.cfg.threshold_bytes
                                    || p.since.elapsed() >= absorber.cfg.threshold_age
                            })
                            .map(|(h, _)| *h)
                            .collect();
                        for hash in due {
                            match absorber.absorb_one(&hash).await {
                                Ok(absorbed) => {
                                    if absorbed {
                                        pending.remove(&hash);
                                    }
                                    // key missing: keep pending; retried when
                                    // the next keyed request arrives.
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "absorb failed for {}: {e}",
                                        hex(&hash)
                                    );
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Returns Ok(false) if the stream key isn't available.
    async fn absorb_one(&self, hash: &StorageHash) -> anyhow::Result<bool> {
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
        let transformer = Arc::new(AesBlockTransformer::new(&key));
        let path = history_db_path(hash);
        let db = Db::builder(path.as_str(), self.data_store.clone())
            .with_settings(history_settings())
            .with_db_cache(history_cache())
            .with_block_transformer(transformer.clone())
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
        streams_slate::primary_scrub::record_history_baselines(
            self.integrity_store.clone(),
            self.data_store.clone(),
            &path,
            self.cfg.integrity_max_object_bytes,
            transformer,
        )
        .await?;

        // Advance the readers' boundary + trim (deferred) in the shard log.
        self.shard.submit_absorbed(*hash, absorbed_upto).await;
        tracing::info!(
            "absorbed {} records into {} (upto {})",
            items.len(),
            path,
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
    hash: &StorageHash,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_cache_is_bounded() {
        let cache = KeyCache::with_limits(2, Duration::from_secs(60));
        cache.put([1; 32], StreamKey([1; 32]), [1; 16]);
        cache.put([2; 32], StreamKey([2; 32]), [2; 16]);
        cache.put([3; 32], StreamKey([3; 32]), [3; 16]);
        assert!(cache.get(&[1; 32]).is_none());
        assert_eq!(cache.get(&[2; 32]).unwrap().0.0, [2; 32]);
        assert_eq!(cache.get(&[3; 32]).unwrap().0.0, [3; 32]);
        assert_eq!(cache.inner.lock().unwrap().map.len(), 2);
    }

    #[test]
    fn expired_keys_are_removed() {
        let cache = KeyCache::with_limits(2, Duration::ZERO);
        cache.put([1; 32], StreamKey([1; 32]), [1; 16]);
        assert!(cache.get(&[1; 32]).is_none());
        assert!(cache.inner.lock().unwrap().map.is_empty());
    }
}
