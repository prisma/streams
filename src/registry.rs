//! Ops-bucket control plane: stream registry (CAS'd JSON descriptors, D18/D21)
//! and the dynamic shard topology (D3, §3.2).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::crypto::{hex, stream_hash};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDesc {
    pub name: String,
    /// 16-byte hex; minted per creation, bound into HKDF (V9 mandate).
    pub stream_epoch: String,
    /// One-way key fingerprint; wrong-key requests are rejected with 403.
    pub key_fingerprint: String,
    pub created_ms: i64,
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
    #[serde(default)]
    pub deleted: bool,
    /// Configured content type (create-time config; appends must match).
    #[serde(default = "default_content_type")]
    pub content_type: String,
    /// Raw TTL seconds as configured (config-compare + HEAD reporting).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    /// ROUTING-V3: the descriptor-resident segment map. `None` is the
    /// implicit single-segment map — segment 0 covers the whole
    /// keyspace and its engine identity IS `storage_hash()`, so a fresh
    /// stream (and every existing total-order stream) carries zero map
    /// bytes and pays zero extra requests. Materialized by the first
    /// split (CAS on the registry object). See docs/ROUTING-V3.md §2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segments: Option<crate::segmap::SegmentMap>,
    /// Product-surface vNext (spec Stage 1/3): the collection's durable
    /// seal state. Monotonic; set only through the seal lifecycle.
    #[serde(default)]
    pub sealed: bool,
    /// Immutable watch definitions (spec Stage 2/7). Empty for streams
    /// created without watches; JSON streams only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch_definitions: Vec<WatchDefinition>,
    /// Storage-layout generation (spec: pre-launch clean switch). New
    /// descriptors write LAYOUT_VERSION; a reader that finds any OTHER
    /// value — including 0, the serde default every pre-cutover
    /// descriptor deserializes to — refuses the namespace with
    /// `unsupported_storage_layout` instead of decoding, translating,
    /// or rewriting it.
    #[serde(default)]
    pub layout_version: u32,
}

/// The storage-layout generation this binary writes and the ONLY one it
/// reads. There are no layout bridges: opening a namespace written by a
/// different layout is refused (pre-launch hard cutover).
pub const LAYOUT_VERSION: u32 = 3;

/// One immutable watch definition (spec Stage 2 §3.2): a name plus the
/// ordered JSON-pointer fields whose canonical extracted values derive
/// the watch key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchDefinition {
    pub name: String,
    pub fields: Vec<String>,
}

/// One resolved append/read target under the unified routing model:
/// which segment of the stream owns a routing key right now.
#[derive(Debug, Clone, PartialEq)]
pub struct SegRoute {
    pub seg_id: u32,
    /// Engine identity (record keyspace / history `inc` hash).
    pub identity: [u8; 16],
    /// Shard-routing hash: which shard's engine takes the write. A
    /// segment's persisted route_hash wins; zeros (and the implicit
    /// map) mean the parent stream's default route.
    pub shard_route: [u8; 16],
    /// The segment is sealed (mid-transition): the caller refreshes the
    /// descriptor once and re-resolves before erroring.
    pub sealed: bool,
    /// The routing key's fixed-point position (sketch feeding + splits).
    pub point: u64,
    /// The routing key's 128-bit hash (postings/sketch identity) —
    /// computed once here so hot paths never re-hash.
    pub key_hash: crate::crypto::RoutingKeyHash,
    /// This segment's key-point range (sketch bin domain).
    pub lo: u64,
    pub hi: u64,
}

/// Decode a stored descriptor, enforcing the pre-launch clean-switch
/// layout gate: any layout_version other than LAYOUT_VERSION — including
/// 0, which every pre-cutover descriptor deserializes to — refuses the
/// namespace rather than decoding it (spec §0: no legacy decoders).
pub(crate) fn decode_desc(raw: &[u8]) -> Result<StreamDesc, object_store::Error> {
    let d: StreamDesc = serde_json::from_slice(raw).map_err(|e| object_store::Error::Generic {
        store: "registry",
        source: format!("descriptor parse: {e}").into(),
    })?;
    if d.layout_version != LAYOUT_VERSION {
        return Err(object_store::Error::Generic {
            store: "registry",
            source: format!(
                "unsupported_storage_layout: descriptor '{}' has layout {} (this binary \
                 reads only {}); this namespace was written by a different implementation \
                 — deploy against a fresh bucket/PATH_PREFIX",
                d.name, d.layout_version, LAYOUT_VERSION
            )
            .into(),
        });
    }
    Ok(d)
}

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

impl StreamDesc {
    pub fn epoch_bytes(&self) -> Option<[u8; 16]> {
        crate::crypto::unhex(&self.stream_epoch)?.try_into().ok()
    }

    /// Storage identity: derived from (name, stream_epoch) so a recreated
    /// stream gets a fresh keyspace — full delete/recreate isolation.
    pub fn storage_hash(&self) -> [u8; 16] {
        crate::crypto::stream_hash(&format!("{}\u{0}inc\u{0}{}", self.name, self.stream_epoch))
    }

    pub fn is_json(&self) -> bool {
        media_type(&self.content_type) == "application/json"
    }

    /// Fixed-point position of a routing key in the [0,1) keyspace —
    /// the coordinate the segment map partitions. The empty/default key
    /// is an ordinary key at stream_hash("")'s position.
    pub fn key_point(routing_key: &str) -> u64 {
        let h = crate::crypto::stream_hash(routing_key);
        u64::from_be_bytes(h[..8].try_into().expect("hash prefix"))
    }

    /// Engine identity of a dynamic-map segment (ROUTING-V3 §2).
    /// Segment 0 is ALWAYS `storage_hash()` — that equality is what
    /// makes every pre-v3 total-order stream already-migrated, with its
    /// whole history as segment 0 and zero data movement.
    pub fn dynamic_segment_identity(&self, seg_id: u32) -> [u8; 16] {
        if seg_id == 0 {
            return self.storage_hash();
        }
        crate::crypto::stream_hash(&format!(
            "{}\u{0}segid\u{0}{}\u{0}{}",
            self.name, seg_id, self.stream_epoch
        ))
    }

    /// THE unified routing resolution (ROUTING-V3 §1-2): routing key →
    /// the segment that owns it right now. Handles every layout:
    ///
    /// - descriptor-resident dynamic map (`segments: Some`) — the v3
    ///   model; selects the live segment containing the key point;
    /// - everything else — the implicit single-segment map: segment 0,
    ///   identity `storage_hash()`, parent's shard route. This arm IS
    ///   the old total-order behavior, unchanged to the byte.
    ///
    /// Legacy `scaling` descriptors are routed by their child-stream
    /// machinery upstream of this call until PR4 folds them in; this
    /// function never sees their parent appends.
    /// The physical shard route of one segment: its persisted
    /// route_hash when assigned (split children get real, independent
    /// routes — review blocker 1: a split must add capacity, not just
    /// lineage), the shard-prefix hash for prefix-pinned segments, and
    /// the parent stream route for the implicit/seg-0 case.
    pub fn segment_route(&self, seg: &crate::segmap::SegmentDesc) -> [u8; 16] {
        if seg.route_hash != [0u8; 16] {
            seg.route_hash
        } else if seg.shard_prefix.is_empty() {
            crate::crypto::stream_hash(&self.name)
        } else {
            crate::crypto::stream_hash(&seg.shard_prefix)
        }
    }

    /// segment_route by id; unknown ids fall back to the parent route
    /// (the implicit single segment).
    pub fn segment_route_by_id(&self, seg_id: u32) -> [u8; 16] {
        self.segments
            .as_ref()
            .and_then(|m| m.get(seg_id))
            .map(|sg| self.segment_route(sg))
            .unwrap_or_else(|| crate::crypto::stream_hash(&self.name))
    }

    pub fn resolve_segment(&self, routing_key: &str) -> SegRoute {
        let parent_route = crate::crypto::stream_hash(&self.name);
        let key_hash = crate::crypto::RoutingKeyHash::of(routing_key);
        let point = u64::from_be_bytes(key_hash.0[..8].try_into().expect("hash prefix"));
        if let Some(map) = &self.segments {
            // Live segment containing the point; a well-formed map has
            // exactly one. A malformed map (no live cover) falls back to
            // sealed-any-cover so the caller's refresh path can heal.
            let live = map
                .segments
                .iter()
                .find(|s| s.is_live() && s.contains(point));
            // No live cover = mid-transition (a seal published before
            // its successors, or a scaler died between the two): pick
            // the NEWEST sealed cover — the deepest lineage point, the
            // one whose successor the refresh will reveal — never a
            // long-superseded ancestor.
            let chosen = live.or_else(|| {
                map.segments
                    .iter()
                    .filter(|s| s.contains(point))
                    .max_by_key(|s| (s.created_ms, s.seg_id))
            });
            if let Some(seg) = chosen {
                let shard_route = self.segment_route(seg);
                return SegRoute {
                    seg_id: seg.seg_id,
                    identity: self.dynamic_segment_identity(seg.seg_id),
                    shard_route,
                    sealed: !seg.is_live(),
                    point,
                    key_hash,
                    lo: seg.lo,
                    hi: seg.hi,
                };
            }
            // Unreachable for any map save() accepts; fall through to
            // the implicit segment rather than failing the append.
        }
        SegRoute {
            seg_id: 0,
            identity: self.storage_hash(),
            shard_route: parent_route,
            sealed: false,
            point,
            key_hash,
            lo: 0,
            hi: crate::segmap::KEYSPACE_END,
        }
    }
}

/// Media type with parameters stripped, lowercased.
pub fn media_type(ct: &str) -> String {
    ct.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

pub struct Registry {
    store: Arc<dyn ObjectStore>,
    cache: Mutex<HashMap<String, CachedDesc>>,
    cache_ttl: Duration,
}

struct CachedDesc {
    desc: Option<StreamDesc>,
    at: Instant,
    /// Store ETag of the object this entry was read from. TTL refreshes
    /// revalidate with If-None-Match instead of refetching: descriptors
    /// are immutable for the life of an incarnation, so almost every
    /// refresh is a 304 — uncharged on Tigris — instead of a billable
    /// GET (object-store cost review, item 5).
    etag: Option<String>,
}

fn desc_path(name: &str) -> ObjPath {
    // Hash-keyed path: names are arbitrary UTF-8; the descriptor carries the
    // real name. Two hex chars of fan-out keep prefixes listable.
    let h = hex(&stream_hash(name));
    ObjPath::from(format!("registry/by-name/{}/{}.json", &h[..2], h))
}

impl Registry {
    /// Ops-bucket handle (segment maps live beside stream descriptors).
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub fn new(store: Arc<dyn ObjectStore>) -> Registry {
        Registry {
            store,
            cache: Mutex::new(HashMap::new()),
            cache_ttl: Duration::from_secs(5),
        }
    }

    /// Bounded cache insert: the descriptor cache previously grew with
    /// every distinct name ever touched (static-audit memory finding —
    /// creates alone put 100k entries in it). At the cap, expired
    /// entries purge first (TTL is seconds, so this is almost always
    /// enough), then the oldest entry falls out.
    fn cache_insert(&self, name: String, entry: CachedDesc) {
        const REGISTRY_CACHE_MAX: usize = 65_536;
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= REGISTRY_CACHE_MAX && !cache.contains_key(&name) {
            let ttl = self.cache_ttl;
            cache.retain(|_, e| e.at.elapsed() < ttl);
            if cache.len() >= REGISTRY_CACHE_MAX {
                if let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, e)| e.at)
                    .map(|(n, _)| n.clone())
                {
                    cache.remove(&oldest);
                }
            }
        }
        cache.insert(name, entry);
    }

    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Test-only: plant a descriptor in the cache as if this instance
    /// had read it moments ago — the cross-instance stale-descriptor
    /// shape (another instance CAS'd a transition we have not seen).
    #[cfg(test)]
    pub fn test_poison_cache(&self, name: &str, desc: StreamDesc) {
        self.cache_insert(
            name.to_string(),
            CachedDesc {
                desc: Some(desc),
                at: Instant::now(),
                etag: None,
            },
        );
    }

    pub async fn get(&self, name: &str) -> Result<Option<StreamDesc>, object_store::Error> {
        let revalidate = {
            let cache = self.cache.lock().unwrap();
            match cache.get(name) {
                Some(e) if e.at.elapsed() < self.cache_ttl => return Ok(e.desc.clone()),
                Some(e) => e.etag.clone().map(|t| (t, e.desc.clone())),
                None => None,
            }
        };
        // TTL expired on a descriptor we hold an ETag for: conditional
        // refresh. Unchanged (the overwhelmingly common case — a
        // descriptor changes only on delete/recreate/config update) comes
        // back 304 and only renews the TTL; a real change pays for a body.
        let opts = |etag: Option<String>| object_store::GetOptions {
            if_none_match: etag,
            ..Default::default()
        };
        let (etag_sent, cached_desc) = match revalidate {
            Some((t, d)) => (Some(t), d),
            None => (None, None),
        };
        let fetched = match self
            .store
            .get_opts(&desc_path(name), opts(etag_sent.clone()))
            .await
        {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                let raw = r.bytes().await?;
                // Fail CLOSED on a corrupt descriptor: treating it as absent
                // would let a create/recreate path overwrite a live stream's
                // identity (key epoch, incarnation) — worse than an error.
                match decode_desc(&raw) {
                    Ok(d) => (Some(d), etag),
                    Err(e) => {
                        return Err(object_store::Error::Generic {
                            store: "registry",
                            source: format!("descriptor for {name:?}: {e}").into(),
                        });
                    }
                }
            }
            Err(object_store::Error::NotModified { .. }) => (cached_desc, etag_sent),
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(e) => return Err(e),
        };
        self.cache_insert(
            name.to_string(),
            CachedDesc {
                desc: fetched.0.clone(),
                at: Instant::now(),
                etag: fetched.1,
            },
        );
        Ok(fetched.0)
    }

    /// Create a descriptor; on a lost CAS race, return the winner's.
    pub async fn create(
        &self,
        desc: StreamDesc,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        let raw = serde_json::to_vec(&desc).expect("desc json");
        match self
            .store
            .put_opts(
                &desc_path(&desc.name),
                PutPayload::from(raw),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(put) => {
                self.cache_insert(
                    desc.name.clone(),
                    CachedDesc {
                        desc: Some(desc.clone()),
                        at: Instant::now(),
                        etag: put.e_tag,
                    },
                );
                Ok((true, desc))
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.invalidate(&desc.name);
                let existing =
                    self.get(&desc.name)
                        .await?
                        .ok_or_else(|| object_store::Error::NotFound {
                            path: desc.name.clone(),
                            source: "raced create then missing".into(),
                        })?;
                Ok((false, existing))
            }
            Err(e) => Err(e),
        }
    }

    /// Replace a dead (deleted/expired) descriptor with a fresh incarnation.
    /// Predicated CAS: the replacement applies only while the current
    /// descriptor is still dead per `still_dead`. Racing recreators get
    /// exactly one winner; a loser observes the winner's live descriptor
    /// (`(false, winner)`) instead of overwriting its incarnation.
    pub async fn recreate(
        &self,
        name: &str,
        fresh: StreamDesc,
        still_dead: impl Fn(&StreamDesc) -> bool,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        for _ in 0..5 {
            let got = match self.store.get(&desc_path(name)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(object_store::Error::NotFound {
                        path: name.to_string(),
                        source: "recreate on missing descriptor".into(),
                    });
                }
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let current: StreamDesc = decode_desc(&raw)?;
            if !still_dead(&current) {
                self.cache_insert(
                    name.to_string(),
                    CachedDesc {
                        desc: Some(current.clone()),
                        at: Instant::now(),
                        etag: etag.clone(),
                    },
                );
                return Ok((false, current));
            }
            let body = serde_json::to_vec(&fresh).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(name),
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(name);
                    return Ok((true, fresh));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "descriptor CAS retries exhausted".into(),
        })
    }

    /// CAS-update the descriptor (delete = tombstone).
    pub async fn update<F: Fn(&mut StreamDesc)>(
        &self,
        name: &str,
        apply: F,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let got = match self.store.get(&desc_path(name)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            // Fail CLOSED on corruption (was: treated as missing).
            let mut desc: StreamDesc = decode_desc(&raw)?;
            apply(&mut desc);
            let body = serde_json::to_vec(&desc).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(name),
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(name);
                    return Ok(Some(desc));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "descriptor CAS retries exhausted".into(),
        })
    }

    /// CAS-mutate a live descriptor (ROUTING-V3 scaler transitions):
    /// fresh read (bypassing the cache) → mutate → conditional PUT on
    /// the store etag. Returns Ok(true) when OUR write landed, Ok(false)
    /// when `mutate` declined or the descriptor is gone; etag conflicts
    /// error and the caller re-evaluates (another instance decided
    /// first — the same discipline segmap.json used).
    pub async fn cas_update(
        &self,
        name: &str,
        mutate: impl FnOnce(&mut StreamDesc) -> bool,
    ) -> anyhow::Result<bool> {
        let path = desc_path(name);
        let got = match self.store.get(&path).await {
            Ok(g) => g,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let etag = got.meta.e_tag.clone();
        let bytes = got.bytes().await?;
        let mut desc: StreamDesc = decode_desc(&bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
        if desc.deleted {
            return Ok(false);
        }
        if !mutate(&mut desc) {
            return Ok(false);
        }
        let body = serde_json::to_vec(&desc)?;
        let mode = match etag {
            Some(e_tag) => PutMode::Update(UpdateVersion {
                e_tag: Some(e_tag),
                version: None,
            }),
            None => PutMode::Overwrite,
        };
        self.store
            .put_opts(
                &path,
                PutPayload::from(body),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await?;
        self.invalidate(name);
        Ok(true)
    }

    pub fn invalidate(&self, name: &str) {
        self.cache.lock().unwrap().remove(name);
    }

    /// Force a cached entry past its TTL so tests can exercise the
    /// refresh path without sleeping through the real TTL.
    #[cfg(test)]
    fn expire_for_tests(&self, name: &str) {
        if let Some(e) = self.cache.lock().unwrap().get_mut(name) {
            e.at -= self.cache_ttl + Duration::from_secs(1);
        }
    }

    pub async fn list(&self, limit: usize) -> Result<Vec<StreamDesc>, object_store::Error> {
        use futures_util::TryStreamExt;
        let prefix = ObjPath::from("registry/by-name");
        let mut out = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await? {
            if out.len() >= limit {
                break;
            }
            if let Ok(r) = self.store.get(&meta.location).await {
                if let Ok(raw) = r.bytes().await {
                    if let Ok(d) = decode_desc(&raw) {
                        if !d.deleted {
                            out.push(d);
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

// ---- shard topology ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub version: u64,
    /// Complete binary prefix code over the stream-hash bit space. "" = one
    /// shard covering everything.
    pub shards: Vec<String>,
}

const TOPOLOGY_PATH: &str = "topology.json";

pub async fn load_or_init_topology(
    store: &Arc<dyn ObjectStore>,
    initial_shards: usize,
) -> Result<Topology, object_store::Error> {
    let path = ObjPath::from(TOPOLOGY_PATH);
    match store.get(&path).await {
        Ok(r) => {
            let raw = r.bytes().await?;
            // Fail CLOSED: a corrupt topology must abort boot. Panicking is
            // wrong (crash loop) and treating it as missing would be far
            // worse (re-initializing re-shards the whole keyspace).
            return serde_json::from_slice(&raw).map_err(|e| object_store::Error::Generic {
                store: "registry",
                source: format!("corrupt topology object: {e}").into(),
            });
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    let bits = (initial_shards.max(1) as f64).log2() as usize;
    assert_eq!(
        1 << bits,
        initial_shards.max(1),
        "initial shards must be a power of two"
    );
    let shards: Vec<String> = if bits == 0 {
        vec![String::new()]
    } else {
        (0..initial_shards)
            .map(|i| format!("{:0width$b}", i, width = bits))
            .collect()
    };
    let topo = Topology { version: 1, shards };
    let raw = serde_json::to_vec(&topo).expect("topology json");
    match store
        .put_opts(
            &path,
            PutPayload::from(raw),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(topo),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let r = store.get(&path).await?;
            let raw = r.bytes().await?;
            serde_json::from_slice(&raw).map_err(|e| object_store::Error::Generic {
                store: "registry",
                source: format!("corrupt topology object: {e}").into(),
            })
        }
        Err(e) => Err(e),
    }
}

fn hash_bits(hash: &[u8; 16]) -> String {
    let mut bits = String::with_capacity(24);
    for byte in hash.iter().take(3) {
        bits.push_str(&format!("{byte:08b}"));
    }
    bits
}

/// Longest-prefix match of the stream hash's leading bits against the shard
/// set. `shards` must form a complete prefix code.
pub fn shard_for_hash(shards: &[String], hash: &[u8; 16]) -> String {
    let bits = hash_bits(hash);
    shards
        .iter()
        .filter(|p| bits.starts_with(p.as_str()))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

/// Does `hash` fall inside the shard identified by bit-prefix `prefix`?
pub fn shard_prefix_matches(prefix: &str, hash: &[u8; 16]) -> bool {
    hash_bits(hash).starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    fn desc(name: &str, epoch: &str, deleted: bool) -> StreamDesc {
        StreamDesc {
            name: name.into(),
            stream_epoch: epoch.into(),
            key_fingerprint: "fp".into(),
            created_ms: 1,
            expires_at_ms: None,
            deleted,
            content_type: "application/json".into(),
            ttl_secs: None,
            segments: None,
            sealed: false,
            watch_definitions: Vec::new(),
            layout_version: LAYOUT_VERSION,
        }
    }

    /// Pre-launch clean switch: a descriptor written by the previous
    /// experimental layout (no layout_version, or any other value) is
    /// REFUSED — never decoded, translated, or rewritten.
    #[tokio::test]
    async fn layout_gate_refuses_foreign_namespaces() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(store.clone());
        // Old-shape descriptor: valid JSON, no layout_version field.
        let old = serde_json::json!({
            "name": "legacy",
            "stream_epoch": "00000000000000000000000000000000",
            "key_fingerprint": "fp",
            "created_ms": 1,
            "profile": "queue",
            "content_type": "application/json",
        });
        put_raw(&store, "legacy", old.to_string().as_bytes()).await;
        let err = reg.get("legacy").await.expect_err("gate must refuse");
        assert!(
            err.to_string().contains("unsupported_storage_layout"),
            "wrong refusal: {err}"
        );
        // A current-layout descriptor round-trips.
        let d = desc("fresh", "00000000000000000000000000000001", false);
        put_raw(&store, "fresh", &serde_json::to_vec(&d).unwrap()).await;
        assert!(reg.get("fresh").await.unwrap().is_some());
    }

    async fn put_raw(store: &Arc<dyn ObjectStore>, name: &str, body: &[u8]) {
        store
            .put(
                &desc_path(name),
                object_store::PutPayload::from(body.to_vec()),
            )
            .await
            .unwrap();
    }

    /// Wrapper that counts get traffic and how it resolved, so the
    /// conditional-refresh path is provable rather than assumed.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: std::sync::atomic::AtomicU64,
        conditional: std::sync::atomic::AtomicU64,
        not_modified: std::sync::atomic::AtomicU64,
    }
    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            use std::sync::atomic::Ordering::Relaxed;
            self.gets.fetch_add(1, Relaxed);
            if options.if_none_match.is_some() {
                self.conditional.fetch_add(1, Relaxed);
            }
            let r = self.inner.get_opts(location, options).await;
            if matches!(&r, Err(object_store::Error::NotModified { .. })) {
                self.not_modified.fetch_add(1, Relaxed);
            }
            r
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A TTL refresh of an unchanged descriptor must be a conditional GET
    /// answered 304 (uncharged on Tigris), never a billable body fetch —
    /// and a genuinely changed descriptor must still come through.
    #[tokio::test]
    async fn ttl_refresh_of_unchanged_descriptor_is_a_free_304() {
        use std::sync::atomic::Ordering::Relaxed;
        let counting = Arc::new(CountingStore {
            inner: Arc::new(object_store::memory::InMemory::new()),
            gets: Default::default(),
            conditional: Default::default(),
            not_modified: Default::default(),
        });
        let reg = Registry::new(counting.clone());
        let (created, _) = reg.create(desc("s", "e1", false)).await.unwrap();
        assert!(created);

        // Warm read: cache hit, no store traffic at all.
        assert_eq!(reg.get("s").await.unwrap().unwrap().stream_epoch, "e1");
        assert_eq!(
            counting.gets.load(Relaxed),
            0,
            "warm read touched the store"
        );

        // TTL expiry on an unchanged descriptor: exactly one conditional
        // GET, answered 304, still serving the cached descriptor.
        reg.expire_for_tests("s");
        assert_eq!(reg.get("s").await.unwrap().unwrap().stream_epoch, "e1");
        assert_eq!(
            counting.conditional.load(Relaxed),
            1,
            "refresh was not conditional"
        );
        assert_eq!(
            counting.not_modified.load(Relaxed),
            1,
            "refresh paid for a body"
        );

        // The 304 renews the TTL: the next read is a cache hit again.
        let gets_now = counting.gets.load(Relaxed);
        assert_eq!(reg.get("s").await.unwrap().unwrap().stream_epoch, "e1");
        assert_eq!(
            counting.gets.load(Relaxed),
            gets_now,
            "304 did not renew the TTL"
        );

        // A real change (delete tombstone) must come through on the next
        // refresh — the conditional path must never pin a stale view.
        reg.update("s", |d| d.deleted = true).await.unwrap();
        reg.expire_for_tests("s");
        // update() invalidates, so re-prime the cache then expire it.
        assert!(reg.get("s").await.unwrap().unwrap().deleted);
        reg.expire_for_tests("s");
        assert!(reg.get("s").await.unwrap().unwrap().deleted);
    }

    /// A corrupt descriptor must surface as an ERROR — treating it as
    /// absent lets a create/recreate overwrite a live stream's identity.
    #[tokio::test]
    async fn corrupt_descriptor_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(store.clone());
        put_raw(&store, "s1", b"{ not json").await;
        assert!(
            reg.get("s1").await.is_err(),
            "corrupt descriptor returned as absent/ok"
        );
        // update() must also refuse (was: Ok(None), i.e. missing).
        assert!(reg.update("s1", |_| {}).await.is_err());
    }

    /// A corrupt topology must abort boot, never panic and NEVER be treated
    /// as missing (re-initializing re-shards the whole keyspace).
    #[tokio::test]
    async fn corrupt_topology_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &ObjPath::from(TOPOLOGY_PATH),
                object_store::PutPayload::from(b"garbage".to_vec()),
            )
            .await
            .unwrap();
        assert!(load_or_init_topology(&store, 4).await.is_err());
        // The corrupt object must still be there — not replaced by a fresh
        // initialization.
        let raw = store
            .get(&ObjPath::from(TOPOLOGY_PATH))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&raw[..], b"garbage");
    }

    /// Racing recreators of a dead incarnation: exactly one winner; the
    /// loser observes the winner's descriptor instead of overwriting it.
    #[tokio::test]
    async fn recreate_race_has_one_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let reg = Registry::new(store.clone());
        let (created, _) = reg.create(desc("s", "dead", true)).await.unwrap();
        assert!(created);

        let alive = |d: &StreamDesc| !d.deleted;
        let (won_a, got_a) = reg
            .recreate("s", desc("s", "epoch-a", false), |d| !alive(d))
            .await
            .unwrap();
        assert!(won_a, "first recreate must win");
        assert_eq!(got_a.stream_epoch, "epoch-a");

        // Second recreator raced and lost: descriptor is now alive, so the
        // predicate fails and it must observe epoch-a, not install epoch-b.
        let (won_b, got_b) = reg
            .recreate("s", desc("s", "epoch-b", false), |d| !alive(d))
            .await
            .unwrap();
        assert!(!won_b, "second recreate must lose");
        assert_eq!(got_b.stream_epoch, "epoch-a");

        reg.invalidate("s");
        let stored = reg.get("s").await.unwrap().unwrap();
        assert_eq!(stored.stream_epoch, "epoch-a", "loser overwrote the winner");
    }

    // ---- ROUTING-V3 resolution (docs/ROUTING-V3.md §1-2) ------------

    /// Total-order streams (segments: None, no legacy fields) resolve
    /// every key to segment 0 with identity == storage_hash() — the
    /// zero-move migration guarantee.
    #[test]
    fn implicit_map_is_the_old_total_order_layout() {
        let d = desc("t", "e1", false);
        for rk in ["", "a", "user-42", "\u{1F600}"] {
            let r = d.resolve_segment(rk);
            assert_eq!(r.seg_id, 0);
            assert_eq!(r.identity, d.storage_hash());
            assert_eq!(r.shard_route, stream_hash("t"));
            assert!(!r.sealed);
        }
    }

    /// Descriptor-resident dynamic maps: the live segment containing
    /// the key point wins; seg 0's identity is storage_hash() (old data
    /// stays addressable); sealed segments surface `sealed` so the
    /// caller refreshes; a segment with its own shard_prefix routes to
    /// that shard.
    #[test]
    fn dynamic_map_resolution_selects_the_live_cover() {
        let mut d = desc("dyn", "e3", false);
        let mut map = crate::segmap::SegmentMap::initial("", 1);
        // Split the keyspace in half: seg 0 sealed, children 1 and 2.
        let mid = u64::MAX / 2;
        map.segments[0].sealed_ms = Some(2);
        map.segments[0].sealed_next_offset = Some(10);
        map.segments.push(crate::segmap::SegmentDesc {
            seg_id: 1,
            lo: 0,
            hi: mid,
            shard_prefix: String::new(),
            route_hash: [0u8; 16],
            created_ms: 2,
            predecessors: vec![0],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        map.segments.push(crate::segmap::SegmentDesc {
            seg_id: 2,
            lo: mid,
            hi: crate::segmap::KEYSPACE_END,
            shard_prefix: "shard-07".into(),
            route_hash: [0u8; 16],
            created_ms: 2,
            predecessors: vec![0],
            successors: Vec::new(),
            sealed_ms: None,
            sealed_next_offset: None,
        });
        map.next_seg_id = 3;
        map.version = 2;
        d.segments = Some(map);

        assert_eq!(d.dynamic_segment_identity(0), d.storage_hash());

        // Find one key on each side of the midpoint.
        let mut lo_key = None;
        let mut hi_key = None;
        for i in 0..64 {
            let k = format!("k{i}");
            if StreamDesc::key_point(&k) < mid {
                lo_key.get_or_insert(k);
            } else {
                hi_key.get_or_insert(k);
            }
            if lo_key.is_some() && hi_key.is_some() {
                break;
            }
        }
        let (lo_key, hi_key) = (lo_key.unwrap(), hi_key.unwrap());

        let r = d.resolve_segment(&lo_key);
        assert_eq!((r.seg_id, r.sealed), (1, false));
        assert_eq!(r.identity, d.dynamic_segment_identity(1));
        assert_ne!(r.identity, d.storage_hash());
        assert_eq!(
            r.shard_route,
            stream_hash("dyn"),
            "empty prefix = parent route"
        );

        let r = d.resolve_segment(&hi_key);
        assert_eq!((r.seg_id, r.sealed), (2, false));
        assert_eq!(r.shard_route, stream_hash("shard-07"));

        // Seal child 1 with no successor yet (mid-transition crash
        // shape): resolution surfaces sealed=true instead of failing.
        d.segments.as_mut().unwrap().segments[1].sealed_ms = Some(3);
        let r = d.resolve_segment(&lo_key);
        assert_eq!((r.seg_id, r.sealed), (1, true));
    }

    /// Serde: fresh descriptors stay byte-lean (no "segments" key);
    /// pre-v3 JSON (no field at all) parses; a materialized map
    /// round-trips.
    #[test]
    fn descriptor_segments_serde_roundtrip() {
        let d = desc("s", "e4", false);
        let j = serde_json::to_string(&d).unwrap();
        assert!(
            !j.contains("\"segments\""),
            "implicit map must cost zero bytes"
        );
        let legacy: StreamDesc = serde_json::from_str(&j.replace("\"name\"", "\"name\"")).unwrap();
        assert!(legacy.segments.is_none());

        let mut with_map = desc("s2", "e5", false);
        with_map.segments = Some(crate::segmap::SegmentMap::initial("sh", 7));
        let j2 = serde_json::to_string(&with_map).unwrap();
        let back: StreamDesc = serde_json::from_str(&j2).unwrap();
        assert_eq!(back.segments, with_map.segments);
    }
}
