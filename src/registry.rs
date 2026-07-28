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
    /// Profile kind; None = "generic".
    #[serde(default)]
    pub profile: Option<String>,
    /// Configured content type (create-time config; appends must match).
    #[serde(default = "default_content_type")]
    pub content_type: String,
    /// Raw TTL seconds as configured (config-compare + HEAD reporting).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    /// Ordering contract: None/"total" = single totally ordered sequence
    /// (default; unchanged semantics); "per-key" = segmented per-routing-key
    /// order (PER-KEY-ORDERING.md).
    #[serde(default)]
    pub ordering: Option<String>,
    /// Segment count for per-key streams (v1: static, power of two).
    #[serde(default)]
    pub segment_count: u32,
    /// Queue profile: deliveries before a message is settled to the $dlq
    /// routing-key view (default 5).
    #[serde(default)]
    pub queue_max_deliveries: Option<u32>,
    /// Fingerprint of the touch capability token (state-protocol streams):
    /// authorizes /touch/* without granting payload decryption.
    #[serde(default)]
    pub touch_token_fingerprint: Option<String>,
    /// Pinned touch templates (state-protocol): the stream's query families,
    /// declared at creation, durable, loaded when the journal opens. There
    /// is no dynamic template state to lose on restarts or moves.
    #[serde(default)]
    pub touch_templates: Vec<PinnedTemplate>,
    /// Wait-URL signing key (hex, state-protocol): lets the origin verify
    /// the `sig` capability in collapsible wait URLs. Scoped strictly below
    /// the touch token (observation-forging at worst, never decryption).
    #[serde(default)]
    pub touch_sig_key: Option<String>,
    /// Pravega-style auto-scaling (SCALING.md): per-key streams only.
    /// When true, routing keys map through a dynamic segment map to
    /// internal segment streams ("name#segN"); the scaler splits/merges
    /// segments against the per-segment service limits.
    #[serde(default)]
    pub scaling: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinnedTemplate {
    pub entity: String,
    pub fields: Vec<String>,
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

    pub fn is_per_key(&self) -> bool {
        self.ordering.as_deref() == Some("per-key")
    }

    /// Sub-stream identity of one segment of a per-key stream.
    pub fn segment_hash(&self, ordinal: u32) -> [u8; 16] {
        crate::crypto::stream_hash(&format!(
            "{}\u{0}seg\u{0}{}\u{0}{}",
            self.name, ordinal, self.stream_epoch
        ))
    }

    /// Routing key -> segment ordinal (top bits of SHA-256(rk)).
    pub fn segment_for(&self, routing_key: &str) -> u32 {
        let n = self.segment_count.max(1);
        if n == 1 {
            return 0;
        }
        let h = crate::crypto::stream_hash(routing_key);
        let top = u32::from_be_bytes([h[0], h[1], h[2], h[3]]);
        top >> (32 - n.trailing_zeros())
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
        let fetched = match self.store.get_opts(&desc_path(name), opts(etag_sent.clone())).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                let raw = r.bytes().await?;
                // Fail CLOSED on a corrupt descriptor: treating it as absent
                // would let a create/recreate path overwrite a live stream's
                // identity (key epoch, incarnation) — worse than an error.
                match serde_json::from_slice::<StreamDesc>(&raw) {
                    Ok(d) => (Some(d), etag),
                    Err(e) => {
                        return Err(object_store::Error::Generic {
                            store: "registry",
                            source: format!("corrupt descriptor for {name:?}: {e}").into(),
                        });
                    }
                }
            }
            Err(object_store::Error::NotModified { .. }) => (cached_desc, etag_sent),
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(e) => return Err(e),
        };
        self.cache.lock().unwrap().insert(
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
                self.cache.lock().unwrap().insert(
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
            let current: StreamDesc =
                serde_json::from_slice(&raw).map_err(|e| object_store::Error::Generic {
                    store: "registry",
                    source: format!("corrupt descriptor for {name:?}: {e}").into(),
                })?;
            if !still_dead(&current) {
                self.cache.lock().unwrap().insert(
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
            let mut desc: StreamDesc =
                serde_json::from_slice(&raw).map_err(|e| object_store::Error::Generic {
                    store: "registry",
                    source: format!("corrupt descriptor during update: {e}").into(),
                })?;
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
                    if let Ok(d) = serde_json::from_slice::<StreamDesc>(&raw) {
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
            profile: None,
            content_type: "application/json".into(),
            ttl_secs: None,
            ordering: None,
            segment_count: 0,
            queue_max_deliveries: None,
            touch_token_fingerprint: None,
            touch_templates: Vec::new(),
            touch_sig_key: None,
            scaling: false,
        }
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
        assert_eq!(counting.gets.load(Relaxed), 0, "warm read touched the store");

        // TTL expiry on an unchanged descriptor: exactly one conditional
        // GET, answered 304, still serving the cached descriptor.
        reg.expire_for_tests("s");
        assert_eq!(reg.get("s").await.unwrap().unwrap().stream_epoch, "e1");
        assert_eq!(counting.conditional.load(Relaxed), 1, "refresh was not conditional");
        assert_eq!(counting.not_modified.load(Relaxed), 1, "refresh paid for a body");

        // The 304 renews the TTL: the next read is a cache hit again.
        let gets_now = counting.gets.load(Relaxed);
        assert_eq!(reg.get("s").await.unwrap().unwrap().stream_epoch, "e1");
        assert_eq!(counting.gets.load(Relaxed), gets_now, "304 did not renew the TTL");

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
}
