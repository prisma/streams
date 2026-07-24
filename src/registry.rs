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
    cache: Mutex<HashMap<String, (Option<StreamDesc>, Instant)>>,
    cache_ttl: Duration,
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
        if let Some((desc, at)) = self.cache.lock().unwrap().get(name) {
            if at.elapsed() < self.cache_ttl {
                return Ok(desc.clone());
            }
        }
        let fetched = match self.store.get(&desc_path(name)).await {
            Ok(r) => {
                let raw = r.bytes().await?;
                // Fail CLOSED on a corrupt descriptor: treating it as absent
                // would let a create/recreate path overwrite a live stream's
                // identity (key epoch, incarnation) — worse than an error.
                match serde_json::from_slice::<StreamDesc>(&raw) {
                    Ok(d) => Some(d),
                    Err(e) => {
                        return Err(object_store::Error::Generic {
                            store: "registry",
                            source: format!("corrupt descriptor for {name:?}: {e}").into(),
                        });
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => return Err(e),
        };
        self.cache
            .lock()
            .unwrap()
            .insert(name.to_string(), (fetched.clone(), Instant::now()));
        Ok(fetched)
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
            Ok(_) => {
                self.cache
                    .lock()
                    .unwrap()
                    .insert(desc.name.clone(), (Some(desc.clone()), Instant::now()));
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
                self.cache
                    .lock()
                    .unwrap()
                    .insert(name.to_string(), (Some(current.clone()), Instant::now()));
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
