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
    ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase()
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
                serde_json::from_slice::<StreamDesc>(&raw).ok()
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
                let existing = self.get(&desc.name).await?.ok_or_else(|| {
                    object_store::Error::NotFound {
                        path: desc.name.clone(),
                        source: "raced create then missing".into(),
                    }
                })?;
                Ok((false, existing))
            }
            Err(e) => Err(e),
        }
    }

    /// Replace a dead (deleted/expired) descriptor with a fresh incarnation.
    pub async fn recreate(
        &self,
        name: &str,
        fresh: StreamDesc,
    ) -> Result<StreamDesc, object_store::Error> {
        let out = self
            .update(name, |d| {
                *d = fresh.clone();
            })
            .await?;
        out.ok_or_else(|| object_store::Error::NotFound {
            path: name.to_string(),
            source: "recreate on missing descriptor".into(),
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
            let mut desc: StreamDesc = match serde_json::from_slice(&raw) {
                Ok(d) => d,
                Err(_) => return Ok(None),
            };
            apply(&mut desc);
            let body = serde_json::to_vec(&desc).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(name),
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion { e_tag: etag, version: None })),
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
            return Ok(serde_json::from_slice(&raw).expect("topology json"));
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    let bits = (initial_shards.max(1) as f64).log2() as usize;
    assert_eq!(1 << bits, initial_shards.max(1), "initial shards must be a power of two");
    let shards: Vec<String> = if bits == 0 {
        vec![String::new()]
    } else {
        (0..initial_shards).map(|i| format!("{:0width$b}", i, width = bits)).collect()
    };
    let topo = Topology { version: 1, shards };
    let raw = serde_json::to_vec(&topo).expect("topology json");
    match store
        .put_opts(&path, PutPayload::from(raw), PutOptions::from(PutMode::Create))
        .await
    {
        Ok(_) => Ok(topo),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let r = store.get(&path).await?;
            let raw = r.bytes().await?;
            Ok(serde_json::from_slice(&raw).expect("topology json"))
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
