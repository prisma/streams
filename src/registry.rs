//! Ops-bucket control plane: stream registry (CAS'd JSON descriptors, D18/D21)
//! and the dynamic shard topology (D3, §3.2).

use std::collections::{HashMap, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::crypto::{hex, stream_hash};

pub const MAX_FORK_CHILDREN: usize = 10_000;
const MAX_FORK_CHAIN_DEPTH: usize = 1_024;
/// Physical shard-log identity. The stable routing hash is first so a
/// topology prefix is also an exact SlateDB projection range; the second
/// half isolates incarnations (or per-key segments) without changing route.
pub type StorageHash = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDesc {
    /// Tenant isolation owner. Empty only for descriptors written by the
    /// pre-multitenant pilot; those are visible solely to `__legacy__`.
    #[serde(default)]
    pub customer_id: String,
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
    /// Hash of the canonical create-time initial append (body and/or close).
    /// Retries submit it through a reserved durable producer id, while a
    /// different body can never be mistaken for the same create operation.
    #[serde(default)]
    pub initial_request_hash: Option<String>,
    /// Fork creation identity. The inherited prefix is materialized into this
    /// incarnation, but these fields preserve idempotent config comparison and
    /// lifecycle/reference bookkeeping.
    #[serde(default)]
    pub forked_from: Option<String>,
    #[serde(default)]
    pub fork_source_epoch: Option<String>,
    #[serde(default)]
    pub fork_offset: Option<String>,
    #[serde(default)]
    pub fork_sub_offset: Option<u64>,
    /// Idempotent reverse references for soft-delete lifecycle. Materialized
    /// children do not need the source bytes, but the protocol keeps a deleted
    /// source address reserved until its last child is gone.
    #[serde(default)]
    pub fork_children: Vec<String>,
    #[serde(default)]
    pub fork_reference_registered: bool,
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
    pub fn owner(&self) -> &str {
        if self.customer_id.is_empty() {
            "__legacy__"
        } else {
            &self.customer_id
        }
    }

    pub fn epoch_bytes(&self) -> Option<[u8; 16]> {
        crate::crypto::unhex(&self.stream_epoch)?.try_into().ok()
    }

    /// Storage identity: derived from (name, stream_epoch) so a recreated
    /// stream gets a fresh keyspace — full delete/recreate isolation.
    pub fn storage_hash(&self) -> StorageHash {
        let incarnation = if self.customer_id.is_empty() {
            // Storage compatibility for pilot descriptors created before
            // customer identity became part of the descriptor.
            crate::crypto::stream_hash(&format!("{}\u{0}inc\u{0}{}", self.name, self.stream_epoch))
        } else {
            crate::crypto::stream_hash(&format!(
                "{}\u{0}{}\u{0}inc\u{0}{}",
                self.customer_id, self.name, self.stream_epoch
            ))
        };
        composite_storage_hash(self.routing_hash(), incarnation)
    }

    pub fn routing_hash(&self) -> [u8; 16] {
        if self.customer_id.is_empty() {
            crate::crypto::stream_hash(&self.name)
        } else {
            crate::crypto::stream_hash(&format!("{}\u{0}{}", self.customer_id, self.name))
        }
    }

    pub fn is_json(&self) -> bool {
        media_type(&self.content_type) == "application/json"
    }

    pub fn is_per_key(&self) -> bool {
        self.ordering.as_deref() == Some("per-key")
    }

    /// Sub-stream identity of one segment of a per-key stream.
    pub fn segment_hash(&self, ordinal: u32) -> StorageHash {
        let identity = if self.customer_id.is_empty() {
            self.name.clone()
        } else {
            format!("{}\u{0}{}", self.customer_id, self.name)
        };
        let segment = crate::crypto::stream_hash(&format!(
            "{}\u{0}seg\u{0}{}\u{0}{}",
            identity, ordinal, self.stream_epoch
        ));
        composite_storage_hash(self.routing_hash(), segment)
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

fn composite_storage_hash(routing: [u8; 16], incarnation: [u8; 16]) -> StorageHash {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&routing);
    out[16..].copy_from_slice(&incarnation);
    out
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
    cache: Mutex<RegistryCache>,
    cache_ttl: Duration,
}

const DEFAULT_CACHE_CAPACITY: usize = 10_000;

struct CachedDesc {
    value: Option<StreamDesc>,
    inserted_at: Instant,
    generation: u64,
}

/// A bounded FIFO cache. The generation stored in the order queue prevents
/// an old queue entry from evicting a newer value for the same stream.
/// Registry lookups are attacker-influenced, so an unbounded HashMap here is
/// a process-memory denial of service even when every lookup is a miss.
struct RegistryCache {
    entries: HashMap<String, CachedDesc>,
    order: VecDeque<(String, u64)>,
    next_generation: u64,
    capacity: usize,
}

impl RegistryCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            next_generation: 0,
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, name: &str, ttl: Duration) -> Option<Option<StreamDesc>> {
        let entry = self.entries.get(name)?;
        if entry.inserted_at.elapsed() < ttl {
            return Some(entry.value.clone());
        }
        self.entries.remove(name);
        None
    }

    fn insert(&mut self, name: String, value: Option<StreamDesc>) {
        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        self.entries.insert(
            name.clone(),
            CachedDesc {
                value,
                inserted_at: Instant::now(),
                generation,
            },
        );
        self.order.push_back((name, generation));
        while self.entries.len() > self.capacity {
            let Some((candidate, queued_generation)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.generation == queued_generation)
            {
                self.entries.remove(&candidate);
            }
        }
        // Repeated updates of a small key set can otherwise grow the stale
        // order queue forever even though the value map is bounded.
        if self.order.len() > self.capacity.saturating_mul(4) {
            self.order.retain(|(name, generation)| {
                self.entries
                    .get(name)
                    .is_some_and(|entry| entry.generation == *generation)
            });
        }
    }

    fn remove(&mut self, name: &str) {
        self.entries.remove(name);
    }
}

fn desc_path(customer_id: &str, name: &str) -> ObjPath {
    // Hash-keyed path: names are arbitrary UTF-8; the descriptor carries the
    // real name. Two hex chars of fan-out keep prefixes listable.
    let h = hex(&stream_hash(name));
    if customer_id == "__legacy__" {
        ObjPath::from(format!("registry/by-name/{}/{}.json", &h[..2], h))
    } else {
        let customer_hash = hex(&stream_hash(customer_id));
        ObjPath::from(format!(
            "registry/by-customer/{customer_hash}/by-name/{}/{}.json",
            &h[..2],
            h
        ))
    }
}

fn cache_key(customer_id: &str, name: &str) -> String {
    format!("{customer_id}\u{0}{name}")
}

fn validate_descriptor_scope(
    descriptor: &StreamDesc,
    customer_id: &str,
    name: &str,
) -> Result<(), object_store::Error> {
    if descriptor.owner() != customer_id || descriptor.name != name {
        return Err(registry_error(
            "stream descriptor identity does not match its registry path",
        ));
    }
    Ok(())
}

impl Registry {
    pub fn new(store: Arc<dyn ObjectStore>) -> Registry {
        Self::with_cache_capacity(store, DEFAULT_CACHE_CAPACITY)
    }

    fn with_cache_capacity(store: Arc<dyn ObjectStore>, cache_capacity: usize) -> Registry {
        Registry {
            store,
            cache: Mutex::new(RegistryCache::new(cache_capacity)),
            cache_ttl: Duration::from_secs(5),
        }
    }

    pub async fn get(
        &self,
        customer_id: &str,
        name: &str,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        let key = cache_key(customer_id, name);
        if let Some(desc) = self.cache.lock().unwrap().get(&key, self.cache_ttl) {
            return Ok(desc);
        }
        let fetched = match self.store.get(&desc_path(customer_id, name)).await {
            Ok(r) => {
                let raw = r.bytes().await?;
                let descriptor = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
                validate_descriptor_scope(&descriptor, customer_id, name)?;
                Some(descriptor)
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => return Err(e),
        };
        self.cache.lock().unwrap().insert(key, fetched.clone());
        Ok(fetched)
    }

    /// Create a descriptor; on a lost CAS race, return the winner's.
    pub async fn create(
        &self,
        desc: StreamDesc,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        let raw = serde_json::to_vec(&desc).expect("desc json");
        let customer_id = desc.owner().to_string();
        let key = cache_key(&customer_id, &desc.name);
        match self
            .store
            .put_opts(
                &desc_path(&customer_id, &desc.name),
                PutPayload::from(raw),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => {
                self.cache.lock().unwrap().insert(key, Some(desc.clone()));
                Ok((true, desc))
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.invalidate(&customer_id, &desc.name);
                let existing = self.get(&customer_id, &desc.name).await?.ok_or_else(|| {
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

    /// Replace exactly one observed dead descriptor with a fresh incarnation.
    /// A concurrent recreator can win, in which case its descriptor is
    /// returned with `created=false`; it is never overwritten. Comparing the
    /// epoch is what makes delete/recreate a linearizable identity change.
    pub async fn recreate(
        &self,
        customer_id: &str,
        name: &str,
        expected_epoch: &str,
        fresh: StreamDesc,
    ) -> Result<(bool, StreamDesc), object_store::Error> {
        validate_descriptor_scope(&fresh, customer_id, name)?;
        let path = desc_path(customer_id, name);
        for _ in 0..5 {
            let got = match self.store.get(&path).await {
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
            let current = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&current, customer_id, name)?;
            if current.stream_epoch != expected_epoch {
                self.cache
                    .lock()
                    .unwrap()
                    .insert(cache_key(customer_id, name), Some(current.clone()));
                return Ok((false, current));
            }
            let body = serde_json::to_vec(&fresh).map_err(|e| object_store::Error::Generic {
                store: "registry",
                source: Box::new(e),
            })?;
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, name), Some(fresh.clone()));
                    return Ok((true, fresh));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        // Resolve an exhausted race to the winner instead of letting a
        // transient CAS storm turn into an identity overwrite on retry.
        self.invalidate(customer_id, name);
        self.get(customer_id, name)
            .await?
            .map(|d| (false, d))
            .ok_or_else(|| object_store::Error::NotFound {
                path: name.to_string(),
                source: "recreate race ended with missing descriptor".into(),
            })
    }

    /// CAS-update the descriptor (delete = tombstone).
    pub async fn update<F: Fn(&mut StreamDesc)>(
        &self,
        customer_id: &str,
        name: &str,
        apply: F,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let got = match self.store.get(&desc_path(customer_id, name)).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut desc = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&desc, customer_id, name)?;
            apply(&mut desc);
            validate_descriptor_scope(&desc, customer_id, name)?;
            let body = serde_json::to_vec(&desc).expect("desc json");
            match self
                .store
                .put_opts(
                    &desc_path(customer_id, name),
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.invalidate(customer_id, name);
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

    /// Renew a sliding TTL with a descriptor CAS. Reads and writes on TTL
    /// streams are intentionally durable control-plane mutations; HEAD and
    /// fixed Stream-Expires-At streams do not call this path.
    pub async fn renew_ttl(
        &self,
        customer_id: &str,
        name: &str,
        expected_epoch: &str,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut desc = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&desc, customer_id, name)?;
            let now = chrono::Utc::now().timestamp_millis();
            let Some(ttl_secs) = desc.ttl_secs else {
                return Ok(Some(desc));
            };
            if desc.stream_epoch != expected_epoch
                || desc.deleted
                || desc.expires_at_ms.is_some_and(|expires| expires <= now)
            {
                return Ok(Some(desc));
            }
            let ttl_ms = i64::try_from(ttl_secs)
                .ok()
                .and_then(|ttl| ttl.checked_mul(1000))
                .and_then(|ttl| now.checked_add(ttl))
                .ok_or_else(|| registry_error("TTL expiry overflow"))?;
            desc.expires_at_ms = Some(ttl_ms);
            let body = serde_json::to_vec(&desc).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, name), Some(desc.clone()));
                    return Ok(Some(desc));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(object_store::Error::Generic {
            store: "registry",
            source: "TTL renewal CAS retries exhausted".into(),
        })
    }

    pub async fn add_fork_child(
        &self,
        customer_id: &str,
        source_name: &str,
        expected_source_epoch: &str,
        child_name: &str,
    ) -> Result<bool, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, source_name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut source = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&source, customer_id, source_name)?;
            if source.stream_epoch != expected_source_epoch {
                return Ok(false);
            }
            if source.fork_children.iter().any(|child| child == child_name) {
                return Ok(true);
            }
            if source.deleted {
                return Ok(false);
            }
            if source.fork_children.len() >= MAX_FORK_CHILDREN {
                return Err(registry_error("fork child limit reached"));
            }
            source.fork_children.push(child_name.to_string());
            let body = serde_json::to_vec(&source).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, source_name), Some(source));
                    return Ok(true);
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("fork reference CAS retries exhausted"))
    }

    pub async fn remove_fork_child(
        &self,
        customer_id: &str,
        source_name: &str,
        expected_source_epoch: &str,
        child_name: &str,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, source_name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut source = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&source, customer_id, source_name)?;
            if source.stream_epoch != expected_source_epoch {
                return Ok(None);
            }
            let before = source.fork_children.len();
            source.fork_children.retain(|child| child != child_name);
            if source.fork_children.len() == before {
                return Ok(Some(source));
            }
            let body = serde_json::to_vec(&source).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, source_name), Some(source.clone()));
                    return Ok(Some(source));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error(
            "fork reference removal CAS retries exhausted",
        ))
    }

    /// Release a dead leaf's reverse reference and recursively release any
    /// dead ancestors that become childless. An intermediate fork must retain
    /// its own parent reference while it still has a child; otherwise deleting
    /// the middle of a chain makes the root appear fully collected too early.
    pub async fn release_fork_chain(
        &self,
        customer_id: &str,
        leaf: &StreamDesc,
    ) -> Result<(), object_store::Error> {
        if !leaf.fork_children.is_empty() || !leaf.fork_reference_registered {
            return Ok(());
        }

        let mut child_name = leaf.name.clone();
        let mut parent_name = leaf.forked_from.clone();
        let mut parent_epoch = leaf.fork_source_epoch.clone();
        for _ in 0..MAX_FORK_CHAIN_DEPTH {
            let (Some(name), Some(epoch)) = (parent_name.as_deref(), parent_epoch.as_deref())
            else {
                return Ok(());
            };
            let Some(parent) = self
                .remove_fork_child(customer_id, name, epoch, &child_name)
                .await?
            else {
                return Ok(());
            };

            let expired = parent
                .expires_at_ms
                .is_some_and(|expires| expires <= chrono::Utc::now().timestamp_millis());
            if (!parent.deleted && !expired) || !parent.fork_children.is_empty() {
                return Ok(());
            }
            child_name = parent.name.clone();
            parent_name = parent.forked_from.clone();
            parent_epoch = parent.fork_source_epoch.clone();
            if !parent.fork_reference_registered {
                return Ok(());
            }
        }
        Err(registry_error("fork chain exceeds maximum depth"))
    }

    /// Tombstone only the incarnation the caller actually observed.
    pub async fn mark_deleted(
        &self,
        customer_id: &str,
        name: &str,
        expected_epoch: &str,
    ) -> Result<Option<(bool, StreamDesc)>, object_store::Error> {
        for _ in 0..5 {
            let path = desc_path(customer_id, name);
            let got = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
            let etag = got.meta.e_tag.clone();
            let raw = got.bytes().await?;
            let mut desc = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
            validate_descriptor_scope(&desc, customer_id, name)?;
            if desc.stream_epoch != expected_epoch || desc.deleted {
                return Ok(Some((false, desc)));
            }
            desc.deleted = true;
            let body = serde_json::to_vec(&desc).expect("descriptor json");
            match self
                .store
                .put_opts(
                    &path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: etag,
                        version: None,
                    })),
                )
                .await
            {
                Ok(_) => {
                    self.cache
                        .lock()
                        .unwrap()
                        .insert(cache_key(customer_id, name), Some(desc.clone()));
                    return Ok(Some((true, desc)));
                }
                Err(object_store::Error::Precondition { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(registry_error("delete CAS retries exhausted"))
    }

    pub fn invalidate(&self, customer_id: &str, name: &str) {
        self.cache
            .lock()
            .unwrap()
            .remove(&cache_key(customer_id, name));
    }

    pub async fn list(
        &self,
        customer_id: &str,
        limit: usize,
    ) -> Result<Vec<StreamDesc>, object_store::Error> {
        use futures_util::TryStreamExt;
        let prefix = if customer_id == "__legacy__" {
            ObjPath::from("registry/by-name")
        } else {
            ObjPath::from(format!(
                "registry/by-customer/{}/by-name",
                hex(&stream_hash(customer_id))
            ))
        };
        let mut out = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await? {
            if out.len() >= limit {
                break;
            }
            if let Ok(r) = self.store.get(&meta.location).await
                && let Ok(raw) = r.bytes().await
            {
                let d = parse_json::<StreamDesc>(&raw, "stream descriptor")?;
                if d.owner() != customer_id {
                    return Err(registry_error(
                        "stream descriptor owner does not match listing prefix",
                    ));
                }
                if !d.deleted {
                    out.push(d);
                }
            }
        }
        Ok(out)
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(
    raw: &[u8],
    kind: &'static str,
) -> Result<T, object_store::Error> {
    serde_json::from_slice(raw).map_err(|e| object_store::Error::Generic {
        store: "registry",
        source: format!("corrupt {kind}: {e}").into(),
    })
}

// ---- shard topology ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub version: u64,
    /// Physical shard-key layout. v2 is
    /// `[routing_hash16][incarnation_or_segment16][kind][suffix]`.
    #[serde(default)]
    pub storage_format: u32,
    /// Complete binary prefix code over the stream-hash bit space. "" = one
    /// shard covering everything.
    pub shards: Vec<String>,
    /// Generation-specific DB paths for online split children. Unmapped
    /// shards use the canonical `shards/<prefix>` path.
    #[serde(default)]
    pub shard_paths: HashMap<String, String>,
}

impl Topology {
    pub fn db_path(&self, prefix: &str) -> String {
        self.shard_paths.get(prefix).cloned().unwrap_or_else(|| {
            if prefix.is_empty() {
                "shards/root".to_string()
            } else {
                format!("shards/{prefix}")
            }
        })
    }
}

const TOPOLOGY_PATH: &str = "topology.json";

pub async fn load_topology(store: &Arc<dyn ObjectStore>) -> Result<Topology, object_store::Error> {
    let result = store.get(&ObjPath::from(TOPOLOGY_PATH)).await?;
    let raw = result.bytes().await?;
    let topology = parse_json::<Topology>(&raw, "topology")?;
    validate_topology(&topology)?;
    Ok(topology)
}

/// Lexicographic SlateDB projection bounds for a routing-bit prefix. Physical
/// keys begin with the full 16-byte routing hash, so the range remains exact
/// even though incarnation bytes and record suffixes follow it.
pub fn shard_projection_bounds(
    prefix: &str,
) -> Result<(Bound<Bytes>, Bound<Bytes>), object_store::Error> {
    if prefix.len() > 128 || !prefix.bytes().all(|bit| bit == b'0' || bit == b'1') {
        return Err(registry_error("invalid shard projection prefix"));
    }
    if prefix.is_empty() {
        return Ok((Bound::Unbounded, Bound::Unbounded));
    }
    let mut value = 0u128;
    for bit in prefix.bytes() {
        value = (value << 1) | u128::from(bit == b'1');
    }
    let shift = 128 - prefix.len();
    let lower = value << shift;
    let upper = if prefix.bytes().all(|bit| bit == b'1') {
        None
    } else {
        Some((value + 1) << shift)
    };
    Ok((
        Bound::Included(Bytes::copy_from_slice(&lower.to_be_bytes())),
        upper
            .map(|upper| Bound::Excluded(Bytes::copy_from_slice(&upper.to_be_bytes())))
            .unwrap_or(Bound::Unbounded),
    ))
}

/// Publish `parent -> parent0,parent1` with one topology CAS. The caller must
/// have created and durably verified both projected child DBs first; keeping
/// the data-plane work outside this function makes the final visibility step
/// a single linearization point.
pub async fn cas_publish_topology_split(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
    expected_version: u64,
) -> Result<Topology, object_store::Error> {
    cas_publish_topology_split_with_paths(store, parent, expected_version, None).await
}

pub async fn cas_publish_topology_split_with_paths(
    store: &Arc<dyn ObjectStore>,
    parent: &str,
    expected_version: u64,
    child_paths: Option<(&str, &str)>,
) -> Result<Topology, object_store::Error> {
    let path = ObjPath::from(TOPOLOGY_PATH);
    let result = store.get(&path).await?;
    let etag = result.meta.e_tag.clone();
    let raw = result.bytes().await?;
    let mut topology = parse_json::<Topology>(&raw, "topology")?;
    validate_topology(&topology)?;
    if topology.version != expected_version {
        return Err(registry_error(
            "topology version changed before split publish",
        ));
    }
    let Some(index) = topology.shards.iter().position(|prefix| prefix == parent) else {
        return Err(registry_error("split parent is not in the live topology"));
    };
    if parent.len() >= 128 {
        return Err(registry_error("maximum shard-prefix depth reached"));
    }
    topology.shards.remove(index);
    topology.shard_paths.remove(parent);
    topology.shards.push(format!("{parent}0"));
    topology.shards.push(format!("{parent}1"));
    if let Some((zero_path, one_path)) = child_paths {
        topology
            .shard_paths
            .insert(format!("{parent}0"), zero_path.to_string());
        topology
            .shard_paths
            .insert(format!("{parent}1"), one_path.to_string());
    }
    topology.shards.sort();
    topology.version = topology
        .version
        .checked_add(1)
        .ok_or_else(|| registry_error("topology version overflow"))?;
    validate_topology(&topology)?;
    let body = serde_json::to_vec(&topology).expect("topology json");
    store
        .put_opts(
            &path,
            PutPayload::from(body),
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: etag,
                version: None,
            })),
        )
        .await?;
    Ok(topology)
}

pub async fn load_or_init_topology(
    store: &Arc<dyn ObjectStore>,
    initial_shards: usize,
) -> Result<Topology, object_store::Error> {
    let path = ObjPath::from(TOPOLOGY_PATH);
    match store.get(&path).await {
        Ok(r) => {
            let raw = r.bytes().await?;
            let topology = parse_json::<Topology>(&raw, "topology")?;
            validate_topology(&topology)?;
            return Ok(topology);
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    let initial_shards = initial_shards.max(1);
    if !initial_shards.is_power_of_two() {
        return Err(registry_error("initial shards must be a power of two"));
    }
    let bits = initial_shards.trailing_zeros() as usize;
    let shards: Vec<String> = if bits == 0 {
        vec![String::new()]
    } else {
        (0..initial_shards)
            .map(|i| format!("{:0width$b}", i, width = bits))
            .collect()
    };
    let topo = Topology {
        version: 1,
        storage_format: 2,
        shards,
        shard_paths: HashMap::new(),
    };
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
            let topology = parse_json::<Topology>(&raw, "topology")?;
            validate_topology(&topology)?;
            Ok(topology)
        }
        Err(e) => Err(e),
    }
}

/// Longest-prefix match of the stream hash's leading bits against the shard
/// set. `shards` must form a complete prefix code.
pub fn shard_for_hash(shards: &[String], hash: &[u8; 16]) -> String {
    shards
        .iter()
        .filter(|prefix| shard_prefix_matches(prefix, hash))
        .max_by_key(|p| p.len())
        .cloned()
        .unwrap_or_default()
}

/// Does `hash` fall inside the shard identified by bit-prefix `prefix`?
pub fn shard_prefix_matches(prefix: &str, hash: &[u8; 16]) -> bool {
    if prefix.len() > hash.len() * 8 {
        return false;
    }
    prefix.bytes().enumerate().all(|(index, expected)| {
        let actual = b'0' + ((hash[index / 8] >> (7 - index % 8)) & 1);
        expected == actual
    })
}

#[derive(Default)]
struct PrefixNode {
    terminal: bool,
    zero: Option<Box<PrefixNode>>,
    one: Option<Box<PrefixNode>>,
}

fn validate_topology(topology: &Topology) -> Result<(), object_store::Error> {
    if topology.version == 0 {
        return Err(registry_error("topology version must be positive"));
    }
    if topology.storage_format != 2 {
        return Err(registry_error(
            "unsupported storage format; an explicit offline migration is required",
        ));
    }
    if topology.shards.is_empty() {
        return Err(registry_error("topology must contain at least one shard"));
    }
    for (prefix, path) in &topology.shard_paths {
        if !topology.shards.contains(prefix)
            || path.len() > 512
            || !path.starts_with("shards/")
            || path.contains("//")
            || path.split('/').any(|component| component == "..")
        {
            return Err(registry_error("invalid topology shard path mapping"));
        }
        ObjPath::parse(path).map_err(|_| registry_error("invalid topology shard object path"))?;
    }
    let mut root = PrefixNode::default();
    for prefix in &topology.shards {
        if prefix.len() > 128 || !prefix.bytes().all(|b| b == b'0' || b == b'1') {
            return Err(registry_error(
                "topology shard prefixes must be binary and at most 128 bits",
            ));
        }
        let mut node = &mut root;
        for bit in prefix.bytes() {
            if node.terminal {
                return Err(registry_error(
                    "topology contains overlapping shard prefixes",
                ));
            }
            node = if bit == b'0' {
                node.zero.get_or_insert_with(Default::default)
            } else {
                node.one.get_or_insert_with(Default::default)
            };
        }
        if node.terminal || node.zero.is_some() || node.one.is_some() {
            return Err(registry_error(
                "topology contains duplicate or overlapping shard prefixes",
            ));
        }
        node.terminal = true;
    }

    fn complete(node: &PrefixNode) -> bool {
        if node.terminal {
            return node.zero.is_none() && node.one.is_none();
        }
        match (&node.zero, &node.one) {
            (Some(zero), Some(one)) => complete(zero) && complete(one),
            _ => false,
        }
    }
    if !complete(&root) {
        return Err(registry_error(
            "topology shard prefixes do not cover the hash space",
        ));
    }
    Ok(())
}

fn registry_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "registry",
        source: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn descriptor(name: &str, epoch: &str) -> StreamDesc {
        StreamDesc {
            customer_id: "__legacy__".to_string(),
            name: name.to_string(),
            stream_epoch: epoch.to_string(),
            key_fingerprint: format!("fingerprint-{epoch}"),
            created_ms: 1,
            expires_at_ms: None,
            deleted: false,
            profile: None,
            content_type: "application/octet-stream".to_string(),
            ttl_secs: None,
            ordering: None,
            segment_count: 0,
            queue_max_deliveries: None,
            touch_token_fingerprint: None,
            touch_templates: Vec::new(),
            touch_sig_key: None,
            initial_request_hash: None,
            forked_from: None,
            fork_source_epoch: None,
            fork_offset: None,
            fork_sub_offset: None,
            fork_children: Vec::new(),
            fork_reference_registered: false,
        }
    }

    #[test]
    fn cache_is_bounded_and_stale_order_entries_do_not_evict_new_values() {
        let mut cache = RegistryCache::new(2);
        cache.insert("a".into(), Some(descriptor("a", "old")));
        cache.insert("a".into(), Some(descriptor("a", "new")));
        cache.insert("b".into(), Some(descriptor("b", "b")));
        assert_eq!(cache.entries.len(), 2);
        cache.insert("c".into(), Some(descriptor("c", "c")));
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.get("a", Duration::from_secs(60)).is_none());
        assert!(cache.get("b", Duration::from_secs(60)).is_some());
        assert!(cache.get("c", Duration::from_secs(60)).is_some());
    }

    #[tokio::test]
    async fn concurrent_recreate_has_exactly_one_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let seed = Registry::new(store.clone());
        let mut dead = descriptor("stream", "dead");
        dead.deleted = true;
        assert!(seed.create(dead).await.unwrap().0);

        // Separate Registry instances model concurrent servers with
        // independent caches racing on the same control-plane object.
        let left = Registry::new(store.clone());
        let right = Registry::new(store.clone());
        let (a, b) = tokio::join!(
            left.recreate("__legacy__", "stream", "dead", descriptor("stream", "left")),
            right.recreate(
                "__legacy__",
                "stream",
                "dead",
                descriptor("stream", "right")
            ),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert_ne!(a.0, b.0, "exactly one CAS may create the incarnation");
        assert_eq!(
            a.1.stream_epoch, b.1.stream_epoch,
            "loser must observe winner"
        );

        seed.invalidate("__legacy__", "stream");
        let stored = seed.get("__legacy__", "stream").await.unwrap().unwrap();
        assert_eq!(stored.stream_epoch, a.1.stream_epoch);
    }

    #[tokio::test]
    async fn corrupt_descriptor_fails_closed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &desc_path("__legacy__", "bad"),
                PutPayload::from_static(b"not json"),
            )
            .await
            .unwrap();
        let registry = Registry::new(store);
        assert!(registry.get("__legacy__", "bad").await.is_err());
    }

    #[tokio::test]
    async fn customers_can_use_the_same_name_without_visibility_or_storage_collisions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Registry::new(store);
        let mut left = descriptor("orders", "left");
        left.customer_id = "customer-a".into();
        let mut right = descriptor("orders", "right");
        right.customer_id = "customer-b".into();
        assert!(registry.create(left.clone()).await.unwrap().0);
        assert!(registry.create(right.clone()).await.unwrap().0);

        assert_eq!(
            registry
                .get("customer-a", "orders")
                .await
                .unwrap()
                .unwrap()
                .stream_epoch,
            "left"
        );
        assert_eq!(
            registry
                .get("customer-b", "orders")
                .await
                .unwrap()
                .unwrap()
                .stream_epoch,
            "right"
        );
        assert_eq!(registry.list("customer-a", 10).await.unwrap().len(), 1);
        assert_eq!(registry.list("customer-b", 10).await.unwrap().len(), 1);
        assert_ne!(left.storage_hash(), right.storage_hash());
        assert_ne!(left.routing_hash(), right.routing_hash());
    }

    #[tokio::test]
    async fn deleting_a_fork_chain_retains_then_cascades_ancestor_references() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let registry = Registry::new(store);
        let root = descriptor("root", "root-epoch");
        let mut middle = descriptor("middle", "middle-epoch");
        middle.forked_from = Some("root".into());
        middle.fork_source_epoch = Some("root-epoch".into());
        middle.fork_reference_registered = true;
        let mut leaf = descriptor("leaf", "leaf-epoch");
        leaf.forked_from = Some("middle".into());
        leaf.fork_source_epoch = Some("middle-epoch".into());
        leaf.fork_reference_registered = true;
        for desc in [root, middle, leaf] {
            assert!(registry.create(desc).await.unwrap().0);
        }
        assert!(
            registry
                .add_fork_child("__legacy__", "root", "root-epoch", "middle")
                .await
                .unwrap()
        );
        assert!(
            registry
                .add_fork_child("__legacy__", "middle", "middle-epoch", "leaf")
                .await
                .unwrap()
        );

        registry
            .mark_deleted("__legacy__", "root", "root-epoch")
            .await
            .unwrap();
        let middle = registry
            .mark_deleted("__legacy__", "middle", "middle-epoch")
            .await
            .unwrap()
            .unwrap()
            .1;
        registry
            .release_fork_chain("__legacy__", &middle)
            .await
            .unwrap();
        assert_eq!(
            registry
                .get("__legacy__", "root")
                .await
                .unwrap()
                .unwrap()
                .fork_children,
            vec!["middle"]
        );

        let leaf = registry
            .mark_deleted("__legacy__", "leaf", "leaf-epoch")
            .await
            .unwrap()
            .unwrap()
            .1;
        registry
            .release_fork_chain("__legacy__", &leaf)
            .await
            .unwrap();
        assert!(
            registry
                .get("__legacy__", "middle")
                .await
                .unwrap()
                .unwrap()
                .fork_children
                .is_empty()
        );
        assert!(
            registry
                .get("__legacy__", "root")
                .await
                .unwrap()
                .unwrap()
                .fork_children
                .is_empty()
        );
    }

    #[tokio::test]
    async fn invalid_initial_shard_count_is_an_error_not_a_panic() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(load_or_init_topology(&store, 3).await.is_err());
    }

    #[test]
    fn topology_must_be_a_complete_non_overlapping_prefix_code() {
        assert!(
            validate_topology(&Topology {
                version: 1,
                storage_format: 2,
                shards: vec!["0".into(), "10".into(), "11".into()],
                shard_paths: HashMap::new(),
            })
            .is_ok()
        );
        assert!(
            validate_topology(&Topology {
                version: 1,
                storage_format: 2,
                shards: vec!["0".into(), "10".into()],
                shard_paths: HashMap::new(),
            })
            .is_err()
        );
        assert!(
            validate_topology(&Topology {
                version: 1,
                storage_format: 2,
                shards: vec!["0".into(), "00".into(), "1".into()],
                shard_paths: HashMap::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn prefix_matching_uses_the_full_128_bit_hash() {
        let mut hash = [0u8; 16];
        hash[15] = 1;
        let matching = format!("{}1", "0".repeat(127));
        assert!(shard_prefix_matches(&matching, &hash));
        assert_eq!(
            shard_for_hash(&[format!("{}0", "0".repeat(127)), matching.clone()], &hash),
            matching
        );
    }

    #[test]
    fn physical_keys_share_the_stable_topology_prefix_but_isolate_incarnations() {
        let first = descriptor("orders", "epoch-a");
        let second = descriptor("orders", "epoch-b");
        let route = first.routing_hash();
        let first_storage = first.storage_hash();
        let second_storage = second.storage_hash();

        assert_eq!(&first_storage[..16], &route);
        assert_eq!(&second_storage[..16], &route);
        assert_ne!(&first_storage[16..], &second_storage[16..]);
        for prefix in ["0", "1", "0101", "10101010"] {
            assert_eq!(
                shard_prefix_matches(prefix, &route),
                shard_prefix_matches(prefix, first_storage[..16].try_into().unwrap())
            );
        }
    }

    #[test]
    fn projection_bounds_are_exact_for_non_byte_aligned_prefixes() {
        let (lower, upper) = shard_projection_bounds("101").unwrap();
        let Bound::Included(lower) = lower else {
            panic!("lower bound must be included");
        };
        let Bound::Excluded(upper) = upper else {
            panic!("upper bound must be excluded");
        };
        assert_eq!(lower.as_ref(), &(5u128 << 125).to_be_bytes());
        assert_eq!(upper.as_ref(), &(6u128 << 125).to_be_bytes());

        let (_, upper) = shard_projection_bounds("111").unwrap();
        assert!(matches!(upper, Bound::Unbounded));
    }

    #[tokio::test]
    async fn topology_split_publish_is_one_versioned_cas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let initial = load_or_init_topology(&store, 1).await.unwrap();
        let split = cas_publish_topology_split(&store, "", initial.version)
            .await
            .unwrap();
        assert_eq!(split.version, initial.version + 1);
        assert_eq!(split.shards, vec!["0", "1"]);
        assert!(
            cas_publish_topology_split(&store, "0", initial.version)
                .await
                .is_err()
        );
        assert_eq!(load_topology(&store).await.unwrap().shards, vec!["0", "1"]);
    }
}
