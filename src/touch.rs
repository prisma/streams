//! state-protocol profile: per-stream in-memory touch journal, fed by the
//! shard acker after the durable watermark so an invalidation never precedes
//! read visibility.
//!
//! Wait model (PROFILES.md §6): each watch key is a virtual, cursor-addressed
//! invalidation stream served via `GET /touch/key/{key}?cursor=..&sig=..`.
//! Cursors are journal-global (`<epochHex16>:<generation>`), so an entire
//! cohort watching the same key converges on byte-identical URLs and a CDN
//! collapses them into one origin long-poll; touched/catch-up responses are
//! immutable ("first touch of K after C was at G") and therefore cacheable.
//! Herd damping is obsolete on this path — the CDN is the fan-out.
//!
//! Templates are **pinned in the stream descriptor** (durable registry
//! state, loaded when the journal opens). There is no dynamic activation,
//! no heartbeat, no TTL: query families are deploy-time configuration, and
//! restarts/moves cannot lose them.
//!
//! Resource bounds: per-key inverted waiter index (flush cost ∝ touched
//! keys), dead waiters reaped every second, closed buckets as sorted vecs
//! under a global key budget (evicted generations degrade to resync),
//! template caps enforced at descriptor validation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::oneshot;

use crate::registry::StorageHash;
use crate::touch_keys::{arg_string, key_id_of_u64, table_key, template_id, watch_key};

const BUCKET_MS: u64 = 25;
const BUCKET_KEY_CAP: usize = 65_536;
const HISTORY_BUCKETS: usize = 4_096;
/// Global cap on retained history keys (~8 MB as sorted u32 vecs).
const HISTORY_KEY_BUDGET: usize = 2_000_000;
const TOUCH_REGISTRY_CAPACITY: usize = 10_000;
pub const MAX_TEMPLATES_PER_STREAM: usize = 256;
pub const MAX_TEMPLATES_PER_ENTITY: usize = 64;

/// Lock-free derivation view: entity -> [(templateId, sortedFields)].
pub type TemplateSnapshot = Arc<HashMap<String, Vec<(u64, Vec<String>)>>>;

struct ClosedBucket {
    generation: u64,
    keys: Vec<u32>, // sorted
    overflow: bool,
}

#[derive(Debug, Clone, Copy)]
enum WakeReason {
    Touched { generation: u64, end_offset: u64 },
    Closed,
}

struct Waiter {
    keys: Vec<u32>,
    tx: oneshot::Sender<WakeReason>,
}

struct Inner {
    generation: u64,
    current: HashSet<u32>,
    current_overflow: bool,
    dirty: bool,
    end_offset: u64,
    current_end_offset: u64,
    history: VecDeque<ClosedBucket>,
    history_floor: u64,
    history_keys: usize,
    waiters: HashMap<u64, Waiter>,
    next_waiter_id: u64,
    key_index: HashMap<u32, Vec<u64>>,
    snapshot: TemplateSnapshot,
    template_count: usize,
    closed: bool,
    touches: u64,
    wakeups: u64,
    resyncs: u64,
    overflow_buckets: u64,
    reaped: u64,
}

pub struct TouchJournal {
    pub epoch: String,
    inner: Mutex<Inner>,
}

pub enum WaitOutcome {
    Touched {
        cursor: String,
        end_offset: u64,
        proven: bool,
        /// True when the answer is at the journal head at response time:
        /// immutable for this (key, cursor) and safe to CDN-cache. Behind-
        /// head catch-ups jump to the head and must not be cached, or a
        /// lagging client would walk the cached touch chain hop by hop.
        cacheable: bool,
    },
    Timeout {
        cursor: String,
        end_offset: u64,
    },
    Stale {
        cursor: String,
    },
}

impl TouchJournal {
    /// `pinned` = (entity, fields) template list from the stream descriptor.
    pub fn start(pinned: &[(String, Vec<String>)]) -> Arc<TouchJournal> {
        use rand::RngCore;
        let mut e = [0u8; 8];
        rand::rng().fill_bytes(&mut e);
        let mut map: HashMap<String, Vec<(u64, Vec<String>)>> = HashMap::new();
        for (entity, fields) in pinned {
            let mut sorted = fields.clone();
            sorted.sort();
            let tid = template_id(entity, &sorted);
            let list = map.entry(entity.clone()).or_default();
            if !list.iter().any(|(id, _)| *id == tid) {
                list.push((tid, sorted));
            }
        }
        let template_count = map.values().map(|v| v.len()).sum();
        let journal = Arc::new(TouchJournal {
            epoch: e.iter().map(|b| format!("{b:02x}")).collect(),
            inner: Mutex::new(Inner {
                generation: 0,
                current: HashSet::new(),
                current_overflow: false,
                dirty: false,
                end_offset: 0,
                current_end_offset: 0,
                history: VecDeque::new(),
                history_floor: 0,
                history_keys: 0,
                waiters: HashMap::new(),
                next_waiter_id: 0,
                key_index: HashMap::new(),
                snapshot: Arc::new(map),
                template_count,
                closed: false,
                touches: 0,
                wakeups: 0,
                resyncs: 0,
                overflow_buckets: 0,
                reaped: 0,
            }),
        });
        let flusher = journal.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(BUCKET_MS));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ticks = 0u64;
            loop {
                tick.tick().await;
                ticks += 1;
                if flusher.flush_bucket(ticks.is_multiple_of(40)) {
                    return;
                }
            }
        });
        journal
    }

    fn cursor(&self, generation: u64) -> String {
        format!("{}:{}", self.epoch, generation)
    }

    /// Record touched key IDs (shard acker, post-durability).
    pub fn ingest(&self, key_ids: &[u32], next_offset: u64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return;
        }
        inner.touches += key_ids.len() as u64;
        inner.dirty = true;
        inner.current_end_offset = inner.current_end_offset.max(next_offset);
        inner.end_offset = inner.end_offset.max(next_offset);
        if inner.current_overflow {
            return;
        }
        for k in key_ids {
            inner.current.insert(*k);
            if inner.current.len() >= BUCKET_KEY_CAP {
                inner.current_overflow = true;
                inner.current.clear();
                break;
            }
        }
    }

    fn flush_bucket(&self, reap: bool) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return true;
        }
        if reap {
            let dead: Vec<u64> = inner
                .waiters
                .iter()
                .filter(|(_, w)| w.tx.is_closed())
                .map(|(id, _)| *id)
                .collect();
            for id in &dead {
                remove_waiter(&mut inner, *id);
            }
            inner.reaped += dead.len() as u64;
        }
        if !inner.dirty {
            return false;
        }
        inner.generation += 1;
        let generation = inner.generation;
        let overflow = inner.current_overflow;
        let end_offset = inner.current_end_offset;
        let keys = std::mem::take(&mut inner.current);
        inner.current_overflow = false;
        inner.dirty = false;
        if overflow {
            inner.overflow_buckets += 1;
        }

        let candidates: Vec<u64> = if overflow {
            inner.waiters.keys().copied().collect()
        } else {
            let mut c: Vec<u64> = keys
                .iter()
                .filter_map(|k| inner.key_index.get(k))
                .flatten()
                .copied()
                .collect();
            c.sort_unstable();
            c.dedup();
            c
        };
        for id in candidates {
            if let Some(w) = remove_waiter(&mut inner, id) {
                let _ = w.tx.send(WakeReason::Touched {
                    generation,
                    end_offset,
                });
                inner.wakeups += 1;
            }
        }

        let mut sorted: Vec<u32> = keys.into_iter().collect();
        sorted.sort_unstable();
        inner.history_keys += sorted.len();
        inner.history.push_back(ClosedBucket {
            generation,
            keys: sorted,
            overflow,
        });
        while inner.history.len() > HISTORY_BUCKETS || inner.history_keys > HISTORY_KEY_BUDGET {
            if let Some(evicted) = inner.history.pop_front() {
                inner.history_keys -= evicted.keys.len();
                inner.history_floor = evicted.generation;
            } else {
                break;
            }
        }
        false
    }

    /// Fence/move: wake everyone with stale-inducing Closed and stop.
    pub fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        inner.key_index.clear();
        for (_, w) in inner.waiters.drain() {
            let _ = w.tx.send(WakeReason::Closed);
        }
    }

    pub fn snapshot(&self) -> TemplateSnapshot {
        self.inner.lock().unwrap().snapshot.clone()
    }

    /// Derive key IDs for a State Protocol change record against a template
    /// snapshot. Returns None for control messages.
    pub fn derive_key_ids(snapshot: &TemplateSnapshot, record: &Value) -> Option<Vec<u32>> {
        let entity = record.get("type")?.as_str()?;
        if entity.is_empty() {
            return None;
        }
        let op = record
            .get("headers")
            .and_then(|h| h.get("operation"))
            .and_then(|o| o.as_str())?;
        if !matches!(op, "insert" | "update" | "delete") {
            return None;
        }
        let mut out = vec![key_id_of_u64(table_key(entity))];
        if let Some(tpls) = snapshot.get(entity) {
            for (tid, fields) in tpls {
                for source in ["value", "old_value"] {
                    let Some(obj) = record.get(source) else {
                        continue;
                    };
                    if obj.is_null() {
                        continue;
                    }
                    let args: Vec<String> = fields.iter().map(|f| arg_string(obj.get(f))).collect();
                    out.push(key_id_of_u64(watch_key(*tid, &args)));
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        Some(out)
    }

    /// Single-key wait for the collapsible GET path. Cursor semantics:
    /// "now", or "<epoch>:<generation>"; foreign epoch => stale.
    pub async fn wait(
        self: &Arc<Self>,
        cursor: &str,
        key_ids: Vec<u32>,
        timeout: Duration,
    ) -> WaitOutcome {
        let from_gen = {
            let inner = self.inner.lock().unwrap();
            if inner.closed {
                return WaitOutcome::Stale {
                    cursor: self.cursor(inner.generation),
                };
            }
            if cursor == "now" {
                inner.generation
            } else {
                match cursor.split_once(':') {
                    Some((epoch, generation)) if epoch == self.epoch => {
                        match generation.parse::<u64>() {
                            Ok(g) => g,
                            Err(_) => {
                                return WaitOutcome::Stale {
                                    cursor: self.cursor(inner.generation),
                                };
                            }
                        }
                    }
                    _ => {
                        return WaitOutcome::Stale {
                            cursor: self.cursor(inner.generation),
                        };
                    }
                }
            }
        };

        let (waiter_id, rx) = {
            let mut inner = self.inner.lock().unwrap();
            let generation = inner.generation;
            if from_gen < generation {
                if from_gen < inner.history_floor {
                    inner.resyncs += 1;
                    return WaitOutcome::Touched {
                        cursor: self.cursor(generation),
                        end_offset: inner.end_offset,
                        proven: false,
                        cacheable: false,
                    };
                }
                let mut hit = false;
                let mut proven = true;
                for b in inner.history.iter().filter(|b| b.generation > from_gen) {
                    if b.overflow || key_ids.iter().any(|k| b.keys.binary_search(k).is_ok()) {
                        hit = true;
                        proven = !b.overflow;
                        break;
                    }
                }
                if hit {
                    if !proven {
                        inner.resyncs += 1;
                    }
                    // Jump to the head: a lagging client catches up in ONE
                    // response instead of walking the touch chain. The answer
                    // depends on "now", so it is only cacheable when the
                    // first touch IS the head (steady-state cohort wake).
                    return WaitOutcome::Touched {
                        cursor: self.cursor(generation),
                        end_offset: inner.end_offset,
                        proven,
                        cacheable: false,
                    };
                }
            }
            let id = inner.next_waiter_id;
            inner.next_waiter_id += 1;
            let (tx, rx) = oneshot::channel();
            for k in &key_ids {
                inner.key_index.entry(*k).or_default().push(id);
            }
            inner.waiters.insert(id, Waiter { keys: key_ids, tx });
            (id, rx)
        };

        match tokio::time::timeout(timeout, rx).await {
            // A long-poll wake is by definition at the head: immutable for
            // this (key, cursor), safe to cache for late cohort members.
            Ok(Ok(WakeReason::Touched {
                generation,
                end_offset,
            })) => WaitOutcome::Touched {
                cursor: self.cursor(generation),
                end_offset,
                proven: true,
                cacheable: true,
            },
            Ok(Ok(WakeReason::Closed)) => {
                let g = self.inner.lock().unwrap().generation;
                WaitOutcome::Stale {
                    cursor: self.cursor(g),
                }
            }
            _ => {
                let mut inner = self.inner.lock().unwrap();
                remove_waiter(&mut inner, waiter_id);
                WaitOutcome::Timeout {
                    cursor: self.cursor(inner.generation),
                    end_offset: inner.end_offset,
                }
            }
        }
    }

    pub fn meta(&self) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        serde_json::json!({
            "cursor": self.cursor(inner.generation),
            "epoch": self.epoch,
            "generation": inner.generation,
            "bucketMs": BUCKET_MS,
            "activeWaiters": inner.waiters.len(),
            "pinnedTemplates": inner.template_count,
            "pendingKeys": inner.current.len(),
            "historyKeys": inner.history_keys,
            "historyFloor": inner.history_floor,
            "overflowBuckets": inner.overflow_buckets,
            "endOffset": inner.end_offset,
            "totals": {
                "touches": inner.touches,
                "wakeups": inner.wakeups,
                "resyncs": inner.resyncs,
                "reaped": inner.reaped,
            },
        })
    }
}

fn remove_waiter(inner: &mut Inner, id: u64) -> Option<Waiter> {
    let w = inner.waiters.remove(&id)?;
    for k in &w.keys {
        if let Some(list) = inner.key_index.get_mut(k) {
            list.retain(|x| *x != id);
            if list.is_empty() {
                inner.key_index.remove(k);
            }
        }
    }
    Some(w)
}

struct TouchEntry {
    routing_hash: [u8; 16],
    journal: Arc<TouchJournal>,
}

#[derive(Default)]
struct TouchRegistryInner {
    map: HashMap<StorageHash, TouchEntry>,
    order: VecDeque<StorageHash>,
}

/// Per-process registry of journals for state-protocol streams. The map is
/// keyed by the storage hash because callers address journals by stream, but
/// fencing must use the independent routing hash that selected the shard.
pub struct TouchRegistry {
    inner: Mutex<TouchRegistryInner>,
    capacity: usize,
}

impl Default for TouchRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(TouchRegistryInner::default()),
            capacity: TOUCH_REGISTRY_CAPACITY,
        }
    }
}

impl TouchRegistry {
    pub fn journal(
        &self,
        storage_hash: StorageHash,
        routing_hash: [u8; 16],
        pinned: &[(String, Vec<String>)],
    ) -> Arc<TouchJournal> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.map.get_mut(&storage_hash) {
            entry.routing_hash = routing_hash;
            return entry.journal.clone();
        }

        while inner.map.len() >= self.capacity.max(1) {
            let Some(candidate) = inner.order.pop_front() else {
                break;
            };
            if let Some(entry) = inner.map.remove(&candidate) {
                entry.journal.close();
            }
        }

        let journal = TouchJournal::start(pinned);
        inner.map.insert(
            storage_hash,
            TouchEntry {
                routing_hash,
                journal: journal.clone(),
            },
        );
        inner.order.push_back(storage_hash);
        journal
    }

    /// Fence/move of a shard: close + drop every journal whose stream hash
    /// falls in the shard's bit-prefix, waking all their waiters with stale.
    pub fn close_shard(&self, prefix: &str) {
        let mut inner = self.inner.lock().unwrap();
        let closing: Vec<StorageHash> = inner
            .map
            .iter()
            .filter(|(_, entry)| crate::registry::shard_prefix_matches(prefix, &entry.routing_hash))
            .map(|(storage_hash, _)| *storage_hash)
            .collect();
        for storage_hash in &closing {
            if let Some(entry) = inner.map.remove(storage_hash) {
                entry.journal.close();
            }
        }
        inner.order.retain(|hash| !closing.contains(hash));
    }

    pub fn remove(&self, storage_hash: &StorageHash) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.map.remove(storage_hash) {
            entry.journal.close();
        }
        inner.order.retain(|hash| hash != storage_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shard_close_uses_routing_hash_and_wakes_waiters() {
        let registry = TouchRegistry {
            inner: Mutex::new(TouchRegistryInner::default()),
            capacity: 2,
        };
        let storage_hash = [0xff; 32];
        let routing_hash = [0x00; 16];
        let journal = registry.journal(storage_hash, routing_hash, &[]);
        let waiting = tokio::spawn({
            let journal = journal.clone();
            async move { journal.wait("now", vec![1], Duration::from_secs(5)).await }
        });
        tokio::task::yield_now().await;

        registry.close_shard("0");

        assert!(matches!(waiting.await.unwrap(), WaitOutcome::Stale { .. }));
        assert!(registry.inner.lock().unwrap().map.is_empty());
    }

    #[tokio::test]
    async fn registry_eviction_is_bounded_and_closes_the_old_journal() {
        let registry = TouchRegistry {
            inner: Mutex::new(TouchRegistryInner::default()),
            capacity: 1,
        };
        let first = registry.journal([1; 32], [0; 16], &[]);
        let _second = registry.journal([2; 32], [0; 16], &[]);

        assert_eq!(registry.inner.lock().unwrap().map.len(), 1);
        assert!(matches!(
            first.wait("now", vec![1], Duration::ZERO).await,
            WaitOutcome::Stale { .. }
        ));
    }

    #[tokio::test]
    async fn timed_out_waiters_are_removed_immediately() {
        let journal = TouchJournal::start(&[]);
        assert!(matches!(
            journal.wait("now", vec![1], Duration::ZERO).await,
            WaitOutcome::Timeout { .. }
        ));
        assert!(journal.inner.lock().unwrap().waiters.is_empty());
        journal.close();
    }
}
