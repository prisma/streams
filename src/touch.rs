//! Per-stream watch invalidations, published after append durability.
//! WatchService derives keys from the stream's durable watch definitions;
//! this journal owns only epochs, bounded touch history and pending waiters.
//! Per-key waiter indexing bounds flush work to touched keys. Expired history
//! and overflow buckets require resynchronization; closing wakes waiters stale.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

const BUCKET_MS: u64 = 25;
const BUCKET_KEY_CAP: usize = 65_536;
const HISTORY_BUCKETS: usize = 4_096;
/// Global cap on retained history keys (~8 MB as sorted u32 vecs).
const HISTORY_KEY_BUDGET: usize = 2_000_000;
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
    closed: bool,
}

pub(crate) struct TouchJournal {
    pub epoch: String,
    inner: Mutex<Inner>,
}

pub(crate) enum WaitOutcome {
    Touched {
        cursor: String,
        end_offset: u64,
        proven: bool,
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
    pub(crate) fn start(entropy: &dyn crate::runtime::Entropy) -> Arc<TouchJournal> {
        let mut e = [0u8; 8];
        entropy.fill(&mut e);
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
                closed: false,
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
    pub(crate) fn ingest(&self, key_ids: &[u32], next_offset: u64) {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return;
        }
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
    pub(crate) fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        inner.key_index.clear();
        for (_, w) in inner.waiters.drain() {
            let _ = w.tx.send(WakeReason::Closed);
        }
    }

    /// Single-key wait for the collapsible GET path. Cursor semantics:
    /// "now", or "<epoch>:<generation>"; foreign epoch => stale.
    pub(crate) async fn wait(
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

        let rx = {
            let mut inner = self.inner.lock().unwrap();
            let generation = inner.generation;
            if let Some(proven) = inner.catch_up(from_gen, &key_ids) {
                // Behind-head clients catch up in one response.
                return WaitOutcome::Touched {
                    cursor: self.cursor(generation),
                    end_offset: inner.end_offset,
                    proven,
                };
            }
            let id = inner.next_waiter_id;
            inner.next_waiter_id += 1;
            let (tx, rx) = oneshot::channel();
            for k in &key_ids {
                inner.key_index.entry(*k).or_default().push(id);
            }
            inner.waiters.insert(id, Waiter { keys: key_ids, tx });
            rx
        };

        match tokio::time::timeout(timeout, rx).await {
            // A long-poll wake reports the bucket that touched these keys.
            Ok(Ok(WakeReason::Touched {
                generation,
                end_offset,
            })) => WaitOutcome::Touched {
                cursor: self.cursor(generation),
                end_offset,
                proven: true,
            },
            Ok(Ok(WakeReason::Closed)) => {
                let g = self.inner.lock().unwrap().generation;
                WaitOutcome::Stale {
                    cursor: self.cursor(g),
                }
            }
            _ => {
                let inner = self.inner.lock().unwrap();
                WaitOutcome::Timeout {
                    cursor: self.cursor(inner.generation),
                    end_offset: inner.end_offset,
                }
            }
        }
    }
}

impl Inner {
    /// Whether retained history proves a relevant touch after this cursor.
    /// Missing history and an overflow bucket require an unproven catch-up.
    fn catch_up(&self, from: u64, keys: &[u32]) -> Option<bool> {
        if from >= self.generation {
            return None;
        }
        if from < self.history_floor {
            return Some(false);
        }
        self.history
            .iter()
            .filter(|bucket| bucket.generation > from)
            .find(|bucket| {
                bucket.overflow
                    || keys
                        .iter()
                        .any(|key| bucket.keys.binary_search(key).is_ok())
            })
            .map(|bucket| !bucket.overflow)
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

/// Per-process registry of journals for state-protocol streams.
///
/// Map KEY is the collection's STORAGE identity (delete/recreate
/// isolation: a recreated stream must not inherit the old
/// incarnation's journal, pinned templates, or cursor validity). The
/// stored value carries the stream's shard ROUTE hash, because shard
/// bit-prefixes partition the ROUTE space — matching them against
/// storage hashes is the same hash-domain trap that broke the D3
/// victim pick in fleet.rs (see the regression note there) and, here,
/// made close_shard close a random unrelated slice of journals on
/// every fence.
/// One registry slot: the stream's shard route (for close_shard
/// matching) alongside its journal.
type JournalSlot = (crate::crypto::RouteHash, Arc<TouchJournal>);

pub(crate) struct TouchRegistry {
    map: Mutex<HashMap<[u8; 16], JournalSlot>>,
    /// WP-15/PR 4: template-id discriminators draw from the runtime's
    /// entropy capability, not the ambient process RNG.
    entropy: Arc<dyn crate::runtime::Entropy>,
}

impl TouchRegistry {
    pub(crate) fn with_entropy(entropy: Arc<dyn crate::runtime::Entropy>) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            entropy,
        }
    }

    pub(crate) fn journal(
        &self,
        hash: [u8; 16],
        route: crate::crypto::RouteHash,
    ) -> Arc<TouchJournal> {
        let mut map = self.map.lock().unwrap();
        map.entry(hash)
            .or_insert_with(|| (route, TouchJournal::start(&*self.entropy)))
            .1
            .clone()
    }

    /// Fence/move of a shard: close + drop every journal whose stream's
    /// shard ROUTE hash falls in the shard's bit-prefix, waking all
    /// their waiters with stale.
    pub(crate) fn close_shard(&self, prefix: &str) {
        let mut map = self.map.lock().unwrap();
        let closing: Vec<[u8; 16]> = map
            .iter()
            .filter(|(_, (route, _))| crate::registry::shard_prefix_matches(prefix, &route.0))
            .map(|(h, _)| *h)
            .collect();
        for h in closing {
            if let Some((_, j)) = map.remove(&h) {
                j.close();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BUCKET_KEY_CAP, HISTORY_BUCKETS, TouchJournal};

    #[tokio::test]
    async fn catch_up_uses_the_first_relevant_bucket() {
        let journal = TouchJournal::start(&crate::runtime::OsEntropy);
        journal.ingest(&[7], 1);
        journal.flush_bucket(false);
        journal.ingest(&[11], 2);
        journal.flush_bucket(false);
        let overflow: Vec<u32> = (0..u32::try_from(BUCKET_KEY_CAP).unwrap()).collect();
        journal.ingest(&overflow, 3);
        journal.flush_bucket(false);
        journal.ingest(&[7], 4);
        journal.flush_bucket(false);
        for (from, key, expected) in [
            (0, 7, Some(true)),
            (1, 7, Some(false)),
            (3, 7, Some(true)),
            (0, 99, Some(false)),
            (4, 7, None),
            (5, 7, None),
        ] {
            assert_eq!(
                journal.inner.lock().unwrap().catch_up(from, &[key]),
                expected
            );
        }
        journal.close();
    }

    #[tokio::test]
    async fn catch_up_distinguishes_missing_history_from_an_unmatched_key() {
        let journal = TouchJournal::start(&crate::runtime::OsEntropy);
        assert_eq!(journal.inner.lock().unwrap().catch_up(0, &[99]), None);
        for offset in 1..=HISTORY_BUCKETS + 1 {
            journal.ingest(&[7], u64::try_from(offset).unwrap());
            journal.flush_bucket(false);
        }
        {
            let inner = journal.inner.lock().unwrap();
            assert_eq!(inner.history_floor, 1);
            assert_eq!(inner.catch_up(0, &[99]), Some(false));
            assert_eq!(inner.catch_up(1, &[99]), None);
            assert_eq!(inner.catch_up(1, &[7]), Some(true));
            assert_eq!(inner.catch_up(inner.generation, &[7]), None);
        }
        journal.close();
    }
}
