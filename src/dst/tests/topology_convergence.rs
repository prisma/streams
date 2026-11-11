#![cfg(test)]
//! The topology's convergence end to end (#149; SCALING-DESIGN rev 4 §4.4's "1 → 64 → 1 under
//! bursty load", compressed): a one-instance, one-shard cell grows under load to several shards
//! on several instances, then, quiet, merges and scales back in to one instance and one shard,
//! with every record readable. Everything runs as production does: each instance's fleet tick,
//! heartbeat and topology policy loop through S4's split and S6's merge entries. The test plays
//! the platform: it starts the instance the fleet's desired count names and stops the one it no
//! longer names.
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// The policy's lines and the fleet's waits, compressed: a second's sustains, cooldown and
/// anti-flap, 7 appends a second as the split line, three shards an instance (so a split
/// beside a parent needs a second instance). S4's and S6's own wait for the fleet (60 s after a
/// count change) and the count's 30 s grow floor stay as shipped: they are constants of the
/// binary. The scale-in window is 30 s (`fleet::hysteresis::tests::DST_SCALE_IN`,
/// DESIGN-DECISION-grow-shrink-gap-2026-11-11), not compressed below the fleet's own observation:
/// - longer than one peer read plus a beat (8 s: planners re-read their peers one tick in three),
///   so no planner shrinks on a view older than its window;
/// - longer than a split's hand-over with one failed attempt (the 5 s quiesce deadline, the next
///   pass 10 s on, its publish, the sibling's split on its owner's next pass: about 23 s);
/// - shorter than S4's 60 s settle, so the test still pins #261 amendment B2 (without it a 30 s
///   shrink beats the settle);
/// - and the grow floor's 30 s: the fleet shrinks no faster than it grows.
fn compressed(cli: &mut crate::config::CliArgs) {
    cli.fleet_max = FLEET_MAX;
    cli.shards_per_instance_max = 3;
    cli.shard_split_sustain_secs = 1;
    cli.shard_topology_cooldown_secs = 1;
    cli.shard_appends_ceiling = 10;
    cli.shard_merge_sustain_secs = 1;
    cli.shard_split_reversal_min_secs = 1;
    cli.scale_in_secs = 30;
}

/// The load: `LOAD_PER_TICK` creates every `LOAD_TICK`, 20 a second by the clock, so a leaf of
/// a two-leaf cell takes 10 a second against the split line's 7, whatever the host's load.
const LOAD_PER_TICK: u64 = 4;
const LOAD_TICK: Duration = Duration::from_millis(200);

/// The fleet's maximum in `compressed`.
const FLEET_MAX: u64 = 4;

/// How long the quiet half may take to converge from `instances` running at the fourth leaf,
/// derived from the waits it is made of (DESIGN-DECISION-converge-budget-2026-11-11 §2a):
/// - a merge: S4's and S6's settle after a count change (`topology::ops::FLEET_SETTLE_MS`,
///   60 s), a policy pass (`policy_loop::PERIOD`, 10 s) to co-locate the pair, the move
///   cooldown that co-location starts (`REBALANCE_MOVE_COOLDOWN_SECS`' default, 60 s, which
///   `compressed` does not set), a pass to merge, and one attempt lost to the 5 s quiesce
///   deadline with its retry and publish (20 s): 160 s, charged to each of the three merges in
///   full, so a co-location redone after a shrink is covered;
/// - a count change: the 30 s window (`scale_in_secs`, and the grow floor) and one observation
///   (10 s): 40 s. The fleet may still grow to `FLEET_MAX` in the quiet half (a late ask, a
///   merge's co-location with no room) and then shrinks to one: (`FLEET_MAX` - `instances`) +
///   (`FLEET_MAX` - 1) changes;
/// - and 5 s for the last instance's stop.
///
/// Three instances at the fourth leaf give 645 s; two give 685 s; four give 605 s.
fn converge_budget(instances: usize) -> Duration {
    const MERGES: u64 = 3;
    const PER_MERGE_SECS: u64 = 60 + 10 + 60 + 10 + 20;
    const PER_COUNT_CHANGE_SECS: u64 = 30 + 10;
    const STOP_SECS: u64 = 5;
    let instances = u64::try_from(instances).unwrap_or(FLEET_MAX).min(FLEET_MAX);
    let changes = (FLEET_MAX - instances) + (FLEET_MAX - 1);
    Duration::from_secs(MERGES * PER_MERGE_SECS + changes * PER_COUNT_CHANGE_SECS + STOP_SECS)
}

/// The cell: its shared data and fleet stores, and the instances running, by ordinal.
struct Cell {
    data: Arc<dyn ObjectStore>,
    fleet: Arc<dyn ObjectStore>,
    running: BTreeMap<u64, HttpRig>,
    /// When the run began, for the retried appends' times (set by the test).
    began: tokio::time::Instant,
    /// Every append answered 409 or 503 and retried, in the order they ended.
    retries: std::sync::Mutex<Vec<AppendRetry>>,
}

/// An append answered 409 or 503 and retried: whether it may have landed is G7's ambiguity,
/// which the producer trio settles. `deduplicated` is the retry answered as a recognized
/// duplicate: `204` to an append naming a producer (`http::render_append`; a fresh append
/// naming one answers `200`).
struct AppendRetry {
    stream: String,
    first_status: u16,
    first_instance: u64,
    at_secs: f64,
    last_status: u16,
    deduplicated: bool,
}

impl Cell {
    /// A one-shard cell with its topology stored at version 1 and a desired count of one.
    async fn new() -> Self {
        let (data, fleet) = (mem(), mem());
        let seed = r#"{"count":1,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
        let path = Path::from("fleet/desired.json");
        fleet.put(&path, PutPayload::from(seed)).await.unwrap();
        let mut cell = Cell {
            data,
            fleet,
            running: BTreeMap::new(),
            began: tokio::time::Instant::now(),
            retries: std::sync::Mutex::new(Vec::new()),
        };
        cell.start(1).await;
        let topology = crate::topology::model::Topology {
            version: 1,
            shards: cell.running[&1].state.shards.prefixes().shards.clone(),
            last_op: None,
            max_request_body_bytes: None,
        };
        let raw = serde_json::to_vec(&topology).unwrap();
        cell.data
            .put(&Path::from("topology.json"), raw.into())
            .await
            .unwrap();
        cell
    }

    /// Start `streams-<n>` over the cell's stores, its fleet loop running.
    async fn start(&mut self, n: u64) {
        let rig = http_rig_build(
            self.data.clone(),
            RigRuntime::incarnation(n),
            HttpRigOptions {
                fleet_store: Some(self.fleet.clone()),
                instance: Some(format!("streams-{n}")),
                cli: compressed,
                prefixes: vec![String::new()],
                ..Default::default()
            },
        )
        .await;
        assert!(crate::fleet::start_configured(
            rig.state.clone(),
            &rig.tasks
        ));
        self.running.insert(n, rig);
    }

    /// Stop `streams-<n>`: its supervised loops end (its last beat withdraws it) and its engines
    /// close.
    async fn stop(&mut self, n: u64) {
        let rig = self.running.remove(&n).unwrap();
        let report = rig.tasks.shutdown(Duration::from_secs(5)).await;
        assert!(report.aborted.is_empty(), "streams-{n}: {report:?}");
        engine_shutdown(&rig.state).await;
    }

    /// The fleet's published desired count.
    async fn desired(&self) -> u64 {
        let got = self
            .fleet
            .get(&Path::from("fleet/desired.json"))
            .await
            .unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&got.bytes().await.unwrap()).unwrap();
        doc["count"].as_u64().unwrap()
    }

    /// Run the instances the desired count names, `streams-1..=count`, and no others.
    async fn follow(&mut self) {
        let count = self.desired().await;
        for n in 1..=count {
            if !self.running.contains_key(&n) {
                self.start(n).await;
            }
        }
        let extra: Vec<u64> = self
            .running
            .keys()
            .copied()
            .filter(|&n| n > count)
            .collect();
        for n in extra.into_iter().rev() {
            self.stop(n).await;
        }
    }

    /// The stored topology's leaves.
    async fn leaves(&self) -> Vec<String> {
        let (topology, _) = crate::topology::store::read(&*self.data).await.unwrap();
        topology.shards.shards
    }

    /// `method path` with `body` on the instance that serves it: each running instance in turn
    /// until one answers other than 409 (another owns it) or 503 (a split or an open in
    /// progress), for up to 20 s.
    async fn request(&self, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let (status, answer, _) = self.request_with(method, path, &[], body).await;
        (status, answer)
    }

    /// `request` with `extra` headers: the answer, and the first answer's status and instance
    /// when it was 409 or 503 and so retried.
    async fn request_with(
        &self,
        method: &str,
        path: &str,
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> (u16, Vec<u8>, Option<(u16, u64)>) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut first = None;
        loop {
            let (status, answer, instance) = self.ask_each(method, path, extra, body).await;
            if !matches!(status, 409 | 503) || tokio::time::Instant::now() >= deadline {
                return (status, answer, first);
            }
            first.get_or_insert((status, instance));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `method path` on each running instance in turn: the first answer other than 409 or 503,
    /// else the last, with the ordinal of the instance that gave it.
    async fn ask_each(
        &self,
        method: &str,
        path: &str,
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> (u16, Vec<u8>, u64) {
        let mut headers = vec![("content-type", "application/json")];
        headers.extend_from_slice(extra);
        let mut last = (0, Vec::new(), 0);
        for (n, rig) in &self.running {
            let (status, _, answer) = hreq(rig.addr, method, path, &headers, body).await;
            last = (status, answer, *n);
            if !matches!(status, 409 | 503) {
                break;
            }
        }
        last
    }

    /// One tick of the load (DESIGN-DECISION-dst-load-under-hog-2026-11-11 §2): the streams
    /// `n + 1 ..= n + 4` created and appended to concurrently, then the platform follows the
    /// desired count. The caller paces the ticks by the clock, so the rate does not depend on how
    /// long a request takes.
    async fn load_tick(&mut self, n: u64) -> Vec<(String, u64)> {
        let writes = (1..=LOAD_PER_TICK).map(|k| self.write_one(n + k));
        let written = futures_util::future::join_all(writes).await;
        self.follow().await;
        written.into_iter().flatten().collect()
    }

    /// A fresh stream `conv-<n>` created with one record: its path and the records it
    /// acknowledged, `None` when the create did not succeed. The append names the producer
    /// `conv-<n>`, epoch 1, sequence 0, so a retry after a move (G7: it may have landed) is
    /// answered as a duplicate instead of stored twice. Acknowledged: `200` (stored) or `204` (a
    /// recognized duplicate, `http::render_append`).
    async fn write_one(&self, n: u64) -> Option<(String, u64)> {
        let path = format!("/v1/stream/conv-{n}");
        let (created, _) = self.request("PUT", &path, b"").await;
        if created != 200 && created != 201 {
            return None;
        }
        let producer = format!("conv-{n}");
        let trio = [
            ("producer-id", producer.as_str()),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ];
        let (appended, _, first) = self.request_with("POST", &path, &trio, br#"{"n":1}"#).await;
        if let Some((first_status, first_instance)) = first {
            self.retries.lock().unwrap().push(AppendRetry {
                stream: path.clone(),
                first_status,
                first_instance,
                at_secs: self.began.elapsed().as_secs_f64(),
                last_status: appended,
                deduplicated: appended == 204,
            });
        }
        Some((path, u64::from(matches!(appended, 200 | 204))))
    }

    /// The retried appends, one line each, and the run's summary line
    /// `append_retries=<n> deduplicated=<m>`.
    fn retries_report(&self) -> String {
        let retries = self.retries.lock().unwrap();
        let mut out = String::new();
        for r in retries.iter() {
            out.push_str(&format!(
                "\n  retried append {} first {} from streams-{} at {:.1}s, then {} deduplicated={}",
                r.stream,
                r.first_status,
                r.first_instance,
                r.at_secs,
                r.last_status,
                r.deduplicated
            ));
        }
        let deduplicated = retries.iter().filter(|r| r.deduplicated).count();
        out.push_str(&format!(
            "\nappend_retries={} deduplicated={deduplicated}",
            retries.len()
        ));
        out
    }

    /// What the fleet store says, for a failure's message: the desired count's document, and
    /// each beat's instance, owned shards, shard loads, waiting split and topology decision.
    async fn fleet_state(&self) -> String {
        use futures_util::TryStreamExt;
        let listed: Vec<_> = self
            .fleet
            .list(None)
            .try_collect()
            .await
            .unwrap_or_default();
        let mut out = String::new();
        for meta in listed {
            let Ok(got) = self.fleet.get(&meta.location).await else {
                continue;
            };
            let bytes = got.bytes().await.unwrap_or_default();
            let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            let pick = |key: &str| doc.get(key).cloned().unwrap_or_default();
            let line = if doc.get("instance").is_some() {
                let picked: Vec<_> = [
                    "instance",
                    "owned_shards",
                    "shard_loads",
                    "split_refused",
                    "topology_decision",
                    "draining",
                ]
                .iter()
                .map(|key| (*key, pick(key)))
                .collect();
                format!("{picked:?}")
            } else {
                doc.to_string()
            };
            out.push_str(&format!("\n  {}: {line}", meta.location));
        }
        out
    }

    /// Each beat in brief: instance, its topology decision, its waiting split, and its shards'
    /// appends a second.
    async fn beats_brief(&self) -> String {
        use futures_util::TryStreamExt;
        let listed: Vec<_> = self
            .fleet
            .list(None)
            .try_collect()
            .await
            .unwrap_or_default();
        let mut out = Vec::new();
        for meta in listed {
            let Ok(got) = self.fleet.get(&meta.location).await else {
                continue;
            };
            let bytes = got.bytes().await.unwrap_or_default();
            let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            let Some(instance) = doc.get("instance").and_then(|v| v.as_str()) else {
                continue;
            };
            let loads: Vec<String> = doc["shard_loads"]
                .as_array()
                .map(|all| {
                    all.iter()
                        .map(|s| format!("{}:{}", s["prefix"], s["appends_per_s"]))
                        .collect()
                })
                .unwrap_or_default();
            out.push(format!(
                "{instance} [{}] wait={} decision={}",
                loads.join(" "),
                doc["split_refused"],
                doc["topology_decision"]
            ));
        }
        out.join(" | ")
    }

    /// One line of the run's trace: the time since `began`, the streams created, the leaves, the
    /// instances running, the desired count, and each beat in brief.
    async fn snapshot(&self, began: tokio::time::Instant, n: u64) -> String {
        format!(
            "\n  {}s n={n} leaves={:?} running={:?} desired={} {} {}",
            began.elapsed().as_secs(),
            self.leaves().await,
            self.running.keys().collect::<Vec<_>>(),
            self.desired().await,
            self.last_intent().await,
            self.beats_brief().await
        )
    }

    /// How the last topology operation stands: `intent=<op_id>:<state>:<outcome>` from
    /// `topology/intent.json`, so a trace names a split or merge that rolled back and why.
    async fn last_intent(&self) -> String {
        match crate::topology::intent::read(&*self.data).await {
            Ok(Some(held)) => format!(
                "intent={}:{:?}:{:?}",
                held.intent.op_id, held.intent.state, held.intent.outcome
            ),
            Ok(None) => "intent=none".to_string(),
            Err(error) => format!("intent=unreadable:{error}"),
        }
    }

    async fn shutdown(mut self) {
        let all: Vec<u64> = self.running.keys().copied().rev().collect();
        for n in all {
            self.stop(n).await;
        }
    }
}

/// **A one-shard cell grows under load to four shards on two instances, then, quiet, merges and
/// scales back in to one instance and one shard, and every record reads back.** Fresh streams
/// take 20 creates a second by the clock (four concurrently every 200 ms, a stall's ticks lost,
/// never caught up), spread over the hash space, until the stored topology holds
/// four leaves: each split past the first needs a second instance (three shards an instance),
/// which the fleet asks for and the test starts. The load then stops: the fleet scales in to one
/// instance, which the test stops the other of, and the cold siblings merge, deepest first, back
/// to the root. At the end: one instance running and the desired count one, the stored topology
/// the one leaf `""`, and every stream holding exactly the records it acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_one_shard_cell_grows_to_four_shards_on_two_instances_and_converges_back_to_one() {
    let mut cell = Cell::new().await;
    let mut written: BTreeMap<String, u64> = BTreeMap::new();
    let began = tokio::time::Instant::now();
    cell.began = began;
    let grow = began + Duration::from_secs(300);
    let (mut n, mut trace, mut next, mut tick) = (0_u64, Vec::new(), began, began);
    while cell.leaves().await.len() < 4 && tokio::time::Instant::now() < grow {
        if tokio::time::Instant::now() >= next {
            next += Duration::from_secs(10);
            trace.push(cell.snapshot(began, n).await);
        }
        written.extend(cell.load_tick(n).await);
        n += LOAD_PER_TICK;
        // No catch-up: a stall (a split's retries, an instance's start) loses its ticks, so the
        // rate never exceeds 20 a second.
        tick = (tick + LOAD_TICK).max(tokio::time::Instant::now());
        tokio::time::sleep_until(tick).await;
    }
    let grown = (cell.leaves().await, cell.running.len());
    assert!(
        grown.0.len() >= 4 && grown.1 >= 2,
        "grew to {grown:?}, {} records written ({:.1} a second); trace:{}\nfleet:{}{}",
        written.len(),
        written.len() as f64 / began.elapsed().as_secs_f64(),
        trace.concat(),
        cell.fleet_state().await,
        cell.retries_report()
    );
    let fourth = began.elapsed().as_secs();

    let converge = tokio::time::Instant::now() + converge_budget(grown.1);
    let converged = loop {
        if tokio::time::Instant::now() >= next {
            next += Duration::from_secs(10);
            trace.push(cell.snapshot(began, n).await);
        }
        cell.follow().await;
        let state = (
            cell.leaves().await,
            cell.desired().await,
            cell.running.len(),
        );
        if (state.0.len() == 1 && state.1 == 1 && state.2 == 1)
            || tokio::time::Instant::now() >= converge
        {
            break state;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    // The trace, green or red, before any assert: a run is read for a rolled-back intent, a
    // shrink before the fourth leaf (DESIGN-DECISION-grow-shrink-gap-2026-11-11 §3.4) and its
    // retried appends (G7).
    eprintln!(
        "convergence trace:{}\nfleet:{}{}\nconverge_secs={} from the fourth leaf at {fourth}s",
        trace.concat(),
        cell.fleet_state().await,
        cell.retries_report(),
        began.elapsed().as_secs() - fourth
    );
    assert_eq!(converged, (vec![String::new()], 1, 1), "grown {grown:?}");
    assert!(cell.running.contains_key(&1));
    // No instance holds an engine for a prefix that is no leaf (#251 amendment R3-d).
    let leaves = cell.leaves().await;
    for (n, rig) in &cell.running {
        let held = rig.state.shards.held_prefixes();
        assert!(
            held.iter().all(|prefix| leaves.contains(prefix)),
            "streams-{n} holds {held:?} over the leaves {leaves:?}"
        );
    }
    for (path, records) in &written {
        let (status, body) = cell.request("GET", path, b"").await;
        assert_eq!(status, 200, "{path}");
        let read: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let count = u64::try_from(read.as_array().map_or(0, Vec::len)).unwrap();
        assert_eq!(count, *records, "{path} after converging");
    }
    cell.shutdown().await;
}
