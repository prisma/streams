#![cfg(test)]
//! **A co-location's moves do not fence their writers** (GATE-DECISION-f6-addendum-2026-11-17
//! item 3, C3; GATE-DECISION-s22-join-2026-11-20 Addendum 7 (3)). The pair is quiet, not cold: a
//! pair may carry writes under the merge line (`SHARD_MERGE_LINE_PCT`, 25 % of each axis,
//! combined), so one paced appender per half writes through both halves' handoffs to a third
//! instance and through the merge, on `topology_colocation`'s rigs. And the code's side of TLA-048's
//! `MergeOpen` (Addendum 7): a merge whose own half is not open opens it and completes.

use super::billing_closure_debts::JSON;
use super::fixture_http::HttpRig;
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_handoff::{Ack, Tally, stored_ns, stream_in};
use super::topology_colocation::{
    full, full_rig, laid_out, merged_within, moved_onto_then_pruned, pinned, stored_leaves,
    whole_and_shut,
};
use super::topology_policy::seeded;
use object_store::{ObjectStoreExt, PutPayload, path::Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Each appender's pace: 5 appends a second a half, 10 for the pair, against the merge line's 250
/// (25 % of the appends ceiling of 1,000 these rigs run at), and far under the split line.
const PACE: Duration = Duration::from_millis(200);

/// `full`'s rigs (three shards an instance at most, a 20 s merge sustain) with an appends ceiling
/// of 1,000, so the paced pair stays quiet, and no shrink for an hour.
fn quiet_writes(cli: &mut crate::config::CliArgs) {
    full(cli);
    cli.shard_appends_ceiling = 1_000;
    cli.scale_in_secs = 3_600;
}

/// The cell's three members by name and address.
type Members = [(&'static str, std::net::SocketAddr); 3];

/// One sent append: its stream and number, when it was first sent and how long it took (wall ms),
/// its final status and error code, the members its sends went through, in order (one per send
/// the client made: a forward between members adds none), and each send's answer as the client saw
/// it ([`Answer`]).
#[derive(Clone, Debug)]
struct Record {
    stream: String,
    n: u32,
    at_ms: i64,
    took_ms: i64,
    status: u16,
    code: String,
    chain: Vec<&'static str>,
    answers: Vec<Answer>,
}

/// One send's answer as the client saw it: the chain position it was sent at (a `503
/// shard_opening` resend keeps its position), its status and code, the member its
/// `Streams-Replay-To` named, and the member a forward reached (`Streams-Forwarded-To`).
#[derive(Clone, Debug)]
struct Answer {
    hop: usize,
    status: u16,
    code: String,
    replay_to: Option<String>,
    forwarded_to: Option<String>,
}

impl Answer {
    /// `hop:status code ->named fwd=reached`, as the after-park line prints it.
    fn shown(&self) -> String {
        let named = self.replay_to.as_deref().unwrap_or("-");
        let reached = self.forwarded_to.as_deref().unwrap_or("-");
        format!(
            "{}:{} {} ->{named} fwd={reached}",
            self.hop, self.status, self.code
        )
    }
}

impl Record {
    /// How its hops break Addendum 9's rule, if they do: a member may appear twice in its chain
    /// only back to back, when its first answer named itself and its second served the append; no
    /// append alternates between two members (A, B, A, B); and none needs more than three replays
    /// (it ended on a 409 naming a member after its third).
    fn hop_breach(&self) -> Option<&'static str> {
        let chain = &self.chain;
        let last = self.answers.last();
        if chain.len() == 4 && last.is_some_and(|a| a.status == 409 && a.replay_to.is_some()) {
            return Some("needs more than three replays");
        }
        if chain
            .windows(4)
            .any(|w| w[0] == w[2] && w[1] == w[3] && w[0] != w[1])
        {
            return Some("alternates between two members");
        }
        for (i, member) in chain.iter().enumerate() {
            let Some(j) = chain[i + 1..].iter().position(|m| m == member) else {
                continue;
            };
            if !self.served_after_naming_itself(i, i + 1 + j) {
                return Some("visits a member twice");
            }
        }
        None
    }

    /// Whether the visits at chain positions `first` and `second` of one member are the allowed
    /// double visit: back to back, the first's answer a 409 naming that member, the second the
    /// append's last send and served.
    fn served_after_naming_itself(&self, first: usize, second: usize) -> bool {
        let member = self.chain[first];
        let named_itself = self
            .answers
            .iter()
            .rev()
            .find(|a| a.hop == first)
            .is_some_and(|a| a.status == 409 && a.replay_to.as_deref() == Some(member));
        let served = second + 1 == self.chain.len() && self.status == 204;
        second == first + 1 && named_itself && served
    }

    /// Whether its answer leaves the append's outcome unknown: the raw surface's `503 shard_moving`
    /// (the product's `outcome_unknown`), a failed forward, or a `500`.
    fn unknown(&self) -> bool {
        let codes = ["outcome_unknown", "shard_moving", "forward_failed"];
        self.status == 500 || (self.status == 503 && codes.contains(&self.code.as_str()))
    }
}

/// Every sent append's [`Record`].
type Hops = Vec<Record>;

/// What the appenders share: their stop, what they saw, and every append's record.
type Shared = (Arc<AtomicBool>, Arc<Mutex<Tally>>, Arc<Mutex<Hops>>);

/// One append's end ([`sent`]): its final status, the member that answered it, the error code, the
/// sends past the first, the members its sends went through, in order, and each send's answer.
struct Sent {
    status: u16,
    by: String,
    code: String,
    tries: u32,
    chain: Vec<&'static str>,
    answers: Vec<Answer>,
}

/// Append `[{"n": <n>}]` to `stream` every [`PACE`] until the stop, numbering from 0 and sending
/// each to the next member in turn; each final answer goes to the tally ([`sent`]), and each append
/// to the records, with its send time.
async fn paced(members: Members, stream: String, (stop, tally, hops): Shared) {
    let mut n = 0_u32;
    while !stop.load(Ordering::SeqCst) {
        let first = usize::try_from(n).unwrap_or(0) % members.len();
        let at_ms = crate::shard::now_ms();
        let Sent {
            status,
            by,
            code,
            tries,
            chain,
            answers,
        } = sent(&members, first, &stream, n).await;
        let took_ms = crate::shard::now_ms() - at_ms;
        {
            let mut tally = tally.lock().unwrap();
            tally.replayed += tries;
            if status == 204 {
                let at = tokio::time::Instant::now();
                let ack = Ack { n, by, at };
                tally.acked.entry(stream.clone()).or_default().push(ack);
            } else {
                let key = (stream.clone(), status, code.clone());
                *tally.refused.entry(key).or_default() += 1;
            }
        }
        let record = Record {
            stream: stream.clone(),
            n,
            at_ms,
            took_ms,
            status,
            code,
            chain,
            answers,
        };
        hops.lock().unwrap().push(record);
        n += 1;
        tokio::time::sleep(PACE).await;
    }
}

/// One append of `[{"n": <n>}]` to `stream` through `members[first]`, as a client sends it: a
/// `409` replayed to the member its `Streams-Replay-To` names (at most three times, as the SDK does,
/// #264), and a `503 shard_opening` (nothing written: the new owner still opens the shard) sent
/// again after 100 ms, for up to 4 s. The final status, the member that answered it
/// (`Streams-Forwarded-To` when it forwarded), the error code, and the sends past the first.
async fn sent(members: &Members, first: usize, stream: &str, n: u32) -> Sent {
    let path = format!("/v1/stream/{stream}");
    let body = format!(r#"[{{"n":{n}}}]"#);
    let (mut name, mut addr) = members[first];
    let (mut replays, mut tries) = (0_u32, 0_u32);
    let (mut chain, mut answers) = (vec![name], Vec::new());
    loop {
        let (status, headers, answer) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
        let code = serde_json::from_slice::<serde_json::Value>(&answer)
            .ok()
            .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
            .unwrap_or_default();
        let named = headers.get("streams-replay-to");
        answers.push(Answer {
            hop: chain.len() - 1,
            status,
            code: code.clone(),
            replay_to: named.cloned(),
            forwarded_to: headers.get("streams-forwarded-to").cloned(),
        });
        let next = named.and_then(|o| members.iter().copied().find(|(m, _)| *m == o.as_str()));
        match next {
            Some(member) if status == 409 && replays < 3 => {
                replays += 1;
                (name, addr) = member;
                chain.push(name);
            }
            _ if status == 503 && code == "shard_opening" && tries < 40 => {
                tries += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => {
                let by = headers.get("streams-forwarded-to").cloned();
                return Sent {
                    status,
                    by: by.unwrap_or(name.to_string()),
                    code,
                    tries: replays + tries,
                    chain,
                    answers,
                };
            }
        }
    }
}

/// The `n`s stored in `stream`, read through whichever of `members` serves it.
async fn stored_by_one(members: &Members, stream: &str) -> Vec<u32> {
    for (_, addr) in members {
        let (status, _, _) = hreq(*addr, "GET", &format!("/v1/stream/{stream}"), &[], b"").await;
        if status == 200 {
            let mut ns = stored_ns(*addr, stream).await;
            ns.sort_unstable();
            return ns;
        }
    }
    panic!("no member serves {stream}");
}

/// Every member's URL published in `fleet/urls.json`, as a deploy publishes them, so each fleet tick
/// gives every member its peer table and a member that does not own a stream forwards its requests
/// (#235, #250) instead of answering its client `409` (a tick replaces any peer set by hand); within
/// 30 s, every rig's table holds its peers.
async fn forwarding(fleet: &Arc<dyn object_store::ObjectStore>, rigs: &[HttpRig; 3]) {
    let urls: serde_json::Map<String, serde_json::Value> = rigs
        .iter()
        .map(|rig| {
            let name = rig.state.ownership.instance().to_string();
            (
                name,
                serde_json::Value::String(format!("http://{}", rig.addr)),
            )
        })
        .collect();
    let body = serde_json::Value::Object(urls).to_string();
    let path = Path::from("fleet/urls.json");
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !rigs.iter().all(|rig| rig.state.peer.has_peers()) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "every member has its peer table"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The cell: `streams-1` with two streams created on it alone, one in `00` and one in `01`; then a
/// count of three, `streams-2` and `streams-3`, every member's URL published ([`forwarding`]), and
/// every leaf pinned where moves
/// left it (aged 70 s: past the move cooldown and far inside the return window): `streams-1` serves
/// `00` and `10`, `streams-2` serves `01` and `11`, `streams-3` none.
async fn cell() -> (
    Arc<dyn object_store::ObjectStore>,
    Arc<dyn object_store::ObjectStore>,
    [HttpRig; 3],
    [String; 2],
) {
    let (fleet, data) = (seeded().await, mem());
    let leaves = || ["00", "01", "10", "11"].map(str::to_string).to_vec();
    let one = ("streams-1", RigRuntime::first());
    let a = full_rig(&fleet, &data, one, quiet_writes, leaves()).await;
    super::topology_policy::store_topology(&a).await;
    let mut taken = 0;
    let streams = [
        stream_in(&a.state, a.addr, "00", &mut taken).await,
        stream_in(&a.state, a.addr, "01", &mut taken).await,
    ];
    let now = crate::shard::now_ms();
    let desired = format!(r#"{{"count":3,"epoch":2,"reason":"test","computed_at_ms":{now}}}"#);
    let path = Path::from("fleet/desired.json");
    fleet.put(&path, PutPayload::from(desired)).await.unwrap();
    let two = ("streams-2", RigRuntime::incarnation(1));
    let b = full_rig(&fleet, &data, two, quiet_writes, leaves()).await;
    let three = ("streams-3", RigRuntime::incarnation(2));
    let c = full_rig(&fleet, &data, three, quiet_writes, leaves()).await;
    let rigs = [a, b, c];
    forwarding(&fleet, &rigs).await;
    let layout = [
        ("00", "streams-1"),
        ("01", "streams-2"),
        ("10", "streams-1"),
        ("11", "streams-2"),
    ];
    pinned(&fleet, &layout, now + 50_000).await;
    laid_out(&rigs[0], &layout).await;
    (fleet, data, rigs, streams)
}

/// When the merge of the pair `0` began and published, on the wall clock: the first poll (every
/// 20 ms) that saw a merge intent in `data` (its prepare, just before both sources park), and the
/// first that saw `0` a stored leaf; within 300 s.
async fn merge_clock(data: &Arc<dyn object_store::ObjectStore>) -> (Option<i64>, Option<i64>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let mut parked = None;
    while tokio::time::Instant::now() < deadline {
        let held = crate::topology::intent::read(&**data).await.ok().flatten();
        if parked.is_none() && held.is_some_and(|held| held.intent.kind == "merge") {
            parked = Some(crate::shard::now_ms());
        }
        if stored_leaves(data).await.contains(&"0".to_string()) {
            return (parked, Some(crate::shard::now_ms()));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (parked, None)
}

/// The merge's phase line from outside (the merge logs none of its own, unlike a split's "the split
/// published, by phase"): from its intent's first sight (`parked_ms`) to its publication, and the
/// slowest append sent after the park; then every refusal after the park by class, with its hops.
fn print_after_park(records: &[Record], parked_ms: i64, published_ms: Option<i64>) {
    let after: Vec<&Record> = records.iter().filter(|r| r.at_ms >= parked_ms).collect();
    let slowest = after.iter().map(|r| r.took_ms).max().unwrap_or(0);
    let window = published_ms.map(|at| at - parked_ms);
    eprintln!(
        "MERGE_PHASES intent_to_publish_ms={window:?} slowest_append_after_park_ms={slowest}"
    );
    let mut classes: std::collections::BTreeMap<(u16, &str), Vec<String>> = Default::default();
    for r in after.iter().filter(|r| r.status != 204) {
        let answers: Vec<String> = r.answers.iter().map(Answer::shown).collect();
        let hops = format!(
            "{} n={} +{}ms via {} [{}]",
            r.stream,
            r.n,
            r.at_ms - parked_ms,
            r.chain.join(">"),
            answers.join(", ")
        );
        classes
            .entry((r.status, r.code.as_str()))
            .or_default()
            .push(hops);
    }
    eprintln!(
        "COLOCATE_AFTER_PARK sent={} refused={classes:?}",
        after.len()
    );
}

/// C3's two windows (Addendum 8). Until the merge parks (`parked_ms`), the co-location's: only
/// `409 not_ring_owner`, at most 1 % of the appends sent then, and no append visiting a member
/// the hop rule. Over the whole test: no outcome left unknown, and the hop rule (`hop_breach`,
/// Addendum 9: the cell forwards, and a forwarder told it is the owner refreshes and names itself,
/// F6's relayed-409 refresh). After the park, the merge's pause and publication: printed by class,
/// not bounded here, `503 shard_splitting` neither way.
fn judged(records: &[Record], parked_ms: i64) {
    let (before, _): (Vec<&Record>, Vec<&Record>) =
        records.iter().partition(|r| r.at_ms < parked_ms);
    let refused: Vec<&&Record> = before.iter().filter(|r| r.status != 204).collect();
    eprintln!(
        "COLOCATE_BEFORE_PARK parked_ms={parked_ms} sent={} refused={refused:?}",
        before.len()
    );
    assert!(
        refused
            .iter()
            .all(|r| r.status == 409 && r.code == "not_ring_owner"),
        "before the park only 409 not_ring_owner: {refused:?}"
    );
    assert!(
        refused.len() * 100 <= before.len(),
        "before the park, 409s past the replays over 1 %: {} of {}",
        refused.len(),
        before.len()
    );
    let unknown: Vec<&Record> = records.iter().filter(|r| r.unknown()).collect();
    assert!(
        unknown.is_empty(),
        "no append's outcome is unknown: {unknown:?}"
    );
    let breaches: Vec<(&str, &Record)> = records
        .iter()
        .filter_map(|r| r.hop_breach().map(|why| (why, r)))
        .collect();
    assert!(
        breaches.is_empty(),
        "the hop rule (Addendum 9): {breaches:?}"
    );
}

/// What one run of C3's scenario saw: the givers' engines, whether and when the pair merged, the
/// stored leaves at the merge, and every append's record.
struct Run {
    givers: [Arc<crate::shard::ShardEngine>; 2],
    merged: Result<(), String>,
    clock: (Option<i64>, Option<i64>),
    leaves: Vec<String>,
}

/// C3's scenario on [`cell`]: the paced appenders from before the first move to 3 s after the merge
/// (or its 300 s bound), the merge's clock beside them, the halves' writes and the co-location's
/// time checked as the merge is seen; the merge's phase line and the after-park refusals printed.
async fn scenario(
    (fleet, data): (
        &Arc<dyn object_store::ObjectStore>,
        &Arc<dyn object_store::ObjectStore>,
    ),
    rigs: &[HttpRig; 3],
    streams: &[String; 2],
    shared: &Shared,
) -> Run {
    let members: Members = [
        ("streams-1", rigs[0].addr),
        ("streams-2", rigs[1].addr),
        ("streams-3", rigs[2].addr),
    ];
    let appenders = streams
        .iter()
        .map(|stream| paced(members, stream.clone(), shared.clone()));
    let watched = async {
        let acked = |stream: &str| {
            shared
                .1
                .lock()
                .unwrap()
                .acked
                .get(stream)
                .map_or(0, Vec::len)
        };
        while streams.iter().any(|stream| acked(stream) < 3) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let givers = [
            rigs[0]
                .state
                .shards
                .open("00")
                .expect("streams-1 serves 00"),
            rigs[1]
                .state
                .shards
                .open("01")
                .expect("streams-2 serves 01"),
        ];
        let (merged, clock) =
            futures_util::future::join(merged_within(data, fleet, "0", 300), merge_clock(data))
                .await;
        let leaves = stored_leaves(data).await;
        if merged.is_ok() {
            let onto = [("00", "streams-3"), ("01", "streams-3")];
            moved_onto_then_pruned(fleet, "0", &onto).await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        shared.0.store(true, Ordering::SeqCst);
        Run {
            givers,
            merged,
            clock,
            leaves,
        }
    };
    let (_, run) =
        futures_util::future::join(futures_util::future::join_all(appenders), watched).await;
    if let (Some(parked), published) = run.clock {
        print_after_park(&shared.2.lock().unwrap(), parked, published);
    }
    run
}

/// **A co-location's moves do not fence their writers** (C3, Addendum 7 (3) and Addendum 8).
/// [`cell`]'s pair `0` is quiet: one paced appender a half ([`paced`], 10 appends a second for the
/// pair against the line's 250) writes from before the first move to after the merge, through
/// members that forward (#250). Both holders are full (2 + 2 > 3), so the target is `streams-3`
/// (0 + 3): `streams-2` hands `01` to it, then `streams-1` hands `00`, each giver first, and
/// `streams-3` merges. Judged by its own clock ([`judged`]): until the merge parks, only
/// `409 not_ring_owner`, at most 1 %; over the whole test no outcome unknown and no append visiting
/// a member twice; the merge's pause and publication printed, not bounded. And over the whole test:
/// every acked append is stored once and nothing unacked is; neither giver's engine closed `Fenced`
/// and no handoff of any rig fell back to a takeover by fence, and each holder made a planned
/// handoff; the halves' writes are exactly `00` and `01` to `streams-3`, within three move cooldowns
/// of the first; and one merge: the stored leaves are `0`, `10` and `11` when it is first seen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_colocation_move_does_not_fence_its_writer() {
    let (fleet, data, rigs, streams) = cell().await;
    // The failpoint that makes a move that is not giver first fence its writer (Addendum 9's
    // control R-g2b): each giver's tick holds a view that moves a leaf it still has open, so the
    // target opens it first. A planned handoff closes the giver's engine before its write and
    // never meets it.
    for giver in &rigs[..2] {
        let hold = Duration::from_secs(6);
        giver.state.fleet.standing().hold_moves_of_open_leaves(hold);
    }
    let shared: Shared = (
        Arc::new(AtomicBool::new(false)),
        Arc::default(),
        Arc::default(),
    );
    let run = scenario((&fleet, &data), &rigs, &streams, &shared).await;
    run.merged.unwrap();
    assert_eq!(run.leaves, ["0", "10", "11"], "one merge");
    for giver in &run.givers {
        let reason = giver.db.subscribe().borrow().close_reason;
        assert!(
            !matches!(reason, Some(slatedb::CloseReason::Fenced)),
            "{}: {reason:?}",
            giver.prefix
        );
    }
    let parked = run.clock.0.expect("the merge's intent was seen");
    judged(&shared.2.lock().unwrap(), parked);
    let tally = std::mem::take(&mut *shared.1.lock().unwrap());
    let members: Members = [
        ("streams-1", rigs[0].addr),
        ("streams-2", rigs[1].addr),
        ("streams-3", rigs[2].addr),
    ];
    for stream in &streams {
        let mut acked: Vec<u32> = tally.acked[stream].iter().map(|ack| ack.n).collect();
        acked.sort_unstable();
        let stored = stored_by_one(&members, stream).await;
        assert_eq!(stored, acked, "{stream}: stored = acked");
    }
    let handed = |rig: &HttpRig| rig.state.fleet.standing().last_handoff().is_some();
    assert!(
        handed(&rigs[0]) && handed(&rigs[1]),
        "each holder handed its half giver first"
    );
    let [a, b, c] = rigs;
    whole_and_shut(&[&a, &b, &c], &[]).await;
}

/// **C3 alone, measured** (Addendum 8 (2)): the same cell and scenario, nothing judged, for a run in
/// its own locked slot in release on an idle host. It prints the merge's phase line from outside
/// (`MERGE_PHASES`: its intent's first sight to its publication, and the slowest append after the
/// park), the after-park refusals by class with their hops, and `COLOCATE_TIME`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a measurement: run alone, in release, on an idle host (NOTES.md)"]
async fn a_colocation_move_under_writes_measured_alone() {
    let (fleet, data, rigs, streams) = cell().await;
    let shared: Shared = (
        Arc::new(AtomicBool::new(false)),
        Arc::default(),
        Arc::default(),
    );
    let run = scenario((&fleet, &data), &rigs, &streams, &shared).await;
    eprintln!(
        "COLOCATE_MEASURED merged={:?} clock={:?}",
        run.merged, run.clock
    );
    let [a, b, c] = rigs;
    whole_and_shut(&[&a, &b, &c], &[]).await;
}

/// A `POST` of the merge of `prefix` to the operator route: its status and JSON body.
async fn merge_route(addr: std::net::SocketAddr, prefix: &str) -> (u16, serde_json::Value) {
    let body = format!(r#"{{"prefix":"{prefix}"}}"#);
    let route = "/operator/topology/merge";
    let (status, _, answer) = hreq(addr, "POST", route, &[JSON], body.as_bytes()).await;
    (status, serde_json::from_slice(&answer).unwrap_or_default())
}

/// The cell's intent in `store` once the operation `op_id` has ended, within 20 s.
async fn ended(
    store: &dyn object_store::ObjectStore,
    op_id: &str,
) -> crate::topology::intent::Intent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let held = crate::topology::intent::read(store).await.unwrap().unwrap();
        if held.intent.op_id == op_id && held.intent.is_terminal() {
            return held.intent;
        }
        assert!(tokio::time::Instant::now() < deadline, "the merge ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The `n`s stored in `stream`, read through `addr`, in order.
async fn ns_of(addr: std::net::SocketAddr, stream: &str) -> Vec<u64> {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{stream}"), &[], b"").await;
    assert_eq!(status, 200, "{stream}");
    let records: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap_or_default();
    records.iter().filter_map(|r| r["n"].as_u64()).collect()
}

/// **A merge whose own half is not open opens it and completes** (GATE-DECISION-s22-join-2026-11-20
/// Addendum 7; the code's side of TLA-048's `MergeOpen`, whose need the colocate-partner trace
/// showed: a half handed first, then a restart, leaves the target's own half closed with no entry
/// naming it). `merge_k`'s one-instance rig splits `00` into `000` and `001`, each holding a
/// stream's records; `001`'s engine is then retired and closed, as a restart leaves it, so only
/// `000` is open here. S6's merge entry (the operator route's path, which the policy's
/// `merge_entry` calls) opens `001` itself once the retirement's holdoff has passed, asked again as
/// the policy's next pass asks (`start_merge` resolves each half,
/// `Adoption::Internal`): it answers 202, the merge ends `Reclaimed` at the next version with `00`
/// a leaf and neither half one, and both streams read back the records they had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_own_half_is_not_open_opens_it_and_completes() {
    use super::merge_k::{halves_of_00, rig, split_00};
    use crate::shard_directory::RetirementReason;
    let rig = rig().await;
    let version = split_00(&rig.state, &rig.parent).await;
    let halves = halves_of_00(&rig.state).await;
    let kept = [
        ns_of(rig.addr, &rig.low).await,
        ns_of(rig.addr, &rig.high).await,
    ];
    let closed = rig
        .state
        .shards
        .retire("001", RetirementReason::SweepEviction, |_, _| true);
    assert!(matches!(
        closed,
        crate::shard_directory::RetireOutcome::Retired(_)
    ));
    drop(halves);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while rig.state.shards.is_open("001") {
        assert!(tokio::time::Instant::now() < deadline, "001 closes");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(rig.state.shards.is_open("000"), "only 000 is open");
    let tasks = super::split_k::start_ops_on(&rig.state, 8);
    // The retirement's holdoff (HOLDOFF_BASE, 3 s, longer for a short-lived engine) answers an open
    // of `001` `Opening { code: "shard_moving" }` for a while; a restart leaves none. The policy
    // asks again on its next pass (an unavailable merge holds steady), as this loop does.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let (status, started) = loop {
        let (status, started) = merge_route(rig.addr, "00").await;
        let message = started["message"].as_str().unwrap_or_default();
        let held = status == 503 && message.contains("shard_moving");
        if !held || tokio::time::Instant::now() >= deadline {
            break (status, started);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(status, 202, "the merge starts: {started}");
    let op_id = started["op_id"].as_str().unwrap_or_default().to_string();
    let ended = ended(&*rig.store, &op_id).await;
    assert_eq!(ended.state, crate::topology::intent::State::Reclaimed);
    let (topology, _) = crate::topology::store::read(&*rig.store).await.unwrap();
    let leaves = ["00", "000", "001"].map(|p| topology.shards.leaves.contains_key(p));
    assert_eq!(
        (topology.version, leaves),
        (version + 1, [true, false, false])
    );
    let read = [
        ns_of(rig.addr, &rig.low).await,
        ns_of(rig.addr, &rig.high).await,
    ];
    assert_eq!(read, kept, "both streams read back their records");
    tasks.cancel();
    super::fixture_http::engine_shutdown(&rig.state).await;
}

/// A record of `chain` whose sends were answered `answers` (hop, status, the member named), ending
/// with `status`.
fn hopped(chain: &[&'static str], answers: &[(usize, u16, Option<&str>)], status: u16) -> Record {
    let answers = answers
        .iter()
        .map(|(hop, status, named)| Answer {
            hop: *hop,
            status: *status,
            code: String::new(),
            replay_to: named.map(str::to_string),
            forwarded_to: None,
        })
        .collect();
    Record {
        stream: "s".into(),
        n: 0,
        at_ms: 0,
        took_ms: 0,
        status,
        code: String::new(),
        chain: chain.to_vec(),
        answers,
    }
}

/// **The hop rule** (Addendum 9 (2)): F6's double visit (a 409 naming the member it came from,
/// then served there) passes, and so do a plain replay and a send served first; a member visited
/// twice any other way, two members alternating, and a fourth replay needed are each a breach.
#[test]
fn the_hop_rule_allows_only_a_self_naming_409_then_a_served_send() {
    let (a, b, c) = ("streams-1", "streams-2", "streams-3");
    let f6 = hopped(
        &[b, c, c],
        &[(0, 409, Some(c)), (1, 409, Some(c)), (2, 204, None)],
        204,
    );
    assert_eq!(f6.hop_breach(), None, "F6's relayed-409 refresh");
    let plain = hopped(&[a, c], &[(0, 409, Some(c)), (1, 204, None)], 204);
    assert_eq!(plain.hop_breach(), None, "one replay");
    assert_eq!(hopped(&[a], &[(0, 204, None)], 204).hop_breach(), None);
    let back = hopped(
        &[b, c, b],
        &[(0, 409, Some(c)), (1, 409, Some(b)), (2, 204, None)],
        204,
    );
    assert_eq!(back.hop_breach(), Some("visits a member twice"), "A, B, A");
    let unserved = hopped(
        &[b, c, c],
        &[(0, 409, Some(c)), (1, 409, Some(c)), (2, 503, None)],
        503,
    );
    assert_eq!(
        unserved.hop_breach(),
        Some("visits a member twice"),
        "not served"
    );
    let alternating = [
        (0, 409, Some(c)),
        (1, 409, Some(b)),
        (2, 409, Some(c)),
        (3, 409, Some(b)),
    ];
    let pingpong = hopped(&[b, c, b, c], &alternating, 409);
    assert_eq!(pingpong.hop_breach(), Some("needs more than three replays"));
    let four = [
        (0, 409, Some(c)),
        (1, 409, Some(a)),
        (2, 409, Some(c)),
        (3, 204, None),
    ];
    assert_eq!(
        hopped(&[b, c, a, c], &four, 204).hop_breach(),
        Some("visits a member twice"),
        "a member again after another"
    );
    let ab = [
        (0, 409, Some(c)),
        (1, 409, Some(b)),
        (2, 409, Some(c)),
        (3, 204, None),
    ];
    assert_eq!(
        hopped(&[b, c, b, c], &ab, 204).hop_breach(),
        Some("alternates between two members")
    );
}
