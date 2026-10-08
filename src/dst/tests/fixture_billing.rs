//! Bounded waits on the usage drain. A wait checks every round it takes
//! against the ledger, since `drain_once` answers exactly the envelopes its
//! round appended to `_usage`, and fails as soon as the drain stops emitting
//! while the rows it waits on stay dirty. A drain that misreports its rounds
//! (answering a constant 0 or 1, as cargo-mutants' replacements of
//! `drain_once` do) fails the waiting scenario within a few rounds instead of
//! polling out the wait's whole budget, so the billing filter ends long
//! before its mutation timeout.

use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Engine = Arc<crate::shard::ShardEngine>;

/// One wait on the drain: where `_usage` ended after its last round, what
/// that round appended, and how many rounds in a row appended nothing while
/// the rows it waits on stayed dirty.
#[derive(Default)]
pub(super) struct DrainWait {
    ledger: Ledger,
    last: Option<usize>,
    empty: usize,
}

/// Where a wait last read `_usage` to end: not yet (before its first
/// round), or at a position (`None`: its start).
#[derive(Default)]
enum Ledger {
    #[default]
    Unread,
    EndsAt(Option<String>),
}

impl DrainWait {
    /// One drain round, checked against the ledger. The ledger's end is read
    /// before the wait's first round, not when the wait starts, so a wait
    /// that never drains reads nothing and opens no shard.
    pub(super) async fn round(&mut self, state: &State) -> usize {
        let from = match std::mem::take(&mut self.ledger) {
            Ledger::EndsAt(end) => end,
            Ledger::Unread => ledger_after(state, None).await.1,
        };
        let answered = crate::billing::drain_once(state).await.expect("drain");
        let (appended, end) = ledger_after(state, from).await;
        assert_eq!(
            answered, appended,
            "drain_once answered {answered} envelopes; its round appended {appended} to `_usage`"
        );
        self.ledger = Ledger::EndsAt(end);
        self.last = Some(answered);
        answered
    }

    /// The rows the wait drains for are still dirty after its last round. A
    /// round visits at least one open shard, in rotation, and a shard twice
    /// at most before it reads that shard's dirty index from the start, so
    /// more rounds in a row that append nothing than twice the open shards
    /// (plus two, for an acknowledgement landing between a probe and a
    /// round) mean the drain is not emitting what the wait needs.
    pub(super) fn still_dirty(&mut self, state: &State, what: &str) {
        match self.last.take() {
            None => return,
            Some(0) => self.empty += 1,
            Some(_) => self.empty = 0,
        }
        let limit = 2 * state.shards.engines().len() + 2;
        assert!(
            self.empty <= limit,
            "{what} stayed dirty through {} drain rounds in a row that appended nothing to `_usage`",
            self.empty
        );
    }
}

/// The envelopes `_usage` holds after `from` (from its start for `None`),
/// and where it ends. A read at the end answers an empty page (`[]`), and
/// every page before it holds at least one envelope, so the loop ends.
async fn ledger_after(state: &State, from: Option<String>) -> (usize, Option<String>) {
    let key = state.billing.usage_key().expect("the rig meters usage");
    let (mut envelopes, mut at) = (0, from);
    while let Some((body, next)) =
        crate::billing::system_read(state, crate::billing::USAGE_STREAM, &key, at.clone())
            .await
            .expect("`_usage` reads")
    {
        let page: Vec<serde_json::Value> = if body.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice(&body).expect("ledger page")
        };
        if page.is_empty() {
            break;
        }
        envelopes += page.len();
        at = Some(next);
    }
    (envelopes, at)
}

/// Drains until none of `ids` is dirty on `engine`. A row acked CLEAN is
/// never revisited by the dirty-row reconciler: only the walk or the debt
/// pass can close it.
pub(super) async fn ack_clean(state: &State, engine: &Engine, ids: &[[u8; 16]]) {
    let mut wait = DrainWait::default();
    for _ in 0..200 {
        wait.round(state).await;
        let dirty = engine.usage_dirty_scan().await.unwrap();
        if dirty.iter().all(|(hash, _)| !ids.contains(hash)) {
            return;
        }
        wait.still_dirty(state, "rows to ack clean");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("rows were never acked clean");
}
