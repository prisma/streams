#![cfg(test)]
//! `Standing`'s units: the ring between ticks, the drain's marks, the return-home skips, the
//! join's per-boot state and the co-location shield.

use super::{Attempts, DocTimes, Standing, Viewed, ViewedBeat};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

/// The ring between ticks is the given document's over the tick's last pass's candidates (§22
/// C1, C2): none before a pass; for a document without `members`, today's rule over that pass's
/// candidates (C17), which the next pass replaces; for a document listing a member with a boot
/// id and no evidence against it, that member (PR #173's survivors `Standing::set_candidates`
/// as `()` and `Standing::ring_of` as `None`; the join's ring, not today's rule, for the last).
#[test]
fn the_ring_between_ticks_is_the_documents_over_the_last_pass() {
    let standing = Standing::default();
    let bare = super::super::Overrides::default();
    assert_eq!(standing.ring_of(&bare), None, "no pass yet");
    standing.note_pass(2, &["streams-2", "streams-1", "streams-3"]);
    let both = vec!["streams-1".to_string(), "streams-2".to_string()];
    assert_eq!(standing.ring_of(&bare), Some(both));
    let listing = r#"{"entries":{},"members":[{"name":"streams-2","boot_id":"b-2"}]}"#;
    let listed: super::super::Overrides = serde_json::from_str(listing).unwrap();
    let second = vec!["streams-2".to_string()];
    assert_eq!(standing.ring_of(&listed), Some(second));
    standing.note_pass(3, &["streams-3"]);
    assert_eq!(standing.ring_of(&bare), Some(vec!["streams-3".to_string()]));
}

/// The count of the back-off's epoch is the tick's last pass's: none before a pass, then the count
/// each pass read (99472c14d's survivors `Standing::pass_count` as `None`, `Some(0)` and `Some(1)`).
#[test]
fn the_pass_count_is_the_last_passs() {
    let standing = Standing::default();
    assert_eq!(standing.pass_count(), None, "no pass yet");
    standing.note_pass(3, &["streams-1", "streams-2", "streams-3"]);
    assert_eq!(standing.pass_count(), Some(3));
    standing.note_pass(2, &["streams-1", "streams-2"]);
    assert_eq!(standing.pass_count(), Some(2));
}

/// The policy view's count (C11, `Instance::within_count`): every instance is within it before a
/// pass; after one, the ordinals the pass's count names are, and one beyond it is not.
#[test]
fn an_instance_is_within_the_count_of_the_last_pass() {
    let standing = Standing::default();
    assert!(standing.within_count("streams-3"), "no pass yet");
    standing.note_pass(2, &["streams-1", "streams-2", "streams-3"]);
    let within = ["streams-1", "streams-2", "streams-3"].map(|name| standing.within_count(name));
    assert_eq!(within, [true, true, false]);
}

/// A drain has begun from its first handoff and once it is announced, each alone; a planned
/// move's close that did not end clean is counted, once each.
#[test]
fn a_drain_begins_with_its_handoffs_and_a_fenced_close_is_counted() {
    let fresh = Standing::default();
    assert!(!fresh.drain_begun());
    let handing_off = Standing::default();
    handing_off.begin_handing_off_all();
    let announced = Standing::default();
    announced.begin_draining();
    assert_eq!(
        (handing_off.drain_begun(), announced.drain_begun()),
        (true, true)
    );
    assert_eq!(fresh.handoffs_fenced(), 0);
    fresh.note_handoff_fenced();
    fresh.note_handoff_fenced();
    assert_eq!(fresh.handoffs_fenced(), 2);
}

/// A planned move's attempts are counted as they end, those that did not start apart and those
/// backed off apart (Addendum 31), with the last one's end and step (Addendum 28); the failpoint
/// fails as many flushes under the park as it was armed with, then none.
#[test]
fn a_planned_moves_attempts_are_counted_by_end_and_step() {
    use super::super::handoff::Outcome;
    let standing = Standing::default();
    assert_eq!(standing.attempts(), Attempts::default());
    standing.note_attempt(Outcome::NotStarted, "flush");
    standing.note_attempt(Outcome::Evacuate, "park");
    standing.note_attempt(Outcome::NotStarted, "claim");
    standing.note_attempt(Outcome::BackedOff, "backoff");
    standing.note_attempt(Outcome::Moved, "commit");
    let counted = Attempts {
        ended: 5,
        not_started: 2,
        backed_off: 1,
        last: Some((Outcome::Moved, "commit")),
    };
    assert_eq!(standing.attempts(), counted);
    standing.miss_parked_flushes(2);
    let misses = [(); 3].map(|()| standing.take_parked_flush_miss());
    assert_eq!(misses, [true, true, false]);
}

/// The next beat publishes the split demand the latest tick set.
#[test]
fn the_latest_split_demand_is_what_the_next_beat_reads() {
    let standing = Standing::default();
    assert_eq!(standing.split_demand(), 0);
    standing.set_split_demand(7);
    assert_eq!(standing.split_demand(), 7);
    standing.set_split_demand(0);
    assert_eq!(standing.split_demand(), 0);
}

/// Return-home skips an entry younger than its window, to the millisecond, and any entry
/// whose prefix the live intent names as a source, until the beat reads no intent.
#[test]
fn return_home_skips_a_young_entry_and_a_live_intents_source() {
    let standing = Standing::default();
    let window = 300_000;
    assert!(standing.return_home_skips("00", window - 1, 300));
    assert!(!standing.return_home_skips("00", window, 300));
    assert!(!standing.return_home_skips("00", 2_000, 0));
    standing.set_topology_intent(Some(crate::topology::intent::Live {
        op_id: "op".to_owned(),
        kind: "merge".to_owned(),
        owner: "streams-1".to_owned(),
        sources: vec!["00".to_owned(), "01".to_owned()],
    }));
    assert!(standing.return_home_skips("00", window, 300));
    assert!(standing.return_home_skips("01", i64::MAX, 300));
    assert!(!standing.return_home_skips("1", window, 300));
    standing.set_topology_intent(None);
    assert!(!standing.return_home_skips("00", window, 300));
}

/// **A shield holds its halves until it lapses, at its instant, and a renewal keeps it**
/// (GATE-DECISION-s22-join-2026-11-20 Addendum 13 (2)): `00` and `01` shielded are unmovable
/// beside a live intent's source `1`; renewing `00` at 20 s keeps `01`'s mark; at 30 s `01`
/// lapses exactly (shielded and unmovable no more) while `00` holds; at 50 s `00` lapses too.
#[tokio::test(start_paused = true, crate = "crate::test_rt")]
async fn a_shield_holds_its_halves_until_it_lapses_and_a_renewal_keeps_it() {
    let standing = Standing::default();
    let lapse = Duration::from_secs(30);
    let names = |list: &[&str]| list.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
    standing.set_topology_sources(names(&["1"]));
    standing.shield(&names(&["00", "01"]), lapse);
    let sorted = |mut list: Vec<String>| {
        list.sort();
        list
    };
    assert_eq!(sorted(standing.unmovable()), names(&["00", "01", "1"]));
    assert!(standing.shielded("00") && !standing.shielded("1"));
    tokio::time::advance(Duration::from_secs(20)).await;
    standing.shield(&names(&["00"]), lapse);
    assert!(standing.shielded("01"), "a renewal keeps the other marks");
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        (standing.shielded("00"), standing.shielded("01")),
        (true, false),
        "01 lapses at its instant"
    );
    assert_eq!(sorted(standing.unmovable()), names(&["00", "1"]));
    tokio::time::advance(Duration::from_secs(20)).await;
    standing.set_topology_intent(None);
    assert_eq!(
        (standing.shielded("00"), standing.unmovable()),
        (false, vec![])
    );
}

/// **Return-home skips a live split's children** (GATE-DECISION-f2-children-on-actor A4), by
/// the predicate the stale prune uses: while the split of `0` is live, `00` and `01` are
/// skipped at any age, `1` and `000` are not; a live merge's sources keep their own entries and
/// its product `0` is skipped too (§22 C19: the actor's entry for it); with no live intent none
/// is skipped.
#[test]
fn return_home_skips_a_live_splits_children() {
    let standing = Standing::default();
    let live = |kind: &str, sources: &[&str]| crate::topology::intent::Live {
        op_id: "op".to_owned(),
        kind: kind.to_owned(),
        owner: "streams-1".to_owned(),
        sources: sources.iter().map(|s| (*s).to_owned()).collect(),
    };
    let old = i64::MAX;
    standing.set_topology_intent(Some(live("split", &["0"])));
    let skipped = ["0", "00", "01", "1", "000"].map(|p| standing.return_home_skips(p, old, 300));
    assert_eq!(skipped, [true, true, true, false, false]);
    standing.set_topology_intent(Some(live("merge", &["00", "01"])));
    let skipped = ["00", "01", "000", "0"].map(|p| standing.return_home_skips(p, old, 300));
    assert_eq!(skipped, [true, true, false, true]);
    standing.set_topology_intent(None);
    assert!(!standing.return_home_skips("00", old, 300));
}

fn beat(instance: &str) -> crate::fleet::Heartbeat {
    let document = format!(
        r#"{{"instance":"{instance}","ts_ms":1,"rps":0.0,"owned_shards":[],"draining":false,"seq":1}}"#
    );
    serde_json::from_str(&document).unwrap()
}

/// The topology policy reads the cell from the tick's latest heartbeat read, aged from
/// that read on the monotonic clock.
#[test]
fn the_cached_beats_are_the_latest_reads_aged_from_it() {
    assert!(Standing::default().cached_beats().is_none());
    let standing = Standing {
        origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
        ..Standing::default()
    };
    standing.cache_beats(&[beat("streams-1"), beat("streams-2")]);
    let (age, beats) = standing.cached_beats().unwrap();
    assert!(age < 500, "{age}");
    let names: Vec<&str> = beats.iter().map(|beat| beat.instance.as_str()).collect();
    assert_eq!(names, ["streams-1", "streams-2"]);
    *standing.cached.lock().unwrap() = (1_001, beats);
    let (age, _) = standing.cached_beats().unwrap();
    assert!((4_000..4_500).contains(&age), "{age}");
    standing.cache_beats(&[]);
    assert!(standing.cached_beats().unwrap().1.is_empty());
}

/// The policy reads both documents' times, aged by the older read.
#[test]
fn the_fleet_documents_times_are_the_latest_reads_aged_by_the_older() {
    let none = DocTimes {
        desired_computed_ms: None,
        youngest_override_ms: None,
        age_ms: None,
    };
    let standing = Standing {
        origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
        ..Standing::default()
    };
    assert_eq!(standing.doc_times(), none);
    standing.note_desired(Some(7));
    assert_eq!(
        standing.doc_times().age_ms,
        None,
        "the overrides were not read yet"
    );
    standing.note_overrides(None);
    let times = standing.doc_times();
    assert_eq!(
        (times.desired_computed_ms, times.youngest_override_ms),
        (Some(7), None)
    );
    assert!(times.age_ms.unwrap() < 500, "{times:?}");
    standing.docs.lock().unwrap().desired_read = 1_001;
    standing.note_overrides(Some(9));
    let times = standing.doc_times();
    assert_eq!(times.youngest_override_ms, Some(9));
    assert!((4_000..4_500).contains(&times.age_ms.unwrap()), "{times:?}");
    standing.note_desired(None);
    assert_eq!(standing.doc_times().desired_computed_ms, None);
}

/// The next beat publishes the topology policy's latest pass.
#[test]
fn the_next_beat_reads_the_topology_policys_latest_pass() {
    let standing = Standing::default();
    assert_eq!(
        (standing.split_refused(), standing.shards_resident()),
        (None, 0)
    );
    standing.set_topology(Some("instance_full"), 7);
    assert_eq!(
        (standing.split_refused(), standing.shards_resident()),
        (Some("instance_full"), 7)
    );
    assert_eq!(standing.topology_decision(), "");
    standing.set_topology_decision("hold=cooldown".into());
    assert_eq!(standing.topology_decision(), "hold=cooldown");
    standing.set_topology(None, 3);
    assert_eq!(
        (standing.split_refused(), standing.shards_resident()),
        (None, 3)
    );
}

#[test]
fn progress_ages_from_its_last_mark_and_is_absent_before_the_first() {
    assert_eq!(Standing::default().progress_age_ms(), None);
    // Marked one second after an origin five seconds ago.
    let standing = Standing {
        origin: Instant::now().checked_sub(Duration::from_secs(5)).unwrap(),
        progress: AtomicU64::new(1_001),
        ..Standing::default()
    };
    let aged = standing.progress_age_ms().unwrap();
    assert!((4_000..4_500).contains(&aged), "{aged}");
    standing.mark_progress();
    assert!(standing.progress_age_ms().unwrap() < 500);
}

#[test]
fn the_view_records_the_beats_of_the_read_it_was_computed_from() {
    let standing = Standing::default();
    let beat = |seq| {
        Viewed::from([(
            "streams-2".to_string(),
            ViewedBeat {
                boot_id: "b".into(),
                seq,
            },
        )])
    };
    standing.read(beat(7));
    assert!(standing.viewed().is_empty(), "no view published yet");
    standing.mark_progress();
    assert_eq!(standing.viewed(), beat(7));
    standing.read(beat(8));
    assert_eq!(standing.viewed(), beat(7), "a read alone publishes nothing");
}

#[tokio::test(crate = "crate::test_rt")]
async fn a_drain_marks_every_later_beat_and_asks_for_one_at_once() {
    let standing = Standing::default();
    assert_eq!((standing.next_beat(), standing.next_beat()), (1, 2));
    assert!(!standing.draining());
    assert_eq!(standing.draining_from(), None);
    let unasked = tokio::time::timeout(Duration::from_millis(50), standing.beat_requested());
    assert!(
        unasked.await.is_err(),
        "no beat is asked for before a drain"
    );
    standing.begin_draining();
    assert!(standing.draining());
    assert_eq!(standing.draining_from(), Some(3));
    assert_eq!(standing.next_beat(), 3);
    tokio::time::timeout(Duration::from_secs(1), standing.beat_requested())
        .await
        .expect("the drain's beat is asked for at once");
}

/// The membership passes count from 1, and the join records the pass that wrote it (the
/// `fleet joined` line's `passes`).
#[test]
fn the_join_records_the_pass_that_wrote_it() {
    let standing = Standing::default();
    assert_eq!(standing.joined_at_pass(), None);
    let passes = (standing.next_join_pass(), standing.next_join_pass());
    assert_eq!((passes, standing.note_joined()), ((1, 2), 2));
    assert_eq!(
        (standing.next_join_pass(), standing.joined_at_pass()),
        (3, Some(2))
    );
}

/// C18's flag (the join ruling, Addendum 33): a boot is no member until a pass reads it listed with
/// its own boot id, and then stays one; C13's boot-time verdict is not touched by it.
#[test]
fn a_boot_is_a_member_once_a_pass_reads_it_listed_and_stays_one() {
    let standing = Standing::default();
    standing.decide_boot(false);
    assert!(!standing.decided_member(), "no pass read it yet");
    standing.note_member();
    standing.note_member();
    assert_eq!(
        (standing.decided_member(), standing.boot_young()),
        (true, Some(false))
    );
}

/// C13's verdict reads back as decided, and nothing before (#183's survivors `boot_young` as
/// `Some(false)` and `Some(true)`).
#[test]
fn a_boots_verdict_reads_back_as_decided() {
    let (young, old) = (Standing::default(), Standing::default());
    assert_eq!(young.boot_young(), None);
    young.decide_boot(true);
    old.decide_boot(false);
    assert_eq!(
        (young.boot_young(), old.boot_young()),
        (Some(true), Some(false))
    );
}

/// The `JOIN_PHASE` clock starts at the first waiting pass, keeps that start while the joiner
/// waits, and is cleared by a pass that does not wait (#183's survivors `note_uncovered` as `()`
/// and `join_phase_since` as `None`).
#[test]
fn the_join_phase_clock_starts_at_the_first_waiting_pass_and_clears_when_not_waiting() {
    let standing = Standing::default();
    assert_eq!(standing.join_phase_since(), None);
    standing.note_uncovered(true);
    let started = standing.join_phase_since();
    standing.note_uncovered(true);
    let kept = standing.join_phase_since();
    standing.note_uncovered(false);
    assert_eq!(
        (
            started.is_some(),
            kept == started,
            standing.join_phase_since()
        ),
        (true, true, None)
    );
}

/// A peer listed unstamped while live is reported once, at its `UNSTAMPED_PASSES`-th pass, and
/// counted; a pass without it forgets it, so its count starts again (#183's survivors in
/// `note_unstamped`: the returned names, the `+=` and the `==`).
#[test]
fn a_peer_unstamped_for_its_passes_is_reported_once_and_forgotten_when_gone() {
    let standing = Standing::default();
    let peer = || vec!["streams-2".to_string()];
    let passes = crate::fleet::join::UNSTAMPED_PASSES;
    let first: Vec<Vec<String>> = (1..passes)
        .map(|_| standing.note_unstamped(peer()))
        .collect();
    let reached = standing.note_unstamped(peer());
    let after = standing.note_unstamped(peer());
    assert_eq!(
        (
            first.concat(),
            reached,
            after,
            standing.unstamped_listings()
        ),
        (Vec::<String>::new(), peer(), Vec::<String>::new(), 1)
    );
    assert!(standing.note_unstamped(Vec::new()).is_empty());
    let again: Vec<Vec<String>> = (0..passes)
        .map(|_| standing.note_unstamped(peer()))
        .collect();
    assert_eq!(
        (again.last().cloned(), standing.unstamped_listings()),
        (Some(peer()), 2),
        "forgotten, then reported again after its passes"
    );
}

/// A boot listed and undecided past its wait is counted and answers why it is not ready, until a
/// pass finds it decided (#183's survivors `note_undecided_boot` as `()` and `undecided_boot` as
/// `None`, `Some("")` and `Some("xyzzy")`).
#[test]
fn an_undecided_boot_is_counted_and_answers_why_until_decided() {
    let standing = Standing::default();
    assert_eq!(standing.undecided_boot(), None);
    standing.note_undecided_boot(true);
    let why = standing.undecided_boot();
    standing.note_undecided_boot(false);
    assert_eq!(
        (why, standing.undecided_boot(), standing.undecided_boots()),
        (
            Some("fleet boot undecided: listed, and no decision of this boot in the document"),
            None,
            1
        )
    );
}

/// The first beat waits until the beats are allowed, and not before (#183's survivor
/// `beats_allowed` as `()`); allowing them twice is allowing them once.
#[tokio::test]
async fn the_first_beat_waits_until_the_beats_are_allowed() {
    let standing = Standing::default();
    let early = tokio::time::timeout(Duration::from_millis(100), standing.beats_allowed()).await;
    standing.allow_beats();
    standing.allow_beats();
    let allowed = tokio::time::timeout(Duration::from_secs(5), standing.beats_allowed()).await;
    assert_eq!((early.is_ok(), allowed.is_ok()), (false, true));
}

/// A join past `JOIN_PHASE` and a planned move H1 kept are each counted once per note (#183's
/// survivors `note_join_unpinned` and `note_h1_kept` as `()`).
#[test]
fn an_unpinned_join_and_an_h1_kept_move_are_counted() {
    let standing = Standing::default();
    standing.note_join_unpinned();
    standing.note_h1_kept();
    standing.note_h1_kept();
    assert_eq!((standing.joins_unpinned(), standing.h1_kept()), (1, 2));
}

/// Addendum 46's guard (`Standing::settled`): a beat that is only stale counts its member gone once
/// this boot has read that same beat `WATCH` apart; one read short of it does not, a newer beat
/// starts the watch over, a beat that qualifies again leaves it, and a beat that states its end
/// (draining or withdrawn) counts at once.
#[tokio::test(start_paused = true, crate = "crate::test_rt")]
async fn a_stale_beat_counts_gone_only_after_it_stood_still_two_periods() {
    use super::super::record::{Candidates, boot_of};
    let standing = Standing::default();
    let pass = |ts_ms: i64, stated: bool, qualifies: bool| {
        let held = vec![("streams-2".to_string(), (boot_of("b"), qualifies))];
        let beats = vec![("streams-2".to_string(), (ts_ms, stated))];
        Candidates::held(2, "streams-1", held).with_beats(beats)
    };
    let gone = vec!["streams-2".to_string()];
    assert!(
        standing.settled(&pass(100, false, false)).is_empty(),
        "stale once"
    );
    tokio::time::advance(super::WATCH - Duration::from_millis(1)).await;
    assert!(
        standing.settled(&pass(100, false, false)).is_empty(),
        "short of two periods"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        standing.settled(&pass(100, false, false)),
        gone,
        "stood still"
    );
    assert!(
        standing.settled(&pass(200, false, false)).is_empty(),
        "a newer beat"
    );
    tokio::time::advance(super::WATCH).await;
    assert_eq!(standing.settled(&pass(200, false, false)), gone);
    assert!(
        standing.settled(&pass(300, false, true)).is_empty(),
        "qualifies again"
    );
    assert!(
        standing.settled(&pass(300, false, false)).is_empty(),
        "the watch starts over"
    );
    assert_eq!(
        standing.settled(&pass(300, true, false)),
        gone,
        "a stated end at once"
    );
}

/// The tests' skew stamps this runtime's beats that far behind the wall clock, never ahead, so a
/// peer reads them as stale (Addendum 46's skew DST; #193's survivor `-` to `+` in
/// `Standing::beat_stamp`).
#[test]
fn a_skewed_runtime_stamps_its_beats_behind_the_wall_clock() {
    let standing = Standing::default();
    let before = crate::shard::now_ms();
    assert!(standing.beat_stamp() >= before, "no skew: the wall clock");
    standing.skew_beats(40_000);
    let stamp = standing.beat_stamp();
    let after = crate::shard::now_ms();
    assert!(
        (before - 40_000..=after - 40_000).contains(&stamp),
        "{stamp} is 40 s behind {before}..={after}"
    );
}
