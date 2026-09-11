//! The fleet loop's unit tests: peer choice, return-home budget and victim selection.
#![cfg(test)]
use super::*;
use std::collections::HashMap;

fn peers(v: &[(&str, f64, u64)]) -> HashMap<String, (f64, u64)> {
    v.iter()
        .map(|(n, c, l)| (n.to_string(), (*c, *l)))
        .collect()
}

// Regression: ladder pass 3 did 7 moves in 10 minutes because moves
// were allowed to peers that were themselves behind.
#[test]
fn target_is_the_coolest_healthy_peer() {
    let p = peers(&[("a", 90.0, 0), ("b", 10.0, 0), ("c", 50.0, 0)]);
    assert_eq!(pick_move_target(&p, "a", 60).as_deref(), Some("b"));
}

#[test]
fn target_excludes_self() {
    let p = peers(&[("a", 1.0, 0), ("b", 80.0, 0)]);
    assert_eq!(pick_move_target(&p, "a", 60).as_deref(), Some("b"));
}

#[test]
fn no_target_when_every_peer_is_also_lagging() {
    // fleet-wide backlog: hold shards rather than pass them around
    let p = peers(&[("a", 5.0, 90), ("b", 5.0, 80), ("c", 5.0, 70)]);
    assert_eq!(pick_move_target(&p, "a", 60), None);
}

// Regression: FLEET-CAMPAIGN.md — 4 shards over 4 instances drew
// 1/1/2/0; the rebalancer moved one off the 2-owner, return-home
// handed it straight back once the home was healthy, and ownership
// oscillated on a ~300 s period.
// Round-19: fleet/urls.json and heartbeats are bucket-writable
// inputs, and relays send the fleet-internal credential (sometimes a
// customer stream key) to whatever they name. Anything that is not a
// bare origin must be refused before it is stored.
#[test]
fn peer_urls_must_be_bare_origins() {
    // Dev escape hatch on: plain http loopback is a valid origin.
    let allow = |u: &str| valid_peer_url_with(u, true, None);
    assert!(allow("https://cv-abc.fra.prisma.build"));
    assert!(allow("http://127.0.0.1:8091"));
    // userinfo, path, query, fragment, backslash, whitespace
    assert!(!allow("https://evil@attacker.example"));
    assert!(!allow("https://host.example/path"));
    assert!(!allow("https://host.example?x=1"));
    assert!(!allow("https://host.example#f"));
    assert!(!allow("https://host.example\\@evil"));
    assert!(!allow("https://host .example"));
    // non-http schemes and bare hosts
    assert!(!allow("file:///etc/passwd"));
    assert!(!allow("host.example"));
    assert!(!allow(""));
    // non-numeric port
    assert!(!allow("http://host.example:evil"));
    // TLS mandatory once the dev escape hatch is off
    let strict = |u: &str| valid_peer_url_with(u, false, None);
    assert!(!strict("http://127.0.0.1:8091"));
    assert!(strict("https://cv-abc.fra.prisma.build"));
}

#[test]
fn peer_domain_allowlist_is_enforced_when_set() {
    let v = |u: &str| valid_peer_url_with(u, false, Some("prisma.build"));
    assert!(v("https://cv-abc.fra.prisma.build"));
    assert!(v("https://prisma.build"));
    assert!(!v("https://attacker.example"));
    // suffix-matching must not accept a lookalike domain
    assert!(!v("https://evilprisma.build"));
}

#[test]
fn return_home_suppressed_at_fair_share() {
    // home kept 1 of its 2 shards; mean is ceil(4/4)=1; returning
    // would make 2 > 1 — the exact flap case.
    assert!(!return_home_allowed(1, 1, 4, 4));
}

#[test]
fn drained_home_still_gets_refilled() {
    // ladder p3: the once-lagged instance owns nothing; the mean is
    // 1; refilling to 1 is exactly fair.
    assert!(return_home_allowed(0, 1, 4, 4));
}

#[test]
fn fine_granularity_returns_normally() {
    // 16 shards over 4 instances: home at 3 takes its 4th back.
    assert!(return_home_allowed(3, 1, 16, 4));
    assert!(!return_home_allowed(4, 1, 16, 4));
}

#[test]
fn multiple_pending_returns_share_the_budget() {
    // two overrides pointing at the same healthy home in one pass:
    // the second must count the first (16/4: home at 2, gains 1+1
    // fine; a third would breach).
    assert!(return_home_allowed(2, 1, 16, 4));
    assert!(return_home_allowed(2, 2, 16, 4));
    assert!(!return_home_allowed(2, 3, 16, 4));
}

#[test]
fn empty_active_set_never_returns() {
    assert!(!return_home_allowed(0, 1, 4, 0));
}

#[test]
fn target_must_be_well_under_the_threshold_not_merely_under_it() {
    // threshold/2 gate: a peer at 40s with a 60s threshold is not healthy
    let p = peers(&[("a", 5.0, 0), ("b", 5.0, 40)]);
    assert_eq!(pick_move_target(&p, "a", 60), None);
}

// Regression: victim was derived via shard_for_hash(lag_map_key), but
// the lag map is keyed by storage_hash while shards are chosen by
// stream_hash(name) - so it almost never matched and D3 never moved.
#[test]
fn victim_is_the_laggiest_shard_we_actually_serve() {
    let lags = vec![
        ("000".to_string(), 10),
        ("011".to_string(), 90),
        ("111".to_string(), 40),
    ];
    let served = vec!["000".to_string(), "111".to_string()];
    // 011 is laggier but we do not serve it
    assert_eq!(pick_victim_shard(&lags, &served).as_deref(), Some("111"));
}

#[test]
fn no_victim_when_we_serve_nothing_with_lag() {
    let lags = vec![("011".to_string(), 90)];
    let served = vec!["000".to_string()];
    assert_eq!(pick_victim_shard(&lags, &served), None);
}

#[test]
fn no_victim_when_nothing_lags() {
    let served = vec!["000".to_string()];
    assert_eq!(pick_victim_shard(&[], &served), None);
}
