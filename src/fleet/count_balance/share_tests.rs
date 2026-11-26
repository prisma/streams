#![cfg(test)]
//! The count balance's fair share over the members within the count (Addendum 41 B item 2).

use std::collections::HashMap;

use super::FleetRead;
use crate::fleet::{OverrideEntry, Overrides};

/// Six leaves owned three, one and two by `streams-1`, `streams-2` and `streams-3`. With a count of
/// two the share is three over the two members within it, so `streams-1` at three moves nothing,
/// and `streams-3`, beyond the count but still in the ring, has no rank: its leaves are its own
/// leave's to move. With a count of three the share is two and `streams-1` gives `streams-2` one.
#[test]
fn the_share_divides_by_the_members_within_the_count() {
    let active: Vec<String> = ["streams-1", "streams-2", "streams-3"]
        .map(String::from)
        .to_vec();
    let owners = [
        "streams-1",
        "streams-1",
        "streams-1",
        "streams-2",
        "streams-3",
        "streams-3",
    ];
    let prefixes: Vec<String> = (0..6).map(|k| format!("{k:03b}")).collect();
    let entry = |to: &str| OverrideEntry {
        to: to.to_string(),
        ms: 0,
    };
    let entries: HashMap<String, OverrideEntry> = prefixes
        .iter()
        .zip(owners)
        .map(|(prefix, owner)| (prefix.clone(), entry(owner)))
        .collect();
    let read = |count| FleetRead {
        active: active.clone(),
        count,
        lags: active.iter().map(|member| (member.clone(), 0)).collect(),
        reserved: HashMap::new(),
        sources: Vec::new(),
        overrides: Overrides {
            entries: entries.clone().into_iter().collect(),
            ..Overrides::default()
        },
        aged_before_ms: 1,
    };
    let moves = [
        read(2).plan(&prefixes, "streams-1", 10),
        read(2).plan(&prefixes, "streams-3", 10),
        read(3).plan(&prefixes, "streams-1", 10),
    ];
    let three = Some(("000".to_string(), "streams-2".to_string()));
    assert_eq!(moves, [None, None, three]);
}
