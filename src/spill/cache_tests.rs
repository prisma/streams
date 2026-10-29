//! The sealed-read caches' byte budget: least recently used out first, an
//! oversized value never kept, the gauges exact.
use super::cache::ByteLru;

#[test]
fn the_least_recently_used_leaves_first_within_the_budget() {
    let lru: ByteLru<u32> = ByteLru::new(800);
    for i in 0..8u32 {
        lru.insert("a", u64::from(i), i, 100);
    }
    assert_eq!(lru.bytes(), 800);
    assert_eq!(lru.get("a", 0), Some(0), "touch 0: now the most recent");
    lru.insert("b", 0, 99, 100);
    assert_eq!(lru.bytes(), 800, "one value out for the new one");
    assert_eq!(lru.get("a", 1), None, "1 was the least recently used");
    assert_eq!(lru.get("a", 0), Some(0));
    assert_eq!(lru.get("b", 0), Some(99));
    lru.insert("b", 0, 7, 50);
    assert_eq!(
        (lru.get("b", 0), lru.bytes()),
        (Some(7), 750),
        "a replaced value's size is returned"
    );
    let j = lru.json();
    assert_eq!(j["evictions"], 1);
    assert_eq!(j["misses"], 1);
    assert_eq!(j["hits"], 4);
}

#[test]
fn a_value_over_an_eighth_of_the_budget_is_not_kept() {
    let lru: ByteLru<u32> = ByteLru::new(800);
    lru.insert("a", 0, 1, 100);
    lru.insert("a", 1, 2, 101);
    assert_eq!(
        (lru.get("a", 0), lru.get("a", 1), lru.bytes()),
        (Some(1), None, 100)
    );
}
