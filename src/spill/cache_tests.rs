//! The sealed-read caches' byte budget: least recently used out first, an
//! oversized value never kept, the gauges exact; and the consolidation
//! byte semaphore's size.
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

/// A touch makes its value the most recent: every value inserted before
/// it leaves first, the last insert before the touch included.
#[test]
fn a_touch_takes_a_newer_place_than_every_insert_before_it() {
    let lru: ByteLru<u32> = ByteLru::new(800);
    for i in 0..8u32 {
        lru.insert("a", u64::from(i), i, 100);
    }
    assert_eq!(lru.get("a", 0), Some(0));
    for i in 8..15u32 {
        lru.insert("a", u64::from(i), i, 100);
    }
    assert_eq!(lru.get("a", 7), None, "7 left before the touched 0");
    assert_eq!((lru.get("a", 0), lru.get("a", 14)), (Some(0), Some(14)));
    assert_eq!(lru.bytes(), 800);
}

/// The process caches keep a value up to their item cap, and the debug
/// gauges name both budgets.
#[test]
fn the_process_caches_keep_their_largest_item_and_report_their_budgets() {
    use super::cache::{CHUNK_CACHE_BYTES, CHUNK_CACHE_MAX_ITEM, INDEX_CACHE_BYTES};
    let path = "cache-tests/the-process-caches-keep-their-largest-item";
    let big = bytes::Bytes::from(vec![7u8; 4]);
    super::cache::chunk_cache().insert(path, 0, big.clone(), CHUNK_CACHE_MAX_ITEM);
    assert_eq!(super::cache::chunk_cache().get(path, 0), Some(big));
    let j = super::cache::json();
    assert_eq!(j["chunk_cache"]["budget"], CHUNK_CACHE_BYTES);
    assert_eq!(j["index_cache"]["budget"], INDEX_CACHE_BYTES);
    assert_eq!(
        (CHUNK_CACHE_BYTES, INDEX_CACHE_BYTES),
        (32 << 20, 16 << 20),
        "the budgets the memory budget lines (spill::budget) add up"
    );
}

/// The consolidation semaphore holds one KiB permit per byte budgeted
/// (whatever other tests hold, never more than the budget is free).
#[test]
fn the_consolidation_semaphore_admits_at_most_its_byte_budget() {
    use super::budget::{CONSOLIDATION_BYTES, consolidation_permits};
    let budget = usize::try_from(CONSOLIDATION_BYTES / 1024).unwrap();
    assert_eq!(budget, 8192);
    assert!(consolidation_permits().available_permits() <= budget);
}
