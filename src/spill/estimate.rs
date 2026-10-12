//! The memtable trigger in SlateDB's own units (review B2).
//!
//! SlateDB freezes the active memtable when
//! `estimate_encoded_size_compacted(entry_num, entries_size_in_bytes)`
//! reaches `l0_sst_size_bytes` (SlateDB `0717cc1`, `batch_write.rs:313-341`,
//! `format/sst.rs:1043-1160`). This is that formula, for the shard DB's
//! format (4 KiB blocks, one bloom filter policy `_bf` at 10 bits per key,
//! `min_filter_keys` 1,000, no expire timestamps).
//!
//! Its inputs come from two of SlateDB's own counters read as deltas since
//! the last forced memtable flush: `slatedb.db.write_ops` (one per batch op,
//! so every row version and tombstone) and `slatedb.db.memtable_write_bytes`
//! (key + value bytes per memtable entry). The memtable's
//! `entries_size_in_bytes` adds 16 B per entry (the sequence number and the
//! create timestamp every write carries, `types.rs:48-60`).
//! `spill::estimate_tests` calibrates this against the row at which SlateDB
//! actually freezes.

const BLOCK_SIZE: u64 = 4096;
const MIN_FILTER_KEYS: u64 = 1000;
const BLOOM_BITS_PER_KEY: u64 = 10;
/// `"_bf"`, the default bloom policy's name.
const FILTER_NAME_LEN: u64 = 3;
/// Sequence number (8) and create timestamp (8) per memtable entry.
const ENTRY_META: u64 = 16;
/// Row codec framing per entry: key prefix len u16, key suffix len u16,
/// value len u32, flags u8.
const ROW_FRAMING: u64 = 9;
/// Block offset per entry (u16).
const OFFSET: u64 = 2;
/// Per block: its checksum (4), its index entry (12 B key guess + 2 B
/// offset) and its stats (3 × u16).
const PER_BLOCK: u64 = 4 + 14 + 6;
/// Index checksum (4), metadata (79 + metadata offset 8 + version 2) and the
/// fixed part of the stats block (24 + 16 + 4).
const FIXED: u64 = 4 + 89 + 44;

/// SlateDB's estimate of the L0 SST a memtable of `entries` entries holding
/// `kv_bytes` of keys and values would encode to.
pub(crate) fn l0_estimate(entries: u64, kv_bytes: u64) -> u64 {
    if entries == 0 {
        return 0;
    }
    let entries_size = kv_bytes + ENTRY_META * entries;
    let row_encoded = entries_size + ROW_FRAMING * entries;
    let blocks = row_encoded.div_ceil(BLOCK_SIZE);
    let filter = if entries >= MIN_FILTER_KEYS {
        // [num_filters u16] [name_len u16][name][data_len u64] bloom
        // ([bits] + num_probes u16) and the composite block's CRC.
        2 + (2 + FILTER_NAME_LEN + 8) + (entries * BLOOM_BITS_PER_KEY).div_ceil(8) + 2 + 4
    } else {
        0
    };
    row_encoded + OFFSET * entries + PER_BLOCK * blocks + FIXED + filter
}
