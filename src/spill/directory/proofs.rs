//! Kani proofs for the seal directory's key summaries: KANI-099. A keyed
//! read skips a sealed chunk whose summary excludes its key without a GET
//! (`shard::seal::read`), so a summary that excluded a key its chunk holds
//! would silently drop that key's records. A bloom summary sets each held
//! key hash's probe bits and admits a key when all of them are set.
//!
//! The proof is split where the cost is. Bit bookkeeping (`set_probe_bits`
//! then `holds_probe_bits`) holds for any seven probes inside a bloom of any
//! width whose other bits are arbitrary (a bloom's other keys only OR bits
//! in). Probe placement (`probes`, through `halves`) puts every probe of
//! every key hash inside a bloom of any width. `set_bits` and `holds_bits`
//! pass `probes(hash, bloom.len())` to those two and are proven end to end
//! at widths 1 and 256 only: a symbolic width through both runs two
//! divisions the solver must show agree, which exceeds the hour. The key
//! hash itself and the named-key summary's binary search are outside.
use super::MAX_BLOOM_BYTES;
use super::row::{BLOOM_HASHES, holds_bits, holds_probe_bits, probes, set_bits, set_probe_bits};

const PROBES: usize = 7;

/// KANI-099/bits: any seven probes inside a bloom of any width, set into it
/// over arbitrary prior bits, are all held afterwards.
#[kani::proof]
#[kani::unwind(9)]
fn kani_099_a_bloom_admits_every_key_it_holds() {
    assert_eq!(u64::try_from(PROBES).ok(), Some(BLOOM_HASHES));
    let len = kani::any_where(|len: &usize| (1..=MAX_BLOOM_BYTES).contains(len));
    let mut storage: [u8; MAX_BLOOM_BYTES] = kani::any();
    let bloom = &mut storage[..len];
    let probe: [usize; PROBES] = kani::any();
    kani::assume(probe.iter().all(|bit| *bit < len * 8));
    set_probe_bits(bloom, probe.iter().copied());
    assert!(
        holds_probe_bits(bloom, probe.iter().copied()),
        "a bloom admits every key it holds"
    );
    kani::cover!(len == MAX_BLOOM_BYTES, "a bloom at its byte cap");
    kani::cover!(len == 1, "a one-byte bloom");
}

/// KANI-099/probes: every probe of every key hash lies inside a bloom of any
/// width.
#[kani::proof]
#[kani::unwind(9)]
fn kani_099_every_probe_lies_inside_the_bloom() {
    let len = kani::any_where(|len: &usize| (1..=MAX_BLOOM_BYTES).contains(len));
    let key: [u8; 16] = kani::any();
    assert!(
        probes(&key, len).all(|bit| bit < len * 8),
        "every probe lies inside the bloom"
    );
    kani::cover!(len == MAX_BLOOM_BYTES, "probes at the byte cap");
}

/// `set_bits` then `holds_bits` end to end at a width of `LEN` bytes.
fn admits<const LEN: usize>() {
    let mut bloom: [u8; LEN] = kani::any();
    let key: [u8; 16] = kani::any();
    set_bits(&mut bloom, &key);
    assert!(
        holds_bits(&bloom, &key),
        "a bloom admits every key it holds"
    );
}

/// KANI-099/one-byte: a one-byte bloom admits its key, end to end.
#[kani::proof]
#[kani::unwind(9)]
fn kani_099_a_one_byte_bloom_admits_its_key() {
    admits::<1>();
}

/// KANI-099/capped: a bloom at its byte cap admits its key, end to end.
#[kani::proof]
#[kani::unwind(9)]
fn kani_099_a_capped_bloom_admits_its_key() {
    admits::<MAX_BLOOM_BYTES>();
}
