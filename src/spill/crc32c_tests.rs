//! CRC-32C: the check value, RFC 3720's vectors, the slicing-by-16 path
//! against the bit-at-a-time reference, and the linearity the object's
//! exhaustive bit-flip proof rests on.
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::crc32c::{Crc32c, crc32c, reference, single_bit, zero_byte};

#[test]
fn the_check_value_is_e3069283() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(reference(b"123456789"), 0xE306_9283);
}

/// RFC 3720 appendix B.4 (iSCSI), and the empty message.
#[test]
fn rfc3720_vectors() {
    assert_eq!(crc32c(&[]), 0);
    assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
    let up: Vec<u8> = (0u8..32).collect();
    assert_eq!(crc32c(&up), 0x46DD_794E);
    let down: Vec<u8> = (0u8..32).rev().collect();
    assert_eq!(crc32c(&down), 0x113F_DB5C);
}

/// Every length from 0 to 1,100 and random cuts into pieces agree with the
/// bit-at-a-time reference.
#[test]
fn slicing_by_16_matches_the_reference_in_any_pieces() {
    let mut rng = StdRng::seed_from_u64(0xC2C);
    let mut data = vec![0u8; 1_100];
    rng.fill(&mut data[..]);
    for len in 0..=data.len() {
        let want = reference(&data[..len]);
        assert_eq!(crc32c(&data[..len]), want, "len {len}");
        let mut c = Crc32c::new();
        let mut at = 0;
        while at < len {
            let step = rng.random_range(1..=len - at);
            c.update(&data[at..at + step]);
            at += step;
        }
        assert_eq!(c.finish(), want, "len {len} in pieces");
    }
}

/// The register difference a one-bit error at byte `j` of an `n`-byte
/// message leaves is `Z^(n-1-j)(single_bit(b))`, whatever the message: the
/// identity `spill::object_tests` iterates over every position.
#[test]
fn a_bit_flip_changes_the_crc_by_its_syndrome() {
    let mut rng = StdRng::seed_from_u64(0x51D);
    for _ in 0..300 {
        let n = rng.random_range(1..3_000);
        let mut data = vec![0u8; n];
        rng.fill(&mut data[..]);
        let (j, b) = (rng.random_range(0..n), rng.random_range(0..8u32));
        let before = crc32c(&data);
        data[j] ^= 1 << b;
        let mut syndrome = single_bit(b);
        for _ in 0..n - 1 - j {
            syndrome = zero_byte(syndrome);
        }
        assert_eq!(before ^ crc32c(&data), syndrome, "n {n} j {j} bit {b}");
    }
}
