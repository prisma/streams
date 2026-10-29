//! CRC-32C (Castagnoli), written in tree (DESIGN §3.2 item 22: the only
//! CRC-32C crate in the lock file is reached through `object_store`, and
//! naming it would stale every formal receipt).
//!
//! Reflected polynomial 0x82F63B78, initial value and final XOR all ones,
//! table-driven, slicing-by-16: sixteen 256-entry tables built at compile
//! time, sixteen input bytes per step. The CPU's CRC32C instruction
//! (SSE4.2, the ARMv8 CRC extension) would need `unsafe` intrinsics; the
//! table path is portable safe code, and the seal's encode measured it at
//! a small share of a seal's time (results-spikeB §3).

/// The reflected Castagnoli polynomial.
const POLY: u32 = 0x82F6_3B78;

/// `TABLES[0]` is the classic byte table; `TABLES[k][i]` is the register
/// contribution of byte `i` followed by `k` zero bytes.
static TABLES: [[u32; 256]; 16] = tables();

#[expect(
    clippy::cast_possible_truncation,
    reason = "tables; the byte index is below 256, so the cast is exact; a const fn has no fallible conversion, and building the tables at run time would add a lazy static to every checksum"
)]
const fn tables() -> [[u32; 256]; 16] {
    let mut t = [[0u32; 256]; 16];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 == 1 { (c >> 1) ^ POLY } else { c >> 1 };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut s = 1;
    while s < 16 {
        let mut i = 0;
        while i < 256 {
            let prev = t[s - 1][i];
            t[s][i] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            i += 1;
        }
        s += 1;
    }
    t
}

/// A running CRC-32C over data fed in pieces (an object body is a list of
/// `Bytes` slices, never one buffer).
#[derive(Clone, Copy)]
pub(crate) struct Crc32c(u32);

impl Crc32c {
    pub(crate) fn new() -> Self {
        Self(!0)
    }

    pub(crate) fn update(&mut self, data: &[u8]) {
        self.0 = update(self.0, data);
    }

    pub(crate) fn finish(self) -> u32 {
        !self.0
    }
}

/// CRC-32C of `data`.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    let mut c = Crc32c::new();
    c.update(data);
    c.finish()
}

#[inline]
fn t(table: usize, index: u32) -> u32 {
    TABLES[table][(index & 0xFF) as usize]
}

#[inline]
fn word(block: &[u8; 16], at: usize) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(block.get(at..at + 4).unwrap_or(&[0; 4]));
    u32::from_le_bytes(bytes)
}

fn update(mut crc: u32, data: &[u8]) -> u32 {
    let (blocks, rest) = data.as_chunks::<16>();
    for b in blocks {
        let a = word(b, 0) ^ crc;
        let c = word(b, 4);
        let d = word(b, 8);
        let e = word(b, 12);
        crc = t(15, a)
            ^ t(14, a >> 8)
            ^ t(13, a >> 16)
            ^ t(12, a >> 24)
            ^ t(11, c)
            ^ t(10, c >> 8)
            ^ t(9, c >> 16)
            ^ t(8, c >> 24)
            ^ t(7, d)
            ^ t(6, d >> 8)
            ^ t(5, d >> 16)
            ^ t(4, d >> 24)
            ^ t(3, e)
            ^ t(2, e >> 8)
            ^ t(1, e >> 16)
            ^ t(0, e >> 24);
    }
    for &byte in rest {
        crc = t(0, crc ^ u32::from(byte)) ^ (crc >> 8);
    }
    crc
}

/// The register after one zero byte, from register `reg`: the linear map
/// the exhaustive bit-flip proof iterates (`spill::crc32c_tests`).
#[cfg(test)]
pub(super) fn zero_byte(reg: u32) -> u32 {
    t(0, reg) ^ (reg >> 8)
}

/// The register contribution of one byte holding only bit `bit`, from a
/// zero register.
#[cfg(test)]
pub(super) fn single_bit(bit: u32) -> u32 {
    t(0, 1 << bit)
}

/// Bit-at-a-time reference, for the table cross-check.
#[cfg(test)]
pub(super) fn reference(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
        }
    }
    !crc
}
