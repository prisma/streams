//! Provisional continuation (TLA-018-F3). An applied page may end past the
//! durable frontier. The position it returns continues a suffix that a
//! replacement owner can lose and rewrite at the same offsets, so a tail
//! comparison cannot tell a continuation from a stale position. A
//! [`Continuation`] names the writer history that served the suffix and
//! digests what the client observed from its first unproven offset; the
//! next read either proves that history unchanged or refuses with the durable
//! recovery position.
#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]

use super::read::{PlainBatch, PlainRec, ReadPage, ReadPosition};

/// The shard writer that served a provisional suffix: the shard DB prefix and
/// the SlateDB writer epoch its engine claimed when it opened. Opening a
/// writer claims a new epoch in the manifest before its first write, and it
/// fences every earlier writer, so no two writers of one DB share an epoch.
/// Within one writer, applied state only moves forward: a suffix it applied
/// either becomes durable unchanged or is lost with the writer. An equal
/// history therefore proves the suffix was not replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WriterHistory([u8; 16]);

impl WriterHistory {
    /// A page from an owner that reports no history (a release before this
    /// change). It never equals a writer's history, so the next read
    /// verifies the observation instead of trusting the owner.
    pub(crate) const UNKNOWN: Self = Self([0; 16]);

    pub(crate) fn of(engine: &crate::shard::ShardEngine) -> Self {
        Self::of_writer(&engine.prefix, engine.writer_epoch)
    }

    fn of_writer(prefix: &str, writer_epoch: u64) -> Self {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"\0streams-writer-history\0");
        hash.update(prefix.as_bytes());
        hash.update([0]);
        hash.update(writer_epoch.to_le_bytes());
        Self(truncate(&hash.finalize()))
    }
}

fn truncate(full: &[u8]) -> [u8; 16] {
    let mut out = [0; 16];
    if let Some(prefix) = full.first_chunk::<16>() {
        out = *prefix;
    }
    out
}

/// The digest of an empty observation.
const EMPTY: [u8; 16] = [0; 16];

/// Keys the observation digest with a subkey of the stream key, so a token
/// never carries a bare hash of plaintext that someone holding the token but
/// not the key could test guesses against.
pub(crate) struct ObservationKey([u8; 32]);

impl ObservationKey {
    pub(crate) fn of(key: &crate::crypto::StreamKey, epoch: &[u8; 16]) -> Self {
        Self(crate::crypto::derive_subkey(
            key,
            epoch,
            "\u{0}read-continuation\u{0}",
            0,
        ))
    }
}

/// One observed record folded into a running digest. The fold is sequential,
/// so a digest carried across pages equals the digest of their concatenation.
fn fold(key: &ObservationKey, seed: [u8; 16], record: &PlainRec) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(key.0);
    hash.update(seed);
    hash.update(record.off.to_le_bytes());
    hash.update(
        u64::try_from(record.rkey.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hash.update(record.rkey.as_bytes());
    hash.update(
        u64::try_from(record.payload.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hash.update(&record.payload[..]);
    truncate(&hash.finalize())
}

/// A read position that continues a provisional suffix.
///
/// Invariants, all relative to the position `at` it continues:
/// `recover < at` and `from <= at`. Records the client observed below
/// `recover` were durable when observed. `digest` folds the records the
/// client observed in `[from, at)`, and the client observed every record
/// the read's selector matched there. When `from > recover`, the client
/// never observed `[recover, from)` (it started at the applied tail), so a
/// replaced history cannot be verified and always resynchronises.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Continuation {
    history: WriterHistory,
    recover: u64,
    from: u64,
    digest: [u8; 16],
}

impl Continuation {
    /// The continuation a page returns with its next position, or `None`
    /// when that position is durable (nothing provisional to continue).
    /// `incoming` is the verified continuation the page started at, if it
    /// started at one; `durable` is the page's durable resume position.
    #[expect(
        clippy::too_many_arguments,
        reason = "Continuation::after_page; a page's continuation is a function of its start, records and both positions, the continuation it began at, and the writer and key it is observed under; a struct would exist only to carry them into this one computation"
    )]
    pub(crate) fn after_page(
        incoming: Option<&Self>,
        start: u64,
        records: &PlainBatch,
        next: ReadPosition,
        durable: ReadPosition,
        history: WriterHistory,
        key: &ObservationKey,
    ) -> Option<Self> {
        if next.segment != durable.segment || next.after <= durable.after {
            return None;
        }
        let recover = durable.after;
        let (from, seed, observed_from) = if recover >= start {
            (recover, EMPTY, recover)
        } else {
            match incoming {
                Some(carried) => (carried.from, carried.digest, start),
                None => (start, EMPTY, start),
            }
        };
        let digest = records
            .iter()
            .filter(|record| record.off >= observed_from)
            .fold(seed, |digest, record| fold(key, digest, record));
        Some(Self {
            history,
            recover,
            from,
            digest,
        })
    }

    /// Whether this continuation can belong to position `at`.
    pub(crate) const fn fits(&self, at: u64) -> bool {
        self.recover < at && self.from <= at
    }

    pub(crate) const fn history(&self) -> WriterHistory {
        self.history
    }

    /// The durable position a refused continuation resumes from.
    pub(crate) const fn recover(&self) -> u64 {
        self.recover
    }

    /// The first offset a verification read must cover.
    pub(crate) const fn from(&self) -> u64 {
        self.from
    }

    /// Whether `page`, a read of `[from, at)` on another writer's history,
    /// holds exactly what the client observed. An observation that does not
    /// reach down to the recovery position, or a page that did not cover the
    /// whole range, proves nothing.
    pub(crate) fn observed_in(&self, page: &ReadPage, at: u64, key: &ObservationKey) -> bool {
        self.from <= self.recover
            && page.completed
            && page.end >= at
            && page
                .recs
                .iter()
                .filter(|record| record.off < at)
                .fold(EMPTY, |digest, record| fold(key, digest, record))
                == self.digest
    }

    /// Wire parts for a signed cursor: history, recovery offset, digest start
    /// and digest.
    pub(crate) const fn to_parts(self) -> ([u8; 16], u64, u64, [u8; 16]) {
        (self.history.0, self.recover, self.from, self.digest)
    }

    /// Rebuilds a continuation from an authenticated token. The caller
    /// checks it [`fits`](Self::fits) the position it arrived with.
    pub(crate) const fn from_parts(
        history: [u8; 16],
        recover: u64,
        from: u64,
        digest: [u8; 16],
    ) -> Self {
        Self {
            history: WriterHistory(history),
            recover,
            from,
            digest,
        }
    }

    /// The peer header form: hex of the wire parts.
    pub(crate) fn to_header(self) -> String {
        let mut raw = Vec::with_capacity(48);
        raw.extend_from_slice(&self.history.0);
        raw.extend_from_slice(&self.recover.to_le_bytes());
        raw.extend_from_slice(&self.from.to_le_bytes());
        raw.extend_from_slice(&self.digest);
        crate::crypto::hex(&raw)
    }

    pub(crate) fn from_header(value: &str) -> Option<Self> {
        let raw = crate::crypto::unhex(value)?;
        let raw: &[u8; 48] = raw.as_slice().try_into().ok()?;
        let (history, rest) = raw.split_first_chunk::<16>()?;
        let (recover, rest) = rest.split_first_chunk::<8>()?;
        let (from, digest) = rest.split_first_chunk::<8>()?;
        Some(Self::from_parts(
            *history,
            u64::from_le_bytes(*recover),
            u64::from_le_bytes(*from),
            *digest.first_chunk::<16>()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{Continuation, ObservationKey, PlainBatch, ReadPage, ReadPosition, WriterHistory};
    use crate::application::read::Watermarks;

    fn batch(records: &[(u64, &str)]) -> PlainBatch {
        let mut batch = PlainBatch::default();
        for (off, body) in records {
            let bytes = body.as_bytes().to_vec();
            let len = bytes.len();
            batch.push_decoded(bytes, std::iter::once((*off, 0..len, String::new())));
        }
        batch
    }

    fn at(after: u64) -> ReadPosition {
        ReadPosition { segment: 3, after }
    }

    fn page(records: &[(u64, &str)], end: u64, completed: bool) -> ReadPage {
        ReadPage {
            watermarks: Watermarks {
                durable: end,
                applied: end,
            },
            recs: batch(records),
            last: records.last().map(|(off, _)| *off),
            end,
            completed,
        }
    }

    const H1: WriterHistory = WriterHistory([1; 16]);

    fn key() -> ObservationKey {
        ObservationKey::of(&crate::crypto::StreamKey([9; 32]), &[1; 16])
    }

    #[test]
    fn a_durable_next_position_carries_no_continuation() {
        let records = batch(&[(0, "a"), (1, "b")]);
        assert_eq!(
            Continuation::after_page(None, 0, &records, at(2), at(2), H1, &key()),
            None
        );
        let hopped = ReadPosition {
            segment: 4,
            after: 0,
        };
        assert_eq!(
            Continuation::after_page(None, 0, &records, hopped, hopped, H1, &key()),
            None
        );
    }

    #[test]
    fn a_digest_carried_across_pages_equals_the_single_page_digest() {
        // One page over [1, 4) with durable frontier 1 ...
        let whole = Continuation::after_page(
            None,
            1,
            &batch(&[(1, "b"), (2, "c"), (3, "d")]),
            at(4),
            at(1),
            H1,
            &key(),
        )
        .unwrap();
        // ... equals [1, 2) then [2, 4) while the frontier stays at 1.
        let first =
            Continuation::after_page(None, 1, &batch(&[(1, "b")]), at(2), at(1), H1, &key())
                .unwrap();
        let second = Continuation::after_page(
            Some(&first),
            2,
            &batch(&[(2, "c"), (3, "d")]),
            at(4),
            at(1),
            H1,
            &key(),
        )
        .unwrap();
        assert_eq!(whole, second);
        assert_eq!((second.recover(), second.from()), (1, 1));
        // A frontier that passes the page start drops the proven prefix.
        let advanced = Continuation::after_page(
            Some(&first),
            2,
            &batch(&[(2, "c"), (3, "d")]),
            at(4),
            at(3),
            H1,
            &key(),
        )
        .unwrap();
        assert_eq!((advanced.recover(), advanced.from()), (3, 3));
        assert!(advanced.observed_in(&page(&[(3, "d")], 4, true), 4, &key()));
    }

    #[test]
    fn only_the_observed_records_prove_a_replaced_history() {
        let claim = Continuation::after_page(
            None,
            0,
            &batch(&[(0, "a"), (1, "b"), (2, "c")]),
            at(3),
            at(1),
            H1,
            &key(),
        )
        .unwrap();
        assert!(claim.fits(3) && !claim.fits(1));
        assert!(claim.observed_in(&page(&[(1, "b"), (2, "c")], 5, true), 3, &key()));
        // A replacement record, a missing record, a short tail or a partial
        // page proves nothing.
        assert!(!claim.observed_in(&page(&[(1, "X"), (2, "c")], 5, true), 3, &key()));
        assert!(!claim.observed_in(&page(&[(2, "c")], 5, true), 3, &key()));
        assert!(!claim.observed_in(&page(&[(1, "b")], 2, true), 3, &key()));
        assert!(!claim.observed_in(&page(&[(1, "b"), (2, "c")], 5, false), 3, &key()));
        // The digest is keyed: another stream key never proves it.
        let other = ObservationKey::of(&crate::crypto::StreamKey([8; 32]), &[1; 16]);
        assert!(!claim.observed_in(&page(&[(1, "b"), (2, "c")], 5, true), 3, &other));
    }

    #[test]
    fn records_from_the_continued_position_on_are_not_part_of_the_observation() {
        // The client observed [1, 3); a verification page that also holds
        // the records at and past 3 still proves exactly that.
        let claim = Continuation::after_page(
            None,
            0,
            &batch(&[(0, "a"), (1, "b"), (2, "c")]),
            at(3),
            at(1),
            H1,
            &key(),
        )
        .unwrap();
        let wider = page(&[(1, "b"), (2, "c"), (3, "d"), (4, "e")], 5, true);
        assert!(claim.observed_in(&wider, 3, &key()));
    }

    #[test]
    fn a_session_started_at_the_applied_tail_cannot_be_verified() {
        // "now" at applied tail 4 with durable frontier 2: offsets [2, 4)
        // were never observed, so another history can never prove them.
        let claim =
            Continuation::after_page(None, 4, &PlainBatch::default(), at(4), at(2), H1, &key())
                .unwrap();
        assert_eq!((claim.recover(), claim.from()), (2, 4));
        assert!(!claim.observed_in(&page(&[], 6, true), 4, &key()));
    }

    #[test]
    fn the_peer_header_round_trips_and_rejects_malformed_values() {
        let claim = Continuation::from_parts([7; 16], 5, 3, [9; 16]);
        assert_eq!(Continuation::from_header(&claim.to_header()), Some(claim));
        assert_eq!(Continuation::from_header("zz"), None);
        assert_eq!(Continuation::from_header(&"00".repeat(47)), None);
        assert_ne!(
            WriterHistory::of_writer("00", 1),
            WriterHistory::of_writer("00", 2)
        );
        assert_ne!(
            WriterHistory::of_writer("00", 1),
            WriterHistory::of_writer("01", 1)
        );
        assert_ne!(WriterHistory::of_writer("00", 1), WriterHistory::UNKNOWN);
    }
}
