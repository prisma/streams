use super::*;
type ProducerPlane = ([u8; 16], String);
type ProducerRecord = (u64, u64, u64, [u8; 16]);
#[derive(Default)]
pub(super) struct ProducerOverlay {
    pub rows: HashMap<ProducerPlane, ProducerRecord>,
    pub seqs: HashMap<[u8; 16], String>,
}
#[derive(Default)]
pub(super) struct QueueOverlay {
    pub state: Option<crate::queue::QueueState>,
    // mt-lint: allow(name-keyed-map): consumer names are scoped by the transaction's tenant-qualified stream hash
    pub configs: HashMap<String, crate::queue::ConsumerRecord>,
}
#[derive(Default)]
pub(super) struct BillingOverlay {
    pub meta: Option<crate::billing::SegmentBillingMetaV1>,
    pub dirty: bool,
    pub month_finals: Vec<crate::billing::SegmentSnapshot>,
}
#[derive(Default)]
pub(super) struct FrameEffects {
    pub payload_bytes: u64,
    pub added_bytes: u64,
    pub retired_bytes: u64,
    /// The group's sealed pages of this stream, for the ring once durable.
    pub ring: RingBatch,
    /// The group's page cipher and the subkey it was derived from.
    cipher: Option<(
        [u8; crate::crypto::KEY_LEN],
        Arc<crate::crypto_page::PageCipher>,
    )>,
}
pub(super) struct StreamOverlay {
    pub handle: Arc<StreamHandle>,
    pub fields: TailFields,
    pub base: TailFields,
    pub producer: ProducerOverlay,
    pub queue: QueueOverlay,
    pub billing: BillingOverlay,
    pub frames: FrameEffects,
}
impl StreamOverlay {
    #[expect(
        clippy::unwrap_used,
        reason = "StreamOverlay::new; a poisoned stream state may hold half-applied fields, producers or queue rows; recovering it could publish or overlay state the write never covered"
    )]
    pub(super) fn new(
        handle: Arc<StreamHandle>,
        billing: Option<crate::billing::SegmentBillingMetaV1>,
    ) -> Self {
        let fields = handle.state.lock().unwrap().applied.clone();
        Self {
            handle,
            base: fields.clone(),
            fields,
            producer: ProducerOverlay::default(),
            queue: QueueOverlay::default(),
            billing: BillingOverlay {
                meta: billing,
                ..Default::default()
            },
            frames: FrameEffects::default(),
        }
    }
}
impl BillingOverlay {
    /// Close the row's storage at `at`: the storage integral advances to
    /// that instant (a month closed on the way is staged as its final), the
    /// gauge goes to zero, and the row takes one version and is written.
    ///
    /// A close that would change nothing is skipped whole: the gauge is
    /// already zero and `at` is not after the storage clock. A skip leaves
    /// `dirty` as it found it, because an earlier op of the group may have
    /// set it. No row: nothing to close.
    pub(super) fn close_storage(&mut self, at: i64) {
        let Some(bm) = self.meta.as_mut() else {
            return;
        };
        if bm.owned_frame_bytes_current == 0 && at <= bm.storage_accounted_through_ms {
            return;
        }
        let finals = &mut self.month_finals;
        bm.advance_storage_clock(at, |closed| {
            finals.push(closed.to_snapshot(true));
        });
        bm.owned_frame_bytes_current = 0;
        bm.usage_version += 1;
        self.dirty = true;
    }
}

impl FrameEffects {
    /// The page cipher for `subkey` in this stream's `segment`. Every
    /// request of the group under the same subkey (stream key, epoch, routing
    /// key and key version) shares one derivation of the segment page key
    /// and its key schedule; another subkey derives its own. Each page still
    /// draws its own nonce. The key material lives only as long as the group.
    pub(super) fn cipher(
        &mut self,
        subkey: &[u8; crate::crypto::KEY_LEN],
        segment: &[u8; 16],
    ) -> Arc<crate::crypto_page::PageCipher> {
        if let Some((derived_from, cipher)) = &self.cipher
            && same_key(derived_from, subkey)
        {
            return cipher.clone();
        }
        let cipher = Arc::new(crate::crypto_page::PageCipher::new(subkey, segment));
        self.cipher = Some((*subkey, cipher.clone()));
        cipher
    }
}

/// Whether two subkeys are equal, in time independent of where they differ.
fn same_key(a: &[u8; crate::crypto::KEY_LEN], b: &[u8; crate::crypto::KEY_LEN]) -> bool {
    a.iter().zip(b).fold(0, |differ, (x, y)| differ | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::{BillingOverlay, FrameEffects};
    use crate::crypto_page::{CheckedPage, PageCipher, PageLane};
    use bytes::Bytes;
    use std::sync::Arc;

    /// Seal `payload` at offset `first` with `cipher` and open it with a
    /// fresh cipher of `subkey`.
    fn reopen(
        cipher: &PageCipher,
        subkey: &[u8; 32],
        segment: &[u8; 16],
        first: u64,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        let lane = PageLane {
            ts_ms: 2,
            key_version: 0,
            routing_key: "",
        };
        let sealed = cipher.seal(&lane, first, &[payload]).unwrap();
        let page = CheckedPage::admit(Bytes::from(sealed.bytes), sealed.last).unwrap();
        let opened = PageCipher::new(subkey, segment).open(&page).ok()?;
        opened
            .records()
            .next()
            .map(|record| record.payload.to_vec())
    }

    /// A close with no billing row changes nothing: no row appears, nothing
    /// is marked for writing and no month is staged.
    #[test]
    fn a_close_without_a_row_changes_nothing() {
        let mut billing = BillingOverlay::default();
        billing.close_storage(1_790_003_600_000);
        assert!(billing.meta.is_none());
        assert_eq!((billing.dirty, billing.month_finals.len()), (false, 0));
    }

    /// A group's requests under one subkey share one cipher, and a request
    /// under another subkey gets its own: its pages open under its own
    /// subkey and not under the first.
    #[test]
    fn a_group_shares_a_cipher_per_subkey() {
        let (segment, first, other) = ([3; 16], [7; 32], [8; 32]);
        let mut frames = FrameEffects::default();
        let cipher = frames.cipher(&first, &segment);
        let again = frames.cipher(&first, &segment);
        assert!(Arc::ptr_eq(&cipher, &again), "one derivation per subkey");
        let mut last = [7; 32];
        last[31] = 9;
        for subkey in [other, last] {
            let theirs = frames.cipher(&subkey, &segment);
            assert!(!Arc::ptr_eq(&cipher, &theirs), "{subkey:?} derives its own");
            assert_eq!(
                reopen(&theirs, &subkey, &segment, 1, b"record").as_deref(),
                Some(&b"record"[..])
            );
            assert_eq!(reopen(&theirs, &first, &segment, 1, b"record"), None);
        }
        let back = frames.cipher(&first, &segment);
        assert_eq!(
            reopen(&back, &first, &segment, 3, b"again").as_deref(),
            Some(&b"again"[..])
        );
    }
}
