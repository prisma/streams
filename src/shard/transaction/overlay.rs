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
    /// The native queue's rows staged by this group (§5).
    pub native: Option<super::native_queue::stage::NativeStage>,
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
    /// The group's pages of this stream as unsealed pages (layout 6): one
    /// merge operand on its pending row, and its handle's pending pages
    /// once published.
    pub pending: Vec<crate::spill::pending::PendingPage>,
    /// Set when this group seals the stream (`transaction::seal`): the
    /// boundary it moves to and the applied pages its remainder keeps.
    pub sealed: Option<super::seal::SealedAt>,
    /// The seal is the group's only op on the stream: its waiters wait for
    /// nothing it changed.
    pub seal_only: bool,
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
impl StreamOverlay {
    /// Publish this group's applied effects into its stream's state: the
    /// tail (with a billed group's usage counters and static meta, layout 6
    /// f3), its unsealed pages (b1, less those a seal took), the pressure attribution, producers,
    /// sequences and queue rows.
    pub(super) fn publish_into(&self, st: &mut StreamState) {
        st.applied = self.fields.clone();
        for page in &self.frames.pending {
            st.pending.push(page.clone());
        }
        st.pending.seal_below(self.fields.absorbed);
        if let Some(b) = self.handle.pressure.get() {
            b.frames_added(self.frames.added_bytes);
            b.frames_retired(self.frames.retired_bytes);
        }
        for (plane, v) in &self.producer.rows {
            st.producers.insert(plane.clone(), *v);
        }
        for (kh, v) in &self.producer.seqs {
            st.seqs.insert(*kh, v.clone());
        }
        if let Some(q) = &self.queue.state {
            st.queue = (*q).clone();
        }
    }
}
impl StreamOverlay {
    /// Wake the stream's applied waiters, unless the group's only op on it
    /// was a seal, which changes nothing they wait for (layout 6).
    pub(super) fn notify_applied(&self) {
        if !self.frames.seal_only {
            self.handle.applied_notify.notify_waiters();
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
            key_version: 0,
            routing_key: "",
        };
        let sealed = cipher
            .seal(&lane, first, &crate::crypto_page::stamped(2, &[payload]))
            .unwrap();
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
