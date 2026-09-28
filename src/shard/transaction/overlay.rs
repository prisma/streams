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
    pub ring: Vec<(u64, Bytes)>,
    /// The group's frame cipher and the subkey it was derived from.
    cipher: Option<(
        [u8; crate::crypto::KEY_LEN],
        Arc<crate::crypto::FrameCipher>,
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
impl FrameEffects {
    /// The frame cipher for `subkey` in this stream's `segment`. Every
    /// request of the group under the same subkey (stream key, epoch, routing
    /// key and key version) shares one derivation of the segment frame key
    /// and its key schedule; another subkey derives its own. Each record
    /// still draws its own nonce. The key material lives only as long as the
    /// group.
    pub(super) fn cipher(
        &mut self,
        subkey: &[u8; crate::crypto::KEY_LEN],
        segment: &[u8; 16],
        compression: crate::crypto::FrameCompression,
    ) -> Arc<crate::crypto::FrameCipher> {
        if let Some((derived_from, cipher)) = &self.cipher
            && same_key(derived_from, subkey)
        {
            return cipher.clone();
        }
        let cipher = Arc::new(crate::crypto::FrameCipher::new(
            subkey,
            segment,
            compression,
        ));
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
    use super::FrameEffects;
    use crate::crypto::{FrameCompression, decode_frame, decrypt_frame};
    use std::sync::Arc;

    /// A group's requests under one subkey share one cipher, and a request
    /// under another subkey gets its own: its frames decrypt under its own
    /// subkey and not under the first.
    #[test]
    fn a_group_shares_a_cipher_per_subkey() {
        let (segment, first, other) = ([3; 16], [7; 32], [8; 32]);
        let mut frames = FrameEffects::default();
        let cipher = frames.cipher(&first, &segment, FrameCompression::Disabled);
        let again = frames.cipher(&first, &segment, FrameCompression::Disabled);
        assert!(Arc::ptr_eq(&cipher, &again), "one derivation per subkey");
        let mut last = [7; 32];
        last[31] = 9;
        for subkey in [other, last] {
            let theirs = frames.cipher(&subkey, &segment, FrameCompression::Disabled);
            assert!(!Arc::ptr_eq(&cipher, &theirs), "{subkey:?} derives its own");
            let frame = theirs.encrypt(&segment, 1, 2, 0, "", b"record");
            let decoded = decode_frame(&frame).unwrap();
            assert_eq!(
                decrypt_frame(&subkey, &segment, &decoded, &frame).unwrap(),
                b"record"
            );
            assert!(decrypt_frame(&first, &segment, &decoded, &frame).is_err());
        }
        let back = frames.cipher(&first, &segment, FrameCompression::Disabled);
        let frame = back.encrypt(&segment, 3, 4, 0, "", b"again");
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(
            decrypt_frame(&first, &segment, &decoded, &frame).unwrap(),
            b"again"
        );
    }
}
