//! Retain canonical admission through filtering/decryption without reparsing
//! scalar metadata. Shared bytes remain immutable and are never plaintext.
use super::{RecordCorruption, decode_at, decode_row};
use crate::crypto::{DecodedFrame, ReadFrameHeader};
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedFrame {
    raw: Bytes,
    offset: u64,
    timestamp: i64,
    key_version: u32,
    routing_end: u32,
    header_len: u32,
    version: u8,
}
impl CheckedFrame {
    pub(crate) fn from_row(
        key: &[u8],
        prefix: &[u8],
        raw: Bytes,
    ) -> Result<Self, RecordCorruption> {
        let parsed = decode_row(key, prefix, &raw)?;
        let metadata = Self::metadata(&parsed);
        Ok(Self::retain(raw, metadata))
    }
    pub(crate) fn from_ring(
        raw: &Bytes,
        offset: u64,
        selector: Option<&str>,
    ) -> Result<Option<Self>, RecordCorruption> {
        let parsed = decode_at(raw, offset)?;
        if selector.is_some_and(|key| key != parsed.header.routing_key) {
            return Ok(None);
        }
        Ok(Some(Self::retain(raw.clone(), Self::metadata(&parsed))))
    }
    fn metadata(frame: &DecodedFrame<'_>) -> (u64, i64, u32, u32, u32, u8) {
        (
            frame.header.offset,
            frame.header.ts_ms,
            frame.header.key_version,
            (23 + frame.header.routing_key.len()) as u32,
            frame.header_len as u32,
            frame.ver,
        )
    }
    fn retain(
        raw: Bytes,
        (offset, timestamp, key_version, routing_end, header_len, version): (
            u64,
            i64,
            u32,
            u32,
            u32,
            u8,
        ),
    ) -> Self {
        Self {
            raw,
            offset,
            timestamp,
            key_version,
            routing_end,
            header_len,
            version,
        }
    }
    pub(crate) fn view(&self) -> DecodedFrame<'_> {
        DecodedFrame {
            header: ReadFrameHeader {
                offset: self.offset,
                ts_ms: self.timestamp,
                key_version: self.key_version,
                // Safe conversion of the retained, validated range. No owned
                // routing string, unchecked UTF-8, or scalar frame reparse.
                routing_key: std::str::from_utf8(&self.raw[23..self.routing_end as usize])
                    .expect("admitted UTF-8"),
            },
            header_len: self.header_len as usize,
            ciphertext: &self.raw[self.header_len as usize + 4..],
            ver: self.version,
        }
    }
}
impl std::ops::Deref for CheckedFrame {
    type Target = Bytes;
    fn deref(&self) -> &Bytes {
        &self.raw
    }
}
impl AsRef<[u8]> for CheckedFrame {
    fn as_ref(&self) -> &[u8] {
        &self.raw
    }
}
impl From<CheckedFrame> for Bytes {
    fn from(frame: CheckedFrame) -> Self {
        frame.raw
    }
}
impl PartialEq<Bytes> for CheckedFrame {
    fn eq(&self, other: &Bytes) -> bool {
        self.raw == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{FrameCipher, FrameCompression, decrypt_frame};

    #[test]
    fn o1_checked_views_retain_exact_metadata_and_borrow_admitted_storage() {
        let long = "x".repeat(u16::MAX as usize);
        for lane in ["", "short", "κλειδί", long.as_str()] {
            for compression in [FrameCompression::Disabled, FrameCompression::ZstdLevel1] {
                let raw = Bytes::from(FrameCipher::new(&[7; 32], &[8; 16], compression).encrypt(
                    &[8; 16],
                    17,
                    -12,
                    9,
                    lane,
                    &vec![0x5a; 4096],
                ));
                let key = crate::shard::record_key(&[8; 16], 17);
                let admitted = CheckedFrame::from_row(&key, &key[..17], raw.clone()).unwrap();
                let checked = admitted.view();
                assert_eq!(
                    (
                        checked.header.offset,
                        checked.header.ts_ms,
                        checked.header.key_version
                    ),
                    (17, -12, 9)
                );
                assert_eq!(checked.header.routing_key, lane);
                assert_eq!(checked.header.routing_key.as_ptr(), raw[23..].as_ptr());
                assert_eq!(
                    checked.ciphertext.as_ptr(),
                    raw[checked.header_len + 4..].as_ptr()
                );
                assert_eq!(
                    decrypt_frame(&[7; 32], &[8; 16], &checked, &admitted).unwrap(),
                    vec![0x5a; 4096]
                );
                assert!(
                    CheckedFrame::from_ring(&raw, 17, Some("nonmatching"))
                        .unwrap()
                        .is_none()
                );
                assert_eq!(
                    CheckedFrame::from_ring(&raw, 17, Some(lane))
                        .unwrap()
                        .unwrap(),
                    admitted
                );
                assert_eq!(Bytes::from(admitted), raw);
            }
        }
    }

    #[test]
    fn o1_structural_admission_never_substitutes_for_authentication() {
        let mut raw = FrameCipher::new(&[7; 32], &[8; 16], FrameCompression::Disabled)
            .encrypt(&[8; 16], 17, 0, 1, "other", b"payload");
        let key = crate::shard::record_key(&[8; 16], 17);
        let end = raw.len() - 1;
        raw[end] ^= 1;
        let frame = CheckedFrame::from_row(&key, &key[..17], Bytes::from(raw.clone())).unwrap();
        assert!(decrypt_frame(&[7; 32], &[8; 16], &frame.view(), &frame).is_err());
        raw[23] = 0xff;
        assert!(
            CheckedFrame::from_ring(&Bytes::from(raw), 17, Some("wanted")).is_err(),
            "malformed nonmatching metadata must not become successful progress"
        );
    }
}
