use super::*;
use crate::application::read::Watermarks;
use crate::application::read_retention_probe::Probe;
use crate::crypto::{FrameCipher, FrameCompression, StreamKey, derive_subkey};
fn page() -> ReadPage {
    ReadPage {
        recs: PlainBatch::default(),
        watermarks: Watermarks {
            durable: 64,
            applied: 64,
        },
        last: None,
        end: 64,
        completed: true,
    }
}
#[expect(
    clippy::cast_possible_truncation,
    reason = "o2b_mixed_compressed_pages_preserve_failure_and_withholding_boundaries; the fixture's offsets are counters below 256 that seed and check each payload byte; checked conversions would only restate the fixture's size"
)]
#[tokio::test]
async fn o2b_mixed_compressed_pages_preserve_failure_and_withholding_boundaries() {
    let key = StreamKey([7; 32]);
    let epoch = [8; 16];
    let hash = [9; 16];
    let subkey = derive_subkey(&key, &epoch, "", 0);
    let plain = FrameCipher::new(&subkey, &hash, FrameCompression::Disabled);
    let compressed = FrameCipher::new(&subkey, &hash, FrameCompression::ZstdLevel1);
    let frames: Vec<_> = (0..64)
        .map(|off| {
            let cipher = if off % 2 == 0 { &plain } else { &compressed };
            let raw =
                bytes::Bytes::from(cipher.encrypt(&hash, off, 123, 0, "", &vec![off as u8; 2048]));
            crate::shard::record::CheckedFrame::from_ring(&raw, off, None)
                .unwrap()
                .unwrap()
        })
        .collect();
    let probe = Probe::default();
    probe
        .scope(async {
            let mut complete = page();
            assert!(
                decode_frames_into(
                    &frames,
                    &mut ReadKeys::new(&key, &epoch, hash),
                    &mut complete,
                    &mut PageBudget::new(128 << 10)
                )
                .unwrap()
            );
            assert_eq!(complete.recs.len(), 64);
            assert_eq!(complete.recs.retained_capacity(), 128 << 10);
            assert!(
                complete.recs.contiguous().is_none(),
                "independent fallback owners must not masquerade as one buffer"
            );
            for rec in &complete.recs {
                assert!(rec.payload.iter().all(|b| *b == rec.off as u8));
            }
            for index in 0..64 {
                assert_eq!(complete.recs[index].off, index as u64);
            }
            drop(complete);
            assert_eq!(probe.live(), 0);
            let mut partial = page();
            assert!(
                !decode_frames_into(
                    &frames,
                    &mut ReadKeys::new(&key, &epoch, hash),
                    &mut partial,
                    &mut PageBudget::new(5 << 10)
                )
                .unwrap()
            );
            assert_eq!(partial.recs.len(), 2);
            assert_eq!(partial.last, Some(1));
            assert_eq!(partial.recs.retained_capacity(), 4096);
            drop(partial);
            assert_eq!(probe.live(), 0);
            let mut frames = frames;
            let mut damaged = frames[63].to_vec();
            *damaged.last_mut().unwrap() ^= 1;
            frames[63] = crate::shard::record::CheckedFrame::from_ring(
                &bytes::Bytes::from(damaged),
                63,
                None,
            )
            .unwrap()
            .unwrap();
            let mut failed = page();
            assert!(
                decode_frames_into(
                    &frames,
                    &mut ReadKeys::new(&key, &epoch, hash),
                    &mut failed,
                    &mut PageBudget::new(128 << 10)
                )
                .is_err()
            );
            assert!(failed.recs.is_empty());
            assert_eq!(
                probe.live(),
                0,
                "all tentative owners must be released on authentication failure"
            );
        })
        .await;
}
