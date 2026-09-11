//! Authenticated page construction. Uncompressed runs share one planned buffer;
//! fallback decoders transfer independent buffers without an aggregate copy.
use super::read::ReadPage;
use super::read_batch::PlainBatch;
use super::read_budget::PageBudget;
use super::read_keys::ReadKeys;
use crate::crypto::Decrypted;

#[expect(
    clippy::excessive_nesting,
    reason = "decode_frames_into; the decode nests the first-record oversize verdict and the truncation of an admitted-then-refused append inside the per-frame loop; flattening them would separate the verdicts from the frame they refuse"
)]
pub(super) fn decode_frames_into(
    frames: &[crate::shard::record::CheckedFrame],
    keys: &mut ReadKeys<'_>,
    out: &mut ReadPage,
    budget: &mut PageBudget,
) -> Result<bool, String> {
    let mut planned = budget.clone();
    let mut capacity = 0;
    for raw in frames {
        let frame = raw.view();
        if matches!(
            frame.ver,
            crate::crypto::FRAME_VER_Z | crate::crypto::LEGACY_FRAME_VER_Z
        ) {
            break;
        }
        let len = frame.ciphertext.len().saturating_sub(16);
        if !planned.admit(len, frame.header.routing_key) {
            break;
        }
        capacity += len;
    }
    let mut plaintext = Vec::with_capacity(capacity);
    let mut auth = Vec::new();
    let mut pending = Vec::new();
    let mut batch = PlainBatch::default();
    let result = (|| {
        for raw in frames {
            let frame = raw.view();
            let offset = frame.header.offset;
            if !budget.metadata_fits(frame.header.routing_key) {
                out.last = offset.checked_sub(1);
                return Ok(false);
            }
            let Some(decoded) = keys.decrypt_append(
                &frame,
                raw,
                budget.decode_limit(),
                &mut plaintext,
                &mut auth,
            )?
            else {
                if out.recs.is_empty() && batch.is_empty() && pending.is_empty() {
                    return Err("decoded record exceeds 32 MiB".into());
                }
                out.last = offset.checked_sub(1);
                return Ok(false);
            };
            let len = match &decoded {
                Decrypted::Appended(range) => range.len(),
                Decrypted::Owned(bytes) => bytes.len(),
            };
            if !budget.admit(len, frame.header.routing_key) {
                if let Decrypted::Appended(range) = decoded {
                    plaintext.truncate(range.start);
                }
                out.last = offset.checked_sub(1);
                return Ok(false);
            }
            match decoded {
                Decrypted::Appended(range) => {
                    pending.push((offset, range, frame.header.routing_key.to_owned()))
                }
                Decrypted::Owned(bytes) => {
                    batch
                        .push_decoded(std::mem::take(&mut plaintext), std::mem::take(&mut pending));
                    batch.push_decoded(
                        bytes,
                        std::iter::once((offset, 0..len, frame.header.routing_key.to_owned())),
                    );
                }
            }
            out.last = Some(offset);
        }
        Ok(true)
    })();
    // No tentative plaintext escapes a failed authentication. A bounded partial
    // publishes exactly the previously admitted prefix, including mixed formats.
    if result.is_ok() {
        batch.push_decoded(plaintext, pending);
        out.recs.append_admitted(batch);
        debug_assert_eq!(out.recs.retained_capacity(), budget.admitted_bytes());
    }
    result
}

#[cfg(test)]
#[path = "read_decode/tests.rs"]
mod tests;
