//! Checked cursor fields and authenticated payloads. Encoders retain the wire format.
#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]

use super::{
    CatalogCursor, KIND_CATALOG_V1, KIND_KEY_V2, KIND_LEASE_V2, KIND_SCAN_V2, KeyCursor,
    LeaseToken, MAC_LEN, MessageId, SCAN_CURSOR_MAX, ScanCursor, StreamKey, mac_key, mac16, unb64,
};
use crate::tenant::ProjectId;

// The historical bound was 21849, but base64 length 1 modulo 4 is invalid.
// Use its largest decodable length, preserving kind-error priority.
const SCAN_ENCODED_MAX: usize = SCAN_CURSOR_MAX.div_ceil(3) * 4;

/// Advance only after the next complete fixed-width field is available.
fn field<const N: usize>(input: &mut &[u8]) -> Option<[u8; N]> {
    let (value, rest) = input.split_first_chunk::<N>()?;
    *input = rest;
    Some(*value)
}

/// Shared position prefix carried by key cursors, message IDs and leases.
fn position(input: &mut &[u8]) -> Option<MessageId> {
    Some(MessageId {
        epoch: field(input)?,
        key_hash: field(input)?,
        seg_id: u32::from_le_bytes(field(input)?),
        offset: u64::from_le_bytes(field(input)?),
    })
}

fn signed_payload<'a>(raw: &'a [u8], key: &[u8; 32]) -> Option<&'a [u8]> {
    let (payload, mac) = raw.split_last_chunk::<MAC_LEN>()?;
    let want = mac16(key, payload);
    let difference = mac.iter().zip(want).fold(0u8, |acc, (a, b)| acc | (a ^ b));
    (difference == 0).then_some(payload)
}

fn product_payload<'a>(raw: &'a [u8], project: &ProjectId, key: &StreamKey) -> Option<&'a [u8]> {
    let (_, after_kind) = raw.split_first()?;
    let (epoch, _) = after_kind.split_first_chunk::<16>()?;
    signed_payload(raw, &mac_key(project, key, epoch))
}

impl KeyCursor {
    /// Kind errors precede shape errors; authentication binds the requested project and stream.
    pub(crate) fn decode(
        s: &str,
        project: &ProjectId,
        key: &StreamKey,
        expect_epoch: &[u8; 16],
        expect_key_hash: &[u8; 16],
    ) -> Result<Self, &'static str> {
        let invalid = "invalid_cursor";
        let raw = unb64(s).ok_or(invalid)?;
        if raw.first() != Some(&KIND_KEY_V2) {
            return Err("wrong_cursor_kind");
        }
        let raw: &[u8; 45 + MAC_LEN] = raw.as_slice().try_into().map_err(|_| invalid)?;
        let payload = product_payload(raw, project, key).ok_or(invalid)?;
        let (_, mut fields) = payload.split_first().ok_or(invalid)?;
        let value = position(&mut fields).ok_or(invalid)?;
        if &value.epoch != expect_epoch || &value.key_hash != expect_key_hash {
            return Err(invalid);
        }
        Ok(Self {
            epoch: value.epoch,
            key_hash: value.key_hash,
            seg_id: value.seg_id,
            offset: value.offset,
        })
    }
}

impl CatalogCursor {
    pub(crate) fn decode(
        s: &str,
        expect_project: &ProjectId,
        key: Option<&[u8; 32]>,
    ) -> Option<String> {
        let raw = unb64(s)?;
        let body = match key {
            Some(key) => signed_payload(&raw, key)?,
            None => raw.as_slice(),
        };
        let (kind, mut fields) = body.split_first()?;
        if *kind != KIND_CATALOG_V1 {
            return None;
        }
        let project_len = usize::from(u16::from_le_bytes(field(&mut fields)?));
        let (project, name) = fields.split_at_checked(project_len)?;
        if std::str::from_utf8(project).ok()? != expect_project.as_str() {
            return None;
        }
        String::from_utf8(name.to_vec()).ok()
    }
}

impl ScanCursor {
    pub(crate) fn decode(
        s: &str,
        project: &ProjectId,
        key: &StreamKey,
        expect_epoch: &[u8; 16],
        now_ms: i64,
    ) -> Result<Self, &'static str> {
        let invalid = "invalid_cursor";
        if s.len() > SCAN_ENCODED_MAX {
            return Err(invalid);
        }
        let raw = unb64(s).ok_or(invalid)?;
        if raw.first() != Some(&KIND_SCAN_V2) {
            return Err("wrong_cursor_kind");
        }
        let payload = product_payload(&raw, project, key).ok_or(invalid)?;
        let (_, mut fields) = payload.split_first().ok_or(invalid)?;
        let epoch = field(&mut fields).ok_or(invalid)?;
        let map_version = u64::from_le_bytes(field(&mut fields).ok_or(invalid)?);
        let count = u32::from_le_bytes(field(&mut fields).ok_or(invalid)?);
        let (rows, tail) = fields.split_last_chunk::<20>().ok_or(invalid)?;
        let (chunks, remainder) = rows.as_chunks::<12>();
        // Admit the actual encoded row count before allocating. The wire-size
        // cap is stricter than the old, redundant 4096-segment bound.
        if !remainder.is_empty() || usize::try_from(count).ok() != Some(chunks.len()) {
            return Err(invalid);
        }
        let segments = chunks
            .iter()
            .map(|row| {
                let mut row = row.as_slice();
                Some((
                    u32::from_le_bytes(field(&mut row)?),
                    u64::from_le_bytes(field(&mut row)?),
                ))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(invalid)?;
        let mut tail = tail.as_slice();
        let current_index = u32::from_le_bytes(field(&mut tail).ok_or(invalid)?);
        let current_offset = u64::from_le_bytes(field(&mut tail).ok_or(invalid)?);
        let expires_at_ms = i64::from_le_bytes(field(&mut tail).ok_or(invalid)?);
        if &epoch != expect_epoch {
            return Err(invalid);
        }
        if now_ms > expires_at_ms {
            return Err("scan_expired");
        }
        Ok(Self {
            epoch,
            map_version,
            segments,
            current_index,
            current_offset,
            expires_at_ms,
        })
    }
}

// The service emits message IDs and decodes lease tokens. Standalone message-ID
// decoding is retained only for its wire-format verification tests.
#[cfg(test)]
impl MessageId {
    pub(crate) fn decode(
        s: &str,
        project: &ProjectId,
        key: &StreamKey,
        expect_epoch: &[u8; 16],
    ) -> Result<Self, &'static str> {
        let invalid = "invalid_message_id";
        let raw = unb64(s).ok_or(invalid)?;
        if raw.first() != Some(&super::KIND_MSG_V2) {
            return Err("wrong_token_kind");
        }
        let raw: &[u8; 45 + MAC_LEN] = raw.as_slice().try_into().map_err(|_| invalid)?;
        let payload = product_payload(raw, project, key).ok_or(invalid)?;
        let (_, mut fields) = payload.split_first().ok_or(invalid)?;
        let value = position(&mut fields).ok_or(invalid)?;
        if &value.epoch != expect_epoch {
            return Err(invalid);
        }
        Ok(value)
    }
}

impl LeaseToken {
    pub(crate) fn decode(
        s: &str,
        project: &ProjectId,
        key: &StreamKey,
        expect_epoch: &[u8; 16],
    ) -> Result<Self, &'static str> {
        let invalid = "invalid_lease_token";
        let raw = unb64(s).ok_or(invalid)?;
        if raw.first() != Some(&KIND_LEASE_V2) {
            return Err("wrong_token_kind");
        }
        let raw: &[u8; 65 + MAC_LEN] = raw.as_slice().try_into().map_err(|_| invalid)?;
        let payload = product_payload(raw, project, key).ok_or(invalid)?;
        let (_, mut fields) = payload.split_first().ok_or(invalid)?;
        let msg = position(&mut fields).ok_or(invalid)?;
        let lease_gen = u32::from_le_bytes(field(&mut fields).ok_or(invalid)?);
        let consumer_gen = u64::from_le_bytes(field(&mut fields).ok_or(invalid)?);
        let deadline_ms = i64::from_le_bytes(field(&mut fields).ok_or(invalid)?);
        if &msg.epoch != expect_epoch {
            return Err(invalid);
        }
        Ok(Self {
            msg,
            lease_gen,
            consumer_gen,
            deadline_ms,
        })
    }
}
