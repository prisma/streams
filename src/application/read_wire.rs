//! Bounded wire decoding shared by replay and physical-span scan peers.
use super::read::{PlainBatch, ReadPage, Watermarks};
use super::read_budget::{MAX_PAGE_RECORDS, PageBudget, max_wire_bytes};
use super::read_remote::RemoteSpanError;
use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct WireRecord {
    pub off: u64,
    #[serde(alias = "rk")]
    pub key: String,
    #[serde(alias = "p")]
    pub payload: String,
}

// Reject the 4097th element before materializing it. A body-byte cap alone
// does not cap allocation overhead from millions of tiny JSON objects.
pub(super) fn records<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<WireRecord>, D::Error> {
    struct Records;
    impl<'de> serde::de::Visitor<'de> for Records {
        type Value = Vec<WireRecord>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "at most {MAX_PAGE_RECORDS} records")
        }
        #[expect(
            clippy::excessive_nesting,
            reason = "visit_seq; the visitor nests the record-limit stop inside the sequence walk; flattening it would separate the stop from the element it refuses"
        )]
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut records = Vec::new();
            while records.len() < MAX_PAGE_RECORDS {
                let Some(record) = seq.next_element()? else {
                    return Ok(records);
                };
                records.push(record);
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("peer page record limit exceeded"));
            }
            Ok(records)
        }
    }
    deserializer.deserialize_seq(Records)
}

pub(super) fn decode_records(
    records: Vec<WireRecord>,
    max_bytes: usize,
) -> Result<PlainBatch, RemoteSpanError> {
    let mut budget = PageBudget::new(max_bytes);
    let mut decoded = PlainBatch::default();
    for record in records {
        if !budget.metadata_fits(&record.key)
            || record.payload.len() > budget.decode_limit().div_ceil(3) * 4
        {
            return Err(invalid("peer page exceeds its payload or metadata budget"));
        }
        let payload = base64::engine::general_purpose::STANDARD
            .decode(record.payload)
            .map_err(|e| invalid(&e.to_string()))?;
        if !decoded.admit_owned(record.off, payload, record.key, &mut budget) {
            return Err(invalid("peer page exceeds its plaintext budget"));
        }
    }
    Ok(decoded)
}

pub(super) async fn body(
    content_length: Option<u64>,
    stream: impl futures_util::Stream<Item = Result<Bytes, reqwest::Error>>,
) -> Result<Bytes, RemoteSpanError> {
    use futures_util::StreamExt;
    if content_length.is_some_and(|len| len > max_wire_bytes() as u64) {
        return Err(invalid("peer page exceeds the wire bound"));
    }
    futures_util::pin_mut!(stream);
    let mut bytes = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| RemoteSpanError::Transport(e.to_string()))?;
        if chunk.len() > max_wire_bytes().saturating_sub(bytes.len()) {
            return Err(invalid("peer page exceeds the wire bound"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}

#[derive(Deserialize)]
struct WireScanPage {
    #[serde(deserialize_with = "records")]
    items: Vec<WireRecord>,
    #[serde(deserialize_with = "required_option")]
    last: Option<u64>,
    end: u64,
    completed: bool,
}
pub(super) fn scan_page(bytes: &[u8], max_bytes: usize) -> Result<ReadPage, RemoteSpanError> {
    let page: WireScanPage = serde_json::from_slice(bytes).map_err(|e| invalid(&e.to_string()))?;
    Ok(ReadPage {
        watermarks: Watermarks {
            durable: page.end,
            applied: page.end,
        },
        recs: decode_records(page.items, max_bytes)?,
        last: page.last,
        end: page.end,
        completed: page.completed,
    })
}
fn invalid(message: &str) -> RemoteSpanError {
    RemoteSpanError::InvalidResponse(message.into())
}

fn required_option<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    Option::deserialize(d)
}

#[cfg(test)]
#[path = "read_wire_tests.rs"]
mod tests;
