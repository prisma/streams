//! JSON prefix selection with a hard bound on both rows and encoded bytes.
use serde::Serialize;
use std::io::{self, Write};

pub(crate) enum Selection {
    Encoded {
        body: Vec<u8>,
        count: usize,
    },
    /// The first event cannot fit even in an otherwise empty batch.
    Oversized,
}
struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    overflow: bool,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.overflow = true;
            return Err(io::Error::other("journal row exceeds byte budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
/// Selection does not remove anything. Callers hold their queue lock until
/// they transfer exactly the selected prefix into the cancellation guard.
pub(crate) fn encode_prefix<'a, T: Serialize + 'a>(
    events: impl Iterator<Item = &'a T>,
    max_bytes: usize,
) -> Result<Selection, String> {
    if max_bytes < 2 {
        return Err("journal byte budget cannot encode an array".into());
    }
    let mut body = Vec::with_capacity(max_bytes.min(4096));
    body.push(b'[');
    let mut count = 0;
    for event in events.take(512) {
        let mut row = BoundedWriter {
            bytes: Vec::with_capacity(max_bytes.min(4096)),
            limit: max_bytes - 2,
            overflow: false,
        };
        if let Err(error) = serde_json::to_writer(&mut row, event) {
            if !row.overflow {
                return Err(error.to_string());
            }
            if count == 0 {
                return Ok(Selection::Oversized);
            }
            break;
        }
        let with_array_end = body
            .len()
            .saturating_add(row.bytes.len())
            .saturating_add(1 + usize::from(count > 0));
        if with_array_end > max_bytes {
            break;
        }
        if count > 0 {
            body.push(b',');
        }
        body.extend_from_slice(&row.bytes);
        count += 1;
    }
    body.push(b']');
    Ok(Selection::Encoded { body, count })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn array_delimiters_and_escaped_strings_count_toward_exact_limit() {
        let event = "\"\\\n".repeat(100);
        let expected = serde_json::to_vec(&[&event]).unwrap();
        let Selection::Encoded { body, count } =
            encode_prefix([&event].into_iter(), expected.len()).unwrap()
        else {
            panic!("exact fit must encode");
        };
        assert_eq!(count, 1);
        assert_eq!(body, expected);
        assert!(matches!(
            encode_prefix([&event].into_iter(), expected.len() - 1).unwrap(),
            Selection::Oversized
        ));
    }
}
