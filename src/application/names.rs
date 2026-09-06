//! Product-addressable names shared by application workflows and route parsing.
const RESERVED_FINAL_SEGMENTS: [&str; 3] = ["records", "consumers", "watches"];

pub struct ProductStreamName(crate::tenant::CanonicalStreamName);

/// Why a name is not product-addressable. Wire messages are pinned to
/// the exact pre-WP-03 strings (characterization rule: same wire).
pub enum ProductNameError {
    Structural(crate::tenant::NameError),
    ReservedSubresourceName,
    SubresourceShapedName,
}

impl ProductNameError {
    pub(crate) fn message(&self) -> &'static str {
        use crate::tenant::NameError as N;
        match self {
            // The product surface collapses length problems into one
            // message and reports positions never — exactly as before.
            Self::Structural(N::Empty | N::TooLong { .. }) => {
                "stream name must be 1-512 UTF-8 bytes"
            }
            Self::Structural(N::ControlChar { .. }) => "control characters are not allowed",
            Self::Structural(N::EmptyComponent) => "empty path segments are not allowed",
            Self::Structural(N::DotComponent) => "'.' and '..' segments are not allowed",
            Self::Structural(N::ReservedRoot) => "the __ds namespace is reserved",
            Self::ReservedSubresourceName => {
                "'records', 'consumers' and 'watches' are reserved subresource names"
            }
            Self::SubresourceShapedName => {
                "this name is already a subresource path (…/records, …/consumers/{name}, …/watches/…)"
            }
        }
    }
}

impl TryFrom<&str> for ProductStreamName {
    type Error = ProductNameError;

    fn try_from(raw: &str) -> Result<Self, ProductNameError> {
        // No structural rule is repeated here (PR 4.1): the canonical
        // layer owns error precedence, including the historical
        // `__ds/..` corner (structural problems before the reserved
        // root); this adapter only maps one canonical error.
        let canonical =
            crate::tenant::CanonicalStreamName::new(raw).map_err(ProductNameError::Structural)?;
        let segments: Vec<&str> = raw.split('/').collect();
        if let Some(last) = segments.last()
            && RESERVED_FINAL_SEGMENTS.contains(last)
        {
            return Err(ProductNameError::ReservedSubresourceName);
        }
        // A name that itself reads as a subresource path would be
        // unaddressable: `x/consumers/records` as a COLLECTION can never
        // be written, because that URL already means consumer "records"
        // on collection `x`. Refuse it at creation rather than hand out
        // a name whose own URL points somewhere else.
        if split_subresource(raw).is_some() {
            return Err(ProductNameError::SubresourceShapedName);
        }
        Ok(Self(canonical))
    }
}

impl ProductStreamName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_canonical(self) -> crate::tenant::CanonicalStreamName {
        self.0
    }
}

pub(crate) fn split_subresource(path: &str) -> Option<(&str, &str)> {
    let seg: Vec<&str> = path.split('/').collect();
    let n = seg.len();
    // (segments consumed from the end, the shape's leading keyword)
    let shapes: [(usize, &str); 7] = [
        (4, "watches"),   // watches/{watch}/keys/{key}
        (2, "watches"),   // watches/{watch}
        (2, "consumers"), // consumers/{consumer}
        (2, "usage"),     // usage/current
        (1, "watches"),   // watches
        (1, "records"),   // records
        (1, "usage"),     // usage (§10 customer lookup)
    ];
    for (take, head) in shapes {
        if n <= take || seg[n - take] != head {
            continue;
        }
        if take == 4 && seg[n - 2] != "keys" {
            continue;
        }
        let stream_len: usize = seg[..n - take].iter().map(|s| s.len() + 1).sum();
        let stream = &path[..stream_len - 1];
        if stream.is_empty() || !addressable_name(stream) {
            continue;
        }
        return Some((stream, &path[stream_len..]));
    }
    None
}

// mt-lint: allow(name-param-shared-core): identity-neutral URL shape predicate; no state or tenant lookup.
pub(crate) fn addressable_name(name: &str) -> bool {
    !name
        .rsplit('/')
        .next()
        .is_some_and(|last| RESERVED_FINAL_SEGMENTS.contains(&last))
}

pub(crate) fn valid_consumer_name(n: &str) -> Option<String> {
    if n.is_empty()
        || n.len() > 128
        || n.contains('/')
        || n == "."
        || n == ".."
        || n.chars().any(|c| c.is_control())
        || n.contains(':')
    {
        return None;
    }
    Some(n.to_string())
}
