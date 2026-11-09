//! The merged leaf's sentinel rows (S6-DESIGN rev 6 §7.3, L2 at a merge): from the two sources'
//! sentinel rows, by `keyspace::merge_rules`. A unioned key both sources hold must have equal
//! values, and lands once. The per-object counts (`'R'`, `'O'`) sum, and a zero sum is absent.
//! Dropped, fresh, recomputed and absent tags are left to the merge. A row it cannot account for
//! refuses the whole rebuild, so a merge never seeds `M` from state it does not understand.

use std::collections::BTreeMap;

use crate::keyspace::SENTINEL;
use crate::keyspace::merge_rules::{MergeRule, merge_rule};
use crate::spill::gc::{decode_refcount, encode_refcount};
use crate::spill::runindex::{decode_orphans, encode_orphans};

/// A source's sentinel rows, key to value.
pub(crate) type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

/// Why the rows cannot be merged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MergeRefusal {
    /// A key both sources hold, with differing values, under a union rule (an immutable `'K'` or
    /// `'I'` row copied to both, or an entity that should live in one source).
    Differs(Vec<u8>),
    /// A count both sources hold that does not decode, or names one object with two lengths.
    Count(Vec<u8>),
    /// A key that is no sentinel row, or of a tag without a merge rule.
    Unknown(Vec<u8>),
}

/// `M`'s rows from the sources' `a` and `b` (see the module).
pub(crate) fn merged_sentinel(a: &Rows, b: &Rows) -> Result<Rows, MergeRefusal> {
    let mut out = Rows::new();
    for key in a.keys().chain(b.keys()) {
        if out.contains_key(key) {
            continue;
        }
        let rule = key
            .strip_prefix(&SENTINEL[..])
            .and_then(|rest| rest.first())
            .and_then(|t| merge_rule(*t))
            .ok_or_else(|| MergeRefusal::Unknown(key.clone()))?;
        let value = match (rule, a.get(key), b.get(key)) {
            (MergeRule::Union, Some(x), Some(y)) if x != y => {
                return Err(MergeRefusal::Differs(key.clone()));
            }
            (MergeRule::Union, Some(x), _) | (MergeRule::Union, None, Some(x)) => Some(x.clone()),
            (MergeRule::Sum, x, y) => summed(key, x, y)?,
            _ => None,
        };
        if let Some(value) = value {
            out.insert(key.clone(), value);
        }
    }
    Ok(out)
}

/// The sum of a count both, one or neither source holds; `None` when it is zero.
fn summed(
    key: &[u8],
    x: Option<&Vec<u8>>,
    y: Option<&Vec<u8>>,
) -> Result<Option<Vec<u8>>, MergeRefusal> {
    let refusal = || MergeRefusal::Count(key.to_vec());
    let is_refcount = key.get(16) == Some(&crate::keyspace::tag::REFCOUNT);
    if is_refcount {
        let decode = |v: Option<&Vec<u8>>| {
            v.map(|v| decode_refcount(v).ok_or_else(refusal))
                .transpose()
        };
        let sum = match (decode(x)?, decode(y)?) {
            (Some(p), Some(q)) => crate::spill::gc::merged_refcount(p, q).ok_or_else(refusal)?,
            (Some(p), None) | (None, Some(p)) => p,
            (None, None) => return Ok(None),
        };
        return Ok((sum.live_chunks > 0).then(|| encode_refcount(&sum)));
    }
    let decode = |v: Option<&Vec<u8>>| v.map(|v| decode_orphans(v).ok_or_else(refusal)).transpose();
    let sum = match (decode(x)?, decode(y)?) {
        (Some(p), Some(q)) => p.add(q),
        (Some(p), None) | (None, Some(p)) => p,
        (None, None) => return Ok(None),
    };
    Ok((sum.chunks > 0).then(|| encode_orphans(sum)))
}

#[cfg(test)]
mod tests;
