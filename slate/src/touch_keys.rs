//! State Protocol touch-key derivation (live.md §"64-bit routing keys").
//! Pure functions shared by the server and clients/benchmarks.
//!
//!   tableKey(entity)              = XXH3_64("tbl\0" + entity)
//!   templateId(entity, fields)    = XXH3_64("tpl\0" + entity + "\0" + join(sortedFields, "\0"))
//!   watchKey(templateId, args...) = XXH3_64("key\0" + templateIdBytesBE + "\0" + join(args, "\0"))
//!
//! 64-bit keys are encoded as 16 lowercase hex chars; the journal's hot-path
//! key IDs are the low 32 bits of the 64-bit key (or xxh32 of the raw string
//! for non-hex16 keys).

use xxhash_rust::xxh3::xxh3_64;
use xxhash_rust::xxh32::xxh32;

pub fn table_key(entity: &str) -> u64 {
    let mut buf = Vec::with_capacity(4 + entity.len());
    buf.extend_from_slice(b"tbl\0");
    buf.extend_from_slice(entity.as_bytes());
    xxh3_64(&buf)
}

/// `fields` must be sorted by name before calling (canonical form).
pub fn template_id(entity: &str, sorted_fields: &[String]) -> u64 {
    let mut buf = Vec::with_capacity(5 + entity.len() + 16 * sorted_fields.len());
    buf.extend_from_slice(b"tpl\0");
    buf.extend_from_slice(entity.as_bytes());
    buf.push(0);
    for (i, f) in sorted_fields.iter().enumerate() {
        if i > 0 {
            buf.push(0);
        }
        buf.extend_from_slice(f.as_bytes());
    }
    xxh3_64(&buf)
}

/// Args must be encoded in sorted-field order.
pub fn watch_key(template_id: u64, args: &[String]) -> u64 {
    let mut buf = Vec::with_capacity(13 + 24 * args.len());
    buf.extend_from_slice(b"key\0");
    buf.extend_from_slice(&template_id.to_be_bytes());
    for a in args {
        buf.push(0);
        buf.extend_from_slice(a.as_bytes());
    }
    xxh3_64(&buf)
}

pub fn key_hex(key: u64) -> String {
    format!("{key:016x}")
}

/// Journal-level uint32 key ID for an API-level key string.
pub fn key_id_of(key: &str) -> u32 {
    if key.len() == 16 {
        if let Ok(v) = u64::from_str_radix(key, 16) {
            return v as u32;
        }
    }
    xxh32(key.as_bytes(), 0)
}

pub fn key_id_of_u64(key: u64) -> u32 {
    key as u32
}

/// Canonical string encoding of a JSON field value for watch-key args
/// (encodings collapse to their string form; nulls/missing become "").
pub fn arg_string(v: Option<&serde_json::Value>) -> String {
    match v {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_and_distinct() {
        let t = table_key("public.todos");
        assert_eq!(t, table_key("public.todos"));
        assert_ne!(t, table_key("public.users"));

        let tpl = template_id("public.todos", &["tenantId".into()]);
        let w1 = watch_key(tpl, &["t1".into()]);
        let w2 = watch_key(tpl, &["t2".into()]);
        assert_ne!(w1, w2);
        assert_eq!(w1, watch_key(tpl, &["t1".into()]));
        assert_eq!(key_hex(w1).len(), 16);
        assert_eq!(key_id_of(&key_hex(w1)), w1 as u32);
    }
}
