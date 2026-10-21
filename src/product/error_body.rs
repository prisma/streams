//! The product error body (`perr`, spec Stage 8 §11) written straight into
//! one string. Every refusal answers with it, so a refusal flood no longer
//! builds two JSON maps and their keys per answer; the bytes are the ones
//! `serde_json` writes for the same object (its keys sorted:
//! `code`, `details`, `message`, `retryable`).

/// `{"error":{"code":…,"details":…,"message":…,"retryable":…}}`, without
/// `details` when there are none.
pub(super) fn error_body(
    code: &str,
    message: &str,
    details: Option<serde_json::Value>,
    retryable: bool,
) -> String {
    let mut body = String::with_capacity(64 + code.len() + message.len());
    body.push_str("{\"error\":{\"code\":");
    push_json_string(&mut body, code);
    if let Some(details) = details {
        body.push_str(",\"details\":");
        body.push_str(&details.to_string());
    }
    body.push_str(",\"message\":");
    push_json_string(&mut body, message);
    body.push_str(if retryable {
        ",\"retryable\":true}}"
    } else {
        ",\"retryable\":false}}"
    });
    body
}

/// `text` as a JSON string, escaped as `serde_json` escapes it: `"` and
/// `\`, the five short control escapes, every other control character as
/// `\u00xx`; everything else as it is.
fn push_json_string(body: &mut String, text: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    body.push('"');
    for c in text.chars() {
        match c {
            '"' => body.push_str("\\\""),
            '\\' => body.push_str("\\\\"),
            '\n' => body.push_str("\\n"),
            '\r' => body.push_str("\\r"),
            '\t' => body.push_str("\\t"),
            '\u{8}' => body.push_str("\\b"),
            '\u{c}' => body.push_str("\\f"),
            c if c < ' ' => {
                let byte = c as usize;
                body.push_str("\\u00");
                body.push(char::from(HEX[byte >> 4]));
                body.push(char::from(HEX[byte & 0xf]));
            }
            c => body.push(c),
        }
    }
    body.push('"');
}

#[cfg(test)]
mod tests {
    use super::error_body;
    use serde_json::{Map, Value};

    /// The body as `perr` built it before (`json!` maps, serialized), from
    /// the same maps built by hand.
    fn reference(code: &str, message: &str, details: Option<Value>, retryable: bool) -> String {
        let mut e = Map::new();
        e.insert("code".into(), code.into());
        e.insert("message".into(), message.into());
        e.insert("retryable".into(), retryable.into());
        if let Some(d) = details {
            e.insert("details".into(), d);
        }
        let mut body = Map::new();
        body.insert("error".into(), Value::Object(e));
        Value::Object(body).to_string()
    }

    /// Every character below 0x80, a few beyond it, the product's own
    /// codes and messages, with and without details, either retryability:
    /// byte for byte the reference's body.
    #[test]
    fn the_body_is_the_reference_body_byte_for_byte() {
        let every_ascii: String = (0u8..0x80).map(char::from).collect();
        let texts = [
            "project_rate_limit",
            "the project's rate quota is exhausted; retry",
            "",
            every_ascii.as_str(),
            "naïve – ✓ 𝄞 \u{2028}\u{2029}\u{7f}\u{80}\u{9f}",
            "quote \" backslash \\ slash / tab \t nul \0",
        ];
        let parse = |text: &str| serde_json::from_str::<Value>(text).ok();
        let details = [
            None,
            parse(r#"{"dimension": "stored_bytes", "limit": 10, "stored": 11}"#),
            parse(r#"["a", {"z": 1, "a": "\n"}]"#),
            parse("null"),
        ];
        assert!(details[1..].iter().all(Option::is_some));
        let cases = texts
            .iter()
            .flat_map(|code| texts.iter().map(move |message| (code, message)));
        for (code, message) in cases {
            for (detail, retryable) in details.iter().flat_map(|d| [(d, false), (d, true)]) {
                assert_eq!(
                    error_body(code, message, detail.clone(), retryable),
                    reference(code, message, detail.clone(), retryable),
                    "{code:?} {message:?} {detail:?} {retryable}"
                );
            }
        }
    }
}
