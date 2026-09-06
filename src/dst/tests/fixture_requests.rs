//! Fixture requests.

pub(super) const RIG_KEY_B64: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="; // skey() = [7u8; 32]

/// Minimal HTTP/1.1 client: returns (status, lowercased headers, body).
pub(super) async fn hreq(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    // The rig key is supplied by default, but a caller that passes its
    // OWN stream-encryption-key must not end up sending two — the
    // server reads the first, so the injected one would silently win
    // and a "wrong key" test would quietly exercise the right key.
    let mut req = format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    if !extra
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("stream-encryption-key"))
    {
        req.push_str(&format!("stream-encryption-key: {RIG_KEY_B64}\r\n"));
    }
    req.push_str(&format!("content-length: {}\r\n", body.len()));
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    // A peer RESET after a complete response is normal on macOS when
    // the server closes while the client still has unread request bytes
    // queued; treat it as end-of-response and let the parse below judge
    // completeness (a truly truncated read fails at the header
    // terminator).
    if let Err(e) = s.read_to_end(&mut buf).await
        && (e.kind() != std::io::ErrorKind::ConnectionReset || buf.is_empty())
    {
        panic!("response read: {e}");
    }
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator");
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = std::collections::HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let mut raw_body = buf[split + 4..].to_vec();
    if headers.get("transfer-encoding").map(|v| v == "chunked") == Some(true) {
        // Connection: close + chunked — decode.
        let mut out = Vec::new();
        let mut rest: &[u8] = &raw_body;
        while let Some(le) = rest.windows(2).position(|w| w == b"\r\n") {
            let n =
                usize::from_str_radix(std::str::from_utf8(&rest[..le]).unwrap_or("0").trim(), 16)
                    .unwrap_or(0);
            if n == 0 {
                break;
            }
            let start = le + 2;
            out.extend_from_slice(&rest[start..start + n]);
            rest = &rest[start + n + 2..];
        }
        raw_body = out;
    }
    (status, headers, raw_body)
}

/// One page of a keyed/keyless read. Returns (status, headers, records).
pub(super) async fn read_page(
    addr: std::net::SocketAddr,
    stream: &str,
    key: Option<&str>,
    tok: Option<&str>,
) -> (
    u16,
    std::collections::HashMap<String, String>,
    Vec<serde_json::Value>,
) {
    // Keyed pages go through the PRODUCT route (the singular route is
    // the default-key view only); the product route's cursor parameter
    // accepts the same opaque tokens the lineage reader emits, so these
    // tests still drive the lineage machinery directly.
    let (st, h, b) = match key {
        Some(k) => {
            let mut path = format!("/v1/streams/{stream}/records?routingKey={k}");
            if let Some(t) = tok {
                path.push_str(&format!("&cursor={t}"));
            }
            preq(
                addr,
                "GET",
                &path,
                &[("prisma-encryption-key", PRISMA_KEY)],
                b"",
            )
            .await
        }
        None => {
            let mut path = format!("/v1/stream/{stream}?x=1");
            if let Some(t) = tok {
                path.push_str(&format!("&offset={t}"));
            }
            hreq(addr, "GET", &path, &[], b"").await
        }
    };
    let recs = if b.is_empty() {
        Vec::new()
    } else {
        serde_json::from_slice::<Vec<serde_json::Value>>(&b).unwrap_or_default()
    };
    (st, h, recs)
}

/// Drain a key fully, asserting NO page ever reports closure. Returns
/// (records, final headers).
pub(super) async fn drain_no_closure(
    addr: std::net::SocketAddr,
    stream: &str,
    key: Option<&str>,
) -> (
    Vec<serde_json::Value>,
    std::collections::HashMap<String, String>,
) {
    let mut tok: Option<String> = None;
    let mut out = Vec::new();
    for _ in 0..64 {
        let (st, h, recs) = read_page(addr, stream, key, tok.as_deref()).await;
        assert!(st == 200 || st == 204, "page status {st}");
        assert!(
            !h.contains_key("stream-closed") && !h.contains_key("prisma-sealed"),
            "a transition must never report closure (headers: {h:?})"
        );
        out.extend(recs);
        // Keyed pages come from the product surface (Prisma-*), keyless
        // ones from the raw surface (Stream-*).
        let nxt = h
            .get("prisma-next-cursor")
            .or_else(|| h.get("stream-next-offset"))
            .cloned();
        let utd = h
            .get("prisma-up-to-date")
            .or_else(|| h.get("stream-up-to-date"))
            .map(|v| v == "true")
            == Some(true);
        if utd || nxt.is_none() || nxt == tok {
            return (out, h);
        }
        tok = nxt;
    }
    panic!("drain did not settle");
}

// ---- product-surface foundation (spec Stages 7/8 core + clean switch) --

pub(super) const PRISMA_KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";

/// The consumer's incarnation token from GET (round 17: DELETE names
/// an incarnation, never a name). Callers on a Deleting/Deleted record
/// must capture the token BEFORE deletion begins.
pub(super) async fn consumer_version(
    addr: std::net::SocketAddr,
    stream: &str,
    cname: &str,
) -> String {
    let (st, h, _) = preq(
        addr,
        "GET",
        &format!("/v1/streams/{stream}/consumers/{cname}"),
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"",
    )
    .await;
    assert_eq!(st, 200, "consumer_version: GET {stream}/{cname} -> {st}");
    h.get("prisma-consumer-version")
        .expect("prisma-consumer-version header")
        .clone()
}

pub(super) async fn preq(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    s.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    // A peer RESET after a complete response is normal on macOS when
    // the server closes while the client still has unread request bytes
    // queued; treat it as end-of-response and let the parse below judge
    // completeness (a truly truncated read fails at the header
    // terminator).
    if let Err(e) = s.read_to_end(&mut buf).await
        && (e.kind() != std::io::ErrorKind::ConnectionReset || buf.is_empty())
    {
        panic!("response read: {e}");
    }
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator");
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = std::collections::HashMap::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let mut raw_body = buf[split + 4..].to_vec();
    if headers.get("transfer-encoding").map(|v| v == "chunked") == Some(true) {
        let mut out = Vec::new();
        let mut rest: &[u8] = &raw_body;
        while let Some(le) = rest.windows(2).position(|w| w == b"\r\n") {
            let n =
                usize::from_str_radix(std::str::from_utf8(&rest[..le]).unwrap_or("0").trim(), 16)
                    .unwrap_or(0);
            if n == 0 {
                break;
            }
            let start = le + 2;
            out.extend_from_slice(&rest[start..start + n]);
            rest = &rest[start + n + 2..];
        }
        raw_body = out;
    }
    (status, headers, raw_body)
}
