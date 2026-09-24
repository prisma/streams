//! JSON record fidelity: a JSON collection stores each record as the client's
//! own text less the whitespace outside strings, admits and bills it on those
//! stored bytes, identifies requests by client bytes, and serves the stored
//! text unchanged on every read surface.
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_livefeed::hub_sse_collect;
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use serde_json::value::RawValue;

const KEY: [(&str, &str); 1] = [("prisma-encryption-key", PRISMA_KEY)];
const JSON: &[u8] = br#"{"format":{"kind":"json"}}"#;

/// A pretty-printed record whose every byte a re-encoding used to change:
/// a literal the default float parse moved, an integer beyond u64, `-0`, an
/// exponent form, escapes, and a duplicate key.
const CLIENT: &str = "{\n  \"f\" : 1.7802719962921167e-19,\n  \"big\" : 123456789012345678901234567890,\n  \"z\" : -0,\n  \"e\" : 1E+2,\n  \"s\" : \"a \\\"b\\\" \\u00e9\\/ \",\n  \"a\" : 1,\n  \"a\" : 2\n}\n";
const STORED: &str = r#"{"f":1.7802719962921167e-19,"big":123456789012345678901234567890,"z":-0,"e":1E+2,"s":"a \"b\" \u00e9\/ ","a":1,"a":2}"#;

#[derive(serde::Deserialize)]
struct Valued {
    value: Box<RawValue>,
}
#[derive(serde::Deserialize)]
struct Pulled {
    messages: Vec<Valued>,
}

async fn create(addr: std::net::SocketAddr, name: &str) {
    let (status, _, body) = preq(addr, "PUT", &format!("/v1/streams/{name}"), &KEY, JSON).await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
}

async fn append(
    addr: std::net::SocketAddr,
    name: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, Vec<u8>) {
    let path = format!("/v1/streams/{name}/records");
    let (status, _, answer) = preq(addr, "POST", &path, headers, body.as_bytes()).await;
    (status, answer)
}

/// The per-record ceiling, the per-stream ingest capacity and billing all
/// measure the stored text: a record on the ceiling is admitted however much
/// whitespace its client text carries and refused one byte over; literals a
/// re-encoding expanded (`3e23`, `1e15`) are admitted on their own length; a
/// body larger than the ingest capacity only by whitespace is admitted; and
/// the billed ingest bytes are the stored lengths.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_records_are_admitted_measured_and_billed_on_their_stored_text() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let admission = crate::config::AdmissionConfig {
        limit_bytes_per_sec: 4096.0,
        limit_reqs_per_sec: 0.0,
        limit_recs_per_sec: 0.0,
        limit_burst_secs: 1.0,
        ..Default::default()
    };
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            admission: Some(admission),
            ..Default::default()
        },
    )
    .await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    create(addr, "admit").await;
    let mut stored = Vec::new();

    state.admission.set_record_ceiling(STORED.len());
    assert_eq!(
        append(addr, "admit", &KEY, CLIENT).await.0,
        200,
        "on the ceiling"
    );
    stored.push(STORED.to_string());
    let over = CLIENT.replacen("\"a\" : 2\n", "\"a\" : 22\n", 1);
    let (status, answer) = append(addr, "admit", &KEY, &over).await;
    assert_eq!(
        status,
        413,
        "one byte over: {}",
        String::from_utf8_lossy(&answer)
    );

    for literal in ["3e23", "1e15"] {
        let record = format!("[{}]", vec![literal; 200].join(","));
        state.admission.set_record_ceiling(record.len());
        let (status, answer) = append(addr, "admit", &KEY, &record).await;
        assert_eq!(
            status,
            200,
            "{literal}: {}",
            String::from_utf8_lossy(&answer)
        );
        stored.push(record);
    }

    // 4096 B of ingest capacity: a record of 3000 stored bytes padded to
    // 6000 client bytes is admitted, one of 5000 stored bytes is not.
    state.admission.set_record_ceiling(1 << 20);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let padded = |stored: usize, client: usize| {
        let text = format!(r#"{{"pad":"{}"}}"#, "x".repeat(stored - 10));
        (format!("{text}{}", " ".repeat(client - stored)), text)
    };
    let (client, text) = padded(3000, 6000);
    let (status, answer) = append(addr, "admit", &KEY, &client).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&answer));
    stored.push(text);
    let (client, _) = padded(5000, 5000);
    let (status, answer) = append(addr, "admit", &KEY, &client).await;
    assert_eq!(status, 413, "{}", String::from_utf8_lossy(&answer));

    let (status, _, body) = preq(addr, "GET", "/v1/streams/admit/records", &KEY, b"").await;
    assert_eq!(status, 200);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        format!("[{}]", stored.join(","))
    );
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("admit"))
        .await
        .unwrap()
        .unwrap();
    let segment = desc.resolve_segment("");
    let engine = state.engine_for(&segment.shard_route).await.unwrap();
    let meta = engine
        .billing_meta(segment.identity)
        .await
        .expect("meta row");
    let billed: usize = stored.iter().map(String::len).sum();
    assert_eq!(meta.ingest_payload_bytes_total, billed as u64);
    engine_shutdown(&state).await;
}

/// Request identities cover the client's bytes, never a re-serialisation.
/// A byte-identical producer retry is a duplicate; a retry at the same
/// sequence whose JSON is only reformatted is a different request, refused
/// `producer_sequence_reused` by design. A seal's operation covers its
/// final's stored text, so a retry with the same final (re-spaced or not)
/// completes idempotently, and a different final is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_request_identities_cover_the_client_bytes() {
    let (state, addr) = http_rig(mem()).await;
    create(addr, "idem").await;
    let producer = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let client = r#"{ "n" : 1.50 }"#;
    for duplicate in [false, true] {
        let (status, answer) = append(addr, "idem", &producer, client).await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&answer));
        let answer: serde_json::Value = serde_json::from_slice(&answer).unwrap();
        assert_eq!(answer["duplicate"], duplicate);
    }
    let (status, answer) = append(addr, "idem", &producer, r#"{"n":1.50}"#).await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&answer));
    assert!(String::from_utf8_lossy(&answer).contains("producer_sequence_reused"));
    let (_, _, body) = preq(addr, "GET", "/v1/streams/idem/records", &KEY, b"").await;
    assert_eq!(body, br#"[{"n":1.50}]"#);

    create(addr, "idemseal").await;
    for body in [
        r#"{"final":{ "n" : 1.50 }}"#,
        r#"{"final":{ "n" : 1.50 }}"#,
        r#"{ "final" : {"n":1.50} }"#,
    ] {
        let path = "/v1/streams/idemseal:seal";
        let (status, _, answer) = preq(addr, "POST", path, &KEY, body.as_bytes()).await;
        assert_eq!(status, 200, "{body}: {}", String::from_utf8_lossy(&answer));
    }
    let different = br#"{"final":{"n":1.5}}"#;
    let (status, _, answer) =
        preq(addr, "POST", "/v1/streams/idemseal:seal", &KEY, different).await;
    // A different final is a different operation on a sealed collection:
    // refused (AlreadySealed, rendered by `seal_error_response`).
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&answer));
    assert!(String::from_utf8_lossy(&answer).contains("already sealed"));
    let (_, _, body) = preq(addr, "GET", "/v1/streams/idemseal/records", &KEY, b"").await;
    assert_eq!(body, br#"[{"n":1.50}]"#);
    engine_shutdown(&state).await;
}

/// One pretty-printed record, byte-identical to its minified client text on
/// every read surface: a product read, a raw read, a scan, SSE (one event on
/// one `data:` line), the peer segment-scan wire a cross-node read decodes,
/// a consumer Receive's `value`, and the dead-letter copy's `value`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_read_surface_serves_the_minified_client_text() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let wrapped = format!("[{STORED}]");
    for name in ["fid", "fid-dlq"] {
        create(addr, name).await;
    }
    let (status, answer) = append(addr, "fid", &KEY, CLIENT).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&answer));

    let (_, _, body) = preq(addr, "GET", "/v1/streams/fid/records", &KEY, b"").await;
    assert_eq!(String::from_utf8(body).unwrap(), wrapped, "product read");

    let ct = [("content-type", "application/json")];
    assert_eq!(
        hreq(addr, "PUT", "/v1/stream/fidraw", &ct, b"").await.0,
        201
    );
    let (status, _, _) = hreq(addr, "POST", "/v1/stream/fidraw", &ct, CLIENT.as_bytes()).await;
    assert_eq!(status, 204);
    let (_, _, body) = hreq(addr, "GET", "/v1/stream/fidraw", &[], b"").await;
    assert_eq!(String::from_utf8(body).unwrap(), wrapped, "raw read");

    let (status, _, body) = preq(addr, "GET", "/v1/streams/fid:scan", &KEY, b"").await;
    assert_eq!(status, 200);
    let scanned: Vec<Valued> = serde_json::from_slice(&body).unwrap();
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].value.get(), STORED, "scan");

    let mut sse = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET /v1/streams/fid/records:sse HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    tokio::io::AsyncWriteExt::write_all(&mut sse, request.as_bytes())
        .await
        .unwrap();
    let (text, _) = hub_sse_collect(&mut sse, 10, |t| t.contains("\"upToDate\":true")).await;
    assert_eq!(text.matches("event: data\n").count(), 1, "{text}");
    assert!(
        text.contains(&format!("event: data\ndata:{wrapped}\n\n")),
        "SSE: {text}"
    );

    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("fid"))
        .await
        .unwrap()
        .unwrap();
    let target = crate::application::read_remote::InternalTarget::of(&desc, 0).unwrap();
    let mut peer = crate::peer::client()
        .get(format!("http://{addr}/v1/internal/segment-scan/fid"))
        .bearer_auth("dst-internal-token")
        .header("streams-internal-from", "0")
        .header("streams-internal-max-bytes", "65536")
        .header("stream-encryption-key", PRISMA_KEY);
    for (name, value) in target.headers() {
        peer = peer.header(name, value);
    }
    let page: serde_json::Value = peer.send().await.unwrap().json().await.unwrap();
    use base64::Engine;
    let wire = page["items"][0]["p"].as_str().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(wire)
        .unwrap();
    assert_eq!(String::from_utf8(decoded).unwrap(), STORED, "peer wire");

    let config = br#"{"maxAttempts":1,"deadLetterStream":"fid-dlq"}"#;
    let path = "/v1/streams/fid/consumers/work";
    assert_eq!(preq(addr, "PUT", path, &KEY, config).await.0, 201);
    let pull = "/v1/streams/fid/consumers/work:pull";
    let (status, _, body) = preq(addr, "POST", pull, &KEY, br#"{"visibilityMs":1000}"#).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let pulled: Pulled = serde_json::from_slice(&body).unwrap();
    assert_eq!(pulled.messages.len(), 1);
    assert_eq!(pulled.messages[0].value.get(), STORED, "consumer Receive");

    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let (status, _, body) = preq(addr, "POST", pull, &KEY, br#"{"waitMs":200}"#).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let (_, _, body) = preq(addr, "GET", "/v1/streams/fid-dlq/records", &KEY, b"").await;
    let copies: Vec<Valued> = serde_json::from_slice(&body).unwrap();
    assert_eq!(copies.len(), 1, "{}", String::from_utf8_lossy(&body));
    assert_eq!(copies[0].value.get(), STORED, "dead-letter copy");
    engine_shutdown(&state).await;
}
