//! Wake-up notifications (docs/QUEUES-DESIGN.md §9.2): the signing secret
//! is derived, never stored (`HMAC(root, label ‖ queue ‖ version)`), and a
//! notification is signed `v1=<hex HMAC-SHA256(secret, sentAt "." body)>`.
//! The egress policy admits only https on 443 to public addresses, resolved
//! once and pinned for the request, with no redirects.
use hmac::{Hmac, Mac};
use sha2::Sha256;

const LABEL: &[u8] = b"queue-wakeup/v1";

/// The process's wake-up secret root, installed once by the HTTP layer
/// from its server-held key; notifications are off until it is.
static ROOT: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();

pub(crate) fn install_root(root: [u8; 32]) {
    ROOT.get_or_init(|| root);
}

pub(crate) fn root() -> Option<[u8; 32]> {
    ROOT.get().copied()
}

pub(crate) fn wakeup_secret(root: &[u8; 32], hash: &[u8; 16], version: u32) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(root)
        .unwrap_or_else(|_| unreachable!("HMAC takes any key length"));
    mac.update(LABEL);
    mac.update(hash);
    mac.update(&version.to_be_bytes());
    mac.finalize().into_bytes().into()
}

pub(crate) fn sign(secret: &[u8; 32], sent_at: i64, body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret)
        .unwrap_or_else(|_| unreachable!("HMAC takes any key length"));
    mac.update(sent_at.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("v1={}", crate::crypto::hex(&mac.finalize().into_bytes()))
}

/// Whether `ip` is a public unicast address the egress policy allows.
pub(crate) fn public_address(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xC0) == 64)
                || (o[0] == 169 && o[1] == 254)
                || o[0] >= 240)
        }
        std::net::IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return public_address(std::net::IpAddr::V4(v4));
            }
            let s = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00
                || (s[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// The notification's body.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Notification {
    pub queue: String,
    pub group: &'static str,
    pub due: u64,
    pub oldest_due_age_ms: i64,
    pub nonce: String,
    pub sent_at: i64,
}

/// POST a notification under the egress policy; true when the receiver
/// answered 2xx within the timeout.
pub(crate) async fn deliver(
    url: &str,
    secret: &[u8; 32],
    note: &Notification,
) -> Result<bool, String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
    if parsed.scheme() != "https" || parsed.port_or_known_default() != Some(443) {
        return Err("wake-up URLs must be https on port 443".into());
    }
    let host = parsed
        .host_str()
        .ok_or("wake-up URL has no host")?
        .to_owned();
    let addr = tokio::net::lookup_host((host.as_str(), 443))
        .await
        .map_err(|e| e.to_string())?
        .find(|a| public_address(a.ip()))
        .ok_or("wake-up host resolves to no public address")?;
    let client = reqwest::Client::builder()
        .resolve(&host, addr)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|e| e.to_string())?;
    let body = serde_json::to_vec(note).map_err(|e| e.to_string())?;
    let resp = client
        .post(parsed)
        .header("content-type", "application/json")
        .header("streams-signature", sign(secret, note.sent_at, &body))
        .body(body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp.status().is_success())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_differ_by_queue_and_version_and_signatures_bind_the_time() {
        let root = [1u8; 32];
        let a = wakeup_secret(&root, &[2; 16], 1);
        assert_ne!(a, wakeup_secret(&root, &[3; 16], 1));
        assert_ne!(a, wakeup_secret(&root, &[2; 16], 2));
        assert_ne!(sign(&a, 1, b"x"), sign(&a, 2, b"x"));
        assert!(sign(&a, 1, b"x").starts_with("v1="));
    }

    #[test]
    fn the_egress_policy_refuses_private_and_special_addresses() {
        for bad in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(
                !public_address(bad.parse().unwrap()),
                "{bad} must be refused"
            );
        }
        for good in ["1.1.1.1", "2606:4700:4700::1111"] {
            assert!(public_address(good.parse().unwrap()), "{good} is public");
        }
    }
}
