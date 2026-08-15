//! streams-keys: baseline key-lifecycle CLI (README D20).
//!
//! The external key service implements this contract; this tool pins the
//! envelope format and serves development/benchmarking.

use clap::{Parser, Subcommand};

#[path = "../crypto.rs"]
mod crypto;
#[path = "../tenant.rs"]
#[allow(dead_code)] // shared identity module; side bins use only crypto
mod tenant;

#[derive(Parser)]
#[command(name = "streams-keys", about = "Stream key lifecycle + envelope tool")]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a new 32-byte stream key (base64url).
    Generate,
    /// Print the registry fingerprint for a key + stream epoch.
    Fingerprint {
        #[arg(long, env = "STREAM_KEY")]
        key: String,
        /// 32-hex-char stream epoch from the stream descriptor.
        #[arg(long)]
        epoch: String,
    },
    /// Derive the touch capability token (authorizes /touch/* without
    /// granting payload decryption).
    DeriveTouchToken {
        #[arg(long, env = "STREAM_KEY")]
        key: String,
        #[arg(long)]
        epoch: String,
    },
    /// Derive the per-routing-key subkey (what an SDK hands to a reader).
    DeriveSubkey {
        #[arg(long, env = "STREAM_KEY")]
        key: String,
        #[arg(long)]
        epoch: String,
        #[arg(long)]
        routing_key: String,
        #[arg(long, default_value_t = 0)]
        key_version: u32,
    },
    /// Decrypt a hex-encoded wire frame (offline SDK reference).
    DecryptFrame {
        #[arg(long, env = "STREAM_KEY")]
        key: String,
        #[arg(long)]
        epoch: String,
        #[arg(long)]
        stream: String,
        /// Hex frame bytes (as returned by an unkeyed read).
        #[arg(long)]
        frame_hex: String,
    },
}

fn parse_epoch(s: &str) -> anyhow::Result<[u8; 16]> {
    let raw = crypto::unhex(s).ok_or_else(|| anyhow::anyhow!("epoch must be hex"))?;
    raw.try_into()
        .map_err(|_| anyhow::anyhow!("epoch must be 16 bytes (32 hex chars)"))
}

fn main() -> anyhow::Result<()> {
    match Args::parse().cmd {
        Cmd::Generate => {
            let mut key = [0u8; 32];
            getrandom(&mut key)?;
            println!("{}", crypto::StreamKey(key).to_b64());
        }
        Cmd::Fingerprint { key, epoch } => {
            let k = crypto::StreamKey::from_b64(&key).map_err(anyhow::Error::msg)?;
            println!("{}", k.fingerprint(&parse_epoch(&epoch)?));
        }
        Cmd::DeriveTouchToken { key, epoch } => {
            let k = crypto::StreamKey::from_b64(&key).map_err(anyhow::Error::msg)?;
            println!(
                "{}",
                crypto::hex(&crypto::touch_token(&k, &parse_epoch(&epoch)?))
            );
        }
        Cmd::DeriveSubkey {
            key,
            epoch,
            routing_key,
            key_version,
        } => {
            let k = crypto::StreamKey::from_b64(&key).map_err(anyhow::Error::msg)?;
            let sub = crypto::derive_subkey(&k, &parse_epoch(&epoch)?, &routing_key, key_version);
            println!("{}", crypto::hex(&sub));
        }
        Cmd::DecryptFrame {
            key,
            epoch,
            stream,
            frame_hex,
        } => {
            let k = crypto::StreamKey::from_b64(&key).map_err(anyhow::Error::msg)?;
            let epoch = parse_epoch(&epoch)?;
            let raw = crypto::unhex(frame_hex.trim())
                .ok_or_else(|| anyhow::anyhow!("frame must be hex"))?;
            let frame =
                crypto::decode_frame(&raw).ok_or_else(|| anyhow::anyhow!("invalid frame"))?;
            let hash = crypto::stream_hash(&stream);
            let sub = crypto::derive_subkey(
                &k,
                &epoch,
                &frame.header.routing_key,
                frame.header.key_version,
            );
            let pt =
                crypto::decrypt_frame(&sub, &hash, &frame, &raw).map_err(anyhow::Error::msg)?;
            println!(
                "offset={} ts_ms={} routing_key={:?} payload={}",
                frame.header.offset,
                frame.header.ts_ms,
                frame.header.routing_key,
                String::from_utf8_lossy(&pt)
            );
        }
    }
    Ok(())
}

fn getrandom(buf: &mut [u8]) -> anyhow::Result<()> {
    use rand::RngCore;
    rand::rng().fill_bytes(buf);
    Ok(())
}
