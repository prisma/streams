// Build identity for release certification (R28 review): the campaign
// verifier compares git commit + binary sha + boot id against its
// manifest; the commit must come from the build itself, not a wrapper.
use std::process::Command;

fn main() {
    let rev = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=STREAMS_GIT_COMMIT={rev}");
    println!(
        "cargo:rustc-env=STREAMS_BUILD_UNIX={}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    println!("cargo:rerun-if-changed=.git/HEAD");
}
