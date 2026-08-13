// Build identity for release certification (R28 review): the campaign
// verifier compares git commit + binary sha + boot id against its
// manifest; the commit must come from the build itself, not a wrapper.
use std::process::Command;

fn main() {
    let rev = std::env::var("STREAMS_GIT_COMMIT").ok().filter(|v| !v.is_empty());
    let rev = rev.unwrap_or_else(|| {
        Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
    });
    println!("cargo:rustc-env=STREAMS_GIT_COMMIT={rev}");
    println!(
        "cargo:rustc-env=STREAMS_BUILD_UNIX={}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    // R29: `.git/HEAD` alone goes stale — on a branch it keeps saying
    // `ref: refs/heads/<branch>` while commits advance, so cargo never
    // reruns this script and the embedded commit lags. Watch the
    // resolved ref file and packed-refs too, and let a release builder
    // override via env.
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Ok(head) = std::fs::read_to_string(".git/HEAD") {
        if let Some(r) = head.strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=.git/{}", r.trim());
        }
    }
    println!("cargo:rerun-if-changed=.git/packed-refs");
    println!("cargo:rerun-if-env-changed=STREAMS_GIT_COMMIT");
}
