// Build identity for release certification (R28 review): the campaign
// verifier compares git commit + binary sha + boot id against its
// manifest; the commit must come from the build itself, not a wrapper.
use std::path::Path;
use std::process::Command;

#[expect(
    clippy::disallowed_methods,
    reason = "build identity owner; the release builder supplies commit and reproducible time through two explicit environment inputs; reading these at build time avoids runtime provenance overrides"
)]
fn main() {
    let rev = std::env::var("STREAMS_GIT_COMMIT")
        .ok()
        .filter(|v| !v.is_empty());
    let rev = rev
        .unwrap_or_else(|| git_output(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into()));
    println!("cargo:rustc-env=STREAMS_GIT_COMMIT={rev}");
    // SOURCE_DATE_EPOCH (reproducible-builds convention): the release
    // builder injects one timestamp and records the SAME value in the
    // campaign manifest, so verify-running can require exact equality
    // with the binary's /v1/debug/load build_unix. Without the
    // override, manifest (upload clock) and binary (compile clock)
    // could never match (R30: first rc.1 verify failed exactly here).
    let ts = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        });
    println!("cargo:rustc-env=STREAMS_BUILD_UNIX={ts}");
    // Resolve per-worktree HEAD and shared refs through Git. Watching absent
    // .git/HEAD or packed-refs paths makes Cargo rerun this script forever.
    println!("cargo:rerun-if-changed=build.rs");
    watch_git_identity();
    println!("cargo:rerun-if-env-changed=STREAMS_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn watch_git_identity() {
    let mut visited = std::collections::HashSet::new();
    let mut reference = String::from("HEAD");
    while visited.insert(reference.clone()) {
        let Some(path) = git_output(&["rev-parse", "--git-path", &reference]) else {
            return; // Source archives use the explicit release overrides.
        };
        // Include every symbolic link, not just its final target. A packed
        // branch has no loose file yet; its existing parent catches creation.
        watch_existing(Path::new(&path));
        let Some(next) = git_output(&["symbolic-ref", "--quiet", "--no-recurse", &reference])
        else {
            break;
        };
        reference = next;
    }
    if let Some(path) = git_output(&["rev-parse", "--git-path", "packed-refs"])
        && Path::new(&path).is_file()
    {
        println!("cargo:rerun-if-changed={path}");
    }
}

fn watch_existing(path: &Path) {
    if let Some(existing) = path.ancestors().find(|ancestor| ancestor.exists()) {
        println!("cargo:rerun-if-changed={}", existing.display());
    }
}
