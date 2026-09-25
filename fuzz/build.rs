// This crate compiles src/postings.rs by path, whose Kani proof module is
// `#[cfg(kani)] mod proofs;` (verification/README.md). Declare the cfg name,
// as the root crate's build.rs does, so ordinary builds check it instead of
// refusing it as unexpected.
fn main() {
    println!("cargo::rustc-check-cfg=cfg(kani)");
}
