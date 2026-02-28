use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // In workspace: README is at ../README.md. In packaged crate: Cargo puts it at README.md.
    let in_crate = manifest_dir.join("README.md");
    let in_workspace = manifest_dir.join("..").join("README.md");
    let readme_src = if in_crate.exists() {
        in_crate
    } else if in_workspace.exists() {
        in_workspace
    } else {
        panic!("README.md not found in crate root or workspace root (../README.md)");
    };

    std::fs::copy(&readme_src, out_dir.join("README.md")).expect("copy README to OUT_DIR");
    println!("cargo:rerun-if-changed=../README.md");
    println!("cargo:rerun-if-changed=README.md");
}
