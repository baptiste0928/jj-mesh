//! Embeds the commit sha the binary was built from as `JJ_MESH_COMMIT`.

use std::{env, path::Path, process::Command};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    println!("cargo::rerun-if-env-changed=JJ_MESH_COMMIT");
    for file in ["HEAD", "index"] {
        if Path::new(&manifest_dir).join(".git").join(file).exists() {
            println!("cargo::rerun-if-changed=.git/{file}");
        }
    }

    let commit = env::var("JJ_MESH_COMMIT")
        .ok()
        .filter(|sha| !sha.is_empty())
        .unwrap_or_else(|| git_commit(&manifest_dir));

    println!("cargo::rustc-env=JJ_MESH_COMMIT={commit}");
}

fn git_commit(dir: &str) -> String {
    let commit = match Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .current_dir(dir)
        .output()
        .ok()
    {
        Some(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        _ => return "unknown".to_owned(),
    };

    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(dir)
        .output()
        .is_ok_and(|output| output.status.success() && !output.stdout.is_empty());

    if dirty { format!("{commit}+") } else { commit }
}
