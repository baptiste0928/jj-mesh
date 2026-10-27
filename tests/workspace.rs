//! Tests covering workspaces created while the daemon runs.

mod harness;

use std::process::Command;

use harness::{Machine, TestMesh, wait_until};
use jj_mesh::{config::ConfigDir, daemon::control::WorkspaceHold};
use tracing_subscriber::layer::SubscriberExt as _;

/// A held workspace is only kept fresh once its creator lets go, so the
/// settings it writes meanwhile are the ones the daemon reads.
#[tokio::test]
async fn held_workspace_waits_for_its_creator() {
    let mesh = TestMesh::new();
    let machine = mesh.machine("machine-a").await;
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(machine.logs.layer()));
    let dir = mesh.jj.init_repo("proj");
    assert!(take(machine.config_dir(), "proj", "extra").await.is_err());
    machine.add_repo("proj", &dir).await;

    let hold = take(machine.config_dir(), "proj", "extra").await.unwrap();
    assert!(take(machine.config_dir(), "proj", "extra").await.is_err());
    mesh.jj
        .jj(&dir, &["workspace", "add", "--name", "extra", "../extra"]);
    // An unheld workspace added after it is watched by a scan that also
    // claimed `extra`.
    mesh.jj
        .jj(&dir, &["workspace", "add", "--name", "free", "../free"]);
    wait_until("free watched", || watching(&machine, "free")).await;
    assert!(hold.is_held());
    assert!(!watching(&machine, "extra"));

    drop(hold);
    wait_until("extra watched", || watching(&machine, "extra")).await;
}

/// Cloning a repo already here with a setting writes it to the new
/// workspace's config, and the daemon then watches the workspace.
#[tokio::test]
async fn clone_writes_workspace_settings() {
    let mesh = TestMesh::new();
    let machine = mesh.machine("machine-a").await;
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(machine.logs.layer()));
    let dir = mesh.jj.init_repo("proj");
    machine.add_repo("proj", &dir).await;

    let config = machine.config_dir().path().to_owned();
    let mut clone = Command::new(env!("CARGO_BIN_EXE_jj-mesh"));
    mesh.jj
        .env(&mut clone)
        .current_dir(mesh.jj.path())
        .arg("-C")
        .arg(&config)
        .args(["clone", "proj", "extra", "--snapshot-interval", "5m"]);
    #[expect(clippy::disallowed_methods, reason = "no span to keep in tests")]
    let out = tokio::task::spawn_blocking(move || clone.output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let extra = mesh.jj.path().join("extra");
    let list = |dir, level| {
        mesh.jj
            .jj_output(dir, &["config", "list", level, "jj-mesh"])
    };
    assert_eq!(
        list(&extra, "--workspace"),
        "jj-mesh.snapshot-interval = \"5m\"\n"
    );
    assert_eq!(list(&dir, "--repo"), "");
    wait_until("extra watched", || watching(&machine, "machine-a-extra")).await;
}

/// Takes a hold as the CLI does, off the async runtime.
async fn take(dir: ConfigDir, repo: &str, workspace: &str) -> anyhow::Result<WorkspaceHold> {
    let (repo, workspace) = (repo.to_owned(), workspace.to_owned());
    #[expect(clippy::disallowed_methods, reason = "no span to keep in tests")]
    tokio::task::spawn_blocking(move || WorkspaceHold::take(&dir, &repo, &workspace))
        .await
        .unwrap()
}

/// Whether the daemon started keeping `workspace` fresh.
fn watching(machine: &Machine, workspace: &str) -> bool {
    machine.logs.subscribe().entries.iter().any(|entry| {
        entry.message == "watching workspace"
            && entry.fields.contains(&format!("workspace={workspace} "))
    })
}
