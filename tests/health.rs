//! Tests covering repo health: colocation conflicts pausing sync, store
//! reconfiguration underneath a running daemon, and peer status reports.

mod harness;

use std::fs;

use harness::{TestMesh, add_and_clone, connect, descriptions, wait_converged};
use jj_mesh::net::sync::RepoHealthState;

/// Converting a second instance of a mesh repo to colocated pauses sync on
/// both machines (instead of ping-ponging git HEAD imports), and reverting
/// the conversion resumes it.
#[tokio::test(flavor = "multi_thread")]
async fn second_colocated_instance_pauses_sync() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    connect(&a, &b).await;

    // A's instance is colocated (jj's default init layout); B clones with
    // the supported non-colocated layout.
    let dir_a = mesh.jj.init_repo("proj");
    assert!(dir_a.join(".git").is_dir());
    let dir_b = add_and_clone(&mesh, &a, &b, &dir_a, "proj").await;
    assert!(!dir_b.join(".git").exists());

    // Convert B's instance to colocated, the way such conversions happen
    // on disk: the git store moves to `.git` and git_target repoints.
    fs::rename(dir_b.join(".jj/repo/store/git"), dir_b.join(".git")).unwrap();
    fs::write(dir_b.join(".jj/repo/store/git_target"), "../../../.git").unwrap();

    // A change on A wakes B's repo task, which must notice the store
    // reconfiguration, reopen, and pause on the colocation conflict
    // instead of syncing.
    fs::write(dir_a.join("a.txt"), "a\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "while converting"]);
    b.wait("proj paused on B", |s| {
        s.paused.iter().any(|p| p.repo == "proj")
    })
    .await;
    // B's colocated announcement pauses A symmetrically.
    a.wait("proj paused on A", |s| {
        s.paused.iter().any(|p| p.repo == "proj")
    })
    .await;
    assert!(!descriptions(&mesh, &dir_b).contains("while converting"));

    // B's self-reported health reaches A.
    a.wait("B reports the pause", |s| {
        s.peer_reports.iter().any(|r| {
            r.report
                .repos
                .iter()
                .any(|repo| repo.name == "proj" && repo.state == RepoHealthState::Paused)
        })
    })
    .await;

    // Reverting the conversion resumes sync: B announces non-colocated,
    // both sides unpause, and changes flow again.
    fs::write(dir_b.join(".jj/repo/store/git_target"), "git").unwrap();
    fs::rename(dir_b.join(".git"), dir_b.join(".jj/repo/store/git")).unwrap();
    mesh.jj.jj(&dir_b, &["new", "-m", "resume"]);
    a.wait("proj resumed on A", |s| s.paused.is_empty()).await;
    b.wait("proj resumed on B", |s| s.paused.is_empty()).await;

    fs::write(dir_a.join("a2.txt"), "a2\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "after resume"]);
    wait_converged(&dir_a, &dir_b).await;
    assert!(descriptions(&mesh, &dir_b).contains("while converting"));
    assert!(descriptions(&mesh, &dir_b).contains("after resume"));
}

/// Peers report their health to each other, and a healthy mesh shows every
/// repo `Ok` with the daemon and jj versions attached.
#[tokio::test(flavor = "multi_thread")]
async fn peers_report_their_health() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    connect(&a, &b).await;

    let dir_a = mesh.jj.init_repo("proj");
    add_and_clone(&mesh, &a, &b, &dir_a, "proj").await;

    a.wait("B's health report", |s| {
        s.peer_reports.iter().any(|r| {
            r.report.daemon_version == env!("CARGO_PKG_VERSION")
                && r.report
                    .repos
                    .iter()
                    .any(|repo| repo.name == "proj" && repo.state == RepoHealthState::Ok)
        })
    })
    .await;
}
