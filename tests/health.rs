//! Tests covering repo health: peer status reports.

mod harness;

use harness::{TestMesh, add_and_clone};
use jj_mesh::daemon::control::RepoHealthState;

/// Peers report their health to each other, and a healthy mesh shows every
/// repo `Ok` with the daemon and jj versions attached.
#[tokio::test]
async fn peers_report_their_health() {
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;
    add_and_clone(&mesh, &a, &b, "proj").await;

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
