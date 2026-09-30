//! Tests covering the daemon event stream behind `jj-mesh logs`.

mod harness;

use harness::{Machine, TestMesh, WAIT_TIMEOUT};
use jj_mesh::daemon::control::{ControlClient, LogEntry, Request, Response};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::SubscriberExt as _;

/// Records `machine`'s events while the guard lives. Only events logged on
/// the test's runtime thread are seen, not those of blocking tasks.
fn record(machine: &Machine) -> DefaultGuard {
    tracing::subscriber::set_default(tracing_subscriber::registry().with(machine.logs.layer()))
}

/// Opens a logs stream, returning the client and the backlog.
async fn open(machine: &Machine, follow: bool) -> (ControlClient, Vec<LogEntry>) {
    let mut client = machine.client().await;
    client.send(&Request::Logs { follow }).await.unwrap();
    let Response::LogsStart(start) = client.recv(Some(WAIT_TIMEOUT)).await.unwrap() else {
        panic!("expected the stream header");
    };
    assert_eq!(start.dropped, 0);
    let mut backlog = Vec::new();
    for _ in 0..start.backlog {
        backlog.push(next_entry(&mut client).await);
    }
    (client, backlog)
}

/// Receives the next event of a logs stream.
async fn next_entry(client: &mut ControlClient) -> LogEntry {
    match client.recv(Some(WAIT_TIMEOUT)).await.unwrap() {
        Response::Log(entry) => entry,
        other => panic!("unexpected response {other:?}"),
    }
}

fn renamed(entry: &LogEntry, name: &str) -> bool {
    entry.message == "machine renamed" && entry.fields == format!("name={name}")
}

/// Without follow, the stream ends after the backlog.
#[tokio::test]
async fn logs_end_after_backlog() {
    let mesh = TestMesh::new();
    let machine = mesh.machine("machine-a").await;
    let _guard = record(&machine);
    machine.rename("first").await;

    let (mut client, backlog) = open(&machine, false).await;
    assert!(backlog.iter().any(|e| renamed(e, "first")), "{backlog:?}");
    assert!(client.recv(Some(WAIT_TIMEOUT)).await.is_err());
}

/// A follower gets the buffered events, then new ones as they happen.
#[tokio::test]
async fn follow_streams_new_events() {
    let mesh = TestMesh::new();
    let machine = mesh.machine("machine-a").await;
    let _guard = record(&machine);
    machine.rename("first").await;

    let (mut client, backlog) = open(&machine, true).await;
    assert!(backlog.iter().any(|e| renamed(e, "first")), "{backlog:?}");

    machine.rename("second").await;
    loop {
        let entry = next_entry(&mut client).await;
        if entry.message == "machine renamed" {
            assert!(renamed(&entry, "second"), "{entry:?}");
            break;
        }
    }
}
