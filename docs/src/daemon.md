# Daemon

Each machine runs a single long-lived daemon, the only holder of the identity
key: concurrent processes sharing one endpoint identity would race discovery
and connections. The CLI does everything through the daemon's local control
socket (pairing, joins, status, mesh mutations), and binding the socket
doubles as the single-instance guard.

## Connections

The daemon maintains a persistent connection to every peer. Both sides dial
and duplicate connections are resolved deterministically, so the mesh heals no
matter which machine comes back online last. Every (re)connect replays the
latest mesh state and announcements, which is what makes the fire-and-forget
parts of the [sync protocol](sync.md) safe.

## Watching repositories

Every mutating jj command atomically swaps marker files in the repo's op
heads directory, so watching that directory gives a reliable per-command
change signal without polling. The daemon compares the head set against the
last one it saw, which absorbs event bursts as well as its own writes during
sync.

## Working copy automation

Beyond replicating history, the daemon keeps working copies fresh:

- **Auto-snapshot** watches the working copy and snapshots changes as they
  are saved, so edits propagate without waiting for a jj command to run.
- **Auto-update-stale** refreshes a workspace that a synced operation left
  stale, sparing the user a manual `jj workspace update-stale`.

Both are configurable in the user configuration, globally and per repo, since
the right behavior differs per machine: a VM running a coding agent wants
both, a laptop may not. The daemon never competes with the user for the
working copy: if the working copy lock is held, it backs off and retries
later.

## Failure model

The daemon is fail-fast: any unexpected subsystem exit takes the whole daemon
down, so the service manager restarts it into a known-good state instead of
leaving a zombie that still looks healthy. This is safe because the design is
restart-tolerant end to end: connections re-establish, mesh state and
announcements replay, and interrupted syncs retry.
