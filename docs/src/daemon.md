# Daemon

Each machine runs a single long-lived daemon, the only holder of the identity
key: concurrent processes sharing one endpoint identity would race discovery
and connections. A lock file next to the control socket guards the single
instance, and the CLI performs every mesh mutation (pairing, joins, repo
changes) through that socket.

## Connections

The daemon maintains a persistent connection to every peer. Both sides dial,
and duplicate connections are resolved deterministically (the connection
whose dialer has the lower endpoint id survives), so the mesh heals no
matter which machine comes back online last. Every (re)connect replays the
latest mesh state and announcements, which is what makes the fire-and-forget
parts of the [sync protocol](sync.md) safe.

## Routing

Peer connections and repo sync tasks do not know each other: the *hub* sits
between them and implements the latest-wins semantics of the sync protocol
in both directions.

```text
peer task ──route()──► Inbox (per repo) ──drain()──► repo task
peer task ◄──sender─── Outbox (per peer) ◄─publish()─ repo task
```

## Watching repositories

Every mutating jj command atomically swaps marker files in the repo's op
heads directory, so watching that directory gives a reliable per-command
change signal without polling. The daemon compares the head set against the
last one it saw, which absorbs event bursts as well as its own writes during
sync.

## Working copy automation

Beyond replicating history, the daemon keeps working copies fresh:

- **Auto-snapshot** watches the working copy and, once something changed,
  snapshots it on a configurable interval, so edits propagate without
  waiting for a jj command to run.
- **Auto-update-stale** refreshes a workspace that a synced operation left
  stale, sparing the user a manual `jj workspace update-stale`.

Both are configurable, globally and per repo, and run through the user's jj
binary, so they take the working-copy lock like any jj command and respect the
user's jj configuration.
