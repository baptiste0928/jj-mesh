# Overview

This documentation describes the internal architecture of `jj-mesh`, its main
design decisions and their trade-offs, for developers who want to understand
or contribute to it.

## Bird's-eye view

`jj-mesh` synchronizes [jj](https://docs.jj-vcs.dev/) repositories across
machines:

```text
┌─────┐ control  ┌────────┐    iroh (QUIC)    ┌──────────────┐
│ CLI │──socket─►│ daemon │◄─────────────────►│ peer daemons │
└─────┘          └───┬────┘                   └──────────────┘
              watch  │  sync
                     ▼
              jj repositories
```

Here are the key parts:

- **[The mesh](mesh.md)** is the set of machines on which the repos are
  synced. They are connected peer-to-peer using
  [iroh](https://www.iroh.computer/), without a central server ever holding
  the data. A machine enters the mesh by pairing with any existing member, and
  membership propagates to everyone from there.
- **[The sync protocol](sync.md)** syncs the *git objects* and the
  [*operation log*](https://docs.jj-vcs.dev/latest/operation-log/) of the
  repositories. It's a pull-based protocol: changes are announced over the
  mesh, then interested machines ask for the data.
- **[The daemon](daemon.md)** keeps the connections active in the background
  and watches the repositories to sync as soon as changes happen. It is
  controlled by the `jj-mesh` CLI.

> `jj-mesh` is aimed **to be used by a single user**, across machines operated
> by this user. There are no permissions or other features for multi-user
> collaboration, and the entire security model (see below) assumes so.

## Security model

The [README](https://github.com/baptiste0928/jj-mesh#security-and-privacy)
describes the security model: every peer has full read/write access to every
shared repo and connections are end-to-end encrypted and authenticated with
per-machine Ed25519 keys.

## Consistency

As `jj-mesh` synchronizes the full history of repositories, it is designed
so no data gets corrupted or lost in the process, and users can always
restore a good state. `jj`'s design makes this easy.

- **The sync protocol is append-only.** No op or object ever gets deleted
  during a sync to avoid any corruption. The op log is append-only by design,
  and `jj` is designed to cleanly handle divergent op logs without losing any
  data. Only git refs move, to follow the synced state.
- **Objects are never re-encoded.** Even if we need to *read* objects from
  both git and jj storage during the sync, all synced data is transferred as
  raw bytes and never re-encoded. We perform checksums to ensure the resulting
  on-disk files are identical and avoid corruption.
- **Updates are atomic.** Especially when touching git storage, we follow the
  same conventions as `jj` itself and perform atomic swaps of git refs to
  never leave the repo in an invalid state.
- **`jj` version is strictly enforced.** As synchronization depends on
  internals of jj's default backend, the daemon enforces that users have a
  compatible jj version and that no custom backend is in use before
  performing any sync.
