# Overview

This documentation describes the internal architecture of `jj-mesh`, as well as
the main design decisions and trade-offs. If you wish to contribute or
understand how it works, you're in the right place!

## Bird's-eye view

`jj-mesh` synchronizes [jj](https://docs.jj-vcs.dev/) repositories across
machines. Here are the key parts, each described in its own chapter:

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

`jj-mesh` is meant to be used **with a mesh of personal machines**. Every peer
has full read/write access to every shared repo, and can add or remove other
peers. It is not meant for multi-user collaboration, or adding untrusted peers
to the mesh.

The daemon syncs changes as soon as they are snapshotted by `jj`, and does
not propagate deletions such as `jj op abandon` (see *Consistency* below).
The daemon also snapshots the working copy by default. Per `jj`'s design,
content of snapshots may live indefinitely as long as it is referenced by
the op log. This means **secrets can be replicated across every machine of
the mesh** (and future ones, if still referenceable by the op log) if they
are ever snapshotted.

Connections between peers are made with `iroh` and are **fully end-to-end
encrypted**, and peers are authenticated using their cryptographically secure
Ed25519 key. Adding a machine to the mesh requires a one-time secret generated
by one of the existing peers.

While peers are authenticated, **their data is not trusted**: every message
has a bounded size, and everything a peer sends is validated or hash-verified
before touching disk. A compromised peer can write to shared repos, which is
inherent to the model, but it should not be able to corrupt local storage,
silently rewrite history, or exhaust the daemon's resources.

We rely on public `iroh` relays to perform the initial handshake (needed for
NAT traversal), and in case a direct connection cannot be established, to
forward the traffic. These relays cannot read any of the data transiting, but
they will see peers' IP addresses. See [iroh's Security and
Privacy](https://docs.iroh.computer/concepts/security-privacy).

## Consistency

As `jj-mesh` synchronizes the full history of repositories, it makes its
best effort to ensure no data ever gets corrupted or lost in the process,
and that users can always restore to a good state. This is thankfully made
easy by `jj`'s design.

- **The sync protocol is append-only.** No data ever gets deleted during a
  sync to avoid any corruption. The op log is append-only by design, and `jj`
  is designed to cleanly handle divergent op logs without losing any data.
- **Objects are never re-encoded.** Even if we need to *read* objects from
  both git and jj storage during the sync, all synced data is transferred as
  raw bytes and never re-encoded. We perform checksums to ensure the resulting
  on-disk files are identical and avoid corruption.
- **Updates are atomic.** Especially when touching git storage on colocated
  repos, we follow the same conventions as `jj` itself and perform atomic
  swaps of git refs to never leave the repo in an invalid state.
- **`jj` version is strictly enforced.** As synchronization depends on
  internals of jj's default backend, the daemon enforces that users have a
  compatible jj version and that no custom backend is in use before
  performing any sync.
