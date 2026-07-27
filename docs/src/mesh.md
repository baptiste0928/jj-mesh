# Mesh

A mesh is a set of *machines* and a set of *repos*. Machines are identified by
an Ed25519 key (their iroh endpoint identity) and carry a human-readable name;
repos are identified by their name. There is no central coordinator: every
machine keeps its own copy of the mesh state, and the copies converge.

## Membership

Any member can add a new machine to the mesh by [pairing](#pairing) with it.
Membership then propagates on its own: peers exchange the machines and repos
they know on every connection, whenever anything changes, and periodically
as anti-entropy, so the whole mesh converges without ever pairing the same
machine twice. A machine that learns something new re-broadcasts it, which
is what carries membership to peers it is not directly exchanging with; a
machine that learns nothing stays silent, which is what stops the echo.

Machines are identified by their key and carry a name for humans, so two
machines may legitimately share a name (commands then take the endpoint id
to disambiguate). Each machine's record is a **versioned register**: every
local change bumps a counter, and a merge keeps the higher version, so both
removals (as tombstones) and re-additions propagate. On equal versions
removal wins, then the smaller name, so every machine settles on the same
record without needing clocks. Removal is best-effort rather than a
security boundary: a machine that is partitioned when a peer is removed,
and re-adds that peer meanwhile, will win with its higher version. Revoking
a machine you no longer trust means rotating what it had access to.

Repos are advertised exactly like machines, and their mesh records are
versioned registers too: registering a repo on one machine makes it visible,
and joinable by its name, everywhere, and forgetting one retires the name
mesh-wide (every machine stops syncing it, none of them touch its files).
The name can then be reused, since the re-registration outranks the
tombstone. Repos also carry a random internal id, used to catch the
pathological case where two machines concurrently create different repos
under the same name: same name but different id is a conflict surfaced to
the user, never silently merged.

Both replicated sets are capped, because a machine has to be able to gossip
its whole view in one message: past the cap new entries stop being adopted.
The caps sit far above what a personal mesh reaches, and the alternative is
a state file that grows until it can no longer be exchanged at all.

## Pairing

Pairing is how a machine enters the mesh, and the only moment a connection
from an unknown endpoint is accepted. A member (`jj-mesh pair`) opens a
pairing window and prints a *ticket*: its address plus a fresh one-time
secret, which the user carries out-of-band (copy/paste) to the joining
machine. The secret proves the joiner holds the ticket; the machine identities
themselves are authenticated by the connection handshake, so the exchange only
carries the human-readable names.

The host persists the new peer *before* confirming it, and the joiner persists
only after seeing that confirmation, so no side ever ends up trusting a peer
that refused it. A failure in between at worst leaves the host paired
one-sidedly, which is fine: pairing is idempotent, so a half-paired mesh
recovers by simply running the pairing again. Once paired, the joiner learns
the rest of the mesh through the normal membership propagation.

## Storage

Each machine stores everything mesh-related in its config directory
(`~/.config/jj-mesh`), split in three files by ownership:

- **The identity key**, in its own file, only ever read by the daemon.
- **`mesh.json`**, this machine's copy of the mesh state, in two parts: what
  is replicated by the gossip (peer records, tombstones included, and the
  mesh-wide repo list) and what is strictly local (the repos registered
  here, with their paths). It is owned and written exclusively by the
  daemon; the CLI mutates it through the daemon's control socket, and users
  are not meant to edit it.
- **The user configuration**, the only hand-edited file, holding local
  settings such as the per-repo watching behavior (see [Daemon](daemon.md)).

## Security

Outside of an open pairing window, connections are refused unless the remote
identity is a known, non-removed peer. And authentication is all a peer gets:
as described in the [overview](overview.md), everything it sends is still
bounded and validated.
