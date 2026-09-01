# Mesh

A mesh is a set of *machines* and a set of *repos*. Machines are identified by
an Ed25519 key (their iroh endpoint identity) and carry a human-readable name;
repos are identified by their name. There is no central coordinator: every
machine keeps its own copy of the mesh state, and the copies converge.

## Membership

Any member can add a new machine to the mesh by [pairing](#pairing) with it.
Membership then propagates on its own: peers exchange the machines and repos
they know

- on every (re)connection,
- whenever anything changes locally,
- and periodically, as anti-entropy.

A machine that learns something new re-broadcasts it, carrying membership to
peers it does not exchange with directly; a machine that learns nothing stays
silent, which stops the echo. The whole mesh thus converges without ever
pairing the same machine twice.

Machines are identified by their key and carry a name for humans, so two
machines may legitimately share a name (commands then take the endpoint id
to disambiguate). Each machine's record is a **versioned register**, merged
with the same rule everywhere so every machine settles on the same record
without needing clocks:

- every local change bumps a version counter, and the higher version wins;
- on equal versions, removal (kept as a tombstone) wins over presence;
- on a full tie, the smaller name wins.

A machine's own record (its name, the hostname by default) is stored on the
machine and gossiped with the rest.

Repos are advertised like machines, and their mesh records are versioned
registers too (tie-breaking on the smaller *id* instead of the name):
registering a repo on one machine makes it visible everywhere, and clonable
from any online machine that holds it. Removing one retires the name
mesh-wide (every machine stops syncing it, none of them touch its files).
The name can then be reused, since the re-registration outranks the
tombstone. Repos also carry a random internal id, used to catch the case where
two machines concurrently create different repos under the same name.

Both replicated sets are capped so a machine can always gossip its whole
view in one message: past the cap, new entries stop being adopted. The caps
sit far above what a personal mesh reaches.

## Pairing

Pairing is how a machine enters the mesh, and the only moment a connection
from an unknown endpoint is accepted:

1. A member (`jj-mesh peer add`) opens a pairing window and prints a
   *ticket*: its address plus a fresh one-time secret.
2. The user carries the ticket out-of-band (copy/paste) to the joining
   machine.
3. The joiner connects and presents the secret, proving it holds the
   ticket. The machine identities themselves are authenticated by the
   connection handshake, so the exchange only carries the human-readable
   names.
4. The host persists the new peer, then confirms it.
5. The joiner persists the host only after seeing that confirmation.

Persisting in this order ensures no side ever ends up trusting a peer that
refused it. Once paired, the joiner learns the rest of the mesh through the
normal membership propagation.

## Storage

Each machine stores everything mesh-related in its config directory
(`~/.config/jj-mesh`), split in three files by ownership:

- **The identity key**, in its own file, only ever read by the daemon.
- **`mesh.json`**, this machine's copy of the mesh state, in two parts: what
  is replicated by the gossip (this machine's own record, the peer records,
  tombstones included, and the mesh-wide repo list) and what is strictly
  local (the repos registered here, with their paths). Only the daemon writes it: the CLI mutates it
  through the daemon's control socket, and reads it directly only for
  pre-checks and completion, treating what it sees as advisory.
- **The user configuration**, the only hand-edited file, holding local
  settings such as the working copy automation (see [Daemon](daemon.md)).
  The daemon reads it once at start: edits apply on the next restart.

## Security

Outside of an open pairing window, connections are refused unless the remote
identity is a known, non-removed peer. And authentication is all a peer gets:
as described in the [overview](overview.md), everything it sends is still
bounded and validated.
