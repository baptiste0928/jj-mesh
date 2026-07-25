# Mesh

A mesh is a set of *machines* and a set of *repos*. Machines are identified by
an Ed25519 key (their iroh endpoint identity) and carry a human-readable name;
repos are identified by their name. There is no central coordinator: every
machine keeps its own copy of the mesh state, and the copies converge.

## Membership

Any member can add a new machine to the mesh by [pairing](#pairing) with it.
Membership then propagates on its own: peers exchange the machines and repos
they know on every connection, and broadcast new ones as they appear, so the
whole mesh converges without ever pairing the same machine twice. Removals
propagate the same way, as tombstones, so a removed peer cannot be re-added by
a machine that missed the news.

Repos are advertised exactly like machines: registering a repo on one machine
makes it visible, and joinable by its name, everywhere. Repos also carry a
random internal id, used to catch the pathological case where two machines
concurrently create different repos under the same name: same name but
different id is a conflict surfaced to the user, never silently merged.

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
- **`peers.json`**, this machine's copy of the converging mesh state: peers,
  tombstones and repos. It is owned and written exclusively by the daemon;
  the CLI mutates it through the daemon's control socket, and users are not
  meant to edit it.
- **The user configuration**, the only hand-edited file, holding local
  settings such as the per-repo watching behavior (see [Daemon](daemon.md)).

## Security

Outside of an open pairing window, connections are refused unless the remote
identity is a known, non-removed peer. And authentication is all a peer gets:
as described in the [overview](overview.md), everything it sends is still
bounded and validated.
