# Sync protocol

All sync traffic runs over one persistent QUIC connection per peer. Messages
are length-prefixed [postcard](https://docs.rs/postcard) encodings, and every
read is bounded by the receiver: the length prefix is peer-controlled, so no
message may allocate more than its context allows.

The protocol is pull-based, in two halves: cheap *announcements* advertise
state, and an explicit *fetch* transfers it. Pushing data would make the
sender responsible for knowing what the receiver needs and for retrying; with
tiny idempotent announcements and receiver-driven fetches, the receiver
controls exactly what enters its repo (validation lives on one side), and
delivery needs no guarantees: the latest announcement always suffices.

## Announcements

Whenever a repo's op heads change (and on watch start, and on every peer
(re)connect), the daemon sends each peer an announcement with the repo's
current op head ids. Announcements are fire-and-forget, with latest-wins
semantics: they are coalesced per repo (a slow peer only ever receives the
newest snapshot, never a backlog) and carry a sequence number so the receiver
can discard reordered ones. Lost, dropped or stale announcements therefore
never need recovery: the next change or reconnect heals them.

## Fetch

A machine that sees announced heads it lacks opens a stream to that peer and
pulls them, in two phases:

1. **Op phase.** The fetcher sends the op heads it *wants* plus a sample of
   operations it *has* (its heads and exponentially spaced ancestors, so the
   response stays proportional to the actual delta even after a long
   divergence). The server walks its op log and streams back the delta of
   views and operations.
2. **Git phase.** From the fetched views and ops, the fetcher computes which
   commits they reference that its git store lacks, and requests them. The
   server streams the full object closure, stopping at the fetcher's current
   heads.

Serving is read-only, concurrency-limited, and dispatched independently of the
serving daemon's own sync work, so two machines can fetch from each other
simultaneously without deadlocking.

## Raw bytes

Ops and views travel as the *raw bytes* stored on the server, under their
stored ids. Re-encoding is not an option: jj computes operation and view ids
by hashing its in-memory structures at write time, and objects written by
older jj versions do not survive a decode + re-encode round trip with
identical ids, so replicating decoded objects would silently fork ids across
the mesh. A consequence is that `jj-mesh` pins an exact `jj-lib` version and
only supports the default jj backends, which it verifies before touching a
repo.

## Validation and apply

The fetcher authenticates the peer but trusts nothing it sends. Before any
write:

- Every op and view must decode against jj's own schema: a
  stored-but-unreadable object would break every jj command in the repo.
- Every op's parents and view must be in the batch or already stored, and
  every op must be reachable from a wanted head. This blocks a poisoned
  batch from tricking the fetcher into unlisting a local head and silently
  rolling back history.
- Every git object is hashed and must match its claimed id.

Only then does the apply run, in an order chosen so that a crash at any point
leaves the repo consistent: first anti-GC keep refs for the new heads, then
views and ops (parents-first), and only at the very end the op head
publication that makes anything visible to jj. Which local heads a new head
supersedes is established by walking ancestry through validated data only;
when in doubt the old head stays listed and jj reconciles the divergence.

## Collocated repos

In collocated repos (where `.git` sits next to `.jj`), git tools read the git
refs directly, so after applying synced state the daemon mirrors the new
view's git refs into `.git`. The mirror is deliberately conservative and
follows jj's own exporter semantics: each ref is compare-and-swapped from the
value the previous view knew, refs the user moved directly in git are left
alone, and HEAD is never touched. It only happens when the sync is a clean
fast-forward; under divergence there is no single previous view to reconcile
against, so the mirror is skipped and jj's next import sorts it out.

A mesh repo supports at most one collocated checkout. jj records the
collocated `.git`'s HEAD in the view (`git_head`), which the mesh replicates,
while the HEAD file itself is machine-local state jj pins to the local
working copy's parent. With a second collocated checkout the two machines
permanently disagree on that single field: every synced operation makes jj
re-import the local HEAD as a working-copy move, which resurrects rewritten
commits as divergent changes and ping-pongs `import git head` operations
across the mesh. This is why clones never collocate; only the machine that
originally added the repo gets git interop.

## Cloning a repo

`jj-mesh repo clone` bootstraps a repo onto a new machine: it creates a fresh
non-collocated jj repo (see above), gives its workspace a machine-unique
name (mesh machines must never share one), and pulls the repo's full state
from a peer that advertises it.
The fresh repo's init operations are unrelated to the mesh history, so the
pull is divergent by construction; the next jj command merges the fresh
workspace into the replicated history.

## Safety properties

The properties the protocol is designed around, useful as a review checklist:

- Nothing is written to disk before it is validated or hash-verified.
- Replicated bytes never overwrite an existing object: for content-addressed
  stores, first write wins.
- No op head is published before the ops and views it exposes are readable.
- A peer cannot cause a local head to be unlisted without supplying a valid
  operation history that supersedes it.
- Every peer-controlled quantity is bounded: message sizes, list lengths,
  object sizes, walk budgets, concurrent handshakes and serves.
- Interrupted syncs are safe to retry, and announcements make the retry
  automatic.
