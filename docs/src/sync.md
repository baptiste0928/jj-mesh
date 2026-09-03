# Sync protocol

All sync traffic runs over one persistent QUIC connection per peer. Messages
are length-prefixed [postcard](https://docs.rs/postcard) encodings, and every
read is bounded by a cap the receiver enforces per message type.

The protocol is pull-based, in two halves: cheap _announcements_ advertise
state, and an explicit _fetch_ transfers it.

```text
announcer                                  fetcher
    │                                         │
    │── announce: op head ids ───────────────►│ (missing heads?)
    │                                         │
    │◄─ fetch: wanted heads + has-sample ─────│
    │── op log delta: views + ops ───────────►│ 1. op phase
    │                                         │
    │◄─ missing commit ids ───────────────────│
    │── git object closure ──────────────────►│ 2. git phase
    │                                         │
    │                                         │ validate, apply, index, publish
```

Pushing data would make the sender responsible for knowing what the receiver
needs and for retrying; with tiny idempotent announcements and
receiver-driven fetches, the receiver controls exactly what enters its repo
(validation lives on one side), and delivery needs no guarantees: the latest
announcement always suffices.

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

1. **Op phase.** The fetcher sends the op heads it _wants_ plus a sample of
   operations it _has_ (its heads and exponentially spaced ancestors, so the
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

Ops and views travel as the _raw bytes_ stored on the server, under their
stored ids. Re-encoding is not an option: jj computes operation and view ids
by hashing its in-memory structures at write time, and objects written by
older jj versions do not survive a decode + re-encode round trip with
identical ids, so replicating decoded objects would silently fork ids across
the mesh. A consequence is that `jj-mesh` pins an exact `jj-lib` version and
only supports the default jj backends, which it verifies before touching a
repo.

## Validation and apply

The fetcher authenticates the peer but trusts nothing it sends. Before
anything becomes part of the repo:

- Every op and view must decode against jj's own schema: a
  stored-but-unreadable object would break every jj command in the repo.
- Every op's parents and view must be in the batch or already stored, and
  every op must be reachable from a wanted head. This blocks a poisoned
  batch from tricking the fetcher into unlisting a local head and silently
  rolling back history.
- Every git object must match its claimed id: loose objects are re-hashed
  one by one, packfiles (used by clones) are hash-verified while indexing.

The apply then writes in an order chosen so that a crash at any point leaves
the repo consistent. Git objects already landed during the fetch (they are
content-addressed and invisible until published), then come:

1. anti-GC keep refs for the new views' head commits;
2. views and ops, parents-first;
3. change-id extras, imported from the new commits;
4. the commit index, built for the incoming heads;
5. the git ref mirror (see below);
6. only at the very end, the op head publication that makes everything
   visible to jj.

Which local heads a new head supersedes is established by walking ancestry
through validated data only; when in doubt the old head stays listed and jj
reconciles the divergence.

## Git refs

jj records the refs of its git repo in the view and, when it imports git,
treats any ref that differs from that record as a move the user made in
git. A sync that leaves the git repo stale is therefore undone at the next
import, and the undo spreads to every peer: in a colocated repo (`.git`
next to `.jj`) at the next jj command, and after a clone's first pull it
deletes every bookmark mesh-wide. A non-colocated repo imports git only
when the user enables colocation, with the same outcome.

The apply thus mirrors the synced refs into the git repo before publishing
(step 5 above). It computes what jj's own view merge gives each ref after
the publication and moves the ref there by compare-and-swap. When the git
repo lives inside `.jj`, nothing but jj writes to it, so the expected value
is what the repo holds and any difference is staleness; the daemon runs the
same repair when it starts watching the repo. For a colocated `.git` or an
external git repo, the expected value is the merged view before the
publication, so refs the user moved directly in git fail their swap and
stay for jj to import. Only refs jj imports are touched, whatever names a
peer's views carry; refs the op heads conflict on are left to jj.

## Cloning a repo

`jj-mesh repo clone` bootstraps a repo onto a new machine: it creates a fresh jj
repo (colocated depending on the user's jj settings, or as `--colocate`), gives
its workspace a machine-unique name , and pulls the repo's full state from a
peer that advertises it. 

## Safety properties

The properties the protocol is designed around, useful as a review checklist:

- Nothing synced becomes visible before it is validated or hash-verified.
- Replicated bytes never overwrite an existing object: for content-addressed
  stores, first write wins.
- No op head is published before the ops and views it exposes are readable.
- A peer cannot cause a local head to be unlisted without supplying a valid
  operation history that supersedes it.
- Interrupted syncs are safe to retry, and the retry is automatic: a timer
  after a failed fetch, the next announcement otherwise.
