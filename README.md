# jj-mesh

`jj-mesh` is a peer-to-peer synchronization daemon for [`jj`](https://docs.jj-vcs.dev/)
repositories. **It lets [workspaces](https://docs.jj-vcs.dev/latest/working-copy/#workspaces) on
different machines share the same repo**: each has its own working copy, while changes, bookmarks
and the [operation log](https://docs.jj-vcs.dev/latest/operation-log/) are replicated within
seconds, without pushing to a remote.

It is made to sync across your personal machines, for example to:
- Run coding agents on a server and inspect their changes locally.
- Keep work-in-progress changes in sync between your desktop and laptop.

The daemon runs in the background on each machine:
- **Edits are synced without running `jj`**: working copies are watched and snapshotted regularly.
- **Workspaces can be edited from another machine**: when a working-copy commit is rewritten
  remotely, the stale working copy is updated automatically.
- **Concurrent operations are safe**: the operation log is replicated, not only commits and
  bookmarks, so `jj` merges operations made on different machines and everything stays
  recoverable.
- **No server required**: machines connect directly using [iroh](https://www.iroh.computer/),
  including behind NAT.

![Screenshot of `jj-mesh status`](./.github/screenshot.jpeg)

## Installation

`jj-mesh` is tested on Linux and macOS, and requires **`jj` 0.46 or 0.47** to be installed. Windows
is unsupported. The daemon runs in the background as a user service, which `jj-mesh setup` installs
for you (see [Usage](#usage)).

You can watch this repository on GitHub to be notified of new releases.

### Pre-built binaries

Binaries for Linux (x86_64, aarch64) and macOS (Apple Silicon) are attached to each
[release](https://github.com/baptiste0928/jj-mesh/releases). This installs the latest one for your
machine in `~/.local/bin`:

```sh
curl -fL "https://github.com/baptiste0928/jj-mesh/releases/latest/download/jj-mesh-$(uname -s)-$(uname -m).tar.gz" | tar xz -C ~/.local/bin
```

Make sure `~/.local/bin` is in your `PATH`, or extract the archive to another directory that is.

### With Nix

For [Nix](https://nixos.org/) users, this repository is a flake exposing a
[Home Manager module](./home-module.nix) and a [NixOS module](./nixos-module.nix). Both install
`jj-mesh` and run the daemon as a user service. See each file for usage.

### With `cargo`

You can build `jj-mesh` from source with `cargo`, Rust's package manager (which you can install
with [rustup](https://rust-lang.org/tools/install/)):

```sh
cargo install --git https://github.com/baptiste0928/jj-mesh.git --locked
```

## Usage

### Pairing machines

To get started, pair your machines together: they form a mesh, in which every repo you add gets
synced. Run `jj-mesh setup` on your first machine to start the daemon and get a one-time pairing
ticket, then pass the ticket to the other machine:

```sh
# On the first machine
$ jj-mesh setup

# Redeem the ticket on the second machine
$ jj-mesh setup jjmesh-pair-...
```

Once they are paired, you'll see the other machine show up in `jj-mesh status`. Run `jj-mesh setup`
again whenever you want to add another machine.

> [!IMPORTANT]
> `jj-mesh` is meant to be used **only across personal machines you control**. Every paired machine
> has full read/write access to every repo in the mesh, and can pair other machines.
>
> With auto-snapshot (on by default), **any secret written to a non-ignored file is synced to every
> machine within seconds**. Deleting the file does not remove it since the snapshot stays in the
> operation log of every machine. Keep secrets in ignored files, and rotate any that got synced.

### Syncing repos

You can now add a repo to the mesh, and clone it on another machine to start syncing:

```sh
# On the first machine
$ jj-mesh add ~/path/to/repo

# On the other machine
$ jj-mesh clone <name>
```

From there, the daemon keeps both copies in sync. Each machine gets a workspace named after it,
which you can see and edit from anywhere in the mesh.

You can add more workspaces on any machine with `jj workspace add` as usual, as long as their names
are unique across the mesh.

`jj-mesh status` shows the state of the mesh, `jj-mesh logs` the recent activity of the daemon, and
`jj-mesh help` lists the available commands.

### Server mode

If you have an always-on machine, you can put it in server mode. It then keeps a copy of every
repo in the mesh, without a working copy, so your repos are always available to your other
machines:

```sh
# When setting up the machine
$ jj-mesh setup --server

# On a machine already in the mesh
$ jj-mesh server enable
```

You can still have working copies on a server using `jj-mesh clone`.

### Configuration

Settings live in the `[jj-mesh]` table of your [jj config](https://docs.jj-vcs.dev/latest/config/),
and can be set per user, repo or workspace. Restart the daemon with `jj-mesh service restart` to
apply your changes.

```toml
[jj-mesh]
# Delay between automatic snapshots, in seconds or as a duration like "1m" (0 disables auto-snapshot)
snapshot-interval = 20
# Update stale working copies
update-stale = true
```

## How it works

`jj-mesh` works by syncing every [operation](https://docs.jj-vcs.dev/latest/operation-log) that
happens in watched repos. The daemon watches `.jj/repo/op_heads/`, and sends the new heads to its
peers along with the [git objects](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects) they
reference.

Repos are read and written with `jj-lib`. Only the default storage backends are supported, and
nothing specific to `jj-mesh` is ever written inside the repos. Operations written at the same time
on two machines are merged by `jj`, as it does for concurrent commands on a single machine.

The sync itself is append-only, which makes everything recoverable through the operation log.
Operations are never modified by `jj-mesh` during the transfer, and everything is validated and
written in an order that keeps the repo consistent even if the `jj-mesh` daemon stops at any point.

## Security and privacy

Machines connect directly to each other with [iroh](https://www.iroh.computer/), which uses
[hole punching](https://en.wikipedia.org/wiki/Hole_punching_(networking)) to get through most
networks. Connections are always end-to-end encrypted and mutually authenticated with per-machine
keys, so only paired machines can connect to each other.

There is no permission system or authoritative server by design. Every paired machine is trusted
and has full read-write access on all synced repos (see [Pairing machines](#pairing-machines)).

We use iroh's public servers for discovery, where each machine publishes its address for its peers
to find, and as relays when a direct connection fails. See
[iroh's Security & Privacy](https://docs.iroh.computer/concepts/security-privacy) for more
information.

## Contributing

Bug reports and feedback are welcome. Please open an issue to discuss any larger feature request
first. Obviously low-quality contributions (such as AI-generated ones without human oversight)
will be closed immediately.
