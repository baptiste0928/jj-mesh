# jj-mesh

`jj-mesh` is a peer-to-peer synchronization daemon for [`jj`](https://docs.jj-vcs.dev/) repositories.
It replicates both the *git objects* and the *[operation log](https://docs.jj-vcs.dev/latest/operation-log/)*
over a private mesh (using [iroh](https://www.iroh.computer/)).

**This allows all synced machines to share the same repo state** (changes, bookmarks, undo history)
while having different working copies. It is similar to the built-in [`jj workspace`](https://docs.jj-vcs.dev/latest/working-copy/#workspaces)
but works across machines:

- **Changes are replicated instantly** in the background, including working-copy commits and unnamed heads.
- The daemon snapshots working copies regularly, so edits get synced without running `jj`.
- Stale working copies are updated automatically. You can edit files on another machine by editing
  its current change.
- Since the [op log](https://docs.jj-vcs.dev/latest/operation-log/) is synced, concurrent operations
  get merged automatically and everything is recoverable.

![Screenshot of `jj-mesh status`](./docs/screenshot.png)

`jj-mesh` is made to sync across your personal machines. You can for example use it to run a coding
agent on a server and inspect its edits locally, or keep your work-in-progress changes in sync
between your desktop and laptop.

## Installation

`jj-mesh` is **experimental** and not yet distributed by package managers. You'll
need to compile it from source, as we don't provide pre-built binaries. You can watch this
repository on GitHub to be notified of any updates.

It is currently tested on Linux and supports macOS as well, and **supports `jj` 0.43 and above**.
Windows is unsupported.

### With `cargo`

You can install `jj-mesh` with `cargo`, Rust's package manager (which you can install with
[rustup](https://rust-lang.org/tools/install/)):

```sh
$ cargo install --git https://github.com/baptiste0928/jj-mesh.git --locked
```

Once installed, you'll need to create a user service to start the daemon in the background. We
provide a command to do that for you, with `systemd` on Linux and `launchd` on macOS.

```sh
$ jj-mesh service install
```

### With Nix and Home Manager

For [Nix](https://nixos.org/) and [Home Manager](https://github.com/nix-community/home-manager) users,
this repository contains a flake exposing a Home Manager module which installs `jj-mesh` and runs the
daemon as a user service.

Add the flake as an input:

```nix
inputs.jj-mesh.url = "github:baptiste0928/jj-mesh";
```

Then enable the service in your Home Manager configuration:

```nix
{ inputs, ... }:
{
  imports = [ inputs.jj-mesh.homeModules.default ];

  services.jj-mesh = {
    enable = true;
    # Optional, to manage config.toml declaratively:
    settings = { };
  };
}
```

## Usage

Start by pairing your machines together. After setting up the daemon, run `jj-mesh peer add` on one
machine to print a one-time pairing ticket, then redeem it on the other machine:

```sh
# On the first machine
$ jj-mesh peer add

# Redeem the ticket on the second machine
$ jj-mesh peer add jjmesh-pair-...
```

If the connection is established, you'll see the machine show up in `jj-mesh status`. You can add
more machines by running `jj-mesh peer add` again.

> [!IMPORTANT]
> `jj-mesh` is meant to be used **across personal machines you control** only. Once a machine gets
> added to the mesh, it gets full read/write access to all added repos and can add other machines.
>
> Also, since the daemon regularly snapshots the working copy by default, **any secret written in
> a non-ignored file is synced almost instantly**. Once synced, it lives in the operation log of
> every machine and can be recovered.

After you've added the machines, you can add a repo to the mesh and clone it on another machine to
start syncing:

```sh
# On the first machine, inside the repo
$ jj-mesh repo add

# On the other machine
$ jj-mesh repo clone <name>
```

> Due to a [current limitation of jj](https://github.com/jj-vcs/jj/issues/8052), **only one instance
> of each repo can be colocated** (= with a `.git` folder usable by plain git tools). All repos
> cloned by `jj-mesh repo clone` are not colocated to avoid any issues.

From there, the daemon will keep both copies in sync. Use `jj-mesh status` to check the status,
and `jj-mesh help` to list available commands.

You can configure the daemon in `~/.config/jj-mesh/config.toml` to **disable or adjust auto-snapshots**
or disable updating stale working copies. A template file to edit is written on first start.

## How it works

`jj-mesh` works mainly by continuously syncing any [operation](https://docs.jj-vcs.dev/latest/operation-log)
that happens in watched repositories. It watches the `.jj/repo/op_heads/` folder which is updated
anytime an operation is performed, then announces the new heads. Any interested peer will then fetch
its missing operations, as well as the [git objects](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects)
it references.

If auto-snapshot is enabled (which is the default), the daemon will also watch any non-ignored file
in the repo and regularly perform a snapshot to save the working copy state in jj (this is usually
done automatically before any `jj` command). Auto-snapshot syncs the contents in near real-time,
without waiting for the next `jj` command to be run.

Most of it works thanks to the operation log. Concurrent operations get merged automatically by
`jj`, which avoids many conflicts that occur when using regular `git`-only sync.

Check out [`docs/`](./docs/src) for more information about the internals.

## Security and privacy

`jj-mesh` uses [iroh](https://www.iroh.computer/) to connect directly between machines, which
uses [hole punching](https://en.wikipedia.org/wiki/Hole_punching_(networking)) to get a direct
connection in various network conditions. Connections are mutually authenticated with a
per-machine private key, so only paired machines can connect to each other.

We use iroh's public relays to perform the initial handshake, and sometimes as a proxy when a
direct connection fails. All data is always fully end-to-end encrypted in transit. See
[iroh's Security & Privacy](https://docs.iroh.computer/concepts/security-privacy) for more
information.

## Contributing

Bug reports and feedback are welcome. **Pull requests other than simple fixes
will generally not be accepted**. If you wish to suggest a larger change, open
an issue to discuss it first.
