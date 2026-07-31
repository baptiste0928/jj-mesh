# jj-mesh

`jj-mesh` is a bi-directional sync service for [`jj`](https://docs.jj-vcs.dev/).
It allows to have copies of a repository across multiple machines, while keeping
them continously in sync.

It is similar to [workspaces](https://docs.jj-vcs.dev/latest/working-copy/#workspaces),
but the working copies can be on multiple machines. It can be useful e.g. if you want
to run coding agents in external VMs.

Changes are replicated instantly across machines in the background, by syncing
both the *git objects* and the *[operation log](https://docs.jj-vcs.dev/latest/operation-log/)*.

## Installation

### Nix (Home Manager)

The flake exports a Home Manager module that installs `jj-mesh` and runs the
daemon as a user service (systemd on Linux, launchd on macOS). Add the flake
as an input:

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
    # Optional, manages config.toml declaratively:
    settings = {
      snapshot-interval = 30;
      repos.work.update-stale = false;
    };
  };
}
```

The daemon invokes jj through a pinned store path: the `programs.jujutsu`
package when enabled, `pkgs.jujutsu` otherwise (override with
`services.jj-mesh.jjPackage`). No manual path setup is needed, and the
service restarts automatically when the configuration changes.

Do not combine the module with `jj-mesh service install`: both manage the
same service definition.

### Other setups

Install the `jj-mesh` binary on your `PATH`, then install and start the user
service with:

```sh
jj-mesh service install
```
