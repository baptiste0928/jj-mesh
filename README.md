# jj-mesh

`jj-mesh` is a bi-directional sync service for [`jj`](https://docs.jj-vcs.dev/).
It allows to have copies of a repository across multiple machines, while keeping
them continously in sync.

It is similar to [workspaces](https://docs.jj-vcs.dev/latest/working-copy/#workspaces),
but the working copies can be on multiple machines. It can be useful e.g. if you want
to run coding agents in external VMs.

Changes are replicated instantly across machines in the background, by syncing
both the *git objects* and the *[operation log](https://docs.jj-vcs.dev/latest/operation-log/)*.
