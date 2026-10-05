# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Added `jj-mesh setup` command to setup a new machine** in a single step. It setups the service
  and pairs the machine with the mesh.
- Recent daemon logs can be viewed with the new `jj-mesh logs` command. Existing logs have been
  made less noisy.
- New `jj-mesh clone` and `jj-mesh add` top-level aliases for `jj-mesh repo clone` and
  `jj-mesh repo add`. Clone now asks which repo to clone when no name is given.

### Changed
- **Auto-snapshot and update-stale now run on secondary workspaces**, which are listed in
  `jj-mesh status`.
- **Settings moved to the `[jj-mesh]` table of your jj config**, and can now be set per user, repo
  or workspace.
- Syncing is now paused for a workspace if it is claimed by multiple machines in the mesh.

### Fixed
- **Syncs use less data.** Missing objects are now computed much more accurately, we could
  previously sometimes re-send hundreds of objects for a single-line change.
- Peers changes were sometimes rejected after a restart when the local machine did not
  notice the old connection drop (e.g. while suspended).
- Shallow and partial git clones are now explicitely refused.
- The daemon installed on macOS now logs to `~/Library/Logs/jj-mesh.log`.

## [0.2.0] - 2026-09-06

### Changed
- **Repos can now be colocated on every machine.** `jj-mesh repo clone` respects your global jj
  `git.colocate` setting (on by default).
- **`jj` 0.45 is required.** Older versions are no longer supported.
- Current machine can now be renamed with `jj-mesh peer rename`.

### Fixed
- Git refs are now synced even for non-colocated repositories. This allows you to run
  `jj git colocation enable` safely on any previously cloned repo.
- Improved memory usage when idle, including a case where memory could grow
  multiple GB when you have a large `/etc/hosts`.
- Improved reliability of git refs syncing on concurrent operations.
- The cli now errors immediately if the daemon is running a different version.

## [0.1.1] - 2026-08-10

### Changed
- **Added `jj` 0.44 to the supported versions.** `jj` 0.43 is still supported.
- `jj-mesh status` now displays a warning when a peer is running a different `jj` version than the
  local one.
- The Nix flake now uses `buildRustPackage` instead of [crane](https://crane.dev/). It no longer
  leaves large build artifacts in the user's Nix store after compiling from source.

### Fixed

- Synced op heads are now made visible *after* the index has been built. This prevents the user's
  `jj` commands from blocking while indexing is in progress.
- The leftover destination directory is now removed when cloning a repo from the mesh fails.
- `jj-mesh service start/stop/restart` now works when installed with Home Manager on macOS.

## [0.1.0] - 2026-08-04

Initial version of `jj-mesh`.

[unreleased]: https://github.com/baptiste0928/jj-mesh/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/baptiste0928/jj-mesh/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/baptiste0928/jj-mesh/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/baptiste0928/jj-mesh/releases/tag/v0.1.0
