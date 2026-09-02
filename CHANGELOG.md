# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Repos can now be colocated on every machine.** `jj-mesh repo clone` respects your global jj
  `git.colocate` setting (on by default).
- **`jj` 0.45 is required.** Older versions are no longer supported.
- Current machine can now be renamed with `jj-mesh peer rename`.

### Fixed
- Improved memory usage when idle, including a case where memory could grow
  multiple GB when you have a large `/etc/hosts`.

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

[unreleased]: https://github.com/baptiste0928/jj-mesh/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/baptiste0928/jj-mesh/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/baptiste0928/jj-mesh/releases/tag/v0.1.0
