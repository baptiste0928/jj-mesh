# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- ***BREAKING*** Updated supported `jj` version to 0.44.
- Synced op heads are now made visible *after* the index has been built. This prevent user's `jj`
  commands to block when indexing is in progress.
- The Nix flake now uses `buildRustPackage` instead of [crane](https://crane.dev/). It no longer
  leaves large build artifacts in the user nix store after compiling from source.

## [0.1.0] - 2026-08-04

Initial version of `jj-mesh`.

[unreleased]: https://github.com/baptiste0928/jj-mesh/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/baptiste0928/jj-mesh/releases/tag/v0.1.0
