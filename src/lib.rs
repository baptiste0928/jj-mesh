//! jj-mesh is a peer-to-peer sync service for personal jj repositories. It
//! syncs op log and git objects across machines to instantly replicate changes.
//!
//! Machines are connected peer-to-peer using `iroh`.
//!
//! This crate hosts both the management CLI and the sync daemon, both exposed
//! as a single `jj-mesh` binary.

#![warn(clippy::pedantic)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

pub mod cli;
pub mod config;
pub mod net;
