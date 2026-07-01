// SPDX-License-Identifier: BUSL-1.1

//! Local-state inspector methods on [`TestClusterNode`].
//!
//! These read-only accessors let integration tests assert that an
//! applier ran on every node (catalog caches, trigger/schedule/stream
//! registries, lease/drain maps, etc.) without reaching through
//! private fields. Grouped here to keep `lifecycle.rs` focused on
//! spawn/shutdown.

mod catalog;
mod crdt;
mod lease;
mod snapshot;
mod topology;
