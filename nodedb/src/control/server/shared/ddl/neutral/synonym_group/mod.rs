// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral synonym group DDL — CREATE / DROP / SHOW.
//!
//! Each handler runs the tenant-admin gate, the duplicate / existence check
//! against the in-memory `synonym_registry`, the `propose_catalog_entry` +
//! `LocalOnly` manual catalog write, and the in-memory registry update.
//!
//! A group belongs to one database and one tenant. Every check and every
//! write carries the session's `database_id`, matching the catalog key and
//! the per-database FTS backend.
//!
//! The Data-Plane FTS install belongs to the post-apply lane, which runs on
//! every node. A handler reaches it only on the `LocalOnly` path, where no
//! applier runs, and calls the same function the lane calls.

pub mod create;
pub mod drop;
pub mod show;

pub use create::create_synonym_group;
pub use drop::drop_synonym_group;
pub use show::show_synonym_groups;
