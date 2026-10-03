// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral synonym group DDL — CREATE / DROP / SHOW.
//!
//! Each handler runs the tenant-admin gate, the duplicate / existence check
//! against the in-memory `synonym_registry`, and the `propose_catalog_entry`.
//!
//! A group belongs to one database and one tenant. Every check and every
//! write carries the session's `database_id`, matching the catalog key and
//! the per-database FTS backend.
//!
//! The registry update and the Data-Plane FTS install belong to the
//! post-apply lane, which runs on every node, this one included.

pub mod create;
pub mod drop;
pub mod show;

pub use create::create_synonym_group;
pub use drop::drop_synonym_group;
pub use show::show_synonym_groups;
