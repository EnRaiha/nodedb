// SPDX-License-Identifier: BUSL-1.1

//! Cluster catalog — persistent storage for topology, routing, cluster
//! metadata, and ghost stub refcounts.
//!
//! Uses redb (embedded B-Tree ACID storage) with MessagePack
//! serialization so nodes can recover cluster state after a restart
//! without contacting peers.
//!
//! Split across files:
//! - [`schema`]: redb table definitions, key constants, the catalog
//!   format version, and shared error helpers.
//! - [`core`]: `ClusterCatalog` struct, `open`, cluster-id +
//!   bootstrap-check + CA-cert persistence, and the migration
//!   dispatch invoked from `open`.
//! - [`ops`]: topology / routing save+load.
//! - [`ghosts`]: ghost stub save / load / enumerate / delete.
//! - [`migration`]: catalog format-version dispatcher. Stamps
//!   every fresh catalog with `CATALOG_FORMAT_VERSION`, refuses
//!   catalogs stamped by a newer binary (downgrade protection),
//!   and is the single place where future breaking schema
//!   changes land as explicit `v{N} → v{N+1}` arms.

pub mod boot_epoch;
pub mod cluster_settings;
pub mod core;
pub mod ghosts;
pub mod migration;
pub mod ops;
pub mod schema;

pub use cluster_settings::{ClusterSettings, PlacementHashId, placement_hash};
pub use core::ClusterCatalog;
