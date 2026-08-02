// SPDX-License-Identifier: BUSL-1.1

//! Production metadata-group commit applier.
//!
//! Single branch for DDL: decode the opaque `CatalogDdl { payload }`
//! as a host-side [`CatalogEntry`], write through to `SystemCatalog`
//! redb via [`catalog_entry::apply_to`], and spawn the post-apply
//! side effects (Data Plane register, sequence registry sync, etc.).
//! All 16 per-DDL-object types are handled by adding a variant to
//! `CatalogEntry` — nothing in this file changes per type.
//!
//! The applier broadcasts `CatalogChangeEvent` (for future
//! prepared-statement / catalog cache invalidation). The per-group
//! apply watermark is maintained by the Raft tick loop directly via
//! [`nodedb_cluster::GroupAppliedWatchers`] — the applier no longer
//! owns its own watcher because that primitive is now keyed by
//! `group_id` and shared across every Raft group on the node.
//!
//! Split by concern:
//! - [`types`]: the `MetadataCommitApplier` struct, construction, and
//!   `CatalogChangeEvent`.
//! - [`lease_events`]: descriptor-drain and CA-trust-change effects.
//! - [`surrogate`]: cross-engine surrogate HWM + HiLo batch reservation.
//! - [`sync_and_routing`]: Lite sync-producer register/fence + live
//!   routing-table `SetPlacement`.
//! - [`catalog_ddl`]: `CatalogDdl` / `CatalogDdlAudited` decode + apply.
//! - [`dispatch`]: the recursive `apply_host_side_effects` entry point
//!   and `impl MetadataApplier for MetadataCommitApplier`.
//! - [`audit`]: audit and CA-trust helpers (kept as its own file; used
//!   by [`catalog_ddl`] and [`lease_events`]).
//! - [`wedge`]: transient-vs-permanent classification of an apply failure
//!   and the readiness marker a permanent one leaves behind.

mod audit;
mod catalog_ddl;
mod dispatch;
mod lease_events;
mod surrogate;
mod sync_and_routing;
mod types;
mod wedge;

#[cfg(test)]
mod tests;

pub use types::{CATALOG_CHANNEL_CAPACITY, CatalogChangeEvent, MetadataCommitApplier};
pub use wedge::{ApplyFailureClass, MetadataApplyWedge, WedgeReport, classify};
