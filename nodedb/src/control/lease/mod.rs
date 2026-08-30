// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease acquisition and release via the metadata raft group.
//!
//! Wraps the `MetadataEntry::DescriptorLeaseGrant` /
//! `DescriptorLeaseRelease` raft path that already exists in
//! `nodedb-cluster`. The cluster crate owns the canonical lease
//! state in `MetadataCache.leases` (a
//! `HashMap<(DescriptorId, node_id), DescriptorLease>`), populated
//! by every node's commit applier as soon as a grant or release
//! entry commits on the metadata raft group.
//!
//! This module provides the host-side API surface — `acquire_lease`
//! and `release_leases` — that proposes those entries and blocks
//! on the local applied watermark, mirroring the
//! `metadata_proposer::propose_catalog_entry` pattern.
//!
//! The planner acquires a lease before reading a descriptor to prevent
//! stale reads across DDL. DDL drain consumes the `MetadataCache.leases`
//! view before committing a new descriptor version. On `SIGTERM`, leases
//! are released explicitly so they drain faster than expiry.

pub mod descriptor_lookup;
pub mod drain;
pub mod drain_propose;
pub mod gc;
mod leader_wait;
pub mod propose;
pub mod refcount;
pub mod release;
pub mod renewal;
pub mod shutdown_release;
mod wall_time;

pub(super) use wall_time::wall_now_ns;

pub use descriptor_lookup::{descriptor_id_and_prior_version, descriptor_id_for_implicit_clear};
pub use drain::{DescriptorDrainTracker, DrainEntry};
pub use drain_propose::drain_for_ddl;
pub(super) use leader_wait::{PROPOSE_TIMEOUT, propose_and_wait};
pub use propose::{DEFAULT_LEASE_DURATION, acquire_lease, compute_expires_at, force_refresh_lease};
pub(crate) use propose::{acquire_lease_after_admission, ensure_not_draining};
pub use refcount::{LeaseRefCount, QueryLeaseScope};
pub use release::release_leases;
pub use renewal::{LeaseRenewalConfig, LeaseRenewalLoop};
