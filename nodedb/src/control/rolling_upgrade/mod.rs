// SPDX-License-Identifier: BUSL-1.1

//! Cluster wire-version view.
//!
//! There is no rolling-upgrade window before 1.0: `nodedb_types::wire_version`
//! pins `MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION` (floor == ceiling),
//! and every join and wire-version handshake additionally requires exact
//! `WIRE_BUILD_ID` equality — a cluster can only ever contain nodes on one
//! build. No code path gates on a version.
//!
//! Layout:
//!
//! - [`versions`] — `should_compat_mode`.
//! - [`view`] — `ClusterVersionView` plus `compute_from_topology`
//!   and the version predicates. Pure functions, no shared mutable state.

pub mod versions;
pub mod view;

pub use versions::should_compat_mode;
pub use view::{ClusterVersionView, compute_from_topology};
