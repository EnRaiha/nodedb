// SPDX-License-Identifier: BUSL-1.1

//! Wire version constants + static compatibility checks.
//!
//! See `view::ClusterVersionView` for the live-topology-derived
//! feature-gate predicates.

use super::view::ClusterVersionView;
#[cfg(test)]
use crate::version::{MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION};

// The join window is open: `MIN_WIRE_FORMAT_VERSION < WIRE_FORMAT_VERSION`
// (1..=2), so mixed-version clusters can form during an N-1 rolling
// upgrade and these gates are live — `min_version >= V` now discriminates
// inside the window instead of being constant-true.
//
// Every gate below is pinned to `1` — the value each feature shipped
// under. Keep them there and follow one rule: *bump a gate to the new
// WIRE value in the same PR that lands its wire-shape change.* Raising a
// gate without a wire-shape change just switches the feature OFF
// permanently and silently routes to a legacy fallback; lowering one
// below its original value lets pre-feature nodes activate it.

/// Wire-format version that introduced the replicated catalog DDL
/// path (`CatalogEntry` proposed via the metadata raft group).
///
/// Before this version, catalog DDL was applied directly on the
/// originating node and never replicated. Mixing the two paths in
/// a rolling upgrade window would silently diverge state across
/// nodes, so [`crate::control::metadata_proposer::propose_catalog_entry`]
/// gates on this constant via
/// [`ClusterVersionView::can_activate_feature`] and falls back to
/// the legacy direct-write path until every node in the cluster
/// has caught up.
pub const DISTRIBUTED_CATALOG_VERSION: u16 = 1;

/// Wire-format version that introduced monotonic descriptor
/// versioning (`descriptor_version: u64` + `modification_hlc: Hlc`
/// on every `Stored*` type stamped by the metadata applier at
/// commit time).
///
/// Before this version, `Stored*` records had no version / HLC
/// fields on the wire. In a mixed-version cluster during rolling
/// upgrade, an older applier would fail to re-stamp on
/// write-through (it has no stamp logic), so we keep the stamping
/// path disabled in compat mode and let resolvers treat
/// `descriptor_version == 0` as "unknown, always re-fetch". Once
/// every node reports `wire_version >= 3`, the applier transitions
/// to stamping.
pub const DESCRIPTOR_VERSIONING_VERSION: u16 = 1;

/// Wire version that introduced the replicated
/// `DescriptorDrainStart` / `DescriptorDrainEnd` metadata entries.
/// Mixed-version clusters below this version skip drain via the
/// compat-mode fallback in `drain_for_ddl`.
pub const DESCRIPTOR_DRAIN_VERSION: u16 = 1;

/// Check if a message from a remote node should be accepted.
///
/// Accepts versions in the rolling-upgrade window
/// `[MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION]`; anything newer or
/// older than the window is rejected.
pub fn accept_message(remote_version: u16) -> crate::Result<()> {
    crate::version::check_wire_compatibility(remote_version)
}

/// Determine if this node should operate in compatibility mode.
///
/// Compat mode is active when the cluster has mixed versions. In
/// compat mode, new features that require the latest version are
/// disabled.
pub fn should_compat_mode(view: &ClusterVersionView) -> bool {
    view.is_mixed_version()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_same_version() {
        assert!(accept_message(WIRE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn reject_newer() {
        assert!(accept_message(WIRE_FORMAT_VERSION + 1).is_err());
    }

    /// With the window open, a message from an N-1 node (at the floor)
    /// is accepted.
    #[test]
    fn older_in_window_accepted() {
        if WIRE_FORMAT_VERSION > MIN_WIRE_FORMAT_VERSION {
            assert!(accept_message(MIN_WIRE_FORMAT_VERSION).is_ok());
        }
    }

    /// A version below the floor is rejected.
    #[test]
    fn older_than_floor_rejected() {
        assert!(accept_message(0).is_err());
    }
}
