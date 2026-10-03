// SPDX-License-Identifier: BUSL-1.1

//! Static compatibility checks.
//!
//! See `view::ClusterVersionView` for the live-topology-derived
//! predicates.
//!
//! `MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION`, so a node rejects any
//! peer whose version differs and a mixed-version cluster never forms. No
//! feature gates on a version: every node of a cluster runs one build.

use super::view::ClusterVersionView;

/// Whether the cluster reports mixed versions. Observability only: a
/// cluster that formed never reports them.
pub fn should_compat_mode(view: &ClusterVersionView) -> bool {
    view.is_mixed_version()
}
