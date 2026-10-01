// SPDX-License-Identifier: Apache-2.0

//! Single source of truth for the `WIRE_FORMAT_VERSION` constant and the
//! `WIRE_BUILD_ID` build identity, shared between every crate that needs to
//! stamp or interpret them.
//!
//! `WIRE_FORMAT_VERSION` is the *cluster-wide* wire format version, distinct
//! from:
//! - `nodedb_cluster::wire::WIRE_VERSION` (the binary frame layout
//!   version of the `VShardEnvelope`),
//! - the RPC frame header version in
//!   `nodedb_cluster::rpc_codec::header` (a private constant of that
//!   module).
//!
//! # The enforced invariant before 1.0: exact build identity
//!
//! `WIRE_FORMAT_VERSION` stays at `1` until the first stable release — see
//! below for why a bump buys nothing pre-1.0. That leaves a hole: two builds
//! can change wire shapes (a new enum variant, an RPC field) without
//! touching this constant, so a version-only check lets them join the same
//! cluster and misdecode each other. `WIRE_BUILD_ID` closes it: every join
//! and every wire-version handshake also compares this value for exact
//! equality, so a cluster can only ever contain nodes running one build.
//!
//! `WIRE_BUILD_ID` is the current commit's short git hash, or
//! `CARGO_PKG_VERSION` when git is unavailable (a crates.io build) — see
//! `build.rs`. It intentionally excludes dirty-tree state: hashing local
//! edits would force a rebuild of every dependent crate on every save.
//!
//! # DO NOT BUMP `WIRE_FORMAT_VERSION` BEFORE 1.0
//!
//! Read this before changing it — the reflex to bump on any wire-shape
//! change is wrong here:
//!
//! - **There is nothing to be compatible with.** Pre-1.0 there are no
//!   deployed clusters, so there is no older peer a new build must talk to.
//! - **A bump cannot buy a rolling upgrade.** `MIN_WIRE_FORMAT_VERSION ==
//!   WIRE_FORMAT_VERSION` (floor == ceiling), so a node rejects *any* peer
//!   whose version differs. Mixed-version clusters cannot form at all, which
//!   makes every `wire_version >= V` feature gate dead code: inside a cluster
//!   that exists, all nodes are provably on this exact version. Adding such a
//!   gate is unreachable-branch hardening, not safety.
//! - **This value is NOT persisted.** It is stamped on `NodeInfo` for the
//!   handshake and drives `ClusterVersionView`, nothing more. The version
//!   written into stored raft-log and metadata entries is
//!   `nodedb_cluster::wire_version::WireVersion::CURRENT`, which is separate
//!   and independent. Changing the constant here therefore cannot orphan or
//!   corrupt anything already on disk.
//!
//! So: adding a new enum variant, RPC, or payload field needs NO bump.
//! `WIRE_BUILD_ID` already forces every node in a working cluster onto the
//! same build. Ratcheting this pre-1.0 only invents a stop-the-world upgrade
//! requirement that does not otherwise exist, and would leave 1.0 shipping
//! as "wire version 20" for no reason.
//!
//! After 1.0, when real deployments exist and a genuine compatibility window
//! is introduced, this becomes meaningful — bump it then, deliberately, and
//! only alongside an actual `MIN_WIRE_FORMAT_VERSION < WIRE_FORMAT_VERSION`
//! support window.

/// Cluster-wide wire format version. Stamped on every `NodeInfo` and
/// returned by `nodedb::version::WIRE_FORMAT_VERSION` (a re-export).
///
/// WARNING: pinned at 1 until 1.0. See the module docs above before changing.
pub const WIRE_FORMAT_VERSION: u16 = 1;

/// Minimum wire format version this build can read. Equal to
/// `WIRE_FORMAT_VERSION`: floor == ceiling, no backward compat window.
pub const MIN_WIRE_FORMAT_VERSION: u16 = WIRE_FORMAT_VERSION;

/// This build's exact identity: the short git commit hash, or
/// `CARGO_PKG_VERSION` when git is unavailable (a crates.io build). Set by
/// `build.rs`.
///
/// Compared for exact equality on every join and wire-version handshake —
/// the enforced invariant before 1.0. See the module docs above.
pub const WIRE_BUILD_ID: &str = env!("NODEDB_WIRE_BUILD_ID");

// Compile-time invariants — these constants must satisfy:
//   - MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION
//   - WIRE_FORMAT_VERSION > 0
const _: () = assert!(MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION);
const _: () = assert!(WIRE_FORMAT_VERSION > 0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_build_id_is_non_empty() {
        assert!(!WIRE_BUILD_ID.is_empty(), "WIRE_BUILD_ID must not be empty");
    }
}
