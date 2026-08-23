// SPDX-License-Identifier: Apache-2.0

//! Single source of truth for the `WIRE_FORMAT_VERSION` constant
//! shared between every crate that needs to stamp or interpret it.
//!
//! This is the *cluster-wide* wire format version, distinct from:
//! - `nodedb_cluster::wire::WIRE_VERSION` (the binary frame layout
//!   version of the `VShardEnvelope`),
//! - the RPC frame header version in
//!   `nodedb_cluster::rpc_codec::header` (a private constant of that
//!   module).
//!
//! # Window semantics
//!
//! A peer is compatible iff its version lies in
//! `[MIN_WIRE_FORMAT_VERSION, WIRE_FORMAT_VERSION]`. Bump WIRE only
//! alongside an actual wire-shape change (new enum variant, RPC,
//! payload field). Keep MIN at the oldest release this build supports
//! (N-1 policy); never raise MIN without a coordinated cluster-wide
//! migration. The value is stamped on `NodeInfo`, never persisted into
//! raft-log/metadata (that is `wire_version::WireVersion::CURRENT`,
//! separate and independent), so a bump cannot orphan on-disk state.

/// Cluster-wide wire format version. Stamped on every `NodeInfo` and
/// returned by `nodedb::version::WIRE_FORMAT_VERSION` (a re-export).
pub const WIRE_FORMAT_VERSION: u16 = 2;

/// Minimum wire format version this build can read. The floor of the
/// join window: peers at or above this version (and at or below
/// `WIRE_FORMAT_VERSION`) are accepted, enabling N-1 rolling upgrades.
pub const MIN_WIRE_FORMAT_VERSION: u16 = 1;

// Compile-time invariants — these constants must satisfy:
//   - MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION
//   - WIRE_FORMAT_VERSION > 0
const _: () = assert!(MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION);
const _: () = assert!(WIRE_FORMAT_VERSION > 0);
