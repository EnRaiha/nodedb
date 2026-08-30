// SPDX-License-Identifier: BUSL-1.1

//! Well-known Raft group identifiers.

/// Well-known Raft group ID for the metadata group.
/// Distinct from data vShard groups (which start at group 1).
pub const METADATA_GROUP_ID: u64 = 0;
