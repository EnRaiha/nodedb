// SPDX-License-Identifier: BUSL-1.1

//! VShardMessageType discriminant constants for array shard RPCs.
//!
//! These values mirror the `VShardMessageType` repr(u16) variants added
//! in `crate::wire`. Kept here as typed constants so coordinator and
//! handler code can refer to them without importing the full enum.

/// Coordinator → shard: execute a coord-range slice.
pub const ARRAY_SHARD_SLICE_REQ: u32 = 80;
/// Shard → coordinator: slice result rows.
pub const ARRAY_SHARD_SLICE_RESP: u32 = 81;
/// Coordinator → shard: compute a partial aggregate.
pub const ARRAY_SHARD_AGG_REQ: u32 = 82;
/// Shard → coordinator: partial aggregate result.
pub const ARRAY_SHARD_AGG_RESP: u32 = 83;
/// Coordinator → shard: write cell batch.
pub const ARRAY_SHARD_PUT_REQ: u32 = 84;
/// Shard → coordinator: put acknowledgement.
pub const ARRAY_SHARD_PUT_RESP: u32 = 85;
/// Coordinator → shard: delete cells by exact coords.
pub const ARRAY_SHARD_DELETE_REQ: u32 = 86;
/// Shard → coordinator: delete acknowledgement.
pub const ARRAY_SHARD_DELETE_RESP: u32 = 87;
/// Coordinator → shard: surrogate bitmap scan.
pub const ARRAY_SHARD_SURROGATE_BITMAP_REQ: u32 = 88;
/// Shard → coordinator: surrogate bitmap result.
pub const ARRAY_SHARD_SURROGATE_BITMAP_RESP: u32 = 89;
/// Coordinator → shard: pair two arrays' cells and apply a binary op.
pub const ARRAY_SHARD_ELEMENTWISE_REQ: u32 = 90;
/// Shard → coordinator: paired result rows.
pub const ARRAY_SHARD_ELEMENTWISE_RESP: u32 = 91;
/// Coordinator → shard: flush or compact an array.
pub const ARRAY_SHARD_MAINT_REQ: u32 = 92;
/// Shard → coordinator: maintenance acknowledgement.
pub const ARRAY_SHARD_MAINT_RESP: u32 = 93;
