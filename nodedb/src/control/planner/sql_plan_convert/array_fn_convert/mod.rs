// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::Array*` → `PhysicalTask` lowering for the read/maint
//! function surface (ARRAY_SLICE, which ARRAY_PROJECT plans to, ARRAY_AGG,
//! ARRAY_ELEMENTWISE, ARRAY_FLUSH, ARRAY_COMPACT).

pub mod aggregate;
pub mod elementwise;
pub mod helpers;
pub mod maint;
pub mod region;
pub mod slice;
pub mod tile_keys;

pub(super) use aggregate::convert_agg;
pub(super) use elementwise::convert_elementwise;
pub(super) use maint::{convert_compact, convert_flush};
pub(super) use slice::convert_slice;
