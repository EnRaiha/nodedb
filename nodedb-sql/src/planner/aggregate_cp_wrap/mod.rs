// SPDX-License-Identifier: Apache-2.0

//! Control-Plane projection over grouped results.

mod expression;
mod projection;
mod wrap;

pub(crate) use projection::items_need_group_row_tail;
pub use wrap::wrap_aggregate_cp_items;
