// SPDX-License-Identifier: BUSL-1.1

//! Resolve and apply for a governed point/bulk document write.

pub(in crate::data::executor) mod apply;
mod apply_row;
mod bulk;
mod context;
pub(in crate::data::executor) mod dispatch;
mod point;
mod upsert;
