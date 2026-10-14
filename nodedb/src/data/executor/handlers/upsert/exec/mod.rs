// SPDX-License-Identifier: BUSL-1.1

//! `execute_upsert` split by branch: [`dispatch`] probes for an existing row
//! and hands off to [`overwrite`] (merge into an existing row) or [`insert`]
//! (no existing row, insert fresh).

mod batch;
mod batch_plan;
mod dispatch;
mod insert;
mod merge_body;
mod overwrite;

pub(in crate::data::executor) use dispatch::UpsertParams;
pub(in crate::data::executor) use merge_body::{decode_stored_row, merge_upsert_body};
