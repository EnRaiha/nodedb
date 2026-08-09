// SPDX-License-Identifier: BUSL-1.1

//! `execute_upsert` split by branch: [`dispatch`] probes for an existing row
//! and hands off to [`overwrite`] (merge into an existing row) or [`insert`]
//! (no existing row, insert fresh).

mod dispatch;
mod insert;
mod overwrite;

pub(in crate::data::executor) use dispatch::UpsertParams;
