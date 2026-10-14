// SPDX-License-Identifier: BUSL-1.1

//! Upsert handler: insert if absent, merge fields if present.

pub mod exec;
pub mod merge;

pub(in crate::data::executor) use exec::{UpsertParams, decode_stored_row, merge_upsert_body};
pub(in crate::data::executor) use merge::{apply_on_conflict_updates, merge_values};
