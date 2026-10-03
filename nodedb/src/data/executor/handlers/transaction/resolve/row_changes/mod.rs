// SPDX-License-Identifier: BUSL-1.1

//! The net change of every row a committing transaction writes, recorded in
//! its redo record for the change stream.

pub mod entry;
pub mod grouped;
pub mod keyed;

pub(in crate::data::executor::handlers::transaction::resolve) use entry::StagedRowCollections;
