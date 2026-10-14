// SPDX-License-Identifier: BUSL-1.1

//! Read of the source relation of a Control-Plane orchestrated write
//! (`INSERT ... SELECT`, `MERGE`, `UPDATE ... FROM`).

pub(crate) mod context;
pub(crate) mod pages;
pub(crate) mod query;
pub(crate) mod reader;
pub(crate) mod shape;

pub(crate) use context::SourceReadContext;
pub(crate) use reader::{SourceReader, read_source_rows};
