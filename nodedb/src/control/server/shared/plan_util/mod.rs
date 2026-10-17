// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral physical-plan classification helpers shared by every
//! server entrypoint (pgwire, native, http) and the transaction orchestrator.

mod collection;
mod collection_meta;
mod collection_rows;
mod engine;
mod read_key;

pub(crate) use collection::{extract_collection, metered_collections};
pub(crate) use engine::plan_engine;
pub(crate) use read_key::read_key_of;
