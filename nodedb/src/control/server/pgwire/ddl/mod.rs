// SPDX-License-Identifier: BUSL-1.1

pub mod catalog_propose;
pub mod database;
#[path = "router/mod.rs"]
pub mod router;
pub mod temp_table;

pub use router::dispatch;
