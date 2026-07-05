// SPDX-License-Identifier: BUSL-1.1

//! KV engine operation handlers for the Data Plane executor.

mod atomic;
mod batch;
mod crud;
mod dispatch;
mod field;
mod index;
mod materialize_scan;
mod scan;
mod sorted;
mod transfer;
mod ttl;

pub(in crate::data::executor) mod field_compute;
pub(in crate::data::executor) mod transfer_compute;
