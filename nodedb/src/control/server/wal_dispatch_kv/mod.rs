// SPDX-License-Identifier: BUSL-1.1

//! WAL append for KV engine operations.

mod append;
mod encode;

pub use append::wal_append_kv_op;
pub(crate) use encode::encode_kv_put;
