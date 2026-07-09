// SPDX-License-Identifier: BUSL-1.1

//! WAL append logic for write operations.
//!
//! Serializes write plans as MessagePack and appends to the appropriate
//! WAL record type. Read operations are no-ops.

mod core;
mod timeseries;
mod vector;

pub use core::{wal_append_if_write, wal_append_if_write_with_creds};
pub use timeseries::{ColumnarWalAppendArgs, wal_append_columnar, wal_append_timeseries};
pub use vector::{
    VectorDeleteWalArgs, VectorPutWalArgs, wal_append_vector_delete_by_surrogate,
    wal_append_vector_put,
};

// Payload encoders shared by the autocommit WAL path and transaction resolve, so
// each engine's record shape lives in exactly one place.
pub(crate) use timeseries::{encode_columnar_batch_payload, encode_timeseries_batch_payload};
pub(crate) use vector::{
    encode_vector_batch_put_payload, encode_vector_delete_by_surrogate_payload,
    encode_vector_delete_payload, encode_vector_put_payload,
};

pub(crate) use super::wal_dispatch_fts_spatial::{
    encode_spatial_delete_payload, encode_spatial_put_payload,
};
pub use super::wal_dispatch_fts_spatial::{
    wal_append_fts_delete, wal_append_fts_index, wal_append_spatial_delete, wal_append_spatial_put,
};
