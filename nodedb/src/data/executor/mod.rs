// SPDX-License-Identifier: BUSL-1.1

pub(crate) mod checkpoint_encoding;
pub mod core_loop;
mod dispatch;
pub(crate) mod doc_format;
pub mod enforcement;
pub mod handlers;
pub(super) mod msgpack_utils;
pub mod response_codec;
mod scan_normalize;
mod snapshot;
mod sparse_vector_checkpoint;
mod spatial_checkpoint;
pub(super) mod strict_format;
pub(in crate::data) mod sync_gate;
pub mod task;
mod vector_checkpoint;
mod wal_replay;
mod wal_replay_fts;
mod wal_replay_spatial;
mod wal_replay_vector;
