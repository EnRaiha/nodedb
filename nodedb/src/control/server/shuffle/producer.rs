// SPDX-License-Identifier: BUSL-1.1

//! Producer-side helper for cross-node streaming shuffle (E1).
//!
//! Control Plane (Tokio) only. Sends a pre-partitioned set of row batches to a
//! single target node's receiver inbox for one `(shuffle_id, part, side)`.
//!
//! # E4 is the partitioning caller
//!
//! This helper takes **already-partitioned** payloads — one batch per call,
//! all destined for the same `(shuffle_id, part, side)` on `target`. Computing
//! `partition_hash(row, keys) % num_parts`, grouping rows by partition, and
//! routing each partition to its owning node is the responsibility of the
//! planner-side shuffle emitter (E4), not this unit.

use nodedb_cluster::{NexarTransport, ShufflePushRequest};

use crate::{Error, Result};

/// Send one shuffle push stream to `target`: a `ShufflePushRequest` opener,
/// then one chunk per `batches` element, then a clean `End`.
///
/// Each `batches` element is a standalone msgpack array of rows (the
/// `RowBatch.payload` convention). The transport opens a bidi stream, writes
/// the frames, and finishes the send half without awaiting a reply — the
/// receiver deposits the chunks into its inbox and never writes back.
pub async fn send_shuffle_push(
    transport: &NexarTransport,
    target: u64,
    req: ShufflePushRequest,
    batches: Vec<Vec<u8>>,
) -> Result<()> {
    transport
        .send_shuffle_push(target, req, batches)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("shuffle push to node {target}: {e}"),
        })
}
