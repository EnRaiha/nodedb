// SPDX-License-Identifier: BUSL-1.1

//! Snapshot chunk buffering and assembly for inbound array sync.

use std::collections::BTreeMap;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::{SnapshotChunk, SnapshotHeader, assemble_chunks};
use nodedb_types::sync::wire::array::{
    ArrayRejectMsg, ArrayRejectReason, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};
use tracing::{error, warn};

use super::inbound::{InboundOutcome, OriginArrayInbound};
use super::reject::build_reject;

/// The catch-up sender emits chunks no larger than 256 KiB. Enforce the same
/// bound before retaining untrusted payload bytes.
const MAX_CHUNK_BYTES: usize = 256 * 1024;
/// A single snapshot cannot claim an unbounded number of chunk slots.
const MAX_CHUNKS_PER_SNAPSHOT: u32 = 4_096;
/// Limit simultaneously buffered snapshot streams per inbound session.
const MAX_CONCURRENT_ASSEMBLIES: usize = 64;
/// Limit total retained payload across all assemblies in one inbound session.
const MAX_BUFFERED_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

type SnapshotKey = (String, [u8; 18]);

/// In-flight snapshot scratch buffer keyed by `(array, snapshot_hlc_bytes)`
/// inside [`OriginArrayInbound`].
pub(super) struct SnapshotAssembly {
    pub(super) header: Option<SnapshotHeader>,
    pub(super) total_chunks: Option<u32>,
    pub(super) chunks: BTreeMap<u32, SnapshotChunk>,
    pub(super) payload_bytes: usize,
}

impl SnapshotAssembly {
    pub(super) fn new() -> Self {
        Self {
            header: None,
            total_chunks: None,
            chunks: BTreeMap::new(),
            payload_bytes: 0,
        }
    }
}

fn reject(array: &str, hlc: Hlc, detail: impl Into<String>) -> Option<ArrayRejectMsg> {
    Some(build_reject(
        array,
        hlc,
        ArrayRejectReason::ShapeInvalid,
        detail.into(),
    ))
}

impl OriginArrayInbound {
    fn discard_snapshot_assembly(&self, key: &SnapshotKey) {
        if let Ok(mut snapshots) = self.snapshots().lock() {
            snapshots.remove(key);
        }
    }

    fn buffered_snapshot_bytes(
        snapshots: &std::collections::HashMap<SnapshotKey, SnapshotAssembly>,
    ) -> usize {
        snapshots.values().map(|entry| entry.payload_bytes).sum()
    }

    /// Buffer an incoming snapshot header.
    pub fn handle_snapshot_header(
        &self,
        msg: &ArraySnapshotMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let header: SnapshotHeader = match zerompk::from_msgpack(&msg.header_payload) {
            Ok(h) => h,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: snapshot header decode failed");
                return Err(reject(
                    &msg.array,
                    Hlc::ZERO,
                    format!("snapshot header decode: {e}"),
                ));
            }
        };
        let key = (msg.array.clone(), header.snapshot_hlc.to_bytes());

        // Authorization deliberately precedes every snapshot-buffer mutation,
        // including terminal-error cleanup of a partial assembly.
        let _authorized_scope = self
            .authorize_array(
                &msg.array,
                header.snapshot_hlc,
                crate::control::security::identity::Permission::Write,
            )?
            .into_scope();
        if header.array != msg.array {
            self.discard_snapshot_assembly(&key);
            return Err(reject(
                &msg.array,
                header.snapshot_hlc,
                "snapshot header array does not match message array",
            ));
        }
        if header.total_chunks == 0 || header.total_chunks > MAX_CHUNKS_PER_SNAPSHOT {
            self.discard_snapshot_assembly(&key);
            return Err(reject(
                &msg.array,
                header.snapshot_hlc,
                format!("snapshot header total_chunks must be 1..={MAX_CHUNKS_PER_SNAPSHOT}"),
            ));
        }

        let mut snapshots = match self.snapshots().lock() {
            Ok(g) => g,
            Err(_) => {
                error!(array = %msg.array, "array_inbound: snapshot mutex poisoned");
                return Err(None);
            }
        };
        if !snapshots.contains_key(&key) && snapshots.len() >= MAX_CONCURRENT_ASSEMBLIES {
            return Err(reject(
                &msg.array,
                header.snapshot_hlc,
                "too many concurrent snapshot assemblies",
            ));
        }
        if let Some(entry) = snapshots.get(&key) {
            if entry
                .total_chunks
                .is_some_and(|total| total != header.total_chunks)
            {
                snapshots.remove(&key);
                return Err(reject(
                    &msg.array,
                    header.snapshot_hlc,
                    "snapshot header total_chunks conflicts with buffered chunks",
                ));
            }
            if entry.chunks.len() > header.total_chunks as usize {
                snapshots.remove(&key);
                return Err(reject(
                    &msg.array,
                    header.snapshot_hlc,
                    "snapshot header has fewer chunks than already buffered",
                ));
            }
        }
        let entry = snapshots.entry(key).or_insert_with(SnapshotAssembly::new);
        entry.total_chunks = Some(header.total_chunks);
        entry.header = Some(header.clone());

        Ok(InboundOutcome::SnapshotPartial {
            received: entry.chunks.len() as u32,
            total: header.total_chunks,
        })
    }

    /// Buffer a snapshot chunk and, when complete, assemble and apply all ops.
    pub async fn handle_snapshot_chunk(
        &self,
        msg: &ArraySnapshotChunkMsg,
    ) -> Result<InboundOutcome, Option<ArrayRejectMsg>> {
        let snapshot_hlc = Hlc::from_bytes(&msg.snapshot_hlc_bytes);
        let key = (msg.array.clone(), msg.snapshot_hlc_bytes);

        // Authorization deliberately precedes every snapshot-buffer mutation,
        // including terminal-error cleanup of a partial assembly.
        let _authorized_scope = self
            .authorize_array(
                &msg.array,
                snapshot_hlc,
                crate::control::security::identity::Permission::Write,
            )?
            .into_scope();
        if msg.total_chunks == 0 || msg.total_chunks > MAX_CHUNKS_PER_SNAPSHOT {
            self.discard_snapshot_assembly(&key);
            return Err(reject(
                &msg.array,
                snapshot_hlc,
                format!("snapshot chunk total_chunks must be 1..={MAX_CHUNKS_PER_SNAPSHOT}"),
            ));
        }
        if msg.chunk_index >= msg.total_chunks {
            self.discard_snapshot_assembly(&key);
            return Err(reject(
                &msg.array,
                snapshot_hlc,
                "snapshot chunk index is outside total_chunks",
            ));
        }
        if msg.payload.len() > MAX_CHUNK_BYTES {
            self.discard_snapshot_assembly(&key);
            return Err(reject(
                &msg.array,
                snapshot_hlc,
                format!("snapshot chunk exceeds {MAX_CHUNK_BYTES} bytes"),
            ));
        }

        let assembled: Option<(SnapshotHeader, Vec<SnapshotChunk>)> = {
            let mut snapshots = match self.snapshots().lock() {
                Ok(g) => g,
                Err(_) => {
                    error!(array = %msg.array, "array_inbound: snapshot mutex poisoned (chunk)");
                    return Err(None);
                }
            };
            if !snapshots.contains_key(&key) && snapshots.len() >= MAX_CONCURRENT_ASSEMBLIES {
                return Err(reject(
                    &msg.array,
                    snapshot_hlc,
                    "too many concurrent snapshot assemblies",
                ));
            }
            let chunk = SnapshotChunk {
                array: msg.array.clone(),
                chunk_index: msg.chunk_index,
                total_chunks: msg.total_chunks,
                payload: msg.payload.clone(),
                snapshot_hlc,
            };
            if let Some(entry) = snapshots.get(&key) {
                if entry
                    .total_chunks
                    .is_some_and(|total| total != msg.total_chunks)
                {
                    snapshots.remove(&key);
                    return Err(reject(
                        &msg.array,
                        snapshot_hlc,
                        "snapshot chunk total_chunks conflicts with assembly",
                    ));
                }
                if entry.header.as_ref().is_some_and(|header| {
                    header.array != msg.array
                        || header.snapshot_hlc.to_bytes() != msg.snapshot_hlc_bytes
                        || header.total_chunks != msg.total_chunks
                }) {
                    snapshots.remove(&key);
                    return Err(reject(
                        &msg.array,
                        snapshot_hlc,
                        "snapshot chunk does not match its header",
                    ));
                }
                if entry
                    .chunks
                    .get(&msg.chunk_index)
                    .is_some_and(|existing| existing != &chunk)
                {
                    snapshots.remove(&key);
                    return Err(reject(
                        &msg.array,
                        snapshot_hlc,
                        "conflicting duplicate snapshot chunk",
                    ));
                }
            }
            let is_new_chunk = snapshots
                .get(&key)
                .is_none_or(|entry| !entry.chunks.contains_key(&msg.chunk_index));
            if is_new_chunk
                && Self::buffered_snapshot_bytes(&snapshots).saturating_add(chunk.payload.len())
                    > MAX_BUFFERED_SNAPSHOT_BYTES
            {
                snapshots.remove(&key);
                return Err(reject(
                    &msg.array,
                    snapshot_hlc,
                    "snapshot buffer byte limit exceeded",
                ));
            }

            let entry = snapshots
                .entry(key.clone())
                .or_insert_with(SnapshotAssembly::new);
            entry.total_chunks = Some(msg.total_chunks);
            if is_new_chunk {
                entry.payload_bytes += chunk.payload.len();
                entry.chunks.insert(msg.chunk_index, chunk);
            }
            let complete = entry.header.as_ref().and_then(|header| {
                (entry.chunks.len() == header.total_chunks as usize)
                    .then(|| (header.clone(), entry.chunks.values().cloned().collect()))
            });
            if complete.is_some() {
                // The assembly is terminal once complete. Remove it before
                // decoding/applying so every subsequent failure releases it.
                snapshots.remove(&key);
            }
            complete
        };

        let Some((header, mut chunks)) = assembled else {
            let snapshots = match self.snapshots().lock() {
                Ok(g) => g,
                Err(_) => return Err(None),
            };
            let received = snapshots
                .get(&key)
                .map(|e| e.chunks.len() as u32)
                .unwrap_or(0);
            return Ok(InboundOutcome::SnapshotPartial {
                received,
                total: msg.total_chunks,
            });
        };

        let snapshot = match assemble_chunks(&header, &mut chunks) {
            Ok(s) => s,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: assemble_chunks failed");
                return Err(reject(
                    &msg.array,
                    header.snapshot_hlc,
                    format!("assemble_chunks: {e}"),
                ));
            }
        };
        let ops = match op_codec::decode_op_batch(&snapshot.tile_blob) {
            Ok(ops) => ops,
            Err(e) => {
                warn!(array = %msg.array, error = %e, "array_inbound: snapshot op batch decode failed");
                return Err(reject(
                    &msg.array,
                    header.snapshot_hlc,
                    format!("decode_op_batch: {e}"),
                ));
            }
        };

        let mut ops_applied = 0;
        for op in &ops {
            let raw = match op_codec::encode_op(op) {
                Ok(bytes) => bytes,
                Err(e) => {
                    return Err(reject(
                        &msg.array,
                        header.snapshot_hlc,
                        format!("snapshot op encode: {e}"),
                    ));
                }
            };
            match self.apply_op(op.clone(), &raw, None).await {
                Ok(InboundOutcome::Applied) => ops_applied += 1,
                Ok(_) => {}
                Err(reject) => return Err(reject),
            }
        }

        Ok(InboundOutcome::SnapshotApplied { ops_applied })
    }
}
