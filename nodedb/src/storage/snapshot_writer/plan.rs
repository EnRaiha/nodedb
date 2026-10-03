// SPDX-License-Identifier: BUSL-1.1

//! A base snapshot ready to write: images checked, LSN bounds read, and every
//! image cut into content-addressed chunks.
//!
//! Planning needs no store. The caller learns every chunk id the base will
//! list before any store I/O, and pins them against garbage collection.

use std::collections::HashSet;

use super::cdc::CdcParams;
use super::chunk_id::ChunkKeyer;
use super::chunks::{ImageChunks, cut_image};
use crate::data::snapshot::{CoreSnapshot, NodeSnapshot};

/// Per-core LSN bounds read from the encoded core snapshots.
#[derive(Debug, Clone)]
pub(super) struct CoreBounds {
    pub lowest_floor: u64,
    pub highest_floor: u64,
    pub applied_high: u64,
    pub data_bytes: u64,
}

impl CoreBounds {
    fn read(core_snapshots: &[(usize, Vec<u8>)]) -> crate::Result<Self> {
        let mut bounds = Self {
            lowest_floor: u64::MAX,
            highest_floor: 0,
            applied_high: 0,
            data_bytes: 0,
        };
        for (core_id, bytes) in core_snapshots {
            let snapshot =
                CoreSnapshot::from_bytes(bytes).map_err(|e| crate::Error::BadRequest {
                    detail: format!("core {core_id} snapshot does not decode: {e}"),
                })?;
            let floor = snapshot.replay_floor();
            bounds.lowest_floor = bounds.lowest_floor.min(floor);
            bounds.highest_floor = bounds.highest_floor.max(floor);
            bounds.applied_high = bounds.applied_high.max(snapshot.applied_high_lsn());
            bounds.data_bytes += bytes.len() as u64;
        }
        Ok(bounds)
    }
}

/// A checked base with every image cut into chunks.
#[derive(Debug)]
pub struct BasePlan {
    /// One encoded image per core, ordered by core id from zero.
    pub(super) cores: Vec<Vec<u8>>,
    pub(super) node: Vec<u8>,
    pub(super) bounds: CoreBounds,
    pub(super) core_chunks: Vec<ImageChunks>,
    pub(super) node_chunks: ImageChunks,
    pub(super) cold_segments: Vec<String>,
    pub(super) metadata_applied_index: u64,
    pub(super) metadata_captured_index: u64,
    pub(super) metadata_timeline: u64,
}

impl BasePlan {
    /// Check the images and cut them into chunks.
    ///
    /// `core_snapshots` holds one `(core_id, encoded CoreSnapshot)` per core,
    /// with ids unique and contiguous from zero in any order. `node` holds
    /// only node-level components. This is CPU work over every image byte, so
    /// an async caller runs it on a blocking thread.
    pub fn new(
        mut core_snapshots: Vec<(usize, Vec<u8>)>,
        node: &NodeSnapshot,
        cold_segments: Vec<String>,
        key: &nodedb_wal::crypto::WalEncryptionKey,
        params: CdcParams,
    ) -> crate::Result<Self> {
        params.validate()?;
        if core_snapshots.is_empty() {
            return Err(crate::Error::BadRequest {
                detail: "no core snapshots provided".into(),
            });
        }
        core_snapshots.sort_unstable_by_key(|(core_id, _)| *core_id);
        if core_snapshots
            .iter()
            .enumerate()
            .any(|(expected, (core_id, _))| *core_id != expected)
        {
            return Err(crate::Error::BadRequest {
                detail: "snapshot core IDs must be unique and contiguous from zero".into(),
            });
        }
        if let Some(file) = node.files.iter().find(|f| !f.component.is_node_level()) {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "node image holds {:?} file {:?}; only node-level components belong there",
                    file.component, file.path
                ),
            });
        }

        let bounds = CoreBounds::read(&core_snapshots)?;
        let metadata_applied_index = node.metadata_applied_index;
        let metadata_captured_index = node.metadata_captured_index;
        let metadata_timeline = node.metadata_timeline;
        let node = node.to_bytes()?;
        let keyer = ChunkKeyer::new(key)?;
        let cores: Vec<Vec<u8>> = core_snapshots.into_iter().map(|(_, bytes)| bytes).collect();
        let core_chunks = cores
            .iter()
            .map(|image| cut_image(image, params, &keyer))
            .collect::<crate::Result<Vec<_>>>()?;
        let node_chunks = cut_image(&node, params, &keyer)?;
        Ok(Self {
            cores,
            node,
            bounds,
            core_chunks,
            node_chunks,
            cold_segments,
            metadata_applied_index,
            metadata_captured_index,
            metadata_timeline,
        })
    }

    /// Every chunk id the base lists, each once.
    pub fn chunk_ids(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.core_chunks
            .iter()
            .chain(std::iter::once(&self.node_chunks))
            .flat_map(|image| image.refs.iter())
            .filter(|chunk| seen.insert(chunk.id.as_str()))
            .map(|chunk| chunk.id.clone())
            .collect()
    }

    /// The cold-tier keys the base references.
    pub fn cold_segments(&self) -> &[String] {
        &self.cold_segments
    }
}
