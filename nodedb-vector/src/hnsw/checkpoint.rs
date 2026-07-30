// SPDX-License-Identifier: Apache-2.0

//! HNSW checkpoint serialization and deserialization.
//!
//! Serialization format: rkyv with a `RKHNS\0` magic header.

use std::cell::RefCell;

use crate::distance::DistanceMetric;
use crate::error::VectorError;
use crate::hnsw::arena::BeamSearchArena;
use crate::hnsw::flat_neighbors::FlatNeighborStore;
use crate::hnsw::graph::{ARENA_INITIAL_CAPACITY, HnswIndex, Node, NodeStorage, Xorshift64};

/// Magic header for rkyv-serialized HNSW snapshots (6 bytes).
const HNSW_RKYV_MAGIC: &[u8; 6] = b"RKHNS\0";
/// Current format version for rkyv-serialized HNSW snapshots.
pub const HNSW_FORMAT_VERSION: u8 = 1;

/// rkyv-serialized HNSW snapshot. SoA layout for better rkyv compatibility
/// (flat Vecs instead of Vec<struct>).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct HnswSnapshotRkyv {
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub metric: u8,
    pub entry_point: Option<u32>,
    pub max_layer: usize,
    pub rng_state: u64,
    /// Per-node vectors (SoA).
    pub node_vectors: Vec<Vec<f32>>,
    /// Per-node neighbor lists (SoA).
    pub node_neighbors: Vec<Vec<Vec<u32>>>,
    /// Per-node deleted flags (SoA).
    pub node_deleted: Vec<bool>,
}

impl HnswIndex {
    /// Serialize ONLY the graph topology (neighbors, entry point, params) to
    /// rkyv bytes.  Vector data is intentionally omitted — `node_vectors` is
    /// filled with empty `Vec<f32>` placeholders.
    ///
    /// Pair with [`Self::from_graph_checkpoint`] on restore.  The same
    /// `RKHNS\0` magic and version byte are used so `from_checkpoint` can
    /// decode the bytes, but the resulting index has empty node storage.
    ///
    /// This is the Lite pagedb-segment write path: vectors live in the segment,
    /// graph topology lives in the B+ tree.  Origin never calls this method.
    pub fn graph_checkpoint_to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        let snapshot = HnswSnapshotRkyv {
            dim: self.dim,
            m: self.params.m,
            m0: self.params.m0,
            ef_construction: self.params.ef_construction,
            metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            // Placeholder: one empty vec per node so the node count is preserved.
            node_vectors: vec![Vec::new(); self.nodes.len()],
            node_neighbors: if let Some(ref flat) = self.flat_neighbors {
                flat.to_nested(self.nodes.len())
            } else {
                self.nodes.iter().map(|n| n.neighbors.clone()).collect()
            },
            node_deleted: self.nodes.iter().map(|n| n.deleted).collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot).map_err(|e| {
            VectorError::CheckpointSerializationError {
                detail: e.to_string(),
            }
        })?;
        let mut buf = Vec::with_capacity(HNSW_RKYV_MAGIC.len() + 1 + rkyv_bytes.len());
        buf.extend_from_slice(HNSW_RKYV_MAGIC);
        buf.push(HNSW_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv_bytes);
        Ok(buf)
    }

    /// Serialize the index to rkyv bytes (with magic header) for storage.
    ///
    /// Magic header `RKHNS\0` allows `from_checkpoint` to detect format.
    pub fn checkpoint_to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        let snapshot = HnswSnapshotRkyv {
            dim: self.dim,
            m: self.params.m,
            m0: self.params.m0,
            ef_construction: self.params.ef_construction,
            metric: self.params.metric as u8,
            entry_point: self.entry_point,
            max_layer: self.max_layer,
            rng_state: self.rng.0,
            node_vectors: self.export_vectors()?,
            node_neighbors: if let Some(ref flat) = self.flat_neighbors {
                flat.to_nested(self.nodes.len())
            } else {
                self.nodes.iter().map(|n| n.neighbors.clone()).collect()
            },
            node_deleted: self.nodes.iter().map(|n| n.deleted).collect(),
        };
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot).map_err(|e| {
            VectorError::CheckpointSerializationError {
                detail: e.to_string(),
            }
        })?;
        let mut buf = Vec::with_capacity(HNSW_RKYV_MAGIC.len() + 1 + rkyv_bytes.len());
        buf.extend_from_slice(HNSW_RKYV_MAGIC);
        buf.push(HNSW_FORMAT_VERSION);
        buf.extend_from_slice(&rkyv_bytes);
        Ok(buf)
    }

    /// Restore an index from a checkpoint snapshot.
    ///
    /// Returns:
    /// - `Ok(Some(index))` — successfully decoded (rkyv format).
    /// - `Ok(None)` — bytes do not start with the `RKHNS\0` magic header.
    /// - `Err(VectorError::UnsupportedVersion)` — magic matches `RKHNS\0` but
    ///   the version byte is not `HNSW_FORMAT_VERSION`; the caller must reject
    ///   the buffer.
    /// - `Err(VectorError::CheckpointDeserializationError)` — magic and
    ///   version matched but the rkyv payload itself failed to decode
    ///   (structurally corrupt checkpoint).
    pub fn from_checkpoint(bytes: &[u8]) -> Result<Option<Self>, crate::error::VectorError> {
        let header_len = HNSW_RKYV_MAGIC.len() + 1; // magic + version byte
        if bytes.len() > header_len && &bytes[..HNSW_RKYV_MAGIC.len()] == HNSW_RKYV_MAGIC {
            let version = bytes[HNSW_RKYV_MAGIC.len()];
            if version != HNSW_FORMAT_VERSION {
                return Err(crate::error::VectorError::UnsupportedVersion {
                    found: version,
                    expected: HNSW_FORMAT_VERSION,
                });
            }
            return Ok(Some(Self::from_rkyv_checkpoint(&bytes[header_len..])?));
        }
        // No recognized magic prefix — no index to restore.
        Ok(None)
    }

    /// Restore from rkyv-serialized bytes.
    fn from_rkyv_checkpoint(bytes: &[u8]) -> Result<Self, VectorError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let snap: HnswSnapshotRkyv =
            rkyv::from_bytes::<HnswSnapshotRkyv, rkyv::rancor::Error>(&aligned).map_err(|e| {
                VectorError::CheckpointDeserializationError {
                    detail: format!("hnsw rkyv decode: {e}"),
                }
            })?;
        Self::from_hnsw_snapshot(snap)
    }

    /// Reconstruct HnswIndex from deserialized snapshot fields.
    fn from_hnsw_snapshot(snap: HnswSnapshotRkyv) -> Result<Self, VectorError> {
        use nodedb_types::hnsw::HnswParams;

        let metric = match snap.metric {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };

        let flat = FlatNeighborStore::from_nested(&snap.node_neighbors);

        let nodes: Vec<Node> = snap
            .node_vectors
            .into_iter()
            .zip(snap.node_deleted)
            .map(|(vector, deleted)| Node {
                storage: NodeStorage::F32(vector),
                neighbors: Vec::new(),
                deleted,
            })
            .collect();

        let initial_capacity = snap.ef_construction.max(ARENA_INITIAL_CAPACITY);
        Ok(Self {
            dim: snap.dim,
            params: HnswParams {
                m: snap.m,
                m0: snap.m0,
                ef_construction: snap.ef_construction,
                metric,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
            nodes,
            entry_point: snap.entry_point,
            max_layer: snap.max_layer,
            rng: Xorshift64::new(snap.rng_state),
            flat_neighbors: Some(flat),
            arena: RefCell::new(BeamSearchArena::new(initial_capacity)),
            #[cfg(not(target_arch = "wasm32"))]
            backing: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::distance::DistanceMetric;
    use crate::hnsw::{HnswIndex, HnswParams};

    fn make_index() -> HnswIndex {
        HnswIndex::with_seed(
            3,
            HnswParams {
                m: 4,
                m0: 8,
                ef_construction: 32,
                metric: DistanceMetric::L2,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            },
            12345,
        )
    }

    #[test]
    fn checkpoint_roundtrip() {
        let mut idx = make_index();
        for i in 0..50 {
            idx.insert(vec![(i as f32) * 0.1, (i as f32) * 0.2, (i as f32) * 0.3])
                .unwrap();
        }

        let bytes = idx.checkpoint_to_bytes().unwrap();
        assert!(!bytes.is_empty());

        let restored = HnswIndex::from_checkpoint(&bytes).unwrap().unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.dim(), 3);
        assert_eq!(restored.entry_point(), idx.entry_point());
        assert_eq!(restored.max_layer(), idx.max_layer());

        let query = vec![1.0, 2.0, 3.0];
        let orig_results = idx.search(&query, 5, 32);
        let rest_results = restored.search(&query, 5, 32);
        assert_eq!(orig_results.len(), rest_results.len());
        for (a, b) in orig_results.iter().zip(rest_results.iter()) {
            assert_eq!(a.id, b.id);
            assert!((a.distance - b.distance).abs() < 1e-6);
        }
    }

    #[test]
    fn golden_header_layout() {
        let mut idx = make_index();
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let bytes = idx.checkpoint_to_bytes().unwrap();
        // Magic at bytes[0..6].
        assert_eq!(&bytes[0..6], b"RKHNS\0");
        // Version byte at bytes[6].
        assert_eq!(bytes[6], super::HNSW_FORMAT_VERSION);
        // rkyv payload follows immediately.
        assert!(bytes.len() > 7);
    }

    #[test]
    fn version_mismatch_returns_error() {
        let mut idx = make_index();
        idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        let mut bytes = idx.checkpoint_to_bytes().unwrap();
        // Corrupt the version byte to an unsupported value.
        bytes[6] = 0;
        match HnswIndex::from_checkpoint(&bytes) {
            Err(crate::error::VectorError::UnsupportedVersion { found, expected }) => {
                assert_eq!(found, 0);
                assert_eq!(expected, super::HNSW_FORMAT_VERSION);
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion error, got Ok"),
        }
    }

    #[test]
    fn corrupt_rkyv_payload_returns_deserialization_error() {
        // Valid magic + valid version byte, but the rkyv payload itself is
        // garbage. This must surface as a typed decode error, not silently
        // collapse to `Ok(None)` (which would look identical to "no
        // checkpoint present" and drop the restore on the floor).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RKHNS\0");
        bytes.push(super::HNSW_FORMAT_VERSION);
        bytes.extend_from_slice(&[0xFFu8; 64]);

        match HnswIndex::from_checkpoint(&bytes) {
            Err(crate::error::VectorError::CheckpointDeserializationError { .. }) => {}
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected CheckpointDeserializationError, got Ok"),
        }
    }
}
