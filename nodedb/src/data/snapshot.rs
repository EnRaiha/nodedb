// SPDX-License-Identifier: BUSL-1.1

//! Physical snapshot format.
//!
//! A [`CoreSnapshot`] is the image of one Data Plane core's durable files,
//! taken right after a forced checkpoint. A [`NodeSnapshot`] is the image of
//! the node-level stores: the catalogs, the Event Plane stores, the array
//! sync stores, and the WAL-wrapped CRDT signing root. Restore writes every file back to the same path under an
//! empty data directory, and the normal boot path opens it.

use crate::types::replay_stamp::ReplayStamp;

/// The store a snapshot file belongs to.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum SnapshotComponent {
    /// `system.redb`: the node's system catalog.
    SystemCatalog = 0,
    /// `cluster.redb`: the node's cluster catalog.
    ClusterCatalog = 1,
    /// The core's sparse redb store: documents, secondary indexes, full-text
    /// postings, column statistics, and hash-chain heads.
    Sparse = 2,
    /// The core's graph edge redb store.
    Graph = 3,
    Kv = 4,
    SparseVector = 5,
    SyncHwm = 6,
    Columnar = 7,
    GraphLabel = 8,
    Array = 9,
    Timeseries = 10,
    Vector = 11,
    Crdt = 12,
    Spatial = 13,
    /// A durable Event Plane redb store under `event_plane/`.
    EventPlane = 14,
    /// A durable array sync redb store under `array_sync/`.
    ArraySync = 15,
    /// `wal/crdt_signing_root.enc`: the CRDT signing root wrapped by the WAL
    /// key. `system.redb` holds its fingerprint, and boot refuses a root
    /// that does not match.
    WalKeys = 16,
}

impl SnapshotComponent {
    /// Whether the component is node-level rather than owned by one core.
    pub fn is_node_level(self) -> bool {
        matches!(
            self,
            Self::SystemCatalog
                | Self::ClusterCatalog
                | Self::EventPlane
                | Self::ArraySync
                | Self::WalKeys
        )
    }
}

/// One captured file.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotFile {
    pub component: SnapshotComponent,
    /// Path relative to the data directory, `/`-separated.
    pub path: String,
    pub bytes: Vec<u8>,
}

/// One captured directory that boot requires to exist, even with no file in
/// it. A checkpoint generation with nothing to hold is such a directory: its
/// MANIFEST names it, and the loader lists it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotDir {
    pub component: SnapshotComponent,
    /// Path relative to the data directory, `/`-separated.
    pub path: String,
}

/// The physical image of one Data Plane core.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CoreSnapshot {
    /// The records the captured files hold: every record at or below
    /// `stamp.prefix` that this core applied, plus the ranges above it.
    pub stamp: ReplayStamp,
    pub files: Vec<SnapshotFile>,
    /// Directories restore creates beside `files`: every live checkpoint
    /// generation directory, so an empty one comes back too.
    pub dirs: Vec<SnapshotDir>,
}

impl CoreSnapshot {
    pub fn empty() -> Self {
        Self {
            stamp: ReplayStamp::default(),
            files: Vec::new(),
            dirs: Vec::new(),
        }
    }

    /// The lowest LSN the WAL must keep above to bring this core forward.
    pub fn replay_floor(&self) -> u64 {
        self.stamp.prefix
    }

    /// The highest LSN whose effect the captured files include.
    pub fn applied_high_lsn(&self) -> u64 {
        self.stamp.highest()
    }

    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        encode("CoreSnapshot", self)
    }

    pub fn from_bytes(data: &[u8]) -> crate::Result<Self> {
        decode("CoreSnapshot", data)
    }

    /// Total captured file bytes.
    pub fn approx_size(&self) -> usize {
        self.files
            .iter()
            .map(|f| f.path.len() + f.bytes.len())
            .sum()
    }
}

/// The physical image of the node-level redb stores.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NodeSnapshot {
    pub files: Vec<SnapshotFile>,
    /// The metadata group's applied index when capture began, `0` with no
    /// cluster. The catalog images hold every entry at or below it, so a
    /// cluster restore replays the metadata log from the entry above it.
    pub metadata_applied_index: u64,
    /// The metadata group's applied index when capture ended. The catalog
    /// images hold no entry above it.
    pub metadata_captured_index: u64,
    /// The metadata timeline the catalog images belong to (see
    /// `storage::metadata_timeline`).
    pub metadata_timeline: u64,
}

impl NodeSnapshot {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        encode("NodeSnapshot", self)
    }

    pub fn from_bytes(data: &[u8]) -> crate::Result<Self> {
        decode("NodeSnapshot", data)
    }
}

fn encode<T: zerompk::ToMessagePack>(what: &str, value: &T) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("{what} encode: {e}"),
    })
}

fn decode<T: for<'a> zerompk::FromMessagePack<'a>>(what: &str, data: &[u8]) -> crate::Result<T> {
    zerompk::from_msgpack(data).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("{what} decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::replay_stamp::LsnRange;

    #[test]
    fn empty_snapshot_roundtrip() {
        let bytes = CoreSnapshot::empty().to_bytes().unwrap();
        let decoded = CoreSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, CoreSnapshot::empty());
        assert_eq!(decoded.replay_floor(), 0);
        assert_eq!(decoded.applied_high_lsn(), 0);
    }

    #[test]
    fn snapshot_with_files_roundtrip() {
        let snap = CoreSnapshot {
            stamp: ReplayStamp {
                prefix: 40,
                applied_above: vec![LsnRange { start: 42, end: 45 }],
            },
            files: vec![
                SnapshotFile {
                    component: SnapshotComponent::Sparse,
                    path: "sparse/core-0.redb".into(),
                    bytes: vec![1, 2, 3],
                },
                SnapshotFile {
                    component: SnapshotComponent::Kv,
                    path: "kv-ckpt/core-0/MANIFEST".into(),
                    bytes: vec![4],
                },
            ],
            dirs: vec![SnapshotDir {
                component: SnapshotComponent::Kv,
                path: "kv-ckpt/core-0/gen-0".into(),
            }],
        };
        let decoded = CoreSnapshot::from_bytes(&snap.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, snap);
        assert_eq!(decoded.replay_floor(), 40);
        assert_eq!(decoded.applied_high_lsn(), 45);
        assert!(decoded.approx_size() > 0);
    }

    #[test]
    fn garbage_is_a_decode_error() {
        assert!(CoreSnapshot::from_bytes(&[0xC1, 0xFF]).is_err());
        assert!(NodeSnapshot::from_bytes(&[0xC1, 0xFF]).is_err());
    }
}
