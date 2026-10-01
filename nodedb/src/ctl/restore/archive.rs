// SPDX-License-Identifier: BUSL-1.1

//! The WAL archive of one node life, read offline.

use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

use super::error::RestoreError;
use crate::storage::cold::ColdStorage;
use crate::wal::archiver::wal_archive_segment_key;

/// One archived segment: its object and every checksum marker beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedSegment {
    pub first_lsn: u64,
    pub size: u64,
    pub crc32c: Vec<u32>,
}

/// A fetched segment image whose checksum matched a marker.
#[derive(Debug)]
pub struct Fetched {
    pub key: String,
    pub bytes: Vec<u8>,
    pub crc32c: u32,
}

/// The archived segments of one node life, in LSN order.
///
/// A segment object without a checksum marker never counts as archived: the
/// archiver writes the marker after the object, so the upload can be
/// unfinished. Such a segment is left out, and a restore that needs it sees a
/// gap.
pub struct Archive<'a> {
    cold: &'a ColdStorage,
    node_id: u64,
    incarnation: String,
    segments: Vec<ArchivedSegment>,
}

impl<'a> Archive<'a> {
    pub async fn list(
        cold: &'a ColdStorage,
        node_id: u64,
        incarnation: &str,
    ) -> Result<Self, RestoreError> {
        let listed = cold.archived_wal_segments(node_id, incarnation, 0).await?;
        let mut segments: Vec<ArchivedSegment> = listed
            .into_iter()
            .filter_map(|(first_lsn, remote)| {
                let size = remote.size?;
                (!remote.crc32c.is_empty()).then_some(ArchivedSegment {
                    first_lsn,
                    size,
                    crc32c: remote.crc32c,
                })
            })
            .collect();
        segments.sort_by_key(|seg| seg.first_lsn);
        Ok(Self {
            cold,
            node_id,
            incarnation: incarnation.to_string(),
            segments,
        })
    }

    pub fn segments(&self) -> &[ArchivedSegment] {
        &self.segments
    }

    /// Index of the segment whose range holds `lsn`: the last one starting at
    /// or below it.
    pub fn containing(&self, lsn: u64) -> Option<usize> {
        self.segments
            .partition_point(|seg| seg.first_lsn <= lsn)
            .checked_sub(1)
    }

    /// The object key of the segment at `first_lsn`.
    pub fn key(&self, first_lsn: u64) -> String {
        wal_archive_segment_key(
            self.cold.prefix(),
            self.node_id,
            &self.incarnation,
            first_lsn,
        )
    }

    /// Fetch the segment at `index` and check its size and checksum.
    pub async fn fetch(&self, index: usize) -> Result<Fetched, RestoreError> {
        let segment = self
            .segments
            .get(index)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "archived WAL segment index {index} is past the {} listed",
                    self.segments.len()
                ),
            })?;
        let key = self.key(segment.first_lsn);
        let location = ObjectPath::from(key.as_str());
        let storage_err = |e: object_store::Error| crate::Error::ColdStorage {
            detail: format!("fetch archived WAL segment {key}: {e}"),
        };
        let raw = self
            .cold
            .object_store()
            .get(&location)
            .await
            .map_err(storage_err)?
            .bytes()
            .await
            .map_err(storage_err)?;
        let bytes = Vec::from(raw);
        if bytes.len() as u64 != segment.size {
            return Err(RestoreError::SizeMismatch {
                key,
                actual: bytes.len() as u64,
                listed: segment.size,
            });
        }
        let crc32c = crc32c::crc32c(&bytes);
        if !segment.crc32c.contains(&crc32c) {
            return Err(RestoreError::ChecksumMismatch {
                key,
                actual: crc32c,
                expected: segment.crc32c.clone(),
            });
        }
        Ok(Fetched { key, bytes, crc32c })
    }
}

#[cfg(test)]
mod tests {
    use object_store::PutPayload;

    use super::*;
    use crate::storage::cold::ColdStorageConfig;

    fn cold(dir: &std::path::Path) -> ColdStorage {
        ColdStorage::new(ColdStorageConfig {
            local_dir: Some(dir.to_path_buf()),
            ..Default::default()
        })
        .unwrap()
    }

    async fn put(cold: &ColdStorage, key: &str, bytes: &[u8]) {
        cold.object_store()
            .put(&ObjectPath::from(key), PutPayload::from(bytes.to_vec()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_segment_without_a_marker_is_not_archived() {
        let dir = tempfile::tempdir().unwrap();
        let cold = cold(dir.path());
        let seg = dir.path().join("seg");
        for (first, bytes) in [(1u64, b"one".as_slice()), (11, b"two".as_slice())] {
            std::fs::write(&seg, bytes).unwrap();
            cold.upload_wal_segment(&seg, 4, "inc", first, &[])
                .await
                .unwrap();
        }
        put(
            &cold,
            &wal_archive_segment_key(cold.prefix(), 4, "inc", 21),
            b"three",
        )
        .await;

        let archive = Archive::list(&cold, 4, "inc").await.unwrap();
        let firsts: Vec<u64> = archive.segments().iter().map(|s| s.first_lsn).collect();
        assert_eq!(firsts, [1, 11]);
        assert_eq!(archive.containing(0), None);
        assert_eq!(archive.containing(1), Some(0));
        assert_eq!(archive.containing(15), Some(1));
        assert_eq!(archive.containing(99), Some(1));
        assert_eq!(archive.fetch(1).await.unwrap().bytes, b"two");
    }

    #[tokio::test]
    async fn a_segment_that_matches_no_marker_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let cold = cold(dir.path());
        let seg = dir.path().join("seg");
        std::fs::write(&seg, b"original").unwrap();
        cold.upload_wal_segment(&seg, 4, "inc", 1, &[])
            .await
            .unwrap();
        put(
            &cold,
            &wal_archive_segment_key(cold.prefix(), 4, "inc", 1),
            b"tampered",
        )
        .await;

        let archive = Archive::list(&cold, 4, "inc").await.unwrap();
        assert!(matches!(
            archive.fetch(0).await,
            Err(RestoreError::ChecksumMismatch { .. })
        ));
    }
}
