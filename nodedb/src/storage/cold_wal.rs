// SPDX-License-Identifier: BUSL-1.1

//! WAL archive operations on cold storage: segment upload, checksum markers,
//! and the archive listing recovery reads.

use std::path::Path;

use object_store::{ObjectStore, PutPayload};
use tracing::info;

use super::cold::ColdStorage;

impl ColdStorage {
    /// Upload a sealed WAL segment to its archive key, then its checksum
    /// marker, then delete every marker in `stale_crc32c` that differs from
    /// the new one. A segment without its marker never counts as archived.
    ///
    /// Returns the segment object key.
    pub async fn upload_wal_segment(
        &self,
        segment_path: &Path,
        node_id: u64,
        incarnation: &str,
        first_lsn: u64,
        stale_crc32c: &[u32],
    ) -> crate::Result<String> {
        use crate::wal::archiver::{wal_archive_checksum_key, wal_archive_segment_key};

        let path_buf = segment_path.to_path_buf();
        let segment_display = segment_path.display().to_string();
        let data = tokio::task::spawn_blocking(move || std::fs::read(&path_buf))
            .await
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("spawn_blocking join: {e}"),
            })?
            .map_err(|e| crate::Error::ColdStorage {
                detail: format!("read WAL segment {segment_display}: {e}"),
            })?;
        let crc32c = crc32c::crc32c(&data);

        let prefix = self.prefix();
        let object_path = wal_archive_segment_key(prefix, node_id, incarnation, first_lsn);
        let marker_path = wal_archive_checksum_key(prefix, node_id, incarnation, first_lsn, crc32c);
        for (key, payload) in [
            (&object_path, PutPayload::from(data)),
            (&marker_path, PutPayload::default()),
        ] {
            self.store()
                .put_opts(
                    &object_store::path::Path::from(key.as_str()),
                    payload,
                    object_store::PutOptions::default(),
                )
                .await
                .map_err(|e| crate::Error::ColdStorage {
                    detail: format!("upload WAL archive object {key}: {e}"),
                })?;
        }
        let stale: Vec<u32> = stale_crc32c
            .iter()
            .copied()
            .filter(|stale| *stale != crc32c)
            .collect();
        self.delete_wal_checksum_markers(node_id, incarnation, first_lsn, &stale)
            .await?;

        info!(node_id, first_lsn, crc32c, path = %object_path, "WAL segment archived to cold storage");
        Ok(object_path)
    }

    /// Delete the checksum markers `crc32c` of the segment at `first_lsn`.
    /// A marker already gone counts as deleted.
    pub async fn delete_wal_checksum_markers(
        &self,
        node_id: u64,
        incarnation: &str,
        first_lsn: u64,
        crc32c: &[u32],
    ) -> crate::Result<()> {
        use object_store::ObjectStoreExt;

        let prefix = self.prefix();
        for crc in crc32c {
            let key = crate::wal::archiver::wal_archive_checksum_key(
                prefix,
                node_id,
                incarnation,
                first_lsn,
                *crc,
            );
            let location = object_store::path::Path::from(key.as_str());
            match self.store().delete(&location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => {
                    return Err(crate::Error::ColdStorage {
                        detail: format!("delete stale WAL checksum marker {key}: {e}"),
                    });
                }
            }
        }
        Ok(())
    }

    /// Delete the archived segment at `first_lsn`, then its checksum markers
    /// `crc32c`. An object already gone counts as deleted.
    pub async fn delete_archived_wal_segment(
        &self,
        node_id: u64,
        incarnation: &str,
        first_lsn: u64,
        crc32c: &[u32],
    ) -> crate::Result<()> {
        use object_store::ObjectStoreExt;

        let key = crate::wal::archiver::wal_archive_segment_key(
            self.prefix(),
            node_id,
            incarnation,
            first_lsn,
        );
        let location = object_store::path::Path::from(key.as_str());
        match self.store().delete(&location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => {
                return Err(crate::Error::ColdStorage {
                    detail: format!("delete archived WAL segment {key}: {e}"),
                });
            }
        }
        self.delete_wal_checksum_markers(node_id, incarnation, first_lsn, crc32c)
            .await
    }

    /// What the archive of one node life holds for every
    /// `first_lsn >= from_first_lsn`: object size and checksum markers.
    pub async fn archived_wal_segments(
        &self,
        node_id: u64,
        incarnation: &str,
        from_first_lsn: u64,
    ) -> crate::Result<std::collections::HashMap<u64, crate::wal::archiver::RemoteSegment>> {
        use crate::wal::archiver::{
            ArchivedObject, RemoteSegment, parse_wal_archive_filename, wal_archive_node_prefix,
        };
        use futures::TryStreamExt;

        let node_prefix = wal_archive_node_prefix(self.prefix(), node_id, incarnation);
        let prefix = object_store::path::Path::from(node_prefix.clone());
        // The offset is exclusive. The segment name without its extension
        // sorts directly below `wal-{from_first_lsn:020}.seg` and its markers.
        let offset =
            object_store::path::Path::from(format!("{node_prefix}wal-{from_first_lsn:020}"));
        let list_err = |e: object_store::Error| crate::Error::ColdStorage {
            detail: format!("list archived WAL segments under {node_prefix}: {e}"),
        };

        let mut archived: std::collections::HashMap<u64, RemoteSegment> =
            std::collections::HashMap::new();
        let mut stream = self.store().list_with_offset(Some(&prefix), &offset);
        while let Some(meta) = stream.try_next().await.map_err(list_err)? {
            match meta
                .location
                .filename()
                .and_then(parse_wal_archive_filename)
            {
                Some(ArchivedObject::Segment { first_lsn }) => {
                    archived.entry(first_lsn).or_default().size = Some(meta.size);
                }
                Some(ArchivedObject::Checksum { first_lsn, crc32c }) => {
                    archived.entry(first_lsn).or_default().crc32c.push(crc32c);
                }
                None => {}
            }
        }
        Ok(archived)
    }
}
