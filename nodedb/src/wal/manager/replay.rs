// SPDX-License-Identifier: BUSL-1.1

use nodedb_wal::WalRecord;
use tracing::info;

use super::core::WalManager;
use crate::types::Lsn;

impl WalManager {
    /// Validate each WAL segment for startup integrity.
    ///
    /// Returns `Err` if any non-empty segment contains no valid WAL records —
    /// a reliable signal that the segment was corrupted (wrong magic, truncated
    /// header, etc.) rather than simply rolled over empty.
    ///
    /// This check is intentionally strict: a segment file with content that
    /// does not parse as WAL records is treated as fatal corruption, not as an
    /// empty WAL. The WAL replay path is lenient (stops at the first invalid
    /// record) — this method is the complementary hard check run at startup.
    pub fn validate_for_startup(&self) -> crate::Result<()> {
        let segments =
            nodedb_wal::segment::discover_segments(&self.wal_dir).map_err(crate::Error::Wal)?;

        for seg in &segments {
            let file_len = std::fs::metadata(&seg.path).map(|m| m.len()).unwrap_or(0);

            if file_len == 0 {
                continue;
            }
            if file_len == nodedb_wal::preamble::PREAMBLE_SIZE as u64 {
                use std::io::Read as _;
                let mut bytes = [0u8; nodedb_wal::preamble::PREAMBLE_SIZE];
                let mut file = std::fs::File::open(&seg.path).map_err(|error| {
                    crate::Error::SegmentCorrupted {
                        detail: format!("read WAL preamble '{}': {error}", seg.path.display()),
                    }
                })?;
                file.read_exact(&mut bytes)
                    .map_err(|error| crate::Error::SegmentCorrupted {
                        detail: format!("read WAL preamble '{}': {error}", seg.path.display()),
                    })?;
                nodedb_wal::preamble::SegmentPreamble::from_bytes(
                    &bytes,
                    &nodedb_wal::preamble::WAL_PREAMBLE_MAGIC,
                )
                .map_err(crate::Error::Wal)?;
                continue;
            }

            let info = nodedb_wal::recovery::recover(&seg.path).map_err(crate::Error::Wal)?;

            if info.end_offset == 0 {
                return Err(crate::Error::SegmentCorrupted {
                    detail: format!(
                        "WAL segment '{}' is non-empty ({file_len} bytes) but contains no valid \
                         WAL records — the segment appears to be corrupted",
                        seg.path.display()
                    ),
                });
            }
        }

        Ok(())
    }

    /// Replay all committed records from the WAL.
    ///
    /// Payloads come back as plaintext: the manager's key ring is handed to the
    /// replay driver, which decrypts each record inside the WAL layer. Every
    /// record type is encrypted when a key is configured, so replaying without
    /// the ring would hand ciphertext to every engine's decoder.
    pub fn replay(&self) -> crate::Result<Vec<WalRecord>> {
        let records = nodedb_wal::segmented::replay_all_segments(
            &self.wal_dir,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)?;
        info!(records = records.len(), "WAL replay complete");
        Ok(records)
    }

    /// Replay committed records from the WAL starting at `from_lsn`.
    pub fn replay_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let records = wal
            .replay_from(from_lsn.as_u64())
            .map_err(crate::Error::Wal)?;
        Ok(records)
    }

    /// Replay WAL records from `from_lsn` using mmap (tier-2 catchup).
    pub fn replay_mmap_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let records = nodedb_wal::mmap_reader::replay_segments_mmap(
            self.wal_dir(),
            from_lsn.as_u64(),
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)?;
        Ok(records)
    }

    /// Paginated mmap replay: reads at most `max_records` from `from_lsn`.
    ///
    /// **Note:** Uses mmap, which cannot see data written via O_DIRECT to the
    /// active segment. Use `replay_from_limit` for the catch-up task instead.
    pub fn replay_mmap_from_limit(
        &self,
        from_lsn: Lsn,
        max_records: usize,
    ) -> crate::Result<(Vec<WalRecord>, bool)> {
        nodedb_wal::mmap_reader::replay_segments_mmap_limit(
            self.wal_dir(),
            from_lsn.as_u64(),
            max_records,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)
    }

    /// Paginated sequential replay: reads at most `max_records` from `from_lsn`.
    pub fn replay_from_limit(
        &self,
        from_lsn: Lsn,
        max_records: usize,
    ) -> crate::Result<(Vec<WalRecord>, bool)> {
        nodedb_wal::segmented::replay_from_limit_dir(
            self.wal_dir(),
            from_lsn.as_u64(),
            max_records,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)
    }
}
