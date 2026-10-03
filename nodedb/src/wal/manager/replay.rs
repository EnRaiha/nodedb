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

    /// Drop every record named by a `WriteAborted` marker in the same stream.
    ///
    /// A forward write record is appended before the Data Plane decides whether
    /// to accept the write, so a refusal always arrives with the record already
    /// in the log. Every replay stream this manager hands out is filtered here,
    /// at its source, rather than re-checked inside each engine's replay arm:
    /// the predicate is the record header's LSN and nothing else, so a per-arm
    /// gate would be dozens of identical checks and the first one forgotten
    /// silently resurrects that engine's refused writes.
    ///
    /// Requires the whole stream in hand — the abort marker is always at a
    /// HIGHER LSN than the record it names, so a streaming filter could not see
    /// it in time. The paginated `replay_*_limit` readers below therefore
    /// cannot use this and gate at their own call site.
    fn without_aborted_writes(records: Vec<WalRecord>) -> crate::Result<Vec<WalRecord>> {
        let filters = nodedb_wal::extract_replay_filters(&records).map_err(crate::Error::Wal)?;
        if filters.aborted.is_empty() {
            return Ok(records);
        }
        let before = records.len();
        let kept = nodedb_wal::drop_aborted_records(records, &filters.aborted);
        info!(
            dropped = before - kept.len(),
            "WAL replay excluded records for writes the engine refused"
        );
        Ok(kept)
    }

    /// Record every time anchor in `records` and drop the anchors from the
    /// stream. Anchors are commit-time metadata, and no engine replays them.
    ///
    /// The paginated readers keep anchors in their pages: a page of only
    /// anchors otherwise reads as empty while `has_more` is set.
    fn without_time_anchors(&self, mut records: Vec<WalRecord>) -> crate::Result<Vec<WalRecord>> {
        self.time_anchors
            .absorb_replayed(&records)
            .map_err(crate::Error::Wal)?;
        records.retain(|record| {
            nodedb_wal::record::RecordType::from_raw(record.logical_record_type())
                != Some(nodedb_wal::record::RecordType::TimeAnchor)
        });
        Ok(records)
    }

    /// Anchor every record recovered at open that no persisted anchor covers.
    ///
    /// Boot calls this once after [`Self::replay`] and before any append, so
    /// the WAL frontier is the last recovered LSN.
    pub fn anchor_recovered_tail(&self) {
        let last_recovered = self.next_lsn().as_u64().saturating_sub(1);
        self.time_anchors.cover_recovered(last_recovered);
    }

    /// Replay all committed records from the WAL.
    ///
    /// Payloads come back as plaintext: the manager's key ring is handed to the
    /// replay driver, which decrypts each record inside the WAL layer. Every
    /// record type is encrypted when a key is configured, so replaying without
    /// the ring would hand ciphertext to every engine's decoder.
    ///
    /// Records naming a refused write are excluded — see
    /// [`Self::without_aborted_writes`].
    pub fn replay(&self) -> crate::Result<Vec<WalRecord>> {
        let records = nodedb_wal::segmented::replay_all_segments(
            &self.wal_dir,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)?;
        let records = self.without_time_anchors(records)?;
        let records = Self::without_aborted_writes(records)?;
        info!(records = records.len(), "WAL replay complete");
        Ok(records)
    }

    /// Replay committed records from the WAL starting at `from_lsn`.
    ///
    /// A `from_lsn` below the earliest LSN the WAL still retains fails with
    /// [`nodedb_wal::WalError::ReplayBelowRetainedFloor`] rather than returning
    /// the shorter suffix that survived truncation. A caller recovering from a
    /// persisted position must treat that as unrecoverable: the records it is
    /// asking for are gone, and a short answer is indistinguishable from a
    /// complete one. The same applies to [`Self::replay_mmap_from`],
    /// [`Self::replay_from_limit`], and [`Self::replay_mmap_from_limit`].
    pub fn replay_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let records = {
            let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.replay_from(from_lsn.as_u64())
                .map_err(crate::Error::Wal)?
        };
        let records = self.without_time_anchors(records)?;
        Self::without_aborted_writes(records)
    }

    /// Replay WAL records from `from_lsn` using mmap (tier-2 catchup).
    pub fn replay_mmap_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let records = nodedb_wal::mmap_reader::replay_segments_mmap(
            self.wal_dir(),
            from_lsn.as_u64(),
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)?;
        let records = self.without_time_anchors(records)?;
        Self::without_aborted_writes(records)
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
    ///
    /// Unlike [`Self::replay`] / [`Self::replay_from`], the page is NOT filtered
    /// for refused writes: a page can end between a forward record and the
    /// abort marker that names it, so the filter has to run where the caller
    /// decides what to do with the page.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    fn put(wal: &WalManager, payload: &[u8]) -> Lsn {
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                payload,
            )
            .expect("append")
    }

    #[test]
    fn time_anchors_survive_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");

        let live = {
            let wal = WalManager::open_for_testing(&path).expect("open wal");
            put(&wal, b"a");
            put(&wal, b"b");
            wal.sync().expect("sync");
            put(&wal, b"c");
            wal.sync().expect("sync");
            wal.time_anchors().anchors()
        };
        // The empty log's open anchor is in memory only.
        let persisted: Vec<_> = live.into_iter().filter(|a| a.lsn > 0).collect();
        assert_eq!(
            persisted.iter().map(|a| a.lsn).collect::<Vec<_>>(),
            vec![3, 5]
        );

        let wal = WalManager::open_for_testing(&path).expect("reopen wal");
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 3, "anchors are not handed to engine replay");
        wal.anchor_recovered_tail();
        assert_eq!(wal.time_anchors().anchors(), persisted);

        let first_ns = persisted[0].hlc_wall_ns;
        assert_eq!(
            wal.time_anchors()
                .lsn_at_or_before(first_ns)
                .expect("lookup"),
            3
        );
        assert!(wal.time_anchors().lsn_at_or_before(first_ns - 1).is_err());
    }
}
