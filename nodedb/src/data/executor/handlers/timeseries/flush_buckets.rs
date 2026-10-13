// SPDX-License-Identifier: BUSL-1.1

//! Stage and commit the partitions of one flush whose rows span several
//! intervals.
//!
//! The commit makes the flush durable. Installing it is the roll-forward in
//! `flush_rollforward`, the same step a later flush, the maintenance tick
//! and boot recovery run.

use std::path::Path;

use nodedb_wal::crypto::WalEncryptionKey;

use crate::data::executor::timeseries_checkpoint::stamp::{TsReplayStamp, write_ts_stamp};
use crate::engine::timeseries::columnar_memtable::ColumnarFlushView;
use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
use crate::engine::timeseries::interval_split::{IntervalBucket, gather_bucket};

use super::flush_commit::{FlushManifest, FlushMove, commit_flush, pending_name, remove_staged};
use super::flush_rollforward::{CommittedFlush, CommittedPartition};

/// The flush and the collection directory it writes into.
pub(in crate::data::executor) struct BucketFlush<'a> {
    pub segment_dir: &'a Path,
    pub view: &'a ColumnarFlushView<'a>,
    pub buckets: &'a [IntervalBucket],
    pub stamp: &'a TsReplayStamp,
    pub flush_wal_lsn: u64,
    pub kek: Option<&'a WalEncryptionKey>,
}

/// Stage every bucket and commit the flush. Returns the committed flush,
/// not yet installed.
///
/// A failure before the commit leaves only staged directories, which the next
/// multi-bucket flush or a restart removes.
pub(in crate::data::executor) fn stage_and_commit(
    flush: BucketFlush<'_>,
) -> crate::Result<CommittedFlush> {
    let BucketFlush {
        segment_dir,
        view,
        buckets,
        stamp,
        flush_wal_lsn,
        kek,
    } = flush;
    // No manifest is present (the caller rolled any forward), so every staged
    // directory is the remains of a flush that never committed.
    remove_staged(segment_dir)?;
    let flush_id = format!("{flush_wal_lsn:016x}");
    let writer = ColumnarSegmentWriter::new(segment_dir);

    let mut moves = Vec::with_capacity(buckets.len());
    let mut partitions = Vec::with_capacity(buckets.len());
    for (index, bucket) in buckets.iter().enumerate() {
        let rows = gather_bucket(view, bucket)?;
        let pending = pending_name(&flush_id, index);
        let pending_dir = segment_dir.join(&pending);
        std::fs::create_dir_all(&pending_dir).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("create staged partition {}: {e}", pending_dir.display()),
        })?;
        // Every partition of the flush carries the flush's stamp. The commit
        // makes them visible together, so the stamp never names a record
        // whose rows a missing partition holds.
        write_ts_stamp(&pending_dir, stamp)?;
        let meta = writer
            .write_partition(
                &pending,
                &rows.view(),
                bucket.interval_ms(),
                flush_wal_lsn,
                kek,
            )
            .map_err(|e| crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("stage partition {}: {e}", pending_dir.display()),
            })?;
        let installed = super::flush::unique_partition_name(
            segment_dir,
            rows.min_ts,
            rows.max_ts,
            flush_wal_lsn,
        )?;
        moves.push(FlushMove {
            pending,
            installed: installed.clone(),
        });
        partitions.push(CommittedPartition {
            name: installed,
            meta,
            rows,
        });
    }

    let manifest = FlushManifest { moves };
    commit_flush(segment_dir, &manifest)?;
    Ok(CommittedFlush {
        manifest,
        partitions,
        stamp: stamp.clone(),
    })
}
