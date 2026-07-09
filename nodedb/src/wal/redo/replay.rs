// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for [`RecordType::TransactionRedo`] WAL records.
//!
//! A `TransactionRedo` record groups an ordered set of engine-native
//! sub-records ([`RedoSubRecord`]) — each already in the exact payload shape
//! that engine's own per-op WAL record uses. This module turns each
//! `TransactionRedo` back into a set of per-op [`WalRecord`]s and feeds them to
//! the SAME per-engine replay paths the standalone (autocommit) records use.
//!
//! ## Why reconstitute rather than apply a blob
//!
//! There is no global "skip records ≤ checkpoint LSN" barrier — each engine
//! self-manages idempotency (KV rebuilds from empty; columnar/timeseries gate on
//! their flushed-LSN watermark; array on its `ArrayFlush` watermark; vector /
//! spatial / FTS restore a checkpoint then replay). Routing every sub-record
//! through its engine's existing `replay_*_wal` inherits that per-engine
//! discipline exactly. Applying the redo record as one monolithic blob, or
//! bypassing an engine's replay function, would defeat it — re-applying a
//! columnar append a checkpoint already absorbed duplicates the row.
//!
//! ## Dispatch
//!
//! Each `replay_*_wal` already filters the slice it is handed by
//! `RecordType`, so the reconstituted records are handed to every engine arm
//! and each self-selects its own records. `RecordType::Put` / `Delete` are
//! shared by KV, document, and graph; those three arms disambiguate by payload
//! shape (KV by its leading string discriminator; document and graph by their
//! mutually-exclusive tuple shapes — see the `replay_document_redo` and
//! `replay_graph_redo` arms in `crate::data::executor`).
//!
//! `calvin_stamp` is ignored here: it is read only by the Calvin recovery scan,
//! never gates engine replay.

use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};

use super::RedoRecord;
use crate::data::executor::core_loop::CoreLoop;

/// Reconstitute every sub-record of every `TransactionRedo` record into a flat,
/// LSN-ordered `Vec<WalRecord>` carrying each sub-record's own engine
/// `record_type` and payload, plus the enclosing redo record's header identity
/// (tenant / vshard / database / lsn).
///
/// Non-`TransactionRedo` records are skipped. A redo record whose payload fails
/// to decode is logged and skipped rather than aborting recovery — a single
/// corrupt group must not sink the whole replay; the CRC check upstream already
/// gates gross corruption.
///
/// Reconstituted records are always plaintext (`encryption_key: None`): the
/// enclosing record was already decrypted when the WAL was read into memory, so
/// its sub-payloads are cleartext and these records never touch disk.
fn reconstitute_redo_records(records: &[WalRecord]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TransactionRedo) {
            continue;
        }
        let redo = match RedoRecord::from_bytes(&record.payload) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    lsn = record.header.lsn,
                    error = %e,
                    "skipping malformed TransactionRedo WAL record"
                );
                continue;
            }
        };
        for sub in redo.ops {
            match WalRecord::new(WalRecordArgs {
                record_type: sub.record_type,
                lsn: record.header.lsn,
                tenant_id: record.header.tenant_id,
                vshard_id: record.header.vshard_id,
                database_id: record.header.database_id,
                payload: sub.payload,
                encryption_key: None,
                preamble_bytes: None,
            }) {
                Ok(wr) => out.push(wr),
                Err(e) => tracing::warn!(
                    lsn = record.header.lsn,
                    sub_record_type = sub.record_type,
                    error = %e,
                    "skipping redo sub-record that failed to reconstitute"
                ),
            }
        }
    }
    out
}

impl CoreLoop {
    /// Replay all `TransactionRedo` records: reconstitute each sub-record into a
    /// per-op [`WalRecord`] and dispatch to that engine's existing replay path.
    ///
    /// Must run AFTER the per-op replays (`replay_vector_wal` et al.) so that
    /// collection-level state those establish — notably `VectorParams` records
    /// emitted by `CREATE VECTOR INDEX`, which the document and vector arms need
    /// before rebuilding any HNSW index — is already in place. Every redo op is
    /// an absolute overwrite or a watermark-gated append, so running last is
    /// safe. Must also run after checkpoint restores, for the same reason the
    /// per-op replays do.
    ///
    /// CRDT is intentionally NOT dispatched here: CRDT deltas ride their own
    /// `CrdtDelta` records via `replay_crdt_wal`, never redo sub-records.
    pub(crate) fn replay_transaction_redo_wal(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let reconstituted = reconstitute_redo_records(records);
        if reconstituted.is_empty() {
            return;
        }

        // Vector params (VectorParams sub-records) register before vector puts,
        // exactly as in the standalone replay ordering.
        self.replay_vector_wal(&reconstituted, num_cores, tombstones);
        self.replay_kv_wal(&reconstituted, num_cores, tombstones);
        self.replay_timeseries_wal(&reconstituted, num_cores, tombstones);
        self.replay_array_wal(&reconstituted, num_cores, tombstones);
        self.replay_fts_wal(&reconstituted, num_cores, tombstones);
        self.replay_spatial_wal(&reconstituted, num_cores, tombstones);
        // Document and graph have no standalone replay — they survive today via
        // redb's synchronous commit at apply time. Under write-ahead-then-install
        // a crash between append and install loses them, so redo replays them too.
        // `apply_point_put` rebuilds any secondary vector index inline, so no
        // separate `replay_document_vector_wal` pass is needed here.
        self.replay_document_redo(&reconstituted, num_cores, tombstones);
        self.replay_graph_redo(&reconstituted, num_cores, tombstones);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{RedoRecord, RedoSubRecord};

    fn redo_wal_record(lsn: u64, tenant_id: u64, vshard_id: u32, record: &RedoRecord) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id,
            vshard_id,
            database_id: 0,
            payload: record.to_bytes().expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn reconstitute_preserves_type_payload_and_header() {
        let redo = RedoRecord {
            version: 1,
            ops: vec![
                RedoSubRecord {
                    record_type: RecordType::VectorPut as u32,
                    payload: vec![1, 2, 3],
                },
                RedoSubRecord {
                    record_type: RecordType::SpatialPut as u32,
                    payload: vec![4, 5],
                },
            ],
            calvin_stamp: None,
        };
        let outer = redo_wal_record(77, 9, 3, &redo);

        let recon = reconstitute_redo_records(std::slice::from_ref(&outer));
        assert_eq!(recon.len(), 2);
        assert_eq!(recon[0].logical_record_type(), RecordType::VectorPut as u32);
        assert_eq!(recon[0].payload, vec![1, 2, 3]);
        assert_eq!(
            recon[1].logical_record_type(),
            RecordType::SpatialPut as u32
        );
        assert_eq!(recon[1].payload, vec![4, 5]);
        // Enclosing header identity propagates to every sub-record.
        for r in &recon {
            assert_eq!(r.header.lsn, 77);
            assert_eq!(r.header.tenant_id, 9);
            assert_eq!(r.header.vshard_id, 3);
        }
    }

    #[test]
    fn reconstitute_skips_non_redo_and_malformed() {
        // A non-redo record is skipped.
        let put = WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: vec![9, 9, 9],
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        // A redo-typed record with an undecodable payload is skipped, not fatal.
        let bad = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 2,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: vec![0xff, 0xff, 0xff],
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let recon = reconstitute_redo_records(&[put, bad]);
        assert!(recon.is_empty());
    }
}
