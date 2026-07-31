// SPDX-License-Identifier: BUSL-1.1

//! Unified WAL-replay orchestration for a data-plane core on startup.
//!
//! Both the production data-plane runtime (`crate::data::runtime`) and the
//! integration-test core-loop runner call this ONE method so the replay
//! sequence never drifts between them.

use nodedb_wal::{TombstoneSet, WalRecord};
use tracing::{error, info};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Replay every WAL record class into this core's engines, in the exact
    /// order restart correctness requires. No-op when `records` is empty.
    pub fn replay_all_wal(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &TombstoneSet,
    ) {
        if records.is_empty() {
            return;
        }
        let core_id = self.core_id;
        self.replay_vector_wal(records, num_cores, tombstones);
        // Direct-upsert / sparse / multi-vector writes. Runs after
        // `replay_vector_wal` so any `VectorParams` for a collection are
        // registered, and after the checkpoints loaded above so the
        // per-collection watermark gates re-application.
        self.replay_vector_extended_wal(records, num_cores, tombstones);
        // Runs after `replay_vector_wal` so the `VectorParams` records
        // emitted by `CREATE VECTOR INDEX` have registered per-collection
        // index params before secondary vector indexes are rebuilt from
        // document `Put` records.
        self.replay_document_vector_wal(records, num_cores, tombstones);
        self.replay_kv_wal(records, num_cores, tombstones);
        self.replay_timeseries_wal(records, num_cores, tombstones);
        self.replay_array_wal(records, num_cores, tombstones);
        // CRDT deltas and document/list intents share Loro state, so replay
        // their standalone WAL records together in global LSN order. CRDT has
        // no TransactionRedo subrecords; its admission-boundary writes are
        // independently durable standalone records.
        self.replay_crdt_wal_ordered(records, num_cores, tombstones);
        self.replay_fts_wal(records, num_cores, tombstones);
        self.replay_spatial_wal(records, num_cores, tombstones);
        // Graph node labels have no redb-backed durability (unlike
        // edges, rebuilt into the CSR from the `EdgeStore` before this
        // replay sequence runs) — a WAL record is their only durable
        // backing, so they get their own standalone replay pass here.
        self.replay_graph_node_label_wal(records, num_cores);

        // Replay committed-transaction redo groups LAST among the
        // engine replays: each `TransactionRedo` record is decomposed
        // into per-op records fed back through the same per-engine
        // replay paths above. Running last guarantees collection-level
        // state those establish — notably the `VectorParams` a
        // `CREATE VECTOR INDEX` wrote as a standalone record, which the
        // vector and document arms need before rebuilding an HNSW index
        // — is already in place. Every redo op is an absolute overwrite
        // or a watermark-gated append, so ordering after the standalone
        // replays (and after the checkpoint restores above) is safe.
        //
        // Fatal on error, for the same reason the sync-HWM replay below is: a
        // redo group that cannot be reconstituted is a committed transaction
        // that cannot be applied, and continuing would open the database with
        // a hole in the replayed suffix.
        if let Err(e) = self.replay_transaction_redo_wal(records, num_cores, tombstones) {
            error!(
                core_id,
                error = %e,
                "StartupError: committed-transaction redo replay failed — \
                 refusing to start with an incompletely replayed WAL"
            );
            std::process::exit(1);
        }

        // Reconstruct sync HWM maps from SyncSeqAdvance records so
        // post-restart deduplication is correct. Fatal on error —
        // a partially-recovered HWM is not safe to operate with.
        match crate::wal::replay::replay_sync_hwm_records(records) {
            Ok((maps, stats)) => {
                if stats.records > 0 {
                    info!(
                        core_id,
                        records = stats.records,
                        "sync HWM WAL replay complete"
                    );
                }
                self.install_sync_hwm_maps(maps);
            }
            Err(e) => {
                error!(
                    core_id,
                    error = %e,
                    "StartupError: sync HWM WAL replay failed — \
                     refusing to start with a partially-recovered idempotency gate"
                );
                std::process::exit(1);
            }
        }
    }
}
