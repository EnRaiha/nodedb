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
        self.replay_crdt_wal(records, num_cores, tombstones);
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
        self.replay_transaction_redo_wal(records, num_cores, tombstones);

        // CRDT list-op intent replay runs last among the per-engine
        // engine replays: it re-executes position-based
        // insert/delete/move ops through the same live handlers, so
        // it must run after `replay_crdt_wal` has restored the
        // collection's underlying Loro document state (snapshot /
        // delta import) that the list containers live inside.
        self.replay_crdt_list_wal(records, num_cores, tombstones);

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
