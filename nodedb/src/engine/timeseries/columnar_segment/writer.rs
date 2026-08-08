// SPDX-License-Identifier: BUSL-1.1

//! Columnar segment writer.
//!
//! Every file here is a checkpoint-class write: the coordinated checkpoint
//! reports these partitions as the timeseries engine's durability and the WAL
//! segments below that LSN are then unlinked, so a correctly-named file full of
//! zeros after power loss is not a degraded segment — it is the only copy of
//! those rows, gone. All writes therefore route through
//! `nodedb_wal::segment::atomic_write_fsync` (data fsynced before the rename,
//! parent directory fsynced after it) rather than a bare `fs::write`.
//!
//! `partition.meta` is written LAST and is the commit point: the boot-side
//! registry load treats a partition directory without a readable meta as
//! nothing, so a crash part-way through leaves an unreferenced directory rather
//! than a partition missing columns.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nodedb_types::timeseries::{PartitionMeta, PartitionState};
use nodedb_wal::crypto::WalEncryptionKey;

use super::super::columnar_memtable::{ColumnType, ColumnarFlushView, ColumnarSchema};
use super::codec::encode_column;
use super::encrypt::encrypt_file;
use super::error::SegmentError;
use super::schema::schema_to_json;
use super::util::dir_size;

/// Writes drained columnar memtable data to a partition directory.
pub struct ColumnarSegmentWriter {
    base_dir: PathBuf,
}

impl ColumnarSegmentWriter {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Write a flush payload to a partition directory.
    ///
    /// Takes a BORROWED [`ColumnarFlushView`] rather than an owned drain result
    /// so the caller can land the segment before the rows leave the memtable.
    ///
    /// When `kek` is `Some`, every output file is wrapped in a `SEGT`
    /// AES-256-GCM envelope before being written to disk.
    pub fn write_partition(
        &self,
        partition_name: &str,
        view: &ColumnarFlushView<'_>,
        interval_ms: u64,
        flush_wal_lsn: u64,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<PartitionMeta, SegmentError> {
        let partition_dir = self.base_dir.join(partition_name);
        std::fs::create_dir_all(&partition_dir)
            .map_err(|e| SegmentError::Io(format!("create dir: {e}")))?;

        let mut column_stats = HashMap::new();
        let mut resolved_codecs = Vec::with_capacity(view.schema.columns.len());

        // Column FILES are named from the memtable's schema, and the partition
        // records that same schema as its own — so a partition inherits whatever
        // column names the memtable was built with, permanently. Nothing here
        // re-derives them from the collection's declaration at read time.
        //
        // That is only sound because a memtable is always built from the
        // DECLARED schema: `initial_ts_schema` resolves it from `doc_configs`,
        // which the boot path seeds from the durable catalog BEFORE WAL replay
        // (`seed_catalog_state` -> `replay_wal_and_rebuild_indexes`). For a
        // declared collection to reach the inference fallback and flush a
        // partition under inferred names — `timestamp.col` instead of the
        // declared TIME_KEY — the seed would have to arrive empty, which needs
        // one of: the catalog unreadable at boot (now an error rather than a
        // silent empty seed, see `CatalogForRead::open`), a core spawned
        // without `doc_config_seed`, or the collection missing from the
        // catalog. All three are boot-integrity failures, not steady state.
        //
        // If that ever regresses, the damage is durable and silent: those
        // partitions keep projecting under the inferred name after the
        // regression is fixed, because each partition is read against its own
        // stored schema. The repair would be a rename at partition load keyed
        // on `schema.timestamp_idx` — the time column's identity is positional,
        // so the mapping is unambiguous and needs no rewrite.
        for (i, (col_name, col_type)) in view.schema.columns.iter().enumerate() {
            let col_data = &view.columns[i];
            let requested_codec = view.schema.codec(i);

            let (encoded, resolved_codec, stats) =
                encode_column(col_data, *col_type, requested_codec)?;

            let file_bytes = maybe_encrypt(kek, &encoded)?;
            durable_write(&partition_dir.join(format!("{col_name}.col")), &file_bytes)?;

            // Write symbol dictionary for tag columns.
            if *col_type == ColumnType::Symbol
                && let Some(dict) = view.symbol_dicts.get(&i)
            {
                let dict_json = sonic_rs::to_vec(dict)
                    .map_err(|e| SegmentError::Io(format!("serialize dict: {e}")))?;
                let sym_bytes = maybe_encrypt(kek, &dict_json)?;
                durable_write(&partition_dir.join(format!("{col_name}.sym")), &sym_bytes)?;
            }

            column_stats.insert(col_name.clone(), stats);
            resolved_codecs.push(resolved_codec);
        }

        // Write schema with resolved codecs.
        let schema_with_codecs = ColumnarSchema {
            columns: view.schema.columns.clone(),
            timestamp_idx: view.schema.timestamp_idx,
            codecs: resolved_codecs
                .iter()
                .map(|c| c.into_column_codec())
                .collect(),
        };
        let schema_json = sonic_rs::to_vec(&schema_to_json(&schema_with_codecs))
            .map_err(|e| SegmentError::Io(format!("serialize schema: {e}")))?;
        let schema_bytes = maybe_encrypt(kek, &schema_json)?;
        durable_write(&partition_dir.join("schema.json"), &schema_bytes)?;

        // Build and write sparse index.
        let sparse_idx = super::super::sparse_index::SparseIndex::build(
            view.columns,
            view.schema,
            view.row_count,
            super::super::sparse_index::DEFAULT_BLOCK_SIZE,
        );
        let sparse_bytes = sparse_idx.to_bytes();
        let sparse_file_bytes = maybe_encrypt(kek, &sparse_bytes)?;
        durable_write(&partition_dir.join("sparse_index.bin"), &sparse_file_bytes)?;

        let size_bytes = dir_size(&partition_dir)?;

        let meta = PartitionMeta {
            min_ts: view.min_ts,
            max_ts: view.max_ts,
            row_count: view.row_count,
            size_bytes,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms,
            last_flushed_wal_lsn: flush_wal_lsn,
            column_stats,
            max_system_ts: view.max_system_ts,
        };

        let meta_json = sonic_rs::to_vec(&meta)
            .map_err(|e| SegmentError::Io(format!("serialize meta: {e}")))?;
        let meta_bytes = maybe_encrypt(kek, &meta_json)?;
        // Last, and the commit point: a reader that cannot read a meta treats
        // the whole directory as absent.
        durable_write(&partition_dir.join("partition.meta"), &meta_bytes)?;

        // The files above each fsynced `partition_dir`; this fsyncs the
        // directory that NAMES it, without which the whole partition can be
        // missing after power loss even though every file inside it landed.
        nodedb_wal::segment::fsync_directory(&self.base_dir)
            .map_err(|e| SegmentError::Io(format!("fsync {}: {e}", self.base_dir.display())))?;

        Ok(meta)
    }
}

/// Write one segment file durably: data fsynced before the rename, parent
/// directory fsynced after it.
///
/// Routed through the shared `nodedb_wal::segment::atomic_write_fsync` rather
/// than re-implemented, so the ordering cannot drift per call site.
fn durable_write(path: &Path, bytes: &[u8]) -> Result<(), SegmentError> {
    let mut tmp = path.to_path_buf();
    let ext = path
        .extension()
        .map(|e| e.to_string_lossy().into_owned())
        .unwrap_or_default();
    tmp.set_extension(format!("{ext}.tmp"));
    nodedb_wal::segment::atomic_write_fsync(&tmp, path, bytes)
        .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))
}

/// Encrypt `bytes` with `kek` if present, otherwise return as-is.
fn maybe_encrypt(kek: Option<&WalEncryptionKey>, bytes: &[u8]) -> Result<Vec<u8>, SegmentError> {
    match kek {
        Some(key) => encrypt_file(key, bytes),
        None => Ok(bytes.to_vec()),
    }
}

/// Ensure that encrypted-file detection is accessible from tests.
#[cfg(test)]
pub(super) fn file_is_encrypted(bytes: &[u8]) -> Result<bool, SegmentError> {
    super::encrypt::is_encrypted(bytes)
}
