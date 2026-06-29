// SPDX-License-Identifier: BUSL-1.1

//! CRDT tenant checkpoint load operations for [`CoreLoop`].
//!
//! The matching write path lives in `handlers/control/snapshot.rs`
//! (`checkpoint_crdt_engines`). Checkpoints are written per-core to
//! `{data_dir}/crdt-ckpt/core-{core_id}/tenant-{tid}.ckpt` because
//! `data_dir` is shared across cores and each core only owns the CRDT
//! fragments routed to its vShards.

use super::core_loop::CoreLoop;

/// Canonical path for a core's CRDT checkpoint directory.
///
/// Used by the write path (`checkpoint_crdt_engines`), the load path
/// (`load_crdt_checkpoints`), and the restore path
/// (`restore_crdt_checkpoints`) so all three stay in sync if the scheme
/// changes. The previous bug was exactly a path divergence between writer
/// and reader — centralising here prevents recurrence.
pub(crate) fn crdt_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("crdt-ckpt").join(format!("core-{core_id}"))
}

impl CoreLoop {
    /// Load CRDT tenant checkpoints from disk on startup, before WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/crdt-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own fragments.
    ///
    /// Each `tenant-{tid}.ckpt` is a full Loro snapshot; importing it is the
    /// same idempotent `state.import` used by delta apply, so a subsequent WAL
    /// replay that re-imports deltas already folded into the checkpoint is a
    /// safe no-op.
    pub fn load_crdt_checkpoints(&mut self) {
        let ckpt_dir = crdt_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return;
        }

        let entries = match std::fs::read_dir(&ckpt_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // Checkpoint filenames are `"tenant-{tid}.ckpt"`.
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let Some(tid) = stem
                .strip_prefix("tenant-")
                .and_then(|s| s.parse::<u64>().ok())
            else {
                tracing::warn!(
                    core = self.core_id,
                    file = %stem,
                    "unparseable CRDT checkpoint filename; skipping (WAL replay rebuilds)"
                );
                continue;
            };
            let tid = crate::types::TenantId::new(tid);

            let Ok(bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&path) else {
                continue;
            };

            match self.get_crdt_engine(tid) {
                Ok(engine) => {
                    if let Err(e) = engine.import_snapshot_bytes(&bytes) {
                        tracing::warn!(
                            core = self.core_id,
                            tenant = tid.as_u64(),
                            error = %e,
                            "CRDT checkpoint import failed; WAL replay rebuilds"
                        );
                        continue;
                    }
                    loaded += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        tenant = tid.as_u64(),
                        error = %e,
                        "failed to create CRDT engine for checkpoint load"
                    );
                }
            }
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "CRDT checkpoints loaded");
        }
    }
}
