// SPDX-License-Identifier: BUSL-1.1

//! CRDT tenant checkpoint load operations for [`CoreLoop`].
//!
//! The matching write path lives in `handlers/control/checkpoint_crdt.rs`
//! (`checkpoint_crdt_engines`). Checkpoints are written per-core to
//! `{data_dir}/crdt-ckpt/core-{core_id}/tenant-{tid}-coll-{hex(collection)}.ckpt` because
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

/// Per-collection checkpoint filename: `tenant-{tid}-coll-{hex(collection)}.ckpt`.
///
/// The collection is hex-encoded so the filename is filesystem-safe (collection
/// names may contain `/`, `:` or `-`) and unambiguously parseable: hex contains
/// only `[0-9a-f]`, so the `-coll-` separator never collides with the encoded
/// name and the numeric tenant id never collides with the encoding.
pub(crate) fn crdt_ckpt_filename(tenant_id: u64, collection: &str) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(collection.len() * 2);
    for b in collection.as_bytes() {
        // infallible: writing to a String never returns Err
        let _ = write!(hex, "{b:02x}");
    }
    format!("tenant-{tenant_id}-coll-{hex}.ckpt")
}

/// Parse a per-collection checkpoint file stem (no extension) back into
/// `(tenant_id, collection)`. Returns `None` for the pre-per-collection
/// `tenant-{tid}` scheme or any unparseable stem.
fn parse_crdt_ckpt_stem(stem: &str) -> Option<(u64, String)> {
    let rest = stem.strip_prefix("tenant-")?;
    let (tid_str, hex) = rest.split_once("-coll-")?;
    let tenant_id = tid_str.parse::<u64>().ok()?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let raw = hex.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    let collection = String::from_utf8(bytes).ok()?;
    Some((tenant_id, collection))
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
        let mut skipped_legacy = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // Checkpoint filenames are `"tenant-{tid}-coll-{hex(collection)}.ckpt"`.
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let Some((tid, collection)) = parse_crdt_ckpt_stem(&stem) else {
                // Pre-per-collection `tenant-{tid}.ckpt` (or otherwise
                // unparseable). No released data to preserve; WAL replay
                // rebuilds. Count and skip.
                skipped_legacy += 1;
                continue;
            };
            let tid = crate::types::TenantId::new(tid);

            let bytes = match nodedb_wal::segment::read_checkpoint_dontneed(&path) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        tenant = tid.as_u64(),
                        %collection,
                        error = %e,
                        "CRDT checkpoint read failed; WAL replay rebuilds"
                    );
                    continue;
                }
            };

            match self.get_crdt_engine(tid) {
                Ok(engine) => {
                    if let Err(e) = engine.import_snapshot_bytes(&collection, &bytes) {
                        tracing::warn!(
                            core = self.core_id,
                            tenant = tid.as_u64(),
                            %collection,
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
        if skipped_legacy > 0 {
            tracing::info!(
                core = self.core_id,
                skipped_legacy,
                "skipped pre-per-collection CRDT checkpoint files; WAL replay rebuilds"
            );
        }
    }
}
