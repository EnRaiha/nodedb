// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for the sync idempotency-gate checkpoint.
//!
//! The write path and the load path build every path through these helpers so
//! the two can never drift. A path divergence between writer and reader is
//! silent, and its symptom — a gate that comes back empty — is
//! indistinguishable from never having checkpointed at all.

/// Filename of the single file holding a core's whole gate state.
pub(super) const SYNC_HWM_CKPT_STATE: &str = "STATE";

/// Canonical path for a core's sync-HWM checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores and the gate maps are per-core state; without it, cores would
/// race-overwrite each other's file and every core but the last would restore a
/// gate belonging to a different core.
pub(super) fn sync_hwm_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir
        .join("sync-hwm-ckpt")
        .join(format!("core-{core_id}"))
}

/// Path of the state file itself.
pub(super) fn sync_hwm_ckpt_state_path(ckpt_dir: &std::path::Path) -> std::path::PathBuf {
    ckpt_dir.join(SYNC_HWM_CKPT_STATE)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `sync_hwm_ckpt_dir` must isolate cores sharing one `data_dir` — without
    /// the per-core subdir a core would restore another core's gate, admitting
    /// or rejecting frames against a high-watermark that was never its own.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = sync_hwm_ckpt_dir(base, 0);
        let d1 = sync_hwm_ckpt_dir(base, 1);
        assert_ne!(d0, d1);
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn state_path_lives_under_the_core_dir() {
        let dir = sync_hwm_ckpt_dir(std::path::Path::new("/data"), 3);
        assert_eq!(
            sync_hwm_ckpt_state_path(&dir),
            dir.join(SYNC_HWM_CKPT_STATE)
        );
    }
}
