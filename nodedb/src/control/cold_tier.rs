// SPDX-License-Identifier: BUSL-1.1

//! Periodic cold tiering task: promotes old L1 data segments to L2 cold storage.
//!
//! Each cycle scans `{data_dir}/segments/` for segment files whose modification
//! time exceeds `tier_after_secs`. Eligible files are uploaded as raw binary
//! objects to the configured cold store under `{prefix}segments/{name}`, then
//! deleted locally on success.
//!
//! Runs on the Control Plane (Tokio) — `ColdStorage` is async and `Send + Sync`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use crate::config::server::ColdStorageSettings;
use crate::control::state::SharedState;
use crate::storage::segment::decrypt_untrusted_segment_bytes;

/// Spawn the cold tiering background task.
///
/// Scans `data_dir/segments/` every `tier_check_interval_secs` seconds and
/// uploads segment files older than `tier_after_secs` to cold storage. The
/// local file is removed after a successful upload.
///
/// The task exits cleanly when `shutdown_rx` is set to `true`.
pub fn spawn_cold_tier_task(
    shared: Arc<SharedState>,
    settings: ColdStorageSettings,
    data_dir: PathBuf,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let check_interval = Duration::from_secs(settings.tier_check_interval_secs);
    let tier_after = Duration::from_secs(settings.tier_after_secs);
    let prefix = settings.prefix.clone();
    let segments_dir = data_dir.join("segments");

    tokio::spawn(async move {
        info!(
            check_interval_secs = settings.tier_check_interval_secs,
            tier_after_secs = settings.tier_after_secs,
            segments_dir = %segments_dir.display(),
            "cold tier task started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("cold tier task stopping on shutdown signal");
                        return;
                    }
                }
            }

            let cold = match shared.cold_storage.as_ref() {
                Some(c) => Arc::clone(c),
                None => {
                    // Cold storage was removed from shared state; stop the task.
                    warn!("cold tier: cold_storage is None, stopping task");
                    return;
                }
            };

            let key = match shared.wal.encryption_key().cloned() {
                Some(key) => key,
                None => {
                    warn!("cold tier: refusing object-store uploads without a WAL encryption key");
                    return;
                }
            };
            run_tier_cycle_at(&cold, &segments_dir, tier_after, &prefix, &key).await;
        }
    })
}

/// Run one tiering cycle against the given segments directory.
///
/// Uploads eligible segment files (older than `tier_after`) to the cold store
/// under `{prefix}segments/{filename}`, then removes the local copies.
pub(crate) async fn run_tier_cycle_at(
    cold: &crate::storage::cold::ColdStorage,
    segments_dir: &std::path::Path,
    tier_after: Duration,
    prefix: &str,
    key: &nodedb_wal::crypto::WalEncryptionKey,
) {
    let now = SystemTime::now();

    let entries = match read_dir_sync(segments_dir).await {
        Ok(e) => e,
        Err(e) => {
            warn!(
                error = %e,
                dir = %segments_dir.display(),
                "cold tier: failed to read segments directory"
            );
            return;
        }
    };

    let mut tiered: u64 = 0;
    let mut errors: u64 = 0;

    for entry_path in entries {
        let age = match file_age(&entry_path, now) {
            Some(a) => a,
            None => {
                debug!(path = %entry_path.display(), "cold tier: skipping entry (mtime unavailable)");
                continue;
            }
        };

        if age < tier_after {
            debug!(
                path = %entry_path.display(),
                age_secs = age.as_secs(),
                "cold tier: segment too recent, skipping"
            );
            continue;
        }

        let segment_name = match entry_path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_owned(),
            None => {
                warn!(path = %entry_path.display(), "cold tier: invalid segment filename, skipping");
                continue;
            }
        };

        let object_path = format!("{}segments/{}", prefix, segment_name);
        let entry_path_clone = entry_path.clone();

        match upload_raw_segment(cold, &entry_path_clone, &object_path, key).await {
            Ok(()) => {
                info!(
                    segment = %segment_name,
                    object_path = %object_path,
                    age_secs = age.as_secs(),
                    "cold tier: segment uploaded, removing local copy"
                );

                // Remove local file after successful upload.
                let remove_path = entry_path.clone();
                let remove_result =
                    tokio::task::spawn_blocking(move || std::fs::remove_file(&remove_path))
                        .await
                        .map_err(|e| format!("spawn_blocking join: {e}"))
                        .and_then(|r| r.map_err(|e| e.to_string()));

                match remove_result {
                    Ok(()) => {
                        tiered += 1;
                    }
                    Err(e) => {
                        warn!(
                            segment = %segment_name,
                            error = %e,
                            "cold tier: failed to remove local segment after upload"
                        );
                        errors += 1;
                    }
                }
            }
            Err(e) => {
                warn!(segment = %segment_name, error = %e, "cold tier: upload failed");
                errors += 1;
            }
        }
    }

    if tiered > 0 || errors > 0 {
        info!(tiered, errors, "cold tier cycle complete");
    } else {
        debug!("cold tier cycle: no eligible segments");
    }
}

/// Upload the raw bytes of a local segment file to the object store at `object_path`.
async fn upload_raw_segment(
    cold: &crate::storage::cold::ColdStorage,
    local_path: &std::path::Path,
    object_path: &str,
    key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<()> {
    let path_buf = local_path.to_path_buf();
    let data = tokio::task::spawn_blocking(move || std::fs::read(&path_buf))
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("spawn_blocking join: {e}"),
        })?
        .map_err(|e| crate::Error::Internal {
            detail: format!("read segment file: {e}"),
        })?;

    decrypt_untrusted_segment_bytes(&data, key).map_err(|error| crate::Error::Storage {
        engine: "cold-tier".into(),
        detail: format!("refusing unauthenticated segment upload: {error}"),
    })?;

    let store = cold.object_store();
    let opath = object_store::path::Path::from(object_path.to_owned());

    store
        .put_opts(
            &opath,
            object_store::PutPayload::from(data),
            object_store::PutOptions::default(),
        )
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("object store put: {e}"),
        })?;

    Ok(())
}

/// Read a directory on a blocking thread, returning paths of all regular files.
async fn read_dir_sync(dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut result = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.metadata()?.is_file() {
                result.push(entry.path());
            }
        }
        Ok(result)
    })
    .await
    .map_err(|e| std::io::Error::other(format!("spawn_blocking join: {e}")))?
}

/// Return the elapsed time since a file was last modified, or `None` on any error.
fn file_age(path: &std::path::Path, now: SystemTime) -> Option<Duration> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    now.duration_since(modified).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cold::{ColdStorageConfig, ParquetCompression};
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn forged_sseg_framing_is_not_uploaded_or_deleted() {
        let local = tempfile::tempdir().expect("local segment directory");
        let cold_dir = tempfile::tempdir().expect("cold object directory");
        let path = local.path().join("forged.seg");
        let mut forged = vec![0u8; 36 + nodedb_wal::crypto::AUTH_TAG_SIZE];
        forged[..4].copy_from_slice(b"SSEG");
        forged[4..6].copy_from_slice(&1u16.to_le_bytes());
        std::fs::write(&path, forged).expect("write forged envelope");

        let cold = crate::storage::cold::ColdStorage::new(ColdStorageConfig {
            local_dir: Some(cold_dir.path().to_path_buf()),
            compression: ParquetCompression::Zstd,
            ..ColdStorageConfig::default()
        })
        .expect("cold storage");
        let key = nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0xA5; 32])
            .expect("test encryption key");

        assert!(
            upload_raw_segment(&cold, &path, "segments/forged.seg", &key)
                .await
                .is_err()
        );
        assert!(path.exists(), "forged source must not be deleted");
        let object = object_store::path::Path::from("segments/forged.seg");
        assert!(cold.object_store().head(&object).await.is_err());
    }
}
