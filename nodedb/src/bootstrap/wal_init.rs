// SPDX-License-Identifier: BUSL-1.1

//! WAL initialization, validation, replay, and tombstone loading.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::wal::WalManager;

/// Open, validate, and replay the WAL. Load the tombstone set from redb + WAL.
///
/// Returns `(wal, wal_records, tombstones)`. Exits the process on unrecoverable
/// corruption — a dirty WAL cannot be safely replayed.
pub fn init_wal(
    config: &ServerConfig,
) -> anyhow::Result<(
    Arc<WalManager>,
    Arc<[nodedb_wal::WalRecord]>,
    nodedb_wal::TombstoneSet,
)> {
    let wal_segment_target = config.checkpoint.wal_segment_target_bytes();
    let wal = {
        let mut mgr = WalManager::open_with_tuning(
            &config.wal_dir(),
            false,
            wal_segment_target,
            &config.tuning.wal,
        )?;
        if let Some(ref enc) = config.encryption {
            let key = nodedb_wal::crypto::WalEncryptionKey::from_file(&enc.key_path)
                .map_err(crate::Error::Wal)?;
            mgr.set_encryption_ring(nodedb_wal::crypto::KeyRing::new(key))?;
            info!(key_path = %enc.key_path.display(), "WAL encryption enabled");
        }
        Arc::new(mgr)
    };
    info!(next_lsn = %wal.next_lsn(), "WAL ready");

    if let Err(e) = wal.validate_for_startup() {
        tracing::error!(
            error = %e,
            "StartupError: WAL validation failed — cannot start with corrupted WAL segments"
        );
        std::process::exit(1);
    }

    let wal_records: Arc<[nodedb_wal::WalRecord]> = match wal.replay() {
        Ok(records) => {
            if !records.is_empty() {
                info!(records = records.len(), "WAL records loaded for replay");
            }
            Arc::from(records.into_boxed_slice())
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "StartupError: WAL replay failed — cannot start with a corrupt or unreadable WAL"
            );
            std::process::exit(1);
        }
    };

    tracing::warn!(
        catalog = %config.catalog_path().display(),
        "redb catalog stored unencrypted; use a dm-crypt/LUKS volume \
         for at-rest catalog encryption"
    );

    let tombstones = load_tombstones(config, &wal_records)?;
    Ok((wal, wal_records, tombstones))
}

fn load_tombstones(
    config: &ServerConfig,
    wal_records: &Arc<[nodedb_wal::WalRecord]>,
) -> anyhow::Result<nodedb_wal::TombstoneSet> {
    let catalog_path = config.catalog_path();
    let mut set = nodedb_wal::extract_tombstones(wal_records)
        .map_err(|error| anyhow::anyhow!("extract WAL tombstones: {error}"))?;
    let catalog = crate::control::security::catalog::SystemCatalog::open(&catalog_path)
        .map_err(|error| anyhow::anyhow!("open catalog for WAL tombstones: {error}"))?;
    let persisted = catalog
        .load_wal_tombstones()
        .map_err(|error| anyhow::anyhow!("load persisted WAL tombstones: {error}"))?;
    if !persisted.is_empty() {
        info!(
            persisted = persisted.len(),
            in_wal = set.len(),
            "merging persisted collection tombstones into replay set"
        );
    }
    set.extend(persisted);
    Ok(set)
}
