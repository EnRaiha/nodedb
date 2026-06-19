// SPDX-License-Identifier: BUSL-1.1
//! Filesystem path layout for timeseries segment directories, scoped by
//! database + tenant. Legacy (pre-scoping) layout was `ts/{collection}`.
use std::path::{Path, PathBuf};

/// The on-disk base directory for one timeseries collection's segments.
/// Layout: `{data_dir}/ts/{database_id}/{tenant_id}/{collection}`.
pub(crate) fn ts_collection_dir(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> PathBuf {
    data_dir
        .join("ts")
        .join(database_id.to_string())
        .join(tenant_id.to_string())
        .join(collection)
}

/// Legacy unscoped layout: `{data_dir}/ts/{collection}`.
fn legacy_ts_collection_dir(data_dir: &Path, collection: &str) -> PathBuf {
    data_dir.join("ts").join(collection)
}

/// Lazy one-time migration: if the new scoped dir does not exist but the
/// legacy `ts/{collection}` dir does, atomically rename it into place.
/// Called on first access (ensure_ts_registry) where (db, tid) are known —
/// the owning tenant migrates its own data. Same-filesystem rename = atomic.
///
/// TOCTOU note: the `.exists()` checks and the `rename` are not a single
/// atomic unit. A race with another process is theoretically possible, but
/// the Data Plane is per-core/single-threaded so no two cores race over the
/// same (db, tid, collection) triple. External processes touching the data
/// directory while the database is running violate the exclusive-ownership
/// contract and are out of scope. If `rename` fails (e.g. `ENOTEMPTY` because
/// `new_dir` was concurrently created), the `io::Error` is propagated to the
/// caller and surfaces as `Error::Storage` — no silent corruption.
pub(crate) fn migrate_legacy_ts_dir(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> std::io::Result<()> {
    let new_dir = ts_collection_dir(data_dir, database_id, tenant_id, collection);
    let legacy = legacy_ts_collection_dir(data_dir, collection);
    if !new_dir.exists() && legacy.exists() && legacy.is_dir() {
        if let Some(parent) = new_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&legacy, &new_dir)?;
    }
    Ok(())
}
