// SPDX-License-Identifier: BUSL-1.1
//! Filesystem path layout for timeseries segment directories, scoped by
//! database + tenant.
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
