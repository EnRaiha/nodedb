// SPDX-License-Identifier: BUSL-1.1

//! `REWRITE PARTITIONS FOR <name>`
//!
//! Triggers an async background rewrite of all sealed partitions
//! for a timeseries collection. Non-blocking — returns immediately.
//! Useful for reclaiming space after column drops or applying new compression.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::ddl_err;

/// REWRITE PARTITIONS FOR <name>
pub fn rewrite_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // REWRITE PARTITIONS FOR <name>
    if parts.len() < 4 {
        return Err(ddl_err(
            "42601",
            "syntax: REWRITE PARTITIONS FOR <collection>",
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    // Verify collection exists and is timeseries.
    {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &name) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(ddl_err(
                    "42809",
                    format!("'{name}' is not a timeseries collection"),
                ));
            }
            _ => {
                return Err(ddl_err(
                    "42P01",
                    format!("collection '{name}' does not exist"),
                ));
            }
        }
    }

    // Collect partition directories to rewrite.
    let partitions_to_rewrite: Vec<String> = if let Some(registries) = state.timeseries_registries()
    {
        let key = format!("{}:{}", tenant_id.as_u64(), name);
        let regs = crate::control::lock_utils::lock_or_recover(registries.lock(), "ts_registries");
        if let Some(registry) = regs.get(&key) {
            registry
                .iter()
                .filter(|(_, e)| {
                    e.meta.state == nodedb_types::timeseries::PartitionState::Sealed
                        || e.meta.state == nodedb_types::timeseries::PartitionState::Merged
                })
                .map(|(_, e)| e.dir_name.clone())
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let sealed_count = partitions_to_rewrite.len();
    tracing::info!(
        collection = name,
        sealed_partitions = sealed_count,
        "REWRITE PARTITIONS scheduled (async, non-blocking)"
    );

    if sealed_count > 0 {
        let wal_dir = state.wal.wal_dir();
        let ts_base = wal_dir
            .parent()
            .unwrap_or(wal_dir)
            .join("timeseries")
            .to_path_buf();
        let collection_name = name.clone();

        tokio::task::spawn_blocking(move || {
            let mut rewritten = 0usize;
            for dir_name in &partitions_to_rewrite {
                let partition_dir = ts_base.join(dir_name);
                if !partition_dir.exists() {
                    continue;
                }
                match crate::engine::timeseries::merge::merge_partitions(
                    &ts_base,
                    std::slice::from_ref(&partition_dir),
                    &format!("{dir_name}.rewrite"),
                ) {
                    Ok(result) => {
                        let rewrite_dir = ts_base.join(format!("{dir_name}.rewrite"));
                        let backup_dir = ts_base.join(format!("{dir_name}.old"));
                        if nodedb_wal::segment::atomic_swap_dirs_fsync(
                            &partition_dir,
                            &backup_dir,
                            &rewrite_dir,
                        )
                        .is_ok()
                        {
                            let _ = std::fs::remove_dir_all(&backup_dir);
                            // Write updated metadata to partition.meta (on-disk source of truth).
                            let meta_path = partition_dir.join("partition.meta");
                            let meta_tmp = partition_dir.join("partition.meta.tmp");
                            let meta_bytes =
                                sonic_rs::to_vec_pretty(&result.meta).unwrap_or_default();
                            let _ = nodedb_wal::segment::atomic_write_fsync(
                                &meta_tmp,
                                &meta_path,
                                &meta_bytes,
                            );
                            rewritten += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            partition = dir_name,
                            error = %e,
                            "rewrite failed for partition"
                        );
                    }
                }
            }
            tracing::info!(
                collection = collection_name,
                rewritten,
                total = sealed_count,
                "REWRITE PARTITIONS completed"
            );
        });
    }

    Ok(vec![DdlResult::Status {
        command: "REWRITE PARTITIONS".to_string(),
        rows_affected: None,
    }])
}
