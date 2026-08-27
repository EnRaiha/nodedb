// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time drain of a connection's buffered transactional DDL.
//!
//! One transaction's DDL commits as a single unit. Where the metadata raft
//! group is active the batch goes through it fenced by a preparation lease;
//! on a single node, or in rolling-upgrade compat mode, the same entries are
//! applied to the local catalog in statement order instead.

use std::sync::Arc;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, encode_entry};

use crate::control::metadata_proposer::MetadataRaftHandle;
use crate::control::state::SharedState;

use super::ddl_buffer::{DdlBuffer, take};
use super::outcome::AbortReason;

/// Drain the connection's DDL buffer and commit it as one unit.
///
/// Returns `None` when there was nothing to flush or the flush succeeded.
pub(super) fn flush(state: &SharedState) -> Option<AbortReason> {
    let buffered = take()?;
    if buffered.is_empty() {
        return None;
    }
    match state.metadata_raft.get() {
        Some(handle) if replicated_ddl_active(state) => flush_replicated(state, handle, buffered),
        _ => flush_local(state, buffered),
    }
}

/// True when DDL on this node replicates through the metadata raft group.
/// False on a single node and while the cluster runs in mixed-version compat
/// mode, both of which make the originating node the sole writer.
fn replicated_ddl_active(state: &SharedState) -> bool {
    state
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION)
}

/// Apply every buffered entry to the local catalog in statement order.
///
/// The unreplicated twin of `apply_locally_if_needed`: no applier will run, so
/// the committing connection lands each entry itself. The first failure aborts
/// the COMMIT and leaves the remaining entries unapplied.
fn flush_local(state: &SharedState, buffered: DdlBuffer) -> Option<AbortReason> {
    let catalog = state.credentials.catalog();
    for item in buffered {
        if let Err(error) = crate::control::catalog_entry::apply::apply_to(&item.entry, catalog) {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: format!("transactional DDL local apply: {error}"),
            }));
        }
    }
    None
}

/// Propose every buffered entry as one fenced metadata-Raft batch.
fn flush_replicated(
    state: &SharedState,
    handle: &Arc<dyn MetadataRaftHandle>,
    buffered: DdlBuffer,
) -> Option<AbortReason> {
    let _local_guard = match state.metadata_ddl_lock.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: "metadata DDL preparation lock poisoned".into(),
            }));
        }
    };
    let distributed_guard = match crate::control::metadata_proposer::acquire_ddl_prepare_lease(
        state,
        handle.as_ref(),
    ) {
        Ok(guard) => guard,
        Err(error) => return Some(AbortReason::DdlPropose(error)),
    };

    for item in &buffered {
        if let Some((descriptor_id, prior_version)) =
            crate::control::lease::descriptor_id_and_prior_version(&item.entry, state)
            && prior_version > 0
            && let Err(error) = crate::control::lease::drain_for_ddl(
                state,
                descriptor_id,
                prior_version,
                crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            )
        {
            return Some(AbortReason::DdlPropose(error));
        }
    }
    let audits: Vec<_> = buffered.iter().map(|item| item.audit.clone()).collect();
    let entries: Vec<_> = buffered.into_iter().map(|item| item.entry).collect();
    let stamped = if state
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION)
    {
        crate::control::catalog_entry::descriptor_stamp::stamp_batch(
            entries,
            &state.hlc_clock,
            state.credentials.catalog(),
        )
    } else {
        entries
    };

    let mut sub_entries = Vec::with_capacity(stamped.len());
    for (entry, audit) in stamped.into_iter().zip(audits) {
        let payload = match crate::control::catalog_entry::encode(&entry) {
            Ok(payload) => payload,
            Err(error) => return Some(AbortReason::DdlPropose(error)),
        };
        sub_entries.push(match audit {
            Some(ctx) => MetadataEntry::CatalogDdlAudited {
                payload,
                auth_user_id: ctx.auth_user_id,
                auth_user_name: ctx.auth_user_name,
                sql_text: ctx.sql_text,
            },
            None => MetadataEntry::CatalogDdl { payload },
        });
    }
    let prepared = MetadataEntry::DdlPrepared {
        token: distributed_guard.token(),
        entry: Box::new(MetadataEntry::Batch {
            entries: sub_entries,
        }),
    };
    let raw = match encode_entry(&prepared) {
        Ok(raw) => raw,
        Err(error) => {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: format!("DDL batch encode: {error}"),
            }));
        }
    };
    let log_index = match handle.propose(raw) {
        Ok(index) => index,
        Err(error) => {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: format!("DDL batch propose: {error}"),
            }));
        }
    };
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    let outcome = tokio::task::block_in_place(|| {
        watcher.wait_for(
            log_index,
            crate::control::metadata_proposer::DEFAULT_PROPOSE_TIMEOUT,
        )
    });
    if !outcome.is_reached() {
        return Some(AbortReason::DdlPropose(crate::Error::Internal {
            detail: format!(
                "DDL batch propose timed out waiting for log index {log_index} (current: {})",
                watcher.current()
            ),
        }));
    }
    if state
        .metadata_ddl_applied_token
        .load(std::sync::atomic::Ordering::Acquire)
        != distributed_guard.token()
    {
        return Some(AbortReason::DdlPropose(crate::Error::Internal {
            detail: "DDL preparation ownership was superseded before apply".into(),
        }));
    }
    None
}
