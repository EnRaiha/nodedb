// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time drain of a connection's buffered transactional DDL.
//!
//! One transaction's DDL commits as a single unit. With a metadata raft group
//! the batch goes through it fenced by a preparation lease. Without one — no
//! `[cluster]` and `server.single_node_calvin = false`, the only shape that
//! skips `start_raft` — the entries land locally in statement order instead.

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
/// False only in mixed-version compat mode, where the originating node is the
/// sole writer. `MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION` today, so no
/// admissible peer reports below the gate and this is currently always true.
fn replicated_ddl_active(state: &SharedState) -> bool {
    state
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION)
}

/// Apply every buffered entry locally, in statement order, then run the same
/// two post-apply phases the metadata applier runs. Skipping them leaves the
/// catalog and the live registries disagreeing until restart. Reached only
/// without a metadata raft group; the first failure aborts the COMMIT.
fn flush_local(state: &SharedState, buffered: DdlBuffer) -> Option<AbortReason> {
    let shared = match state.self_arc() {
        Ok(shared) => shared,
        Err(error) => return Some(AbortReason::DdlPropose(error)),
    };
    let catalog = shared.credentials.catalog();
    let total = buffered.len();
    for (position, item) in buffered.into_iter().enumerate() {
        match crate::control::catalog_entry::apply::apply_to(&item.entry, catalog) {
            // Wrote nothing (if-absent create for an existing descriptor):
            // the applier suppresses post-apply here, so this must too.
            Ok(false) => continue,
            Ok(true) => {}
            Err(error) => {
                return Some(AbortReason::DdlPropose(crate::Error::Internal {
                    detail: format!(
                        "transactional DDL local apply failed on statement {} of {} \
                         (catalog entry {}): {error}; roll back and re-run the transaction",
                        position + 1,
                        total,
                        item.entry.kind()
                    ),
                }));
            }
        }
        crate::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync(
            &item.entry,
            &shared,
        );
        // No raft index exists here; the async phase uses it purely as the
        // purge LSN for storage reclaim, which the local DDL paths take from
        // the WAL instead.
        let purge_lsn = shared.wal.next_lsn().as_u64();
        crate::control::catalog_entry::post_apply::spawn_post_apply_async_side_effects(
            item.entry,
            Arc::clone(&shared),
            purge_lsn,
        );
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

#[cfg(test)]
mod tests {
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::gateway::Gateway;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use crate::control::security::credential::CredentialStore;
    use crate::control::security::identity::Permission;
    use crate::wal::WalManager;

    use super::super::{conn_scope, ddl_buffer};
    use super::{SharedState, flush};

    use std::sync::Arc;

    /// A `SharedState` shaped like the one deployment that reaches
    /// `flush_local`: no metadata raft group (`[cluster]` absent AND
    /// `server.single_node_calvin = false`). The gateway is installed as
    /// `bootstrap::state_wiring` installs it, so `self_arc` resolves.
    fn local_only_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let credentials = Arc::new(
            CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
            .expect("construct shared state");
        let gateway = Arc::new(Gateway::new(Arc::clone(&state)));
        assert!(state.gateway.set(gateway).is_ok(), "gateway installs once");
        // Guards the branch under test: with a handle installed, `flush` would
        // take `flush_replicated` and none of these assertions would mean
        // anything.
        assert!(
            state.metadata_raft.get().is_none(),
            "fixture must have no metadata raft group, or flush takes the replicated branch"
        );
        (state, dir)
    }

    /// Buffer `entries` in one connection scope and flush them as COMMIT does.
    /// True when the flush reported no abort.
    async fn buffer_and_flush(state: &SharedState, entries: Vec<CatalogEntry>) -> bool {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            for entry in entries {
                assert!(ddl_buffer::try_buffer(entry), "buffer is active");
            }
            flush(state).is_none()
        })
        .await
    }

    #[tokio::test]
    async fn local_flush_populates_the_sequence_registry() {
        let (state, _dir) = local_only_state();
        assert!(
            !state.sequence_registry.exists(7, "orders_seq"),
            "registry starts empty"
        );

        let stored = StoredSequence::new(7, "orders_seq".into(), "alice".into());
        let ok = buffer_and_flush(&state, vec![CatalogEntry::PutSequence(Box::new(stored))]).await;

        assert!(ok, "local flush must not abort");
        assert!(
            state
                .credentials
                .catalog()
                .get_sequence(7, "orders_seq")
                .expect("catalog read")
                .is_some(),
            "the catalog write is the part that already worked"
        );
        assert!(
            state.sequence_registry.exists(7, "orders_seq"),
            "flush_local must run the post-apply sync phase: without it the catalog \
             and the live registry disagree until restart, and NEXTVAL / DROP SEQUENCE \
             report the sequence as missing"
        );
    }

    #[tokio::test]
    async fn local_flush_installs_a_replicated_grant() {
        let (state, _dir) = local_only_state();
        let stored =
            state
                .permissions
                .prepare_permission("widgets", "analyst", Permission::Read, "alice");
        assert!(
            !state
                .permissions
                .permission_exists("widgets", "analyst", Permission::Read),
            "grant cache starts empty"
        );

        let ok =
            buffer_and_flush(&state, vec![CatalogEntry::PutPermission(Box::new(stored))]).await;

        assert!(ok, "local flush must not abort");
        assert!(
            state
                .permissions
                .permission_exists("widgets", "analyst", Permission::Read),
            "flush_local must install the replicated grant, or the evaluator keeps \
             refusing a grant the catalog already holds"
        );
    }

    #[tokio::test]
    async fn local_flush_hooks_every_buffered_entry() {
        let (state, _dir) = local_only_state();
        let entries = vec![
            CatalogEntry::PutSequence(Box::new(StoredSequence::new(
                7,
                "first_seq".into(),
                "alice".into(),
            ))),
            CatalogEntry::PutSequence(Box::new(StoredSequence::new(
                7,
                "second_seq".into(),
                "alice".into(),
            ))),
        ];

        assert!(
            buffer_and_flush(&state, entries).await,
            "flush must not abort"
        );
        assert!(state.sequence_registry.exists(7, "first_seq"));
        assert!(
            state.sequence_registry.exists(7, "second_seq"),
            "the hook must run per entry, not once for the batch"
        );
    }

    #[tokio::test]
    async fn flush_outside_a_transaction_is_inert() {
        let (state, _dir) = local_only_state();
        assert!(
            conn_scope::scoped(async { flush(&state).is_none() }).await,
            "no buffer means nothing to flush"
        );
    }
}
