// SPDX-License-Identifier: BUSL-1.1

//! Cluster-wide write stop on a KV clone's source for the copy.
//!
//! KV keeps no row versions. An overwrite keeps the key's surrogate, and a
//! delete removes the row, so neither a surrogate ceiling nor an as-of bound
//! can hide a source write made after the clone. The copy is exact only if no
//! source write lands while it runs.
//!
//! The replicated descriptor drain gives that on every node: its start entry
//! applies everywhere, every node's planner then refuses a new plan on the
//! source collection, and the start returns only once no node holds a lease
//! on it, so no write admitted earlier is still in flight. The materializer's
//! own source scans run as pre-built gateway plans, which take no lease, so
//! the drain does not block them.
//!
//! The drain is owned by the clone collection
//! ([`nodedb_cluster::DrainOwner::CloneMaterialize`]) and covers every
//! descriptor version. A DDL on the source neither ends it nor escapes it by
//! bumping the version, and two copies from one source each hold their own.
//!
//! ## Claims and recovery
//!
//! A replicated claim row (`_system.clone_source_drains`) is written before
//! the drain starts and removed after it ends. A crash leaves the claim, and
//! [`recover_orphaned_source_drains`], run by the singleton worker before each
//! sweep, then settles it: a claim whose clone collection still needs its
//! copy stays, and the sweep re-drives that copy under the drain. Any other
//! claim ends its drain and is removed. The drain always ends before its claim
//! goes, so no crash leaves a drain with no claim to find it.

use std::future::Future;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::clone::cow_entry::replicate_async;
use crate::control::lease::{drain_for_owner_async, end_drain_async, move_source_descriptor};
use crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::catalog::clone_source_drains::CloneSourceDrain;
use crate::control::state::SharedState;

/// Await `copy` with the source collection of `coll` drained cluster-wide.
///
/// A collection whose source no longer exists has no writer to stop.
pub(super) async fn with_source_drain<T>(
    state: &SharedState,
    coll: &StoredCollection,
    copy: impl Future<Output = crate::Result<T>>,
) -> crate::Result<T> {
    let Some(origin) = &coll.cloned_from else {
        return copy.await;
    };
    if state
        .credentials
        .catalog()
        .get_collection(
            origin.source_database,
            coll.tenant_id,
            &origin.source_collection,
        )?
        .is_none()
    {
        return copy.await;
    }
    let claim = CloneSourceDrain {
        clone_database: coll.database_id.as_u64(),
        tenant_id: coll.tenant_id,
        clone_collection: coll.name.clone(),
        source_database: origin.source_database.as_u64(),
        source_collection: origin.source_collection.clone(),
    };
    replicate_async(
        state,
        &CatalogEntry::PutCloneSourceDrain(Box::new(claim.clone())),
    )
    .await?;
    if let Err(error) = drain_for_owner_async(
        state,
        source_descriptor(&claim),
        drain_owner(&claim),
        DEFAULT_DRAIN_TIMEOUT,
    )
    .await
    {
        // `drain_for_owner_async` ended its own drain on failure.
        release(state, &claim).await?;
        return Err(error);
    }
    let copied = copy.await;
    let released = release(state, &claim).await;
    let value = copied?;
    released?;
    Ok(value)
}

/// Settle every claim no running copy needs. Run by the singleton worker.
///
/// A claim whose clone collection still delegates to its source stays: the
/// sweep re-drives that copy, which ends the drain when it finishes. Every
/// other claim ends its own drain and is removed.
pub(super) async fn recover_orphaned_source_drains(state: &SharedState) -> crate::Result<()> {
    let claims = state.credentials.catalog().list_clone_source_drains()?;
    for claim in claims {
        let still_cloning = state
            .credentials
            .catalog()
            .get_collection(
                claim.clone_database_id(),
                claim.tenant_id,
                &claim.clone_collection,
            )?
            .is_some_and(|coll| coll.cloned_from.is_some());
        if !still_cloning {
            release(state, &claim).await?;
        }
    }
    Ok(())
}

/// End `claim`'s own drain, then remove the claim. Other owners' drains on
/// the source stay. The drain ends first: a crash in between leaves a claim
/// that recovery settles again.
async fn release(state: &SharedState, claim: &CloneSourceDrain) -> crate::Result<()> {
    end_drain_async(state, source_descriptor(claim), drain_owner(claim)).await?;
    replicate_async(
        state,
        &CatalogEntry::DeleteCloneSourceDrain {
            clone_database: claim.clone_database,
            tenant_id: claim.tenant_id,
            clone_collection: claim.clone_collection.clone(),
        },
    )
    .await
}

fn drain_owner(claim: &CloneSourceDrain) -> nodedb_cluster::DrainOwner {
    nodedb_cluster::DrainOwner::CloneMaterialize {
        clone_database: claim.clone_database,
        tenant_id: claim.tenant_id,
        clone_collection: claim.clone_collection.clone(),
    }
}

fn source_descriptor(claim: &CloneSourceDrain) -> nodedb_cluster::DescriptorId {
    move_source_descriptor(
        claim.source_database,
        claim.tenant_id,
        &claim.source_collection,
    )
}

#[cfg(test)]
mod tests {
    use nodedb_types::{CloneOrigin, DatabaseId, Lsn};

    use super::*;

    /// Write `coll` the way a committed `PutCollection` applies it: the
    /// collection row plus its `StoredOwner` row. A later replicated entry's
    /// apply checks catalog integrity, and a collection row without its
    /// owner row fails that check.
    fn put_collection(state: &SharedState, coll: &StoredCollection) {
        crate::control::catalog_entry::apply::collection::put(coll, state.credentials.catalog())
            .unwrap();
    }

    /// The source is drained for exactly the copy, on success and on error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_is_drained_for_the_copy_only() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;

        let source_db = DatabaseId::new(1024);
        let mut source = StoredCollection::stamped_for_test(1, "kv_src", "admin");
        source.database_id = source_db;
        source.descriptor_version = 3;
        put_collection(state, &source);
        let mut clone = StoredCollection::new(1, "kv_src", "admin");
        clone.database_id = DatabaseId::new(1025);
        clone.cloned_from = Some(CloneOrigin {
            source_database: source_db,
            source_collection: "kv_src".into(),
            as_of_lsn: Lsn::new(1),
            clone_created_at: Lsn::new(2),
            kv_surrogate_ceiling: None,
        });
        let id = move_source_descriptor(source_db.as_u64(), 1, "kv_src");

        let seen = with_source_drain(state, &clone, async {
            Ok(state.lease_drain.is_draining(&id, 3))
        })
        .await
        .unwrap();
        assert!(seen, "the source is drained while the copy runs");
        assert!(
            !state.lease_drain.is_draining(&id, 3),
            "the drain ends after"
        );

        let failed: crate::Result<()> = with_source_drain(state, &clone, async {
            Err(crate::Error::Internal {
                detail: "copy failed".into(),
            })
        })
        .await;
        assert!(failed.is_err());
        assert!(
            !state.lease_drain.is_draining(&id, 3),
            "a failed copy ends the drain too"
        );
        assert!(
            state
                .credentials
                .catalog()
                .list_clone_source_drains()
                .unwrap()
                .is_empty(),
            "every claim is released"
        );
        cluster.shutdown().await;
    }

    /// A drain left by a crashed copy whose clone collection is gone ends at
    /// recovery. One whose clone still needs its copy stays for the sweep.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_ends_orphaned_drains_only() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let catalog = state.credentials.catalog();

        let source_db = DatabaseId::new(1024);
        let mut source = StoredCollection::stamped_for_test(1, "kv_src", "admin");
        source.database_id = source_db;
        source.descriptor_version = 2;
        put_collection(state, &source);

        // A live clone: its claim and drain must survive recovery.
        let live_db = DatabaseId::new(1025);
        let mut live = StoredCollection::stamped_for_test(1, "kv_src", "admin");
        live.database_id = live_db;
        live.cloned_from = Some(CloneOrigin {
            source_database: source_db,
            source_collection: "kv_src".into(),
            as_of_lsn: Lsn::new(1),
            clone_created_at: Lsn::new(2),
            kv_surrogate_ceiling: None,
        });
        put_collection(state, &live);

        let claim = |clone_database: u64| CloneSourceDrain {
            clone_database,
            tenant_id: 1,
            clone_collection: "kv_src".into(),
            source_database: source_db.as_u64(),
            source_collection: "kv_src".into(),
        };
        let id = move_source_descriptor(source_db.as_u64(), 1, "kv_src");
        // Both copies crashed holding their drains. The dropped clone's claim
        // shares the source with the live one.
        for clone_database in [1025, 1026] {
            let row = claim(clone_database);
            drain_for_owner_async(state, id.clone(), drain_owner(&row), DEFAULT_DRAIN_TIMEOUT)
                .await
                .unwrap();
            catalog.put_clone_source_drain(&row).unwrap();
        }
        assert_eq!(state.lease_drain.total_count(), 2);

        recover_orphaned_source_drains(state).await.unwrap();
        assert_eq!(
            catalog.list_clone_source_drains().unwrap(),
            vec![claim(1025)]
        );
        assert_eq!(
            state.lease_drain.snapshot()[0].1,
            drain_owner(&claim(1025)),
            "only the dropped clone's drain ends"
        );
        assert!(
            state.lease_drain.is_draining(&id, 2),
            "the live clone's copy still needs the drain"
        );

        // The live clone finishes materializing; recovery now ends the drain.
        live.cloned_from = None;
        live.clone_status = nodedb_types::CloneStatus::Materialized;
        put_collection(state, &live);
        recover_orphaned_source_drains(state).await.unwrap();
        assert!(catalog.list_clone_source_drains().unwrap().is_empty());
        assert!(!state.lease_drain.is_draining(&id, 2));
        cluster.shutdown().await;
    }

    /// A DDL on the source during the copy ends only its own drain. The source
    /// stays drained, at the bumped version too, until the copy releases it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ddl_during_copy_leaves_source_drained_until_release() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;

        let source_db = DatabaseId::new(1024);
        let mut source = StoredCollection::stamped_for_test(1, "kv_src", "admin");
        source.database_id = source_db;
        source.descriptor_version = 3;
        put_collection(state, &source);
        let mut clone = StoredCollection::new(1, "kv_src", "admin");
        clone.database_id = DatabaseId::new(1025);
        clone.cloned_from = Some(CloneOrigin {
            source_database: source_db,
            source_collection: "kv_src".into(),
            as_of_lsn: Lsn::new(1),
            clone_created_at: Lsn::new(2),
            kv_surrogate_ceiling: None,
        });
        let id = move_source_descriptor(source_db.as_u64(), 1, "kv_src");

        with_source_drain(state, &clone, async {
            // The DDL drains the source, then its bumped `PutCollection`
            // applies and runs the implicit clear.
            crate::control::lease::drain_for_ddl_async(
                state,
                id.clone(),
                3,
                DEFAULT_DRAIN_TIMEOUT,
                0,
            )
            .await?;
            let mut altered = source.clone();
            altered.descriptor_version = 4;
            crate::control::lease::clear_implicit_drains(
                state,
                &CatalogEntry::PutCollection(Box::new(altered)),
            )?;
            assert!(
                state.lease_drain.is_draining(&id, 3),
                "the old version stays drained"
            );
            assert!(
                state.lease_drain.is_draining(&id, 4),
                "the bumped version is drained too"
            );
            assert_eq!(
                state.lease_drain.total_count(),
                1,
                "only the DDL's drain ended"
            );
            // A statement refused meanwhile is retryable and names the owner.
            match crate::control::lease::ensure_not_draining(state, &id, 4) {
                Err(error @ crate::Error::RetryableSchemaChanged { .. }) => assert!(
                    error.to_string().contains("clone materializing"),
                    "the refusal names the drain owner: {error}"
                ),
                other => panic!("expected a retryable drain refusal, got {other:?}"),
            }
            Ok(())
        })
        .await
        .unwrap();
        assert!(
            !state.lease_drain.is_draining(&id, 4),
            "the copy's release ends it"
        );
        assert_eq!(state.lease_drain.total_count(), 0);
        cluster.shutdown().await;
    }
}
