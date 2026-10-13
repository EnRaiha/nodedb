// SPDX-License-Identifier: BUSL-1.1

//! Install a collection's CRDT constraint set as part of the DDL that changes it.
//!
//! A DDL that changes a collection's derived constraint set bumps its
//! `constraint_version` in the catalog. A CRDT apply is admitted against that
//! version, and its data-group apply is fenced until the version is installed
//! on the applying replica. The DDL therefore proposes the matching
//! `ConstraintChange` on the collection's data Raft log before it returns.
//!
//! The install rides the data log, never the metadata apply: every CRDT apply
//! that the new version admits is proposed later on the same log, so every
//! replica installs the version before it applies such a delta. Installing
//! from the metadata apply would race the data log on each replica.
//!
//! The constraint-reconcile loop stays as the safety net. It re-delivers the
//! committed set after a leader change, and a re-delivery of the same version
//! is a no-op on every replica.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::{StoredCollection, SystemCatalog, collection_constraints};
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ConstraintChangeOp, ReplicatedEntry, ReplicatedWrite, propose_replicated_entry,
};
use crate::types::DatabaseId;

/// A collection whose constraint version a DDL changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConstraintTarget {
    database_id: DatabaseId,
    tenant_id: u64,
    name: String,
}

/// The collections among stamped, not yet applied `entries` whose
/// `constraint_version` differs from the committed row. Call it after the
/// stamp and before the entries apply, so the committed row is the prior one.
pub(crate) fn constraint_targets(
    catalog: &SystemCatalog,
    entries: &[CatalogEntry],
) -> crate::Result<Vec<ConstraintTarget>> {
    let mut targets: Vec<ConstraintTarget> = Vec::new();
    for entry in entries {
        let stored = match entry {
            CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
                stored
            }
            _ => continue,
        };
        let prior_version = catalog
            .get_committed_collection(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| prior.constraint_version)
            .unwrap_or(0);
        if stored.constraint_version == prior_version {
            continue;
        }
        let target = ConstraintTarget {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
        };
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    Ok(targets)
}

/// Propose the committed constraint set of every target on its collection's
/// data Raft log, and wait until this node applied each one.
///
/// Reads each target's committed row, so call it after the DDL applied. A
/// target the DDL left absent or inactive installs nothing.
pub(crate) async fn install_constraints(
    shared: &SharedState,
    targets: &[ConstraintTarget],
) -> crate::Result<()> {
    if targets.is_empty() {
        return Ok(());
    }
    let catalog = shared.credentials.catalog();
    let proposer = shared.async_raft_proposer()?;
    for target in targets {
        let Some(stored) =
            catalog.get_committed_collection(target.database_id, target.tenant_id, &target.name)?
        else {
            continue;
        };
        if !stored.is_active {
            continue;
        }
        let entry = constraint_change_entry(target.database_id, &stored)?;
        let deadline =
            crate::control::server::shared::session::StatementDeadline::of_statement(shared)
                .tokio_instant();
        propose_replicated_entry(shared, proposer, entry, deadline).await?;
    }
    Ok(())
}

/// The `ConstraintChange` entry that installs `stored`'s derived constraint
/// set at its `constraint_version`, on the data group of its vShard.
pub(crate) fn constraint_change_entry(
    database_id: DatabaseId,
    stored: &StoredCollection,
) -> crate::Result<ReplicatedEntry> {
    let constraints = collection_constraints(stored);
    let mut blobs = Vec::with_capacity(constraints.len());
    for constraint in &constraints {
        blobs.push(zerompk::to_msgpack_vec(constraint).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "constraint of collection '{}' does not encode: {e}",
                    stored.name
                ),
            }
        })?);
    }
    let vshard_id = nodedb_cluster::routing::vshard_for_collection(
        nodedb_types::CollectionKey::from_bare(database_id, &stored.name),
    );
    Ok(ReplicatedEntry::new(
        stored.tenant_id,
        database_id.as_u64(),
        vshard_id,
        ReplicatedWrite::ConstraintChange {
            collection: stored.name.clone(),
            op: ConstraintChangeOp::Set,
            constraint_version: stored.constraint_version,
            constraints: blobs,
        },
    ))
}
