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
//! The install routes through the gateway like any write. The node that
//! leads the collection's data group proposes it, so a node that holds no
//! replica of that group installs the same way as one that does.
//!
//! The DDL is already committed when its install runs, so a failed install
//! never fails the DDL. The constraint-reconcile loop is the safety net. It
//! re-delivers the committed set, and a re-delivery of the same version is a
//! no-op on every replica.

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::{StoredCollection, SystemCatalog, collection_constraints};
use crate::control::server::exchange::owner_dispatch::dispatch_to_owner;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

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

/// Install the committed constraint set of every target on its collection's
/// data Raft log. Each install returns once the group's leader applied it.
///
/// Reads each target's committed row, so call it after the DDL applied. A
/// target the DDL left absent or inactive installs nothing.
///
/// The DDL is committed, so an install error never reaches its caller. The
/// error goes to the recorder, and the reconcile loop re-delivers the set.
pub(crate) async fn install_constraints(shared: &SharedState, targets: &[ConstraintTarget]) {
    for target in targets {
        if let Err(error) = install_target(shared, target).await {
            tracing::warn!(
                collection = %target.name,
                tenant_id = target.tenant_id,
                %error,
                "constraint install after a committed DDL failed; the reconcile loop re-delivers it"
            );
            crate::diag::constraint_install_deferred(
                &error,
                target.database_id.as_u64(),
                target.tenant_id,
                &target.name,
            );
        }
    }
}

/// Install the committed constraint set of `target`.
async fn install_target(shared: &SharedState, target: &ConstraintTarget) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let Some(stored) =
        catalog.get_committed_collection(target.database_id, target.tenant_id, &target.name)?
    else {
        return Ok(());
    };
    if !stored.is_active {
        return Ok(());
    }
    deliver_constraints(shared, target.database_id, &stored).await
}

/// Propose `stored`'s derived constraint set on its collection's data Raft
/// log, and wait until the group's leader applied it.
///
/// The plan routes through the gateway to the node that leads the group. That
/// node proposes it, on any node this runs on.
///
/// Returns a boxed future: DDL reaches this from inside the gateway's own
/// statement routing, and the box gives that cycle a finite size.
pub(crate) fn deliver_constraints<'a>(
    shared: &'a SharedState,
    database_id: DatabaseId,
    stored: &'a StoredCollection,
) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let collection = QualifiedCollection::new(database_id, &stored.name);
        let plan = constraint_change_plan(collection.clone(), stored)?;
        dispatch_to_owner(
            shared,
            TenantId::new(stored.tenant_id),
            database_id,
            collection.as_str(),
            plan,
            "constraint install",
        )
        .await?;
        Ok(())
    })
}

/// The `SetConstraints` plan that installs `stored`'s derived constraint set
/// at its `constraint_version`.
///
/// `collection` is the database-qualified name. A CRDT apply checks the
/// installed version under that name, and the plan homes to its vShard.
fn constraint_change_plan(
    collection: QualifiedCollection,
    stored: &StoredCollection,
) -> crate::Result<PhysicalPlan> {
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
    Ok(PhysicalPlan::Crdt(CrdtOp::SetConstraints {
        collection,
        constraint_version: stored.constraint_version,
        constraints: blobs,
    }))
}
