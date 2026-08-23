// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-version fence for an interactive transaction's buffered writes.
//!
//! A buffered write is planned at STATEMENT time and dispatched at COMMIT —
//! an arbitrarily later point, bounded only by how long the client holds the
//! block open. The statement's `QueryLeaseScope` is retained for the whole
//! block, but a lease grant never compares the requested version against the
//! catalog: a lease taken after a DDL already committed is still granted at
//! the superseded version. Re-comparing the retained holds against the
//! catalog immediately before the durable dispatch is what closes that
//! window.

use std::collections::BTreeMap;

use nodedb_cluster::{DescriptorId, DescriptorKind};

use crate::control::gateway::version_check::check_descriptor_versions;
use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

use super::connection::SessionId;
use super::outcome::AbortReason;
use super::store::SessionStore;

/// Re-compare every descriptor version this transaction's buffered writes were
/// planned against with the catalog as it stands now.
///
/// Returns `Some(AbortReason::SchemaChanged)` when the catalog has moved on,
/// which aborts the COMMIT before any durable write.
pub(super) fn check_buffered_descriptors(
    catalog: &SystemCatalog,
    sessions: &SessionStore,
    session_id: SessionId,
) -> Option<AbortReason> {
    let holds = sessions.tx_descriptor_versions(session_id);
    check_holds(catalog, &holds)
}

/// The comparison itself, over an explicit hold list.
///
/// Only collection descriptors carry a catalog version to compare; other
/// kinds are not collections and are skipped. Holds are grouped by the
/// database and tenant they name so each group is compared against its own
/// catalog scope rather than the session's assumed one.
pub(super) fn check_holds(
    catalog: &SystemCatalog,
    holds: &[(DescriptorId, u64)],
) -> Option<AbortReason> {
    let mut by_scope: BTreeMap<(u64, u64), Vec<(&str, u64)>> = BTreeMap::new();
    for (id, version) in holds {
        if id.kind != DescriptorKind::Collection {
            continue;
        }
        by_scope
            .entry((id.database_id, id.tenant_id))
            .or_default()
            .push((id.name.as_str(), *version));
    }

    for ((database_id, tenant_id), entries) in by_scope {
        if let Err(error) = check_descriptor_versions(
            catalog,
            DatabaseId::new(database_id),
            tenant_id,
            entries,
        ) {
            return Some(AbortReason::SchemaChanged {
                detail: error.to_string(),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;

    const TENANT: u64 = 11;

    fn catalog_with(collections: &[(&str, u64)]) -> SystemCatalog {
        let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
        for (name, version) in collections {
            let mut stored = StoredCollection::new(TENANT, name, "owner");
            stored.descriptor_version = *version;
            catalog
                .put_collection(DatabaseId::DEFAULT, &stored)
                .expect("store collection");
        }
        catalog
    }

    fn hold(kind: DescriptorKind, name: &str, version: u64) -> (DescriptorId, u64) {
        (
            DescriptorId::new(DatabaseId::DEFAULT.as_u64(), TENANT, kind, name),
            version,
        )
    }

    #[test]
    fn unchanged_versions_pass() {
        let catalog = catalog_with(&[("orders", 3)]);
        assert!(check_holds(&catalog, &[hold(DescriptorKind::Collection, "orders", 3)]).is_none());
    }

    #[test]
    fn a_version_bump_since_the_statement_aborts_the_commit() {
        let catalog = catalog_with(&[("orders", 4)]);
        match check_holds(&catalog, &[hold(DescriptorKind::Collection, "orders", 3)]) {
            Some(AbortReason::SchemaChanged { detail }) => {
                assert!(
                    detail.contains("orders"),
                    "detail names the collection: {detail}"
                );
            }
            _ => panic!("expected SchemaChanged"),
        }
    }

    #[test]
    fn non_collection_holds_are_skipped() {
        let catalog = catalog_with(&[]);
        assert!(check_holds(&catalog, &[hold(DescriptorKind::Index, "orders_by_id", 9)]).is_none());
    }

    #[test]
    fn a_hold_in_another_tenant_is_compared_against_that_tenant() {
        let catalog = catalog_with(&[("orders", 3)]);
        let other = (
            DescriptorId::new(
                DatabaseId::DEFAULT.as_u64(),
                TENANT + 1,
                DescriptorKind::Collection,
                "orders",
            ),
            3,
        );
        // The same name at the same version in a tenant that has no such
        // collection must not be satisfied by this tenant's row.
        assert!(matches!(
            check_holds(&catalog, &[other]),
            Some(AbortReason::SchemaChanged { .. })
        ));
    }
}
