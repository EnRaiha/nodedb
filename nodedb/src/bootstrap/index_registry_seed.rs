// SPDX-License-Identifier: BUSL-1.1

//! Boot-time seeding of the catalog index registry from a pre-registry
//! catalog.
//!
//! Before the registry existed, an index left three unrelated traces: a
//! `StoredIndex` inside its collection (secondary indexes), a row in
//! `_system.vector_index_params` keyed `(tenant, collection, field)` (vector
//! indexes), and an ownership row under one of five `object_type`s filed
//! against database 0 (every kind). None of them recorded the pairing of
//! name → kind → collection → fields, which is what makes an index
//! droppable.
//!
//! This pass reconstructs that pairing for catalogs written by earlier
//! versions, and re-files each index's ownership row under the database its
//! collection actually lives in so database-scoped owner lookups resolve.
//! It is idempotent: an index that already has a registry record is left
//! alone, so it is a no-op on every boot after the first.

use std::sync::Arc;

use tracing::{info, warn};

use crate::bootstrap::constraint_reconcile::load_collections;
use crate::control::security::catalog::{
    IndexKind, StoredCollection, StoredIndexRecord, StoredOwner, SystemCatalog,
};
use crate::control::state::SharedState;

/// What one seeding pass reconstructed.
#[derive(Debug, Default, PartialEq)]
pub struct SeedStats {
    /// Records created from a collection's own `indexes` vector.
    pub secondary: usize,
    /// Records created from `_system.vector_index_params` rows.
    pub vector: usize,
    /// Records created from an ownership row alone (full-text, spatial,
    /// sparse — kinds that never had any other durable trace).
    pub from_ownership: usize,
    /// Ownership rows re-filed from database 0 onto their real database.
    pub owners_refiled: usize,
}

impl SeedStats {
    fn total(&self) -> usize {
        self.secondary + self.vector + self.from_ownership
    }
}

/// Reconstruct missing index records for every collection in the catalog.
///
/// Never fails the boot: a catalog that cannot be read here is a catalog the
/// readiness gates reject anyway, and a partially seeded registry is strictly
/// better than none — every record it does write makes one more index
/// droppable. Failures are logged with the index they concern.
pub fn seed_index_registry(shared: &Arc<SharedState>) -> SeedStats {
    let catalog = shared.credentials.catalog();
    let collections = match load_collections(catalog) {
        Ok(collections) => collections,
        Err(e) => {
            warn!(error = %e, "index registry seeding: failed to load collections");
            return SeedStats::default();
        }
    };

    let mut stats = SeedStats::default();
    for (database_id, collection) in &collections {
        seed_collection(catalog, database_id.as_u64(), collection, &mut stats);
    }

    if stats.total() > 0 || stats.owners_refiled > 0 {
        info!(
            secondary = stats.secondary,
            vector = stats.vector,
            from_ownership = stats.from_ownership,
            owners_refiled = stats.owners_refiled,
            "index registry seeded from pre-registry catalog"
        );
    }
    stats
}

fn seed_collection(
    catalog: &SystemCatalog,
    database_id: u64,
    collection: &StoredCollection,
    stats: &mut SeedStats,
) {
    let tenant_id = collection.tenant_id;

    // Secondary indexes: the collection record names them exactly.
    for index in &collection.indexes {
        let record = StoredIndexRecord {
            database_id,
            tenant_id,
            name: index.name.clone(),
            kind: IndexKind::Secondary,
            collection: collection.name.clone(),
            fields: vec![index.field.clone()],
            is_active: collection.is_active,
        };
        if register(catalog, record, stats, |s| &mut s.secondary) {
            refile_owner(
                catalog,
                IndexKind::Secondary,
                database_id,
                tenant_id,
                &index.name,
                stats,
            );
        }
    }

    // Vector indexes: the params row carries `(collection, field)` but no
    // name, and the ownership ledger carries names but no column. Pair them by
    // the name that mentions the column, then fall back to the remaining names
    // in order, so every params row ends up with exactly one record and no
    // name is left unclaimed.
    let params = match catalog.list_all_vector_index_params() {
        Ok(params) => params,
        Err(e) => {
            warn!(error = %e, "index registry seeding: failed to read vector index params");
            Vec::new()
        }
    };
    let mut unclaimed = legacy_owner_names(catalog, IndexKind::Vector, tenant_id);
    for entry in params
        .iter()
        .filter(|p| p.tenant_id == tenant_id && p.collection == collection.name)
    {
        let name = claim_name(&mut unclaimed, &entry.field_name)
            .unwrap_or_else(|| default_vector_name(&collection.name, &entry.field_name));
        let record = StoredIndexRecord {
            database_id,
            tenant_id,
            name: name.clone(),
            kind: IndexKind::Vector,
            collection: collection.name.clone(),
            fields: vec![entry.field_name.clone()],
            is_active: collection.is_active,
        };
        if register(catalog, record, stats, |s| &mut s.vector) {
            refile_owner(
                catalog,
                IndexKind::Vector,
                database_id,
                tenant_id,
                &name,
                stats,
            );
        }
    }

    // Full-text, spatial and sparse indexes left an ownership row and nothing
    // else. The generated full-text names embed the collection, so those can
    // be attributed; the others cannot be attributed from the ledger alone and
    // are left for the operator to re-create, which the warning names.
    for kind in [IndexKind::FullText, IndexKind::Spatial, IndexKind::Sparse] {
        for name in legacy_owner_names(catalog, kind, tenant_id) {
            let Some(fields) = attribute_legacy(kind, &name, &collection.name) else {
                continue;
            };
            let record = StoredIndexRecord {
                database_id,
                tenant_id,
                name: name.clone(),
                kind,
                collection: collection.name.clone(),
                fields,
                is_active: collection.is_active,
            };
            if register(catalog, record, stats, |s| &mut s.from_ownership) {
                refile_owner(catalog, kind, database_id, tenant_id, &name, stats);
            }
        }
    }
}

/// Write `record` unless the index already has one. Returns whether a record
/// was written.
fn register(
    catalog: &SystemCatalog,
    record: StoredIndexRecord,
    stats: &mut SeedStats,
    counter: fn(&mut SeedStats) -> &mut usize,
) -> bool {
    match catalog.get_index_record(record.database_id, record.tenant_id, &record.name) {
        Ok(Some(_)) => return false,
        Ok(None) => {}
        Err(e) => {
            warn!(index = %record.name, error = %e, "index registry seeding: registry read failed");
            return false;
        }
    }
    let name = record.name.clone();
    match catalog.put_index_record(&record) {
        Ok(()) => {
            *counter(stats) += 1;
            true
        }
        Err(e) => {
            warn!(index = %name, error = %e, "index registry seeding: registry write failed");
            false
        }
    }
}

/// Index names filed under `kind` for this tenant against database 0 — where
/// every pre-registry index ownership row landed.
fn legacy_owner_names(catalog: &SystemCatalog, kind: IndexKind, tenant_id: u64) -> Vec<String> {
    match catalog.load_all_owners() {
        Ok(owners) => {
            let mut names: Vec<String> = owners
                .into_iter()
                .filter(|o| {
                    o.tenant_id == tenant_id
                        && o.object_type == kind.owner_object_type()
                        && o.database_id == 0
                })
                .map(|o| o.object_name)
                .collect();
            names.sort();
            names
        }
        Err(e) => {
            warn!(error = %e, "index registry seeding: failed to read ownership rows");
            Vec::new()
        }
    }
}

/// Take the name that mentions `field`, else the first remaining one.
fn claim_name(unclaimed: &mut Vec<String>, field: &str) -> Option<String> {
    let position = unclaimed
        .iter()
        .position(|name| !field.is_empty() && name.contains(field))
        .or(if unclaimed.is_empty() { None } else { Some(0) })?;
    Some(unclaimed.remove(position))
}

fn default_vector_name(collection: &str, field: &str) -> String {
    if field.is_empty() {
        format!("{collection}_vector_idx")
    } else {
        format!("{collection}_{field}_vector_idx")
    }
}

/// Fields covered by a legacy index of `kind` named `name`, if it can be
/// attributed to `collection` at all.
fn attribute_legacy(kind: IndexKind, name: &str, collection: &str) -> Option<Vec<String>> {
    match kind {
        // Generated as `fts_{collection}_{field}`, so both parts are
        // recoverable.
        IndexKind::FullText => name
            .strip_prefix(&format!("fts_{collection}_"))
            .map(|field| vec![field.to_string()]),
        // A spatial or sparse index's ownership row records only a name, and
        // the placeholder names (`_auto_spatial` / `_auto_sparse`) were
        // tenant-global. Attributing them to a collection by guesswork would
        // register an index against the wrong one, so only a name that starts
        // with the collection is attributed; the field is unknown and left
        // empty, which the teardown for these kinds does not need.
        IndexKind::Spatial | IndexKind::Sparse => {
            name.starts_with(&format!("{collection}_")).then(Vec::new)
        }
        IndexKind::Secondary | IndexKind::Vector => None,
        // A sorted index has never had a legacy ownership row: the registry
        // record its `CREATE SORTED INDEX` files is the only place it was ever
        // recorded, so there is nothing here to attribute.
        IndexKind::Sorted => None,
    }
}

/// Re-file an index's ownership row from database 0 onto `database_id`, so the
/// database-scoped owner lookup the DROP path performs resolves it.
fn refile_owner(
    catalog: &SystemCatalog,
    kind: IndexKind,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    stats: &mut SeedStats,
) {
    if database_id == 0 {
        return;
    }
    let object_type = kind.owner_object_type();
    let legacy = match catalog.load_all_owners() {
        Ok(owners) => owners.into_iter().find(|o| {
            o.database_id == 0
                && o.tenant_id == tenant_id
                && o.object_type == object_type
                && o.object_name == name
        }),
        Err(e) => {
            warn!(index = %name, error = %e, "index registry seeding: owner read failed");
            return;
        }
    };
    let Some(legacy) = legacy else {
        return;
    };
    let refiled = StoredOwner {
        database_id,
        object_type: object_type.to_string(),
        object_name: name.to_string(),
        tenant_id,
        owner_username: legacy.owner_username,
    };
    if let Err(e) = catalog.put_owner(&refiled) {
        warn!(index = %name, error = %e, "index registry seeding: owner re-file failed");
        return;
    }
    if let Err(e) = catalog.delete_owner(object_type, 0, tenant_id, name) {
        warn!(index = %name, error = %e, "index registry seeding: legacy owner removal failed");
        return;
    }
    stats.owners_refiled += 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_prefers_the_name_that_names_the_column() {
        let mut unclaimed = vec!["idx_docs_image".to_string(), "idx_docs_text".to_string()];
        assert_eq!(
            claim_name(&mut unclaimed, "text"),
            Some("idx_docs_text".to_string())
        );
        assert_eq!(unclaimed, vec!["idx_docs_image".to_string()]);
    }

    #[test]
    fn claim_falls_back_to_the_first_remaining_name() {
        let mut unclaimed = vec!["whatever".to_string()];
        assert_eq!(
            claim_name(&mut unclaimed, "emb"),
            Some("whatever".to_string())
        );
        assert!(unclaimed.is_empty());
        assert_eq!(claim_name(&mut unclaimed, "emb"), None);
    }

    #[test]
    fn fulltext_names_are_attributed_to_their_collection_and_field() {
        assert_eq!(
            attribute_legacy(IndexKind::FullText, "fts_articles_body", "articles"),
            Some(vec!["body".to_string()])
        );
        // A name generated for another collection must not be attributed here.
        assert_eq!(
            attribute_legacy(IndexKind::FullText, "fts_notes_body", "articles"),
            None
        );
    }

    #[test]
    fn placeholder_spatial_names_are_not_guessed() {
        assert_eq!(
            attribute_legacy(IndexKind::Spatial, "_auto_spatial", "places"),
            None
        );
        assert_eq!(
            attribute_legacy(IndexKind::Spatial, "places_geo_idx", "places"),
            Some(Vec::new())
        );
    }

    #[test]
    fn default_vector_names_distinguish_columns() {
        assert_eq!(default_vector_name("docs", "emb"), "docs_emb_vector_idx");
        assert_eq!(default_vector_name("docs", ""), "docs_vector_idx");
    }
}
