// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for collections.
//!
//! DDL inside an explicit transaction is buffered per connection and lands in
//! the catalog only at COMMIT. Without an overlay every later statement in that
//! same transaction resolves names against the committed catalog and misses its
//! own `CREATE`. [`resolve_collection`] replays the connection's buffered
//! entries over a committed catalog read, so the transaction sees its own DDL
//! in statement order while every other session still sees only committed
//! state.
//!
//! The overlay is derived from the buffer, never a second copy of it: ROLLBACK
//! discards the buffer and the overlay vanishes with it, and COMMIT takes the
//! buffer before flushing, so the flush itself reads the committed catalog.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::StoredCollection;
use crate::control::server::shared::session::ddl_buffer;

/// True when `entry` mutates the collection `(database_id, tenant_id, name)`.
/// Names are compared exactly — the DDL layer has already normalized them, and
/// a near-match must miss rather than shadow an unrelated collection.
fn targets(entry: &CatalogEntry, database_id: DatabaseId, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeactivateCollection {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
            ..
        }
        | CatalogEntry::PurgeCollection {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
        } => *entry_db == database_id.as_u64() && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(current: Option<StoredCollection>, entry: &CatalogEntry) -> Option<StoredCollection> {
    match entry {
        CatalogEntry::PutCollection(stored) => Some((**stored).clone()),
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            Some(current.unwrap_or_else(|| (**stored).clone()))
        }
        CatalogEntry::DeactivateCollection {
            descriptor_version,
            modification_hlc,
            ..
        } => current.map(|mut stored| {
            stored.is_active = false;
            // Mirror the committed apply, or a transaction reads back ordering
            // metadata its own COMMIT will not produce. Version `0` is the
            // pre-stamping sentinel — buffered entries are stamped at flush,
            // so in-transaction the row keeps what it already had.
            if *descriptor_version != 0 {
                stored.descriptor_version = *descriptor_version;
                stored.modification_hlc = *modification_hlc;
            }
            stored
        }),
        CatalogEntry::PurgeCollection { .. } => None,
        _ => current,
    }
}

/// Resolve one collection through this connection's uncommitted DDL.
///
/// `committed` is what the catalog itself holds; it is returned unchanged
/// outside a transaction and outside any connection scope.
pub fn resolve_collection(
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredCollection>,
) -> Option<StoredCollection> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

/// Merge this connection's uncommitted DDL into a committed tenant listing.
///
/// A buffered create appends, a buffered ALTER replaces in place, a buffered
/// soft-drop flips `is_active`, and a buffered purge removes the row.
pub fn resolve_tenant_collections(
    database_id: DatabaseId,
    tenant_id: u64,
    committed: Vec<StoredCollection>,
) -> Vec<StoredCollection> {
    let overlaid = ddl_buffer::with_buffered(|buffered| {
        let mut rows = committed.clone();
        for item in buffered {
            merge_row(&mut rows, database_id, tenant_id, &item.entry);
        }
        rows
    });
    overlaid.unwrap_or(committed)
}

/// Apply one buffered entry to a tenant listing.
fn merge_row(
    rows: &mut Vec<StoredCollection>,
    database_id: DatabaseId,
    tenant_id: u64,
    entry: &CatalogEntry,
) {
    let Some(name) = collection_name(entry) else {
        return;
    };
    if !targets(entry, database_id, tenant_id, name) {
        return;
    }
    match rows.iter().position(|row| row.name == name) {
        // Replaced in place so a buffered ALTER keeps the listing's order.
        Some(index) => match step(Some(rows[index].clone()), entry) {
            Some(resolved) => rows[index] = resolved,
            None => {
                rows.remove(index);
            }
        },
        None => {
            if let Some(resolved) = step(None, entry) {
                rows.push(resolved);
            }
        }
    }
}

/// The collection name a buffered entry mutates, when it mutates one.
fn collection_name(entry: &CatalogEntry) -> Option<&str> {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            Some(stored.name.as_str())
        }
        CatalogEntry::DeactivateCollection { name, .. }
        | CatalogEntry::PurgeCollection { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::conn_scope;

    const TENANT: u64 = 7;

    fn stored(name: &str) -> StoredCollection {
        StoredCollection::new(TENANT, name, "alice")
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutCollection(Box::new(stored(name)))
    }

    fn deactivate(name: &str) -> CatalogEntry {
        CatalogEntry::DeactivateCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TENANT,
            name: name.to_owned(),
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn purge(name: &str) -> CatalogEntry {
        CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TENANT,
            name: name.to_owned(),
        }
    }

    fn resolve(name: &str, committed: Option<StoredCollection>) -> Option<StoredCollection> {
        resolve_collection(DatabaseId::DEFAULT, TENANT, name, committed)
    }

    #[tokio::test]
    async fn outside_a_transaction_the_committed_row_wins() {
        conn_scope::scoped(async {
            assert!(resolve("orders", None).is_none());
            assert!(resolve("orders", Some(stored("orders"))).is_some());
        })
        .await;
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("orders")));
            let resolved = resolve("orders", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "orders");
            assert!(resolved.is_active);
        })
        .await;
    }

    #[tokio::test]
    async fn a_buffered_create_does_not_leak_to_another_name() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("orders")));
            assert!(resolve("invoices", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_purge_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("orders"));
            ddl_buffer::try_buffer(purge("orders"));
            assert!(resolve("orders", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn a_buffered_soft_drop_hides_a_committed_collection() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(deactivate("orders"));
            let resolved = resolve("orders", Some(stored("orders"))).expect("row is kept");
            assert!(
                !resolved.is_active,
                "a soft-dropped collection must read back inactive inside its own transaction"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn a_buffered_alter_shadows_the_committed_shape() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            let mut altered = stored("orders");
            altered.fields.push(("total".into(), "INT".into()));
            ddl_buffer::try_buffer(CatalogEntry::PutCollection(Box::new(altered)));
            let resolved = resolve("orders", Some(stored("orders"))).expect("row resolves");
            assert!(
                resolved.fields.iter().any(|(field, _)| field == "total"),
                "the altered shape must win over the committed one"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn if_absent_never_clobbers_an_existing_shape() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            let mut announced = stored("orders");
            announced.fields.push(("ghost".into(), "INT".into()));
            ddl_buffer::try_buffer(CatalogEntry::PutCollectionIfAbsent(Box::new(announced)));
            let resolved = resolve("orders", Some(stored("orders"))).expect("row resolves");
            assert!(resolved.fields.iter().all(|(field, _)| field != "ghost"));
        })
        .await;
    }

    #[tokio::test]
    async fn another_tenant_is_untouched() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("orders"));
            assert!(resolve_collection(DatabaseId::DEFAULT, TENANT + 1, "orders", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn a_listing_reflects_creates_drops_and_purges() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("fresh"));
            ddl_buffer::try_buffer(deactivate("legacy"));
            ddl_buffer::try_buffer(purge("gone"));
            let rows = resolve_tenant_collections(
                DatabaseId::DEFAULT,
                TENANT,
                vec![stored("legacy"), stored("gone")],
            );
            let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
            assert!(
                names.contains(&"fresh"),
                "buffered create appears: {names:?}"
            );
            assert!(
                !names.contains(&"gone"),
                "buffered purge removes: {names:?}"
            );
            let legacy = rows
                .iter()
                .find(|row| row.name == "legacy")
                .expect("soft-dropped row is kept");
            assert!(!legacy.is_active);
        })
        .await;
    }

    #[tokio::test]
    async fn a_discarded_buffer_leaves_nothing_visible() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("orders"));
            ddl_buffer::discard();
            assert!(
                resolve("orders", None).is_none(),
                "ROLLBACK discards the buffer, so the overlay must vanish with it"
            );
        })
        .await;
    }
}
