// SPDX-License-Identifier: BUSL-1.1

//! Route a replicated array cell write to the array incarnation it targets.
//!
//! A data-group replica can apply a cell write after its metadata group
//! already applied a MOVE TENANT or a DROP of the array. The proposer stamps
//! the incarnation it wrote against. The replica then applies the write under
//! its own key, routes it to the database the incarnation moved to, or
//! refuses it as superseded when the incarnation no longer exists.
//!
//! The incarnation's gate (`control::write_gate`) orders the two sides:
//! a replica routes under it shared, and the array delete's post-apply moves
//! or drops the array under it exclusive.

use nodedb_types::Hlc;

use crate::control::array_catalog::ArrayCatalog;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

/// Where a cell write for `(tenant, database, name)` at an incarnation lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CellRoute {
    /// The key the write names still holds its incarnation.
    Here,
    /// The incarnation moved to this database.
    Moved(DatabaseId),
    /// The incarnation no longer exists: the write has nothing to mutate.
    Superseded,
}

/// Decide the route against the in-memory array mirror. An unstamped side
/// (`Hlc::ZERO`) matches by key alone.
pub(crate) fn route(
    mirror: &ArrayCatalog,
    tenant_id: TenantId,
    database_id: DatabaseId,
    name: &str,
    incarnation: Hlc,
) -> CellRoute {
    if let Some(entry) = mirror.lookup_by_name_in_database(tenant_id, database_id, name)
        && (incarnation == Hlc::ZERO
            || entry.incarnation == Hlc::ZERO
            || entry.incarnation == incarnation)
    {
        return CellRoute::Here;
    }
    if incarnation == Hlc::ZERO {
        return CellRoute::Superseded;
    }
    mirror
        .all_entries()
        .into_iter()
        .find(|entry| {
            entry.array_id.tenant_id == tenant_id
                && entry.name == name
                && entry.incarnation == incarnation
        })
        .map_or(CellRoute::Superseded, |entry| {
            CellRoute::Moved(entry.array_id.database_id)
        })
}

/// The incarnation whose gate a write for `(tenant, database, name)` at
/// `incarnation` holds: the stamped one, or for an unstamped write the one
/// its key holds now. `None` when an unstamped write names no array.
pub(crate) fn gate_incarnation(
    mirror: &ArrayCatalog,
    tenant_id: TenantId,
    database_id: DatabaseId,
    name: &str,
    incarnation: Hlc,
) -> Option<Hlc> {
    if incarnation != Hlc::ZERO {
        return Some(incarnation);
    }
    mirror
        .lookup_by_name_in_database(tenant_id, database_id, name)
        .map(|entry| entry.incarnation)
}

/// Stamp the incarnation of the array a cell write names, as this proposer's
/// mirror holds it. Every other entry passes through.
pub(crate) fn stamp_incarnation(state: &SharedState, entry: &mut ReplicatedEntry) {
    let (ReplicatedWrite::ArrayCellPut {
        array, incarnation, ..
    }
    | ReplicatedWrite::ArrayCellDelete {
        array, incarnation, ..
    }
    | ReplicatedWrite::ArrayOp {
        array, incarnation, ..
    }) = &mut entry.write
    else {
        return;
    };
    let mirror = match state.array_catalog.read() {
        Ok(mirror) => mirror,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(row) = mirror.lookup_by_name_in_database(
        TenantId::new(entry.tenant_id),
        DatabaseId::new(entry.database_id),
        array,
    ) {
        *incarnation = row.incarnation;
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::ArrayId;

    use super::*;
    use crate::control::array_catalog::ArrayCatalogEntry;

    fn row(db: u64, name: &str, incarnation: Hlc) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::new(db), name),
            name: name.to_string(),
            schema_msgpack: vec![0x90],
            schema_hash: 7,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: incarnation,
            incarnation,
        }
    }

    fn at(mirror: &ArrayCatalog, db: u64, incarnation: Hlc) -> CellRoute {
        route(
            mirror,
            TenantId::new(1),
            DatabaseId::new(db),
            "grid",
            incarnation,
        )
    }

    #[test]
    fn a_write_follows_its_moved_incarnation() {
        let mut mirror = ArrayCatalog::new();
        mirror
            .register(row(4, "grid", Hlc::new(10, 0)))
            .expect("register");
        assert_eq!(at(&mirror, 4, Hlc::new(10, 0)), CellRoute::Here);
        assert_eq!(
            at(&mirror, 3, Hlc::new(10, 0)),
            CellRoute::Moved(DatabaseId::new(4))
        );
    }

    /// A write for a dropped incarnation is superseded, even when a later
    /// incarnation of the same name holds the key.
    #[test]
    fn a_write_for_a_dropped_incarnation_is_superseded() {
        let mut mirror = ArrayCatalog::new();
        assert_eq!(at(&mirror, 3, Hlc::new(10, 0)), CellRoute::Superseded);
        mirror
            .register(row(3, "grid", Hlc::new(20, 0)))
            .expect("register");
        assert_eq!(at(&mirror, 3, Hlc::new(10, 0)), CellRoute::Superseded);
        assert_eq!(at(&mirror, 3, Hlc::new(20, 0)), CellRoute::Here);
    }

    #[test]
    fn an_unstamped_write_matches_by_key() {
        let mut mirror = ArrayCatalog::new();
        mirror
            .register(row(3, "grid", Hlc::new(20, 0)))
            .expect("register");
        assert_eq!(at(&mirror, 3, Hlc::ZERO), CellRoute::Here);
        assert_eq!(at(&mirror, 5, Hlc::ZERO), CellRoute::Superseded);
    }
}
