// SPDX-License-Identifier: BUSL-1.1

//! The collections a snapshot install clears, per core.
//!
//! A lagging follower's local catalog still lists collections dropped after
//! its lag point, and its engines still hold rows deleted before the snapshot
//! index. Clearing every in-group collection before the install removes both.
//! Every core clears every listed collection: a collection's rows live on its
//! home core, but its graph edges live on the cores of their endpoints. Only
//! the home core reclaims the collection's shared on-disk L1 files, so two
//! cores never race to unlink the same tree.

use std::collections::HashSet;

use nodedb_physical::physical_plan::SnapshotClearTarget;
use nodedb_types::id::{CollectionKey, DatabaseId, QualifiedCollection};

use crate::control::security::catalog::SystemCatalog;

use super::error::SnapshotInstallError;
use super::split::CoreMap;

/// One active collection whose vShard belongs to the installing group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCollection {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Bare catalog name.
    pub name: String,
}

/// Every active collection of the local catalog that routes into
/// `group_vshards`, across every database.
pub fn group_collections(
    group_id: u64,
    catalog: &SystemCatalog,
    group_vshards: &HashSet<u32>,
) -> Result<Vec<GroupCollection>, SnapshotInstallError> {
    if group_vshards.is_empty() {
        return Ok(Vec::new());
    }
    let collections = catalog
        .load_all_collections_across_databases()
        .map_err(|source| SnapshotInstallError::Catalog { group_id, source })?;
    Ok(collections
        .iter()
        .filter(|c| {
            c.is_active
                && group_vshards.contains(&nodedb_cluster::routing::vshard_for_collection(
                    CollectionKey::from_bare(c.database_id, &c.name),
                ))
        })
        .map(|c| GroupCollection {
            database_id: c.database_id.as_u64(),
            tenant_id: c.tenant_id,
            name: c.name.clone(),
        })
        .collect())
}

/// The clear list of each core: every collection on every core, with the L1
/// reclaim set on the collection's home core only.
pub fn clear_targets_per_core(
    group_id: u64,
    collections: &[GroupCollection],
    cores: &CoreMap,
) -> Result<Vec<Vec<SnapshotClearTarget>>, SnapshotInstallError> {
    let mut per_core: Vec<Vec<SnapshotClearTarget>> = vec![Vec::new(); cores.num_cores()];
    for coll in collections {
        let db = DatabaseId::new(coll.database_id);
        let home = cores.core_of(
            group_id,
            nodedb_cluster::routing::vshard_for_collection(CollectionKey::from_bare(
                db, &coll.name,
            )),
        )?;
        let stored = QualifiedCollection::new(db, &coll.name)
            .as_str()
            .to_string();
        for (core, targets) in per_core.iter_mut().enumerate() {
            targets.push(SnapshotClearTarget {
                database_id: coll.database_id,
                tenant_id: coll.tenant_id,
                collection: stored.clone(),
                reclaim_l1_files: core == home,
            });
        }
    }
    Ok(per_core)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::router::vshard::VShardRouter;

    #[test]
    fn every_core_clears_and_only_the_home_core_reclaims() {
        let cores = CoreMap::from_router(&VShardRouter::round_robin(4));
        let collections: Vec<GroupCollection> = (0..8)
            .map(|i| GroupCollection {
                database_id: if i % 2 == 0 { 0 } else { 1025 },
                tenant_id: 1,
                name: format!("c{i}"),
            })
            .collect();

        let per_core = clear_targets_per_core(3, &collections, &cores).unwrap();
        assert_eq!(per_core.len(), 4);
        for targets in &per_core {
            assert_eq!(targets.len(), collections.len());
        }
        for (i, coll) in collections.iter().enumerate() {
            let reclaiming: Vec<usize> = per_core
                .iter()
                .enumerate()
                .filter(|(_, t)| t[i].reclaim_l1_files)
                .map(|(core, _)| core)
                .collect();
            let db = DatabaseId::new(coll.database_id);
            let home = cores
                .core_of(
                    3,
                    CollectionKey::from_bare(db, &coll.name).vshard().as_u32(),
                )
                .unwrap();
            assert_eq!(reclaiming, vec![home], "collection {}", coll.name);
            assert_eq!(
                per_core[0][i].collection,
                QualifiedCollection::new(db, &coll.name).as_str()
            );
        }
    }
}
