// SPDX-License-Identifier: BUSL-1.1

//! Install a group 0 [`MetadataImage`].
//!
//! [`write_metadata_image`] is the durable half both installs share: the
//! replicated `_system.*` tables in one write transaction, the committed
//! consumer offsets, the CA trust set, and the routing and cluster epoch
//! keys of the cluster catalog. A crash part-way leaves the staged snapshot
//! install in place, and boot runs the install again from the start.
//!
//! [`install_metadata_image_offline`] writes a plain data directory no
//! server has open. [`install_metadata_image`] installs into a running node:
//! after the durable write it rebuilds every registry the catalog feeds and
//! reconciles the Data Plane with the new catalog.

use std::path::Path;
use std::sync::Arc;

use nodedb_cluster::{ClusterCatalog, METADATA_GROUP_ID, RoutingTable};

use crate::control::cluster::ca_trust::replace_trusted_cas;
use crate::control::cluster::tls::TLS_SUBDIR;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::data::executor::snapshot::layout::{CLUSTER_CATALOG_FILE, SYSTEM_CATALOG_FILE};
use crate::event::cdc::OffsetStore;

use super::format::{MetadataImage, decode_metadata_image};
use super::inventory::Inventory;
use super::reconcile::reconcile_data_plane;
use super::reload::{RaftOwnedState, reload_registries};

fn cluster_err(what: &str) -> impl Fn(nodedb_cluster::ClusterError) -> crate::Error + '_ {
    move |e| crate::Error::Internal {
        detail: format!("metadata image: {what}: {e}"),
    }
}

/// How an install sets the cluster epoch the cluster catalog holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochPolicy {
    /// Keep the higher of the stored epoch and the image's. A live install
    /// uses this: a node never stamps an older generation than it applied.
    KeepHigher,
    /// Store the image's epoch, even when it is lower than the stored one.
    /// For a restore that rewinds the whole cluster to the image.
    UseImage,
}

/// Write `image` durably: the replicated tables, the consumer offsets, the
/// CA trust set, `routing`, and the cluster epoch under `epoch`.
pub fn write_metadata_image(
    system: &SystemCatalog,
    cluster: &ClusterCatalog,
    offsets: &OffsetStore,
    tls_dir: &Path,
    image: &MetadataImage,
    routing: &RoutingTable,
    epoch: EpochPolicy,
) -> crate::Result<()> {
    system.replace_replicated_tables(&image.tables)?;
    offsets.replace_all_offsets(&image.consumer_offsets)?;
    replace_trusted_cas(tls_dir, &image.trusted_cas)?;
    cluster
        .save_routing(routing)
        .map_err(cluster_err("save routing"))?;
    let persisted = cluster
        .load_cluster_epoch()
        .map_err(cluster_err("load cluster epoch"))?
        .unwrap_or(0);
    let write = match epoch {
        EpochPolicy::KeepHigher => image.cluster_epoch > persisted,
        EpochPolicy::UseImage => image.cluster_epoch != persisted,
    };
    if write {
        cluster
            .save_cluster_epoch(image.cluster_epoch)
            .map_err(cluster_err("save cluster epoch"))?;
    }
    Ok(())
}

/// Install `image` into the catalogs of the node data directory `data_dir`.
///
/// No server can have the directory open. The Raft logs are not touched.
/// `epoch` sets the cluster epoch rule: a point-in-time restore that rewinds
/// every node passes [`EpochPolicy::UseImage`]; a restore that must never
/// lower the generation passes [`EpochPolicy::KeepHigher`].
pub fn install_metadata_image_offline(
    data_dir: &Path,
    image: &MetadataImage,
    epoch: EpochPolicy,
) -> crate::Result<()> {
    let system = SystemCatalog::open(&data_dir.join(SYSTEM_CATALOG_FILE))?;
    let cluster = ClusterCatalog::open(&data_dir.join(CLUSTER_CATALOG_FILE))
        .map_err(cluster_err("open cluster catalog"))?;
    let offsets = OffsetStore::open(data_dir)?;
    write_metadata_image(
        &system,
        &cluster,
        &offsets,
        &data_dir.join(TLS_SUBDIR),
        image,
        &image.routing_table()?,
        epoch,
    )
}

/// `image_routing` with this node's own view of every data group it is a
/// member or learner of. Those groups apply their own conf changes on this
/// node, so their membership here is at least as new as the image's.
pub fn merge_routing(
    local: &RoutingTable,
    image_routing: RoutingTable,
    node_id: u64,
) -> RoutingTable {
    let mut merged = image_routing;
    for (group_id, info) in local.group_members() {
        if *group_id == METADATA_GROUP_ID {
            continue;
        }
        if info.members.contains(&node_id) || info.learners.contains(&node_id) {
            merged.set_group_members(*group_id, info.members.clone());
            merged.set_group_learners(*group_id, info.learners.clone());
        }
    }
    merged
}

/// Install an encoded image into this running node.
///
/// The Raft snapshot applier calls this under the group 0 install gate, so
/// no metadata entry applies until it returns. Only then does the caller
/// adopt the snapshot index.
pub async fn install_metadata_image(
    shared: &Arc<SharedState>,
    cluster: &ClusterCatalog,
    raft: &RaftOwnedState,
    bytes: &[u8],
) -> crate::Result<()> {
    let image = decode_metadata_image(bytes)?;
    let catalog = shared.credentials.catalog();
    let before = Inventory::read(catalog)?;

    let live_routing = shared
        .cluster_routing
        .as_ref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "metadata snapshot install: this node has no routing table".into(),
        })?;
    let routing = {
        let local = live_routing.read().unwrap_or_else(|p| p.into_inner());
        merge_routing(&local, image.routing_table()?, shared.node_id)
    };
    write_metadata_image(
        catalog,
        cluster,
        &shared.offset_store,
        &shared.data_dir.join(TLS_SUBDIR),
        &image,
        &routing,
        EpochPolicy::KeepHigher,
    )?;
    *live_routing.write().unwrap_or_else(|p| p.into_inner()) = routing;
    // The image's entries never apply here, so their stamps reach the clock
    // only through the high-water the image carries.
    crate::control::cluster::metadata_stamp::fold_metadata_stamp_hwm(shared)?;
    if let Some(epoch) = shared.cluster_epoch.get() {
        epoch.advance_applied(image.cluster_epoch);
    }

    let after = Inventory::read(catalog)?;
    reload_registries(shared, raft, &before, &after).await?;
    reconcile_data_plane(shared, &before, &after).await?;
    tracing::info!(
        applied_index = image.applied_index,
        tables = image.tables.len(),
        "installed metadata snapshot image"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_types::DatabaseId;

    /// A data group this node belongs to keeps its local membership; the
    /// metadata group and foreign groups take the image's.
    #[test]
    fn merge_keeps_local_membership_of_hosted_data_groups() {
        let mut local = RoutingTable::uniform(2, &[1, 2, 3], 2);
        local.set_group_members(1, vec![1, 3]);
        local.set_group_members(0, vec![1]);
        let mut image = RoutingTable::uniform(2, &[1, 2, 3], 2);
        image.set_group_members(1, vec![2, 3]);
        image.set_group_members(0, vec![1, 2, 3]);
        let merged = merge_routing(&local, image, 1);
        assert_eq!(merged.group_info(1).unwrap().members, vec![1, 3]);
        assert_eq!(merged.group_info(0).unwrap().members, vec![1, 2, 3]);
    }

    /// An image captured from one data directory installs offline into
    /// another: the replicated tables, offsets, routing, and epoch arrive.
    #[test]
    fn offline_capture_then_install_copies_the_image() {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let routing = RoutingTable::uniform(2, &[1, 2], 2);
        let image = {
            let system = SystemCatalog::open(&source.path().join(SYSTEM_CATALOG_FILE)).unwrap();
            system
                .put_collection(
                    DatabaseId::DEFAULT,
                    &StoredCollection::stamped_for_test(1, "orders", "admin"),
                )
                .unwrap();
            let cluster = ClusterCatalog::open(&source.path().join(CLUSTER_CATALOG_FILE)).unwrap();
            cluster.save_routing(&routing).unwrap();
            cluster.save_cluster_epoch(4).unwrap();
            let offsets = OffsetStore::open(source.path()).unwrap();
            super::super::capture::capture_metadata_image(
                &system,
                &cluster,
                &offsets,
                &source.path().join(TLS_SUBDIR),
                17,
                2,
            )
            .unwrap()
        };

        install_metadata_image_offline(target.path(), &image, EpochPolicy::UseImage).unwrap();

        let system = SystemCatalog::open(&target.path().join(SYSTEM_CATALOG_FILE)).unwrap();
        assert!(
            system
                .get_collection(DatabaseId::DEFAULT, 1, "orders")
                .unwrap()
                .is_some()
        );
        let cluster = ClusterCatalog::open(&target.path().join(CLUSTER_CATALOG_FILE)).unwrap();
        assert_eq!(cluster.load_cluster_epoch().unwrap(), Some(4));
        assert_eq!(
            cluster.load_routing().unwrap().unwrap().num_groups(),
            routing.num_groups()
        );
        assert_eq!(image.applied_index, 17);
        assert_eq!(image.applied_term, 2);
    }
}
