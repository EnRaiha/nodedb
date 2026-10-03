// SPDX-License-Identifier: BUSL-1.1

//! Where each snapshot component lives under a data directory.
//!
//! Capture reads a component from these paths, and restore refuses any file
//! that falls outside them, so a snapshot cannot write anywhere else.

use std::path::{Component, Path, PathBuf};

use crate::data::snapshot::SnapshotComponent;

/// The system catalog redb file, relative to the data directory.
pub(crate) const SYSTEM_CATALOG_FILE: &str = "system.redb";

/// The cluster catalog redb file, relative to the data directory.
pub(crate) const CLUSTER_CATALOG_FILE: &str = "cluster.redb";

/// The directory of the Event Plane's durable redb stores.
pub(crate) const EVENT_PLANE_DIR: &str = "event_plane";

/// The directory of the array sync redb stores.
pub(crate) const ARRAY_SYNC_DIR: &str = "array_sync";

/// The WAL-wrapped CRDT signing root, relative to the data directory.
pub(crate) const CRDT_SIGNING_ROOT_FILE: &str = "wal/crdt_signing_root.enc";

/// The timeseries partition tree. Every core shares it: one core owns each
/// collection directory `ts/{database_id}/{tenant_id}/{collection}`.
const TIMESERIES_ROOT: &str = "ts";

/// Depth of a timeseries file below the data directory:
/// `ts/{database_id}/{tenant_id}/{collection}/…/{file}`.
const TIMESERIES_MIN_DEPTH: usize = 5;

/// The core's sparse redb store.
pub(crate) fn sparse_store_path(data_dir: &Path, core_id: usize) -> PathBuf {
    data_dir.join(format!("sparse/core-{core_id}.redb"))
}

/// The core's graph edge redb store.
pub(crate) fn graph_store_path(data_dir: &Path, core_id: usize) -> PathBuf {
    data_dir.join(format!("graph/core-{core_id}.redb"))
}

/// The core's array engine root.
pub(crate) fn array_root(data_dir: &Path, core_id: usize) -> PathBuf {
    data_dir.join(format!("array/core-{core_id}"))
}

/// The timeseries partition tree under `data_dir`.
pub(crate) fn timeseries_root(data_dir: &Path) -> PathBuf {
    data_dir.join(TIMESERIES_ROOT)
}

/// Whether the component is one file rather than a directory tree.
pub(crate) fn is_single_file(component: SnapshotComponent) -> bool {
    matches!(
        component,
        SnapshotComponent::SystemCatalog
            | SnapshotComponent::ClusterCatalog
            | SnapshotComponent::Sparse
            | SnapshotComponent::Graph
            | SnapshotComponent::WalKeys
    )
}

/// The file or directory a component occupies, relative to the data
/// directory. `core_id` is ignored for node-level components and for
/// timeseries, whose root every core shares.
pub(crate) fn component_root(component: SnapshotComponent, core_id: usize) -> PathBuf {
    let base = Path::new("");
    match component {
        SnapshotComponent::SystemCatalog => PathBuf::from(SYSTEM_CATALOG_FILE),
        SnapshotComponent::ClusterCatalog => PathBuf::from(CLUSTER_CATALOG_FILE),
        SnapshotComponent::EventPlane => PathBuf::from(EVENT_PLANE_DIR),
        SnapshotComponent::ArraySync => PathBuf::from(ARRAY_SYNC_DIR),
        SnapshotComponent::WalKeys => PathBuf::from(CRDT_SIGNING_ROOT_FILE),
        SnapshotComponent::Sparse => sparse_store_path(base, core_id),
        SnapshotComponent::Graph => graph_store_path(base, core_id),
        SnapshotComponent::Kv => super::super::kv_checkpoint::kv_ckpt_dir(base, core_id),
        SnapshotComponent::SparseVector => {
            super::super::sparse_vector_checkpoint::sparse_vector_ckpt_dir(base, core_id)
        }
        SnapshotComponent::SyncHwm => {
            super::super::sync_hwm_checkpoint::sync_hwm_ckpt_dir(base, core_id)
        }
        SnapshotComponent::Columnar => {
            super::super::columnar_checkpoint::columnar_ckpt_dir(base, core_id)
        }
        SnapshotComponent::GraphLabel => {
            super::super::graph_label_checkpoint::graph_label_ckpt_dir(base, core_id)
        }
        SnapshotComponent::Array => array_root(base, core_id),
        SnapshotComponent::Timeseries => timeseries_root(base),
        SnapshotComponent::Vector => {
            super::super::vector_checkpoint::vector_ckpt_dir(base, core_id)
        }
        SnapshotComponent::Crdt => super::super::crdt_checkpoint::crdt_ckpt_dir(base, core_id),
        SnapshotComponent::Spatial => {
            super::super::spatial_checkpoint::spatial_ckpt_dir(base, core_id)
        }
    }
}

/// Check that a snapshot file path is a plain relative path inside the root
/// its component owns, and return it.
///
/// `core_id` is `None` for a node-level file and the owning core for every
/// other component.
pub(crate) fn check_restore_path(
    component: SnapshotComponent,
    core_id: Option<usize>,
    path: &str,
) -> crate::Result<PathBuf> {
    let owner = match (component.is_node_level(), core_id) {
        (true, None) => 0,
        (false, Some(core_id)) => core_id,
        _ => {
            return Err(outside(
                component,
                path,
                "the component does not belong to this snapshot object",
            ));
        }
    };
    let rel = PathBuf::from(path);
    let plain = !path.is_empty()
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
        && rel.components().all(|c| matches!(c, Component::Normal(_)));
    if !plain {
        return Err(outside(
            component,
            path,
            "the path is not plain and relative",
        ));
    }

    let root = component_root(component, owner);
    let inside = if is_single_file(component) {
        rel == root
    } else if matches!(
        component,
        SnapshotComponent::EventPlane | SnapshotComponent::ArraySync
    ) {
        // One redb file directly under the store directory.
        rel.parent() == Some(root.as_path())
    } else if component == SnapshotComponent::Timeseries {
        rel.starts_with(&root) && rel.components().count() >= TIMESERIES_MIN_DEPTH
    } else {
        rel.starts_with(&root) && rel != root
    };
    if !inside {
        return Err(outside(
            component,
            path,
            &format!("the component owns {}", root.display()),
        ));
    }
    Ok(rel)
}

fn outside(component: SnapshotComponent, path: &str, why: &str) -> crate::Error {
    crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("refusing snapshot file {path:?} of {component:?}: {why}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_match_the_paths_the_engines_open() {
        let dir = Path::new("/data");
        assert_eq!(
            dir.join(component_root(SnapshotComponent::Sparse, 3)),
            sparse_store_path(dir, 3)
        );
        assert_eq!(
            dir.join(component_root(SnapshotComponent::Kv, 3)),
            super::super::super::kv_checkpoint::kv_ckpt_dir(dir, 3)
        );
        assert_eq!(
            dir.join(component_root(SnapshotComponent::Array, 3)),
            array_root(dir, 3)
        );
        assert_eq!(
            component_root(SnapshotComponent::SystemCatalog, 3),
            PathBuf::from("system.redb")
        );
    }

    #[test]
    fn a_file_inside_its_root_is_accepted() {
        assert!(
            check_restore_path(SnapshotComponent::Sparse, Some(1), "sparse/core-1.redb").is_ok()
        );
        assert!(
            check_restore_path(SnapshotComponent::Kv, Some(1), "kv-ckpt/core-1/MANIFEST").is_ok()
        );
        assert!(
            check_restore_path(
                SnapshotComponent::Timeseries,
                Some(0),
                "ts/0/1/metrics/p-1/partition.meta"
            )
            .is_ok()
        );
        assert!(check_restore_path(SnapshotComponent::SystemCatalog, None, "system.redb").is_ok());
        assert!(
            check_restore_path(
                SnapshotComponent::EventPlane,
                None,
                "event_plane/mv_state.redb"
            )
            .is_ok()
        );
        assert!(
            check_restore_path(SnapshotComponent::ArraySync, None, "array_sync/op_log.redb")
                .is_ok()
        );
        assert!(
            check_restore_path(
                SnapshotComponent::WalKeys,
                None,
                "wal/crdt_signing_root.enc"
            )
            .is_ok()
        );
    }

    #[test]
    fn a_file_outside_its_root_is_refused() {
        for (component, core, path) in [
            (SnapshotComponent::Sparse, Some(1), "sparse/core-0.redb"),
            (SnapshotComponent::Kv, Some(1), "kv-ckpt/core-0/MANIFEST"),
            (SnapshotComponent::Kv, Some(1), "kv-ckpt/core-1"),
            (
                SnapshotComponent::Kv,
                Some(1),
                "kv-ckpt/core-1/../../system.redb",
            ),
            (SnapshotComponent::Kv, Some(1), "/kv-ckpt/core-1/MANIFEST"),
            (SnapshotComponent::Kv, Some(1), "kv-ckpt//core-1/MANIFEST"),
            (SnapshotComponent::Timeseries, Some(0), "ts/0/1/metrics"),
            (SnapshotComponent::SystemCatalog, Some(0), "system.redb"),
            (SnapshotComponent::Sparse, None, "sparse/core-0.redb"),
            (SnapshotComponent::SystemCatalog, None, "wal/wal-1.seg"),
            (SnapshotComponent::EventPlane, None, "event_plane/a/b.redb"),
            (
                SnapshotComponent::EventPlane,
                Some(0),
                "event_plane/mv_state.redb",
            ),
            (
                SnapshotComponent::ArraySync,
                None,
                "event_plane/op_log.redb",
            ),
            (
                SnapshotComponent::WalKeys,
                None,
                "wal/wal-00000000000000000001.seg",
            ),
            (
                SnapshotComponent::WalKeys,
                Some(0),
                "wal/crdt_signing_root.enc",
            ),
        ] {
            assert!(
                check_restore_path(component, core, path).is_err(),
                "{component:?} {core:?} {path} must be refused"
            );
        }
    }
}
