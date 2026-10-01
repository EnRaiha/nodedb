// SPDX-License-Identifier: BUSL-1.1

//! Execute one node's part of a cluster restore.
//!
//! The base and the archived WAL go in as a node restore writes them. Then
//! every Raft group of the node starts a new log at its place at the point,
//! all at the term of the restore's generation, and the metadata group's log
//! holds the entries after the base. The cluster epoch starts at the same
//! value. The node's TLS directory is kept.

use std::path::{Path, PathBuf};

use nodedb_cluster::calvin::{SEQUENCER_GROUP_ID, SequencerEntry};
use nodedb_cluster::raft_bootstrap::{GroupLogStart, group_log_path, start_group_log};
use nodedb_cluster::{ClusterCatalog, METADATA_GROUP_ID};
use nodedb_raft::message::LogEntry;

use super::super::archive::Archive;
use super::super::env::RestoreEnv;
use super::super::error::RestoreError;
use super::super::execute::{RestoreOutcome, execute_plan};
use super::super::life::Life;
use super::plan::{ClusterRestorePlan, GroupPlace};
use crate::control::cluster::metadata_image::{
    EpochPolicy, capture_metadata_image, install_metadata_image_offline,
};
use crate::control::cluster::tls::TLS_SUBDIR;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::tenant_group_marks::StoredGroupMark;
use crate::control::state::tenant_marks::MarkSite;
use crate::data::executor::snapshot::layout::{CLUSTER_CATALOG_FILE, SYSTEM_CATALOG_FILE};
use crate::event::cdc::OffsetStore;
use crate::storage::metadata_timeline::{TimelineBranch, cluster_restore_timeline, put_branch};
use crate::storage::restore_generation::{claim_generation, fence_of, write_generation_marker};
use crate::storage::snapshot_files::clear_dir_contents;

/// What a cluster restore wrote.
#[derive(Debug, Clone)]
pub struct ClusterOutcome {
    pub node: RestoreOutcome,
    pub generation: u64,
    /// The term every group starts at, and the cluster epoch.
    pub fence: u64,
    /// Groups this node hosts in the restored routing table that recorded
    /// no place at the point. Each starts with no log and catches up from
    /// its leader.
    pub unrecorded_groups: Vec<u64>,
}

/// Write `plan` into `env.data_dir`. A failure leaves the directory holding
/// only its TLS directory.
pub async fn execute_cluster_plan(
    env: &RestoreEnv,
    life: &Life,
    archive: &Archive<'_>,
    plan: &ClusterRestorePlan,
) -> Result<ClusterOutcome, RestoreError> {
    let tls = TlsParking::park(&env.data_dir)?;
    let generation = match claim_generation(
        &env.cold.object_store(),
        env.cold.prefix(),
        plan.restore_point,
    )
    .await
    {
        Ok(generation) => generation,
        Err(e) => return Err(tls.restore_after(e.into())),
    };
    // The restored cluster writes the generation's timeline, which branches
    // off the restored one at the point. Every node writes the same record.
    let branch = TimelineBranch {
        timeline: cluster_restore_timeline(generation),
        parent: plan.wal.base.metadata_timeline,
        branch_index: plan.restore_point,
    };
    if let Err(e) = put_branch(
        &env.cold.object_store(),
        env.cold.prefix(),
        branch,
        &format!("node-{}", env.node_id),
        &env.key,
    )
    .await
    {
        return Err(tls.restore_after(e.into()));
    }
    let node = match execute_plan(env, life, archive, &plan.wal).await {
        Ok(node) => node,
        Err(e) => return Err(tls.restore_after(e)),
    };
    let tls_back = tls.unpark();
    let finished = tls_back.and_then(|()| finish(env, plan, generation));
    match finished {
        Ok((fence, unrecorded_groups)) => Ok(ClusterOutcome {
            node,
            generation,
            fence,
            unrecorded_groups,
        }),
        Err(error) => Err(clear_keeping_tls(&env.data_dir, error)),
    }
}

/// Everything after the base and the WAL: the catalogs at the new epoch,
/// the group logs, and the generation marker.
fn finish(
    env: &RestoreEnv,
    plan: &ClusterRestorePlan,
    generation: u64,
) -> Result<(u64, Vec<u64>), RestoreError> {
    let fence = fence_of(generation)?;
    let data_dir = &env.data_dir;
    let (metadata_entries, points) = plan.metadata.restored_entries(fence, plan.watermark);

    let image = {
        let system = SystemCatalog::open(&data_dir.join(SYSTEM_CATALOG_FILE))?;
        system.put_metadata_timeline(cluster_restore_timeline(generation))?;
        for point in &points {
            system.put_restore_point(point)?;
        }
        let cluster = ClusterCatalog::open(&data_dir.join(CLUSTER_CATALOG_FILE)).map_err(|e| {
            crate::Error::Internal {
                detail: format!("open restored cluster catalog: {e}"),
            }
        })?;
        let offsets = OffsetStore::open(data_dir)?;
        let mut image = capture_metadata_image(
            &system,
            &cluster,
            &offsets,
            &data_dir.join(TLS_SUBDIR),
            plan.metadata.base_index,
            fence,
        )?;
        image.cluster_epoch = fence;
        image
    };
    // The image carries the generation's fence as its epoch: above every
    // epoch the cluster reached before, so a node the restore missed reads
    // as behind and stands down.
    install_metadata_image_offline(data_dir, &image, EpochPolicy::UseImage)?;
    rebuild_node_counters(data_dir, plan)?;

    start_group_log(
        &group_log_path(data_dir, METADATA_GROUP_ID),
        &GroupLogStart {
            snapshot_index: plan.metadata.base_index,
            snapshot_term: fence,
            current_term: fence,
            entries: &metadata_entries,
        },
    )
    .map_err(cluster_err)?;
    for place in &plan.groups {
        start_group(data_dir, place, fence)?;
    }

    let recorded: Vec<u64> = plan.groups.iter().map(|place| place.group_id).collect();
    let mut unrecorded_groups: Vec<u64> = image
        .routing_table()?
        .group_members()
        .iter()
        .filter(|(group_id, info)| {
            **group_id != METADATA_GROUP_ID
                && !recorded.contains(*group_id)
                && (info.members.contains(&env.node_id) || info.learners.contains(&env.node_id))
        })
        .map(|(group_id, _)| *group_id)
        .collect();
    unrecorded_groups.sort_unstable();

    write_generation_marker(data_dir, generation)?;
    Ok((fence, unrecorded_groups))
}

/// The counters a restore sets at its point, in the restored catalog.
///
/// - Surrogates: the high-water mark rises to the highest surrogate ever
///   issued, after the point too, so no surrogate is issued twice. Every
///   reservation at or below the point counts as applied.
/// - Tenant write marks: each is the newest kept write the replay holds, or
///   the base's mark when that is newer. A base mark at or above the
///   watermark belongs to a dropped write, and stops below it.
fn rebuild_node_counters(data_dir: &Path, plan: &ClusterRestorePlan) -> Result<(), RestoreError> {
    let system = SystemCatalog::open(&data_dir.join(SYSTEM_CATALOG_FILE))?;

    let hwm = system.get_surrogate_hwm()?;
    let reserve_index = system.get_surrogate_reserve_index()?;
    let ceiling = plan
        .surrogates
        .ceiling(hwm, reserve_index)
        .max(plan.wal.surrogate_hwm.unwrap_or(0));
    system.put_surrogate_reserve_state(ceiling.max(hwm), reserve_index.max(plan.restore_point))?;

    let below = plan.watermark.saturating_sub(1);
    let mut marks: Vec<StoredGroupMark> = system
        .load_tenant_group_marks()?
        .into_iter()
        .map(|mark| StoredGroupMark {
            hlc: mark.hlc.min(below),
            ..mark
        })
        .collect();
    for (&(group_id, tenant_id), &hlc) in &plan.replayed_marks {
        match marks
            .iter_mut()
            .find(|m| m.group_id == group_id && m.tenant_id == tenant_id && m.restore_id == 0)
        {
            Some(mark) => mark.hlc = mark.hlc.max(hlc),
            None => marks.push(StoredGroupMark {
                group_id,
                tenant_id,
                hlc,
                site: MarkSite::ReplicatedApply.code(),
                collection: String::new(),
                restore_id: 0,
            }),
        }
    }
    system.replace_tenant_group_marks(&marks)?;
    Ok(())
}

/// Start `place`'s group log at its index. The sequencer's log opens with the
/// epoch that followed the point, so it mints no restored epoch again.
fn start_group(data_dir: &Path, place: &GroupPlace, fence: u64) -> Result<(), RestoreError> {
    let entries = if place.group_id == SEQUENCER_GROUP_ID {
        let data = zerompk::to_msgpack_vec(&SequencerEntry::EpochFloor {
            next_epoch: place.next_epoch,
            epoch_system_ms: i64::try_from(place.epoch_system_ms).unwrap_or(i64::MAX),
        })
        .map_err(|e| crate::Error::Internal {
            detail: format!("encode sequencer epoch floor: {e}"),
        })?;
        vec![LogEntry {
            term: fence,
            index: place.index + 1,
            data,
        }]
    } else {
        Vec::new()
    };
    start_group_log(
        &group_log_path(data_dir, place.group_id),
        &GroupLogStart {
            snapshot_index: place.index,
            snapshot_term: fence,
            current_term: fence,
            entries: &entries,
        },
    )
    .map_err(cluster_err)
}

fn cluster_err(e: nodedb_cluster::ClusterError) -> RestoreError {
    crate::Error::Internal {
        detail: format!("start restored raft log: {e}"),
    }
    .into()
}

/// Empty `data_dir` except its TLS directory, after `error`.
fn clear_keeping_tls(data_dir: &Path, error: RestoreError) -> RestoreError {
    let cleanup = TlsParking::park(data_dir).and_then(|tls| match clear_dir_contents(data_dir) {
        Ok(()) => tls.unpark(),
        Err(e) => Err(tls.restore_after(e.into())),
    });
    match cleanup {
        Ok(()) => error,
        Err(cleanup) => RestoreError::CleanupFailed {
            error: Box::new(error),
            cleanup: Box::new(cleanup),
            data_dir: data_dir.to_path_buf(),
        },
    }
}

/// The node's TLS directory, moved beside the data directory while the
/// restore needs the data directory empty.
struct TlsParking {
    tls: PathBuf,
    parked: Option<PathBuf>,
}

impl TlsParking {
    fn park(data_dir: &Path) -> Result<Self, RestoreError> {
        // no-objectstore: the TLS directory lives in the local data directory.
        let tls = data_dir.join(TLS_SUBDIR);
        let name = data_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parked = data_dir.with_file_name(format!(".{name}.restore-tls"));
        if parked.exists() {
            return Err(RestoreError::Usage {
                detail: format!(
                    "{} holds the TLS directory an interrupted restore moved aside; move it \
                     back to {} and run the restore again",
                    parked.display(),
                    tls.display()
                ),
            });
        }
        if !tls.exists() {
            return Ok(Self { tls, parked: None });
        }
        std::fs::rename(&tls, &parked).map_err(crate::Error::Io)?;
        Ok(Self {
            tls,
            parked: Some(parked),
        })
    }

    fn unpark(self) -> Result<(), RestoreError> {
        let Some(parked) = self.parked else {
            return Ok(());
        };
        // no-objectstore: the TLS directory lives in the local data directory.
        if let Some(parent) = self.tls.parent() {
            std::fs::create_dir_all(parent).map_err(crate::Error::Io)?;
        }
        std::fs::rename(&parked, &self.tls).map_err(crate::Error::Io)?;
        Ok(())
    }

    /// Move the TLS directory back after `error`.
    fn restore_after(self, error: RestoreError) -> RestoreError {
        let data_dir = self.tls.parent().map(Path::to_path_buf).unwrap_or_default();
        match self.unpark() {
            Ok(()) => error,
            Err(cleanup) => RestoreError::CleanupFailed {
                error: Box::new(error),
                cleanup: Box::new(cleanup),
                data_dir,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tls_directory_survives_a_failed_restore() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(data_dir.join(TLS_SUBDIR)).unwrap();
        std::fs::write(data_dir.join(TLS_SUBDIR).join("node.crt"), b"cert").unwrap();

        let parked = TlsParking::park(&data_dir).unwrap();
        assert!(!data_dir.join(TLS_SUBDIR).exists());
        assert!(
            matches!(TlsParking::park(&data_dir), Err(RestoreError::Usage { .. })),
            "a second restore refuses while the TLS directory is moved aside"
        );
        std::fs::write(data_dir.join("system.redb"), b"partial").unwrap();
        let error = parked.restore_after(RestoreError::Usage {
            detail: "failed".into(),
        });
        assert!(matches!(error, RestoreError::Usage { .. }));

        let error = clear_keeping_tls(&data_dir, error);
        assert!(matches!(error, RestoreError::Usage { .. }));
        assert!(!data_dir.join("system.redb").exists());
        assert_eq!(
            std::fs::read(data_dir.join(TLS_SUBDIR).join("node.crt")).unwrap(),
            b"cert"
        );
    }

    #[test]
    fn the_sequencer_log_opens_at_its_epoch_floor() {
        let dir = tempfile::tempdir().unwrap();
        let fence = 1 << 40;
        start_group(
            dir.path(),
            &GroupPlace {
                group_id: SEQUENCER_GROUP_ID,
                index: 30,
                next_epoch: 12,
                epoch_system_ms: 1_900_000_000_000,
            },
            fence,
        )
        .unwrap();
        start_group(
            dir.path(),
            &GroupPlace {
                group_id: 4,
                index: 17,
                next_epoch: 0,
                epoch_system_ms: 0,
            },
            fence,
        )
        .unwrap();

        use nodedb_raft::storage::LogStorage;
        let sequencer = nodedb_cluster::raft_storage::RedbLogStorage::open(&group_log_path(
            dir.path(),
            SEQUENCER_GROUP_ID,
        ))
        .unwrap();
        assert_eq!(sequencer.snapshot_metadata(), (30, fence));
        let entries = sequencer.load_entries_after(30).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 31);
        assert!(matches!(
            zerompk::from_msgpack::<SequencerEntry>(&entries[0].data).unwrap(),
            SequencerEntry::EpochFloor {
                next_epoch: 12,
                epoch_system_ms: 1_900_000_000_000,
            }
        ));

        let data =
            nodedb_cluster::raft_storage::RedbLogStorage::open(&group_log_path(dir.path(), 4))
                .unwrap();
        assert_eq!(data.snapshot_metadata(), (17, fence));
        assert!(data.load_entries_after(17).unwrap().is_empty());
        assert_eq!(data.load_applied_index().unwrap(), 17);
    }
}
