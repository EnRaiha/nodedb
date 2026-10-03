// SPDX-License-Identifier: BUSL-1.1

//! The metadata group's part of a node restore.
//!
//! A base holds the catalogs at its metadata applied index. A node restore
//! brings them to the target by the rule the cluster restore uses: an
//! archived metadata log entry after the base survives when its HLC stamp is
//! below the target's time. The restored metadata log starts at
//! the base's index and holds every entry through the last surviving one,
//! each dropped entry emptied, for the next boot to apply.
//!
//! Boot replays the WAL before it applies the metadata log, and replays a
//! strict collection's rows by the collection's catalog config. So the
//! restore also writes every surviving catalog change into the restored
//! catalog. Boot's apply of the same entries finds them applied.
//!
//! A leader stamps metadata entries in log order, so the archive covers every
//! stamp up to the newest one it holds. A target past that frontier is
//! refused: the archive can lack a catalog change the target keeps.
//!
//! The restored node writes a new metadata timeline that branches off the
//! restored one at the last index it keeps (see
//! `crate::storage::metadata_timeline`), so its history never mixes with
//! the entries the old timeline writes after that index.

use std::collections::BTreeMap;
use std::path::Path;

use nodedb_cluster::metadata_group::codec::decode_entry;
use nodedb_cluster::raft_bootstrap::{GroupLogStart, group_log_path, start_group_log};
use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, PendingDdlObject, entry_stamp};

use super::cluster::metadata::{MetadataTail, SurrogateLog, fetch_metadata_after};
use super::env::RestoreEnv;
use super::error::RestoreError;
use super::plan::RestorePlan;
use super::timeline::target_ns;
use crate::control::catalog_entry;
use crate::control::catalog_entry::descriptor_validate::{ValidationOutcome, validate};
use crate::control::security::catalog::SystemCatalog;
use crate::data::executor::snapshot::layout::SYSTEM_CATALOG_FILE;
use crate::storage::metadata_timeline::{TimelineBranch, mint_node_restore_timeline, put_branch};
use crate::storage::raft_log_archive::fetch_all_frontiers;

/// The metadata log a node restore replays after its base.
#[derive(Debug, Clone)]
pub struct NodeMetadata {
    /// The target's time, HLC nanoseconds. An entry stamped at or after it
    /// is dropped.
    pub watermark: u64,
    /// Entries after the base, through the last one stamped below the
    /// watermark.
    pub tail: MetadataTail,
    /// Index of the last entry the restored log holds.
    pub through: u64,
    /// Term of the entry at `through`. The restored log starts at it.
    pub term: u64,
    /// Every surrogate reservation the archived log holds after the base.
    pub surrogates: SurrogateLog,
    /// Newest HLC the metadata log archive covers, nanoseconds.
    pub archived_through_ns: u64,
    /// The metadata timeline the restore reads. The restored node starts a
    /// new timeline that branches off it at `through`.
    pub timeline: u64,
}

/// What the metadata log asks of a node restore's plan.
pub(super) enum MetadataStep {
    /// The base holds no cluster catalogs: there is no metadata log.
    Absent,
    Ready(NodeMetadata),
    /// The base's catalogs can hold entry `index`, which the restore drops.
    /// Only an older base serves.
    BaseHolds {
        index: u64,
    },
}

/// Plan the metadata log `plan` replays after its base.
pub(super) async fn plan_node_metadata(
    env: &RestoreEnv,
    plan: &RestorePlan,
) -> Result<MetadataStep, RestoreError> {
    let base_index = plan.base.metadata_applied_index;
    if base_index == 0 {
        return Ok(MetadataStep::Absent);
    }
    let watermark = match plan.requested_us {
        Some(micros) => target_ns(micros),
        None => plan
            .target_commit_ns
            .ok_or(RestoreError::TargetTimeUnknown {
                target_lsn: plan.target_lsn,
            })?,
    };
    let timeline = plan.base.metadata_timeline;
    let store = env.cold.object_store();
    let archived_through_ns = fetch_all_frontiers(&store, env.cold.prefix(), timeline, &env.key)
        .await?
        .iter()
        .map(|frontier| frontier.stamped_through_ns)
        .max()
        .unwrap_or(0);
    if archived_through_ns < watermark {
        return Err(RestoreError::MetadataNotArchived {
            target_ns: watermark,
            archived_through_ns,
        });
    }
    // The entry at the base's index gives the term when no entry follows.
    let archived = fetch_metadata_after(&env.cold, &env.key, timeline, base_index - 1).await?;
    if let Some(index) = archived.first_dropped(plan.base.metadata_captured_index, watermark) {
        return Ok(MetadataStep::BaseHolds { index });
    }
    let through = archived
        .entries
        .iter()
        .rev()
        .find(|(_, entry)| entry_stamp(&entry.data).is_some_and(|stamp| stamp < watermark))
        .map_or(base_index, |(index, _)| *index)
        .max(base_index);
    let term = archived
        .entries
        .get(&through)
        .map(|entry| entry.term)
        .ok_or(RestoreError::MetadataLogGap {
            from: base_index,
            through,
            missing: through,
        })?;
    Ok(MetadataStep::Ready(NodeMetadata {
        watermark,
        tail: archived.tail(base_index, through)?,
        through,
        term,
        surrogates: archived.surrogates(),
        archived_through_ns,
        timeline,
    }))
}

/// Start the restored node's metadata timeline: mint it, and record in cold
/// storage that it branches off `meta.timeline` at `meta.through`. Returns
/// the new timeline.
pub(super) async fn branch_node_timeline(
    env: &RestoreEnv,
    meta: &NodeMetadata,
) -> Result<u64, RestoreError> {
    let timeline = mint_node_restore_timeline()?;
    put_branch(
        &env.cold.object_store(),
        env.cold.prefix(),
        TimelineBranch {
            timeline,
            parent: meta.timeline,
            branch_index: meta.through,
        },
        &format!("node-{}", env.node_id),
        &env.key,
    )
    .await?;
    Ok(timeline)
}

/// Bring the restored catalog in `data_dir` to the target on `timeline`, and
/// start the metadata log after the base with the kept entries.
/// `wal_surrogate_hwm` is the highest surrogate the archived WAL reserved.
pub(super) fn install_node_metadata(
    data_dir: &Path,
    meta: &NodeMetadata,
    timeline: u64,
    wal_surrogate_hwm: Option<u32>,
) -> Result<(), RestoreError> {
    let (entries, points) = meta.tail.restored_entries(meta.term, meta.watermark);
    {
        let system = SystemCatalog::open(&data_dir.join(SYSTEM_CATALOG_FILE))?;
        system.put_metadata_timeline(timeline)?;
        for point in &points {
            system.put_restore_point(point)?;
        }
        let mut replay = CatalogReplay::new(&system)?;
        for entry in entries.iter().filter(|entry| !entry.data.is_empty()) {
            let decoded = decode_entry(&entry.data).map_err(|e| crate::Error::Internal {
                detail: format!("decode archived metadata log entry {}: {e}", entry.index),
            })?;
            replay.apply(&decoded)?;
        }
        // Every reservation the archive holds counts, after the target too,
        // so no surrogate is issued twice. Every one through the restored
        // log's end counts as applied.
        let hwm = system.get_surrogate_hwm()?;
        let reserve_index = system.get_surrogate_reserve_index()?;
        let ceiling = meta
            .surrogates
            .ceiling(hwm, reserve_index)
            .max(wal_surrogate_hwm.unwrap_or(0));
        system.put_surrogate_reserve_state(ceiling.max(hwm), reserve_index.max(meta.through))?;
    }
    start_group_log(
        &group_log_path(data_dir, METADATA_GROUP_ID),
        &GroupLogStart {
            snapshot_index: meta.tail.base_index,
            snapshot_term: meta.term,
            current_term: meta.term,
            entries: &entries,
        },
    )
    .map_err(|e| crate::Error::Internal {
        detail: format!("start the restored metadata log: {e}"),
    })?;
    Ok(())
}

/// The catalog changes of kept metadata entries, written offline in log
/// order. It follows the applier's rules for the entries that carry catalog
/// changes: the DDL preparation lease decides which prepared DDL applies, and
/// a pending DDL's objects apply at its finalize. The lease and pending
/// records are tracked here and never written: boot's apply writes them.
struct CatalogReplay<'a> {
    catalog: &'a SystemCatalog,
    owner: Option<u64>,
    pending: BTreeMap<u64, Vec<PendingDdlObject>>,
}

impl<'a> CatalogReplay<'a> {
    fn new(catalog: &'a SystemCatalog) -> crate::Result<Self> {
        let pending = catalog
            .load_pending_ddl()?
            .into_iter()
            .map(|record| (record.token, record.objects))
            .collect();
        Ok(Self {
            catalog,
            owner: catalog.load_ddl_owner()?.map(|(token, _)| token),
            pending,
        })
    }

    fn apply(&mut self, entry: &MetadataEntry) -> crate::Result<()> {
        match entry {
            MetadataEntry::CatalogDdl { payload }
            | MetadataEntry::CatalogDdlAudited { payload, .. } => self.apply_payload(payload),
            MetadataEntry::Batch { entries } => {
                for sub in entries {
                    self.apply(sub)?;
                }
                Ok(())
            }
            MetadataEntry::DdlPrepared { token, entry } => {
                if self.owner == Some(*token) {
                    self.apply(entry)?;
                }
                Ok(())
            }
            MetadataEntry::DdlPrepareAcquire { token, .. } => {
                if self.owner.is_none() {
                    self.owner = Some(*token);
                }
                Ok(())
            }
            MetadataEntry::DdlPrepareRelease { token } => {
                if self.owner == Some(*token) {
                    self.owner = None;
                }
                Ok(())
            }
            // Propose and finalize apply only under the lease owner's token,
            // as on the applier.
            MetadataEntry::DdlPendingPropose { token, objects, .. } => {
                if self.owner == Some(*token) {
                    self.pending.insert(*token, objects.clone());
                }
                Ok(())
            }
            MetadataEntry::DdlPendingFinalize { token } => {
                if self.owner != Some(*token) {
                    return Ok(());
                }
                for object in self.pending.remove(token).unwrap_or_default() {
                    let (PendingDdlObject::Create { entry }
                    | PendingDdlObject::Alter { entry, .. }) = object;
                    self.apply(&entry)?;
                }
                Ok(())
            }
            MetadataEntry::DdlPendingCancel { token } => {
                self.pending.remove(token);
                Ok(())
            }
            // Every other entry changes no catalog row WAL replay reads.
            // Boot's apply writes its effects.
            _ => Ok(()),
        }
    }

    fn apply_payload(&self, payload: &[u8]) -> crate::Result<()> {
        let entry = catalog_entry::decode(payload)?;
        if matches!(
            validate(&entry, self.catalog)?,
            ValidationOutcome::AlreadyApplied
        ) {
            return Ok(());
        }
        catalog_entry::apply::apply_to(&entry, self.catalog)?;
        Ok(())
    }
}
