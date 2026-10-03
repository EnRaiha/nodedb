// SPDX-License-Identifier: BUSL-1.1

//! Planning of one node's part of a cluster restore.
//!
//! Every replica of a group recorded the group's place at the restore point
//! in its WAL: the log index of the cut barrier, the vShards the group
//! homed, and for the Calvin sequencer the next epoch. Those records and the
//! point's watermark give the [`CutRule`] the WAL splits by. The plan picks
//! a base that holds nothing the rule drops, in data or in metadata, replays
//! the archived WAL through the last record the rule keeps, and refuses
//! every dropped record on the way.

use std::collections::{BTreeMap, HashMap};

use nodedb_cluster::METADATA_GROUP_ID;

use super::super::archive::Archive;
use super::super::cut_rule::{CutRule, scan_cut};
use super::super::env::RestoreEnv;
use super::super::error::RestoreError;
use super::super::life::{Base, Life};
use super::super::plan::{RestorePlan, Scanner, plan_wal};
use super::metadata::{MetadataTail, SurrogateLog, fetch_metadata_after};

/// One Raft group's place at the restore point on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupPlace {
    pub group_id: u64,
    /// Log index of the group's first cut barrier for the point.
    pub index: u64,
    /// For the Calvin sequencer, the first epoch after the point.
    pub next_epoch: u64,
    /// For the Calvin sequencer, the highest epoch instant (ms) applied
    /// before the point. `0` for none.
    pub epoch_system_ms: u64,
}

#[derive(Debug)]
pub struct ClusterRestorePlan {
    pub restore_point: u64,
    /// The point's watermark HLC, nanoseconds.
    pub watermark: u64,
    /// The base and the archived WAL cut at the last kept record.
    pub wal: RestorePlan,
    /// Records at or below the target the restore drops.
    pub dropped_below_target: u64,
    /// Every data group and the sequencer, by group id.
    pub groups: Vec<GroupPlace>,
    /// The metadata log entries after the base, through the point.
    pub metadata: MetadataTail,
    /// Newest kept commit HLC the replayed WAL holds, by `(group, tenant)`.
    pub replayed_marks: HashMap<(u64, u64), u64>,
    /// Every surrogate reservation the archived metadata log holds.
    pub surrogates: SurrogateLog,
}

/// The point's records on this node.
struct Recorded {
    watermark: u64,
    groups: Vec<GroupPlace>,
    rule: CutRule,
}

/// Plan this node's part of a restore to `restore_point`.
pub async fn plan_cluster_restore(
    env: &RestoreEnv,
    life: &Life,
    archive: &Archive<'_>,
    restore_point: u64,
) -> Result<ClusterRestorePlan, RestoreError> {
    let mut scanner = Scanner::new(archive, &env.key);
    let Recorded {
        watermark,
        groups,
        rule,
    } = recorded(&mut scanner, env.node_id, life, restore_point).await?;

    // Pass one: where the kept and dropped records lie.
    let mut summaries = Vec::with_capacity(archive.segments().len());
    for index in 0..archive.segments().len() {
        let fetched = archive.fetch(index).await?;
        summaries.push(scan_cut(&fetched.key, &fetched.bytes, &rule, 0, u64::MAX)?);
    }
    let target_lsn = summaries
        .iter()
        .filter_map(|scan| scan.max_kept)
        .max()
        .ok_or_else(|| not_archived(env, life, restore_point))?;
    let first_dropped = summaries.iter().filter_map(|scan| scan.first_dropped).min();

    // The metadata log above the lowest usable base, from every node life.
    // Every base with catalogs of a cluster names the metadata timeline the
    // life writes, and the restore point lies on it.
    let (lowest, timeline) = life
        .bases
        .iter()
        .filter(|base| usable_catalogs(base, restore_point))
        .map(|base| (base.metadata_applied_index, base.metadata_timeline))
        .min()
        .ok_or(RestoreError::NoClusterBase {
            restore_point,
            first_dropped_lsn: first_dropped,
        })?;
    let archived = fetch_metadata_after(&env.cold, &env.key, timeline, lowest).await?;
    let first_dropped_entry = archived.first_dropped(restore_point, watermark);
    let base = pick_base(
        &life.bases,
        restore_point,
        first_dropped,
        first_dropped_entry,
    )?;
    // A snapshot install between the base and the target replaced rows no
    // WAL record carries, so the base must begin after it.
    let base = match newest_install(&mut scanner, target_lsn)
        .await?
        .filter(|install| *install > base.meta.begin_lsn.as_u64())
    {
        Some(install_lsn) => {
            let later: Vec<Base> = life
                .bases
                .iter()
                .filter(|base| base.meta.begin_lsn.as_u64() >= install_lsn)
                .cloned()
                .collect();
            pick_base(&later, restore_point, first_dropped, first_dropped_entry).map_err(|_| {
                RestoreError::NoBaseAfterSnapshotInstall {
                    install_lsn,
                    target_lsn,
                }
            })?
        }
        None => base,
    };
    let metadata = archived.tail(base.metadata_applied_index, restore_point)?;
    let surrogates = archived.surrogates();

    // Pass two: the dropped records and the tenant marks of the replay.
    let applied_high = base.meta.applied_high_lsn.as_u64();
    let segments = archive.segments();
    let mut dropped = Vec::new();
    let mut replayed_marks: HashMap<(u64, u64), u64> = HashMap::new();
    for (index, segment) in segments.iter().enumerate() {
        let ends_after_base = segments
            .get(index + 1)
            .is_none_or(|next| next.first_lsn.saturating_sub(1) > applied_high);
        if segment.first_lsn > target_lsn || !ends_after_base {
            continue;
        }
        let fetched = archive.fetch(index).await?;
        let scan = scan_cut(
            &fetched.key,
            &fetched.bytes,
            &rule,
            target_lsn,
            applied_high,
        )?;
        dropped.extend(scan.dropped);
        for (key, hlc) in scan.marks {
            let mark = replayed_marks.entry(key).or_insert(0);
            *mark = (*mark).max(hlc);
        }
    }
    let dropped_below_target = dropped.len() as u64;

    let mut wal = RestorePlan {
        node_id: env.node_id,
        incarnation: life.incarnation.clone(),
        base,
        target_lsn,
        requested_us: None,
        target_commit_ns: None,
        segments: Vec::new(),
        cut: None,
        refused: Vec::new(),
        aborts_scanned_through: None,
        surrogate_hwm: None,
        metadata: None,
    };
    // Every reservation the archive holds survives the cut: a kept write can
    // use a surrogate that a record the cut drops reserved.
    for index in 0..archive.segments().len() {
        let (_, scan) = scanner.scan(index).await?;
        wal.surrogate_hwm = wal.surrogate_hwm.max(scan.surrogate_hwm);
    }
    wal.surrogate_hwm = wal.surrogate_hwm.max(surrogates.alloc_hwm);
    plan_wal(&mut scanner, &mut wal, &dropped, env.alignment).await?;
    Ok(ClusterRestorePlan {
        restore_point,
        watermark,
        wal,
        dropped_below_target,
        groups,
        metadata,
        replayed_marks,
        surrogates,
    })
}

fn not_archived(env: &RestoreEnv, life: &Life, restore_point: u64) -> RestoreError {
    RestoreError::RestorePointNotArchived {
        restore_point,
        node_id: env.node_id,
        incarnation: life.incarnation.clone(),
    }
}

/// The point's watermark, each group's place, and the cut rule, from the
/// point's records in the archived WAL. A group recorded more than once
/// takes its lowest index, and the LSN of that record is its barrier.
async fn recorded(
    scanner: &mut Scanner<'_, '_>,
    node_id: u64,
    life: &Life,
    restore_point: u64,
) -> Result<Recorded, RestoreError> {
    let mut watermarks = Vec::new();
    let mut first: BTreeMap<u64, (u64, nodedb_wal::record::RestorePointPayload)> = BTreeMap::new();
    for index in 0..scanner.archive.segments().len() {
        let (_, scan) = scanner.scan(index).await?;
        for (lsn, point) in scan
            .restore_points
            .into_iter()
            .filter(|(_, p)| p.id == restore_point)
        {
            if !watermarks.contains(&point.hlc) {
                watermarks.push(point.hlc);
            }
            let held = first.get(&point.group_id);
            if held.is_none_or(|(_, held)| point.applied_index < held.applied_index) {
                first.insert(point.group_id, (lsn, point));
            }
        }
    }
    let watermark = match watermarks.as_slice() {
        [] => {
            return Err(RestoreError::RestorePointNotArchived {
                restore_point,
                node_id,
                incarnation: life.incarnation.clone(),
            });
        }
        [watermark] => *watermark,
        _ => {
            return Err(RestoreError::RestorePointWatermarks {
                restore_point,
                watermarks,
            });
        }
    };
    let node_barrier = first
        .get(&METADATA_GROUP_ID)
        .map(|(lsn, _)| *lsn)
        .or_else(|| first.values().map(|(lsn, _)| *lsn).min())
        .unwrap_or(0);
    let mut rule = CutRule {
        watermark,
        node_barrier,
        ..CutRule::default()
    };
    let mut groups = Vec::new();
    for (group_id, (lsn, point)) in first {
        if group_id == METADATA_GROUP_ID {
            continue;
        }
        for vshard in &point.vshards {
            rule.vshard_barrier.insert(*vshard, lsn);
            rule.vshard_group.insert(*vshard, group_id);
        }
        groups.push(GroupPlace {
            group_id,
            index: point.applied_index,
            next_epoch: point.next_epoch,
            epoch_system_ms: point.epoch_system_ms,
        });
    }
    Ok(Recorded {
        watermark,
        groups,
        rule,
    })
}

/// The LSN of the newest snapshot install at or below `through` anywhere in
/// the archive.
async fn newest_install(
    scanner: &mut Scanner<'_, '_>,
    through: u64,
) -> Result<Option<u64>, RestoreError> {
    let mut newest = None;
    for index in 0..scanner.archive.segments().len() {
        let (_, scan) = scanner.scan(index).await?;
        newest = newest.max(
            scan.installs
                .into_iter()
                .filter(|lsn| *lsn <= through)
                .max(),
        );
    }
    Ok(newest)
}

/// Whether `base` captured cluster catalogs that hold no metadata entry
/// above the point.
fn usable_catalogs(base: &Base, restore_point: u64) -> bool {
    base.metadata_applied_index > 0 && base.metadata_captured_index <= restore_point
}

/// The newest base that holds nothing the restore drops: no dropped write,
/// no dropped metadata entry, and no metadata entry above the point.
fn pick_base(
    bases: &[Base],
    restore_point: u64,
    first_dropped: Option<u64>,
    first_dropped_entry: Option<u64>,
) -> Result<Base, RestoreError> {
    bases
        .iter()
        .filter(|base| {
            usable_catalogs(base, restore_point)
                && first_dropped.is_none_or(|lsn| base.meta.applied_high_lsn.as_u64() < lsn)
                && first_dropped_entry.is_none_or(|index| base.metadata_captured_index < index)
        })
        .max_by_key(|base| (base.meta.applied_high_lsn.as_u64(), base.meta.created_at_us))
        .cloned()
        .ok_or(RestoreError::NoClusterBase {
            restore_point,
            first_dropped_lsn: first_dropped,
        })
}

#[cfg(test)]
mod tests {
    use crate::storage::snapshot::{SNAPSHOT_FORMAT_VERSION, SnapshotKind, SnapshotMeta};
    use crate::types::Lsn;

    use super::*;

    fn base(id: u64, applied_high: u64, metadata_captured_index: u64) -> Base {
        Base {
            prefix: format!("snap-{id}"),
            meta: SnapshotMeta {
                format_version: SNAPSHOT_FORMAT_VERSION,
                snapshot_id: id,
                begin_lsn: Lsn::new(applied_high),
                end_lsn: Lsn::new(applied_high),
                applied_high_lsn: Lsn::new(applied_high),
                created_at_us: id,
                created_by: "node-1".into(),
                kind: SnapshotKind::Base,
                parent_id: None,
                data_bytes: 0,
            },
            metadata_applied_index: metadata_captured_index.min(1),
            metadata_captured_index,
            metadata_timeline: 0,
        }
    }

    #[test]
    fn the_base_holds_nothing_the_restore_drops() {
        let bases = [
            base(1, 10, 5),
            base(2, 20, 9),
            base(3, 30, 12),
            base(4, 40, 0),
        ];
        let pick = |point, first_dropped, first_entry| {
            pick_base(&bases, point, first_dropped, first_entry).map(|b| b.meta.snapshot_id)
        };
        assert_eq!(pick(20, Some(35), None).unwrap(), 3);
        assert_eq!(
            pick(20, Some(25), None).unwrap(),
            2,
            "base 3 holds a dropped write"
        );
        assert_eq!(
            pick(10, None, None).unwrap(),
            2,
            "base 3 captured catalogs after the point"
        );
        assert_eq!(
            pick(20, None, Some(12)).unwrap(),
            2,
            "base 3's catalogs hold the dropped metadata entry 12"
        );
        assert!(
            matches!(
                pick(20, Some(5), None),
                Err(RestoreError::NoClusterBase {
                    restore_point: 20,
                    first_dropped_lsn: Some(5)
                })
            ),
            "every base holds a dropped write"
        );
    }
}
