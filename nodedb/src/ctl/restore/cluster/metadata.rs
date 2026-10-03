// SPDX-License-Identifier: BUSL-1.1

//! The metadata group's part of a cluster restore.
//!
//! A base holds the catalogs at its metadata applied index. The metadata log
//! entries after it, through the restore point, come from the metadata log
//! archive, and the restored log holds them for the next boot to apply.
//!
//! An entry survives by the rule data writes follow: the HLC stamp the
//! leader gave it is below the point's watermark. An entry after the point's index
//! never survives. A write that depends on an entry's effect carries an HLC
//! above the entry's stamp, and above the watermark once the point's entry
//! applied, so data and metadata split at the same instant. An unstamped
//! entry is cluster-internal (membership, epoch) and survives by position
//! alone.
//!
//! A dropped entry becomes an empty entry, so every index keeps its place.
//! Restore point entries become empty entries too: applying one cuts every
//! group of the restored cluster for a point it already passed. Their
//! catalog rows are written directly.

use std::collections::BTreeMap;

use nodedb_cluster::metadata_group::codec::decode_entry;
use nodedb_cluster::{MetadataEntry, entry_stamp};
use nodedb_raft::message::LogEntry;

use super::super::error::RestoreError;
use crate::control::security::catalog::restore_points::StoredRestorePoint;
use crate::storage::cold::ColdStorage;
use crate::storage::metadata_timeline::resolve_chain;
use crate::storage::raft_log_archive::{ArchivedLogEntry, fetch_chunk, list_all_chunks};

/// Every archived metadata log entry above an index, from every node life.
#[derive(Debug, Clone, Default)]
pub struct ArchivedMetadata {
    /// Entries above the index the fetch started at, by index.
    pub entries: BTreeMap<u64, ArchivedLogEntry>,
}

/// The metadata log entries a restore replays after its base.
#[derive(Debug, Clone, Default)]
pub struct MetadataTail {
    /// The base's metadata applied index. The restored log starts after it.
    pub base_index: u64,
    /// Entries `base_index + 1` through the restore point, in order.
    pub entries: Vec<ArchivedLogEntry>,
}

/// What the archived metadata log reserved in surrogates, at any index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurrogateLog {
    /// The highest `SurrogateAlloc` high-water mark.
    pub alloc_hwm: Option<u32>,
    /// Batch size of each `SurrogateReserve` entry, by index.
    pub reserves: BTreeMap<u64, u32>,
}

impl SurrogateLog {
    /// The highest surrogate ever issued, given the catalog's high-water
    /// mark `hwm` after it applied every reservation through `reserve_index`.
    pub fn ceiling(&self, hwm: u32, reserve_index: u64) -> u32 {
        let carved: u64 = self
            .reserves
            .range(reserve_index.saturating_add(1)..)
            .map(|(_, batch)| u64::from(*batch))
            .sum();
        let reserved = u32::try_from(u64::from(hwm).saturating_add(carved)).unwrap_or(u32::MAX);
        reserved.max(self.alloc_hwm.unwrap_or(0))
    }
}

/// Whether the restore drops the metadata entry `data` for a point with
/// watermark `watermark`. Restore point entries are handled apart.
fn dropped_at(data: &[u8], watermark: u64) -> bool {
    entry_stamp(data).is_some_and(|stamp| stamp >= watermark)
}

fn is_restore_point(data: &[u8]) -> bool {
    matches!(decode_entry(data), Ok(MetadataEntry::RestorePoint { .. }))
}

impl ArchivedMetadata {
    /// The lowest index at or below `through` whose entry the restore drops
    /// at `watermark`. A base whose catalogs hold that entry is unusable.
    pub fn first_dropped(&self, through: u64, watermark: u64) -> Option<u64> {
        self.entries
            .range(..=through)
            .find(|(_, entry)| dropped_at(&entry.data, watermark) && !is_restore_point(&entry.data))
            .map(|(index, _)| *index)
    }

    /// Entries `base_index + 1` through `through`, refusing a hole.
    pub fn tail(&self, base_index: u64, through: u64) -> Result<MetadataTail, RestoreError> {
        let from = base_index.saturating_add(1);
        let mut entries = Vec::new();
        for index in from..=through {
            let entry = self
                .entries
                .get(&index)
                .ok_or(RestoreError::MetadataLogGap {
                    from,
                    through,
                    missing: index,
                })?;
            entries.push(entry.clone());
        }
        Ok(MetadataTail {
            base_index,
            entries,
        })
    }

    /// Every surrogate reservation the entries hold.
    pub fn surrogates(&self) -> SurrogateLog {
        let mut log = SurrogateLog::default();
        for (index, entry) in &self.entries {
            match decode_entry(&entry.data) {
                Ok(MetadataEntry::SurrogateAlloc { hwm }) => {
                    log.alloc_hwm = log.alloc_hwm.max(Some(hwm));
                }
                Ok(MetadataEntry::SurrogateReserve { batch_size, .. }) => {
                    log.reserves.insert(*index, batch_size);
                }
                _ => {}
            }
        }
        log
    }
}

impl MetadataTail {
    /// The entries as the restored log holds them, every one at `term`, and
    /// the restore points among them.
    pub fn restored_entries(
        &self,
        term: u64,
        watermark: u64,
    ) -> (Vec<LogEntry>, Vec<StoredRestorePoint>) {
        let mut points = Vec::new();
        let entries = self
            .entries
            .iter()
            .map(|entry| {
                let data = match decode_entry(&entry.data) {
                    Ok(MetadataEntry::RestorePoint { hlc, created_at_ms }) => {
                        points.push(StoredRestorePoint {
                            id: entry.index,
                            hlc,
                            created_at_ms,
                        });
                        Vec::new()
                    }
                    _ if dropped_at(&entry.data, watermark) => Vec::new(),
                    _ => entry.data.clone(),
                };
                LogEntry {
                    term,
                    index: entry.index,
                    data,
                }
            })
            .collect();
        (entries, points)
    }
}

/// Fetch every archived metadata log entry above `after` of the history
/// `timeline` holds. `timeline`'s own archive serves each index it holds,
/// and each ancestor serves only the lower indexes its descendants lack,
/// through the index they branch at. Every node life of a timeline archives
/// the same log, so any of them serves an index another lacks.
pub async fn fetch_metadata_after(
    cold: &ColdStorage,
    key: &nodedb_wal::crypto::WalEncryptionKey,
    timeline: u64,
    after: u64,
) -> Result<ArchivedMetadata, RestoreError> {
    let store = cold.object_store();
    let mut held = ArchivedMetadata::default();
    for span in resolve_chain(&store, cold.prefix(), timeline, key).await? {
        if span.through.is_some_and(|through| through <= after) {
            continue;
        }
        for chunk in list_all_chunks(&store, cold.prefix(), span.timeline).await? {
            let lo = chunk.first.max(after.saturating_add(1));
            let hi = span
                .through
                .map_or(chunk.last, |through| through.min(chunk.last));
            if lo > hi || (lo..=hi).all(|index| held.entries.contains_key(&index)) {
                continue;
            }
            // The first span to hold an index wins: the timeline itself, then
            // each ancestor in turn.
            for entry in fetch_chunk(&store, &chunk.key, key).await?.entries {
                if entry.index > after && span.holds(entry.index) {
                    held.entries.entry(entry.index).or_insert(entry);
                }
            }
        }
    }
    Ok(held)
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::metadata_group::codec::encode_entry;
    use nodedb_cluster::stamp_entry;

    use super::*;
    use crate::storage::cold::ColdStorageConfig;
    use crate::storage::raft_log_archive::{
        ArchiveLife, ArchivedLogChunk, chunk_key, put_chunk, seal_chunk,
    };

    const W: u64 = 1_000;

    fn wal_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x21; 32]).unwrap()
    }

    fn entry(index: u64, data: Vec<u8>) -> ArchivedLogEntry {
        ArchivedLogEntry {
            index,
            term: 3,
            data,
        }
    }

    fn ddl(token: u64, stamp: Option<u64>) -> Vec<u8> {
        let bytes = encode_entry(&MetadataEntry::DdlPendingFinalize { token }).unwrap();
        match stamp {
            Some(stamp) => stamp_entry(&bytes, stamp),
            None => bytes,
        }
    }

    fn held(entries: Vec<ArchivedLogEntry>) -> ArchivedMetadata {
        ArchivedMetadata {
            entries: entries.into_iter().map(|e| (e.index, e)).collect(),
        }
    }

    /// Archive entries `first..=last` of `timeline` as node `node_id`. Each
    /// entry's data names its timeline and index.
    async fn archive(cold: &ColdStorage, timeline: u64, node_id: u64, first: u64, last: u64) {
        let life = ArchiveLife {
            timeline,
            node_id,
            incarnation: "inc",
        };
        let key = chunk_key(cold.prefix(), life, first, last);
        let chunk = ArchivedLogChunk {
            group_id: 0,
            prev_term: 3,
            entries: (first..=last)
                .map(|i| entry(i, vec![timeline as u8, i as u8]))
                .collect(),
        };
        let sealed = seal_chunk(&chunk, &key, "node", &wal_key()).unwrap();
        put_chunk(&cold.object_store(), &key, sealed).await.unwrap();
    }

    fn cold_in(dir: &tempfile::TempDir) -> ColdStorage {
        ColdStorage::new(ColdStorageConfig {
            local_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn every_life_serves_the_entries_and_a_hole_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let cold = cold_in(&dir);
        archive(&cold, 0, 1, 1, 4).await;
        archive(&cold, 0, 1, 8, 12).await;
        archive(&cold, 0, 2, 3, 9).await;

        let archived = fetch_metadata_after(&cold, &wal_key(), 0, 2).await.unwrap();
        let tail = archived.tail(2, 10).unwrap();
        let indexes: Vec<u64> = tail.entries.iter().map(|e| e.index).collect();
        assert_eq!(indexes, (3..=10).collect::<Vec<_>>());
        assert!(matches!(
            archived.tail(2, 14),
            Err(RestoreError::MetadataLogGap {
                from: 3,
                through: 14,
                missing: 13
            })
        ));
        assert!(
            archived.tail(7, 7).unwrap().entries.is_empty(),
            "a base at the point replays nothing"
        );
    }

    /// A restore branched timeline 5 off the root at index 6, from a base at
    /// index 4. The restored log copies 5 and 6, and timeline 5 wrote its own
    /// 7 through 10. The root went on to index 12. A fetch of timeline 5 takes
    /// its own copies over the root's, the root only below them, and never
    /// the root's own 7 through 12.
    #[tokio::test]
    async fn a_branched_timeline_takes_its_parent_only_through_the_branch() {
        let dir = tempfile::tempdir().unwrap();
        let cold = cold_in(&dir);
        archive(&cold, 0, 1, 1, 12).await;
        archive(&cold, 5, 1, 5, 10).await;
        crate::storage::metadata_timeline::put_branch(
            &cold.object_store(),
            cold.prefix(),
            crate::storage::metadata_timeline::TimelineBranch {
                timeline: 5,
                parent: 0,
                branch_index: 6,
            },
            "node-1",
            &wal_key(),
        )
        .await
        .unwrap();

        let child = fetch_metadata_after(&cold, &wal_key(), 5, 2).await.unwrap();
        let sources: Vec<(u64, u8)> = child
            .entries
            .values()
            .map(|e| (e.index, e.data[0]))
            .collect();
        assert_eq!(
            sources,
            [
                (3, 0),
                (4, 0),
                (5, 5),
                (6, 5),
                (7, 5),
                (8, 5),
                (9, 5),
                (10, 5)
            ]
        );
        let root = fetch_metadata_after(&cold, &wal_key(), 0, 8).await.unwrap();
        assert!(
            root.entries.values().all(|e| e.data[0] == 0),
            "the root's own history never takes a child's entries"
        );
        assert_eq!(root.entries.len(), 4);
    }

    /// A DDL issued after the watermark but applied before the point's entry
    /// is dropped. So is every later write that depends on it: those carry
    /// an HLC above the DDL's stamp.
    #[test]
    fn an_entry_stamped_at_or_after_the_watermark_is_dropped() {
        let point = stamp_entry(
            &encode_entry(&MetadataEntry::RestorePoint {
                hlc: W,
                created_at_ms: 9,
            })
            .unwrap(),
            W + 5,
        );
        let archived = held(vec![
            entry(5, ddl(1, Some(W - 1))),
            entry(6, ddl(2, Some(W))),
            entry(7, ddl(3, None)),
            entry(8, point),
        ]);
        assert_eq!(archived.first_dropped(8, W), Some(6));
        let (entries, points) = archived.tail(4, 8).unwrap().restored_entries(1 << 40, W);
        let kept: Vec<bool> = entries.iter().map(|e| !e.data.is_empty()).collect();
        assert_eq!(kept, [true, false, true, false]);
        assert!(entries.iter().all(|e| e.term == 1 << 40));
        assert_eq!(
            points,
            [StoredRestorePoint {
                id: 8,
                hlc: W,
                created_at_ms: 9
            }]
        );
    }

    #[test]
    fn the_surrogate_ceiling_counts_every_later_reservation() {
        let reserve = |batch_size| {
            encode_entry(&MetadataEntry::SurrogateReserve {
                node_id: 1,
                request_id: 1,
                batch_size,
            })
            .unwrap()
        };
        let archived = held(vec![
            entry(3, reserve(10)),
            entry(
                5,
                encode_entry(&MetadataEntry::SurrogateAlloc { hwm: 40 }).unwrap(),
            ),
            entry(9, reserve(16)),
            entry(20, reserve(100)),
        ]);
        let log = archived.surrogates();
        assert_eq!(log.alloc_hwm, Some(40));
        assert_eq!(
            log.ceiling(30, 3),
            146,
            "reservations 9 and 20 follow the catalog's cursor"
        );
        assert_eq!(log.ceiling(0, 20), 40, "an alloc above every carve wins");
    }
}
