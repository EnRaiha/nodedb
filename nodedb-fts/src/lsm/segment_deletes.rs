// SPDX-License-Identifier: Apache-2.0

//! Per-segment delete sets of one index.
//!
//! A segment is immutable. Removing a document after its postings reached a
//! segment records the document in the delete set of every segment the index
//! holds at that moment. A query skips each segment's postings of its deleted
//! documents. A merge drops them, and the merged segment starts with no
//! deletes. A document indexed again after its removal lands in the memtable
//! and then in a newer segment, which no older delete set names.
//!
//! The sets persist in the index's backend metadata under
//! [`SEGMENT_DELETES_META_KEY`]. A set whose segment no longer exists is
//! ignored on read and dropped on the next write.

use std::collections::BTreeMap;

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::backend::FtsBackend;
use crate::index::FtsIndexError;
use crate::scope::IndexScope;

/// Metadata sub-key of an index's segment delete sets.
pub const SEGMENT_DELETES_META_KEY: &str = "segment_deletes";

/// Segment id → the documents removed after that segment was written.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentDeletes {
    sets: BTreeMap<String, SurrogateBitmap>,
}

impl SegmentDeletes {
    /// The stored delete sets of `index`, restricted to `live_segments`.
    /// A stored blob that does not decode is a typed corruption error.
    pub fn load<B: FtsBackend>(
        backend: &B,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        live_segments: &[String],
    ) -> Result<Self, FtsIndexError<B::Error>> {
        let Some(bytes) = backend
            .read_meta(database_id, tid, index, SEGMENT_DELETES_META_KEY)
            .map_err(FtsIndexError::Backend)?
        else {
            return Ok(Self::default());
        };
        let entries: Vec<(String, SurrogateBitmap)> =
            zerompk::from_msgpack(&bytes).map_err(|e| FtsIndexError::CorruptState {
                subkey: SEGMENT_DELETES_META_KEY,
                detail: e.to_string(),
            })?;
        let sets = entries
            .into_iter()
            .filter(|(segment_id, _)| live_segments.contains(segment_id))
            .collect();
        Ok(Self { sets })
    }

    /// Persist these delete sets as the delete sets of `index`.
    pub fn store<B: FtsBackend>(
        &self,
        backend: &B,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
    ) -> Result<(), FtsIndexError<B::Error>> {
        let entries: Vec<(&String, &SurrogateBitmap)> = self.sets.iter().collect();
        let bytes = zerompk::to_msgpack_vec(&entries).map_err(|e| FtsIndexError::StateEncode {
            subkey: SEGMENT_DELETES_META_KEY,
            detail: e.to_string(),
        })?;
        backend
            .write_meta(database_id, tid, index, SEGMENT_DELETES_META_KEY, &bytes)
            .map_err(FtsIndexError::Backend)
    }

    /// Record `doc_id` as removed from each of `segments`.
    pub fn mark_removed(&mut self, segments: &[String], doc_id: Surrogate) {
        for segment_id in segments {
            self.sets
                .entry(segment_id.clone())
                .or_default()
                .insert(doc_id);
        }
    }

    /// The documents removed from `segment_id`. `None` when it has none.
    pub fn deleted(&self, segment_id: &str) -> Option<&SurrogateBitmap> {
        self.sets.get(segment_id).filter(|set| !set.is_empty())
    }

    /// Move out the documents removed from `segment_id`. `None` when it has
    /// none.
    pub fn take(&mut self, segment_id: &str) -> Option<SurrogateBitmap> {
        self.sets.remove(segment_id).filter(|set| !set.is_empty())
    }

    /// Whether no segment has a removed document.
    pub fn is_empty(&self) -> bool {
        self.sets.values().all(SurrogateBitmap::is_empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::MemoryBackend;

    const DB: u64 = 0;
    const T: u64 = 1;
    const DOCS: IndexScope<'static> = IndexScope::document("docs");

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn delete_sets_round_trip_and_drop_removed_segments() {
        let backend = MemoryBackend::new();
        let mut deletes = SegmentDeletes::default();
        deletes.mark_removed(&ids(&["L0:1", "L0:2"]), Surrogate(7));
        deletes.store(&backend, DB, T, DOCS).unwrap();

        let loaded = SegmentDeletes::load(&backend, DB, T, DOCS, &ids(&["L0:1", "L0:2"])).unwrap();
        assert_eq!(loaded, deletes);
        assert!(
            loaded
                .deleted("L0:1")
                .is_some_and(|s| s.contains(Surrogate(7)))
        );

        // A merged-away segment's set is not read back.
        let loaded = SegmentDeletes::load(&backend, DB, T, DOCS, &ids(&["L0:2"])).unwrap();
        assert!(loaded.deleted("L0:1").is_none());
        assert!(loaded.deleted("L0:2").is_some());
        assert!(loaded.deleted("L1:3").is_none());
    }

    #[test]
    fn an_undecodable_blob_is_a_corruption_error() {
        let backend = MemoryBackend::new();
        backend
            .write_meta(DB, T, DOCS, SEGMENT_DELETES_META_KEY, &[0xc1, 0xff])
            .unwrap();
        let err = SegmentDeletes::load(&backend, DB, T, DOCS, &ids(&["L0:1"])).unwrap_err();
        assert!(matches!(err, FtsIndexError::CorruptState { .. }), "{err}");
    }
}
