// SPDX-License-Identifier: Apache-2.0

//! Document writes reuse ordered analyzer output and preserve surrogate bounds.

use super::FtsIndex;
use crate::{
    backend::FtsBackend,
    block::CompactPosting,
    codec::smallfloat,
    index::error::{FtsIndexError, MAX_INDEXABLE_SURROGATE},
    lsm::segment_deletes::SegmentDeletes,
    scope::IndexScope,
};
use nodedb_types::Surrogate;
use std::collections::HashMap;
use tracing::debug;

impl<B: FtsBackend> FtsIndex<B> {
    /// Index a document's text content into one index. The analyzer is the
    /// one bound to the index's collection.
    ///
    /// Returns `Err(FtsIndexError::SurrogateOutOfRange)` for `Surrogate::ZERO`
    /// or values exceeding `MAX_INDEXABLE_SURROGATE`. `Surrogate::ZERO` is the
    /// unassigned sentinel. Fieldnorm arrays use raw `u32` surrogates as
    /// indexes. Values near `u32::MAX` cause multi-GiB allocations. Runtime
    /// bounds checks run before analysis and remain active in release builds.
    pub fn index_document<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        doc_id: Surrogate,
        text: &str,
    ) -> Result<(), FtsIndexError<B::Error>> {
        let index = index.into();
        Self::check_surrogate(doc_id)?;

        let tokens = self
            .analyze_for_collection(database_id, tid, index.collection(), text)
            .map_err(FtsIndexError::backend)?;
        self.index_analyzed_document(database_id, tid, index, doc_id, &tokens)
    }

    /// Index ordered tokens from this collection's current analyzer. Callers
    /// preserve token order and hold analyzer config stable through indexing.
    ///
    /// Returns `Err(FtsIndexError::SurrogateOutOfRange)` for `Surrogate::ZERO`
    /// or values exceeding `MAX_INDEXABLE_SURROGATE`.
    pub fn index_analyzed_document<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        doc_id: Surrogate,
        tokens: &[String],
    ) -> Result<(), FtsIndexError<B::Error>> {
        let index = index.into();
        Self::check_surrogate(doc_id)?;
        if tokens.is_empty() {
            return Ok(());
        }

        let mut term_data: HashMap<&str, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_data.entry(token.as_str()).or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        let doc_len = tokens.len() as u32;
        let fieldnorm = smallfloat::encode(doc_len);

        let term_count = term_data.len();
        self.memtable.insert_doc(
            database_id,
            tid,
            index,
            doc_id,
            doc_len,
            term_data.into_iter().map(|(term, (freq, positions))| {
                (
                    term,
                    CompactPosting {
                        doc_id,
                        term_freq: freq,
                        fieldnorm,
                        positions,
                    },
                )
            }),
        );

        // Write document length, fieldnorm, and update incremental stats.
        self.backend
            .write_doc_length(database_id, tid, index, doc_id, doc_len)
            .map_err(FtsIndexError::backend)?;
        self.write_fieldnorm(database_id, tid, index, doc_id, doc_len)
            .map_err(FtsIndexError::backend)?;
        self.backend
            .increment_stats(database_id, tid, index, doc_len)
            .map_err(FtsIndexError::backend)?;

        if self.memtable.should_flush() {
            self.flush_all_memtables()?;
        }

        debug!(
            database_id,
            tid,
            collection = index.collection(),
            field = index.field_key(),
            doc_id = doc_id.0,
            tokens = tokens.len(),
            terms = term_count,
            "indexed document"
        );
        Ok(())
    }

    /// Remove a document from one index.
    ///
    /// The memtable drops the document's postings. Postings already flushed
    /// to a segment stay in that immutable segment, so the document enters
    /// the delete set of every segment the index holds: reads skip those
    /// postings and a merge drops them. The corpus stats lose the document's
    /// length. A document the index does not hold changes nothing.
    pub fn remove_document<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        doc_id: Surrogate,
    ) -> Result<(), FtsIndexError<B::Error>> {
        let index = index.into();
        let doc_len = self
            .backend
            .read_doc_length(database_id, tid, index, doc_id)
            .map_err(FtsIndexError::backend)?;

        self.memtable.remove_doc(database_id, tid, index, doc_id);

        let Some(len) = doc_len else {
            return Ok(());
        };
        let segments = self
            .backend
            .list_segments(database_id, tid, index)
            .map_err(FtsIndexError::backend)?;
        if !segments.is_empty() {
            let mut deletes =
                SegmentDeletes::load(&self.backend, database_id, tid, index, &segments)?;
            deletes.mark_removed(&segments, doc_id);
            deletes.store(&self.backend, database_id, tid, index)?;
        }
        self.backend
            .remove_doc_length(database_id, tid, index, doc_id)
            .map_err(FtsIndexError::backend)?;
        self.backend
            .decrement_stats(database_id, tid, index, len)
            .map_err(FtsIndexError::backend)?;

        Ok(())
    }

    fn check_surrogate(doc_id: Surrogate) -> Result<(), FtsIndexError<B::Error>> {
        let raw = doc_id.as_u32();
        if raw == 0 || raw > MAX_INDEXABLE_SURROGATE {
            return Err(FtsIndexError::SurrogateOutOfRange { surrogate: doc_id });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::backend::memory::MemoryBackend;
    use crate::test_support::test_governor;

    use super::*;

    const DB: u64 = 0;
    const T: u64 = 1;
    const DOCS: IndexScope<'static> = IndexScope::document("docs");

    fn make_index() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new(), test_governor())
    }

    #[test]
    fn index_writes_to_memtable() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "hello world greeting")
            .unwrap();

        assert!(!idx.memtable.is_empty());
        assert!(idx.memtable.posting_count() > 0);
    }

    #[test]
    fn index_surrogate_stored() {
        let idx = make_index();
        // Surrogates must be in 1..=MAX_INDEXABLE_SURROGATE. Surrogate::ZERO is the unassigned sentinel and is rejected at index time.
        idx.index_document(DB, T, "docs", Surrogate(10), "hello world greeting")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(11), "hello rust language")
            .unwrap();

        let (count, _) = idx.backend.collection_stats(DB, T, DOCS).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn remove_decrements_stats() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(10), "hello world")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(11), "hello rust")
            .unwrap();

        idx.remove_document(DB, T, "docs", Surrogate(10)).unwrap();

        let (count, _) = idx.backend.collection_stats(DB, T, DOCS).unwrap();
        assert_eq!(count, 1);
    }

    fn hits(idx: &FtsIndex<MemoryBackend>, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = idx
            .search(
                DB,
                T,
                DOCS,
                crate::FtsSearchParams {
                    query,
                    top_k: usize::MAX,
                    fuzzy_enabled: false,
                    mode: crate::posting::QueryMode::Or,
                    prefilter: None,
                },
            )
            .unwrap()
            .into_iter()
            .map(|r| r.doc_id.0)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// A document removed after its postings reached a segment stops
    /// matching, leaves the document frequency and the corpus stats, and
    /// leaves the segment at the next merge. Indexed again, it matches its
    /// new text only.
    #[test]
    fn remove_after_flush_hides_segment_postings_and_merge_drops_them() {
        use crate::lsm::compaction::{CompactLevelParams, SegmentMeta, compact_level, parse_level};
        use crate::lsm::segment::reader::SegmentReader;
        use crate::{DocScore, TextQuery};

        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "rust tokio")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(2), "rust axum")
            .unwrap();
        idx.flush_memtable(DB, T, "docs").unwrap();
        idx.index_document(DB, T, "docs", Surrogate(3), "rust serde")
            .unwrap();
        idx.flush_memtable(DB, T, "docs").unwrap();
        assert_eq!(hits(&idx, "tokio"), vec![1]);

        idx.remove_document(DB, T, "docs", Surrogate(1)).unwrap();
        assert!(hits(&idx, "tokio").is_empty(), "a removed document matches");
        assert_eq!(hits(&idx, "rust"), vec![2, 3]);
        let blocks = idx.term_blocks(DB, T, DOCS, &["rust".into()]).unwrap();
        assert_eq!(blocks[0].df, 2, "document frequency counts live documents");
        assert_eq!(idx.backend.collection_stats(DB, T, DOCS).unwrap(), (2, 4));
        let scorer = idx
            .doc_scorer(
                DB,
                T,
                DOCS,
                TextQuery {
                    query: "tokio",
                    fuzzy_enabled: false,
                    mode: crate::posting::QueryMode::Or,
                },
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            scorer.score(&[Surrogate(1)]).unwrap(),
            vec![DocScore::Absent]
        );

        idx.index_document(DB, T, "docs", Surrogate(1), "rust hyper")
            .unwrap();
        idx.flush_memtable(DB, T, "docs").unwrap();
        assert_eq!(hits(&idx, "hyper"), vec![1]);
        assert!(hits(&idx, "tokio").is_empty());
        assert_eq!(hits(&idx, "rust"), vec![1, 2, 3]);

        let segments: Vec<SegmentMeta> = idx
            .backend
            .list_segments(DB, T, DOCS)
            .unwrap()
            .into_iter()
            .map(|segment_id| SegmentMeta {
                level: parse_level(&segment_id),
                segment_id,
                size: 0,
            })
            .collect();
        assert_eq!(segments.len(), 3);
        let governor = test_governor();
        let (merged, merged_ids) = compact_level(CompactLevelParams {
            backend: &idx.backend,
            database_id: DB,
            tid: T,
            index: DOCS,
            segments: &segments,
            level: 0,
            governor: &governor,
        })
        .unwrap()
        .expect("three level-0 segments merge");
        let reader = SegmentReader::open(merged.clone()).unwrap();
        assert!(
            reader.find_term("tokio").is_none(),
            "the merge drops dead postings"
        );
        assert_eq!(reader.df("rust"), 3);

        idx.backend
            .write_segment(DB, T, DOCS, "L1:00000000000000ff", &merged)
            .unwrap();
        for id in &merged_ids {
            idx.backend.remove_segment(DB, T, DOCS, id).unwrap();
        }
        assert!(hits(&idx, "tokio").is_empty());
        assert_eq!(hits(&idx, "rust"), vec![1, 2, 3]);

        // The merged segment carries no delete set: a later removal hides it.
        idx.remove_document(DB, T, "docs", Surrogate(2)).unwrap();
        assert_eq!(hits(&idx, "rust"), vec![1, 3]);
    }

    /// A flush after a restart does not reuse a stored segment id.
    #[test]
    fn a_flush_never_reuses_a_stored_segment_id() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "alpha")
            .unwrap();
        idx.flush_memtable(DB, T, "docs").unwrap();

        let restarted = FtsIndex::new(idx.backend, test_governor());
        restarted
            .index_document(DB, T, "docs", Surrogate(2), "bravo")
            .unwrap();
        restarted.flush_memtable(DB, T, "docs").unwrap();
        assert_eq!(
            restarted.backend.list_segments(DB, T, DOCS).unwrap().len(),
            2
        );
        assert_eq!(hits(&restarted, "alpha"), vec![1]);
        assert_eq!(hits(&restarted, "bravo"), vec![2]);
    }

    #[test]
    fn index_updates_stats() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(10), "hello world greeting")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(11), "hello rust language")
            .unwrap();

        let (count, total) = idx.backend.collection_stats(DB, T, DOCS).unwrap();
        assert_eq!(count, 2);
        assert!(total > 0);
    }

    #[test]
    fn field_index_keeps_its_own_postings_and_stats() {
        let idx = make_index();
        let title = IndexScope::field("docs", "title").unwrap();
        idx.index_document(DB, T, title, Surrogate(1), "rust")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(1), "rust handbook")
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate(2), "rust nomicon")
            .unwrap();

        assert_eq!(idx.backend.collection_stats(DB, T, title).unwrap(), (1, 1));
        assert_eq!(idx.backend.collection_stats(DB, T, DOCS).unwrap(), (2, 4));
        assert_eq!(idx.memtable.get_postings(DB, T, title, "rust").len(), 1);
        assert_eq!(idx.memtable.get_postings(DB, T, DOCS, "rust").len(), 2);

        idx.remove_document(DB, T, title, Surrogate(1)).unwrap();
        assert_eq!(idx.backend.collection_stats(DB, T, title).unwrap(), (0, 0));
        assert!(idx.memtable.get_postings(DB, T, title, "rust").is_empty());
        assert_eq!(idx.memtable.get_postings(DB, T, DOCS, "rust").len(), 2);
    }

    #[test]
    fn empty_text_is_noop() {
        let idx = make_index();
        idx.index_document(DB, T, "docs", Surrogate(1), "the a is")
            .unwrap();
        assert_eq!(idx.backend.collection_stats(DB, T, DOCS).unwrap(), (0, 0));
        assert!(idx.memtable.is_empty());
    }

    // ── Surrogate boundary tests ──────────────────────────────────────────────

    /// Spec: Surrogate::ZERO (the unassigned sentinel) must be rejected at index
    /// time with FtsIndexError::SurrogateOutOfRange, not written into the index.
    #[test]
    fn index_document_rejects_zero_surrogate() {
        let idx = make_index();
        let err = idx
            .index_document(DB, T, "docs", Surrogate(0), "hello world")
            .unwrap_err();
        assert!(
            matches!(err, FtsIndexError::SurrogateOutOfRange { surrogate } if surrogate == Surrogate(0)),
            "expected SurrogateOutOfRange(sur:0), got {err}"
        );
    }

    /// Spec: Surrogate(u32::MAX) must be rejected — it is reserved as a sentinel
    /// and would also cause a 4 GiB fieldnorm array resize.
    #[test]
    fn index_document_rejects_u32_max_surrogate() {
        let idx = make_index();
        let err = idx
            .index_document(DB, T, "docs", Surrogate(u32::MAX), "hello world")
            .unwrap_err();
        assert!(
            matches!(err, FtsIndexError::SurrogateOutOfRange { .. }),
            "expected SurrogateOutOfRange, got {err}"
        );
    }

    /// Check the last valid surrogate constant and a representative valid input.
    #[test]
    fn index_document_accepts_max_indexable_surrogate() {
        // Indexing MAX_INDEXABLE_SURROGATE requires multi-GiB fieldnorm arrays.
        // This fixture uses Surrogate(1) and checks the constant and sentinel boundary separately.
        let idx = make_index();
        // Check a valid surrogate without allocating the largest fieldnorm array.
        idx.index_document(DB, T, "docs", Surrogate(1), "boundary check")
            .unwrap();
        // Confirm the constant is correct.
        assert_eq!(
            crate::index::error::MAX_INDEXABLE_SURROGATE,
            u32::MAX - 1,
            "MAX_INDEXABLE_SURROGATE must be u32::MAX - 1"
        );
    }

    /// Spec: the SurrogateOutOfRange error message must be informative.
    #[test]
    fn surrogate_out_of_range_error_is_informative() {
        let err: FtsIndexError<crate::backend::memory::MemoryError> =
            FtsIndexError::SurrogateOutOfRange {
                surrogate: Surrogate(0),
            };
        let msg = err.to_string();
        assert!(
            msg.contains("out of the indexable range"),
            "error message must mention range: {msg}"
        );
        assert!(
            msg.contains("unassigned sentinel"),
            "error message must explain zero sentinel: {msg}"
        );
    }
    #[test]
    fn analyzed_tokens_match_text_indexing_and_surrogate_validation() {
        let text_index = make_index();
        let token_index = make_index();
        let text = "Alpha alpha beta gamma";
        let tokens = token_index
            .analyze_for_collection(DB, T, "docs", text)
            .unwrap();
        text_index
            .index_document(DB, T, "docs", Surrogate(1), text)
            .unwrap();
        token_index
            .index_analyzed_document(DB, T, "docs", Surrogate(1), &tokens)
            .unwrap();
        assert_eq!(
            text_index.memtable.stats(DB, T, DOCS),
            token_index.memtable.stats(DB, T, DOCS)
        );
        let mut terms = text_index.memtable.terms(DB, T, DOCS);
        terms.sort();
        for term in terms {
            let expected = text_index.memtable.get_postings(DB, T, DOCS, &term);
            let actual = token_index.memtable.get_postings(DB, T, DOCS, &term);
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(expected) {
                assert_eq!(actual.doc_id, expected.doc_id);
                assert_eq!(actual.term_freq, expected.term_freq);
                assert_eq!(actual.fieldnorm, expected.fieldnorm);
                assert_eq!(actual.positions, expected.positions);
            }
        }
        assert_eq!(
            text_index.backend.collection_stats(DB, T, DOCS).unwrap(),
            token_index.backend.collection_stats(DB, T, DOCS).unwrap()
        );
        for id in [Surrogate::ZERO, Surrogate(u32::MAX)] {
            assert!(matches!(
                token_index.index_analyzed_document(DB, T, "docs", id, &tokens),
                Err(FtsIndexError::SurrogateOutOfRange { .. })
            ));
            assert!(matches!(
                token_index.index_analyzed_document(DB, T, "docs", id, &[]),
                Err(FtsIndexError::SurrogateOutOfRange { .. })
            ));
        }
        let empty = make_index();
        empty
            .index_analyzed_document(DB, T, "docs", Surrogate(1), &[])
            .unwrap();
        assert!(empty.memtable.is_empty());
    }
}
