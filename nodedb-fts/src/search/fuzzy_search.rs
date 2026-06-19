// SPDX-License-Identifier: Apache-2.0

//! Fuzzy term lookup for the FtsIndex.

use crate::backend::FtsBackend;
use crate::fuzzy;
use crate::index::FtsIndex;
use crate::lsm::query as lsm_query;
use crate::posting::Posting;

impl<B: FtsBackend> FtsIndex<B> {
    /// Find the best fuzzy-matching term and return its posting list.
    pub(crate) fn fuzzy_lookup(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        query_term: &str,
    ) -> Result<(Vec<Posting>, bool), B::Error> {
        let mut all_terms = lsm_query::collect_all_terms(
            &self.backend,
            database_id,
            tid,
            collection,
            self.memtable(),
        )?;

        let backend_terms = self
            .backend
            .collection_terms(database_id, tid, collection)?;
        for t in backend_terms {
            all_terms.push(t);
        }
        all_terms.sort();
        all_terms.dedup();

        let matches = fuzzy::fuzzy_match(query_term, all_terms.iter().map(String::as_str));

        if let Some((best_term, _dist)) = matches.first() {
            let postings = self
                .backend
                .read_postings(database_id, tid, collection, best_term)?;
            if !postings.is_empty() {
                return Ok((postings, true));
            }

            let tokens = vec![best_term.to_string()];
            let term_blocks = lsm_query::collect_merged_term_blocks(
                &self.backend,
                database_id,
                tid,
                collection,
                self.memtable(),
                &tokens,
                self.governor.as_ref(),
            )?;
            if !term_blocks.is_empty() && term_blocks[0].df > 0 {
                let mut postings = Vec::new();
                for block in &term_blocks[0].blocks {
                    for i in 0..block.doc_ids.len() {
                        postings.push(Posting {
                            doc_id: block.doc_ids[i],
                            term_freq: block.term_freqs[i],
                            positions: block.positions[i].clone(),
                        });
                    }
                }
                if !postings.is_empty() {
                    return Ok((postings, true));
                }
            }
        }

        Ok((Vec::new(), false))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::FtsIndex;
    use nodedb_types::Surrogate;

    const DB: u64 = 0;
    const T: u64 = 1;

    #[test]
    fn fuzzy_lookup_finds_close_term() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", Surrogate(1), "distributed database systems")
            .unwrap();

        let (postings, is_fuzzy) = idx.fuzzy_lookup(DB, T, "docs", "databse").unwrap();
        assert!(is_fuzzy, "should find fuzzy match");
        assert!(!postings.is_empty(), "should return postings from LSM");
    }

    #[test]
    fn fuzzy_lookup_no_match() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.index_document(DB, T, "docs", Surrogate(1), "hello world")
            .unwrap();

        let (postings, is_fuzzy) = idx.fuzzy_lookup(DB, T, "docs", "zzzzzzz").unwrap();
        assert!(postings.is_empty());
        assert!(!is_fuzzy);
    }
}
