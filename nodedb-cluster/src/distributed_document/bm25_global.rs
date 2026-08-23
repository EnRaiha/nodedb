// SPDX-License-Identifier: BUSL-1.1

//! Global IDF computation for distributed BM25 text search.
//!
//! BM25 scoring depends on IDF (Inverse Document Frequency) — how rare a
//! term is across the ENTIRE corpus. When documents are sharded, each shard
//! only knows its local DF. Scores from different shards are incomparable
//! without global IDF.
//!
//! Two-phase scatter-gather:
//! 1. **Phase 1 (DF collection)**: Ask all shards for local document
//!    frequencies and total doc counts for the search terms.
//! 2. **Coordinator**: Compute global IDF from aggregated DFs.
//! 3. **Phase 2 (Scored search)**: Send global IDF to all shards. Each
//!    shard computes BM25 with the shared IDF, returns its local top-K.
//! 4. **Coordinator**: Merge-sort by BM25 score, return global top-K.
//!
//! Both aggregates are reachable only through a completeness check: an IDF
//! short of one shard mis-scores every hit, and a merge short of one shard
//! drops results, and neither is distinguishable from a correct answer by
//! its type.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::gather::{Bm25GatherError, DEFAULT_GATHER_TIMEOUT, GlobalIdf, MergedScoredHits};
use crate::error::{ClusterError, Result};

/// Per-shard document frequency report (Phase 1 response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDfReport {
    pub shard_id: u32,
    /// Total documents on this shard.
    pub total_docs: u64,
    /// Sum of all document lengths on this shard (for global avg_doc_len).
    pub total_token_sum: u64,
    /// Per-term document frequency: `term → count of docs containing term`.
    pub term_dfs: HashMap<String, u64>,
}

/// A scored search hit from a shard (Phase 2 response).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredHit {
    pub doc_id: String,
    pub bm25_score: f64,
    pub shard_id: u32,
}

/// Coordinator for 2-phase distributed BM25.
pub struct GlobalIdfCoordinator {
    /// Search terms for this query.
    terms: Vec<String>,
    /// Shards this query was scattered to.
    shard_ids: Vec<u32>,
    /// Phase 1 DF reports, deduplicated by shard.
    df_reports: BTreeMap<u32, ShardDfReport>,
    /// Phase 2 scored hits, deduplicated by shard.
    scored_hits: BTreeMap<u32, Vec<ScoredHit>>,
    /// Computed global IDF, cached after Phase 1 completes.
    global_idf: Option<GlobalIdf>,
    /// When the query started, for timeout reporting.
    started_at: Instant,
    /// How long a shard may stay silent before it is reported as timed out.
    gather_timeout: Duration,
}

impl GlobalIdfCoordinator {
    pub fn new(terms: Vec<String>, shard_ids: Vec<u32>) -> Self {
        Self {
            terms,
            shard_ids,
            df_reports: BTreeMap::new(),
            scored_hits: BTreeMap::new(),
            global_idf: None,
            started_at: Instant::now(),
            gather_timeout: DEFAULT_GATHER_TIMEOUT,
        }
    }

    /// Override how long a shard may stay silent before it is timed out.
    pub fn with_timeout(mut self, gather_timeout: Duration) -> Self {
        self.gather_timeout = gather_timeout;
        self
    }

    // -- Phase 1: Collect DFs --

    /// Record a shard's DF report.
    ///
    /// Rejects a shard outside the scatter set and a second report from a
    /// shard that already answered: either would let Phase 1 read as complete
    /// while another shard is still silent.
    pub fn add_df_report(&mut self, report: ShardDfReport) -> Result<()> {
        let shard_id = report.shard_id;
        if !self.shard_ids.contains(&shard_id) {
            return Err(Bm25GatherError::UnexpectedShard {
                vshard_id: shard_id,
            }
            .into());
        }
        if self.df_reports.insert(shard_id, report).is_some() {
            return Err(Bm25GatherError::DuplicateDfReport {
                vshard_id: shard_id,
            }
            .into());
        }
        Ok(())
    }

    /// Whether all shards have reported Phase 1.
    pub fn phase1_complete(&self) -> bool {
        self.missing_shards(self.df_reports.keys().copied())
            .is_empty()
    }

    /// Compute global IDF from every shard's DF report.
    ///
    /// Refuses while any shard is missing: an IDF built from part of the
    /// corpus mis-scores every hit in Phase 2 while reading as a correct one.
    /// A shard silent past the gather timeout is reported as
    /// [`ClusterError::ShardTimeout`] instead, naming the first missing shard.
    ///
    /// Uses the standard BM25 IDF formula:
    /// `idf(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)`
    /// where N = total docs, df(t) = docs containing term t.
    pub fn compute_global_idf(&mut self) -> Result<&GlobalIdf> {
        self.check_phase1()?;

        let total_docs: u64 = self.df_reports.values().map(|r| r.total_docs).sum();
        let total_token_sum: u64 = self.df_reports.values().map(|r| r.total_token_sum).sum();
        let avg_doc_len = if total_docs > 0 {
            total_token_sum as f64 / total_docs as f64
        } else {
            1.0
        };

        let mut global_dfs: HashMap<String, u64> = HashMap::new();
        for report in self.df_reports.values() {
            for (term, &df) in &report.term_dfs {
                *global_dfs.entry(term.clone()).or_insert(0) += df;
            }
        }

        let mut term_idfs = HashMap::new();
        let n = total_docs as f64;
        for term in &self.terms {
            let df = global_dfs.get(term).copied().unwrap_or(0) as f64;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            term_idfs.insert(term.clone(), idf);
        }

        Ok(self
            .global_idf
            .insert(GlobalIdf::new(total_docs, avg_doc_len, term_idfs)))
    }

    /// Global IDF computed earlier in this query, `None` before Phase 1 completes.
    pub fn global_idf(&self) -> Option<&GlobalIdf> {
        self.global_idf.as_ref()
    }

    // -- Phase 2: Merge scored results --

    /// Record one shard's scored hits.
    ///
    /// Rejects a shard outside the scatter set and a second batch from a shard
    /// that already answered.
    pub fn record_scored_hits(&mut self, shard_id: u32, hits: Vec<ScoredHit>) -> Result<()> {
        if !self.shard_ids.contains(&shard_id) {
            return Err(Bm25GatherError::UnexpectedShard {
                vshard_id: shard_id,
            }
            .into());
        }
        if self.scored_hits.insert(shard_id, hits).is_some() {
            return Err(Bm25GatherError::DuplicateScoredHits {
                vshard_id: shard_id,
            }
            .into());
        }
        Ok(())
    }

    /// Whether all shards have returned scored hits.
    pub fn phase2_complete(&self) -> bool {
        self.missing_shards(self.scored_hits.keys().copied())
            .is_empty()
    }

    /// Global top-K by BM25 score across every shard.
    ///
    /// Refuses while any shard is missing: a short merge drops results while
    /// reading as a complete ranking. A shard silent past the gather timeout
    /// is reported as [`ClusterError::ShardTimeout`] instead, naming the first
    /// missing shard.
    pub fn merge_scored_hits(&self, top_k: usize) -> Result<MergedScoredHits> {
        self.check_phase2()?;

        let mut all_hits: Vec<ScoredHit> = self
            .scored_hits
            .values()
            .flat_map(|hits| hits.iter().cloned())
            .collect();
        all_hits.sort_by(|a, b| {
            b.bm25_score
                .partial_cmp(&a.bm25_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.shard_id.cmp(&b.shard_id))
        });
        all_hits.truncate(top_k);
        Ok(MergedScoredHits::new(all_hits))
    }

    /// Shards that were scattered to and are absent from `reported`.
    fn missing_shards(&self, reported: impl Iterator<Item = u32>) -> Vec<u32> {
        let reported: std::collections::BTreeSet<u32> = reported.collect();
        self.shard_ids
            .iter()
            .copied()
            .filter(|id| !reported.contains(id))
            .collect()
    }

    /// `Some(err)` once a missing shard has been silent past the gather timeout.
    fn timeout_error(&self, first_missing: u32) -> Option<ClusterError> {
        let elapsed = self.started_at.elapsed();
        if elapsed < self.gather_timeout {
            return None;
        }
        Some(ClusterError::ShardTimeout {
            vshard_id: first_missing,
            elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        })
    }

    fn check_phase1(&self) -> Result<()> {
        let missing = self.missing_shards(self.df_reports.keys().copied());
        let Some(&first_missing) = missing.first() else {
            return Ok(());
        };
        if let Some(err) = self.timeout_error(first_missing) {
            return Err(err);
        }
        Err(Bm25GatherError::DfReportsIncomplete {
            reported: self.df_reports.len(),
            expected: self.shard_ids.len(),
            missing,
        }
        .into())
    }

    fn check_phase2(&self) -> Result<()> {
        let missing = self.missing_shards(self.scored_hits.keys().copied());
        let Some(&first_missing) = missing.first() else {
            return Ok(());
        };
        if let Some(err) = self.timeout_error(first_missing) {
            return Err(err);
        }
        Err(Bm25GatherError::ScoredHitsIncomplete {
            responded: self.scored_hits.len(),
            expected: self.shard_ids.len(),
            missing,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df_report(
        shard_id: u32,
        total_docs: u64,
        total_token_sum: u64,
        dfs: &[(&str, u64)],
    ) -> ShardDfReport {
        ShardDfReport {
            shard_id,
            total_docs,
            total_token_sum,
            term_dfs: dfs
                .iter()
                .map(|(term, df)| ((*term).to_string(), *df))
                .collect(),
        }
    }

    fn hit(doc_id: &str, bm25_score: f64, shard_id: u32) -> ScoredHit {
        ScoredHit {
            doc_id: doc_id.to_string(),
            bm25_score,
            shard_id,
        }
    }

    #[test]
    fn global_idf_aggregates_every_shard() {
        let mut coord =
            GlobalIdfCoordinator::new(vec!["rust".into(), "database".into()], vec![0, 1]);

        coord
            .add_df_report(df_report(
                0,
                1000,
                100_000,
                &[("rust", 50), ("database", 200)],
            ))
            .expect("shard 0 is in the scatter set");
        coord
            .add_df_report(df_report(
                1,
                1000,
                120_000,
                &[("rust", 30), ("database", 300)],
            ))
            .expect("shard 1 is in the scatter set");

        assert!(coord.phase1_complete());
        let idf = coord.compute_global_idf().expect("every shard reported");

        assert_eq!(idf.total_docs(), 2000);
        // Global avg_doc_len = (100_000 + 120_000) / 2000 = 110.0
        assert!((idf.avg_doc_len() - 110.0).abs() < f64::EPSILON);
        // "rust": df=80, N=2000 → idf = ln((2000-80+0.5)/(80+0.5)+1) ≈ 3.2
        assert!(idf.idf("rust").expect("term was queried") > 3.0);
        // "database": df=500, N=2000 → idf = ln((2000-500+0.5)/(500+0.5)+1) ≈ 1.4
        assert!(idf.idf("database").expect("term was queried") > 1.0);
        assert!(idf.term_idfs()["database"] < idf.term_idfs()["rust"]); // "rust" is rarer.
    }

    #[test]
    fn global_idf_refused_while_a_shard_is_silent() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into()], vec![0, 1]);
        coord
            .add_df_report(df_report(0, 1000, 100_000, &[("rust", 50)]))
            .expect("shard 0 is in the scatter set");

        match coord.compute_global_idf() {
            Err(ClusterError::Bm25Gather(Bm25GatherError::DfReportsIncomplete {
                reported,
                expected,
                missing,
            })) => {
                assert_eq!(reported, 1);
                assert_eq!(expected, 2);
                assert_eq!(missing, vec![1]);
            }
            other => panic!("expected an incomplete-DF error, got {other:?}"),
        }
        assert!(coord.global_idf().is_none());
    }

    #[test]
    fn silent_shard_past_deadline_reports_timeout() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into()], vec![0, 1, 2])
            .with_timeout(Duration::from_millis(0));
        coord
            .add_df_report(df_report(0, 1000, 100_000, &[("rust", 50)]))
            .expect("shard 0 is in the scatter set");

        match coord.compute_global_idf() {
            Err(ClusterError::ShardTimeout { vshard_id, .. }) => assert_eq!(vshard_id, 1),
            other => panic!("expected a shard timeout, got {other:?}"),
        }
    }

    #[test]
    fn second_df_report_from_one_shard_refused() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into()], vec![0, 1]);
        coord
            .add_df_report(df_report(0, 1000, 100_000, &[("rust", 50)]))
            .expect("shard 0 is in the scatter set");

        match coord.add_df_report(df_report(0, 1000, 100_000, &[("rust", 50)])) {
            Err(ClusterError::Bm25Gather(Bm25GatherError::DuplicateDfReport { vshard_id })) => {
                assert_eq!(vshard_id, 0)
            }
            other => panic!("expected a duplicate-report error, got {other:?}"),
        }
        assert!(!coord.phase1_complete());
    }

    #[test]
    fn df_report_from_unscattered_shard_refused() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into()], vec![0, 1]);
        match coord.add_df_report(df_report(5, 10, 100, &[("rust", 1)])) {
            Err(ClusterError::Bm25Gather(Bm25GatherError::UnexpectedShard { vshard_id })) => {
                assert_eq!(vshard_id, 5)
            }
            other => panic!("expected an unexpected-shard error, got {other:?}"),
        }
    }

    #[test]
    fn scored_hits_merge_ranks_every_shard() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into()], vec![0, 1]);
        coord
            .record_scored_hits(0, vec![hit("a1", 5.0, 0), hit("a2", 3.0, 0)])
            .expect("shard 0 is in the scatter set");
        coord
            .record_scored_hits(1, vec![hit("b1", 4.5, 1), hit("b2", 2.0, 1)])
            .expect("shard 1 is in the scatter set");
        assert!(coord.phase2_complete());

        let merged = coord.merge_scored_hits(3).expect("every shard answered");
        assert_eq!(merged.len(), 3);
        assert_eq!(merged.hits()[0].doc_id, "a1"); // score 5.0
        assert_eq!(merged.hits()[1].doc_id, "b1"); // score 4.5
        assert_eq!(merged.hits()[2].doc_id, "a2"); // score 3.0
    }

    #[test]
    fn scored_hits_merge_refused_while_a_shard_is_silent() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rust".into()], vec![0, 1]);
        coord
            .record_scored_hits(0, vec![hit("a1", 5.0, 0)])
            .expect("shard 0 is in the scatter set");

        match coord.merge_scored_hits(3) {
            Err(ClusterError::Bm25Gather(Bm25GatherError::ScoredHitsIncomplete {
                responded,
                expected,
                missing,
            })) => {
                assert_eq!(responded, 1);
                assert_eq!(expected, 2);
                assert_eq!(missing, vec![1]);
            }
            other => panic!("expected an incomplete-merge error, got {other:?}"),
        }
    }

    #[test]
    fn rare_term_has_higher_idf() {
        let mut coord = GlobalIdfCoordinator::new(vec!["rare".into(), "common".into()], vec![0]);
        coord
            .add_df_report(df_report(
                0,
                10_000,
                1_000_000,
                &[("rare", 5), ("common", 9000)],
            ))
            .expect("shard 0 is in the scatter set");
        let idf = coord.compute_global_idf().expect("every shard reported");
        assert!(idf.term_idfs()["rare"] > idf.term_idfs()["common"]);
    }
}
