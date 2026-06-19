// SPDX-License-Identifier: BUSL-1.1

//! Text search, hybrid search, and BM25-score-scan handlers for the Data Plane CoreLoop.

use std::collections::HashMap;

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Default hybrid search weight: 0.5 = equal vector + text.
const DEFAULT_VECTOR_WEIGHT: f32 = 0.5;

/// Upper bound on hits fetched by `BM25ScoreScan` to populate per-row scores.
/// The downstream BMW scorer pre-allocates `Vec::with_capacity(top_k)`, so
/// `usize::MAX` would overflow on element-size multiplication. One million is
/// well above any realistic collection size for in-process score injection
/// while staying safely allocatable.
const BM25_SCAN_MAX_HITS: usize = 1_000_000;

impl CoreLoop {
    /// Execute a full-text search using BM25 + optional fuzzy matching.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_text_search(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        query: &str,
        top_k: usize,
        fuzzy: bool,
        prefilter: Option<&nodedb_types::SurrogateBitmap>,
        rls_filters: &[u8],
    ) -> Response {
        let tenant_id = TenantId::new(tid);
        debug!(core = self.core_id, tid, %collection, %query, top_k, fuzzy, "text search");

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // Fetch extra candidates when RLS is active.
        let fetch_k = if rls_filters.is_empty() {
            top_k
        } else {
            top_k.saturating_mul(2).max(20)
        };

        let results = match self
            .inverted
            .search(tenant_id, collection, query, fetch_k, fuzzy, prefilter)
        {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let strict_schema = self.strict_schema_for(tenant_id, collection);
        let rows = self.hydrate_text_hits(
            task.request.database_id.as_u64(),
            tid,
            collection,
            results.iter().map(|r| (r.doc_id, r.score, r.fuzzy)),
            top_k,
            rls_filters,
            strict_schema.as_ref(),
        );

        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    fn strict_schema_for(
        &self,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<nodedb_types::columnar::StrictSchema> {
        let key = (tenant_id, collection.to_string());
        self.doc_configs.get(&key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn hydrate_text_hits<I>(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        hits: I,
        top_k: usize,
        rls_filters: &[u8],
        strict_schema: Option<&nodedb_types::columnar::StrictSchema>,
    ) -> Vec<DocumentRow>
    where
        I: IntoIterator<Item = (nodedb_types::Surrogate, f32, bool)>,
    {
        let mut rows: Vec<DocumentRow> = Vec::new();
        for (surrogate, score, fuzzy) in hits {
            if rows.len() >= top_k {
                break;
            }
            let hex_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
            let bytes_opt = match self.sparse.get(database_id, tid, collection, &hex_key) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        err = %e,
                        %hex_key,
                        %collection,
                        "sparse store error during text hit hydration; skipping row"
                    );
                    continue;
                }
            };
            // When the sparse store has no body for this surrogate the document
            // was indexed for FTS without a corresponding document write (e.g.
            // FtsIndex frames synced from Lite). Return a minimal row containing
            // only the surrogate-derived ID so callers that only project `id`
            // (the common case in sync interop tests and CDC pipelines) still
            // receive a result. RLS filters are skipped when there is no body.
            let mut value = if let Some(ref bytes) = bytes_opt {
                if !rls_filters.is_empty()
                    && !super::rls_eval::rls_check_msgpack_bytes(rls_filters, bytes)
                {
                    continue;
                }
                decode_scanned_document(bytes, strict_schema)
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            };
            if let serde_json::Value::Object(ref mut map) = value {
                map.insert(
                    "score".to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(score as f64)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
                map.insert("fuzzy".to_string(), serde_json::Value::Bool(fuzzy));
            }
            rows.push(DocumentRow {
                id: hex_key,
                data: value,
            });
        }
        rows
    }

    /// Execute an exact phrase search.
    ///
    /// Returns only documents where `terms` appear as a contiguous sequence.
    /// Scoring is positional: documents with the phrase nearer the start rank higher.
    pub(in crate::data::executor) fn execute_phrase_search(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        terms: &[String],
        top_k: usize,
        prefilter: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        let tenant_id = TenantId::new(tid);
        debug!(core = self.core_id, tid, %collection, term_count = terms.len(), top_k, "phrase search");

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let results = match self
            .inverted
            .phrase_search(tenant_id, collection, terms, top_k, prefilter)
        {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let strict_schema = self.strict_schema_for(tenant_id, collection);
        let rows = self.hydrate_text_hits(
            task.request.database_id.as_u64(),
            tid,
            collection,
            results.iter().map(|r| (r.doc_id, r.score, false)),
            top_k,
            &[],
            strict_schema.as_ref(),
        );
        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a full-collection scan with BM25 score injected per row.
    ///
    /// Runs an FTS search to build a surrogate → score map, then scans every
    /// document in the collection. Each document is returned with `score_alias`
    /// injected as an additional field. Documents whose surrogate does not appear
    /// in the score map receive `null` for the score column.
    pub(in crate::data::executor) fn execute_bm25_score_scan(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        query: &str,
        score_alias: &str,
        fuzzy: bool,
    ) -> Response {
        let tenant_id = TenantId::new(tid);
        debug!(core = self.core_id, tid, %collection, %query, %score_alias, "bm25 score scan");

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // Build a surrogate → score map from FTS hits. Bounded top_k: heap
        // allocation in BMW search is `Vec::with_capacity(top_k)`, so a literal
        // `usize::MAX` overflows on `top_k * size_of::<Element>()`.
        let score_map: HashMap<nodedb_types::Surrogate, f32> = match self.inverted.search(
            tenant_id,
            collection,
            query,
            BM25_SCAN_MAX_HITS,
            fuzzy,
            None,
        ) {
            Ok(hits) => hits.into_iter().map(|h| (h.doc_id, h.score)).collect(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Retrieve the strict schema (if any) so binary-tuple rows decode correctly.
        let config_key = (tenant_id, collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Scan all documents and inject the score field.
        let scan_result = self.sparse.scan_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            BM25_SCAN_MAX_HITS,
        );
        let docs = match scan_result {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let mut rows: Vec<DocumentRow> = Vec::with_capacity(docs.len());
        for (hex_key, bytes) in &docs {
            let mut value = decode_scanned_document(bytes, strict_schema.as_ref());
            // Inject score into the document object.
            if let serde_json::Value::Object(ref mut map) = value {
                let score = crate::engine::document::store::doc_id_to_surrogate(hex_key)
                    .and_then(|s| score_map.get(&s).copied());
                match score {
                    Some(s) => {
                        map.insert(
                            score_alias.to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(s as f64)
                                    .unwrap_or_else(|| serde_json::Number::from(0)),
                            ),
                        );
                    }
                    None => {
                        map.insert(score_alias.to_string(), serde_json::Value::Null);
                    }
                }
            }
            rows.push(DocumentRow {
                id: hex_key.clone(),
                data: value,
            });
        }

        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a hybrid search: vector + text, fused via weighted RRF.
    ///
    /// `score_alias` overrides the response field name for the RRF score
    /// column. When `None` the executor uses the fixed default `rrf_score`.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_hybrid_search(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        query_vector: &[f32],
        query_text: &str,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        vector_weight: f32,
        filter_bitmap: Option<&nodedb_types::SurrogateBitmap>,
        rls_filters: &[u8],
        score_alias: Option<&str>,
    ) -> Response {
        let tenant_id = TenantId::new(tid);
        debug!(
            core = self.core_id,
            tid,
            %collection,
            %query_text,
            top_k,
            vector_weight,
            "hybrid search"
        );

        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let weight = if vector_weight <= 0.0 || vector_weight >= 1.0 {
            DEFAULT_VECTOR_WEIGHT
        } else {
            vector_weight
        };
        let text_weight = 1.0 - weight;

        // Fetch more candidates than top_k from each engine so RRF has
        // enough material to fuse. 3x is a good balance.
        let fetch_k = top_k.saturating_mul(3).max(20);

        // 1. Vector search.
        let index_key = CoreLoop::vector_index_key(tid, collection, "");
        let vector_collection = self.vector_collections.get(&index_key);
        let vector_results = if let Some(index) = vector_collection {
            if index.is_empty() {
                Vec::new()
            } else {
                let ef = if ef_search > 0 {
                    ef_search.max(fetch_k)
                } else {
                    fetch_k.saturating_mul(4).max(64)
                };
                match filter_bitmap {
                    Some(surrogate_bm) => {
                        let mut buf = Vec::with_capacity(surrogate_bm.0.serialized_size());
                        if surrogate_bm.0.serialize_into(&mut buf).is_ok() {
                            index.search_with_bitmap_bytes(query_vector, fetch_k, ef, &buf)
                        } else {
                            index.search(query_vector, fetch_k, ef)
                        }
                    }
                    None => index.search(query_vector, fetch_k, ef),
                }
            }
        } else {
            Vec::new()
        };

        // 2. Text search (no surrogate prefilter for the text leg of hybrid search).
        let text_results = self
            .inverted
            .search(tenant_id, collection, query_text, fetch_k, fuzzy, None)
            .unwrap_or_default();

        // 3. Build ranked lists for weighted RRF.
        // Higher weight → lower k → steeper rank discount → more influence.
        use crate::query::fusion::{RankedResult, reciprocal_rank_fusion_weighted};

        let base_k = 60.0_f64;
        let k_vector = if weight > 0.01 {
            base_k / weight as f64
        } else {
            base_k * 100.0
        };
        let k_text = if text_weight > 0.01 {
            base_k / text_weight as f64
        } else {
            base_k * 100.0
        };

        // Translate vector local-hnsw IDs to surrogate-hex doc_ids so the
        // vector and text legs share the same RRF key space. Headless rows
        // (no surrogate binding) fall back to a non-fusable sentinel —
        // they cannot match any FTS doc_id, which is the correct behavior.
        let vector_ranked: Vec<RankedResult> = vector_results
            .iter()
            .enumerate()
            .map(|(rank, r)| {
                let document_id = vector_collection
                    .and_then(|c| c.get_surrogate(r.id))
                    .map(crate::engine::document::store::surrogate_to_doc_id)
                    .unwrap_or_else(|| format!("__local_{}", r.id));
                RankedResult {
                    document_id,
                    rank,
                    score: r.distance,
                    source: "vector",
                }
            })
            .collect();

        let text_ranked: Vec<RankedResult> = text_results
            .iter()
            .enumerate()
            .map(|(rank, r)| RankedResult {
                document_id: crate::engine::document::store::surrogate_to_doc_id(r.doc_id),
                rank,
                score: r.score,
                source: "text",
            })
            .collect();

        let fused = reciprocal_rank_fusion_weighted(
            &[vector_ranked, text_ranked],
            &[k_vector, k_text],
            top_k,
        );

        // Build response with per-engine rank diagnostics.
        // RLS post-fusion: filter fused results by looking up each document.
        let results: Vec<_> = fused
            .iter()
            .filter(|f| {
                if rls_filters.is_empty() {
                    return true;
                }
                match self.sparse.get(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    &f.document_id,
                ) {
                    Ok(Some(bytes)) => {
                        super::rls_eval::rls_check_msgpack_bytes(rls_filters, &bytes)
                    }
                    _ => false,
                }
            })
            .map(|f| {
                let vector_rank = vector_results.iter().position(|r| {
                    let doc_id = vector_collection
                        .and_then(|c| c.get_surrogate(r.id))
                        .map(crate::engine::document::store::surrogate_to_doc_id)
                        .unwrap_or_else(|| format!("__local_{}", r.id));
                    doc_id == f.document_id
                });
                let text_rank = text_results.iter().position(|r| {
                    crate::engine::document::store::surrogate_to_doc_id(r.doc_id) == f.document_id
                });

                super::super::response_codec::HybridSearchHit {
                    doc_id: &f.document_id,
                    score_field: score_alias.unwrap_or("rrf_score"),
                    rrf_score: f.rrf_score,
                    vector_rank,
                    text_rank,
                }
            })
            .collect();

        if let Some(ref m) = self.metrics {
            m.record_fts_search(0);
        }
        match super::super::response_codec::encode(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
