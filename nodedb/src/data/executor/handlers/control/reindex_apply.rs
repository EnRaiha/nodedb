// SPDX-License-Identifier: BUSL-1.1

//! Background-thread rebuild functions and Data-Plane cutover appliers for
//! concurrent index rebuild. See `reindex.rs` for the dispatch/poll surface.
//!
//! All functions in this module either run on a plain OS thread (operating on
//! pure `Send` data) or run on the owning Data Plane core during cutover. They
//! never spawn tokio tasks and never share `!Send` state across threads.

use tracing::{error, info, warn};

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TenantId;

// ── Background-thread output types (all Send) ────────────────────────────────

pub(super) enum RebuildOutput {
    /// Serialized rebuilt HNSW index bytes.
    Hnsw { bytes: Vec<u8> },
    /// Serialized rebuilt CSR bytes.
    Csr { bytes: Vec<u8> },
    /// Compacted FTS data ready for write-back.
    Fts(FtsRebuild),
}

/// Compacted FTS state produced by a rebuild thread, ready to apply to the live
/// backend on the owning Data Plane core.
pub(super) struct FtsRebuild {
    pub postings: Vec<(String, Vec<nodedb_fts::posting::Posting>)>,
    pub doc_lengths: Vec<(nodedb_types::Surrogate, u32)>,
    pub doc_count: u32,
    pub total_tokens: u64,
    pub analyzer_meta: Option<Vec<u8>>,
}

// ── Background thread rebuild functions (pure Send, no !Send types) ──────────

pub(super) fn rebuild_hnsw_thread(
    vectors: Vec<Vec<f32>>,
    dim: usize,
    params: nodedb_vector::HnswParams,
) -> crate::Result<RebuildOutput> {
    let mut index = nodedb_vector::HnswIndex::new(dim, params);
    for v in vectors {
        index.insert(v).map_err(|e| crate::Error::Storage {
            engine: "vector".to_string(),
            detail: format!("HNSW insert: {e}"),
        })?;
    }
    Ok(RebuildOutput::Hnsw {
        bytes: index
            .checkpoint_to_bytes()
            .map_err(|e| crate::Error::Storage {
                engine: "vector".to_string(),
                detail: format!("HNSW checkpoint encode: {e}"),
            })?,
    })
}

pub(super) fn rebuild_fts_thread(input: FtsRebuild) -> crate::Result<RebuildOutput> {
    // Compact: deduplicate posting entries by surrogate, keeping highest TF.
    let mut compacted: Vec<(String, Vec<nodedb_fts::posting::Posting>)> =
        Vec::with_capacity(input.postings.len());
    for (term, mut ps) in input.postings {
        ps.sort_unstable_by(|a, b| {
            a.doc_id
                .as_u32()
                .cmp(&b.doc_id.as_u32())
                .then(b.term_freq.cmp(&a.term_freq))
        });
        ps.dedup_by_key(|p| p.doc_id.as_u32());
        if !ps.is_empty() {
            compacted.push((term, ps));
        }
    }
    Ok(RebuildOutput::Fts(FtsRebuild {
        postings: compacted,
        ..input
    }))
}

pub(super) fn rebuild_csr_thread(snapshot_bytes: Vec<u8>) -> crate::Result<RebuildOutput> {
    let restored = nodedb_graph::CsrIndex::from_checkpoint(&snapshot_bytes)
        .map_err(|e| crate::Error::Storage {
            engine: "graph".to_string(),
            detail: format!("CSR restore: {e}"),
        })?
        .ok_or_else(|| crate::Error::Storage {
            engine: "graph".to_string(),
            detail: "CSR checkpoint bytes missing magic header".to_string(),
        })?;

    let mut rebuilt = restored;
    rebuilt.compact().map_err(|e| crate::Error::Storage {
        engine: "graph".to_string(),
        detail: format!("CSR compact: {e}"),
    })?;

    let bytes = rebuilt
        .checkpoint_to_bytes()
        .map_err(|e| crate::Error::Storage {
            engine: "graph".to_string(),
            detail: format!("CSR re-serialize: {e}"),
        })?;

    Ok(RebuildOutput::Csr { bytes })
}

// ── Cutover: apply rebuilt state to Data Plane in-memory structures ───────────

pub(super) fn apply_hnsw(
    core: &mut CoreLoop,
    database_id: &nodedb_types::DatabaseId,
    tenant_id: &TenantId,
    collection_key: &str,
    bytes: Vec<u8>,
) {
    use nodedb_vector::HnswIndex;
    use nodedb_vector::collection::segment::SealedSegment;
    use nodedb_vector::collection::tier::StorageTier;

    let index = match HnswIndex::from_checkpoint(&bytes) {
        Ok(Some(idx)) => idx,
        Ok(None) => {
            warn!(
                core = core.core_id,
                collection = %collection_key,
                "HNSW rebuild: checkpoint had no magic; skipping cutover"
            );
            return;
        }
        Err(e) => {
            error!(
                core = core.core_id,
                collection = %collection_key,
                error = %e,
                "HNSW rebuild: restore failed; live index unchanged"
            );
            return;
        }
    };

    let key = (*database_id, *tenant_id, collection_key.to_string());
    if let Some(coll) = core.vector_collections.get_mut(&key) {
        let new_seg = SealedSegment {
            index,
            base_id: 0,
            sq8: None,
            pq: None,
            tier: StorageTier::L0Ram,
            mmap_vectors: None,
        };
        coll.replace_sealed(vec![new_seg]);
        info!(
            target: "nodedb::reindex",
            core = core.core_id,
            collection = %collection_key,
            "atomic_cutover",
        );
    }
}

pub(super) fn apply_fts(
    core: &mut CoreLoop,
    database_id: &nodedb_types::DatabaseId,
    tenant_id: &TenantId,
    collection_key: &str,
    rebuild: FtsRebuild,
) {
    use nodedb_fts::backend::FtsBackend;

    let FtsRebuild {
        postings,
        doc_lengths,
        doc_count,
        total_tokens,
        analyzer_meta,
    } = rebuild;

    let db_u64 = database_id.as_u64();
    let tid = tenant_id.as_u64();
    let backend = core.inverted.backend();

    // Purge existing postings for the collection, then write the compacted set.
    if let Err(e) = backend.purge_collection(db_u64, tid, collection_key) {
        error!(
            core = core.core_id,
            collection = %collection_key,
            error = %e,
            "FTS rebuild purge failed; index may be inconsistent"
        );
        return;
    }

    for (term, ps) in &postings {
        if let Err(e) = backend.write_postings(db_u64, tid, collection_key, term, ps) {
            error!(
                core = core.core_id,
                term,
                error = %e,
                "FTS rebuild write_postings failed"
            );
            return;
        }
    }
    for (surrogate, len) in &doc_lengths {
        if let Err(e) = backend.write_doc_length(db_u64, tid, collection_key, *surrogate, *len) {
            error!(
                core = core.core_id,
                error = %e,
                "FTS rebuild write_doc_length failed"
            );
            return;
        }
    }
    // Restore collection-level stats via synthetic increment.
    for _ in 0..doc_count {
        // increment_stats with doc_len=0 just bumps the doc counter.
        let _ = backend.increment_stats(db_u64, tid, collection_key, 0);
    }
    // Restore total_tokens by writing a single doc with all tokens.
    if total_tokens > 0 && doc_count > 0 {
        let tokens_per_doc = (total_tokens / doc_count as u64) as u32;
        let _ = backend.increment_stats(db_u64, tid, collection_key, tokens_per_doc);
    }

    if let Some(meta_bytes) = analyzer_meta
        && let Err(e) = backend.write_meta(db_u64, tid, collection_key, "analyzer", &meta_bytes)
    {
        warn!(
            core = core.core_id,
            error = %e,
            "FTS rebuild: analyzer meta write failed"
        );
    }

    info!(
        core = core.core_id,
        collection = %collection_key,
        terms = postings.len(),
        docs = doc_lengths.len(),
        "FTS cutover: postings replaced with compacted snapshot"
    );
}

pub(super) fn apply_csr(
    core: &mut CoreLoop,
    database_id: &nodedb_types::DatabaseId,
    tenant_id: &TenantId,
    collection_key: &str,
    bytes: Vec<u8>,
) {
    let rebuilt = match nodedb_graph::CsrIndex::from_checkpoint(&bytes) {
        Ok(Some(r)) => r,
        Ok(None) => {
            warn!(
                core = core.core_id,
                collection = %collection_key,
                "CSR rebuild: empty checkpoint; skipping cutover"
            );
            return;
        }
        Err(e) => {
            error!(
                core = core.core_id,
                collection = %collection_key,
                error = %e,
                "CSR rebuild: restore failed; live index unchanged"
            );
            return;
        }
    };

    let rebuilt = if let Some(gov) = core.governor.clone() {
        rebuilt.with_governor_attached(gov)
    } else {
        rebuilt
    };

    core.csr
        .install_partition(*database_id, *tenant_id, rebuilt);
    info!(
        core = core.core_id,
        collection = %collection_key,
        "CSR cutover: partition replaced with rebuilt index"
    );
}
