// SPDX-License-Identifier: BUSL-1.1

//! Decode one WAL `VectorParams` record and re-register the index
//! configuration it carries.
//!
//! Split out of `wal_replay_vector` so that file stays focused on driving the
//! record scan; the tuple-shape compatibility ladder lives here with the
//! encoder it mirrors (`control::server::wal_dispatch::vector`).

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::vector::distance::DistanceMetric;
use crate::engine::vector::hnsw::HnswParams;

impl CoreLoop {
    /// Restore the vector-index configuration a `VectorParams` record carries.
    ///
    /// A record for a tombstoned collection, or one whose payload matches none
    /// of the known tuple shapes, is counted in `skipped` and otherwise
    /// ignored — it describes an index that no longer exists, or one written
    /// by a version whose shape this build cannot read.
    pub(in crate::data::executor) fn restore_vector_params_record(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        record_lsn: u64,
        payload: &[u8],
        tombstones: &nodedb_wal::replay::filter::DatabaseTombstones<'_>,
        skipped: &mut usize,
    ) {
        // Fields are only ever appended to this tuple: the declared
        // dimension is the 10th element and the vector field name the
        // 9th. Older records carry 9 (no dim), 8 (no field name), or 4
        // (no quantization params) — try the widest shape first and
        // fall back, defaulting the missing tail to "not declared".
        let decoded = zerompk::from_msgpack::<(
            String,
            usize,
            usize,
            String,
            String,
            usize,
            usize,
            usize,
            String,
            usize,
        )>(payload)
        .ok()
        .map(|(c, m, ef, metric, _it, _pq, _ic, _ip, field, dim)| (c, m, ef, metric, field, dim))
        .or_else(|| {
            zerompk::from_msgpack::<(
                String,
                usize,
                usize,
                String,
                String,
                usize,
                usize,
                usize,
                String,
            )>(payload)
            .ok()
            .map(|(c, m, ef, metric, _it, _pq, _ic, _ip, field)| (c, m, ef, metric, field, 0))
        })
        .or_else(|| {
            zerompk::from_msgpack::<(String, usize, usize, String)>(payload)
                .ok()
                .map(|(c, m, ef, metric)| (c, m, ef, metric, String::new(), 0))
        });
        if let Some((collection, m, ef_construction, metric, field_name, dim)) = decoded {
            if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                *skipped += 1;
                return;
            }
            let index_key =
                CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
            let metric_enum = match metric.as_str() {
                "l2" | "euclidean" => DistanceMetric::L2,
                "cosine" => DistanceMetric::Cosine,
                "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
                "manhattan" | "l1" => DistanceMetric::Manhattan,
                "chebyshev" | "linf" => DistanceMetric::Chebyshev,
                "hamming" => DistanceMetric::Hamming,
                "jaccard" => DistanceMetric::Jaccard,
                "pearson" => DistanceMetric::Pearson,
                _ => DistanceMetric::Cosine,
            };
            let params = HnswParams {
                m,
                m0: m * 2,
                ef_construction,
                metric: metric_enum,
                dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
            };
            if dim > 0 {
                self.declared_dims.insert(index_key.clone(), dim);
            }
            self.vector_params.insert(index_key, params);
            tracing::debug!(
                core = self.core_id,
                %collection,
                field = %field_name,
                dim,
                m,
                ef_construction,
                %metric,
                "WAL replay: restored vector params"
            );
        }
    }
}
