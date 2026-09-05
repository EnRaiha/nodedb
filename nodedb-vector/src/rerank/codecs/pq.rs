// SPDX-License-Identifier: Apache-2.0

//! `RerankCodec` wrapper for Product Quantization (PQ).
//!
//! PQ is a training-based codec: `train()` runs k-means over a sample of
//! vectors to learn per-subspace codebooks. Until training is complete,
//! `encode` and `prepare_query` return `RerankError::NotTrained`.
//!
//! Distance uses the ADC (Asymmetric Distance Computation) model: the query
//! is kept in FP32 and a per-subspace lookup table is precomputed once via
//! `prepare_query`; each candidate lookup is then O(M) table additions.
//! The prepared form maps directly to `PreparedQuery::Lut` — the existing
//! variant already holds `Vec<Vec<f32>>` which is exactly the PQ distance
//! table (`lut[sub][centroid]`).

use nodedb_codec::vector_quant::layout::UnifiedQuantizedVectorRef;
use nodedb_mem::ScopedMemory;

use crate::{
    quantize::pq::PqCodec,
    rerank::codec::{CodecName, PreparedQuery, RerankCodec},
    rerank::types::RerankError,
};

// ── packed_bits_len helper ────────────────────────────────────────────────────

/// PQ stores one centroid-index byte per subspace: packed_bits_len == m.
#[inline]
fn pq_packed_bits_len(m: usize) -> usize {
    m
}

// ── PqRerank ──────────────────────────────────────────────────────────────────

/// Object-safe `RerankCodec` wrapper around `PqCodec`.
///
/// The codec starts untrained. `encode` and `prepare_query` return
/// `RerankError::NotTrained` until `train()` has been called with a
/// representative sample of vectors.
///
/// `from_codec` accepts a pre-trained `PqCodec` (used when restoring from
/// a snapshot).
pub struct PqRerank {
    codec: Option<PqCodec>,
    dim: usize,
    m: usize,
    k: usize,
    max_iter: usize,
    /// Charges the codebooks a future `train()` call learns against the
    /// bound database, tenant, and engine budget.
    memory: ScopedMemory,
}

impl PqRerank {
    /// Construct an untrained PQ codec configuration.
    ///
    /// `m` is the number of subspaces; `k` is centroids per subspace.
    /// Defaults used by higher-level callers: `m = 8`, `k = 256`.
    /// `encode` / `distance_prepared` return `RerankError::NotTrained` until
    /// `train()` has been called.
    pub fn new(dim: usize, m: usize, k: usize, memory: ScopedMemory) -> Self {
        Self {
            codec: None,
            dim,
            m,
            k,
            max_iter: 25,
            memory,
        }
    }

    /// Construct from a pre-trained codec (used when restoring from snapshot).
    pub fn from_codec(codec: PqCodec, memory: ScopedMemory) -> Self {
        let dim = codec.dim;
        let m = codec.m;
        let k = codec.k;
        Self {
            codec: Some(codec),
            dim,
            m,
            k,
            max_iter: 25,
            memory,
        }
    }
}

impl RerankCodec for PqRerank {
    /// Encode a full-precision vector to PQ bytes (one centroid index per subspace).
    ///
    /// The serialized form is the raw `UnifiedQuantizedVector` buffer
    /// (`as_bytes()`): 32-byte `QuantHeader` followed by `m` code bytes.
    fn encode(&self, v: &[f32]) -> Result<Vec<u8>, RerankError> {
        if v.len() != self.dim {
            return Err(RerankError::BadInput(format!(
                "pq encode: vector len {} != codec dim {}",
                v.len(),
                self.dim
            )));
        }
        let codec = self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained(
                "pq: codec must be trained before encoding (call train() with a sample of vectors)"
                    .to_string(),
            )
        })?;
        use nodedb_codec::vector_quant::codec::VectorCodec;
        let quantized = <PqCodec as VectorCodec>::encode(codec, v);
        Ok(quantized.as_ref().as_bytes().to_vec())
    }

    /// Prepare the query by precomputing the M×K asymmetric distance table.
    ///
    /// The prepared form is `PreparedQuery::Lut` where `lut[sub][centroid]`
    /// holds the squared L2 distance from the query's sub-vector to each
    /// centroid of subspace `sub`. This is the standard ADC lookup table.
    fn prepare_query(&self, q: &[f32]) -> Result<PreparedQuery, RerankError> {
        if q.len() != self.dim {
            return Err(RerankError::BadInput(format!(
                "pq prepare_query: query len {} != codec dim {}",
                q.len(),
                self.dim
            )));
        }
        let codec = self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained(
                "pq: codec must be trained before prepare_query (call train() with a sample of vectors)"
                    .to_string(),
            )
        })?;
        use nodedb_codec::vector_quant::codec::VectorCodec;
        let pq_query = <PqCodec as VectorCodec>::prepare_query(codec, q);
        Ok(PreparedQuery::Lut(pq_query.distance_table))
    }

    /// Compute asymmetric ADC distance from a prepared query to a PQ-encoded
    /// candidate.
    ///
    /// Expects `PreparedQuery::Lut` produced by `prepare_query`.
    fn distance_prepared(
        &self,
        prepared: &PreparedQuery,
        encoded: &[u8],
    ) -> Result<f32, RerankError> {
        let lut = match prepared {
            PreparedQuery::Lut(t) => t,
            _ => {
                return Err(RerankError::BadInput(
                    "pq distance: expected PreparedQuery::Lut".to_string(),
                ));
            }
        };

        let packed_len = pq_packed_bits_len(self.m);
        let uqv_ref = UnifiedQuantizedVectorRef::from_bytes(encoded, packed_len).map_err(|e| {
            RerankError::BadInput(format!("pq distance: failed to parse encoded bytes: {e}"))
        })?;

        let packed = uqv_ref.packed_bits();
        // ADC: sum lut[sub][code[sub]] for each subspace.
        let dist = packed
            .iter()
            .enumerate()
            .map(|(sub, &code)| {
                lut.get(sub)
                    .and_then(|row| row.get(code as usize).copied())
                    .unwrap_or(0.0)
            })
            .sum();
        Ok(dist)
    }

    fn name(&self) -> CodecName {
        CodecName::Pq
    }

    fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
        let codec = self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained("pq sidecar serialize: codec not trained".to_string())
        })?;
        codec
            .to_bytes()
            .map_err(|e| RerankError::BadInput(format!("pq to_bytes: {e}")))
    }

    /// Train PQ codebooks via k-means on a sample of vectors.
    ///
    /// Validates that:
    /// - `samples` is non-empty.
    /// - Every sample has length `self.dim`.
    /// - `self.dim % self.m == 0` (PQ requires divisible dimensionality).
    /// - At least `self.k` samples are provided (k-means needs ≥ k points).
    ///
    /// On success, stores the trained codec and subsequent `encode` /
    /// `distance_prepared` calls will succeed.
    fn train(&mut self, samples: &[&[f32]]) -> Result<(), RerankError> {
        if samples.is_empty() {
            return Err(RerankError::BadInput(
                "pq train: empty sample set".to_string(),
            ));
        }
        for s in samples {
            if s.len() != self.dim {
                return Err(RerankError::BadInput(format!(
                    "pq train: sample has len {} but codec dim is {}",
                    s.len(),
                    self.dim
                )));
            }
        }
        if !self.dim.is_multiple_of(self.m) {
            return Err(RerankError::BadInput(format!(
                "pq train: dim ({}) must be divisible by m ({})",
                self.dim, self.m
            )));
        }
        if samples.len() < self.k {
            return Err(RerankError::BadInput(format!(
                "pq train: need >= k samples for k-means, got {}",
                samples.len()
            )));
        }
        let codec = PqCodec::train(
            samples,
            self.dim,
            self.m,
            self.k,
            self.max_iter,
            self.memory.clone(),
        );
        self.codec = Some(codec);
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    const DIM: usize = 32;
    const M: usize = 4;
    const K: usize = 8;
    const N: usize = 64;

    fn det_vec(i: usize, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| ((i * 31 + j) % 100) as f32 / 100.0)
            .collect()
    }

    fn trained() -> PqRerank {
        let vecs: Vec<Vec<f32>> = (0..N).map(|i| det_vec(i, DIM)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut codec = PqRerank::new(DIM, M, K, test_memory());
        codec.train(&refs).expect("train must succeed");
        codec
    }

    #[test]
    fn train_then_encode_roundtrip() {
        let codec = trained();
        let v = det_vec(0, DIM);
        let enc = codec.encode(&v).expect("encode");
        let prep = codec.prepare_query(&v).expect("prepare_query");
        let dist = codec.distance_prepared(&prep, &enc).expect("distance");
        assert!(dist.is_finite(), "distance must be finite, got {dist}");
        assert!(dist >= 0.0, "distance must be non-negative, got {dist}");
        // Self-distance should be small for ADC on identical vector.
        assert!(dist < 1.0, "self-distance too large: {dist}");
    }

    #[test]
    fn encode_before_train_returns_not_trained() {
        let codec = PqRerank::new(DIM, M, K, test_memory());
        let v = det_vec(0, DIM);
        let err = codec.encode(&v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("not trained") || msg.contains("trained"),
            "expected 'trained' in error, got: {msg}"
        );
    }

    #[test]
    fn train_with_wrong_dim_sample_fails() {
        let vecs: Vec<Vec<f32>> = (0..N).map(|i| det_vec(i, DIM)).collect();
        let mut refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let bad = det_vec(0, DIM + 4);
        refs.push(bad.as_slice());
        let mut codec = PqRerank::new(DIM, M, K, test_memory());
        let err = codec.train(&refs).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("bad input"),
            "expected bad input error, got: {msg}"
        );
    }

    #[test]
    fn train_with_indivisible_dim_fails() {
        // dim=33, m=4: 33 % 4 != 0
        let vecs: Vec<Vec<f32>> = (0..16).map(|i| det_vec(i, 33)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut codec = PqRerank::new(33, 4, 8, test_memory());
        let err = codec.train(&refs).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("divisible"),
            "expected divisibility error, got: {msg}"
        );
    }

    #[test]
    fn train_with_too_few_samples_fails() {
        // k=8 but only 4 samples
        let vecs: Vec<Vec<f32>> = (0..4).map(|i| det_vec(i, DIM)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut codec = PqRerank::new(DIM, M, 8, test_memory());
        let err = codec.train(&refs).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("k samples") || msg.contains("bad input"),
            "expected sample count error, got: {msg}"
        );
    }

    #[test]
    fn name_is_pq() {
        let codec = PqRerank::new(DIM, M, K, test_memory());
        assert_eq!(codec.name(), CodecName::Pq);
    }
}
