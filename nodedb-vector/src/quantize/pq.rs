// SPDX-License-Identifier: Apache-2.0

//! Product Quantization (PQ): 8-16x compression for large datasets.
//!
//! Splits D-dimensional vectors into M subvectors, clusters each subspace
//! with K=256 centroids via k-means. Each vector is encoded as M bytes
//! (one centroid index per subvector).
//!
//! Distance is computed via precomputed lookup tables: for each query,
//! build a `[M][K]` table of distances from the query's subvectors to
//! all centroids. Then the distance to any encoded vector is just M
//! table lookups + additions — O(M) per candidate vs O(D) for FP32.
//!
//! Trade-off: 2-5% recall loss vs SQ8's <1%, but 2-4x more compression
//! (8-16x total vs 4x for SQ8). Best for cost-sensitive large datasets.

use std::mem::size_of;
use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor};
use nodedb_types::decode_bounds::checked_decode_capacity;
use serde::{Deserialize, Serialize};

use crate::error::VectorError;

/// Hard ceiling for a decoded PQ vector. This bounds corrupted persisted
/// configuration even when the codec has no memory governor attached.
const MAX_PQ_DECODE_DIM: usize = 1_048_576;
const MAX_PQ_CODEBOOK_BYTES: usize = 64 * 1024 * 1024;
// MessagePack stores each f32 as a marker plus four payload bytes and also
// carries nested-array headers. A 96 MiB envelope covers every codec whose
// complete decoded allocation (floats plus Vec headers) fits the 64 MiB cap.
const MAX_PQ_SERIALIZED_BYTES: usize = 96 * 1024 * 1024;

fn pq_codebook_allocation_bytes(m: usize, k: usize, sub_dim: usize) -> Option<usize> {
    let outer_headers = m.checked_mul(size_of::<Vec<Vec<f32>>>())?;
    let centroid_count = m.checked_mul(k)?;
    let centroid_headers = centroid_count.checked_mul(size_of::<Vec<f32>>())?;
    let float_bytes = centroid_count
        .checked_mul(sub_dim)?
        .checked_mul(size_of::<f32>())?;
    outer_headers
        .checked_add(centroid_headers)?
        .checked_add(float_bytes)
}

/// Reserve `bytes` from `governor` for `EngineId::Vector`, or succeed silently
/// when no governor is configured. The returned guard (if any) must be kept
/// alive for the duration of the allocation it covers.
#[inline]
fn try_reserve_or_skip(
    governor: &Option<Arc<MemoryGovernor>>,
    bytes: usize,
) -> Result<Option<nodedb_mem::BudgetGuard>, VectorError> {
    match governor {
        Some(g) => Ok(Some(g.reserve(EngineId::Vector, bytes)?)),
        None => Ok(None),
    }
}

/// PQ codec with trained codebooks.
#[derive(
    Clone, Debug, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PqCodec {
    /// Original vector dimensionality.
    pub dim: usize,
    /// Number of subvectors (subspaces).
    pub m: usize,
    /// Centroids per subvector (fixed at 256 for u8 encoding).
    pub k: usize,
    /// Dimensions per subvector: `dim / m`.
    pub sub_dim: usize,
    /// Codebooks: `codebooks[subspace][centroid][sub_dim_component]`.
    /// Total: M × K × sub_dim floats.
    codebooks: Vec<Vec<Vec<f32>>>,

    /// Optional memory governor. Skipped during serialization — it is a
    /// runtime concern only, not part of the on-disk format.
    #[serde(skip, default)]
    #[msgpack(ignore)]
    governor: Option<Arc<MemoryGovernor>>,
}

impl PqCodec {
    /// Attach a memory governor to this codec.
    ///
    /// Once set, heap-significant operations (`train`, `encode_batch`,
    /// `build_distance_table`, `decode`, `to_bytes`) will charge the
    /// `EngineId::Vector` budget before allocating and release the reservation
    /// when the returned value is dropped (RAII).  When no governor is set
    /// those operations proceed unconditionally, preserving backward
    /// compatibility with callers that do not use the memory governor.
    ///
    /// The governor is a runtime concern only — it is **not** serialized.
    pub fn with_governor(mut self, governor: Arc<MemoryGovernor>) -> Self {
        self.governor = Some(governor);
        self
    }

    /// Train PQ codebooks from a set of training vectors via k-means.
    ///
    /// `m` = number of subvectors (must divide `dim` evenly).
    /// `k` = centroids per subvector (typically 256).
    /// `max_iter` = k-means iterations (20 is usually sufficient).
    pub fn train(vectors: &[&[f32]], dim: usize, m: usize, k: usize, max_iter: usize) -> Self {
        assert!(!vectors.is_empty());
        assert!(
            dim > 0
                && dim <= MAX_PQ_DECODE_DIM
                && m > 0
                && k > 0
                && k <= usize::from(u8::MAX) + 1
                && k <= vectors.len()
        );
        assert!(
            dim.is_multiple_of(m),
            "dim ({dim}) must be divisible by m ({m})"
        );

        let sub_dim = dim / m;
        let codebook_bytes = pq_codebook_allocation_bytes(m, k, sub_dim);
        assert!(codebook_bytes.is_some_and(|bytes| bytes <= MAX_PQ_CODEBOOK_BYTES));
        let mut codebooks = Vec::with_capacity(m);

        for sub in 0..m {
            let offset = sub * sub_dim;
            // Extract sub-vectors for this subspace.
            let sub_vectors: Vec<&[f32]> = vectors
                .iter()
                .map(|v| &v[offset..offset + sub_dim])
                .collect();

            let centroids = kmeans(&sub_vectors, sub_dim, k, max_iter);
            codebooks.push(centroids);
        }

        Self {
            dim,
            m,
            k,
            sub_dim,
            codebooks,
            governor: None,
        }
    }

    /// Encode a vector: for each subvector, find the nearest centroid index.
    ///
    /// This is a per-vector hot-path operation.  Governor charging is
    /// intentionally skipped here to avoid atomic overhead on every candidate
    /// during search; use [`encode_batch`] for bulk encoding with budget
    /// enforcement.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dim);
        let mut code = Vec::with_capacity(self.m);
        for sub in 0..self.m {
            let offset = sub * self.sub_dim;
            let sub_vec = &vector[offset..offset + self.sub_dim];
            let nearest = self.nearest_centroid(sub, sub_vec);
            code.push(nearest as u8);
        }
        code
    }

    /// Batch encode all vectors into a contiguous byte array.
    ///
    /// Charges `m * vectors.len()` bytes to the governor budget (if set)
    /// before allocating the output buffer.  The guard is released at
    /// the end of this call — the buffer itself remains alive.
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Result<Vec<u8>, VectorError> {
        let capacity = self.m * vectors.len();
        let _g = try_reserve_or_skip(&self.governor, capacity * size_of::<u8>())?;
        let mut out = Vec::with_capacity(capacity);
        for v in vectors {
            out.extend(self.encode(v));
        }
        Ok(out)
    }

    /// Build an asymmetric distance table for a query vector.
    ///
    /// Returns `table[sub][centroid]` = distance from query's sub-vector
    /// to each centroid. Pre-computing this table makes distance evaluation
    /// O(M) per candidate instead of O(D).
    ///
    /// Charges `m * k * size_of::<f32>()` bytes to the governor (if set)
    /// before allocating the table.
    pub fn build_distance_table(&self, query: &[f32]) -> Result<Vec<Vec<f32>>, VectorError> {
        debug_assert_eq!(query.len(), self.dim);
        let total_bytes = self.m * self.k * size_of::<f32>();
        let _g = try_reserve_or_skip(&self.governor, total_bytes)?;
        let mut table = Vec::with_capacity(self.m);
        for sub in 0..self.m {
            let offset = sub * self.sub_dim;
            let sub_query = &query[offset..offset + self.sub_dim];
            let mut dists = Vec::with_capacity(self.k);
            for centroid in &self.codebooks[sub] {
                let d = l2_sub(sub_query, centroid);
                dists.push(d);
            }
            table.push(dists);
        }
        Ok(table)
    }

    /// Compute asymmetric distance using a precomputed distance table.
    ///
    /// O(M) per candidate — just M table lookups and additions.
    #[inline]
    pub fn asymmetric_distance(&self, table: &[Vec<f32>], code: &[u8]) -> f32 {
        debug_assert_eq!(code.len(), self.m);
        let mut dist = 0.0f32;
        for (sub, &c) in code.iter().enumerate() {
            dist += table[sub][c as usize];
        }
        dist
    }

    /// Decode a PQ code back to an approximate FP32 vector.
    ///
    /// Charges `dim * size_of::<f32>()` bytes to the governor (if set)
    /// before allocating the output buffer.
    pub fn decode(&self, code: &[u8]) -> Result<Vec<f32>, VectorError> {
        self.validate_shape()?;
        let decoded_dim = self
            .m
            .checked_mul(self.sub_dim)
            .filter(|&value| value == self.dim && value <= MAX_PQ_DECODE_DIM)
            .ok_or(VectorError::DimensionMismatch {
                expected: self.dim,
                got: 0,
            })?;
        if code.len() != self.m {
            return Err(VectorError::DimensionMismatch {
                expected: self.m,
                got: code.len(),
            });
        }
        if code.iter().any(|&index| usize::from(index) >= self.k) {
            return Err(VectorError::DeserializationFailed(
                "PQ code contains an out-of-range centroid index".to_string(),
            ));
        }
        let output_capacity = checked_decode_capacity(
            decoded_dim,
            size_of::<f32>(),
            0,
            0,
            MAX_PQ_DECODE_DIM,
            MAX_PQ_DECODE_DIM * size_of::<f32>(),
        )
        .ok_or(VectorError::DimensionMismatch {
            expected: self.dim,
            got: 0,
        })?;
        let allocation_bytes = output_capacity * size_of::<f32>();
        let _g = try_reserve_or_skip(&self.governor, allocation_bytes)?;
        let mut out = Vec::with_capacity(output_capacity);
        for (sub, &c) in code.iter().enumerate() {
            out.extend_from_slice(&self.codebooks[sub][c as usize]);
        }
        Ok(out)
    }

    /// Serialize the codec to bytes with a versioned magic header.
    ///
    /// Format: `[NDPQ\0\0 (6 bytes)][version: u8 = 1][msgpack payload]`
    ///
    /// Charges the estimated serialized size to the governor (if set) before
    /// allocating the output buffer.  The estimate is conservative:
    /// `m * k * sub_dim * size_of::<f32>() + 64` (header + framing overhead).
    pub fn to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        self.validate_shape()?;
        const MAGIC: &[u8; 6] = b"NDPQ\0\0";
        const VERSION: u8 = 1;
        let estimated = self.m * self.k * self.sub_dim * size_of::<f32>() + 64;
        let _g = try_reserve_or_skip(&self.governor, estimated)?;
        let payload = zerompk::to_msgpack_vec(self).unwrap_or_default();
        let mut out = Vec::with_capacity(7 + payload.len());
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Deserialize the codec from bytes produced by [`Self::to_bytes`].
    ///
    /// Returns `VectorError::InvalidMagic` if the header does not match
    /// `NDPQ\0\0`, and `VectorError::UnsupportedVersion` for unknown versions.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, VectorError> {
        const MAGIC: &[u8; 6] = b"NDPQ\0\0";
        const PQ_FORMAT_VERSION: u8 = 1;

        if bytes.len() < 7 || &bytes[0..6] != MAGIC {
            return Err(VectorError::InvalidMagic);
        }
        let version = bytes[6];
        if version != PQ_FORMAT_VERSION {
            return Err(VectorError::UnsupportedVersion {
                found: version,
                expected: PQ_FORMAT_VERSION,
            });
        }
        let payload = &bytes[7..];
        if payload.len() > MAX_PQ_SERIALIZED_BYTES {
            return Err(VectorError::DeserializationFailed(
                "PQ codec payload exceeds the decode limit".to_string(),
            ));
        }
        super::pq_decode::preflight_pq_payload(payload, MAX_PQ_DECODE_DIM, MAX_PQ_CODEBOOK_BYTES)?;
        let codec = zerompk::from_msgpack::<Self>(payload)
            .map_err(|e| VectorError::DeserializationFailed(e.to_string()))?;
        codec.validate_shape()?;
        Ok(codec)
    }

    fn validate_shape(&self) -> Result<(), VectorError> {
        let valid_scalar_shape = self.dim > 0
            && self.m > 0
            && self.k > 0
            && self.k <= usize::from(u8::MAX) + 1
            && self.sub_dim > 0
            && self
                .m
                .checked_mul(self.sub_dim)
                .is_some_and(|dim| dim == self.dim && dim <= MAX_PQ_DECODE_DIM);
        let codebook_bytes = pq_codebook_allocation_bytes(self.m, self.k, self.sub_dim);
        let valid_codebooks = self.codebooks.len() == self.m
            && self.codebooks.iter().all(|book| {
                book.len() == self.k && book.iter().all(|centroid| centroid.len() == self.sub_dim)
            });
        if !valid_scalar_shape
            || !codebook_bytes.is_some_and(|bytes| bytes <= MAX_PQ_CODEBOOK_BYTES)
            || !valid_codebooks
        {
            return Err(VectorError::DeserializationFailed(
                "invalid PQ codec shape".to_string(),
            ));
        }
        Ok(())
    }

    fn nearest_centroid(&self, subspace: usize, sub_vec: &[f32]) -> usize {
        let mut best_idx = 0;
        let mut best_dist = f32::MAX;
        for (i, centroid) in self.codebooks[subspace].iter().enumerate() {
            let d = l2_sub(sub_vec, centroid);
            if d < best_dist {
                best_dist = d;
                best_idx = i;
            }
        }
        best_idx
    }
}

/// L2 squared distance for sub-vectors (used in k-means and encoding).
#[inline]
fn l2_sub(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

/// Simple k-means clustering for PQ codebook training.
///
/// Uses proper k-means++ initialization (weighted d² sampling) with a
/// deterministic seed so training is reproducible across runs.
fn kmeans(data: &[&[f32]], dim: usize, k: usize, max_iter: usize) -> Vec<Vec<f32>> {
    let n = data.len();
    if n == 0 || k == 0 {
        return Vec::new();
    }
    let k = k.min(n); // Can't have more centroids than data points.

    // K-means++ initialization with deterministic xorshift.
    let mut rng = crate::hnsw::Xorshift64::new(0xC0FF_EEDE_ADBE_EF42);

    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(data[0].to_vec());

    let mut min_dists = vec![f32::MAX; n];
    // Update against the first centroid.
    for (i, point) in data.iter().enumerate() {
        let d = l2_sub(point, &centroids[0]);
        if d < min_dists[i] {
            min_dists[i] = d;
        }
    }

    for _ in 1..k {
        let total: f64 = min_dists.iter().map(|&d| d as f64).sum();
        let next_idx = if total < f64::EPSILON {
            // All points coincide with existing centroids.
            0
        } else {
            let target = rng.next_f64() * total;
            let mut acc = 0.0f64;
            let mut chosen = n - 1;
            for (i, &d) in min_dists.iter().enumerate() {
                acc += d as f64;
                if acc >= target {
                    chosen = i;
                    break;
                }
            }
            chosen
        };
        centroids.push(data[next_idx].to_vec());
        // Incrementally update min_dists against the new centroid.
        let last = centroids.last().expect("just pushed");
        for (i, point) in data.iter().enumerate() {
            let d = l2_sub(point, last);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }
    }

    // K-means iterations.
    let mut assignments = vec![0usize; n];
    for _ in 0..max_iter {
        // Assignment step.
        let mut changed = false;
        for (i, point) in data.iter().enumerate() {
            let mut best = 0;
            let mut best_d = f32::MAX;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = l2_sub(point, centroid);
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            if assignments[i] != best {
                assignments[i] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }

        // Update step: recompute centroids as means.
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for (i, point) in data.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..dim {
                sums[c][d] += point[d];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..dim {
                    centroids[c][d] = sums[c][d] / counts[c] as f32;
                }
            }
        }
    }

    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_clustered_data() -> Vec<Vec<f32>> {
        // 4 clusters in 4D space, 50 points each.
        let mut vecs = Vec::new();
        for cluster in 0..4 {
            let center = cluster as f32 * 10.0;
            for i in 0..50 {
                vecs.push(vec![
                    center + (i as f32) * 0.1,
                    center + (i as f32) * 0.05,
                    center - (i as f32) * 0.1,
                    center + (i as f32) * 0.02,
                ]);
            }
        }
        vecs
    }

    #[test]
    #[should_panic]
    fn train_rejects_dimension_above_decode_limit() {
        let vector = [0.0];
        PqCodec::train(&[&vector], MAX_PQ_DECODE_DIM + 1, 1, 1, 1);
    }

    #[test]
    #[should_panic]
    fn train_rejects_raw_64_mib_codebook_once_container_overhead_is_counted() {
        let vector = [0.0];
        let vectors = vec![vector.as_slice(); 256];
        PqCodec::train(&vectors, 65_536, 1, 256, 1);
    }

    #[test]
    #[should_panic]
    fn train_rejects_codebook_above_decode_limit() {
        let vector = [0.0];
        let vectors = vec![vector.as_slice(); 17];
        PqCodec::train(&vectors, MAX_PQ_DECODE_DIM, 1, 17, 1);
    }

    #[test]
    #[should_panic]
    fn train_rejects_more_centroids_than_training_vectors() {
        let vector = [0.0];
        PqCodec::train(&[&vector], 1, 1, 2, 1);
    }

    #[test]
    #[should_panic]
    fn train_rejects_centroid_count_above_u8_encoding_range() {
        let vector = [0.0];
        PqCodec::train(&[&vector], 1, 1, 257, 1);
    }

    #[test]
    fn decode_rejects_unbounded_configured_dimension_before_allocation() {
        let codec = PqCodec {
            dim: MAX_PQ_DECODE_DIM + 1,
            m: 1,
            k: 1,
            sub_dim: MAX_PQ_DECODE_DIM + 1,
            codebooks: vec![vec![vec![]]],
            governor: None,
        };
        assert!(codec.decode(&[0]).is_err());
    }

    #[test]
    fn from_bytes_rejects_oversized_outer_array_before_allocation() {
        // PqCodec's zerompk representation is a five-element array:
        // dim, m, k, sub_dim, codebooks.
        let mut payload = vec![0x95, 1, 1, 1, 1, 0xdd];
        payload.extend_from_slice(&u32::try_from(MAX_PQ_DECODE_DIM + 1).unwrap().to_be_bytes());
        let mut bytes = b"NDPQ\0\0\x01".to_vec();
        bytes.extend_from_slice(&payload);
        assert!(matches!(
            PqCodec::from_bytes(&bytes),
            Err(VectorError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_malformed_codebook_shape() {
        let malformed = PqCodec {
            dim: 4,
            m: 2,
            k: 1,
            sub_dim: 2,
            codebooks: vec![vec![vec![0.0, 0.0]]],
            governor: None,
        };
        let payload = zerompk::to_msgpack_vec(&malformed).unwrap();
        let mut bytes = b"NDPQ\0\0\x01".to_vec();
        bytes.extend_from_slice(&payload);
        assert!(matches!(
            PqCodec::from_bytes(&bytes),
            Err(VectorError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn decode_rejects_out_of_range_centroid_index() {
        let codec = PqCodec {
            dim: 1,
            m: 1,
            k: 1,
            sub_dim: 1,
            codebooks: vec![vec![vec![0.0]]],
            governor: None,
        };
        assert!(matches!(
            codec.decode(&[1]),
            Err(VectorError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        for v in &vecs {
            let code = codec.encode(v);
            assert_eq!(code.len(), 2); // M=2 bytes
            let decoded = codec.decode(&code).unwrap();
            assert_eq!(decoded.len(), 4);
        }
    }

    #[test]
    fn distance_table_gives_correct_ordering() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        let codes: Vec<Vec<u8>> = vecs.iter().map(|v| codec.encode(v)).collect();
        let query = &[5.0, 5.0, 5.0, 5.0];
        let table = codec.build_distance_table(query).unwrap();

        // Find nearest via PQ distance.
        let mut pq_dists: Vec<(usize, f32)> = codes
            .iter()
            .enumerate()
            .map(|(i, c)| (i, codec.asymmetric_distance(&table, c)))
            .collect();
        pq_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Find nearest via exact L2.
        let mut exact_dists: Vec<(usize, f32)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (i, l2_sub(query, v)))
            .collect();
        exact_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Top-5 from PQ should have significant overlap with exact top-10.
        let pq_top: std::collections::HashSet<usize> = pq_dists[..5].iter().map(|x| x.0).collect();
        let exact_top: std::collections::HashSet<usize> =
            exact_dists[..10].iter().map(|x| x.0).collect();
        let overlap = pq_top.intersection(&exact_top).count();
        assert!(overlap >= 3, "PQ recall too low: {overlap}/5 in top-10");
    }

    #[test]
    fn batch_encode() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        let batch = codec.encode_batch(&refs).unwrap();
        assert_eq!(batch.len(), 2 * 200); // M=2, N=200
    }

    // golden format test — verifies the on-disk layout is stable.
    #[test]
    fn pq_codec_golden_format() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        let bytes = codec.to_bytes().unwrap();

        // Magic header.
        assert_eq!(&bytes[0..6], b"NDPQ\0\0", "magic mismatch");
        // Version byte.
        assert_eq!(bytes[6], 1u8, "version must be 1");
        // The complete persisted envelope must pass bounded preflight and decode.
        let restored = PqCodec::from_bytes(&bytes).expect("persisted PQ codec must decode");
        assert_eq!(restored.dim, codec.dim);
        assert_eq!(restored.m, codec.m);
    }

    #[test]
    fn pq_version_mismatch_returns_error() {
        // Craft a header with magic correct but version = 0 (unsupported).
        let mut crafted = b"NDPQ\0\0".to_vec();
        crafted.push(0u8); // wrong version
        crafted.extend_from_slice(b"\x80"); // minimal valid msgpack map

        let err = PqCodec::from_bytes(&crafted).unwrap_err();
        assert!(
            matches!(
                err,
                VectorError::UnsupportedVersion {
                    found: 0,
                    expected: 1
                }
            ),
            "expected UnsupportedVersion, got: {err:?}"
        );
    }

    #[test]
    fn pq_invalid_magic_returns_error() {
        let bad: &[u8] = b"JUNK\0\0\x01some-payload";
        let err = PqCodec::from_bytes(bad).unwrap_err();
        assert!(
            matches!(err, VectorError::InvalidMagic),
            "expected InvalidMagic, got: {err:?}"
        );
    }
}
