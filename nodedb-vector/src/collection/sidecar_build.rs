// SPDX-License-Identifier: Apache-2.0

//! Factory function for building a [`CodecSidecar`] from a collection's live vectors.
//!
//! Called from `complete_build` and from the checkpoint restore path. Instantiates
//! the appropriate `RerankCodec` wrapper, trains it when needed, encodes all provided
//! (id, vec) pairs into the sidecar, and returns it.

use std::sync::Arc;

use nodedb_mem::ScopedMemory;
use nodedb_types::VectorQuantization;

use crate::error::VectorError;
use crate::rerank::codec::RerankCodec;
use crate::rerank::codecs::bbq::DEFAULT_OVERSAMPLE;
use crate::rerank::codecs::rabitq::DEFAULT_ROTATION_SEED;
use crate::rerank::codecs::{BbqRerank, BinaryRerank, PqRerank, RaBitQRerank, Sq8Rerank};
use crate::rerank::sidecar::CodecSidecar;

/// Maximum training samples fed to codecs that run k-means internally.
/// Larger sets add training time with diminishing accuracy returns.
const MAX_TRAINING_SAMPLES: usize = 10_000;

/// Build a [`CodecSidecar`] for the given quantization over all provided (id, vec) pairs.
///
/// - `quantization == None` → returns `Ok(None)` (no sidecar needed).
/// - `Sq8` / `Binary` → no external training needed; codec is functional after `new`.
/// - `Pq` / `RaBitQ` / `Bbq` → trains from the provided sample vectors (capped at
///   `MAX_TRAINING_SAMPLES` for efficiency), then encodes all vectors.
/// - `Ternary` / `Opq` → returns `Err(VectorError::BadInput(...))` matching the
///   existing `validate_options` gate: these quantization variants have no HNSW-integrated
///   path yet.
///
/// After training, every (id, vec) pair is encoded into the sidecar. Individual
/// encode failures emit a `tracing::warn` and are skipped; the sidecar may be
/// partially populated in that case, and affected rows degrade to FP32 rerank.
pub(crate) fn build_sidecar(
    quantization: VectorQuantization,
    dim: usize,
    samples: &[(u32, Vec<f32>)],
    memory: ScopedMemory,
) -> Result<Option<CodecSidecar>, VectorError> {
    if samples.is_empty() {
        // Nothing to train on or encode — return an empty sidecar for non-None quantizations
        // so the collection is marked as having one (future inserts will populate it).
        if quantization == VectorQuantization::None {
            return Ok(None);
        }
    }

    let codec: Arc<dyn RerankCodec> = match quantization {
        VectorQuantization::None => return Ok(None),

        VectorQuantization::Sq8 => {
            let mut codec = Sq8Rerank::new(dim);
            if !samples.is_empty() {
                let vecs: Vec<&[f32]> = samples
                    .iter()
                    .take(MAX_TRAINING_SAMPLES)
                    .map(|(_, v)| v.as_slice())
                    .collect();
                codec
                    .train(&vecs)
                    .map_err(|e| VectorError::BadInput(format!("sq8 sidecar train failed: {e}")))?;
            }
            Arc::new(codec)
        }

        VectorQuantization::Binary => {
            // Binary has no learned state — new() is fully functional.
            Arc::new(BinaryRerank::new(dim))
        }

        VectorQuantization::Pq => {
            let mut codec = PqRerank::new(dim, 8, 256, memory);
            if !samples.is_empty() {
                let vecs: Vec<&[f32]> = samples
                    .iter()
                    .take(MAX_TRAINING_SAMPLES)
                    .map(|(_, v)| v.as_slice())
                    .collect();
                codec
                    .train(&vecs)
                    .map_err(|e| VectorError::BadInput(format!("pq sidecar train failed: {e}")))?;
            }
            Arc::new(codec)
        }

        VectorQuantization::RaBitQ => {
            let mut codec = RaBitQRerank::new(dim, DEFAULT_ROTATION_SEED);
            if !samples.is_empty() {
                let vecs: Vec<&[f32]> = samples
                    .iter()
                    .take(MAX_TRAINING_SAMPLES)
                    .map(|(_, v)| v.as_slice())
                    .collect();
                codec.train(&vecs).map_err(|e| {
                    VectorError::BadInput(format!("rabitq sidecar train failed: {e}"))
                })?;
            }
            Arc::new(codec)
        }

        VectorQuantization::Bbq => {
            let mut codec = BbqRerank::new(dim, DEFAULT_OVERSAMPLE);
            if !samples.is_empty() {
                let vecs: Vec<&[f32]> = samples
                    .iter()
                    .take(MAX_TRAINING_SAMPLES)
                    .map(|(_, v)| v.as_slice())
                    .collect();
                codec
                    .train(&vecs)
                    .map_err(|e| VectorError::BadInput(format!("bbq sidecar train failed: {e}")))?;
            }
            Arc::new(codec)
        }

        VectorQuantization::Ternary | VectorQuantization::Opq => {
            return Err(VectorError::BadInput(format!(
                "quantization {:?} has no HNSW-integrated sidecar path yet",
                quantization
            )));
        }

        // Exhaustive match: any new variant added to VectorQuantization must be handled here.
        _ => {
            return Err(VectorError::BadInput(format!(
                "quantization {:?} is not handled by the sidecar builder",
                quantization
            )));
        }
    };

    let mut sidecar = CodecSidecar::new(codec);

    for (id, vec) in samples {
        if let Err(e) = sidecar.encode_and_insert(*id, vec) {
            tracing::warn!(
                id,
                error = %e,
                "sidecar build: encode_and_insert failed; this vector will fall back to FP32 rerank"
            );
        }
    }

    Ok(Some(sidecar))
}
