// SPDX-License-Identifier: Apache-2.0

//! BBQ kernel benches: the fused path against a reconstruct-and-measure
//! baseline.
//!
//! The baseline models the shape of the pre-fusion rerank pass — decode the
//! prepared payload into a `Vec<f32>` per candidate, then reconstruct each
//! dimension — so the allocation and pass counts are comparable. It is an
//! approximation of that path, not a copy of it: it uses a fixed `1/√dim` scale
//! in place of the candidate's stored corrective factor, and reads the sign bits
//! at a fixed header offset. It measures pass and allocation shape, not the
//! codec's arithmetic.
//!
//! Fixtures are built once per thread, outside the timed region, so only the
//! kernel loop is measured.
//!
//! Run with: cargo bench -p nodedb-vector --bench bbq_kernel

use fluxbench::bench;
use fluxbench::prelude::*;
use std::hint::black_box;

use nodedb_vector::rerank::codec::{PreparedQuery, RerankCodec};
use nodedb_vector::rerank::codecs::BbqRerank;

/// Installs fluxbench's tracking allocator so the harness reports heap bytes and
/// allocation counts per benchmark: the fused path shows zero allocations per
/// candidate, the baseline shows one `Vec<f32>`.
#[global_allocator]
static ALLOC: fluxbench::TrackingAllocator = fluxbench::TrackingAllocator;

const OVERSAMPLE: u8 = 4;
const CANDIDATES: usize = 256;

fn det_vec(i: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| (((i * 31 + j) % 100) as f32 / 100.0) - 0.5)
        .collect()
}

/// Trained codec, prepared query, and 256 encoded candidates for `dim`.
fn setup(dim: usize) -> (BbqRerank, PreparedQuery, Vec<Vec<u8>>) {
    let vecs: Vec<Vec<f32>> = (0..CANDIDATES).map(|i| det_vec(i, dim)).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    let mut codec = BbqRerank::new(dim, OVERSAMPLE);
    codec.train(&refs).expect("train");
    let prepared = codec.prepare_query(&vecs[0]).expect("prepare_query");
    let encoded: Vec<Vec<u8>> = vecs
        .iter()
        .map(|v| codec.encode(v).expect("encode"))
        .collect();
    (codec, prepared, encoded)
}

thread_local! {
    static STATE_128: (BbqRerank, PreparedQuery, Vec<Vec<u8>>) = setup(128);
    static STATE_768: (BbqRerank, PreparedQuery, Vec<Vec<u8>>) = setup(768);
}

/// The baseline: decode the prepared payload to `Vec<f32>` per candidate, then
/// reconstruct each dimension. `encoded` carries a 32-byte quant header before
/// the sign bits, and the reconstruction uses a fixed `1/√dim` scale rather than
/// the candidate's stored `residual_norm` — see the module docs.
fn unfused_l2(payload: &[u8], encoded: &[u8], dim: usize) -> f32 {
    let centered: Vec<f32> = payload[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f32::from_le_bytes(*b))
        .collect();
    let scale = 1.0f32 / (dim as f32).sqrt();
    let mut acc = 0.0f32;
    for i in 0..dim {
        let bit = (encoded[32 + i / 8] >> (7 - (i % 8))) & 1;
        let recon = if bit != 0 { scale } else { -scale };
        let d = centered[i] - recon;
        acc += d * d;
    }
    acc.sqrt()
}

#[bench(id = "bbq_fused_128", group = "bbq_kernel")]
fn bbq_fused_128(b: &mut Bencher) {
    STATE_128.with(|(codec, prepared, encoded)| {
        b.iter(|| {
            let mut acc = 0.0f32;
            for e in encoded {
                acc += codec.distance_prepared(prepared, e).expect("distance");
            }
            black_box(acc)
        });
    });
}

#[bench(id = "bbq_unfused_128", group = "bbq_kernel")]
fn bbq_unfused_128(b: &mut Bencher) {
    STATE_128.with(|(_codec, prepared, encoded)| {
        let payload = match prepared {
            PreparedQuery::Bytes(b) => b.as_slice(),
            _ => panic!("bbq prepared form is Bytes"),
        };
        b.iter(|| {
            let mut acc = 0.0f32;
            for e in encoded {
                acc += unfused_l2(payload, e, 128);
            }
            black_box(acc)
        });
    });
}

#[bench(id = "bbq_fused_768", group = "bbq_kernel")]
fn bbq_fused_768(b: &mut Bencher) {
    STATE_768.with(|(codec, prepared, encoded)| {
        b.iter(|| {
            let mut acc = 0.0f32;
            for e in encoded {
                acc += codec.distance_prepared(prepared, e).expect("distance");
            }
            black_box(acc)
        });
    });
}

#[bench(id = "bbq_unfused_768", group = "bbq_kernel")]
fn bbq_unfused_768(b: &mut Bencher) {
    STATE_768.with(|(_codec, prepared, encoded)| {
        let payload = match prepared {
            PreparedQuery::Bytes(b) => b.as_slice(),
            _ => panic!("bbq prepared form is Bytes"),
        };
        b.iter(|| {
            let mut acc = 0.0f32;
            for e in encoded {
                acc += unfused_l2(payload, e, 768);
            }
            black_box(acc)
        });
    });
}

fn main() {
    if let Err(e) = fluxbench::run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
