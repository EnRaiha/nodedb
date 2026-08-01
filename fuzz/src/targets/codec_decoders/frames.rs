use nodedb_codec::vector_quant::codec::VectorCodec;

use crate::targets::codec_decoders::{
    codecs::column_codecs,
    helpers::{
        bounded_seed_bytes, decode_bbq_frame, decode_delta_frame, decode_double_delta_frame,
        decode_fastlanes_frame, decode_generated_frame, decode_gorilla_stream_frame,
        decode_lz4_frame, decode_near_valid_variants, decode_rabitq_frame, decode_raw_frame,
        decode_zstd_frame,
    },
};

pub(crate) fn run_valid_frames(seed: &[u8]) {
    let bytes = bounded_seed_bytes(seed);
    let i64_values: Vec<i64> = bytes
        .iter()
        .enumerate()
        .map(|(index, byte)| i64::from(*byte) + index as i64 * 17)
        .collect();
    let f64_values: Vec<f64> = bytes
        .iter()
        .enumerate()
        .map(|(index, byte)| f64::from(*byte) / 8.0 + index as f64)
        .collect();
    let f32_values: Vec<f32> = bytes
        .iter()
        .enumerate()
        .map(|(index, byte)| f32::from(*byte) / 16.0 + index as f32)
        .collect();

    decode_generated_frame(
        seed,
        nodedb_codec::alp::encode(&f64_values),
        nodedb_codec::alp::decode,
    );
    decode_generated_frame(
        seed,
        nodedb_codec::alp_rd::encode(&f64_values),
        nodedb_codec::alp_rd::decode,
    );

    let operations = vec![
        nodedb_codec::crdt_compress::CrdtOp {
            lamport: i64_values[0] as u64,
            actor_id: 1,
            content: bytes.clone(),
        },
        nodedb_codec::crdt_compress::CrdtOp {
            lamport: i64_values[0].saturating_add(1) as u64,
            actor_id: 2,
            content: bytes.iter().rev().copied().collect(),
        },
    ];
    decode_generated_frame(
        seed,
        nodedb_codec::crdt_compress::encode(&operations),
        nodedb_codec::crdt_compress::decode,
    );

    if let Ok(frame) = nodedb_codec::delta::encode(&i64_values) {
        decode_delta_frame(&frame);
        decode_near_valid_variants(seed, &frame, decode_delta_frame);
    }
    let mut delta_encoder = nodedb_codec::delta::DeltaEncoder::new();
    if delta_encoder.push_batch(&i64_values).is_ok()
        && let Ok(frame) = delta_encoder.finish()
    {
        decode_delta_frame(&frame);
        decode_near_valid_variants(seed, &frame, decode_delta_frame);
    }
    if let Ok(frame) = nodedb_codec::double_delta::encode(&i64_values) {
        decode_double_delta_frame(&frame);
        decode_near_valid_variants(seed, &frame, decode_double_delta_frame);
    }
    let mut double_delta_encoder = nodedb_codec::double_delta::DoubleDeltaEncoder::new();
    if double_delta_encoder.push_batch(&i64_values).is_ok()
        && let Ok(frame) = double_delta_encoder.finish()
    {
        decode_double_delta_frame(&frame);
        decode_near_valid_variants(seed, &frame, decode_double_delta_frame);
    }

    if let Ok(frame) = nodedb_codec::fastlanes::encode(&i64_values) {
        let _ = nodedb_codec::fastlanes::decode(&frame);
        let _ = nodedb_codec::fastlanes::block_count(&frame);
        let _ = nodedb_codec::fastlanes::block_byte_offsets(&frame);
        let _ = nodedb_codec::fastlanes::decode_single_block(&frame, 0);
        let _ = nodedb_codec::fastlanes::decode_block_range(&frame, 0, 1);
        if let Ok(mut blocks) = nodedb_codec::fastlanes::BlockIterator::new(&frame) {
            let _ = blocks.skip_block();
        }
        decode_near_valid_variants(seed, &frame, decode_fastlanes_frame);
    }

    let strings: Vec<&[u8]> = vec![bytes.as_slice(), b"valid"];
    decode_generated_frame(
        seed,
        nodedb_codec::fsst::encode(&strings),
        nodedb_codec::fsst::decode,
    );
    let delimiter = b'|';
    decode_generated_frame(
        seed,
        nodedb_codec::fsst::encode_delimited(&bytes, delimiter),
        |frame| nodedb_codec::fsst::decode_delimited(frame, delimiter),
    );

    let f64_frame = nodedb_codec::gorilla::encode_f64(&f64_values);
    let _ = nodedb_codec::gorilla::decode_f64(&f64_frame);
    decode_near_valid_variants(seed, &f64_frame, nodedb_codec::gorilla::decode_f64);
    let timestamp_frame = nodedb_codec::gorilla::encode_timestamps(&i64_values);
    let _ = nodedb_codec::gorilla::decode_timestamps(&timestamp_frame);
    decode_near_valid_variants(
        seed,
        &timestamp_frame,
        nodedb_codec::gorilla::decode_timestamps,
    );
    let mut encoder = nodedb_codec::gorilla::GorillaEncoder::new();
    for (index, value) in f64_values.iter().enumerate() {
        encoder.encode(i64_values[index], *value);
    }
    let streaming_frame = encoder.finish();
    decode_gorilla_stream_frame(&streaming_frame);
    decode_near_valid_variants(seed, &streaming_frame, decode_gorilla_stream_frame);

    if let Ok(frame) = nodedb_codec::lz4::encode_with_block_size(&bytes, 64) {
        let _ = nodedb_codec::lz4::decode(&frame);
        let _ = nodedb_codec::lz4::decode_block(&frame, 0);
        let _ = nodedb_codec::lz4::Lz4Decoder::decode_all(&frame);
        let _ = nodedb_codec::lz4::Lz4Decoder::decode_block(&frame, 0);
        let _ = nodedb_codec::lz4::Lz4Decoder::block_count(&frame);
        decode_near_valid_variants(seed, &frame, decode_lz4_frame);
    }
    if let Ok(mut encoder) = nodedb_codec::lz4::Lz4Encoder::with_block_size(64)
        && encoder.push(&bytes).is_ok()
        && let Ok(frame) = encoder.finish()
    {
        let _ = nodedb_codec::lz4::decode(&frame);
        let _ = nodedb_codec::lz4::decode_block(&frame, 0);
        decode_near_valid_variants(seed, &frame, decode_lz4_frame);
    }

    decode_generated_frame(
        seed,
        nodedb_codec::pcodec::encode_f64(&f64_values),
        nodedb_codec::pcodec::decode_f64,
    );
    decode_generated_frame(
        seed,
        nodedb_codec::pcodec::encode_i64(&i64_values),
        nodedb_codec::pcodec::decode_i64,
    );
    decode_generated_frame(
        seed,
        nodedb_codec::rans::encode(&bytes),
        nodedb_codec::rans::decode,
    );

    let raw_frame = nodedb_codec::raw::encode(&bytes);
    decode_raw_frame(&raw_frame);
    decode_near_valid_variants(seed, &raw_frame, decode_raw_frame);
    let mut raw_encoder = nodedb_codec::raw::RawEncoder::new();
    raw_encoder.push(&bytes);
    let raw_frame = raw_encoder.finish();
    decode_raw_frame(&raw_frame);
    decode_near_valid_variants(seed, &raw_frame, decode_raw_frame);

    let dims = f32_values.len();
    decode_generated_frame(
        seed,
        nodedb_codec::spherical::encode(&f32_values, dims, 1),
        nodedb_codec::spherical::decode,
    );
    decode_generated_frame(
        seed,
        nodedb_codec::spherical::encode_raw(&f32_values, dims, 1),
        nodedb_codec::spherical::decode,
    );
    if let Ok(frame) = nodedb_codec::zstd_codec::encode(&bytes) {
        decode_zstd_frame(&frame);
        decode_near_valid_variants(seed, &frame, decode_zstd_frame);
    }
    let mut zstd_encoder = nodedb_codec::zstd_codec::ZstdEncoder::new();
    if zstd_encoder.push(&bytes).is_ok()
        && let Ok(frame) = zstd_encoder.finish()
    {
        decode_zstd_frame(&frame);
        decode_near_valid_variants(seed, &frame, decode_zstd_frame);
    }

    let training = [f32_values.as_slice()];
    let bbq = nodedb_codec::vector_quant::bbq::BbqCodec::calibrate(&training, dims, 1);
    if let Ok(frame) = bbq.to_bytes() {
        let _ = nodedb_codec::vector_quant::codec_envelope::peek_version(
            nodedb_codec::vector_quant::bbq::BbqCodec::ENVELOPE_MAGIC,
            &frame,
        );
        let _ = nodedb_codec::vector_quant::bbq::BbqCodec::from_bytes(&frame);
        decode_near_valid_variants(seed, &frame, decode_bbq_frame);
    }
    let rabitq = nodedb_codec::vector_quant::rabitq::RaBitQCodec::calibrate(&training, dims, 1);
    if let Ok(frame) = rabitq.to_bytes() {
        let _ = nodedb_codec::vector_quant::codec_envelope::peek_version(
            nodedb_codec::vector_quant::rabitq::RaBitQCodec::ENVELOPE_MAGIC,
            &frame,
        );
        let _ = nodedb_codec::vector_quant::rabitq::RaBitQCodec::from_bytes(&frame);
        decode_near_valid_variants(seed, &frame, decode_rabitq_frame);
    }

    let header = nodedb_codec::vector_quant::layout::QuantHeader {
        quant_mode: nodedb_codec::vector_quant::layout::QuantMode::Binary as u16,
        dim: dims as u16,
        global_scale: 1.0,
        residual_norm: 0.0,
        dot_quantized: 0.0,
        outlier_bitmask: 0,
        reserved: [0; 8],
    };
    if let Ok(vector) = nodedb_codec::vector_quant::layout::UnifiedQuantizedVector::new(
        header,
        &bytes[..dims.div_ceil(8)],
        &[],
    ) {
        let packed_bits_len = vector.packed_bits().len();
        let _ = nodedb_codec::vector_quant::layout::UnifiedQuantizedVectorRef::from_bytes(
            vector.as_bytes(),
            packed_bits_len,
        );
        decode_near_valid_variants(seed, vector.as_bytes(), |frame| {
            let _ = nodedb_codec::vector_quant::layout::UnifiedQuantizedVectorRef::from_bytes(
                frame,
                packed_bits_len,
            );
        });
    }

    let ternary = nodedb_codec::vector_quant::ternary::TernaryCodec::new(dims);
    let ternary_vector = ternary.encode(&f32_values);
    let ternary_bits = ternary_vector.0.packed_bits();
    let _ = nodedb_codec::vector_quant::ternary::packing::unpack_hot(ternary_bits, dims);
    decode_near_valid_variants(seed, ternary_bits, |frame| {
        nodedb_codec::vector_quant::ternary::packing::unpack_hot(frame, dims)
    });
    let trits: Vec<i8> = bytes
        .iter()
        .map(|byte| match byte % 3 {
            0 => -1,
            1 => 0,
            _ => 1,
        })
        .collect();
    let cold = nodedb_codec::vector_quant::ternary::packing::pack_cold(&trits);
    let hot = nodedb_codec::vector_quant::ternary::packing::pack_hot(&trits);
    let _ = nodedb_codec::vector_quant::ternary::packing::unpack_cold(&cold, dims);
    decode_near_valid_variants(seed, &cold, |frame| {
        nodedb_codec::vector_quant::ternary::packing::unpack_cold(frame, dims)
    });
    let _ = nodedb_codec::vector_quant::ternary::packing::unpack_hot(&hot, dims);
    decode_near_valid_variants(seed, &hot, |frame| {
        nodedb_codec::vector_quant::ternary::packing::unpack_hot(frame, dims)
    });
    let _ = nodedb_codec::vector_quant::ternary::packing::cold_to_hot(&cold, dims);
    decode_near_valid_variants(seed, &cold, |frame| {
        nodedb_codec::vector_quant::ternary::packing::cold_to_hot(frame, dims)
    });

    let pipeline_bytes: Vec<u8> = (0..4).map(|index| bytes[index % bytes.len()]).collect();
    for codec in column_codecs() {
        decode_generated_frame(
            seed,
            nodedb_codec::pipeline::encode_i64_pipeline(&i64_values, codec),
            |frame| nodedb_codec::pipeline::decode_i64_pipeline(frame, codec),
        );
        decode_generated_frame(
            seed,
            nodedb_codec::pipeline::encode_f64_pipeline(&f64_values, codec),
            |frame| nodedb_codec::pipeline::decode_f64_pipeline(frame, codec),
        );
        decode_generated_frame(
            seed,
            nodedb_codec::pipeline::encode_bytes_pipeline(&pipeline_bytes, codec),
            |frame| nodedb_codec::pipeline::decode_bytes_pipeline(frame, codec),
        );
    }
}
