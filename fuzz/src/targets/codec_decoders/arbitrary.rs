use crate::targets::codec_decoders::{codecs::column_codecs, frames::run_valid_frames};

const MAX_CODEC_INPUT_BYTES: usize = 1024 * 1024;
const MAX_TERNARY_DIM: usize = 4096;
const MAX_VALID_SEED_BYTES: usize = 32;
const EMPTY_GORILLA_STREAM: [u8; 4] = [0; 4];

pub fn run(data: &[u8]) {
    let bounded = &data[..data.len().min(MAX_CODEC_INPUT_BYTES)];

    let _ = nodedb_codec::alp::decode(bounded);
    let _ = nodedb_codec::alp_rd::decode(bounded);
    let _ = nodedb_codec::crdt_compress::decode(bounded);
    let _ = nodedb_codec::delta::decode(bounded);
    if let Ok(mut decoder) = nodedb_codec::delta::DeltaDecoder::new(bounded) {
        let _ = decoder.remaining();
        let _ = decoder.next_value();
        let _ = decoder.next_value();
    }
    let _ = nodedb_codec::delta::DeltaDecoder::decode_all(bounded);
    let _ = nodedb_codec::double_delta::decode(bounded);
    if let Ok(mut decoder) = nodedb_codec::double_delta::DoubleDeltaDecoder::new(bounded) {
        let _ = decoder.remaining();
        let _ = decoder.next_value();
        let _ = decoder.next_value();
    }
    let _ = nodedb_codec::double_delta::DoubleDeltaDecoder::decode_all(bounded);
    let _ = nodedb_codec::fastlanes::decode(bounded);
    let _ = nodedb_codec::fastlanes::block_count(bounded);
    let _ = nodedb_codec::fastlanes::block_byte_offsets(bounded);
    let _ = nodedb_codec::fastlanes::decode_single_block(bounded, 0);
    let _ = nodedb_codec::fastlanes::decode_block_range(bounded, 0, 1);
    if let Ok(mut blocks) = nodedb_codec::fastlanes::BlockIterator::new(bounded) {
        let _ = blocks.skip_block();
        let _ = blocks.skip_block();
    }
    let _ = nodedb_codec::fsst::decode(bounded);
    let delimiter = match bounded.first() {
        Some(byte) => *byte,
        None => b'\n',
    };
    let _ = nodedb_codec::fsst::decode_delimited(bounded, delimiter);
    let _ = nodedb_codec::gorilla::decode_f64(bounded);
    let _ = nodedb_codec::gorilla::decode_timestamps(bounded);
    let mut gorilla = nodedb_codec::gorilla::GorillaDecoder::new(bounded);
    let _ = gorilla.next_sample();
    let _ = gorilla.next_sample();
    let mut fixed_gorilla = nodedb_codec::gorilla::GorillaDecoder::new(&EMPTY_GORILLA_STREAM);
    let _ = fixed_gorilla.decode_all();
    let _ = nodedb_codec::lz4::decode(bounded);
    let _ = nodedb_codec::lz4::decode_block(bounded, 0);
    let _ = nodedb_codec::lz4::Lz4Decoder::decode_all(bounded);
    let _ = nodedb_codec::lz4::Lz4Decoder::decode_block(bounded, 0);
    let _ = nodedb_codec::lz4::Lz4Decoder::block_count(bounded);
    let _ = nodedb_codec::pcodec::decode_f64(bounded);
    let _ = nodedb_codec::pcodec::decode_i64(bounded);
    let _ = nodedb_codec::rans::decode(bounded);
    let _ = nodedb_codec::raw::decode(bounded);
    let _ = nodedb_codec::raw::decode_ref(bounded);
    let _ = nodedb_codec::raw::RawDecoder::decode_all(bounded);
    let _ = nodedb_codec::raw::RawDecoder::decode_ref(bounded);
    let _ = nodedb_codec::spherical::decode(bounded);
    let _ = nodedb_codec::zstd_codec::decode(bounded);
    let _ = nodedb_codec::zstd_codec::uncompressed_size(bounded);
    let _ = nodedb_codec::zstd_codec::compression_level(bounded);
    let _ = nodedb_codec::zstd_codec::ZstdDecoder::decode_all(bounded);
    let _ = nodedb_codec::zstd_codec::ZstdDecoder::uncompressed_size(bounded);
    let _ = nodedb_codec::vector_quant::bbq::BbqCodec::from_bytes(bounded);
    let _ = nodedb_codec::vector_quant::rabitq::RaBitQCodec::from_bytes(bounded);
    let _ = nodedb_codec::vector_quant::codec_envelope::peek_version(
        nodedb_codec::vector_quant::bbq::BbqCodec::ENVELOPE_MAGIC,
        bounded,
    );
    let _ = nodedb_codec::vector_quant::codec_envelope::peek_version(
        nodedb_codec::vector_quant::rabitq::RaBitQCodec::ENVELOPE_MAGIC,
        bounded,
    );
    let packed_bits_len = bounded
        .first()
        .map_or(0, |byte| usize::from(*byte).min(bounded.len()));
    let _ = nodedb_codec::vector_quant::layout::UnifiedQuantizedVectorRef::from_bytes(
        bounded,
        packed_bits_len,
    );
    let ternary_dim = bounded.len().min(MAX_TERNARY_DIM);
    let _ = nodedb_codec::vector_quant::ternary::packing::unpack_cold(bounded, ternary_dim);
    let _ = nodedb_codec::vector_quant::ternary::packing::unpack_hot(bounded, ternary_dim);
    let _ = nodedb_codec::vector_quant::ternary::packing::cold_to_hot(bounded, ternary_dim);
    for codec in column_codecs() {
        let _ = nodedb_codec::pipeline::decode_i64_pipeline(bounded, codec);
        let _ = nodedb_codec::pipeline::decode_f64_pipeline(bounded, codec);
        let _ = nodedb_codec::pipeline::decode_bytes_pipeline(bounded, codec);
    }

    run_valid_frames(&data[..data.len().min(MAX_VALID_SEED_BYTES)]);
}
