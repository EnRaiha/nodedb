const MAX_VALID_VALUES: usize = 8;
const MAX_MUTATED_FRAME_BYTES: usize = 64 * 1024;

pub(crate) fn bounded_seed_bytes(seed: &[u8]) -> Vec<u8> {
    let zero = [0u8];
    let source = if seed.is_empty() { &zero[..] } else { seed };
    let count = source.len().clamp(3, MAX_VALID_VALUES);
    (0..count)
        .map(|index| source[index % source.len()])
        .collect()
}

pub(crate) fn decode_delta_frame(frame: &[u8]) {
    let _ = nodedb_codec::delta::decode(frame);
    if let Ok(mut decoder) = nodedb_codec::delta::DeltaDecoder::new(frame) {
        let _ = decoder.remaining();
        let _ = decoder.next_value();
        let _ = decoder.next_value();
    }
    let _ = nodedb_codec::delta::DeltaDecoder::decode_all(frame);
}

pub(crate) fn decode_double_delta_frame(frame: &[u8]) {
    let _ = nodedb_codec::double_delta::decode(frame);
    if let Ok(mut decoder) = nodedb_codec::double_delta::DoubleDeltaDecoder::new(frame) {
        let _ = decoder.remaining();
        let _ = decoder.next_value();
        let _ = decoder.next_value();
    }
    let _ = nodedb_codec::double_delta::DoubleDeltaDecoder::decode_all(frame);
}

pub(crate) fn decode_raw_frame(frame: &[u8]) {
    let _ = nodedb_codec::raw::decode(frame);
    let _ = nodedb_codec::raw::decode_ref(frame);
    let _ = nodedb_codec::raw::RawDecoder::decode_all(frame);
    let _ = nodedb_codec::raw::RawDecoder::decode_ref(frame);
}

pub(crate) fn decode_zstd_frame(frame: &[u8]) {
    let _ = nodedb_codec::zstd_codec::decode(frame);
    let _ = nodedb_codec::zstd_codec::uncompressed_size(frame);
    let _ = nodedb_codec::zstd_codec::compression_level(frame);
    let _ = nodedb_codec::zstd_codec::ZstdDecoder::decode_all(frame);
    let _ = nodedb_codec::zstd_codec::ZstdDecoder::uncompressed_size(frame);
}

pub(crate) fn decode_fastlanes_frame(frame: &[u8]) {
    let _ = nodedb_codec::fastlanes::decode(frame);
    let _ = nodedb_codec::fastlanes::block_count(frame);
    let _ = nodedb_codec::fastlanes::block_byte_offsets(frame);
    let _ = nodedb_codec::fastlanes::decode_single_block(frame, 0);
    let _ = nodedb_codec::fastlanes::decode_block_range(frame, 0, 1);
    if let Ok(mut blocks) = nodedb_codec::fastlanes::BlockIterator::new(frame) {
        let _ = blocks.skip_block();
    }
}

pub(crate) fn decode_gorilla_stream_frame(frame: &[u8]) {
    let mut decoder = nodedb_codec::gorilla::GorillaDecoder::new(frame);
    let _ = decoder.next_sample();
    let _ = decoder.next_sample();
    let mut decoder = nodedb_codec::gorilla::GorillaDecoder::new(frame);
    let _ = decoder.decode_all();
}

pub(crate) fn decode_lz4_frame(frame: &[u8]) {
    let _ = nodedb_codec::lz4::decode(frame);
    let _ = nodedb_codec::lz4::decode_block(frame, 0);
    let _ = nodedb_codec::lz4::Lz4Decoder::decode_all(frame);
    let _ = nodedb_codec::lz4::Lz4Decoder::decode_block(frame, 0);
    let _ = nodedb_codec::lz4::Lz4Decoder::block_count(frame);
}

pub(crate) fn decode_bbq_frame(frame: &[u8]) {
    let _ = nodedb_codec::vector_quant::codec_envelope::peek_version(
        nodedb_codec::vector_quant::bbq::BbqCodec::ENVELOPE_MAGIC,
        frame,
    );
    let _ = nodedb_codec::vector_quant::bbq::BbqCodec::from_bytes(frame);
}

pub(crate) fn decode_rabitq_frame(frame: &[u8]) {
    let _ = nodedb_codec::vector_quant::codec_envelope::peek_version(
        nodedb_codec::vector_quant::rabitq::RaBitQCodec::ENVELOPE_MAGIC,
        frame,
    );
    let _ = nodedb_codec::vector_quant::rabitq::RaBitQCodec::from_bytes(frame);
}

pub(crate) fn decode_generated_frame<T>(
    seed: &[u8],
    encoded: Result<Vec<u8>, nodedb_codec::CodecError>,
    mut decode: impl FnMut(&[u8]) -> T,
) {
    if let Ok(frame) = encoded {
        let _ = decode(&frame);
        decode_near_valid_variants(seed, &frame, decode);
    }
}

pub(crate) fn decode_near_valid_variants<T>(
    seed: &[u8],
    frame: &[u8],
    mut decode: impl FnMut(&[u8]) -> T,
) {
    if frame.is_empty() || frame.len() > MAX_MUTATED_FRAME_BYTES {
        return;
    }

    let header_len = frame.len().min(8);
    let header_index = usize::from(seed_byte(seed, 0)) % header_len;
    let mut header_flip = frame.to_vec();
    header_flip[header_index] ^= 1 << (seed_byte(seed, 1) & 7);
    let _ = decode(&header_flip);

    if frame.len() > header_len {
        let body_index = header_len + usize::from(seed_byte(seed, 2)) % (frame.len() - header_len);
        let mut body_flip = frame.to_vec();
        body_flip[body_index] ^= 1 << (seed_byte(seed, 3) & 7);
        let _ = decode(&body_flip);
    }

    let truncated_len = frame.len() - 1 - usize::from(seed_byte(seed, 4)) % frame.len();
    let _ = decode(&frame[..truncated_len]);
}

fn seed_byte(seed: &[u8], index: usize) -> u8 {
    if seed.is_empty() {
        0
    } else {
        seed[index % seed.len()]
    }
}
