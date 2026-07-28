// SPDX-License-Identifier: Apache-2.0

//! Interleaved rANS (Asymmetric Numeral Systems) entropy coder.
//!
//! Compresses byte streams to the Shannon entropy limit — optimal
//! compression ratio at Huffman-like speed. Used as the terminal
//! compressor for cold/S3 tier partitions where ratio matters more
//! than decompression speed.
//!
//! 4-stream interleaving breaks the sequential dependency chain:
//! the CPU processes all streams simultaneously, achieving high
//! throughput despite the inherently sequential nature of ANS.
//!
//! Wire format:
//! ```text
//! [4 bytes] uncompressed size (LE u32)
//! [256 × 4 bytes] frequency table (LE u32 per byte value)
//! [4 bytes] compressed size (LE u32)
//! [N bytes] interleaved rANS bitstream (4 streams)
//! ```

use crate::bounds::{
    checked_add, checked_capacity, checked_range, decoded_len, encode_input_len, encode_u32_len,
    u32_to_usize,
};
use crate::error::CodecError;

/// Number of interleaved streams.
const NUM_STREAMS: usize = 4;

/// rANS probability scale (power of 2 for fast division).
const PROB_BITS: u32 = 14;
const PROB_SCALE: u32 = 1 << PROB_BITS;

/// rANS state lower bound.
const RANS_L: u32 = 1 << 23;
const RANS_STATE_UPPER_BOUND: u32 = RANS_L << 8;

/// Frequency table header size: 256 × 4 bytes = 1024 bytes.
const FREQ_TABLE_SIZE: usize = 256 * 4;

/// Full header: 4 (uncomp size) + 1024 (freq table) + 4 (comp size).
const HEADER_SIZE: usize = 4 + FREQ_TABLE_SIZE + 4;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compress bytes using interleaved rANS.
pub fn encode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    let encoded_len = encode_input_len(data.len(), "rANS")?;
    if data.is_empty() {
        let out = vec![0u8; HEADER_SIZE];
        // uncompressed_size = 0, freq table = all zeros, compressed_size = 0
        return Ok(out);
    }

    // Build frequency table.
    let mut freqs = [0u32; 256];
    for &b in data {
        freqs[b as usize] += 1;
    }

    // Normalize frequencies to sum to PROB_SCALE.
    let norm_freqs = normalize_frequencies(&freqs, data.len());

    // Build cumulative frequency table.
    let (cum_freqs, sym_freqs) = build_cum_table(&norm_freqs);

    // Encode using 4 interleaved streams.
    // Each stream processes every 4th byte: stream 0 gets bytes 0,4,8,...
    let mut streams: [Vec<u8>; NUM_STREAMS] = std::array::from_fn(|_| Vec::new());
    let mut states = [RANS_L; NUM_STREAMS];

    // Encode in REVERSE order (rANS encodes backward, decodes forward).
    for i in (0..data.len()).rev() {
        let stream_idx = i % NUM_STREAMS;
        let sym = data[i] as usize;
        let freq = sym_freqs[sym];
        let start = cum_freqs[sym];

        if freq == 0 {
            continue; // Symbol with zero frequency — shouldn't happen after normalization.
        }

        rans_encode_symbol(
            &mut states[stream_idx],
            &mut streams[stream_idx],
            start,
            freq,
        );
    }

    // Flush final states.
    for i in 0..NUM_STREAMS {
        let s = states[i];
        streams[i].push((s & 0xFF) as u8);
        streams[i].push(((s >> 8) & 0xFF) as u8);
        streams[i].push(((s >> 16) & 0xFF) as u8);
        streams[i].push(((s >> 24) & 0xFF) as u8);
    }

    // Build output.
    let total_compressed: usize = streams.iter().map(|s| s.len()).sum();
    let mut out = Vec::with_capacity(HEADER_SIZE + total_compressed + NUM_STREAMS * 4);

    // Header: uncompressed size.
    out.extend_from_slice(&encoded_len.to_le_bytes());

    // Frequency table (raw counts for decoding).
    for &f in &norm_freqs {
        out.extend_from_slice(&f.to_le_bytes());
    }

    // Compressed size.
    let comp_payload_size = total_compressed + NUM_STREAMS * 4; // streams + per-stream sizes
    out.extend_from_slice(
        &encode_u32_len(comp_payload_size, "rANS compressed payload")?.to_le_bytes(),
    );

    // Per-stream sizes (4 bytes each).
    for s in &streams {
        out.extend_from_slice(&encode_u32_len(s.len(), "rANS stream")?.to_le_bytes());
    }

    // Stream data (reversed — rANS bitstream is read backward).
    for s in &streams {
        out.extend_from_slice(s);
    }

    Ok(out)
}

/// Decompress interleaved rANS data.
pub fn decode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    let header = checked_range(data, 0, HEADER_SIZE, "rANS header")?;
    let uncompressed_size = decoded_len(
        u32_to_usize(
            u32::from_le_bytes(header[0..4].try_into().map_err(|_| CodecError::Corrupt {
                detail: "rANS output size".into(),
            })?),
            "rANS output",
        )?,
        "rANS",
    )?;
    let mut norm_freqs = [0u32; 256];
    for (index, frequency) in norm_freqs.iter_mut().enumerate() {
        let offset = 4 + index * 4;
        *frequency = u32::from_le_bytes(header[offset..offset + 4].try_into().map_err(|_| {
            CodecError::Corrupt {
                detail: "rANS frequency".into(),
            }
        })?);
    }
    let compressed_size = u32_to_usize(
        u32::from_le_bytes(header[HEADER_SIZE - 4..].try_into().map_err(|_| {
            CodecError::Corrupt {
                detail: "rANS compressed size".into(),
            }
        })?),
        "rANS compressed size",
    )?;
    if uncompressed_size == 0 {
        if compressed_size != 0 || data.len() != HEADER_SIZE {
            return Err(CodecError::Corrupt {
                detail: "nonempty rANS payload for empty output".into(),
            });
        }
        return Ok(Vec::new());
    }
    validate_frequencies(&norm_freqs)?;
    let stream_sizes_len = NUM_STREAMS * 4;
    let frame = checked_range(
        data,
        HEADER_SIZE,
        compressed_size,
        "rANS compressed payload",
    )?;
    if checked_add(HEADER_SIZE, compressed_size, "rANS frame end")? != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after rANS frame".into(),
        });
    }
    if compressed_size < stream_sizes_len {
        return Err(CodecError::Corrupt {
            detail: "rANS compressed size omits stream sizes".into(),
        });
    }
    let mut stream_sizes = [0usize; NUM_STREAMS];
    let mut payload_len = 0usize;
    for (index, size) in stream_sizes.iter_mut().enumerate() {
        let start = index * 4;
        *size = u32_to_usize(
            u32::from_le_bytes(frame[start..start + 4].try_into().map_err(|_| {
                CodecError::Corrupt {
                    detail: "rANS stream size".into(),
                }
            })?),
            "rANS stream size",
        )?;
        if *size < 4 {
            return Err(CodecError::Corrupt {
                detail: format!("rANS stream {index} too short for state"),
            });
        }
        payload_len = checked_add(payload_len, *size, "rANS stream lengths")?;
    }
    if checked_add(stream_sizes_len, payload_len, "rANS stream framing")? != compressed_size {
        return Err(CodecError::Corrupt {
            detail: "rANS compressed-size metadata mismatch".into(),
        });
    }
    let (cum_freqs, sym_freqs) = build_cum_table(&norm_freqs);
    let lookup = build_decode_table(&cum_freqs, &sym_freqs);
    let mut streams: [&[u8]; NUM_STREAMS] = [&[]; NUM_STREAMS];
    let mut pos = stream_sizes_len;
    for (index, stream) in streams.iter_mut().enumerate() {
        *stream = checked_range(frame, pos, stream_sizes[index], "rANS stream range")?;
        pos = checked_add(pos, stream_sizes[index], "rANS stream offset")?;
    }
    let mut states = [0u32; NUM_STREAMS];
    let mut stream_pos = [0usize; NUM_STREAMS];
    for index in 0..NUM_STREAMS {
        let stream = streams[index];
        let end = stream.len();
        states[index] = u32::from_le_bytes(stream[end - 4..end].try_into().map_err(|_| {
            CodecError::Corrupt {
                detail: "rANS state".into(),
            }
        })?);
        if !(RANS_L..RANS_STATE_UPPER_BOUND).contains(&states[index]) {
            return Err(CodecError::Corrupt {
                detail: format!("rANS stream {index} has invalid initial state"),
            });
        }
        stream_pos[index] = end - 4;
    }
    let output_capacity = checked_capacity(uncompressed_size, 1, "rANS decoded bytes")?;
    let mut output = vec![0u8; output_capacity];
    for (index, out_byte) in output.iter_mut().enumerate() {
        let stream_index = index % NUM_STREAMS;
        let (symbol, state) =
            rans_decode_symbol(states[stream_index], &lookup, &cum_freqs, &sym_freqs);
        *out_byte = symbol;
        states[stream_index] =
            rans_decode_renorm(state, streams[stream_index], &mut stream_pos[stream_index]);
    }
    for index in 0..NUM_STREAMS {
        if stream_pos[index] != 0 || states[index] != RANS_L {
            return Err(CodecError::Corrupt {
                detail: format!("rANS stream {index} has noncanonical terminal state"),
            });
        }
    }
    Ok(output)
}

// ---------------------------------------------------------------------------
// rANS core operations
// ---------------------------------------------------------------------------

fn rans_encode_symbol(state: &mut u32, bitstream: &mut Vec<u8>, start: u32, freq: u32) {
    // Renormalize: output bytes until state is in the correct range.
    let max_state = ((RANS_L >> PROB_BITS) << 8) * freq;
    while *state >= max_state {
        bitstream.push((*state & 0xFF) as u8);
        *state >>= 8;
    }

    // Encode symbol.
    *state = ((*state / freq) << PROB_BITS) + (*state % freq) + start;
}

fn rans_decode_symbol(
    state: u32,
    lookup: &[u8; PROB_SCALE as usize],
    cum_freqs: &[u32; 257],
    sym_freqs: &[u32; 256],
) -> (u8, u32) {
    let slot = state & (PROB_SCALE - 1);
    let sym = lookup[slot as usize];
    let start = cum_freqs[sym as usize];
    let freq = sym_freqs[sym as usize];

    let new_state = freq * (state >> PROB_BITS) + slot - start;
    (sym, new_state)
}

fn rans_decode_renorm(mut state: u32, stream: &[u8], pos: &mut usize) -> u32 {
    while state < RANS_L && *pos > 0 {
        *pos -= 1;
        state = (state << 8) | stream[*pos] as u32;
    }
    state
}

// ---------------------------------------------------------------------------
// Frequency table operations
// ---------------------------------------------------------------------------

fn validate_frequencies(freqs: &[u32; 256]) -> Result<(), CodecError> {
    let mut total = 0u32;
    for &frequency in freqs {
        total = total
            .checked_add(frequency)
            .ok_or_else(|| CodecError::Corrupt {
                detail: "rANS frequency total overflows".into(),
            })?;
    }
    if total != PROB_SCALE {
        return Err(CodecError::Corrupt {
            detail: format!("rANS frequencies sum to {total}, expected {PROB_SCALE}"),
        });
    }
    Ok(())
}

/// Normalize raw frequencies to sum to PROB_SCALE.
fn normalize_frequencies(freqs: &[u32; 256], total: usize) -> [u32; 256] {
    let mut norm = [0u32; 256];
    let mut sum = 0u32;
    let total_f64 = total as f64;

    // First pass: proportional scaling.
    for i in 0..256 {
        if freqs[i] > 0 {
            // Ensure every present symbol gets at least frequency 1.
            norm[i] = ((freqs[i] as f64 / total_f64 * PROB_SCALE as f64).round() as u32).max(1);
            sum += norm[i];
        }
    }

    // Adjust to make sum exactly PROB_SCALE.
    if sum > 0 {
        while sum > PROB_SCALE {
            // Find the symbol with the highest frequency and reduce it.
            let max_idx = norm
                .iter()
                .enumerate()
                .filter(|(_, f)| **f > 1)
                .max_by_key(|(_, f)| **f)
                .map(|(i, _)| i)
                .unwrap_or(0);
            norm[max_idx] -= 1;
            sum -= 1;
        }
        while sum < PROB_SCALE {
            let max_idx = norm
                .iter()
                .enumerate()
                .max_by_key(|(_, f)| **f)
                .map(|(i, _)| i)
                .unwrap_or(0);
            norm[max_idx] += 1;
            sum += 1;
        }
    }

    norm
}

/// Build cumulative frequency table.
fn build_cum_table(freqs: &[u32; 256]) -> ([u32; 257], [u32; 256]) {
    let mut cum = [0u32; 257];
    let sym_freqs = *freqs;
    for i in 0..256 {
        cum[i + 1] = cum[i] + freqs[i];
    }
    (cum, sym_freqs)
}

/// Build decode lookup table: for each slot in [0, PROB_SCALE), which symbol?
fn build_decode_table(
    cum_freqs: &[u32; 257],
    _sym_freqs: &[u32; 256],
) -> [u8; PROB_SCALE as usize] {
    let mut table = [0u8; PROB_SCALE as usize];
    for sym in 0..256u16 {
        let start = cum_freqs[sym as usize] as usize;
        let end = cum_freqs[sym as usize + 1] as usize;
        for entry in table[start..end].iter_mut() {
            *entry = sym as u8;
        }
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_byte() {
        let encoded = encode(&[42]).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, vec![42]);
    }

    #[test]
    fn repeated_bytes() {
        let data = vec![0u8; 10_000];
        let encoded = encode(&data).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        // Highly repetitive → near-zero entropy → excellent compression.
        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(
            ratio > 2.0,
            "repeated bytes should compress >2x, got {ratio:.1}x"
        );
    }

    #[test]
    fn text_data() {
        let text = b"the quick brown fox jumps over the lazy dog. ";
        let data: Vec<u8> = text.iter().copied().cycle().take(10_000).collect();
        let encoded = encode(&data).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(ratio > 1.5, "text should compress >1.5x, got {ratio:.1}x");
    }

    #[test]
    fn uniform_random_data() {
        // Uniform random → ~8 bits/byte → no compression possible.
        let mut data = vec![0u8; 5000];
        let mut rng: u64 = 12345;
        for byte in &mut data {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (rng >> 33) as u8;
        }
        let encoded = encode(&data).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn all_byte_values() {
        // All 256 byte values present.
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let encoded = encode(&data).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn skewed_distribution() {
        // 90% zeros, 10% ones — should compress well.
        let mut data = vec![0u8; 10_000];
        for i in 0..1000 {
            data[i * 10] = 1;
        }
        let encoded = encode(&data).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.5,
            "skewed data should compress >1.5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn better_than_raw_on_structured() {
        // Structured data after type-aware preprocessing (typical pipeline output).
        let mut data = Vec::with_capacity(10_000);
        for i in 0..10_000 {
            data.push((i % 16) as u8); // Low entropy, 4 bits/byte → 2x compression.
        }
        let encoded = encode(&data).expect("encode");
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.5,
            "low-entropy data should compress >1.5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0, 0]).is_err()); // too short for freq table
    }

    #[test]
    fn hostile_lengths_and_frequencies_fail_before_output_allocation() {
        let mut frame = vec![0u8; HEADER_SIZE];
        frame[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&frame),
            Err(CodecError::ResourceLimit { .. })
        ));

        let encoded = encode(b"bounded rans").expect("encode");
        let mut malformed = encoded.clone();
        malformed[4..8].copy_from_slice(&PROB_SCALE.wrapping_add(1).to_le_bytes());
        assert!(matches!(
            decode(&malformed),
            Err(CodecError::Corrupt { .. })
        ));
        let mut bad_size = encoded;
        bad_size[HEADER_SIZE - 4..HEADER_SIZE].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&bad_size).is_err());
    }

    #[test]
    fn noncanonical_stream_padding_and_high_state_are_rejected() {
        let encoded = encode(&[42]).expect("encode");
        let size_start = HEADER_SIZE;
        let stream_zero_size = u32::from_le_bytes([
            encoded[size_start],
            encoded[size_start + 1],
            encoded[size_start + 2],
            encoded[size_start + 3],
        ]);
        let stream_zero_start = HEADER_SIZE + NUM_STREAMS * 4;
        let state_start = stream_zero_start + stream_zero_size as usize - 4;

        let mut padded = encoded.clone();
        padded.insert(state_start, 0xaa);
        padded[size_start..size_start + 4].copy_from_slice(&(stream_zero_size + 1).to_le_bytes());
        let compressed_size = u32::from_le_bytes([
            padded[HEADER_SIZE - 4],
            padded[HEADER_SIZE - 3],
            padded[HEADER_SIZE - 2],
            padded[HEADER_SIZE - 1],
        ]);
        padded[HEADER_SIZE - 4..HEADER_SIZE].copy_from_slice(&(compressed_size + 1).to_le_bytes());
        assert!(matches!(decode(&padded), Err(CodecError::Corrupt { .. })));

        let mut high_state = encoded;
        high_state[state_start..state_start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&high_state),
            Err(CodecError::Corrupt { .. })
        ));
    }
}
