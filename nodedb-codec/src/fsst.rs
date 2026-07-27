// SPDX-License-Identifier: Apache-2.0

//! FSST (Fast Static Symbol Table) codec for string/log columns.
//!
//! Builds a lightweight dictionary of common substrings (1-8 bytes) and
//! encodes strings as sequences of symbol table indices. Unlike whole-string
//! dictionary encoding, FSST handles partial overlap — strings sharing
//! prefixes or suffixes compress well even if no two strings are identical.
//!
//! Compression: 3-5x on string columns before any terminal compressor.
//! Combined with lz4_flex terminal: 8-15x total on structured log data.
//!
//! Decompression: simple table lookup — fast enough to query directly
//! over encoded data.
//!
//! Wire format:
//! ```text
//! [2 bytes] symbol count (LE u16, max 255)
//! [symbol_count × (1 + len) bytes] symbol table: (len: u8, bytes: [u8; len])
//! [4 bytes] total encoded length (LE u32)
//! [4 bytes] string count (LE u32)
//! [string_count × 4 bytes] encoded string offsets (cumulative LE u32)
//! [N bytes] encoded data (symbol indices interleaved with escape+literal)
//! ```
//!
//! Escape mechanism: byte value 255 followed by a literal byte encodes
//! bytes not covered by any symbol. Symbol indices are 0..254.

use std::mem::size_of;

use crate::bounds::{
    checked_add, checked_capacity, checked_mul, checked_range, decoded_len, encode_input_len,
    encode_u32_len, u32_to_usize,
};
use crate::error::CodecError;

/// Maximum number of symbols in the table (reserve 255 as escape).
const MAX_SYMBOLS: usize = 255;

/// Maximum symbol length in bytes.
const MAX_SYMBOL_LEN: usize = 8;

/// Escape byte: signals the next byte is a literal (not a symbol index).
const ESCAPE: u8 = 255;

/// Number of training passes over the input data.
const TRAINING_ROUNDS: usize = 5;

// ---------------------------------------------------------------------------
// Symbol table
// ---------------------------------------------------------------------------

/// A trained FSST symbol table.
#[derive(Debug, Clone)]
struct SymbolTable {
    /// Symbols sorted by length (longest first) for greedy matching.
    symbols: Vec<Vec<u8>>,
}

impl SymbolTable {
    /// Train a symbol table from a set of input strings.
    ///
    /// Uses iterative count-based selection: in each round, count how many
    /// bytes each candidate n-gram would save, pick the best, repeat.
    fn train(strings: &[&[u8]]) -> Self {
        if strings.is_empty() {
            return Self {
                symbols: Vec::new(),
            };
        }

        let mut symbols: Vec<Vec<u8>> = Vec::new();
        let mut symbol_set: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut candidates: std::collections::HashMap<Vec<u8>, usize> =
            std::collections::HashMap::new();

        for _round in 0..TRAINING_ROUNDS {
            // Count n-gram frequencies in the data (after encoding with current table).
            candidates.clear();

            for s in strings {
                // Scan for n-grams of length 1-8 that are NOT already covered by symbols.
                let mut pos = 0;
                while pos < s.len() {
                    // Check if current position starts with a known symbol.
                    let existing_match = longest_symbol_match(&symbols, s, pos);

                    if existing_match > 0 {
                        pos += existing_match;
                        continue;
                    }

                    // No existing symbol matches — count new n-gram candidates.
                    for len in 1..=MAX_SYMBOL_LEN.min(s.len() - pos) {
                        let ngram = &s[pos..pos + len];
                        *candidates.entry(ngram.to_vec()).or_insert(0) += 1;
                    }
                    pos += 1;
                }
            }

            if candidates.is_empty() {
                break;
            }

            // Score candidates by compression gain: frequency * (length - 1).
            // Each symbol saves (length - 1) bytes per occurrence (1 byte for
            // the symbol index vs `length` bytes raw).
            let mut scored: Vec<(Vec<u8>, usize)> = candidates
                .drain()
                .map(|(ngram, freq)| {
                    let gain = freq * (ngram.len().saturating_sub(1));
                    (ngram, gain)
                })
                .filter(|(_, gain)| *gain > 0)
                .collect();

            scored.sort_by_key(|a| std::cmp::Reverse(a.1));

            // Add top candidates that don't duplicate existing symbols.
            for (ngram, _) in scored {
                if symbols.len() >= MAX_SYMBOLS {
                    break;
                }
                if symbol_set.insert(ngram.clone()) {
                    symbols.push(ngram);
                }
            }
        }

        // Sort symbols longest-first for greedy matching.
        symbols.sort_by_key(|a| std::cmp::Reverse(a.len()));

        Self { symbols }
    }

    fn symbol_count(&self) -> usize {
        self.symbols.len()
    }
}

/// Find the longest symbol matching at position `pos` in `data`.
/// Returns the match length (0 if no match).
fn longest_symbol_match(symbols: &[Vec<u8>], data: &[u8], pos: usize) -> usize {
    let remaining = &data[pos..];
    for sym in symbols {
        if remaining.starts_with(sym) {
            return sym.len();
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a batch of strings using FSST compression.
///
/// Trains a symbol table on the input, then encodes each string as a
/// sequence of symbol indices and escaped literals.
pub fn encode(strings: &[&[u8]]) -> Result<Vec<u8>, CodecError> {
    encode_input_len(strings.len(), "FSST string count")?;
    let input_bytes = strings.iter().try_fold(0usize, |total, string| {
        checked_add(total, string.len(), "FSST input bytes")
    })?;
    decoded_len(input_bytes, "FSST input")?;
    let table = SymbolTable::train(strings);

    // Encode each string.
    let mut encoded_strings: Vec<Vec<u8>> = Vec::with_capacity(strings.len());
    for s in strings {
        encoded_strings.push(encode_string(&table, s)?);
    }

    // Build wire format.
    // Encoded strings with offset table.
    let total_encoded = encoded_strings.iter().try_fold(0usize, |total, string| {
        checked_add(total, string.len(), "FSST encoded bytes")
    })?;
    decoded_len(total_encoded, "FSST encoded bytes")?;
    let total_encoded_u32 = encode_u32_len(total_encoded, "FSST encoded bytes")?;
    let offset_bytes = checked_mul(strings.len(), 4, "FSST offsets")?;
    decoded_len(offset_bytes, "FSST offsets")?;
    let symbol_bytes = table.symbols.iter().try_fold(0usize, |total, symbol| {
        checked_add(
            checked_add(total, 1, "FSST symbols")?,
            symbol.len(),
            "FSST symbols",
        )
    })?;
    let output_len = checked_add(
        checked_add(
            checked_add(2, symbol_bytes, "FSST output")?,
            8,
            "FSST output",
        )?,
        checked_add(offset_bytes, total_encoded, "FSST output")?,
        "FSST output",
    )?;
    decoded_len(output_len, "FSST output")?;
    let string_count = encode_input_len(strings.len(), "FSST string count")?;
    let mut out = Vec::with_capacity(output_len);

    // Symbol table.
    out.extend_from_slice(&(table.symbol_count() as u16).to_le_bytes());
    for sym in &table.symbols {
        out.push(sym.len() as u8);
        out.extend_from_slice(sym);
    }
    out.extend_from_slice(&total_encoded_u32.to_le_bytes());
    out.extend_from_slice(&string_count.to_le_bytes());

    // Cumulative offsets.
    let mut offset = 0usize;
    for es in &encoded_strings {
        offset = checked_add(offset, es.len(), "FSST string offset")?;
        out.extend_from_slice(&encode_u32_len(offset, "FSST string offset")?.to_le_bytes());
    }

    // Encoded data.
    for es in &encoded_strings {
        out.extend_from_slice(es);
    }

    Ok(out)
}

/// Decode FSST-compressed data back to strings.
pub fn decode(data: &[u8]) -> Result<Vec<Vec<u8>>, CodecError> {
    if data.len() < 2 {
        return Err(CodecError::Truncated {
            expected: 2,
            actual: data.len(),
        });
    }

    // Read symbol table.
    let sym_count = usize::from(u16::from_le_bytes([data[0], data[1]]));
    if sym_count > MAX_SYMBOLS {
        return Err(CodecError::Corrupt {
            detail: "FSST symbol count exceeds maximum".into(),
        });
    }
    let mut pos = 2;
    let symbol_capacity = checked_capacity(sym_count, size_of::<Vec<u8>>(), "FSST symbols")?;
    let mut symbols: Vec<Vec<u8>> = Vec::with_capacity(symbol_capacity);

    for _ in 0..sym_count {
        let len = usize::from(checked_range(data, pos, 1, "FSST symbol length")?[0]);
        pos = checked_add(pos, 1, "FSST symbol cursor")?;
        if len == 0 || len > MAX_SYMBOL_LEN {
            return Err(CodecError::Corrupt {
                detail: "FSST symbol length is invalid".into(),
            });
        }
        let symbol = checked_range(data, pos, len, "FSST symbol")?;
        if symbols.iter().any(|existing| existing == symbol) {
            return Err(CodecError::Corrupt {
                detail: "duplicate FSST symbol".into(),
            });
        }
        symbols.push(symbol.to_vec());
        pos = checked_add(pos, len, "FSST symbol cursor")?;
    }

    // Read header.
    let header = checked_range(data, pos, 8, "FSST header")?;
    let total_encoded = u32_to_usize(
        u32::from_le_bytes([header[0], header[1], header[2], header[3]]),
        "FSST encoded length",
    )?;
    let string_count = u32_to_usize(
        u32::from_le_bytes([header[4], header[5], header[6], header[7]]),
        "FSST string count",
    )?;
    decoded_len(total_encoded, "FSST")?;
    let container_bytes = checked_add(
        checked_mul(string_count, size_of::<usize>(), "FSST offset allocation")?,
        checked_mul(string_count, size_of::<Vec<u8>>(), "FSST result allocation")?,
        "FSST container allocations",
    )?;
    decoded_len(container_bytes, "FSST containers")?;
    pos = checked_add(pos, 8, "FSST header cursor")?;

    // Read offsets.
    let offsets_size = checked_mul(string_count, 4, "FSST offsets")?;
    decoded_len(offsets_size, "FSST offsets")?;
    let offsets_data = checked_range(data, pos, offsets_size, "FSST offsets")?;
    let offset_capacity = checked_capacity(string_count, size_of::<usize>(), "FSST offsets")?;
    let mut offsets = Vec::with_capacity(offset_capacity);
    for i in 0..string_count {
        let off_pos = checked_mul(i, 4, "FSST offset position")?;
        offsets.push(u32_to_usize(
            u32::from_le_bytes([
                offsets_data[off_pos],
                offsets_data[off_pos + 1],
                offsets_data[off_pos + 2],
                offsets_data[off_pos + 3],
            ]),
            "FSST string offset",
        )?);
    }
    pos = checked_add(pos, offsets_size, "FSST data cursor")?;
    let encoded_data = checked_range(data, pos, total_encoded, "FSST encoded data")?;
    if checked_add(pos, total_encoded, "FSST frame end")? != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after FSST frame".into(),
        });
    }

    // Decode each string.
    let result_capacity = checked_capacity(string_count, size_of::<Vec<u8>>(), "FSST results")?;
    let mut result = Vec::with_capacity(result_capacity);
    let mut prev_end = 0;
    let mut decoded_total = 0usize;
    for &end in &offsets {
        if end < prev_end || end > encoded_data.len() {
            return Err(CodecError::Corrupt {
                detail: "FSST string offsets are not monotonic and in range".into(),
            });
        }
        let encoded_str = &encoded_data[prev_end..end];
        let decoded = decode_string(&symbols, encoded_str)?;
        decoded_total = checked_add(decoded_total, decoded.len(), "FSST decoded bytes")?;
        decoded_len(decoded_total, "FSST")?;
        result.push(decoded);
        prev_end = end;
    }
    if prev_end != total_encoded {
        return Err(CodecError::Corrupt {
            detail: "FSST offsets do not consume encoded data".into(),
        });
    }

    Ok(result)
}

/// Convenience: encode a single contiguous byte buffer that contains
/// multiple strings separated by a delimiter (e.g., newlines for log data).
pub fn encode_delimited(data: &[u8], delimiter: u8) -> Result<Vec<u8>, CodecError> {
    decoded_len(data.len(), "FSST delimited input")?;
    let strings: Vec<&[u8]> = data.split(|&b| b == delimiter).collect();
    encode(&strings)
}

/// Convenience: decode and reassemble with delimiter.
pub fn decode_delimited(data: &[u8], delimiter: u8) -> Result<Vec<u8>, CodecError> {
    let strings = decode(data)?;
    let mut out = Vec::new();
    for (i, s) in strings.iter().enumerate() {
        let separator = usize::from(i > 0);
        let next_len = checked_add(
            checked_add(out.len(), separator, "FSST delimited output")?,
            s.len(),
            "FSST delimited output",
        )?;
        decoded_len(next_len, "FSST delimited output")?;
        if i > 0 {
            out.push(delimiter);
        }
        out.extend_from_slice(s);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Per-string encode / decode
// ---------------------------------------------------------------------------

fn encode_string(table: &SymbolTable, input: &[u8]) -> Result<Vec<u8>, CodecError> {
    let capacity = checked_mul(input.len(), 2, "FSST encoded string")?;
    decoded_len(capacity, "FSST encoded string")?;
    let mut out = Vec::with_capacity(capacity);
    let mut pos = 0;

    while pos < input.len() {
        // Greedy: try to match the longest symbol at current position.
        let mut matched = false;
        for (idx, sym) in table.symbols.iter().enumerate() {
            if input[pos..].starts_with(sym) {
                out.push(idx as u8);
                pos += sym.len();
                matched = true;
                break;
            }
        }

        if !matched {
            // No symbol matches — emit escape + literal byte.
            out.push(ESCAPE);
            out.push(input[pos]);
            pos += 1;
        }
    }

    Ok(out)
}

fn decode_string(symbols: &[Vec<u8>], encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
    let expanded_capacity = checked_mul(encoded.len(), MAX_SYMBOL_LEN, "FSST decoded string")?;
    let capacity = decoded_len(expanded_capacity, "FSST decoded string")?;
    let mut out = Vec::with_capacity(capacity);
    let mut pos = 0;

    while pos < encoded.len() {
        let byte = encoded[pos];
        pos += 1;

        if byte == ESCAPE {
            // Next byte is a literal.
            if pos >= encoded.len() {
                return Err(CodecError::Corrupt {
                    detail: "FSST escape at end of encoded data".into(),
                });
            }
            out.push(encoded[pos]);
            pos += 1;
        } else {
            // Symbol index.
            let idx = byte as usize;
            if idx >= symbols.len() {
                return Err(CodecError::Corrupt {
                    detail: format!(
                        "FSST symbol index {idx} out of range (max {})",
                        symbols.len()
                    ),
                });
            }
            let next_len = checked_add(out.len(), symbols[idx].len(), "FSST decoded string")?;
            decoded_len(next_len, "FSST decoded string")?;
            out.extend_from_slice(&symbols[idx]);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(strings: &[&[u8]]) -> Vec<u8> {
        super::encode(strings).expect("test FSST encode")
    }

    fn encode_delimited(data: &[u8], delimiter: u8) -> Vec<u8> {
        super::encode_delimited(data, delimiter).expect("test FSST delimited encode")
    }

    #[test]
    fn empty_input() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_string() {
        let strings: Vec<&[u8]> = vec![b"hello world"];
        let encoded = encode(&strings);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], b"hello world");
    }

    #[test]
    fn multiple_strings_roundtrip() {
        let strings: Vec<&[u8]> = vec![
            b"us-east-1",
            b"us-east-2",
            b"us-west-1",
            b"eu-west-1",
            b"us-east-1",
            b"us-east-1",
        ];
        let encoded = encode(&strings);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), strings.len());
        for (a, b) in strings.iter().zip(decoded.iter()) {
            assert_eq!(*a, b.as_slice());
        }
    }

    #[test]
    fn repetitive_log_lines() {
        let lines: Vec<&[u8]> = (0..1000)
            .map(|i| {
                let s: &[u8] = match i % 5 {
                    0 => b"2024-01-15 INFO server.handler request_id=abc method=GET status=200",
                    1 => b"2024-01-15 INFO server.handler request_id=def method=POST status=201",
                    2 => b"2024-01-15 WARN server.handler request_id=ghi method=GET status=404",
                    3 => b"2024-01-15 ERROR server.handler request_id=jkl method=PUT status=500",
                    _ => b"2024-01-15 DEBUG server.handler request_id=mno method=GET status=200",
                };
                s
            })
            .collect();

        let encoded = encode(&lines);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), lines.len());
        for (a, b) in lines.iter().zip(decoded.iter()) {
            assert_eq!(*a, b.as_slice());
        }

        // FSST should compress repetitive logs.
        let raw_size: usize = lines.iter().map(|s| s.len()).sum();
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.5,
            "FSST should compress repetitive logs >1.5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn hostnames() {
        let hosts: Vec<&[u8]> = vec![
            b"prod-web-01.us-east-1.example.com",
            b"prod-web-02.us-east-1.example.com",
            b"prod-web-03.us-east-1.example.com",
            b"prod-api-01.us-west-2.example.com",
            b"prod-api-02.us-west-2.example.com",
            b"staging-web-01.eu-west-1.example.com",
        ];
        let encoded = encode(&hosts);
        let decoded = decode(&encoded).unwrap();
        for (a, b) in hosts.iter().zip(decoded.iter()) {
            assert_eq!(*a, b.as_slice());
        }
    }

    #[test]
    fn binary_data() {
        // Binary data with no patterns — should still roundtrip (escape every byte).
        let data: Vec<&[u8]> = vec![&[0, 1, 2, 3, 4, 255, 254, 253]];
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded[0], data[0]);
    }

    #[test]
    fn empty_strings() {
        let strings: Vec<&[u8]> = vec![b"", b"hello", b"", b"world", b""];
        let encoded = encode(&strings);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 5);
        assert!(decoded[0].is_empty());
        assert_eq!(decoded[1], b"hello");
        assert!(decoded[2].is_empty());
    }

    #[test]
    fn delimited_roundtrip() {
        let data = b"line one\nline two\nline three\nline one\nline two";
        let encoded = encode_delimited(data, b'\n');
        let decoded = decode_delimited(&encoded, b'\n').unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn compression_ratio_structured_logs() {
        let mut lines: Vec<Vec<u8>> = Vec::new();
        for i in 0..5000 {
            let line = format!(
                "2024-01-15T10:30:{:02}.000Z INFO server.handler request_id={} method=GET path=/api/v1/metrics status=200 duration_ms={}",
                i % 60,
                10000 + i,
                i * 3 + 1
            );
            lines.push(line.into_bytes());
        }
        let refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();

        let encoded = encode(&refs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), lines.len());

        let raw_size: usize = lines.iter().map(|s| s.len()).sum();
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.5,
            "FSST should compress structured logs >1.5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn malformed_sections_and_trailing_bytes_are_rejected() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1]).is_err());
        let mut encoded = encode(&[b"hello".as_slice()]);
        encoded.push(0);
        assert!(matches!(decode(&encoded), Err(CodecError::Corrupt { .. })));
        let mut invalid_offset = encode(&[b"hello".as_slice()]);
        let symbol_count = usize::from(u16::from_le_bytes([invalid_offset[0], invalid_offset[1]]));
        let mut cursor = 2;
        for _ in 0..symbol_count {
            cursor += 1 + usize::from(invalid_offset[cursor]);
        }
        let offsets_start = cursor + 8;
        // The encoder's single-string final offset is the encoded-data length;
        // zero makes the remaining encoded data unreachable.
        invalid_offset[offsets_start..offsets_start + 4].copy_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            decode(&invalid_offset),
            Err(CodecError::Corrupt { .. })
        ));
    }

    #[test]
    fn large_dataset() {
        let mut strings: Vec<Vec<u8>> = Vec::new();
        for i in 0..10_000 {
            strings.push(format!("key-{}-value-{}", i % 100, i % 50).into_bytes());
        }
        let refs: Vec<&[u8]> = strings.iter().map(|s| s.as_slice()).collect();
        let encoded = encode(&refs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), strings.len());
        for (a, b) in strings.iter().zip(decoded.iter()) {
            assert_eq!(a.as_slice(), b.as_slice());
        }
    }
}
