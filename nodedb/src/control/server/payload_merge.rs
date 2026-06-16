// SPDX-License-Identifier: BUSL-1.1

//! Merge several standalone msgpack-array payloads into one msgpack array.
//!
//! A streamed scan result arrives as multiple chunks, and each chunk is encoded
//! as its OWN msgpack array (`encode_raw_document_rows` per chunk). Downstream
//! consumers decode a single msgpack array, so the chunks must be merged into
//! one array — raw byte concatenation would leave every chunk after the first as
//! a separate trailing array that the decoder silently ignores (truncating the
//! result to the first chunk).
//!
//! Used by both `dispatch_utils::collect_bounded_response` (the per-request
//! bounded collector) and `exchange::gather` (the cross-core/vShard gather).

use nodedb_query::msgpack_scan;

/// Extract individual msgpack elements from one msgpack-array payload.
///
/// If the payload is not a valid msgpack array, it is returned as a single
/// element with a warning logged (matching the gather fallback).
pub(crate) fn extract_msgpack_elements(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.is_empty() {
        return Vec::new();
    }

    let Some((count, mut pos)) = msgpack_scan::array_header(payload, 0) else {
        tracing::warn!(
            payload_len = payload.len(),
            "payload_merge: payload is not a msgpack array; treating as single element"
        );
        return vec![payload.to_vec()];
    };

    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= payload.len() {
            break;
        }
        let start = pos;
        match msgpack_scan::skip_value(payload, pos) {
            Some(next) => {
                rows.push(payload[start..next].to_vec());
                pos = next;
            }
            None => {
                tracing::warn!(
                    pos,
                    payload_len = payload.len(),
                    "payload_merge: could not skip msgpack element; stopping early"
                );
                break;
            }
        }
    }
    rows
}

/// Encode a list of pre-extracted msgpack elements into a single msgpack array.
pub(crate) fn encode_msgpack_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let total_data: usize = rows.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);

    let n = rows.len();
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }

    for row in rows {
        out.extend_from_slice(row);
    }
    out
}

/// Merge several standalone msgpack-array payloads into one msgpack array
/// containing every element from every input array, in order.
pub(crate) fn merge_msgpack_arrays(payloads: &[Vec<u8>]) -> Vec<u8> {
    let mut elements: Vec<Vec<u8>> = Vec::new();
    for payload in payloads {
        elements.extend(extract_msgpack_elements(payload));
    }
    encode_msgpack_array(&elements)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A msgpack array of `n` small integer elements (0..n), as a standalone
    /// array payload like a streamed scan chunk.
    fn int_array(start: usize, n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (start..start + n).map(|i| vec![(i % 128) as u8]).collect();
        encode_msgpack_array(&rows)
    }

    #[test]
    fn merge_concatenates_all_elements() {
        let chunks = vec![
            int_array(0, 1000),
            int_array(1000, 1000),
            int_array(2000, 500),
        ];
        let merged = merge_msgpack_arrays(&chunks);
        let elements = extract_msgpack_elements(&merged);
        assert_eq!(
            elements.len(),
            2500,
            "merging three array chunks must yield every element, not just the first chunk"
        );
    }

    #[test]
    fn single_array_round_trips() {
        let one = int_array(0, 3);
        let merged = merge_msgpack_arrays(std::slice::from_ref(&one));
        assert_eq!(extract_msgpack_elements(&merged).len(), 3);
    }

    #[test]
    fn empty_input_is_empty_array() {
        let merged = merge_msgpack_arrays(&[]);
        assert_eq!(extract_msgpack_elements(&merged).len(), 0);
    }
}
