// SPDX-License-Identifier: BUSL-1.1

//! Raw-msgpack passthrough encoders and the inverse decoder.
//!
//! The "raw" pattern eliminates the decode→re-encode cycle on document reads
//! by writing storage bytes directly into the response. The decoder accepts
//! both raw scan rows (`{id, data}` wrappers) and plain msgpack rows produced
//! by aggregate/join paths.

use super::super::msgpack_utils::write_str;

/// Encode document rows with raw MessagePack passthrough for the data field.
///
/// Each row is `(doc_id, raw_msgpack_bytes)`. The raw bytes are written directly
/// into the output without decoding to `serde_json::Value` first. This eliminates
/// the decode→re-encode cycle that was the main serialization tax on document reads.
///
/// Output format: msgpack array of `{"id": "<doc_id>", "data": <raw_msgpack_value>}`.
///
/// Visibility is `pub(crate)` (not Data-Plane-only) so the Control Plane sync
/// layer can re-encode the rows that survive shape-predicate filtering before
/// shipping a snapshot to subscribers.
pub(crate) fn encode_raw_document_rows(rows: &[(String, Vec<u8>)]) -> crate::Result<Vec<u8>> {
    let data_size: usize = rows.iter().map(|(id, d)| id.len() + d.len() + 16).sum();
    let mut buf = Vec::with_capacity(data_size + 8);

    msgpack_write_array_header(&mut buf, rows.len());

    for (id, data_bytes) in rows {
        // Write map header (2 entries: "id" and "data").
        buf.push(0x82); // fixmap with 2 entries

        write_str(&mut buf, "id");
        write_str(&mut buf, id);

        write_str(&mut buf, "data");

        // Raw passthrough: write the msgpack bytes directly as the value.
        // These bytes are already a valid msgpack map from storage.
        buf.extend_from_slice(data_bytes);
    }

    Ok(buf)
}

/// Decode concatenated row payloads into `(doc_id, msgpack_data)` pairs.
///
/// Also used by the Control Plane sync layer to filter snapshot documents
/// by a shape predicate before sending them to subscribers.
///
/// Input: zero or more msgpack arrays back-to-back. Elements may be either:
/// - raw scan rows from `encode_raw_document_rows` with `{id, data}` wrappers
/// - plain msgpack rows from aggregate/join paths serialized via `encode_json_vec`
///
/// For wrapped scan rows, the `data` field's raw bytes are extracted. For
/// plain rows, the entire row value is returned as `msgpack_data`.
pub(crate) fn decode_raw_scan_to_docs(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    use nodedb_query::msgpack_scan;

    let mut results = Vec::new();
    let mut pos = 0;

    while pos < bytes.len() {
        let first = bytes[pos];
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && pos + 3 <= bytes.len() {
            (
                u16::from_be_bytes([bytes[pos + 1], bytes[pos + 2]]) as usize,
                3,
            )
        } else if first == 0xdd && pos + 5 <= bytes.len() {
            (
                u32::from_be_bytes([
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                    bytes[pos + 4],
                ]) as usize,
                5,
            )
        } else {
            break;
        };

        let mut inner = pos + hdr_len;
        for _ in 0..count {
            if inner >= bytes.len() {
                break;
            }

            let elem_start = inner;
            let elem_end = msgpack_scan::skip_value(bytes, inner).unwrap_or(bytes.len());

            let id = msgpack_scan::extract_field(bytes, elem_start, "id")
                .and_then(|(s, _e)| msgpack_scan::read_value(bytes, s))
                .and_then(|v| match v {
                    nodedb_types::Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_default();

            let data = msgpack_scan::extract_field(bytes, elem_start, "data")
                .map(|(s, e)| bytes[s..e].to_vec())
                .unwrap_or_else(|| bytes[elem_start..elem_end].to_vec());

            results.push((id, data));

            inner = elem_end;
        }
        pos = inner;
    }

    results
}

/// Storage→relational boundary: flatten gathered storage rows into a single
/// flat relational row array.
///
/// Gathered output is heterogeneous by producer — document/columnar scans emit
/// the storage wire format (`{id, data:<value>}` wrappers, carrying the
/// surrogate), while computed producers (aggregates, joins) already emit flat
/// column maps. This extracts the inner `data` value for storage rows and passes
/// flat rows through unchanged, returning one msgpack array of flat rows.
///
/// This is the ONE place storage rows become relational rows. The
/// relational-operator layer (hash-join probe, `ProviderScan`) consumes only
/// this flat shape and never sees — or sniffs for — a `{id, data}` wrapper. The
/// `{id, data}` format stays confined to the storage / transport / sync layer.
pub fn flatten_to_relational_rows(bytes: &[u8]) -> Vec<u8> {
    let flat: Vec<Vec<u8>> = decode_raw_scan_to_docs(bytes)
        .into_iter()
        .map(|(_id, data)| data)
        .collect();
    encode_binary_rows(&flat)
}

/// Encode a list of pre-built binary msgpack rows into a single msgpack array.
///
/// Each row is already a valid msgpack value (typically a map). This just
/// wraps them in an array header and concatenates — zero decode.
pub fn encode_binary_rows(rows: &[Vec<u8>]) -> Vec<u8> {
    let data_size: usize = rows.iter().map(|r| r.len()).sum();
    let mut buf = Vec::with_capacity(data_size + 8);
    msgpack_write_array_header(&mut buf, rows.len());
    for row in rows {
        buf.extend_from_slice(row);
    }
    buf
}

/// Write a msgpack array header.
pub(super) fn msgpack_write_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}
