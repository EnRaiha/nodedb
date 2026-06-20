// SPDX-License-Identifier: BUSL-1.1

//! Shared msgpack-row-array explode helper for the cross-node shuffle path.
//!
//! A `ShufflePushChunk` payload is a standalone msgpack ARRAY of join rows — the
//! same `encode_binary_rows` shape the Data Plane produces for a scan frame. Two
//! shuffle sites need to split that array back into its individual row elements:
//!
//! - the RECEIVE side ([`super::inbox::ShuffleInbox::append_chunk`]) explodes an
//!   arriving chunk into `[u32 LE len][row]` staging frames;
//! - the PRODUCE side ([`super::fanout::ShuffleFanoutSink`]) explodes each scan
//!   batch so it can hash-partition each row before fanning it out.
//!
//! Both reuse this ONE helper (rather than duplicating the array walk) so the
//! per-row byte slices are byte-identical to what `RowSource::ShuffleStream`
//! later reads. It mirrors the Data Plane's `decode_flat_row_array`
//! (`provider_scan.rs`) using the shared `nodedb_query::msgpack_scan` reader.

use nodedb_query::msgpack_scan;

/// Explode a flat msgpack row array into per-row byte slices — one element per
/// join row, borrowed from `bytes` (zero-copy).
///
/// An empty payload yields zero rows. A present-but-malformed header or a
/// truncated element is a hard [`crate::Error::Storage`] error rather than a
/// silent partial decode — a dropped row would corrupt a shuffle partition.
pub(crate) fn explode_row_array(bytes: &[u8]) -> crate::Result<Vec<&[u8]>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let Some((count, mut pos)) = msgpack_scan::array_header(bytes, 0) else {
        return Err(crate::Error::Storage {
            engine: "shuffle-stage".into(),
            detail: "malformed shuffle chunk: expected a msgpack array header".into(),
        });
    };
    let mut rows = Vec::with_capacity(count);
    for i in 0..count {
        let start = pos;
        let Some(end) = msgpack_scan::skip_value(bytes, pos) else {
            return Err(crate::Error::Storage {
                engine: "shuffle-stage".into(),
                detail: format!("malformed shuffle chunk: truncated row {i} of {count}"),
            });
        };
        rows.push(&bytes[start..end]);
        pos = end;
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_payload_yields_no_rows() {
        assert!(explode_row_array(&[]).expect("empty ok").is_empty());
    }

    #[test]
    fn explodes_each_array_element() {
        // [0x93] = fixarray(3) of three positive fixints.
        let bytes = vec![0x93, 0x01, 0x02, 0x03];
        let rows = explode_row_array(&bytes).expect("explode");
        assert_eq!(rows, vec![&[0x01u8][..], &[0x02u8][..], &[0x03u8][..]]);
    }

    #[test]
    fn truncated_array_is_hard_error() {
        // fixarray(1) header but no body element.
        let res = explode_row_array(&[0x91]);
        assert!(
            matches!(res, Err(crate::Error::Storage { .. })),
            "a malformed chunk must surface a Storage error, never a silent drop"
        );
    }

    #[test]
    fn non_array_header_is_hard_error() {
        // 0xc0 = msgpack NIL, not an array header.
        let res = explode_row_array(&[0xc0]);
        assert!(matches!(res, Err(crate::Error::Storage { .. })));
    }
}
