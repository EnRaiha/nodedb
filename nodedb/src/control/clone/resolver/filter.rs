// SPDX-License-Identifier: BUSL-1.1

//! Tombstone filtering for source-side scan responses.

/// Filter tombstoned source surrogates from response bytes.
///
/// Given the raw msgpack payload from a source scan, returns a filtered
/// payload that excludes rows whose surrogates are in `tombstoned`. Returns
/// the payload unchanged when nothing is filtered, and `None` only when a
/// non-empty payload is not a msgpack array.
pub fn filter_tombstoned_rows(
    payload: &[u8],
    tombstoned: &std::collections::HashSet<u32>,
) -> Option<Vec<u8>> {
    use nodedb_query::msgpack_scan;

    if tombstoned.is_empty() || payload.is_empty() {
        return Some(payload.to_vec());
    }

    let (count, mut offset) = msgpack_scan::array_header(payload, 0)?;
    let mut kept: Vec<&[u8]> = Vec::with_capacity(count);

    for _ in 0..count {
        let entry_start = offset;
        // The "id" field is the surrogate in hex (e.g., "0000001a").
        let surrogate_opt = msgpack_scan::extract_field(payload, offset, "id")
            .and_then(|(s, _)| msgpack_scan::read_str(payload, s))
            .and_then(|id_str| u32::from_str_radix(id_str, 16).ok());

        let next = msgpack_scan::skip_value(payload, offset)?;
        offset = next;

        if surrogate_opt.is_some_and(|s| tombstoned.contains(&s)) {
            continue; // skip tombstoned row
        }
        kept.push(&payload[entry_start..next]);
    }

    if kept.len() == count {
        return Some(payload.to_vec());
    }

    Some(encode_msgpack_array(&kept))
}

/// Encode a slice of raw msgpack values as a msgpack array.
fn encode_msgpack_array(items: &[&[u8]]) -> Vec<u8> {
    let count = items.len();
    let mut buf = Vec::with_capacity(items.iter().map(|b| b.len()).sum::<usize>() + 5);

    // msgpack array header.
    if count <= 15 {
        buf.push(0x90 | (count as u8));
    } else if count <= 0xFFFF {
        buf.push(0xdc);
        buf.push((count >> 8) as u8);
        buf.push(count as u8);
    } else {
        buf.push(0xdd);
        buf.push((count >> 24) as u8);
        buf.push((count >> 16) as u8);
        buf.push((count >> 8) as u8);
        buf.push(count as u8);
    }
    for item in items {
        buf.extend_from_slice(item);
    }
    buf
}
