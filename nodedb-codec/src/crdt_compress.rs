// SPDX-License-Identifier: Apache-2.0

//! CRDT state compression for Loro deltas.
//!
//! CRDT operations have a specific data signature:
//! - **Lamport timestamps**: monotonically increasing → Delta → FastLanes
//! - **Actor IDs**: purely entropic, but few unique actors → dictionary dedup
//! - **Content (text edits, JSON)**: contiguous → RLE + FSST for strings
//!
//! This module compresses CRDT operation batches for:
//! - Sync bandwidth reduction (Pattern B)
//! - Long-term storage efficiency (Pattern C)

use std::collections::HashSet;
use std::mem::size_of;

use crate::bounds::{
    checked_add, checked_capacity, checked_mul, checked_range, decoded_len, encode_input_len,
    encode_u32_len, u32_to_usize,
};
use crate::error::CodecError;

/// A CRDT operation for compression.
#[derive(Debug, Clone)]
pub struct CrdtOp {
    /// Lamport timestamp.
    pub lamport: u64,
    /// Actor ID (hash or index into actor dictionary).
    pub actor_id: u64,
    /// Operation payload (text content, JSON fragment, etc.).
    pub content: Vec<u8>,
}

/// Compressed CRDT operation batch.
///
/// Wire format:
/// ```text
/// [4 bytes] op count (LE u32)
/// [2 bytes] actor dictionary size (LE u16)
/// [actor_count × 8 bytes] actor IDs (LE u64)
/// [N bytes] Delta-encoded Lamport timestamps (nodedb-codec delta format)
/// [4 bytes] actor_index block size (LE u32)
/// [M bytes] actor indices (u8 if ≤256 actors, u16 otherwise)
/// [4 bytes] content block size (LE u32)
/// [K bytes] FSST-compressed content (newline-delimited)
/// ```
pub fn encode(ops: &[CrdtOp]) -> Result<Vec<u8>, CodecError> {
    if ops.is_empty() {
        return Ok(0u32.to_le_bytes().to_vec());
    }

    let count = encode_input_len(ops.len(), "CRDT operation count")?;

    // Build actor dictionary.
    let mut actor_dict: Vec<u64> = Vec::new();
    let mut actor_map: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for op in ops {
        actor_map.entry(op.actor_id).or_insert_with(|| {
            let idx = actor_dict.len();
            actor_dict.push(op.actor_id);
            idx
        });
    }

    if actor_dict.len() > u16::MAX as usize {
        return Err(CodecError::ResourceLimit {
            resource: "CRDT actor dictionary entries".into(),
            requested: actor_dict.len(),
            limit: u16::MAX as usize,
        });
    }

    // Delta-encode Lamport timestamps.
    let lamports: Vec<i64> = ops.iter().map(|op| op.lamport as i64).collect();
    let lamport_block = crate::delta::encode(&lamports)?;

    // Actor indices.
    let use_u8 = actor_dict.len() <= 256;
    let actor_indices: Vec<u8> = if use_u8 {
        ops.iter().map(|op| actor_map[&op.actor_id] as u8).collect()
    } else {
        ops.iter()
            .flat_map(|op| (actor_map[&op.actor_id] as u16).to_le_bytes())
            .collect()
    };

    // FSST-compress content (treat each op's content as a separate string).
    let content_refs: Vec<&[u8]> = ops.iter().map(|op| op.content.as_slice()).collect();
    let content_block = crate::fsst::encode(&content_refs)?;

    // Build output.
    let actor_bytes = checked_mul(actor_dict.len(), 8, "CRDT actor dictionary")?;
    let capacity = checked_add(
        checked_add(
            checked_add(6, actor_bytes, "CRDT output")?,
            checked_add(4, lamport_block.len(), "CRDT output")?,
            "CRDT output",
        )?,
        checked_add(
            checked_add(5, actor_indices.len(), "CRDT output")?,
            checked_add(4, content_block.len(), "CRDT output")?,
            "CRDT output",
        )?,
        "CRDT output",
    )?;
    decoded_len(capacity, "CRDT output")?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(
        &u16::try_from(actor_dict.len())
            .map_err(|_| CodecError::ResourceLimit {
                resource: "CRDT actor dictionary entries".into(),
                requested: actor_dict.len(),
                limit: u16::MAX as usize,
            })?
            .to_le_bytes(),
    );
    for &actor in &actor_dict {
        out.extend_from_slice(&actor.to_le_bytes());
    }
    out.extend_from_slice(
        &encode_u32_len(lamport_block.len(), "CRDT Lamport block")?.to_le_bytes(),
    );
    out.extend_from_slice(&lamport_block);
    out.push(if use_u8 { 1 } else { 2 }); // index width marker
    out.extend_from_slice(
        &encode_u32_len(actor_indices.len(), "CRDT actor-index block")?.to_le_bytes(),
    );
    out.extend_from_slice(&actor_indices);
    out.extend_from_slice(
        &encode_u32_len(content_block.len(), "CRDT content block")?.to_le_bytes(),
    );
    out.extend_from_slice(&content_block);

    Ok(out)
}

/// Decode compressed CRDT operations.
pub fn decode(data: &[u8]) -> Result<Vec<CrdtOp>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Truncated {
            expected: 4,
            actual: data.len(),
        });
    }

    let count = u32_to_usize(
        u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
        "CRDT operation count",
    )?;
    let per_operation_overhead = checked_add(
        size_of::<CrdtOp>(),
        checked_add(
            size_of::<i64>(),
            size_of::<usize>(),
            "CRDT temporary operation overhead",
        )?,
        "CRDT decoded operation overhead",
    )?;
    decoded_len(
        checked_mul(count, per_operation_overhead, "CRDT decoded operations")?,
        "CRDT",
    )?;
    if count == 0 {
        if data.len() != 4 {
            return Err(CodecError::Corrupt {
                detail: "non-canonical empty CRDT frame".into(),
            });
        }
        return Ok(Vec::new());
    }

    let mut pos = 4;
    let actor_count = usize::from(u16::from_le_bytes(
        checked_range(data, pos, 2, "CRDT actor count")?
            .try_into()
            .map_err(|_| CodecError::Corrupt {
                detail: "invalid CRDT actor count".into(),
            })?,
    ));
    if actor_count == 0 || actor_count > count {
        return Err(CodecError::Corrupt {
            detail: "CRDT actor count is invalid".into(),
        });
    }
    pos = checked_add(pos, 2, "CRDT actor cursor")?;
    let actor_bytes = checked_mul(actor_count, 8, "CRDT actor dictionary")?;
    let actor_data = checked_range(data, pos, actor_bytes, "CRDT actor dictionary")?;
    let actor_dict: Vec<u64> = actor_data
        .chunks_exact(8)
        .map(|c| {
            Ok(u64::from_le_bytes(c.try_into().map_err(|_| {
                CodecError::Corrupt {
                    detail: "invalid CRDT actor".into(),
                }
            })?))
        })
        .collect::<Result<_, _>>()?;
    let unique_actors: HashSet<u64> = actor_dict.iter().copied().collect();
    if unique_actors.len() != actor_dict.len() {
        return Err(CodecError::Corrupt {
            detail: "duplicate CRDT actor dictionary entry".into(),
        });
    }
    pos = checked_add(pos, actor_bytes, "CRDT actor cursor")?;
    let lamport_size = u32_to_usize(
        u32::from_le_bytes(
            checked_range(data, pos, 4, "CRDT Lamport size")?
                .try_into()
                .map_err(|_| CodecError::Corrupt {
                    detail: "invalid CRDT Lamport size".into(),
                })?,
        ),
        "CRDT Lamport block",
    )?;
    pos = checked_add(pos, 4, "CRDT Lamport cursor")?;
    let lamports = crate::delta::decode(checked_range(
        data,
        pos,
        lamport_size,
        "CRDT Lamport block",
    )?)?;
    if lamports.len() != count {
        return Err(CodecError::Corrupt {
            detail: "CRDT Lamport count mismatch".into(),
        });
    }
    pos = checked_add(pos, lamport_size, "CRDT Lamport cursor")?;
    let index_width = checked_range(data, pos, 1, "CRDT actor-index width")?[0];
    let expected_width = if actor_count <= 256 { 1 } else { 2 };
    if index_width != expected_width {
        return Err(CodecError::Corrupt {
            detail: "CRDT actor-index width is non-canonical".into(),
        });
    }
    pos = checked_add(pos, 1, "CRDT actor-index cursor")?;
    let index_size = u32_to_usize(
        u32::from_le_bytes(
            checked_range(data, pos, 4, "CRDT actor-index size")?
                .try_into()
                .map_err(|_| CodecError::Corrupt {
                    detail: "invalid CRDT actor-index size".into(),
                })?,
        ),
        "CRDT actor-index block",
    )?;
    pos = checked_add(pos, 4, "CRDT actor-index cursor")?;
    let expected_index_size = checked_mul(count, usize::from(index_width), "CRDT actor indices")?;
    if index_size != expected_index_size {
        return Err(CodecError::Corrupt {
            detail: "CRDT actor-index count mismatch".into(),
        });
    }
    let index_data = checked_range(data, pos, index_size, "CRDT actor-index block")?;
    let actor_indices: Vec<usize> = if index_width == 1 {
        index_data.iter().map(|&index| usize::from(index)).collect()
    } else {
        index_data
            .chunks_exact(2)
            .map(|chunk| usize::from(u16::from_le_bytes([chunk[0], chunk[1]])))
            .collect()
    };
    if actor_indices.iter().any(|&index| index >= actor_count) {
        return Err(CodecError::Corrupt {
            detail: "CRDT actor index out of range".into(),
        });
    }
    let actor_capacity = checked_capacity(
        actor_count,
        size_of::<bool>(),
        "CRDT actor first-seen allocation",
    )?;
    let mut first_seen = vec![false; actor_capacity];
    let mut next_actor = 0usize;
    for &index in &actor_indices {
        if !first_seen[index] {
            if index != next_actor {
                return Err(CodecError::Corrupt {
                    detail: "CRDT actor dictionary is not in first-use order".into(),
                });
            }
            first_seen[index] = true;
            next_actor += 1;
        }
    }
    if next_actor != actor_count {
        return Err(CodecError::Corrupt {
            detail: "CRDT actor dictionary contains unused entries".into(),
        });
    }
    pos = checked_add(pos, index_size, "CRDT actor-index cursor")?;
    let content_size = u32_to_usize(
        u32::from_le_bytes(
            checked_range(data, pos, 4, "CRDT content size")?
                .try_into()
                .map_err(|_| CodecError::Corrupt {
                    detail: "invalid CRDT content size".into(),
                })?,
        ),
        "CRDT content block",
    )?;
    pos = checked_add(pos, 4, "CRDT content cursor")?;
    let contents = crate::fsst::decode(checked_range(
        data,
        pos,
        content_size,
        "CRDT content block",
    )?)?;
    if contents.len() != count || checked_add(pos, content_size, "CRDT frame end")? != data.len() {
        return Err(CodecError::Corrupt {
            detail: "CRDT content count or frame length mismatch".into(),
        });
    }

    // Reconstruct ops.
    let operation_capacity = checked_capacity(count, size_of::<CrdtOp>(), "CRDT operations")?;
    let mut ops = Vec::with_capacity(operation_capacity);
    for i in 0..count {
        ops.push(CrdtOp {
            lamport: lamports[i] as u64,
            actor_id: actor_dict[actor_indices[i]],
            content: contents[i].clone(),
        });
    }

    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn basic_roundtrip() {
        let ops = vec![
            CrdtOp {
                lamport: 1,
                actor_id: 100,
                content: b"insert 'hello'".to_vec(),
            },
            CrdtOp {
                lamport: 2,
                actor_id: 100,
                content: b"insert ' world'".to_vec(),
            },
            CrdtOp {
                lamport: 3,
                actor_id: 200,
                content: b"delete [0..5]".to_vec(),
            },
        ];
        let encoded = encode(&ops).unwrap();
        let decoded = decode(&encoded).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].lamport, 1);
        assert_eq!(decoded[0].actor_id, 100);
        assert_eq!(decoded[0].content, b"insert 'hello'");
        assert_eq!(decoded[2].actor_id, 200);
    }

    #[test]
    fn compression_with_many_ops() {
        let mut ops = Vec::new();
        for i in 0..1000 {
            ops.push(CrdtOp {
                lamport: i,
                actor_id: i % 5, // 5 actors
                content: format!("op-{i}: set key_{} = value_{}", i % 50, i).into_bytes(),
            });
        }
        let encoded = encode(&ops).unwrap();
        let decoded = decode(&encoded).unwrap();

        assert_eq!(decoded.len(), 1000);
        for (orig, dec) in ops.iter().zip(decoded.iter()) {
            assert_eq!(orig.lamport, dec.lamport);
            assert_eq!(orig.actor_id, dec.actor_id);
            assert_eq!(orig.content, dec.content);
        }

        // Should compress well — monotonic lamports + few actors + repetitive content.
        let raw_size: usize = ops.iter().map(|op| 16 + op.content.len()).sum();
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.2,
            "CRDT ops should compress >1.2x, got {ratio:.2}x"
        );
    }

    #[test]
    fn rejects_count_mismatch_invalid_indices_and_trailing_bytes() {
        let ops = vec![CrdtOp {
            lamport: 1,
            actor_id: 7,
            content: b"x".to_vec(),
        }];
        let mut trailing = encode(&ops).unwrap();
        trailing.push(0);
        assert!(matches!(decode(&trailing), Err(CodecError::Corrupt { .. })));

        let mut malformed = encode(&ops).unwrap();
        let actor_count = usize::from(u16::from_le_bytes([malformed[4], malformed[5]]));
        let mut cursor = 6 + actor_count * 8;
        let lamport_size =
            u32::from_le_bytes(malformed[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4 + lamport_size + 1 + 4;
        malformed[cursor] = 1;
        assert!(matches!(
            decode(&malformed),
            Err(CodecError::Corrupt { .. })
        ));
    }

    #[test]
    fn wide_actor_indices_roundtrip() {
        let ops: Vec<CrdtOp> = (0..300)
            .map(|i| CrdtOp {
                lamport: i,
                actor_id: i,
                content: vec![b'x'],
            })
            .collect();
        let encoded = encode(&ops).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), ops.len());
        for (expected, actual) in ops.iter().zip(&decoded) {
            assert_eq!(actual.actor_id, expected.actor_id);
        }
    }

    #[test]
    fn actor_dictionary_dedup() {
        let ops: Vec<CrdtOp> = (0..100)
            .map(|i| CrdtOp {
                lamport: i,
                actor_id: 42, // single actor
                content: b"x".to_vec(),
            })
            .collect();
        let encoded = encode(&ops).unwrap();
        let decoded = decode(&encoded).unwrap();

        for op in &decoded {
            assert_eq!(op.actor_id, 42);
        }
    }
}
