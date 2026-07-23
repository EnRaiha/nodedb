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

    let count = ops.len() as u32;

    // Build actor dictionary.
    let mut actor_dict: Vec<u64> = Vec::new();
    let mut actor_map = std::collections::HashMap::new();
    for op in ops {
        actor_map.entry(op.actor_id).or_insert_with(|| {
            let idx = actor_dict.len() as u16;
            actor_dict.push(op.actor_id);
            idx
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
            .flat_map(|op| actor_map[&op.actor_id].to_le_bytes())
            .collect()
    };

    // FSST-compress content (treat each op's content as a separate string).
    let content_refs: Vec<&[u8]> = ops.iter().map(|op| op.content.as_slice()).collect();
    let content_block = crate::fsst::encode(&content_refs);

    // Build output.
    let mut out = Vec::new();
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&(actor_dict.len() as u16).to_le_bytes());
    for &actor in &actor_dict {
        out.extend_from_slice(&actor.to_le_bytes());
    }
    out.extend_from_slice(&(lamport_block.len() as u32).to_le_bytes());
    out.extend_from_slice(&lamport_block);
    out.push(if use_u8 { 1 } else { 2 }); // index width marker
    out.extend_from_slice(&(actor_indices.len() as u32).to_le_bytes());
    out.extend_from_slice(&actor_indices);
    out.extend_from_slice(&(content_block.len() as u32).to_le_bytes());
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

    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut pos = 4;

    // Actor dictionary.
    if pos + 2 > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + 2,
            actual: data.len(),
        });
    }
    let actor_count = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;

    let actor_bytes = actor_count * 8;
    if pos + actor_bytes > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + actor_bytes,
            actual: data.len(),
        });
    }
    let actor_dict: Vec<u64> = data[pos..pos + actor_bytes]
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
        .collect();
    pos += actor_bytes;

    // Lamport block.
    if pos + 4 > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + 4,
            actual: data.len(),
        });
    }
    let lamport_size =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;
    if pos + lamport_size > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + lamport_size,
            actual: data.len(),
        });
    }
    let lamports = crate::delta::decode(&data[pos..pos + lamport_size])?;
    pos += lamport_size;

    // Actor index width + data.
    if pos >= data.len() {
        return Err(CodecError::Truncated {
            expected: pos + 1,
            actual: data.len(),
        });
    }
    let index_width = data[pos];
    pos += 1;

    if pos + 4 > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + 4,
            actual: data.len(),
        });
    }
    let index_size =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;
    if pos + index_size > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + index_size,
            actual: data.len(),
        });
    }
    let actor_indices: Vec<usize> = if index_width == 1 {
        data[pos..pos + index_size]
            .iter()
            .map(|&b| b as usize)
            .collect()
    } else {
        data[pos..pos + index_size]
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]) as usize)
            .collect()
    };
    pos += index_size;

    // Content block.
    if pos + 4 > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + 4,
            actual: data.len(),
        });
    }
    let content_size =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;
    if pos + content_size > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + content_size,
            actual: data.len(),
        });
    }
    let contents = crate::fsst::decode(&data[pos..pos + content_size])?;

    // Reconstruct ops.
    let mut ops = Vec::with_capacity(count);
    for i in 0..count {
        let lamport = if i < lamports.len() {
            lamports[i] as u64
        } else {
            0
        };
        let actor_idx = if i < actor_indices.len() {
            actor_indices[i]
        } else {
            0
        };
        let actor_id = if actor_idx < actor_dict.len() {
            actor_dict[actor_idx]
        } else {
            0
        };
        let content = if i < contents.len() {
            contents[i].clone()
        } else {
            Vec::new()
        };

        ops.push(CrdtOp {
            lamport,
            actor_id,
            content,
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
