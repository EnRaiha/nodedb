// SPDX-License-Identifier: Apache-2.0

//! Allocation-free validation for persisted product-quantizer codebooks.

use std::mem::size_of;

use crate::error::VectorError;

fn invalid_payload(detail: &str) -> VectorError {
    VectorError::DeserializationFailed(detail.to_string())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn byte(&mut self) -> Result<u8, VectorError> {
        let value = self
            .bytes
            .get(self.position)
            .copied()
            .ok_or_else(|| invalid_payload("truncated PQ payload"))?;
        self.position += 1;
        Ok(value)
    }

    fn exact<const N: usize>(&mut self) -> Result<[u8; N], VectorError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or_else(|| invalid_payload("PQ payload offset overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .and_then(|slice| slice.try_into().ok())
            .ok_or_else(|| invalid_payload("truncated PQ payload"))?;
        self.position = end;
        Ok(value)
    }

    fn array_len(&mut self) -> Result<usize, VectorError> {
        match self.byte()? {
            marker @ 0x90..=0x9f => Ok(usize::from(marker & 0x0f)),
            0xdc => Ok(usize::from(u16::from_be_bytes(self.exact()?))),
            0xdd => usize::try_from(u32::from_be_bytes(self.exact()?))
                .map_err(|_| invalid_payload("PQ array length exceeds platform size")),
            _ => Err(invalid_payload("expected PQ array")),
        }
    }

    fn usize(&mut self) -> Result<usize, VectorError> {
        let value = match self.byte()? {
            marker @ 0x00..=0x7f => u64::from(marker),
            0xcc => u64::from(self.byte()?),
            0xcd => u64::from(u16::from_be_bytes(self.exact()?)),
            0xce => u64::from(u32::from_be_bytes(self.exact()?)),
            0xcf => u64::from_be_bytes(self.exact()?),
            _ => return Err(invalid_payload("expected PQ unsigned integer")),
        };
        usize::try_from(value).map_err(|_| invalid_payload("PQ integer exceeds platform size"))
    }

    fn f32(&mut self) -> Result<(), VectorError> {
        if self.byte()? != 0xca {
            return Err(invalid_payload("expected PQ float component"));
        }
        let _: [u8; 4] = self.exact()?;
        Ok(())
    }
}

/// Prove every allocation-bearing MessagePack cardinality before the generic
/// decoder is allowed to reserve its nested vectors.
pub(crate) fn preflight_pq_payload(
    payload: &[u8],
    max_dimension: usize,
    max_allocation_bytes: usize,
) -> Result<(), VectorError> {
    let mut reader = Cursor::new(payload);
    if reader.array_len()? != 5 {
        return Err(invalid_payload("invalid PQ field count"));
    }
    let metadata = (
        reader.usize()?,
        reader.usize()?,
        reader.usize()?,
        reader.usize()?,
    );
    let observed = read_codebook_shape(&mut reader, max_dimension, max_allocation_bytes)?;
    if observed != (metadata.1, metadata.2, metadata.3) {
        return Err(invalid_payload("PQ codebook shape does not match metadata"));
    }
    if metadata.0 > max_dimension
        || metadata
            .1
            .checked_mul(metadata.3)
            .is_none_or(|dimension| dimension != metadata.0)
    {
        return Err(invalid_payload("invalid PQ dimensions"));
    }
    Ok(())
}

fn read_codebook_shape(
    reader: &mut Cursor<'_>,
    max_dimension: usize,
    max_allocation_bytes: usize,
) -> Result<(usize, usize, usize), VectorError> {
    let outer = reader.array_len()?;
    if outer == 0 || outer > max_dimension {
        return Err(invalid_payload("invalid PQ codebook count"));
    }
    let mut expected_centroids = None;
    let mut expected_components = None;
    let mut total_bytes = outer
        .checked_mul(size_of::<Vec<Vec<f32>>>())
        .ok_or_else(|| invalid_payload("PQ codebook allocation overflow"))?;

    for _ in 0..outer {
        let centroid_count = reader.array_len()?;
        if centroid_count == 0 || centroid_count > usize::from(u8::MAX) + 1 {
            return Err(invalid_payload("invalid PQ centroid count"));
        }
        if expected_centroids
            .replace(centroid_count)
            .is_some_and(|value| value != centroid_count)
        {
            return Err(invalid_payload("inconsistent PQ centroid count"));
        }
        total_bytes = total_bytes
            .checked_add(
                centroid_count
                    .checked_mul(size_of::<Vec<f32>>())
                    .ok_or_else(|| invalid_payload("PQ centroid allocation overflow"))?,
            )
            .ok_or_else(|| invalid_payload("PQ codebook allocation overflow"))?;

        for _ in 0..centroid_count {
            let components = reader.array_len()?;
            if components == 0 || components > max_dimension {
                return Err(invalid_payload("invalid PQ centroid dimension"));
            }
            if expected_components
                .replace(components)
                .is_some_and(|value| value != components)
            {
                return Err(invalid_payload("inconsistent PQ centroid dimension"));
            }
            total_bytes = total_bytes
                .checked_add(
                    components
                        .checked_mul(size_of::<f32>())
                        .ok_or_else(|| invalid_payload("PQ component allocation overflow"))?,
                )
                .ok_or_else(|| invalid_payload("PQ codebook allocation overflow"))?;
            if total_bytes > max_allocation_bytes {
                return Err(invalid_payload("PQ codebook allocation exceeds limit"));
            }
            for _ in 0..components {
                reader.f32()?;
            }
        }
    }

    Ok((
        outer,
        expected_centroids.ok_or_else(|| invalid_payload("missing PQ centroids"))?,
        expected_components.ok_or_else(|| invalid_payload("missing PQ components"))?,
    ))
}
