// SPDX-License-Identifier: BUSL-1.1

//! Chunking of a bulk load's rows into cell batches, one batch per vShard.

use std::collections::HashMap;

use nodedb_sql::types_array::ArrayInsertRow;

/// The encoded size one load chunk targets: 4 MiB of `ArrayPutCell`
/// msgpack, by the per-row upper bound.
///
/// One chunk is one `ArrayOp::Put`: one plan, one WAL record and one Data
/// Plane batch. 4 MiB stays far under the 64 MiB WAL record cap, so a chunk
/// never fails at the WAL append. It keeps the Control Plane's per-chunk
/// buffers small, so a load holds its input plus a few chunks. It is large
/// enough that the per-chunk costs (one plan, one WAL append, one SPSC round
/// trip) spread over tens of thousands of cells.
pub(crate) const ARRAY_COPY_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// How many chunk sizes of rows the chunker holds across all its vShard
/// buckets. Past it the fullest bucket dispatches, so a load that touches
/// many shards still holds bounded memory.
const PENDING_CHUNKS: usize = 4;

/// One chunk to dispatch: its rows, all bound for one vShard, in input
/// order, the 1-based number of its first row, and the upper bound of its
/// cells' encoded bytes.
pub(super) struct Chunk {
    pub rows: Vec<ArrayInsertRow>,
    pub first_row: usize,
    pub bytes: usize,
}

struct Bucket {
    rows: Vec<ArrayInsertRow>,
    bytes: usize,
    first_row: usize,
}

/// Buckets rows by the vShard their tile routes to and hands out a bucket as
/// a chunk once it reaches the chunk size.
///
/// A repeated coordinate routes to the same vShard, so it lands in the same
/// bucket after its first write. Chunks of one bucket dispatch in input
/// order, so the last write of a coordinate wins.
pub(super) struct ArrayChunker {
    chunk_bytes: usize,
    pending_cap: usize,
    pending: usize,
    buckets: HashMap<u32, Bucket>,
}

impl ArrayChunker {
    pub(super) fn new(chunk_bytes: usize) -> Self {
        Self {
            chunk_bytes,
            pending_cap: chunk_bytes.saturating_mul(PENDING_CHUNKS),
            pending: 0,
            buckets: HashMap::new(),
        }
    }

    /// Add row `row_no`, bound for `vshard`, whose cell encodes to at most
    /// `bound` bytes. Returns a chunk to dispatch now: the row's bucket once
    /// it reaches the chunk size, else the fullest bucket once the pending
    /// total reaches its cap.
    pub(super) fn push(
        &mut self,
        vshard: u32,
        row_no: usize,
        row: ArrayInsertRow,
        bound: usize,
    ) -> Option<Chunk> {
        let bucket = self.buckets.entry(vshard).or_insert_with(|| Bucket {
            rows: Vec::new(),
            bytes: 0,
            first_row: row_no,
        });
        if bucket.rows.is_empty() {
            bucket.first_row = row_no;
        }
        bucket.rows.push(row);
        bucket.bytes += bound;
        self.pending += bound;
        if bucket.bytes >= self.chunk_bytes {
            return self.take(vshard);
        }
        if self.pending >= self.pending_cap {
            let fullest = self
                .buckets
                .iter()
                .max_by_key(|(_, b)| b.bytes)
                .map(|(&v, _)| v)?;
            return self.take(fullest);
        }
        None
    }

    /// The buckets left at the end of the input, as chunks in the order of
    /// their first rows.
    pub(super) fn finish(&mut self) -> Vec<Chunk> {
        self.pending = 0;
        let mut chunks: Vec<Chunk> = self
            .buckets
            .drain()
            .map(|(_, bucket)| bucket)
            .filter(|b| !b.rows.is_empty())
            .map(|b| Chunk {
                rows: b.rows,
                first_row: b.first_row,
                bytes: b.bytes,
            })
            .collect();
        chunks.sort_by_key(|c| c.first_row);
        chunks
    }

    fn take(&mut self, vshard: u32) -> Option<Chunk> {
        let bucket = self.buckets.remove(&vshard)?;
        self.pending -= bucket.bytes;
        Some(Chunk {
            rows: bucket.rows,
            first_row: bucket.first_row,
            bytes: bucket.bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types_array::{ArrayAttrLiteral, ArrayCoordLiteral};

    fn row(n: i64) -> ArrayInsertRow {
        ArrayInsertRow {
            coords: vec![ArrayCoordLiteral::Int64(n)],
            attrs: vec![ArrayAttrLiteral::Int64(n)],
        }
    }

    #[test]
    fn one_bucket_chunks_by_size_in_order() {
        let mut chunker = ArrayChunker::new(100);
        let mut chunks = Vec::new();
        for n in 1..=10 {
            chunks.extend(chunker.push(0, n as usize, row(n), 30));
        }
        chunks.extend(chunker.finish());
        let sizes: Vec<usize> = chunks.iter().map(|c| c.rows.len()).collect();
        assert_eq!(sizes, vec![4, 4, 2]);
        let firsts: Vec<usize> = chunks.iter().map(|c| c.first_row).collect();
        assert_eq!(firsts, vec![1, 5, 9]);
    }

    #[test]
    fn the_pending_cap_flushes_the_fullest_bucket() {
        let mut chunker = ArrayChunker::new(100);
        // Buckets of 95, 90, 90 and 90 bytes: 365 pending, under the 400 cap.
        assert!(chunker.push(1, 1, row(1), 95).is_none());
        for v in 2..=4u32 {
            assert!(chunker.push(v, v as usize, row(i64::from(v)), 90).is_none());
        }
        // 40 bytes on a fifth bucket reach 405. No bucket is full, so the
        // fullest, bucket 1, dispatches.
        let chunk = chunker.push(5, 5, row(5), 40).expect("flush");
        assert_eq!(chunk.first_row, 1);
        assert_eq!(chunk.rows.len(), 1);
        let left: Vec<usize> = chunker.finish().iter().map(|c| c.first_row).collect();
        assert_eq!(left, vec![2, 3, 4, 5]);
    }

    #[test]
    fn many_small_buckets_flush_before_the_cap_is_passed() {
        let mut chunker = ArrayChunker::new(100);
        let mut flushed = 0;
        for v in 0..20u32 {
            if chunker
                .push(v, v as usize + 1, row(i64::from(v)), 25)
                .is_some()
            {
                flushed += 1;
            }
        }
        assert!(flushed > 0, "pending rows across buckets stay bounded");
    }
}
