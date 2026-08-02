// SPDX-License-Identifier: Apache-2.0

//! Writer for the NDVS v2 on-disk vector segment format.

use std::path::Path;

use super::format::{
    DTYPE_F32, FOOTER_SIZE, FORMAT_VERSION, HEADER_SIZE, MAGIC, VectorSegmentCodec, vec_pad,
};

/// Write a v2 NDVS segment file to `path`.
///
/// `surrogate_ids[i]` is the u64 surrogate for `vectors[i]`. The slice may be
/// empty, in which case all surrogate IDs are written as 0.
///
/// The segment is assembled in memory and published with a single
/// `write → sync_data → rename → fsync_dir` sequence, so `path` only ever names
/// a complete, footer-terminated segment. Writing into the final path directly
/// had two crash windows: truncating it destroyed an existing segment before
/// anything valid replaced it, and a crash between the body and footer syncs
/// left a final-named file that no reader could validate.
///
/// # Errors
///
/// Returns `std::io::Error` on any I/O failure or arithmetic overflow.
pub fn write_segment(
    path: &Path,
    dim: usize,
    vectors: &[&[f32]],
    surrogate_ids: &[u64],
) -> std::io::Result<()> {
    debug_assert!(
        surrogate_ids.is_empty() || surrogate_ids.len() == vectors.len(),
        "surrogate_ids length must match vectors length or be empty"
    );

    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "segment path has no parent directory",
        )
    })?;
    std::fs::create_dir_all(parent)?;

    let count = vectors.len() as u64;

    let vec_bytes = dim
        .checked_mul(vectors.len())
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "vector data size overflow")
        })?;
    let surrogate_bytes = vectors.len().checked_mul(8).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "surrogate block size overflow",
        )
    })?;
    let pad_bytes = vec_pad(vec_bytes);
    let body_len = HEADER_SIZE + vec_bytes + pad_bytes + surrogate_bytes;

    let mut buf = Vec::with_capacity(body_len + FOOTER_SIZE);

    // Header — 32 bytes.
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    buf.push(DTYPE_F32);
    buf.push(VectorSegmentCodec::None as u8);
    buf.extend_from_slice(&[0u8; 10]); // reserved (header total 32, 8-byte aligned)

    // Vector data block — D × N × 4 bytes, row-major, no framing.
    for v in vectors {
        debug_assert_eq!(v.len(), dim, "vector dimension mismatch during write");
        // Safe: `f32` has no padding or invalid bit patterns, so its backing
        // storage is always a valid byte slice of the same lifetime.
        let bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 4) };
        buf.extend_from_slice(bytes);
    }

    // Pad to 8-byte alignment so the surrogate ID block is naturally aligned.
    if pad_bytes > 0 {
        buf.extend_from_slice(&[0u8; 8][..pad_bytes]);
    }

    // Surrogate ID block — N × 8 bytes.
    for i in 0..vectors.len() {
        let sid: u64 = surrogate_ids.get(i).copied().unwrap_or(0);
        buf.extend_from_slice(&sid.to_le_bytes());
    }

    if buf.len() != body_len {
        return Err(std::io::Error::other(format!(
            "unexpected segment body size: {} vs {body_len}",
            buf.len()
        )));
    }

    // CRC32C over the body (header + vector block + pad + surrogate block).
    let checksum = crc32c::crc32c(&buf);

    let mut created_by = [0u8; 32];
    let ver = env!("CARGO_PKG_VERSION").as_bytes();
    let copy_len = ver.len().min(31);
    created_by[..copy_len].copy_from_slice(&ver[..copy_len]);

    // Footer — 46 bytes.
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes()); // format_version [0..2]
    buf.extend_from_slice(&created_by); // created_by                       [2..34]
    buf.extend_from_slice(&checksum.to_le_bytes()); // checksum             [34..38]
    buf.extend_from_slice(&(FOOTER_SIZE as u32).to_le_bytes()); // size     [38..42]
    buf.extend_from_slice(&MAGIC); // trailing magic                        [42..46]

    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".ndvs-tmp");
    let tmp = std::path::PathBuf::from(tmp);

    nodedb_wal::segment::atomic_write_fsync(&tmp, path, &buf)
        .map_err(|e| std::io::Error::other(format!("publish segment {}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_footer_checksum(bytes: &[u8]) -> u32 {
        let start = bytes.len() - FOOTER_SIZE;
        u32::from_le_bytes([
            bytes[start + 34],
            bytes[start + 35],
            bytes[start + 36],
            bytes[start + 37],
        ])
    }

    /// The published file is always complete: body + footer, checksum covering
    /// the body, trailing magic intact, and no tmp file left beside it.
    #[test]
    fn publishes_a_complete_footer_terminated_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seg.ndvs");
        let v = [1.0f32, 2.0, 3.0];

        write_segment(&path, 3, &[&v], &[7]).expect("write");

        let bytes = std::fs::read(&path).expect("read");
        assert!(bytes.len() > FOOTER_SIZE);
        assert_eq!(&bytes[bytes.len() - 4..], &MAGIC, "trailing magic");
        assert_eq!(
            read_footer_checksum(&bytes),
            crc32c::crc32c(&bytes[..bytes.len() - FOOTER_SIZE]),
            "footer checksum must cover exactly the body"
        );

        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir")
            .flatten()
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .collect();
        assert_eq!(
            leftovers,
            vec!["seg.ndvs".to_string()],
            "the staging file must be renamed away, not left behind"
        );
    }

    /// Rewriting a segment replaces it via rename — the destination is never
    /// truncated in place, so it never exists in a bodyless or footerless state.
    #[test]
    fn rewrite_replaces_the_previous_segment_wholesale() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seg.ndvs");

        let a = [1.0f32, 2.0];
        write_segment(&path, 2, &[&a, &a, &a], &[1, 2, 3]).expect("first write");
        let first_len = std::fs::metadata(&path).expect("stat").len();

        let b = [9.0f32, 8.0];
        write_segment(&path, 2, &[&b], &[4]).expect("second write");
        let bytes = std::fs::read(&path).expect("read");

        assert_ne!(bytes.len() as u64, first_len, "smaller segment replaced it");
        assert_eq!(&bytes[bytes.len() - 4..], &MAGIC);
        assert_eq!(
            read_footer_checksum(&bytes),
            crc32c::crc32c(&bytes[..bytes.len() - FOOTER_SIZE])
        );
        assert_eq!(
            std::fs::read_dir(dir.path()).expect("read_dir").count(),
            1,
            "no staging file survives a rewrite"
        );
    }

    /// An empty segment is still a valid, footer-terminated file.
    #[test]
    fn empty_segment_is_valid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.ndvs");

        write_segment(&path, 4, &[], &[]).expect("write");

        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(bytes.len(), HEADER_SIZE + FOOTER_SIZE);
        assert_eq!(
            read_footer_checksum(&bytes),
            crc32c::crc32c(&bytes[..HEADER_SIZE])
        );
    }
}
