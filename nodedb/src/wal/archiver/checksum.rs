// SPDX-License-Identifier: BUSL-1.1

//! CRC32C of whole local segment files, the identity an archived segment is
//! checked against.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Read size for checksumming a segment without loading it whole.
const CHECKSUM_CHUNK_BYTES: usize = 1 << 20;

/// CRC32C of the file at `path`.
pub fn segment_crc32c(path: &Path) -> std::io::Result<u32> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = vec![0u8; CHECKSUM_CHUNK_BYTES];
    let mut crc = 0u32;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            return Ok(crc);
        }
        crc = crc32c::crc32c_append(crc, &buf[..n]);
    }
}

/// CRC32C of every `(first_lsn, path)` pair, read off the async runtime.
pub async fn local_crc32c(files: Vec<(u64, PathBuf)>) -> crate::Result<HashMap<u64, u32>> {
    tokio::task::spawn_blocking(move || {
        files
            .into_iter()
            .map(|(first_lsn, path)| segment_crc32c(&path).map(|crc| (first_lsn, crc)))
            .collect::<std::io::Result<HashMap<u64, u32>>>()
    })
    .await
    .map_err(|e| crate::Error::ColdStorage {
        detail: format!("spawn_blocking join: {e}"),
    })?
    .map_err(crate::Error::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_checksum_equals_the_one_shot_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        let bytes: Vec<u8> = (0..(CHECKSUM_CHUNK_BYTES * 2 + 17))
            .map(|i| (i % 251) as u8)
            .collect();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(segment_crc32c(&path).unwrap(), crc32c::crc32c(&bytes));
    }
}
