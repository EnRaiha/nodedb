// SPDX-License-Identifier: Apache-2.0

//! Segment preamble: 16-byte plaintext header written at offset 0 of every
//! WAL segment file and every storage segment file.
//!
//! The preamble persists the encryption epoch so that the correct nonce can be
//! reconstructed after a process restart, snapshot restore, or segment copy.
//!
//! ## On-disk layout (16 bytes, plaintext)
//!
//! ```text
//! Bytes  Field        Type     Notes
//! ─────────────────────────────────────────────────────────────────
//!  0.. 4  magic        [u8;4]   b"WALP" for WAL segments
//!                               b"SEGP" for storage segments
//!  4.. 6  version      u16 LE   Must be 1; future versions can change layout
//!  6.. 7  cipher_alg   u8       0 = AES-256-GCM; all others reserved
//!  7.. 8  kid          u8       Key ID within a key-ring slot (0 = current KEK)
//!  8..12  epoch        [u8;4]   Random per-WAL-lifetime epoch for nonce construction
//! 12..16  reserved     [u8;4]   Must be zero on write; ignored on read
//! ```
//!
//! The preamble is **plaintext** and is included as Additional Authenticated
//! Data (AAD) on every encrypted record / segment payload. This prevents an
//! attacker from swapping preambles between segments (preamble-swap defense).
//!
//! ## Rejected versions
//!
//! Any preamble with `version != PREAMBLE_VERSION` is rejected at open time.
//! Pre-launch: no migration path. Hard error, not a warning.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::error::{Result, WalError};

/// Size of the preamble in bytes.
pub const PREAMBLE_SIZE: usize = 16;

/// Preamble version this binary understands.
pub const PREAMBLE_VERSION: u16 = 1;

/// `cipher_alg` value for AES-256-GCM.
pub const CIPHER_AES_256_GCM: u8 = 0;

/// Magic bytes for WAL segment preambles.
pub const WAL_PREAMBLE_MAGIC: [u8; 4] = *b"WALP";

/// Magic bytes for storage segment preambles.
pub const SEG_PREAMBLE_MAGIC: [u8; 4] = *b"SEGP";

/// Preamble written at offset 0 of every segment file (WAL or storage).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentPreamble {
    /// File-type magic: [`WAL_PREAMBLE_MAGIC`] or [`SEG_PREAMBLE_MAGIC`].
    pub magic: [u8; 4],
    /// Layout version (must equal [`PREAMBLE_VERSION`]).
    pub version: u16,
    /// Cipher algorithm: 0 = AES-256-GCM.
    pub cipher_alg: u8,
    /// Key ID within the active key ring (0 = current KEK).
    pub kid: u8,
    /// Random epoch generated at WAL-lifetime start; used as the high 4 bytes
    /// of every AES-GCM nonce in this segment.
    pub epoch: [u8; 4],
    // reserved: 4 bytes (zero on write, ignored on read)
}

impl SegmentPreamble {
    /// Construct a new preamble for a WAL segment.
    pub fn new_wal(epoch: [u8; 4]) -> Self {
        Self {
            magic: WAL_PREAMBLE_MAGIC,
            version: PREAMBLE_VERSION,
            cipher_alg: CIPHER_AES_256_GCM,
            kid: 0,
            epoch,
        }
    }

    /// Construct a new preamble for a storage segment.
    pub fn new_seg(epoch: [u8; 4]) -> Self {
        Self {
            magic: SEG_PREAMBLE_MAGIC,
            version: PREAMBLE_VERSION,
            cipher_alg: CIPHER_AES_256_GCM,
            kid: 0,
            epoch,
        }
    }

    /// Serialize to exactly [`PREAMBLE_SIZE`] bytes.
    pub fn to_bytes(&self) -> [u8; PREAMBLE_SIZE] {
        let mut buf = [0u8; PREAMBLE_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6] = self.cipher_alg;
        buf[7] = self.kid;
        buf[8..12].copy_from_slice(&self.epoch);
        // buf[12..16] stays zero (reserved)
        buf
    }

    /// Deserialize and validate a preamble, checking magic and version.
    ///
    /// `expected_magic` must be either [`WAL_PREAMBLE_MAGIC`] or
    /// [`SEG_PREAMBLE_MAGIC`].
    pub fn from_bytes(buf: &[u8; PREAMBLE_SIZE], expected_magic: &[u8; 4]) -> Result<Self> {
        let magic: [u8; 4] = [buf[0], buf[1], buf[2], buf[3]];

        if &magic != expected_magic {
            return Err(WalError::EncryptionError {
                detail: format!(
                    "preamble magic mismatch: expected {:?}, got {:?}",
                    expected_magic, magic
                ),
            });
        }

        let version = u16::from_le_bytes([buf[4], buf[5]]);
        if version != PREAMBLE_VERSION {
            return Err(WalError::UnsupportedVersion {
                version,
                supported: PREAMBLE_VERSION,
            });
        }

        let cipher_alg = buf[6];
        let kid = buf[7];
        let epoch: [u8; 4] = [buf[8], buf[9], buf[10], buf[11]];

        Ok(Self {
            magic,
            version,
            cipher_alg,
            kid,
            epoch,
        })
    }

    /// The epoch bytes, for passing to nonce construction.
    pub fn epoch(&self) -> &[u8; 4] {
        &self.epoch
    }
}

/// [`PREAMBLE_SIZE`] as an offset, without a lossy cast.
fn preamble_size_offset() -> Result<u64> {
    u64::try_from(PREAMBLE_SIZE).map_err(|_| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL preamble size does not fit u64",
        ))
    })
}

/// Parse a preamble sitting at the start of a segment image.
///
/// Returns `Ok(None)` when the bytes do not begin with `expected_magic`:
/// segments written without encryption carry no preamble and start with a
/// record header instead. A preamble this binary cannot interpret is a hard
/// error — scanning its bytes as a record header would make every record in
/// the segment invisible rather than surfacing the version mismatch.
///
/// This is the single detection point shared by the file-based and mmap-based
/// readers so they can never disagree about where records begin.
pub fn parse_leading_preamble(
    bytes: &[u8],
    expected_magic: &[u8; 4],
) -> Result<Option<SegmentPreamble>> {
    let Some(head) = bytes.get(..PREAMBLE_SIZE) else {
        // Too short to hold a preamble.
        return Ok(None);
    };
    if head.get(..4) != Some(&expected_magic[..]) {
        return Ok(None);
    }
    let buf: &[u8; PREAMBLE_SIZE] = head.try_into().map_err(|_| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL preamble slice conversion failed",
        ))
    })?;
    Ok(Some(SegmentPreamble::from_bytes(buf, expected_magic)?))
}

/// Consume a leading preamble from an open segment file.
///
/// Returns the parsed preamble (if any) and the offset the first record starts
/// at. When no preamble is present the cursor is rewound to 0 so the caller can
/// read records straight away.
pub fn read_leading_preamble(
    file: &mut File,
    expected_magic: &[u8; 4],
) -> Result<(Option<SegmentPreamble>, u64)> {
    let mut buf = [0u8; PREAMBLE_SIZE];
    match file.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            file.seek(SeekFrom::Start(0))?;
            return Ok((None, 0));
        }
        Err(e) => return Err(WalError::Io(e)),
    }

    match parse_leading_preamble(&buf, expected_magic)? {
        Some(preamble) => Ok((Some(preamble), preamble_size_offset()?)),
        None => {
            file.seek(SeekFrom::Start(0))?;
            Ok((None, 0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_preamble_roundtrip() {
        let epoch = [0xAA, 0xBB, 0xCC, 0xDD];
        let p = SegmentPreamble::new_wal(epoch);
        let bytes = p.to_bytes();
        let parsed = SegmentPreamble::from_bytes(&bytes, &WAL_PREAMBLE_MAGIC).unwrap();
        assert_eq!(p, parsed);
        assert_eq!(parsed.epoch, epoch);
        assert_eq!(parsed.cipher_alg, CIPHER_AES_256_GCM);
        assert_eq!(parsed.kid, 0);
        assert_eq!(parsed.version, PREAMBLE_VERSION);
    }

    #[test]
    fn seg_preamble_roundtrip() {
        let epoch = [0x11, 0x22, 0x33, 0x44];
        let p = SegmentPreamble::new_seg(epoch);
        let bytes = p.to_bytes();
        let parsed = SegmentPreamble::from_bytes(&bytes, &SEG_PREAMBLE_MAGIC).unwrap();
        assert_eq!(p, parsed);
    }

    #[test]
    fn wrong_magic_rejected() {
        let p = SegmentPreamble::new_wal([0u8; 4]);
        let bytes = p.to_bytes();
        // Reading a WAL preamble as a SEG preamble must fail.
        assert!(SegmentPreamble::from_bytes(&bytes, &SEG_PREAMBLE_MAGIC).is_err());
    }

    #[test]
    fn unsupported_version_rejected() {
        let p = SegmentPreamble::new_wal([0u8; 4]);
        let mut bytes = p.to_bytes();
        // Bump version to 2 (unsupported).
        bytes[4] = 2;
        bytes[5] = 0;
        assert!(matches!(
            SegmentPreamble::from_bytes(&bytes, &WAL_PREAMBLE_MAGIC),
            Err(WalError::UnsupportedVersion { version: 2, .. })
        ));
    }

    #[test]
    fn kid_and_cipher_alg_roundtrip() {
        // Construct manually with non-zero kid.
        let p = SegmentPreamble {
            magic: WAL_PREAMBLE_MAGIC,
            version: PREAMBLE_VERSION,
            cipher_alg: CIPHER_AES_256_GCM,
            kid: 3,
            epoch: [1, 2, 3, 4],
        };
        let bytes = p.to_bytes();
        let parsed = SegmentPreamble::from_bytes(&bytes, &WAL_PREAMBLE_MAGIC).unwrap();
        assert_eq!(parsed.kid, 3);
        assert_eq!(parsed.epoch, [1, 2, 3, 4]);
    }

    #[test]
    fn leading_preamble_detected_only_on_matching_magic() {
        let bytes = SegmentPreamble::new_wal([9, 9, 9, 9]).to_bytes();
        let parsed = parse_leading_preamble(&bytes, &WAL_PREAMBLE_MAGIC).unwrap();
        assert_eq!(parsed.map(|p| p.epoch), Some([9, 9, 9, 9]));

        // A record header (different magic) is not a preamble.
        assert!(
            parse_leading_preamble(&[0xABu8; PREAMBLE_SIZE], &WAL_PREAMBLE_MAGIC)
                .unwrap()
                .is_none()
        );
        // Too short to hold a preamble.
        assert!(
            parse_leading_preamble(&bytes[..4], &WAL_PREAMBLE_MAGIC)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn leading_preamble_version_mismatch_is_an_error() {
        let mut bytes = SegmentPreamble::new_wal([0u8; 4]).to_bytes();
        bytes[4] = 7;
        assert!(matches!(
            parse_leading_preamble(&bytes, &WAL_PREAMBLE_MAGIC),
            Err(WalError::UnsupportedVersion { version: 7, .. })
        ));
    }

    #[test]
    fn reserved_bytes_are_zero() {
        let p = SegmentPreamble::new_wal([0xFF; 4]);
        let bytes = p.to_bytes();
        assert_eq!(&bytes[12..16], &[0u8, 0, 0, 0]);
    }
}
