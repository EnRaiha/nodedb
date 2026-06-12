// SPDX-License-Identifier: Apache-2.0

//! `WalRecord` — header + payload with encryption + checksum helpers.

use super::header::{
    ENCRYPTED_FLAG, HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE, RecordHeader, WAL_FORMAT_VERSION, WAL_MAGIC,
};
use crate::error::{Result, WalError};
use crate::preamble::PREAMBLE_SIZE;

/// A complete WAL record: header + payload.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub header: RecordHeader,
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Create a new WAL record with computed CRC32C.
    ///
    /// If `encryption_key` is provided, the payload is encrypted before
    /// CRC computation. The ciphertext includes a 16-byte auth tag.
    ///
    /// `preamble_bytes` — when encryption is active, the 16-byte segment
    /// preamble that was written at offset 0 of this segment file. It is
    /// concatenated with the record header bytes to form the AAD, binding
    /// the ciphertext to its segment (preamble-swap defense). Pass `None`
    /// for unencrypted records (the argument is ignored in that case).
    ///
    /// `database_id` is stored in header bytes 34-41 (previously reserved,
    /// zero-filled). Pre-existing records with zeros decode to `DatabaseId(0)`
    /// (the default database), preserving backward compatibility.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        record_type: u32,
        lsn: u64,
        tenant_id: u64,
        vshard_id: u32,
        database_id: u64,
        payload: Vec<u8>,
        encryption_key: Option<&crate::crypto::WalEncryptionKey>,
        preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
    ) -> Result<Self> {
        if payload.len() > MAX_WAL_PAYLOAD_SIZE {
            return Err(WalError::PayloadTooLarge {
                size: payload.len(),
                max: MAX_WAL_PAYLOAD_SIZE,
            });
        }

        let (final_payload, encrypted) = if let Some(key) = encryption_key {
            let temp_header = RecordHeader {
                magic: WAL_MAGIC,
                format_version: WAL_FORMAT_VERSION,
                record_type,
                lsn,
                tenant_id,
                vshard_id,
                payload_len: 0,
                database_id,
                reserved: [0u8; 8],
                crc32c: 0,
            };
            let header_bytes = temp_header.to_bytes();
            // AAD = preamble_bytes || header_bytes — binds ciphertext to both
            // the segment it lives in and the record header it belongs to.
            let aad = build_aad(preamble_bytes, &header_bytes);
            let ciphertext = key.encrypt_aad(lsn, &aad, &payload)?;
            (ciphertext, true)
        } else {
            (payload, false)
        };

        let record_type = if encrypted {
            record_type | ENCRYPTED_FLAG
        } else {
            record_type
        };

        let mut header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            payload_len: final_payload.len() as u32,
            database_id,
            reserved: [0u8; 8],
            crc32c: 0,
        };

        header.crc32c = header.compute_checksum(&final_payload);

        Ok(Self {
            header,
            payload: final_payload,
        })
    }

    /// Decrypt the payload if the record is encrypted.
    ///
    /// `epoch` must come from the on-disk segment preamble, not from the
    /// current in-memory key. `preamble_bytes` must be the same 16-byte
    /// preamble that was used as part of the AAD during encryption.
    pub fn decrypt_payload(
        &self,
        epoch: &[u8; 4],
        preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
        encryption_key: Option<&crate::crypto::WalEncryptionKey>,
    ) -> Result<Vec<u8>> {
        if !self.is_encrypted() {
            return Ok(self.payload.clone());
        }

        let key = encryption_key.ok_or_else(|| WalError::EncryptionError {
            detail: "record is encrypted but no decryption key provided".into(),
        })?;

        let mut aad_header = self.header;
        aad_header.record_type &= !ENCRYPTED_FLAG;
        aad_header.payload_len = 0;
        aad_header.crc32c = 0;
        let header_bytes = aad_header.to_bytes();
        let aad = build_aad(preamble_bytes, &header_bytes);

        key.decrypt_aad(epoch, self.header.lsn, &aad, &self.payload)
    }

    /// Decrypt the payload using a key ring (supports dual-key rotation).
    ///
    /// `epoch` must come from the on-disk segment preamble. `preamble_bytes`
    /// must match the preamble bytes written at the start of this segment.
    pub fn decrypt_payload_ring(
        &self,
        epoch: &[u8; 4],
        preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
        ring: Option<&crate::crypto::KeyRing>,
    ) -> Result<Vec<u8>> {
        if !self.is_encrypted() {
            return Ok(self.payload.clone());
        }

        let ring = ring.ok_or_else(|| WalError::EncryptionError {
            detail: "record is encrypted but no decryption key ring provided".into(),
        })?;

        let mut aad_header = self.header;
        aad_header.record_type &= !ENCRYPTED_FLAG;
        aad_header.payload_len = 0;
        aad_header.crc32c = 0;
        let header_bytes = aad_header.to_bytes();
        let aad = build_aad(preamble_bytes, &header_bytes);

        ring.decrypt_aad(epoch, self.header.lsn, &aad, &self.payload)
    }

    /// Whether this record's payload is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.header.record_type & ENCRYPTED_FLAG != 0
    }

    /// Logical record type with the encryption flag stripped.
    pub fn logical_record_type(&self) -> u32 {
        self.header.record_type & !ENCRYPTED_FLAG
    }

    /// Verify the CRC32C checksum.
    pub fn verify_checksum(&self) -> Result<()> {
        let expected = self.header.crc32c;
        let actual = self.header.compute_checksum(&self.payload);
        if expected != actual {
            return Err(WalError::ChecksumMismatch {
                lsn: self.header.lsn,
                expected,
                actual,
            });
        }
        Ok(())
    }

    /// Total size on disk: header + payload.
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }
}

/// Build the AAD buffer: `preamble_bytes || header_bytes`.
///
/// When `preamble_bytes` is `None` (no encryption or legacy path), the AAD
/// is just the header bytes. When present, the preamble is prepended.
pub(crate) fn build_aad(
    preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
    header_bytes: &[u8; HEADER_SIZE],
) -> Vec<u8> {
    match preamble_bytes {
        Some(p) => {
            let mut aad = Vec::with_capacity(PREAMBLE_SIZE + HEADER_SIZE);
            aad.extend_from_slice(p);
            aad.extend_from_slice(header_bytes);
            aad
        }
        None => header_bytes.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::RecordType;
    use super::*;

    #[test]
    fn checksum_roundtrip() {
        let payload = b"hello nodedb";
        let record = WalRecord::new(
            RecordType::Put as u32,
            1,
            0,
            0,
            0,
            payload.to_vec(),
            None,
            None,
        )
        .unwrap();
        record.verify_checksum().unwrap();
    }

    #[test]
    fn checksum_detects_corruption() {
        let payload = b"hello nodedb";
        let mut record = WalRecord::new(
            RecordType::Put as u32,
            1,
            0,
            0,
            0,
            payload.to_vec(),
            None,
            None,
        )
        .unwrap();
        record.payload[0] ^= 0xFF;
        assert!(matches!(
            record.verify_checksum(),
            Err(WalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn payload_too_large_rejected() {
        let big_payload = vec![0u8; MAX_WAL_PAYLOAD_SIZE + 1];
        assert!(matches!(
            WalRecord::new(RecordType::Put as u32, 1, 0, 0, 0, big_payload, None, None),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn anchor_payload_in_record() {
        use super::super::anchor::LsnMsAnchorPayload;
        let anchor = LsnMsAnchorPayload::new(42, 1_700_000_000_000);
        let record = WalRecord::new(
            RecordType::LsnMsAnchor as u32,
            42,
            0,
            0,
            0,
            anchor.to_bytes().to_vec(),
            None,
            None,
        )
        .unwrap();
        record.verify_checksum().unwrap();
        assert_eq!(record.logical_record_type(), RecordType::LsnMsAnchor as u32);
        let decoded = LsnMsAnchorPayload::from_bytes(&record.payload).unwrap();
        assert_eq!(decoded, anchor);
    }
}
