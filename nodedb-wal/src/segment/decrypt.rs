// SPDX-License-Identifier: Apache-2.0

//! Segment-scoped payload decryption.
//!
//! Decryption belongs at the point records leave the WAL layer, not at each
//! consumer. A record's AAD binds its ciphertext to the segment preamble it
//! was written under, so only code that still holds the open segment knows the
//! epoch needed to decrypt it. Pushing that knowledge outwards would mean every
//! replay consumer re-deriving it — and any consumer that forgot would silently
//! feed ciphertext into an engine decoder.
//!
//! [`SegmentDecryptor`] is therefore constructed once per segment by the replay
//! drivers and applied to every record before it is handed out. A record that
//! is marked encrypted with no key ring available is a hard
//! [`WalError::EncryptedRecordWithoutKey`], never a passthrough and never a skip.

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::preamble::{PREAMBLE_SIZE, SegmentPreamble};
use crate::record::{RecordHeader, WalRecord};

/// The per-segment inputs to the AAD: the epoch used to rebuild the nonce, and
/// the preamble bytes that were prepended to the header at encryption time.
struct SegmentAad {
    epoch: [u8; 4],
    preamble_bytes: [u8; PREAMBLE_SIZE],
}

/// Turns the records of one WAL segment back into plaintext.
///
/// Owns copies of the segment's AAD inputs rather than borrowing the reader, so
/// a driver can keep reading records mutably while decrypting them.
pub struct SegmentDecryptor<'a> {
    ring: Option<&'a KeyRing>,
    aad: Option<SegmentAad>,
}

impl<'a> SegmentDecryptor<'a> {
    /// Build a decryptor for a segment from its preamble (absent on segments
    /// written without encryption) and the replay key ring (absent when the
    /// database is not configured for WAL encryption).
    pub fn new(preamble: Option<&SegmentPreamble>, ring: Option<&'a KeyRing>) -> Self {
        Self {
            ring,
            aad: preamble.map(|p| SegmentAad {
                epoch: *p.epoch(),
                preamble_bytes: p.to_bytes(),
            }),
        }
    }

    /// Return `record` as plaintext, with `ENCRYPTED_FLAG` cleared and its CRC
    /// recomputed. Unencrypted records pass through untouched.
    pub fn decrypt_record(&self, record: WalRecord) -> Result<WalRecord> {
        if !record.is_encrypted() {
            return Ok(record);
        }
        let (ring, aad) = self.require_keys(record.header.lsn)?;
        record.into_decrypted(&aad.epoch, Some(&aad.preamble_bytes), Some(ring))
    }

    /// Return the plaintext for a payload that was read separately from its
    /// header, as the lazy reader does. Unencrypted payloads pass through.
    pub fn decrypt_payload(&self, header: &RecordHeader, payload: Vec<u8>) -> Result<Vec<u8>> {
        let record = WalRecord {
            header: *header,
            payload,
        };
        if !record.is_encrypted() {
            return Ok(record.payload);
        }
        let (ring, aad) = self.require_keys(header.lsn)?;
        record.decrypt_payload_ring(&aad.epoch, Some(&aad.preamble_bytes), Some(ring))
    }

    /// Resolve the key ring and segment AAD, or explain which one is missing.
    fn require_keys(&self, lsn: u64) -> Result<(&'a KeyRing, &SegmentAad)> {
        let ring = self.ring.ok_or(WalError::EncryptedRecordWithoutKey {
            lsn,
            context: "WAL segment replay",
        })?;
        // An encrypted record can only exist in a segment whose preamble
        // recorded the epoch it was encrypted under. A missing preamble means
        // the segment's leading bytes are gone, not that the record is legible.
        let aad = self.aad.as_ref().ok_or_else(|| WalError::CorruptRecord {
            lsn,
            detail: "encrypted record in a segment with no preamble — the epoch \
                     needed to decrypt it is unrecoverable"
                .into(),
        })?;
        Ok((ring, aad))
    }
}
