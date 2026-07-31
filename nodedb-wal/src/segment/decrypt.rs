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
//! [`SegmentDecryptor`] is therefore constructed once per segment — by the
//! readers themselves, at open time — and applied to every record before it is
//! handed out. A record that is marked encrypted with no key ring available is
//! a hard [`WalError::EncryptedRecordWithoutKey`], never a passthrough and
//! never a skip.

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
/// Owns both the segment's AAD inputs and its key ring rather than borrowing
/// them: a reader builds one at open time and keeps it for as long as the file
/// handle lives, and borrowing the caller's ring would put a lifetime on every
/// reader type that outlives the call that constructed it.
pub struct SegmentDecryptor {
    ring: Option<KeyRing>,
    aad: Option<SegmentAad>,
}

impl SegmentDecryptor {
    /// Build a decryptor for a segment from its preamble (absent on segments
    /// written without encryption) and the replay key ring (absent when the
    /// database is not configured for WAL encryption).
    pub fn new(preamble: Option<&SegmentPreamble>, ring: Option<&KeyRing>) -> Self {
        Self {
            ring: ring.cloned(),
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
    fn require_keys(&self, lsn: u64) -> Result<(&KeyRing, &SegmentAad)> {
        const SITE: &str = "WAL segment replay";
        let ring = match self.ring.as_ref() {
            Some(ring) => ring,
            None => {
                let err = WalError::EncryptedRecordWithoutKey { lsn, context: SITE };
                crate::diag::encrypted_record_without_key(&err, lsn, SITE);
                return Err(err);
            }
        };
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
