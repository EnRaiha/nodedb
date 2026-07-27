// SPDX-License-Identifier: Apache-2.0

//! Per-backup DEK + KEK wrapping for encrypted backup envelopes.
//!
//! ## Wire layout after the 52-byte HEADER (version byte = 1):
//!
//! ```text
//! ┌─ CRYPTO BLOCK (68 bytes) ──────────────────────────────────────────────┐
//! │ kek_fingerprint : [u8; 8]   first 8 bytes of SHA-256(KEK)             │
//! │ dek_nonce       : [u8; 12]  AES-256-GCM nonce for DEK wrapping        │
//! │ wrapped_dek     : [u8; 48]  AES-256-GCM(KEK, dek_nonce, DEK)         │
//! │                             = 32-byte ciphertext + 16-byte tag        │
//! └────────────────────────────────────────────────────────────────────────┘
//! ┌─ ENCRYPTED SECTION × section_cnt ──────────────────────────────────────┐
//! │ origin_node_id  : u64                                                  │
//! │ body_len        : u32  (length of ciphertext, i.e. plaintext + 16 tag) │
//! │ section_nonce   : [u8; 12]  fresh random nonce per section             │
//! │ encrypted_body  : body_len bytes  (AES-256-GCM ciphertext + tag)       │
//! │ body_crc        : u32  (crc32c of the ciphertext — wire error only)    │
//! └────────────────────────────────────────────────────────────────────────┘
//! ┌─ TRAILER ───────────────────────────────────────────────────────────────┐
//! │ trailer_crc : u32  (crc32c over everything preceding this field)       │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### Nonce strategy
//!
//! Each section receives an independent 12-byte random nonce from
//! `getrandom`. Random per-section nonces are preferred over
//! deterministic `base_nonce XOR section_index` because they carry no
//! implicit ordering requirement and remain safe even if sections are
//! ever reordered, duplicated by a buggy producer, or if the same DEK
//! is reused across multiple calls (it must not be, but the random nonce
//! provides defence in depth). With 12-byte nonces the collision
//! probability for a single DEK lifetime (one backup) is negligible even
//! for millions of sections: P ≈ n²/2^97.
//!
//! ### KEK fingerprint
//!
//! The first 8 bytes of SHA-256(KEK) are embedded so that the restore
//! path can detect a mismatched KEK before attempting any cryptography,
//! providing a clear `WrongBackupKek` error rather than an opaque
//! authentication failure.

use aes_gcm::Aes256Gcm;
// aes-gcm 0.10 still re-exports `generic_array` 0.14 even though that crate's
// types are now marked deprecated in favour of generic-array 1.x. Upgrading is
// gated on aes-gcm itself shipping a release on generic-array 1.x; until then
// allow the deprecation locally rather than spamming the build log.
#[allow(deprecated)]
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, KeyInit};
use sha2::{Digest, Sha256};

use super::types::{Envelope, EnvelopeError, EnvelopeMeta, Section, read2, read4, read8};
use super::types::{HEADER_LEN, MAGIC, TRAILER_LEN, VERSION};
use super::write::{EnvelopeWriter, write_header};

/// Size of the crypto block inserted after the header in version-2 envelopes.
///
/// Layout: kek_fingerprint(8) + dek_nonce(12) + wrapped_dek(48) = 68 bytes.
const CRYPTO_BLOCK_LEN: usize = 68;

/// Per-section overhead in an encrypted envelope:
/// origin(8) + body_len(4) + section_nonce(12) + body_crc(4) = 28 bytes.
/// (The body itself is `plaintext_len + 16` due to the AES-GCM tag.)
const ENC_SECTION_OVERHEAD: usize = 28;

// ── KEK fingerprint ──────────────────────────────────────────────────────────

/// Compute the 8-byte KEK fingerprint: first 8 bytes of SHA-256(kek).
fn kek_fingerprint(kek: &[u8; 32]) -> [u8; 8] {
    let hash = Sha256::digest(kek.as_slice());
    let mut fp = [0u8; 8];
    fp.copy_from_slice(&hash[..8]);
    fp
}

// ── random helpers ───────────────────────────────────────────────────────────

fn random_bytes<const N: usize>() -> Result<[u8; N], EnvelopeError> {
    let mut buf = [0u8; N];
    getrandom::fill(&mut buf).map_err(|e| EnvelopeError::RandomFailure(e.to_string()))?;
    Ok(buf)
}

// ── AES-256-GCM helpers ──────────────────────────────────────────────────────

#[allow(deprecated)]
fn aes_encrypt(
    key_bytes: &[u8; 32],
    nonce_bytes: &[u8; 12],
    plaintext: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    let key = GenericArray::from(*key_bytes);
    let cipher = Aes256Gcm::new(&key);
    let nonce = GenericArray::from(*nonce_bytes);
    cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| EnvelopeError::EncryptionFailed)
}

#[allow(deprecated)]
fn aes_decrypt(
    key_bytes: &[u8; 32],
    nonce_bytes: &[u8; 12],
    ciphertext: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    let key = GenericArray::from(*key_bytes);
    let cipher = Aes256Gcm::new(&key);
    let nonce = GenericArray::from(*nonce_bytes);
    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| EnvelopeError::DecryptionFailed)
}

// ── EnvelopeWriter extension ─────────────────────────────────────────────────

impl EnvelopeWriter {
    /// Finalize with encryption. Produces a version-1 encrypted envelope.
    ///
    /// - Generates a random 32-byte DEK via `getrandom`.
    /// - Wraps the DEK with the KEK using AES-256-GCM (random 12-byte nonce).
    /// - Encrypts each section body with the DEK (independent random 12-byte nonce).
    /// - Embeds the KEK fingerprint so restore can detect the wrong KEK up front.
    pub fn finalize_encrypted(self, kek: &[u8; 32]) -> Result<Vec<u8>, EnvelopeError> {
        // Generate a fresh DEK for this backup.
        let dek: [u8; 32] = random_bytes()?;

        // Wrap the DEK with the KEK.
        let dek_nonce: [u8; 12] = random_bytes()?;
        let wrapped_dek = aes_encrypt(kek, &dek_nonce, &dek)?;
        // wrapped_dek should be 48 bytes: 32 ciphertext + 16 tag.
        debug_assert_eq!(wrapped_dek.len(), 48);

        let fingerprint = kek_fingerprint(kek);

        // Pre-encrypt all section bodies so we know the final size.
        let mut enc_sections: Vec<(u64, [u8; 12], Vec<u8>)> =
            Vec::with_capacity(self.sections.len());
        for section in &self.sections {
            let nonce: [u8; 12] = random_bytes()?;
            let ciphertext = aes_encrypt(&dek, &nonce, &section.body)?;
            enc_sections.push((section.origin_node_id, nonce, ciphertext));
        }

        // Compute total size.
        let mut total_size = HEADER_LEN + CRYPTO_BLOCK_LEN + TRAILER_LEN;
        for (_, _, ct) in &enc_sections {
            total_size += ENC_SECTION_OVERHEAD + ct.len();
        }

        let mut out = Vec::with_capacity(total_size);

        // Header.
        write_header(&mut out, &self.meta, self.sections.len() as u16, VERSION);

        // Crypto block.
        out.extend_from_slice(&fingerprint);
        out.extend_from_slice(&dek_nonce);
        out.extend_from_slice(&wrapped_dek);

        // Encrypted sections.
        for (origin_node_id, nonce, ciphertext) in &enc_sections {
            out.extend_from_slice(&origin_node_id.to_le_bytes());
            out.extend_from_slice(&(ciphertext.len() as u32).to_le_bytes());
            out.extend_from_slice(nonce);
            out.extend_from_slice(ciphertext);
            let body_crc = crc32c::crc32c(ciphertext);
            out.extend_from_slice(&body_crc.to_le_bytes());
        }

        // Trailer CRC over everything emitted so far.
        let trailer_crc = crc32c::crc32c(&out);
        out.extend_from_slice(&trailer_crc.to_le_bytes());

        Ok(out)
    }
}

// ── Decryption ────────────────────────────────────────────────────────────────

/// Parse and decrypt an encrypted backup envelope (version 1 with crypto block).
///
/// Verifies the KEK fingerprint before attempting decryption, surfacing
/// [`EnvelopeError::WrongBackupKek`] when the presented key does not match
/// the one used at backup time. On tag mismatch surfaces
/// [`EnvelopeError::DecryptionFailed`].
pub fn parse_encrypted(
    bytes: &[u8],
    max_total: u64,
    kek: &[u8; 32],
) -> Result<Envelope, EnvelopeError> {
    if bytes.len() as u64 > max_total {
        return Err(EnvelopeError::OverSizeTotal { cap: max_total });
    }

    let min_len = HEADER_LEN + CRYPTO_BLOCK_LEN + TRAILER_LEN;
    if bytes.len() < min_len {
        return Err(EnvelopeError::Truncated);
    }

    // Header checks.
    let header_bytes = &bytes[..HEADER_LEN];
    if &header_bytes[0..4] != MAGIC {
        return Err(EnvelopeError::BadMagic);
    }
    let version = header_bytes[4];
    if version != VERSION {
        return Err(EnvelopeError::UnsupportedVersion(version));
    }

    // Validate header CRC.
    let claimed_header_crc = u32::from_le_bytes(read4(&header_bytes[48..52]));
    let actual_header_crc = crc32c::crc32c(&header_bytes[..48]);
    if claimed_header_crc != actual_header_crc {
        return Err(EnvelopeError::HeaderCrcMismatch);
    }

    let meta = EnvelopeMeta {
        tenant_id: u64::from_le_bytes(read8(&header_bytes[8..16])),
        source_vshard_count: u16::from_le_bytes(read2(&header_bytes[16..18])),
        hash_seed: u64::from_le_bytes(read8(&header_bytes[24..32])),
        snapshot_watermark: u64::from_le_bytes(read8(&header_bytes[32..40])),
    };
    let section_count = u16::from_le_bytes(read2(&header_bytes[40..42]));

    // Crypto block immediately after header.
    let cb_start = HEADER_LEN;
    let cb = &bytes[cb_start..cb_start + CRYPTO_BLOCK_LEN];
    let stored_fingerprint: [u8; 8] = cb[0..8].try_into().expect("slice is 8 bytes");
    let dek_nonce: [u8; 12] = cb[8..20].try_into().expect("slice is 12 bytes");
    let wrapped_dek: &[u8] = &cb[20..68]; // 48 bytes

    // Verify fingerprint before any crypto work.
    let presented_fingerprint = kek_fingerprint(kek);
    if presented_fingerprint != stored_fingerprint {
        return Err(EnvelopeError::WrongBackupKek);
    }

    // Unwrap the DEK.
    let dek_vec = aes_decrypt(kek, &dek_nonce, wrapped_dek)?;
    if dek_vec.len() != 32 {
        return Err(EnvelopeError::DecryptionFailed);
    }
    let mut dek = [0u8; 32];
    dek.copy_from_slice(&dek_vec);

    // Trailer.
    let trailer_start = bytes.len() - TRAILER_LEN;
    let claimed_trailer_crc = u32::from_le_bytes(read4(&bytes[trailer_start..]));
    let actual_trailer_crc = crc32c::crc32c(&bytes[..trailer_start]);
    if claimed_trailer_crc != actual_trailer_crc {
        return Err(EnvelopeError::TrailerCrcMismatch);
    }

    // Parse and decrypt sections.
    let mut cursor = HEADER_LEN + CRYPTO_BLOCK_LEN;
    // Ciphertext includes the mandatory AES-GCM authentication tag, so every
    // encrypted section consumes its fixed framing plus at least 16 bytes.
    let section_capacity = super::read::checked_section_capacity(
        section_count,
        trailer_start.saturating_sub(cursor),
        ENC_SECTION_OVERHEAD + 16,
    )?;
    let mut sections = Vec::with_capacity(section_capacity);

    for _ in 0..section_count {
        // Each encrypted section: origin(8) + body_len(4) + nonce(12) + ciphertext(body_len) + crc(4)
        if cursor + ENC_SECTION_OVERHEAD > trailer_start {
            return Err(EnvelopeError::Truncated);
        }
        let origin_node_id = u64::from_le_bytes(read8(&bytes[cursor..cursor + 8]));
        let ct_len = u32::from_le_bytes(read4(&bytes[cursor + 8..cursor + 12])) as usize;
        let nonce_start = cursor + 12;
        let nonce_end = nonce_start + 12;
        let ct_start = nonce_end;
        let ct_end = ct_start + ct_len;
        let crc_end = ct_end + 4;

        if crc_end > trailer_start {
            return Err(EnvelopeError::Truncated);
        }

        // Verify ciphertext CRC (wire error detection, not authentication).
        let ciphertext = &bytes[ct_start..ct_end];
        let claimed_body_crc = u32::from_le_bytes(read4(&bytes[ct_end..crc_end]));
        if crc32c::crc32c(ciphertext) != claimed_body_crc {
            return Err(EnvelopeError::BodyCrcMismatch);
        }

        let section_nonce: [u8; 12] = bytes[nonce_start..nonce_end]
            .try_into()
            .expect("slice is 12 bytes");

        // Decrypt — this verifies the AES-GCM authentication tag.
        let plaintext = aes_decrypt(&dek, &section_nonce, ciphertext)?;

        sections.push(Section {
            origin_node_id,
            body: plaintext,
        });
        cursor = crc_end;
    }

    if cursor != trailer_start {
        return Err(EnvelopeError::Truncated);
    }

    Ok(Envelope { meta, sections })
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_envelope::types::{DEFAULT_MAX_TOTAL_BYTES, EnvelopeMeta};
    use crate::backup_envelope::write::EnvelopeWriter;

    fn meta() -> EnvelopeMeta {
        EnvelopeMeta {
            tenant_id: 77,
            source_vshard_count: 256,
            hash_seed: 0xCAFE,
            snapshot_watermark: 999,
        }
    }

    fn test_kek() -> [u8; 32] {
        [0xA1u8; 32]
    }

    fn test_kek2() -> [u8; 32] {
        [0xB2u8; 32]
    }

    fn make_writer_with_sections() -> EnvelopeWriter {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"alpha payload".to_vec()).unwrap();
        w.push_section(2, b"beta data chunk".to_vec()).unwrap();
        w.push_section(3, vec![]).unwrap();
        w
    }

    #[test]
    fn encrypted_parse_rejects_huge_section_count_with_tiny_body_before_allocation() {
        let kek = test_kek();
        let mut bytes = EnvelopeWriter::new(meta())
            .finalize_encrypted(&kek)
            .unwrap();
        bytes[40..42].copy_from_slice(&u16::MAX.to_le_bytes());
        let header_crc = crc32c::crc32c(&bytes[..48]);
        bytes[48..52].copy_from_slice(&header_crc.to_le_bytes());
        let trailer_start = bytes.len() - TRAILER_LEN;
        let trailer_crc = crc32c::crc32c(&bytes[..trailer_start]);
        bytes[trailer_start..].copy_from_slice(&trailer_crc.to_le_bytes());
        assert_eq!(
            parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &kek),
            Err(EnvelopeError::Truncated)
        );
    }

    #[test]
    fn encrypted_roundtrips_with_correct_kek() {
        let kek = test_kek();
        let w = make_writer_with_sections();
        let bytes = w.finalize_encrypted(&kek).unwrap();

        let env = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &kek).unwrap();
        assert_eq!(env.meta, meta());
        assert_eq!(env.sections.len(), 3);
        assert_eq!(env.sections[0].origin_node_id, 1);
        assert_eq!(env.sections[0].body, b"alpha payload");
        assert_eq!(env.sections[1].origin_node_id, 2);
        assert_eq!(env.sections[1].body, b"beta data chunk");
        assert_eq!(env.sections[2].body, b"");
    }

    #[test]
    fn wrong_kek_returns_wrong_backup_kek_error() {
        let kek = test_kek();
        let wrong_kek = test_kek2();

        let w = make_writer_with_sections();
        let bytes = w.finalize_encrypted(&kek).unwrap();

        assert_eq!(
            parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &wrong_kek).unwrap_err(),
            EnvelopeError::WrongBackupKek,
        );
    }

    #[test]
    fn tampering_with_section_body_fails_auth_tag() {
        let kek = test_kek();
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"secret data".to_vec()).unwrap();
        let mut bytes = w.finalize_encrypted(&kek).unwrap();

        // Locate the first section's ciphertext start:
        // HEADER(52) + CRYPTO_BLOCK(68) + origin(8) + body_len(4) + nonce(12) = 144
        let ct_start = HEADER_LEN + CRYPTO_BLOCK_LEN + 8 + 4 + 12;
        bytes[ct_start] ^= 0xFF; // flip one byte

        // Also fix the ciphertext CRC so it doesn't fail on wire check before crypto.
        let ct_len_start = HEADER_LEN + CRYPTO_BLOCK_LEN + 8;
        let ct_len =
            u32::from_le_bytes(bytes[ct_len_start..ct_len_start + 4].try_into().unwrap()) as usize;
        let crc_start = ct_start + ct_len;
        let new_crc = crc32c::crc32c(&bytes[ct_start..crc_start]);
        bytes[crc_start..crc_start + 4].copy_from_slice(&new_crc.to_le_bytes());

        // Also fix the trailer CRC.
        let trailer_start = bytes.len() - TRAILER_LEN;
        let new_trailer = crc32c::crc32c(&bytes[..trailer_start]);
        bytes[trailer_start..].copy_from_slice(&new_trailer.to_le_bytes());

        let err = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &kek).unwrap_err();
        assert_eq!(err, EnvelopeError::DecryptionFailed);
    }

    #[test]
    fn tampering_with_wrapped_dek_fails_decryption() {
        let kek = test_kek();
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"data".to_vec()).unwrap();
        let mut bytes = w.finalize_encrypted(&kek).unwrap();

        // wrapped_dek starts at HEADER(52) + fingerprint(8) + dek_nonce(12) = 72
        let wd_start = HEADER_LEN + 8 + 12;
        bytes[wd_start] ^= 0xFF;

        // Fix trailer CRC so decryption is attempted.
        let trailer_start = bytes.len() - TRAILER_LEN;
        let new_trailer = crc32c::crc32c(&bytes[..trailer_start]);
        bytes[trailer_start..].copy_from_slice(&new_trailer.to_le_bytes());

        let err = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &kek).unwrap_err();
        assert_eq!(err, EnvelopeError::DecryptionFailed);
    }

    #[test]
    fn backup_kek_and_wal_kek_are_independent() {
        // Write two different key files and verify that `FileKeyProvider`-like
        // usage returns distinct keys. Here we simulate the independence by
        // showing two different byte arrays produce different fingerprints —
        // which is what matters for the separate-KEK requirement.
        let wal_kek = [0x11u8; 32];
        let backup_kek = [0x22u8; 32];

        assert_ne!(
            kek_fingerprint(&wal_kek),
            kek_fingerprint(&backup_kek),
            "wal kek and backup kek must have different fingerprints"
        );

        // A backup encrypted with backup_kek cannot be opened with wal_kek.
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"payload".to_vec()).unwrap();
        let bytes = w.finalize_encrypted(&backup_kek).unwrap();

        assert_eq!(
            parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &wal_kek).unwrap_err(),
            EnvelopeError::WrongBackupKek,
        );

        // And succeeds with backup_kek.
        let env = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &backup_kek).unwrap();
        assert_eq!(env.sections[0].body, b"payload");
    }

    #[test]
    fn empty_envelope_encrypted_roundtrips() {
        let kek = test_kek();
        let bytes = EnvelopeWriter::new(meta())
            .finalize_encrypted(&kek)
            .unwrap();
        let env = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &kek).unwrap();
        assert_eq!(env.meta, meta());
        assert!(env.sections.is_empty());
    }
}
