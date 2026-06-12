// SPDX-License-Identifier: Apache-2.0

//! WAL payload encryption using AES-256-GCM.
//!
//! Design:
//! - Header stays plaintext (needed for recovery scanning — magic, lsn, tenant_id)
//! - Payload is encrypted before CRC computation
//! - CRC covers the ciphertext (detects corruption of encrypted data)
//! - Nonce = `[4-byte random epoch][8-byte LSN]` — epoch is generated per WAL
//!   lifetime to prevent nonce reuse after snapshot restore or WAL truncation
//! - Additional Authenticated Data (AAD) = header bytes (binds ciphertext to its header)
//!
//! On-disk format for encrypted payload:
//! ```text
//! [header(30B plaintext)] [ciphertext(payload_len bytes)] [auth_tag(16B)]
//! ```
//! `payload_len` includes the 16-byte auth tag.

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit};

use crate::error::{Result, WalError};
use crate::record::HEADER_SIZE;
use crate::secure_mem;

/// Security gate for WAL key files: enforces no-symlink, regular-file,
/// Unix mode bits (no group/world), and owner-UID checks.
///
/// Mirrors the logic in `nodedb::control::security::keystore::key_file_security`
/// but is duplicated here so that `nodedb-wal` has zero dependency on the
/// `nodedb` application crate.
fn check_key_file_wal(path: &std::path::Path) -> Result<()> {
    let symlink_meta = std::fs::symlink_metadata(path).map_err(|e| WalError::EncryptionError {
        detail: format!("cannot stat WAL key file {}: {e}", path.display()),
    })?;

    if symlink_meta.file_type().is_symlink() {
        return Err(WalError::EncryptionError {
            detail: format!(
                "WAL key file {} is a symlink, which is not permitted \
                 (path traversal / TOCTOU risk)",
                path.display()
            ),
        });
    }

    if !symlink_meta.is_file() {
        return Err(WalError::EncryptionError {
            detail: format!("WAL key file {} is not a regular file", path.display()),
        });
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let mode = symlink_meta.mode();
        if mode & 0o077 != 0 {
            return Err(WalError::EncryptionError {
                detail: format!(
                    "WAL key file {} has insecure permissions: 0o{:03o} \
                     (must be 0o400 or 0o600 — no group or world access)",
                    path.display(),
                    mode & 0o777,
                ),
            });
        }

        let file_uid = symlink_meta.uid();
        // SAFETY: geteuid() is always safe to call; it has no preconditions.
        let process_uid = unsafe { libc::geteuid() };
        if file_uid != process_uid {
            return Err(WalError::EncryptionError {
                detail: format!(
                    "WAL key file {} is owned by UID {} but process runs as UID {} \
                     — key files must be owned by the server process user",
                    path.display(),
                    file_uid,
                    process_uid,
                ),
            });
        }
    }

    // On Windows: ACL-based permission enforcement is not implemented.
    // The symlink check above still applies on all platforms.

    Ok(())
}

/// AES-256-GCM key with a random per-lifetime epoch for nonce disambiguation.
///
/// The epoch is generated randomly at construction time. Each WAL lifetime
/// (process start, snapshot restore, segment creation) gets a fresh epoch,
/// ensuring that nonces are never reused even if LSNs restart from 1.
#[derive(Clone)]
pub struct WalEncryptionKey {
    cipher: Aes256Gcm,
    /// Raw key bytes (kept for producing new instances with a fresh epoch).
    key_bytes: [u8; 32],
    /// Random 4-byte epoch: occupies the high 4 bytes of the 12-byte nonce.
    /// Disambiguates nonces across WAL lifetimes with the same key.
    epoch: [u8; 4],
}

impl WalEncryptionKey {
    /// Create from a 32-byte key with a fresh random epoch.
    ///
    /// Returns an error if the OS RNG (`getrandom`) is unavailable. Without
    /// a fresh epoch we cannot guarantee nonce uniqueness across WAL
    /// lifetimes, so panicking would silently risk nonce reuse on RNG
    /// failure — better to surface it.
    pub fn from_bytes(key: &[u8; 32]) -> Result<Self> {
        let mut epoch = [0u8; 4];
        getrandom::fill(&mut epoch).map_err(|e| WalError::EncryptionError {
            detail: format!("getrandom failed while generating epoch: {e}"),
        })?;
        // mlock key_bytes so they are not swapped to disk. Best-effort: if the
        // OS refuses (e.g. RLIMIT_MEMLOCK exceeded in a container) we log and
        // continue rather than aborting startup.
        let mut key_bytes = *key;
        secure_mem::mlock_key_bytes(key_bytes.as_mut_ptr(), 32);
        Ok(Self {
            cipher: Aes256Gcm::new(key.into()),
            key_bytes,
            epoch,
        })
    }

    /// Create from a 32-byte key with a **caller-supplied epoch**.
    ///
    /// Use this when reopening a WAL segment whose epoch was read from the
    /// on-disk preamble, so that the nonce is reconstructed identically to
    /// the nonce used at encryption time.
    pub fn with_epoch(key: &[u8; 32], epoch: [u8; 4]) -> Self {
        Self {
            cipher: Aes256Gcm::new(key.into()),
            key_bytes: *key,
            epoch,
        }
    }

    /// Produce a new key instance with the same key material but a fresh
    /// random epoch. Used when rolling to a new WAL segment — each segment
    /// gets its own epoch so the per-segment nonce space is independent.
    pub fn with_fresh_epoch(&self) -> Result<Self> {
        Self::from_bytes(&self.key_bytes)
    }

    /// Load key from a file (must contain exactly 32 bytes).
    ///
    /// Before reading, the file is checked for:
    /// - No symlinks (TOCTOU / path-traversal risk).
    /// - Regular file (not a device, FIFO, or directory).
    /// - Unix: no group or world access bits (`mode & 0o077 == 0`).
    /// - Unix: file owner matches the current process UID.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        check_key_file_wal(path)?;
        let key_bytes = std::fs::read(path).map_err(WalError::Io)?;
        if key_bytes.len() != 32 {
            return Err(WalError::EncryptionError {
                detail: format!(
                    "encryption key must be exactly 32 bytes, got {}",
                    key_bytes.len()
                ),
            });
        }
        let mut key_arr = zeroize::Zeroizing::new([0u8; 32]);
        key_arr.copy_from_slice(&key_bytes);
        Self::from_bytes(&key_arr)
    }

    /// Encrypt a payload. Returns ciphertext + auth_tag (16 bytes appended).
    ///
    /// - `lsn`: used to derive a deterministic nonce
    /// - `header_bytes`: used as AAD (additional authenticated data)
    /// - `plaintext`: the payload to encrypt
    pub fn encrypt(
        &self,
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        self.encrypt_aad(lsn, header_bytes, plaintext)
    }

    /// Encrypt with a caller-provided AAD slice (may be longer than HEADER_SIZE,
    /// e.g. preamble bytes prepended to the header).
    pub fn encrypt_aad(&self, lsn: u64, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let nonce = lsn_to_nonce(&self.epoch, lsn);
        self.cipher
            .encrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| WalError::EncryptionError {
                detail: "AES-256-GCM encryption failed".into(),
            })
    }

    /// The random epoch for this key instance.
    pub fn epoch(&self) -> &[u8; 4] {
        &self.epoch
    }

    /// Decrypt a payload. Input is ciphertext + auth_tag (16 bytes at end).
    ///
    /// - `epoch`: must be the epoch that was used during encryption, read from
    ///   the on-disk segment preamble — **not** `self.epoch`, which reflects
    ///   the current in-memory lifetime and may differ after a restart.
    /// - `lsn`: must match the LSN used during encryption
    /// - `header_bytes`: must match the header used during encryption (AAD)
    /// - `ciphertext`: the encrypted payload (includes 16-byte auth tag)
    pub fn decrypt(
        &self,
        epoch: &[u8; 4],
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        self.decrypt_aad(epoch, lsn, header_bytes, ciphertext)
    }

    /// Decrypt with a caller-provided AAD slice.
    pub fn decrypt_aad(
        &self,
        epoch: &[u8; 4],
        lsn: u64,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        let nonce = lsn_to_nonce(epoch, lsn);
        self.cipher
            .decrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| WalError::EncryptionError {
                detail: "AES-256-GCM decryption failed (corrupted or wrong key)".into(),
            })
    }
}

/// Key ring supporting dual-key reads for seamless key rotation.
///
/// During rotation: new writes use the current key, reads try current
/// then fall back to previous. Once all old data is re-encrypted,
/// the previous key is removed.
#[derive(Clone)]
pub struct KeyRing {
    current: WalEncryptionKey,
    previous: Option<WalEncryptionKey>,
}

impl KeyRing {
    /// Create a key ring with only the current key.
    pub fn new(current: WalEncryptionKey) -> Self {
        Self {
            current,
            previous: None,
        }
    }

    /// Create a key ring with current + previous key (for rotation).
    pub fn with_previous(current: WalEncryptionKey, previous: WalEncryptionKey) -> Self {
        Self {
            current,
            previous: Some(previous),
        }
    }

    /// Encrypt using the current key.
    pub fn encrypt(
        &self,
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        self.current.encrypt(lsn, header_bytes, plaintext)
    }

    /// Encrypt with a caller-provided AAD slice.
    pub fn encrypt_aad(&self, lsn: u64, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        self.current.encrypt_aad(lsn, aad, plaintext)
    }

    /// Decrypt: try current key first, then previous (if set).
    ///
    /// `epoch` is the encryption epoch stored in the WAL segment preamble.
    /// This enables seamless key rotation — old data encrypted with the
    /// previous key can still be read while new data uses the current key.
    pub fn decrypt(
        &self,
        epoch: &[u8; 4],
        lsn: u64,
        header_bytes: &[u8; HEADER_SIZE],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        self.decrypt_aad(epoch, lsn, header_bytes, ciphertext)
    }

    /// Decrypt with a caller-provided AAD slice.
    pub fn decrypt_aad(
        &self,
        epoch: &[u8; 4],
        lsn: u64,
        aad: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        match (
            self.current.decrypt_aad(epoch, lsn, aad, ciphertext),
            self.previous.as_ref(),
        ) {
            (Ok(plaintext), _) => Ok(plaintext),
            (Err(_), Some(prev)) => prev.decrypt_aad(epoch, lsn, aad, ciphertext),
            (Err(e), None) => Err(e),
        }
    }

    /// Get the current key (for encryption operations).
    pub fn current(&self) -> &WalEncryptionKey {
        &self.current
    }

    /// Whether a previous key is present (rotation in progress).
    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }

    /// Remove the previous key (rotation complete).
    pub fn clear_previous(&mut self) {
        self.previous = None;
    }
}

/// AES-256-GCM auth tag size in bytes.
pub const AUTH_TAG_SIZE: usize = 16;

// ── Segment envelope ───────────────────────────────────────────────────────
//
// Shared at-rest framing reused by every engine (array, columnar, vector,
// spatial, timeseries) so the AES-256-GCM call site lives in one place.
//
// Layout:
// ```text
// [magic (4B)] [version_u16_le (2B)] [cipher_u8 (1B)] [kid_u8 (1B)]
// [epoch (4B)] [reserved (4B)] [AES-256-GCM ciphertext (plaintext + 16B tag)]
// ```
//
// The 16-byte preamble is supplied as AAD, preventing preamble-swap attacks.
// The nonce is derived from `(epoch, lsn = 0)`; per-write random epoch
// guarantees nonce uniqueness without needing a monotonic counter.

/// Size of the segment envelope preamble in bytes.
pub const SEGMENT_ENVELOPE_PREAMBLE_SIZE: usize = 16;

/// Minimum size of a valid encrypted envelope: preamble + AES-GCM auth tag.
pub const SEGMENT_ENVELOPE_MIN_SIZE: usize = SEGMENT_ENVELOPE_PREAMBLE_SIZE + AUTH_TAG_SIZE;

/// Current segment-envelope preamble layout version.
const SEGMENT_ENVELOPE_VERSION: u16 = 1;

/// Cipher algorithm tag stored in the preamble: 0 = AES-256-GCM.
const SEGMENT_ENVELOPE_CIPHER_AES_256_GCM: u8 = 0;

/// Fixed LSN input for envelope nonces. Per-write random epoch (stored in
/// the preamble) is what actually disambiguates nonces.
const SEGMENT_ENVELOPE_NONCE_LSN: u64 = 0;

fn encode_envelope_preamble(
    magic: &[u8; 4],
    epoch: &[u8; 4],
) -> [u8; SEGMENT_ENVELOPE_PREAMBLE_SIZE] {
    let mut buf = [0u8; SEGMENT_ENVELOPE_PREAMBLE_SIZE];
    buf[0..4].copy_from_slice(magic);
    buf[4..6].copy_from_slice(&SEGMENT_ENVELOPE_VERSION.to_le_bytes());
    buf[6] = SEGMENT_ENVELOPE_CIPHER_AES_256_GCM;
    buf[7] = 0; // kid = 0 (current KEK)
    buf[8..12].copy_from_slice(epoch);
    // buf[12..16] = reserved zeros
    buf
}

/// Encrypt `plaintext` into a self-describing segment envelope.
///
/// Returns `preamble || AES-256-GCM(plaintext) || auth_tag`. The caller
/// supplies the 4-byte `magic` that identifies its envelope variant
/// (`SEGA`, `SEGC`, `SEGT`, `SEGV`, …).
pub fn encrypt_segment_envelope(
    key: &WalEncryptionKey,
    magic: &[u8; 4],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let fresh_key = key.with_fresh_epoch()?;
    let epoch = *fresh_key.epoch();
    let preamble = encode_envelope_preamble(magic, &epoch);
    let ciphertext = fresh_key.encrypt_aad(SEGMENT_ENVELOPE_NONCE_LSN, &preamble, plaintext)?;
    let mut out = Vec::with_capacity(SEGMENT_ENVELOPE_PREAMBLE_SIZE + ciphertext.len());
    out.extend_from_slice(&preamble);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Decrypt a segment envelope produced by [`encrypt_segment_envelope`].
///
/// The caller supplies the expected `magic`; mismatches surface as
/// [`WalError::EncryptionError`].
pub fn decrypt_segment_envelope(
    key: &WalEncryptionKey,
    magic: &[u8; 4],
    blob: &[u8],
) -> Result<Vec<u8>> {
    if blob.len() < SEGMENT_ENVELOPE_MIN_SIZE {
        return Err(WalError::EncryptionError {
            detail: "encrypted envelope too short".into(),
        });
    }
    let preamble: [u8; SEGMENT_ENVELOPE_PREAMBLE_SIZE] = blob[..SEGMENT_ENVELOPE_PREAMBLE_SIZE]
        .try_into()
        .expect("slice is preamble size");
    if &preamble[0..4] != magic {
        return Err(WalError::EncryptionError {
            detail: "envelope preamble magic mismatch".into(),
        });
    }
    let version = u16::from_le_bytes([preamble[4], preamble[5]]);
    if version != SEGMENT_ENVELOPE_VERSION {
        return Err(WalError::EncryptionError {
            detail: format!("unsupported envelope preamble version {version}"),
        });
    }
    let mut epoch = [0u8; 4];
    epoch.copy_from_slice(&preamble[8..12]);
    let ciphertext = &blob[SEGMENT_ENVELOPE_PREAMBLE_SIZE..];
    key.decrypt_aad(&epoch, SEGMENT_ENVELOPE_NONCE_LSN, &preamble, ciphertext)
}

/// Derive a 12-byte nonce from an epoch and LSN.
///
/// AES-256-GCM requires a 96-bit (12 byte) nonce that must never repeat
/// for the same key. Layout: `[4-byte random epoch][8-byte LSN]`.
/// The epoch is generated randomly per WAL lifetime, so even if LSNs
/// restart from 1 after a snapshot restore, the nonces remain unique.
fn lsn_to_nonce(epoch: &[u8; 4], lsn: u64) -> aes_gcm::Nonce<aes_gcm::aead::consts::U12> {
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[..4].copy_from_slice(epoch);
    nonce_bytes[4..12].copy_from_slice(&lsn.to_le_bytes());
    nonce_bytes.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    fn test_header(lsn: u64) -> [u8; HEADER_SIZE] {
        let mut h = [0u8; HEADER_SIZE];
        h[8..16].copy_from_slice(&lsn.to_le_bytes());
        h
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);
        let plaintext = b"hello nodedb encryption";

        let ciphertext = key.encrypt(1, &header, plaintext).unwrap();
        assert_ne!(&ciphertext[..plaintext.len()], plaintext);
        assert_eq!(ciphertext.len(), plaintext.len() + AUTH_TAG_SIZE);

        let decrypted = key.decrypt(&epoch, 1, &header, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = WalEncryptionKey::from_bytes(&[0x01; 32]).unwrap();
        let epoch1 = *key1.epoch();
        let key2 = WalEncryptionKey::from_bytes(&[0x02; 32]).unwrap();
        let header = test_header(1);

        let ciphertext = key1.encrypt(1, &header, b"secret").unwrap();
        assert!(key2.decrypt(&epoch1, 1, &header, &ciphertext).is_err());
    }

    #[test]
    fn wrong_lsn_fails() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);

        let ciphertext = key.encrypt(1, &header, b"secret").unwrap();
        // Different LSN = different nonce = decryption fails.
        assert!(key.decrypt(&epoch, 2, &header, &ciphertext).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);

        let mut ciphertext = key.encrypt(1, &header, b"secret").unwrap();
        ciphertext[0] ^= 0xFF;
        assert!(key.decrypt(&epoch, 1, &header, &ciphertext).is_err());
    }

    #[test]
    fn tampered_header_fails() {
        let key = test_key();
        let epoch = *key.epoch();
        let header1 = test_header(1);

        let ciphertext = key.encrypt(1, &header1, b"secret").unwrap();

        // Tamper the AAD (header).
        let mut header2 = header1;
        header2[0] = 0xFF;
        assert!(key.decrypt(&epoch, 1, &header2, &ciphertext).is_err());
    }

    #[test]
    fn empty_payload() {
        let key = test_key();
        let epoch = *key.epoch();
        let header = test_header(1);

        let ciphertext = key.encrypt(1, &header, b"").unwrap();
        assert_eq!(ciphertext.len(), AUTH_TAG_SIZE); // Just the tag.

        let decrypted = key.decrypt(&epoch, 1, &header, &ciphertext).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn different_lsns_produce_different_ciphertext() {
        let key = test_key();
        let plaintext = b"same payload";

        let ct1 = key.encrypt(1, &test_header(1), plaintext).unwrap();
        let ct2 = key.encrypt(2, &test_header(2), plaintext).unwrap();
        assert_ne!(ct1, ct2);
    }

    #[test]
    #[cfg(unix)]
    fn from_file_0o600_accepted() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0x42u8; 32]).unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        WalEncryptionKey::from_file(&path).expect("0o600 key file must be accepted");
    }

    #[test]
    #[cfg(unix)]
    fn from_file_0o400_accepted() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0x42u8; 32]).unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();

        WalEncryptionKey::from_file(&path).expect("0o400 key file must be accepted");
    }

    #[test]
    #[cfg(unix)]
    fn from_file_0o644_rejected() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0x42u8; 32]).unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let err = match WalEncryptionKey::from_file(&path) {
            Ok(_) => panic!("expected insecure-permissions error, got Ok"),
            Err(e) => e,
        };
        let detail = format!("{err:?}");
        assert!(
            detail.contains("insecure") || detail.contains("644"),
            "expected insecure-permissions error, got: {detail}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn from_file_symlink_rejected() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.bin");
        let mut f = std::fs::File::create(&target).unwrap();
        f.write_all(&[0x42u8; 32]).unwrap();
        drop(f);
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();

        let link = dir.path().join("link.bin");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = match WalEncryptionKey::from_file(&link) {
            Ok(_) => panic!("expected symlink rejection, got Ok"),
            Err(e) => e,
        };
        let detail = format!("{err:?}");
        assert!(
            detail.contains("symlink"),
            "expected symlink rejection, got: {detail}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn same_lsn_different_wal_lifetimes_produce_different_ciphertext() {
        // Simulate two WAL lifetimes: same key bytes, same LSN=1, but
        // separate WalEncryptionKey instances (each gets a fresh random epoch).
        // This models: write at LSN=1, wipe WAL, restart with same key,
        // write at LSN=1 again. The two ciphertexts must differ.
        let key_bytes = [0x42u8; 32];
        let key1 = WalEncryptionKey::from_bytes(&key_bytes).unwrap();
        let key2 = WalEncryptionKey::from_bytes(&key_bytes).unwrap();
        let header = test_header(1);
        let pt = b"same plaintext in two wal lifetimes";

        let ct1 = key1.encrypt(1, &header, pt).unwrap();
        let ct2 = key2.encrypt(1, &header, pt).unwrap();

        // SPEC: different WAL lifetimes (different epochs) must produce
        // different ciphertext even with the same key bytes and LSN.
        assert_ne!(
            ct1, ct2,
            "nonce reuse: same (key_bytes, lsn) must not produce identical ciphertext across WAL lifetimes"
        );
    }
}
