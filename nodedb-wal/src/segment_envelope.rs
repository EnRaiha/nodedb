// SPDX-License-Identifier: Apache-2.0

//! Authenticated envelopes shared by at-rest engine segments.
//!
//! # Format version is locked at 1 before 1.0
//!
//! There is no pre-1.0 compatibility window. The current layout is the only
//! supported layout, so changing it updates version 1 rather than adding a
//! legacy decoder or incrementing the version. After 1.0, format evolution must
//! introduce an explicit compatibility policy before this constant can change.
//!
//! Each envelope derives a fresh AES-256-GCM data key. HKDF uses a 128-bit
//! random salt and domain-separated metadata containing a distinct 96-bit
//! random nonce. Each derived key has a structural one-encryption budget, so
//! restart-safe nonce uniqueness does not depend on a persisted counter.

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit};
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::crypto::{AUTH_TAG_SIZE, WalEncryptionKey};
use crate::error::{Result, WalError};

const SEGMENT_ENVELOPE_VERSION: u16 = 1;
const CIPHER_AES_256_GCM: u8 = 0;
const CURRENT_KEY_ID: u8 = 0;
const SALT_SIZE: usize = 16;
const NONCE_SIZE: usize = 12;
const SEED_SIZE: usize = SALT_SIZE + NONCE_SIZE;
const SALT_START: usize = 8;
const NONCE_START: usize = SALT_START + SALT_SIZE;
const HKDF_DOMAIN: &[u8] = b"NodeDB segment envelope data key v1\0";

/// Maximum AES-GCM encryptions permitted under one derived segment data key.
///
/// The KEK is used only by HKDF. Every envelope derives a non-cloneable data
/// key and this budget is enforced before invoking AES-GCM.
pub const SEGMENT_ENVELOPE_DATA_KEY_ENCRYPTION_BUDGET: u8 = 1;

/// Size of the current segment-envelope preamble.
pub const SEGMENT_ENVELOPE_PREAMBLE_SIZE: usize = 36;

/// Minimum valid envelope: current preamble plus an authentication tag.
pub const SEGMENT_ENVELOPE_MIN_SIZE: usize = SEGMENT_ENVELOPE_PREAMBLE_SIZE + AUTH_TAG_SIZE;

/// Application ceiling below both 32-bit address-space and AES-GCM message limits.
pub const SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES: usize =
    u32::MAX as usize - SEGMENT_ENVELOPE_PREAMBLE_SIZE - AUTH_TAG_SIZE;

const _: () = assert!(SEGMENT_ENVELOPE_VERSION == 1);
const _: () = assert!(SEGMENT_ENVELOPE_DATA_KEY_ENCRYPTION_BUDGET == 1);

struct OneShotDataKey {
    cipher: Option<Aes256Gcm>,
    remaining: u8,
}

impl OneShotDataKey {
    fn new(key: &Zeroizing<[u8; 32]>) -> Self {
        Self {
            cipher: Some(Aes256Gcm::new((&**key).into())),
            remaining: SEGMENT_ENVELOPE_DATA_KEY_ENCRYPTION_BUDGET,
        }
    }

    fn encrypt(
        &mut self,
        nonce: &[u8; NONCE_SIZE],
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        if self.remaining == 0 {
            return Err(encryption_error(
                "segment data-key encryption budget exhausted",
            ));
        }
        let cipher = self
            .cipher
            .take()
            .ok_or_else(|| encryption_error("segment data-key encryption budget exhausted"))?;
        self.remaining -= 1;
        cipher
            .encrypt(
                &(*nonce).into(),
                aes_gcm::aead::Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| encryption_error("AES-256-GCM segment encryption failed"))
    }
}

/// Encrypt a segment using a full random seed and a one-use derived data key.
///
/// Every public call obtains a fresh 128-bit salt and 96-bit nonce from the OS
/// RNG. Those values are serialized in the authenticated preamble and feed the
/// HKDF derivation, so each envelope receives a distinct data-key derivation
/// context. The data key is intentionally internal and is never exposed.
pub fn encrypt_segment_envelope(
    key: &WalEncryptionKey,
    magic: &[u8; 4],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    encrypt_with_seed_source(key, magic, plaintext, |seed| {
        getrandom::fill(seed).map_err(|error| {
            encryption_error(format!(
                "getrandom failed while generating segment envelope seed: {error}"
            ))
        })
    })
}

fn encrypt_with_seed_source(
    key: &WalEncryptionKey,
    magic: &[u8; 4],
    plaintext: &[u8],
    fill: impl FnOnce(&mut [u8; SEED_SIZE]) -> Result<()>,
) -> Result<Vec<u8>> {
    validate_plaintext_len(plaintext.len())?;
    let mut seed = Zeroizing::new([0u8; SEED_SIZE]);
    fill(&mut seed)?;

    let mut preamble = [0u8; SEGMENT_ENVELOPE_PREAMBLE_SIZE];
    preamble[0..4].copy_from_slice(magic);
    preamble[4..6].copy_from_slice(&SEGMENT_ENVELOPE_VERSION.to_le_bytes());
    preamble[6] = CIPHER_AES_256_GCM;
    preamble[7] = CURRENT_KEY_ID;
    preamble[SALT_START..NONCE_START].copy_from_slice(&seed[..SALT_SIZE]);
    preamble[NONCE_START..].copy_from_slice(&seed[SALT_SIZE..]);

    let salt: &[u8; SALT_SIZE] = preamble[SALT_START..NONCE_START]
        .try_into()
        .map_err(|_| encryption_error("invalid segment salt layout"))?;
    let nonce: &[u8; NONCE_SIZE] = preamble[NONCE_START..]
        .try_into()
        .map_err(|_| encryption_error("invalid segment nonce layout"))?;
    let derived = derive_data_key(key, magic, salt, nonce)?;
    let mut one_shot = OneShotDataKey::new(&derived);
    let ciphertext = one_shot.encrypt(nonce, &preamble, plaintext)?;

    let capacity = SEGMENT_ENVELOPE_PREAMBLE_SIZE
        .checked_add(ciphertext.len())
        .ok_or_else(|| encryption_error("segment envelope length overflow"))?;
    let mut envelope = Vec::with_capacity(capacity);
    envelope.extend_from_slice(&preamble);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

/// Decrypt a current-format segment envelope.
pub fn decrypt_segment_envelope(
    key: &WalEncryptionKey,
    magic: &[u8; 4],
    blob: &[u8],
) -> Result<Vec<u8>> {
    validate_header(blob, magic)?;
    let preamble = blob
        .get(..SEGMENT_ENVELOPE_PREAMBLE_SIZE)
        .ok_or_else(|| encryption_error("truncated segment envelope preamble"))?;
    let salt: &[u8; SALT_SIZE] = preamble[SALT_START..NONCE_START]
        .try_into()
        .map_err(|_| encryption_error("invalid segment salt layout"))?;
    let nonce: &[u8; NONCE_SIZE] = preamble[NONCE_START..]
        .try_into()
        .map_err(|_| encryption_error("invalid segment nonce layout"))?;
    let derived = derive_data_key(key, magic, salt, nonce)?;
    let cipher = Aes256Gcm::new((&*derived).into());
    let ciphertext = blob
        .get(SEGMENT_ENVELOPE_PREAMBLE_SIZE..)
        .ok_or_else(|| encryption_error("truncated segment envelope ciphertext"))?;
    cipher
        .decrypt(
            &(*nonce).into(),
            aes_gcm::aead::Payload {
                msg: ciphertext,
                aad: preamble,
            },
        )
        .map_err(|_| encryption_error("AES-256-GCM segment decryption failed"))
}

fn validate_header(blob: &[u8], magic: &[u8; 4]) -> Result<()> {
    let ciphertext_len = blob
        .len()
        .checked_sub(SEGMENT_ENVELOPE_PREAMBLE_SIZE)
        .ok_or_else(|| encryption_error("encrypted envelope truncated"))?;
    validate_ciphertext_len(ciphertext_len)?;
    if blob.get(..4) != Some(magic.as_slice()) {
        return Err(encryption_error("envelope preamble magic mismatch"));
    }
    let version: [u8; 2] = blob
        .get(4..6)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| encryption_error("encrypted envelope missing version"))?;
    if u16::from_le_bytes(version) != SEGMENT_ENVELOPE_VERSION {
        return Err(encryption_error("unsupported segment envelope version"));
    }
    if blob.get(6).copied() != Some(CIPHER_AES_256_GCM) {
        return Err(encryption_error("unsupported segment envelope cipher"));
    }
    if blob.get(7).copied() != Some(CURRENT_KEY_ID) {
        return Err(encryption_error("unsupported segment envelope key id"));
    }
    Ok(())
}

fn validate_ciphertext_len(len: usize) -> Result<()> {
    let plaintext_len = len
        .checked_sub(AUTH_TAG_SIZE)
        .ok_or_else(|| encryption_error("encrypted envelope missing authentication tag"))?;
    validate_plaintext_len(plaintext_len)
}

fn validate_plaintext_len(len: usize) -> Result<()> {
    if len > SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES {
        return Err(encryption_error(
            "segment envelope plaintext exceeds encryption limit",
        ));
    }
    Ok(())
}

fn derive_data_key(
    key: &WalEncryptionKey,
    magic: &[u8; 4],
    salt: &[u8; SALT_SIZE],
    nonce: &[u8; NONCE_SIZE],
) -> Result<Zeroizing<[u8; 32]>> {
    const INFO_LEN: usize = HKDF_DOMAIN.len() + 2 + 1 + 1 + 4 + NONCE_SIZE;
    let mut info = [0u8; INFO_LEN];
    let mut cursor = 0;
    for component in [
        HKDF_DOMAIN,
        &SEGMENT_ENVELOPE_VERSION.to_le_bytes(),
        &[CIPHER_AES_256_GCM],
        &[CURRENT_KEY_ID],
        magic,
        nonce,
    ] {
        let end = cursor + component.len();
        info[cursor..end].copy_from_slice(component);
        cursor = end;
    }

    let hkdf = Hkdf::<Sha256>::new(Some(salt), key.key_bytes());
    let mut derived = Zeroizing::new([0u8; 32]);
    hkdf.expand(&info, &mut *derived)
        .map_err(|_| encryption_error("HKDF segment data-key derivation failed"))?;
    Ok(derived)
}

fn encryption_error(detail: impl Into<String>) -> WalError {
    WalError::EncryptionError {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAGIC: [u8; 4] = *b"SEGT";

    fn key(byte: u8) -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[byte; 32]).expect("test key")
    }

    fn deterministic_envelope(
        key: &WalEncryptionKey,
        magic: &[u8; 4],
        plaintext: &[u8],
        seed: [u8; SEED_SIZE],
    ) -> Result<Vec<u8>> {
        encrypt_with_seed_source(key, magic, plaintext, |output| {
            output.copy_from_slice(&seed);
            Ok(())
        })
    }

    #[test]
    fn current_format_round_trip_has_locked_version_and_serialized_nonce() {
        let key = key(0x42);
        let mut seed = [0u8; SEED_SIZE];
        for (index, byte) in seed.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let envelope = deterministic_envelope(&key, &MAGIC, b"secret", seed).expect("encrypt");
        assert_eq!(
            envelope.len(),
            SEGMENT_ENVELOPE_PREAMBLE_SIZE + 6 + AUTH_TAG_SIZE
        );
        assert_eq!(
            u16::from_le_bytes([envelope[4], envelope[5]]),
            SEGMENT_ENVELOPE_VERSION
        );
        assert_eq!(SEGMENT_ENVELOPE_VERSION, 1);
        assert_eq!(
            &envelope[NONCE_START..SEGMENT_ENVELOPE_PREAMBLE_SIZE],
            &seed[SALT_SIZE..]
        );
        assert_eq!(
            decrypt_segment_envelope(&key, &MAGIC, &envelope).expect("decrypt"),
            b"secret"
        );
    }

    #[test]
    fn old_epoch_layout_is_rejected_instead_of_preserved() {
        let key = key(0x42);
        let mut old = vec![0u8; 16 + AUTH_TAG_SIZE];
        old[0..4].copy_from_slice(&MAGIC);
        old[4..6].copy_from_slice(&SEGMENT_ENVELOPE_VERSION.to_le_bytes());
        assert!(decrypt_segment_envelope(&key, &MAGIC, &old).is_err());
    }

    #[test]
    fn derived_key_changes_with_salt_nonce_and_magic() {
        let key = key(0x42);
        let salt = [1u8; SALT_SIZE];
        let nonce = [2u8; NONCE_SIZE];
        let baseline = derive_data_key(&key, &MAGIC, &salt, &nonce).expect("derive");

        let mut other_salt = salt;
        other_salt[0] ^= 1;
        let mut other_nonce = nonce;
        other_nonce[0] ^= 1;
        assert_ne!(
            *baseline,
            *derive_data_key(&key, &MAGIC, &other_salt, &nonce).expect("salt")
        );
        assert_ne!(
            *baseline,
            *derive_data_key(&key, &MAGIC, &salt, &other_nonce).expect("nonce")
        );
        assert_ne!(
            *baseline,
            *derive_data_key(&key, b"SEGA", &salt, &nonce).expect("magic")
        );
    }

    #[test]
    fn data_key_budget_rejects_second_encryption_without_ciphertext() {
        let key = key(0x42);
        let derived =
            derive_data_key(&key, &MAGIC, &[1; SALT_SIZE], &[2; NONCE_SIZE]).expect("derive");
        let mut one_shot = OneShotDataKey::new(&derived);
        let nonce = [2; NONCE_SIZE];
        let first = one_shot
            .encrypt(&nonce, b"aad", b"one")
            .expect("the one permitted encryption succeeds");
        assert!(
            !first.is_empty(),
            "successful encryption returns ciphertext"
        );

        // A second call has no cipher to invoke: the budget check returns the
        // typed encryption error before AES-GCM can produce any ciphertext.
        let second = one_shot.encrypt(&nonce, b"aad", b"two");
        assert!(
            matches!(
                second,
                Err(WalError::EncryptionError { ref detail })
                    if detail == "segment data-key encryption budget exhausted"
            ),
            "second encryption must return the typed budget-exhausted error"
        );
    }

    #[test]
    fn public_envelopes_use_fresh_random_salt_and_nonce_per_call() {
        let key = key(0x42);
        let first = encrypt_segment_envelope(&key, &MAGIC, b"same plaintext")
            .expect("first public envelope encryption");
        let second = encrypt_segment_envelope(&key, &MAGIC, b"same plaintext")
            .expect("second public envelope encryption");

        // The serialized salt and nonce are the public, authenticated inputs
        // to HKDF; unequal seeds therefore mean different data-key derivation
        // contexts. Roundtrip and tamper rejection remain covered above by the
        // existing current-format and authenticated-region tests.
        assert_ne!(
            &first[SALT_START..SEGMENT_ENVELOPE_PREAMBLE_SIZE],
            &second[SALT_START..SEGMENT_ENVELOPE_PREAMBLE_SIZE],
            "each public envelope must receive a fresh random salt and nonce"
        );
    }

    #[test]
    fn wrong_key_and_every_authenticated_region_fail() {
        let encryption_key = key(0x42);
        let envelope = deterministic_envelope(&encryption_key, &MAGIC, b"secret", [3; SEED_SIZE])
            .expect("encrypt");
        assert!(decrypt_segment_envelope(&key(0x41), &MAGIC, &envelope).is_err());

        for index in [
            0,
            4,
            6,
            7,
            SALT_START,
            NONCE_START,
            SEGMENT_ENVELOPE_PREAMBLE_SIZE,
            envelope.len() - 1,
        ] {
            let mut tampered = envelope.clone();
            tampered[index] ^= 1;
            assert!(
                decrypt_segment_envelope(&encryption_key, &MAGIC, &tampered).is_err(),
                "tamper index {index}"
            );
        }
    }

    #[test]
    fn every_truncation_is_rejected_without_panic() {
        let key = key(0x42);
        let envelope =
            deterministic_envelope(&key, &MAGIC, b"current", [4; SEED_SIZE]).expect("encrypt");
        for length in 0..envelope.len() {
            assert!(decrypt_segment_envelope(&key, &MAGIC, &envelope[..length]).is_err());
        }
    }

    #[test]
    fn rng_failure_emits_no_envelope() {
        let key = key(0x42);
        let result = encrypt_with_seed_source(&key, &MAGIC, b"secret", |_| {
            Err(encryption_error("injected RNG failure"))
        });
        assert!(result.is_err());
    }

    #[test]
    fn plaintext_limit_is_enforced_before_encryption_or_decryption() {
        assert!(validate_plaintext_len(SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES).is_ok());
        assert!(
            validate_ciphertext_len(SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES + AUTH_TAG_SIZE).is_ok()
        );
        if let Some(oversized) = SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES.checked_add(1) {
            assert!(validate_plaintext_len(oversized).is_err());
            assert!(validate_ciphertext_len(oversized + AUTH_TAG_SIZE).is_err());
        }
    }
}
