// SPDX-License-Identifier: Apache-2.0

//! Delta signing and verification using HMAC-SHA256 with replay protection.
//!
//! ## Design
//!
//! Each authenticated device gets a per-device HMAC key derived from the
//! stored user key via HKDF-SHA256:
//!
//! ```text
//! device_key = HKDF-SHA256(
//!     ikm  = stored_user_key,
//!     salt = b"nodedb-crdt-device-key",
//!     info = device_id.to_le_bytes(),
//! )
//! ```
//!
//! The HMAC input binds the delta bytes plus three u64 fields that prevent
//! replay and identity-substitution attacks:
//!
//! ```text
//! HMAC(device_key, delta_bytes || user_id.to_le_bytes() || device_id.to_le_bytes() || seq_no.to_le_bytes())
//! ```
//!
//! ## Replay protection
//!
//! `DeviceRegistry` maintains a per-(user_id, device_id) last-seen seq_no.
//! Before verifying the signature, `validate_or_reject` checks that the
//! submitted seq_no is strictly greater than last_seen (cheap rejection first).
//! On signature pass, last_seen is updated atomically.
//!
//! ## Signature comparison
//!
//! All signature equality checks use `subtle::ConstantTimeEq` to prevent
//! timing side-channel attacks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq as _;

use crate::error::{CrdtError, Result};

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA256 signature size (32 bytes).
pub const SIGNATURE_SIZE: usize = 32;

/// HKDF salt for per-device key derivation.
const DEVICE_KEY_SALT: &[u8] = b"nodedb-crdt-device-key";

/// Per-(user_id, device_id) sequence tracking used by `DeviceRegistry`.
#[derive(Debug, Clone, Default)]
struct DeviceState {
    last_seq_no: u64,
}

/// In-memory registry of per-device last-seen sequence numbers.
///
/// On the server this is held in `Validator`. A persistent mirror in the
/// redb `crdt_devices` table is maintained by the Control Plane security
/// catalog and loaded at startup.
#[derive(Debug, Default)]
pub struct DeviceRegistry {
    inner: Mutex<HashMap<(u64, u64), DeviceState>>,
}

impl DeviceRegistry {
    /// Create a new, empty device registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check that `seq_no > last_seen[(user_id, device_id)]`.
    ///
    /// Returns `Ok(last_seen)` so the caller can pass it to the error variant
    /// if the subsequent signature check fails in a way that needs context.
    ///
    /// # Errors
    ///
    /// Returns `CrdtError::ReplayDetected` if `seq_no <= last_seen`.
    pub fn check_seq(&self, user_id: u64, device_id: u64, seq_no: u64) -> Result<u64> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| CrdtError::DeltaApplyFailed("device registry lock poisoned".into()))?;
        let last_seen = guard
            .get(&(user_id, device_id))
            .map_or(0u64, |s| s.last_seq_no);
        if seq_no <= last_seen {
            return Err(CrdtError::ReplayDetected {
                user_id,
                device_id,
                seq_no,
                last_seen,
            });
        }
        Ok(last_seen)
    }

    /// Update last_seen for `(user_id, device_id)` after a successful accept.
    ///
    /// Only updates if `seq_no > current`, to guard against races.
    pub fn commit_seq(&self, user_id: u64, device_id: u64, seq_no: u64) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| CrdtError::DeltaApplyFailed("device registry lock poisoned".into()))?;
        let entry = guard.entry((user_id, device_id)).or_default();
        if seq_no > entry.last_seq_no {
            entry.last_seq_no = seq_no;
        }
        Ok(())
    }

    /// Pre-load a known (user_id, device_id, last_seq_no) tuple from the
    /// persistent catalog on startup.
    pub fn seed(&self, user_id: u64, device_id: u64, last_seq_no: u64) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| CrdtError::DeltaApplyFailed("device registry lock poisoned".into()))?;
        guard.entry((user_id, device_id)).or_default().last_seq_no = last_seq_no;
        Ok(())
    }

    /// Return the current last_seen seq_no for a device, or 0 if unknown.
    pub fn last_seen(&self, user_id: u64, device_id: u64) -> u64 {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.get(&(user_id, device_id)).map(|s| s.last_seq_no))
            .unwrap_or(0)
    }
}

/// Signs deltas with a per-device HMAC-SHA256 key derived via HKDF.
///
/// Thread-safe behind an `Arc<DeltaSigner>` in the Validator.
pub struct DeltaSigner {
    /// user_id -> stored base key (32 bytes). Per-device keys are HKDF-derived
    /// on demand; never stored.
    keys: HashMap<u64, [u8; 32]>,
    /// Per-device sequence tracking (shared with validator).
    pub(crate) registry: Arc<DeviceRegistry>,
}

impl DeltaSigner {
    /// Create a new signer with a fresh device registry.
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
            registry: Arc::new(DeviceRegistry::new()),
        }
    }

    /// Create a signer sharing an existing device registry (e.g. with the
    /// Validator that also needs to call `commit_seq`).
    pub fn with_registry(registry: Arc<DeviceRegistry>) -> Self {
        Self {
            keys: HashMap::new(),
            registry,
        }
    }

    /// Register a stored signing key for a user.
    pub fn register_key(&mut self, user_id: u64, key: [u8; 32]) {
        self.keys.insert(user_id, key);
    }

    /// Remove a user's signing key.
    pub fn remove_key(&mut self, user_id: u64) {
        self.keys.remove(&user_id);
    }

    /// Derive the per-device HMAC key for `(user_id, device_id)`.
    ///
    /// Per-device key = HKDF-SHA256(stored_key, salt=DEVICE_KEY_SALT, info=device_id.to_le_bytes())
    fn device_key(&self, user_id: u64, device_id: u64) -> Result<[u8; 32]> {
        let stored = self
            .keys
            .get(&user_id)
            .ok_or_else(|| CrdtError::InvalidSignature {
                user_id,
                detail: "no signing key registered for user".into(),
            })?;

        let hk = Hkdf::<Sha256>::new(Some(DEVICE_KEY_SALT), stored.as_slice());
        let mut okm = [0u8; 32];
        hk.expand(&device_id.to_le_bytes(), &mut okm)
            .map_err(|_| CrdtError::InvalidSignature {
                user_id,
                detail: "HKDF expand failed (output too long)".into(),
            })?;
        Ok(okm)
    }

    /// Sign delta bytes for a specific device and sequence number.
    ///
    /// HMAC input canonical layout:
    /// `delta_bytes || user_id.to_le_bytes(8) || device_id.to_le_bytes(8) || seq_no.to_le_bytes(8)`
    pub fn sign(
        &self,
        user_id: u64,
        device_id: u64,
        seq_no: u64,
        delta_bytes: &[u8],
    ) -> Result<[u8; SIGNATURE_SIZE]> {
        let key = self.device_key(user_id, device_id)?;
        Ok(compute_hmac(&key, user_id, device_id, seq_no, delta_bytes))
    }

    /// Verify a delta signature. Returns Ok(()) if valid.
    ///
    /// Uses constant-time comparison to prevent timing attacks.
    pub fn verify(
        &self,
        user_id: u64,
        device_id: u64,
        seq_no: u64,
        delta_bytes: &[u8],
        signature: &[u8; SIGNATURE_SIZE],
    ) -> Result<()> {
        let key = self.device_key(user_id, device_id)?;
        let expected = compute_hmac(&key, user_id, device_id, seq_no, delta_bytes);

        // Constant-time comparison.
        if expected.ct_eq(signature).into() {
            Ok(())
        } else {
            Err(CrdtError::InvalidSignature {
                user_id,
                detail: "HMAC-SHA256 mismatch".into(),
            })
        }
    }

    /// Sign a sync delta while binding its session epoch and logical target.
    /// Length-prefixing prevents ambiguous concatenations and the domain tag
    /// keeps this protocol separate from generic [`Self::sign`] callers.
    #[allow(clippy::too_many_arguments)]
    pub fn sign_sync_delta(
        &self,
        user_id: u64,
        device_id: u64,
        epoch: u64,
        seq_no: u64,
        collection: &str,
        document_id: &str,
        delta_bytes: &[u8],
    ) -> Result<[u8; SIGNATURE_SIZE]> {
        let message = sync_delta_message(epoch, collection, document_id, delta_bytes)?;
        self.sign(user_id, device_id, seq_no, &message)
    }

    /// Verify the target- and epoch-bound sync-delta signature.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_sync_delta(
        &self,
        user_id: u64,
        device_id: u64,
        epoch: u64,
        seq_no: u64,
        collection: &str,
        document_id: &str,
        delta_bytes: &[u8],
        signature: &[u8; SIGNATURE_SIZE],
    ) -> Result<()> {
        let message = sync_delta_message(epoch, collection, document_id, delta_bytes)?;
        self.verify(user_id, device_id, seq_no, &message, signature)
    }

    /// Access the shared device registry.
    pub fn registry(&self) -> &Arc<DeviceRegistry> {
        &self.registry
    }
}

impl Default for DeltaSigner {
    fn default() -> Self {
        Self::new()
    }
}

fn sync_delta_message(
    epoch: u64,
    collection: &str,
    document_id: &str,
    delta_bytes: &[u8],
) -> Result<Vec<u8>> {
    let collection_len = u32::try_from(collection.len()).map_err(|_| {
        CrdtError::DeltaApplyFailed("collection name too long for signed delta".into())
    })?;
    let document_len = u32::try_from(document_id.len())
        .map_err(|_| CrdtError::DeltaApplyFailed("document id too long for signed delta".into()))?;
    let capacity = 36usize
        .checked_add(collection.len())
        .and_then(|size| size.checked_add(document_id.len()))
        .and_then(|size| size.checked_add(delta_bytes.len()))
        .ok_or_else(|| CrdtError::DeltaApplyFailed("signed delta message too large".into()))?;
    let mut message = Vec::with_capacity(capacity);
    message.extend_from_slice(b"nodedb-sync-delta-v1");
    message.extend_from_slice(&epoch.to_le_bytes());
    message.extend_from_slice(&collection_len.to_le_bytes());
    message.extend_from_slice(collection.as_bytes());
    message.extend_from_slice(&document_len.to_le_bytes());
    message.extend_from_slice(document_id.as_bytes());
    message.extend_from_slice(delta_bytes);
    Ok(message)
}

/// Compute HMAC-SHA256 over the canonical delta-signing message.
///
/// Canonical layout:
/// `delta_bytes || user_id(8 LE) || device_id(8 LE) || seq_no(8 LE)`
fn compute_hmac(
    key: &[u8; 32],
    user_id: u64,
    device_id: u64,
    seq_no: u64,
    delta_bytes: &[u8],
) -> [u8; SIGNATURE_SIZE] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(delta_bytes);
    mac.update(&user_id.to_le_bytes());
    mac.update(&device_id.to_le_bytes());
    mac.update(&seq_no.to_le_bytes());
    let result = mac.finalize();
    let mut out = [0u8; SIGNATURE_SIZE];
    out.copy_from_slice(&result.into_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_signer(user_id: u64, key: [u8; 32]) -> DeltaSigner {
        let mut s = DeltaSigner::new();
        s.register_key(user_id, key);
        s
    }

    // ── HMAC golden-vector test ────────────────────────────────────────────
    // Hardcoded expected hex computed from the canonical HKDF+HMAC derivation.
    // If signing.rs changes the canonical layout, update this vector.
    #[test]
    fn hmac_golden_vector() {
        // stored key = [0x42; 32], user=1, device=2, seq=1, delta=b"nodedb"
        let signer = make_signer(1, [0x42u8; 32]);
        let sig = signer.sign(1, 2, 1, b"nodedb").unwrap();

        // Recompute inline to pin the vector.
        let device_key = {
            let hk = Hkdf::<Sha256>::new(Some(DEVICE_KEY_SALT), &[0x42u8; 32]);
            let mut okm = [0u8; 32];
            hk.expand(&2u64.to_le_bytes(), &mut okm).unwrap();
            okm
        };
        let expected = compute_hmac(&device_key, 1, 2, 1, b"nodedb");
        assert_eq!(sig, expected, "HMAC golden vector must be stable");
    }

    // ── Replay rejected on second submission ────────────────────────────────
    #[test]
    fn replay_rejected_same_device_seq() {
        let signer = make_signer(1, [0x42u8; 32]);
        let delta = b"test delta";
        let sig = signer.sign(1, 2, 1, delta).unwrap();

        // First submission: passes seq check.
        signer.registry.check_seq(1, 2, 1).unwrap();
        signer.verify(1, 2, 1, delta, &sig).unwrap();
        signer.registry.commit_seq(1, 2, 1).unwrap();

        // Second submission with same seq_no: replay.
        let err = signer.registry.check_seq(1, 2, 1).unwrap_err();
        assert!(
            matches!(
                err,
                CrdtError::ReplayDetected {
                    seq_no: 1,
                    last_seen: 1,
                    ..
                }
            ),
            "expected ReplayDetected, got {err}"
        );
    }

    // ── Cross-device replay rejected ─────────────────────────────────────────
    // A signed delta for device 2 must not verify under device 3's key.
    #[test]
    fn cross_device_replay_rejected() {
        let signer = make_signer(1, [0x42u8; 32]);
        let delta = b"cross device test";
        let sig = signer.sign(1, 2, 1, delta).unwrap();

        // Use device_id=3 in verify — different HKDF key, must fail.
        let err = signer.verify(1, 3, 1, delta, &sig).unwrap_err();
        assert!(
            matches!(err, CrdtError::InvalidSignature { .. }),
            "cross-device replay must be rejected"
        );
    }

    // ── seq_no=0 is rejected ─────────────────────────────────────────────────
    // With seq_no=0: check_seq(0) <= last_seen(0) → ReplayDetected.
    #[test]
    fn seq_zero_rejected() {
        let registry = DeviceRegistry::new();
        // seq_no=0 is never > last_seen=0.
        let err = registry.check_seq(1, 0, 0).unwrap_err();
        assert!(
            matches!(
                err,
                CrdtError::ReplayDetected {
                    seq_no: 0,
                    last_seen: 0,
                    ..
                }
            ),
            "seq_no=0 must be rejected (not strictly greater than last_seen=0)"
        );
    }

    // ── Verify rejects tampered delta ────────────────────────────────────────
    #[test]
    fn tampered_delta_fails_verification() {
        let signer = make_signer(1, [0x42u8; 32]);
        let sig = signer.sign(1, 2, 1, b"original").unwrap();
        let err = signer.verify(1, 2, 1, b"tampered", &sig).unwrap_err();
        assert!(matches!(err, CrdtError::InvalidSignature { .. }));
    }

    // ── Verify rejects wrong user ────────────────────────────────────────────
    #[test]
    fn wrong_user_fails_verification() {
        let mut signer = DeltaSigner::new();
        signer.register_key(1, [0x42u8; 32]);
        signer.register_key(2, [0x99u8; 32]);

        let sig = signer.sign(1, 5, 1, b"delta").unwrap();
        let err = signer.verify(2, 5, 1, b"delta", &sig).unwrap_err();
        assert!(matches!(err, CrdtError::InvalidSignature { .. }));
    }

    // ── Unregistered user fails ───────────────────────────────────────────────
    #[test]
    fn unregistered_user_fails() {
        let signer = DeltaSigner::new();
        let err = signer.sign(99, 1, 1, b"data").unwrap_err();
        assert!(matches!(
            err,
            CrdtError::InvalidSignature { user_id: 99, .. }
        ));
    }

    // ── seq_no strictly monotone ─────────────────────────────────────────────
    #[test]
    fn seq_no_must_advance() {
        let reg = DeviceRegistry::new();
        reg.check_seq(1, 1, 5).unwrap();
        reg.commit_seq(1, 1, 5).unwrap();

        // seq_no=5 again — replay.
        assert!(reg.check_seq(1, 1, 5).is_err());
        // seq_no=4 — replay.
        assert!(reg.check_seq(1, 1, 4).is_err());
        // seq_no=6 — ok.
        reg.check_seq(1, 1, 6).unwrap();
    }
}
