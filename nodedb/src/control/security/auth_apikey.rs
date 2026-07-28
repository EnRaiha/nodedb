// SPDX-License-Identifier: BUSL-1.1

//! Auth-scoped API keys: `nda_` format, bound to `_system.auth_users`.
//!
//! Distinct from DB user keys (`ndb_`). Auth keys:
//! - Are bound to JIT-provisioned auth users (Mode 2)
//! - Support per-key scope restriction (subset of user's scopes)
//! - Support per-key rate limits
//! - Track last-used timestamp and IP
//! - Support key rotation with configurable overlap period

use std::collections::HashMap;

use parking_lot::RwLock;
use tracing::info;

/// An auth-scoped API key record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuthApiKey {
    /// Key ID (prefix of the token): `nda_<key_id>`.
    pub key_id: String,
    /// SHA-256 hash of the secret portion.
    pub secret_hash: Vec<u8>,
    /// Auth user ID this key is bound to.
    pub auth_user_id: String,
    /// Tenant ID.
    pub tenant_id: u64,
    /// Scopes this key is restricted to (subset of user's scopes). Empty = inherit all.
    pub scopes: Vec<String>,
    /// Per-key rate limit (QPS). 0 = use user/tier default.
    pub rate_limit_qps: u64,
    /// Per-key burst capacity. 0 = use default.
    pub rate_limit_burst: u64,
    /// Unix timestamp when the key expires. 0 = no expiry.
    pub expires_at: u64,
    /// Unix timestamp when the key was created.
    pub created_at: u64,
    /// Whether this key has been revoked.
    pub is_revoked: bool,
    /// Last used timestamp (updated on each verification).
    pub last_used_at: u64,
    /// Last used IP address.
    pub last_used_ip: String,
    // Key rotation overlap window: both old and new keys verify during the
    // configured overlap period; old keys revoke when `superseded_at` is set
    // and that timestamp elapses, or immediately when `overlap_hours == 0`
    // (set `is_revoked = true` to avoid a clock-edge race at `now == superseded_at`).
    //
    // `replaces_key_id` — forward audit pointer on the new key.
    // `superseded_at`   — deadline on the old key (`0` = not superseded yet).
    // `superseded_by`   — reverse pointer on the old key.
    // `is_valid()`      — single gate enforcing all three conditions.
    /// Forward audit pointer on the new key: ID of the old key this key replaces.
    pub replaces_key_id: Option<String>,
    /// Deadline on the old key: Unix timestamp after which `verify()` must
    /// reject this key. `0` means this key has not been superseded yet.
    pub superseded_at: u64,
    /// Reverse pointer on the old key: key_id of the replacement key.
    pub superseded_by: Option<String>,
}

impl AuthApiKey {
    /// Single source of truth for "is this key currently honourable by
    /// verify()?". All validity gates (`verify`, `touch`, `list_for_user`,
    /// `list_all`) funnel through here so a new invalidation condition,
    /// once added, cannot be forgotten in one call site.
    pub fn is_valid(&self, now: u64) -> bool {
        if self.is_revoked {
            return false;
        }
        if self.expires_at > 0 && now > self.expires_at {
            return false;
        }
        if self.superseded_at > 0 && now > self.superseded_at {
            return false;
        }
        true
    }
}

/// Coupled indexes for auth API key verification.
///
/// Both maps share one lock so every mutation publishes an internally
/// consistent key/hash pair and no caller can acquire them in a conflicting
/// order.
#[derive(Default)]
struct AuthApiKeyState {
    /// key_id → AuthApiKey.
    keys: HashMap<String, AuthApiKey>,
    /// secret_hash (hex) → key_id for O(1) lookup during verification.
    hash_index: HashMap<String, String>,
}

/// Auth API key store.
pub struct AuthApiKeyStore {
    state: RwLock<AuthApiKeyState>,
}

impl AuthApiKeyStore {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(AuthApiKeyState::default()),
        }
    }

    /// Create a new auth API key. Returns the full token (shown once).
    pub fn create_key(
        &self,
        auth_user_id: &str,
        tenant_id: u64,
        scopes: Vec<String>,
        rate_limit_qps: u64,
        rate_limit_burst: u64,
        expires_days: u64,
    ) -> String {
        self.create_key_inner(
            auth_user_id,
            tenant_id,
            scopes,
            rate_limit_qps,
            rate_limit_burst,
            expires_days,
        )
        .1
    }

    /// Build an unpublished key record and token. Callers publish the record
    /// and hash index together through [`Self::insert_key`].
    fn build_key_record(
        auth_user_id: &str,
        tenant_id: u64,
        scopes: Vec<String>,
        rate_limit_qps: u64,
        rate_limit_burst: u64,
        expires_days: u64,
    ) -> (AuthApiKey, String, String) {
        let key_id = generate_key_id();
        let secret = generate_secret();
        let token = format!("nda_{key_id}_{secret}");
        let secret_hash = hash_secret(&secret);
        let now = now_secs();
        let expires_at = if expires_days > 0 {
            now + expires_days * 86_400
        } else {
            0
        };
        let hash_hex = hex_encode(&secret_hash);
        let record = AuthApiKey {
            key_id,
            secret_hash,
            auth_user_id: auth_user_id.into(),
            tenant_id,
            scopes,
            rate_limit_qps,
            rate_limit_burst,
            expires_at,
            created_at: now,
            is_revoked: false,
            last_used_at: 0,
            last_used_ip: String::new(),
            replaces_key_id: None,
            superseded_at: 0,
            superseded_by: None,
        };
        (record, hash_hex, token)
    }

    fn insert_key(state: &mut AuthApiKeyState, record: AuthApiKey, hash_hex: String) {
        let key_id = record.key_id.clone();
        state.hash_index.insert(hash_hex, key_id.clone());
        state.keys.insert(key_id, record);
    }

    fn create_key_inner(
        &self,
        auth_user_id: &str,
        tenant_id: u64,
        scopes: Vec<String>,
        rate_limit_qps: u64,
        rate_limit_burst: u64,
        expires_days: u64,
    ) -> (String, String) {
        let (record, hash_hex, token) = Self::build_key_record(
            auth_user_id,
            tenant_id,
            scopes,
            rate_limit_qps,
            rate_limit_burst,
            expires_days,
        );
        let key_id = record.key_id.clone();
        Self::insert_key(&mut self.state.write(), record, hash_hex);
        (key_id, token)
    }

    /// Verify an `nda_` token. Returns the key record if valid.
    pub fn verify(&self, token: &str) -> Option<AuthApiKey> {
        let stripped = token.strip_prefix("nda_")?;
        let parts: Vec<&str> = stripped.splitn(2, '_').collect();
        if parts.len() != 2 {
            return None;
        }
        let secret = parts[1];
        let secret_hash = hash_secret(secret);
        let hash_hex = hex_encode(&secret_hash);

        let state = self.state.read();
        let key_id = state.hash_index.get(&hash_hex)?;
        let key = state.keys.get(key_id)?;

        if !key.is_valid(now_secs()) {
            return None;
        }

        Some(key.clone())
    }

    /// Update last-used tracking after successful verification.
    /// No-op on keys that are no longer valid — prevents audit trails from
    /// silently recording post-rotation use of a dead key as if it were live.
    pub fn touch(&self, key_id: &str, ip: &str) {
        let now = now_secs();
        let mut state = self.state.write();
        if let Some(key) = state.keys.get_mut(key_id) {
            if !key.is_valid(now) {
                return;
            }
            key.last_used_at = now;
            key.last_used_ip = ip.to_string();
        }
    }

    /// Revoke a key.
    pub fn revoke(&self, key_id: &str) -> bool {
        let mut state = self.state.write();
        if let Some(key) = state.keys.get_mut(key_id) {
            key.is_revoked = true;
            true
        } else {
            false
        }
    }

    /// Rotate a key: create a new key that replaces the old one.
    /// Both are valid during the overlap period.
    pub fn rotate(&self, old_key_id: &str, overlap_hours: u64) -> Option<String> {
        // Generate the replacement before acquiring the write lock, but do not
        // publish it. The source is re-read and validated under that one lock
        // before either index is mutated.
        let source = {
            let state = self.state.read();
            let source = state.keys.get(old_key_id)?;
            if !source.is_valid(now_secs()) || source.superseded_by.is_some() {
                return None;
            }
            source.clone()
        };
        let (mut replacement, hash_hex, token) = Self::build_key_record(
            &source.auth_user_id,
            source.tenant_id,
            source.scopes.clone(),
            source.rate_limit_qps,
            source.rate_limit_burst,
            0,
        );
        let replacement_id = replacement.key_id.clone();
        replacement.replaces_key_id = Some(old_key_id.to_owned());

        let mut state = self.state.write();
        let now = now_secs();
        let old = state.keys.get_mut(old_key_id)?;
        if !old.is_valid(now) || old.superseded_by.is_some() {
            return None;
        }
        old.superseded_by = Some(replacement_id.clone());
        if overlap_hours == 0 {
            old.is_revoked = true;
        } else {
            old.superseded_at = now + overlap_hours * 3600;
        }
        Self::insert_key(&mut state, replacement, hash_hex);

        info!(
            old_key = %old_key_id,
            new_key = %replacement_id,
            overlap_hours,
            "auth API key rotated"
        );
        Some(token)
    }

    /// List all keys for an auth user.
    pub fn list_for_user(&self, auth_user_id: &str) -> Vec<AuthApiKey> {
        let now = now_secs();
        let state = self.state.read();
        state
            .keys
            .values()
            .filter(|key| key.auth_user_id == auth_user_id && key.is_valid(now))
            .cloned()
            .collect()
    }

    /// List all keys (admin view).
    pub fn list_all(&self) -> Vec<AuthApiKey> {
        let now = now_secs();
        let state = self.state.read();
        state
            .keys
            .values()
            .filter(|key| key.is_valid(now))
            .cloned()
            .collect()
    }
}

impl Default for AuthApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

fn generate_key_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = now_secs();
    format!("{ts:x}{seq:04x}")
}

fn generate_secret() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    // 32 bytes of cryptographic randomness + sequential suffix for uniqueness.
    let mut random_bytes = [0u8; 32];
    getrandom::fill(&mut random_bytes).unwrap_or_else(|_| {
        // Fallback: use timestamp + counter if getrandom unavailable (e.g., early boot).
        let ts = now_secs();
        random_bytes[..8].copy_from_slice(&ts.to_le_bytes());
        random_bytes[8..16].copy_from_slice(&seq.to_le_bytes());
    });
    format!("{}{seq:08x}", hex_encode(&random_bytes))
}

fn hash_secret(secret: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(secret.as_bytes()).to_vec()
}

fn now_secs() -> u64 {
    crate::control::security::time::now_secs()
}

/// Hex-encode a byte slice (lowercase).
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_and_hash_indexes_remain_usable_after_panic_while_state_write_locked() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("panic-safe", 1, vec![], 0, 0, 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = store.state.write();
            panic!("simulated interrupted auth-key cache update");
        }));
        assert!(result.is_err());

        let key = store
            .verify(&token)
            .expect("lookup after interrupted update");
        let hash_hex = hex_encode(&key.secret_hash);
        let state = store.state.read();
        assert_eq!(state.hash_index.get(&hash_hex), Some(&key.key_id));
        assert!(state.keys.contains_key(&key.key_id));
    }

    #[test]
    fn concurrent_verify_and_create_complete_without_lock_order_deadlock() {
        use std::sync::{Arc, Barrier};

        let store = Arc::new(AuthApiKeyStore::new());
        let token = store.create_key("existing", 1, vec![], 0, 0, 0);
        let start = Arc::new(Barrier::new(3));

        let verify_store = Arc::clone(&store);
        let verify_start = Arc::clone(&start);
        let verifier = std::thread::spawn(move || {
            verify_start.wait();
            for _ in 0..1_000 {
                assert!(verify_store.verify(&token).is_some());
            }
        });
        let create_store = Arc::clone(&store);
        let create_start = Arc::clone(&start);
        let creator = std::thread::spawn(move || {
            create_start.wait();
            for _ in 0..1_000 {
                create_store.create_key("created", 1, vec![], 0, 0, 0);
            }
        });

        start.wait();
        verifier.join().expect("verification thread must complete");
        creator.join().expect("creation thread must complete");
    }

    #[test]
    fn create_and_verify() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("user_42", 1, vec![], 0, 0, 0);

        assert!(token.starts_with("nda_"));
        let key = store.verify(&token).unwrap();
        assert_eq!(key.auth_user_id, "user_42");
        assert_eq!(key.tenant_id, 1);
    }

    #[test]
    fn invalid_token_rejected() {
        let store = AuthApiKeyStore::new();
        assert!(store.verify("nda_bad_token").is_none());
        assert!(store.verify("ndb_wrong_prefix").is_none());
    }

    #[test]
    fn revoked_key_rejected() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let key = store.verify(&token).unwrap();
        store.revoke(&key.key_id);
        assert!(store.verify(&token).is_none());
    }

    #[test]
    fn scoped_key() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key(
            "u1",
            1,
            vec!["profile:read".into(), "orders:write".into()],
            100,
            200,
            30,
        );
        let key = store.verify(&token).unwrap();
        assert_eq!(key.scopes, vec!["profile:read", "orders:write"]);
        assert_eq!(key.rate_limit_qps, 100);
        assert!(key.expires_at > 0);
    }

    #[test]
    fn last_used_tracking() {
        let store = AuthApiKeyStore::new();
        let token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let key = store.verify(&token).unwrap();
        assert_eq!(key.last_used_at, 0);

        store.touch(&key.key_id, "10.0.0.1");
        let key2 = store.verify(&token).unwrap();
        assert!(key2.last_used_at > 0);
        assert_eq!(key2.last_used_ip, "10.0.0.1");
    }

    #[test]
    fn key_rotation() {
        let store = AuthApiKeyStore::new();
        let old_token = store.create_key("u1", 1, vec!["scope_a".into()], 0, 0, 0);
        let old_key = store.verify(&old_token).unwrap();

        let new_token = store.rotate(&old_key.key_id, 24).unwrap();
        assert!(new_token.starts_with("nda_"));

        // Both old and new should be valid during overlap.
        assert!(store.verify(&old_token).is_some());
        assert!(store.verify(&new_token).is_some());

        // New key should inherit scopes.
        let new_key = store.verify(&new_token).unwrap();
        assert_eq!(new_key.scopes, vec!["scope_a"]);
        assert!(new_key.replaces_key_id.is_some());
    }

    #[test]
    fn rotated_old_key_invalid_after_overlap_window() {
        // Spec: once the overlap window configured on rotate() has elapsed,
        // the old token must be rejected by verify(). Operators rotate
        // precisely so that the old (possibly-leaked) key stops working.
        let store = AuthApiKeyStore::new();
        let old_token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let old_key_id = store.verify(&old_token).unwrap().key_id;

        // Zero-hour overlap: cutover as soon as wall clock advances past now.
        let _new_token = store.rotate(&old_key_id, 0).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));

        assert!(
            store.verify(&old_token).is_none(),
            "rotate(old, 0) must invalidate the old token after the overlap window; \
             a leaked key that triggered the rotation must not remain valid indefinitely"
        );
    }

    #[test]
    fn rotate_chain_invalidates_intermediate_key() {
        // Spec: rotation chains invalidate every superseded key, not just the
        // first. After A→B→C with zero overlap, both A and B must be rejected.
        let store = AuthApiKeyStore::new();
        let token_a = store.create_key("u1", 1, vec![], 0, 0, 0);
        let id_a = store.verify(&token_a).unwrap().key_id;

        let token_b = store.rotate(&id_a, 0).unwrap();
        let id_b = store.verify(&token_b).unwrap().key_id;

        let _token_c = store.rotate(&id_b, 0).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));

        assert!(
            store.verify(&token_a).is_none(),
            "rotation chain must invalidate the oldest key past its overlap window"
        );
        assert!(
            store.verify(&token_b).is_none(),
            "rotation chain must invalidate the intermediate key past its overlap window"
        );
    }

    #[test]
    fn list_for_user_excludes_superseded_keys_past_overlap() {
        // Spec: listing active keys must reflect supersession, not only the
        // is_revoked bit. An operator auditing active credentials should not
        // see a rotated-out key as still active after its overlap elapsed.
        let store = AuthApiKeyStore::new();
        let token_old = store.create_key("u1", 1, vec![], 0, 0, 0);
        let old_id = store.verify(&token_old).unwrap().key_id;

        let _token_new = store.rotate(&old_id, 0).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));

        let active = store.list_for_user("u1");
        assert_eq!(
            active.len(),
            1,
            "after rotation + overlap elapsed, only the replacement key should be listed as active; \
             got {} active keys",
            active.len()
        );
        assert!(
            active.iter().all(|k| k.key_id != old_id),
            "superseded key_id {old_id} must not appear in list_for_user after overlap elapses"
        );
    }

    #[test]
    fn rotate_persists_invalidation_marker_on_old_key() {
        // Regression guard against the specific silent-failure mode: rotate()
        // historically decorated only the NEW key and left the OLD key
        // untouched, so verify() had no way to learn the old key was being
        // retired. The fix must leave an invalidation marker on the OLD
        // record itself so verify() can reject by inspecting the old record
        // alone — no cross-record lookup.
        let store = AuthApiKeyStore::new();
        let old_token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let old_id = store.verify(&old_token).unwrap().key_id;

        let _new_token = store.rotate(&old_id, 24).unwrap();

        let state = store.state.read();
        let old = state
            .keys
            .get(&old_id)
            .expect("old key record still present");
        assert!(
            old.is_revoked || old.expires_at > 0 || old.superseded_at > 0,
            "rotate() must leave an invalidation marker on the old key record itself; \
             verify() cannot enforce supersession from the new-key side alone"
        );
        assert!(
            old.superseded_by.is_some(),
            "rotate() must set superseded_by reverse pointer on the old key"
        );
    }

    #[test]
    fn rotate_zero_overlap_revokes_old_key_immediately() {
        // Spec: overlap_hours == 0 means "immediate cutover, no race window".
        // Implementation must set is_revoked=true on the old key rather than
        // superseded_at=now, because now > now is false for the first ~1s —
        // a real auth window during which a leaked key still verifies.
        // No sleep in this test: the cutover must be synchronous.
        let store = AuthApiKeyStore::new();
        let old_token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let old_id = store.verify(&old_token).unwrap().key_id;

        let _new_token = store.rotate(&old_id, 0).unwrap();

        assert!(
            store.verify(&old_token).is_none(),
            "rotate(old, 0 /* zero overlap */) must invalidate the old key in the same \
             instant, without depending on the wall clock advancing past now_secs()"
        );
    }

    #[test]
    fn touch_is_noop_on_superseded_key() {
        // Spec: touch() must not update last_used_at / last_used_ip on a key
        // that is no longer valid. Otherwise audit trails silently record
        // post-rotation use of a dead key as if it were live.
        let store = AuthApiKeyStore::new();
        let old_token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let old_id = store.verify(&old_token).unwrap().key_id;

        let _new_token = store.rotate(&old_id, 0).unwrap();
        // Old key is now invalid (zero-overlap → immediate revoke).
        store.touch(&old_id, "10.0.0.99");

        let state = store.state.read();
        let old = state.keys.get(&old_id).expect("record still present");
        assert_eq!(
            old.last_used_ip, "",
            "touch() must be a no-op on an invalidated key; got last_used_ip={:?}",
            old.last_used_ip
        );
        assert_eq!(
            old.last_used_at, 0,
            "touch() must not update last_used_at on an invalidated key"
        );
    }

    #[test]
    fn list_all_excludes_superseded_keys_past_overlap() {
        // Spec: list_all() is the admin view. Like list_for_user(), it must
        // reflect supersession, not only the is_revoked bit.
        let store = AuthApiKeyStore::new();
        let token_old = store.create_key("u1", 1, vec![], 0, 0, 0);
        let old_id = store.verify(&token_old).unwrap().key_id;

        let _token_new = store.rotate(&old_id, 0).unwrap();

        let all = store.list_all();
        assert_eq!(
            all.len(),
            1,
            "list_all must exclude superseded keys; got {} entries",
            all.len()
        );
        assert!(
            all.iter().all(|k| k.key_id != old_id),
            "superseded key {old_id} must not appear in list_all"
        );
    }

    #[test]
    fn rotating_revoked_key_fails() {
        // Spec: rotating a key that is already revoked is nonsensical — there
        // is nothing to overlap with. rotate() must return None rather than
        // silently minting a replacement for a dead key.
        let store = AuthApiKeyStore::new();
        let token = store.create_key("u1", 1, vec![], 0, 0, 0);
        let key_id = store.verify(&token).unwrap().key_id;
        assert!(store.revoke(&key_id));

        assert!(
            store.rotate(&key_id, 24).is_none(),
            "rotate() on a revoked key must fail, not silently create a replacement"
        );
        let state = store.state.read();
        assert_eq!(state.keys.len(), 1);
        assert_eq!(state.hash_index.len(), 1);
    }

    #[test]
    fn rotating_superseded_key_fails() {
        // Spec: rotating a key that is itself already superseded (A→B, then
        // attempting A→C) is the same nonsense as rotating a revoked key —
        // the source key is no longer authoritative. rotate() must fail.
        let store = AuthApiKeyStore::new();
        let token_a = store.create_key("u1", 1, vec![], 0, 0, 0);
        let id_a = store.verify(&token_a).unwrap().key_id;

        let token_b = store.rotate(&id_a, 24).unwrap();
        // A remains credential-valid during the overlap, but is no longer an
        // authoritative rotation source. A second rotation must not mint C.
        assert!(store.verify(&token_a).is_some());
        assert!(store.verify(&token_b).is_some());
        assert!(
            store.rotate(&id_a, 24).is_none(),
            "rotate() on an already-superseded key must fail; chain rotations \
             must go through the current-active key, not an overlapping source"
        );
        let state = store.state.read();
        assert_eq!(state.keys.len(), 2);
        assert_eq!(state.hash_index.len(), 2);
    }

    #[test]
    fn list_for_user() {
        let store = AuthApiKeyStore::new();
        store.create_key("u1", 1, vec![], 0, 0, 0);
        store.create_key("u1", 1, vec![], 0, 0, 0);
        store.create_key("u2", 1, vec![], 0, 0, 0);

        assert_eq!(store.list_for_user("u1").len(), 2);
        assert_eq!(store.list_for_user("u2").len(), 1);
    }
}
