// SPDX-License-Identifier: BUSL-1.1

//! Token primitives — generation, hashing, parsing.
//!
//! Public token format is documented on the parent module. This file owns the
//! crypto/encoding primitives so they can be tested in isolation and reused
//! between the in-memory cache (`store`) and the raft preparation path
//! (`store::prepare_key`).

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;

pub(super) fn generate_key_id() -> crate::Result<String> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|e| crate::Error::Internal {
        detail: format!("OS random source failed generating an API key id: {e}"),
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn generate_secret() -> crate::Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| crate::Error::Internal {
        detail: format!("OS random source failed generating an API key secret: {e}"),
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// SHA-256 hash of an API key secret for storage.
///
/// API key secrets are high-entropy random tokens, so a fast cryptographic
/// hash (without key stretching) is appropriate — unlike passwords which
/// require Argon2id.
pub(super) fn hash_secret(secret: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.finalize().to_vec()
}

/// Parse and validate a `ndb_<key_id>.<secret>` token.
///
/// Returns `(key_id_str, secret_str)` only if:
/// - The `ndb_` prefix is present.
/// - Exactly one `.` separator follows (not in base64url alphabet → unambiguous).
/// - Both halves decode cleanly as base64url-no-pad.
/// - key_id decodes to exactly 8 bytes; secret decodes to exactly 32 bytes.
pub(super) fn parse_token(token: &str) -> Option<(&str, &str)> {
    let body = token.strip_prefix("ndb_")?;
    let dot_pos = body.find('.')?;
    if dot_pos == 0 || dot_pos == body.len() - 1 {
        return None;
    }
    let key_id = &body[..dot_pos];
    let secret = &body[dot_pos + 1..];

    // Validate key_id: must decode to exactly 8 bytes.
    let key_id_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(key_id)
        .ok()?;
    if key_id_bytes.len() != 8 {
        return None;
    }

    // Validate secret: must decode to exactly 32 bytes.
    let secret_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(secret)
        .ok()?;
    if secret_bytes.len() != 32 {
        return None;
    }

    Some((key_id, secret))
}

pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub(super) fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_token_format() {
        // Build a valid token manually: 8-byte key_id, 32-byte secret.
        let key_id_bytes = [0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe];
        let secret_bytes = [0x42u8; 32];
        let key_id_enc = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key_id_bytes);
        let secret_enc = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret_bytes);
        let token = format!("ndb_{key_id_enc}.{secret_enc}");

        let (kid, sec) = parse_token(&token).unwrap();
        assert_eq!(kid, key_id_enc);
        assert_eq!(sec, secret_enc);

        // Invalid cases.
        assert!(parse_token("not_valid").is_none());
        assert!(parse_token("ndb_.emptykeyid").is_none());
        assert!(parse_token("ndb_onlynoseparator").is_none());
        // Underscore separator (no dot) rejected.
        assert!(parse_token("ndb_abc123_secretpart").is_none());
    }

    #[test]
    fn encode_decode_roundtrip_1000() {
        for _ in 0..1000 {
            let mut key_bytes = [0u8; 8];
            let mut secret_bytes = [0u8; 32];
            getrandom::fill(&mut key_bytes).unwrap();
            getrandom::fill(&mut secret_bytes).unwrap();

            let key_enc = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key_bytes);
            let secret_enc = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret_bytes);

            let key_dec = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(&key_enc)
                .unwrap();
            let secret_dec = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(&secret_enc)
                .unwrap();

            assert_eq!(key_dec.as_slice(), &key_bytes);
            assert_eq!(secret_dec.as_slice(), &secret_bytes);
        }
    }

    #[test]
    fn entropy_coverage_10000_keys() {
        // Generate 10000 keys, collect chars at each position, assert each position
        // sees ≥ 50 distinct base64url chars (out of 64 possible).
        let key_id_len = 11usize;
        let secret_len = 43usize;

        let mut key_id_chars: Vec<std::collections::HashSet<char>> = (0..key_id_len)
            .map(|_| std::collections::HashSet::new())
            .collect();
        let mut secret_chars: Vec<std::collections::HashSet<char>> = (0..secret_len)
            .map(|_| std::collections::HashSet::new())
            .collect();

        for _ in 0..10_000 {
            let key_id = generate_key_id().unwrap();
            let secret = generate_secret().unwrap();

            assert_eq!(
                key_id.len(),
                key_id_len,
                "key_id length must be {key_id_len}"
            );
            assert_eq!(
                secret.len(),
                secret_len,
                "secret length must be {secret_len}"
            );

            for (pos, ch) in key_id.chars().enumerate() {
                key_id_chars[pos].insert(ch);
            }
            for (pos, ch) in secret.chars().enumerate() {
                secret_chars[pos].insert(ch);
            }
        }

        // 8 bytes → 11 base64url chars. Positions 0-9 encode 6 full bits each (64 possible
        // values). Position 10 encodes only the remaining 2 bits (4 possible values: A/Q/g/w).
        for (pos, chars) in key_id_chars.iter().enumerate() {
            let min_distinct = if pos < key_id_len - 1 { 50 } else { 4 };
            assert!(
                chars.len() >= min_distinct,
                "key_id position {pos} only saw {} distinct chars (expected ≥ {min_distinct})",
                chars.len()
            );
        }
        // 32 bytes → 43 base64url chars. Positions 0-41 encode 6 full bits each.
        // Position 42 encodes only 4 bits (16 possible values).
        for (pos, chars) in secret_chars.iter().enumerate() {
            let min_distinct = if pos < secret_len - 1 { 50 } else { 16 };
            assert!(
                chars.len() >= min_distinct,
                "secret position {pos} only saw {} distinct chars (expected ≥ {min_distinct})",
                chars.len()
            );
        }
    }
}
