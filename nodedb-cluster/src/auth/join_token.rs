// SPDX-License-Identifier: BUSL-1.1

//! HMAC-SHA256 join-token issuance and constant-time verification.
//!
//! Token format (opaque to callers, transmitted as hex):
//! ```text
//! [version: u8 | for_node: u64 LE | expiry: u64 LE |
//!  bootstrap_issuer_spki: 32 bytes | ca_len: u32 LE |
//!  bootstrap_ca_der: ca_len bytes | mac: 32 bytes]
//! ```
//! The MAC covers every field before it. Embedding the cluster CA certificate
//! lets the joiner authenticate the bootstrap TLS listener before transmitting
//! the bearer token; the certificate is safe to expose and its integrity is
//! bound to the out-of-band token.
//!
//! The `nodedb` crate's `ctl::join_token` module is a thin CLI wrapper
//! that delegates issuance to [`issue_token`] here. Verification is
//! consumed by the bootstrap-listener handler in
//! `nodedb/src/control/cluster/bootstrap_listener.rs`.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

const TOKEN_VERSION: u8 = 1;
/// Number of fixed bytes before the embedded CA certificate.
pub const TOKEN_HEADER_LEN: usize = 1 + 8 + 8 + 32 + 4;
/// Number of bytes in the HMAC-SHA256 tag.
pub const TOKEN_MAC_LEN: usize = 32;
/// Upper bound for the DER-encoded bootstrap CA certificate.
pub const MAX_BOOTSTRAP_CA_BYTES: usize = 16 * 1024;

/// Error returned by token operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("token wrong length")]
    WrongLength,
    #[error("token contains invalid hex")]
    InvalidHex,
    #[error("invalid token MAC")]
    InvalidMac,
    #[error("token expired")]
    Expired,
    #[error("hmac key length invalid")]
    HmacKeyLength,
    #[error("bootstrap CA certificate is empty or too large")]
    InvalidCaCertificateLength,
    #[error("unsupported token version")]
    UnsupportedVersion,
}

/// Fields authenticated by a verified join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedJoinToken {
    pub for_node: u64,
    pub expiry_unix_secs: u64,
    pub bootstrap_issuer_spki: [u8; 32],
    pub bootstrap_ca_der: Vec<u8>,
}

/// Issue a new HMAC-SHA256 join token bound to the bootstrap CA certificate.
pub fn issue_token_bytes(
    secret: &[u8; 32],
    for_node: u64,
    expiry_unix_secs: u64,
    bootstrap_issuer_spki: [u8; 32],
    bootstrap_ca_der: &[u8],
) -> Result<Vec<u8>, TokenError> {
    if bootstrap_ca_der.is_empty() || bootstrap_ca_der.len() > MAX_BOOTSTRAP_CA_BYTES {
        return Err(TokenError::InvalidCaCertificateLength);
    }
    let ca_len = u32::try_from(bootstrap_ca_der.len())
        .map_err(|_| TokenError::InvalidCaCertificateLength)?;
    let mut body = Vec::with_capacity(TOKEN_HEADER_LEN + bootstrap_ca_der.len());
    body.push(TOKEN_VERSION);
    body.extend_from_slice(&for_node.to_le_bytes());
    body.extend_from_slice(&expiry_unix_secs.to_le_bytes());
    body.extend_from_slice(&bootstrap_issuer_spki);
    body.extend_from_slice(&ca_len.to_le_bytes());
    body.extend_from_slice(bootstrap_ca_der);
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).map_err(|_| TokenError::HmacKeyLength)?;
    mac.update(&body);
    body.extend_from_slice(&mac.finalize().into_bytes());
    Ok(body)
}

/// Convenience: issue a token and return it as a lowercase hex string.
pub fn issue_token(
    secret: &[u8; 32],
    for_node: u64,
    expiry_unix_secs: u64,
    bootstrap_issuer_spki: [u8; 32],
    bootstrap_ca_der: &[u8],
) -> Result<String, TokenError> {
    let bytes = issue_token_bytes(
        secret,
        for_node,
        expiry_unix_secs,
        bootstrap_issuer_spki,
        bootstrap_ca_der,
    )?;
    Ok(token_to_hex(&bytes))
}

/// Encode raw token bytes as a lowercase hex string.
pub fn token_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Compute SHA-256 of the token bytes. Used as the stable identity for
/// state-machine tracking (never stores the raw token).
pub fn token_hash(token_hex: &str) -> Result<[u8; 32], TokenError> {
    use sha2::Digest;
    let bytes = hex_decode(token_hex)?;
    Ok(sha2::Sha256::digest(&bytes).into())
}

/// Extract the token-bound bootstrap CA before opening the TLS connection.
/// The caller obtains the token out of band; the server subsequently verifies
/// the MAC over this same certificate before issuing any credentials.
pub fn bootstrap_ca_cert(token_hex: &str) -> Result<Vec<u8>, TokenError> {
    let bytes = hex_decode(token_hex)?;
    let layout = parse_layout(&bytes)?;
    Ok(bytes[layout.ca_range].to_vec())
}

/// Extract the token-bound bootstrap issuer SPKI before opening TLS.
pub fn bootstrap_issuer_spki(token_hex: &str) -> Result<[u8; 32], TokenError> {
    let bytes = hex_decode(token_hex)?;
    let layout = parse_layout(&bytes)?;
    Ok(layout.issuer_spki)
}

struct ParsedTokenLayout {
    for_node: u64,
    expiry: u64,
    issuer_spki: [u8; 32],
    ca_range: std::ops::Range<usize>,
    body_len: usize,
}

/// Verify a hex-encoded token against `secret` in constant time.
pub fn verify_token(token_hex: &str, secret: &[u8; 32]) -> Result<VerifiedJoinToken, TokenError> {
    let bytes = hex_decode(token_hex)?;
    let layout = parse_layout(&bytes)?;
    let (body, tag) = bytes.split_at(layout.body_len);
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).map_err(|_| TokenError::HmacKeyLength)?;
    mac.update(body);
    mac.verify_slice(tag).map_err(|_| TokenError::InvalidMac)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_else(|_| {
            tracing::error!(
                "system clock is before UNIX_EPOCH during token verification; \
                 using 0 (epoch) — check NTP/RTC configuration"
            );
            0
        });
    if now > layout.expiry {
        return Err(TokenError::Expired);
    }
    Ok(VerifiedJoinToken {
        for_node: layout.for_node,
        expiry_unix_secs: layout.expiry,
        bootstrap_issuer_spki: layout.issuer_spki,
        bootstrap_ca_der: bytes[layout.ca_range].to_vec(),
    })
}

fn parse_layout(bytes: &[u8]) -> Result<ParsedTokenLayout, TokenError> {
    if bytes.len() < TOKEN_HEADER_LEN + TOKEN_MAC_LEN {
        return Err(TokenError::WrongLength);
    }
    if bytes[0] != TOKEN_VERSION {
        return Err(TokenError::UnsupportedVersion);
    }
    let for_node = u64::from_le_bytes(
        bytes[1..9]
            .try_into()
            .map_err(|_| TokenError::WrongLength)?,
    );
    let expiry = u64::from_le_bytes(
        bytes[9..17]
            .try_into()
            .map_err(|_| TokenError::WrongLength)?,
    );
    let issuer_spki = bytes[17..49]
        .try_into()
        .map_err(|_| TokenError::WrongLength)?;
    let ca_len = u32::from_le_bytes(
        bytes[49..53]
            .try_into()
            .map_err(|_| TokenError::WrongLength)?,
    ) as usize;
    if ca_len == 0 || ca_len > MAX_BOOTSTRAP_CA_BYTES {
        return Err(TokenError::InvalidCaCertificateLength);
    }
    let body_len = TOKEN_HEADER_LEN
        .checked_add(ca_len)
        .ok_or(TokenError::WrongLength)?;
    if bytes.len() != body_len + TOKEN_MAC_LEN {
        return Err(TokenError::WrongLength);
    }
    Ok(ParsedTokenLayout {
        for_node,
        expiry,
        issuer_spki,
        ca_range: TOKEN_HEADER_LEN..body_len,
        body_len,
    })
}

fn hex_decode(s: &str) -> Result<Vec<u8>, TokenError> {
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        if chunk.len() != 2 {
            return Err(TokenError::InvalidHex);
        }
        let hi = hex_digit(chunk[0]).ok_or(TokenError::InvalidHex)?;
        let lo = hex_digit(chunk[1]).ok_or(TokenError::InvalidHex)?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CA_DER: &[u8] = b"test bootstrap ca der";
    const ISSUER_SPKI: [u8; 32] = [0x5a; 32];

    fn fresh_expiry() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs()
            + 60
    }

    #[test]
    fn roundtrip_binds_node_expiry_and_bootstrap_ca() {
        let secret = [0x11u8; 32];
        let expiry = fresh_expiry();
        let hex = issue_token(&secret, 42, expiry, ISSUER_SPKI, CA_DER).expect("issue token");
        let verified = verify_token(&hex, &secret).expect("verify token");
        assert_eq!(verified.for_node, 42);
        assert_eq!(verified.expiry_unix_secs, expiry);
        assert_eq!(verified.bootstrap_issuer_spki, ISSUER_SPKI);
        assert_eq!(verified.bootstrap_ca_der, CA_DER);
        assert_eq!(bootstrap_ca_cert(&hex).expect("extract CA"), CA_DER);
    }

    #[test]
    fn rejects_tampered_mac_or_bootstrap_ca() {
        let secret = [0x11u8; 32];
        let hex =
            issue_token(&secret, 1, fresh_expiry(), ISSUER_SPKI, CA_DER).expect("issue token");
        let mut tampered_mac = hex.clone();
        let len = tampered_mac.len();
        let orig = u8::from_str_radix(&tampered_mac[len - 2..], 16).expect("hex");
        tampered_mac.replace_range(len - 2.., &format!("{:02x}", orig ^ 0xff));
        assert_eq!(
            verify_token(&tampered_mac, &secret).expect_err("tampered MAC"),
            TokenError::InvalidMac
        );

        let mut tampered_ca = hex;
        let ca_byte = TOKEN_HEADER_LEN * 2;
        let orig = u8::from_str_radix(&tampered_ca[ca_byte..ca_byte + 2], 16).expect("hex");
        tampered_ca.replace_range(ca_byte..ca_byte + 2, &format!("{:02x}", orig ^ 0xff));
        assert_eq!(
            verify_token(&tampered_ca, &secret).expect_err("tampered CA"),
            TokenError::InvalidMac
        );
    }

    #[test]
    fn rejects_wrong_secret_expiry_and_malformed_tokens() {
        let secret = [0x22u8; 32];
        let hex =
            issue_token(&secret, 5, fresh_expiry(), ISSUER_SPKI, CA_DER).expect("issue token");
        assert_eq!(
            verify_token(&hex, &[0x33; 32]).expect_err("wrong secret"),
            TokenError::InvalidMac
        );
        let expired = issue_token(&secret, 5, 1, ISSUER_SPKI, CA_DER).expect("issue expired token");
        assert_eq!(
            verify_token(&expired, &secret).expect_err("expired"),
            TokenError::Expired
        );
        assert_eq!(
            verify_token("deadbeef", &secret).expect_err("short token"),
            TokenError::WrongLength
        );
        assert_eq!(
            issue_token(&secret, 5, fresh_expiry(), ISSUER_SPKI, &[]).expect_err("empty CA"),
            TokenError::InvalidCaCertificateLength
        );
    }

    #[test]
    fn token_hash_is_stable() {
        let secret = [0x44u8; 32];
        let hex =
            issue_token(&secret, 7, fresh_expiry(), ISSUER_SPKI, CA_DER).expect("issue token");
        let first = token_hash(&hex).expect("hash token");
        assert_eq!(first, token_hash(&hex).expect("hash token again"));
        assert!(first.iter().any(|byte| *byte != 0));
    }
}
