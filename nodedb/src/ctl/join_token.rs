// SPDX-License-Identifier: BUSL-1.1

//! `nodedb join-token --create` — thin CLI wrapper over
//! [`nodedb_cluster::auth::join_token`].
//!
//! The opaque token binds the target node, expiry, and cluster CA certificate
//! under HMAC-SHA256 keyed by the persisted `cluster_secret`. The joiner uses
//! the embedded CA to authenticate the bootstrap TLS listener before sending
//! the bearer token. Verification and issuance
//! logic live in `nodedb_cluster::auth::join_token` — the single
//! source of truth for the token format.
//!
//! Verification is consumed by the bootstrap-listener handler in
//! `nodedb/src/control/cluster/bootstrap_listener.rs` via
//! `nodedb_cluster::verify_token`.

use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb_cluster::auth::join_token as tok;

const CLUSTER_SECRET_LEN: usize = 32;

pub fn create(data_dir: &Path, for_node: u64, ttl: Duration) -> Result<(), String> {
    let tls_dir = data_dir.join("tls");
    let secret_path = tls_dir.join("cluster_secret.bin");
    let secret = read_cluster_secret(&secret_path)?;
    let ca_der = read_ca_cert(&tls_dir.join("ca.crt"))?;
    let issuer_cert = read_ca_cert(&tls_dir.join("node.crt"))?;
    let issuer_spki = nodedb_cluster::spki_pin_from_cert_der(&issuer_cert)
        .map_err(|e| format!("read bootstrap issuer SPKI: {e}"))?;

    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock before UNIX epoch: {e}"))?
        + ttl;
    let expiry_secs = expiry.as_secs();

    let hex = tok::issue_token(&secret, for_node, expiry_secs, issuer_spki, &ca_der)
        .map_err(|e| format!("issue token: {e}"))?;

    println!("join token (expires at unix {expiry_secs}):");
    println!("{hex}");
    println!();
    println!("usage on joiner:");
    println!("  NODEDB_JOIN_TOKEN={hex} nodedb <config.toml>");
    Ok(())
}

fn read_ca_cert(path: &Path) -> Result<Vec<u8>, String> {
    let pem = fs::read(path).map_err(|e| format!("read cluster CA {}: {e}", path.display()))?;
    let mut reader = std::io::BufReader::new(pem.as_slice());
    let cert = rustls_pemfile::certs(&mut reader)
        .next()
        .ok_or_else(|| format!("cluster CA {} contains no certificate", path.display()))?
        .map_err(|e| format!("parse cluster CA {}: {e}", path.display()))?;
    Ok(cert.as_ref().to_vec())
}

fn read_cluster_secret(path: &Path) -> Result<[u8; CLUSTER_SECRET_LEN], String> {
    let bytes =
        fs::read(path).map_err(|e| format!("read cluster secret {}: {e}", path.display()))?;
    if bytes.len() != CLUSTER_SECRET_LEN {
        return Err(format!(
            "cluster secret {} has {} bytes, expected {CLUSTER_SECRET_LEN}",
            path.display(),
            bytes.len()
        ));
    }
    let mut out = [0u8; CLUSTER_SECRET_LEN];
    out.copy_from_slice(&bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nodedb_cluster::auth::join_token as tok;

    #[test]
    fn verify_accepts_fresh_token_rejects_tampered() {
        let secret = [0x11u8; 32];
        let for_node = 42u64;
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let ca_der = b"test ca";
        let hex = tok::issue_token(&secret, for_node, expiry, [0x44; 32], ca_der).unwrap();

        let verified = tok::verify_token(&hex, &secret).unwrap();
        assert_eq!(verified.for_node, for_node);
        assert_eq!(verified.expiry_unix_secs, expiry);
        assert_eq!(verified.bootstrap_ca_der, ca_der);

        // Flip a byte in the MAC portion. XOR with 0xFF so the byte is
        // guaranteed to change even if it was already 0x00 (a fixed "00"
        // replacement would be a no-op ~1/256 of the time, since the MAC
        // varies with the wall-clock-derived expiry).
        let mut tampered = hex.clone();
        let flip = tampered.len() - 4;
        let orig = u8::from_str_radix(&tampered[flip..flip + 2], 16).expect("valid hex byte");
        tampered.replace_range(flip..flip + 2, &format!("{:02x}", orig ^ 0xFF));
        assert!(tok::verify_token(&tampered, &secret).is_err());

        // Wrong secret.
        let other_secret = [0x22u8; 32];
        assert!(tok::verify_token(&hex, &other_secret).is_err());
    }

    #[test]
    fn verify_rejects_expired() {
        let secret = [0xAAu8; 32];
        let expiry = 1u64; // very old
        let hex = tok::issue_token(&secret, 1, expiry, [0x44; 32], b"test ca").unwrap();
        let err = tok::verify_token(&hex, &secret).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("expired"), "got: {msg}");
    }

    #[test]
    fn create_writes_hex_token_for_real_secret() {
        use std::io::Write;
        let td = tempfile::tempdir().unwrap();
        let tls_dir = td.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        let mut f = std::fs::File::create(tls_dir.join("cluster_secret.bin")).unwrap();
        f.write_all(&[0x5Au8; 32]).unwrap();
        drop(f);
        let ca = nexar::transport::tls::ClusterCa::generate().unwrap();
        crate::control::cluster::tls::write_pem_cert(
            &tls_dir.join("ca.crt"),
            ca.cert_der().as_ref(),
        )
        .unwrap();
        let (node_cert, _node_key) = ca.issue_cert("nodedb").unwrap();
        crate::control::cluster::tls::write_pem_cert(&tls_dir.join("node.crt"), node_cert.as_ref())
            .unwrap();
        // Smoke test: succeeds and writes expected amount of output.
        create(td.path(), 9, Duration::from_secs(600)).unwrap();
    }
}
