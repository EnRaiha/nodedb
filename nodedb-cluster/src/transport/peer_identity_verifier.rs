// SPDX-License-Identifier: BUSL-1.1

//! Peer-identity verification for the Raft mTLS transport.
//!
//! After the TLS handshake completes, rustls has verified that the peer's
//! certificate chains to the cluster CA (or an overlap-window CA during
//! rotation).  That is a *cluster membership* check, not a *node identity*
//! check: any node with a valid CA-signed cert can pretend to be any other
//! node, and a brief CA compromise promotes an attacker to full Raft voter
//! status.
//!
//! This module adds a second layer: after the TLS handshake we compare the
//! peer's leaf cert against the pinned identity we recorded for that node_id
//! during the join flow.  Two mechanisms are tried in order:
//!
//! 1. **SPIFFE URI SAN** — if both the pinned record and the peer's cert
//!    carry a `spiffe://` URI SAN, compare them for equality.
//! 2. **SPKI fingerprint** — SHA-256 digest of the peer's SubjectPublicKeyInfo
//!    DER blob (the public-key algorithm + key material, minus the cert
//!    metadata).  Compared against the 32-byte pin stored in `NodeInfo`.
//!
//! A topology peer is accepted only when at least one mechanism matches.
//! Legacy `NodeInfo` rows carrying neither identity are rejected. Initial seed
//! discovery and newly issued joiner enrollment are separately bounded by the
//! TLS verifier and the identity-bound JoinRequest admission path.
//!
//! # Key-rotation SPKI overlap (known gap)
//!
//! During CA rotation the cluster temporarily trusts two CAs simultaneously
//! (see the `additional_ca_certs` field on `TlsCredentials`).  While the
//! overlap window is open, every node re-issues its leaf cert under the new
//! CA.  The SPKI pin captures the *public key*, not the *signature*.  If an
//! operator re-keys the node (generates a new key-pair and issues a new cert)
//! rather than merely re-signing the old key, the SPKI pin changes and the
//! node will be rejected until an authorised admin updates its `NodeInfo`
//! through the metadata Raft group.  Operators who want zero-downtime rotation
//! should re-sign the *same* public key under the new CA (SPKI pin unchanged),
//! or pre-register the new SPKI pin before cutting over.

use sha2::{Digest, Sha256};
use tracing::{debug, warn};
use x509_parser::prelude::*;

use crate::error::{ClusterError, Result};
use crate::topology::NodeInfo;

/// QUIC transport error code sent when peer-identity verification fails.
///
/// Carried as a QUIC application-layer error code in the `CONNECTION_CLOSE`
/// frame (`error_code = 0x02`).  Peers that receive this code know
/// deterministically that they failed the identity pin check (as opposed to
/// a codec error `0x01` or a version mismatch `0x01`).
pub const IDENTITY_MISMATCH_QUIC_ERROR: quinn::VarInt = quinn::VarInt::from_u32(0x02);

/// Outcome of identity verification.
#[derive(Debug, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// The peer's certificate matched the pinned identity via the given method.
    Accepted { method: VerifyMethod },
    /// The peer's identity did not match any pinned value.
    Rejected,
}

/// Which verification method produced a positive match.
#[derive(Debug, PartialEq, Eq)]
pub enum VerifyMethod {
    Spiffe,
    SpkiPin,
}

/// Extract the SHA-256 SPKI fingerprint from a DER-encoded leaf certificate.
///
/// The SPKI blob is the `SubjectPublicKeyInfo` field of the cert (algorithm
/// OID + key material).  This is stable across cert renewals that keep the
/// same key-pair — it changes only when the key-pair changes.
pub fn spki_pin_from_cert_der(cert_der: &[u8]) -> Result<[u8; 32]> {
    let (_, cert) = X509Certificate::from_der(cert_der).map_err(|e| ClusterError::Transport {
        detail: format!("parse peer cert for SPKI pin: {e}"),
    })?;
    let spki_der = cert.public_key().raw;
    let digest: [u8; 32] = Sha256::digest(spki_der).into();
    Ok(digest)
}

/// Extract the first `spiffe://` URI SAN from a DER-encoded leaf certificate.
///
/// Returns `None` when the cert carries no URI SAN or when none of the URI
/// SANs use the `spiffe://` scheme.
pub fn enrollment_identity_from_cert_der(cert_der: &[u8]) -> Option<(u64, u64)> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    for san in cert
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|ext| &ext.value.general_names)
        .into_iter()
        .flatten()
    {
        let GeneralName::DNSName(name) = san else {
            continue;
        };
        let Some(rest) = name.strip_prefix("nodedb-enrollment-") else {
            continue;
        };
        let (node_id, expires_at_ms) = rest.split_once('-')?;
        return Some((node_id.parse().ok()?, expires_at_ms.parse().ok()?));
    }
    None
}

pub fn spiffe_id_from_cert_der(cert_der: &[u8]) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    for san in cert
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|ext| &ext.value.general_names)
        .into_iter()
        .flatten()
    {
        if let GeneralName::URI(uri) = san
            && uri.starts_with("spiffe://")
        {
            return Some(uri.to_string());
        }
    }
    None
}

/// Verify that the TLS-presented leaf certificate matches the pinned identity
/// recorded for `node_id` in the topology.
///
/// # Arguments
///
/// * `node_id` — the `from_node_id` declared in the HMAC envelope (already
///   MAC-verified by the caller).  Used to look up `NodeInfo` in `info`.
/// * `info` — `NodeInfo` for the peer, as stored in the cluster topology.
///   Carries the SPIFFE id and SPKI pin recorded at join time.
/// * `peer_cert_der` — the peer's leaf certificate in DER form, extracted
///   from the TLS connection after the handshake.
///
/// Returns [`VerifyOutcome::Accepted`] on success and
/// [`VerifyOutcome::Rejected`] on mismatch or a legacy unpinned topology row.
pub fn verify_peer_identity(info: &NodeInfo, peer_cert_der: &[u8]) -> VerifyOutcome {
    let has_any_pin = info.spiffe_id.is_some() || info.spki_pin.is_some();

    if !has_any_pin {
        warn!(
            node_id = info.node_id,
            "rejecting topology peer with no pinned identity"
        );
        return VerifyOutcome::Rejected;
    }

    // --- SPIFFE first ---
    if let Some(pinned_spiffe) = &info.spiffe_id {
        let peer_spiffe = spiffe_id_from_cert_der(peer_cert_der);
        match peer_spiffe {
            Some(ref s) if s == pinned_spiffe => {
                debug!(
                    node_id = info.node_id,
                    spiffe = %s,
                    "peer identity verified via SPIFFE URI SAN"
                );
                return VerifyOutcome::Accepted {
                    method: VerifyMethod::Spiffe,
                };
            }
            Some(ref s) => {
                // Peer has a SPIFFE id but it doesn't match.  Fall through to
                // SPKI check — the operator may have issued a new cert with a
                // different SPIFFE id but kept the same key-pair.
                debug!(
                    node_id = info.node_id,
                    expected = %pinned_spiffe,
                    got = %s,
                    "SPIFFE URI SAN mismatch; trying SPKI pin"
                );
            }
            None => {
                debug!(
                    node_id = info.node_id,
                    "peer cert carries no SPIFFE URI SAN; trying SPKI pin"
                );
            }
        }
    }

    // --- SPKI pin fallback ---
    if let Some(pinned_spki) = &info.spki_pin {
        let peer_spki = match spki_pin_from_cert_der(peer_cert_der) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    node_id = info.node_id,
                    error = %e,
                    "failed to extract SPKI pin from peer cert; rejecting"
                );
                return VerifyOutcome::Rejected;
            }
        };

        if &peer_spki == pinned_spki {
            debug!(
                node_id = info.node_id,
                "peer identity verified via SPKI fingerprint"
            );
            return VerifyOutcome::Accepted {
                method: VerifyMethod::SpkiPin,
            };
        }

        warn!(
            node_id = info.node_id,
            "SPKI pin mismatch — rejecting connection"
        );
    }

    VerifyOutcome::Rejected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeInfo, NodeState};

    fn make_node_no_pin(node_id: u64) -> NodeInfo {
        NodeInfo::new(
            node_id,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        )
    }

    fn make_node_spki(node_id: u64, pin: [u8; 32]) -> NodeInfo {
        let mut n = make_node_no_pin(node_id);
        n.spki_pin = Some(pin);
        n
    }

    fn make_node_spiffe(node_id: u64, spiffe: &str) -> NodeInfo {
        let mut n = make_node_no_pin(node_id);
        n.spiffe_id = Some(spiffe.to_string());
        n
    }

    /// Build a minimal self-signed cert DER using rcgen, extract the SPKI pin
    /// from it via our code, then verify that the pin matches.
    fn gen_cert_and_pin() -> (Vec<u8>, [u8; 32]) {
        use rcgen::{CertificateParams, KeyPair};
        let key = KeyPair::generate().unwrap();
        let params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let cert_der = cert.der().to_vec();
        let pin = spki_pin_from_cert_der(&cert_der).unwrap();
        (cert_der, pin)
    }

    #[test]
    fn topology_peer_without_pins_is_rejected() {
        let node = make_node_no_pin(1);
        let outcome = verify_peer_identity(&node, &[]);
        assert_eq!(outcome, VerifyOutcome::Rejected);
    }

    #[test]
    fn spki_match_accepted() {
        let (cert_der, pin) = gen_cert_and_pin();
        let node = make_node_spki(2, pin);
        let outcome = verify_peer_identity(&node, &cert_der);
        assert_eq!(
            outcome,
            VerifyOutcome::Accepted {
                method: VerifyMethod::SpkiPin
            }
        );
    }

    #[test]
    fn spki_mismatch_rejected() {
        let (cert_der, _pin) = gen_cert_and_pin();
        let wrong_pin = [0xFFu8; 32];
        let node = make_node_spki(3, wrong_pin);
        let outcome = verify_peer_identity(&node, &cert_der);
        assert_eq!(outcome, VerifyOutcome::Rejected);
    }

    #[test]
    fn spiffe_match_accepted() {
        use rcgen::{CertificateParams, Ia5String, KeyPair, SanType};
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let spiffe_uri =
            Ia5String::try_from("spiffe://cluster.local/ns/nodedb/node/42".to_string()).unwrap();
        params.subject_alt_names.push(SanType::URI(spiffe_uri));
        let cert = params.self_signed(&key).unwrap();
        let cert_der = cert.der().to_vec();

        let node = make_node_spiffe(42, "spiffe://cluster.local/ns/nodedb/node/42");
        let outcome = verify_peer_identity(&node, &cert_der);
        assert_eq!(
            outcome,
            VerifyOutcome::Accepted {
                method: VerifyMethod::Spiffe
            }
        );
    }

    #[test]
    fn spiffe_mismatch_falls_through_to_spki_rejection() {
        use rcgen::{CertificateParams, Ia5String, KeyPair, SanType};
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let spiffe_uri =
            Ia5String::try_from("spiffe://cluster.local/ns/nodedb/node/99".to_string()).unwrap();
        params.subject_alt_names.push(SanType::URI(spiffe_uri));
        let cert = params.self_signed(&key).unwrap();
        let cert_der = cert.der().to_vec();

        // Pin SPIFFE to a different id, no SPKI pin.
        let node = make_node_spiffe(42, "spiffe://cluster.local/ns/nodedb/node/42");
        let outcome = verify_peer_identity(&node, &cert_der);
        // SPIFFE mismatched, no SPKI pin → rejected.
        assert_eq!(outcome, VerifyOutcome::Rejected);
    }

    #[test]
    fn envelope_node_id_cross_check_semantics() {
        // The cross-check between the HMAC envelope's from_node_id and the
        // TLS-derived node_id is exercised in server.rs integration tests.
        // Here we verify that verify_peer_identity is node-id-agnostic: two
        // distinct NodeInfo records with different node_ids but the same SPKI
        // pin both pass with the same cert.
        let (cert_der, pin) = gen_cert_and_pin();
        let node_a = make_node_spki(10, pin);
        let node_b = make_node_spki(20, pin);
        assert_eq!(
            verify_peer_identity(&node_a, &cert_der),
            VerifyOutcome::Accepted {
                method: VerifyMethod::SpkiPin
            }
        );
        assert_eq!(
            verify_peer_identity(&node_b, &cert_der),
            VerifyOutcome::Accepted {
                method: VerifyMethod::SpkiPin
            }
        );
    }
}
