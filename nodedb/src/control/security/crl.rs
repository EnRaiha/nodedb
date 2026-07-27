// SPDX-License-Identifier: BUSL-1.1

//! Certificate Revocation List (CRL) loading and checking.
//!
//! When `crl_path` is configured, the CRL file is loaded on startup and
//! periodically reloaded at `cert_reload_interval_secs`. During mTLS
//! handshake, the client certificate's serial number is checked against
//! the revoked serials set.
//!
//! ## CRL format
//!
//! PEM-encoded X.509 CRL (-----BEGIN X509 CRL-----). Multiple CRLs may
//! be concatenated in a single file.
//!
//! ## Integration
//!
//! `CrlStore` is wrapped in `Arc<RwLock<>>` and shared between the TLS
//! acceptor (reads) and the CRL reload background task (writes).

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, warn};

/// A set of revoked certificate serial numbers.
///
/// Serial numbers are stored as hex strings (uppercase) for fast lookup.
/// This is transport-agnostic — it doesn't depend on rustls internals.
#[derive(Debug, Clone, Default)]
pub struct CrlStore {
    /// Set of revoked serial numbers (hex-encoded, uppercase).
    revoked_serials: HashSet<String>,
    /// When the CRL was last loaded (epoch microseconds).
    loaded_at_us: u64,
    /// Number of times the CRL has been reloaded.
    reload_count: u64,
}

impl CrlStore {
    /// Create an empty CRL store (no revocations).
    pub fn new() -> Self {
        Self::default()
    }

    /// Load revoked serials from a PEM-encoded CRL file.
    ///
    /// Parses the CRL, extracts all revoked certificate serial numbers,
    /// and stores them as hex strings for O(1) lookup.
    pub fn load_from_file(path: &Path) -> Result<Self, CrlError> {
        let pem_data = fs::read(path).map_err(|e| CrlError::IoError {
            path: path.display().to_string(),
            detail: e.to_string(),
        })?;

        Self::load_from_pem(&pem_data)
    }

    /// Load from PEM bytes (for testing without files).
    pub fn load_from_pem(pem_data: &[u8]) -> Result<Self, CrlError> {
        let mut revoked_serials = HashSet::new();

        // Parse PEM blocks looking for X509 CRL entries.
        for pem in pem::parse_many(pem_data).map_err(|e| CrlError::ParseError {
            detail: format!("PEM parse failed: {e}"),
        })? {
            if pem.tag() != "X509 CRL" {
                continue;
            }

            // Parse the DER-encoded CRL.
            let serials = parse_crl_der(pem.contents())?;
            revoked_serials.extend(serials);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        info!(revoked_count = revoked_serials.len(), "CRL loaded");

        Ok(Self {
            revoked_serials,
            loaded_at_us: now,
            reload_count: 0,
        })
    }

    /// Check if a certificate serial number is revoked.
    ///
    /// `serial_hex` should be the certificate's serial number as a
    /// hex string (uppercase, no leading zeros stripped).
    pub fn is_revoked(&self, serial_hex: &str) -> bool {
        self.revoked_serials.contains(serial_hex)
    }

    /// Number of revoked serials in the store.
    pub fn revoked_count(&self) -> usize {
        self.revoked_serials.len()
    }

    /// When the CRL was last loaded.
    pub fn loaded_at_us(&self) -> u64 {
        self.loaded_at_us
    }

    /// Number of CRL reloads.
    pub fn reload_count(&self) -> u64 {
        self.reload_count
    }

    /// Reload the CRL from file, replacing the current revoked set.
    pub fn reload_from_file(&mut self, path: &Path) -> Result<(), CrlError> {
        let new = Self::load_from_file(path)?;
        self.revoked_serials = new.revoked_serials;
        self.loaded_at_us = new.loaded_at_us;
        self.reload_count += 1;
        Ok(())
    }
}

/// Parse a DER-encoded CRL and extract revoked serial numbers as hex strings.
///
/// Uses a minimal ASN.1 parser to extract the revokedCertificates sequence
/// from the TBSCertList structure (RFC 5280 Section 5.1).
fn parse_crl_der(der: &[u8]) -> Result<Vec<String>, CrlError> {
    // CRL structure (RFC 5280):
    //   CertificateList ::= SEQUENCE {
    //     tbsCertList     TBSCertList,
    //     signatureAlgorithm AlgorithmIdentifier,
    //     signatureValue  BIT STRING
    //   }
    //   TBSCertList ::= SEQUENCE {
    //     version         INTEGER OPTIONAL,
    //     signature       AlgorithmIdentifier,
    //     issuer          Name,
    //     thisUpdate      Time,
    //     nextUpdate      Time OPTIONAL,
    //     revokedCertificates SEQUENCE OF SEQUENCE {
    //       userCertificate  CertificateSerialNumber,
    //       revocationDate   Time,
    //       crlEntryExtensions Extensions OPTIONAL
    //     } OPTIONAL,
    //     crlExtensions   [0] EXPLICIT Extensions OPTIONAL
    //   }
    //
    // We use x509-parser for robust ASN.1 handling.

    use x509_parser::prelude::FromDer;
    use x509_parser::revocation_list::CertificateRevocationList;

    let (_, crl) = CertificateRevocationList::from_der(der).map_err(|e| CrlError::ParseError {
        detail: format!("CRL DER parse failed: {e}"),
    })?;

    let mut serials = Vec::new();
    for revoked in crl.iter_revoked_certificates() {
        let serial = revoked.raw_serial_as_string();
        serials.push(serial.to_uppercase());
    }

    Ok(serials)
}

/// Thread-safe, reloadable CRL store.
pub type SharedCrlStore = Arc<RwLock<CrlStore>>;

/// Create a shared CRL store from a file path.
///
/// Returns `None` if no CRL path is configured.
pub fn load_shared_crl(crl_path: Option<&Path>) -> Result<Option<SharedCrlStore>, CrlError> {
    match crl_path {
        Some(path) => {
            let store = CrlStore::load_from_file(path)?;
            Ok(Some(Arc::new(RwLock::new(store))))
        }
        None => Ok(None),
    }
}

/// Spawn a background task that periodically reloads the CRL.
pub fn spawn_crl_reload_task(
    crl_store: SharedCrlStore,
    crl_path: std::path::PathBuf,
    interval_secs: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(interval_secs);
        info!(
            interval_secs,
            path = %crl_path.display(),
            "CRL reload task started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("CRL reload task stopped");
                        return;
                    }
                }
            }

            let mut store = crl_store.write();
            if let Err(e) = store.reload_from_file(&crl_path) {
                warn!(
                    error = %e,
                    path = %crl_path.display(),
                    "CRL reload failed"
                );
            }
        }
    })
}

/// Check a client certificate serial against the CRL store.
///
/// Returns `true` if the certificate should be rejected (is revoked).
pub fn check_revocation(crl_store: &SharedCrlStore, serial_hex: &str) -> bool {
    crl_store.read().is_revoked(serial_hex)
}

/// CRL-related errors.
#[derive(Debug, thiserror::Error)]
pub enum CrlError {
    #[error("CRL I/O error ({path}): {detail}")]
    IoError { path: String, detail: String },

    #[error("CRL parse error: {detail}")]
    ParseError { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_crl_store() {
        let store = CrlStore::new();
        assert_eq!(store.revoked_count(), 0);
        assert!(!store.is_revoked("DEADBEEF"));
    }

    #[test]
    fn manual_serial_check() {
        let mut store = CrlStore::new();
        store.revoked_serials.insert("0A".into());
        store.revoked_serials.insert("FF".into());

        assert!(store.is_revoked("0A"));
        assert!(store.is_revoked("FF"));
        assert!(!store.is_revoked("0B"));
    }

    #[test]
    fn revocation_remains_enforced_after_panic_while_write_locked() {
        let shared = Arc::new(RwLock::new(CrlStore::new()));
        shared.write().revoked_serials.insert("ABCD".into());

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = shared.write();
            panic!("simulated interrupted CRL update");
        }));
        assert!(outcome.is_err());
        assert!(check_revocation(&shared, "ABCD"));
    }

    #[test]
    fn shared_crl_no_path() {
        let result = load_shared_crl(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn shared_crl_nonexistent_path() {
        let result = load_shared_crl(Some(Path::new("/nonexistent/crl.pem")));
        assert!(result.is_err());
    }

    #[test]
    fn revocation_check_thread_safe() {
        let store = Arc::new(RwLock::new(CrlStore::new()));
        store.write().revoked_serials.insert("AA".into());

        assert!(check_revocation(&store, "AA"));
        assert!(!check_revocation(&store, "BB"));
    }
}
