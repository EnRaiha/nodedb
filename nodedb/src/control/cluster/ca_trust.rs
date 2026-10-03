// SPDX-License-Identifier: BUSL-1.1

//! The overlap CA trust set under `tls/ca.d/`: one PEM file per CA, named by
//! its fingerprint.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use nodedb_cluster::transport::pki_types::CertificateDer;

use super::tls::{CA_TRUST_DIR, read_single_cert, write_pem_cert};

/// Load every PEM-encoded CA certificate from `tls_dir/ca.d/*.crt`,
/// sorted by filename for deterministic output. Missing directory is
/// treated as "no overlap CAs" and returns an empty vec.
pub(crate) fn load_extra_cas(tls_dir: &Path) -> crate::Result<Vec<CertificateDer<'static>>> {
    let dir = tls_dir.join(CA_TRUST_DIR);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let listing = fs::read_dir(&dir).map_err(|e| crate::Error::Config {
        detail: format!("read ca.d {}: {e}", dir.display()),
    })?;
    let entries = crt_paths(&dir, listing.map(|r| r.map(|e| e.path())))?;
    let mut out = Vec::with_capacity(entries.len());
    for p in entries {
        out.push(read_single_cert(&p)?);
    }
    Ok(out)
}

/// The `.crt` paths of a `ca.d` listing, sorted by file name.
///
/// An entry the listing cannot read fails the load. Skipping it will
/// silently drop a trusted CA.
fn crt_paths(
    dir: &Path,
    listing: impl Iterator<Item = std::io::Result<PathBuf>>,
) -> crate::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in listing {
        let path = entry.map_err(|e| crate::Error::Config {
            detail: format!("read ca.d entry in {}: {e}", dir.display()),
        })?;
        if path.extension().and_then(|s| s.to_str()) == Some("crt") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Write a PEM-encoded CA cert into `tls_dir/ca.d/<fp_hex>.crt`.
/// Called by the production applier when a `CaTrustChange { add: ... }`
/// entry commits.
pub fn write_trusted_ca(tls_dir: &Path, ca_der: &[u8]) -> crate::Result<[u8; 32]> {
    let dir = tls_dir.join(CA_TRUST_DIR);
    fs::create_dir_all(&dir).map_err(|e| crate::Error::Config {
        detail: format!("create ca.d dir {}: {e}", dir.display()),
    })?;
    let cert = CertificateDer::from(ca_der.to_vec());
    let fp = nodedb_cluster::ca_fingerprint(&cert);
    let name = format!("{}.crt", nodedb_cluster::ca_fingerprint_hex(&fp));
    write_pem_cert(&dir, &name, ca_der)?;
    Ok(fp)
}

/// Delete the overlap-CA file identified by `fp` from `tls_dir/ca.d/`.
/// No-op (and returns `Ok(())`) when the file isn't present — applier
/// behaviour must be idempotent across re-apply and snapshot replay.
pub fn remove_trusted_ca(tls_dir: &Path, fp: &[u8; 32]) -> crate::Result<()> {
    let dir = tls_dir.join(CA_TRUST_DIR);
    let path = dir.join(format!("{}.crt", nodedb_cluster::ca_fingerprint_hex(fp)));
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::Error::Config {
            detail: format!("remove ca.d entry {}: {e}", path.display()),
        }),
    }
}

/// DER of every CA in `tls_dir/ca.d/`, sorted by file name.
pub fn trusted_ca_ders(tls_dir: &Path) -> crate::Result<Vec<Vec<u8>>> {
    Ok(load_extra_cas(tls_dir)?
        .into_iter()
        .map(|cert| cert.as_ref().to_vec())
        .collect())
}

/// Make `tls_dir/ca.d/` hold exactly the CAs in `ders`: write each, then
/// remove every other CA file.
pub fn replace_trusted_cas(tls_dir: &Path, ders: &[Vec<u8>]) -> crate::Result<()> {
    let mut keep: HashSet<[u8; 32]> = HashSet::with_capacity(ders.len());
    for der in ders {
        keep.insert(write_trusted_ca(tls_dir, der)?);
    }
    for der in trusted_ca_ders(tls_dir)? {
        let fp = nodedb_cluster::ca_fingerprint(&CertificateDer::from(der));
        if !keep.contains(&fp) {
            remove_trusted_ca(tls_dir, &fp)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listing entry that fails to read fails the load with a config
    /// error naming the directory.
    #[test]
    fn unreadable_entry_fails_the_load() {
        let dir = Path::new("/etc/nodedb/tls/ca.d");
        let listing = vec![
            Ok(dir.join("a.crt")),
            Err(std::io::Error::other("entry vanished")),
            Ok(dir.join("b.crt")),
        ];
        let err = crt_paths(dir, listing.into_iter()).expect_err("an unreadable entry must fail");
        let crate::Error::Config { detail } = err else {
            panic!("expected a config error, got {err:?}");
        };
        assert!(detail.contains("/etc/nodedb/tls/ca.d"), "detail: {detail}");
        assert!(detail.contains("entry vanished"), "detail: {detail}");
    }

    /// Only `.crt` entries load, sorted by file name.
    #[test]
    fn crt_entries_load_sorted() {
        let dir = Path::new("/tls/ca.d");
        let listing = vec![
            Ok(dir.join("b.crt")),
            Ok(dir.join("notes.txt")),
            Ok(dir.join("a.crt")),
        ];
        let paths = crt_paths(dir, listing.into_iter()).expect("readable listing");
        assert_eq!(paths, vec![dir.join("a.crt"), dir.join("b.crt")]);
    }
}
