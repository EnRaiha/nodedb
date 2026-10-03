// SPDX-License-Identifier: BUSL-1.1

//! The node incarnation: a random id minted once per data directory.
//!
//! It is part of every archive key. A wiped data directory mints a new one, so
//! a node that reuses its node id never matches the objects its earlier life
//! uploaded.

use std::path::Path;

/// File in the data directory that holds the incarnation.
const INCARNATION_FILE: &str = "node_incarnation";

/// Random bytes in an incarnation.
const INCARNATION_BYTES: usize = 16;

/// A 128-bit random id, written as 32 lowercase hex digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incarnation(String);

impl Incarnation {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        let valid = raw.len() == INCARNATION_BYTES * 2
            && raw
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        valid.then(|| Self(raw.to_owned()))
    }
}

/// Read the incarnation from `data_dir`, or mint and persist one when the
/// directory has none. A file that exists but does not parse is an error:
/// replacing it orphans every object archived under the old id.
///
/// An empty `data_dir` names a node with no data directory. Such a node keeps
/// no WAL to archive, so it is refused rather than resolved against the
/// process's working directory.
pub fn load_or_mint_incarnation(data_dir: &Path) -> crate::Result<Incarnation> {
    if data_dir.as_os_str().is_empty() {
        return Err(crate::Error::Storage {
            engine: "wal_archive".into(),
            detail: "WAL archiving needs a data directory; this node has none".into(),
        });
    }
    let path = data_dir.join(INCARNATION_FILE);
    match std::fs::read_to_string(&path) {
        Ok(raw) => Incarnation::parse(&raw).ok_or_else(|| crate::Error::Storage {
            engine: "wal_archive".into(),
            detail: format!(
                "{} holds no valid node incarnation (32 hex digits); \
                 remove it only if the data directory is new",
                path.display()
            ),
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => mint(data_dir),
        Err(e) => Err(crate::Error::Io(e)),
    }
}

fn mint(data_dir: &Path) -> crate::Result<Incarnation> {
    let mut bytes = [0u8; INCARNATION_BYTES];
    getrandom::fill(&mut bytes).map_err(|error| crate::Error::Storage {
        engine: "wal_archive".into(),
        detail: format!("mint node incarnation: {error}"),
    })?;
    let id = hex::encode(bytes);
    std::fs::create_dir_all(data_dir)?;
    nodedb_wal::segment::atomic_write_fsync(data_dir, INCARNATION_FILE, id.as_bytes())
        .map_err(crate::Error::Wal)?;
    Ok(Incarnation(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incarnation_is_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_mint_incarnation(dir.path()).unwrap();
        let again = load_or_mint_incarnation(dir.path()).unwrap();
        assert_eq!(first, again);
        assert_eq!(first.as_str().len(), 32);
    }

    #[test]
    fn a_wiped_data_directory_mints_a_new_incarnation() {
        let before = tempfile::tempdir().unwrap();
        let after = tempfile::tempdir().unwrap();
        assert_ne!(
            load_or_mint_incarnation(before.path()).unwrap(),
            load_or_mint_incarnation(after.path()).unwrap()
        );
    }

    #[test]
    fn a_corrupt_incarnation_file_is_an_error_not_a_new_id() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(INCARNATION_FILE), b"not-hex").unwrap();
        assert!(load_or_mint_incarnation(dir.path()).is_err());
    }

    #[test]
    fn a_node_with_no_data_directory_writes_no_incarnation() {
        assert!(load_or_mint_incarnation(std::path::Path::new("")).is_err());
        assert!(!std::path::Path::new(INCARNATION_FILE).exists());
    }
}
