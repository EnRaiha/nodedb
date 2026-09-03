// SPDX-License-Identifier: BUSL-1.1

//! Implicit-sequence enumeration for `PurgeCollection` cascade.
//!
//! `SERIAL` / `BIGSERIAL` columns transparently create a sequence
//! named `{collection}_{field}_seq` at `CREATE TABLE` time. Those
//! sequences are owned by the collection even though there is no
//! explicit FK — when the collection is dropped with `CASCADE`, they
//! must go too.
//!
//! The enumeration is deliberately conservative: we only match
//! sequence names of the form `{collection}_<suffix>_seq`. A
//! user-named sequence that happens to share the prefix by coincidence
//! is treated as a dependent, which is the safe side — the operator
//! must explicitly re-create it if they want to keep it. The
//! alternative (look up declared `SERIAL` columns on the collection)
//! would miss sequences created by earlier schema versions whose
//! metadata has since changed.

use crate::control::security::catalog::SystemCatalog;

/// Enumerate implicit sequences owned by `(database_id, tenant_id,
/// collection)`. Returns sequence names only.
pub fn find_implicit_sequences(
    catalog: &SystemCatalog,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let prefix = format!("{collection}_");
    let all = catalog.load_sequences_for_tenant(database_id, tenant_id)?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter_map(|s| {
            if s.name.starts_with(&prefix) && s.name.ends_with("_seq") {
                Some(s.name)
            } else {
                None
            }
        })
        .collect();
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use tempfile::TempDir;

    fn cat() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        (cat, tmp)
    }

    fn seq(database: u64, tenant: u64, name: &str) -> StoredSequence {
        StoredSequence::new(database, tenant, name.to_string(), "o".into())
    }

    #[test]
    fn matches_implicit_serial_pattern() {
        let (c, _t) = cat();
        c.put_sequence(&seq(4, 1, "users_id_seq")).unwrap();
        c.put_sequence(&seq(4, 1, "users_created_at_seq")).unwrap();
        c.put_sequence(&seq(4, 1, "orders_id_seq")).unwrap();
        let found = find_implicit_sequences(&c, 4, 1, "users").unwrap();
        assert_eq!(found, vec!["users_created_at_seq", "users_id_seq"]);
    }

    #[test]
    fn skips_cross_tenant() {
        let (c, _t) = cat();
        c.put_sequence(&seq(4, 1, "users_id_seq")).unwrap();
        c.put_sequence(&seq(4, 2, "users_id_seq")).unwrap();
        assert_eq!(find_implicit_sequences(&c, 4, 2, "users").unwrap().len(), 1);
    }

    #[test]
    fn skips_another_database() {
        let (c, _t) = cat();
        c.put_sequence(&seq(4, 1, "users_id_seq")).unwrap();
        c.put_sequence(&seq(5, 1, "users_id_seq")).unwrap();
        assert_eq!(find_implicit_sequences(&c, 5, 1, "users").unwrap().len(), 1);
    }

    #[test]
    fn ignores_non_seq_suffix() {
        let (c, _t) = cat();
        c.put_sequence(&seq(4, 1, "users_id_counter")).unwrap();
        assert!(
            find_implicit_sequences(&c, 4, 1, "users")
                .unwrap()
                .is_empty()
        );
    }
}
