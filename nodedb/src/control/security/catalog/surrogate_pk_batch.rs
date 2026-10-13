// SPDX-License-Identifier: BUSL-1.1

//! Batched surrogate ↔ PK catalog ops for the `_system.surrogate_pk{,_rev}_v3`
//! tables.
//!
//! A batch binds many keys of one collection in one redb write transaction.
//! The transaction commits once with the database's default durability, so a
//! batch of N keys costs one durable sync instead of N.

use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use crate::types::SurrogateBindEntry;

use super::surrogate_hwm::{HWM_KEY, SURROGATE_HWM};
use super::types::{SURROGATE_PK_REV_V3, SURROGATE_PK_V3, SystemCatalog, catalog_err};

/// The outcome of binding one key in a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchBinding {
    /// The surrogate the key is bound to after the batch commits.
    pub surrogate: Surrogate,
    /// Whether this batch wrote the binding. `false` when the key was bound
    /// before the batch, or earlier in the same batch.
    pub inserted: bool,
}

impl SystemCatalog {
    /// Bind many keys of one collection in one write transaction, first-wins.
    ///
    /// A key already bound in the catalog keeps its binding. A key repeated in
    /// `rows` binds to its first surrogate. Every new binding writes the
    /// forward and the reverse row. Returns one [`BatchBinding`] per row, in
    /// `rows` order.
    pub fn put_surrogates(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        rows: &[(&[u8], Surrogate)],
    ) -> crate::Result<Vec<BatchBinding>> {
        self.put_surrogates_with_hwm(key, tenant_id, rows, None)
    }

    /// [`Self::put_surrogates`] that also writes the allocator high-watermark
    /// row in the same transaction when `hwm` is `Some`. The bindings and the
    /// watermark reach disk in one commit.
    pub fn put_surrogates_with_hwm(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        rows: &[(&[u8], Surrogate)],
        hwm: Option<u32>,
    ) -> crate::Result<Vec<BatchBinding>> {
        if rows.is_empty() && hwm.is_none() {
            return Ok(Vec::new());
        }
        let db_id = key.database_id().as_u64();
        let tid = tenant_id.as_u64();
        let collection = key.name();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk batch write txn", e))?;
        let mut out = Vec::with_capacity(rows.len());
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV_V3)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            for &(pk_bytes, surrogate) in rows {
                let existing = fwd
                    .get((db_id, tid, collection, pk_bytes))
                    .map_err(|e| catalog_err("get surrogate_pk", e))?
                    .map(|v| Surrogate::new(v.value()));
                if let Some(bound) = existing {
                    out.push(BatchBinding {
                        surrogate: bound,
                        inserted: false,
                    });
                    continue;
                }
                fwd.insert((db_id, tid, collection, pk_bytes), surrogate.as_u32())
                    .map_err(|e| catalog_err("insert surrogate_pk", e))?;
                rev.insert((db_id, tid, collection, surrogate.as_u32()), pk_bytes)
                    .map_err(|e| catalog_err("insert surrogate_pk_rev", e))?;
                out.push(BatchBinding {
                    surrogate,
                    inserted: true,
                });
            }
            if let Some(hwm) = hwm {
                let mut table = txn
                    .open_table(SURROGATE_HWM)
                    .map_err(|e| catalog_err("open surrogate_hwm", e))?;
                table
                    .insert(HWM_KEY, hwm)
                    .map_err(|e| catalog_err("insert surrogate_hwm", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk batch commit", e))?;
        Ok(out)
    }

    /// Insert or overwrite many bindings of one collection in one write
    /// transaction, as [`Self::put_surrogate`] does for one. A later row for a
    /// key replaces an earlier one. Used where the caller's bindings are
    /// authoritative, such as a Raft snapshot install.
    pub fn overwrite_surrogates(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        rows: &[(&[u8], Surrogate)],
    ) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let db_id = key.database_id().as_u64();
        let tid = tenant_id.as_u64();
        let collection = key.name();
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk overwrite txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK_V3)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV_V3)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            for &(pk_bytes, surrogate) in rows {
                fwd.insert((db_id, tid, collection, pk_bytes), surrogate.as_u32())
                    .map_err(|e| catalog_err("insert surrogate_pk", e))?;
                rev.insert((db_id, tid, collection, surrogate.as_u32()), pk_bytes)
                    .map_err(|e| catalog_err("insert surrogate_pk_rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk overwrite commit", e))
    }

    /// [`Self::overwrite_surrogates`] for bindings across collections: one
    /// write transaction per `(database, tenant, collection)` the entries name.
    pub fn overwrite_surrogate_entries(&self, entries: &[SurrogateBindEntry]) -> crate::Result<()> {
        let mut groups: Vec<(u64, u64, &str)> = Vec::new();
        for e in entries {
            let group = (e.database_id, e.tenant_id, e.collection.as_str());
            if !groups.contains(&group) {
                groups.push(group);
            }
        }
        for (database_id, tenant_id, collection) in groups {
            let rows: Vec<(&[u8], Surrogate)> = entries
                .iter()
                .filter(|e| {
                    e.database_id == database_id
                        && e.tenant_id == tenant_id
                        && e.collection == collection
                })
                .map(|e| (e.pk.as_slice(), Surrogate::new(e.surrogate)))
                .collect();
            self.overwrite_surrogates(
                CollectionKey::from_bare(DatabaseId::new(database_id), collection),
                TenantId::new(tenant_id),
                &rows,
            )?;
        }
        Ok(())
    }

    /// The surrogate bound to each of `pks` in one collection, in `pks` order,
    /// read in one read transaction. `None` for a key with no binding.
    pub fn get_surrogates_for_pks(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        pks: &[&[u8]],
    ) -> crate::Result<Vec<Option<Surrogate>>> {
        if pks.is_empty() {
            return Ok(Vec::new());
        }
        let db_id = key.database_id().as_u64();
        let tid = tenant_id.as_u64();
        let collection = key.name();
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk batch read txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK_V3)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        pks.iter()
            .map(|pk_bytes| {
                table
                    .get((db_id, tid, collection, *pk_bytes))
                    .map_err(|e| catalog_err("get surrogate_pk", e))
                    .map(|hit| hit.map(|v| Surrogate::new(v.value())))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};

    use super::BatchBinding;
    use crate::control::security::catalog::SystemCatalog;

    const T0: TenantId = TenantId::new(0);

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).expect("open catalog");
        (dir, cat)
    }

    fn users() -> CollectionKey<'static> {
        CollectionKey::from_bare(DatabaseId::DEFAULT, "users")
    }

    #[test]
    fn a_batch_writes_forward_and_reverse_rows() {
        let (_dir, cat) = open();
        let rows: [(&[u8], Surrogate); 2] =
            [(b"alice", Surrogate::new(3)), (b"bob", Surrogate::new(4))];
        let out = cat.put_surrogates(users(), T0, &rows).expect("put");
        assert!(out.iter().all(|b| b.inserted));
        for (pk, surrogate) in rows {
            assert_eq!(
                cat.get_surrogate_for_pk(users(), T0, pk).expect("get"),
                Some(surrogate)
            );
            assert_eq!(
                cat.get_pk_for_surrogate(users(), T0, surrogate)
                    .expect("get rev"),
                Some(pk.to_vec())
            );
        }
    }

    #[test]
    fn an_existing_binding_wins_over_the_batch() {
        let (_dir, cat) = open();
        cat.put_surrogate(users(), T0, b"alice", Surrogate::new(9))
            .expect("put");
        let rows: [(&[u8], Surrogate); 1] = [(b"alice", Surrogate::new(5))];
        let out = cat.put_surrogates(users(), T0, &rows).expect("put");
        assert_eq!(
            out,
            vec![BatchBinding {
                surrogate: Surrogate::new(9),
                inserted: false
            }]
        );
        assert_eq!(
            cat.get_pk_for_surrogate(users(), T0, Surrogate::new(5))
                .expect("get rev"),
            None,
            "a losing surrogate writes no reverse row"
        );
    }

    #[test]
    fn a_key_repeated_in_one_batch_binds_its_first_surrogate() {
        let (_dir, cat) = open();
        let rows: [(&[u8], Surrogate); 2] =
            [(b"alice", Surrogate::new(1)), (b"alice", Surrogate::new(2))];
        let out = cat.put_surrogates(users(), T0, &rows).expect("put");
        assert_eq!(out[0].surrogate, Surrogate::new(1));
        assert!(out[0].inserted);
        assert_eq!(out[1].surrogate, Surrogate::new(1));
        assert!(!out[1].inserted);
        assert_eq!(
            cat.get_pk_for_surrogate(users(), T0, Surrogate::new(2))
                .expect("get rev"),
            None
        );
    }

    #[test]
    fn the_watermark_commits_with_the_batch() {
        let (_dir, cat) = open();
        let rows: [(&[u8], Surrogate); 1] = [(b"alice", Surrogate::new(7))];
        cat.put_surrogates_with_hwm(users(), T0, &rows, Some(7))
            .expect("put");
        assert_eq!(cat.get_surrogate_hwm().expect("hwm"), 7);
    }

    #[test]
    fn a_batch_read_answers_in_key_order() {
        let (_dir, cat) = open();
        cat.put_surrogate(users(), T0, b"bob", Surrogate::new(2))
            .expect("put");
        let pks: [&[u8]; 2] = [b"alice", b"bob"];
        assert_eq!(
            cat.get_surrogates_for_pks(users(), T0, &pks).expect("get"),
            vec![None, Some(Surrogate::new(2))]
        );
    }
}
