// SPDX-License-Identifier: BUSL-1.1

//! Batched surrogate ↔ PK catalog ops on the surrogate PK map tables.
//!
//! A batch binds many keys of one collection in one redb write transaction.
//! The transaction commits once with the database's default durability, so a
//! batch of N keys costs one durable sync instead of N. A batch sorts its keys
//! and walks each table with one cursor (see [`super::surrogate_sorted`]).

use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};
use redb::ReadableDatabase;

use crate::types::SurrogateBindEntry;

use super::surrogate_hwm::{HWM_KEY, SURROGATE_HWM};
use super::surrogate_scope::{read_scope, rev_key, scope_or_create};
use super::surrogate_sorted::{SortedKeys, insert_rev_sorted, merge_insert, probe};
use super::types::{SURROGATE_PK, SURROGATE_PK_REV, SystemCatalog, catalog_err};

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
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk batch write txn", e))?;
        let mut out = Vec::with_capacity(rows.len());
        {
            if !rows.is_empty() {
                let scope = scope_or_create(&txn, key, tenant_id)?;
                let keys = SortedKeys::new(scope, rows.iter().map(|&(pk, _)| pk));
                // A repeated key binds the surrogate of its first occurrence.
                let carried = |u: usize| rows[keys.first(u)].1.as_u32();
                let before = {
                    let mut fwd = txn
                        .open_table(SURROGATE_PK)
                        .map_err(|e| catalog_err("open surrogate_pk", e))?;
                    merge_insert(&mut fwd, &keys, carried)?
                };
                let reverse: Vec<(u128, &[u8])> = (0..keys.len())
                    .filter(|&u| before[u].is_none())
                    .map(|u| (rev_key(scope, carried(u)), keys.pk(u)))
                    .collect();
                let mut rev = txn
                    .open_table(SURROGATE_PK_REV)
                    .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
                insert_rev_sorted(&mut rev, reverse)?;
                for i in 0..rows.len() {
                    let u = keys.unique_of(i);
                    out.push(match before[u] {
                        Some(bound) => BatchBinding {
                            surrogate: Surrogate::new(bound),
                            inserted: false,
                        },
                        None => BatchBinding {
                            surrogate: Surrogate::new(carried(u)),
                            inserted: keys.first(u) == i,
                        },
                    });
                }
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
    /// transaction. A later row for a key replaces an earlier one. A key that
    /// moves to a new surrogate loses the reverse row of its old one. Used
    /// where the caller's bindings are authoritative, such as a Raft snapshot
    /// install.
    pub fn overwrite_surrogates(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        rows: &[(&[u8], Surrogate)],
    ) -> crate::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk overwrite txn", e))?;
        {
            let scope = scope_or_create(&txn, key, tenant_id)?;
            let keys = SortedKeys::new(scope, rows.iter().map(|&(pk, _)| pk));
            // The last occurrence of a key is its authoritative surrogate.
            let mut last: Vec<u32> = vec![0; keys.len()];
            for (i, &(_, surrogate)) in rows.iter().enumerate() {
                last[keys.unique_of(i)] = surrogate.as_u32();
            }
            let mut stale: Vec<u128> = Vec::new();
            {
                let mut fwd = txn
                    .open_table(SURROGATE_PK)
                    .map_err(|e| catalog_err("open surrogate_pk", e))?;
                for (u, &surrogate) in last.iter().enumerate() {
                    let old = fwd
                        .insert(keys.key(u), surrogate)
                        .map_err(|e| catalog_err("insert surrogate_pk", e))?
                        .map(|v| v.value());
                    if let Some(old) = old.filter(|&old| old != surrogate) {
                        stale.push(rev_key(scope, old));
                    }
                }
            }
            let mut rev = txn
                .open_table(SURROGATE_PK_REV)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            for old in stale {
                rev.remove(old)
                    .map_err(|e| catalog_err("remove surrogate_pk_rev", e))?;
            }
            let reverse: Vec<(u128, &[u8])> = last
                .iter()
                .enumerate()
                .map(|(u, &surrogate)| (rev_key(scope, surrogate), keys.pk(u)))
                .collect();
            insert_rev_sorted(&mut rev, reverse)?;
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
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk batch read txn", e))?;
        let Some(scope) = read_scope(&txn, key, tenant_id)? else {
            return Ok(vec![None; pks.len()]);
        };
        let table = txn
            .open_table(SURROGATE_PK)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        let keys = SortedKeys::new(scope, pks.iter().copied());
        let found = probe(&table, &keys)?;
        Ok((0..pks.len())
            .map(|i| found[keys.unique_of(i)].map(Surrogate::new))
            .collect())
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

    /// A later batch that rewrites half the keys of an earlier one keeps
    /// every earlier binding, binds the new keys, and leaves the reverse
    /// table the exact inverse of the forward table.
    #[test]
    fn a_later_overlapping_batch_keeps_bindings_and_the_reverse_map_inverse() {
        let (_dir, cat) = open();
        let pks: Vec<Vec<u8>> = (0..400u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let first: Vec<(&[u8], Surrogate)> = pks[..300]
            .iter()
            .enumerate()
            .rev()
            .map(|(i, pk)| (pk.as_slice(), Surrogate::new(1 + i as u32)))
            .collect();
        cat.put_surrogates(users(), T0, &first)
            .expect("first batch");
        let second: Vec<(&[u8], Surrogate)> = pks[150..]
            .iter()
            .enumerate()
            .map(|(i, pk)| (pk.as_slice(), Surrogate::new(10_000 + i as u32)))
            .collect();
        let out = cat
            .put_surrogates(users(), T0, &second)
            .expect("second batch");
        for (i, binding) in out.iter().enumerate() {
            let index = 150 + i;
            if index < 300 {
                assert_eq!(binding.surrogate, Surrogate::new(1 + index as u32));
                assert!(!binding.inserted);
            } else {
                assert_eq!(binding.surrogate, Surrogate::new(10_000 + i as u32));
                assert!(binding.inserted);
            }
        }
        let scanned = cat
            .scan_surrogates_for_collection(users(), T0)
            .expect("scan");
        assert_eq!(scanned.len(), 400);
        for (pk, surrogate) in &scanned {
            assert_eq!(
                cat.get_pk_for_surrogate(users(), T0, *surrogate)
                    .expect("rev"),
                Some(pk.clone())
            );
        }
        for losing in 10_000..10_150u32 {
            assert_eq!(
                cat.get_pk_for_surrogate(users(), T0, Surrogate::new(losing))
                    .expect("rev"),
                None,
                "a losing carried surrogate writes no reverse row"
            );
        }
        let refs: Vec<&[u8]> = pks.iter().map(Vec::as_slice).collect();
        let read = cat
            .get_surrogates_for_pks(users(), T0, &refs)
            .expect("read");
        for (pk, hit) in refs.iter().zip(&read) {
            assert_eq!(
                *hit,
                cat.get_surrogate_for_pk(users(), T0, pk).expect("get")
            );
        }
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
