// SPDX-License-Identifier: BUSL-1.1

//! Sorted batch access to the surrogate PK map tables.
//!
//! A batch sorts its keys once. A read walks the forward table with one
//! cursor, and a write splices every new row in with one cursor. A cursor
//! seeks again only past a run of [`RESEEK_AFTER`] rows the batch does not
//! name. A batch of keys absent from the table costs one seek.

use std::cmp::Ordering;
use std::ops::Bound;

use redb::{ReadableTable, Table};

use super::surrogate_scope::SCOPE_PREFIX_LEN;
use super::types::catalog_err;

/// Rows a cursor steps past before it seeks to the next batch key instead.
const RESEEK_AFTER: usize = 16;

/// The distinct forward-table keys of one batch, in key order.
pub(super) struct SortedKeys {
    /// Every input key, scope prefix included, in input order.
    buf: Vec<u8>,
    /// Per distinct key, in key order: its byte range in `buf`.
    spans: Vec<(usize, usize)>,
    /// Per distinct key, in key order: the input index of its first occurrence.
    first: Vec<usize>,
    /// Per input index: the position of its key in `spans`.
    unique_of: Vec<usize>,
}

impl SortedKeys {
    /// Sort and deduplicate the forward-table keys of `pks` in `scope`.
    pub(super) fn new<'p>(scope: u64, pks: impl ExactSizeIterator<Item = &'p [u8]>) -> Self {
        let n = pks.len();
        let mut buf = Vec::new();
        let mut input_spans = Vec::with_capacity(n);
        let prefix = scope.to_be_bytes();
        for pk in pks {
            let start = buf.len();
            buf.extend_from_slice(&prefix);
            buf.extend_from_slice(pk);
            input_spans.push((start, buf.len()));
        }
        let key = |i: usize| &buf[input_spans[i].0..input_spans[i].1];
        let mut order: Vec<usize> = (0..n).collect();
        // Equal keys keep input order, so the first occurrence sorts first.
        order.sort_unstable_by(|&a, &b| key(a).cmp(key(b)).then(a.cmp(&b)));
        let mut spans: Vec<(usize, usize)> = Vec::with_capacity(n);
        let mut first: Vec<usize> = Vec::with_capacity(n);
        let mut unique_of = vec![0usize; n];
        for &i in &order {
            let is_repeat = first.last().is_some_and(|&f| key(f) == key(i));
            if !is_repeat {
                spans.push(input_spans[i]);
                first.push(i);
            }
            unique_of[i] = spans.len() - 1;
        }
        Self {
            buf,
            spans,
            first,
            unique_of,
        }
    }

    /// How many distinct keys the batch holds.
    pub(super) fn len(&self) -> usize {
        self.spans.len()
    }

    /// Distinct key `u`, scope prefix included.
    pub(super) fn key(&self, u: usize) -> &[u8] {
        let (start, end) = self.spans[u];
        &self.buf[start..end]
    }

    /// The pk bytes of distinct key `u`, without the scope prefix.
    pub(super) fn pk(&self, u: usize) -> &[u8] {
        &self.key(u)[SCOPE_PREFIX_LEN..]
    }

    /// The input index of the first occurrence of distinct key `u`.
    pub(super) fn first(&self, u: usize) -> usize {
        self.first[u]
    }

    /// The distinct key position of input index `i`.
    pub(super) fn unique_of(&self, i: usize) -> usize {
        self.unique_of[i]
    }
}

/// The value bound to each distinct key of `keys`, `None` for an absent key.
pub(super) fn probe(
    table: &impl ReadableTable<&'static [u8], u32>,
    keys: &SortedKeys,
) -> crate::Result<Vec<Option<u32>>> {
    let n = keys.len();
    let mut out = vec![None; n];
    let mut u = 0;
    while u < n {
        let mut cursor = table
            .lower_bound(Bound::Included(keys.key(u)))
            .map_err(|e| catalog_err("seek surrogate_pk", e))?;
        let mut skipped = 0;
        loop {
            let Some((k, v)) = cursor
                .next()
                .map_err(|e| catalog_err("step surrogate_pk", e))?
            else {
                return Ok(out);
            };
            let existing = k.value();
            // Batch keys below the next stored key are absent.
            while u < n && keys.key(u) < existing {
                u += 1;
                skipped = 0;
            }
            if u == n {
                return Ok(out);
            }
            if keys.key(u) == existing {
                out[u] = Some(v.value());
                u += 1;
                skipped = 0;
                if u == n {
                    return Ok(out);
                }
            } else {
                skipped += 1;
                if skipped >= RESEEK_AFTER {
                    break;
                }
            }
        }
    }
    Ok(out)
}

/// Bind each absent distinct key of `keys` to `value_of(u)` first-wins, and
/// return the value each key held before: `Some` for a key already bound,
/// `None` for a key this call inserted.
pub(super) fn merge_insert(
    table: &mut Table<'_, &'static [u8], u32>,
    keys: &SortedKeys,
    value_of: impl Fn(usize) -> u32,
) -> crate::Result<Vec<Option<u32>>> {
    let n = keys.len();
    let mut out = vec![None; n];
    let mut u = 0;
    while u < n {
        let mut cursor = table
            .lower_bound_mut(Bound::Included(keys.key(u)))
            .map_err(|e| catalog_err("seek surrogate_pk", e))?;
        let mut skipped = 0;
        while u < n {
            let next = cursor
                .peek_next()
                .map_err(|e| catalog_err("peek surrogate_pk", e))?
                .map(|(k, v)| (keys.key(u).cmp(k.value()), v.value()));
            match next {
                // The key sorts before the next stored row, or past the last
                // one: it is absent.
                None | Some((Ordering::Less, _)) => {
                    cursor
                        .insert_before(keys.key(u), value_of(u))
                        .map_err(|e| catalog_err("insert surrogate_pk", e))?;
                    u += 1;
                    skipped = 0;
                }
                Some((Ordering::Equal, bound)) => {
                    out[u] = Some(bound);
                    u += 1;
                    skipped = 0;
                    cursor
                        .next()
                        .map_err(|e| catalog_err("step surrogate_pk", e))?;
                }
                Some((Ordering::Greater, _)) => {
                    skipped += 1;
                    if skipped >= RESEEK_AFTER {
                        break;
                    }
                    cursor
                        .next()
                        .map_err(|e| catalog_err("step surrogate_pk", e))?;
                }
            }
        }
        cursor
            .close()
            .map_err(|e| catalog_err("close surrogate_pk cursor", e))?;
    }
    Ok(out)
}

/// Write each `(rev_key, pk_bytes)` row into the reverse table, overwriting
/// a row already stored under the key. A key repeated in `rows` keeps its
/// last pk.
pub(super) fn insert_rev_sorted(
    table: &mut Table<'_, u128, &'static [u8]>,
    mut rows: Vec<(u128, &[u8])>,
) -> crate::Result<()> {
    rows.sort_by_key(|&(key, _)| key);
    rows.dedup_by(|later, earlier| {
        let repeat = later.0 == earlier.0;
        if repeat {
            earlier.1 = later.1;
        }
        repeat
    });
    let mut overwrites: Vec<(u128, &[u8])> = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let mut cursor = table
            .lower_bound_mut(Bound::Included(rows[i].0))
            .map_err(|e| catalog_err("seek surrogate_pk_rev", e))?;
        let mut skipped = 0;
        while i < rows.len() {
            let (key, pk) = rows[i];
            let next = cursor
                .peek_next()
                .map_err(|e| catalog_err("peek surrogate_pk_rev", e))?
                .map(|(k, _)| key.cmp(&k.value()));
            match next {
                None | Some(Ordering::Less) => {
                    cursor
                        .insert_before(key, pk)
                        .map_err(|e| catalog_err("insert surrogate_pk_rev", e))?;
                    i += 1;
                    skipped = 0;
                }
                Some(Ordering::Equal) => {
                    overwrites.push((key, pk));
                    i += 1;
                    skipped = 0;
                    cursor
                        .next()
                        .map_err(|e| catalog_err("step surrogate_pk_rev", e))?;
                }
                Some(Ordering::Greater) => {
                    skipped += 1;
                    if skipped >= RESEEK_AFTER {
                        break;
                    }
                    cursor
                        .next()
                        .map_err(|e| catalog_err("step surrogate_pk_rev", e))?;
                }
            }
        }
        cursor
            .close()
            .map_err(|e| catalog_err("close surrogate_pk_rev cursor", e))?;
    }
    for (key, pk) in overwrites {
        table
            .insert(key, pk)
            .map_err(|e| catalog_err("overwrite surrogate_pk_rev", e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::surrogate_scope::fwd_key;
    use redb::{ReadableDatabase, TableDefinition};

    const FWD: TableDefinition<&[u8], u32> = TableDefinition::new("fwd");
    const REV: TableDefinition<u128, &[u8]> = TableDefinition::new("rev");

    fn db() -> (tempfile::TempDir, redb::Database) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = redb::Database::create(dir.path().join("t.redb")).expect("create");
        (dir, db)
    }

    #[test]
    fn keys_sort_dedupe_and_keep_the_first_occurrence() {
        let pks: [&[u8]; 4] = [b"b", b"a", b"b", b"c"];
        let keys = SortedKeys::new(3, pks.iter().copied());
        assert_eq!(keys.len(), 3);
        assert_eq!(keys.pk(0), b"a");
        assert_eq!(keys.pk(1), b"b");
        assert_eq!(keys.first(1), 0, "the first `b` is input 0");
        assert_eq!(keys.unique_of(2), 1);
        assert_eq!(&keys.key(2)[..SCOPE_PREFIX_LEN], &3u64.to_be_bytes());
    }

    /// Stored rows interleave with batch keys, past the reseek distance too.
    #[test]
    fn merge_is_first_wins_and_probe_agrees_with_point_gets() {
        let (_dir, db) = db();
        let stored: Vec<Vec<u8>> = (0..200u32)
            .filter(|i| i % 3 == 0)
            .map(|i| fwd_key(0, format!("k{i:04}").as_bytes()))
            .collect();
        let txn = db.begin_write().expect("txn");
        {
            let mut fwd = txn.open_table(FWD).expect("open");
            for (n, pk) in stored.iter().enumerate() {
                fwd.insert(pk.as_slice(), n as u32 + 1_000).expect("seed");
            }
        }
        txn.commit().expect("commit");

        let batch: Vec<Vec<u8>> = (0..200u32)
            .filter(|i| i % 2 == 0 || *i > 150)
            .map(|i| format!("k{i:04}").into_bytes())
            .collect();
        let raw: Vec<&[u8]> = batch.iter().map(Vec::as_slice).collect();
        let keys = SortedKeys::new(0, raw.iter().copied());
        let txn = db.begin_write().expect("txn");
        let before = {
            let mut fwd = txn.open_table(FWD).expect("open");
            merge_insert(&mut fwd, &keys, |u| u as u32).expect("merge")
        };
        txn.commit().expect("commit");

        let txn = db.begin_read().expect("read");
        let fwd = txn.open_table(FWD).expect("open");
        let probed = probe(&fwd, &keys).expect("probe");
        for u in 0..keys.len() {
            let got = fwd.get(keys.key(u)).expect("get").map(|v| v.value());
            assert_eq!(probed[u], got, "probe of key {u}");
            let was_stored = stored.iter().any(|s| s.as_slice() == keys.key(u));
            assert_eq!(before[u].is_some(), was_stored, "key {u}");
            if !was_stored {
                assert_eq!(got, Some(u as u32), "an absent key takes its new value");
            }
        }
    }

    #[test]
    fn a_probe_of_an_empty_table_finds_nothing() {
        let (_dir, db) = db();
        let txn = db.begin_write().expect("txn");
        txn.open_table(FWD).expect("create");
        txn.commit().expect("commit");
        let pks: [&[u8]; 2] = [b"x", b"y"];
        let keys = SortedKeys::new(1, pks.iter().copied());
        let txn = db.begin_read().expect("read");
        let fwd = txn.open_table(FWD).expect("open");
        assert_eq!(probe(&fwd, &keys).expect("probe"), vec![None, None]);
    }

    #[test]
    fn reverse_rows_insert_in_order_and_overwrite_a_stored_key() {
        let (_dir, db) = db();
        let txn = db.begin_write().expect("txn");
        {
            let mut rev = txn.open_table(REV).expect("open");
            rev.insert(5u128, b"old".as_slice()).expect("seed");
            let rows: Vec<(u128, &[u8])> =
                vec![(9, b"nine"), (5, b"five"), (1, b"one"), (9, b"last")];
            insert_rev_sorted(&mut rev, rows).expect("insert");
        }
        txn.commit().expect("commit");
        let txn = db.begin_read().expect("read");
        let rev = txn.open_table(REV).expect("open");
        let get = |k: u128| rev.get(k).expect("get").map(|v| v.value().to_vec());
        assert_eq!(get(1), Some(b"one".to_vec()));
        assert_eq!(get(5), Some(b"five".to_vec()));
        assert_eq!(get(9), Some(b"last".to_vec()));
    }
}
