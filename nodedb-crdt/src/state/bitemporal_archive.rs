// SPDX-License-Identifier: Apache-2.0

//! Bitemporal version archive for CRDT collections.
//!
//! Physically preserves **superseded** versions of a row before it is
//! overwritten, so `FOR SYSTEM_TIME AS OF` reads and cross-peer
//! divergence reconstruction see the full history — not just the
//! latest value Loro's `insert_container` would leave behind.
//!
//! Storage layout: a single sibling `LoroMap` at root key
//! `__bitemporal_history__`. Archive keys are flat strings of the form
//! `{collection}\0{row_id}\0{sys_ms:020}` so lexicographic ordering
//! matches numeric `_ts_system` ordering, and prefix scans bound the
//! set of versions for one logical row.
//!
//! Retention: [`purge_history_before`] deletes archived entries with
//! `sys_ms < cutoff_ms` across all row ids in the collection. The
//! *current* (live) row is never touched — only archive entries —
//! so `AS OF now()` reads always see the latest version regardless of
//! retention configuration.

use loro::{LoroMap, LoroValue, ValueOrContainer};

use super::core::CrdtState;
use crate::error::{CrdtError, Result};

/// Root Loro map holding archived versions for every bitemporal
/// collection. Single top-level sibling keeps the doc shape stable —
/// adding a bitemporal collection does not rewrite existing structure.
pub const HISTORY_ROOT: &str = "__bitemporal_history__";

/// Format the flat archive key. `sys_ms` is zero-padded so that keys
/// sort lexicographically in `_ts_system` order. NUL separators avoid
/// collisions with user-supplied `/` or `:` inside collection or row
/// identifiers.
pub fn archive_key(collection: &str, row_id: &str, sys_ms: i64) -> String {
    format!("{collection}\u{0}{row_id}\u{0}{sys_ms:020}")
}

/// Parse an archive key; returns `None` when the key does not have the
/// three NUL-separated segments produced by [`archive_key`].
pub(super) fn parse_archive_key(key: &str) -> Option<(&str, &str, i64)> {
    let mut parts = key.splitn(3, '\u{0}');
    let collection = parts.next()?;
    let row_id = parts.next()?;
    let sys_ms = parts.next()?.parse::<i64>().ok()?;
    Some((collection, row_id, sys_ms))
}

impl CrdtState {
    /// Upsert a row with prior-version archiving for bitemporal
    /// collections. When a row with `row_id` already exists and carries
    /// a finite `_ts_system`, its field map is copied into the
    /// bitemporal history sibling before the new fields overwrite the
    /// live row. Non-bitemporal callers should stay on
    /// [`CrdtState::upsert`] — the archive has a non-trivial doc-size
    /// cost and is only meaningful when the collection participates in
    /// `AS OF` / audit queries.
    pub fn upsert_versioned(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        if let Some((prior_sys_ms, prior_fields)) = self.prior_system_snapshot(collection, row_id) {
            let archive = self.doc.get_map(HISTORY_ROOT);
            let key = archive_key(collection, row_id, prior_sys_ms);
            let slot = archive
                .insert_container(&key, LoroMap::new())
                .map_err(|e| CrdtError::Loro(format!("archive insert: {e}")))?;
            for (k, v) in &prior_fields {
                slot.insert(k.as_str(), v.clone())
                    .map_err(|e| CrdtError::Loro(format!("archive field: {e}")))?;
            }
        }
        self.upsert(collection, row_id, fields)
    }

    /// Read the row as it was at `asof_ms` (system-time). Scans the
    /// archive for the highest `sys_ms <= asof_ms`; falls back to the
    /// current row when its `_ts_system <= asof_ms`; returns `None`
    /// when no version existed at or before the requested time.
    pub fn read_row_as_of(
        &self,
        collection: &str,
        row_id: &str,
        asof_ms: i64,
    ) -> Option<LoroValue> {
        let archive = self.doc.get_map(HISTORY_ROOT);
        let mut best: Option<(i64, LoroValue)> = None;

        for key in archive.keys() {
            let key_str = key.to_string();
            let (c, r, ts) = match parse_archive_key(&key_str) {
                Some(t) => t,
                None => continue,
            };
            if c != collection || r != row_id || ts > asof_ms {
                continue;
            }
            if let Some(ValueOrContainer::Container(loro::Container::Map(m))) =
                archive.get(&key_str)
                && best.as_ref().is_none_or(|(b, _)| ts > *b)
            {
                best = Some((ts, m.get_value()));
            }
        }

        if let Some(LoroValue::Map(current_map)) = self.read_row(collection, row_id)
            && let Some(&LoroValue::I64(cur_ts)) = current_map.get("_ts_system")
            && cur_ts <= asof_ms
            && best.as_ref().is_none_or(|(b, _)| cur_ts > *b)
        {
            return Some(LoroValue::Map(current_map));
        }

        best.map(|(_, v)| v)
    }

    /// Count archived versions for a row (live row excluded).
    /// Primarily for tests and operational introspection.
    pub fn archive_version_count(&self, collection: &str, row_id: &str) -> usize {
        let archive = self.doc.get_map(HISTORY_ROOT);
        archive
            .keys()
            .filter(|k| {
                parse_archive_key(k).is_some_and(|(c, r, _)| c == collection && r == row_id)
            })
            .count()
    }

    /// Drop archived versions with `sys_ms < cutoff_ms` for the given
    /// collection. Returns the number of archive entries deleted. The
    /// live row is never touched — retention only reclaims history, so
    /// the current state of every logical row remains readable even
    /// when the entire archive is pruned.
    pub fn purge_history_before(&self, collection: &str, cutoff_ms: i64) -> Result<usize> {
        let archive = self.doc.get_map(HISTORY_ROOT);
        let victims: Vec<String> = archive
            .keys()
            .filter_map(|k| {
                let ks = k.to_string();
                let matches = parse_archive_key(&ks)
                    .is_some_and(|(c, _, ts)| c == collection && ts < cutoff_ms);
                matches.then_some(ks)
            })
            .collect();
        let count = victims.len();
        for key in victims {
            archive
                .delete(&key)
                .map_err(|e| CrdtError::Loro(format!("archive delete: {e}")))?;
        }
        Ok(count)
    }

    /// Read the live row's (`_ts_system`, field-map) pair when both the
    /// row exists and carries a finite system stamp. Used exclusively
    /// by `upsert_versioned` to decide whether to archive the prior
    /// version; a row without `_ts_system` is treated as non-archivable
    /// (typically the first insert into a bitemporal collection or a
    /// row that predates the bitemporal flag on the collection).
    fn prior_system_snapshot(
        &self,
        collection: &str,
        row_id: &str,
    ) -> Option<(i64, Vec<(String, LoroValue)>)> {
        let current = match self.read_row(collection, row_id)? {
            LoroValue::Map(m) => m,
            _ => return None,
        };
        let sys_ms = match current.get("_ts_system")? {
            LoroValue::I64(n) => *n,
            _ => return None,
        };
        let fields: Vec<(String, LoroValue)> = current
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        Some((sys_ms, fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> LoroValue {
        LoroValue::I64(ms)
    }

    fn string(s: &str) -> LoroValue {
        LoroValue::String(s.into())
    }

    fn make_state() -> CrdtState {
        CrdtState::new(1).unwrap()
    }

    #[test]
    fn first_upsert_without_prior_ts_does_not_archive() {
        let s = make_state();
        s.upsert_versioned("users", "u1", &[("name", string("alice"))])
            .unwrap();
        assert_eq!(s.archive_version_count("users", "u1"), 0);
    }

    #[test]
    fn upsert_versioned_archives_prior_with_ts_system() {
        let s = make_state();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("alice")), ("_ts_system", ts(100))],
        )
        .unwrap();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("alice2")), ("_ts_system", ts(200))],
        )
        .unwrap();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("alice3")), ("_ts_system", ts(300))],
        )
        .unwrap();
        assert_eq!(s.archive_version_count("users", "u1"), 2);
    }

    #[test]
    fn read_row_as_of_returns_historical_version() {
        let s = make_state();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("v1")), ("_ts_system", ts(100))],
        )
        .unwrap();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("v2")), ("_ts_system", ts(200))],
        )
        .unwrap();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("v3")), ("_ts_system", ts(300))],
        )
        .unwrap();

        let at_150 = s.read_row_as_of("users", "u1", 150).unwrap();
        if let LoroValue::Map(m) = at_150 {
            assert_eq!(m.get("name").unwrap(), &string("v1"));
        } else {
            panic!("expected map");
        }
        let at_250 = s.read_row_as_of("users", "u1", 250).unwrap();
        if let LoroValue::Map(m) = at_250 {
            assert_eq!(m.get("name").unwrap(), &string("v2"));
        } else {
            panic!("expected map");
        }
        let at_999 = s.read_row_as_of("users", "u1", 999).unwrap();
        if let LoroValue::Map(m) = at_999 {
            assert_eq!(m.get("name").unwrap(), &string("v3"));
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn read_row_as_of_returns_none_before_first_version() {
        let s = make_state();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("v1")), ("_ts_system", ts(100))],
        )
        .unwrap();
        assert!(s.read_row_as_of("users", "u1", 50).is_none());
    }

    #[test]
    fn purge_history_before_drops_superseded_versions() {
        let s = make_state();
        for (name, t) in [("v1", 100), ("v2", 200), ("v3", 300), ("v4", 400)] {
            s.upsert_versioned(
                "users",
                "u1",
                &[("name", string(name)), ("_ts_system", ts(t))],
            )
            .unwrap();
        }
        // Three archived: 100, 200, 300. Latest (400) is live.
        assert_eq!(s.archive_version_count("users", "u1"), 3);
        let dropped = s.purge_history_before("users", 250).unwrap();
        assert_eq!(dropped, 2); // 100 and 200
        assert_eq!(s.archive_version_count("users", "u1"), 1); // 300 remains
        // Live row still intact.
        let live = s.read_row("users", "u1").unwrap();
        if let LoroValue::Map(m) = live {
            assert_eq!(m.get("name").unwrap(), &string("v4"));
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn purge_history_before_never_drops_live_row() {
        let s = make_state();
        s.upsert_versioned(
            "users",
            "u1",
            &[("name", string("only")), ("_ts_system", ts(100))],
        )
        .unwrap();
        // No prior to archive on first write.
        let dropped = s.purge_history_before("users", i64::MAX).unwrap();
        assert_eq!(dropped, 0);
        let live = s.read_row("users", "u1").unwrap();
        if let LoroValue::Map(m) = live {
            assert_eq!(m.get("name").unwrap(), &string("only"));
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn purge_scoped_to_collection() {
        let s = make_state();
        for coll in ["users", "orders"] {
            for (name, t) in [("v1", 100), ("v2", 200)] {
                s.upsert_versioned(coll, "row", &[("v", string(name)), ("_ts_system", ts(t))])
                    .unwrap();
            }
        }
        assert_eq!(s.archive_version_count("users", "row"), 1);
        assert_eq!(s.archive_version_count("orders", "row"), 1);
        let dropped = s.purge_history_before("users", i64::MAX).unwrap();
        assert_eq!(dropped, 1);
        assert_eq!(s.archive_version_count("users", "row"), 0);
        assert_eq!(s.archive_version_count("orders", "row"), 1); // untouched
    }
}
