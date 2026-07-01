// SPDX-License-Identifier: Apache-2.0

//! Post-import write-set extraction.
//!
//! Given a Loro version frontier captured immediately before an import, these
//! helpers compute the rows a delta *actually* wrote (independent of any
//! row-id the sender claimed) and assemble a [`ProposedChange`] for a single
//! committed row so it can be re-checked against installed constraints.
//!
//! The write-set is row-granular only — collection + row-id pairs. The
//! validator re-reads the full row, so field-level detail is unnecessary here.
//! Ordering is deterministic (`BTreeSet` for the write-set, sorted field vec
//! for the change) because every replica must agree on the same result.

use std::collections::BTreeSet;

use loro::event::Diff;
use loro::{ContainerID, ContainerType, Frontiers, LoroValue};
use nodedb_types::Surrogate;

use crate::error::{CrdtError, Result};
use crate::validator::ProposedChange;

use super::core::CrdtState;

impl CrdtState {
    /// Capture the current version frontier. Take this *before* an import so a
    /// later [`write_set_since`](Self::write_set_since) can diff against it.
    pub fn frontier(&self) -> Frontiers {
        self.doc.state_frontiers()
    }

    /// Compute the `(collection, row_id)` pairs written between `before` and the
    /// current state — the rows a just-imported delta actually touched.
    ///
    /// Two container shapes surface a written row:
    /// - the collection's root map gains/updates a key (the row-id), and
    /// - the row's own (normal) map container.
    ///
    /// For the row container, `get_path_to_container` returns the full path
    /// from root to target, *including* a trailing element for the target
    /// itself: `[(collection_root, Key(collection)), (row_container,
    /// Key(row_id))]`. The collection name therefore comes from the
    /// second-to-last element's root ContainerID, and the row-id from the
    /// last element's key index. Reading both from the same (first) element
    /// would resolve the collection root as if it were a row named after the
    /// collection.
    ///
    /// Both shapes map to the same `(collection, row_id)`. Loro's
    /// `ContainerID` / `Index` / `Diff` are foreign enums; the fallthrough arm
    /// intentionally ignores irrelevant shapes (sequence/tree indices, non-map
    /// roots, list/text/tree/counter diffs).
    pub fn write_set_since(&self, before: &Frontiers) -> Result<Vec<(String, String)>> {
        let after = self.doc.state_frontiers();
        let batch = self
            .doc
            .diff(before, &after)
            .map_err(|e| CrdtError::Loro(e.to_string()))?;

        let mut keys = BTreeSet::<(String, String)>::new();
        for (cid, diff) in batch.iter() {
            match cid {
                ContainerID::Root {
                    name,
                    container_type: ContainerType::Map,
                } => {
                    if let Diff::Map(md) = diff {
                        for k in md.updated.keys() {
                            keys.insert((name.to_string(), k.to_string()));
                        }
                    }
                }
                ContainerID::Normal { .. } => {
                    // Path is root → target, last element being the target
                    // container itself: the owning collection root is the
                    // second-to-last element, the row-id the last element's key.
                    if let Some(path) = self.doc.get_path_to_container(cid)
                        && path.len() >= 2
                        && let (Some((parent, _)), Some((_, idx))) =
                            (path.get(path.len() - 2), path.last())
                        && let (Some((name, _)), Some(key)) = (parent.as_root(), idx.as_key())
                    {
                        keys.insert((name.to_string(), key.to_string()));
                    }
                }
                _ => {}
            }
        }

        // `BTreeSet` ⇒ deterministic sorted output.
        Ok(keys.into_iter().collect())
    }

    /// Assemble a [`ProposedChange`] from a committed row's current fields.
    ///
    /// Returns `None` when the row is absent (a pure delete leaves nothing to
    /// validate). The field vec is sorted by key so the change is byte-identical
    /// across replicas (the underlying map iterates in nondeterministic order).
    pub fn build_change_from_row(
        &self,
        collection: &str,
        row_id: &str,
        surrogate: Surrogate,
    ) -> Option<ProposedChange> {
        match self.read_row(collection, row_id)? {
            LoroValue::Map(m) => {
                let mut fields: Vec<(String, LoroValue)> =
                    m.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
                fields.sort_by(|a, b| a.0.cmp(&b.0));
                Some(ProposedChange {
                    collection: collection.to_string(),
                    row_id: row_id.to_string(),
                    surrogate,
                    fields,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> LoroValue {
        LoroValue::String(v.into())
    }

    fn n(v: i64) -> LoroValue {
        LoroValue::I64(v)
    }

    // (1) A single-row insert surfaces exactly that row.
    #[test]
    fn single_insert_write_set() {
        let state = CrdtState::new(1).unwrap();
        let before = state.frontier();
        state
            .upsert("users", "u1", &[("name", s("Alice"))])
            .unwrap();

        let ws = state.write_set_since(&before).unwrap();
        assert_eq!(ws, vec![("users".to_string(), "u1".to_string())]);
    }

    // (2) A two-row blob imported from a peer surfaces both rows, sorted.
    #[test]
    fn two_row_blob_write_set() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "a", &[("x", n(1))]).unwrap();
        src.upsert("users", "b", &[("x", n(2))]).unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        let before = dst.frontier();
        dst.import(&snapshot).unwrap();

        let ws = dst.write_set_since(&before).unwrap();
        assert_eq!(
            ws,
            vec![
                ("users".to_string(), "a".to_string()),
                ("users".to_string(), "b".to_string()),
            ]
        );
    }

    // (3) The write-set reflects the row a delta REALLY wrote, not a claimed id.
    #[test]
    fn write_set_reveals_real_row_not_claimed() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("orders", "real-1", &[("amt", n(10))]).unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        let before = dst.frontier();
        dst.import(&snapshot).unwrap();

        let ws = dst.write_set_since(&before).unwrap();
        assert_eq!(ws, vec![("orders".to_string(), "real-1".to_string())]);
        assert!(!ws.contains(&("orders".to_string(), "fake-claimed".to_string())));
    }

    // (4) A cross-collection blob surfaces the real collection it wrote into.
    #[test]
    fn write_set_reveals_real_collection() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("secret", "s1", &[("v", n(9))]).unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        let before = dst.frontier();
        dst.import(&snapshot).unwrap();

        let ws = dst.write_set_since(&before).unwrap();
        assert_eq!(ws, vec![("secret".to_string(), "s1".to_string())]);
    }

    // (5) A peer delta that merges ONE field of an existing row surfaces that
    //     one row, and the post-import row still carries ALL fields (so NOT
    //     NULL can see the untouched required fields). Modeled via a
    //     field-level merge (`insert` on the existing row container) exported
    //     as an incremental delta — NOT an `upsert`, which is whole-row
    //     replace and would wipe the untouched field.
    #[test]
    fn update_only_write_set_and_full_change() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("email", s("a")), ("name", s("x"))])
            .unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        dst.import(&snapshot).unwrap();
        let before = dst.frontier();

        // Field-level merge of only `name` on the EXISTING row container.
        let src_vv = src.doc.oplog_vv();
        match src.doc.get_map("users").get("u1") {
            Some(loro::ValueOrContainer::Container(loro::Container::Map(row))) => {
                row.insert("name", s("y")).unwrap();
            }
            _ => panic!("row container missing"),
        }
        let delta = src.export_updates_since(&src_vv).unwrap();
        dst.import(&delta).unwrap();

        let ws = dst.write_set_since(&before).unwrap();
        assert_eq!(ws, vec![("users".to_string(), "u1".to_string())]);

        let change = dst
            .build_change_from_row("users", "u1", Surrogate::ZERO)
            .unwrap();
        let field_names: Vec<String> = change.fields.iter().map(|(k, _)| k.clone()).collect();
        // Sorted: email before name — both present despite only name merging.
        assert_eq!(field_names, vec!["email".to_string(), "name".to_string()]);
        assert!(change.fields.contains(&("name".to_string(), s("y"))));
        assert!(change.fields.contains(&("email".to_string(), s("a"))));
    }

    // (6) The output is deterministic across repeated runs on fresh docs.
    #[test]
    fn write_set_is_deterministic() {
        fn run() -> Vec<(String, String)> {
            let src = CrdtState::new(2).unwrap();
            src.upsert("users", "b", &[("x", n(2))]).unwrap();
            src.upsert("users", "a", &[("x", n(1))]).unwrap();
            src.upsert("secret", "s1", &[("v", n(9))]).unwrap();
            let snapshot = src.export_snapshot().unwrap();

            let dst = CrdtState::new(1).unwrap();
            let before = dst.frontier();
            dst.import(&snapshot).unwrap();
            dst.write_set_since(&before).unwrap()
        }

        let first = run();
        let second = run();
        assert_eq!(first, second);
    }

    // (7) A deleted row yields no change to validate.
    #[test]
    fn deleted_row_yields_no_change() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("users", "u1", &[("name", s("Alice"))])
            .unwrap();
        state.delete("users", "u1").unwrap();

        assert!(
            state
                .build_change_from_row("users", "u1", Surrogate::ZERO)
                .is_none()
        );
    }
}
