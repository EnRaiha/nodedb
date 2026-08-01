// SPDX-License-Identifier: Apache-2.0

//! Re-authoring a document's contents under a new Loro peer id.
//!
//! A Loro operation's identity is `(peer_id, counter)`, and the peer id of an
//! operation is fixed the moment it is committed — `LoroDoc::set_peer_id`
//! only changes who authors *subsequent* operations. So a replica that must
//! stop using a peer id (because another replica owns it, or because this
//! replica's own history under it was forked) cannot keep its existing
//! document: every operation in it stays attributed to the peer id being
//! abandoned, and re-exporting them merely re-sends what the owner will
//! refuse or what the merge will discard as a replay.
//!
//! [`CrdtState::rekey`] therefore builds a *new* document under the new peer
//! id and rebuilds the current contents into it, so the result is authored end
//! to end by the new identity.
//!
//! Two properties make the rebuild safe to hand straight to sync:
//!
//! - It walks the live `ValueOrContainer` tree through [`restore_containers`]
//!   rather than re-writing a flattened projection. The flattened form
//!   collapses nested CRDT containers (a row's block list, the archive's
//!   version slots) into plain values, destroying them.
//! - It rebuilds one row at a time and records the counter range each row's
//!   operations occupy, so the caller can export one self-contained delta per
//!   row. A single delta spanning every row is not independently applicable by
//!   a receiver that commits per row.
//!
//! A row's archived versions are rebuilt inside that row's range, so the
//! per-row delta carries the row's history with it.

use loro::LoroValue;

use crate::error::{CrdtError, Result};

use super::bitemporal_archive::{HISTORY_ROOT, parse_archive_key};
use super::core::CrdtState;
use super::restore_containers;

/// One row re-authored by [`CrdtState::rekey`], and the span of the new peer's
/// operation sequence its rebuild occupies.
///
/// Feed the span to [`CrdtState::export_local_range`] to get a delta carrying
/// exactly this row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RekeyedRow {
    /// Row this range rebuilds, including any archived versions of it.
    pub row_id: String,
    /// First counter of the row's operations (inclusive).
    pub from_counter: i32,
    /// Counter past the row's last operation (exclusive).
    pub to_counter: i32,
}

/// The result of re-authoring a document under a new peer id.
pub struct Rekeyed {
    /// The rebuilt document. Shares no operation history with the original —
    /// that is the point, and it means the new version vector is unrelated to
    /// the old one, so callers must re-push rather than resume from a stored
    /// version.
    pub state: CrdtState,
    /// Per-row spans, in rebuild order.
    pub rows: Vec<RekeyedRow>,
}

impl CrdtState {
    /// Re-author this state's `collection` under `new_peer_id`.
    ///
    /// `self` is left untouched, so a failed rekey cannot leave the caller
    /// holding a half-converted document — it is mid-recovery and has no other
    /// copy of the data.
    ///
    /// Fails rather than dropping anything it cannot reproduce: a root
    /// container other than `collection` or the bitemporal archive, or a
    /// rebuild whose result does not match the original value.
    pub fn rekey(&self, collection: &str, new_peer_id: u64) -> Result<Rekeyed> {
        self.reject_unreproducible_roots(collection)?;

        // `LoroDoc::get_map` materializes a root that does not exist yet, so
        // the archive root is only ever touched on a document that already has
        // one — otherwise reading the source would add an empty root to it,
        // mutating the very state this must leave untouched.
        let has_archive = self.has_root(HISTORY_ROOT);

        let rekeyed = Self::new(new_peer_id)?;
        if has_archive {
            // Match the source's shape even when no row carries history: an
            // archive root that exists on one side and not the other is a
            // difference the completeness check below would (correctly) refuse.
            rekeyed.doc.get_map(HISTORY_ROOT);
        }
        let mut rows = Vec::new();

        for row_id in self.rekey_row_ids(collection, has_archive) {
            let from_counter = rekeyed.local_op_counter();
            self.rebuild_row_into(&rekeyed, collection, &row_id, has_archive)?;
            let to_counter = rekeyed.local_op_counter();
            if to_counter > from_counter {
                rows.push(RekeyedRow {
                    row_id,
                    from_counter,
                    to_counter,
                });
            }
        }

        rekeyed.doc.commit();

        // The rebuild is the caller's only copy of its data once it adopts the
        // result. A mismatch here means some shape was silently not
        // reproduced, and shipping that would lose exactly the writes the
        // rotation exists to save.
        if rekeyed.doc.get_deep_value() != self.doc.get_deep_value() {
            return Err(CrdtError::Loro(format!(
                "rekey of '{collection}' did not reproduce the document's value"
            )));
        }

        Ok(Rekeyed {
            state: rekeyed,
            rows,
        })
    }

    /// Every row to rebuild: the live rows, plus rows that survive only as
    /// archived versions (a deleted row keeps its history).
    fn rekey_row_ids(&self, collection: &str, has_archive: bool) -> Vec<String> {
        let mut ids = self.row_ids(collection);
        if !has_archive {
            return ids;
        }
        let live: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();

        let mut archived: Vec<String> = self
            .doc
            .get_map(HISTORY_ROOT)
            .keys()
            .filter_map(|key| {
                let (archived_collection, row_id, _) = parse_archive_key(&key)?;
                (archived_collection == collection && !live.contains(row_id))
                    .then(|| row_id.to_string())
            })
            .collect();
        archived.sort();
        archived.dedup();

        ids.extend(archived);
        ids
    }

    /// Rebuild one row — its live container and its archived versions — into
    /// `dst`, so the whole row occupies one contiguous counter range.
    fn rebuild_row_into(
        &self,
        dst: &CrdtState,
        collection: &str,
        row_id: &str,
        has_archive: bool,
    ) -> Result<()> {
        let src_collection = self.doc.get_map(collection);
        let dst_collection = dst.doc.get_map(collection);
        if let Some(value) = src_collection.get(row_id) {
            restore_containers::rebuild_map_field(&dst_collection, row_id, value)?;
        }

        if !has_archive {
            return Ok(());
        }
        let src_archive = self.doc.get_map(HISTORY_ROOT);
        let archive_keys: Vec<String> = src_archive
            .keys()
            .filter(|key| {
                parse_archive_key(key).is_some_and(|(c, r, _)| c == collection && r == row_id)
            })
            .map(|key| key.to_string())
            .collect();
        if archive_keys.is_empty() {
            return Ok(());
        }

        let dst_archive = dst.doc.get_map(HISTORY_ROOT);
        for key in archive_keys {
            if let Some(value) = src_archive.get(&key) {
                restore_containers::rebuild_map_field(&dst_archive, &key, value)?;
            }
        }
        Ok(())
    }

    /// Whether `root` already exists on this document.
    ///
    /// Read from the value tree rather than via `get_map`, which would create
    /// the root as a side effect of asking.
    fn has_root(&self, root: &str) -> bool {
        match self.doc.get_deep_value() {
            LoroValue::Map(roots) => roots.contains_key(root),
            _ => false,
        }
    }

    /// Refuse a document carrying a root the per-row rebuild does not visit.
    ///
    /// Skipping one would drop its contents on a path the caller cannot audit,
    /// which is worse than refusing: the caller is recovering from a refused
    /// identity and has nothing else to fall back on.
    fn reject_unreproducible_roots(&self, collection: &str) -> Result<()> {
        let LoroValue::Map(roots) = self.doc.get_deep_value() else {
            return Ok(());
        };
        for (name, value) in roots.iter() {
            if name.as_str() != collection && name.as_str() != HISTORY_ROOT {
                return Err(CrdtError::Loro(format!(
                    "rekey of '{collection}': unexpected root container '{name}' would not be \
                     re-authored"
                )));
            }
            if !matches!(value, LoroValue::Map(_)) {
                return Err(CrdtError::Loro(format!(
                    "rekey of '{collection}': root '{name}' is not a map and cannot be re-authored"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::super::bitemporal_archive::HISTORY_ROOT;
    use super::CrdtState;

    const OLD_PEER: u64 = 7;
    const NEW_PEER: u64 = 99;

    #[test]
    fn rekeyed_state_carries_scalar_rows() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert(
                "users",
                "u1",
                &[
                    ("name", LoroValue::String("alice".into())),
                    ("age", LoroValue::I64(30)),
                ],
            )
            .expect("upsert");

        let rekeyed = state.rekey("users", NEW_PEER).expect("rekey");

        assert_eq!(rekeyed.state.peer_id(), NEW_PEER);
        assert_eq!(
            rekeyed.state.read_field("users", "u1", "name"),
            Some(LoroValue::String("alice".into()))
        );
        assert_eq!(
            rekeyed.state.read_field("users", "u1", "age"),
            Some(LoroValue::I64(30))
        );
    }

    #[test]
    fn rekeyed_state_preserves_nested_containers() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert("pages", "p1", &[("title", LoroValue::String("v1".into()))])
            .expect("upsert");
        state
            .list_insert_fields(
                "pages",
                "p1",
                "blocks",
                0,
                &[("id".into(), LoroValue::String("blk-0".into()))],
            )
            .expect("block");

        let rekeyed = state.rekey("pages", NEW_PEER).expect("rekey").state;

        // A scalar re-upsert would have flattened the block list into a plain
        // value, leaving a row that can never take another list operation.
        assert_eq!(
            rekeyed
                .list_length("pages", "p1", "blocks")
                .expect("length"),
            1,
            "the nested block list must survive as a live container"
        );
        rekeyed
            .list_insert_fields(
                "pages",
                "p1",
                "blocks",
                1,
                &[("id".into(), LoroValue::String("blk-1".into()))],
            )
            .expect("the rebuilt list must still accept list operations");
        assert_eq!(
            rekeyed
                .list_length("pages", "p1", "blocks")
                .expect("length"),
            2
        );
    }

    #[test]
    fn rekeyed_state_preserves_the_bitemporal_archive() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert_versioned(
                "readings",
                "r1",
                &[
                    ("value", LoroValue::I64(1)),
                    ("_ts_system", LoroValue::I64(1_000)),
                ],
            )
            .expect("v1");
        state
            .upsert_versioned(
                "readings",
                "r1",
                &[
                    ("value", LoroValue::I64(2)),
                    ("_ts_system", LoroValue::I64(2_000)),
                ],
            )
            .expect("v2");
        assert_eq!(state.archive_version_count("readings", "r1"), 1);

        let rekeyed = state.rekey("readings", NEW_PEER).expect("rekey").state;

        assert!(
            rekeyed.collection_names().iter().any(|n| n == HISTORY_ROOT),
            "the archive root must be rebuilt, not just the live rows"
        );
        assert_eq!(
            rekeyed.archive_version_count("readings", "r1"),
            1,
            "archived versions must survive a rekey"
        );
        assert_eq!(
            rekeyed.read_row_as_of("readings", "r1", 1_500),
            state.read_row_as_of("readings", "r1", 1_500),
            "as-of reads must agree across the rekey"
        );
    }

    #[test]
    fn rekeyed_state_shares_no_history_with_the_abandoned_peer() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("upsert");

        let rekeyed = state.rekey("users", NEW_PEER).expect("rekey").state;

        // An operation still attributed to the abandoned peer is exactly what
        // the owning replica refuses and what the merge discards as a replay.
        let vv = rekeyed.oplog_version_vector();
        assert_eq!(
            vv.get(&OLD_PEER).copied().unwrap_or(0),
            0,
            "no operation may remain attributed to the abandoned peer"
        );
        assert!(
            vv.get(&NEW_PEER).copied().unwrap_or(0) > 0,
            "the new peer must own the rebuilt operations"
        );
    }

    #[test]
    fn rekey_leaves_the_source_state_intact() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("upsert");

        let _rekeyed = state.rekey("users", NEW_PEER).expect("rekey");

        assert_eq!(state.peer_id(), OLD_PEER);
        assert_eq!(
            state.read_field("users", "u1", "name"),
            Some(LoroValue::String("alice".into())),
            "a rekey must not mutate the state it copies from"
        );
    }

    #[test]
    fn each_row_gets_its_own_applicable_delta() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        for row in ["u1", "u2", "u3"] {
            state
                .upsert("users", row, &[("name", LoroValue::String(row.into()))])
                .expect("upsert");
        }

        let rekeyed = state.rekey("users", NEW_PEER).expect("rekey");

        assert_eq!(rekeyed.rows.len(), 3, "one span per row");

        // Every row must be carried by exactly one span, and the spans must
        // together reconstruct the collection at the receiver. A span that
        // covered two rows would make Origin commit them as one, and a row
        // covered by none would be silently absent after the rotation.
        let receiver = CrdtState::new(1234).expect("receiver");
        for row in &rekeyed.rows {
            let bytes = rekeyed
                .state
                .export_local_range(row.from_counter, row.to_counter)
                .expect("range export");
            assert!(!bytes.is_empty(), "row {} exported nothing", row.row_id);

            receiver.import(&bytes).expect("row delta must apply");
            assert_eq!(
                receiver.read_field("users", &row.row_id, "name"),
                Some(LoroValue::String(row.row_id.as_str().into())),
                "row {} did not arrive from its own span",
                row.row_id
            );
        }

        assert_eq!(
            receiver.row_ids("users").len(),
            3,
            "the spans must reconstruct the whole collection"
        );
    }

    #[test]
    fn rows_surviving_only_as_history_are_rekeyed() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert_versioned(
                "readings",
                "r1",
                &[
                    ("value", LoroValue::I64(1)),
                    ("_ts_system", LoroValue::I64(1_000)),
                ],
            )
            .expect("v1");
        state
            .upsert_versioned(
                "readings",
                "r1",
                &[
                    ("value", LoroValue::I64(2)),
                    ("_ts_system", LoroValue::I64(2_000)),
                ],
            )
            .expect("v2");
        state.delete("readings", "r1").expect("delete");

        let rekeyed = state.rekey("readings", NEW_PEER).expect("rekey");

        assert_eq!(
            rekeyed.state.archive_version_count("readings", "r1"),
            1,
            "a deleted row's history must not be dropped by the rekey"
        );
        assert!(
            rekeyed.rows.iter().any(|r| r.row_id == "r1"),
            "the archived-only row must still get a delta so Origin receives it"
        );
    }

    #[test]
    fn an_unreproducible_root_is_refused() {
        let state = CrdtState::new(OLD_PEER).expect("state");
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("upsert");
        state
            .upsert("other", "o1", &[("name", LoroValue::String("bob".into()))])
            .expect("upsert");

        let Err(err) = state.rekey("users", NEW_PEER) else {
            panic!("a root outside the rekeyed collection must not be silently dropped");
        };
        assert!(
            err.to_string().contains("other"),
            "the refusal must name the root it could not re-author, got: {err}"
        );
    }
}
