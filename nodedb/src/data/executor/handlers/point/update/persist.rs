// SPDX-License-Identifier: BUSL-1.1

//! Landing the post-update image, with its secondary indexes, in one write.
//!
//! Separate from image construction because the concern here is atomicity, not
//! value: which of three mutually exclusive write shapes the collection takes,
//! and what has to travel with the body so no index is left describing the old
//! value. A bitemporal collection appends a version and diffs the VERSIONED
//! index; a plain collection with index paths diffs the secondary btree in the
//! same redb transaction as the body; a collection with neither takes the bare
//! self-committing put. Keeping the three side by side in one file is what
//! makes it visible that only the last one is allowed to skip the diff, and
//! that a body whose index diff cannot be computed must fail rather than write
//! alone.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::IndexPath;
use crate::types::{DatabaseId, Lsn, TenantId};

/// Inputs to [`CoreLoop::persist_point_update`].
pub(in crate::data::executor) struct PointUpdatePersist<'a> {
    pub(in crate::data::executor) config_key: &'a (DatabaseId, TenantId, String),
    pub(in crate::data::executor) database_id: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    /// Storage key (the surrogate hex).
    pub(in crate::data::executor) row_key: &'a str,
    /// The row as it was before this update — the old side of the index diff.
    pub(in crate::data::executor) current_bytes: &'a [u8],
    /// The row as it will be stored.
    pub(in crate::data::executor) updated_bytes: &'a [u8],
    pub(in crate::data::executor) bitemporal: bool,
    pub(in crate::data::executor) sys_from_ms: i64,
    pub(in crate::data::executor) wal_lsn: Option<Lsn>,
}

impl CoreLoop {
    /// Write the post-update body and reconcile the collection's secondary
    /// indexes with it.
    pub(in crate::data::executor) fn persist_point_update(
        &mut self,
        params: PointUpdatePersist<'_>,
    ) -> crate::Result<()> {
        let PointUpdatePersist {
            config_key,
            database_id,
            tid,
            collection,
            row_key,
            current_bytes,
            updated_bytes,
            bitemporal,
            sys_from_ms,
            wal_lsn,
        } = params;

        // The plain `INDEXES` secondary-index paths for this collection.
        // The non-bitemporal write must reconcile these atomically with
        // the primary body so a changed value can't leave a stale index
        // entry pointing at the old value.
        let index_paths: Vec<IndexPath> = self
            .doc_configs
            .get(config_key)
            .map(|c| c.index_paths.clone())
            .unwrap_or_default();

        let write_result = if bitemporal {
            // Bitemporal collections keep secondary-index entries in the
            // versioned index only; the update must tombstone values it
            // dropped and assert current values, atomically with the new
            // body. Decode old/new docs (storage-mode-aware) so the
            // reindex sees the real indexed values for strict + schemaless.
            let index_paths = self
                .doc_configs
                .get(config_key)
                .map(|c| c.index_paths.clone())
                .unwrap_or_default();
            // An unregistered collection has no index paths to maintain,
            // so it still takes the plain versioned put. A REGISTERED
            // one whose stored images will not decode is the separate,
            // non-skippable case: writing the body without the index
            // diff desyncs the versioned index exactly the way the
            // non-bitemporal branch below refuses to.
            let images = match self.doc_configs.get(config_key) {
                Some(cfg) => {
                    let old = self.decode_stored_document(cfg, current_bytes);
                    let new = self.decode_stored_document(cfg, updated_bytes);
                    Some(old.and_then(|o| new.map(|n| (o, n))))
                }
                None => None,
            };
            match images {
                Some(Ok((old_doc, new_doc))) => self
                    .bitemporal_update_reindex(
                        super::super::update_reindex::BitemporalUpdateReindex {
                            database_id,
                            tid,
                            collection,
                            doc_id: row_key,
                            sys_from_ms,
                            valid_from_ms: i64::MIN,
                            valid_until_ms: i64::MAX,
                            new_body: updated_bytes,
                            index_paths: &index_paths,
                            old_doc: Some(&old_doc),
                            new_doc: &new_doc,
                            wal_lsn,
                        },
                    )
                    .map(|()| None::<Vec<u8>>),
                Some(Err(e)) => Err(crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!(
                        "bitemporal update: document failed to decode for \
                         versioned-index diff (collection {collection}, id {row_key}): {e}"
                    ),
                }),
                None => self
                    .sparse
                    .versioned_put(crate::engine::sparse::btree_versioned::VersionedPut {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        doc_id: row_key,
                        sys_from_ms,
                        valid_from_ms: i64::MIN,
                        valid_until_ms: i64::MAX,
                        body: updated_bytes,
                    })
                    .map(|()| None::<Vec<u8>>),
            }
        } else if index_paths.is_empty() {
            // No secondary index to maintain — nothing to diff, so the
            // self-committing put is sufficient and avoids a redundant
            // decode of both document images.
            self.sparse
                .put(database_id, tid, collection, row_key, updated_bytes)
        } else {
            // Reconcile the plain secondary index atomically with the
            // primary body. Decode old/new (storage-mode-aware) so the
            // SET diff drops values the update removed and asserts the
            // new ones in the same redb transaction — otherwise a later
            // lookup on the new value misses the row and a lookup on the
            // old value wrongly returns it. Mirrors the bitemporal branch.
            let images = match self.doc_configs.get(config_key) {
                Some(cfg) => {
                    let old = self.decode_stored_document(cfg, current_bytes);
                    let new = self.decode_stored_document(cfg, updated_bytes);
                    old.and_then(|o| new.map(|n| (o, n)))
                }
                None => Err(crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: "collection has index paths but no registered config".into(),
                }),
            };
            match images {
                Ok((old_doc, new_doc)) => self
                    .nonbitemporal_update_reindex(
                        super::super::update_reindex::NonbitemporalUpdateReindex {
                            database_id,
                            tid,
                            collection,
                            doc_id: row_key,
                            new_body: updated_bytes,
                            index_paths: &index_paths,
                            old_doc: &old_doc,
                            new_doc: &new_doc,
                            wal_lsn,
                        },
                    )
                    .map(|()| None::<Vec<u8>>),
                Err(e) => {
                    // Both images are documents we just read / re-encoded.
                    // If one fails to decode we cannot compute the
                    // secondary-index diff, so we must NOT write the
                    // primary alone — that would silently desync the index
                    // (the very bug this path fixes). Fail loud, carrying
                    // the reason the image was unreadable.
                    Err(crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: format!(
                            "non-bitemporal update: document failed to decode for \
                             secondary-index diff (collection {collection}, id {row_key}): {e}"
                        ),
                    })
                }
            }
        };

        write_result.map(|_prior| ())
    }
}
