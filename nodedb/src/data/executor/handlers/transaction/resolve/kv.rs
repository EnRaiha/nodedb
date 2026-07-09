// SPDX-License-Identifier: BUSL-1.1

//! KV serializer for transaction resolve.
//!
//! Turns the KV post-images a transaction staged into its overlay into the
//! engine-native WAL sub-record shapes the KV replay path decodes — the SAME
//! shapes the autocommit KV WAL path produces, so producer and replay share one
//! encoding:
//!
//! * A staged value ([`Staged::Put`]) → `RecordType::Put`, `encode_kv_put`'s
//!   `("kv_put", collection, key, value, ttl_ms, [expire_at_ms])`. The overlay
//!   holds the resolved ABSOLUTE post-image (an atomic `Incr`/`Cas`/`GetSet`
//!   stages its computed value, never a delta), so resolve emits exactly that.
//!   When the overlay carries an absolute expiry ([`StagedTtl::ExpireAt`]) for
//!   the slot, the six-element form carries it verbatim so replay installs the
//!   exact instant instead of recomputing `now + ttl`. A `Persist` (or no TTL
//!   delta) emits the five-element form (no expiry).
//! * A staged tombstone ([`Staged::Tombstone`]) → `RecordType::Delete`,
//!   `("kv_delete", collection, [key])`.
//!
//! ## `ttl_ms` in the redo payload
//!
//! The overlay stores only the resolved ABSOLUTE expiry instant, never the
//! original relative `ttl_ms`. Replay of the six-element form installs the
//! absolute instant and ignores the relative `ttl_ms`; the five-element form
//! carries no expiry at all. The `ttl_ms` slot is therefore vestigial in a redo
//! sub-record, and is set to `0` in every case.
//!
//! ## Determinism
//!
//! The overlay keys slots by surrogate in a `HashMap`, so entries are collected
//! into a `BTreeMap` keyed by the overlay doc-id (lowercase-hex of the KV key)
//! before emitting. Two replicas resolving the same transaction produce
//! byte-identical redo ops.

use std::collections::BTreeMap;

use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch_kv::encode::encode_kv_put;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl, TxnOverlay};
use crate::data::executor::handlers::transaction::stage_write::unhex_key;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

/// The relative `ttl_ms` written into every KV redo sub-record. The absolute
/// expiry instant (when present) is authoritative on replay and the overlay
/// never retains the original relative TTL, so this slot is always `0`.
const RESOLVE_TTL_MS: u64 = 0;

/// Append the redo sub-records for every KV post-image staged in `overlay`
/// for `coll_key` to `ops`, in deterministic doc-id order.
pub(super) fn serialize_kv_collection(
    overlay: &TxnOverlay,
    coll_key: &(DatabaseId, TenantId, String),
    collection: &str,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut entries: BTreeMap<String, &Staged> = BTreeMap::new();
    for (doc_id, staged) in overlay.iter_doc_entries_for_collection(coll_key) {
        entries.insert(doc_id.to_string(), staged);
    }

    for (doc_id, staged) in entries {
        let key = unhex_key(&doc_id).ok_or_else(|| crate::Error::Internal {
            detail: format!("kv resolve: overlay doc-id '{doc_id}' is not valid hex"),
        })?;
        match staged {
            Staged::Put(value) => {
                let expire_at_ms = match overlay.get_ttl_by_doc_id(coll_key, &doc_id) {
                    Some(StagedTtl::ExpireAt(ms)) => Some(ms),
                    Some(StagedTtl::Persist) | None => None,
                };
                let payload = encode_kv_put(collection, &key, value, RESOLVE_TTL_MS, expire_at_ms)?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload,
                });
            }
            Staged::Tombstone => {
                let payload = zerompk::to_msgpack_vec(&("kv_delete", collection, vec![key]))
                    .map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("kv resolve delete: {e}"),
                    })?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::Delete as u32,
                    payload,
                });
            }
        }
    }
    Ok(())
}
