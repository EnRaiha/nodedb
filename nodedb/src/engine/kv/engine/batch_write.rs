//! Batch PUT operations for the KV engine.

use nodedb_types::Surrogate;

use super::KvEngine;
use crate::engine::kv::batch_put::KvBatchPutParams;
use crate::engine::kv::engine_write::KvPutParams;

impl KvEngine {
    /// BATCH PUT: insert/update multiple pairs. Returns count of new keys.
    ///
    /// `surrogates` carries each entry's stable cross-engine identity,
    /// same order and length as `entries` -- assigned by the CP-side
    /// `SurrogateAssigner` from `(collection, key)`, same mechanism as a
    /// single-key `put`. Pass `Surrogate::ZERO` per-entry only from internal
    /// RMW callers that do not allocate one (existing entries preserve
    /// their bound surrogate either way, per `put`'s semantics).
    pub fn batch_put(&mut self, params: KvBatchPutParams<'_>) -> usize {
        let KvBatchPutParams {
            database_id,
            tenant_id,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        } = params;
        let mut new_count = 0;
        for (i, (key, value)) in entries.iter().enumerate() {
            let surrogate = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
            if self
                .put(KvPutParams {
                    database_id,
                    tenant_id,
                    collection,
                    key: key.as_slice(),
                    value: value.as_slice(),
                    ttl_ms,
                    now_ms,
                    surrogate,
                })
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }

    /// BATCH PUT installing an already-resolved absolute expiry instant on
    /// every entry. Mirrors [`KvEngine::put_with_absolute_expiry`]: WAL redo
    /// replay uses this so a TTL'd batch recovers with the exact expiry the
    /// original write computed, rather than recomputing `now_ms + ttl_ms` at
    /// recovery time (which would push expiry forward by the crash-to-restart
    /// delay). `params.ttl_ms` is carried through `put_with_absolute_expiry`
    /// only for `KvPutParams`'s shape; the installed expiry is `expire_at_ms`
    /// verbatim, same for every entry in the batch.
    pub fn batch_put_with_absolute_expiry(
        &mut self,
        params: KvBatchPutParams<'_>,
        expire_at_ms: u64,
    ) -> usize {
        let KvBatchPutParams {
            database_id,
            tenant_id,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        } = params;
        let mut new_count = 0;
        for (i, (key, value)) in entries.iter().enumerate() {
            let surrogate = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
            if self
                .put_with_absolute_expiry(
                    KvPutParams {
                        database_id,
                        tenant_id,
                        collection,
                        key: key.as_slice(),
                        value: value.as_slice(),
                        ttl_ms,
                        now_ms,
                        surrogate,
                    },
                    expire_at_ms,
                )
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }
}
