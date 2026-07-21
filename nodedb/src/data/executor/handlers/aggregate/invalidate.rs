// SPDX-License-Identifier: BUSL-1.1

//! Shared derived-result-cache invalidation sweep, used by every write and
//! object-lifecycle path that can make cached collection results stale. The
//! cache stores both aggregate and facet payloads.
//!
//! Keyed on `(database, tenant, "{collection}\0...")` — every cached entry for
//! a given collection shares that null-byte-terminated prefix (see
//! `aggregate_cache_key` in `cache_key.rs` and `facet_cache_key` in
//! `facet.rs`). Routing all invalidation through this method keeps the encoded
//! key invariant out of callers and evicts exactly one collection's entries.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Evict every cached aggregate or facet result for
    /// `(database_id, tid, collection)`.
    ///
    /// Write callers gate this on whether rows changed. Lifecycle callers run
    /// it unconditionally when an object is unregistered.
    pub(in crate::data::executor) fn invalidate_aggregate_cache_for_collection(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) {
        let database_key = crate::types::DatabaseId::new(database_id);
        let tid_key = crate::types::TenantId::new(tid);
        let coll_prefix = format!("{collection}\0");
        self.aggregate_cache.retain(|(d, t, rest), _| {
            !(*d == database_key && *t == tid_key && rest.starts_with(&coll_prefix))
        });
    }
}
