// SPDX-License-Identifier: Apache-2.0

//! The document handle and every value derived from it.

use std::cell::RefCell;
use std::ops::Deref;

use loro::LoroDoc;

/// A `LoroDoc` together with the values cached from it.
///
/// Compaction does not mutate a document, it replaces one: `compact_history`
/// and `compact_at_version` build a fresh doc from a shallow snapshot and swap
/// it in. Anything derived from the old doc and stored beside it survives that
/// swap and goes on describing a document that no longer exists.
///
/// The size estimate is the case that bites. A shallow snapshot preserves the
/// version vector — peers have to keep delta-syncing across a compaction — so
/// a cache keyed on the version alone still looks current after the bytes it
/// measured are gone. A caller polling the estimate to decide when to compact
/// would then never observe its own compaction landing, and would compact
/// again, and again.
///
/// Keeping the cache *inside* the cell removes the possibility: `replace` is
/// the only way to swap the document, and it drops the derived state with it.
/// Reads go through `Deref`, so every `self.doc.…` call site is untouched.
pub(in crate::state) struct DocumentCell {
    doc: LoroDoc,
    /// Snapshot size and the oplog version it was measured at. `None` until
    /// the estimate is first asked for.
    memory_estimate: RefCell<Option<(loro::VersionVector, usize)>>,
}

impl DocumentCell {
    /// Wrap a document with an empty derived-value cache.
    pub(in crate::state) fn new(doc: LoroDoc) -> Self {
        Self {
            doc,
            memory_estimate: RefCell::new(None),
        }
    }

    /// Swap in a different document, discarding everything cached from the
    /// previous one. The only way to reassign the document.
    pub(in crate::state) fn replace(&mut self, doc: LoroDoc) {
        *self = Self::new(doc);
    }

    /// Snapshot size in bytes, as a proxy for memory footprint.
    ///
    /// Loro exposes no direct memory metric. A snapshot export is proportional
    /// to state size, which is good enough for pressure monitoring but costs
    /// O(document) — and the callers that want it are polling loops. It is
    /// therefore measured once per version rather than once per call.
    /// The version is a sound cache key because `oplog_vv` counts operations
    /// that are still in an open transaction — a write is visible to the key
    /// the moment it happens, not when it commits. `state::tests` pins that
    /// property, since the cache is wrong the day it stops holding.
    pub(in crate::state) fn estimated_bytes(&self) -> usize {
        let version = self.doc.oplog_vv();
        if let Some((measured_at, bytes)) = self.memory_estimate.borrow().as_ref()
            && *measured_at == version
        {
            return *bytes;
        }

        let Ok(snapshot) = self.doc.export(loro::ExportMode::Snapshot) else {
            // A failed export is not a measurement. Caching the zero would pin
            // the document at "empty" until its next write moved the version.
            return 0;
        };
        let bytes = snapshot.len();
        *self.memory_estimate.borrow_mut() = Some((version, bytes));
        bytes
    }
}

impl Deref for DocumentCell {
    type Target = LoroDoc;

    fn deref(&self) -> &Self::Target {
        &self.doc
    }
}
