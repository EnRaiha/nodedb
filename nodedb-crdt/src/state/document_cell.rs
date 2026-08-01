// SPDX-License-Identifier: Apache-2.0

//! The document handle and every value derived from it.

use std::cell::RefCell;
use std::ops::Deref;

use loro::LoroDoc;

/// A measured point relating the document's operation count to its encoded
/// size, taken from one real snapshot export.
struct Calibration {
    ops: usize,
    bytes: usize,
}

impl Calibration {
    /// Encoded size implied by `ops`, holding the measured bytes-per-operation
    /// ratio fixed. Widened to `u128` because a large document's
    /// `bytes * ops` overflows `usize` on 32-bit targets long before either
    /// factor does.
    fn scale_to(&self, ops: usize) -> usize {
        let scaled = self.bytes as u128 * ops as u128 / self.ops as u128;
        usize::try_from(scaled).unwrap_or(usize::MAX)
    }

    /// Whether `ops` is close enough to the measured point to interpolate
    /// from it. Encoded density is stable within a document — what changes it
    /// is a different *kind* of content, which arrives gradually — so the
    /// ratio is re-measured once the operation count has halved or doubled.
    ///
    /// Bounding recalibration to a doubling makes the export cost amortise to
    /// O(1) per write, rather than being paid on every write.
    fn covers(&self, ops: usize) -> bool {
        ops > 0 && ops <= self.ops.saturating_mul(2) && ops.saturating_mul(2) >= self.ops
    }
}

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
    /// Last real measurement of encoded size, and the operation count it was
    /// taken at. `None` until the estimate is first asked for.
    calibration: RefCell<Option<Calibration>>,
    /// Real snapshot exports performed to answer `estimated_bytes`. The point
    /// of the calibration is that this grows logarithmically with the number
    /// of writes, not linearly, which is a property worth asserting.
    #[cfg(test)]
    exports: std::cell::Cell<usize>,
}

impl DocumentCell {
    /// Wrap a document with an empty derived-value cache.
    pub(in crate::state) fn new(doc: LoroDoc) -> Self {
        Self {
            doc,
            calibration: RefCell::new(None),
            #[cfg(test)]
            exports: std::cell::Cell::new(0),
        }
    }

    /// Swap in a different document, discarding everything cached from the
    /// previous one. The only way to reassign the document.
    pub(in crate::state) fn replace(&mut self, doc: LoroDoc) {
        *self = Self::new(doc);
    }

    /// Estimated encoded size in bytes, as a proxy for memory footprint.
    ///
    /// Loro exposes no direct memory metric, and a snapshot export — the
    /// honest proxy — costs O(document). Callers put this on the write path
    /// (a memory governor updated after every operation), so paying a full
    /// re-encode per call means every write re-serialises the whole document:
    /// ~100 ms per write on a 4 MB document, and proportionally worse above
    /// that.
    ///
    /// So the export is used to *calibrate* rather than to answer. `len_ops`
    /// is an inlined oplog counter, and it counts operations that are still in
    /// an open transaction, so it tracks writes the moment they happen. The
    /// answer is that counter scaled by the measured bytes-per-operation, and
    /// a real export runs only when the count leaves the calibrated range.
    ///
    /// Exact whenever the document has not changed since it was measured;
    /// an interpolation otherwise, which is what a pressure signal needs.
    pub(in crate::state) fn estimated_bytes(&self) -> usize {
        let ops = self.doc.len_ops();
        if let Some(calibration) = self.calibration.borrow().as_ref() {
            if ops == calibration.ops {
                return calibration.bytes;
            }
            if calibration.covers(ops) {
                return calibration.scale_to(ops);
            }
        }

        #[cfg(test)]
        self.exports.set(self.exports.get() + 1);
        let Ok(snapshot) = self.doc.export(loro::ExportMode::Snapshot) else {
            // A failed export is not a measurement. Caching the zero would pin
            // the document at "empty" until enough writes moved it out of
            // range again.
            return 0;
        };
        let bytes = snapshot.len();
        // An empty document is not a ratio: it would divide by zero, and the
        // export that measured it was trivial anyway.
        if ops > 0 {
            *self.calibration.borrow_mut() = Some(Calibration { ops, bytes });
        }
        bytes
    }

    /// How many real snapshot exports `estimated_bytes` has performed.
    #[cfg(test)]
    pub(in crate::state) fn export_count(&self) -> usize {
        self.exports.get()
    }
}

impl Deref for DocumentCell {
    type Target = LoroDoc;

    fn deref(&self) -> &Self::Target {
        &self.doc
    }
}
