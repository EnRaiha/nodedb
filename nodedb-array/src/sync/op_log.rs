// SPDX-License-Identifier: Apache-2.0

//! Append-only operation log abstraction for array CRDT sync.
//!
//! [`OpLog`] is a storage-agnostic trait. Concrete implementations live in
//! `nodedb-lite` (redb-backed) and `nodedb` (WAL-backed); [`InMemoryOpLog`]
//! is a `BTreeMap`-backed implementation suitable for tests in any crate
//! that depends on `nodedb-array`.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::error::{ArrayError, ArrayResult};
use crate::sync::hlc::Hlc;
use crate::sync::op::ArrayOp;

/// Boxed fallible iterator over array ops, returned by [`OpLog`] scans.
pub type OpIter<'a> = Box<dyn Iterator<Item = ArrayResult<ArrayOp>> + 'a>;

/// Storage-agnostic interface for an append-only array operation log.
///
/// Implementations are expected to be `Send + Sync` and durable. An
/// in-memory implementation suitable for tests is provided by
/// [`InMemoryOpLog`] below.
pub trait OpLog: Send + Sync {
    /// Append an operation to the log.
    ///
    /// Must be idempotent: re-appending an op with the same `(array, hlc)`
    /// is a no-op.
    fn append(&self, op: &ArrayOp) -> ArrayResult<()>;

    /// Iterate all ops with `hlc >= from`, in HLC order, across all arrays.
    fn scan_from<'a>(&'a self, from: Hlc) -> ArrayResult<OpIter<'a>>;

    /// Iterate ops for `array` with `from <= hlc <= to`, in HLC order.
    fn scan_range<'a>(&'a self, array: &str, from: Hlc, to: Hlc) -> ArrayResult<OpIter<'a>>;

    /// Return the total number of ops in the log (across all arrays).
    fn len(&self) -> ArrayResult<u64>;

    /// Return `true` if the log contains no ops in any array.
    fn is_empty(&self) -> ArrayResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Drop all ops whose `hlc < hlc` and return the count dropped.
    ///
    /// Used by GC after snapshotting ops below the min-ack frontier.
    fn drop_below(&self, hlc: Hlc) -> ArrayResult<u64>;
}

// ─── In-memory implementation ───────────────────────────────────────────────

type OpMap = BTreeMap<(String, [u8; 18]), ArrayOp>;

/// `BTreeMap`-backed [`OpLog`] implementation.
///
/// Key: `(array_name, hlc_bytes)` so iteration is in HLC order per array.
/// Operations with the same key (same array + same HLC) are idempotent.
pub struct InMemoryOpLog {
    ops: Mutex<OpMap>,
}

impl InMemoryOpLog {
    /// Create a new empty log.
    pub fn new() -> Self {
        Self {
            ops: Mutex::new(BTreeMap::new()),
        }
    }

    fn lock(&self) -> ArrayResult<std::sync::MutexGuard<'_, OpMap>> {
        self.ops.lock().map_err(|_| ArrayError::HlcLockPoisoned)
    }
}

impl Default for InMemoryOpLog {
    fn default() -> Self {
        Self::new()
    }
}

impl OpLog for InMemoryOpLog {
    fn append(&self, op: &ArrayOp) -> ArrayResult<()> {
        let key = (op.header.array.clone(), op.header.hlc.to_bytes());
        self.lock()?.entry(key).or_insert_with(|| op.clone());
        Ok(())
    }

    fn scan_from<'a>(&'a self, from: Hlc) -> ArrayResult<OpIter<'a>> {
        let guard = self.lock()?;
        let results: Vec<ArrayOp> = guard
            .iter()
            .filter(|((_, hlc_bytes), _)| {
                let hlc = Hlc::from_bytes(hlc_bytes);
                hlc >= from
            })
            .map(|(_, op)| op.clone())
            .collect();
        Ok(Box::new(results.into_iter().map(Ok)))
    }

    fn scan_range<'a>(&'a self, array: &str, from: Hlc, to: Hlc) -> ArrayResult<OpIter<'a>> {
        let guard = self.lock()?;
        let results: Vec<ArrayOp> = guard
            .iter()
            .filter(|((arr, hlc_bytes), _)| {
                if arr != array {
                    return false;
                }
                let hlc = Hlc::from_bytes(hlc_bytes);
                hlc >= from && hlc <= to
            })
            .map(|(_, op)| op.clone())
            .collect();
        Ok(Box::new(results.into_iter().map(Ok)))
    }

    fn len(&self) -> ArrayResult<u64> {
        Ok(self.lock()?.len() as u64)
    }

    fn drop_below(&self, hlc: Hlc) -> ArrayResult<u64> {
        let mut guard = self.lock()?;
        let before = guard.len() as u64;
        guard.retain(|(_, hlc_bytes), _| Hlc::from_bytes(hlc_bytes) >= hlc);
        let after = guard.len() as u64;
        Ok(before - after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::op::{ArrayOpHeader, ArrayOpKind};
    use crate::sync::replica_id::ReplicaId;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;

    fn replica() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64, logical: u16) -> Hlc {
        Hlc::new(ms, logical, replica()).unwrap()
    }

    fn make_op(array: &str, ms: u64, logical: u16) -> ArrayOp {
        ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: hlc(ms, logical),
                schema_hlc: hlc(1, 0),
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(ms as i64)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    #[test]
    fn append_then_scan() {
        let log = InMemoryOpLog::new();
        log.append(&make_op("arr", 10, 0)).unwrap();
        log.append(&make_op("arr", 20, 0)).unwrap();
        assert_eq!(log.len().unwrap(), 2);

        let ops: Vec<_> = log
            .scan_from(Hlc::ZERO)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn scan_from_skips_below() {
        let log = InMemoryOpLog::new();
        for ms in [10, 20, 30, 40] {
            log.append(&make_op("arr", ms, 0)).unwrap();
        }

        let ops: Vec<_> = log
            .scan_from(hlc(25, 0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        // Only ms=30 and ms=40 should appear.
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.header.hlc.physical_ms >= 25));
    }

    #[test]
    fn drop_below_drops_correctly() {
        let log = InMemoryOpLog::new();
        for ms in [10, 20, 30] {
            log.append(&make_op("arr", ms, 0)).unwrap();
        }
        let dropped = log.drop_below(hlc(20, 0)).unwrap();
        assert_eq!(dropped, 1); // ms=10 dropped
        assert_eq!(log.len().unwrap(), 2);
    }

    #[test]
    fn scan_range_filters_array() {
        let log = InMemoryOpLog::new();
        log.append(&make_op("a", 10, 0)).unwrap();
        log.append(&make_op("b", 20, 0)).unwrap();
        log.append(&make_op("a", 30, 0)).unwrap();

        let ops: Vec<_> = log
            .scan_range("a", Hlc::ZERO, hlc(u64::MAX >> 16, 0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|op| op.header.array == "a"));
    }

    #[test]
    fn len_counts_all() {
        let log = InMemoryOpLog::new();
        assert_eq!(log.len().unwrap(), 0);
        log.append(&make_op("x", 1, 0)).unwrap();
        log.append(&make_op("y", 2, 0)).unwrap();
        assert_eq!(log.len().unwrap(), 2);
        // Idempotent re-append.
        log.append(&make_op("x", 1, 0)).unwrap();
        assert_eq!(log.len().unwrap(), 2);
    }
}
