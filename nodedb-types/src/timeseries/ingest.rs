// SPDX-License-Identifier: Apache-2.0

//! Ingest types, time range, and symbol dictionary.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A single metric sample (timestamp + scalar value).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MetricSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A single log entry (timestamp + arbitrary bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp_ms: i64,
    pub data: Vec<u8>,
}

/// Result of an ingest operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IngestResult {
    /// Write accepted, memtable healthy.
    Ok,
    /// Write accepted, but memtable should be flushed (memory pressure).
    FlushNeeded,
    /// Write rejected — memory budget exhausted and cannot evict further.
    Rejected,
}

impl IngestResult {
    pub fn is_flush_needed(&self) -> bool {
        matches!(self, Self::FlushNeeded)
    }

    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }
}

/// Time range for queries (inclusive on both ends).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeRange {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn contains(&self, ts: i64) -> bool {
        ts >= self.start_ms && ts <= self.end_ms
    }

    /// Whether two ranges overlap.
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start_ms <= other.end_ms && other.start_ms <= self.end_ms
    }
}

/// Bidirectional symbol dictionary for tag value interning.
///
/// Tag columns store 4-byte u32 IDs instead of full strings. Shared by
/// Origin columnar segments, Lite native segments, and WASM segments.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SymbolDictionary {
    /// String → symbol ID.
    forward: HashMap<String, u32>,
    /// Symbol ID → string (index = id).
    reverse: Vec<String>,
}

impl SymbolDictionary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve a string to its symbol ID, inserting if new.
    ///
    /// Returns `None` if the dictionary has reached `max_cardinality`.
    pub fn resolve(&mut self, value: &str, max_cardinality: u32) -> Option<u32> {
        if let Some(&id) = self.forward.get(value) {
            return Some(id);
        }
        if self.reverse.len() as u32 >= max_cardinality {
            return None;
        }
        let id = self.reverse.len() as u32;
        self.forward.insert(value.to_string(), id);
        self.reverse.push(value.to_string());
        Some(id)
    }

    /// Look up a string by symbol ID.
    pub fn get(&self, id: u32) -> Option<&str> {
        self.reverse.get(id as usize).map(|s| s.as_str())
    }

    /// Look up a symbol ID by string.
    pub fn get_id(&self, value: &str) -> Option<u32> {
        self.forward.get(value).copied()
    }

    /// Number of symbols.
    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
    }

    /// Merge another dictionary into this one.
    ///
    /// Returns a remap table: `old_id → new_id` for the source dictionary.
    pub fn merge(&mut self, other: &SymbolDictionary, max_cardinality: u32) -> Vec<u32> {
        let mut remap = Vec::with_capacity(other.reverse.len());
        for symbol in &other.reverse {
            match self.resolve(symbol, max_cardinality) {
                Some(new_id) => remap.push(new_id),
                None => remap.push(u32::MAX),
            }
        }
        remap
    }
}
