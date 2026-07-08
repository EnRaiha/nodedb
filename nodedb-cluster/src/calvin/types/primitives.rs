// SPDX-License-Identifier: BUSL-1.1

//! Primitive Calvin type definitions.
//!
//! [`SortedVec`], [`EngineKeySet`], and [`PassiveReadKey`] live in
//! `nodedb-types` so the physical-plan IR can reference them without
//! pulling in the distributed scheduler. [`DependentReadSpec`] stays
//! here because it is scheduler-internal.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use nodedb_types::calvin::{
    EngineKeySet, EngineTag, PassiveReadKey, ReadKeyIdent, SortedVec, VersionedReadEntry,
    VersionedReadSet,
};

/// Describes the passive-read participants for a dependent-read Calvin txn.
///
/// Each entry maps a vshard id to the keys that vshard must read and broadcast
/// to all active participants before any writes can proceed.
///
/// `BTreeMap` is mandatory here: the sequencer and scheduler must iterate
/// vshards in a deterministic order (determinism contract).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DependentReadSpec {
    /// Passive participants: vshard → keys to read.
    pub passive_reads: BTreeMap<u32, Vec<PassiveReadKey>>,
}

impl DependentReadSpec {
    /// Total estimated serialized bytes across all passive read keys.
    ///
    /// Used by the sequencer admission check to enforce
    /// `max_dependent_read_bytes_per_txn`.  This is an O(1)-per-key
    /// estimate, not an exact serialized size.
    pub fn total_bytes(&self) -> usize {
        self.passive_reads
            .values()
            .flat_map(|ks| ks.iter())
            .map(|k| k.engine_key.serialized_size_hint())
            .sum()
    }
}
