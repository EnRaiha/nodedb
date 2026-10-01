// SPDX-License-Identifier: BUSL-1.1

//! `ArrayCatalogEntry` — the persistent schema digest for one array.
//!
//! The schema is carried as an opaque msgpack blob so
//! `nodedb-array`'s typed `ArraySchema` (and its transitive types) don't
//! leak into Control-Plane catalog code. The Data Plane decodes the
//! blob on open.

use nodedb_array::types::ArrayId;
use serde::{Deserialize, Serialize};

fn default_prefix_bits() -> u8 {
    8
}

/// One catalog row for a registered array.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCatalogEntry {
    pub array_id: ArrayId,
    pub name: String,
    /// zerompk-encoded `ArraySchema` (decoded on Data-Plane open).
    pub schema_msgpack: Vec<u8>,
    /// Content-addressed hash of the schema, used to reject open-with-
    /// mismatched-schema attempts without re-decoding the blob.
    pub schema_hash: u64,
    /// Wall-clock creation time, epoch-millis.
    pub created_at_ms: i64,
    /// Number of high-order Hilbert-prefix bits used to assign tiles to
    /// vShards. Immutable after CREATE ARRAY. Default 8 → 256 buckets.
    /// Existing rows without this field deserialize to 8.
    #[serde(default = "default_prefix_bits")]
    pub prefix_bits: u8,
    /// Bitemporal audit retention window in milliseconds. Compaction can
    /// discard superseded tile versions older than `now - audit_retain_ms`.
    /// `None` means retain all versions forever (default, non-bitemporal).
    #[serde(default)]
    pub audit_retain_ms: Option<i64>,
    /// Compliance floor for `audit_retain_ms`. When set, the purge engine
    /// rejects any runtime attempt to shorten `audit_retain_ms` below this
    /// value. `None` means no floor (default).
    #[serde(default)]
    pub minimum_audit_retain_ms: Option<u64>,
    /// Stamped at propose time on every replicated put. Fences a replayed
    /// delete to the incarnation it targeted.
    #[serde(default)]
    pub modification_hlc: nodedb_types::Hlc,
    /// The `modification_hlc` of the `PutArray` that created the array. ALTER
    /// and MOVE TENANT keep it, so it names one array wherever the array
    /// lives. `Hlc::ZERO` on a row no proposer stamped.
    #[serde(default)]
    pub incarnation: nodedb_types::Hlc,
}

/// The target of a MOVE TENANT array rekey.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayMove {
    /// The database the array moves to.
    pub target_db_id: u64,
    /// The moved tenant. With the source database it names the move's drain.
    pub mover_tenant_id: u64,
}
