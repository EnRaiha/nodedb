// SPDX-License-Identifier: BUSL-1.1

//! Parameter structs shared by the KV CRUD handlers.

use nodedb_types::Surrogate;

/// Parameters for `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` on KV.
pub(in crate::data::executor) struct KvInsertOnConflictUpdateParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    pub surrogate: Surrogate,
}

/// Parameters for a KV point `GET`.
pub(in crate::data::executor) struct KvGetParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub rls_filters: &'a [u8],
    pub surrogate_ceiling: Option<u32>,
}

/// Parameters for a KV point write (`PUT` / `INSERT` / `INSERT ... IF ABSENT`).
pub(in crate::data::executor) struct KvWriteParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub surrogate: Surrogate,
}
