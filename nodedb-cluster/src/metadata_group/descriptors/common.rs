// SPDX-License-Identifier: BUSL-1.1

//! Shared descriptor identity + header.

use nodedb_types::Hlc;
use serde::{Deserialize, Serialize};

use crate::metadata_group::state::DescriptorState;

/// Globally unique, database- and tenant-scoped identifier for a schema descriptor.
///
/// `kind` disambiguates object types sharing the same
/// `(database_id, tenant_id, name)` (e.g. a collection and an index can both
/// be named `orders`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DescriptorId {
    pub database_id: u64,
    pub tenant_id: u64,
    pub kind: DescriptorKind,
    pub name: String,
}

impl DescriptorId {
    pub fn new(
        database_id: u64,
        tenant_id: u64,
        kind: DescriptorKind,
        name: impl Into<String>,
    ) -> Self {
        Self {
            database_id,
            tenant_id,
            kind,
            name: name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn database_id_participates_in_equality_hash_and_wire_format() {
        let alpha = DescriptorId::new(7, 1, DescriptorKind::Collection, "orders");
        let beta = DescriptorId::new(8, 1, DescriptorKind::Collection, "orders");

        assert_ne!(alpha, beta);
        let mut ids = HashSet::new();
        ids.insert(alpha.clone());
        ids.insert(beta.clone());
        assert_eq!(ids.len(), 2);

        let encoded = zerompk::to_msgpack_vec(&alpha).expect("encode descriptor id");
        let decoded: DescriptorId = zerompk::from_msgpack(&encoded).expect("decode descriptor id");
        assert_eq!(decoded, alpha);
        assert_eq!(decoded.database_id, 7);
    }
}

/// Discriminant for [`DescriptorId`] — one variant per schema object type.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DescriptorKind {
    Collection,
    Index,
    Trigger,
    Sequence,
    User,
    Role,
    Grant,
    Rls,
    ChangeStream,
    MaterializedView,
    Schedule,
    Function,
    Procedure,
    Tenant,
    ApiKey,
    AuditRetention,
}

/// Common header embedded in every descriptor.
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
pub struct DescriptorHeader {
    pub id: DescriptorId,
    /// Monotonic version; incremented on every Alter.
    pub version: u64,
    /// HLC at which this version was committed.
    pub modification_time: Hlc,
    /// Lifecycle state.
    pub state: DescriptorState,
}

impl DescriptorHeader {
    pub fn new_public(id: DescriptorId, version: u64, modification_time: Hlc) -> Self {
        Self {
            id,
            version,
            modification_time,
            state: DescriptorState::Public,
        }
    }
}
