// SPDX-License-Identifier: BUSL-1.1

//! Owner of one descriptor drain.

use serde::{Deserialize, Serialize};

/// Who started a descriptor drain.
///
/// A descriptor stays drained while any owner's drain remains. Each owner ends
/// only its own drain, so one owner never ends another's early.
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
pub enum DrainOwner {
    /// The catalog DDL altering the descriptor. The replicated DDL preparation
    /// lease serialises every DDL, so at most one DDL drain exists per
    /// descriptor. The apply of that DDL's own catalog entry ends it.
    Ddl,
    /// A `MOVE TENANT` of `tenant_id` out of `source_db_id`. The apply of its
    /// cutover entry ends it.
    MoveTenant { tenant_id: u64, source_db_id: u64 },
    /// The clone materializer copying a source into one clone collection.
    CloneMaterialize {
        clone_database: u64,
        tenant_id: u64,
        clone_collection: String,
    },
}

impl std::fmt::Display for DrainOwner {
    /// Names the operation the drain belongs to, for a refused statement's
    /// error message.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ddl => f.write_str("a DDL is altering the collection"),
            Self::MoveTenant {
                tenant_id,
                source_db_id,
            } => write!(
                f,
                "the collection is being moved: MOVE TENANT {tenant_id} out of database \
                 {source_db_id}"
            ),
            Self::CloneMaterialize {
                clone_database,
                clone_collection,
                ..
            } => write!(
                f,
                "clone materializing: '{clone_collection}' in database {clone_database} \
                 is copying from this collection"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_names_the_owner() {
        assert!(
            DrainOwner::MoveTenant {
                tenant_id: 3,
                source_db_id: 1024
            }
            .to_string()
            .contains("being moved")
        );
        assert!(
            DrainOwner::CloneMaterialize {
                clone_database: 1025,
                tenant_id: 1,
                clone_collection: "kv".into(),
            }
            .to_string()
            .contains("clone materializing")
        );
    }
}
