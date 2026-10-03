// SPDX-License-Identifier: BUSL-1.1

/// A write the change feed cannot carry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChangeStreamError {
    /// A write on `collection` yields change events but applies outside any
    /// replicated entry. Every change event is published at its entry's log
    /// position, so such a write has no position on any feed.
    #[error(
        "a write on '{collection}' yields change events but took an unreplicated route; every \
         such write must apply through its replicated entry"
    )]
    UnreplicatedChange { collection: String },
}

impl From<ChangeStreamError> for crate::Error {
    fn from(error: ChangeStreamError) -> Self {
        Self::Internal {
            detail: error.to_string(),
        }
    }
}
