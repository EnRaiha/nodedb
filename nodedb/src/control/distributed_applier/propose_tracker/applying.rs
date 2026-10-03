// SPDX-License-Identifier: BUSL-1.1

//! The oldest committed entry of a group the apply loop has not finished.

/// The oldest committed entry of a group the apply loop has not finished:
/// what a propose waiter that timed out names as the entry its group's
/// applied index waits behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyingEntry {
    pub group_id: u64,
    pub log_index: u64,
    /// The collection the entry's plan writes, once the apply decoded it.
    pub collection: Option<String>,
}

impl std::fmt::Display for ApplyingEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group {} index {}", self.group_id, self.log_index)?;
        if let Some(collection) = &self.collection {
            write!(f, " writing '{collection}'")?;
        }
        Ok(())
    }
}
