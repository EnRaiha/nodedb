// SPDX-License-Identifier: BUSL-1.1

//! The node that served each vShard the running statement read.
//!
//! A read's versions are positions in the WAL of the node that served it, so
//! only that node's versions compare with them at commit. Every read dispatch
//! notes the vShard it read and the node that answered. The read-set capture
//! stamps each entry with that node (`ReadSetEntry::home_node`). A vShard two
//! nodes served in one statement has no one node, noted as `0`, and a commit
//! then treats its read as changed.
//!
//! The notes live in the connection scope and start empty with each
//! statement (`deadline::enter`). Outside a connection scope nothing is noted.

use std::collections::HashMap;

use super::conn_scope::with_scope;

/// Note that `node` served a read of `vshard` in the running statement.
pub fn note(vshard: u32, node: u64) {
    with_scope((), |scope| {
        scope
            .served_reads
            .borrow_mut()
            .entry(vshard)
            .and_modify(|seen| {
                if *seen != node {
                    *seen = 0;
                }
            })
            .or_insert(node);
    });
}

/// The node that served the running statement's read of `vshard`: `Some(0)`
/// when two nodes did, `None` when no read of it was noted.
pub fn served_by(vshard: u32) -> Option<u64> {
    with_scope(None, |scope| {
        scope.served_reads.borrow().get(&vshard).copied()
    })
}

/// Forget every note: a statement starts with no reads served.
pub(super) fn clear() {
    with_scope((), |scope| scope.served_reads.borrow_mut().clear());
}

/// An empty note map for a fresh connection scope.
pub(super) fn empty() -> std::cell::RefCell<HashMap<u32, u64>> {
    std::cell::RefCell::new(HashMap::new())
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    #[tokio::test]
    async fn a_vshard_two_nodes_served_has_no_node() {
        conn_scope::scoped(async {
            note(3, 1);
            note(3, 1);
            note(4, 1);
            note(4, 2);
            assert_eq!(served_by(3), Some(1));
            assert_eq!(served_by(4), Some(0));
            assert_eq!(served_by(5), None);
            clear();
            assert_eq!(served_by(3), None);
        })
        .await;
    }
}
