// SPDX-License-Identifier: BUSL-1.1

//! Metadata timelines: the histories of the metadata log.
//!
//! A cluster that never restored writes the root timeline. A restore starts
//! a new timeline that branches off the restored one at the restore index:
//! the last metadata log index the restore keeps. The parent can go on
//! writing entries above the branch index, and those belong to the parent
//! alone.
//!
//! The restored log copies the parent's entries from its base up to the
//! branch index, with each entry the restore drops emptied. The child's
//! archive holds those copies, and they win over the parent's objects at the
//! same index. The parent serves only the indexes at or below the branch
//! index the child lacks: entries every restored base already held, which
//! the restore never drops.
//!
//! The archive keys every node life's objects by the timeline it writes (see
//! [`crate::storage::raft_log_archive`]), so two histories never share a key.
//! Each child timeline has a descriptor at
//! `{prefix}raft/timelines/t{timeline:020}.bin` naming its parent and branch
//! index. [`resolve_chain`] walks the descriptors from a timeline to the root.
//!
//! A cluster restore's timeline is its restore generation, so every node of
//! the restore writes the same one. A node restore mints a random timeline
//! with the top bit set, which no generation reaches.

use std::collections::BTreeSet;
use std::sync::Arc;

use object_store::ObjectStore;

use crate::storage::raft_log_archive::{fetch_raw, open_bound, put_chunk, seal_bound};

/// The timeline of a cluster that never restored.
pub const ROOT_TIMELINE: u64 = 0;

/// Every node-restore timeline has this bit set.
const NODE_RESTORE_BIT: u64 = 1 << 63;

/// Bound into a descriptor's authenticated payload, ahead of its key.
const TIMELINE_MAGIC: &[u8; 4] = b"RTLN";

/// Longest chain [`resolve_chain`] walks before it reports a cycle.
const MAX_CHAIN: usize = 4096;

/// Where a child timeline branches off its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct TimelineBranch {
    pub timeline: u64,
    pub parent: u64,
    /// The last index the child takes from the parent's history. Every
    /// index above it is the child's own.
    pub branch_index: u64,
}

/// The indexes one timeline of a chain serves: every index through
/// `through`, or every index for the timeline the chain starts at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineSpan {
    pub timeline: u64,
    pub through: Option<u64>,
}

impl TimelineSpan {
    /// Whether this span serves `index`.
    pub fn holds(&self, index: u64) -> bool {
        self.through.is_none_or(|through| index <= through)
    }
}

fn timeline_err(detail: String) -> crate::Error {
    crate::Error::ColdStorage { detail }
}

/// The key of `timeline`'s descriptor.
pub fn descriptor_key(prefix: &str, timeline: u64) -> String {
    format!("{prefix}raft/timelines/t{timeline:020}.bin")
}

/// The timeline of the cluster restore that claimed `generation`.
pub fn cluster_restore_timeline(generation: u64) -> u64 {
    generation
}

/// A new timeline for a node restore.
pub fn mint_node_restore_timeline() -> crate::Result<u64> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|error| crate::Error::Storage {
        engine: "metadata_timeline".into(),
        detail: format!("mint metadata timeline: {error}"),
    })?;
    Ok(u64::from_le_bytes(bytes) | NODE_RESTORE_BIT)
}

/// Store the descriptor of `branch.timeline`, replacing the one there. Every
/// node of a cluster restore writes the same descriptor.
pub async fn put_branch(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    branch: TimelineBranch,
    node_name: &str,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<()> {
    let key = descriptor_key(prefix, branch.timeline);
    let body = zerompk::to_msgpack_vec(&branch)
        .map_err(|e| timeline_err(format!("encode metadata timeline {key}: {e}")))?;
    let sealed = seal_bound(
        TIMELINE_MAGIC,
        &key,
        &body,
        node_name,
        branch.branch_index,
        wal_key,
    )?;
    put_chunk(store, &key, sealed).await
}

/// The descriptor of `timeline`, `None` when it has none.
pub async fn fetch_branch(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    timeline: u64,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Option<TimelineBranch>> {
    let key = descriptor_key(prefix, timeline);
    let Some(raw) = fetch_raw(store, &key).await? else {
        return Ok(None);
    };
    let body = open_bound(TIMELINE_MAGIC, &key, &raw, wal_key)?;
    let branch: TimelineBranch = zerompk::from_msgpack(&body)
        .map_err(|e| timeline_err(format!("decode metadata timeline {key}: {e}")))?;
    if branch.timeline != timeline {
        return Err(timeline_err(format!(
            "metadata timeline descriptor {key} names timeline {}",
            branch.timeline
        )));
    }
    Ok(Some(branch))
}

/// The spans of `timeline` and every timeline it descends from, `timeline`
/// first and the root last. A reader takes each index from the first span
/// that holds it and has it archived.
pub async fn resolve_chain(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    timeline: u64,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<TimelineSpan>> {
    let mut spans = Vec::new();
    let mut seen = BTreeSet::new();
    let mut current = timeline;
    let mut through: Option<u64> = None;
    loop {
        if !seen.insert(current) || seen.len() > MAX_CHAIN {
            return Err(timeline_err(format!(
                "metadata timeline {timeline} descends from a cycle at timeline {current}"
            )));
        }
        spans.push(TimelineSpan {
            timeline: current,
            through,
        });
        match fetch_branch(store, prefix, current, wal_key).await? {
            Some(branch) => {
                // An ancestor serves no index a descendant took as its own.
                through = Some(
                    through.map_or(branch.branch_index, |limit| limit.min(branch.branch_index)),
                );
                current = branch.parent;
            }
            None if current == ROOT_TIMELINE => return Ok(spans),
            None => {
                return Err(timeline_err(format!(
                    "metadata timeline {current} has no descriptor under {}",
                    descriptor_key(prefix, current)
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn wal_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x21; 32]).unwrap()
    }

    #[tokio::test]
    async fn a_chain_runs_from_the_root_to_the_timeline() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for branch in [
            TimelineBranch {
                timeline: 3,
                parent: ROOT_TIMELINE,
                branch_index: 10,
            },
            TimelineBranch {
                timeline: 8,
                parent: 3,
                branch_index: 25,
            },
        ] {
            put_branch(&store, "p/", branch, "node-1", &wal_key())
                .await
                .unwrap();
        }
        let chain = resolve_chain(&store, "p/", 8, &wal_key()).await.unwrap();
        assert_eq!(
            chain,
            [
                TimelineSpan {
                    timeline: 8,
                    through: None
                },
                TimelineSpan {
                    timeline: 3,
                    through: Some(25)
                },
                TimelineSpan {
                    timeline: 0,
                    through: Some(10)
                },
            ]
        );
        assert!(chain[0].holds(26));
        assert!(chain[1].holds(25) && !chain[1].holds(26));
        assert!(chain[2].holds(10) && !chain[2].holds(11));

        assert_eq!(
            resolve_chain(&store, "p/", ROOT_TIMELINE, &wal_key())
                .await
                .unwrap(),
            [TimelineSpan {
                timeline: 0,
                through: None
            }]
        );
        assert!(
            resolve_chain(&store, "p/", 99, &wal_key()).await.is_err(),
            "a timeline with no descriptor has no chain"
        );
    }

    #[test]
    fn node_restore_timelines_never_meet_a_generation() {
        let timeline = mint_node_restore_timeline().unwrap();
        assert_ne!(timeline & NODE_RESTORE_BIT, 0);
        assert_eq!(cluster_restore_timeline(5), 5);
    }
}
