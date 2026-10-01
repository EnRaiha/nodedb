// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard graph reads recorded into the transaction read-set.
//!
//! A cluster graph read (a MATCH, a walk, a gathered algorithm) reads edges on
//! many key vShards through a coordinator that holds no session. The
//! coordinator notes every vShard it read, each at the watermark that shard
//! served, in the connection scope ([`note`]). Each protocol records the noted
//! reads when the request ends ([`record_pending`]).
//!
//! Each vShard becomes one [`ReadSetEntry`] homed on that vShard. Commit
//! validation then checks the entry on the vShard that holds the edges:
//!
//! - A Calvin participant checks the collection's write floor on that vShard.
//! - A read-only commit fetches that floor from the vShard's leader
//!   (`commit::homed_reads`).
//!
//! The read version is the served watermark. A collection's write floor on a
//! core never exceeds the core watermark, and a later write to the collection
//! on that core carries a higher LSN. So a floor above the recorded watermark
//! means the collection changed on that vShard after the read.

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::conn_scope::with_scope;
use super::connection::SessionId;
use super::read_set::{EngineTag, ReadKey, ReadOrigin, ReadSetEntry};
use super::store::SessionStore;

/// One vShard a graph read observed, at the watermark its core served, and
/// the node that served it (`0` when more than one did).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardObservation {
    pub vshard: VShardId,
    pub watermark: Lsn,
    pub node: u64,
}

/// The vShards one cross-shard graph read observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphShardReads {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The database-qualified collection the read scoped, or `None` when it
    /// walked every collection.
    pub collection: Option<String>,
    pub shards: Vec<ShardObservation>,
}

/// Note a cross-shard graph read for the request now running. Outside a
/// connection scope there is no session to record it for, so it is dropped.
pub fn note(reads: GraphShardReads) {
    if reads.shards.is_empty() {
        return;
    }
    let mut pending = Some(reads);
    with_scope((), |scope| {
        if let Some(reads) = pending.take() {
            scope.graph_reads.borrow_mut().push(reads);
        }
    });
}

/// Take every graph read the request noted, oldest first.
pub fn take() -> Vec<GraphShardReads> {
    with_scope(Vec::new(), |scope| {
        std::mem::take(&mut *scope.graph_reads.borrow_mut())
    })
}

/// Record the graph reads the request noted into `session_id`'s transaction
/// read-set. Outside a transaction block the reads are dropped.
pub fn record_pending(sessions: &SessionStore, session_id: SessionId) {
    let pending = take();
    if pending.is_empty() || !sessions.is_in_transaction_block(session_id) {
        return;
    }
    for reads in pending {
        let entries = homed_entries(sessions, session_id, reads);
        sessions.record_read_entries(session_id, entries);
    }
}

/// One predicate entry per observed vShard, homed there.
fn homed_entries(
    sessions: &SessionStore,
    session_id: SessionId,
    reads: GraphShardReads,
) -> Vec<ReadSetEntry> {
    let GraphShardReads {
        tenant_id,
        database_id,
        collection,
        shards,
    } = reads;
    let collection = collection.unwrap_or_default();
    // The session's own committed writes to the collection raise the read
    // version, as `record_read_set` does, so a read never aborts on the
    // session's own earlier write.
    let own_write_version = if collection.is_empty() {
        Lsn::ZERO
    } else {
        sessions.own_write_version(session_id, database_id, tenant_id, &collection)
    };
    shards
        .into_iter()
        .map(|shard| ReadSetEntry {
            engine: EngineTag::Graph,
            database_id,
            tenant_id,
            collection: collection.clone(),
            key: ReadKey::Predicate,
            read_lsn: shard.watermark,
            read_version_lsn: shard.watermark.max(own_write_version),
            origin: ReadOrigin::Session,
            home: Some(shard.vshard),
            home_node: shard.node,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    fn session_id() -> SessionId {
        SessionId::from(
            "127.0.0.1:5611"
                .parse::<std::net::SocketAddr>()
                .expect("test address"),
        )
    }

    fn reads(collection: Option<&str>) -> GraphShardReads {
        GraphShardReads {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            collection: collection.map(str::to_owned),
            shards: vec![
                ShardObservation {
                    vshard: VShardId::new(3),
                    watermark: Lsn::new(10),
                    node: 1,
                },
                ShardObservation {
                    vshard: VShardId::new(9),
                    watermark: Lsn::new(20),
                    node: 2,
                },
            ],
        }
    }

    fn store_in_block() -> (SessionStore, SessionId) {
        let sessions = SessionStore::new();
        let session_id = session_id();
        sessions.ensure_session(match session_id {
            SessionId::LegacySocket(addr) => addr,
            SessionId::Connection(_) => unreachable!("legacy test identity"),
        });
        sessions.begin(session_id, Lsn::new(5), 0).expect("begin");
        (sessions, session_id)
    }

    #[tokio::test]
    async fn each_observed_vshard_becomes_one_homed_entry() {
        let (sessions, session_id) = store_in_block();
        conn_scope::scoped(async {
            note(reads(Some("db1.edges")));
            record_pending(&sessions, session_id);
        })
        .await;
        let entries = sessions.take_read_set(session_id);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].home, Some(VShardId::new(3)));
        assert_eq!(entries[0].read_version_lsn, Lsn::new(10));
        assert_eq!(entries[1].home, Some(VShardId::new(9)));
        assert_eq!(entries[1].read_version_lsn, Lsn::new(20));
        assert_eq!(entries[1].home_node, 2);
        assert!(entries.iter().all(|e| e.key == ReadKey::Predicate));
        assert!(entries.iter().all(|e| e.collection == "db1.edges"));
    }

    #[tokio::test]
    async fn a_read_outside_a_transaction_block_records_nothing() {
        let sessions = SessionStore::new();
        let session_id = session_id();
        conn_scope::scoped(async {
            note(reads(None));
            record_pending(&sessions, session_id);
            assert!(take().is_empty(), "the pending reads are drained");
        })
        .await;
        assert!(sessions.take_read_set(session_id).is_empty());
    }
}
