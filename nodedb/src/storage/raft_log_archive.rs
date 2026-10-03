// SPDX-License-Identifier: BUSL-1.1

//! The metadata Raft log archive in cold storage.
//!
//! Every metadata timeline owns `{prefix}raft/t{timeline:020}/` (see
//! [`crate::storage::metadata_timeline`]), and every node life on it owns
//! `{node_id}/{incarnation}/` below that. Each object
//! `log-{first:020}-{last:020}.bin` holds the committed entries `first` through
//! `last` of Raft group 0, plus the term of the entry before `first`, so a
//! restore can start a log at any archived index. Objects are encrypted with
//! the WAL key and bound to their own key: an object moved to another name
//! fails to open.

use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::storage::segment::{
    SegmentFooter, decrypt_untrusted_segment_bytes, encrypt_untrusted_segment_bytes,
};
use crate::types::Lsn;

/// Bound into every object's authenticated payload, ahead of its key.
const CONTEXT_MAGIC: &[u8; 4] = b"RLOG";

/// One archived Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArchivedLogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

/// One archive object: consecutive entries of one group.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArchivedLogChunk {
    pub group_id: u64,
    /// Term of the entry before the first one.
    pub prev_term: u64,
    pub entries: Vec<ArchivedLogEntry>,
}

/// Where one archive object sits, and the indexes it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRef {
    pub key: String,
    pub first: u64,
    pub last: u64,
}

/// One node life's place in the archive: its timeline, node id and
/// incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLife<'a> {
    pub timeline: u64,
    pub node_id: u64,
    pub incarnation: &'a str,
}

/// The directory of one metadata timeline's archive, ending in `/`.
pub fn timeline_dir(prefix: &str, timeline: u64) -> String {
    format!("{prefix}raft/t{timeline:020}/")
}

/// The directory of one node life's Raft log archive, ending in `/`.
pub fn raft_archive_prefix(prefix: &str, life: ArchiveLife<'_>) -> String {
    format!(
        "{}{}/{}/",
        timeline_dir(prefix, life.timeline),
        life.node_id,
        life.incarnation
    )
}

/// The key of the object holding entries `first` through `last`.
pub fn chunk_key(prefix: &str, life: ArchiveLife<'_>, first: u64, last: u64) -> String {
    format!(
        "{}log-{first:020}-{last:020}.bin",
        raft_archive_prefix(prefix, life)
    )
}

/// `(first, last)` of an archive object name, `None` for any other name.
pub fn parse_chunk_name(name: &str) -> Option<(u64, u64)> {
    let range = name.strip_prefix("log-")?.strip_suffix(".bin")?;
    let (first, last) = range.split_once('-')?;
    let (first, last) = (first.parse().ok()?, last.parse().ok()?);
    (first <= last).then_some((first, last))
}

fn context(key: &str) -> Vec<u8> {
    let mut context = Vec::with_capacity(CONTEXT_MAGIC.len() + key.len());
    context.extend_from_slice(CONTEXT_MAGIC);
    context.extend_from_slice(key.as_bytes());
    context
}

fn archive_err(detail: String) -> crate::Error {
    crate::Error::ColdStorage { detail }
}

/// Seal `chunk` for `key`.
pub fn seal_chunk(
    chunk: &ArchivedLogChunk,
    key: &str,
    node_name: &str,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let (Some(first), Some(last)) = (chunk.entries.first(), chunk.entries.last()) else {
        return Err(archive_err(format!(
            "raft log archive object {key} holds no entry"
        )));
    };
    let body = zerompk::to_msgpack_vec(chunk)
        .map_err(|e| archive_err(format!("encode raft log archive object {key}: {e}")))?;
    let mut payload = context(key);
    payload.extend_from_slice(&body);
    let footer = SegmentFooter::new(
        node_name,
        crc32c::crc32c(&payload),
        Lsn::new(first.index),
        Lsn::new(last.index),
    );
    encrypt_untrusted_segment_bytes(&payload, &footer, wal_key)
}

/// Open the object stored at `key`, and check it holds exactly the indexes
/// its name gives, in order with no hole.
pub fn open_chunk(
    raw: &[u8],
    key: &str,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<ArchivedLogChunk> {
    let payload = decrypt_untrusted_segment_bytes(raw, wal_key)?;
    let expected = context(key);
    let body = payload
        .strip_prefix(expected.as_slice())
        .ok_or_else(|| archive_err(format!("raft log archive object {key} names another key")))?;
    let chunk: ArchivedLogChunk = zerompk::from_msgpack(body)
        .map_err(|e| archive_err(format!("decode raft log archive object {key}: {e}")))?;
    let name = key.rsplit('/').next().unwrap_or(key);
    let (first, last) = parse_chunk_name(name)
        .ok_or_else(|| archive_err(format!("{key} is not a raft log archive object name")))?;
    let contiguous = chunk
        .entries
        .iter()
        .zip(first..)
        .all(|(entry, index)| entry.index == index);
    let len = u64::try_from(chunk.entries.len()).unwrap_or(u64::MAX);
    if !contiguous || len != last - first + 1 {
        return Err(archive_err(format!(
            "raft log archive object {key} does not hold exactly entries {first}..={last}"
        )));
    }
    Ok(chunk)
}

/// Every archive object of one node life, in index order.
pub async fn list_chunks(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    life: ArchiveLife<'_>,
) -> crate::Result<Vec<ChunkRef>> {
    use futures::TryStreamExt;

    let dir = raft_archive_prefix(prefix, life);
    let location = ObjectPath::from(dir.clone());
    let objects: Vec<_> = store
        .list(Some(&location))
        .try_collect()
        .await
        .map_err(|e| archive_err(format!("list raft log archive under {dir}: {e}")))?;
    let mut chunks: Vec<ChunkRef> = objects
        .into_iter()
        .filter_map(|meta| {
            let (first, last) = parse_chunk_name(meta.location.filename()?)?;
            Some(ChunkRef {
                key: meta.location.to_string(),
                first,
                last,
            })
        })
        .collect();
    chunks.sort_by_key(|chunk| chunk.first);
    Ok(chunks)
}

/// Every archive object of every node life on `timeline`, in index order.
/// Every node of a timeline archives the same metadata log, so any of them
/// serves an index another lacks.
pub async fn list_all_chunks(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    timeline: u64,
) -> crate::Result<Vec<ChunkRef>> {
    use futures::TryStreamExt;

    let dir = timeline_dir(prefix, timeline);
    let objects: Vec<_> = store
        .list(Some(&ObjectPath::from(dir.clone())))
        .try_collect()
        .await
        .map_err(|e| archive_err(format!("list raft log archive under {dir}: {e}")))?;
    let mut chunks: Vec<ChunkRef> = objects
        .into_iter()
        .filter_map(|meta| {
            let (first, last) = parse_chunk_name(meta.location.filename()?)?;
            Some(ChunkRef {
                key: meta.location.to_string(),
                first,
                last,
            })
        })
        .collect();
    chunks.sort_by_key(|chunk| (chunk.first, chunk.last));
    Ok(chunks)
}

/// How far one node life's archive covers the metadata log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ArchiveFrontier {
    /// Highest index the archive holds.
    pub through: u64,
    /// The newest stamp, HLC nanoseconds, of an entry at or below `through`.
    /// Stamps rise with the log index, so every entry stamped at or below it
    /// sits at or below `through`.
    pub stamped_through_ns: u64,
}

/// Bound into a frontier object's authenticated payload, ahead of its key.
const FRONTIER_MAGIC: &[u8; 4] = b"RFRT";

/// Name of a node life's frontier object.
const FRONTIER_NAME: &str = "frontier.bin";

/// The key of one node life's frontier object.
pub fn frontier_key(prefix: &str, life: ArchiveLife<'_>) -> String {
    format!("{}{FRONTIER_NAME}", raft_archive_prefix(prefix, life))
}

/// Encrypt `body` for `key`, bound to `magic` and the key: the object opens
/// only under that key, with that magic. `lsn` fills the footer's range.
pub(crate) fn seal_bound(
    magic: &[u8; 4],
    key: &str,
    body: &[u8],
    node_name: &str,
    lsn: u64,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let mut payload = bound_context(magic, key);
    payload.extend_from_slice(body);
    let footer = SegmentFooter::new(
        node_name,
        crc32c::crc32c(&payload),
        Lsn::new(lsn),
        Lsn::new(lsn),
    );
    encrypt_untrusted_segment_bytes(&payload, &footer, wal_key)
}

/// The body [`seal_bound`] sealed into `raw` for `key` under `magic`.
pub(crate) fn open_bound(
    magic: &[u8; 4],
    key: &str,
    raw: &[u8],
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let payload = decrypt_untrusted_segment_bytes(raw, wal_key)?;
    let expected = bound_context(magic, key);
    payload
        .strip_prefix(expected.as_slice())
        .map(<[u8]>::to_vec)
        .ok_or_else(|| archive_err(format!("raft log archive object {key} names another key")))
}

fn bound_context(magic: &[u8; 4], key: &str) -> Vec<u8> {
    let mut context = Vec::with_capacity(magic.len() + key.len());
    context.extend_from_slice(magic);
    context.extend_from_slice(key.as_bytes());
    context
}

/// Fetch the raw object at `key`, `None` when there is none.
pub(crate) async fn fetch_raw(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> crate::Result<Option<Vec<u8>>> {
    let fetch_err = |e: object_store::Error| archive_err(format!("fetch {key}: {e}"));
    match store.get(&ObjectPath::from(key)).await {
        Ok(result) => Ok(Some(result.bytes().await.map_err(fetch_err)?.to_vec())),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(fetch_err(e)),
    }
}

/// Seal and store `frontier` under `key`, replacing the one there.
pub async fn put_frontier(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    frontier: ArchiveFrontier,
    node_name: &str,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<()> {
    let body = zerompk::to_msgpack_vec(&frontier)
        .map_err(|e| archive_err(format!("encode raft log archive frontier {key}: {e}")))?;
    let sealed = seal_bound(
        FRONTIER_MAGIC,
        key,
        &body,
        node_name,
        frontier.through,
        wal_key,
    )?;
    put_chunk(store, key, sealed).await
}

/// The frontier of every node life on `timeline`. Every node of a timeline
/// archives the same metadata log, so the newest frontier of any life bounds
/// what the archive holds.
pub async fn fetch_all_frontiers(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    timeline: u64,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<ArchiveFrontier>> {
    use futures::TryStreamExt;

    let dir = timeline_dir(prefix, timeline);
    let objects: Vec<_> = store
        .list(Some(&ObjectPath::from(dir.clone())))
        .try_collect()
        .await
        .map_err(|e| archive_err(format!("list raft log archive under {dir}: {e}")))?;
    let mut frontiers = Vec::new();
    for meta in objects {
        if meta.location.filename() != Some(FRONTIER_NAME) {
            continue;
        }
        let key = meta.location.to_string();
        let Some(raw) = fetch_raw(store, &key).await? else {
            continue;
        };
        let body = open_bound(FRONTIER_MAGIC, &key, &raw, wal_key)?;
        frontiers
            .push(zerompk::from_msgpack(&body).map_err(|e| {
                archive_err(format!("decode raft log archive frontier {key}: {e}"))
            })?);
    }
    Ok(frontiers)
}

/// Store `chunk` under `key`.
pub async fn put_chunk(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    sealed: Vec<u8>,
) -> crate::Result<()> {
    store
        .put(&ObjectPath::from(key), PutPayload::from(sealed))
        .await
        .map_err(|e| archive_err(format!("upload raft log archive object {key}: {e}")))?;
    Ok(())
}

/// Fetch and open the object at `key`.
pub async fn fetch_chunk(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    wal_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<ArchivedLogChunk> {
    let fetch_err = |e: object_store::Error| archive_err(format!("fetch {key}: {e}"));
    let raw = store
        .get(&ObjectPath::from(key))
        .await
        .map_err(fetch_err)?
        .bytes()
        .await
        .map_err(fetch_err)?;
    open_chunk(&raw, key, wal_key)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn wal_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x11; 32]).unwrap()
    }

    fn chunk(first: u64, last: u64) -> ArchivedLogChunk {
        ArchivedLogChunk {
            group_id: 0,
            prev_term: 1,
            entries: (first..=last)
                .map(|index| ArchivedLogEntry {
                    index,
                    term: 2,
                    data: index.to_le_bytes().to_vec(),
                })
                .collect(),
        }
    }

    fn life(timeline: u64, node_id: u64) -> ArchiveLife<'static> {
        ArchiveLife {
            timeline,
            node_id,
            incarnation: "inc",
        }
    }

    #[test]
    fn names_round_trip() {
        let key = chunk_key("data/", life(7, 3), 5, 9);
        assert_eq!(
            key,
            "data/raft/t00000000000000000007/3/inc/\
             log-00000000000000000005-00000000000000000009.bin"
        );
        assert_eq!(
            parse_chunk_name(key.rsplit('/').next().unwrap()),
            Some((5, 9))
        );
        assert_eq!(parse_chunk_name("log-9-5.bin"), None);
        assert_eq!(parse_chunk_name("wal-1.seg"), None);
    }

    #[tokio::test]
    async fn a_chunk_round_trips_and_refuses_another_key() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = chunk_key("data/", life(0, 3), 5, 9);
        let sealed = seal_chunk(&chunk(5, 9), &key, "node-3", &wal_key()).unwrap();
        put_chunk(&store, &key, sealed.clone()).await.unwrap();
        assert_eq!(
            fetch_chunk(&store, &key, &wal_key()).await.unwrap(),
            chunk(5, 9)
        );

        let moved = chunk_key("data/", life(0, 3), 10, 14);
        put_chunk(&store, &moved, sealed).await.unwrap();
        assert!(fetch_chunk(&store, &moved, &wal_key()).await.is_err());

        let listed = list_chunks(&store, "data/", life(0, 3)).await.unwrap();
        let ranges: Vec<(u64, u64)> = listed.iter().map(|c| (c.first, c.last)).collect();
        assert_eq!(ranges, [(5, 9), (10, 14)]);

        let other = chunk_key("data/", life(0, 4), 1, 4);
        put_chunk(
            &store,
            &other,
            seal_chunk(&chunk(1, 4), &other, "node-4", &wal_key()).unwrap(),
        )
        .await
        .unwrap();
        // Another timeline's objects never list with this one's.
        let branch = chunk_key("data/", life(9, 3), 1, 4);
        put_chunk(
            &store,
            &branch,
            seal_chunk(&chunk(1, 4), &branch, "node-3", &wal_key()).unwrap(),
        )
        .await
        .unwrap();
        let all = list_all_chunks(&store, "data/", 0).await.unwrap();
        let ranges: Vec<(u64, u64)> = all.iter().map(|c| (c.first, c.last)).collect();
        assert_eq!(ranges, [(1, 4), (5, 9), (10, 14)]);
    }

    #[tokio::test]
    async fn frontiers_round_trip_beside_chunks_and_refuse_another_key() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = chunk_key("data/", life(0, 3), 1, 4);
        put_chunk(
            &store,
            &key,
            seal_chunk(&chunk(1, 4), &key, "node-3", &wal_key()).unwrap(),
        )
        .await
        .unwrap();
        let frontier = ArchiveFrontier {
            through: 4,
            stamped_through_ns: 77,
        };
        let key = frontier_key("data/", life(0, 3));
        put_frontier(&store, &key, frontier, "node-3", &wal_key())
            .await
            .unwrap();
        assert_eq!(
            fetch_all_frontiers(&store, "data/", 0, &wal_key())
                .await
                .unwrap(),
            [frontier]
        );
        assert!(
            fetch_all_frontiers(&store, "data/", 5, &wal_key())
                .await
                .unwrap()
                .is_empty(),
            "another timeline has its own frontiers"
        );
        let chunks = list_chunks(&store, "data/", life(0, 3)).await.unwrap();
        assert_eq!(chunks.len(), 1, "a frontier is not a chunk");

        let raw = store
            .get(&ObjectPath::from(key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        put_chunk(&store, &frontier_key("data/", life(0, 4)), raw.to_vec())
            .await
            .unwrap();
        assert!(
            fetch_all_frontiers(&store, "data/", 0, &wal_key())
                .await
                .is_err(),
            "a frontier moved to another life fails to open"
        );
    }
}
