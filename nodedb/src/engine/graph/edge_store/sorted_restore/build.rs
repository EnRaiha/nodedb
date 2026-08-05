// SPDX-License-Identifier: BUSL-1.1

use std::fs::{self, File};
use std::io;
use std::path::Path;

use redb::{Database, SortedTableBuilder, SortedTableOptions};

use super::types::{SortedEdgeRecord, SortedNodeRecord, SortedRestoreOptions, SortedStatsRecord};
use crate::engine::graph::edge_store::stats::GRAPH_STATS;
use crate::engine::graph::edge_store::store::{
    EDGES, EdgeStore, NODE_SURROGATES, REVERSE_EDGES, redb_err,
};

impl EdgeStore {
    /// Atomically restore an entire edge store from globally sorted durable rows.
    ///
    /// Each input must be ordered by redb's typed `(database, tenant, key)`
    /// comparator. The source iterators are consumed once, and an iterator
    /// failure aborts the scratch transaction without replacing `path`.
    pub fn restore_sorted_at_path<N, F, R, S>(
        path: &Path,
        nodes: N,
        forward_edges: F,
        reverse_edges: R,
        stats: S,
        options: SortedRestoreOptions,
    ) -> crate::Result<Self>
    where
        N: IntoIterator<Item = crate::Result<SortedNodeRecord>>,
        F: IntoIterator<Item = crate::Result<SortedEdgeRecord>>,
        R: IntoIterator<Item = crate::Result<SortedEdgeRecord>>,
        S: IntoIterator<Item = crate::Result<SortedStatsRecord>>,
    {
        let parent = path.parent().ok_or_else(|| crate::Error::BadRequest {
            detail: "sorted restore path has no parent directory".into(),
        })?;
        fs::create_dir_all(parent)?;
        let scratch = tempfile::Builder::new()
            .prefix("nodedb-sorted-restore-")
            .tempfile_in(parent)?
            .into_temp_path();
        let mut database_builder = Database::builder();
        if let Some(cache_size) = options.cache_size {
            database_builder.set_cache_size(cache_size);
        }
        let database = database_builder
            .create(&scratch)
            .map_err(|error| redb_err("create sorted restore scratch", error))?;
        let packing = SortedTableOptions::default().with_target_page_size(options.target_page_size);
        let mut source_error = None;
        let transaction = database
            .begin_write()
            .map_err(|error| redb_err("begin sorted restore", error))?
            .build_sorted_table_with_options(NODE_SURROGATES, packing, |builder| {
                feed_nodes(builder, nodes.into_iter(), &mut source_error)
            })
            .and_then(|transaction| {
                transaction.build_sorted_table_with_options(EDGES, packing, |builder| {
                    feed_edges(builder, forward_edges.into_iter(), &mut source_error)
                })
            })
            .and_then(|transaction| {
                transaction.build_sorted_table_with_options(REVERSE_EDGES, packing, |builder| {
                    feed_edges(builder, reverse_edges.into_iter(), &mut source_error)
                })
            })
            .and_then(|transaction| {
                transaction.build_sorted_table_with_options(GRAPH_STATS, packing, |builder| {
                    feed_stats(builder, stats.into_iter(), &mut source_error)
                })
            });
        if let Some(error) = source_error {
            return Err(error);
        }
        let transaction = transaction.map_err(|error| redb_err("build sorted restore", error))?;
        transaction
            .commit()
            .map_err(|error| redb_err("commit sorted restore", error))?;
        drop(database);
        scratch.persist(path).map_err(|error| error.error)?;
        File::open(parent)?.sync_all()?;
        match options.cache_size {
            Some(cache_size) => Self::open_with_cache_size(path, cache_size),
            None => Self::open(path),
        }
    }
}

fn feed_nodes<I>(
    builder: &mut SortedTableBuilder<(u64, u64, &str), u32>,
    records: I,
    source_error: &mut Option<crate::Error>,
) -> Result<(), redb::Error>
where
    I: Iterator<Item = crate::Result<SortedNodeRecord>>,
{
    for record in records {
        let record = source_record(record, source_error)?;
        builder.insert(
            (
                record.database.as_u64(),
                record.tenant.as_u64(),
                record.node.as_str(),
            ),
            record.surrogate,
        )?;
    }
    Ok(())
}

fn feed_edges<I>(
    builder: &mut SortedTableBuilder<(u64, u64, &str), &[u8]>,
    records: I,
    source_error: &mut Option<crate::Error>,
) -> Result<(), redb::Error>
where
    I: Iterator<Item = crate::Result<SortedEdgeRecord>>,
{
    for record in records {
        let record = source_record(record, source_error)?;
        builder.insert(
            (
                record.database.as_u64(),
                record.tenant.as_u64(),
                record.key.as_str(),
            ),
            record.value.as_slice(),
        )?;
    }
    Ok(())
}

fn feed_stats<I>(
    builder: &mut SortedTableBuilder<(u64, u64, &str), &[u8]>,
    records: I,
    source_error: &mut Option<crate::Error>,
) -> Result<(), redb::Error>
where
    I: Iterator<Item = crate::Result<SortedStatsRecord>>,
{
    for record in records {
        let record = source_record(record, source_error)?;
        builder.insert(
            (
                record.database.as_u64(),
                record.tenant.as_u64(),
                record.key.as_str(),
            ),
            record.value.as_slice(),
        )?;
    }
    Ok(())
}

fn source_record<T>(
    record: crate::Result<T>,
    source_error: &mut Option<crate::Error>,
) -> Result<T, redb::Error> {
    match record {
        Ok(record) => Ok(record),
        Err(error) => {
            *source_error = Some(error);
            Err(redb::Error::Io(io::Error::other(
                "sorted restore source failed",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use redb::{Database, ReadableDatabase};

    use super::*;
    use crate::engine::graph::edge_store::{Direction, EdgeValuePayload};
    use nodedb_types::{DatabaseId, TenantId};

    fn edge(key: &str, value: Vec<u8>) -> SortedEdgeRecord {
        SortedEdgeRecord {
            database: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            key: key.into(),
            value,
        }
    }

    #[test]
    fn sorted_restore_publishes_all_tables_and_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.redb");
        let value = EdgeValuePayload::new(0, i64::MAX, b"{}".to_vec())
            .encode()
            .unwrap();
        let store = EdgeStore::restore_sorted_at_path(
            &path,
            [Ok(SortedNodeRecord {
                database: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
                node: "a".into(),
                surrogate: 7,
            })],
            [Ok(edge(
                concat!("g\0a\0edge\0b\0", "00000000000000000001"),
                value,
            ))],
            [Ok(edge(
                concat!("g\0b\0edge\0a\0", "00000000000000000001"),
                Vec::new(),
            ))],
            [Ok(SortedStatsRecord {
                database: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
                key: "g\0summary".into(),
                value: b"stats".to_vec(),
            })],
            SortedRestoreOptions::default(),
        )
        .unwrap();
        assert_eq!(
            store
                .neighbors(0, TenantId::new(1), "g", "a", None, Direction::Both)
                .unwrap()
                .len(),
            1
        );
        drop(store);
        let reopened = EdgeStore::open(&path).unwrap();
        assert!(
            reopened
                .get_edge(0, TenantId::new(1), "g", "a", "edge", "b")
                .unwrap()
                .is_some()
        );
        drop(reopened);
        let database = Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        let nodes = read.open_table(NODE_SURROGATES).unwrap();
        assert_eq!(nodes.get((0, 1, "a")).unwrap().unwrap().value(), 7);
        let reverse = read.open_table(REVERSE_EDGES).unwrap();
        assert_eq!(
            reverse
                .get((0, 1, concat!("g\0b\0edge\0a\0", "00000000000000000001"),))
                .unwrap()
                .unwrap()
                .value(),
            b""
        );
        let stats = read.open_table(GRAPH_STATS).unwrap();
        assert_eq!(
            stats.get((0, 1, "g\0summary")).unwrap().unwrap().value(),
            b"stats"
        );
    }

    #[test]
    fn ordered_and_source_failures_leave_the_old_target_intact() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.redb");
        let original = EdgeStore::open(&path).unwrap();
        drop(original);
        let before = fs::read(&path).unwrap();
        let reversed = [
            Ok(SortedNodeRecord {
                database: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
                node: "z".into(),
                surrogate: 1,
            }),
            Ok(SortedNodeRecord {
                database: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
                node: "a".into(),
                surrogate: 2,
            }),
        ];
        assert!(
            EdgeStore::restore_sorted_at_path(
                &path,
                reversed,
                std::iter::empty(),
                std::iter::empty(),
                std::iter::empty(),
                SortedRestoreOptions::default(),
            )
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        let source = [Err(crate::Error::BadRequest {
            detail: "source failure".into(),
        })];
        let error = match EdgeStore::restore_sorted_at_path(
            &path,
            source,
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            SortedRestoreOptions::default(),
        ) {
            Err(error) => error,
            Ok(_) => panic!("source failure must abort sorted restore"),
        };
        assert!(matches!(error, crate::Error::BadRequest { detail } if detail == "source failure"));
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}
