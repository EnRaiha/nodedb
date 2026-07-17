// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memtable checkpoint flush + boot-side partition-registry load.
//!
//! ## What is at stake
//!
//! `columnar_memtables` holds one `ColumnarMemtable` per timeseries collection,
//! and every ILP / JSON / msgpack ingest lands there. Those rows advance the
//! core watermark (`execute_timeseries_ingest` calls `note_collection_write_lsn`
//! with the Control Plane's `wal_lsn`), so the periodic checkpoint reported them
//! as durable and the manager truncated the `TimeseriesBatch` records that were
//! their only copy. The memtable reached a segment only when the ingest path
//! crossed its 64 MiB threshold or when the idle timer in
//! `handlers/compact/maintenance.rs` happened to fire — neither of which is
//! ordered against the truncation the checkpoint authorises. A restart on a
//! collection ingesting below the threshold returned it missing every row since
//! the last idle flush.
//!
//! ## Why this reuses `flush_ts_collection` rather than writing a checkpoint blob
//!
//! Like the array engine, and unlike KV / columnar / the sync gate, timeseries
//! already has a real durable form and a boot path that reads it:
//!
//! * `flush_ts_collection` encodes the memtable into a partition directory via
//!   `ColumnarSegmentWriter` — per-column codecs, symbol dictionaries, a sparse
//!   block index, and a `partition.meta` carrying the block statistics the scan
//!   prunes on — and registers it in `ts_registries`.
//! * [`CoreLoop::load_ts_registries`] rebuilds `ts_registries` from those
//!   directories at boot, and `handlers/timeseries/raw_scan` reads registered
//!   partitions and the live memtable together, so a restored partition answers
//!   a scan exactly as the memtable did before the flush.
//!
//! A checkpoint blob would persist a second, redundant copy of state the engine
//! already knows how to write and read back — and a worse one: it would bypass
//! the column codecs, the sparse index, and the merge path that later compacts
//! those partitions. The bug was never a missing format; it was that the only
//! flush was a threshold and a timer.
//!
//! ## What LSN is durable after a flush
//!
//! The core watermark. A timeseries row reaches the memtable before its ingest
//! returns, and only then does `note_collection_write_lsn` raise the watermark,
//! so on this core's own thread — where the checkpoint runs, between tasks —
//! every row with `lsn <= watermark` is in a memtable. Flushing every non-empty
//! memtable therefore puts all of them in a partition.
//!
//! The `last_flushed_wal_lsn` each partition records is a different and narrower
//! number: the highest `TimeseriesBatch` LSN folded into it
//! (`ts_max_ingested_lsn`), which is what replay's dedup gate compares against.
//! It is not the checkpoint's answer and must not be — the watermark is core-wide
//! across every engine, while that stamp is per-collection and counts only
//! timeseries records.
//!
//! A collection whose memtable is empty flushes nothing and leaves its last
//! partition's stamp where it stands. That is not a gap: an empty memtable means
//! every row ever ingested into it is already in a partition, so the watermark is
//! durable for that collection either way.
//!
//! ## Why no `ReplayFloors` field
//!
//! Timeseries replay is NOT idempotent — an ingest is an APPEND, and
//! `raw_scan` reads partitions and memtable together, so re-applying a record
//! already folded into a partition shows every one of its rows twice. It needs a
//! floor, and it already HAS one, older and finer than `ReplayFloors`:
//! `replay_timeseries_wal` skips any record at or below the highest
//! `last_flushed_wal_lsn` across the collection's registered partitions.
//!
//! That gate is per-collection, which is what timeseries needs and what a
//! `ReplayFloors` field could not give it — those are engine-wide, because a KV
//! or columnar record can span two collections. A `TimeseriesBatch` names
//! exactly one. Adding a second, coarser floor beside the existing one would
//! gate records the partitions do not contain.
//!
//! What that gate DID lack is a boot-time load: `ts_registries` was populated
//! only lazily, by the first scan of a collection (`ensure_ts_registry`), which
//! runs long after `replay_all_wal`. So at replay the registry was empty, the
//! gate found no partitions, and every retained record replayed on top of the
//! partitions that already held it. [`CoreLoop::load_ts_registries`] closes that
//! by populating the registries before replay — which is exactly the role
//! `load_kv_checkpoints` plays for `ReplayFloors::kv`.

mod flush;
mod load;
