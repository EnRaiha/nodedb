// SPDX-License-Identifier: Apache-2.0

//! WAL throughput benchmarks.
//!
//! Risk 4: Validate 100K writes/sec WAL target.
//!
//! Run with: cargo bench -p nodedb-wal --bench wal_throughput

use fluxbench::prelude::*;
use fluxbench::{bench, synthetic, verify};
use std::hint::black_box;
use std::sync::{Arc, Mutex};

use nodedb_wal::group_commit::{GroupCommitter, PendingWrite};
use nodedb_wal::record::RecordType;
use nodedb_wal::writer::WalWriter;

/// Single-threaded WAL append + fsync — 1000 x 128-byte records per iteration.
#[bench(id = "wal_append_fsync_1k_128b", group = "wal_write", tags = "core")]
fn wal_append_fsync_1k(b: &mut Bencher) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench.wal");
    let payload = vec![0xCDu8; 128];

    b.iter(|| {
        let _ = std::fs::remove_file(&path);
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        for _ in 0..1000 {
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &payload)
                .unwrap();
        }
        writer.sync().unwrap();
        black_box(writer.next_lsn())
    });
}

/// WAL append-only (no fsync) — measures raw serialization + buffer speed.
#[bench(id = "wal_append_only_10k_128b", group = "wal_write")]
fn wal_append_only_10k(b: &mut Bencher) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_nofs.wal");
    let payload = vec![0xCDu8; 128];

    b.iter(|| {
        let _ = std::fs::remove_file(&path);
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        for _ in 0..10_000 {
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &payload)
                .unwrap();
        }
        black_box(writer.next_lsn())
    });
}

/// Group commit — 10 threads each submitting 1000 records through GroupCommitter.
#[bench(
    id = "wal_group_commit_10t_1k",
    group = "wal_group_commit",
    tags = "core"
)]
fn wal_group_commit(b: &mut Bencher) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_gc.wal");
    let payload = vec![0xCDu8; 128];

    b.iter(|| {
        let _ = std::fs::remove_file(&path);
        let writer = Arc::new(Mutex::new(
            WalWriter::open_without_direct_io(&path).unwrap(),
        ));
        let committer = Arc::new(GroupCommitter::new());

        let handles: Vec<_> = (0..10)
            .map(|t| {
                let c = Arc::clone(&committer);
                let w = Arc::clone(&writer);
                let p = payload.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        c.submit(
                            &w,
                            PendingWrite {
                                record_type: RecordType::Put as u32,
                                tenant_id: t,
                                vshard_id: 0,
                                database_id: 0,
                                payload: p.clone(),
                            },
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let w = writer.lock().unwrap();
        black_box(w.next_lsn())
    });
}

// 1000 records per iteration of wal_append_fsync_1k_128b.
#[synthetic(
    id = "wal_writes_per_sec",
    formula = "1000 / wal_append_fsync_1k_128b * 1000000000",
    unit = "writes/s"
)]
#[allow(dead_code)]
struct WalWritesPerSec;

// 10 threads * 1000 records = 10K records per group commit iteration.
#[synthetic(
    id = "wal_group_commit_writes_per_sec",
    formula = "10000 / wal_group_commit_10t_1k * 1000000000",
    unit = "writes/s"
)]
#[allow(dead_code)]
struct WalGroupCommitWritesPerSec;

// Target: 100K writes/sec with group commit.
#[verify(
    expr = "wal_group_commit_writes_per_sec > 100000",
    severity = "critical"
)]
#[allow(dead_code)]
struct WalGroupCommitTarget;

fn main() {
    if let Err(e) = fluxbench::run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
