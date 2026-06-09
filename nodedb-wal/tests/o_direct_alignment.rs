// SPDX-License-Identifier: BUSL-1.1

//! O_DIRECT alignment invariants for the WAL write path.
//!
//! The WAL contract: when a writer is configured with `use_direct_io = true`,
//! every byte the kernel sees on a write SQE/syscall must satisfy the O_DIRECT
//! constraints — buffer aligned, length a multiple of the logical block size,
//! and the file offset of the next write a multiple of the same. The writer's
//! internal `file_offset` MUST advance by the padded I/O size, not the
//! unpadded record length, otherwise subsequent submissions land on an
//! unaligned offset and the kernel returns `-EINVAL`.

#![cfg(all(feature = "io-uring", target_os = "linux"))]

use std::path::PathBuf;

use nodedb_wal::align::{DEFAULT_ALIGNMENT, is_aligned};
use nodedb_wal::double_write::{DoubleWriteBuffer, DwbMode};
use nodedb_wal::record::{HEADER_SIZE, RecordType, WalRecord};
use nodedb_wal::uring_writer::{UringWriter, UringWriterConfig};

fn target_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
}

fn direct_io_config() -> UringWriterConfig {
    UringWriterConfig {
        use_direct_io: true,
        ..Default::default()
    }
}

#[test]
fn uring_file_offset_advances_by_padded_size_after_sub_page_batch() {
    let path = target_dir().join("uring_offset_padded.wal");
    let _ = std::fs::remove_file(&path);

    let mut writer = match UringWriter::open(&path, direct_io_config()) {
        Ok(w) => w,
        Err(_) => return, // O_DIRECT unsupported on this filesystem; skip.
    };

    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"small")
        .unwrap();
    writer.submit_and_sync().unwrap();

    // A single sub-page batch must advance file_offset by the padded length
    // (one alignment block), not the unpadded HEADER_SIZE + payload length.
    let unpadded = (HEADER_SIZE + 5) as u64;
    let padded = DEFAULT_ALIGNMENT as u64;
    assert_ne!(
        writer.file_offset(),
        unpadded,
        "file_offset advanced by unpadded length — next O_DIRECT write will be at an unaligned offset"
    );
    assert_eq!(writer.file_offset(), padded);
    assert!(
        is_aligned(writer.file_offset() as usize, DEFAULT_ALIGNMENT),
        "file_offset {} is not aligned to {}",
        writer.file_offset(),
        DEFAULT_ALIGNMENT
    );
}

#[test]
fn uring_two_sub_page_batches_both_succeed_under_o_direct() {
    let path = target_dir().join("uring_two_batches.wal");
    let _ = std::fs::remove_file(&path);

    let mut writer = match UringWriter::open(&path, direct_io_config()) {
        Ok(w) => w,
        Err(_) => return,
    };

    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"first-batch")
        .unwrap();
    writer.submit_and_sync().unwrap();

    // Second batch: with the offset bug, this submission lands on an
    // unaligned offset and the io_uring CQE returns -EINVAL.
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"second-batch")
        .expect("append must not fail");
    writer
        .submit_and_sync()
        .expect("second submit_and_sync must succeed under O_DIRECT");

    assert_eq!(writer.file_offset(), 2 * DEFAULT_ALIGNMENT as u64);
}

#[test]
fn uring_many_sub_page_batches_remain_aligned() {
    let path = target_dir().join("uring_many_batches.wal");
    let _ = std::fs::remove_file(&path);

    let mut writer = match UringWriter::open(&path, direct_io_config()) {
        Ok(w) => w,
        Err(_) => return,
    };

    for i in 0..16u32 {
        writer
            .append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("rec-{i}").as_bytes(),
            )
            .unwrap();
        writer
            .submit_and_sync()
            .unwrap_or_else(|e| panic!("batch {i} failed: {e}"));
        assert!(
            is_aligned(writer.file_offset() as usize, DEFAULT_ALIGNMENT),
            "after batch {i}, file_offset {} is not block-aligned",
            writer.file_offset()
        );
    }

    assert_eq!(writer.file_offset(), 16 * DEFAULT_ALIGNMENT as u64);
}

#[test]
fn dwb_direct_mode_write_and_recover() {
    let path = target_dir().join("dwb_direct.dwb");
    let _ = std::fs::remove_file(&path);

    let mut dwb = match DoubleWriteBuffer::open(&path, DwbMode::Direct) {
        Ok(d) => d,
        Err(_) => return,
    };

    for lsn in 1..=3u64 {
        let rec = WalRecord::new(
            RecordType::Put as u32,
            lsn,
            1,
            0,
            0,
            format!("direct-{lsn}").into_bytes(),
            None,
            None,
        )
        .unwrap();
        dwb.write_record_deferred(&rec).unwrap();
    }
    dwb.flush().unwrap();

    for lsn in 1..=3u64 {
        let rec = dwb.recover_record(lsn).unwrap().expect("recoverable");
        assert_eq!(rec.payload, format!("direct-{lsn}").into_bytes());
    }
}
