// SPDX-License-Identifier: Apache-2.0

//! The non-recording implementation of the WAL's report sites.
//!
//! Compiled when the `diagnostics` feature is off, and unconditionally on
//! wasm32 where there is no filesystem to write a report to. Every entry point
//! keeps the signature of its recording counterpart so call sites are free of
//! `cfg`, and every one is empty so the WAL behaves byte-for-byte as it did
//! before the recorder existed.

use std::path::Path;

use crate::error::WalError;

#[inline]
pub fn mid_file_corruption(
    _err: &WalError,
    _path: &Path,
    _offset: u64,
    _resync_offset: u64,
    _resync_lsn: u64,
    _last_lsn: u64,
) {
}

#[inline]
pub fn segment_lsn_gap(
    _err: &WalError,
    _path: &Path,
    _previous_path: &Path,
    _previous_last_lsn: u64,
    _expected_lsn: u64,
    _found_lsn: u64,
) {
}

#[inline]
pub fn replay_below_retained_floor(
    _err: &WalError,
    _path: &Path,
    _from_lsn: u64,
    _retained_floor_lsn: u64,
) {
}

#[inline]
pub fn durability_lost(_err: &WalError, _detail: &str) {}

#[inline]
pub fn encrypted_record_without_key(_err: &WalError, _lsn: u64, _site: &'static str) {}

#[inline]
pub fn out_of_space(_err: &WalError, _site: &'static str, _file_offset: u64, _pending_bytes: u64) {}
