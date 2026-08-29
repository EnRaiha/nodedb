// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "failpoints")]
mod crash_injection;
mod crash_recovery;
#[cfg(feature = "failpoints")]
mod durability_fsync;
mod durability_stress;
mod dwb_protection;
mod encrypted_replay;
#[cfg(feature = "diagnostics")]
mod faultbox_corruption_report;
#[cfg(not(feature = "diagnostics"))]
mod faultbox_disabled;
#[cfg(not(target_arch = "wasm32"))]
mod mmap_reader_madvise;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
mod o_direct_alignment;
#[cfg(target_os = "linux")]
mod o_direct_replay;
mod reader_ciphertext_invariant;
mod reader_preamble_and_continuity;
mod segment_lsn_integrity;
mod wal_collection_tombstone;
mod wal_encryption;
