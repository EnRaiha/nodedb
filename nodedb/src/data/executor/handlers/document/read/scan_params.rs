// SPDX-License-Identifier: BUSL-1.1

//! Shared parameter bundle for the versioned (bitemporal / audit-log) document
//! scan handlers. Groups the fields common to `execute_document_scan_as_of`
//! and `execute_document_scan_all_versions` so neither needs a long positional
//! argument list.

/// Borrowed inputs for a versioned document scan.
///
/// The system-time cutoff is intentionally *not* part of this bundle: the
/// all-versions (audit-log) scan has no cutoff, while the `AS OF` scan carries
/// one as a separate argument. Everything else is shared.
pub(in crate::data::executor) struct VersionedScanParams<'a> {
    pub collection: &'a str,
    pub limit: usize,
    pub offset: usize,
    pub filters: &'a [u8],
    pub projection: &'a [String],
    pub valid_at_ms: Option<i64>,
}
