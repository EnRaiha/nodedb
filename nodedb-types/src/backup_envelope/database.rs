// SPDX-License-Identifier: Apache-2.0

//! A database backup: one encrypted envelope whose sections are whole tenant
//! envelopes.
//!
//! The outer envelope's `meta.tenant_id` is [`DATABASE_BACKUP_TENANT`]. Its
//! first section, [`SECTION_ORIGIN_DATABASE_MANIFEST`], names the database and
//! lists its tenants. Every other section's `origin_node_id` is a tenant id,
//! and its body is that tenant's envelope, covering this database only. The
//! outer authentication tag covers the whole set, so a dropped tenant fails
//! the parse.

/// `meta.tenant_id` of a database backup's outer envelope.
pub const DATABASE_BACKUP_TENANT: u64 = u64::MAX;

/// Section carrying a database backup's [`DatabaseBackupManifest`].
pub const SECTION_ORIGIN_DATABASE_MANIFEST: u64 = 0xFFFF_FFFF_FFFF_FFE0;

/// What a database backup holds.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DatabaseBackupManifest {
    /// Name of the backed-up database. A restore targets the same name.
    pub database: String,
    /// HLC wall time, in nanoseconds, of the one consistent cut every tenant
    /// was captured at. Every row in the backup committed at or below it.
    pub cut_hlc: u64,
    /// Every tenant with a section, in section order.
    pub tenants: Vec<u64>,
}
