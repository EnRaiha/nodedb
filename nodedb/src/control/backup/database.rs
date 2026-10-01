// SPDX-License-Identifier: BUSL-1.1

//! `BACKUP DATABASE` and `RESTORE DATABASE`: every tenant of one database.
//!
//! The backup captures every tenant with a collection or an array in the
//! database at one consistent cut (see [`super::cut_capture`]): no row
//! written after the cut is in the backup. It frames one tenant envelope per
//! tenant, scoped to that database, and packs them into one encrypted
//! database envelope (see `nodedb_types::backup_envelope::database`) whose
//! manifest records the cut. The restore checks every tenant envelope as a
//! DRY RUN first ([`check_database`]), so a refused tenant stops the restore
//! before any write, then restores each tenant through the tenant restore,
//! verification included ([`apply_database`]).

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_types::backup_envelope::{
    DATABASE_BACKUP_TENANT, DEFAULT_MAX_TOTAL_BYTES, DatabaseBackupManifest, DatabaseDataSection,
    EnvelopeMeta, EnvelopeWriter, SECTION_ORIGIN_DATABASE_MANIFEST, parse_encrypted,
};

use crate::Error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot};

use super::RestoreStats;
use super::cut_capture::collect::capture_database;

fn backup_kek(state: &SharedState) -> Result<&[u8; 32], Error> {
    state.backup_kek.as_deref().ok_or_else(|| Error::Internal {
        detail: "database backup: no [backup_encryption] KEK configured; set \
                 [backup_encryption] in the server config"
            .into(),
    })
}

fn envelope_error(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Internal {
        detail: format!("database backup envelope ({what}): {e}"),
    }
}

/// Every tenant with a collection or an array in `database_id`: the tenants
/// a backup of it captures.
pub fn database_tenants(
    state: &SharedState,
    database_id: DatabaseId,
) -> Result<BTreeSet<u64>, Error> {
    let catalog = state.credentials.catalog();
    let mut tenants: BTreeSet<u64> = catalog
        .load_all_collections(database_id)?
        .iter()
        .map(|coll| coll.tenant_id)
        .collect();
    tenants.extend(
        catalog
            .load_all_arrays()?
            .iter()
            .filter(|array| array.array_id.database_id == database_id)
            .map(|array| array.array_id.tenant_id.as_u64()),
    );
    Ok(tenants)
}

/// The rows of `tenants`, from [`database_tenants`], in `database_id`, named
/// `name`, as one encrypted database envelope.
pub async fn backup_database(
    state: &Arc<SharedState>,
    database_id: DatabaseId,
    name: &str,
    tenants: &BTreeSet<u64>,
) -> Result<Vec<u8>, Error> {
    let kek = *backup_kek(state)?;
    let mut captured = capture_database(state, database_id, tenants).await?;

    let manifest = DatabaseBackupManifest {
        database: name.to_string(),
        cut_hlc: captured.cut,
        tenants: tenants.iter().copied().collect(),
    };
    let mut writer = EnvelopeWriter::new(EnvelopeMeta {
        tenant_id: DATABASE_BACKUP_TENANT,
        source_vshard_count: VSHARD_COUNT as u16,
        hash_seed: 0,
        snapshot_watermark: captured.cut,
    });
    let body = zerompk::to_msgpack_vec(&manifest).map_err(|e| envelope_error("manifest", e))?;
    writer
        .push_section(SECTION_ORIGIN_DATABASE_MANIFEST, body)
        .map_err(|e| envelope_error("manifest", e))?;
    for &tenant_id in tenants {
        let snapshot = captured.tenants.remove(&tenant_id).unwrap_or_default();
        let envelope =
            tenant_envelope(state, tenant_id, database_id, captured.cut, snapshot).await?;
        writer
            .push_section(tenant_id, envelope.to_vec())
            .map_err(|e| envelope_error("tenant section", e))?;
    }
    writer
        .finalize_encrypted(&kek)
        .map_err(|e| envelope_error("encryption", e))
}

/// The envelope of `tenant_id` scoped to `database_id`, holding `snapshot`,
/// its rows captured at the cut `cut`.
async fn tenant_envelope(
    state: &Arc<SharedState>,
    tenant_id: u64,
    database_id: DatabaseId,
    cut: u64,
    snapshot: TenantDataSnapshot,
) -> Result<bytes::Bytes, Error> {
    let mut databases = super::metadata::tenant_databases(state, tenant_id)?;
    databases.retain(|database| database.id() == database_id);
    let section = DatabaseDataSection {
        database_id: database_id.as_u64(),
        snapshot: super::metadata::encode_section_part("tenant capture", &snapshot)?,
    };
    let body = super::metadata::encode_section_part("data section", &section)?;
    super::orchestrator::assemble_envelope(
        state,
        tenant_id,
        cut,
        &databases,
        vec![(state.node_id, body)],
    )
    .await
}

/// A decrypted database backup whose manifest names the database to restore.
pub struct OpenedBackup {
    manifest: DatabaseBackupManifest,
    /// `(tenant_id, tenant envelope)` in manifest order.
    tenants: Vec<(u64, Vec<u8>)>,
}

impl OpenedBackup {
    /// Every tenant the backup restores, in manifest order.
    pub fn tenant_ids(&self) -> &[u64] {
        &self.manifest.tenants
    }

    /// HLC wall time, in nanoseconds, of the backup's consistent cut.
    pub fn cut_hlc(&self) -> u64 {
        self.manifest.cut_hlc
    }
}

/// What a database restore did, in total and per tenant.
#[derive(Debug, Default)]
pub struct DatabaseRestore {
    pub total: RestoreStats,
    /// `(tenant_id, that tenant's stats)` in manifest order.
    pub tenants: Vec<(u64, RestoreStats)>,
}

/// Decrypt the database backup `bytes` and check that it holds the database
/// `name`. Writes nothing.
pub fn open_database_backup(
    state: &SharedState,
    name: &str,
    bytes: &[u8],
) -> Result<OpenedBackup, Error> {
    let kek = *backup_kek(state)?;
    let env = parse_encrypted(bytes, DEFAULT_MAX_TOTAL_BYTES, &kek)?;
    if env.meta.tenant_id != DATABASE_BACKUP_TENANT {
        return Err(Error::BadRequest {
            detail: "the backup object is a tenant backup, not a database backup; restore it \
                     with COPY tenant_restore(<tenant>) FROM STDIN"
                .into(),
        });
    }
    let mut sections = env.sections.into_iter();
    let manifest_section = sections
        .next()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_DATABASE_MANIFEST)
        .ok_or_else(|| Error::Internal {
            detail: "invalid backup format: the database backup has no manifest".into(),
        })?;
    let manifest: DatabaseBackupManifest =
        zerompk::from_msgpack(&manifest_section.body).map_err(|_| Error::Internal {
            detail: "invalid backup format: the database backup manifest is not decodable".into(),
        })?;
    if manifest.database != name {
        return Err(Error::BadRequest {
            detail: format!(
                "the backup holds database '{}', not '{name}'; restore it with RESTORE \
                 DATABASE {} FROM ...",
                manifest.database, manifest.database
            ),
        });
    }
    let tenants: Vec<(u64, Vec<u8>)> = sections.map(|s| (s.origin_node_id, s.body)).collect();
    if !tenants
        .iter()
        .map(|(id, _)| *id)
        .eq(manifest.tenants.iter().copied())
    {
        return Err(Error::Internal {
            detail: "invalid backup format: the database backup's tenant sections do not \
                     match its manifest"
                .into(),
        });
    }
    Ok(OpenedBackup { manifest, tenants })
}

/// Check every tenant envelope of `backup` as a DRY RUN, verification of the
/// envelope included. Writes nothing. The stats list every collection's rows,
/// which a restore admits its write quota against.
pub async fn check_database(
    state: &Arc<SharedState>,
    backup: &OpenedBackup,
    force: bool,
) -> Result<DatabaseRestore, Error> {
    restore_each(state, backup, true, force).await
}

/// Restore every tenant of `backup` through the tenant restore. Run
/// [`check_database`] first: a tenant refused there stops the restore before
/// any tenant writes.
pub async fn apply_database(
    state: &Arc<SharedState>,
    backup: &OpenedBackup,
    force: bool,
) -> Result<DatabaseRestore, Error> {
    restore_each(state, backup, false, force).await
}

async fn restore_each(
    state: &Arc<SharedState>,
    backup: &OpenedBackup,
    dry_run: bool,
    force: bool,
) -> Result<DatabaseRestore, Error> {
    let mut result = DatabaseRestore {
        total: RestoreStats {
            dry_run,
            ..Default::default()
        },
        tenants: Vec::with_capacity(backup.tenants.len()),
    };
    for (tenant_id, envelope) in &backup.tenants {
        let stats = super::restore_tenant(state, *tenant_id, envelope, dry_run, force).await?;
        result.total.absorb(&stats);
        result.tenants.push((*tenant_id, stats));
    }
    Ok(result)
}
