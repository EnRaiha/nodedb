// SPDX-License-Identifier: BUSL-1.1

//! The envelope side of restore verification.
//!
//! Before its first write, a restore recomputes every collection part's tally
//! from the envelope's own rows and compares it with the tally the backup
//! recorded. A mismatch refuses the envelope: nothing is written. The
//! recomputation also keeps what the destination check needs: the expected
//! tallies, the key of every keyed row, and every row with a TTL.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use nodedb_types::backup_envelope::{
    CollectionVerification, Envelope, SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_VERIFICATION,
    StoredCollectionBlob, VerificationMismatch, VerificationPhase,
};

use crate::Error;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot};

use super::canonical::KeyHash;
use super::walk::{BindIndex, DocShape, PartKey, Shapes, Tallies, Walk};

/// A backed-up row with a TTL.
pub(crate) struct ExpiringRow {
    pub part: PartKey,
    pub key: KeyHash,
    pub hash: [u8; 32],
    pub expire_at_ms: u64,
}

/// The rows the destination must hold for one backed-up database.
#[derive(Default)]
pub(crate) struct ExpectedRows {
    pub tallies: Tallies,
    /// The key of every row of each keyed part.
    pub keys: HashMap<PartKey, HashSet<KeyHash>>,
    pub expiring: Vec<ExpiringRow>,
}

/// Walk `snap` into the tallies, keys and TTL rows the destination must hold.
pub(crate) fn expect_rows(
    walk: &Walk<'_>,
    snap: &TenantDataSnapshot,
) -> Result<ExpectedRows, Error> {
    let mut rows = ExpectedRows::default();
    walk.snapshot(snap, &mut |row| {
        let part: PartKey = (row.collection, row.part);
        rows.tallies.entry(part.clone()).or_default().add(&row.hash);
        if let Some(key) = row.key {
            rows.keys.entry(part.clone()).or_default().insert(key);
            if row.expire_at_ms != 0 {
                rows.expiring.push(ExpiringRow {
                    part,
                    key,
                    hash: row.hash,
                    expire_at_ms: row.expire_at_ms,
                });
            }
        }
    })?;
    Ok(rows)
}

/// What the destination must hold for one backed-up database.
#[derive(Default)]
pub(crate) struct DatabaseExpectation {
    pub shapes: Shapes,
    pub rows: ExpectedRows,
}

impl DatabaseExpectation {
    /// Every collection the database's rows belong to.
    pub fn collections(&self) -> BTreeSet<String> {
        self.rows
            .tallies
            .keys()
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// What the destination must hold, by source database id.
#[derive(Default)]
pub(crate) struct Expectation {
    pub databases: BTreeMap<u64, DatabaseExpectation>,
}

/// Recompute every tally of `env` from `merged`, its data sections merged per
/// source database, and compare each with the tally the envelope records.
pub(crate) fn expect_envelope(
    state: &SharedState,
    tenant_id: u64,
    env: &Envelope,
    merged: &BTreeMap<u64, TenantDataSnapshot>,
) -> Result<Expectation, Error> {
    let recorded = recorded_tallies(env)?;
    let mut shapes = envelope_shapes(env)?;
    let kek = state.wal.encryption_key();

    let mut expectation = Expectation::default();
    for (source, snap) in merged {
        let binds = BindIndex::new(
            snap.surrogate_pk
                .iter()
                .filter(|b| b.tenant_id == tenant_id)
                .map(|b| (b.collection.as_str(), b.surrogate, b.pk.as_slice())),
        );
        let mut database = DatabaseExpectation {
            shapes: shapes.remove(source).unwrap_or_default(),
            ..Default::default()
        };
        let walk = Walk {
            tenant_id,
            database_id: DatabaseId::new(*source),
            shapes: &database.shapes,
            binds: &binds,
            kek,
        };
        database.rows = expect_rows(&walk, snap)?;
        expectation.databases.insert(*source, database);
    }

    let mut mismatches = Vec::new();
    let sources: BTreeSet<u64> = recorded
        .keys()
        .copied()
        .chain(expectation.databases.keys().copied())
        .collect();
    let empty = Tallies::new();
    for source in sources {
        let found = expectation
            .databases
            .get(&source)
            .map_or(&empty, |d| &d.rows.tallies);
        mismatches.extend(compare(
            source,
            recorded.get(&source).unwrap_or(&empty),
            found,
        ));
    }
    if mismatches.is_empty() {
        Ok(expectation)
    } else {
        Err(Error::RestoreVerificationFailed {
            phase: VerificationPhase::Envelope,
            mismatches,
        })
    }
}

/// What the destination must hold once `snap` is re-issued: `snap` is the
/// capture of `tenant_id`'s `collections` in `database_id`. MOVE TENANT
/// checks its target with it.
pub(crate) fn expect_capture(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    collections: &[StoredCollection],
    snap: &TenantDataSnapshot,
) -> Result<Expectation, Error> {
    let binds = BindIndex::new(
        snap.surrogate_pk
            .iter()
            .filter(|b| b.tenant_id == tenant_id)
            .map(|b| (b.collection.as_str(), b.surrogate, b.pk.as_slice())),
    );
    let mut database = DatabaseExpectation {
        shapes: collections
            .iter()
            .filter(|coll| coll.tenant_id == tenant_id)
            .map(|coll| (coll.name.clone(), DocShape::of(coll)))
            .collect(),
        ..Default::default()
    };
    let walk = Walk {
        tenant_id,
        database_id,
        shapes: &database.shapes,
        binds: &binds,
        kek: state.wal.encryption_key(),
    };
    database.rows = expect_rows(&walk, snap)?;
    let mut expectation = Expectation::default();
    expectation.databases.insert(database_id.as_u64(), database);
    Ok(expectation)
}

/// Every collection part whose tally in `found` differs from `expected`.
pub(crate) fn compare(
    database_id: u64,
    expected: &Tallies,
    found: &Tallies,
) -> Vec<VerificationMismatch> {
    let parts: BTreeSet<&PartKey> = expected.keys().chain(found.keys()).collect();
    parts
        .into_iter()
        .filter_map(|part| {
            let expected = expected.get(part).copied().unwrap_or_default();
            let found = found.get(part).copied().unwrap_or_default();
            (expected != found).then(|| VerificationMismatch {
                database_id,
                collection: part.0.clone(),
                part: part.1,
                expected,
                found,
            })
        })
        .collect()
}

/// The tallies the envelope's one verification section records, by source
/// database id.
fn recorded_tallies(env: &Envelope) -> Result<BTreeMap<u64, Tallies>, Error> {
    let mut sections = env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_VERIFICATION);
    let (Some(section), None) = (sections.next(), sections.next()) else {
        return Err(Error::Internal {
            detail: "invalid backup format: the backup must carry exactly one verification \
                     section"
                .into(),
        });
    };
    let records: Vec<CollectionVerification> =
        zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
            detail: "invalid backup format: verification section is not decodable".into(),
        })?;
    let mut recorded: BTreeMap<u64, Tallies> = BTreeMap::new();
    for record in records {
        let database = recorded.entry(record.database_id).or_default();
        if database
            .insert((record.collection, record.part), record.tally)
            .is_some()
        {
            return Err(Error::Internal {
                detail: "invalid backup format: verification section repeats a collection".into(),
            });
        }
    }
    Ok(recorded)
}

/// The document shape of every collection in the envelope's catalog rows, by
/// source database id.
fn envelope_shapes(env: &Envelope) -> Result<HashMap<u64, Shapes>, Error> {
    let mut shapes: HashMap<u64, Shapes> = HashMap::new();
    for section in env
        .sections
        .iter()
        .filter(|s| s.origin_node_id == SECTION_ORIGIN_CATALOG_ROWS)
    {
        let blobs: Vec<StoredCollectionBlob> =
            zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                detail: "invalid backup format: catalog-rows section is not decodable".into(),
            })?;
        for blob in blobs {
            let coll: StoredCollection =
                zerompk::from_msgpack(&blob.bytes).map_err(|_| Error::Internal {
                    detail: format!(
                        "invalid backup format: catalog row of '{}' is not decodable",
                        blob.name
                    ),
                })?;
            shapes
                .entry(blob.database_id)
                .or_default()
                .insert(coll.name.clone(), DocShape::of(&coll));
        }
    }
    Ok(shapes)
}
