// SPDX-License-Identifier: BUSL-1.1

//! The verification section a backup records.
//!
//! Each data section is one node's disjoint share of one database, so each
//! collection part's tally is the sum of its sections' tallies.

use std::collections::{BTreeMap, HashMap};

use nodedb_types::backup_envelope::{
    CollectionVerification, DatabaseDataSection, SurrogateBindBlob,
};

use crate::Error;
use crate::control::backup::metadata::TenantDatabase;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantDataSnapshot};

use super::walk::{BindIndex, DocShape, Shapes, Tallies, Walk};

/// Encode the verification section of a backup of `tenant_id`.
///
/// `data_sections` are the `(origin_node_id, body)` data sections, each body
/// an encoded [`DatabaseDataSection`]. `binds` are the backup's primary-key
/// binds.
pub(in crate::control::backup) fn verification_section(
    state: &SharedState,
    tenant_id: u64,
    databases: &[TenantDatabase],
    data_sections: &[(u64, Vec<u8>)],
    binds: &[SurrogateBindBlob],
) -> Result<Vec<u8>, Error> {
    let kek = state.wal.encryption_key();
    let shapes: HashMap<u64, Shapes> = databases
        .iter()
        .map(|database| {
            let shapes = database
                .collections
                .iter()
                .map(|coll| (coll.name.clone(), DocShape::of(coll)))
                .collect();
            (database.id().as_u64(), shapes)
        })
        .collect();
    let mut bind_index: HashMap<u64, BindIndex> = HashMap::new();
    for database in databases {
        let id = database.id().as_u64();
        let own = binds
            .iter()
            .filter(|b| b.database_id == id && b.tenant_id == tenant_id)
            .map(|b| (b.collection.as_str(), b.surrogate, b.pk.as_slice()));
        bind_index.insert(id, BindIndex::new(own));
    }

    let no_shapes = Shapes::new();
    let no_binds = BindIndex::default();
    let mut tallies: BTreeMap<u64, Tallies> = BTreeMap::new();
    for (_node_id, body) in data_sections {
        let section: DatabaseDataSection = zerompk::from_msgpack(body).map_err(decode)?;
        let snap: TenantDataSnapshot = zerompk::from_msgpack(&section.snapshot).map_err(decode)?;
        let database_id = section.database_id;
        let walk = Walk {
            tenant_id,
            database_id: DatabaseId::new(database_id),
            shapes: shapes.get(&database_id).unwrap_or(&no_shapes),
            binds: bind_index.get(&database_id).unwrap_or(&no_binds),
            kek,
        };
        let database = tallies.entry(database_id).or_default();
        walk.snapshot(&snap, &mut |row| {
            database
                .entry((row.collection, row.part))
                .or_default()
                .add(&row.hash);
        })?;
    }

    let records: Vec<CollectionVerification> = tallies
        .into_iter()
        .flat_map(|(database_id, parts)| {
            parts
                .into_iter()
                .map(move |((collection, part), tally)| CollectionVerification {
                    database_id,
                    collection,
                    part,
                    tally,
                })
        })
        .collect();
    super::super::metadata::encode_section_part("verification", &records)
}

fn decode(e: impl std::fmt::Display) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup verification: decode a data section: {e}"),
    }
}
