// SPDX-License-Identifier: Apache-2.0

//! Bulk-load (`COPY ... FROM`) planning: the target resolves to its engine
//! rules, and the rules answer how the engine loads rows.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::engine_rules::{self, BulkLoadParams, BulkLoadPlan};
use crate::error::Result;
use crate::types::EngineType;

/// Plan one chunk of a bulk load into `params.collection`, or say how its
/// engine loads rows when `params.cells` is empty.
///
/// An array lives in its own catalog namespace, so it resolves before the
/// collection lookup can miss it, as `TRUNCATE` resolves it. An unknown
/// target loads row by row: each row's `INSERT` reports the unknown table.
pub fn plan_bulk_load(params: BulkLoadParams, catalog: &dyn SqlCatalog) -> Result<BulkLoadPlan> {
    if catalog.array_exists(&params.collection) {
        return engine_rules::resolve_engine_rules(EngineType::Array).plan_bulk_load(params);
    }
    match catalog.get_collection(DatabaseId::DEFAULT, &params.collection)? {
        Some(info) => engine_rules::resolve_engine_rules(info.engine).plan_bulk_load(params),
        None => Ok(BulkLoadPlan::RowInserts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ArrayCatalogView, SqlCatalogError};
    use crate::error::SqlError;
    use crate::types::CollectionInfo;

    struct ArrayOnly;

    impl SqlCatalog for ArrayOnly {
        fn get_collection(
            &self,
            _: DatabaseId,
            _: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(None)
        }
        fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
            (name == "scene").then(|| ArrayCatalogView {
                name: name.into(),
                dims: Vec::new(),
                attrs: Vec::new(),
                tile_extents: Vec::new(),
                georef: None,
            })
        }
    }

    fn probe(name: &str) -> Result<BulkLoadPlan> {
        plan_bulk_load(
            BulkLoadParams {
                collection: name.into(),
                cells: Vec::new(),
            },
            &ArrayOnly,
        )
    }

    #[test]
    fn an_array_loads_in_cell_chunks() {
        assert!(matches!(probe("scene"), Ok(BulkLoadPlan::CellChunks(_))));
    }

    #[test]
    fn an_unknown_target_loads_row_by_row() {
        assert!(matches!(probe("missing"), Ok(BulkLoadPlan::RowInserts)));
    }

    #[test]
    fn timeseries_and_spatial_refuse_a_bulk_load() {
        for engine in [EngineType::Timeseries, EngineType::Spatial] {
            let refused =
                engine_rules::resolve_engine_rules(engine).plan_bulk_load(BulkLoadParams {
                    collection: "c".into(),
                    cells: Vec::new(),
                });
            assert!(matches!(refused, Err(SqlError::Unsupported { .. })));
        }
    }

    #[test]
    fn row_engines_load_row_by_row() {
        for engine in [
            EngineType::DocumentSchemaless,
            EngineType::DocumentStrict,
            EngineType::KeyValue,
            EngineType::Columnar,
            EngineType::ContinuousAggregate,
        ] {
            let plan = engine_rules::resolve_engine_rules(engine).plan_bulk_load(BulkLoadParams {
                collection: "c".into(),
                cells: Vec::new(),
            });
            assert!(matches!(plan, Ok(BulkLoadPlan::RowInserts)));
        }
    }
}
