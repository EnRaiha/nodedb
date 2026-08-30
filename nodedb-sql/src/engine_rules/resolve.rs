// SPDX-License-Identifier: Apache-2.0

//! Engine-type to `EngineRules` implementation lookup.

use crate::types::EngineType;

use super::rules::EngineRules;
use super::{array, columnar, document_schemaless, document_strict, kv, spatial, timeseries};

/// Resolve the engine rules for a given engine type.
///
/// No catch-all — compiler enforces exhaustiveness.
pub fn resolve_engine_rules(engine: EngineType) -> &'static dyn EngineRules {
    match engine {
        EngineType::DocumentSchemaless => &document_schemaless::SchemalessRules,
        EngineType::DocumentStrict => &document_strict::StrictRules,
        EngineType::KeyValue => &kv::KvRules,
        EngineType::Columnar => &columnar::ColumnarRules,
        EngineType::Timeseries => &timeseries::TimeseriesRules,
        EngineType::Spatial => &spatial::SpatialRules,
        EngineType::Array => &array::ArrayRules,
    }
}
