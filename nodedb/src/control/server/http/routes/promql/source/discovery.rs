// SPDX-License-Identifier: BUSL-1.1

//! Which metric collections a selector reads.
//!
//! A metric is a timeseries collection named after the metric, with a
//! numeric `value` column. Remote write and OTLP both store metrics in this
//! shape. Discovery reads the replicated catalog, which is Control Plane
//! metadata. It never reads stored rows.

use std::sync::Arc;

use nodedb_physical::physical_plan::TieringPlan;
use nodedb_types::columnar::ColumnType;
use nodedb_types::{CollectionType, ColumnarProfile};

use crate::control::promql::{LabelMatchOp, PromqlError};
use crate::control::security::audit::{ArcAuditEmitter, NoopAuditEmitter};
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::Permission;
use crate::control::server::shared::authorization::{authorize_collection, authorize_database};

use super::scope::ReadScope;
use super::windows::SelectorRead;

/// The column holding a metric's sample value.
pub(crate) const VALUE_COLUMN: &str = "value";

/// A metric collection a selector reads.
#[derive(Debug, Clone)]
pub(crate) struct MetricCollection {
    pub name: String,
    /// The declared `TIME_KEY` column.
    pub time_key: String,
    /// The AUTO_TIER layout of the collection's retention policy. A raw
    /// read over a tiered span then fails on the owning core instead of
    /// returning a gap.
    pub tiering: Option<TieringPlan>,
}

/// The metric collections `read` selects, in catalog order.
///
/// A metric named exactly (`up`, or `{__name__="up"}`) that the caller cannot
/// read is a permission error. A metric that does not exist reads as no
/// series. Name regexes and nameless selectors skip collections the caller
/// cannot read, the way a catalog listing shows only visible objects.
pub(crate) fn metric_collections(
    scope: &ReadScope<'_>,
    read: &SelectorRead,
) -> crate::Result<Vec<MetricCollection>> {
    let exact = read.name.clone().or_else(|| {
        read.matchers
            .iter()
            .find(|m| m.name == "__name__" && m.op == LabelMatchOp::Equal)
            .map(|m| m.value.clone())
    });
    let name_matchers: Vec<_> = read
        .matchers
        .iter()
        .filter(|m| m.name == "__name__")
        .collect();
    let candidates = match exact {
        Some(name) => exact_collection(scope, &name)?.into_iter().collect(),
        None => readable_metric_collections(scope)?,
    };
    Ok(candidates
        .into_iter()
        .filter(|metric| name_matchers.iter().all(|m| m.matches(&metric.name)))
        .collect())
}

/// Every metric collection in the scope's database the caller can read.
pub(crate) fn readable_metric_collections(
    scope: &ReadScope<'_>,
) -> crate::Result<Vec<MetricCollection>> {
    let audit = ArcAuditEmitter(Arc::clone(&scope.state.audit));
    authorize_database(scope.identity, scope.database_id, &audit)?;
    let catalog = scope.state.credentials.catalog();
    let mut out = Vec::new();
    for stored in
        catalog.load_collections_for_tenant(scope.database_id, scope.identity.tenant_id.as_u64())?
    {
        if !stored.is_active {
            continue;
        }
        let Ok(metric) = metric_shape(scope, &stored) else {
            continue;
        };
        // A probe, not an access: a denial here only hides the name.
        if authorize_collection(
            scope.identity,
            scope.database_id,
            &stored.name,
            Permission::Read,
            &scope.state.permissions,
            &scope.state.roles,
            &NoopAuditEmitter,
        )
        .is_err()
        {
            continue;
        }
        out.push(metric);
    }
    Ok(out)
}

fn exact_collection(scope: &ReadScope<'_>, name: &str) -> crate::Result<Option<MetricCollection>> {
    let audit = ArcAuditEmitter(Arc::clone(&scope.state.audit));
    authorize_database(scope.identity, scope.database_id, &audit)?;
    let catalog = scope.state.credentials.catalog();
    let Some(stored) =
        catalog.get_collection(scope.database_id, scope.identity.tenant_id.as_u64(), name)?
    else {
        return Ok(None);
    };
    if !stored.is_active {
        return Ok(None);
    }
    authorize_collection(
        scope.identity,
        scope.database_id,
        name,
        Permission::Read,
        &scope.state.permissions,
        &scope.state.roles,
        &audit,
    )?;
    Ok(Some(metric_shape(scope, &stored)?))
}

fn metric_shape(
    scope: &ReadScope<'_>,
    stored: &StoredCollection,
) -> Result<MetricCollection, PromqlError> {
    let time_key = match &stored.collection_type {
        CollectionType::Columnar(ColumnarProfile::Timeseries { time_key, .. }) => time_key.clone(),
        CollectionType::Columnar(ColumnarProfile::Plain | ColumnarProfile::Spatial { .. })
        | CollectionType::Document(_)
        | CollectionType::KeyValue(_) => {
            return Err(not_a_metric(stored, "it is not a timeseries collection"));
        }
    };
    let numeric_value = stored.fields.iter().any(|(name, ty)| {
        name == VALUE_COLUMN
            && matches!(
                ty.split_whitespace()
                    .next()
                    .and_then(|t| t.parse::<ColumnType>().ok()),
                Some(ColumnType::Float64 | ColumnType::Int64)
            )
    });
    if !numeric_value {
        return Err(not_a_metric(stored, "it has no numeric `value` column"));
    }
    let tiering = scope
        .state
        .retention_policy_registry
        .get_for_collection(
            scope.database_id.as_u64(),
            scope.identity.tenant_id.as_u64(),
            &stored.name,
        )
        .filter(|policy| policy.auto_tier)
        .map(|policy| crate::control::planner::auto_tier::tiering_plan(&policy));
    Ok(MetricCollection {
        name: stored.name.clone(),
        time_key,
        tiering,
    })
}

fn not_a_metric(stored: &StoredCollection, detail: &str) -> PromqlError {
    PromqlError::NotAMetricCollection {
        collection: stored.name.clone(),
        detail: detail.to_owned(),
    }
}
