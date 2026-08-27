// SPDX-License-Identifier: BUSL-1.1

//! The handles the Control Plane and the Data-Plane cores both hold.
//!
//! Each one is created before the cores are spawned and handed to
//! `SharedState::open` unchanged, so both planes reference ONE instance.

use std::sync::Arc;

use crate::bridge::dispatch::Dispatcher;
use crate::bridge::quiesce::CollectionQuiesce;
use crate::control::array_catalog::ArrayCatalogHandle;
use crate::control::metrics::SystemMetrics;

/// Handles shared with every Data-Plane core, taken by
/// [`SharedState::open`](crate::control::state::SharedState::open).
///
/// `system_metrics` lives here because the cores write the gauges the HTTP
/// `/metrics` route renders: a second registry on either side reads zero.
pub struct DataPlaneHandles {
    /// SPSC bridge to the cores.
    pub dispatcher: Dispatcher,
    /// Scan-quiesce registry every core consults before starting a scan.
    pub quiesce: Arc<CollectionQuiesce>,
    /// ND-array catalog every core resolves array plans against.
    pub array_catalog: ArrayCatalogHandle,
    /// Metrics registry the cores write and `/metrics` reads.
    pub system_metrics: Arc<SystemMetrics>,
}
