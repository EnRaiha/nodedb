// SPDX-License-Identifier: BUSL-1.1

//! How long a metadata proposal and a DDL drain wait.

use std::time::Duration;

/// Default window of no apply progress after which a metadata propose
/// returns an error.
pub const DEFAULT_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default upper bound on how long a DDL drain will wait for
/// prior-version leases to release before giving up. Must be at
/// least `ClusterTransportTuning::descriptor_lease_duration_secs`
/// so an existing lease gets at least one full lifetime to
/// expire naturally. 35 seconds matches the 300s lease duration
/// plus a 30-second grace minus the typical 5-minute default
/// cut down for test budget.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(35);
