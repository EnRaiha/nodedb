// SPDX-License-Identifier: BUSL-1.1

//! Shim that re-exports the shared test harness from
//! `nodedb-test-support`. The real implementation lives there so the
//! cluster integration test crate (`nodedb-cluster-tests`) can share
//! it without duplication. Per-file `pub use` lines preserve the
//! `common::pgwire_harness::TestServer` paths the existing tests use.

#[allow(unused_imports)]
pub use nodedb_test_support::{
    array_sync, cluster_harness, insert_returning_engines, jwks_fixture, make_cdc_event,
    native_harness, now_ms, pgwire_auth_helpers, pgwire_harness, tx_batch_helpers,
};
