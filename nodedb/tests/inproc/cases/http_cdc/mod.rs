// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for CDC endpoints.
//!
//! Endpoints covered:
//! - GET /v1/cdc/{collection}       — SSE change-data-capture stream
//! - GET /v1/cdc/{collection}/poll  — poll-based CDC
//!
//! Contracts asserted:
//! - Routes exist (not 404) under Trust mode
//! - 401 without bearer token under Password mode
//! - Cross-tenant tenant_id query param rejected
//! - Wrong HTTP method → 405
//! - Poll results stay inside the selected database and the caller's tenant
//! - Opaque cursors resume poll and SSE streams without a repeat or a gap
//! - A role without a collection grant is refused before any event leaves

mod cursor;
mod routes_and_auth;
mod scope;
