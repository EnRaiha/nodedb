// SPDX-License-Identifier: BUSL-1.1

//! Catalog rows an integration test writes straight to the catalog.
//!
//! The catalog refuses a collection row with no incarnation. A fixture that
//! bypasses the proposer builds its row here, stamped as the proposer stamps
//! a create.

use std::sync::OnceLock;

use nodedb::control::security::catalog::StoredCollection;
use nodedb_types::HlcClock;

/// A collection stamped as the proposer stamps a create: a fresh incarnation
/// and descriptor version 1. Each call names a new incarnation.
pub fn stamped_collection(tenant_id: u64, name: &str, owner: &str) -> StoredCollection {
    static CLOCK: OnceLock<HlcClock> = OnceLock::new();
    let hlc = CLOCK.get_or_init(HlcClock::new).now();
    StoredCollection {
        descriptor_version: 1,
        modification_hlc: hlc,
        incarnation: hlc,
        ..StoredCollection::new(tenant_id, name, owner)
    }
}
