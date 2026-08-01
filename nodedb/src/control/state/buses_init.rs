// SPDX-License-Identifier: BUSL-1.1

//! Helper for wiring up the security bus pair during SharedState construction.

use std::sync::{Arc, Mutex};

use crate::control::security::audit::AuditLog;
use crate::control::security::buses::{SessionInvalidationBus, UserChangeBus};
use crate::control::security::sessions::SessionRegistry;

/// Construct both security buses, subscribe the consumer receivers, spawn the
/// audit-log consumer task, and return the three values that `SharedState`
/// fields require.
///
/// `session_registry` is shared with the consumer so hard-revoke events drive
/// kill signals before audit rows are written.
pub(super) fn init_security_buses(
    audit: Arc<Mutex<AuditLog>>,
    session_registry: Arc<SessionRegistry>,
) -> (
    SessionInvalidationBus,
    UserChangeBus,
    Option<tokio::task::JoinHandle<()>>,
) {
    let (si_bus, si_rx) = SessionInvalidationBus::new();
    let (uc_bus, uc_rx) = UserChangeBus::new();
    let handle =
        crate::control::security::buses::spawn_bus_consumer(si_rx, uc_rx, audit, session_registry);
    (si_bus, uc_bus, handle)
}
