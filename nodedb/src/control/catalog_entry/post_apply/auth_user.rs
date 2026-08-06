// SPDX-License-Identifier: BUSL-1.1

//! Auth-user post-apply side effects — install the replicated record into the
//! in-memory `auth_users` cache, so the next request on this node sees the
//! status the entry carries, and restore the escalation ladder that produced
//! it so this node counts from the same rung.

use std::sync::Arc;

use crate::control::security::catalog::StoredAuthUser;
use crate::control::state::SharedState;

pub fn put(stored: StoredAuthUser, shared: Arc<SharedState>) {
    shared.auth_users.install_replicated(&stored);
    shared
        .escalation
        .hydrate_suspensions(&stored.id, stored.escalation_suspensions);
}
