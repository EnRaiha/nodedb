// SPDX-License-Identifier: BUSL-1.1

//! Skipping a trigger body an earlier firing of the same event committed.
//!
//! The trigger action lane fires an event again on a new owner when the
//! earlier owner's cursor commit did not land. A body fired from the Event
//! Plane commits its event's key with its writes, and every replica records
//! the key. The new owner finds it and does not run the body again.

use tracing::debug;

use crate::control::planner::procedural::executor::core::StatementExecutor;
use crate::control::security::catalog::trigger_types::StoredTrigger;

use super::fire_common::TriggerOutcome;

/// The outcome of `trigger`'s body when it does not run: `Some` with no error
/// when its key is recorded, `Some` with the error when the key cannot be
/// read, and `None` when the body runs.
pub(super) fn skipped(
    executor: &StatementExecutor<'_>,
    trigger: &StoredTrigger,
    collection: &str,
) -> Option<TriggerOutcome> {
    let error = match executor.already_applied() {
        Ok(false) => return None,
        Ok(true) => {
            debug!(
                trigger = %trigger.name,
                collection,
                "trigger body already applied for this event; not run again"
            );
            None
        }
        Err(error) => Some(crate::Error::BadRequest {
            detail: format!(
                "trigger '{}' on '{}': its applied key cannot be read: {error}",
                trigger.name, collection
            ),
        }),
    };
    Some(TriggerOutcome {
        trigger_name: trigger.name.clone(),
        error,
    })
}
