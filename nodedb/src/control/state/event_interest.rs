// SPDX-License-Identifier: BUSL-1.1

//! The registries whose collections the Data Plane emits on-demand write
//! events for.

use crate::event::interest::InterestSources;

use super::SharedState;

impl SharedState {
    /// The consumed-collection slices of every registry that feeds an Event
    /// Plane consumer. Each registry republishes its slice after every
    /// change, so the returned slices stay current.
    pub fn event_interest_sources(&self) -> InterestSources {
        InterestSources {
            triggers: self.trigger_registry.interest(),
            change_streams: self.stream_registry.interest(),
            event_definitions: self.credentials.catalog().event_definition_interest(),
            dml_audit: self.audit_dml_cache.interest(),
        }
    }
}
